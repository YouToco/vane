package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/YouToco/vane/types"
)

// fakePushTrigger 是 PushTrigger 的可编程假实现。
type fakePushTrigger struct {
	runID string
	err   error
	calls int
}

func (f *fakePushTrigger) TriggerPushNow(_ context.Context, _ int64) (string, error) {
	f.calls++
	return f.runID, f.err
}

// push_now 的错误分流：并发护栏的确定性拒绝（CodeValidation）要把文案回给模型
// 自纠而不是上抛——该分支在 TriggerPushNow 加 WorkflowExecutionErrorWhenAlreadyStarted
// 之前是死代码，专门补消费端覆盖，防契约在 scheduler 与 tools 两端各自漂移。
func TestPushNowTool_Execute(t *testing.T) {
	t.Run("成功触发返回runID文案", func(t *testing.T) {
		ft := &fakePushTrigger{runID: "push-agent-7"}
		tool := &pushNowTool{pusher: ft}
		got, err := tool.Execute(context.Background(), 7, json.RawMessage("{}"))
		if err != nil || !strings.Contains(got, "push-agent-7") || !strings.Contains(got, "已触发") {
			t.Fatalf("成功触发应返回含 runID 的文案, 实得 got=%q err=%v", got, err)
		}
		if ft.calls != 1 {
			t.Fatalf("应恰好触发 1 次, 实得 %d", ft.calls)
		}
	})

	t.Run("已在进行按文案回模型不上抛", func(t *testing.T) {
		// Cause 按生产形态给：TriggerPushNow 恒以 Temporal serviceerror 为 Cause，
		// 服务端原文不得跟着 Error() 一起进模型上下文。
		ae := types.NewAppError(types.CodeValidation,
			"已有一次推送正在进行，请等它完成后再触发",
			fmt.Errorf("Workflow execution is already running. WorkflowId: push-agent-7"))
		ae.Retryable = false
		tool := &pushNowTool{pusher: &fakePushTrigger{err: ae}}
		got, err := tool.Execute(context.Background(), 7, json.RawMessage("{}"))
		if err != nil {
			t.Fatalf("确定性拒绝不应上抛, 实得 err=%v", err)
		}
		if !strings.Contains(got, "已有一次推送正在进行") {
			t.Fatalf("应把拒绝文案回给模型, 实得 %q", got)
		}
		if strings.Contains(got, "already running") || strings.Contains(got, "VALIDATION") {
			t.Fatalf("拒绝文案不得携带错误链/错误码, 实得 %q", got)
		}
	})

	t.Run("基础设施错误照旧上抛", func(t *testing.T) {
		cause := types.NewAppError(types.CodeInternal, "触发即时推送失败", fmt.Errorf("temporal down"))
		tool := &pushNowTool{pusher: &fakePushTrigger{err: cause}}
		got, err := tool.Execute(context.Background(), 7, json.RawMessage("{}"))
		if err == nil || got != "" {
			t.Fatalf("基础设施错误应上抛, 实得 got=%q err=%v", got, err)
		}
		if !errors.Is(err, types.ErrInternal) {
			t.Fatalf("应保留原错误链, 实得 %v", err)
		}
	})
}

// ============================================================
// view_profile（M5 契约 §12.3）：读工具，画像为空是常态起点而非错误。
// fakeStore（loop_test.go）同时满足 profileStore 窄接口，无需数据库。
// ============================================================

func TestViewProfileTool(t *testing.T) {
	// 读工具红线：Mutating=true 会让 loop 把它挂起去出确认卡——查画像不该要人点确认。
	if (&viewProfileTool{}).Mutating() {
		t.Fatal("view_profile 必须是读工具（Mutating=false）")
	}

	t.Run("有画像时渲染中文各字段", func(t *testing.T) {
		fs := newFakeStore()
		fs.profiles[7] = &types.Profile{
			UserID:     7,
			Industry:   "金融",
			Occupation: "量化研究员",
			Tags:       []string{"AI", "宏观"},
			Summary:    "关注 AI 与宏观经济。",
		}
		got, err := (&viewProfileTool{st: fs}).Execute(context.Background(), 7, json.RawMessage("{}"))
		if err != nil {
			t.Fatalf("查画像不应报错: %v", err)
		}
		want := "当前画像——行业：金融；职业：量化研究员；关注标签：AI、宏观；摘要：关注 AI 与宏观经济。"
		if got != want {
			t.Fatalf("渲染不符:\n实得 %q\n期望 %q", got, want)
		}
	})

	t.Run("字段为空时标注未设置", func(t *testing.T) {
		// 模型据此知道缺什么、该引导采集什么；summary 归演化独有，为空整段省略，
		// 不引导模型对它下手。
		fs := newFakeStore()
		fs.profiles[7] = &types.Profile{UserID: 7}
		got, err := (&viewProfileTool{st: fs}).Execute(context.Background(), 7, json.RawMessage("{}"))
		if err != nil {
			t.Fatalf("查画像不应报错: %v", err)
		}
		want := "当前画像——行业：（未设置）；职业：（未设置）；关注标签：（未设置）"
		if got != want {
			t.Fatalf("空字段应标注未设置且不带摘要段:\n实得 %q\n期望 %q", got, want)
		}
	})

	t.Run("NotFound 回固定引导文案且不报错", func(t *testing.T) {
		fs := newFakeStore() // 无画像行
		got, err := (&viewProfileTool{st: fs}).Execute(context.Background(), 7, json.RawMessage("{}"))
		if err != nil {
			t.Fatalf("画像为空是常态起点, 不应报错: %v", err)
		}
		// 契约 §12.3 锁死的文案，逐字钉住。
		want := "画像为空：还不了解你。可以告诉我你的行业、职业和关注的主题。"
		if got != want {
			t.Fatalf("空画像文案不符:\n实得 %q\n期望 %q", got, want)
		}
	})

	t.Run("非 NotFound 的 DB 错误照旧上抛", func(t *testing.T) {
		fs := newFakeStore()
		fs.profileGetErr = types.NewAppError(types.CodeDatabase, "连接池耗尽", nil)
		got, err := (&viewProfileTool{st: fs}).Execute(context.Background(), 7, json.RawMessage("{}"))
		if err == nil || got != "" {
			t.Fatalf("基础设施错误应上抛（不得伪装成「画像为空」骗模型去引导采集）, 实得 got=%q err=%v", got, err)
		}
	})
}

// ============================================================
// update_profile（M5 契约 §12.3）：写工具，走 M4 标准确认卡（首采不特例）。
// ============================================================

func TestUpdateProfileTool(t *testing.T) {
	// M4 不变式（AI 出预填、人点执行）：写画像必须走确认卡。
	if !(&updateProfileTool{}).Mutating() {
		t.Fatal("update_profile 必须是写工具（Mutating=true），否则模型可绕过确认卡直接改画像")
	}

	t.Run("三字段全缺省回自纠文案且不触库", func(t *testing.T) {
		fs := newFakeStore()
		got, err := (&updateProfileTool{st: fs}).Execute(context.Background(), 7, json.RawMessage(`{}`))
		if err != nil {
			t.Fatalf("全缺省是模型可自纠的确定性失败, 不应上抛: %v", err)
		}
		if !strings.Contains(got, "没有提供任何要修改的字段") {
			t.Fatalf("应回自纠文案, 实得 %q", got)
		}
		if len(fs.upsertCalls) != 0 {
			t.Fatalf("全缺省不得触库, 实得 %+v", fs.upsertCalls)
		}
	})

	t.Run("参数非法 JSON 回自纠文案且不触库", func(t *testing.T) {
		fs := newFakeStore()
		got, err := (&updateProfileTool{st: fs}).Execute(context.Background(), 7, json.RawMessage(`{"industry":`))
		if err != nil {
			t.Fatalf("非法 JSON 是模型可自纠的确定性失败, 不应上抛: %v", err)
		}
		if !strings.Contains(got, "不是合法 JSON") {
			t.Fatalf("应回自纠文案, 实得 %q", got)
		}
		if len(fs.upsertCalls) != 0 {
			t.Fatalf("非法参数不得触库, 实得 %+v", fs.upsertCalls)
		}
	})

	t.Run("只提供 industry 时其余字段传 nil", func(t *testing.T) {
		fs := newFakeStore()
		got, err := (&updateProfileTool{st: fs}).Execute(context.Background(), 7,
			json.RawMessage(`{"industry":"金融"}`))
		if err != nil {
			t.Fatalf("Execute 意外报错: %v", err)
		}
		if len(fs.upsertCalls) != 1 {
			t.Fatalf("应恰好调用 UpsertProfileFields 1 次, 实得 %d", len(fs.upsertCalls))
		}
		c := fs.upsertCalls[0]
		if c.userID != 7 {
			t.Fatalf("userID = %d, 期望 7", c.userID)
		}
		if c.industry == nil || *c.industry != "金融" {
			t.Fatalf("industry 应非 nil 且为「金融」, 实得 %v", c.industry)
		}
		// nil=不改：未提供的字段传非 nil 会把它误改成空串。
		if c.occupation != nil {
			t.Fatalf("未提供的 occupation 必须传 nil（nil=不改）, 实得 %q", *c.occupation)
		}
		if c.tags != nil {
			t.Fatalf("未提供的 tags 必须传 nil（nil=不改）, 实得 %+v", c.tags)
		}
		if !strings.Contains(got, "画像已更新") || !strings.Contains(got, "行业：金融") {
			t.Fatalf("结果应回执更新后的画像, 实得 %q", got)
		}
	})

	t.Run("tags 超 12 截前 12", func(t *testing.T) {
		fs := newFakeStore()
		if _, err := (&updateProfileTool{st: fs}).Execute(context.Background(), 7,
			json.RawMessage(tagsArgs(15))); err != nil {
			t.Fatalf("超限应截断而非报错（人工替换不得因超限整次作废）: %v", err)
		}
		if len(fs.upsertCalls) != 1 {
			t.Fatalf("应恰好触库 1 次, 实得 %d", len(fs.upsertCalls))
		}
		got := fs.upsertCalls[0].tags
		if len(got) != maxProfileTags {
			t.Fatalf("tags 应截到 %d 个, 实得 %d: %+v", maxProfileTags, len(got), got)
		}
		// 截「前」12 而非后 12：与演化上限一致，人工整体替换不得静默丢演化标签。
		if got[0] != "tag1" || got[11] != "tag12" {
			t.Fatalf("应保留前 12 个, 实得 首=%q 末=%q", got[0], got[11])
		}
	})

	t.Run("tags 恰好 12 个不截断", func(t *testing.T) {
		fs := newFakeStore()
		if _, err := (&updateProfileTool{st: fs}).Execute(context.Background(), 7,
			json.RawMessage(tagsArgs(12))); err != nil {
			t.Fatalf("Execute 意外报错: %v", err)
		}
		if got := fs.upsertCalls[0].tags; len(got) != 12 {
			t.Fatalf("边界值 12 不应被截, 实得 %d", len(got))
		}
	})

	// nil（未提供）vs 空数组（显式清空）的语义区别——产品代码实际语义：
	// updateProfileArgs.Tags 是 []string，`{}` 解出 nil、`{"tags":[]}` 解出非 nil 空切片；
	// capProfileTags 原样透传 nil-ness；store 侧 nil=不改、非 nil=整体替换。
	// 两者在同一条链路上必须始终可区分，否则「清空标签」会退化成「不改」（用户删不掉标签）。
	t.Run("tags 显式空数组传非 nil 空切片即清空", func(t *testing.T) {
		fs := newFakeStore()
		fs.profiles[7] = &types.Profile{UserID: 7, Tags: []string{"AI", "宏观"}}
		got, err := (&updateProfileTool{st: fs}).Execute(context.Background(), 7,
			json.RawMessage(`{"tags":[]}`))
		if err != nil {
			t.Fatalf("Execute 意外报错: %v", err)
		}
		if len(fs.upsertCalls) != 1 {
			t.Fatalf("显式空数组是有效修改, 应触库 1 次, 实得 %d", len(fs.upsertCalls))
		}
		if c := fs.upsertCalls[0]; c.tags == nil {
			t.Fatal("显式空数组必须以非 nil 空切片传给 store（非 nil=整体替换=清空）；传 nil 会退化成「不改」，用户就再也删不掉标签")
		} else if len(c.tags) != 0 {
			t.Fatalf("空数组应原样传空, 实得 %+v", c.tags)
		}
		// fake 对齐生产语义（非 nil 整体替换）→ 标签真被清空。
		if tags := fs.profiles[7].Tags; len(tags) != 0 {
			t.Fatalf("显式空数组应清空标签, 实得 %+v", tags)
		}
		if !strings.Contains(got, "关注标签：（未设置）") {
			t.Fatalf("回执应体现标签已清空, 实得 %q", got)
		}
	})

	t.Run("tags 未提供时不动既有标签", func(t *testing.T) {
		fs := newFakeStore()
		fs.profiles[7] = &types.Profile{UserID: 7, Tags: []string{"AI", "宏观"}}
		got, err := (&updateProfileTool{st: fs}).Execute(context.Background(), 7,
			json.RawMessage(`{"occupation":"量化研究员"}`))
		if err != nil {
			t.Fatalf("Execute 意外报错: %v", err)
		}
		if c := fs.upsertCalls[0]; c.tags != nil {
			t.Fatalf("未提供 tags 必须传 nil, 实得 %+v", c.tags)
		}
		if tags := fs.profiles[7].Tags; len(tags) != 2 {
			t.Fatalf("未提供 tags 时既有标签一个都不能丢, 实得 %+v", tags)
		}
		if !strings.Contains(got, "关注标签：AI、宏观") {
			t.Fatalf("回执应仍含既有标签, 实得 %q", got)
		}
	})

	t.Run("DB 错误照旧上抛", func(t *testing.T) {
		fs := newFakeStore()
		fs.upsertErr = types.NewAppError(types.CodeDatabase, "写入失败", nil)
		got, err := (&updateProfileTool{st: fs}).Execute(context.Background(), 7,
			json.RawMessage(`{"industry":"金融"}`))
		if err == nil || got != "" {
			t.Fatalf("基础设施错误应上抛, 实得 got=%q err=%v", got, err)
		}
	})
}

// Summarize 只列提供的字段（契约 §12.3）：确认卡如实展示本次会改什么。
// 未提供的字段绝不出现——「不改」与「清空」在卡面上必须可区分，否则用户是在为
// 一个自己看不见的变更点确认。
func TestUpdateProfileTool_Summarize(t *testing.T) {
	tool := &updateProfileTool{}

	t.Run("只列提供的字段", func(t *testing.T) {
		got := tool.Summarize(json.RawMessage(`{"industry":"金融"}`))
		if got != "更新画像：行业改为「金融」" {
			t.Fatalf("摘要不符, 实得 %q", got)
		}
		if strings.Contains(got, "职业") || strings.Contains(got, "标签") {
			t.Fatalf("未提供的字段绝不能出现在确认卡上, 实得 %q", got)
		}
	})

	t.Run("多字段以分号连接", func(t *testing.T) {
		got := tool.Summarize(json.RawMessage(`{"industry":"金融","occupation":"量化研究员","tags":["AI","宏观"]}`))
		want := "更新画像：行业改为「金融」；职业改为「量化研究员」；关注标签整体替换为「AI、宏观」"
		if got != want {
			t.Fatalf("摘要不符:\n实得 %q\n期望 %q", got, want)
		}
	})

	t.Run("空串是显式清空", func(t *testing.T) {
		got := tool.Summarize(json.RawMessage(`{"occupation":""}`))
		if got != "更新画像：清空职业" {
			t.Fatalf("空串应说人话「清空」, 实得 %q", got)
		}
	})

	t.Run("空数组显示清空关注标签", func(t *testing.T) {
		got := tool.Summarize(json.RawMessage(`{"tags":[]}`))
		if got != "更新画像：清空关注标签" {
			t.Fatalf("空数组应显示清空, 实得 %q", got)
		}
	})

	t.Run("展示截断后的实际生效值", func(t *testing.T) {
		// 卡面列 13 个而只落 12 个是对用户撒谎。
		got := tool.Summarize(json.RawMessage(tagsArgs(13)))
		if !strings.Contains(got, "tag12") {
			t.Fatalf("摘要应含第 12 个标签, 实得 %q", got)
		}
		if strings.Contains(got, "tag13") {
			t.Fatalf("卡面不得列出会被截掉的标签, 实得 %q", got)
		}
	})

	t.Run("全缺省时明说不会有变更", func(t *testing.T) {
		got := tool.Summarize(json.RawMessage(`{}`))
		if got != "更新画像（未提供任何字段，确认后不会有变更）" {
			t.Fatalf("摘要不符, 实得 %q", got)
		}
	})

	t.Run("非法 JSON 兜底展示原始参数", func(t *testing.T) {
		// Summarize 无 error 通道，不能失败：卡片恒有内容可读。
		got := tool.Summarize(json.RawMessage(`{"industry":`))
		if !strings.Contains(got, "更新画像") || !strings.Contains(got, "参数未能解析") {
			t.Fatalf("应走 summarizeFallback, 实得 %q", got)
		}
	})
}

// tagsArgs 造一个含 n 个标签（tag1..tagN）的 update_profile 参数 JSON。
func tagsArgs(n int) string {
	tags := make([]string, n)
	for i := range tags {
		tags[i] = fmt.Sprintf("tag%d", i+1)
	}
	raw, err := json.Marshal(map[string][]string{"tags": tags})
	if err != nil {
		panic(err)
	}
	return string(raw)
}
