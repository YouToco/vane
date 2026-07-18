package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/YouToco/vane/scheduler"
	"github.com/YouToco/vane/sourcecatalog"
	"github.com/YouToco/vane/sourcespec"
	"github.com/YouToco/vane/types"
	"github.com/YouToco/vane/workflow"
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

// TestAddSourceDescriptionDerivesUnavailableFromCatalog 锁住审计修复：add_source 工具说明里
// 「不支持的能力及原因」必须**派生自 sourcecatalog**，而非手抄进 schema 的会漂移的副本。
// 这是「注册表被 fetcher/sourcespec/agent 三处共用」中 agent 那一处的真接线证明——
// 若有人把 x/search 的 Reason 改回硬编码、或 Description 不再读注册表，本用例会红。
func TestAddSourceDescriptionDerivesUnavailableFromCatalog(t *testing.T) {
	desc := (&addSourceTool{}).Description()

	entry, ok := sourcecatalog.Lookup(types.PlatformX, types.CapSearch)
	if !ok || entry.Available() {
		t.Fatal("前提失效：x/search 应是 sourcecatalog 里的 Unavailable 条目")
	}
	// 说明必须逐字包含注册表里的 Reason（派生而非改写），且点名该能力。
	if !strings.Contains(desc, entry.Reason) {
		t.Errorf("工具说明未含注册表派生的 x/search Reason（疑似硬编码/未读注册表）。\ndesc=%q\nreason=%q", desc, entry.Reason)
	}
	if !strings.Contains(desc, "x/search") {
		t.Errorf("工具说明应点名 x/search，实际：%q", desc)
	}
}

// TestSpecFromArgs_ParamMapping 守住 add_source 的 **agent 独有映射层**（审计 M-3）：
// 每个 capability 的入参必须落到 sourcespec.Build 认得的 param 键上。addSourceTool.Execute
// 持具体 *store.Store 不可 fake，此前这层「8 字段 → params」的翻译零测试——拼错键名
// （screen_name→screenname）不会被 sourcespec 自己的用例发现，只会在生产里产出错误的
// 确认卡预填。抽出纯函数 specFromArgs 后逐 capability 断言：映射产出的 spec 能被 Build 成
// 合法 Source，且关键入参进了幂等键（键名映错则 Build 因缺必填参数而拒绝，本用例即红）。
func TestSpecFromArgs_ParamMapping(t *testing.T) {
	cases := []struct {
		name     string
		args     addSourceArgs
		platform types.Platform
		cap      types.Capability
		wantIn   string // 关键入参必须出现在产出 Source 的 URL（幂等键）里
	}{
		{"web/feed", addSourceArgs{Platform: "web", Capability: "feed", URL: "https://example.com/feed.xml"}, types.PlatformWeb, types.CapFeed, "example.com/feed.xml"},
		{"web/search", addSourceArgs{Platform: "web", Capability: "search", Query: "OpenAI"}, types.PlatformWeb, types.CapSearch, "OpenAI"},
		{"xhs/search", addSourceArgs{Platform: "xhs", Capability: "search", Keyword: "meizhuang"}, types.PlatformXHS, types.CapSearch, "meizhuang"},
		{"xhs/user_posts", addSourceArgs{Platform: "xhs", Capability: "user_posts", UserID: "6a5578b3000000000e03cc00"}, types.PlatformXHS, types.CapUserPosts, "6a5578b3000000000e03cc00"},
		{"x/user_posts", addSourceArgs{Platform: "x", Capability: "user_posts", ScreenName: "OpenAI"}, types.PlatformX, types.CapUserPosts, "OpenAI"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src, msg := sourcespec.Build(specFromArgs(tc.args))
			if msg != "" {
				t.Fatalf("映射后 Build 应成功（键名拼错会在此暴露）, 实得拒绝: %s", msg)
			}
			if src.Platform != tc.platform || src.Capability != tc.cap {
				t.Fatalf("平台/能力不符: %s/%s, 期望 %s/%s", src.Platform, src.Capability, tc.platform, tc.cap)
			}
			if !strings.Contains(src.URL, tc.wantIn) {
				t.Fatalf("关键入参未进入幂等键（疑似映射到错误 param 键）: URL=%q 期望含 %q", src.URL, tc.wantIn)
			}
		})
	}
}

// TestSpecFromArgs_CategoriesMarshaled 单独守 categories 的 JSON 序列化分支
// （唯一非直传的映射：[]string → JSON 字符串进 params["categories"]）。
func TestSpecFromArgs_CategoriesMarshaled(t *testing.T) {
	spec := specFromArgs(addSourceArgs{
		Platform: "web", Capability: "feed", URL: "https://example.com/feed.xml",
		Categories: []string{"Product", "Research"},
	})
	if got := spec.Params["categories"]; got != `["Product","Research"]` {
		t.Fatalf("categories 应序列化为 JSON 数组字符串, 实得 %q", got)
	}
}

// TestSpecFromArgs_IncludeDomainsMarshaled 守 D-2 的 include_domains 映射分支
// （[]string → JSON 字符串进 params，与 categories 同路径），并端到端进幂等键。
func TestSpecFromArgs_IncludeDomainsMarshaled(t *testing.T) {
	spec := specFromArgs(addSourceArgs{
		Platform: "web", Capability: "search", Query: "Claude release",
		IncludeDomains: []string{"anthropic.com", "claude.com"},
	})
	if got := spec.Params["include_domains"]; got != `["anthropic.com","claude.com"]` {
		t.Fatalf("include_domains 应序列化为 JSON 数组字符串, 实得 %q", got)
	}
	src, msg := sourcespec.Build(spec)
	if msg != "" {
		t.Fatalf("Build 应成功: %s", msg)
	}
	if !strings.Contains(src.URL, "include_domains=") {
		t.Fatalf("include_domains 未进幂等键: %q", src.URL)
	}
}

// TestAddSourceTool_Summarize 覆盖确认卡文案的每个 capability 分支（审计 M-3）：
// 预填错内容用户照点即加错源，故每条分支都逐字钉住。
func TestAddSourceTool_Summarize(t *testing.T) {
	tool := &addSourceTool{}
	cases := []struct{ name, args, want string }{
		{"web/feed", `{"platform":"web","capability":"feed","url":"https://x.com/feed"}`, "添加 RSS 信源：https://x.com/feed"},
		{"web/search", `{"platform":"web","capability":"search","query":"AI"}`, "添加搜索信源：搜索词「AI」"},
		{"web/search 带类别", `{"platform":"web","capability":"search","query":"AI","category":"news"}`, "添加搜索信源：搜索词「AI」，类别「news」"},
		{"xhs/search", `{"platform":"xhs","capability":"search","keyword":"美妆"}`, "添加小红书关键词信源：「美妆」"},
		{"xhs/user_posts", `{"platform":"xhs","capability":"user_posts","user_id":"abc123"}`, "添加小红书博主信源：abc123"},
		{"x/user_posts", `{"platform":"x","capability":"user_posts","screen_name":"OpenAI"}`, "添加 X 用户时间线信源：@OpenAI"},
		{"带展示名", `{"platform":"web","capability":"feed","url":"https://x.com/feed","title":"某博客"}`, "添加 RSS 信源：https://x.com/feed，展示名「某博客」"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tool.Summarize(json.RawMessage(tc.args)); got != tc.want {
				t.Fatalf("Summarize 不符:\n实得 %q\n期望 %q", got, tc.want)
			}
		})
	}
	t.Run("非法 JSON 走兜底不 panic", func(t *testing.T) {
		if got := tool.Summarize(json.RawMessage(`{"platform":`)); !strings.Contains(got, "添加信源") {
			t.Fatalf("应走 summarizeFallback, 实得 %q", got)
		}
	})
}

// TestEnableSourceTool 覆盖 enable_source（功能 5.2 重启用入口）：写工具须走确认卡，
// Summarize 如实展示会启用哪个源。Execute 的归属校验（EnableSource 的 SQL WHERE）由
// store 集成测试覆盖（enable_source 持具体 *store.Store 不可 fake）。
func TestEnableSourceTool(t *testing.T) {
	if !(&enableSourceTool{}).Mutating() {
		t.Fatal("enable_source 必须是写工具（Mutating=true），否则可绕过确认卡直接改源状态")
	}
	got := (&enableSourceTool{}).Summarize(json.RawMessage(`{"source_id":7}`))
	if got != "重新启用信源（id=7）" {
		t.Fatalf("Summarize 实得 %q", got)
	}
	if fb := (&enableSourceTool{}).Summarize(json.RawMessage(`{`)); !strings.Contains(fb, "重新启用信源") {
		t.Fatalf("非法 JSON 应走 summarizeFallback, 实得 %q", fb)
	}
}

// TestOtherTools_Summarize 覆盖此前零测试的三个写工具 Summarize（审计 M-3）。
func TestOtherTools_Summarize(t *testing.T) {
	t.Run("remove_source", func(t *testing.T) {
		got := (&removeSourceTool{}).Summarize(json.RawMessage(`{"source_id":42}`))
		if got != "取消订阅信源（id=42）" {
			t.Fatalf("实得 %q", got)
		}
	})
	t.Run("remove_schedule", func(t *testing.T) {
		got := (&removeScheduleTool{}).Summarize(json.RawMessage(`{"schedule_id":"sched-7"}`))
		if got != "删除定时推送任务（id=sched-7）" {
			t.Fatalf("实得 %q", got)
		}
	})
	t.Run("create_schedule cron", func(t *testing.T) {
		got := (&createScheduleTool{}).Summarize(json.RawMessage(
			`{"spec":{"cron":"0 30 8 * * *"},"nl_description":"每天早八"}`))
		want := "创建定时推送任务：按 cron「0 30 8 * * *」触发（时区 Asia/Shanghai），描述「每天早八」"
		if got != want {
			t.Fatalf("实得 %q\n期望 %q", got, want)
		}
	})
	t.Run("create_schedule every_seconds", func(t *testing.T) {
		got := (&createScheduleTool{}).Summarize(json.RawMessage(`{"spec":{"every_seconds":3600}}`))
		// 文案刻意点明 epoch 对齐：不说的话用户会以为是"从现在起每小时"。
		if got != "创建定时推送任务：每 3600 秒触发一次（时区 Asia/Shanghai，按 epoch 对齐）" {
			t.Fatalf("实得 %q", got)
		}
	})
	t.Run("update_schedule 只改频率", func(t *testing.T) {
		got := (&updateScheduleTool{}).Summarize(json.RawMessage(
			`{"schedule_id":"push-1-abc","spec":{"cron":"30 8 * * *"}}`))
		want := "修改定时推送任务（id=push-1-abc）：触发频率改为 按 cron「30 8 * * *」触发（时区 Asia/Shanghai）"
		if got != want {
			t.Fatalf("实得 %q\n期望 %q", got, want)
		}
	})
	// 省略 nl_description = 不改描述，卡面绝不能提它——否则用户以为描述会被动。
	t.Run("update_schedule 省略描述不上卡", func(t *testing.T) {
		got := (&updateScheduleTool{}).Summarize(json.RawMessage(
			`{"schedule_id":"s1","spec":{"every_seconds":7200}}`))
		if strings.Contains(got, "描述") {
			t.Fatalf("省略 nl_description 时卡面不该提描述，实得 %q", got)
		}
	})
	t.Run("update_schedule 显式改描述", func(t *testing.T) {
		got := (&updateScheduleTool{}).Summarize(json.RawMessage(
			`{"schedule_id":"s1","spec":{"cron":"0 9 * * *"},"nl_description":"改成九点"}`))
		if !strings.Contains(got, "描述改为「改成九点」") {
			t.Fatalf("实得 %q", got)
		}
	})
	t.Run("update_schedule 显式清空描述", func(t *testing.T) {
		got := (&updateScheduleTool{}).Summarize(json.RawMessage(
			`{"schedule_id":"s1","spec":{"cron":"0 9 * * *"},"nl_description":""}`))
		if !strings.Contains(got, "清空描述") {
			t.Fatalf("显式空串应显示为清空（与省略可区分），实得 %q", got)
		}
	})
	// Summarize 无 error 通道：非法参数必须兜底成可读文案而非 panic/空串。
	t.Run("非法 JSON 兜底", func(t *testing.T) {
		if got := (&removeSourceTool{}).Summarize(json.RawMessage(`{`)); !strings.Contains(got, "取消订阅信源") {
			t.Fatalf("应走 summarizeFallback, 实得 %q", got)
		}
	})
}

// TestUpdateScheduleTool_契约 钉死 update_schedule 的工具面契约：
// 它是写工具（必须走确认卡）、参数校验与 create_schedule 同源、
// 且**不在 A2A 只读白名单里**（写工具漏进 A2A 就是越权）。
func TestUpdateScheduleTool_契约(t *testing.T) {
	tool := &updateScheduleTool{}

	if tool.Name() != "update_schedule" {
		t.Errorf("工具名不符: %s", tool.Name())
	}
	if !tool.Mutating() {
		t.Error("改调度是写操作，Mutating 必须为 true（否则绕过确认卡）")
	}
	// schema 必须是合法 JSON object schema，且把两个必填项声明出来。
	var schema map[string]any
	if err := json.Unmarshal(tool.Parameters(), &schema); err != nil {
		t.Fatalf("schema 不是合法 JSON: %v", err)
	}
	req, _ := schema["required"].([]any)
	if len(req) != 2 {
		t.Errorf("schedule_id 与 spec 都应必填，实得 %v", req)
	}
	// 工具描述必须点明"原地改、不要删了重建"——这正是它存在的理由，
	// 模型看不到这句就会退回 remove+create 的老路。
	if !strings.Contains(tool.Description(), "原地改") {
		t.Errorf("Description 应说明原地改，实得 %q", tool.Description())
	}
	// every_seconds 的 epoch 对齐语义必须写进 schema，否则模型会把它当"从现在起每 N 秒"。
	if !strings.Contains(string(tool.Parameters()), "epoch") {
		t.Error("schema 应说明 every_seconds 的 epoch 对齐语义")
	}
}

// TestValidateScheduleSpecFields_共用 验证 create 与 update 走的是同一套 spec 判据——
// 两处各写一份就等于同一条规则有两个版本，改地板时必漏一处。
func TestValidateScheduleSpecFields_共用(t *testing.T) {
	cases := []struct {
		name  string
		cron  string
		every int
		bad   bool
	}{
		{"只给 cron", "0 8 * * *", 0, false},
		{"只给 every", "", 3600, false},
		{"都给", "0 8 * * *", 3600, true},
		{"都不给", "", 0, true},
		{"every 低于地板", "", 1800, true},
		{"cron 只有空白等于没给", "   ", 0, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			msg := validateScheduleSpecFields(c.cron, c.every)
			if c.bad && msg == "" {
				t.Error("应被拒绝")
			}
			if !c.bad && msg != "" {
				t.Errorf("不应被拒绝，实得 %q", msg)
			}
			// create 的入口必须与共用函数结论一致。
			viaCreate := validateScheduleArgs(createScheduleArgs{Spec: struct {
				Cron         string `json:"cron"`
				EverySeconds int    `json:"every_seconds"`
				AnchorAt     string `json:"anchor_at"`
				TZ           string `json:"tz"`
			}{Cron: c.cron, EverySeconds: c.every}})
			if (viaCreate == "") != (msg == "") {
				t.Errorf("create 入口与共用校验结论漂移: create=%q shared=%q", viaCreate, msg)
			}
		})
	}
}

// TestBuildTools_包含update_schedule 防装配漏注册：工具写了但没进 BuildTools
// 等于不存在（loop 只认注册过的名字）。
func TestBuildTools_包含update_schedule(t *testing.T) {
	names := map[string]bool{}
	for _, tl := range BuildTools(nil, nil, nil, nil, nil) {
		names[tl.Name()] = true
	}
	for _, want := range []string{"create_schedule", "update_schedule", "remove_schedule", "list_schedules", "view_task_playbook", "edit_task_playbook"} {
		if !names[want] {
			t.Errorf("BuildTools 应包含 %s", want)
		}
	}
}

// ============================================================
// 任务手册工具（Task Playbook P0）
// ============================================================

// fakePlaybookStore 满足 playbookStore：books 存内容，owner 存归属。
// Get 未命中/非本人 → types.ErrNotFound；Upsert 校归属返回 ok，记录入参供断言。
type fakePlaybookStore struct {
	books     map[string]*types.SchedulePlaybook
	owner     map[string]int64
	getErr    error
	upsertErr error
	upserts   []struct {
		userID     int64
		scheduleID string
		content    string
	}
	setPlanErr error
	plans      []struct {
		userID     int64
		scheduleID string
		plan       string
	}
}

func newFakePlaybookStore() *fakePlaybookStore {
	return &fakePlaybookStore{books: map[string]*types.SchedulePlaybook{}, owner: map[string]int64{}}
}

func (f *fakePlaybookStore) GetSchedulePlaybook(_ context.Context, userID int64, scheduleID string) (*types.SchedulePlaybook, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	pb, ok := f.books[scheduleID]
	if !ok || f.owner[scheduleID] != userID {
		return nil, types.NewAppError(types.CodeNotFound, "无手册或非本人", nil)
	}
	return pb, nil
}

func (f *fakePlaybookStore) UpsertSchedulePlaybook(_ context.Context, userID int64, scheduleID, content string) (bool, error) {
	f.upserts = append(f.upserts, struct {
		userID     int64
		scheduleID string
		content    string
	}{userID, scheduleID, content})
	if f.upsertErr != nil {
		return false, f.upsertErr
	}
	if o, ok := f.owner[scheduleID]; ok && o != userID {
		return false, nil // 非本人：归属未命中
	}
	f.owner[scheduleID] = userID
	f.books[scheduleID] = &types.SchedulePlaybook{ScheduleID: scheduleID, Content: content}
	return true, nil
}

// SetFetchPlan 模拟 store 的 UPDATE … FROM schedules 语义：需已存在手册行 + 归属命中才写。
func (f *fakePlaybookStore) SetFetchPlan(_ context.Context, userID int64, scheduleID string, plan json.RawMessage) (bool, error) {
	f.plans = append(f.plans, struct {
		userID     int64
		scheduleID string
		plan       string
	}{userID, scheduleID, string(plan)})
	if f.setPlanErr != nil {
		return false, f.setPlanErr
	}
	if o, ok := f.owner[scheduleID]; ok && o != userID {
		return false, nil // 非本人：归属未命中。
	}
	pb, ok := f.books[scheduleID]
	if !ok {
		return false, nil // 无手册行：计划依附不上。
	}
	pb.FetchPlan = plan
	return true, nil
}

// fakeTranslator 是 playbookTranslator 的假实现：返回预设计划 / err，并记录调用入参。
type fakeTranslator struct {
	plan    json.RawMessage
	err     error
	calls   int
	gotUser int64
	gotText string
}

func (f *fakeTranslator) Translate(_ context.Context, userID int64, content string) (json.RawMessage, error) {
	f.calls++
	f.gotUser = userID
	f.gotText = content
	if f.err != nil {
		return nil, f.err
	}
	return f.plan, nil
}

func TestViewTaskPlaybookTool(t *testing.T) {
	if (&viewTaskPlaybookTool{}).Mutating() {
		t.Fatal("view_task_playbook 必须是读工具（Mutating=false），否则查手册要人点确认卡")
	}
	t.Run("有手册逐字返回", func(t *testing.T) {
		fs := newFakePlaybookStore()
		fs.books["s1"] = &types.SchedulePlaybook{ScheduleID: "s1", Content: "每天早八 AI 官方新闻"}
		fs.owner["s1"] = 7
		got, err := (&viewTaskPlaybookTool{st: fs}).Execute(context.Background(), 7, json.RawMessage(`{"schedule_id":"s1"}`))
		if err != nil {
			t.Fatalf("意外报错: %v", err)
		}
		if got != "任务手册（id=s1）：\n每天早八 AI 官方新闻" {
			t.Fatalf("渲染不符: %q", got)
		}
	})
	t.Run("空内容给引导文案", func(t *testing.T) {
		fs := newFakePlaybookStore()
		fs.books["s1"] = &types.SchedulePlaybook{ScheduleID: "s1", Content: ""}
		fs.owner["s1"] = 7
		got, _ := (&viewTaskPlaybookTool{st: fs}).Execute(context.Background(), 7, json.RawMessage(`{"schedule_id":"s1"}`))
		if !strings.Contains(got, "手册为空") {
			t.Fatalf("空手册应给引导文案: %q", got)
		}
	})
	t.Run("有抓取计划时一并渲染（P1 编译层）", func(t *testing.T) {
		fs := newFakePlaybookStore()
		fs.books["s1"] = &types.SchedulePlaybook{
			ScheduleID: "s1",
			Content:    "AI 官方新闻",
			FetchPlan:  json.RawMessage(`{"sources":[{"platform":"web","capability":"search","title":"搜索: AI","url":"vane://web/search?q=ai"}]}`),
		}
		fs.owner["s1"] = 7
		got, _ := (&viewTaskPlaybookTool{st: fs}).Execute(context.Background(), 7, json.RawMessage(`{"schedule_id":"s1"}`))
		if !strings.Contains(got, "抓取计划（1 个源）") || !strings.Contains(got, "[web/search]") {
			t.Fatalf("应渲染抓取计划: %q", got)
		}
	})
	t.Run("空 fetch_plan 不赘述计划", func(t *testing.T) {
		fs := newFakePlaybookStore()
		fs.books["s1"] = &types.SchedulePlaybook{ScheduleID: "s1", Content: "x", FetchPlan: json.RawMessage(`{"sources":[]}`)}
		fs.owner["s1"] = 7
		got, _ := (&viewTaskPlaybookTool{st: fs}).Execute(context.Background(), 7, json.RawMessage(`{"schedule_id":"s1"}`))
		if strings.Contains(got, "抓取计划") {
			t.Fatalf("零源不该出现计划段: %q", got)
		}
	})
	t.Run("NotFound 回文案不报错", func(t *testing.T) {
		got, err := (&viewTaskPlaybookTool{st: newFakePlaybookStore()}).Execute(context.Background(), 7, json.RawMessage(`{"schedule_id":"nope"}`))
		if err != nil {
			t.Fatalf("NotFound 不应上抛: %v", err)
		}
		if !strings.Contains(got, "没找到你的定时任务") {
			t.Fatalf("应回引导文案: %q", got)
		}
	})
	t.Run("空 schedule_id 自纠不触库", func(t *testing.T) {
		got, err := (&viewTaskPlaybookTool{st: newFakePlaybookStore()}).Execute(context.Background(), 7, json.RawMessage(`{"schedule_id":""}`))
		if err != nil || !strings.Contains(got, "schedule_id 必须是非空") {
			t.Fatalf("应回自纠文案, got=%q err=%v", got, err)
		}
	})
	t.Run("DB 错误上抛不伪装", func(t *testing.T) {
		fs := newFakePlaybookStore()
		fs.getErr = types.NewAppError(types.CodeDatabase, "连接池耗尽", nil)
		got, err := (&viewTaskPlaybookTool{st: fs}).Execute(context.Background(), 7, json.RawMessage(`{"schedule_id":"s1"}`))
		if err == nil || got != "" {
			t.Fatalf("基础设施错误应上抛（不得伪装成无手册）, got=%q err=%v", got, err)
		}
	})
}

func TestEditTaskPlaybookTool(t *testing.T) {
	if !(&editTaskPlaybookTool{}).Mutating() {
		t.Fatal("edit_task_playbook 必须是写工具（Mutating=true），否则绕过确认卡")
	}
	t.Run("正常写入", func(t *testing.T) {
		fs := newFakePlaybookStore()
		fs.owner["s1"] = 7 // 已存在且属本人
		fs.books["s1"] = &types.SchedulePlaybook{ScheduleID: "s1"}
		got, err := (&editTaskPlaybookTool{st: fs}).Execute(context.Background(), 7,
			json.RawMessage(`{"schedule_id":"s1","content":"只要官方源"}`))
		if err != nil {
			t.Fatalf("意外报错: %v", err)
		}
		if len(fs.upserts) != 1 || fs.upserts[0].content != "只要官方源" || fs.upserts[0].userID != 7 {
			t.Fatalf("upsert 入参不符: %+v", fs.upserts)
		}
		if !strings.Contains(got, "已更新定时任务") {
			t.Fatalf("回执不符: %q", got)
		}
	})
	t.Run("空 content 自纠不触库", func(t *testing.T) {
		fs := newFakePlaybookStore()
		got, err := (&editTaskPlaybookTool{st: fs}).Execute(context.Background(), 7,
			json.RawMessage(`{"schedule_id":"s1","content":"  "}`))
		if err != nil || !strings.Contains(got, "手册内容不能为空") {
			t.Fatalf("应回自纠文案, got=%q err=%v", got, err)
		}
		if len(fs.upserts) != 0 {
			t.Fatalf("空内容不得触库: %+v", fs.upserts)
		}
	})
	t.Run("content 超上限截断", func(t *testing.T) {
		fs := newFakePlaybookStore()
		fs.owner["s1"] = 7
		long := strings.Repeat("字", maxPlaybookContentRunes+50)
		if _, err := (&editTaskPlaybookTool{st: fs}).Execute(context.Background(), 7,
			json.RawMessage(`{"schedule_id":"s1","content":"`+long+`"}`)); err != nil {
			t.Fatalf("超限应截断而非报错: %v", err)
		}
		if got := len([]rune(fs.upserts[0].content)); got != maxPlaybookContentRunes {
			t.Fatalf("content 应截到 %d rune, 实得 %d", maxPlaybookContentRunes, got)
		}
	})
	t.Run("归属未命中回文案不报错", func(t *testing.T) {
		fs := newFakePlaybookStore()
		fs.owner["s1"] = 99 // 属别人
		got, err := (&editTaskPlaybookTool{st: fs}).Execute(context.Background(), 7,
			json.RawMessage(`{"schedule_id":"s1","content":"x"}`))
		if err != nil {
			t.Fatalf("归属未命中不应上抛: %v", err)
		}
		if !strings.Contains(got, "没找到你的定时任务") {
			t.Fatalf("应回自纠文案: %q", got)
		}
	})
	t.Run("DB 错误上抛", func(t *testing.T) {
		fs := newFakePlaybookStore()
		fs.upsertErr = types.NewAppError(types.CodeDatabase, "写入失败", nil)
		got, err := (&editTaskPlaybookTool{st: fs}).Execute(context.Background(), 7,
			json.RawMessage(`{"schedule_id":"s1","content":"x"}`))
		if err == nil || got != "" {
			t.Fatalf("基础设施错误应上抛, got=%q err=%v", got, err)
		}
	})
}

func TestEditTaskPlaybookTool_Summarize(t *testing.T) {
	tool := &editTaskPlaybookTool{}
	t.Run("正常展示内容片段", func(t *testing.T) {
		got := tool.Summarize(json.RawMessage(`{"schedule_id":"s1","content":"只要官方源"}`))
		if got != "修改定时任务手册（id=s1）：新内容「只要官方源」" {
			t.Fatalf("摘要不符: %q", got)
		}
	})
	t.Run("超预览上限截断带省略号", func(t *testing.T) {
		long := strings.Repeat("字", playbookSummaryPreviewRunes+20)
		got := tool.Summarize(json.RawMessage(`{"schedule_id":"s1","content":"` + long + `"}`))
		if !strings.Contains(got, "…") {
			t.Fatalf("超预览上限应截断带省略号: %q", got)
		}
	})
	t.Run("非法 JSON 走兜底", func(t *testing.T) {
		if got := tool.Summarize(json.RawMessage(`{`)); !strings.Contains(got, "修改任务手册") {
			t.Fatalf("应走 summarizeFallback: %q", got)
		}
	})
}

// fakeScheduleCreator 是 scheduleCreator 的假实现：返回预设 schedID / err，
// 并记录 CreatePush 收到的 nlDesc（用于验证手册初始化用它作内容）。
type fakeScheduleCreator struct {
	schedID   string
	err       error
	gotNLDesc string
	calls     int
}

func (f *fakeScheduleCreator) CreatePush(_ context.Context, _ int64, _ scheduler.ScheduleSpec, _ workflow.PushScope, nlDesc string) (string, error) {
	f.calls++
	f.gotNLDesc = nlDesc
	if f.err != nil {
		return "", f.err
	}
	return f.schedID, nil
}

// TestCreateScheduleTool_InitializesPlaybook 守决策 D2 的胶水（创建即初始化手册）：
// CreatePush 成功后，用 nl_description 初始化 schedID 对应的手册；best-effort——
// 手册写失败不回滚调度、仍回成功。此前这条接线零测试（Execute 依赖真 Temporal）。
func TestCreateScheduleTool_InitializesPlaybook(t *testing.T) {
	args := json.RawMessage(`{"spec":{"cron":"0 30 8 * * *"},"nl_description":"每天早八 AI 官方新闻"}`)

	t.Run("成功时用 nl 意图初始化手册", func(t *testing.T) {
		sc := &fakeScheduleCreator{schedID: "push-7-abc"}
		pb := newFakePlaybookStore()
		tool := &createScheduleTool{sched: sc, st: pb}
		got, err := tool.Execute(context.Background(), 7, args)
		if err != nil {
			t.Fatalf("意外报错: %v", err)
		}
		if !strings.Contains(got, "push-7-abc") {
			t.Fatalf("回执应含 schedID: %q", got)
		}
		if len(pb.upserts) != 1 {
			t.Fatalf("应恰好初始化一次手册, 实得 %d", len(pb.upserts))
		}
		u := pb.upserts[0]
		if u.userID != 7 || u.scheduleID != "push-7-abc" || u.content != "每天早八 AI 官方新闻" {
			t.Fatalf("手册初始化入参不符: %+v", u)
		}
	})

	t.Run("手册写失败仍回成功（best-effort，不回滚调度）", func(t *testing.T) {
		sc := &fakeScheduleCreator{schedID: "push-7-def"}
		pb := newFakePlaybookStore()
		pb.upsertErr = types.NewAppError(types.CodeDatabase, "写入失败", nil)
		tool := &createScheduleTool{sched: sc, st: pb}
		got, err := tool.Execute(context.Background(), 7, args)
		if err != nil {
			t.Fatalf("手册写失败不应让 create_schedule 上抛（best-effort）: %v", err)
		}
		if !strings.Contains(got, "push-7-def") {
			t.Fatalf("调度是主效果，仍应回成功: %q", got)
		}
	})

	t.Run("CreatePush 失败则不建手册", func(t *testing.T) {
		sc := &fakeScheduleCreator{err: types.NewAppError(types.CodeValidation, "频率过高", nil)}
		pb := newFakePlaybookStore()
		tool := &createScheduleTool{sched: sc, st: pb}
		got, _ := tool.Execute(context.Background(), 7, args)
		if !strings.Contains(got, "频率过高") {
			t.Fatalf("CodeValidation 应回文案: %q", got)
		}
		if len(pb.upserts) != 0 {
			t.Fatalf("调度没建成不该初始化手册, 实得 %+v", pb.upserts)
		}
	})
}

// TestSummarize_锚点上卡 打在**真实调用点**（两个工具的 Summarize），而不是内部的
// formatScheduleSpec——对抗审查实测：只测格式化函数时，updateScheduleTool.Summarize
// 漏传 AnchorAt 的真 bug 照样全绿，卡面对用户说"按 epoch 对齐"而实际锚定，主动说反话。
// 确认卡是写操作的人类同意闸门，它说的必须是即将发生的事。
func TestSummarize_锚点上卡(t *testing.T) {
	const anchored = `{"schedule_id":"push-1-abc","spec":{"every_seconds":259200,"anchor_at":"2026-07-19T20:00:00+08:00"}}`
	const plain = `{"schedule_id":"push-1-abc","spec":{"every_seconds":259200}}`

	for name, sum := range map[string]func(json.RawMessage) string{
		"create": (&createScheduleTool{}).Summarize,
		"update": (&updateScheduleTool{}).Summarize,
	} {
		t.Run(name+" 有锚点说出锚点", func(t *testing.T) {
			got := sum(json.RawMessage(anchored))
			if !strings.Contains(got, "2026-07-19T20:00:00+08:00") || !strings.Contains(got, "起") {
				t.Errorf("卡面应说明从何时起，实得 %q", got)
			}
			if strings.Contains(got, "epoch") {
				t.Errorf("有锚点却说 epoch 对齐＝对用户说反话，实得 %q", got)
			}
		})
		t.Run(name+" 无锚点点明 epoch", func(t *testing.T) {
			got := sum(json.RawMessage(plain))
			if !strings.Contains(got, "epoch") {
				t.Errorf("无锚点应点明 epoch 对齐（否则用户以为从现在起），实得 %q", got)
			}
		})
	}
}

// TestScheduleSchemas_含anchor_at 防"底层支持了但工具面没暴露"：create 与 update
// 两个 schema 都要有 anchor_at，否则模型根本用不上这个能力。
func TestScheduleSchemas_含anchor_at(t *testing.T) {
	for name, raw := range map[string]json.RawMessage{
		"create_schedule": (&createScheduleTool{}).Parameters(),
		"update_schedule": (&updateScheduleTool{}).Parameters(),
	} {
		var schema struct {
			Properties struct {
				Spec struct {
					Properties map[string]any `json:"properties"`
				} `json:"spec"`
			} `json:"properties"`
		}
		if err := json.Unmarshal(raw, &schema); err != nil {
			t.Fatalf("%s schema 不是合法 JSON: %v", name, err)
		}
		if _, ok := schema.Properties.Spec.Properties["anchor_at"]; !ok {
			t.Errorf("%s 的 spec 应暴露 anchor_at", name)
		}
	}
}

// fakeSchedulePusher 捕获工具真正传给 scheduler 的 spec（schedulePusher 收窄成接口就是为了它）。
type fakeSchedulePusher struct {
	gotSpec   scheduler.ScheduleSpec
	gotID     string
	gotNLDesc *string
	calls     int
}

func (f *fakeSchedulePusher) CreatePush(_ context.Context, _ int64, spec scheduler.ScheduleSpec,
	_ workflow.PushScope, _ string) (string, error) {
	f.calls++
	f.gotSpec = spec
	return "push-1-created", nil
}

func (f *fakeSchedulePusher) UpdatePush(_ context.Context, id string, spec scheduler.ScheduleSpec,
	nlDesc *string) error {
	f.calls++
	f.gotID, f.gotSpec, f.gotNLDesc = id, spec, nlDesc
	return nil
}

func (f *fakeSchedulePusher) DeletePush(_ context.Context, id string) error {
	f.calls++
	f.gotID = id
	return nil
}

// TestScheduleTools_接线透传 钉死"工具面广告的字段真的到达 scheduler"。
//
// 为什么必须断言在**捕获到的 spec** 上而不是 schema 里有没有那个 key（对抗审查实测）：
// 删掉 Execute 里的 `AnchorAt: a.Spec.AnchorAt`、或把 DTO 的 json tag 拼错，
// 全仓测试照样绿——用户/模型给了 anchor_at，后端 200 正常返回，调度却静默按 epoch
// 触发，无任何错误信号。这类"接线漏一行"的 bug 只有捕获真实入参才抓得到。
func TestScheduleTools_接线透传(t *testing.T) {
	const anchor = "2026-07-19T20:00:00+08:00"

	t.Run("create 透传 anchor_at", func(t *testing.T) {
		f := &fakeSchedulePusher{}
		_, err := (&createScheduleTool{sched: f, st: newFakePlaybookStore()}).Execute(context.Background(), 1,
			json.RawMessage(`{"spec":{"every_seconds":259200,"anchor_at":"`+anchor+`","tz":"Asia/Shanghai"}}`))
		if err != nil {
			t.Fatalf("Execute 失败: %v", err)
		}
		if f.calls != 1 {
			t.Fatalf("应调用一次 CreatePush，实得 %d", f.calls)
		}
		if f.gotSpec.AnchorAt != anchor {
			t.Errorf("anchor_at 没到达 scheduler（能力静默失效），实得 %q", f.gotSpec.AnchorAt)
		}
		if f.gotSpec.EverySeconds != 259200 || f.gotSpec.TZ != "Asia/Shanghai" {
			t.Errorf("spec 透传不全: %+v", f.gotSpec)
		}
	})

	t.Run("update 透传 anchor_at", func(t *testing.T) {
		f := &fakeSchedulePusher{}
		_, err := (&updateScheduleTool{sched: f}).Execute(context.Background(), 1,
			json.RawMessage(`{"schedule_id":"push-1-abc","spec":{"every_seconds":259200,"anchor_at":"`+anchor+`"}}`))
		if err != nil {
			t.Fatalf("Execute 失败: %v", err)
		}
		if f.gotID != "push-1-abc" {
			t.Errorf("schedule_id 透传错: %q", f.gotID)
		}
		if f.gotSpec.AnchorAt != anchor {
			t.Errorf("anchor_at 没到达 scheduler（能力静默失效），实得 %q", f.gotSpec.AnchorAt)
		}
	})

	t.Run("无锚点时不得凭空造出锚点", func(t *testing.T) {
		f := &fakeSchedulePusher{}
		if _, err := (&createScheduleTool{sched: f, st: newFakePlaybookStore()}).Execute(context.Background(), 1,
			json.RawMessage(`{"spec":{"every_seconds":7200}}`)); err != nil {
			t.Fatalf("Execute 失败: %v", err)
		}
		if f.gotSpec.AnchorAt != "" {
			t.Errorf("未给锚点时应为空（保持 epoch 对齐语义），实得 %q", f.gotSpec.AnchorAt)
		}
	})

	t.Run("remove 透传 id", func(t *testing.T) {
		f := &fakeSchedulePusher{}
		if _, err := (&removeScheduleTool{sched: f}).Execute(context.Background(), 1,
			json.RawMessage(`{"schedule_id":"push-1-xyz"}`)); err != nil {
			t.Fatalf("Execute 失败: %v", err)
		}
		if f.gotID != "push-1-xyz" {
			t.Errorf("schedule_id 透传错: %q", f.gotID)
		}
	})
}

// TestCreateScheduleTool_CompilesPlan 守 P1 编译层：create 初始化手册后，用同一份 content
// 调翻译器编译计划并落库（best-effort，绝不影响调度主效果）。
func TestCreateScheduleTool_CompilesPlan(t *testing.T) {
	args := json.RawMessage(`{"spec":{"cron":"0 30 8 * * *"},"nl_description":"每天早八 AI 官方新闻"}`)

	t.Run("初始化手册后据此编译计划落库", func(t *testing.T) {
		sc := &fakeScheduleCreator{schedID: "push-7-abc"}
		pb := newFakePlaybookStore()
		tr := &fakeTranslator{plan: json.RawMessage(`{"sources":[{"platform":"web","capability":"search","url":"vane://web/search?q=ai"}]}`)}
		tool := &createScheduleTool{sched: sc, st: pb, tr: tr}
		if _, err := tool.Execute(context.Background(), 7, args); err != nil {
			t.Fatalf("意外报错: %v", err)
		}
		if tr.calls != 1 || tr.gotUser != 7 || tr.gotText != "每天早八 AI 官方新闻" {
			t.Fatalf("翻译器入参不符: calls=%d user=%d text=%q", tr.calls, tr.gotUser, tr.gotText)
		}
		if len(pb.plans) != 1 || pb.plans[0].scheduleID != "push-7-abc" || pb.plans[0].userID != 7 {
			t.Fatalf("应把计划落库到刚建的任务: %+v", pb.plans)
		}
	})

	t.Run("翻译失败不影响调度（best-effort）", func(t *testing.T) {
		sc := &fakeScheduleCreator{schedID: "push-7-x"}
		pb := newFakePlaybookStore()
		tr := &fakeTranslator{err: errors.New("llm 超时")}
		tool := &createScheduleTool{sched: sc, st: pb, tr: tr}
		got, err := tool.Execute(context.Background(), 7, args)
		if err != nil {
			t.Fatalf("翻译失败不应上抛: %v", err)
		}
		if !strings.Contains(got, "push-7-x") {
			t.Fatalf("调度仍应回成功: %q", got)
		}
		if len(pb.plans) != 0 {
			t.Fatalf("翻译失败不该落计划: %+v", pb.plans)
		}
	})

	t.Run("手册初始化失败则根本不调翻译器", func(t *testing.T) {
		sc := &fakeScheduleCreator{schedID: "push-7-y"}
		pb := newFakePlaybookStore()
		pb.upsertErr = types.NewAppError(types.CodeDatabase, "写入失败", nil)
		tr := &fakeTranslator{plan: emptyPlanJSON()}
		tool := &createScheduleTool{sched: sc, st: pb, tr: tr}
		if _, err := tool.Execute(context.Background(), 7, args); err != nil {
			t.Fatalf("best-effort 不应上抛: %v", err)
		}
		if tr.calls != 0 {
			t.Fatalf("手册没存成不该编译计划, calls=%d", tr.calls)
		}
	})
}

// TestEditTaskPlaybookTool_CompilesPlan 守 P1 编译层：edit 存下正文后据此重编译计划，
// 回执按编译出的源数分档，翻译失败静默降级为普通成功。
func TestEditTaskPlaybookTool_CompilesPlan(t *testing.T) {
	setup := func(tr playbookTranslator) (*fakePlaybookStore, *editTaskPlaybookTool) {
		fs := newFakePlaybookStore()
		fs.owner["s1"] = 7
		fs.books["s1"] = &types.SchedulePlaybook{ScheduleID: "s1"}
		return fs, &editTaskPlaybookTool{st: fs, tr: tr}
	}
	args := json.RawMessage(`{"schedule_id":"s1","content":"只要 Anthropic 官方"}`)

	t.Run("编译出源时回执带源数", func(t *testing.T) {
		tr := &fakeTranslator{plan: json.RawMessage(`{"sources":[{"platform":"web","capability":"search","url":"vane://web/search?q=a"},{"platform":"web","capability":"feed","url":"https://a.com/rss"}]}`)}
		fs, tool := setup(tr)
		got, err := tool.Execute(context.Background(), 7, args)
		if err != nil {
			t.Fatalf("意外报错: %v", err)
		}
		if tr.gotText != "只要 Anthropic 官方" {
			t.Fatalf("翻译器应收到手册正文: %q", tr.gotText)
		}
		if len(fs.plans) != 1 {
			t.Fatalf("应落库计划一次: %+v", fs.plans)
		}
		if !strings.Contains(got, "2 个抓取源") {
			t.Fatalf("回执应含编译出的源数: %q", got)
		}
	})

	t.Run("零源计划回执不提计划", func(t *testing.T) {
		tr := &fakeTranslator{plan: emptyPlanJSON()}
		_, tool := setup(tr)
		got, _ := tool.Execute(context.Background(), 7, args)
		if strings.Contains(got, "抓取源") {
			t.Fatalf("零源不该提计划: %q", got)
		}
		if !strings.Contains(got, "已更新定时任务") {
			t.Fatalf("仍应回更新成功: %q", got)
		}
	})

	t.Run("翻译失败降级为普通成功、不落计划", func(t *testing.T) {
		tr := &fakeTranslator{err: errors.New("boom")}
		fs, tool := setup(tr)
		got, err := tool.Execute(context.Background(), 7, args)
		if err != nil {
			t.Fatalf("翻译失败不应上抛: %v", err)
		}
		if len(fs.plans) != 0 {
			t.Fatalf("翻译失败不该落计划: %+v", fs.plans)
		}
		if !strings.Contains(got, "已更新定时任务") || strings.Contains(got, "抓取源") {
			t.Fatalf("应降级为普通成功文案: %q", got)
		}
	})

	t.Run("content 归属未命中就不编译计划", func(t *testing.T) {
		tr := &fakeTranslator{plan: emptyPlanJSON()}
		fs := newFakePlaybookStore()
		fs.owner["s1"] = 99 // 属别人
		tool := &editTaskPlaybookTool{st: fs, tr: tr}
		got, _ := tool.Execute(context.Background(), 7, args)
		if tr.calls != 0 {
			t.Fatalf("content upsert 未命中就不该翻译, calls=%d", tr.calls)
		}
		if !strings.Contains(got, "没找到你的定时任务") {
			t.Fatalf("应回归属未命中文案: %q", got)
		}
	})

	t.Run("计划落库报基础设施错误被吞、不污染主效果", func(t *testing.T) {
		// best-effort 铁律：手册正文已存成，SetFetchPlan 报 CodeDatabase 时工具仍回普通成功、
		// 绝不上抛（守 compilePlaybookPlan 的 serr!=nil 分支）。
		tr := &fakeTranslator{plan: json.RawMessage(`{"sources":[{"platform":"web","capability":"search","url":"vane://web/search?q=a"}]}`)}
		fs, tool := setup(tr)
		fs.setPlanErr = types.NewAppError(types.CodeDatabase, "连接池耗尽", nil)
		got, err := tool.Execute(context.Background(), 7, args)
		if err != nil {
			t.Fatalf("计划落库失败不应上抛（best-effort）: %v", err)
		}
		if !strings.Contains(got, "已更新定时任务") || strings.Contains(got, "抓取源") {
			t.Fatalf("应降级为普通成功文案（计划没落成不提源数）: %q", got)
		}
	})

	t.Run("翻译器收到的是 cap 截断后的正文（与落库正文一致）", func(t *testing.T) {
		// 超限手册：翻译器必须收到 capPlaybookContent 截断后的正文，且与 Upsert 落库的正文一致，
		// 否则会把未设边界的超长文本整体发给 LLM（token/成本失控）且编译源与库内正文漂移。
		tr := &fakeTranslator{plan: emptyPlanJSON()}
		fs, tool := setup(tr)
		long := strings.Repeat("字", maxPlaybookContentRunes+50)
		if _, err := tool.Execute(context.Background(), 7,
			json.RawMessage(`{"schedule_id":"s1","content":"`+long+`"}`)); err != nil {
			t.Fatalf("超限应截断而非报错: %v", err)
		}
		if n := len([]rune(tr.gotText)); n != maxPlaybookContentRunes {
			t.Fatalf("翻译器应收到截断到 %d rune 的正文, 实得 %d", maxPlaybookContentRunes, n)
		}
		if tr.gotText != fs.upserts[0].content {
			t.Fatalf("翻译器输入与落库正文必须逐字一致（否则计划与库内正文漂移）")
		}
	})
}
