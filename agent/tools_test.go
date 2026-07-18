package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/YouToco/vane/sourcecatalog"
	"github.com/YouToco/vane/sourcespec"
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
		if got != "创建定时推送任务：每 3600 秒触发一次（时区 Asia/Shanghai）" {
			t.Fatalf("实得 %q", got)
		}
	})
	// Summarize 无 error 通道：非法参数必须兜底成可读文案而非 panic/空串。
	t.Run("非法 JSON 兜底", func(t *testing.T) {
		if got := (&removeSourceTool{}).Summarize(json.RawMessage(`{`)); !strings.Contains(got, "取消订阅信源") {
			t.Fatalf("应走 summarizeFallback, 实得 %q", got)
		}
	})
}
