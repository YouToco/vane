package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/YouToco/vane/acquisitiontool"
	"github.com/YouToco/vane/llm"
	"github.com/YouToco/vane/scheduler"
	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
)

type fakeTaskRunTrigger struct {
	err   error
	calls []string
}

func TestFilterSchedulesByQueryRequiresContiguousReadableMatch(t *testing.T) {
	list := []types.Schedule{
		{
			ID:            "task-ai",
			NLDescription: "每周一上午 9:00 推送 AI 官方重大更新",
			SpecJSON:      json.RawMessage(`{"cron":"0 9 * * 1"}`),
		},
		{
			ID:            "task-finance",
			NLDescription: "每天推送财经日报",
			SpecJSON:      json.RawMessage(`{"cron":"0 8 * * *"}`),
		},
	}
	got := filterSchedulesByQuery(
		list,
		normalizeScheduleLookupText("每周一上午9:00推送AI官方重大更新"),
	)
	if len(got) != 1 || got[0].ID != "task-ai" {
		t.Fatalf("matches=%+v, want task-ai only", got)
	}
	if len(list) != 2 || list[0].ID != "task-ai" ||
		list[1].ID != "task-finance" {
		t.Fatal("filter mutated the caller-owned schedule list")
	}
}

func TestFilterSchedulesByQueryFallsBackToDistinctiveProductToken(t *testing.T) {
	list := []types.Schedule{{
		ID:            "task-kimi",
		NLDescription: "每小时检查 Kimi 会员定价页面的购买状态",
		SpecJSON:      json.RawMessage(`{"every_seconds":3600}`),
	}}
	got := filterSchedulesByQuery(
		list,
		normalizeScheduleLookupText("Kimi 套餐可购买监测"),
	)
	if len(got) != 1 || got[0].ID != "task-kimi" {
		t.Fatalf("matches=%+v, want task-kimi", got)
	}
}

type fakeScheduleListStore struct {
	list []types.Schedule
	err  error
}

func (f fakeScheduleListStore) ListSchedulesByUser(
	context.Context,
	int64,
) ([]types.Schedule, error) {
	return f.list, f.err
}

func TestListSchedulesToolAcceptsOmittedOptionalQuery(t *testing.T) {
	tool := &listSchedulesTool{st: fakeScheduleListStore{}}
	got, err := tool.Execute(context.Background(), 7, json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if got != "当前没有任何定时推送任务。" {
		t.Fatalf("got %q", got)
	}
}

func (f *fakeTaskRunTrigger) TriggerScheduleNowIdempotent(
	_ context.Context,
	scheduleID string,
	_ int64,
	key string,
) error {
	f.calls = append(f.calls, scheduleID+"\x00"+key)
	return f.err
}

func TestRunTaskNowTool_Execute(t *testing.T) {
	t.Run("批量任务使用调用身份生成稳定命令键", func(t *testing.T) {
		runner := &fakeTaskRunTrigger{}
		tool := &runTaskNowTool{runner: runner}
		ctx := withToolInvocationID(context.Background(), "call-stable")
		args := json.RawMessage(`{"schedule_ids":["task-a","task-b","task-a"]}`)
		got, err := tool.Execute(ctx, 7, args)
		if err != nil || !strings.Contains(got, "2 个任务") {
			t.Fatalf("批量立即运行失败: got=%q err=%v", got, err)
		}
		if len(runner.calls) != 2 {
			t.Fatalf("调用数=%d want=2: %v", len(runner.calls), runner.calls)
		}
		keyA := strings.Split(runner.calls[0], "\x00")[1]
		if keyA != runTaskNowIdempotencyKey(7, "call-stable", "task-a") {
			t.Fatalf("幂等键漂移: %q", keyA)
		}
		replay := &fakeTaskRunTrigger{}
		tool.runner = replay
		if _, err := tool.Execute(ctx, 7, args); err != nil {
			t.Fatal(err)
		}
		if replay.calls[0] != runner.calls[0] ||
			replay.calls[1] != runner.calls[1] {
			t.Fatalf("相同工具调用重放未复用命令身份: %v vs %v",
				runner.calls, replay.calls)
		}
	})

	t.Run("缺少调用身份在任何副作用前拒绝", func(t *testing.T) {
		runner := &fakeTaskRunTrigger{}
		tool := &runTaskNowTool{runner: runner}
		got, err := tool.Execute(
			context.Background(), 7,
			json.RawMessage(`{"schedule_ids":["task-a"]}`),
		)
		if err == nil || got != "" || len(runner.calls) != 0 {
			t.Fatalf("缺少调用身份未 fail closed: got=%q err=%v calls=%v",
				got, err, runner.calls)
		}
	})

	t.Run("控制面错误原样上抛", func(t *testing.T) {
		cause := types.NewAppError(
			types.CodeInternal, "触发任务立即运行失败",
			fmt.Errorf("temporal down"),
		)
		runner := &fakeTaskRunTrigger{err: cause}
		tool := &runTaskNowTool{runner: runner}
		ctx := withToolInvocationID(context.Background(), "call-error")
		got, err := tool.Execute(
			ctx, 7, json.RawMessage(`{"schedule_ids":["task-a"]}`),
		)
		if err == nil || got != "" || !errors.Is(err, types.ErrInternal) {
			t.Fatalf("控制面错误应上抛: got=%q err=%v", got, err)
		}
	})
}

func TestScopedToolInvocationIDIncludesAgentTurn(t *testing.T) {
	const providerCallID = "run_task_now_3"
	first := context.WithValue(context.Background(), chatMetaKey{}, chatMeta{
		traceID: "turn-a",
	})
	replay := context.WithValue(context.Background(), chatMetaKey{}, chatMeta{
		traceID: "turn-a",
	})
	second := context.WithValue(context.Background(), chatMetaKey{}, chatMeta{
		traceID: "turn-b",
	})

	firstID := scopedToolInvocationID(first, providerCallID)
	if firstID != scopedToolInvocationID(replay, providerCallID) {
		t.Fatal("same durable Agent turn did not retain its invocation identity")
	}
	if firstID == scopedToolInvocationID(second, providerCallID) {
		t.Fatal("provider call id collision crossed Agent turns")
	}
	if got := scopedToolInvocationID(context.Background(), providerCallID); got != providerCallID {
		t.Fatalf("isolated tool test fallback=%q, want provider call id", got)
	}
}

func TestRunToolCallsScopesProviderCallIDToAgentTurn(t *testing.T) {
	runner := &fakeTaskRunTrigger{}
	var runNow ToolSpec
	for _, spec := range BuildTools(nil, nil, runner, nil, nil) {
		if spec.Name() == "run_task_now" {
			runNow = spec
			break
		}
	}
	if runNow.Tool == nil {
		t.Fatal("run_task_now was not registered")
	}
	loop := &Loop{tools: map[string]ToolSpec{"run_task_now": runNow}}
	call := []llm.ToolCall{{
		ID:        "run_task_now_3",
		Name:      "run_task_now",
		Arguments: `{"schedule_ids":["task-a"]}`,
	}}
	run := func(traceID string) {
		t.Helper()
		state := &toolRunState{
			ownerRequest:           "立即执行一次任务",
			allowedSideEffectTool:  "run_task_now",
			successfulCalls:        make(map[string]struct{}),
			failedCalls:            make(map[string]int),
			sideEffectConstrainedTurn: true,
		}
		ctx := context.WithValue(t.Context(), toolRunKey{}, state)
		ctx = context.WithValue(ctx, chatMetaKey{}, chatMeta{traceID: traceID})
		if _, err := loop.runToolCalls(ctx, 7, nil, call); err != nil {
			t.Fatalf("runToolCalls(%q): %v", traceID, err)
		}
	}

	run("turn-a")
	run("turn-a")
	run("turn-b")
	if len(runner.calls) != 3 {
		t.Fatalf("trigger calls=%d, want 3: %v", len(runner.calls), runner.calls)
	}
	firstKey := strings.Split(runner.calls[0], "\x00")[1]
	replayKey := strings.Split(runner.calls[1], "\x00")[1]
	secondTurnKey := strings.Split(runner.calls[2], "\x00")[1]
	if firstKey != replayKey {
		t.Fatalf("same turn replay changed idempotency key: %q vs %q",
			firstKey, replayKey)
	}
	if firstKey == secondTurnKey {
		t.Fatalf("provider call id collision crossed turns: %q", firstKey)
	}
}

func TestRemoveScheduleIDsRejectsRemovedAndUnknownFields(t *testing.T) {
	for _, args := range []json.RawMessage{
		json.RawMessage(`{"schedule_id":"task-a"}`),
		json.RawMessage(`{"schedule_ids":["task-a"],"unexpected":true}`),
		json.RawMessage(`{"schedule_ids":["task-a"]} {}`),
	} {
		ids, message, malformed := removeScheduleIDs(args)
		if !malformed || message == "" || len(ids) != 0 {
			t.Fatalf("已移除或未知字段必须 fail closed: args=%s ids=%v message=%q malformed=%v",
				args, ids, message, malformed)
		}
	}
}

// ============================================================
// view_profile（M5 契约 §12.3）：读工具，画像为空是常态起点而非错误。
// fakeStore（loop_test.go）同时满足 profileStore 窄接口，无需数据库。
// ============================================================

func TestViewProfileTool(t *testing.T) {
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
// update_profile（M5 契约 §12.3）：仅首次画像采集，明确意图下直接执行。
// ============================================================

func TestUpdateProfileTool(t *testing.T) {
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
		if !strings.Contains(got, "画像已首次创建") || !strings.Contains(got, "行业：金融") {
			t.Fatalf("结果应回执首次创建的画像, 实得 %q", got)
		}
	})

	t.Run("已有画像冲突返回可行动文案且不谎称更新", func(t *testing.T) {
		fs := newFakeStore()
		fs.upsertErr = types.NewAppError(types.CodeConflict, "claim authority active", nil)
		got, err := (&updateProfileTool{st: fs}).Execute(
			context.Background(), 7, json.RawMessage(`{"industry":"金融"}`))
		if err != nil {
			t.Fatalf("authority conflict should be user-actionable: %v", err)
		}
		if !strings.Contains(got, "本次未修改") ||
			!strings.Contains(got, "画像依据") ||
			strings.Contains(got, "画像已更新") {
			t.Fatalf("dishonest conflict result: %q", got)
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

// Summarize 只列提供的字段（契约 §12.3），供审计回执如实展示本次改动。
// 未提供的字段绝不出现；「不改」与「清空」必须可区分。
func TestUpdateProfileTool_Summarize(t *testing.T) {
	tool := &updateProfileTool{}

	t.Run("只列提供的字段", func(t *testing.T) {
		got := tool.Summarize(json.RawMessage(`{"industry":"金融"}`))
		if got != "更新画像：行业改为「金融」" {
			t.Fatalf("摘要不符, 实得 %q", got)
		}
		if strings.Contains(got, "职业") || strings.Contains(got, "标签") {
			t.Fatalf("未提供的字段绝不能出现在操作摘要中, 实得 %q", got)
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
		// 摘要列 13 个而只落 12 个是对用户撒谎。
		got := tool.Summarize(json.RawMessage(tagsArgs(13)))
		if !strings.Contains(got, "tag12") {
			t.Fatalf("摘要应含第 12 个标签, 实得 %q", got)
		}
		if strings.Contains(got, "tag13") {
			t.Fatalf("摘要不得列出会被截掉的标签, 实得 %q", got)
		}
	})

	t.Run("全缺省时明说不会有变更", func(t *testing.T) {
		got := tool.Summarize(json.RawMessage(`{}`))
		if got != "更新画像（未提供任何字段，不会产生变更）" {
			t.Fatalf("摘要不符, 实得 %q", got)
		}
	})

	t.Run("非法 JSON 兜底展示原始参数", func(t *testing.T) {
		// Summarize 无 error 通道，不能失败：审计摘要恒有内容可读。
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

// TestBuildTools_RetiredToolsStayAbsent keeps every retired definition/source
// surface out of the current Agent allowlist. Immutable historical tool calls
// remain audit data, but they can never become executable again.
func TestBuildTools_RetiredToolsStayAbsent(t *testing.T) {
	names := map[string]bool{}
	// Use the same non-nil dependency shape as production. A conditional
	// registration such as `if sched != nil` must not make this guard vacuous.
	for _, tl := range BuildTools(&store.Store{}, &scheduler.Scheduler{}, nil, nil, nil) {
		names[tl.Name()] = true
	}
	for _, retired := range []string{
		"set_task_strictness",
		"list_sources",
		"add_source",
		"remove_source",
		"edit_task_playbook",
	} {
		if names[retired] {
			t.Errorf("BuildTools must not register retired tool %s", retired)
		}
	}
	for _, want := range []string{
		"create_schedule", "remove_schedule", "list_schedules", "view_task_playbook",
	} {
		if !names[want] {
			t.Errorf("BuildTools should retain %s", want)
		}
	}
}

// ============================================================
// 任务手册工具（Task Playbook P0）
// ============================================================

// fakePlaybookStore 满足只读 playbookStore：books 存内容，owner 存归属。
// Get 未命中/非本人 → types.ErrNotFound。
type fakePlaybookStore struct {
	books  map[string]*types.SchedulePlaybook
	owner  map[string]int64
	getErr error
}

type fakeTaskRunEvidenceStore struct {
	evidence *store.TaskLatestRunEvidenceV1
	err      error
}

func (f fakeTaskRunEvidenceStore) GetLatestTaskRunEvidenceV1(
	context.Context,
	int64,
	string,
) (*store.TaskLatestRunEvidenceV1, error) {
	return f.evidence, f.err
}

func TestViewTaskLatestRunToolDoesNotInferMissingRuns(t *testing.T) {
	tool := &viewTaskLatestRunTool{st: fakeTaskRunEvidenceStore{}}
	got, err := tool.Execute(
		context.Background(), 7,
		json.RawMessage(`{"schedule_id":"task-kimi"}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "还没有任何已完成的运行记录") ||
		!strings.Contains(got, "不能据此声称") {
		t.Fatalf("unexpected no-run evidence reply: %q", got)
	}
}

func newFakePlaybookStore() *fakePlaybookStore {
	return &fakePlaybookStore{
		books: map[string]*types.SchedulePlaybook{}, owner: map[string]int64{},
	}
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

func TestViewTaskPlaybookTool(t *testing.T) {
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
	t.Run("旧抓取计划不作为第二产品概念暴露", func(t *testing.T) {
		fs := newFakePlaybookStore()
		fs.books["s1"] = &types.SchedulePlaybook{
			ScheduleID: "s1",
			Content:    "AI 官方新闻",
			FetchPlan:  json.RawMessage(`{"targets":[{"platform":"web","capability":"search","title":"搜索: AI","url":"vane://web/search?q=ai"}]}`),
		}
		fs.owner["s1"] = 7
		got, _ := (&viewTaskPlaybookTool{st: fs}).Execute(context.Background(), 7, json.RawMessage(`{"schedule_id":"s1"}`))
		if got != "任务手册（id=s1）：\nAI 官方新闻" {
			t.Fatalf("旧抓取计划不应暴露: %q", got)
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

// TestSummarize_锚点上卡 pins the current task-creation surface.
func TestSummarize_锚点上卡(t *testing.T) {
	const anchored = `{"schedule_id":"push-1-abc","spec":{"every_seconds":259200,"anchor_at":"2026-07-19T20:00:00+08:00"}}`
	const plain = `{"schedule_id":"push-1-abc","spec":{"every_seconds":259200}}`

	t.Run("有锚点说出锚点", func(t *testing.T) {
		got := (&createScheduleTool{}).Summarize(json.RawMessage(anchored))
		if !strings.Contains(got, "2026-07-19T20:00:00+08:00") || !strings.Contains(got, "起") {
			t.Errorf("卡面应说明从何时起，实得 %q", got)
		}
		if strings.Contains(got, "epoch") {
			t.Errorf("有锚点却说 epoch 对齐＝对用户说反话，实得 %q", got)
		}
	})
	t.Run("无锚点点明 epoch", func(t *testing.T) {
		got := (&createScheduleTool{}).Summarize(json.RawMessage(plain))
		if !strings.Contains(got, "epoch") {
			t.Errorf("无锚点应点明 epoch 对齐（否则用户以为从现在起），实得 %q", got)
		}
	})
}

// TestScheduleSchemas_含anchor_at prevents the creation surface from dropping
// the anchor that the task runtime supports.
func TestScheduleSchemas_含anchor_at(t *testing.T) {
	name, raw := "create_schedule", (&createScheduleTool{}).Parameters()
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

func TestCreateScheduleSchema_RequiresIntentAndToolCalls(t *testing.T) {
	var schema struct {
		Required   []string `json:"required"`
		Properties struct {
			ToolCalls struct {
				MinItems int `json:"minItems"`
				MaxItems int `json:"maxItems"`
				Items    struct {
					OneOf []struct {
						Required   []string `json:"required"`
						Properties struct {
							Name struct {
								Const       string `json:"const"`
								Description string `json:"description"`
							} `json:"name"`
							Arguments struct {
								Properties map[string]any `json:"properties"`
							} `json:"arguments"`
						} `json:"properties"`
					} `json:"oneOf"`
				} `json:"items"`
			} `json:"tool_calls"`
		} `json:"properties"`
	}
	if err := json.Unmarshal((&createScheduleTool{}).Parameters(), &schema); err != nil {
		t.Fatalf("create_schedule schema 不是合法 JSON: %v", err)
	}
	for _, required := range []string{"spec", "intent", "tool_calls"} {
		if !slices.Contains(schema.Required, required) {
			t.Fatalf("create_schedule 缺少根必填字段 %q：%v", required, schema.Required)
		}
	}
	calls := schema.Properties.ToolCalls
	if calls.MinItems != 1 || calls.MaxItems != 64 {
		t.Fatalf("tool_calls 边界不完整：%+v", calls)
	}
	definitions := acquisitiontool.ModelToolDefinitionsV1()
	if len(calls.Items.OneOf) != len(definitions) {
		t.Fatalf("tool_calls definitions=%d want=%d",
			len(calls.Items.OneOf), len(definitions))
	}
	allProperties := make(map[string]any)
	for index, variant := range calls.Items.OneOf {
		for _, required := range []string{"name", "arguments"} {
			if !slices.Contains(variant.Required, required) {
				t.Fatalf("tool_calls[%d] 缺少必填字段 %q：%v",
					index, required, variant.Required)
			}
		}
		if variant.Properties.Name.Const !=
			definitions[index].Contract.Name ||
			strings.TrimSpace(variant.Properties.Name.Description) == "" {
			t.Fatalf("tool_calls[%d] definition drifted: %+v",
				index, variant.Properties.Name)
		}
		for name, property := range variant.Properties.Arguments.Properties {
			allProperties[name] = property
			for _, forbidden := range []string{
				"config", "selectors", "url", "title",
			} {
				if name == forbidden {
					t.Fatalf(
						"tool_calls 不得暴露内部或可伪造字段 %q",
						forbidden,
					)
				}
			}
		}
	}
	for _, required := range []string{
		"query", "include_domains", "feed_url", "page_url", "user_id",
	} {
		if _, ok := allProperties[required]; !ok {
			t.Fatalf("tool_calls 缺少模型可理解字段 %q", required)
		}
	}
	userID, ok := allProperties["user_id"].(map[string]any)
	if !ok || userID["pattern"] != "^[0-9a-f]{24}$" ||
		!strings.Contains(fmt.Sprint(userID["description"]), "24 位小写十六进制") {
		t.Fatalf("xhs user_id schema 必须逐字暴露服务端格式约束：%+v", userID)
	}
}

// fakeSchedulePusher captures task deletion calls.
type fakeSchedulePusher struct {
	gotSpec   scheduler.ScheduleSpec
	gotID     string
	gotIDs    []string
	gotNLDesc *string
	gotKeys   []string
	calls     int
}

func (f *fakeSchedulePusher) UpdatePush(_ context.Context, id string, _ int64, spec scheduler.ScheduleSpec,
	nlDesc *string) error {
	f.calls++
	f.gotID, f.gotSpec, f.gotNLDesc = id, spec, nlDesc
	return nil
}

func (f *fakeSchedulePusher) DeletePush(_ context.Context, id string, _ int64) error {
	f.calls++
	f.gotID = id
	f.gotIDs = append(f.gotIDs, id)
	return nil
}

func (f *fakeSchedulePusher) DeletePushIdempotent(
	_ context.Context,
	id string,
	_ int64,
	key string,
) error {
	f.calls++
	f.gotID = id
	f.gotIDs = append(f.gotIDs, id)
	f.gotKeys = append(f.gotKeys, key)
	return nil
}

func TestRemoveScheduleTool_批量删除接线(t *testing.T) {

	t.Run("remove 批量去重并透传全部 id", func(t *testing.T) {
		f := &fakeSchedulePusher{}
		result, err := (&removeScheduleTool{sched: f}).Execute(context.Background(), 1,
			json.RawMessage(`{"schedule_ids":["push-1-a","push-1-b","push-1-a"]}`))
		if err != nil {
			t.Fatalf("Execute 失败: %v", err)
		}
		if !slices.Equal(f.gotIDs, []string{"push-1-a", "push-1-b"}) {
			t.Errorf("schedule_ids 透传错: %v", f.gotIDs)
		}
		if len(f.gotKeys) != 2 || f.gotKeys[0] == f.gotKeys[1] ||
			f.gotKeys[0] != removeScheduleIdempotencyKey(1, "push-1-a") {
			t.Errorf("批量删除必须为每个目标生成稳定且不同的幂等键: %v", f.gotKeys)
		}
		if result != "已删除 2 个定时推送任务。" {
			t.Errorf("批量回执不符: %q", result)
		}
	})

}
