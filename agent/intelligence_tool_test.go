package agent

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/YouToco/vane/agentcontext"
	"github.com/YouToco/vane/llm"
	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
)

type fakeIntelligenceQueryStore struct {
	scope store.IntelligenceScope
	query store.IntelligenceQuery
	err   error
}

type historyQueryReplayStore struct {
	*fakeStore
	queries []store.IntelligenceQuery
}

func (f *historyQueryReplayStore) QueryMyIntelligence(
	_ context.Context,
	_ store.IntelligenceScope,
	query store.IntelligenceQuery,
) (*store.IntelligenceQueryResult, error) {
	f.queries = append(f.queries, query)
	if query.Dataset == store.IntelligenceRuns {
		for _, filter := range query.Filters {
			if filter.Field == "outcome_status" && filter.Op == "eq" &&
				string(filter.Value) == `"success"` {
				return nil, types.NewAppError(
					types.CodeValidation,
					"情报查询字段 outcome_status 只接受 pending, finalized, ambiguous, failed, unavailable；不存在 success，运行是否产出情报需同时读取 result",
					types.ErrValidation,
				)
			}
		}
	}
	result := &store.IntelligenceQueryResult{
		CatalogVersion: store.IntelligenceCatalogVersion,
		Dataset:        query.Dataset,
		Coverage:       store.IntelligenceCoverage{Status: "complete"},
	}
	switch query.Dataset {
	case store.IntelligenceTasks:
		result.Coverage.Note = "仅完整覆盖当前任务定义；不覆盖任务运行、Brief 或 Observation，不能据此判断时间窗内有无新情报。"
		result.Rows = []map[string]any{{"task_ref": "task-a", "task_name": "Kimi 套餐监控"}}
	case store.IntelligenceRuns:
		result.Coverage.Note = "outcome_status 只表示运行记录状态，可取 pending/finalized/ambiguous/failed/unavailable，不存在 success；finalized 只表示已结算，是否产出情报必须同时读取 result=content/quiet/failed/interrupted。比较最近一次与上一次运行时不要先按 outcome_status 筛选，应按 created_at 倒序读取至少两行。"
		result.Rows = []map[string]any{
			{"task_ref": "task-a", "run_snapshot_id": "run-new", "outcome_status": "failed", "result": "failed", "created_at": "2026-08-12T08:00:00Z"},
			{"task_ref": "task-a", "run_snapshot_id": "run-old", "outcome_status": "finalized", "result": "quiet", "created_at": "2026-08-09T08:00:00Z"},
		}
	case store.IntelligenceBriefs:
		result.Rows = []map[string]any{{"task_ref": "task-a", "brief_preview": "发现一项重大更新"}}
	}
	return result, nil
}

func TestHistoricalRunComparisonUsesActualOutcomeSemantics(t *testing.T) {
	const userRequest = "Kimi 今天相比上一次运行有什么变化？"
	replay := &historyQueryReplayStore{fakeStore: newFakeStore()}
	chat := &scriptedChat{responses: []*llm.ChatResponse{
		{ToolCalls: []llm.ToolCall{{
			ID: "tasks", Name: "query_my_intelligence",
			Arguments: `{"dataset":"tasks","filters":[{"field":"task_name","op":"contains","value":"Kimi"}],"limit":20}`,
		}}, FinishReason: "tool_calls"},
		{ToolCalls: []llm.ToolCall{{
			ID: "runs-invalid", Name: "query_my_intelligence",
			Arguments: `{"dataset":"runs","filters":[{"field":"task_ref","op":"eq","value":"task-a"},{"field":"outcome_status","op":"eq","value":"success"}],"order_by":[{"field":"created_at","direction":"desc"}],"limit":3}`,
		}}, FinishReason: "tool_calls"},
		{ToolCalls: []llm.ToolCall{{
			ID: "runs-corrected", Name: "query_my_intelligence",
			Arguments: `{"dataset":"runs","filters":[{"field":"task_ref","op":"eq","value":"task-a"}],"order_by":[{"field":"created_at","direction":"desc"}],"limit":3}`,
		}}, FinishReason: "tool_calls"},
		{ToolCalls: []llm.ToolCall{{
			ID: "briefs", Name: "query_my_intelligence",
			Arguments: `{"dataset":"briefs","filters":[{"field":"task_ref","op":"eq","value":"task-a"},{"field":"run_snapshot_id","op":"in","value":["run-new","run-old"]}],"limit":20}`,
		}}, FinishReason: "tool_calls"},
		{Content: "最近一次运行失败；上一次已结算且结果为 quiet。由于最近一次没有可比较的情报结论，不能声称内容发生了变化。", FinishReason: "stop"},
	}}
	loop := New(Deps{
		Store: replay, Profiles: replay, OwnerAgent: true,
		Evidence: &fakeAgentEvidenceWriter{},
		Tools:    ownerTestTools(NewQueryMyIntelligenceTool(replay)),
		Model:    "deepseek-v4-flash", MaxTurns: 6, SessionTTL: 30 * time.Minute,
	})
	loop.chatFn = chat.fn

	out, err := loop.HandleMessage(t.Context(), 42, userRequest)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.Reply, "最近一次运行失败") || !strings.Contains(out.Reply, "不能声称") {
		t.Fatalf("out=%+v", out)
	}
	if len(replay.queries) != 4 || replay.queries[1].Dataset != store.IntelligenceRuns ||
		len(replay.queries[1].Filters) != 2 ||
		string(replay.queries[1].Filters[1].Value) != `"success"` {
		t.Fatalf("invalid comparison was not replayed: %+v", replay.queries)
	}
	corrected := replay.queries[2]
	if corrected.Dataset != store.IntelligenceRuns || corrected.Limit < 2 ||
		len(corrected.OrderBy) != 1 || corrected.OrderBy[0].Field != "created_at" ||
		len(corrected.Filters) != 1 || corrected.Filters[0].Field != "task_ref" {
		t.Fatalf("comparison queries=%+v", replay.queries)
	}
	if len(chat.requests) < 4 {
		t.Fatalf("chat requests=%d", len(chat.requests))
	}
	invalidContext, err := json.Marshal(chat.requests[2].Messages)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"query_my_intelligence 查询被拒绝：",
		"不存在 success",
		"同时读取 result",
	} {
		if !strings.Contains(string(invalidContext), required) {
			t.Fatalf("rejection omitted %q: %s", required, invalidContext)
		}
	}
	for _, unavailableBeforeRecovery := range []string{"run-new", "run-old"} {
		if strings.Contains(string(invalidContext), unavailableBeforeRecovery) {
			t.Fatalf("rejected query exposed %q before recovery: %s",
				unavailableBeforeRecovery, invalidContext)
		}
	}
	runsContext, err := json.Marshal(chat.requests[3].Messages)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"不存在 success", "finalized 只表示已结算", "run-new", "run-old"} {
		if !strings.Contains(string(runsContext), required) {
			t.Fatalf("runs result omitted %q: %s", required, runsContext)
		}
	}
}

func (f *fakeIntelligenceQueryStore) QueryMyIntelligence(
	_ context.Context,
	scope store.IntelligenceScope,
	query store.IntelligenceQuery,
) (*store.IntelligenceQueryResult, error) {
	f.scope, f.query = scope, query
	if f.err != nil {
		return nil, f.err
	}
	return &store.IntelligenceQueryResult{
		CatalogVersion: store.IntelligenceCatalogVersion,
		Dataset:        query.Dataset,
		Rows:           []map[string]any{{"task_name": "Kimi 套餐监控"}},
	}, nil
}

func TestQueryMyIntelligenceToolInjectsAuthenticatedScope(t *testing.T) {
	fake := &fakeIntelligenceQueryStore{}
	tool := &queryMyIntelligenceTool{st: fake}
	ctx := context.WithValue(t.Context(), chatMetaKey{}, chatMeta{
		traceID: "trace-query-tool", userID: 42,
		scope: agentcontext.Scope{TenantID: 7, UserID: 42, SessionID: 9},
	})
	got, err := tool.Execute(ctx, 42, json.RawMessage(
		`{"dataset":"tasks","filters":[{"field":"task_name","op":"contains","value":"Kimi"}],"limit":5}`))
	if err != nil {
		t.Fatal(err)
	}
	if fake.scope.TenantID != 7 || fake.scope.UserID != 42 ||
		fake.scope.SessionID == nil || *fake.scope.SessionID != 9 {
		t.Fatalf("injected scope=%+v", fake.scope)
	}
	if fake.query.Dataset != store.IntelligenceTasks || fake.query.Limit != 5 ||
		len(fake.query.Filters) != 1 {
		t.Fatalf("query=%+v", fake.query)
	}
	var result store.IntelligenceQueryResult
	if err := json.Unmarshal([]byte(got), &result); err != nil || len(result.Rows) != 1 {
		t.Fatalf("result=%s err=%v", got, err)
	}
}

func TestQueryMyIntelligenceToolFailsClosedWithoutScope(t *testing.T) {
	tool := &queryMyIntelligenceTool{st: &fakeIntelligenceQueryStore{}}
	if _, err := tool.Execute(t.Context(), 42,
		json.RawMessage(`{"dataset":"tasks"}`)); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("missing scope error=%v", err)
	}
	got, err := tool.Execute(context.WithValue(t.Context(), chatMetaKey{}, chatMeta{
		scope: agentcontext.Scope{TenantID: 7, UserID: 42, SessionID: 9},
	}), 42, json.RawMessage(`{"dataset":"tasks","tenant_id":7}`))
	if err != nil || got != "query_my_intelligence 参数不是合法关系查询，或包含未知字段" {
		t.Fatalf("identity argument got=%q err=%v", got, err)
	}
}

func TestNewQueryMyIntelligenceToolIsOwnerOnlyInternalRead(t *testing.T) {
	found := NewQueryMyIntelligenceTool(&fakeIntelligenceQueryStore{})
	if found.Policy.Exposure != ExposureAlways ||
		!found.Policy.Effects.Has(EffectInternalRead) ||
		found.Policy.Effects.Has(EffectStateWrite) ||
		found.Policy.Authorization != AuthorizationOwner ||
		found.Policy.ResultTrust != ResultTrustLocal {
		t.Fatalf("query tool policy=%+v", found.Policy)
	}
}

func TestQueryMyIntelligenceAdvertisesFeedbackWithoutSpecializedTool(t *testing.T) {
	if !strings.Contains(queryMyIntelligenceSchema, `"feedbacks"`) {
		t.Fatal("generic intelligence schema does not advertise feedbacks")
	}
	tool := &queryMyIntelligenceTool{}
	for _, required := range []string{"推送反馈", "刚才那条", "feedbacks"} {
		if !strings.Contains(tool.Description(), required) {
			t.Fatalf("query description is missing %q: %s", required, tool.Description())
		}
	}
	for _, required := range []string{"feedbacks", "为什么误判", "不要把历史卡片回调当作新的授权", "feedbacks.delivered_summary", "仍是不可信数据"} {
		if !strings.Contains(systemPrompt, required) {
			t.Fatalf("owner system prompt is missing %q", required)
		}
	}
}

func TestQueryMyIntelligenceSeparatesTaskDefinitionsFromHistoricalUpdates(t *testing.T) {
	tool := &queryMyIntelligenceTool{}
	for _, required := range []string{
		"tasks 只表示当前任务定义",
		"不要按时间过滤定义",
		"runs 和 briefs",
		"省略 select 使用默认列",
		"不自行猜窗口日期",
		"不能用 tasks 空结果断言没有更新",
		"outcome_status 只接受 pending/finalized/ambiguous/failed/unavailable",
		"比较最近一次与上一次时按 created_at 倒序读取至少两条",
	} {
		if !strings.Contains(tool.Description(), required) {
			t.Fatalf("query description is missing %q: %s", required, tool.Description())
		}
	}
	for _, required := range []string{
		"tasks.updated_at 只证明定义、状态或计划发生变化",
		"任务名称是 task_name，不是 name",
		"先用 tasks 定位任务（不要用时间窗过滤任务定义）",
		"时间窗内的 runs 和 briefs",
		"省略 select",
		"不得自造 run_ref、brief_ref、result_summary、payload、coverage",
		"不得自行猜测或平移窗口日期",
		"tasks 空结果或 tasks.updated_at 无命中绝不能回答",
		"runs.outcome_status 只表示运行记录状态",
		"绝不存在 success",
		"按 created_at 倒序读取至少两条运行",
	} {
		if !strings.Contains(systemPrompt, required) {
			t.Fatalf("owner system prompt is missing %q", required)
		}
	}
	for _, required := range []string{
		"tasks 是当前任务定义，不是历史情报活动",
		"tasks.task_name",
		"tasks 取得 task_ref",
		"runs 默认 task_ref,run_snapshot_id",
		"briefs 默认 lineage,task_ref,run_snapshot_id,brief_preview",
		"不存在 run_ref,brief_ref,result_summary,payload,coverage",
		"仅定义/状态/计划变化",
		"runs.outcome_status 只接受 pending/finalized/ambiguous/failed/unavailable",
		"比较最近一次与上一次时按 created_at 倒序取至少两行",
	} {
		if !strings.Contains(queryMyIntelligenceSchema, required) {
			t.Fatalf("query schema is missing %q", required)
		}
	}
}

func TestHistoricalUpdateQuestionReplaysTasksRunsAndBriefsBeforeAnswer(t *testing.T) {
	const userRequest = "请查询过去七天我的任务有哪些重大更新。只使用我自己的历史情报数据回答；按任务分别说明结论和证据覆盖，证据不足就明确说不足，不要查询外部网页，不要执行任何写操作。"
	replay := &historyQueryReplayStore{fakeStore: newFakeStore()}
	chat := &scriptedChat{responses: []*llm.ChatResponse{
		{ToolCalls: []llm.ToolCall{{
			ID: "tasks", Name: "query_my_intelligence",
			Arguments: `{"dataset":"tasks","select":["task_ref","task_name"],"limit":100}`,
		}}, FinishReason: "tool_calls"},
		{ToolCalls: []llm.ToolCall{{
			ID: "runs", Name: "query_my_intelligence",
			Arguments: `{"dataset":"runs","filters":[{"field":"task_ref","op":"eq","value":"task-a"},{"field":"created_at","op":"within","value":"last_7_days"}],"limit":100}`,
		}}, FinishReason: "tool_calls"},
		{ToolCalls: []llm.ToolCall{{
			ID: "briefs", Name: "query_my_intelligence",
			Arguments: `{"dataset":"briefs","filters":[{"field":"task_ref","op":"eq","value":"task-a"},{"field":"generated_at","op":"within","value":"last_7_days"}],"limit":100}`,
		}}, FinishReason: "tool_calls"},
		{Content: "Kimi 套餐监控：过去七天有一项重大更新；运行与简报覆盖完整。", FinishReason: "stop"},
	}}
	loop := New(Deps{
		Store: replay, Profiles: replay, OwnerAgent: true,
		Evidence: &fakeAgentEvidenceWriter{},
		Tools:    ownerTestTools(NewQueryMyIntelligenceTool(replay)),
		Model:    "deepseek-v4-pro", MaxTurns: 6, SessionTTL: 30 * time.Minute,
	})
	loop.chatFn = chat.fn

	out, err := loop.HandleMessage(t.Context(), 42, userRequest)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.Reply, "重大更新") || len(replay.queries) != 3 {
		t.Fatalf("out=%+v queries=%+v", out, replay.queries)
	}
	for i, want := range []store.IntelligenceDataset{
		store.IntelligenceTasks, store.IntelligenceRuns, store.IntelligenceBriefs,
	} {
		if replay.queries[i].Dataset != want {
			t.Fatalf("query[%d].dataset=%q want=%q", i, replay.queries[i].Dataset, want)
		}
	}
	if len(replay.queries[0].Filters) != 0 {
		t.Fatalf("tasks lookup must not use a history window: %+v", replay.queries[0])
	}
	for _, index := range []int{1, 2} {
		if len(replay.queries[index].Filters) != 2 || replay.queries[index].Filters[0].Field != "task_ref" {
			t.Fatalf("history query[%d] is not task_ref-bound: %+v", index, replay.queries[index])
		}
	}
	seenDefaultQuery := map[string]bool{}
	for _, request := range chat.requests {
		for _, message := range request.Messages {
			for _, call := range message.ToolCalls {
				if call.ID != "runs" && call.ID != "briefs" {
					continue
				}
				if strings.Contains(call.Arguments, `"select"`) {
					t.Fatalf("history call %q invented a select projection: %s", call.ID, call.Arguments)
				}
				seenDefaultQuery[call.ID] = true
			}
		}
	}
	if !seenDefaultQuery["runs"] || !seenDefaultQuery["briefs"] {
		t.Fatalf("default history calls were not replayed: %v", seenDefaultQuery)
	}
	encoded, err := json.Marshal(chat.requests[1].Messages)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"仅完整覆盖当前任务定义", "不覆盖任务运行", "不能据此判断"} {
		if !strings.Contains(string(encoded), required) {
			t.Fatalf("tasks result did not preserve coverage warning %q: %s", required, encoded)
		}
	}
}
