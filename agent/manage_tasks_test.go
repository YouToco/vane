package agent

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/YouToco/vane/agentcontext"
	"github.com/YouToco/vane/llm"
	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/task"
	"github.com/YouToco/vane/types"
)

type fakeManageTaskQuery struct {
	scope store.IntelligenceScope
	query store.IntelligenceQuery
	rows  []map[string]any
	err   error
}

func (f *fakeManageTaskQuery) QueryMyIntelligence(_ context.Context, scope store.IntelligenceScope, query store.IntelligenceQuery) (*store.IntelligenceQueryResult, error) {
	f.scope, f.query = scope, query
	if f.err != nil {
		return nil, f.err
	}
	return &store.IntelligenceQueryResult{
		CatalogVersion: store.IntelligenceCatalogVersion,
		Dataset:        store.IntelligenceTasks,
		Rows:           f.rows,
		Coverage:       store.IntelligenceCoverage{Status: "complete"},
	}, nil
}

type fakeOwnerActionAuthorizer struct {
	decision OwnerActionDecision
	input    OwnerActionAuthorization
	calls    int
}

func (f *fakeOwnerActionAuthorizer) AuthorizeOwnerAction(_ context.Context, input OwnerActionAuthorization) (OwnerActionDecision, error) {
	f.calls++
	f.input = input
	return f.decision, nil
}

type fakeManageTaskRunner struct {
	refs  []string
	keys  []string
	errAt int
}

func (f *fakeManageTaskRunner) TriggerScheduleNowIdempotent(_ context.Context, ref string, _ int64, key string) error {
	f.refs = append(f.refs, ref)
	f.keys = append(f.keys, key)
	if f.errAt > 0 && len(f.refs) == f.errAt {
		return errors.New("runner failed")
	}
	return nil
}

type fakeManageTaskDeleter struct{ refs, keys []string }

func (f *fakeManageTaskDeleter) DeletePush(_ context.Context, ref string, _ int64) error {
	f.refs = append(f.refs, ref)
	return nil
}
func (f *fakeManageTaskDeleter) DeletePushIdempotent(_ context.Context, ref string, _ int64, key string) error {
	f.refs = append(f.refs, ref)
	f.keys = append(f.keys, key)
	return nil
}

type fakeManageTaskEditor struct {
	prepared task.DefinitionEditProposalInput
	outcome  task.TaskDefinitionEditOutcome
	calls    int
}

func (f *fakeManageTaskEditor) Prepare(_ context.Context, in task.DefinitionEditProposalInput) (task.DefinitionEditProposal, error) {
	f.prepared = in
	return task.DefinitionEditProposal{ID: in.ActionID, Summary: "edit"}, nil
}
func (f *fakeManageTaskEditor) Execute(_ context.Context, _ int64, _ string, _ task.TaskDefinitionEditReceiptTarget) (task.TaskDefinitionEditOutcome, error) {
	f.calls++
	return f.outcome, nil
}

func manageTasksTestContext(owner string) (context.Context, *toolRunState) {
	state := &toolRunState{ownerRequest: owner}
	ctx := context.WithValue(context.Background(), chatMetaKey{}, chatMeta{
		traceID: "turn-manage-1", userID: 42,
		scope: agentcontext.Scope{TenantID: 7, UserID: 42, SessionID: 9},
	})
	ctx = context.WithValue(ctx, toolRunKey{}, state)
	ctx = withProviderToolCallID(ctx, "provider-call-1")
	ctx = withToolInvocationID(ctx, "turn-manage-1\x00provider-call-1")
	return ctx, state
}

func manageTaskRows(refs ...string) []map[string]any {
	rows := make([]map[string]any, 0, len(refs))
	for _, ref := range refs {
		rows = append(rows, map[string]any{
			"task_ref": ref, "task_name": "任务 " + ref,
			"status": "active", "schedule": map[string]any{"cron": "0 9 * * 1"},
		})
	}
	return rows
}

func TestManageTasksRunInjectsScopeAuthorizesAndRecordsReceipt(t *testing.T) {
	queries := &fakeManageTaskQuery{rows: manageTaskRows("kimi", "claude")}
	authorizer := &fakeOwnerActionAuthorizer{decision: OwnerActionAuthorized}
	runner := &fakeManageTaskRunner{}
	spec := NewManageTasksTool(ManageTasksDeps{Queries: queries, Runner: runner, Authorizer: authorizer})
	if err := spec.validate(); err != nil {
		t.Fatalf("tool policy/schema invalid: %v", err)
	}
	ctx, state := manageTasksTestContext("现在运行 Kimi 和 Claude 两个监控任务")
	result, err := spec.Execute(ctx, 42, json.RawMessage(`{"action":"run","task_refs":["kimi","claude"]}`))
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if queries.scope.TenantID != 7 || queries.scope.UserID != 42 || queries.scope.SessionID == nil || *queries.scope.SessionID != 9 {
		t.Fatalf("authenticated scope not injected: %+v", queries.scope)
	}
	if authorizer.calls != 1 || authorizer.input.OwnerRequest != "现在运行 Kimi 和 Claude 两个监控任务" || len(authorizer.input.Targets) != 2 {
		t.Fatalf("authorizer input mismatch: calls=%d input=%+v", authorizer.calls, authorizer.input)
	}
	if strings.Join(runner.refs, ",") != "kimi,claude" || len(runner.keys) != 2 || runner.keys[0] == runner.keys[1] {
		t.Fatalf("durable run calls mismatch: refs=%v keys=%v", runner.refs, runner.keys)
	}
	if !strings.Contains(result, "周期调度未改变") || len(state.actionReceipts) != 1 || !json.Valid(state.actionReceipts[0]) {
		t.Fatalf("result/receipt mismatch: result=%q receipts=%s", result, state.actionReceipts)
	}
}

func TestManageTasksAmbiguousNeverWrites(t *testing.T) {
	queries := &fakeManageTaskQuery{rows: manageTaskRows("kimi")}
	authorizer := &fakeOwnerActionAuthorizer{decision: OwnerActionAmbiguous}
	deleter := &fakeManageTaskDeleter{}
	spec := NewManageTasksTool(ManageTasksDeps{Queries: queries, Deleter: deleter, Authorizer: authorizer})
	ctx, state := manageTasksTestContext("Kimi 那个是不是删掉比较好")
	result, err := spec.Execute(ctx, 42, json.RawMessage(`{"action":"delete","task_refs":["kimi"]}`))
	if err != nil || !strings.Contains(result, "歧义") {
		t.Fatalf("ambiguous result=%q err=%v", result, err)
	}
	if len(deleter.refs) != 0 || len(state.actionReceipts) != 0 {
		t.Fatalf("ambiguous action wrote: refs=%v receipts=%v", deleter.refs, state.actionReceipts)
	}
}

func TestManageTasksPartialFailureKeepsReceiptAndStableTurnKeys(t *testing.T) {
	queries := &fakeManageTaskQuery{rows: manageTaskRows("kimi", "claude")}
	authorizer := &fakeOwnerActionAuthorizer{decision: OwnerActionAuthorized}
	runner := &fakeManageTaskRunner{errAt: 2}
	spec := NewManageTasksTool(ManageTasksDeps{Queries: queries, Runner: runner, Authorizer: authorizer})
	ctx, state := manageTasksTestContext("运行 Kimi 和 Claude")
	result, err := spec.Execute(ctx, 42, json.RawMessage(`{"action":"run","task_refs":["kimi","claude"]}`))
	if err != nil || !strings.Contains(result, "未完成") || len(state.actionReceipts) != 1 || !strings.Contains(string(state.actionReceipts[0]), "partial_failure") || !strings.Contains(string(state.actionReceipts[0]), "kimi") {
		t.Fatalf("partial failure lost receipt: err=%v receipts=%s", err, state.actionReceipts)
	}
	// A provider retry may carry another call id, but the same authenticated
	// owner turn must produce the same per-target durable key.
	ctx = withToolInvocationID(ctx, "turn-manage-1\x00provider-call-2")
	runner.errAt = 0
	if _, err := spec.Execute(ctx, 42, json.RawMessage(`{"action":"run","task_refs":["kimi","claude"]}`)); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if runner.keys[0] != runner.keys[2] || runner.keys[1] != runner.keys[3] {
		t.Fatalf("turn-scoped idempotency keys drifted: %v", runner.keys)
	}
}

func TestManageTasksEditBuildsControllerCommandWithoutUserIDArgument(t *testing.T) {
	queries := &fakeManageTaskQuery{rows: manageTaskRows("kimi")}
	authorizer := &fakeOwnerActionAuthorizer{decision: OwnerActionAuthorized}
	editor := &fakeManageTaskEditor{outcome: task.TaskDefinitionEditOutcome{
		Status: types.TaskDefinitionEditOperationStatusCompleted, TaskID: "kimi",
	}}
	spec := NewManageTasksTool(ManageTasksDeps{Queries: queries, Edits: editor, Authorizer: authorizer})
	ctx, state := manageTasksTestContext("把 Kimi 任务改成每周一九点，手册改为只看官方套餐页")
	result, err := spec.Execute(ctx, 42, json.RawMessage(`{"action":"edit","task_ref":"kimi","changes":{"spec":{"cron":"0 9 * * 1","tz":"Asia/Shanghai"},"intent":"只看官方套餐页"}}`))
	if err != nil || !strings.Contains(result, "已修改") {
		t.Fatalf("edit result=%q err=%v", result, err)
	}
	var command map[string]json.RawMessage
	if err := json.Unmarshal(editor.prepared.RawArgs, &command); err != nil {
		t.Fatal(err)
	}
	if string(command["task_id"]) != `"kimi"` || command["user_id"] != nil || editor.prepared.UserID != 42 || editor.prepared.SessionID == nil || *editor.prepared.SessionID != 9 {
		t.Fatalf("controller command/scope mismatch: command=%s prepared=%+v", editor.prepared.RawArgs, editor.prepared)
	}
	if editor.calls != 1 || len(state.actionReceipts) != 1 {
		t.Fatalf("edit calls/receipts=%d/%d", editor.calls, len(state.actionReceipts))
	}
}

func TestManageTasksFailsClosedForExternalOrMissingTarget(t *testing.T) {
	queries := &fakeManageTaskQuery{rows: nil}
	authorizer := &fakeOwnerActionAuthorizer{decision: OwnerActionAuthorized}
	runner := &fakeManageTaskRunner{}
	spec := NewManageTasksTool(ManageTasksDeps{Queries: queries, Runner: runner, Authorizer: authorizer})
	ctx, state := manageTasksTestContext("运行 Kimi")
	state.untrustedExternalResult = true
	result, err := spec.Execute(ctx, 42, json.RawMessage(`{"action":"run","task_refs":["kimi"]}`))
	if err != nil || !strings.Contains(result, "认证上下文") || queries.query.Dataset != "" {
		t.Fatalf("external fail-close result=%q err=%v query=%+v", result, err, queries.query)
	}

	ctx, _ = manageTasksTestContext("运行 Kimi")
	result, err = spec.Execute(ctx, 42, json.RawMessage(`{"action":"run","task_refs":["kimi"]}`))
	if err != nil || !strings.Contains(result, "无法") || authorizer.calls != 0 || len(runner.refs) != 0 {
		t.Fatalf("missing target fail-close result=%q err=%v auth=%d writes=%v", result, err, authorizer.calls, runner.refs)
	}
}

func TestManageTasksRejectsMalformedUnionBeforeQuery(t *testing.T) {
	queries := &fakeManageTaskQuery{err: errors.New("must not query")}
	spec := NewManageTasksTool(ManageTasksDeps{Queries: queries})
	ctx, _ := manageTasksTestContext("删任务")
	cases := []string{
		`{"action":"edit","task_ref":"a","changes":{}}`,
		`{"action":"run","task_ref":"a"}`,
		`{"action":"delete","task_refs":[" a"]}`,
		`{"action":"create","task_refs":["a"]}`,
		`{"action":"run","task_refs":["a"],"tenant_id":7}`,
	}
	for _, raw := range cases {
		if result, err := spec.Execute(ctx, 42, json.RawMessage(raw)); err != nil || strings.TrimSpace(result) == "" {
			t.Fatalf("raw=%s result=%q err=%v", raw, result, err)
		}
	}
	if queries.query.Dataset != "" {
		t.Fatalf("malformed input reached query: %+v", queries.query)
	}
}

func TestAgentFirstGeneralChatInventoryHidesLegacyTaskTools(t *testing.T) {
	tools := BuildTools(&store.Store{}, nil, nil, nil, nil)
	tools = append(tools, NewQueryMyIntelligenceTool(&fakeManageTaskQuery{}),
		NewManageTasksTool(ManageTasksDeps{}))
	loop, err := NewChecked(Deps{Tools: tools, AgentFirstEnabled: true,
		AgentFirstCanaryUserID: 42, Evidence: &fakeAgentEvidenceWriter{}})
	if err != nil {
		t.Fatalf("build loop: %v", err)
	}
	defs := loop.requestTools(&toolRunState{
		agentFirstEnabled: true, intents: knownToolIntents,
	})
	got := make(map[string]bool, len(defs))
	for _, def := range defs {
		got[def.Name] = true
	}
	for _, required := range []string{"query_my_intelligence", "manage_tasks", "update_profile"} {
		if !got[required] {
			t.Errorf("Agent-first inventory missing %s: %v", required, got)
		}
	}
	for _, retired := range []string{
		"list_schedules", "view_task_playbook", "view_task_latest_run",
		"view_profile", "create_schedule", "edit_task_definition",
		"run_task_now", "remove_schedule",
	} {
		if got[retired] {
			t.Errorf("Agent-first inventory exposed legacy tool %s", retired)
		}
	}
	legacyDefs := loop.requestTools(&toolRunState{
		agentFirstEnabled: false, intents: knownToolIntents,
	})
	for _, def := range legacyDefs {
		if agentFirstOnlyTool(def.Name) {
			t.Fatalf("non-canary inventory exposed %s", def.Name)
		}
	}
	if !strings.Contains(agentFirstSystemNote, "Agent-first 工具环境") ||
		!strings.Contains(agentFirstSystemNote, "query_my_intelligence") ||
		!strings.Contains(agentFirstSystemNote, "manage_tasks") {
		t.Fatalf("Agent-first environment note missing: %q", agentFirstSystemNote)
	}
}

func TestAgentFirstCanaryDoesNotImplicitlyReadOrInjectProfile(t *testing.T) {
	fs := newFakeStore()
	fs.profiles[42] = &types.Profile{UserID: 42, Industry: "SECRET-INDUSTRY"}
	writer := &fakeAgentEvidenceWriter{}
	loop := New(Deps{
		Store: fs, Profiles: fs, Evidence: writer,
		Tools:             []ToolSpec{NewQueryMyIntelligenceTool(&fakeManageTaskQuery{})},
		AgentFirstEnabled: true, AgentFirstCanaryUserID: 42, MaxTurns: 1,
	})
	var request llm.ChatRequest
	loop.chatFn = func(_ context.Context, in llm.ChatRequest) (*llm.ChatResponse, error) {
		request = in
		return &llm.ChatResponse{Content: "直接回答"}, nil
	}
	outcome, err := loop.HandleMessage(t.Context(), 42, "你好")
	if err != nil || outcome.Reply != "直接回答" {
		t.Fatalf("outcome=%+v err=%v", outcome, err)
	}
	encoded, _ := json.Marshal(request.Messages)
	if strings.Contains(string(encoded), "SECRET-INDUSTRY") ||
		!strings.Contains(string(encoded), "Agent-first 工具环境") {
		t.Fatalf("Agent-first request leaked implicit profile or missed environment: %s", encoded)
	}
}

func TestAgentFirstToolsCannotExecuteForNonCanaryUser(t *testing.T) {
	queries := &fakeManageTaskQuery{rows: manageTaskRows("kimi")}
	writer := &fakeAgentEvidenceWriter{}
	queryTool := NewQueryMyIntelligenceTool(queries)
	loop := New(Deps{
		Tools: []ToolSpec{queryTool}, Evidence: writer,
		AgentFirstEnabled: true, AgentFirstCanaryUserID: 42,
	})
	state := &toolRunState{ownerRequest: "查任务", agentFirstEnabled: false}
	ctx := context.WithValue(t.Context(), toolRunKey{}, state)
	ctx = context.WithValue(ctx, chatMetaKey{}, chatMeta{
		traceID: "non-canary", userID: 43,
		scope: agentcontext.Scope{TenantID: 7, UserID: 43, SessionID: 10},
	})
	sessionID := int64(10)
	messages, err := loop.runToolCalls(ctx, 43, &sessionID, []llm.ToolCall{{
		ID: "hallucinated", Name: queryTool.Name(), Arguments: `{"dataset":"tasks"}`,
	}})
	if err != nil || len(messages) != 1 || queries.query.Dataset != "" {
		t.Fatalf("non-canary execution escaped: messages=%+v query=%+v err=%v", messages, queries.query, err)
	}
}

func TestAgentFirstUpdateProfileUsesGenericOwnerAuthorizer(t *testing.T) {
	fs := newFakeStore()
	authorizer := &fakeOwnerActionAuthorizer{decision: OwnerActionAmbiguous}
	spec := NewAuthorizedUpdateProfileTool(fs, authorizer)
	ctx, state := manageTasksTestContext("我是 AI 产品经理，关注机器人")
	state.agentFirstEnabled = true
	result, err := spec.Execute(ctx, 42, json.RawMessage(`{"occupation":"AI 产品经理","tags":["机器人"]}`))
	if err != nil || !strings.Contains(result, "歧义") || len(fs.upsertCalls) != 0 || authorizer.calls != 1 {
		t.Fatalf("ambiguous profile write escaped: result=%q err=%v writes=%d auth=%d", result, err, len(fs.upsertCalls), authorizer.calls)
	}

	authorizer.decision = OwnerActionAuthorized
	loop := New(Deps{Tools: []ToolSpec{spec}, Evidence: &fakeAgentEvidenceWriter{}})
	ctx, state = manageTasksTestContext("我是 AI 产品经理，关注机器人")
	state.agentFirstEnabled = true
	ctx = context.WithValue(ctx, toolRunKey{}, state)
	sessionID := int64(9)
	messages, err := loop.runToolCalls(ctx, 42, &sessionID, []llm.ToolCall{{
		ID: "profile-call", Name: "update_profile",
		Arguments: `{"occupation":"AI 产品经理","tags":["机器人"]}`,
	}})
	if err != nil || len(messages) != 1 || len(fs.upsertCalls) != 1 ||
		len(state.toolEvidence) != 1 {
		t.Fatalf("authorized profile write mismatch: messages=%+v err=%v writes=%d evidence=%d",
			messages, err, len(fs.upsertCalls), len(state.toolEvidence))
	}
}
