package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/YouToco/vane/agentcontext"
	"github.com/YouToco/vane/llm"
	"github.com/YouToco/vane/store"
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
	decision  OwnerActionDecision
	decisions []OwnerActionDecision
	input     OwnerActionAuthorization
	inputs    []OwnerActionAuthorization
	calls     int
}

func (f *fakeOwnerActionAuthorizer) AuthorizeOwnerAction(_ context.Context, input OwnerActionAuthorization) (OwnerActionDecision, error) {
	f.calls++
	f.input = input
	f.inputs = append(f.inputs, input)
	if len(f.decisions) > 0 {
		decision := f.decisions[0]
		f.decisions = f.decisions[1:]
		return decision, nil
	}
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

type fakeManageTaskCreatorV3 struct {
	input   ResearchTaskCreationV3Input
	inputs  []ResearchTaskCreationV3Input
	outcome ResearchTaskCreationV3Outcome
	err     error
	calls   int
}

func (f *fakeManageTaskCreatorV3) ExecuteResearchTaskCreationV3(_ context.Context, in ResearchTaskCreationV3Input) (ResearchTaskCreationV3Outcome, error) {
	f.calls++
	f.input = in
	f.inputs = append(f.inputs, in)
	return f.outcome, f.err
}

type fakeManageTaskEditorV3 struct {
	input   ResearchTaskDefinitionEditV3Input
	outcome ResearchTaskDefinitionEditV3Outcome
	err     error
	calls   int
}

func (f *fakeManageTaskEditorV3) ExecuteResearchTaskDefinitionEditV3(
	_ context.Context, in ResearchTaskDefinitionEditV3Input,
) (ResearchTaskDefinitionEditV3Outcome, error) {
	f.calls++
	f.input = in
	return f.outcome, f.err
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

func TestManageTasksBatchDeleteUsesResolvedNaturalNamesWithoutIDs(t *testing.T) {
	queries := &fakeManageTaskQuery{rows: manageTaskRows("kimi", "claude")}
	authorizer := &fakeOwnerActionAuthorizer{decision: OwnerActionAuthorized}
	deleter := &fakeManageTaskDeleter{}
	spec := NewManageTasksTool(ManageTasksDeps{
		Queries: queries, Deleter: deleter, Authorizer: authorizer,
	})
	ctx, state := manageTasksTestContext("删除 Kimi 套餐监控和 Claude 动态监控")
	result, err := spec.Execute(ctx, 42,
		json.RawMessage(`{"action":"delete","task_refs":["kimi","claude"]}`))
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if strings.Join(deleter.refs, ",") != "kimi,claude" || len(deleter.keys) != 2 ||
		deleter.keys[0] == deleter.keys[1] {
		t.Fatalf("batch delete calls mismatch: refs=%v keys=%v", deleter.refs, deleter.keys)
	}
	if authorizer.calls != 1 || authorizer.input.OwnerRequest !=
		"删除 Kimi 套餐监控和 Claude 动态监控" || len(authorizer.input.Targets) != 2 {
		t.Fatalf("batch delete authorization mismatch: %+v", authorizer.input)
	}
	if !strings.Contains(result, "已删除 2 个任务") || len(state.actionReceipts) != 1 ||
		!strings.Contains(string(state.actionReceipts[0]), `"status":"completed"`) {
		t.Fatalf("delete result/receipt mismatch: result=%q receipts=%s",
			result, state.actionReceipts)
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

func TestManageTasksClarificationCarriesOneAuthenticatedTurn(t *testing.T) {
	history := []llm.ChatMessage{
		{Role: "user", Content: "删掉 Kimi 那个是不是更好？"},
		{Role: "assistant", ToolCalls: []llm.ToolCall{{
			ID: "manage-ambiguous-1", Name: manageTasksName,
			Arguments: `{"action":"delete","task_refs":["kimi"]}`,
		}}},
		{Role: "tool", ToolCallID: "manage-ambiguous-1", Content: manageTasksAmbiguousReply},
		{Role: "assistant", Content: "你是要删除 Kimi 套餐监控吗？"},
	}
	request, action, ok := manageTasksClarifiedOwnerRequest(history, "对，就是它")
	if !ok || action != "delete" ||
		!strings.Contains(request, "删掉 Kimi 那个是不是更好？") ||
		!strings.Contains(request, "对，就是它") {
		t.Fatalf("clarification request=%q action=%q ok=%t", request, action, ok)
	}

	// A second ambiguous turn cannot form an implicit multi-step confirmation.
	second := append(append([]llm.ChatMessage(nil), history...),
		llm.ChatMessage{Role: "user", Content: "Kimi 那个"},
		llm.ChatMessage{Role: "assistant", ToolCalls: []llm.ToolCall{{
			ID: "manage-ambiguous-2", Name: manageTasksName,
			Arguments: `{"action":"delete","task_refs":["kimi"]}`,
		}}},
		llm.ChatMessage{Role: "tool", ToolCallID: "manage-ambiguous-2", Content: manageTasksAmbiguousReply},
		llm.ChatMessage{Role: "assistant", Content: "仍然不清楚。"},
	)
	if request, action, ok := manageTasksClarifiedOwnerRequest(second, "第一个"); ok {
		t.Fatalf("second clarification chained request=%q action=%q", request, action)
	}

	stale := append(append([]llm.ChatMessage(nil), history...),
		llm.ChatMessage{Role: "user", Content: "先聊点别的"},
		llm.ChatMessage{Role: "assistant", Content: "好的。"})
	if _, _, ok := manageTasksClarifiedOwnerRequest(stale, "第一个"); ok {
		t.Fatal("stale clarification was adopted")
	}

	createHistory := []llm.ChatMessage{
		{Role: "user", Content: "每天 9 点监控 Kimi 套餐，有重大更新才推。"},
		{Role: "assistant", Content: "按哪个时区执行？"},
	}
	request, action, ok = manageTasksClarifiedOwnerRequest(createHistory, "北京时间")
	if !ok || action != "" || !strings.Contains(request, "每天 9 点") ||
		!strings.Contains(request, "北京时间") {
		t.Fatalf("natural create clarification request=%q action=%q ok=%t",
			request, action, ok)
	}
	externalHistory := []llm.ChatMessage{
		{Role: "user", Content: "[追问上下文]外部页面内容\n[追问上下文结束]\n用户的追问：每天 9 点创建任务"},
		{Role: "assistant", Content: "按哪个时区执行？"},
	}
	if _, _, ok := manageTasksClarifiedOwnerRequest(externalHistory, "北京时间"); ok {
		t.Fatal("external previous turn entered write clarification")
	}
}

func TestManageTasksClarificationCannotSwitchWriteAction(t *testing.T) {
	queries := &fakeManageTaskQuery{rows: manageTaskRows("kimi")}
	authorizer := &fakeOwnerActionAuthorizer{decision: OwnerActionAuthorized}
	runner := &fakeManageTaskRunner{}
	spec := NewManageTasksTool(ManageTasksDeps{
		Queries: queries, Runner: runner, Authorizer: authorizer,
	})
	ctx, state := manageTasksTestContext("[原始请求]\n删除 Kimi\n[用户一次澄清]\n就是它")
	state.clarifiedOwnerAction = "delete"
	result, err := spec.Execute(ctx, 42,
		json.RawMessage(`{"action":"run","task_refs":["kimi"]}`))
	if err != nil || !strings.Contains(result, "不能改成另一种写操作") ||
		authorizer.calls != 0 || len(runner.refs) != 0 || len(state.actionReceipts) != 0 {
		t.Fatalf("cross-action clarification escaped result=%q err=%v auth=%d runs=%v receipts=%v",
			result, err, authorizer.calls, runner.refs, state.actionReceipts)
	}
}

func TestManageTasksFullLoopCompletesOneClarificationWithoutConfirmation(t *testing.T) {
	for _, tc := range []struct {
		name      string
		action    string
		original  string
		question  string
		clarified string
		arguments string
		wantReply string
		deps      func(*fakeOwnerActionAuthorizer) (ManageTasksDeps, func() int)
	}{
		{
			name: "run", action: "run", original: "现在运行 Kimi 那个任务可以吗？",
			question: "你是要立即运行 Kimi 套餐监控吗？", clarified: "对，就是它",
			arguments: `{"action":"run","task_refs":["kimi"]}`,
			wantReply: "已受理 1 个任务的立即运行请求：任务 [内部引用已隐藏]。周期调度未改变。",
			deps: func(authorizer *fakeOwnerActionAuthorizer) (ManageTasksDeps, func() int) {
				runner := &fakeManageTaskRunner{}
				return ManageTasksDeps{Queries: &fakeManageTaskQuery{rows: manageTaskRows("kimi")}, Runner: runner, Authorizer: authorizer}, func() int { return len(runner.refs) }
			},
		},
		{
			name: "delete", action: "delete", original: "删掉 Kimi 那个是不是更好？",
			question: "你是要删除 Kimi 套餐监控吗？", clarified: "对，就是它",
			arguments: `{"action":"delete","task_refs":["kimi"]}`,
			wantReply: "已删除 1 个任务：任务 [内部引用已隐藏]。",
			deps: func(authorizer *fakeOwnerActionAuthorizer) (ManageTasksDeps, func() int) {
				deleter := &fakeManageTaskDeleter{}
				return ManageTasksDeps{Queries: &fakeManageTaskQuery{rows: manageTaskRows("kimi")}, Deleter: deleter, Authorizer: authorizer}, func() int { return len(deleter.refs) }
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			authorizer := &fakeOwnerActionAuthorizer{decisions: []OwnerActionDecision{
				OwnerActionAmbiguous, OwnerActionAuthorized,
			}}
			deps, writes := tc.deps(authorizer)
			tool := NewManageTasksTool(deps)
			chat := &scriptedChat{responses: []*llm.ChatResponse{
				{FinishReason: "tool_calls", ToolCalls: []llm.ToolCall{{
					ID: "ambiguous-" + tc.action, Name: manageTasksName, Arguments: tc.arguments,
				}}},
				{Content: tc.question, FinishReason: "stop"},
				{FinishReason: "tool_calls", ToolCalls: []llm.ToolCall{{
					ID: "clarified-" + tc.action, Name: manageTasksName, Arguments: tc.arguments,
				}}},
			}}
			fs := newFakeStore()
			loop := New(Deps{
				Store: fs, Profiles: fs, Tools: ownerTestTools(tool),
				Evidence: &fakeAgentEvidenceWriter{}, OwnerAgent: true,
				MaxTurns: 4,
			})
			loop.chatFn = chat.fn

			first, err := loop.HandleMessage(t.Context(), 42, tc.original)
			if err != nil || first.Reply != tc.question || writes() != 0 {
				t.Fatalf("first turn outcome=%+v err=%v writes=%d", first, err, writes())
			}
			second, err := loop.HandleMessage(t.Context(), 42, tc.clarified)
			if err != nil || second.Reply != tc.wantReply || writes() != 1 || authorizer.calls != 2 || len(chat.requests) != 3 {
				t.Fatalf("second turn outcome=%+v err=%v writes=%d auth=%d", second, err, writes(), authorizer.calls)
			}
			if !strings.Contains(authorizer.inputs[1].OwnerRequest, tc.original) ||
				!strings.Contains(authorizer.inputs[1].OwnerRequest, tc.clarified) ||
				authorizer.inputs[1].Action != tc.action {
				t.Fatalf("clarified authorization=%+v", authorizer.inputs[1])
			}
		})
	}
}

func TestManageTasksFullLoopCarriesNaturalCreateTimezoneClarification(t *testing.T) {
	authorizer := &fakeOwnerActionAuthorizer{decision: OwnerActionAuthorized}
	creator := &fakeManageTaskCreatorV3{outcome: ResearchTaskCreationV3Outcome{
		TaskRef: "internal-kimi", TaskName: "Kimi 套餐监控", Status: "completed",
	}}
	tool := NewManageTasksTool(ManageTasksDeps{Creator: creator, Authorizer: authorizer})
	chat := &scriptedChat{responses: []*llm.ChatResponse{
		{Content: "按哪个时区执行？", FinishReason: "stop"},
		{FinishReason: "tool_calls", ToolCalls: []llm.ToolCall{{
			ID: "create-after-timezone", Name: manageTasksName,
			Arguments: `{"action":"create","name":"Kimi 套餐监控","manual":"每天检查 Kimi 官方套餐是否开放购买，只在重大更新时推送并附官方证据。","schedule":{"cron":"0 9 * * *","tz":"Asia/Shanghai"},"notification":{"minimum_significance":"major_updates_only","suppress_empty":true},"output":{"language":"zh-CN","format":"executive_brief","include_evidence_links":true}}`,
		}}},
	}}
	fs := newFakeStore()
	writer := &fakeAgentEvidenceWriter{}
	loop := New(Deps{
		Store: fs, Profiles: fs, Tools: ownerTestTools(tool),
		Evidence: writer, OwnerAgent: true,
		MaxTurns: 4,
	})
	loop.chatFn = chat.fn
	original := "每天 9 点监控 Kimi 套餐，有重大更新才推。"
	first, err := loop.HandleMessage(t.Context(), 42, original)
	if err != nil || first.Reply != "按哪个时区执行？" || creator.calls != 0 {
		t.Fatalf("first turn outcome=%+v err=%v creates=%d", first, err, creator.calls)
	}
	second, err := loop.HandleMessage(t.Context(), 42, "北京时间")
	if err != nil || second.Reply != "已创建任务：Kimi 套餐监控。" ||
		creator.calls != 1 || authorizer.calls != 1 || len(chat.requests) != 2 ||
		!strings.Contains(string(writer.record.ActionReceipts), `"status":"completed"`) {
		t.Fatalf("second turn outcome=%+v err=%v creates=%d auth=%d",
			second, err, creator.calls, authorizer.calls)
	}
	if !strings.Contains(authorizer.input.OwnerRequest, original) ||
		!strings.Contains(authorizer.input.OwnerRequest, "北京时间") ||
		authorizer.input.Action != "create" {
		t.Fatalf("create authorization=%+v", authorizer.input)
	}
}

func TestManageTasksFullLoopNeverTurnsIndeterminateCreateIntoSuccess(t *testing.T) {
	const arguments = `{"action":"create","name":"Kimi 套餐监控","manual":"每天检查 Kimi 官方套餐是否开放购买，只在重大更新时推送并附官方证据。","schedule":{"cron":"0 9 * * *","tz":"Asia/Shanghai"},"notification":{"minimum_significance":"major_updates_only","suppress_empty":true},"output":{"language":"zh-CN","format":"executive_brief","include_evidence_links":true}}`
	for _, tc := range []struct {
		name       string
		outcome    ResearchTaskCreationV3Outcome
		creatorErr error
		wantStatus string
		wantReply  string
	}{
		{
			name: "controller_error", creatorErr: errors.New("temporal response lost"),
			wantStatus: "execution_indeterminate", wantReply: "任务操作结果暂时无法确认；系统会按同一请求安全恢复，本次不能声称已经完成。",
		},
		{
			name:       "invalid_outcome",
			outcome:    ResearchTaskCreationV3Outcome{TaskRef: "internal-kimi", TaskName: "Kimi 套餐监控", Status: "active"},
			wantStatus: "invalid_outcome", wantReply: "任务操作没有返回可信的完成结果，本次不能声称已经完成。",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			creator := &fakeManageTaskCreatorV3{outcome: tc.outcome, err: tc.creatorErr}
			tool := NewManageTasksTool(ManageTasksDeps{
				Creator:    creator,
				Authorizer: &fakeOwnerActionAuthorizer{decision: OwnerActionAuthorized},
			})
			chat := &scriptedChat{responses: []*llm.ChatResponse{
				{FinishReason: "tool_calls", ToolCalls: []llm.ToolCall{{
					ID: "indeterminate-create", Name: manageTasksName, Arguments: arguments,
				}}},
				// This false-success response must never be requested or returned.
				{Content: "任务已创建。", FinishReason: "stop"},
			}}
			fs := newFakeStore()
			writer := &fakeAgentEvidenceWriter{}
			loop := New(Deps{
				Store: fs, Profiles: fs, Tools: ownerTestTools(tool), Evidence: writer,
				OwnerAgent: true, MaxTurns: 3,
			})
			loop.chatFn = chat.fn
			outcome, err := loop.HandleMessage(t.Context(), 42,
				"创建 Kimi 套餐监控，每天北京时间 9 点检查，重大更新才推送。")
			if err != nil || outcome.Reply != tc.wantReply || len(chat.requests) != 1 ||
				strings.Contains(outcome.Reply, "任务已创建") ||
				!strings.Contains(string(writer.record.ActionReceipts), `"status":"`+tc.wantStatus+`"`) ||
				writer.record.AssistantMessage != tc.wantReply {
				t.Fatalf("outcome=%+v err=%v requests=%d record=%+v",
					outcome, err, len(chat.requests), writer.record)
			}
		})
	}
}

func TestManageTasksFullLoopNeverTurnsIndeterminateEditIntoSuccess(t *testing.T) {
	editor := &fakeManageTaskEditorV3{err: errors.New("temporal response lost")}
	tool := NewManageTasksTool(ManageTasksDeps{
		Queries:    &fakeManageTaskQuery{rows: manageTaskRows("internal-kimi")},
		Editor:     editor,
		Authorizer: &fakeOwnerActionAuthorizer{decision: OwnerActionAuthorized},
	})
	chat := &scriptedChat{responses: []*llm.ChatResponse{
		{FinishReason: "tool_calls", ToolCalls: []llm.ToolCall{{
			ID: "indeterminate-edit", Name: manageTasksName,
			Arguments: `{"action":"edit","task_refs":["internal-kimi"],"changes":{"manual":"只查官方原文，无重大更新不推送。"}}`,
		}}},
		{Content: "任务已经修改。", FinishReason: "stop"},
	}}
	fs := newFakeStore()
	writer := &fakeAgentEvidenceWriter{}
	loop := New(Deps{
		Store: fs, Profiles: fs, Tools: ownerTestTools(tool), Evidence: writer,
		OwnerAgent: true, MaxTurns: 3,
	})
	loop.chatFn = chat.fn
	outcome, err := loop.HandleMessage(t.Context(), 42,
		"把 Kimi 套餐任务手册改成只查官方原文，无重大更新不推送。")
	want := "任务操作结果暂时无法确认；系统会按同一请求安全恢复，本次不能声称已经完成。"
	if err != nil || outcome.Reply != want || len(chat.requests) != 1 ||
		editor.calls != 1 || strings.Contains(outcome.Reply, "已经修改") ||
		!strings.Contains(string(writer.record.ActionReceipts),
			`"status":"execution_indeterminate"`) ||
		writer.record.AssistantMessage != want {
		t.Fatalf("outcome=%+v err=%v requests=%d edits=%d record=%+v",
			outcome, err, len(chat.requests), editor.calls, writer.record)
	}
}

func TestManageTasksFullLoopReportsPresealEditFailureAsNotExecuted(t *testing.T) {
	editor := &fakeManageTaskEditorV3{err: fmt.Errorf(
		"%w: invalid schedule", errResearchTaskDefinitionEditNotExecuted)}
	tool := NewManageTasksTool(ManageTasksDeps{
		Queries:    &fakeManageTaskQuery{rows: manageTaskRows("internal-kimi")},
		Editor:     editor,
		Authorizer: &fakeOwnerActionAuthorizer{decision: OwnerActionAuthorized},
	})
	chat := &scriptedChat{responses: []*llm.ChatResponse{{
		FinishReason: "tool_calls", ToolCalls: []llm.ToolCall{{
			ID: "not-executed-edit", Name: manageTasksName,
			Arguments: `{"action":"edit","task_refs":["internal-kimi"],"changes":{"schedule":{"every_seconds":3600,"tz":"Bad/Zone"}}}`,
		}},
	}, {Content: "任务已经修改。", FinishReason: "stop"}}}
	fs := newFakeStore()
	writer := &fakeAgentEvidenceWriter{}
	loop := New(Deps{
		Store: fs, Profiles: fs, Tools: ownerTestTools(tool), Evidence: writer,
		OwnerAgent: true, MaxTurns: 3,
	})
	loop.chatFn = chat.fn
	outcome, err := loop.HandleMessage(t.Context(), 42,
		"把 Kimi 套餐任务改成每小时检查，时区用 Bad/Zone。")
	want := "任务修改未执行，原任务保持不变；请修正变更内容后重试。"
	if err != nil || outcome.Reply != want || len(chat.requests) != 1 ||
		editor.calls != 1 || strings.Contains(outcome.Reply, "系统会按同一请求安全恢复") ||
		!strings.Contains(string(writer.record.ActionReceipts),
			`"status":"not_executed"`) || writer.record.AssistantMessage != want {
		t.Fatalf("outcome=%+v err=%v requests=%d edits=%d record=%+v",
			outcome, err, len(chat.requests), editor.calls, writer.record)
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

func TestOwnerManageTasksCreateRunsThroughNativeV3WithoutCanary(t *testing.T) {
	queries := &fakeManageTaskQuery{err: errors.New("create must not query")}
	authorizer := &fakeOwnerActionAuthorizer{decision: OwnerActionAuthorized}
	creator := &fakeManageTaskCreatorV3{outcome: ResearchTaskCreationV3Outcome{
		TaskRef: "task-internal-kimi", TaskName: "Kimi 套餐监控", Status: "completed",
	}}
	spec := NewManageTasksTool(ManageTasksDeps{Queries: queries, Creator: creator, Authorizer: authorizer})
	ctx, state := manageTasksTestContext("创建 Kimi 套餐监控，每周一九点查官方是否开放购买；无重大更新不推送")
	result, err := spec.Execute(ctx, 42, json.RawMessage(`{
		"action":"create","name":"Kimi 套餐监控",
		"manual":"检查 Kimi 官方套餐页是否开放购买，并与历史证据交叉核验。",
		"schedule":{"cron":"0 9 * * 1","tz":"Asia/Shanghai"},
		"notification":{"minimum_significance":"major_updates_only","suppress_empty":true},
		"output":{"language":"zh-CN","format":"executive_brief","instructions":"给老板看","include_evidence_links":true}
	}`))
	if err != nil || result != "已创建任务：Kimi 套餐监控。" {
		t.Fatalf("create result=%q err=%v", result, err)
	}
	if queries.query.Dataset != "" {
		t.Fatalf("create queried existing tasks: %+v", queries.query)
	}
	if creator.calls != 1 || creator.input.UserID != 42 ||
		creator.input.SessionID == nil || *creator.input.SessionID != 9 ||
		creator.input.Name != "Kimi 套餐监控" ||
		creator.input.Manual == "" || string(creator.input.Schedule) != `{"cron":"0 9 * * 1","tz":"Asia/Shanghai"}` ||
		creator.input.Notification.MinimumSignificance != "major_updates_only" ||
		!creator.input.Notification.SuppressEmpty ||
		creator.input.Output.Format != "executive_brief" ||
		!creator.input.Output.IncludeEvidenceLinks {
		t.Fatalf("native V3 input mismatch: %+v", creator.input)
	}
	if authorizer.calls != 1 || authorizer.input.Action != "create" ||
		len(authorizer.input.Targets) != 1 ||
		authorizer.input.Targets[0].Name != "Kimi 套餐监控" ||
		len(state.actionReceipts) != 1 ||
		!strings.Contains(string(state.actionReceipts[0]), "task-internal-kimi") {
		t.Fatalf("create auth/receipt mismatch: auth=%+v receipts=%s", authorizer.input, state.actionReceipts)
	}
}

func TestManageTasksCreateIdempotencyIgnoresModelRerenderWithinTurn(t *testing.T) {
	authorizer := &fakeOwnerActionAuthorizer{decision: OwnerActionAuthorized}
	creator := &fakeManageTaskCreatorV3{outcome: ResearchTaskCreationV3Outcome{
		TaskRef: "task-internal-kimi", TaskName: "Kimi 套餐监控", Status: "completed",
	}}
	spec := NewManageTasksTool(ManageTasksDeps{Creator: creator, Authorizer: authorizer})
	ctx, _ := manageTasksTestContext("创建 Kimi 套餐监控，每周一九点检查")
	first := json.RawMessage(`{
		"action":"create","name":"Kimi 套餐监控","manual":"检查官方套餐页。",
		"schedule":{"cron":"0 9 * * 1","tz":"Asia/Shanghai"},
		"notification":{"minimum_significance":"major_updates_only","suppress_empty":true},
		"output":{"language":"zh-CN","format":"executive_brief","include_evidence_links":true}
	}`)
	second := json.RawMessage(`{
		"action":"create","name":"Kimi 套餐监控","manual":"检查 Kimi 官方套餐页并交叉核验。",
		"schedule":{"cron":"0 9 * * 1","tz":"Asia/Shanghai"},
		"notification":{"minimum_significance":"major_updates_only","suppress_empty":true},
		"output":{"language":"zh-CN","format":"executive_brief","instructions":"给老板看","include_evidence_links":true}
	}`)
	if _, err := spec.Execute(ctx, 42, first); err != nil {
		t.Fatal(err)
	}
	ctx = withToolInvocationID(ctx, "turn-manage-1\x00provider-call-2")
	if _, err := spec.Execute(ctx, 42, second); err != nil {
		t.Fatal(err)
	}
	if len(creator.inputs) != 2 || creator.inputs[0].ActionID == "" ||
		creator.inputs[0].ActionID != creator.inputs[1].ActionID {
		t.Fatalf("same authenticated create turn drifted action IDs: %+v", creator.inputs)
	}
}

func TestManageTasksEditUsesExactOwnerChangeSetWithoutEchoingUnchangedPolicy(t *testing.T) {
	queries := &fakeManageTaskQuery{rows: manageTaskRows("internal-kimi")}
	authorizer := &fakeOwnerActionAuthorizer{decision: OwnerActionAuthorized}
	editor := &fakeManageTaskEditorV3{}
	spec := NewManageTasksTool(ManageTasksDeps{
		Queries: queries, Editor: editor, Authorizer: authorizer,
	})
	ctx, state := manageTasksTestContext("把 Kimi 套餐监控改成周二上午九点，其他不变")
	raw := json.RawMessage(`{"action":"edit","task_refs":["internal-kimi"],"changes":{"schedule":{"cron":"0 9 * * 2","tz":"Asia/Shanghai"}}}`)
	wantActionID := manageTaskIdempotencyKey(chatMeta{
		traceID: "turn-manage-1",
		scope:   agentcontext.Scope{TenantID: 7, UserID: 42, SessionID: 9},
	}, "edit", "internal-kimi")
	editor.outcome = ResearchTaskDefinitionEditV3Outcome{
		OperationID: wantActionID, TaskRef: "internal-kimi",
		TaskName: "Kimi 套餐监控", Status: "completed",
	}
	result, err := spec.Execute(ctx, 42, raw)
	if err != nil || result != "已修改任务：Kimi 套餐监控。" {
		t.Fatalf("edit result=%q err=%v", result, err)
	}
	if editor.calls != 1 || editor.input.ActionID != wantActionID ||
		editor.input.TenantID != 7 || editor.input.UserID != 42 ||
		editor.input.SessionID != 9 || editor.input.TaskRef != "internal-kimi" ||
		string(editor.input.Changes.Schedule) != `{"cron":"0 9 * * 2","tz":"Asia/Shanghai"}` ||
		editor.input.Changes.Name != nil || editor.input.Changes.Manual != nil ||
		editor.input.Changes.Notification != nil || editor.input.Changes.Output != nil {
		t.Fatalf("native V3 edit input=%+v", editor.input)
	}
	if authorizer.calls != 1 || authorizer.input.Action != "edit" ||
		len(authorizer.input.Targets) != 1 ||
		!strings.Contains(string(authorizer.input.Changes), `"cron":"0 9 * * 2"`) ||
		len(state.actionReceipts) != 1 ||
		!strings.Contains(string(state.actionReceipts[0]), `"action":"edit"`) ||
		strings.Contains(result, "internal-kimi") {
		t.Fatalf("edit authorization/receipt leaked: auth=%+v result=%q receipts=%s",
			authorizer.input, result, state.actionReceipts)
	}
}

func TestManageTasksEditFailsClosedBeforeAuthorizationWithoutNativeV3Executor(t *testing.T) {
	authorizer := &fakeOwnerActionAuthorizer{decision: OwnerActionAuthorized}
	spec := NewManageTasksTool(ManageTasksDeps{
		Queries:    &fakeManageTaskQuery{rows: manageTaskRows("internal-kimi")},
		Authorizer: authorizer,
	})
	ctx, state := manageTasksTestContext("修改 Kimi 任务手册")
	result, err := spec.Execute(ctx, 42, json.RawMessage(
		`{"action":"edit","task_refs":["internal-kimi"],"changes":{"manual":"只查官方原文，无重大更新不推送。"}}`))
	if err != nil || !strings.Contains(result, "未装配") || authorizer.calls != 0 ||
		len(state.actionReceipts) != 0 {
		t.Fatalf("unwired edit escaped result=%q err=%v auth=%d receipts=%s",
			result, err, authorizer.calls, state.actionReceipts)
	}
}

func TestManageTasksCreateWithoutV3ExecutorFailsClosedBeforeAuthorization(t *testing.T) {
	authorizer := &fakeOwnerActionAuthorizer{decision: OwnerActionAuthorized}
	spec := NewManageTasksTool(ManageTasksDeps{Authorizer: authorizer})
	ctx, state := manageTasksTestContext("创建任务")
	result, err := spec.Execute(ctx, 42, json.RawMessage(`{
		"action":"create","name":"测试","manual":"检查官方更新。",
		"schedule":{"every_seconds":3600,"tz":"Asia/Shanghai"},
		"notification":{"minimum_significance":"major_updates_only","suppress_empty":true},
		"output":{"language":"auto","format":"concise_brief","include_evidence_links":true}
	}`))
	if err != nil || !strings.Contains(result, "未装配") ||
		authorizer.calls != 0 || len(state.actionReceipts) != 0 {
		t.Fatalf("unwired create escaped: result=%q err=%v auth=%d receipts=%s",
			result, err, authorizer.calls, state.actionReceipts)
	}
}

func TestManageTasksCreateExecutingDoesNotClaimCompletion(t *testing.T) {
	authorizer := &fakeOwnerActionAuthorizer{decision: OwnerActionAuthorized}
	creator := &fakeManageTaskCreatorV3{outcome: ResearchTaskCreationV3Outcome{
		TaskName: "测试", Status: "executing",
	}}
	spec := NewManageTasksTool(ManageTasksDeps{Creator: creator, Authorizer: authorizer})
	ctx, _ := manageTasksTestContext("创建测试任务")
	result, err := spec.Execute(ctx, 42, json.RawMessage(`{
		"action":"create","name":"测试","manual":"检查官方更新。",
		"schedule":{"every_seconds":3600,"tz":"Asia/Shanghai"},
		"notification":{"minimum_significance":"major_updates_only","suppress_empty":true},
		"output":{"language":"auto","format":"concise_brief","include_evidence_links":true}
	}`))
	if err != nil || !strings.Contains(result, "已受理") || strings.Contains(result, "已创建任务") {
		t.Fatalf("executing create overclaimed: result=%q err=%v", result, err)
	}
}

func TestManageTasksCreateCannotCrossExternalTaintBoundary(t *testing.T) {
	authorizer := &fakeOwnerActionAuthorizer{decision: OwnerActionAuthorized}
	creator := &fakeManageTaskCreatorV3{outcome: ResearchTaskCreationV3Outcome{
		TaskRef: "must-not-exist", TaskName: "测试", Status: "completed",
	}}
	spec := NewManageTasksTool(ManageTasksDeps{Creator: creator, Authorizer: authorizer})
	ctx, state := manageTasksTestContext("根据刚查到的网页创建任务")
	state.untrustedExternalResult = true
	result, err := spec.Execute(ctx, 42, json.RawMessage(`{
		"action":"create","name":"测试","manual":"检查官方更新。",
		"schedule":{"every_seconds":3600,"tz":"Asia/Shanghai"},
		"notification":{"minimum_significance":"major_updates_only","suppress_empty":true},
		"output":{"language":"auto","format":"concise_brief","include_evidence_links":true}
	}`))
	if err != nil || !strings.Contains(result, "认证上下文") ||
		creator.calls != 0 || authorizer.calls != 0 || len(state.actionReceipts) != 0 {
		t.Fatalf("tainted create escaped: result=%q err=%v creator=%d auth=%d receipts=%s",
			result, err, creator.calls, authorizer.calls, state.actionReceipts)
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
		`{"action":"edit","task_refs":["a"],"changes":{}}`,
		`{"action":"edit","task_refs":["a","b"],"changes":{"manual":"更新手册"}}`,
		`{"action":"run","task_ref":"a"}`,
		`{"action":"delete","task_refs":[" a"]}`,
		`{"action":"create","task_refs":["a"]}`,
		`{"action":"create","name":"x","manual":"m","schedule":{"every_seconds":3600,"tz":"Asia/Shanghai"},"notification":{"minimum_significance":"major_updates_only","suppress_empty":false},"output":{"language":"auto","format":"concise_brief","include_evidence_links":true}}`,
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

func TestManageTasksSchemaIsAgentFirstV3Only(t *testing.T) {
	spec := NewManageTasksTool(ManageTasksDeps{})
	if err := spec.validate(); err != nil {
		t.Fatalf("manage_tasks schema invalid: %v", err)
	}
	var schema struct {
		Properties map[string]json.RawMessage `json:"properties"`
	}
	if err := json.Unmarshal(spec.Parameters(), &schema); err != nil {
		t.Fatal(err)
	}
	var action struct {
		Enum []string `json:"enum"`
	}
	if err := json.Unmarshal(schema.Properties["action"], &action); err != nil {
		t.Fatal(err)
	}
	if strings.Join(action.Enum, ",") != "create,edit,run,delete" {
		t.Fatalf("manage_tasks actions=%v", action.Enum)
	}
	for _, required := range []string{"name", "manual", "schedule", "notification", "output", "changes", "task_refs"} {
		if schema.Properties[required] == nil {
			t.Errorf("manage_tasks schema missing %s", required)
		}
	}
	visible := strings.ToLower(spec.Description() + "\n" + string(spec.Parameters()))
	for _, retired := range []string{
		"tool_calls", "source", "fetch",
		"budget", "delivery_policy", "create_schedule",
	} {
		if strings.Contains(visible, retired) {
			t.Errorf("manage_tasks V3 surface retained %q: %s", retired, visible)
		}
	}
}

func TestOwnerGeneralChatInventoryHidesLegacyTaskTools(t *testing.T) {
	tools := BuildOwnerTools(&store.Store{}, ManageTasksDeps{}, nil, nil, nil)
	loop, err := NewChecked(Deps{Tools: tools, OwnerAgent: true,
		Evidence: &fakeAgentEvidenceWriter{}})
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
	if !strings.Contains(systemPrompt, "query_my_intelligence") ||
		!strings.Contains(systemPrompt, "manage_tasks") {
		t.Fatalf("Agent-first environment prompt missing: %q", systemPrompt)
	}
}

func TestOwnerAgentDoesNotImplicitlyReadOrInjectProfile(t *testing.T) {
	fs := newFakeStore()
	fs.profiles[42] = &types.Profile{UserID: 42, Industry: "SECRET-INDUSTRY"}
	writer := &fakeAgentEvidenceWriter{}
	loop := New(Deps{
		Store: fs, Profiles: fs, Evidence: writer,
		Tools:      ownerTestTools(NewQueryMyIntelligenceTool(&fakeManageTaskQuery{})),
		OwnerAgent: true, MaxTurns: 1,
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
		!strings.Contains(string(encoded), "query_my_intelligence") {
		t.Fatalf("Agent-first request leaked implicit profile or missed environment: %s", encoded)
	}
	for _, retiredGuidance := range []string{
		"统一使用 create_schedule 创建", "create_schedule 必须带完整 intent",
		"系统会冻结本任务的 Tool 调用", "改用 create_schedule",
	} {
		if strings.Contains(string(encoded), retiredGuidance) {
			t.Fatalf("Agent-first prompt retained legacy create guidance %q: %s",
				retiredGuidance, encoded)
		}
	}
	if !strings.Contains(string(encoded), "创建、编辑、立即运行和批量删除任务统一使用 manage_tasks") {
		t.Fatalf("Agent-first prompt missed V3 create guidance: %s", encoded)
	}
}

func TestOwnerToolsAreNotGatedByCanaryUserID(t *testing.T) {
	queries := &fakeManageTaskQuery{rows: manageTaskRows("kimi")}
	writer := &fakeAgentEvidenceWriter{}
	queryTool := NewQueryMyIntelligenceTool(queries)
	loop := New(Deps{
		Tools: ownerTestTools(queryTool), Evidence: writer,
		OwnerAgent: true,
	})
	state := &toolRunState{ownerRequest: "查任务", agentFirstEnabled: true}
	ctx := context.WithValue(t.Context(), toolRunKey{}, state)
	ctx = context.WithValue(ctx, chatMetaKey{}, chatMeta{
		traceID: "non-canary", userID: 43,
		scope: agentcontext.Scope{TenantID: 7, UserID: 43, SessionID: 10},
	})
	sessionID := int64(10)
	messages, err := loop.runToolCalls(ctx, 43, &sessionID, []llm.ToolCall{{
		ID: "hallucinated", Name: queryTool.Name(), Arguments: `{"dataset":"tasks"}`,
	}})
	if err != nil || len(messages) != 1 || queries.query.Dataset != "tasks" {
		t.Fatalf("owner tool remained canary-gated: messages=%+v query=%+v err=%v", messages, queries.query, err)
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
