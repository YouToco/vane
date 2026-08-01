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

type evidenceTestTool struct{ result string }

func (*evidenceTestTool) Name() string        { return "evidence_fixture" }
func (*evidenceTestTool) Description() string { return "return a test result" }
func (*evidenceTestTool) Parameters() json.RawMessage {
	return json.RawMessage(`{"type":"object","properties":{}}`)
}
func (t *evidenceTestTool) Execute(context.Context, int64, json.RawMessage) (string, error) {
	return t.result, nil
}
func (*evidenceTestTool) Summarize(json.RawMessage) string { return "test" }

type fakeAgentEvidenceWriter struct {
	tenantID int64
	userID   int64
	record   store.AgentTurnRecordV1
	err      error
}

func (f *fakeAgentEvidenceWriter) CommitAgentTurnRecordV1(
	_ context.Context,
	tenantID, userID int64,
	record store.AgentTurnRecordV1,
) error {
	f.tenantID, f.userID, f.record = tenantID, userID, record
	return f.err
}

func TestConverseCommitsExactEvidenceBeforeReturningReply(t *testing.T) {
	writer := &fakeAgentEvidenceWriter{}
	tool := newToolSpec(&evidenceTestTool{result: strings.Repeat("证", 100000)},
		withToolSurface(ownerPolicy(Effects(EffectInternalRead), BudgetNone),
			ExposureAlways, IntentTasks, ResultTrustLocal, false))
	loop := New(Deps{Tools: []ToolSpec{tool}, Evidence: writer, MaxTurns: 3})
	calls := 0
	loop.chatFn = func(context.Context, llm.ChatRequest) (*llm.ChatResponse, error) {
		calls++
		if calls == 1 {
			return &llm.ChatResponse{ToolCalls: []llm.ToolCall{{
				ID: "provider-call-evidence", Name: tool.Name(), Arguments: `{}`,
			}}}, nil
		}
		return &llm.ChatResponse{Content: "根据工具证据得出结论。"}, nil
	}
	ctx := context.WithValue(t.Context(), chatMetaKey{}, chatMeta{
		traceID: "trace-evidence", userID: 42,
		scope: agentcontext.Scope{TenantID: 7, UserID: 42, SessionID: 9},
	})
	state := &toolRunState{
		activation: &activationState{}, ownerRequest: "查一下历史结果",
		intents: IntentTasks, successfulCalls: map[string]struct{}{},
		failedCalls: map[string]int{},
	}
	sessionID := int64(9)
	outcome, messages, _, err := loop.converse(ctx, 42, &sessionID,
		[]llm.ChatMessage{{Role: "user", Content: state.ownerRequest}}, "", state)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Reply != "根据工具证据得出结论。" || len(messages) == 0 {
		t.Fatalf("outcome=%+v messages=%d", outcome, len(messages))
	}
	if writer.tenantID != 7 || writer.userID != 42 ||
		writer.record.TurnID != "trace-evidence" ||
		writer.record.UserMessage != state.ownerRequest ||
		writer.record.AssistantMessage != outcome.Reply ||
		len(writer.record.ToolEvidence) != 1 {
		t.Fatalf("record tenant=%d user=%d record=%+v",
			writer.tenantID, writer.userID, writer.record)
	}
	evidence := writer.record.ToolEvidence[0]
	if evidence.InvocationID != "provider-call-evidence" ||
		evidence.ToolCall.TenantID == nil || *evidence.ToolCall.TenantID != 7 ||
		len(evidence.Result) > maxModelVisibleToolResultBytes ||
		evidence.OriginalSize <= len(evidence.Result) || evidence.TrustType != "local" {
		t.Fatalf("evidence=%+v result_bytes=%d", evidence, len(evidence.Result))
	}
}

func TestConverseDoesNotReturnReplyWhenEvidenceCommitFails(t *testing.T) {
	wantErr := errors.New("evidence unavailable")
	writer := &fakeAgentEvidenceWriter{err: wantErr}
	loop := New(Deps{Evidence: writer, MaxTurns: 1})
	loop.chatFn = func(context.Context, llm.ChatRequest) (*llm.ChatResponse, error) {
		return &llm.ChatResponse{Content: "不能在未落证据时返回"}, nil
	}
	ctx := context.WithValue(t.Context(), chatMetaKey{}, chatMeta{
		traceID: "trace-evidence-fail", userID: 42,
		scope: agentcontext.Scope{TenantID: 7, UserID: 42, SessionID: 9},
	})
	sessionID := int64(9)
	outcome, messages, _, err := loop.converse(ctx, 42, &sessionID,
		[]llm.ChatMessage{{Role: "user", Content: "测试证据失败"}}, "",
		&toolRunState{activation: &activationState{}, ownerRequest: "测试证据失败"})
	if !errors.Is(err, wantErr) || outcome.Reply != "" || messages != nil {
		t.Fatalf("outcome=%+v messages=%v err=%v", outcome, messages, err)
	}
}

func TestConverseFailsClosedWhenExactToolEvidenceCannotBeCaptured(t *testing.T) {
	writer := &fakeAgentEvidenceWriter{}
	tool := newToolSpec(&evidenceTestTool{result: "sensitive result"},
		withToolSurface(ownerPolicy(Effects(EffectInternalRead), BudgetNone),
			ExposureAlways, IntentTasks, ResultTrustLocal, false))
	loop := New(Deps{Tools: []ToolSpec{tool}, Evidence: writer, MaxTurns: 2})
	loop.chatFn = func(context.Context, llm.ChatRequest) (*llm.ChatResponse, error) {
		return &llm.ChatResponse{ToolCalls: []llm.ToolCall{{
			ID: "", Name: tool.Name(), Arguments: `{}`,
		}}}, nil
	}
	ctx := context.WithValue(t.Context(), chatMetaKey{}, chatMeta{
		traceID: "trace-missing-call-id", userID: 42,
		scope: agentcontext.Scope{TenantID: 7, UserID: 42, SessionID: 9},
	})
	sessionID := int64(9)
	outcome, messages, _, err := loop.converse(ctx, 42, &sessionID,
		[]llm.ChatMessage{{Role: "user", Content: "查证据"}}, "",
		&toolRunState{activation: &activationState{}, ownerRequest: "查证据",
			successfulCalls: map[string]struct{}{}, failedCalls: map[string]int{}})
	if !errors.Is(err, errExactAgentEvidenceCapture) || outcome.Reply != "" || messages != nil || writer.record.TurnID != "" {
		t.Fatalf("outcome=%+v messages=%v err=%v record=%+v", outcome, messages, err, writer.record)
	}
}

func TestAgentFirstRequiresExactEvidenceAndExactCanaryUser(t *testing.T) {
	if _, err := NewChecked(Deps{AgentFirstEnabled: true, AgentFirstCanaryUserID: 42}); err == nil {
		t.Fatal("Agent-first accepted nil evidence writer")
	}
	if _, err := NewChecked(Deps{AgentFirstEnabled: true, Evidence: &fakeAgentEvidenceWriter{}}); err == nil {
		t.Fatal("Agent-first accepted zero canary user")
	}
}

func TestDirectWebEditSealsExactEvidenceAndActionReceipt(t *testing.T) {
	writer := &fakeAgentEvidenceWriter{}
	controller := &fakeDefinitionEditController{execute: task.TaskDefinitionEditOutcome{
		Status: types.TaskDefinitionEditOperationStatusCompleted,
		TaskID: "task-direct-internal-7ab4",
	}}
	tool := newToolSpec(&editTaskDefinitionTool{}, ownerPolicy(Effects(
		EffectDurableProposal, EffectStateWrite, EffectDirectOwnerWrite,
	), BudgetNone))
	loop := New(Deps{
		Tools: []ToolSpec{tool}, Evidence: writer,
		TaskDefinitionEdit: controller, MaxTurns: 2,
	})
	loop.chatFn = func(context.Context, llm.ChatRequest) (*llm.ChatResponse, error) {
		return &llm.ChatResponse{ToolCalls: []llm.ToolCall{{
			ID: "direct-edit-provider-call", Name: definitionEditToolName,
			Arguments: `{"task_id":"task-direct-internal-7ab4","intent":"只推送重大更新"}`,
		}}}, nil
	}
	ctx := context.WithValue(t.Context(), chatMetaKey{}, chatMeta{
		traceID: "trace-direct-edit", userID: 42,
		scope: agentcontext.Scope{TenantID: 7, UserID: 42, SessionID: 9},
	})
	sessionID := int64(9)
	state := &toolRunState{
		activation: &activationState{}, ownerRequest: "把 Kimi 任务改成只推重大更新",
		agentFirstEnabled: true, directTaskDefinitionEditID: "task-direct-internal-7ab4",
		directActionID:  "operation-direct-internal-8bc5",
		successfulCalls: map[string]struct{}{}, failedCalls: map[string]int{},
	}
	outcome, _, _, err := loop.converse(ctx, 42, &sessionID,
		[]llm.ChatMessage{{Role: "user", Content: state.ownerRequest}}, "", state)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Reply != "已修改定时推送任务。" || len(writer.record.ToolEvidence) != 1 {
		t.Fatalf("outcome=%+v record=%+v", outcome, writer.record)
	}
	evidence := writer.record.ToolEvidence[0]
	if evidence.InvocationID != "direct-edit-provider-call" ||
		evidence.ToolName != definitionEditToolName ||
		string(evidence.Result) != outcome.Reply ||
		!strings.Contains(string(evidence.Arguments), "task-direct-internal-7ab4") {
		t.Fatalf("evidence=%+v", evidence)
	}
	if !strings.Contains(string(writer.record.ActionReceipts), `"action":"edit"`) ||
		!strings.Contains(string(writer.record.ActionReceipts), "operation-direct-internal-8bc5") ||
		!strings.Contains(string(writer.record.ActionReceipts), "task-direct-internal-7ab4") {
		t.Fatalf("receipts=%s", writer.record.ActionReceipts)
	}
}

func TestConverseRedactsKnownInternalReferencesBeforeEvidenceAndReply(t *testing.T) {
	writer := &fakeAgentEvidenceWriter{}
	queryStoreWithRef := &fakeManageTaskQuery{rows: []map[string]any{{
		"task_ref": "task-internal-4df9a28b", "task_name": "Kimi 套餐监控",
		"tool_invocation_ids": []any{"tool-invocation-internal-c9e3"},
		"action_receipts": []any{map[string]any{
			"completed_task_refs": []any{"task-internal-receipt-a7c1"},
		}},
		"arguments": `{"task_refs":["task-internal-argument-b8d2"],"schedule_ids":["schedule-internal-d7f4"],"schedule_id":"schedule-internal-e6a5","task_id":"task-internal-f5b6"}`,
	}}}
	tool := NewQueryMyIntelligenceTool(queryStoreWithRef)
	loop := New(Deps{Tools: []ToolSpec{tool}, Evidence: writer, MaxTurns: 2})
	calls := 0
	loop.chatFn = func(context.Context, llm.ChatRequest) (*llm.ChatResponse, error) {
		calls++
		if calls == 1 {
			return &llm.ChatResponse{ToolCalls: []llm.ToolCall{{
				ID: "query-call", Name: tool.Name(), Arguments: `{"dataset":"tasks"}`,
			}}}, nil
		}
		return &llm.ChatResponse{Content: "任务 task-internal-4df9a28b、task-internal-receipt-a7c1、task-internal-argument-b8d2、tool-invocation-internal-c9e3、schedule-internal-d7f4、schedule-internal-e6a5、task-internal-f5b6 当前正常。"}, nil
	}
	ctx := context.WithValue(t.Context(), chatMetaKey{}, chatMeta{
		traceID: "trace-redact-ref", userID: 42,
		scope: agentcontext.Scope{TenantID: 7, UserID: 42, SessionID: 9},
	})
	sessionID := int64(9)
	outcome, _, _, err := loop.converse(ctx, 42, &sessionID,
		[]llm.ChatMessage{{Role: "user", Content: "任务状态"}}, "",
		&toolRunState{activation: &activationState{}, ownerRequest: "任务状态", agentFirstEnabled: true,
			successfulCalls: map[string]struct{}{}, failedCalls: map[string]int{}})
	if err != nil || strings.Contains(outcome.Reply, "task-internal") ||
		!strings.Contains(outcome.Reply, "内部引用已隐藏") ||
		writer.record.AssistantMessage != outcome.Reply {
		t.Fatalf("outcome=%+v err=%v evidence_reply=%q", outcome, err, writer.record.AssistantMessage)
	}
}

func TestGroundedGuardedReplyIsTheExactPersistedEvidence(t *testing.T) {
	writer := &fakeAgentEvidenceWriter{}
	loop := New(Deps{Store: newFakeStore(), Evidence: writer, MaxTurns: 1})
	loop.chatFn = func(context.Context, llm.ChatRequest) (*llm.ChatResponse, error) {
		return &llm.ChatResponse{Content: "raw reply"}, nil
	}
	outcome, err := loop.HandleGroundedMessageGuarded(t.Context(), 42,
		"发生了什么？", "frozen brief", func(reply string) (string, error) {
			return "guarded: " + reply, nil
		})
	if err != nil || outcome.Reply != "guarded: raw reply" ||
		writer.record.AssistantMessage != outcome.Reply {
		t.Fatalf("outcome=%+v err=%v record=%+v", outcome, err, writer.record)
	}
}
