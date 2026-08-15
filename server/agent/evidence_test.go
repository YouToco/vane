package agent

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/YouToco/vane/server/agentcontext"
	"github.com/YouToco/vane/server/llm"
	"github.com/YouToco/vane/server/store"
	"github.com/YouToco/vane/server/types"
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

type fakeAgentTurnReplayReader struct {
	replay store.AgentTurnReplayV1
	err    error
	calls  int
}

func (f *fakeAgentTurnReplayReader) FindAgentTurnReplayV1(
	context.Context, int64, int64, string,
) (store.AgentTurnReplayV1, error) {
	f.calls++
	return f.replay, f.err
}

func TestWebTaskActionReplaysCompletedTurnWithoutModelCall(t *testing.T) {
	reader := &fakeAgentTurnReplayReader{replay: store.AgentTurnReplayV1{
		UserMessage: "创建 Kimi 套餐监控", AssistantMessage: "任务已创建。",
	}}
	loop := New(Deps{
		Store: newFakeStore(), Profiles: newFakeStore(),
		Tools: ownerTestTools(), Evidence: &fakeAgentEvidenceWriter{},
		TurnReplay: reader, OwnerAgent: true,
	})
	loop.chatFn = func(context.Context, llm.ChatRequest) (*llm.ChatResponse, error) {
		t.Fatal("durable replay must not call the model")
		return nil, nil
	}
	out, err := loop.HandleWebTaskActionMessage(
		t.Context(), 7, "a58d8934-3b23-420c-b58a-93c0208e186d", "",
		"创建 Kimi 套餐监控",
	)
	if err != nil || out.Reply != "任务已创建。" || reader.calls != 1 {
		t.Fatalf("out=%+v calls=%d err=%v", out, reader.calls, err)
	}
}

func TestWebTaskActionRejectsRequestIDReusedWithDifferentMessage(t *testing.T) {
	reader := &fakeAgentTurnReplayReader{replay: store.AgentTurnReplayV1{
		UserMessage: "创建 A", AssistantMessage: "已创建 A。",
	}}
	loop := New(Deps{
		Store: newFakeStore(), Profiles: newFakeStore(),
		Tools: ownerTestTools(), Evidence: &fakeAgentEvidenceWriter{},
		TurnReplay: reader, OwnerAgent: true,
	})
	_, err := loop.HandleWebTaskActionMessage(
		t.Context(), 7, "d364eb61-a52c-4ddb-8efa-b47fb8c23dc7", "", "创建 B",
	)
	if !errors.Is(err, types.ErrConflict) {
		t.Fatalf("err=%v, want conflict", err)
	}
}

func TestWebTaskActionUsesStableActionIDAsEvidenceTrace(t *testing.T) {
	const actionID = "58ed32df-4bbd-4e06-b9fe-fae2a954c460"
	reader := &fakeAgentTurnReplayReader{err: types.NewAppError(
		types.CodeNotFound, "missing", types.ErrNotFound,
	)}
	writer := &fakeAgentEvidenceWriter{}
	fs := newFakeStore()
	loop := New(Deps{
		Store: fs, Profiles: fs, Tools: ownerTestTools(), Evidence: writer,
		TurnReplay: reader, OwnerAgent: true, MaxTurns: 1,
	})
	loop.chatFn = func(context.Context, llm.ChatRequest) (*llm.ChatResponse, error) {
		return &llm.ChatResponse{Content: "请补充监控频率？"}, nil
	}
	out, err := loop.HandleWebTaskActionMessage(
		t.Context(), 7, actionID, "", "创建 Kimi 套餐监控",
	)
	if err != nil || out.Reply == "" || writer.record.TurnID != actionID {
		t.Fatalf("out=%+v trace=%q err=%v", out, writer.record.TurnID, err)
	}
}

func TestChannelMessageReplaysCompletedTurnWithoutModelCall(t *testing.T) {
	reader := &fakeAgentTurnReplayReader{replay: store.AgentTurnReplayV1{
		UserMessage: "列出我的任务", AssistantMessage: "你有两个任务。",
	}}
	loop := New(Deps{
		Store: newFakeStore(), Profiles: newFakeStore(),
		Tools: ownerTestTools(), Evidence: &fakeAgentEvidenceWriter{},
		TurnReplay: reader, OwnerAgent: true,
	})
	loop.chatFn = func(context.Context, llm.ChatRequest) (*llm.ChatResponse, error) {
		t.Fatal("channel replay must not call the model")
		return nil, nil
	}
	out, err := loop.HandleChannelMessage(
		t.Context(), 7, "b8e3a943-85e0-54e8-8fc0-5d4c1d371a14", "列出我的任务",
	)
	if err != nil || out.Reply != "你有两个任务。" || reader.calls != 1 {
		t.Fatalf("out=%+v calls=%d err=%v", out, reader.calls, err)
	}
}

func TestChannelMessageUsesStableIDWithoutWebTaskMode(t *testing.T) {
	const turnID = "a47f20c0-461d-5ac7-aa41-04d449125864"
	reader := &fakeAgentTurnReplayReader{err: types.NewAppError(
		types.CodeNotFound, "missing", types.ErrNotFound,
	)}
	writer := &fakeAgentEvidenceWriter{}
	mutating := &fakeTool{name: "channel_mutation", mutating: true}
	spec := newToolSpec(mutating, withToolSurface(ownerPolicy(
		Effects(EffectStateWrite, EffectDirectOwnerWrite), BudgetNone,
	), ExposureAlways, IntentTasks, ResultTrustLocal, true))
	fs := newFakeStore()
	loop := New(Deps{
		Store: fs, Profiles: fs, Tools: ownerTestTools(spec), OwnerAgent: true,
		Evidence: writer, TurnReplay: reader, MaxTurns: 2,
	})
	calls := 0
	loop.chatFn = func(context.Context, llm.ChatRequest) (*llm.ChatResponse, error) {
		calls++
		if calls == 1 {
			return &llm.ChatResponse{ToolCalls: []llm.ToolCall{{
				ID: "channel-call", Name: spec.Name(), Arguments: `{}`,
			}}}, nil
		}
		return &llm.ChatResponse{Content: "已执行。"}, nil
	}
	out, err := loop.HandleChannelMessage(
		t.Context(), 7, turnID, "执行渠道操作",
	)
	if err != nil || out.Reply != "已执行。" || len(mutating.calls) != 1 ||
		writer.record.TurnID != turnID {
		t.Fatalf("out=%+v tool_calls=%d trace=%q err=%v",
			out, len(mutating.calls), writer.record.TurnID, err)
	}
}

func TestChannelMessageRejectsInvalidAuthorityOrStableID(t *testing.T) {
	validID := uuid.NewString()
	for _, tc := range []struct {
		name string
		loop *Loop
		id   string
	}{
		{name: "nil loop", loop: nil, id: validID},
		{name: "not owner", loop: &Loop{turnReplay: &fakeAgentTurnReplayReader{}}, id: validID},
		{name: "no replay store", loop: &Loop{ownerAgent: true}, id: validID},
		{name: "invalid stable id", loop: &Loop{ownerAgent: true, turnReplay: &fakeAgentTurnReplayReader{}}, id: "not-a-uuid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := tc.loop.HandleChannelMessage(t.Context(), 1, tc.id, "hello"); !errors.Is(err, types.ErrValidation) {
				t.Fatalf("err=%v", err)
			}
		})
	}
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

func TestOwnerAgentRequiresExactEvidenceAndOrthogonalCatalog(t *testing.T) {
	if _, err := NewChecked(Deps{OwnerAgent: true}); err == nil {
		t.Fatal("owner Agent accepted nil evidence writer")
	}
	if _, err := NewChecked(Deps{OwnerAgent: true,
		Evidence: &fakeAgentEvidenceWriter{}}); err == nil {
		t.Fatal("owner Agent accepted a catalog without required tools")
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
