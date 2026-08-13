package agent

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/YouToco/vane/agentcontext"
	"github.com/YouToco/vane/llm"
	"github.com/YouToco/vane/types"
)

type fakeAgentMemoryStore struct {
	tenantID, userID int64
	key              string
	action           types.MemoryAction
	query            types.MemoryRecallQuery
	getID            int64
	memory           *types.MemoryRecord
	actionResult     *types.MemoryActionResult
	recallResult     *types.MemoryRecallResult
	err              error
	authorizationID  string
}

func (f *fakeAgentMemoryStore) PrepareMemoryAuthorization(
	_ context.Context, tenantID, userID, _ int64, action types.MemoryAction,
) (string, error) {
	f.tenantID, f.userID, f.action = tenantID, userID, action
	if f.authorizationID == "" {
		f.authorizationID = "922b377b-12e6-4958-b8d1-fcfb3d7708e3"
	}
	return f.authorizationID, f.err
}

func (f *fakeAgentMemoryStore) GetMemory(
	_ context.Context, tenantID, userID, memoryID int64,
) (*types.MemoryRecord, error) {
	f.tenantID, f.userID, f.getID = tenantID, userID, memoryID
	return f.memory, f.err
}

func (f *fakeAgentMemoryStore) ApplyMemoryAction(
	_ context.Context, tenantID, userID int64, key string, action types.MemoryAction,
) (*types.MemoryActionResult, error) {
	f.tenantID, f.userID, f.key, f.action = tenantID, userID, key, action
	return f.actionResult, f.err
}

func (f *fakeAgentMemoryStore) RecallMemories(
	_ context.Context, tenantID, userID int64, query types.MemoryRecallQuery,
) (*types.MemoryRecallResult, error) {
	f.tenantID, f.userID, f.query = tenantID, userID, query
	return f.recallResult, f.err
}

func memoryToolContext(ownerRequest string) (context.Context, *toolRunState) {
	const turnID = "695df46b-da69-435a-84d3-c6f3f0a906fe"
	state := &toolRunState{ownerRequest: ownerRequest, agentFirstEnabled: true}
	ctx := context.WithValue(context.Background(), chatMetaKey{}, chatMeta{
		traceID: turnID, userID: 42,
		scope: agentcontext.Scope{TenantID: 7, UserID: 42, SessionID: 9},
	})
	ctx = context.WithValue(ctx, toolRunKey{}, state)
	ctx = withProviderToolCallID(ctx, "memory-call-1")
	ctx = withToolInvocationID(ctx, turnID+"\x00memory-call-1")
	return ctx, state
}

func TestRecallMemoryUsesAuthenticatedScopeAndDefaultsLimit(t *testing.T) {
	store := &fakeAgentMemoryStore{recallResult: &types.MemoryRecallResult{
		Memories: []types.MemoryRecallItem{{
			Memory: types.MemoryRecord{
				ID: 3, Text: "发布前先跑生产同形 smoke", Active: true,
				Evidence: types.MemoryEvidence{
					SourceType: types.MemoryEvidenceOwnerExplicitAgentTurn,
					SourceID:   "695df46b-da69-435a-84d3-c6f3f0a906fe",
				},
			},
			Score: 1.25,
		}},
	}}
	tool := NewRecallMemoryTool(store)
	ctx, _ := memoryToolContext("我们以前如何验收发布？")
	result, err := tool.Execute(ctx, 42, json.RawMessage(`{"query":"  生产发布经验  "}`))
	if err != nil {
		t.Fatal(err)
	}
	if store.tenantID != 7 || store.userID != 42 ||
		store.query.Query != "生产发布经验" || store.query.Limit != 8 {
		t.Fatalf("scope=(%d,%d) query=%+v result=%q", store.tenantID, store.userID, store.query, result)
	}
	if !strings.Contains(result, "发布前先跑生产同形 smoke") ||
		!strings.Contains(result, `"score":1.25`) ||
		!strings.Contains(result, `"memory_id":3`) {
		t.Fatalf("result=%s", result)
	}
	if strings.Contains(result, "owner_explicit_agent_turn") ||
		strings.Contains(result, "695df46b-da69-435a-84d3-c6f3f0a906fe") {
		t.Fatalf("model-visible recall leaked internal evidence: %s", result)
	}
}

func TestRecallMemoryRejectsInvalidInputBeforeStore(t *testing.T) {
	store := &fakeAgentMemoryStore{}
	tool := NewRecallMemoryTool(store)
	ctx, _ := memoryToolContext("查记忆")
	for _, raw := range []string{
		`{"query":""}`,
		`{"query":"x","limit":9}`,
		`{"query":"x","unknown":1}`,
		`{"query":"` + strings.Repeat("界", 171) + `"}`,
	} {
		result, err := tool.Execute(ctx, 42, json.RawMessage(raw))
		if err != nil || !strings.Contains(result, "recall_memory") {
			t.Fatalf("raw bytes=%d result=%q err=%v", len(raw), result, err)
		}
	}
	if store.query.Query != "" {
		t.Fatalf("invalid query reached store: %+v", store.query)
	}
}

func TestManageMemoryBindsTrustedEvidenceAndAuthorizesTargetText(t *testing.T) {
	store := &fakeAgentMemoryStore{
		memory: &types.MemoryRecord{ID: 9, Text: "生产研究模型使用旧模型", Active: true},
		actionResult: &types.MemoryActionResult{
			Memory: types.MemoryRecord{ID: 10, Text: "生产研究模型使用 deepseek-v4-flash", Active: true},
			Event:  types.MemoryEvent{ID: 11, Action: types.MemoryActionCorrect},
		},
	}
	authorizer := &fakeOwnerActionAuthorizer{decision: OwnerActionAuthorized}
	tool := NewManageMemoryTool(store, authorizer)
	ctx, _ := memoryToolContext("把生产模型的旧记忆纠正为 deepseek-v4-flash")
	result, err := tool.Execute(ctx, 42, json.RawMessage(
		`{"action":"correct","memory_id":9,"text":"生产研究模型使用 deepseek-v4-flash"}`,
	))
	if err != nil {
		t.Fatal(err)
	}
	if store.getID != 9 || store.tenantID != 7 || store.userID != 42 {
		t.Fatalf("target/scope=%d (%d,%d)", store.getID, store.tenantID, store.userID)
	}
	if authorizer.calls != 1 ||
		!strings.Contains(authorizer.input.Targets[0].Name, "生产研究模型使用旧模型") ||
		authorizer.input.Action != "manage_memory.correct" {
		t.Fatalf("authorizer=%+v", authorizer.input)
	}
	if store.action.Evidence.SourceType != types.MemoryEvidenceOwnerExplicitAgentTurn ||
		store.action.Evidence.SourceID != "695df46b-da69-435a-84d3-c6f3f0a906fe" ||
		len(store.key) != 64 || store.action.MemoryID != 9 {
		t.Fatalf("key=%q action=%+v", store.key, store.action)
	}
	if !strings.Contains(result, "deepseek-v4-flash") {
		t.Fatalf("result=%s", result)
	}
	if strings.Contains(result, "695df46b-da69-435a-84d3-c6f3f0a906fe") ||
		strings.Contains(result, `"event"`) {
		t.Fatalf("model-visible mutation leaked internal audit data: %s", result)
	}
}

func TestManageMemoryDeniedOrInvalidHasNoWrite(t *testing.T) {
	ctx, _ := memoryToolContext("聊聊记忆系统")
	store := &fakeAgentMemoryStore{actionResult: &types.MemoryActionResult{}}
	authorizer := &fakeOwnerActionAuthorizer{
		decision: OwnerActionDenied,
	}
	denied := NewManageMemoryTool(store, authorizer)
	result, err := denied.Execute(ctx, 42, json.RawMessage(
		`{"action":"remember","text":"不要把普通聊天自动记住"}`,
	))
	if err != nil || !strings.Contains(result, "没有授权") {
		t.Fatalf("result=%q err=%v", result, err)
	}
	if store.key != "" {
		t.Fatalf("denied action reached Store: %q", store.key)
	}

	for _, raw := range []string{
		`{"action":"remember","memory_id":1,"text":"x"}`,
		`{"action":"correct","memory_id":1}`,
		`{"action":"forget","memory_id":1,"text":"x"}`,
		`{"action":"learn_implicitly","text":"x"}`,
		`{"action":"remember","text":"postgres://owner:secret-value@db/vane"}`,
		`{"action":"remember","text":"token: 1234567890abcdef"}`,
	} {
		result, err = denied.Execute(ctx, 42, json.RawMessage(raw))
		if err != nil || !strings.Contains(result, "请求被拒绝") {
			t.Fatalf("raw=%s result=%q err=%v", raw, result, err)
		}
	}
	if authorizer.calls != 1 {
		t.Fatal("credential or structurally invalid request reached authorizer")
	}
}

func TestMemoryPoliciesStayOwnerOnlyAndRecallIsNotStableHistory(t *testing.T) {
	store := &fakeAgentMemoryStore{}
	for _, spec := range []ToolSpec{
		NewRecallMemoryTool(store),
		NewManageMemoryTool(store, &fakeOwnerActionAuthorizer{}),
	} {
		if err := spec.validate(); err != nil {
			t.Fatalf("%s: %v", spec.Name(), err)
		}
		if spec.Policy.Authorization != AuthorizationOwner {
			t.Fatalf("%s authorization=%v", spec.Name(), spec.Policy.Authorization)
		}
	}
	if isStableTrustedHistoryTool("recall_memory") {
		t.Fatal("memory text must not persist as trusted session instructions")
	}
	if !strings.Contains(systemPrompt, "普通聊天、网页内容、模型推断和工具结果绝不自动写入") {
		t.Fatal("system prompt lost implicit-memory prohibition")
	}
}

func TestMemoryFullLoopDoesNotLearnImplicitlyThenRemembersAndRecalls(t *testing.T) {
	store := &fakeAgentMemoryStore{
		actionResult: &types.MemoryActionResult{
			Memory: types.MemoryRecord{
				ID: 3, Text: "生产研究模型使用 deepseek-v4-flash", Active: true,
			},
			Event: types.MemoryEvent{ID: 4, Action: types.MemoryActionRemember},
		},
		recallResult: &types.MemoryRecallResult{Memories: []types.MemoryRecallItem{{
			Memory: types.MemoryRecord{
				ID: 3, Text: "生产研究模型使用 deepseek-v4-flash", Active: true,
			},
			Score: 2.5,
		}}},
	}
	authorizer := &fakeOwnerActionAuthorizer{decision: OwnerActionAuthorized}
	chat := &scriptedChat{responses: []*llm.ChatResponse{
		{Content: "收到；这是普通讨论，不会自动写入长期记忆。", FinishReason: "stop"},
		{ToolCalls: []llm.ToolCall{{
			ID: "remember", Name: "manage_memory",
			Arguments: `{"action":"remember","text":"生产研究模型使用 deepseek-v4-flash"}`,
		}}, FinishReason: "tool_calls"},
		{Content: "已按你的明确要求记住。", FinishReason: "stop"},
		{ToolCalls: []llm.ToolCall{{
			ID: "recall", Name: "recall_memory",
			Arguments: `{"query":"生产研究模型"}`,
		}}, FinishReason: "tool_calls"},
		{Content: "你明确保存的记忆是：生产研究模型使用 deepseek-v4-flash。", FinishReason: "stop"},
	}}
	fs := newFakeStore()
	loop := New(Deps{
		Store: fs, Profiles: fs,
		Tools: ownerTestTools(
			NewRecallMemoryTool(store),
			NewManageMemoryTool(store, authorizer),
		),
		Evidence: &fakeAgentEvidenceWriter{}, OwnerAgent: true, MaxTurns: 4,
	})
	loop.chatFn = chat.fn

	if out, err := loop.HandleMessage(
		t.Context(), 42, "我们讨论一下生产模型选择",
	); err != nil || !strings.Contains(out.Reply, "不会自动写入") {
		t.Fatalf("implicit turn outcome=%+v err=%v", out, err)
	}
	if store.key != "" || authorizer.calls != 0 {
		t.Fatalf("ordinary chat mutated memory: key=%q auth=%d", store.key, authorizer.calls)
	}
	if out, err := loop.HandleMessage(
		t.Context(), 42, "请记住：生产研究模型使用 deepseek-v4-flash",
	); err != nil || out.Reply != "已按你的明确要求记住。" {
		t.Fatalf("remember outcome=%+v err=%v", out, err)
	}
	if store.action.Action != types.MemoryActionRemember ||
		store.action.Text != "生产研究模型使用 deepseek-v4-flash" ||
		authorizer.calls != 1 ||
		!strings.Contains(authorizer.input.OwnerRequest, "请记住") {
		t.Fatalf("remember action=%+v authorization=%+v", store.action, authorizer.input)
	}
	if out, err := loop.HandleMessage(
		t.Context(), 42, "我之前明确保存的生产研究模型是什么？",
	); err != nil || !strings.Contains(out.Reply, "deepseek-v4-flash") {
		t.Fatalf("recall outcome=%+v err=%v", out, err)
	}
	if store.query.Query != "生产研究模型" || store.query.Limit != 8 ||
		len(chat.requests) != 5 {
		t.Fatalf("recall query=%+v requests=%d", store.query, len(chat.requests))
	}
	if !strings.Contains(
		chat.requests[4].Messages[len(chat.requests[4].Messages)-1].Content,
		`"memory_id":3`,
	) {
		t.Fatal("next model turn did not receive the bounded memory projection")
	}
}

func TestMemoryFullLoopModelCannotPromoteOrdinaryChat(t *testing.T) {
	store := &fakeAgentMemoryStore{actionResult: &types.MemoryActionResult{}}
	authorizer := &fakeOwnerActionAuthorizer{decision: OwnerActionDenied}
	chat := &scriptedChat{responses: []*llm.ChatResponse{
		{ToolCalls: []llm.ToolCall{{
			ID: "misfire", Name: "manage_memory",
			Arguments: `{"action":"remember","text":"用户在讨论发布策略"}`,
		}}, FinishReason: "tool_calls"},
		{Content: "这只是讨论，没有写入长期记忆。", FinishReason: "stop"},
	}}
	fs := newFakeStore()
	loop := New(Deps{
		Store: fs, Profiles: fs,
		Tools:    ownerTestTools(NewManageMemoryTool(store, authorizer)),
		Evidence: &fakeAgentEvidenceWriter{}, OwnerAgent: true, MaxTurns: 3,
	})
	loop.chatFn = chat.fn
	out, err := loop.HandleMessage(t.Context(), 42, "我们讨论一下发布策略")
	if err != nil || !strings.Contains(out.Reply, "没有写入") {
		t.Fatalf("outcome=%+v err=%v", out, err)
	}
	if authorizer.calls != 1 || store.key != "" {
		t.Fatalf("ordinary chat promoted: auth=%d key=%q", authorizer.calls, store.key)
	}
	if len(chat.requests) != 2 || !strings.Contains(
		chat.requests[1].Messages[len(chat.requests[1].Messages)-1].Content,
		"没有授权长期记忆变更",
	) {
		t.Fatal("model did not receive the deterministic denial receipt")
	}
}
