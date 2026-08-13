package agent

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/YouToco/vane/agentcontext"
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
	denied := NewManageMemoryTool(store, &fakeOwnerActionAuthorizer{
		decision: OwnerActionDenied,
	})
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
	} {
		result, err = denied.Execute(ctx, 42, json.RawMessage(raw))
		if err != nil || !strings.Contains(result, "请求被拒绝") {
			t.Fatalf("raw=%s result=%q err=%v", raw, result, err)
		}
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
