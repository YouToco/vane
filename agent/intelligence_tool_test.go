package agent

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/YouToco/vane/agentcontext"
	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
)

type fakeIntelligenceQueryStore struct {
	scope store.IntelligenceScope
	query store.IntelligenceQuery
	err   error
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
