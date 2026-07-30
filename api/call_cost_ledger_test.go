package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/YouToco/vane/auth"
	"github.com/YouToco/vane/types"
)

func TestParseCallCostLedgerQuery(t *testing.T) {
	query, message := parseCallCostLedgerQuery(url.Values{
		"page_size":      {"25"},
		"page_token":     {"cursor"},
		"kind":           {"tool"},
		"provider":       {"exa"},
		"pricing_status": {"provider_reported"},
		"task_id":        {"task-1"},
	})
	if message != "" || query.PageSize != 25 || query.Kind != "tool" ||
		query.Provider != "exa" || query.PageToken != "cursor" {
		t.Fatalf("parsed query=%+v message=%q", query, message)
	}
	for _, pageSize := range []string{"x", "0", "101"} {
		if _, message := parseCallCostLedgerQuery(url.Values{"page_size": {pageSize}}); message == "" {
			t.Fatalf("invalid page_size %q accepted", pageSize)
		}
	}
}

func TestCallCostLedgerEndpointOwnerAndSafeProjection(t *testing.T) {
	if os.Getenv("DATABASE_URL") == "" {
		t.Skip("未设置 DATABASE_URL，跳过逐笔调用账单 API 集成测试")
	}
	st := inviteAPIStore(t)
	ctx := t.Context()
	provider := "ledger-api-" + uuid.NewString()
	callID, err := st.InsertLLMCall(ctx, &types.LLMCall{
		TraceID: "ledger-api-trace-" + uuid.NewString(), SpanName: "agent",
		Provider: provider, Model: "unpriced-model",
		SystemPrompt: "API_SECRET_SYSTEM", UserPrompt: "API_SECRET_USER",
		Completion: "API_SECRET_COMPLETION", Error: "API_SECRET_PROVIDER_ERROR",
		PromptTokens: 123, CompletionTokens: 45, LatencyMs: 678,
	})
	if err != nil {
		t.Fatalf("seed llm call: %v", err)
	}
	cleanupPool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		t.Fatalf("open cleanup database connection: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = cleanupPool.Exec(cleanupCtx, `DELETE FROM llm_calls WHERE id=$1`, callID)
		cleanupPool.Close()
	})

	fake := newFakeAuthStore()
	token, hash, err := auth.NewSessionToken()
	if err != nil {
		t.Fatal(err)
	}
	const ownerID = int64(900001)
	fake.sessions[string(hash)] = &types.Session{
		TokenHash: hash, UserID: ownerID, TenantID: 1,
		ExpiresAt: time.Now().Add(time.Hour),
	}
	fake.members[ownerID] = []types.Membership{{
		TenantID: 1, UserID: ownerID, Role: types.MembershipRoleOwner,
	}}
	mux := http.NewServeMux()
	Mount(mux, Deps{Store: st, Auth: fake, Principal: auth.NewContextResolver()})

	req := httptest.NewRequest(
		http.MethodGet,
		"/api/admin/cost-calls?page_size=10&provider="+provider,
		nil,
	)
	req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: token})
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET cost calls=%d body=%s", rec.Code, rec.Body.String())
	}
	var response callCostLedgerResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v body=%s", err, rec.Body.String())
	}
	if len(response.Items) != 1 || response.Items[0].ID != callID ||
		response.Items[0].LLMUsage == nil || response.Items[0].PricingStatus != "unpriced" ||
		!response.Items[0].Failed {
		t.Fatalf("unexpected ledger projection: %+v", response.Items)
	}
	for _, secret := range []string{
		"API_SECRET_SYSTEM", "API_SECRET_USER", "API_SECRET_COMPLETION",
		"API_SECRET_PROVIDER_ERROR",
	} {
		if strings.Contains(rec.Body.String(), secret) {
			t.Fatalf("API leaked sensitive field %q: %s", secret, rec.Body.String())
		}
	}

	invalid := httptest.NewRequest(
		http.MethodGet,
		"/api/admin/cost-calls?kind=unknown",
		nil,
	)
	invalid.AddCookie(&http.Cookie{Name: sessionCookieName, Value: token})
	invalidRec := httptest.NewRecorder()
	mux.ServeHTTP(invalidRec, invalid)
	if invalidRec.Code != http.StatusBadRequest {
		t.Fatalf("invalid kind=%d body=%s", invalidRec.Code, invalidRec.Body.String())
	}
}
