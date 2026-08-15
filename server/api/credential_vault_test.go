package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/YouToco/vane/server/auth"
	"github.com/YouToco/vane/server/types"
)

func TestLLMCredentialEndpointEncryptsAndNeverEchoesSecretPostgres(t *testing.T) {
	if os.Getenv("DATABASE_URL") == "" {
		requireDatabaseCapability(t)
	}
	st := inviteAPIStore(t)
	if err := st.ConfigureCredentialVault("api-test-key", strings.Repeat("42", 32), ""); err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	user, err := st.UpsertUserByOpenID(ctx,
		fmt.Sprintf("credential-api-owner-%d", time.Now().UnixNano()), "owner")
	if err != nil {
		t.Fatal(err)
	}
	if err := st.AddMembership(ctx, 1, user.ID, types.MembershipRoleOwner); err != nil {
		t.Fatal(err)
	}
	cleanup, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = cleanup.Exec(t.Context(), `DELETE FROM credential_vault_entries WHERE created_by_user_id=$1`, user.ID)
		_, _ = cleanup.Exec(t.Context(), `DELETE FROM memberships WHERE user_id=$1`, user.ID)
		_, _ = cleanup.Exec(t.Context(), `DELETE FROM users WHERE id=$1`, user.ID)
		cleanup.Close()
	})

	fake := newFakeAuthStore()
	token, hash, err := auth.NewSessionToken()
	if err != nil {
		t.Fatal(err)
	}
	fake.sessions[string(hash)] = &types.Session{
		TokenHash: hash, UserID: user.ID, TenantID: 1,
		ExpiresAt: time.Now().Add(time.Hour),
	}
	fake.members[user.ID] = []types.Membership{{
		TenantID: 1, UserID: user.ID, Role: types.MembershipRoleOwner,
	}}
	mux := http.NewServeMux()
	Mount(mux, Deps{Store: st, Auth: fake, Principal: auth.NewContextResolver()})
	cookie := &http.Cookie{Name: sessionCookieName, Value: token}
	const syntheticSecret = "synthetic-llm-secret-never-echo"
	payload := map[string]any{
		"provider": "deepseek", "base_url": "https://api.deepseek.com",
		"api_key": syntheticSecret, "model": "pipeline-model",
		"agent_model": "agent-model", "research_model": "research-model",
		"max_concurrent": 4,
	}
	raw, _ := json.Marshal(payload)
	put := httptest.NewRequest(http.MethodPut, "/api/admin/llm/credentials", bytes.NewReader(raw))
	put.AddCookie(cookie)
	putResponse := httptest.NewRecorder()
	mux.ServeHTTP(putResponse, put)
	if putResponse.Code != http.StatusOK {
		t.Fatalf("PUT status=%d body=%s", putResponse.Code, putResponse.Body.String())
	}
	if strings.Contains(putResponse.Body.String(), syntheticSecret) ||
		!strings.Contains(putResponse.Body.String(), "restart_required") {
		t.Fatalf("PUT response leaked secret or hid activation boundary: %s", putResponse.Body.String())
	}
	var ciphertext []byte
	if err := cleanup.QueryRow(ctx, `SELECT ciphertext FROM credential_vault_entries
		WHERE scope_kind='platform' AND provider='llm' AND purpose='shared_runtime' AND status='active'`,
	).Scan(&ciphertext); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(ciphertext, []byte(syntheticSecret)) {
		t.Fatal("database ciphertext contains the LLM API key")
	}

	get := httptest.NewRequest(http.MethodGet, "/api/admin/llm/credentials", nil)
	get.AddCookie(cookie)
	getResponse := httptest.NewRecorder()
	mux.ServeHTTP(getResponse, get)
	if getResponse.Code != http.StatusOK || strings.Contains(getResponse.Body.String(), syntheticSecret) ||
		!strings.Contains(getResponse.Body.String(), `"configured":true`) {
		t.Fatalf("GET status=%d body=%s", getResponse.Code, getResponse.Body.String())
	}

	deleteRequest := httptest.NewRequest(http.MethodDelete, "/api/admin/llm/credentials", nil)
	deleteRequest.AddCookie(cookie)
	deleteResponse := httptest.NewRecorder()
	mux.ServeHTTP(deleteResponse, deleteRequest)
	if deleteResponse.Code != http.StatusOK {
		t.Fatalf("DELETE status=%d body=%s", deleteResponse.Code, deleteResponse.Body.String())
	}
	var active int
	if err := cleanup.QueryRow(ctx, `SELECT count(*) FROM credential_vault_entries
		WHERE scope_kind='platform' AND provider='llm' AND purpose='shared_runtime' AND status='active'`,
	).Scan(&active); err != nil {
		t.Fatal(err)
	}
	if active != 0 {
		t.Fatalf("active credentials after revoke=%d", active)
	}
}
