package api

import (
	"bytes"
	"encoding/json"
	"errors"
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
	const syntheticAgentSecret = "synthetic-kimi-secret-never-echo"
	payload := map[string]any{
		"provider": "deepseek", "base_url": "https://api.deepseek.com",
		"api_key": syntheticSecret, "model": "pipeline-model",
		"agent_provider": "kimi", "agent_base_url": "https://api.moonshot.cn/v1",
		"agent_api_key": syntheticAgentSecret,
		"agent_model":   "agent-model", "research_model": "research-model",
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
		strings.Contains(putResponse.Body.String(), syntheticAgentSecret) ||
		!strings.Contains(putResponse.Body.String(), "restart_required") {
		t.Fatalf("PUT response leaked secret or hid activation boundary: %s", putResponse.Body.String())
	}
	var ciphertext []byte
	if err := cleanup.QueryRow(ctx, `SELECT ciphertext FROM credential_vault_entries
		WHERE scope_kind='platform' AND provider='llm' AND purpose='shared_runtime' AND status='active'`,
	).Scan(&ciphertext); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(ciphertext, []byte(syntheticSecret)) ||
		bytes.Contains(ciphertext, []byte(syntheticAgentSecret)) {
		t.Fatal("database ciphertext contains the LLM API key")
	}

	get := httptest.NewRequest(http.MethodGet, "/api/admin/llm/credentials", nil)
	get.AddCookie(cookie)
	getResponse := httptest.NewRecorder()
	mux.ServeHTTP(getResponse, get)
	if getResponse.Code != http.StatusOK || strings.Contains(getResponse.Body.String(), syntheticSecret) ||
		strings.Contains(getResponse.Body.String(), syntheticAgentSecret) ||
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

func TestFetchCredentialEndpointEncryptsAndNeverEchoesSecretPostgres(t *testing.T) {
	if os.Getenv("DATABASE_URL") == "" {
		requireDatabaseCapability(t)
	}
	st := inviteAPIStore(t)
	if err := st.ConfigureCredentialVault("api-fetch-test", strings.Repeat("62", 32), ""); err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	user, err := st.UpsertUserByOpenID(ctx,
		fmt.Sprintf("fetch-credential-api-owner-%d", time.Now().UnixNano()), "owner")
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
	const exaSecret = "synthetic-exa-secret-never-echo"
	const tikHubSecret = "synthetic-tikhub-secret-never-echo"
	raw, _ := json.Marshal(map[string]string{
		"exa_api_key": exaSecret, "tikhub_api_key": tikHubSecret,
	})
	put := httptest.NewRequest(http.MethodPut, "/api/admin/fetch/credentials", bytes.NewReader(raw))
	put.AddCookie(cookie)
	putResponse := httptest.NewRecorder()
	mux.ServeHTTP(putResponse, put)
	if putResponse.Code != http.StatusOK ||
		strings.Contains(putResponse.Body.String(), exaSecret) ||
		strings.Contains(putResponse.Body.String(), tikHubSecret) ||
		!strings.Contains(putResponse.Body.String(), "restart_required") {
		t.Fatalf("PUT status=%d body=%s", putResponse.Code, putResponse.Body.String())
	}
	var ciphertext []byte
	if err := cleanup.QueryRow(ctx, `SELECT ciphertext FROM credential_vault_entries
		WHERE scope_kind='platform' AND provider='fetch' AND purpose='shared_runtime' AND status='active'`,
	).Scan(&ciphertext); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(ciphertext, []byte(exaSecret)) || bytes.Contains(ciphertext, []byte(tikHubSecret)) {
		t.Fatal("database ciphertext contains a fetch provider API key")
	}
	get := httptest.NewRequest(http.MethodGet, "/api/admin/fetch/credentials", nil)
	get.AddCookie(cookie)
	getResponse := httptest.NewRecorder()
	mux.ServeHTTP(getResponse, get)
	if getResponse.Code != http.StatusOK || strings.Contains(getResponse.Body.String(), exaSecret) ||
		strings.Contains(getResponse.Body.String(), tikHubSecret) ||
		!strings.Contains(getResponse.Body.String(), `"configured":true`) {
		t.Fatalf("GET status=%d body=%s", getResponse.Code, getResponse.Body.String())
	}
	deleteRequest := httptest.NewRequest(http.MethodDelete, "/api/admin/fetch/credentials", nil)
	deleteRequest.AddCookie(cookie)
	deleteResponse := httptest.NewRecorder()
	mux.ServeHTTP(deleteResponse, deleteRequest)
	if deleteResponse.Code != http.StatusOK {
		t.Fatalf("DELETE status=%d body=%s", deleteResponse.Code, deleteResponse.Body.String())
	}
}

func TestOrdinaryMemberTelegramCredentialIsUserScopedEncryptedAndActivatedPostgres(t *testing.T) {
	if os.Getenv("DATABASE_URL") == "" {
		requireDatabaseCapability(t)
	}
	st := inviteAPIStore(t)
	if err := st.ConfigureCredentialVault("api-user-key", strings.Repeat("24", 32), ""); err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	user, err := st.UpsertUserByOpenID(ctx,
		fmt.Sprintf("credential-api-member-%d", time.Now().UnixNano()), "member")
	if err != nil {
		t.Fatal(err)
	}
	if err := st.AddMembership(ctx, 1, user.ID, types.MembershipRoleMember); err != nil {
		t.Fatal(err)
	}
	cleanupDB, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = cleanupDB.Exec(t.Context(), `DELETE FROM credential_vault_entries WHERE created_by_user_id=$1`, user.ID)
		_, _ = cleanupDB.Exec(t.Context(), `DELETE FROM memberships WHERE user_id=$1`, user.ID)
		_, _ = cleanupDB.Exec(t.Context(), `DELETE FROM users WHERE id=$1`, user.ID)
		cleanupDB.Close()
	})
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/getMe") {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"result":{"id":551199,"username":"member_bot"}}`))
	}))
	defer provider.Close()

	authStore := newFakeAuthStore()
	token, hash, err := auth.NewSessionToken()
	if err != nil {
		t.Fatal(err)
	}
	authStore.sessions[string(hash)] = &types.Session{TokenHash: hash,
		UserID: user.ID, TenantID: 1, ExpiresAt: time.Now().Add(time.Hour)}
	authStore.members[user.ID] = []types.Membership{{
		TenantID: 1, UserID: user.ID, Role: types.MembershipRoleMember}}
	runtime := &fakeTelegramManager{}
	mux := http.NewServeMux()
	Mount(mux, Deps{Store: st, Auth: authStore, Telegram: runtime,
		TelegramAPIBaseURL: provider.URL, Principal: auth.NewContextResolver()})
	const syntheticToken = "551199:synthetic-member-token"
	put := httptest.NewRequest(http.MethodPut, "/api/channels/telegram/credentials",
		strings.NewReader(`{"bot_token":"`+syntheticToken+`"}`))
	put.AddCookie(&http.Cookie{Name: sessionCookieName, Value: token})
	putResponse := httptest.NewRecorder()
	mux.ServeHTTP(putResponse, put)
	if putResponse.Code != http.StatusOK || !runtime.activated ||
		runtime.tenantID != 1 || runtime.userID != user.ID ||
		!strings.Contains(putResponse.Body.String(), `"activation":"active"`) ||
		strings.Contains(putResponse.Body.String(), syntheticToken) {
		t.Fatalf("PUT status=%d runtime=%+v body=%s",
			putResponse.Code, runtime, putResponse.Body.String())
	}
	var ciphertext []byte
	var scopeKind string
	var storedUserID int64
	var externalIdentity string
	if err := cleanupDB.QueryRow(ctx, `SELECT scope_kind,user_id,external_identity,ciphertext
		FROM credential_vault_entries WHERE provider='telegram' AND purpose='bot_api' AND
		created_by_user_id=$1 AND status='active'`, user.ID).Scan(
		&scopeKind, &storedUserID, &externalIdentity, &ciphertext); err != nil {
		t.Fatal(err)
	}
	if scopeKind != "user" || storedUserID != user.ID || externalIdentity != "551199" ||
		bytes.Contains(ciphertext, []byte(syntheticToken)) {
		t.Fatalf("unsafe row scope=%s user=%d external=%s", scopeKind, storedUserID, externalIdentity)
	}

	deleteRequest := httptest.NewRequest(http.MethodDelete,
		"/api/channels/telegram/credentials", nil)
	deleteRequest.AddCookie(&http.Cookie{Name: sessionCookieName, Value: token})
	deleteResponse := httptest.NewRecorder()
	mux.ServeHTTP(deleteResponse, deleteRequest)
	if deleteResponse.Code != http.StatusOK || !runtime.deactivated {
		t.Fatalf("DELETE status=%d runtime=%+v body=%s",
			deleteResponse.Code, runtime, deleteResponse.Body.String())
	}

	runtime.activateErr = errors.New("synthetic activation failure")
	retryPut := httptest.NewRequest(http.MethodPut, "/api/channels/telegram/credentials",
		strings.NewReader(`{"bot_token":"`+syntheticToken+`"}`))
	retryPut.AddCookie(&http.Cookie{Name: sessionCookieName, Value: token})
	retryPutResponse := httptest.NewRecorder()
	mux.ServeHTTP(retryPutResponse, retryPut)
	if retryPutResponse.Code != http.StatusServiceUnavailable ||
		!strings.Contains(retryPutResponse.Body.String(), "failed_restart_required") {
		t.Fatalf("activation failure status=%d body=%s",
			retryPutResponse.Code, retryPutResponse.Body.String())
	}
	runtime.activateErr = nil
	runtime.deactivateErr = errors.New("synthetic deactivation failure")
	retryDelete := httptest.NewRequest(http.MethodDelete,
		"/api/channels/telegram/credentials", nil)
	retryDelete.AddCookie(&http.Cookie{Name: sessionCookieName, Value: token})
	retryDeleteResponse := httptest.NewRecorder()
	mux.ServeHTTP(retryDeleteResponse, retryDelete)
	if retryDeleteResponse.Code != http.StatusServiceUnavailable ||
		!strings.Contains(retryDeleteResponse.Body.String(), "revoked_restart_required") {
		t.Fatalf("deactivation failure status=%d body=%s",
			retryDeleteResponse.Code, retryDeleteResponse.Body.String())
	}
}
