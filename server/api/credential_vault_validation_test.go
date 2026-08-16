package api

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/YouToco/vane/server/auth"
	"github.com/YouToco/vane/server/feishu"
	"github.com/YouToco/vane/server/store"
	"github.com/YouToco/vane/server/types"
)

type credentialManagerFake struct {
	verify feishu.VerifyResult
}

func (*credentialManagerFake) Status() feishu.Status { return feishu.Status{} }
func (f *credentialManagerFake) Verify(context.Context, string, string) feishu.VerifyResult {
	return f.verify
}
func (*credentialManagerFake) Reconfigure(context.Context) error  { return nil }
func (*credentialManagerFake) SendTestCard(context.Context) error { return nil }

func platformCredentialRequest(method, path, body string) (*http.Request, *fakeAuthStore) {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	ctx := auth.WithPrincipal(req.Context(), auth.Principal{
		TenantID: types.SingleTenantID, UserID: 9,
	})
	fake := newFakeAuthStore()
	fake.members[9] = []types.Membership{{
		TenantID: int64(types.SingleTenantID), UserID: 9, Role: types.MembershipRoleOwner,
	}}
	return req.WithContext(ctx), fake
}

func TestCredentialRequestValidationRejectsUnsafeInput(t *testing.T) {
	validBase := `"provider":"deepseek","base_url":"https://api.deepseek.com",` +
		`"api_key":"key","model":"pipeline","agent_model":"agent",` +
		`"research_model":"research","max_concurrent":4`
	tests := []struct {
		name string
		body string
	}{
		{name: "malformed json", body: `{`},
		{name: "unknown field", body: `{` + validBase + `,"unknown":true}`},
		{name: "wrong pipeline provider", body: `{"provider":"kimi","base_url":"https://api.deepseek.com","api_key":"key","model":"pipeline","agent_model":"agent","research_model":"research","max_concurrent":4}`},
		{name: "userinfo in pipeline url", body: `{"provider":"deepseek","base_url":"https://user:pass@api.deepseek.com","api_key":"key","model":"pipeline","agent_model":"agent","research_model":"research","max_concurrent":4}`},
		{name: "pipeline query", body: `{"provider":"deepseek","base_url":"https://api.deepseek.com?secret=x","api_key":"key","model":"pipeline","agent_model":"agent","research_model":"research","max_concurrent":4}`},
		{name: "non official pipeline", body: `{"provider":"deepseek","base_url":"https://proxy.invalid","api_key":"key","model":"pipeline","agent_model":"agent","research_model":"research","max_concurrent":4}`},
		{name: "kimi agent missing key", body: `{` + validBase + `,"agent_provider":"kimi","agent_base_url":"https://api.moonshot.cn/v1"}`},
		{name: "unsupported agent origin", body: `{` + validBase + `,"agent_provider":"kimi","agent_base_url":"https://proxy.invalid","agent_api_key":"agent-key"}`},
		{name: "inherited agent with stray key", body: `{` + validBase + `,"agent_api_key":"stray"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, fake := platformCredentialRequest(http.MethodPut, "/api/admin/llm/credentials", tt.body)
			recorder := httptest.NewRecorder()
			s := &server{deps: Deps{Auth: fake}}
			s.handleLLMCredentialPut(recorder, req)
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
			}
		})
	}
}

func TestCredentialHandlersRejectMissingAuthorityAndInvalidTelegram(t *testing.T) {
	t.Run("status needs principal", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		(&server{}).handleTelegramCredentialStatus(recorder,
			httptest.NewRequest(http.MethodGet, "/api/channels/telegram/credentials", nil))
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
		}
	})
	t.Run("platform owner requires exact membership", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPut, "/api/admin/llm/credentials", strings.NewReader(`{}`))
		req = req.WithContext(auth.WithPrincipal(req.Context(), auth.Principal{
			TenantID: types.SingleTenantID, UserID: 9,
		}))
		recorder := httptest.NewRecorder()
		(&server{deps: Deps{Auth: newFakeAuthStore()}}).handleLLMCredentialPut(recorder, req)
		if recorder.Code != http.StatusNotFound {
			t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
		}
	})
	t.Run("telegram token format", func(t *testing.T) {
		req := telegramPrincipalRequest(http.MethodPut, "/api/channels/telegram/credentials")
		req.Body = http.NoBody
		recorder := httptest.NewRecorder()
		(&server{}).handleTelegramCredentialPut(recorder, req)
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
		}
	})
	t.Run("telegram provider rejects token", func(t *testing.T) {
		provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusUnauthorized)
		}))
		defer provider.Close()
		req := telegramPrincipalRequest(http.MethodPut, "/api/channels/telegram/credentials")
		req.Body = io.NopCloser(strings.NewReader(`{"bot_token":"551199:synthetic"}`))
		recorder := httptest.NewRecorder()
		(&server{deps: Deps{TelegramAPIBaseURL: provider.URL}}).handleTelegramCredentialPut(recorder, req)
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
		}
	})
}

func TestTenantAndPlatformOwnerGatesRequireExactOwnerMembership(t *testing.T) {
	req := telegramPrincipalRequest(http.MethodGet, "/api/owner")
	for _, test := range []struct {
		name        string
		memberships []types.Membership
		platform    bool
		want        bool
	}{
		{name: "tenant owner", memberships: []types.Membership{{TenantID: 7, UserID: 9, Role: types.MembershipRoleOwner}}, want: true},
		{name: "tenant member", memberships: []types.Membership{{TenantID: 7, UserID: 9, Role: types.MembershipRoleMember}}},
		{name: "wrong user owner", memberships: []types.Membership{{TenantID: 7, UserID: 10, Role: types.MembershipRoleOwner}}},
		{name: "platform requires tenant one", memberships: []types.Membership{{TenantID: 7, UserID: 9, Role: types.MembershipRoleOwner}}, platform: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			fake := newFakeAuthStore()
			fake.members[9] = test.memberships
			s := &server{deps: Deps{Auth: fake}}
			recorder := httptest.NewRecorder()
			var got bool
			if test.platform {
				got = s.requirePlatformOwner(recorder, req)
			} else {
				_, got = s.requireTenantOwner(recorder, req)
			}
			if got != test.want {
				t.Fatalf("got=%t want=%t status=%d body=%s",
					got, test.want, recorder.Code, recorder.Body.String())
			}
		})
	}
	missing := httptest.NewRecorder()
	if _, ok := (&server{}).requireTenantOwner(missing,
		httptest.NewRequest(http.MethodGet, "/api/owner", nil)); ok ||
		missing.Code != http.StatusBadRequest {
		t.Fatalf("missing principal ok=%t status=%d", ok, missing.Code)
	}
}

func TestFeishuCredentialValidationAndRotationPostgres(t *testing.T) {
	st := inviteAPIStore(t)
	if err := st.ConfigureCredentialVault("api-feishu-key", strings.Repeat("31", 32), ""); err != nil {
		t.Fatal(err)
	}
	user, err := st.UpsertUserByOpenID(t.Context(),
		fmt.Sprintf("feishu-credential-validation-%d", time.Now().UnixNano()), "owner")
	if err != nil {
		t.Fatal(err)
	}
	if err := st.AddMembership(t.Context(), 1, user.ID, types.MembershipRoleOwner); err != nil {
		t.Fatal(err)
	}
	cleanup, err := pgxpool.New(t.Context(), os.Getenv("DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = cleanup.Exec(t.Context(), `DELETE FROM credential_vault_entries WHERE created_by_user_id=$1`, user.ID)
		_, _ = cleanup.Exec(t.Context(), `DELETE FROM memberships WHERE user_id=$1`, user.ID)
		_, _ = cleanup.Exec(t.Context(), `DELETE FROM users WHERE id=$1`, user.ID)
		cleanup.Close()
	})
	credentialRequest := func(method string) *http.Request {
		req := httptest.NewRequest(method, "/api/channels/feishu/credentials", nil)
		return req.WithContext(auth.WithPrincipal(req.Context(), auth.Principal{
			TenantID: 1, UserID: user.ID,
		}))
	}
	req := credentialRequest(http.MethodPut)
	req.Body = io.NopCloser(strings.NewReader(`{"app_id":"","app_secret":""}`))
	recorder := httptest.NewRecorder()
	s := &server{deps: Deps{Store: st, Manager: &credentialManagerFake{}}}
	missingStatus := httptest.NewRecorder()
	s.handleFeishuCredentialStatus(missingStatus, credentialRequest(http.MethodGet))
	if missingStatus.Code != http.StatusOK ||
		!strings.Contains(missingStatus.Body.String(), `"configured":false`) {
		t.Fatalf("missing status=%d body=%s", missingStatus.Code, missingStatus.Body.String())
	}
	s.handleFeishuCredentialPut(recorder, req)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("empty status=%d body=%s", recorder.Code, recorder.Body.String())
	}

	req = credentialRequest(http.MethodPut)
	req.Body = io.NopCloser(strings.NewReader(`{"app_id":"cli_test","app_secret":"synthetic"}`))
	recorder = httptest.NewRecorder()
	s.handleFeishuCredentialPut(recorder, req)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("verify status=%d body=%s", recorder.Code, recorder.Body.String())
	}

	s.deps.Manager = &credentialManagerFake{verify: feishu.VerifyResult{CredentialsOK: true, BotOK: true}}
	req = credentialRequest(http.MethodPut)
	req.Body = io.NopCloser(strings.NewReader(`{"app_id":"cli_test","app_secret":"synthetic"}`))
	recorder = httptest.NewRecorder()
	s.handleFeishuCredentialPut(recorder, req)
	if recorder.Code != http.StatusOK || strings.Contains(recorder.Body.String(), "synthetic") {
		t.Fatalf("rotate status=%d body=%s", recorder.Code, recorder.Body.String())
	}

	status := httptest.NewRecorder()
	s.handleFeishuCredentialStatus(status,
		credentialRequest(http.MethodGet))
	if status.Code != http.StatusOK || !strings.Contains(status.Body.String(), `"configured":true`) {
		t.Fatalf("status=%d body=%s", status.Code, status.Body.String())
	}
	deleted := httptest.NewRecorder()
	s.handleFeishuCredentialDelete(deleted,
		credentialRequest(http.MethodDelete))
	if deleted.Code != http.StatusOK {
		t.Fatalf("delete status=%d body=%s", deleted.Code, deleted.Body.String())
	}
}

func TestCredentialHandlersSurfaceClosedDatabaseFailuresPostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		requireDatabaseCapability(t)
	}
	st, err := store.New(t.Context(), databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.ConfigureCredentialVault("closed-api-key", strings.Repeat("63", 32), ""); err != nil {
		t.Fatal(err)
	}
	st.Close()
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"result":{"id":551199,"username":"closed_bot"}}`))
	}))
	defer provider.Close()
	fakeAuth := newFakeAuthStore()
	fakeAuth.members[9] = []types.Membership{{TenantID: 1, UserID: 9, Role: types.MembershipRoleOwner}}
	s := &server{deps: Deps{Store: st, Auth: fakeAuth,
		Manager:            &credentialManagerFake{verify: feishu.VerifyResult{CredentialsOK: true}},
		TelegramAPIBaseURL: provider.URL}}
	userRequest := func(method, path, body string) *http.Request {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		return req.WithContext(auth.WithPrincipal(req.Context(), auth.Principal{TenantID: 1, UserID: 9}))
	}
	platformRequest := func(method, body string) *http.Request {
		return userRequest(method, "/api/admin/llm/credentials", body)
	}
	tests := []struct {
		name string
		call func(http.ResponseWriter, *http.Request)
		req  *http.Request
	}{
		{name: "status", call: s.handleTelegramCredentialStatus,
			req: userRequest(http.MethodGet, "/api/channels/telegram/credentials", "")},
		{name: "telegram put", call: s.handleTelegramCredentialPut,
			req: userRequest(http.MethodPut, "/api/channels/telegram/credentials", `{"bot_token":"551199:synthetic"}`)},
		{name: "feishu put", call: s.handleFeishuCredentialPut,
			req: userRequest(http.MethodPut, "/api/channels/feishu/credentials", `{"app_id":"cli_test","app_secret":"synthetic"}`)},
		{name: "llm put", call: s.handleLLMCredentialPut,
			req: platformRequest(http.MethodPut, `{"provider":"deepseek","base_url":"https://api.deepseek.com","api_key":"synthetic","model":"pipeline","agent_model":"agent","research_model":"research","max_concurrent":4}`)},
		{name: "telegram delete", call: s.handleTelegramCredentialDelete,
			req: userRequest(http.MethodDelete, "/api/channels/telegram/credentials", "")},
		{name: "feishu delete", call: s.handleFeishuCredentialDelete,
			req: userRequest(http.MethodDelete, "/api/channels/feishu/credentials", "")},
		{name: "llm delete", call: s.handleLLMCredentialDelete,
			req: platformRequest(http.MethodDelete, "")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			test.call(recorder, test.req)
			if recorder.Code == http.StatusOK {
				t.Fatalf("closed database failure hidden: %s", recorder.Body.String())
			}
		})
	}
}
