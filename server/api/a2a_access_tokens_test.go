package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/server/auth"
	"github.com/YouToco/vane/server/types"
)

type fakeA2AAccessStore struct {
	issued       *types.IssueA2AAccessToken
	issueResult  *types.A2AAccessToken
	issueErr     error
	listTenant   int64
	listUser     int64
	listResult   []types.A2AAccessToken
	revokeTenant int64
	revokeUser   int64
	revokeToken  string
	revokeErr    error
}

func (f *fakeA2AAccessStore) IssueA2AAccessToken(_ context.Context, input types.IssueA2AAccessToken) (*types.A2AAccessToken, error) {
	f.issued = &input
	if f.issueErr != nil {
		return nil, f.issueErr
	}
	if f.issueResult != nil {
		copy := *f.issueResult
		return &copy, nil
	}
	return &types.A2AAccessToken{
		ID: uuid.NewString(), TenantID: input.TenantID,
		PrincipalUserID: input.PrincipalUserID, ActorType: input.ActorType,
		Scopes: input.Scopes, IssuedBy: input.ActorUserID,
		ExpiresAt: input.ExpiresAt, CreatedAt: time.Now(),
	}, nil
}

func (f *fakeA2AAccessStore) ListA2AAccessTokens(_ context.Context, tenantID, userID int64) ([]types.A2AAccessToken, error) {
	f.listTenant, f.listUser = tenantID, userID
	return append([]types.A2AAccessToken(nil), f.listResult...), nil
}

func (f *fakeA2AAccessStore) RevokeA2AAccessToken(_ context.Context, tenantID, userID int64, tokenID string) error {
	f.revokeTenant, f.revokeUser, f.revokeToken = tenantID, userID, tokenID
	return f.revokeErr
}

func a2aAccessTestMux(t *testing.T, tenantID, userID int64, role types.MembershipRole,
	actorType types.ActorType, access A2AAccessStore,
) (*http.ServeMux, *http.Cookie) {
	t.Helper()
	authStore := newFakeAuthStore()
	raw, hash, err := auth.NewSessionToken()
	if err != nil {
		t.Fatal(err)
	}
	authStore.sessions[string(hash)] = &types.Session{
		TokenHash: hash, TenantID: tenantID, UserID: userID, Role: role,
		ActorType: actorType, ExpiresAt: time.Now().Add(time.Hour),
	}
	authStore.members[userID] = []types.Membership{{
		TenantID: tenantID, UserID: userID, Role: role,
	}}
	mux := http.NewServeMux()
	Mount(mux, Deps{Auth: authStore, Principal: auth.NewContextResolver(), A2AAccess: access})
	return mux, &http.Cookie{Name: sessionCookieName, Value: raw}
}

func serveA2AAccessRequest(mux *http.ServeMux, request *http.Request,
	cookie *http.Cookie,
) *httptest.ResponseRecorder {
	if request.Method == http.MethodPost && request.URL.Path == "/api/a2a-tokens" &&
		request.Header.Get(reauthHeaderName) == "" {
		raw, _, err := auth.NewSessionToken()
		if err != nil {
			panic(err)
		}
		request.Header.Set(reauthHeaderName, raw)
	}
	request.AddCookie(cookie)
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	return response
}

func TestA2ATokenIssueUsesPrincipalScopeAndReturnsBearerOnce(t *testing.T) {
	access := &fakeA2AAccessStore{}
	mux, cookie := a2aAccessTestMux(t, 41, 51, types.MembershipRoleMember,
		types.ActorTypeUser, access)
	response := serveA2AAccessRequest(mux, httptest.NewRequest(http.MethodPost,
		"/api/a2a-tokens?tenant_id=999", strings.NewReader(`{
			"actor_type":"user","principal_user_id":51,
			"scopes":["assistant.chat"],"expires_in_days":7
		}`)), cookie)
	if response.Code != http.StatusCreated {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if access.issued == nil || access.issued.TenantID != 41 ||
		access.issued.ActorUserID != 51 || access.issued.PrincipalUserID != 51 {
		t.Fatalf("request escaped principal scope: %+v", access.issued)
	}
	if len(access.issued.TokenHash) != 32 {
		t.Fatalf("Store did not receive a hash-only bearer: %d", len(access.issued.TokenHash))
	}
	if len(access.issued.SessionTokenHash) != 32 || len(access.issued.ReauthProofHash) != 32 {
		t.Fatalf("Store did not receive session-bound reauth: %+v", access.issued)
	}
	var body map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if token, _ := body["token"].(string); token == "" {
		t.Fatalf("issuance response omitted one-time bearer: %s", response.Body.String())
	}
	if strings.Contains(response.Body.String(), "token_hash") {
		t.Fatalf("hash leaked into HTTP response: %s", response.Body.String())
	}
	if response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("issuance response is cacheable: %v", response.Header())
	}
}

func TestA2ATokenIssueRejectsIdentityOverridesAndUnknownScopeFields(t *testing.T) {
	for _, test := range []struct {
		name string
		body string
		code int
	}{
		{"other user", `{"actor_type":"user","principal_user_id":52,"scopes":["assistant.chat"]}`, http.StatusForbidden},
		{"tenant override", `{"tenant_id":999,"scopes":["assistant.chat"]}`, http.StatusBadRequest},
		{"secret-like label whitespace", `{"actor_type":"service_account","principal_user_id":52,"service_account_label":" bot ","scopes":["content.query"]}`, http.StatusBadRequest},
		{"credential label", `{"actor_type":"service_account","principal_user_id":52,"service_account_label":"sk-AAAAAAAAAAAAAAAAAAAA","scopes":["content.query"]}`, http.StatusBadRequest},
		{"control label", `{"actor_type":"service_account","principal_user_id":51,"service_account_label":"bot\tname","scopes":["content.query"]}`, http.StatusBadRequest},
	} {
		t.Run(test.name, func(t *testing.T) {
			access := &fakeA2AAccessStore{}
			mux, cookie := a2aAccessTestMux(t, 41, 51, types.MembershipRoleOwner,
				types.ActorTypeUser, access)
			response := serveA2AAccessRequest(mux, httptest.NewRequest(http.MethodPost,
				"/api/a2a-tokens", strings.NewReader(test.body)), cookie)
			if response.Code != test.code || access.issued != nil {
				t.Fatalf("status=%d issued=%+v body=%s", response.Code, access.issued, response.Body.String())
			}
		})
	}
}

func TestA2ATokenIssueRequiresRecentSessionBoundReauth(t *testing.T) {
	access := &fakeA2AAccessStore{}
	mux, cookie := a2aAccessTestMux(t, 41, 51, types.MembershipRoleOwner,
		types.ActorTypeUser, access)
	request := httptest.NewRequest(http.MethodPost, "/api/a2a-tokens",
		strings.NewReader(`{"scopes":["assistant.chat"]}`))
	request.AddCookie(cookie)
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden || access.issued != nil {
		t.Fatalf("missing reauth issued credential: status=%d issued=%+v body=%s",
			response.Code, access.issued, response.Body.String())
	}
}

func TestA2ATokenListNeverRecoversBearerAndUsesPrincipal(t *testing.T) {
	access := &fakeA2AAccessStore{listResult: []types.A2AAccessToken{{
		ID: uuid.NewString(), TenantID: 61, PrincipalUserID: 71,
		ActorType: types.ActorTypeUser, Scopes: []types.A2AScope{types.A2AScopeContentQuery},
		TokenHash: []byte("must-not-leak"), RawTokenOnce: "raw-must-not-leak",
	}}}
	mux, cookie := a2aAccessTestMux(t, 61, 71, types.MembershipRoleMember,
		types.ActorTypeUser, access)
	response := serveA2AAccessRequest(mux, httptest.NewRequest(http.MethodGet,
		"/api/a2a-tokens?tenant_id=999&user_id=999", nil), cookie)
	if response.Code != http.StatusOK || access.listTenant != 61 || access.listUser != 71 {
		t.Fatalf("status=%d tenant=%d user=%d body=%s", response.Code,
			access.listTenant, access.listUser, response.Body.String())
	}
	if strings.Contains(response.Body.String(), "raw-must-not-leak") ||
		strings.Contains(response.Body.String(), "must-not-leak") ||
		strings.Contains(response.Body.String(), `"token"`) {
		t.Fatalf("list became a bearer recovery endpoint: %s", response.Body.String())
	}
}

func TestA2ATokenRevokeUsesPrincipalAndServiceActorCannotManage(t *testing.T) {
	tokenID := uuid.NewString()
	access := &fakeA2AAccessStore{}
	mux, cookie := a2aAccessTestMux(t, 81, 91, types.MembershipRoleAdmin,
		types.ActorTypeUser, access)
	response := serveA2AAccessRequest(mux, httptest.NewRequest(http.MethodDelete,
		"/api/a2a-tokens/"+tokenID+"?tenant_id=999", nil), cookie)
	if response.Code != http.StatusOK || access.revokeTenant != 81 ||
		access.revokeUser != 91 || access.revokeToken != tokenID {
		t.Fatalf("revoke escaped principal: status=%d values=%d/%d/%s body=%s",
			response.Code, access.revokeTenant, access.revokeUser,
			access.revokeToken, response.Body.String())
	}

	serviceMux, serviceCookie := a2aAccessTestMux(t, 81, 91,
		types.MembershipRoleAdmin, types.ActorTypeServiceAccount, access)
	response = serveA2AAccessRequest(serviceMux, httptest.NewRequest(http.MethodPost,
		"/api/a2a-tokens", strings.NewReader(`{"scopes":["assistant.chat"]}`)), serviceCookie)
	if response.Code != http.StatusForbidden {
		t.Fatalf("service actor minted credentials: status=%d body=%s", response.Code, response.Body.String())
	}
}
