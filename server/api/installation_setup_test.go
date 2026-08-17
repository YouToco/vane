package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/YouToco/vane/server/types"
)

type fakeInstallationSetupStore struct {
	mu          sync.Mutex
	required    bool
	digest      [sha256.Size]byte
	auth        *fakeAuthStore
	claims      int
	requiredErr error
	usableErr   error
	claimErr    error
}

func (f *fakeInstallationSetupStore) InstallationSetupRequired(context.Context) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.required, f.requiredErr
}

func (f *fakeInstallationSetupStore) InstallationBootstrapTokenUsable(
	_ context.Context, digest []byte,
) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.required && string(digest) == string(f.digest[:]), f.usableErr
}

func (f *fakeInstallationSetupStore) ClaimInstallationBootstrap(
	_ context.Context, digest []byte, email, passwordHash string,
	sessionHash []byte, sessionExpiresAt time.Time,
) (*types.User, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.claimErr != nil {
		return nil, f.claimErr
	}
	if !f.required || string(digest) != string(f.digest[:]) {
		return nil, types.NewAppError(types.CodeValidation,
			"初始化令牌无效或已失效", nil)
	}
	f.claims++
	f.required = false
	f.auth.mu.Lock()
	defer f.auth.mu.Unlock()
	f.auth.nextUID++
	normalized := norm(email)
	u := &types.User{ID: f.auth.nextUID, Email: &normalized, PasswordHash: &passwordHash}
	f.auth.users[normalized] = u
	f.auth.members[u.ID] = []types.Membership{{
		TenantID: int64(types.SingleTenantID), UserID: u.ID,
		Role: types.MembershipRoleOwner,
	}}
	f.auth.sessions[string(sessionHash)] = &types.Session{
		TokenHash: sessionHash, UserID: u.ID,
		TenantID: int64(types.SingleTenantID), Role: types.MembershipRoleOwner,
		ActorType: types.ActorTypeUser, ExpiresAt: sessionExpiresAt,
	}
	return u, nil
}

func TestInstallationSetupStatusAndClaim(t *testing.T) {
	token := "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFG"
	authStore := newFakeAuthStore()
	setup := &fakeInstallationSetupStore{
		required: true, digest: sha256.Sum256([]byte(token)), auth: authStore,
	}
	claimed := 0
	mux := http.NewServeMux()
	MountInstallationSetup(mux, Deps{
		Auth: authStore, InstallationSetup: setup,
		SetupClaimed: func() { claimed++ },
	})

	statusReq := httptest.NewRequest(http.MethodGet, "/api/setup/status", nil)
	statusRec := httptest.NewRecorder()
	mux.ServeHTTP(statusRec, statusReq)
	if statusRec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", statusRec.Code, statusRec.Body.String())
	}
	var status map[string]any
	if err := json.Unmarshal(statusRec.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if status["state"] != "setup_required" || status["setup_required"] != true {
		t.Fatalf("unexpected status: %#v", status)
	}
	login := postJSON(t, mux, "/api/auth/login", map[string]string{
		"email": "owner@example.com", "password": "a-secure-password",
	}, nil)
	if login.Code != http.StatusNotFound {
		t.Fatalf("minimal setup mode exposed login/runtime route: %d", login.Code)
	}

	bad := postJSON(t, mux, "/api/setup/claim", map[string]string{
		"token": "abcdefghijklmnopqrstuvwxyz0123456789XXXXXXX",
		"email": "owner@example.com", "password": "a-secure-password",
	}, nil)
	if bad.Code != http.StatusBadRequest || setup.claims != 0 || authStore.sessionCount() != 0 {
		t.Fatalf("bad token crossed claim/hash boundary: status=%d claims=%d sessions=%d body=%s",
			bad.Code, setup.claims, authStore.sessionCount(), bad.Body.String())
	}

	good := postJSON(t, mux, "/api/setup/claim", map[string]string{
		"token": token, "email": "Owner@Example.com", "password": "a-secure-password",
	}, nil)
	if good.Code != http.StatusOK || setup.claims != 1 || claimed != 1 {
		t.Fatalf("claim failed: status=%d claims=%d callback=%d body=%s",
			good.Code, setup.claims, claimed, good.Body.String())
	}
	_ = sessionCookieFrom(t, good)

	statusRec = httptest.NewRecorder()
	mux.ServeHTTP(statusRec, statusReq)
	if statusRec.Code != http.StatusOK ||
		!jsonBodyContains(statusRec.Body.Bytes(), `"state":"active"`) {
		t.Fatalf("claimed install did not become active: %s", statusRec.Body.String())
	}
	replay := postJSON(t, mux, "/api/setup/claim", map[string]string{
		"token": token, "email": "other@example.com", "password": "a-secure-password",
	}, nil)
	if replay.Code != http.StatusBadRequest || setup.claims != 1 {
		t.Fatalf("consumed token replayed: status=%d claims=%d", replay.Code, setup.claims)
	}
}

func jsonBodyContains(raw []byte, want string) bool {
	var compact any
	if json.Unmarshal(raw, &compact) != nil {
		return false
	}
	encoded, _ := json.Marshal(compact)
	return string(encoded) == `{"setup_required":false,"state":"active"}` ||
		string(encoded) == `{"state":"active","setup_required":false}` || string(encoded) == want
}

func TestInstallationSetupPathsArePublicButNarrow(t *testing.T) {
	if !isPublicAuthPath("/api/setup/status") || !isPublicAuthPath("/api/setup/claim") {
		t.Fatal("exact setup endpoints must be public")
	}
	for _, path := range []string{
		"/api/setup", "/api/setup/claim/extra", "/api/admin/llm/credentials",
	} {
		if isPublicAuthPath(path) {
			t.Fatalf("path %q was accidentally made public", path)
		}
	}
}

func TestInstallationSetupFailureAndValidationBoundaries(t *testing.T) {
	token := "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFG"
	valid := map[string]string{
		"token": token, "email": "owner@example.com", "password": "a-secure-password",
	}
	request := func(mux http.Handler, method, path string, body []byte, origin string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, bytes.NewReader(body))
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		return rec
	}
	marshal := func(value any) []byte {
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}

	nilMux := http.NewServeMux()
	MountInstallationSetup(nilMux, Deps{})
	if got := request(nilMux, http.MethodGet, "/api/setup/status", nil, ""); got.Code != http.StatusServiceUnavailable {
		t.Fatalf("nil status=%d body=%s", got.Code, got.Body.String())
	}
	if got := request(nilMux, http.MethodPost, "/api/setup/claim", marshal(valid), ""); got.Code != http.StatusServiceUnavailable {
		t.Fatalf("nil claim=%d body=%s", got.Code, got.Body.String())
	}

	authStore := newFakeAuthStore()
	setup := &fakeInstallationSetupStore{
		required: true, digest: sha256.Sum256([]byte(token)), auth: authStore,
		requiredErr: types.NewAppError(types.CodeDatabase, "status failed", errors.New("db")),
	}
	mux := http.NewServeMux()
	MountInstallationSetup(mux, Deps{
		Auth: authStore, InstallationSetup: setup, Origin: "https://vane.example",
	})
	if got := request(mux, http.MethodGet, "/api/setup/status", nil, ""); got.Code != http.StatusInternalServerError {
		t.Fatalf("status error=%d body=%s", got.Code, got.Body.String())
	}
	setup.requiredErr = nil
	if got := request(mux, http.MethodPost, "/api/setup/claim", marshal(valid), "https://evil.example"); got.Code != http.StatusForbidden {
		t.Fatalf("cross-origin=%d body=%s", got.Code, got.Body.String())
	}
	if got := request(mux, http.MethodPost, "/api/setup/claim", []byte("{"), "https://vane.example"); got.Code != http.StatusBadRequest {
		t.Fatalf("malformed json=%d body=%s", got.Code, got.Body.String())
	}
	for name, mutate := range map[string]func(map[string]string){
		"short token":  func(v map[string]string) { v["token"] = "short" },
		"spaced token": func(v map[string]string) { v["token"] = strings.Repeat("x", 32) + " x" },
		"long token":   func(v map[string]string) { v["token"] = strings.Repeat("x", installationTokenMaxLen+1) },
		"bad email":    func(v map[string]string) { v["email"] = "owner" },
		"bad password": func(v map[string]string) { v["password"] = "short" },
	} {
		t.Run(name, func(t *testing.T) {
			value := map[string]string{}
			for key, item := range valid {
				value[key] = item
			}
			mutate(value)
			got := request(mux, http.MethodPost, "/api/setup/claim", marshal(value), "https://vane.example")
			if got.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s", got.Code, got.Body.String())
			}
		})
	}
	setup.usableErr = types.NewAppError(types.CodeDatabase, "usable failed", errors.New("db"))
	if got := request(mux, http.MethodPost, "/api/setup/claim", marshal(valid), "https://vane.example"); got.Code != http.StatusInternalServerError {
		t.Fatalf("usable error=%d body=%s", got.Code, got.Body.String())
	}
	setup.usableErr = nil
	setup.claimErr = types.NewAppError(types.CodeConflict, "claim lost", nil)
	if got := request(mux, http.MethodPost, "/api/setup/claim", marshal(valid), "https://vane.example"); got.Code != http.StatusConflict {
		t.Fatalf("claim error=%d body=%s", got.Code, got.Body.String())
	}
}

func TestInstallationSetupRateLimit(t *testing.T) {
	token := "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFG"
	authStore := newFakeAuthStore()
	setup := &fakeInstallationSetupStore{
		required: true, digest: sha256.Sum256([]byte(token)), auth: authStore,
	}
	s := &server{deps: Deps{Auth: authStore, InstallationSetup: setup}, limiter: newAuthLimiter()}
	s.limiter.max = 1
	body, _ := json.Marshal(map[string]string{
		"token": token, "email": "owner@example.com", "password": "a-secure-password",
	})
	badBody, _ := json.Marshal(map[string]string{
		"token": "short", "email": "owner@example.com", "password": "a-secure-password",
	})
	req := httptest.NewRequest(http.MethodPost, "/api/setup/claim", bytes.NewReader(badBody))
	rec := httptest.NewRecorder()
	s.handleInstallationSetupClaim(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("first attempt=%d body=%s", rec.Code, rec.Body.String())
	}
	req = httptest.NewRequest(http.MethodPost, "/api/setup/claim", bytes.NewReader(body))
	rec = httptest.NewRecorder()
	s.handleInstallationSetupClaim(rec, req)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("rate limit=%d body=%s", rec.Code, rec.Body.String())
	}
}
