package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/YouToco/vane/server/auth"
	"github.com/YouToco/vane/server/mailer"
	"github.com/YouToco/vane/server/store"
)

type fakeAccountSecurity struct {
	identity         *store.AccountSecurityIdentity
	verificationHash []byte
	resetEmail       string
	reauthSession    []byte
	reauthProof      []byte
	logoutCalled     bool
}

func (f *fakeAccountSecurity) GetAccountSecurityIdentity(context.Context, int64, int64) (*store.AccountSecurityIdentity, error) {
	return f.identity, nil
}
func (f *fakeAccountSecurity) IssueEmailVerification(_ context.Context, _, _ int64, hash []byte, _ time.Time) (string, bool, error) {
	f.verificationHash = bytes.Clone(hash)
	return f.identity.Email, true, nil
}
func (f *fakeAccountSecurity) IssuePasswordReset(_ context.Context, email string, _ []byte, _ time.Time) (string, bool, error) {
	f.resetEmail = email
	if strings.EqualFold(strings.TrimSpace(email), "known@example.com") {
		return "known@example.com", true, nil
	}
	return "", false, nil
}
func (f *fakeAccountSecurity) VerifyEmailWithToken(context.Context, []byte) error { return nil }
func (f *fakeAccountSecurity) PasswordResetTokenUsable(context.Context, []byte) (bool, error) {
	return false, nil
}
func (f *fakeAccountSecurity) ResetPasswordWithToken(context.Context, []byte, string) error {
	return nil
}
func (f *fakeAccountSecurity) IssueReauthProof(_ context.Context, _, _ int64, sessionHash, proofHash []byte, _ time.Time) error {
	f.reauthSession = bytes.Clone(sessionHash)
	f.reauthProof = bytes.Clone(proofHash)
	return nil
}
func (f *fakeAccountSecurity) LogoutAllWithReauth(_ context.Context, _, _ int64, sessionHash, proofHash []byte) (int64, error) {
	if bytes.Equal(sessionHash, f.reauthSession) && bytes.Equal(proofHash, f.reauthProof) {
		f.logoutCalled = true
	}
	return 2, nil
}

type fakeSecurityMailer struct{ messages []mailer.Message }

func (f *fakeSecurityMailer) Send(_ context.Context, message mailer.Message) error {
	f.messages = append(f.messages, message)
	return nil
}

func TestAccountSecurityPublicAndProtectedRouteMutation(t *testing.T) {
	authStore := newFakeAuthStore()
	security := &fakeAccountSecurity{}
	mux := http.NewServeMux()
	Mount(mux, Deps{Auth: authStore, AccountSecurity: security,
		SecurityMailer: &fakeSecurityMailer{}, Principal: auth.NewContextResolver()})

	if rec := postJSON(t, mux, "/api/auth/password-reset/request",
		map[string]string{"email": "missing@example.com"}, nil); rec.Code != http.StatusAccepted {
		t.Fatalf("password reset request must remain public, got %d", rec.Code)
	}
	if rec := postJSON(t, mux, "/api/auth/email-verification/verify",
		map[string]string{"token": "invalid"}, nil); rec.Code != http.StatusBadRequest {
		t.Fatalf("email verification completion must be public, got %d", rec.Code)
	}
	if rec := postJSON(t, mux, "/api/auth/password-reset/complete",
		map[string]string{"token": "invalid", "password": "new-password-123"}, nil); rec.Code != http.StatusBadRequest {
		t.Fatalf("password reset completion must be public, got %d", rec.Code)
	}
	if rec := postJSON(t, mux, "/api/auth/email-verification/request", nil, nil); rec.Code != http.StatusUnauthorized {
		t.Fatalf("verification issuance must require session, got %d", rec.Code)
	}
}

func TestEmailVerificationPersistsOnlyHashAndMailsRawToken(t *testing.T) {
	authStore := newFakeAuthStore()
	user := authStore.addUser(t, "verify@example.com", "verify-password-123", 7)
	security := &fakeAccountSecurity{identity: &store.AccountSecurityIdentity{
		UserID: user.ID, TenantID: 7, Email: "verify@example.com",
		PasswordHash: *user.PasswordHash,
	}}
	mail := &fakeSecurityMailer{}
	mux := http.NewServeMux()
	Mount(mux, Deps{Auth: authStore, AccountSecurity: security,
		SecurityMailer: mail, Principal: auth.NewContextResolver(), Origin: "https://vane.example"})
	login := postJSON(t, mux, "/api/auth/login", map[string]string{
		"email": "verify@example.com", "password": "verify-password-123",
	}, nil)
	cookie := sessionCookieFrom(t, login)
	rec := postJSON(t, mux, "/api/auth/email-verification/request", map[string]any{}, cookie)
	if rec.Code != http.StatusAccepted || len(mail.messages) != 1 {
		t.Fatalf("verification request: status=%d mail=%d body=%s", rec.Code, len(mail.messages), rec.Body.String())
	}
	marker := "token="
	idx := strings.Index(mail.messages[0].Text, marker)
	if idx < 0 {
		t.Fatalf("mail has no token link: %q", mail.messages[0].Text)
	}
	raw := strings.TrimSpace(mail.messages[0].Text[idx+len(marker):])
	if !bytes.Equal(auth.HashSessionToken(raw), security.verificationHash) {
		t.Fatal("stored digest does not match mailed one-time token")
	}
	if bytes.Contains(security.verificationHash, []byte(raw)) {
		t.Fatal("raw token leaked into persistence argument")
	}
}

func TestReauthProofIsBoundToCurrentSessionForLogoutAll(t *testing.T) {
	authStore := newFakeAuthStore()
	user := authStore.addUser(t, "reauth@example.com", "reauth-password-123", 9)
	security := &fakeAccountSecurity{identity: &store.AccountSecurityIdentity{
		UserID: user.ID, TenantID: 9, Email: "reauth@example.com",
		PasswordHash: *user.PasswordHash,
	}}
	mux := http.NewServeMux()
	Mount(mux, Deps{Auth: authStore, AccountSecurity: security,
		SecurityMailer: &fakeSecurityMailer{}, Principal: auth.NewContextResolver()})
	login := postJSON(t, mux, "/api/auth/login", map[string]string{
		"email": "reauth@example.com", "password": "reauth-password-123",
	}, nil)
	cookie := sessionCookieFrom(t, login)
	reauth := postJSON(t, mux, "/api/auth/reauth",
		map[string]string{"password": "reauth-password-123"}, cookie)
	if reauth.Code != http.StatusOK {
		t.Fatalf("reauth status=%d body=%s", reauth.Code, reauth.Body.String())
	}
	var body struct {
		Proof string `json:"proof"`
	}
	if err := json.Unmarshal(reauth.Body.Bytes(), &body); err != nil || body.Proof == "" {
		t.Fatalf("decode proof: %+v err=%v", body, err)
	}
	raw, _ := json.Marshal(map[string]any{})
	req := httptest.NewRequest(http.MethodPost, "/api/auth/logout-all", bytes.NewReader(raw))
	req.AddCookie(cookie)
	req.Header.Set(reauthHeaderName, body.Proof)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !security.logoutCalled {
		t.Fatalf("logout all status=%d called=%v body=%s", rec.Code, security.logoutCalled, rec.Body.String())
	}
}
