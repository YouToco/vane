package store

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/YouToco/vane/server/types"
)

func accountSecurityHash(label string) []byte {
	sum := sha256.Sum256([]byte(label))
	return sum[:]
}

func TestAccountSecurityLifecyclePostgres(t *testing.T) {
	st := workspaceIntegrationStore(t)
	ctx := t.Context()
	if err := migrate(ctx, st.pool.Config().ConnString(), 0); err != nil {
		t.Fatalf("migrate latest account security schema: %v", err)
	}
	user, tenant := workspaceTestAccount(t, st, "account-security")

	verificationHash := accountSecurityHash(fmt.Sprintf("verify-%d", time.Now().UnixNano()))
	email, issued, err := st.IssueEmailVerification(ctx, tenant.ID, user.ID,
		verificationHash, time.Now().Add(time.Hour))
	if err != nil || !issued || email != *user.Email {
		t.Fatalf("issue verification: email=%q issued=%v err=%v", email, issued, err)
	}
	if err := st.VerifyEmailWithToken(ctx, verificationHash); err != nil {
		t.Fatalf("verify email: %v", err)
	}
	if err := st.VerifyEmailWithToken(ctx, verificationHash); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("verification replay must fail closed, got %v", err)
	}
	identity, err := st.GetAccountSecurityIdentity(ctx, tenant.ID, user.ID)
	if err != nil || !identity.EmailVerified {
		t.Fatalf("email was not marked verified: %+v err=%v", identity, err)
	}

	sessionHash := accountSecurityHash(fmt.Sprintf("session-%d", time.Now().UnixNano()))
	if err := st.CreateSession(ctx, sessionHash, user.ID, tenant.ID, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	resetHash := accountSecurityHash(fmt.Sprintf("reset-%d", time.Now().UnixNano()))
	_, issued, err = st.IssuePasswordReset(ctx, *user.Email, resetHash, time.Now().Add(time.Hour))
	if err != nil || !issued {
		t.Fatalf("issue reset: issued=%v err=%v", issued, err)
	}
	usable, err := st.PasswordResetTokenUsable(ctx, resetHash)
	if err != nil || !usable {
		t.Fatalf("reset preflight: usable=%v err=%v", usable, err)
	}
	if err := st.ResetPasswordWithToken(ctx, resetHash, "replacement-password-hash"); err != nil {
		t.Fatalf("reset password: %v", err)
	}
	if _, err := st.LookupSession(ctx, sessionHash); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("password reset must revoke all sessions, got %v", err)
	}
	if err := st.ResetPasswordWithToken(ctx, resetHash, "replay-hash"); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("reset replay must fail closed, got %v", err)
	}

	currentSession := accountSecurityHash(fmt.Sprintf("current-%d", time.Now().UnixNano()))
	otherSession := accountSecurityHash(fmt.Sprintf("other-%d", time.Now().UnixNano()))
	if err := st.CreateSession(ctx, currentSession, user.ID, tenant.ID, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := st.CreateSession(ctx, otherSession, user.ID, tenant.ID, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	proofHash := accountSecurityHash(fmt.Sprintf("proof-%d", time.Now().UnixNano()))
	if err := st.IssueReauthProof(ctx, tenant.ID, user.ID, currentSession, proofHash,
		time.Now().Add(time.Minute)); err != nil {
		t.Fatalf("issue reauth: %v", err)
	}
	count, err := st.LogoutAllWithReauth(ctx, tenant.ID, user.ID, currentSession, proofHash)
	if err != nil || count != 2 {
		t.Fatalf("logout all: count=%d err=%v", count, err)
	}
	if _, err := st.LogoutAllWithReauth(ctx, tenant.ID, user.ID, currentSession, proofHash); !errors.Is(err, types.ErrForbidden) {
		t.Fatalf("reauth replay must fail closed, got %v", err)
	}
}

func TestAccountSecurityForceRLSPostgres(t *testing.T) {
	st := workspaceIntegrationStore(t)
	ctx := t.Context()
	if err := migrate(ctx, st.pool.Config().ConnString(), 0); err != nil {
		t.Fatalf("migrate latest account security schema: %v", err)
	}
	user, tenant := workspaceTestAccount(t, st, "account-security-rls")
	hash := accountSecurityHash(fmt.Sprintf("rls-%d", time.Now().UnixNano()))
	if _, _, err := st.IssueEmailVerification(ctx, tenant.ID, user.ID, hash,
		time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	tx, err := st.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_app`); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM account_security_tokens`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("unscoped account tokens leaked %d rows", count)
	}
	if err := setAccountSecurityScope(ctx, tx, tenant.ID, user.ID); err != nil {
		t.Fatal(err)
	}
	if err := tx.QueryRow(ctx,
		`SELECT count(*) FROM account_security_tokens WHERE tenant_id=$1 AND user_id=$2`,
		tenant.ID, user.ID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("exact scope expected one token, got %d", count)
	}
}
