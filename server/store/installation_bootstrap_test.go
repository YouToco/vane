package store

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/server/types"
)

func TestInstallationBootstrapAtomicClaimAndReplayFence(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		requireDatabaseCapability(t)
	}
	scratchURL, drop := createScratchDB(t.Context(), t, databaseURL)
	t.Cleanup(drop)
	t.Setenv("DATABASE_URL", scratchURL)
	st := tenantTestStore(t)
	ctx := t.Context()
	prefix := fmt.Sprintf("bootstrap-%d", time.Now().UnixNano())
	emails := []string{prefix + "-a@example.com", prefix + "-b@example.com"}

	required, err := st.InstallationSetupRequired(ctx)
	if err != nil || !required {
		t.Fatalf("fresh tenant 1 must require setup: required=%v err=%v", required, err)
	}
	token := sha256.Sum256([]byte("host-local-one-time-token-aaaaaaaa"))
	prepared, err := st.PrepareInstallationBootstrap(
		ctx, token[:], time.Now().Add(5*time.Minute))
	if err != nil || !prepared.SetupRequired || !prepared.TokenAccepted {
		t.Fatalf("prepare failed: %+v err=%v", prepared, err)
	}
	replayed, err := st.PrepareInstallationBootstrap(
		ctx, token[:], time.Now().Add(30*time.Minute))
	if err != nil || !replayed.TokenAccepted || !replayed.ExpiresAt.Equal(prepared.ExpiresAt) {
		t.Fatalf("same host token must replay without extending TTL: first=%+v replay=%+v err=%v",
			prepared, replayed, err)
	}
	wrong := sha256.Sum256([]byte("host-local-one-time-token-bbbbbbbb"))
	if usable, err := st.InstallationBootstrapTokenUsable(ctx, wrong[:]); err != nil || usable {
		t.Fatalf("wrong token became usable: usable=%v err=%v", usable, err)
	}
	if usable, err := st.InstallationBootstrapTokenUsable(ctx, token[:]); err != nil || !usable {
		t.Fatalf("prepared token is not usable: usable=%v err=%v", usable, err)
	}
	// Force the last write in the claim transaction to fail. User, membership,
	// and token consumption must all roll back with the colliding session.
	fixtureUser, err := st.UpsertUserByOpenID(ctx,
		fmt.Sprintf("bootstrap-session-fixture-%d", time.Now().UnixNano()), "fixture")
	if err != nil {
		t.Fatal(err)
	}
	collidingSession := sha256.Sum256([]byte("colliding-bootstrap-session"))
	if _, err := st.pool.Exec(ctx, `
		INSERT INTO user_sessions(token_hash,user_id,tenant_id,expires_at)
		VALUES($1,$2,$3,now()+interval '1 hour')`, collidingSession[:],
		fixtureUser.ID, int64(types.SingleTenantID)); err != nil {
		t.Fatal(err)
	}
	rollbackEmail := prefix + "-rollback@example.com"
	if _, err := st.ClaimInstallationBootstrap(ctx, token[:], rollbackEmail,
		"$argon2id$bootstrap-test", collidingSession[:], time.Now().Add(time.Hour)); err == nil {
		t.Fatal("session collision must fail the claim")
	}
	if required, err := st.InstallationSetupRequired(ctx); err != nil || !required {
		t.Fatalf("failed claim leaked owner membership: required=%v err=%v", required, err)
	}
	if _, err := st.GetUserByEmail(ctx, rollbackEmail); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("failed claim leaked user: %v", err)
	}
	if usable, err := st.InstallationBootstrapTokenUsable(ctx, token[:]); err != nil || !usable {
		t.Fatalf("failed claim consumed token: usable=%v err=%v", usable, err)
	}
	if _, err := st.pool.Exec(ctx,
		`DELETE FROM user_sessions WHERE token_hash=$1`, collidingSession[:]); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, email := range emails {
		email := email
		wg.Add(1)
		go func() {
			defer wg.Done()
			sessionHash := sha256.Sum256([]byte("session-" + email))
			_, claimErr := st.ClaimInstallationBootstrap(
				ctx, token[:], email, "$argon2id$bootstrap-test",
				sessionHash[:], time.Now().Add(time.Hour))
			errs <- claimErr
		}()
	}
	wg.Wait()
	close(errs)
	successes := 0
	for claimErr := range errs {
		if claimErr == nil {
			successes++
			continue
		}
		if !errors.Is(claimErr, types.ErrConflict) {
			t.Fatalf("losing claim must observe installed conflict: %v", claimErr)
		}
	}
	if successes != 1 {
		t.Fatalf("exactly one concurrent claim must win, got %d", successes)
	}
	var ownerCount, sessionCount int
	if err := st.pool.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM memberships WHERE tenant_id=$1 AND role=$2),
		(SELECT count(*) FROM user_sessions WHERE tenant_id=$1)`,
		int64(types.SingleTenantID), types.MembershipRoleOwner).Scan(
		&ownerCount, &sessionCount); err != nil {
		t.Fatal(err)
	}
	if ownerCount != 1 || sessionCount != 1 {
		t.Fatalf("claim must atomically create one owner and one session: owners=%d sessions=%d",
			ownerCount, sessionCount)
	}
	if required, err := st.InstallationSetupRequired(ctx); err != nil || required {
		t.Fatalf("claimed installation reopened setup: required=%v err=%v", required, err)
	}
	if usable, err := st.InstallationBootstrapTokenUsable(ctx, token[:]); err != nil || usable {
		t.Fatalf("consumed token remained usable: usable=%v err=%v", usable, err)
	}
	raw, err := st.GetSetting(ctx, installationBootstrapSettingKey)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "token_sha256") {
		t.Fatalf("claimed receipt retained token digest: %s", raw)
	}
	postClaim, err := st.PrepareInstallationBootstrap(
		ctx, wrong[:], time.Now().Add(5*time.Minute))
	if err != nil || postClaim.SetupRequired {
		t.Fatalf("startup must not reopen claimed installation: %+v err=%v", postClaim, err)
	}
}

func TestInstallationBootstrapRejectsExpiredCorruptAndUnavailableAuthority(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		requireDatabaseCapability(t)
	}
	scratchURL, drop := createScratchDB(t.Context(), t, databaseURL)
	t.Cleanup(drop)
	t.Setenv("DATABASE_URL", scratchURL)
	st := tenantTestStore(t)
	ctx := t.Context()
	tokenA := sha256.Sum256([]byte("bootstrap-boundary-token-aaaaaaaa"))
	tokenB := sha256.Sum256([]byte("bootstrap-boundary-token-bbbbbbbb"))
	session := sha256.Sum256([]byte("bootstrap-boundary-session"))

	if _, err := st.PrepareInstallationBootstrap(ctx, tokenA[:4], time.Now().Add(time.Hour)); types.CodeOf(err) != types.CodeValidation {
		t.Fatalf("short prepare digest=%v", err)
	}
	if _, err := st.PrepareInstallationBootstrap(ctx, tokenA[:], time.Now().Add(-time.Second)); types.CodeOf(err) != types.CodeValidation {
		t.Fatalf("expired prepare=%v", err)
	}
	if usable, err := st.InstallationBootstrapTokenUsable(ctx, tokenA[:4]); err != nil || usable {
		t.Fatalf("short usable digest=%v err=%v", usable, err)
	}
	if usable, err := st.InstallationBootstrapTokenUsable(ctx, tokenA[:]); err != nil || usable {
		t.Fatalf("missing authority usable=%v err=%v", usable, err)
	}
	if _, err := st.ClaimInstallationBootstrap(ctx, tokenA[:4], "owner@example.com",
		"hash", session[:], time.Now().Add(time.Hour)); types.CodeOf(err) != types.CodeValidation {
		t.Fatalf("invalid claim=%v", err)
	}

	prepared, err := st.PrepareInstallationBootstrap(ctx, tokenA[:], time.Now().Add(time.Hour))
	if err != nil || !prepared.TokenAccepted {
		t.Fatalf("prepare=%+v err=%v", prepared, err)
	}
	if _, err := st.pool.Exec(ctx, `UPDATE settings
		SET value=jsonb_set(value::jsonb, '{expires_at}', to_jsonb($2::text))
		WHERE key=$1`, installationBootstrapSettingKey,
		time.Now().Add(-time.Minute).UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	expired, err := st.PrepareInstallationBootstrap(ctx, tokenA[:], time.Now().Add(time.Hour))
	if err != nil || expired.TokenAccepted || !expired.SetupRequired {
		t.Fatalf("expired replay=%+v err=%v", expired, err)
	}
	rotated, err := st.PrepareInstallationBootstrap(ctx, tokenB[:], time.Now().Add(time.Hour))
	if err != nil || !rotated.TokenAccepted {
		t.Fatalf("rotated=%+v err=%v", rotated, err)
	}

	if _, err := st.pool.Exec(ctx, `UPDATE settings SET value='{}'::jsonb WHERE key=$1`,
		installationBootstrapSettingKey); err != nil {
		t.Fatal(err)
	}
	if usable, err := st.InstallationBootstrapTokenUsable(ctx, tokenB[:]); err != nil || usable {
		t.Fatalf("wrong-shape authority usable=%v err=%v", usable, err)
	}
	if _, err := st.pool.Exec(ctx, `UPDATE settings SET value='"bad"'::jsonb WHERE key=$1`,
		installationBootstrapSettingKey); err != nil {
		t.Fatal(err)
	}
	if _, err := st.InstallationBootstrapTokenUsable(ctx, tokenB[:]); types.CodeOf(err) != types.CodeDatabase {
		t.Fatalf("corrupt usable authority=%v", err)
	}
	if _, err := st.PrepareInstallationBootstrap(ctx, tokenB[:], time.Now().Add(time.Hour)); types.CodeOf(err) != types.CodeDatabase {
		t.Fatalf("corrupt prepare authority=%v", err)
	}
	if _, err := st.pool.Exec(ctx, `DELETE FROM settings WHERE key=$1`,
		installationBootstrapSettingKey); err != nil {
		t.Fatal(err)
	}
	if _, err := st.PrepareInstallationBootstrap(ctx, tokenB[:], time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	if _, err := st.ClaimInstallationBootstrap(ctx, tokenA[:], "owner@example.com",
		"hash", session[:], time.Now().Add(time.Hour)); types.CodeOf(err) != types.CodeValidation {
		t.Fatalf("wrong claim token=%v", err)
	}
	if _, err := st.pool.Exec(ctx, `UPDATE tenants SET status='suspended' WHERE id=$1`,
		int64(types.SingleTenantID)); err != nil {
		t.Fatal(err)
	}
	if _, err := st.ClaimInstallationBootstrap(ctx, tokenB[:], "owner@example.com",
		"hash", session[:], time.Now().Add(time.Hour)); types.CodeOf(err) != types.CodeConflict {
		t.Fatalf("suspended tenant claim=%v", err)
	}
	if _, err := st.pool.Exec(ctx, `UPDATE tenants SET status='active' WHERE id=$1`,
		int64(types.SingleTenantID)); err != nil {
		t.Fatal(err)
	}
	existingEmail := fmt.Sprintf("bootstrap-existing-%d@example.com", time.Now().UnixNano())
	if _, err := st.pool.Exec(ctx, `INSERT INTO users(email,password_hash,name) VALUES($1,'hash','')`,
		existingEmail); err != nil {
		t.Fatal(err)
	}
	if _, err := st.ClaimInstallationBootstrap(ctx, tokenB[:], existingEmail,
		"hash", session[:], time.Now().Add(time.Hour)); types.CodeOf(err) != types.CodeConflict {
		t.Fatalf("duplicate owner email claim=%v", err)
	}
	if _, err := st.pool.Exec(ctx, `DELETE FROM users WHERE email=$1`, existingEmail); err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(ctx, `DELETE FROM settings WHERE key=$1`,
		installationBootstrapSettingKey); err != nil {
		t.Fatal(err)
	}
	if _, err := st.ClaimInstallationBootstrap(ctx, tokenB[:], "owner@example.com",
		"hash", session[:], time.Now().Add(time.Hour)); types.CodeOf(err) != types.CodeValidation {
		t.Fatalf("missing claim authority=%v", err)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := st.InstallationSetupRequired(canceled); err == nil {
		t.Fatal("canceled status query must fail")
	}
}

func TestInstallationBootstrapDatabaseFailuresFailClosed(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		requireDatabaseCapability(t)
	}
	scratchURL, drop := createScratchDB(t.Context(), t, databaseURL)
	t.Cleanup(drop)
	t.Setenv("DATABASE_URL", scratchURL)
	st := tenantTestStore(t)
	ctx := t.Context()
	token := sha256.Sum256([]byte("bootstrap-fault-token-aaaaaaaaaa"))

	beginFailure := *st
	beginFailure.beginTx = func(context.Context, pgx.TxOptions) (pgx.Tx, error) {
		return nil, errInjectedCompiledTask
	}
	if _, err := beginFailure.PrepareInstallationBootstrap(
		ctx, token[:], time.Now().Add(time.Hour)); types.CodeOf(err) != types.CodeDatabase {
		t.Fatalf("prepare begin failure=%v", err)
	}
	session := sha256.Sum256([]byte("bootstrap-fault-session-begin"))
	if _, err := beginFailure.ClaimInstallationBootstrap(ctx, token[:],
		"begin@example.com", "hash", session[:], time.Now().Add(time.Hour)); types.CodeOf(err) != types.CodeDatabase {
		t.Fatalf("claim begin failure=%v", err)
	}

	faultStore := func(needle string, commitErr error) *Store {
		copyStore := *st
		copyStore.beginTx = func(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error) {
			realTx, err := st.pool.BeginTx(ctx, opts)
			if err != nil {
				return nil, err
			}
			return &compiledTaskFaultTx{
				Tx: realTx, failContains: needle, commitErr: commitErr,
			}, nil
		}
		return &copyStore
	}
	for _, needle := range []string{
		"pg_advisory_xact_lock", "SELECT EXISTS", "clock_timestamp()",
		"SELECT value", "INSERT INTO settings",
	} {
		if _, err := faultStore(needle, nil).PrepareInstallationBootstrap(
			ctx, token[:], time.Now().Add(time.Hour)); types.CodeOf(err) != types.CodeDatabase {
			t.Fatalf("prepare fault %q=%v", needle, err)
		}
	}
	if _, err := faultStore("", errInjectedCompiledTask).PrepareInstallationBootstrap(
		ctx, token[:], time.Now().Add(time.Hour)); types.CodeOf(err) != types.CodeDatabase {
		t.Fatalf("prepare commit failure=%v", err)
	}
	if _, err := st.PrepareInstallationBootstrap(ctx, token[:], time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := faultStore("", errInjectedCompiledTask).PrepareInstallationBootstrap(
		ctx, token[:], time.Now().Add(time.Hour)); types.CodeOf(err) != types.CodeDatabase {
		t.Fatalf("prepare replay commit failure=%v", err)
	}

	claimFaults := []struct {
		name   string
		needle string
	}{
		{name: "advisory", needle: "pg_advisory_xact_lock"},
		{name: "owner", needle: "SELECT EXISTS"},
		{name: "tenant", needle: "SELECT status FROM tenants"},
		{name: "authority", needle: "SELECT value, clock_timestamp()"},
		{name: "user", needle: "INSERT INTO users"},
		{name: "membership", needle: "INSERT INTO memberships"},
		{name: "session", needle: "INSERT INTO user_sessions"},
		{name: "consume", needle: "UPDATE settings"},
	}
	for index, tc := range claimFaults {
		t.Run(tc.name, func(t *testing.T) {
			session := sha256.Sum256([]byte(fmt.Sprintf("fault-session-%d", index)))
			_, err := faultStore(tc.needle, nil).ClaimInstallationBootstrap(
				ctx, token[:], fmt.Sprintf("fault-%d@example.com", index),
				"hash", session[:], time.Now().Add(time.Hour))
			if types.CodeOf(err) != types.CodeDatabase {
				t.Fatalf("claim fault %q=%v", tc.needle, err)
			}
		})
	}
	commitSession := sha256.Sum256([]byte("fault-session-commit"))
	if _, err := faultStore("", errInjectedCompiledTask).ClaimInstallationBootstrap(
		ctx, token[:], "commit@example.com", "hash", commitSession[:],
		time.Now().Add(time.Hour)); types.CodeOf(err) != types.CodeDatabase {
		t.Fatalf("claim commit failure=%v", err)
	}
}
