package store

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/YouToco/vane/server/types"
)

func configureTestCredentialVault(t *testing.T, st *Store) {
	t.Helper()
	if err := st.ConfigureCredentialVault(
		"test-key-1", strings.Repeat("42", 32),
		"test-key-0="+strings.Repeat("24", 32)); err != nil {
		t.Fatal(err)
	}
}

func credentialVaultTestStore(t *testing.T) (*Store, *sql.DB) {
	t.Helper()
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		requireDatabaseCapability(t)
	}
	database, provider, scratchURL, drop := migration128Scratch(t, databaseURL)
	t.Cleanup(drop)
	if _, err := provider.UpTo(t.Context(), latestMigrationVersion); err != nil {
		t.Fatal(err)
	}
	st, err := New(t.Context(), scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	registerStoreClose(t, st)
	configureTestCredentialVault(t, st)
	return st, database
}

func TestCredentialVaultTenantRotationRetentionAndAuthorization(t *testing.T) {
	st, database := credentialVaultTestStore(t)
	ctx := t.Context()
	ownerID, tenantID := migration129Identity(t, database, "credential-vault-owner")
	var memberID int64
	if err := database.QueryRowContext(ctx, `INSERT INTO users(feishu_open_id,name)
		VALUES($1,'member') RETURNING id`,
		fmt.Sprintf("ou_credential_vault_member_%d", time.Now().UnixNano()),
	).Scan(&memberID); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(ctx, `INSERT INTO memberships(tenant_id,user_id,role)
		VALUES($1,$2,'member')`, tenantID, memberID); err != nil {
		t.Fatal(err)
	}

	scope := CredentialScope{Kind: "tenant", TenantID: tenantID,
		Provider: "telegram", Purpose: fmt.Sprintf("bot_api_%d", time.Now().UnixNano())}
	firstSecret := json.RawMessage(`{"bot_token":"synthetic-first"}`)
	first, err := st.RotateCredential(ctx, scope, firstSecret,
		json.RawMessage(`{"bot_username":"test_bot"}`), ownerID)
	if err != nil {
		t.Fatal(err)
	}
	if first.Generation != 1 || first.Status != "active" || len(first.Fingerprint) != 64 {
		t.Fatalf("unexpected first metadata: %+v", first)
	}
	var storedCiphertext []byte
	if err := st.pool.QueryRow(ctx, `SELECT ciphertext FROM credential_vault_entries
		WHERE tenant_id=$1 AND provider=$2 AND purpose=$3 AND generation=1`,
		scope.TenantID, scope.Provider, scope.Purpose).Scan(&storedCiphertext); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(storedCiphertext, []byte("synthetic-first")) {
		t.Fatal("database ciphertext contains plaintext token")
	}
	if _, err := st.CredentialStatus(ctx, scope, memberID); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("member read err=%v, want hidden not-found", err)
	}
	if _, err := st.RotateCredential(ctx, scope,
		json.RawMessage(`{"bot_token":"stolen"}`), json.RawMessage(`{}`), memberID,
	); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("member rotate err=%v, want hidden not-found", err)
	}

	second, err := st.RotateCredential(ctx, scope,
		json.RawMessage(`{"bot_token":"synthetic-second"}`),
		json.RawMessage(`{"bot_username":"test_bot"}`), ownerID)
	if err != nil {
		t.Fatal(err)
	}
	if second.Generation != 2 || second.Fingerprint == first.Fingerprint {
		t.Fatalf("unexpected second metadata: %+v", second)
	}
	for generation, want := range map[int64]string{1: "synthetic-first", 2: "synthetic-second", 0: "synthetic-second"} {
		var got string
		err := st.UseCredential(ctx, scope, generation,
			func(secret []byte, _ CredentialMetadata) error {
				var payload struct {
					BotToken string `json:"bot_token"`
				}
				if err := json.Unmarshal(secret, &payload); err != nil {
					return err
				}
				got = payload.BotToken
				return nil
			})
		if err != nil || got != want {
			t.Fatalf("generation %d=(%q,%v), want %q", generation, got, err, want)
		}
	}
	var active, retired int
	if err := st.pool.QueryRow(ctx, `SELECT
		count(*) FILTER (WHERE status='active'),count(*) FILTER (WHERE status='retired')
		FROM credential_vault_entries WHERE tenant_id=$1 AND
		provider=$2 AND purpose=$3`, scope.TenantID, scope.Provider, scope.Purpose,
	).Scan(&active, &retired); err != nil {
		t.Fatal(err)
	}
	if active != 1 || retired != 1 {
		t.Fatalf("active=%d retired=%d", active, retired)
	}
}

func TestCredentialVaultConcurrentRotationHasOneActiveGeneration(t *testing.T) {
	st, database := credentialVaultTestStore(t)
	ctx := t.Context()
	var ownerID int64
	if err := database.QueryRowContext(ctx, `INSERT INTO users(feishu_open_id,name)
		VALUES($1,'platform-owner') RETURNING id`,
		"credential-vault-platform-owner-"+fmt.Sprint(time.Now().UnixNano()),
	).Scan(&ownerID); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(ctx, `INSERT INTO memberships(tenant_id,user_id,role)
		VALUES(1,$1,'owner')`, ownerID); err != nil {
		t.Fatal(err)
	}
	scope := CredentialScope{Kind: "platform", Provider: "llm",
		Purpose: fmt.Sprintf("primary_api_%d", time.Now().UnixNano())}
	const writers = 8
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			_, err := st.RotateCredential(ctx, scope,
				json.RawMessage(fmt.Sprintf(`{"api_key":"synthetic-%d"}`, index)),
				json.RawMessage(`{"provider":"openai-compatible"}`), ownerID)
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var total, active int
	var minGeneration, maxGeneration int64
	if err := st.pool.QueryRow(ctx, `SELECT count(*),
		count(*) FILTER (WHERE status='active'),min(generation),max(generation)
		FROM credential_vault_entries WHERE scope_kind='platform' AND
		provider=$1 AND purpose=$2`, scope.Provider, scope.Purpose).Scan(
		&total, &active, &minGeneration, &maxGeneration); err != nil {
		t.Fatal(err)
	}
	if total != writers || active != 1 || minGeneration != 1 || maxGeneration != writers {
		t.Fatalf("total=%d active=%d generations=%d..%d", total, active,
			minGeneration, maxGeneration)
	}
	if err := st.RevokeCredential(ctx, scope, ownerID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.CredentialStatus(ctx, scope, ownerID); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("status after revoke err=%v, want not found", err)
	}
}

func TestMigration137CredentialVaultBoundary(t *testing.T) {
	payload, err := migrationsFS.ReadFile("migrations/137_credential_vault.sql")
	if err != nil {
		t.Fatal(err)
	}
	sqlText := string(payload)
	for _, fragment := range []string{
		"CREATE TABLE credential_vault_entries",
		"scope_kind='platform' AND tenant_id IS NULL",
		"scope_kind='tenant' AND tenant_id IS NOT NULL",
		"octet_length(nonce)=12",
		"uq_credential_vault_platform_active",
		"uq_credential_vault_tenant_active",
		"REVOKE ALL ON credential_vault_entries FROM PUBLIC,vane_app",
		"ALTER TABLE credential_vault_entries ENABLE ROW LEVEL SECURITY",
		"migration 137 down refused: encrypted credential history exists",
	} {
		if !strings.Contains(sqlText, fragment) {
			t.Errorf("migration 137 lost boundary %q", fragment)
		}
	}
	for _, forbidden := range []string{"app_secret TEXT", "bot_token TEXT", "api_key TEXT", "GRANT ALL"} {
		if strings.Contains(sqlText, forbidden) {
			t.Errorf("migration 137 introduced plaintext/overbroad authority %q", forbidden)
		}
	}
}

func TestMigration137DownRefusesCredentialHistoryPostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		requireDatabaseCapability(t)
	}
	database, provider, scratchURL, drop := migration128Scratch(t, databaseURL)
	t.Cleanup(drop)
	if _, err := provider.UpTo(t.Context(), latestMigrationVersion); err != nil {
		t.Fatal(err)
	}
	st, err := New(t.Context(), scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	registerStoreClose(t, st)
	configureTestCredentialVault(t, st)
	var ownerID int64
	if err := database.QueryRowContext(t.Context(), `INSERT INTO users(feishu_open_id,name)
		VALUES($1,'platform-owner') RETURNING id`,
		"credential-vault-down-owner-"+fmt.Sprint(time.Now().UnixNano()),
	).Scan(&ownerID); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), `INSERT INTO memberships(tenant_id,user_id,role)
		VALUES(1,$1,'owner')`, ownerID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.RotateCredential(t.Context(), CredentialScope{
		Kind: "platform", Provider: "llm", Purpose: "down_guard",
	}, json.RawMessage(`{"api_key":"synthetic"}`), json.RawMessage(`{}`), ownerID); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.DownTo(t.Context(), latestMigrationVersion-1); err == nil ||
		!strings.Contains(err.Error(), "encrypted credential history exists") {
		t.Fatalf("migration 137 down accepted retained credential: %v", err)
	}
	if _, err := database.ExecContext(t.Context(), `DELETE FROM credential_vault_entries`); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.DownTo(t.Context(), latestMigrationVersion-1); err != nil {
		t.Fatalf("empty credential vault could not downgrade: %v", err)
	}
	var exists bool
	if err := database.QueryRowContext(t.Context(),
		`SELECT to_regclass('public.credential_vault_entries') IS NOT NULL`).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("credential vault table survived successful migration 137 down")
	}
}
