package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/YouToco/vane/server/internal/testgate"
	"github.com/YouToco/vane/server/types"
)

func channelRuntimeTestStore(
	t *testing.T, database *sql.DB, owner *Store,
) *Store {
	t.Helper()
	const password = "channel-runtime-test-password"
	if _, err := database.ExecContext(t.Context(), `ALTER ROLE vane_channel_runtime
		LOGIN PASSWORD '`+password+`'`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = database.ExecContext(t.Context(),
			`ALTER ROLE vane_channel_runtime NOLOGIN PASSWORD NULL`)
	})
	config := owner.pool.Config().ConnConfig
	runtimeURL := &url.URL{Scheme: "postgres", Host: fmt.Sprintf("%s:%d",
		config.Host, config.Port), Path: "/" + config.Database,
		User: url.UserPassword(channelRuntimeLoginRole, password)}
	query := runtimeURL.Query()
	query.Set("sslmode", "disable")
	runtimeURL.RawQuery = query.Encode()
	runtimeStore, err := New(t.Context(), runtimeURL.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runtimeStore.Close)
	return runtimeStore
}

func TestMigration155ChannelRuntimeDarkGateContract(t *testing.T) {
	migration, err := os.ReadFile("migrations/155_channel_runtime_dark_gate.sql")
	if err != nil {
		t.Fatal(err)
	}
	source := string(migration)
	for _, required := range []string{
		"vane_channel_runtime NOLOGIN NOSUPERUSER",
		"NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS",
		"channel_runtime_authority_attestations",
		"FORCE ROW LEVEL SECURITY",
		"channel_runtime_authority_exact_principal",
		"sync_channel_runtime_authority_v155",
		"deprovision vane_channel_runtime login before downgrade",
	} {
		if !strings.Contains(source, required) {
			t.Errorf("migration 155 omitted %q", required)
		}
	}
	if strings.Contains(source, "GRANT SELECT ON credential_vault_entries") {
		t.Fatal("channel runtime received whole-row credential access")
	}
}

func TestMigration155SchemaOwnerRejectedAndNarrowRuntimeAttestedPostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		testgate.Database(t)
	}
	database, provider, scratchURL, drop := migration128Scratch(t, databaseURL)
	t.Cleanup(drop)
	if _, err := provider.UpTo(t.Context(), 155); err != nil {
		t.Fatal(err)
	}
	owner, err := New(t.Context(), scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(owner.Close)
	configureTestCredentialVault(t, owner)
	userID, tenantID := migration129Identity(t, database, "channel-runtime-owner")
	metadata, err := owner.RotateCredential(t.Context(), CredentialScope{
		Kind: "user", TenantID: tenantID, UserID: userID,
		Provider: "telegram", Purpose: "bot_api",
	}, json.RawMessage(`{"bot_token":"synthetic"}`),
		json.RawMessage(`{"bot_id":710155}`), userID)
	if err != nil {
		t.Fatal(err)
	}
	authority := TelegramRuntimeAuthority{TenantID: tenantID, UserID: userID,
		CredentialGeneration: metadata.Generation, AppIdentity: "710155"}
	if err := owner.VerifyTelegramRuntimeAuthority(t.Context(), authority); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("schema owner entered channel runtime: %v", err)
	}

	runtimeStore := channelRuntimeTestStore(t, database, owner)
	if err := runtimeStore.VerifyTelegramRuntimeAuthority(
		t.Context(), authority); err != nil {
		t.Fatalf("exact narrow runtime rejected: %v", err)
	}
	mutated := authority
	mutated.CredentialGeneration++
	if err := runtimeStore.VerifyTelegramRuntimeAuthority(t.Context(), mutated); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("generation mutation crossed gate: %v", err)
	}
	mutated = authority
	mutated.AppIdentity = "710156"
	if err := runtimeStore.VerifyTelegramRuntimeAuthority(t.Context(), mutated); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("app identity mutation crossed gate: %v", err)
	}
}
