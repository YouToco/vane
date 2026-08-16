package store

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestMigration151GatewayReadsOnlyEncryptedLLMCredentialEnvelopePostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		requireDatabaseCapability(t)
	}
	database, provider, scratchURL, drop := migration128Scratch(t, databaseURL)
	t.Cleanup(drop)
	if _, err := provider.UpTo(t.Context(), 151); err != nil {
		t.Fatal(err)
	}
	st, err := New(t.Context(), scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	registerStoreClose(t, st)
	configureTestCredentialVault(t, st)
	ownerID, _ := migration129Identity(t, database, "gateway-vault-reader")
	const secretValue = "gateway-database-secret-never-plaintext"
	secret, _ := json.Marshal(map[string]string{"api_key": secretValue})
	metadata := json.RawMessage(`{"provider":"deepseek","base_url":"https://api.deepseek.com","model":"deepseek-v4-flash","max_concurrent":8}`)
	rotated, err := st.RotateCredential(t.Context(), CredentialScope{
		Kind: "platform", Provider: "llm", Purpose: "shared_runtime",
	}, secret, metadata, ownerID)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := database.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(t.Context(), `SET LOCAL ROLE vane_research_llm_gateway`); err != nil {
		t.Fatal(err)
	}
	var generation int64
	var ciphertext []byte
	var gotMetadata []byte
	if err := tx.QueryRowContext(t.Context(), `SELECT generation,ciphertext,metadata
		FROM list_research_gateway_llm_credentials_v1()`).Scan(
		&generation, &ciphertext, &gotMetadata); err != nil {
		t.Fatal(err)
	}
	if generation != rotated.Generation || strings.Contains(string(ciphertext), secretValue) ||
		string(gotMetadata) != string(metadata) {
		t.Fatalf("generation=%d metadata=%s", generation, gotMetadata)
	}
	if _, err := tx.ExecContext(t.Context(), `SELECT ciphertext FROM credential_vault_entries`); err == nil {
		t.Fatal("gateway role unexpectedly received credential_vault_entries SELECT")
	}
}

func TestMigration151FunctionIsPurposeAndProviderBound(t *testing.T) {
	payload, err := migrationsFS.ReadFile(
		"migrations/151_research_gateway_credential_vault.sql")
	if err != nil {
		t.Fatal(err)
	}
	text := string(payload)
	for _, required := range []string{
		"SECURITY DEFINER", "c.scope_kind='platform'", "c.provider='llm'",
		"c.purpose='shared_runtime'", "c.status IN ('active','retired')",
		"GRANT EXECUTE ON FUNCTION", "TO vane_research_llm_gateway",
	} {
		if !strings.Contains(text, required) {
			t.Errorf("migration 151 lost boundary %q", required)
		}
	}
	for _, forbidden := range []string{"GRANT SELECT", "api_key", "app_secret", "bot_token"} {
		if strings.Contains(text, forbidden) {
			t.Errorf("migration 151 contains forbidden authority %q", forbidden)
		}
	}
}
