package researchgateway

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func setMinimalProcessConfigV1(t *testing.T) {
	t.Helper()
	for _, key := range []string{"VANE_DB_URL",
		"VANE_DB_RESEARCH_RUNTIME_URL", "VANE_DB_RESEARCH_CONTROL_URL",
		"VANE_DB_RESEARCH_GATEWAY_RUNTIME_URL",
		"VANE_DB_RESEARCH_CAPABILITY_KEY_HEX", "VANE_LLM_API_KEY",
		"VANE_LLM_AGENT_API_KEY", "VANE_FETCH_EXA_API_KEY", "VANE_FETCH_TIKHUB_API_KEY",
		"VANE_GATEWAY_DB_URL", "VANE_GATEWAY_LLM_API_KEY"} {
		t.Setenv(key, "")
	}
	directory := t.TempDir()
	if err := os.WriteFile(filepath.Join(directory, "gateway_db_url"),
		[]byte("postgres://gateway:secret@db/vane"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "credential_vault_active_key"),
		[]byte(strings.Repeat("42", 32)), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CREDENTIALS_DIRECTORY", directory)
	t.Setenv("VANE_GATEWAY_LLM_ROUTES_JSON", "")
	t.Setenv("VANE_CREDENTIAL_VAULT_ACTIVE_KEY_ID", "vault-test-key")
	t.Setenv("VANE_GATEWAY_ALLOWED_UID", "1001")
}

func TestLoadProcessConfigV1AcceptsOnlyMinimalGatewayAuthority(t *testing.T) {
	setMinimalProcessConfigV1(t)
	config, err := LoadProcessConfigV1()
	if err != nil {
		t.Fatal(err)
	}
	if config.AllowedUID != 1001 || config.Vault == nil ||
		config.DatabaseURL != "postgres://gateway:secret@db/vane" {
		t.Fatal("minimal gateway config was not loaded exactly")
	}
}

func TestLoadProcessConfigV1RejectsRetiredRouteEnvironmentAuthority(t *testing.T) {
	setMinimalProcessConfigV1(t)
	t.Setenv("VANE_GATEWAY_LLM_ROUTES_JSON", `[{"credential_generation":2}]`)
	if _, err := LoadProcessConfigV1(); err == nil {
		t.Fatal("retired route environment authority was accepted")
	}
}

func TestLoadProcessConfigV1CannotUseDatabaseCredentialAsVaultKey(t *testing.T) {
	setMinimalProcessConfigV1(t)
	const databaseSecret = "must-not-become-provider-bearer"
	if err := os.WriteFile(filepath.Join(os.Getenv("CREDENTIALS_DIRECTORY"), "gateway_db_url"),
		[]byte(databaseSecret), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(os.Getenv("CREDENTIALS_DIRECTORY"),
		"credential_vault_active_key")); err != nil {
		t.Fatal(err)
	}
	_, err := LoadProcessConfigV1()
	if err == nil || strings.Contains(err.Error(), databaseSecret) {
		t.Fatalf("database credential accepted as vault key or leaked: %v", err)
	}
}

func TestLoadProcessConfigV1RejectsMainSecretsWithoutEchoingValue(t *testing.T) {
	setMinimalProcessConfigV1(t)
	const secret = "must-not-appear-in-error"
	t.Setenv("VANE_LLM_API_KEY", secret)
	_, err := LoadProcessConfigV1()
	if err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("err=%v", err)
	}
}

func TestLoadProcessConfigV1RejectsResearchControlURL(t *testing.T) {
	setMinimalProcessConfigV1(t)
	const secret = "postgres://vane_server_runtime:must-not-appear@db/vane"
	t.Setenv("VANE_DB_RESEARCH_CONTROL_URL", secret)
	_, err := LoadProcessConfigV1()
	if err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("research control credential accepted or leaked: %v", err)
	}
}

func TestLoadProcessConfigV1RejectsRootPeer(t *testing.T) {
	setMinimalProcessConfigV1(t)
	t.Setenv("VANE_GATEWAY_ALLOWED_UID", "0")
	if _, err := LoadProcessConfigV1(); err == nil {
		t.Fatal("root peer UID was accepted")
	}
}
