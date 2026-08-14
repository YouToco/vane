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
	if err := os.WriteFile(filepath.Join(directory, "llm_api_key_gen1"),
		[]byte("gateway-llm-secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CREDENTIALS_DIRECTORY", directory)
	t.Setenv("VANE_GATEWAY_LLM_ROUTES_JSON", `[{
		"provider":"deepseek","endpoint_id":"deepseek-compatible-primary",
		"endpoint_generation":1,"credential_id":"llm-primary",
		"credential_generation":1,"base_url":"https://api.deepseek.com/v1"}]`)
	t.Setenv("VANE_GATEWAY_ALLOWED_UID", "1001")
}

func TestLoadProcessConfigV1AcceptsOnlyMinimalGatewayAuthority(t *testing.T) {
	setMinimalProcessConfigV1(t)
	config, err := LoadProcessConfigV1()
	if err != nil {
		t.Fatal(err)
	}
	if config.AllowedUID != 1001 || len(config.Routes) != 1 ||
		config.Routes[0].LLM.APIKey != "gateway-llm-secret" ||
		config.DatabaseURL != "postgres://gateway:secret@db/vane" {
		t.Fatal("minimal gateway config was not loaded exactly")
	}
}

func TestLoadProcessConfigV1RequiresEveryRetainedGenerationCredential(t *testing.T) {
	setMinimalProcessConfigV1(t)
	t.Setenv("VANE_GATEWAY_LLM_ROUTES_JSON", `[{
		"provider":"deepseek","endpoint_id":"deepseek-compatible-primary",
		"endpoint_generation":2,"credential_id":"llm-primary",
		"credential_generation":2,"base_url":"https://api.deepseek.com/v1"}]`)
	if _, err := LoadProcessConfigV1(); err == nil {
		t.Fatal("missing retained generation credential was accepted")
	}
}

func TestLoadProcessConfigV1CannotSelectDatabaseCredentialAsProviderKey(t *testing.T) {
	setMinimalProcessConfigV1(t)
	const databaseSecret = "must-not-become-provider-bearer"
	if err := os.WriteFile(filepath.Join(os.Getenv("CREDENTIALS_DIRECTORY"), "gateway_db_url"),
		[]byte(databaseSecret), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("VANE_GATEWAY_LLM_ROUTES_JSON", `[{
		"provider":"deepseek","endpoint_id":"deepseek-compatible-primary",
		"endpoint_generation":1,"credential_id":"llm-primary",
		"credential_generation":1,"base_url":"https://api.deepseek.com/v1",
		"credential_name":"gateway_db_url"}]`)
	_, err := LoadProcessConfigV1()
	if err == nil || strings.Contains(err.Error(), databaseSecret) {
		t.Fatalf("database credential alias accepted or leaked: %v", err)
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
