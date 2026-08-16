package researchgateway

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/YouToco/vane/server/credentialvault"
)

type fakeLLMCredentialEnvelopeRepositoryV1 struct {
	rows []LLMCredentialEnvelopeV1
	err  error
}

func (f fakeLLMCredentialEnvelopeRepositoryV1) ListLLMCredentialEnvelopesV1(
	context.Context,
) ([]LLMCredentialEnvelopeV1, error) {
	return f.rows, f.err
}

func testGatewayVault(t *testing.T) *credentialvault.Vault {
	t.Helper()
	config, err := credentialvault.ParseKeyring(
		"gateway-test-key", strings.Repeat("42", 32), "")
	if err != nil {
		t.Fatal(err)
	}
	vault, err := credentialvault.New(config)
	if err != nil {
		t.Fatal(err)
	}
	return vault
}

func testGatewayEnvelope(
	t *testing.T, vault *credentialvault.Vault, generation int64, status, apiKey string,
) LLMCredentialEnvelopeV1 {
	t.Helper()
	secret, _ := json.Marshal(map[string]string{
		"api_key": apiKey, "agent_api_key": "must-not-be-used-by-gateway",
	})
	envelope, err := vault.Seal(credentialvault.Scope{
		Kind: "platform", Provider: "llm", Purpose: "shared_runtime",
		Generation: generation,
	}, secret)
	if err != nil {
		t.Fatal(err)
	}
	metadata, _ := json.Marshal(map[string]any{
		"provider": "deepseek", "base_url": "https://api.deepseek.com",
		"model": "deepseek-v4-flash", "max_concurrent": 8,
	})
	return LLMCredentialEnvelopeV1{
		Generation: generation, EnvelopeVersion: envelope.Version,
		KeyID: envelope.KeyID, Nonce: envelope.Nonce,
		Ciphertext: envelope.Ciphertext, Fingerprint: envelope.Fingerprint,
		Metadata: metadata, Status: status,
	}
}

func TestLoadProcessRoutesV1UsesActiveAndRetiredDatabaseGenerations(t *testing.T) {
	vault := testGatewayVault(t)
	repository := fakeLLMCredentialEnvelopeRepositoryV1{rows: []LLMCredentialEnvelopeV1{
		testGatewayEnvelope(t, vault, 2, "active", "database-key-2"),
		testGatewayEnvelope(t, vault, 1, "retired", "database-key-1"),
	}}
	routes, err := LoadProcessRoutesV1(t.Context(), repository, vault)
	if err != nil {
		t.Fatal(err)
	}
	if len(routes) != 2 || routes[0].CredentialRef.Generation != 2 ||
		routes[0].Endpoint.Generation != 2 || routes[0].LLM.APIKey != "database-key-2" ||
		routes[1].CredentialRef.Generation != 1 || routes[1].LLM.APIKey != "database-key-1" {
		t.Fatalf("unexpected retained gateway routes: %+v", routes)
	}
}

func TestLoadProcessRoutesV1FailsClosedOnRevokedHeadTamperAndRouteDrift(t *testing.T) {
	vault := testGatewayVault(t)
	active := testGatewayEnvelope(t, vault, 2, "active", "database-key-2")
	tests := []struct {
		name string
		rows []LLMCredentialEnvelopeV1
	}{
		{name: "no active head", rows: []LLMCredentialEnvelopeV1{
			testGatewayEnvelope(t, vault, 2, "retired", "database-key-2"),
		}},
		{name: "tampered", rows: func() []LLMCredentialEnvelopeV1 {
			tampered := active
			tampered.Ciphertext = append([]byte(nil), active.Ciphertext...)
			tampered.Ciphertext[0] ^= 1
			return []LLMCredentialEnvelopeV1{tampered}
		}()},
		{name: "route drift", rows: func() []LLMCredentialEnvelopeV1 {
			drifted := active
			drifted.Metadata = json.RawMessage(`{"provider":"deepseek","base_url":"https://attacker.invalid","model":"m","max_concurrent":8}`)
			return []LLMCredentialEnvelopeV1{drifted}
		}()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if routes, err := LoadProcessRoutesV1(t.Context(),
				fakeLLMCredentialEnvelopeRepositoryV1{rows: test.rows}, vault,
			); err == nil || len(routes) != 0 {
				t.Fatalf("routes=%+v err=%v", routes, err)
			}
		})
	}
}
