package researchgateway

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/YouToco/vane/server/config"
	"github.com/YouToco/vane/server/credentialvault"
	"github.com/YouToco/vane/server/runtimepolicy"
)

type ProcessRouteV1 struct {
	Provider      runtimepolicy.ModelProviderIDV1
	Endpoint      runtimepolicy.EndpointRefV1
	CredentialRef runtimepolicy.CredentialRefV1
	LLM           config.LLMConfig
}

type LLMCredentialEnvelopeV1 struct {
	Generation      int64
	EnvelopeVersion string
	KeyID           string
	Nonce           []byte
	Ciphertext      []byte
	Fingerprint     string
	Metadata        json.RawMessage
	Status          string
}

type LLMCredentialEnvelopeRepositoryV1 interface {
	ListLLMCredentialEnvelopesV1(context.Context) ([]LLMCredentialEnvelopeV1, error)
}

type gatewayLLMSecretV1 struct {
	APIKey string `json:"api_key"`
}

type gatewayLLMMetadataV1 struct {
	Provider      string `json:"provider"`
	BaseURL       string `json:"base_url"`
	Model         string `json:"model"`
	MaxConcurrent int    `json:"max_concurrent"`
}

// LoadProcessRoutesV1 materializes only the exact platform LLM generations
// exposed by the gateway's SECURITY DEFINER reader. PostgreSQL never sees the
// KEK; the process never receives table access or another provider's envelope.
func LoadProcessRoutesV1(
	ctx context.Context,
	repository LLMCredentialEnvelopeRepositoryV1,
	vault *credentialvault.Vault,
) ([]ProcessRouteV1, error) {
	if repository == nil || vault == nil {
		return nil, errors.New("research gateway credential authority is unavailable")
	}
	envelopes, err := repository.ListLLMCredentialEnvelopesV1(ctx)
	if err != nil {
		return nil, errors.New("research gateway encrypted credential inventory is unavailable")
	}
	if len(envelopes) == 0 || envelopes[0].Status != "active" {
		return nil, errors.New("research gateway has no active database LLM credential")
	}
	routes := make([]ProcessRouteV1, 0, len(envelopes))
	seenActive := false
	for index, envelope := range envelopes {
		if envelope.Generation <= 0 ||
			(envelope.Status != "active" && envelope.Status != "retired") ||
			(index > 0 && envelope.Generation >= envelopes[index-1].Generation) {
			return nil, errors.New("research gateway LLM credential ledger is invalid")
		}
		if envelope.Status == "active" {
			if seenActive || index != 0 {
				return nil, errors.New("research gateway LLM credential authority is ambiguous")
			}
			seenActive = true
		}
		var metadata gatewayLLMMetadataV1
		if json.Unmarshal(envelope.Metadata, &metadata) != nil ||
			metadata.Provider != "deepseek" ||
			(metadata.BaseURL != "https://api.deepseek.com" &&
				metadata.BaseURL != "https://api.deepseek.com/v1") ||
			strings.TrimSpace(metadata.Model) == "" ||
			metadata.MaxConcurrent < 1 || metadata.MaxConcurrent > 128 {
			return nil, errors.New("research gateway LLM route metadata is invalid")
		}
		plaintext, err := vault.Open(credentialvault.Scope{
			Kind: "platform", Provider: "llm", Purpose: "shared_runtime",
			Generation: envelope.Generation,
		}, credentialvault.Envelope{
			Version: envelope.EnvelopeVersion, KeyID: envelope.KeyID,
			Nonce: envelope.Nonce, Ciphertext: envelope.Ciphertext,
			Fingerprint: envelope.Fingerprint,
		})
		if err != nil {
			return nil, errors.New("research gateway LLM credential failed integrity verification")
		}
		var secret gatewayLLMSecretV1
		decodeErr := json.Unmarshal(plaintext, &secret)
		clear(plaintext)
		secret.APIKey = strings.TrimSpace(secret.APIKey)
		if decodeErr != nil || secret.APIKey == "" || len(secret.APIKey) > 16<<10 {
			return nil, errors.New("research gateway LLM credential is invalid")
		}
		routes = append(routes, ProcessRouteV1{
			Provider: runtimepolicy.ModelProviderDeepSeekV1,
			Endpoint: runtimepolicy.EndpointRefV1{
				ID:         runtimepolicy.EndpointIDDeepSeekCompatiblePrimaryV1,
				Generation: envelope.Generation,
			},
			CredentialRef: runtimepolicy.CredentialRefV1{
				ID:         runtimepolicy.CredentialIDLLMPrimaryV1,
				Generation: envelope.Generation,
			},
			LLM: config.LLMConfig{
				Provider: "deepseek", BaseURL: metadata.BaseURL,
				APIKey: secret.APIKey, Model: metadata.Model,
				MaxConcurrent: metadata.MaxConcurrent,
			},
		})
	}
	if !seenActive {
		return nil, errors.New("research gateway has no active database LLM credential")
	}
	return routes, nil
}
