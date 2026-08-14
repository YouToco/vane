package llm

import (
	"errors"
	"strings"
	"testing"

	"github.com/YouToco/vane/config"
	"github.com/YouToco/vane/runtimepolicy"
)

func validRuntimeModelPolicyV1() runtimepolicy.ModelPolicyV1 {
	return runtimepolicy.ModelPolicyV1{
		SchemaVersion: runtimepolicy.ModelPolicySchemaVersionV1,
		Provider:      runtimepolicy.ModelProviderDeepSeekV1,
		Endpoint: runtimepolicy.EndpointRefV1{
			ID:         runtimepolicy.EndpointIDDeepSeekCompatiblePrimaryV1,
			Generation: runtimepolicy.PrimaryGenerationV1,
		},
		CredentialRef: runtimepolicy.CredentialRefV1{
			ID: runtimepolicy.CredentialIDLLMPrimaryV1, Generation: runtimepolicy.PrimaryGenerationV1,
		},
		Calls: []runtimepolicy.ModelCallV1{
			{Stage: runtimepolicy.ModelStageScore, Model: "deepseek-v4-flash", MaxTokens: 16, DisableThinking: true},
			{Stage: runtimepolicy.ModelStageCardGen, Model: "deepseek-v4-flash", Temperature: 0.7, MaxTokens: 400, DisableThinking: true},
			{Stage: runtimepolicy.ModelStageProfileEvolve, Model: "deepseek-v4-flash", MaxTokens: 800, DisableThinking: true},
		},
	}
}

func TestClient_ValidateRuntimeModelPolicyV1(t *testing.T) {
	valid := validRuntimeModelPolicyV1()
	validClient := New(config.LLMConfig{
		Provider: "deepseek", BaseURL: "https://runtime-model.test",
		APIKey: "opaque-test-value", Model: "legacy-default", MaxConcurrent: 1,
	})

	tests := []struct {
		name        string
		client      *Client
		policy      runtimepolicy.ModelPolicyV1
		wantInvalid bool
		wantRoute   bool
	}{
		{name: "controlled primary route", client: validClient, policy: valid},
		{name: "nil client", policy: valid, wantRoute: true},
		{
			name: "invalid policy fails before resolution", client: validClient,
			policy: func() runtimepolicy.ModelPolicyV1 {
				p := valid
				p.SchemaVersion = "vane.runtime-model-policy/v2"
				return p
			}(),
			wantInvalid: true,
		},
		{
			name: "provider adapter unavailable",
			client: New(config.LLMConfig{
				Provider: "another-provider", BaseURL: "https://runtime-model.test",
				APIKey: "opaque-test-value", Model: "legacy-default", MaxConcurrent: 1,
			}),
			policy: valid, wantRoute: true,
		},
		{
			name: "future endpoint generation unavailable", client: validClient,
			policy: func() runtimepolicy.ModelPolicyV1 {
				p := valid
				p.Endpoint.Generation++
				return p
			}(),
			wantRoute: true,
		},
		{
			name: "future credential generation unavailable", client: validClient,
			policy: func() runtimepolicy.ModelPolicyV1 {
				p := valid
				p.CredentialRef.Generation++
				return p
			}(),
			wantRoute: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.client.ValidateRuntimeModelPolicyV1(test.policy)
			if !test.wantInvalid && !test.wantRoute {
				if err != nil {
					t.Fatalf("ValidateRuntimeModelPolicyV1() error = %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("ValidateRuntimeModelPolicyV1() error = nil")
			}
			if test.wantInvalid && !errors.Is(err, runtimepolicy.ErrInvalidPolicy) {
				t.Errorf("invalid policy error = %v, want ErrInvalidPolicy", err)
			}
			if test.wantRoute {
				if errors.Is(err, runtimepolicy.ErrInvalidPolicy) {
					t.Errorf("valid but unavailable route was misclassified as invalid policy: %v", err)
				}
				if !strings.Contains(err.Error(), "route is not available") && test.client != nil {
					t.Errorf("unavailable route error = %q", err)
				}
			}
		})
	}
}

func TestRuntimeModelResolverV1_BindsExactGenerationWithoutCurrentFallback(t *testing.T) {
	gen1 := New(config.LLMConfig{Provider: "deepseek", BaseURL: "https://gen1.test", APIKey: "key-gen1", MaxConcurrent: 1})
	gen2 := New(config.LLMConfig{Provider: "deepseek", BaseURL: "https://gen2.test", APIKey: "key-gen2", MaxConcurrent: 1})
	route := func(generation int64, client *Client) RuntimeModelRouteV1 {
		return RuntimeModelRouteV1{
			Provider: runtimepolicy.ModelProviderDeepSeekV1,
			Endpoint: runtimepolicy.EndpointRefV1{
				ID: runtimepolicy.EndpointIDDeepSeekCompatiblePrimaryV1, Generation: generation,
			},
			CredentialRef: runtimepolicy.CredentialRefV1{
				ID: runtimepolicy.CredentialIDLLMPrimaryV1, Generation: generation,
			},
			Client: client,
		}
	}
	resolver, err := NewRuntimeModelResolverV1(route(1, gen1), route(2, gen2))
	if err != nil {
		t.Fatal(err)
	}

	policy := validRuntimeModelPolicyV1()
	got, err := resolver.ResolveRuntimeModelPolicyV1(policy)
	if err != nil {
		t.Fatal(err)
	}
	if got != gen1 || got.baseURL != "https://gen1.test" || got.apiKey != "key-gen1" {
		t.Fatalf("gen1 resolved to wrong executor (same_client=%v)", got == gen1)
	}
	policy.Endpoint.Generation = 2
	policy.CredentialRef.Generation = 2
	got, err = resolver.ResolveRuntimeModelPolicyV1(policy)
	if err != nil {
		t.Fatal(err)
	}
	if got != gen2 || got.baseURL != "https://gen2.test" || got.apiKey != "key-gen2" {
		t.Fatalf("gen2 resolved to wrong executor (same_client=%v)", got == gen2)
	}

	currentOnly, err := NewRuntimeModelResolverV1(route(2, gen2))
	if err != nil {
		t.Fatal(err)
	}
	policy.Endpoint.Generation = 1
	policy.CredentialRef.Generation = 1
	if _, err := currentOnly.ResolveRuntimeModelPolicyV1(policy); err == nil {
		t.Fatal("missing gen1 route fell back to current gen2")
	}
}
