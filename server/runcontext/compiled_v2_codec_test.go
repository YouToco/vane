package runcontext

import (
	"bytes"
	"testing"

	"github.com/YouToco/vane/runtimepolicy"
)

func TestEncodePolicyBundleV1_PreservesWideGenerations(t *testing.T) {
	const generation int64 = 9007199254740993
	policy := testPolicyBundleV1(t)
	policy.CapabilityCatalog.Allowed[0].CredentialRef.Generation = generation
	policy.ModelPolicy.Endpoint.Generation = generation
	policy.ModelPolicy.CredentialRef.Generation = generation

	payloads, _, normalized, err := EncodePolicyBundleV1(policy)
	if err != nil {
		t.Fatal(err)
	}
	want := []byte(`"generation":9007199254740993`)
	if bytes.Count(payloads.CapabilityCatalog, want) != 1 ||
		bytes.Count(payloads.ModelPolicy, want) != 2 {
		t.Fatalf("wide generation was not preserved: capability=%s model=%s",
			payloads.CapabilityCatalog, payloads.ModelPolicy)
	}
	if normalized.CapabilityCatalog.Allowed[0].CredentialRef.Generation != generation ||
		normalized.ModelPolicy.Endpoint.Generation != generation ||
		normalized.ModelPolicy.CredentialRef.Generation != generation {
		t.Fatal("normalized policy changed a wide generation")
	}
}

func testPolicyBundleV1(t *testing.T) runtimepolicy.BundleV1 {
	t.Helper()
	policy, err := runtimepolicy.BuildV1(runtimepolicy.BuildInputV1{
		AllowedCapabilities: []runtimepolicy.CapabilityV1{{
			Platform: "web", Capability: "search", Kind: "article",
			ImplementationVersion: runtimepolicy.CapabilityImplementationExaV1,
			CredentialRef: runtimepolicy.CredentialRefV1{
				ID:         runtimepolicy.CredentialIDExaPrimaryV1,
				Generation: runtimepolicy.PrimaryGenerationV1,
			},
		}},
		ScorePrompt: runtimepolicy.PromptStageV1{
			SystemPrompt: "score", RendererVersion: "scorer.render/v1",
		},
		CardGenPrompt: runtimepolicy.PromptStageV1{
			SystemPrompt: "card", RendererVersion: "cardgen.render/v1",
		},
		ProfileEvolvePrompt: runtimepolicy.PromptStageV1{
			SystemPrompt: "profile", RendererVersion: "evolver.render/v1",
		},
		TaskInstructionEnabled: true,
		ModelProvider:          runtimepolicy.ModelProviderDeepSeekV1,
		ModelEndpoint: runtimepolicy.EndpointRefV1{
			ID:         runtimepolicy.EndpointIDDeepSeekCompatiblePrimaryV1,
			Generation: runtimepolicy.PrimaryGenerationV1,
		},
		ModelCredentialRef: runtimepolicy.CredentialRefV1{
			ID:         runtimepolicy.CredentialIDLLMPrimaryV1,
			Generation: runtimepolicy.PrimaryGenerationV1,
		},
		ModelCalls: []runtimepolicy.ModelCallV1{
			{
				Stage: runtimepolicy.ModelStageScore, Model: "model",
				MaxTokens: 16, DisableThinking: true,
			},
			{
				Stage: runtimepolicy.ModelStageCardGen, Model: "model",
				MaxTokens: 64, DisableThinking: true,
			},
			{
				Stage: runtimepolicy.ModelStageProfileEvolve, Model: "model",
				MaxTokens: 64, DisableThinking: true,
			},
		},
		QuotaBuckets: []runtimepolicy.QuotaBucketV1{{
			Name: "llm_tokens", Financial: true,
			EnforcementVersion: runtimepolicy.QuotaEnforcementLLMPrechargeV1,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return policy
}
