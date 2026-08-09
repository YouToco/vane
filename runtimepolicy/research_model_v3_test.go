package runtimepolicy

import (
	"encoding/json"
	"testing"
)

func researchModelPolicyForTest(t *testing.T) ResearchModelPolicyV3 {
	t.Helper()
	policy, err := BuildResearchModelPolicyV3(ResearchModelPolicyV3{
		Provider: ModelProviderDeepSeekV1,
		Endpoint: EndpointRefV1{
			ID: EndpointIDDeepSeekCompatiblePrimaryV1, Generation: 3,
		},
		CredentialRef: CredentialRefV1{
			ID: CredentialIDLLMPrimaryV1, Generation: 4,
		},
		Planner: ResearchModelStageV3{
			Stage: ResearchModelStagePlannerV3, Model: "strong-model",
			Temperature: 0.1, MaxTokens: 4096,
			SystemPrompt:    "Plan only from the trusted task manual and Tool catalog.",
			RendererVersion: "research-planner.render/v3",
		},
		Synthesis: ResearchModelStageV3{
			Stage: ResearchModelStageSynthesisV3, Model: "strong-model",
			Temperature: 0.1, MaxTokens: 8192,
			SystemPrompt:    "Synthesize without Tools from bounded external evidence.",
			RendererVersion: "research-synthesis.render/v3",
		},
		QuotaBucket: "llm_tokens",
	})
	if err != nil {
		t.Fatal(err)
	}
	return policy
}

func TestResearchModelPolicyV3CanonicalRoundTrip(t *testing.T) {
	for _, disableThinking := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy-enabled", true: "v3-disabled"}[disableThinking], func(t *testing.T) {
			policy := researchModelPolicyForTest(t)
			policy.Planner.DisableThinking = disableThinking
			policy.Synthesis.DisableThinking = disableThinking
			payload, err := EncodeResearchModelPolicyV3(policy)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := DecodeResearchModelPolicyV3(payload)
			if err != nil || decoded != policy {
				t.Fatalf("decoded=%+v err=%v", decoded, err)
			}
			if decoded.Planner.DisableThinking != disableThinking ||
				decoded.Synthesis.DisableThinking != disableThinking {
				t.Fatalf("disable_thinking changed during round trip: %+v", decoded)
			}
			digest, err := DigestResearchModelPolicyV3(decoded)
			if err != nil || len(digest) != 64 {
				t.Fatalf("digest=%q err=%v", digest, err)
			}
		})
	}
}

func TestResearchModelPolicyV3RejectsUnknownAndUnretainedRoutes(t *testing.T) {
	policy := researchModelPolicyForTest(t)
	payload, _ := EncodeResearchModelPolicyV3(policy)
	var object map[string]any
	_ = json.Unmarshal(payload, &object)
	object["endpoint_url"] = "https://attacker.invalid"
	tampered, _ := json.Marshal(object)
	if _, err := DecodeResearchModelPolicyV3(tampered); err == nil {
		t.Fatal("unknown endpoint URL field accepted")
	}
	policy.Endpoint.Generation = 0
	if _, err := BuildResearchModelPolicyV3(policy); err == nil {
		t.Fatal("unretained model route accepted")
	}
}

func TestResearchModelPolicyGroundedRenderersRequireFrozenIndependentVerifier(t *testing.T) {
	policy := researchModelPolicyForTest(t)
	policy.Synthesis.RendererVersion = ResearchSynthesisRendererVersionV34
	if _, err := BuildResearchModelPolicyV3(policy); err == nil {
		t.Fatal("v3.4 synthesis accepted without a frozen verifier")
	}
	verifier := ResearchModelStageV3{
		Stage: ResearchModelStageGroundingVerifierV3, Model: "strong-model",
		Temperature: 0, MaxTokens: 4096, DisableThinking: true,
		SystemPrompt:    "Independently verify every claim against cited evidence.",
		RendererVersion: ResearchGroundingVerifierRendererVersionV1,
	}
	policy.GroundingVerifier = &verifier
	built, err := BuildResearchModelPolicyV3(policy)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := EncodeResearchModelPolicyV3(built)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeResearchModelPolicyV3(payload)
	if err != nil || decoded.GroundingVerifier == nil ||
		*decoded.GroundingVerifier != verifier ||
		decoded.Synthesis.RendererVersion != ResearchSynthesisRendererVersionV34 {
		t.Fatalf("decoded=%+v err=%v", decoded, err)
	}
	verifier.RendererVersion = ResearchGroundingVerifierRendererVersionV11
	policy.GroundingVerifier = &verifier
	if _, err := BuildResearchModelPolicyV3(policy); err != nil {
		t.Fatalf("v1.1 verifier renderer rejected: %v", err)
	}
	policy.Synthesis.RendererVersion = ResearchSynthesisRendererVersionV33
	if _, err := BuildResearchModelPolicyV3(policy); err != nil {
		t.Fatalf("retained v3.3 synthesis renderer rejected: %v", err)
	}
	policy.Synthesis.RendererVersion = ResearchSynthesisRendererVersionV34
	verifier.RendererVersion = "research-grounding-verifier.render/future"
	policy.GroundingVerifier = &verifier
	if _, err := BuildResearchModelPolicyV3(policy); err == nil {
		t.Fatal("unknown verifier renderer accepted")
	}

	verifier.RendererVersion = ResearchGroundingVerifierRendererVersionV1
	policy.GroundingVerifier = &verifier
	policy.Synthesis.RendererVersion = ResearchSynthesisRendererVersionV32
	if _, err := BuildResearchModelPolicyV3(policy); err == nil {
		t.Fatal("legacy synthesis renderer accepted an unused verifier policy")
	}
}
