package runtimepolicy

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
)

func TestResearchModelPolicyV34RetainedCanonicalGolden(t *testing.T) {
	policy := researchModelPolicyForTest(t)
	policy.Synthesis.RendererVersion = ResearchSynthesisRendererVersionV34
	policy.GroundingVerifier = &ResearchModelStageV3{
		Stage: ResearchModelStageGroundingVerifierV3, Model: "strong-model",
		MaxTokens: 4096, DisableThinking: true,
		SystemPrompt:    "Independently verify every claim against cited evidence.",
		RendererVersion: ResearchGroundingVerifierRendererVersionV12,
	}
	built, err := BuildResearchModelPolicyV3(policy)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := EncodeResearchModelPolicyV3(built)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(payload)
	const retainedDigest = "747064d87c5edc24e45e7ffe185e7f2f607d6322162aeb8ae64f3cd0cafaa766"
	if got := hex.EncodeToString(sum[:]); got != retainedDigest {
		t.Fatalf("retained v3.4 canonical digest=%s", got)
	}
}

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
	verifier.RendererVersion = ResearchGroundingVerifierRendererVersionV12
	policy.GroundingVerifier = &verifier
	if _, err := BuildResearchModelPolicyV3(policy); err != nil {
		t.Fatalf("v1.2 verifier renderer rejected: %v", err)
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

func TestWithExplicitEventWindowV36FreezesOneCorrector(t *testing.T) {
	policy := researchModelPolicyForTest(t)
	policy.Synthesis.RendererVersion = ResearchSynthesisRendererVersionV34
	policy.GroundingVerifier = &ResearchModelStageV3{
		Stage: ResearchModelStageGroundingVerifierV3, Model: "strong-model",
		MaxTokens: 4096, DisableThinking: true,
		SystemPrompt:    "Independently verify every claim against cited evidence.",
		RendererVersion: ResearchGroundingVerifierRendererVersionV12,
	}
	retained, err := BuildResearchModelPolicyV3(policy)
	if err != nil {
		t.Fatal(err)
	}
	scoped, err := WithExplicitEventWindowV36(retained)
	if err != nil || scoped.Synthesis.RendererVersion !=
		ResearchSynthesisRendererVersionV36 || scoped.GroundingCorrector == nil {
		t.Fatalf("scoped=%+v err=%v", scoped, err)
	}
	if scoped.GroundingCorrector.Stage != ResearchModelStageGroundingCorrectorV3 ||
		scoped.GroundingCorrector.RendererVersion !=
			ResearchGroundingCorrectorRendererVersionV1 ||
		scoped.GroundingCorrector.Temperature != 0 {
		t.Fatalf("corrector=%+v", scoped.GroundingCorrector)
	}
	for _, contract := range []string{
		"重新审计 headline、summary 和 significance",
		"不得把 access 扩大写成默认或无限使用",
		"把 fallback 写成误拒绝",
		"把取得突破写成发布新模型",
		"删除包含它的整句以及 headline 中对应分句",
		"才可写‘全部’‘均有’或数量汇总",
	} {
		if !strings.Contains(scoped.GroundingCorrector.SystemPrompt, contract) {
			t.Fatalf("corrector prompt missing production grounding contract %q: %s",
				contract, scoped.GroundingCorrector.SystemPrompt)
		}
	}
	if retained.GroundingCorrector != nil ||
		retained.Synthesis.RendererVersion != ResearchSynthesisRendererVersionV34 {
		t.Fatalf("retained policy mutated: %+v", retained)
	}

	broken := scoped
	broken.GroundingCorrector = nil
	if _, err := BuildResearchModelPolicyV3(broken); err == nil {
		t.Fatal("v3.6 accepted without corrector")
	}
	broken = scoped
	broken.GroundingVerifier = nil
	if _, err := BuildResearchModelPolicyV3(broken); err == nil {
		t.Fatal("v3.6 accepted a corrector without verifier")
	}
	broken = retained
	corrector := scoped.GroundingCorrector
	broken.GroundingCorrector = corrector
	if _, err := BuildResearchModelPolicyV3(broken); err == nil {
		t.Fatal("retained v3.4 accepted unused corrector")
	}
}
