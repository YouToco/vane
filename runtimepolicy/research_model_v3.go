package runtimepolicy

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"strings"

	"github.com/YouToco/vane/internal/strictjson"
)

const ResearchModelPolicySchemaVersionV3 = "vane.runtime-research-model-policy/v3"

const (
	ResearchModelStagePlannerV3           = "research_planner"
	ResearchModelStageSynthesisV3         = "research_synthesis"
	ResearchModelStageGroundingVerifierV3 = "research_grounding_verifier"

	ResearchPlannerRendererVersionV3  = "research-planner.render/v3"
	ResearchPlannerRendererVersionV31 = "research-planner.render/v3.1"
	ResearchPlannerRendererVersionV32 = "research-planner.render/v3.2"

	ResearchSynthesisRendererVersionV3  = "research-synthesis.render/v3"
	ResearchSynthesisRendererVersionV31 = "research-synthesis.render/v3.1"
	ResearchSynthesisRendererVersionV32 = "research-synthesis.render/v3.2"
	ResearchSynthesisRendererVersionV33 = "research-synthesis.render/v3.3"
	ResearchSynthesisRendererVersionV34 = "research-synthesis.render/v3.4"

	ResearchGroundingVerifierRendererVersionV1  = "research-grounding-verifier.render/v1"
	ResearchGroundingVerifierRendererVersionV11 = "research-grounding-verifier.render/v1.1"
)

type ResearchModelStageV3 struct {
	Stage           string  `json:"stage"`
	Model           string  `json:"model"`
	Temperature     float64 `json:"temperature"`
	MaxTokens       int     `json:"max_tokens"`
	DisableThinking bool    `json:"disable_thinking"`
	SystemPrompt    string  `json:"system_prompt"`
	RendererVersion string  `json:"renderer_version"`
}

// ResearchModelPolicyV3 freezes the retained model route and exact trusted
// prompt generations used for planning and no-Tool synthesis. It never carries
// credentials or endpoint URLs, only controlled generation references.
type ResearchModelPolicyV3 struct {
	SchemaVersion string               `json:"schema_version"`
	Provider      ModelProviderIDV1    `json:"provider"`
	Endpoint      EndpointRefV1        `json:"endpoint"`
	CredentialRef CredentialRefV1      `json:"credential_ref"`
	Planner       ResearchModelStageV3 `json:"planner"`
	Synthesis     ResearchModelStageV3 `json:"synthesis"`
	// GroundingVerifier is optional so byte-frozen V3/V3.1/V3.2 snapshots remain
	// decodable and replayable. New V3.3+ snapshots must freeze this independent
	// no-Tool adjudicator before a candidate Brief can become authoritative.
	GroundingVerifier *ResearchModelStageV3 `json:"grounding_verifier,omitempty"`
	QuotaBucket       string                `json:"quota_bucket"`
}

type researchModelPolicyV3Wire ResearchModelPolicyV3

func BuildResearchModelPolicyV3(policy ResearchModelPolicyV3) (ResearchModelPolicyV3, error) {
	policy.SchemaVersion = ResearchModelPolicySchemaVersionV3
	if err := policy.Validate(); err != nil {
		return ResearchModelPolicyV3{}, err
	}
	return policy, nil
}

func (p ResearchModelPolicyV3) Validate() error {
	if p.SchemaVersion != ResearchModelPolicySchemaVersionV3 ||
		p.Provider != ModelProviderDeepSeekV1 || !p.Endpoint.valid() ||
		p.CredentialRef.validateFor(CredentialIDLLMPrimaryV1) != nil ||
		p.QuotaBucket != "llm_tokens" ||
		!validResearchModelStageV3(p.Planner, ResearchModelStagePlannerV3) ||
		!validResearchModelStageV3(p.Synthesis, ResearchModelStageSynthesisV3) ||
		(p.GroundingVerifier != nil &&
			!validResearchModelStageV3(*p.GroundingVerifier,
				ResearchModelStageGroundingVerifierV3)) ||
		((p.Synthesis.RendererVersion == ResearchSynthesisRendererVersionV33 ||
			p.Synthesis.RendererVersion == ResearchSynthesisRendererVersionV34) &&
			(p.GroundingVerifier == nil ||
				!validResearchGroundingVerifierRendererVersion(
					p.GroundingVerifier.RendererVersion))) ||
		(p.Synthesis.RendererVersion != ResearchSynthesisRendererVersionV33 &&
			p.Synthesis.RendererVersion != ResearchSynthesisRendererVersionV34 &&
			p.GroundingVerifier != nil) {
		return invalidPolicy("research model policy is invalid")
	}
	return nil
}

func validResearchGroundingVerifierRendererVersion(version string) bool {
	return version == ResearchGroundingVerifierRendererVersionV1 ||
		version == ResearchGroundingVerifierRendererVersionV11
}

func (p ResearchModelPolicyV3) MarshalJSON() ([]byte, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	return marshalPolicyJSON(researchModelPolicyV3Wire(p))
}

func (p *ResearchModelPolicyV3) UnmarshalJSON(payload []byte) error {
	if p == nil || !validEncodedPolicySize(payload) {
		return invalidPolicy("research model policy json size is invalid")
	}
	var wire researchModelPolicyV3Wire
	if err := strictjson.DecodeExact(payload, &wire); err != nil {
		return invalidPolicy("research model policy json is invalid")
	}
	decoded := ResearchModelPolicyV3(wire)
	if err := decoded.Validate(); err != nil {
		return err
	}
	canonical, err := marshalPolicyJSON(wire)
	if err != nil || !bytes.Equal(payload, canonical) {
		return invalidPolicy("research model policy json is not canonical")
	}
	*p = decoded
	return nil
}

func EncodeResearchModelPolicyV3(policy ResearchModelPolicyV3) ([]byte, error) {
	return json.Marshal(policy)
}

func DecodeResearchModelPolicyV3(payload []byte) (ResearchModelPolicyV3, error) {
	var policy ResearchModelPolicyV3
	if err := json.Unmarshal(payload, &policy); err != nil {
		return ResearchModelPolicyV3{}, invalidPolicy("research model policy json is invalid")
	}
	return policy, nil
}

func DigestResearchModelPolicyV3(policy ResearchModelPolicyV3) (string, error) {
	payload, err := EncodeResearchModelPolicyV3(policy)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func validResearchModelStageV3(stage ResearchModelStageV3, expected string) bool {
	return stage.Stage == expected && validShortText(stage.Model) &&
		!math.IsNaN(stage.Temperature) && !math.IsInf(stage.Temperature, 0) &&
		stage.Temperature >= 0 && stage.Temperature <= 2 &&
		stage.MaxTokens > 0 && stage.MaxTokens <= maxModelTokens &&
		validPrompt(stage.SystemPrompt) && validShortText(stage.RendererVersion) &&
		!strings.ContainsRune(stage.SystemPrompt, 0)
}
