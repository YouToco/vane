package runtimepolicy

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/YouToco/vane/internal/strictjson"
)

const ResearchToolPolicySchemaVersionV3 = "vane.runtime-tool-policy/v3"

type ResearchToolImplementationV3 string

const (
	ResearchToolExaSearchV3         ResearchToolImplementationV3 = "acquisition.web-search/v1"
	ResearchToolExaContentsV3       ResearchToolImplementationV3 = "acquisition.web-contents/v1"
	ResearchToolKimiProductStatusV3 ResearchToolImplementationV3 = "acquisition.web-product-status/v1"
)

type ResearchToolEffectV3 string

const (
	ResearchToolEffectNetworkReadV3 ResearchToolEffectV3 = "network_read"
	ResearchToolEffectBillableV3    ResearchToolEffectV3 = "billable"
	ResearchToolEffectTrustTaintV3  ResearchToolEffectV3 = "trust_taint"
)

type ResearchToolTrustV3 string

const (
	ResearchToolTrustExternalV3 ResearchToolTrustV3 = "external"
	ResearchToolTrustOfficialV3 ResearchToolTrustV3 = "official"
)

// ResearchToolDefinitionV3 is a non-secret, scheduled-only execution grant.
// Every field is frozen before planning; execution resolves only a retained
// local implementation generation and never trusts provider metadata.
type ResearchToolDefinitionV3 struct {
	Name                     string                       `json:"name"`
	Description              string                       `json:"description"`
	Parameters               json.RawMessage              `json:"parameters"`
	SchemaDigest             string                       `json:"schema_digest"`
	Implementation           ResearchToolImplementationV3 `json:"implementation"`
	ImplementationGeneration int64                        `json:"implementation_generation"`
	Provider                 string                       `json:"provider"`
	Effects                  []ResearchToolEffectV3       `json:"effects"`
	ResultTrust              ResearchToolTrustV3          `json:"result_trust"`
	BudgetBucket             string                       `json:"budget_bucket"`
	CredentialRef            CredentialRefV1              `json:"credential_ref"`
	MaxCostMicroUSD          int64                        `json:"max_cost_micro_usd"`
}

// ResearchToolPolicyV3 is the complete public-read tool surface for one V3
// run. Internal reads, writes, delivery, local handles and activation tools
// have no representation in this allow-set.
type ResearchToolPolicyV3 struct {
	SchemaVersion string                     `json:"schema_version"`
	AllowedTools  []ResearchToolDefinitionV3 `json:"allowed_tools"`
}

type researchToolPolicyV3Wire ResearchToolPolicyV3

func BuildResearchToolPolicyV3(tools []ResearchToolDefinitionV3) (ResearchToolPolicyV3, error) {
	return normalizeResearchToolPolicyV3(ResearchToolPolicyV3{
		SchemaVersion: ResearchToolPolicySchemaVersionV3,
		AllowedTools:  tools,
	})
}

func (p ResearchToolPolicyV3) Validate() error {
	_, err := normalizeResearchToolPolicyV3(p)
	return err
}

func (p ResearchToolPolicyV3) MarshalJSON() ([]byte, error) {
	normalized, err := normalizeResearchToolPolicyV3(p)
	if err != nil {
		return nil, err
	}
	return marshalPolicyJSON(researchToolPolicyV3Wire(normalized))
}

func (p *ResearchToolPolicyV3) UnmarshalJSON(payload []byte) error {
	if p == nil || !validEncodedPolicySize(payload) {
		return invalidPolicy("research tool policy json size is invalid")
	}
	var wire researchToolPolicyV3Wire
	if err := strictjson.DecodeExact(payload, &wire); err != nil {
		return invalidPolicy("research tool policy json is invalid")
	}
	normalized, err := normalizeResearchToolPolicyV3(ResearchToolPolicyV3(wire))
	if err != nil {
		return err
	}
	canonical, err := marshalPolicyJSON(researchToolPolicyV3Wire(normalized))
	if err != nil || !bytes.Equal(payload, canonical) {
		return invalidPolicy("research tool policy json is not canonical")
	}
	*p = normalized
	return nil
}

func EncodeResearchToolPolicyV3(policy ResearchToolPolicyV3) ([]byte, error) {
	return json.Marshal(policy)
}

func DecodeResearchToolPolicyV3(payload []byte) (ResearchToolPolicyV3, error) {
	var policy ResearchToolPolicyV3
	if err := json.Unmarshal(payload, &policy); err != nil {
		return ResearchToolPolicyV3{}, invalidPolicy("research tool policy json is invalid")
	}
	return policy, nil
}

func DigestResearchToolPolicyV3(policy ResearchToolPolicyV3) (string, error) {
	payload, err := EncodeResearchToolPolicyV3(policy)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func normalizeResearchToolPolicyV3(policy ResearchToolPolicyV3) (ResearchToolPolicyV3, error) {
	if policy.SchemaVersion != ResearchToolPolicySchemaVersionV3 ||
		len(policy.AllowedTools) == 0 || len(policy.AllowedTools) > 32 {
		return ResearchToolPolicyV3{}, invalidPolicy("research tool allowlist is invalid")
	}
	policy.AllowedTools = slices.Clone(policy.AllowedTools)
	for index := range policy.AllowedTools {
		tool, err := normalizeResearchToolDefinitionV3(policy.AllowedTools[index])
		if err != nil {
			return ResearchToolPolicyV3{}, err
		}
		policy.AllowedTools[index] = tool
	}
	slices.SortFunc(policy.AllowedTools, func(left, right ResearchToolDefinitionV3) int {
		return strings.Compare(left.Name, right.Name)
	})
	for index := 1; index < len(policy.AllowedTools); index++ {
		if policy.AllowedTools[index-1].Name == policy.AllowedTools[index].Name {
			return ResearchToolPolicyV3{}, invalidPolicy("research tool is duplicated")
		}
	}
	return policy, nil
}

func normalizeResearchToolDefinitionV3(tool ResearchToolDefinitionV3) (ResearchToolDefinitionV3, error) {
	if !validResearchToolTextV3(tool.Name, 128) ||
		!validResearchToolTextV3(tool.Description, 4096) ||
		tool.ImplementationGeneration <= 0 || tool.MaxCostMicroUSD <= 0 ||
		tool.MaxCostMicroUSD > 1_000_000 {
		return ResearchToolDefinitionV3{}, invalidPolicy("research tool grant is invalid")
	}
	var expectedEffects []ResearchToolEffectV3
	switch {
	case tool.Name == "web_search" && tool.Implementation == ResearchToolExaSearchV3,
		tool.Name == "web_contents" && tool.Implementation == ResearchToolExaContentsV3:
		if tool.Provider != "exa" || tool.ResultTrust != ResearchToolTrustExternalV3 ||
			tool.BudgetBucket != "exa_calls" ||
			tool.CredentialRef.validateFor(CredentialIDExaPrimaryV1) != nil {
			return ResearchToolDefinitionV3{}, invalidPolicy("research Tool Exa grant is invalid")
		}
		expectedEffects = []ResearchToolEffectV3{
			ResearchToolEffectBillableV3,
			ResearchToolEffectNetworkReadV3,
			ResearchToolEffectTrustTaintV3,
		}
	case tool.Name == "web_product_status" &&
		tool.Implementation == ResearchToolKimiProductStatusV3:
		if tool.Provider != "kimi" || tool.ResultTrust != ResearchToolTrustOfficialV3 ||
			tool.BudgetBucket != "official_calls" || tool.MaxCostMicroUSD != 1 ||
			tool.CredentialRef != (CredentialRefV1{}) {
			return ResearchToolDefinitionV3{}, invalidPolicy("research Tool official grant is invalid")
		}
		expectedEffects = []ResearchToolEffectV3{
			ResearchToolEffectNetworkReadV3,
			ResearchToolEffectTrustTaintV3,
		}
	default:
		return ResearchToolDefinitionV3{}, invalidPolicy("research tool implementation is unsupported")
	}
	if !slices.Equal(tool.Effects, expectedEffects) {
		return ResearchToolDefinitionV3{}, invalidPolicy("research tool effects are invalid")
	}
	canonicalSchema, err := canonicalResearchToolSchemaV3(tool.Parameters)
	if err != nil {
		return ResearchToolDefinitionV3{}, err
	}
	sum := sha256.Sum256(canonicalSchema)
	expectedSchemaDigest := hex.EncodeToString(sum[:])
	if tool.SchemaDigest != "" && tool.SchemaDigest != expectedSchemaDigest {
		return ResearchToolDefinitionV3{}, invalidPolicy("research tool schema digest differs")
	}
	tool.Parameters = canonicalSchema
	tool.SchemaDigest = expectedSchemaDigest
	tool.Effects = slices.Clone(expectedEffects)
	return tool, nil
}

func canonicalResearchToolSchemaV3(raw json.RawMessage) (json.RawMessage, error) {
	if len(raw) == 0 || len(raw) > 64<<10 {
		return nil, invalidPolicy("research tool schema size is invalid")
	}
	var schema struct {
		Type                 string                     `json:"type"`
		Properties           map[string]json.RawMessage `json:"properties"`
		Required             []string                   `json:"required,omitempty"`
		AdditionalProperties bool                       `json:"additionalProperties"`
	}
	if err := strictjson.DecodeExact(raw, &schema); err != nil || schema.Type != "object" ||
		len(schema.Properties) == 0 || schema.AdditionalProperties {
		return nil, invalidPolicy("research tool schema is not a closed object")
	}
	canonical, err := json.Marshal(schema)
	if err != nil || len(canonical) > 64<<10 {
		return nil, invalidPolicy("research tool schema is invalid")
	}
	return canonical, nil
}

func validResearchToolTextV3(value string, maxBytes int) bool {
	return value != "" && len(value) <= maxBytes && utf8.ValidString(value) &&
		!strings.ContainsRune(value, 0) && strings.TrimSpace(value) == value
}
