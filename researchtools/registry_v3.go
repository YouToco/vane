// Package researchtools owns the scheduled-research-only Tool registry.
// It deliberately has no dependency on the interactive Agent loop.
package researchtools

import (
	"encoding/json"
	"errors"

	"github.com/YouToco/vane/acquisitiontool"
	"github.com/YouToco/vane/runtimepolicy"
)

type RegistryV3 struct {
	policy runtimepolicy.ResearchToolPolicyV3
	digest string
	tools  map[string]runtimepolicy.ResearchToolDefinitionV3
}

// BuildScheduledWebPolicyV3 derives model-visible grants from the exact
// retained acquisition contracts and a frozen fetch capability catalog.
// maxCostMicroUSD is a conservative per-call admission ceiling, not a claim
// about actual provider cost; execution still requires a provider receipt.
func BuildScheduledWebPolicyV3(
	capabilities runtimepolicy.CapabilityCatalogV1,
	maxCostMicroUSD int64,
) (runtimepolicy.ResearchToolPolicyV3, error) {
	if err := capabilities.Validate(); err != nil || maxCostMicroUSD <= 0 {
		return runtimepolicy.ResearchToolPolicyV3{}, errors.New("researchtools: invalid policy input")
	}
	names := []string{"web_search", "web_contents"}
	tools := make([]runtimepolicy.ResearchToolDefinitionV3, 0, len(names))
	for _, name := range names {
		definition, ok := acquisitiontool.LookupModelToolDefinitionV1(name)
		if !ok {
			return runtimepolicy.ResearchToolPolicyV3{}, errors.New("researchtools: retained definition is unavailable")
		}
		capability, ok := findCapability(capabilities, definition.Contract)
		if !ok || capability.ImplementationVersion != runtimepolicy.CapabilityImplementationExaV1 ||
			capability.CredentialRef.ID != runtimepolicy.CredentialIDExaPrimaryV1 {
			return runtimepolicy.ResearchToolPolicyV3{}, errors.New("researchtools: exact fetch route is unavailable")
		}
		implementation := runtimepolicy.ResearchToolExaSearchV3
		if name == "web_contents" {
			implementation = runtimepolicy.ResearchToolExaContentsV3
		}
		tools = append(tools, runtimepolicy.ResearchToolDefinitionV3{
			Name: name, Description: definition.Description,
			Parameters:     definition.ArgumentsSchema,
			Implementation: implementation, ImplementationGeneration: 1,
			Provider: "exa", Effects: []runtimepolicy.ResearchToolEffectV3{
				runtimepolicy.ResearchToolEffectBillableV3,
				runtimepolicy.ResearchToolEffectNetworkReadV3,
				runtimepolicy.ResearchToolEffectTrustTaintV3,
			},
			ResultTrust:  runtimepolicy.ResearchToolTrustExternalV3,
			BudgetBucket: "exa_calls", CredentialRef: capability.CredentialRef,
			MaxCostMicroUSD: maxCostMicroUSD,
		})
	}
	return runtimepolicy.BuildResearchToolPolicyV3(tools)
}

func NewRegistryV3(policy runtimepolicy.ResearchToolPolicyV3) (*RegistryV3, error) {
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	digest, err := runtimepolicy.DigestResearchToolPolicyV3(policy)
	if err != nil {
		return nil, err
	}
	tools := make(map[string]runtimepolicy.ResearchToolDefinitionV3, len(policy.AllowedTools))
	for _, tool := range policy.AllowedTools {
		tools[tool.Name] = tool
	}
	return &RegistryV3{policy: policy, digest: digest, tools: tools}, nil
}

func (r *RegistryV3) Digest() string {
	if r == nil {
		return ""
	}
	return r.digest
}

func (r *RegistryV3) Policy() runtimepolicy.ResearchToolPolicyV3 {
	if r == nil {
		return runtimepolicy.ResearchToolPolicyV3{}
	}
	payload, err := runtimepolicy.EncodeResearchToolPolicyV3(r.policy)
	if err != nil {
		return runtimepolicy.ResearchToolPolicyV3{}
	}
	copy, err := runtimepolicy.DecodeResearchToolPolicyV3(payload)
	if err != nil {
		return runtimepolicy.ResearchToolPolicyV3{}
	}
	return copy
}

// Canonicalize rejects catalog drift before invoking the retained acquisition
// decoder. Unknown fields, unsupported locators and non-materializable targets
// fail before any external effect.
func (r *RegistryV3) Canonicalize(
	expectedDigest, toolName string,
	raw json.RawMessage,
) (json.RawMessage, runtimepolicy.ResearchToolDefinitionV3, error) {
	if r == nil || expectedDigest == "" || expectedDigest != r.digest {
		return nil, runtimepolicy.ResearchToolDefinitionV3{}, errors.New("researchtools: catalog digest differs")
	}
	tool, ok := r.tools[toolName]
	if !ok {
		return nil, runtimepolicy.ResearchToolDefinitionV3{}, errors.New("researchtools: tool is not allowed")
	}
	canonical, err := acquisitiontool.CanonicalizeToolArgumentsV1(toolName, raw)
	if err != nil {
		return nil, runtimepolicy.ResearchToolDefinitionV3{}, err
	}
	if err := ValidateCanonicalArgumentsV3(toolName, canonical); err != nil {
		return nil, runtimepolicy.ResearchToolDefinitionV3{}, err
	}
	return canonical, tool, nil
}

// ValidateCanonicalArgumentsV3 applies scheduled-only route constraints after
// generic schema canonicalization. Legacy ProductStatusFetcher callers retain
// their existing URL compatibility.
func ValidateCanonicalArgumentsV3(toolName string, canonical json.RawMessage) error {
	if toolName != "web_product_status" {
		return nil
	}
	var args struct {
		PageURL string `json:"page_url"`
	}
	if err := json.Unmarshal(canonical, &args); err != nil ||
		args.PageURL != "https://www.kimi.com/membership/pricing" {
		return errors.New("researchtools: official product-status route is not exact")
	}
	return nil
}

func findCapability(
	catalog runtimepolicy.CapabilityCatalogV1,
	contract acquisitiontool.ToolContractV1,
) (runtimepolicy.CapabilityV1, bool) {
	for _, capability := range catalog.Allowed {
		if capability.Platform == string(contract.Platform) &&
			capability.Capability == string(contract.Capability) &&
			capability.Kind == string(contract.Kind) &&
			capability.ImplementationVersion == contract.ImplementationVersion {
			return capability, true
		}
	}
	return runtimepolicy.CapabilityV1{}, false
}
