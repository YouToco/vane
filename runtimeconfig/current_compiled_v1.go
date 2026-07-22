// Package runtimeconfig composes versioned non-secret policies for prepared
// task runs. It deliberately accepts only the small set of deployment values
// that may vary; application config and credential values cannot enter it.
package runtimeconfig

import (
	"fmt"

	"github.com/YouToco/vane/cardgen"
	"github.com/YouToco/vane/evolver"
	"github.com/YouToco/vane/fetcher"
	"github.com/YouToco/vane/runtimepolicy"
	"github.com/YouToco/vane/scorer"
	"github.com/YouToco/vane/sourcecatalog"
	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
)

// CurrentCompiledV1Input contains the complete variable, non-secret input for
// composing the current production compiled policy.
type CurrentCompiledV1Input struct {
	Model                      string
	TaskInstructionEnabled     bool
	ModelEndpointGeneration    int64
	ModelCredentialGeneration  int64
	ExaCredentialGeneration    int64
	TikHubCredentialGeneration int64
}

// BuildCurrentCompiledV1 snapshots the exact currently supported compiled
// capabilities, prompts, model calls, and enforced quota rule. Capabilities
// marked unavailable in sourcecatalog are intentionally omitted.
func BuildCurrentCompiledV1(input CurrentCompiledV1Input) (runtimepolicy.BundleV1, error) {
	capabilities, err := currentCapabilitiesV1(
		input.ExaCredentialGeneration,
		input.TikHubCredentialGeneration,
	)
	if err != nil {
		return runtimepolicy.BundleV1{}, err
	}
	quotas := currentQuotaBucketsV1()

	return runtimepolicy.BuildV1(runtimepolicy.BuildInputV1{
		AllowedCapabilities:    capabilities,
		ScorePrompt:            scorer.CurrentPromptStageV1(),
		CardGenPrompt:          cardgen.CurrentPromptStageV1(),
		ProfileEvolvePrompt:    evolver.CurrentPromptStageV1(),
		TaskInstructionEnabled: input.TaskInstructionEnabled,
		ModelProvider:          runtimepolicy.ModelProviderDeepSeekV1,
		ModelEndpoint: runtimepolicy.EndpointRefV1{
			ID:         runtimepolicy.EndpointIDDeepSeekCompatiblePrimaryV1,
			Generation: input.ModelEndpointGeneration,
		},
		ModelCredentialRef: runtimepolicy.CredentialRefV1{
			ID:         runtimepolicy.CredentialIDLLMPrimaryV1,
			Generation: input.ModelCredentialGeneration,
		},
		ModelCalls: []runtimepolicy.ModelCallV1{
			scorer.CurrentModelCallV1(input.Model),
			cardgen.CurrentModelCallV1(input.Model),
			evolver.CurrentModelCallV1(input.Model),
		},
		QuotaBuckets: quotas,
	})
}

func currentCapabilitiesV1(
	exaCredentialGeneration int64,
	tikHubCredentialGeneration int64,
) ([]runtimepolicy.CapabilityV1, error) {
	entries := sourcecatalog.List()
	capabilities := make([]runtimepolicy.CapabilityV1, 0, len(entries))
	for _, entry := range entries {
		if !entry.Available() {
			continue
		}
		capability, err := currentCapabilityV1(
			entry,
			exaCredentialGeneration,
			tikHubCredentialGeneration,
		)
		if err != nil {
			return nil, err
		}
		capabilities = append(capabilities, capability)
	}
	return capabilities, nil
}

func currentCapabilityV1(
	entry sourcecatalog.Entry,
	exaCredentialGeneration int64,
	tikHubCredentialGeneration int64,
) (runtimepolicy.CapabilityV1, error) {
	capability := runtimepolicy.CapabilityV1{
		Platform:   string(entry.Platform),
		Capability: string(entry.Capability),
		Kind:       string(entry.Kind),
	}

	switch {
	case entry.Platform == types.PlatformWeb && entry.Capability == types.CapFeed:
		capability.ImplementationVersion = runtimepolicy.CapabilityImplementationRSSV1
		capability.DependencyCredentialRefs = []runtimepolicy.CredentialRefV1{{
			ID:         runtimepolicy.CredentialIDExaPrimaryV1,
			Generation: exaCredentialGeneration,
		}}
	case entry.Platform == types.PlatformWeb &&
		(entry.Capability == types.CapSearch || entry.Capability == types.CapContents):
		capability.ImplementationVersion = runtimepolicy.CapabilityImplementationExaV1
		capability.CredentialRef = runtimepolicy.CredentialRefV1{
			ID:         runtimepolicy.CredentialIDExaPrimaryV1,
			Generation: exaCredentialGeneration,
		}
	case fetcher.IsBindingBacked(entry.Platform, entry.Capability):
		capability.ImplementationVersion = runtimepolicy.CapabilityImplementationBindingV1
		capability.CredentialRef = runtimepolicy.CredentialRefV1{
			ID:         runtimepolicy.CredentialIDTikHubPrimaryV1,
			Generation: tikHubCredentialGeneration,
		}
	default:
		return runtimepolicy.CapabilityV1{}, fmt.Errorf(
			"runtimeconfig: available capability %s/%s has no v1 implementation",
			entry.Platform,
			entry.Capability,
		)
	}
	return capability, nil
}

func currentQuotaBucketsV1() []runtimepolicy.QuotaBucketV1 {
	return []runtimepolicy.QuotaBucketV1{{
		Name:               string(store.QuotaLLMTokens),
		Financial:          store.QuotaLLMTokens.IsFinancial(),
		EnforcementVersion: runtimepolicy.QuotaEnforcementLLMPrechargeV1,
	}}
}
