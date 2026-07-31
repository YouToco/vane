// Package runtimeconfig composes versioned non-secret policies for prepared
// task runs. It deliberately accepts only the small set of deployment values
// that may vary; application config and credential values cannot enter it.
package runtimeconfig

import (
	"fmt"

	"github.com/YouToco/vane/acquisitiontool"
	"github.com/YouToco/vane/cardgen"
	"github.com/YouToco/vane/evolver"
	"github.com/YouToco/vane/executivebrief"
	"github.com/YouToco/vane/fetcher"
	"github.com/YouToco/vane/runtimepolicy"
	"github.com/YouToco/vane/scorer"
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
// marked unavailable by their tool definition are intentionally omitted.
func BuildCurrentCompiledV1(input CurrentCompiledV1Input) (runtimepolicy.BundleV1, error) {
	return buildCompiledV1(
		input, cardgen.CurrentPromptStageV1(),
		cardgen.CurrentModelCallV1(input.Model),
		nil, nil, nil,
	)
}

// BuildStructuredInsightCompiledV1 preserves the compiled policy envelope but
// freezes CardGen renderer v2 and its one-call request parameters. Structured
// compiled runtimes, including the Source-free Tool pipeline, share this one
// policy builder instead of maintaining another card-generation policy branch.
func BuildStructuredInsightCompiledV1(
	input CurrentCompiledV1Input,
) (runtimepolicy.BundleV1, error) {
	return buildCompiledV1(
		input, cardgen.StructuredPromptStageV2(),
		cardgen.StructuredModelCallV2(input.Model),
		nil, nil, nil,
	)
}

// BuildExecutiveBriefCompiledV1 extends the structured evidence runtime with
// exactly one issue synthesis stage and one periodic synthesis stage. Legacy
// builders keep both optional fields absent, preserving their canonical bytes.
func BuildExecutiveBriefCompiledV1(
	input CurrentCompiledV1Input,
) (runtimepolicy.BundleV1, error) {
	issuePrompt := executivebrief.CurrentIssuePromptStageV1()
	periodicPrompt := executivebrief.CurrentPeriodicPromptStageV1()
	return buildCompiledV1(
		input, cardgen.StructuredPromptStageV2(),
		cardgen.StructuredModelCallV2(input.Model),
		&issuePrompt, &periodicPrompt,
		[]runtimepolicy.ModelCallV1{
			executivebrief.CurrentIssueModelCallV1(input.Model),
			executivebrief.CurrentPeriodicModelCallV1(input.Model),
		},
	)
}

func buildCompiledV1(
	input CurrentCompiledV1Input,
	cardPrompt runtimepolicy.PromptStageV1,
	cardCall runtimepolicy.ModelCallV1,
	issuePrompt *runtimepolicy.PromptStageV1,
	periodicPrompt *runtimepolicy.PromptStageV1,
	optionalCalls []runtimepolicy.ModelCallV1,
) (runtimepolicy.BundleV1, error) {
	capabilities, err := currentCapabilitiesV1(
		input.ExaCredentialGeneration,
		input.TikHubCredentialGeneration,
	)
	if err != nil {
		return runtimepolicy.BundleV1{}, err
	}
	quotas := currentQuotaBucketsV1()

	modelCalls := []runtimepolicy.ModelCallV1{
		scorer.CurrentModelCallV1(input.Model),
		cardCall,
		evolver.CurrentModelCallV1(input.Model),
	}
	modelCalls = append(modelCalls, optionalCalls...)
	return runtimepolicy.BuildV1(runtimepolicy.BuildInputV1{
		AllowedCapabilities:     capabilities,
		ScorePrompt:             scorer.CurrentPromptStageV1(),
		CardGenPrompt:           cardPrompt,
		ProfileEvolvePrompt:     evolver.CurrentPromptStageV1(),
		IssueSynthesisPrompt:    issuePrompt,
		PeriodicSynthesisPrompt: periodicPrompt,
		TaskInstructionEnabled:  input.TaskInstructionEnabled,
		ModelProvider:           runtimepolicy.ModelProviderDeepSeekV1,
		ModelEndpoint: runtimepolicy.EndpointRefV1{
			ID:         runtimepolicy.EndpointIDDeepSeekCompatiblePrimaryV1,
			Generation: input.ModelEndpointGeneration,
		},
		ModelCredentialRef: runtimepolicy.CredentialRefV1{
			ID:         runtimepolicy.CredentialIDLLMPrimaryV1,
			Generation: input.ModelCredentialGeneration,
		},
		ModelCalls:   modelCalls,
		QuotaBuckets: quotas,
	})
}

func currentCapabilitiesV1(
	exaCredentialGeneration int64,
	tikHubCredentialGeneration int64,
) ([]runtimepolicy.CapabilityV1, error) {
	entries := acquisitiontool.List()
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
	entry acquisitiontool.Entry,
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
	case entry.Platform == types.PlatformWeb && entry.Capability == types.CapProductStatus:
		capability.ImplementationVersion =
			runtimepolicy.CapabilityImplementationProductStatusV1
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
