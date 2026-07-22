package cardgen

import (
	"context"
	"fmt"

	"github.com/YouToco/vane/llm"
	"github.com/YouToco/vane/runtimepolicy"
	"github.com/YouToco/vane/types"
)

// RendererVersionV1 identifies the deterministic card-generation user-prompt
// renderer retained for historical V1 run snapshots.
const RendererVersionV1 = "cardgen.render/v1"

// PolicyV1 is a validated card-generation execution policy. Private fields
// prevent callers from bypassing PreparePolicyV1 with a partial policy.
type PolicyV1 struct {
	isPrepared bool
	execution  cardExecutionV1
}

type cardExecutionV1 struct {
	client                 *llm.Client
	systemPrompt           string
	model                  string
	temperature            float32
	maxTokens              int
	disableThinking        bool
	taskInstructionEnabled bool
	quotaRule              *runtimepolicy.QuotaBucketV1
}

// PrepareCompiledPolicyV1 additionally binds the immutable quota rule used by
// the compiled LLM call.
func PrepareCompiledPolicyV1(
	prompts runtimepolicy.PromptPolicyV1,
	models runtimepolicy.ModelPolicyV1,
	quotas runtimepolicy.QuotaPolicyV1,
	client *llm.Client,
) (PolicyV1, error) {
	if client == nil {
		return PolicyV1{}, fmt.Errorf("%w: cardgen model executor is missing", runtimepolicy.ErrInvalidPolicy)
	}
	policy, err := PreparePolicyV1(prompts, models)
	if err != nil {
		return PolicyV1{}, err
	}
	if err := quotas.Validate(); err != nil {
		return PolicyV1{}, fmt.Errorf("cardgen: validate quota policy v1: %w", err)
	}
	quota, ok := quotas.Bucket("llm_tokens")
	if !ok {
		return PolicyV1{}, fmt.Errorf("%w: cardgen llm quota is missing", runtimepolicy.ErrInvalidPolicy)
	}
	policy.execution.quotaRule = &quota
	policy.execution.client = client
	return policy, nil
}

// CurrentPromptStageV1 returns the exact prompt and renderer used by legacy
// card generation today.
func CurrentPromptStageV1() runtimepolicy.PromptStageV1 {
	return runtimepolicy.PromptStageV1{
		SystemPrompt:    cardSystemPrompt,
		RendererVersion: RendererVersionV1,
	}
}

// CurrentModelCallV1 returns the current card-generation request parameters.
func CurrentModelCallV1(model string) runtimepolicy.ModelCallV1 {
	return runtimepolicy.ModelCallV1{
		Stage:           runtimepolicy.ModelStageCardGen,
		Model:           model,
		Temperature:     0.7,
		MaxTokens:       400,
		DisableThinking: true,
	}
}

// PreparePolicyV1 validates and narrows a complete snapshot policy to the
// card-generation fields supported by this worker generation.
func PreparePolicyV1(
	prompts runtimepolicy.PromptPolicyV1,
	models runtimepolicy.ModelPolicyV1,
) (PolicyV1, error) {
	if err := prompts.Validate(); err != nil {
		return PolicyV1{}, fmt.Errorf("cardgen: validate prompt policy v1: %w", err)
	}
	if err := models.Validate(); err != nil {
		return PolicyV1{}, fmt.Errorf("cardgen: validate model policy v1: %w", err)
	}
	if prompts.CardGen.RendererVersion != RendererVersionV1 {
		return PolicyV1{}, fmt.Errorf(
			"%w: cardgen renderer version is unsupported",
			runtimepolicy.ErrInvalidPolicy,
		)
	}
	call, ok := models.Call(runtimepolicy.ModelStageCardGen)
	if !ok {
		return PolicyV1{}, fmt.Errorf(
			"%w: cardgen model stage is missing",
			runtimepolicy.ErrInvalidPolicy,
		)
	}
	return PolicyV1{
		isPrepared: true,
		execution: cardExecutionV1{
			systemPrompt:           prompts.CardGen.SystemPrompt,
			model:                  call.Model,
			temperature:            float32(call.Temperature),
			maxTokens:              call.MaxTokens,
			disableThinking:        call.DisableThinking,
			taskInstructionEnabled: prompts.TaskInstructionEnabled,
		},
	}, nil
}

// GenerateWithPolicyV1 executes card generation from a validated immutable
// run policy. A zero policy is rejected before profile reads or LLM calls.
func (cg *CardGen) GenerateWithPolicyV1(
	ctx context.Context,
	tenantID int64,
	userID int64,
	item types.ScoredItem,
	traceID string,
	taskInstruction string,
	policy PolicyV1,
	beforeSpend func(context.Context, float64) error,
) (string, error) {
	if !policy.isPrepared {
		return "", fmt.Errorf("%w: cardgen policy v1 is not prepared", runtimepolicy.ErrInvalidPolicy)
	}
	return cg.generate(ctx, tenantID, userID, item, traceID, taskInstruction, policy.execution, beforeSpend)
}

func legacyCardExecutionV1() cardExecutionV1 {
	return cardExecutionV1{
		systemPrompt:           cardSystemPrompt,
		temperature:            0.7,
		maxTokens:              400,
		disableThinking:        true,
		taskInstructionEnabled: true,
	}
}
