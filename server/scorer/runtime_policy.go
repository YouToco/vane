package scorer

import (
	"context"
	"fmt"

	"github.com/YouToco/vane/llm"
	"github.com/YouToco/vane/runtimepolicy"
	"github.com/YouToco/vane/types"
)

// RendererVersionV1 identifies the deterministic scorer user-prompt renderer.
// Historical V1 snapshots keep using this implementation even after a newer
// renderer is introduced.
const RendererVersionV1 = "scorer.render/v1"

// PolicyV1 is a validated scorer execution policy derived from an immutable
// run snapshot. Its fields are deliberately private so callers cannot forge a
// partially validated policy.
type PolicyV1 struct {
	isPrepared bool
	execution  scoreExecutionV1
}

type scoreExecutionV1 struct {
	client                 *llm.Client
	systemPrompt           string
	model                  string
	temperature            float32
	maxTokens              int
	disableThinking        bool
	taskInstructionEnabled bool
	quotaRule              *runtimepolicy.QuotaBucketV1
}

// PrepareCompiledPolicyV1 additionally binds the immutable quota rule actually
// used by llm.Do. PreparePolicyV1 remains available for legacy compatibility
// tests and non-compiled callers.
func PrepareCompiledPolicyV1(
	prompts runtimepolicy.PromptPolicyV1,
	models runtimepolicy.ModelPolicyV1,
	quotas runtimepolicy.QuotaPolicyV1,
	client *llm.Client,
) (PolicyV1, error) {
	if client == nil {
		return PolicyV1{}, fmt.Errorf("%w: scorer model executor is missing", runtimepolicy.ErrInvalidPolicy)
	}
	policy, err := PreparePolicyV1(prompts, models)
	if err != nil {
		return PolicyV1{}, err
	}
	if err := quotas.Validate(); err != nil {
		return PolicyV1{}, fmt.Errorf("scorer: validate quota policy v1: %w", err)
	}
	quota, ok := quotas.Bucket("llm_tokens")
	if !ok {
		return PolicyV1{}, fmt.Errorf("%w: scorer llm quota is missing", runtimepolicy.ErrInvalidPolicy)
	}
	policy.execution.quotaRule = &quota
	policy.execution.client = client
	return policy, nil
}

// CurrentPromptStageV1 returns the exact prompt body and renderer generation
// used by the legacy scorer today.
func CurrentPromptStageV1() runtimepolicy.PromptStageV1 {
	return runtimepolicy.PromptStageV1{
		SystemPrompt:    scoreSystemPrompt,
		RendererVersion: RendererVersionV1,
	}
}

// CurrentModelCallV1 returns the exact model-visible request parameters used
// by the legacy scorer today. model is explicit because it is frozen per run.
func CurrentModelCallV1(model string) runtimepolicy.ModelCallV1 {
	return runtimepolicy.ModelCallV1{
		Stage:           runtimepolicy.ModelStageScore,
		Model:           model,
		Temperature:     0,
		MaxTokens:       16,
		DisableThinking: true,
	}
}

// PreparePolicyV1 validates and narrows a complete snapshot policy to the
// scorer fields supported by this worker generation.
func PreparePolicyV1(
	prompts runtimepolicy.PromptPolicyV1,
	models runtimepolicy.ModelPolicyV1,
) (PolicyV1, error) {
	if err := prompts.Validate(); err != nil {
		return PolicyV1{}, fmt.Errorf("scorer: validate prompt policy v1: %w", err)
	}
	if err := models.Validate(); err != nil {
		return PolicyV1{}, fmt.Errorf("scorer: validate model policy v1: %w", err)
	}
	if prompts.Score.RendererVersion != RendererVersionV1 {
		return PolicyV1{}, fmt.Errorf(
			"%w: scorer renderer version is unsupported",
			runtimepolicy.ErrInvalidPolicy,
		)
	}
	call, ok := models.Call(runtimepolicy.ModelStageScore)
	if !ok {
		return PolicyV1{}, fmt.Errorf(
			"%w: scorer model stage is missing",
			runtimepolicy.ErrInvalidPolicy,
		)
	}
	return PolicyV1{
		isPrepared: true,
		execution: scoreExecutionV1{
			systemPrompt:           prompts.Score.SystemPrompt,
			model:                  call.Model,
			temperature:            float32(call.Temperature),
			maxTokens:              call.MaxTokens,
			disableThinking:        call.DisableThinking,
			taskInstructionEnabled: prompts.TaskInstructionEnabled,
		},
	}, nil
}

// ScoreWithPolicyV1 executes one score call from a validated immutable run
// policy. A zero or forged policy fails before reading profile state or making
// an upstream call.
func (sc *Scorer) ScoreWithPolicyV1(
	ctx context.Context,
	tenantID int64,
	userID int64,
	item types.ContentItem,
	traceID string,
	taskInstruction string,
	policy PolicyV1,
	beforeSpend func(context.Context, float64) error,
) (float64, error) {
	if !policy.isPrepared {
		return 0, fmt.Errorf("%w: scorer policy v1 is not prepared", runtimepolicy.ErrInvalidPolicy)
	}
	return sc.score(ctx, tenantID, userID, item, traceID, taskInstruction, policy.execution, beforeSpend)
}

func legacyScoreExecutionV1() scoreExecutionV1 {
	return scoreExecutionV1{
		systemPrompt:           scoreSystemPrompt,
		temperature:            0,
		maxTokens:              16,
		disableThinking:        true,
		taskInstructionEnabled: true,
	}
}
