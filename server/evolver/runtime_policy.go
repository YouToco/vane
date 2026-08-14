package evolver

import (
	"context"
	"fmt"
	"time"

	"github.com/YouToco/vane/server/llm"
	"github.com/YouToco/vane/server/runtimepolicy"
)

// RendererVersionV1 identifies the deterministic profile-evolution prompt
// renderer retained for historical V1 run snapshots.
const RendererVersionV1 = "evolver.render/v1"

// PolicyV1 is a validated profile-evolution execution policy. Private fields
// prevent callers from constructing an unsupported renderer or partial call.
type PolicyV1 struct {
	isPrepared bool
	execution  evolveExecutionV1
}

// CompiledProfileWritesV1 is the exact-tenant write boundary supplied by the
// Activity. Each callback revalidates the sealed run and performs live
// authorization plus the CAS write in one database transaction.
type CompiledProfileWritesV1 struct {
	Evolve        func(context.Context, string, []string, int64, time.Time, int64, int64, int64) error
	AdvanceCursor func(context.Context, int64, time.Time, int64, int64, int64) error
}

type evolveExecutionV1 struct {
	client          *llm.Client
	systemPrompt    string
	model           string
	temperature     float32
	maxTokens       int
	disableThinking bool
	quotaRule       *runtimepolicy.QuotaBucketV1
}

// PrepareCompiledPolicyV1 additionally binds the immutable quota rule used by
// the compiled profile-evolution LLM call.
func PrepareCompiledPolicyV1(
	prompts runtimepolicy.PromptPolicyV1,
	models runtimepolicy.ModelPolicyV1,
	quotas runtimepolicy.QuotaPolicyV1,
	client *llm.Client,
) (PolicyV1, error) {
	if client == nil {
		return PolicyV1{}, fmt.Errorf("%w: evolver model executor is missing", runtimepolicy.ErrInvalidPolicy)
	}
	policy, err := PreparePolicyV1(prompts, models)
	if err != nil {
		return PolicyV1{}, err
	}
	if err := quotas.Validate(); err != nil {
		return PolicyV1{}, fmt.Errorf("evolver: validate quota policy v1: %w", err)
	}
	quota, ok := quotas.Bucket("llm_tokens")
	if !ok {
		return PolicyV1{}, fmt.Errorf("%w: evolver llm quota is missing", runtimepolicy.ErrInvalidPolicy)
	}
	policy.execution.quotaRule = &quota
	policy.execution.client = client
	return policy, nil
}

// CurrentPromptStageV1 returns the exact prompt and renderer used by legacy
// profile evolution today.
func CurrentPromptStageV1() runtimepolicy.PromptStageV1 {
	return runtimepolicy.PromptStageV1{
		SystemPrompt:    evolveSystemPrompt,
		RendererVersion: RendererVersionV1,
	}
}

// CurrentModelCallV1 returns the current profile-evolution request parameters.
func CurrentModelCallV1(model string) runtimepolicy.ModelCallV1 {
	return runtimepolicy.ModelCallV1{
		Stage:           runtimepolicy.ModelStageProfileEvolve,
		Model:           model,
		Temperature:     0,
		MaxTokens:       800,
		DisableThinking: true,
	}
}

// PreparePolicyV1 validates and narrows a complete snapshot policy to the
// profile-evolution fields supported by this worker generation.
func PreparePolicyV1(
	prompts runtimepolicy.PromptPolicyV1,
	models runtimepolicy.ModelPolicyV1,
) (PolicyV1, error) {
	if err := prompts.Validate(); err != nil {
		return PolicyV1{}, fmt.Errorf("evolver: validate prompt policy v1: %w", err)
	}
	if err := models.Validate(); err != nil {
		return PolicyV1{}, fmt.Errorf("evolver: validate model policy v1: %w", err)
	}
	if prompts.ProfileEvolve.RendererVersion != RendererVersionV1 {
		return PolicyV1{}, fmt.Errorf(
			"%w: evolver renderer version is unsupported",
			runtimepolicy.ErrInvalidPolicy,
		)
	}
	call, ok := models.Call(runtimepolicy.ModelStageProfileEvolve)
	if !ok {
		return PolicyV1{}, fmt.Errorf(
			"%w: evolver model stage is missing",
			runtimepolicy.ErrInvalidPolicy,
		)
	}
	return PolicyV1{
		isPrepared: true,
		execution: evolveExecutionV1{
			systemPrompt:    prompts.ProfileEvolve.SystemPrompt,
			model:           call.Model,
			temperature:     float32(call.Temperature),
			maxTokens:       call.MaxTokens,
			disableThinking: call.DisableThinking,
		},
	}, nil
}

// EvolveWithPolicyV1 executes profile evolution from a validated immutable
// run policy. A zero policy fails before reading profile or feedback state.
func (e *Evolver) EvolveWithPolicyV1(
	ctx context.Context,
	tenantID int64,
	userID int64,
	traceID string,
	policy PolicyV1,
	beforeSpend func(context.Context, float64) error,
	writes CompiledProfileWritesV1,
) error {
	if !policy.isPrepared {
		return fmt.Errorf("%w: evolver policy v1 is not prepared", runtimepolicy.ErrInvalidPolicy)
	}
	if writes.Evolve == nil || writes.AdvanceCursor == nil {
		return fmt.Errorf("%w: compiled profile writes are missing", runtimepolicy.ErrInvalidPolicy)
	}
	return e.evolve(ctx, tenantID, userID, traceID, policy.execution, beforeSpend, &writes)
}

func legacyEvolveExecutionV1() evolveExecutionV1 {
	return evolveExecutionV1{
		systemPrompt:    evolveSystemPrompt,
		temperature:     0,
		maxTokens:       800,
		disableThinking: true,
	}
}
