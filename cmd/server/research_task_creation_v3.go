package main

import (
	"context"
	"errors"

	"github.com/YouToco/vane/config"
	"github.com/YouToco/vane/task"
	"github.com/YouToco/vane/types"
)

// nativeResearchV3CreationPolicy is trusted deployment policy, never model
// input. These bounded defaults match the V3 workflow's production-tested
// envelope; tenant quota remains the independent spend authority.
func nativeResearchV3CreationPolicy() task.CreationV3ServerPolicy {
	return task.CreationV3ServerPolicy{PlannerBudget: types.PlannerBudget{
		MaxPlannerRounds: 8,
		MaxToolCalls:     16,
		MaxTokens:        32_768,
		MaxCostMicroUSD:  1_000_000,
		DurationMs:       300_000,
	}}
}

func shouldInitializeResearchV3Runtime(cfg *config.Config) bool {
	return cfg != nil && (cfg.Pipeline.ResearchV3RuntimeEnabled ||
		cfg.Pipeline.ResearchV3ShadowCanaryScheduleID != "" ||
		cfg.Pipeline.ResearchV3AuthorityCanaryScheduleID != "" ||
		cfg.Agent.AgentFirstOwnerCanary)
}

func shouldEnableResearchV3Delivery(cfg *config.Config) bool {
	return cfg != nil && (cfg.Pipeline.ResearchV3RuntimeEnabled ||
		cfg.Pipeline.ResearchV3AuthorityCanaryScheduleID != "" ||
		cfg.Agent.AgentFirstOwnerCanary)
}

type researchV3ActionAuthorityVerifier interface {
	VerifyEnabledResearchV3ActionAuthorization(
		context.Context, int64, int64, string, string,
	) error
}

// authorizeResearchV3Runtime preserves the exact-task, tokenless shadow lane.
// Every formal Action is admitted only by its own enabled database authority;
// a process-wide runtime flag or the currently selected cutover task is never
// task authority.
func authorizeResearchV3Runtime(
	ctx context.Context,
	cfg *config.Config,
	verifier researchV3ActionAuthorityVerifier,
	identity types.RunIdentity,
	authorityToken string,
) (bool, error) {
	if cfg == nil || identity.TaskID == "" {
		return false, nil
	}
	if authorityToken == "" {
		return identity.TaskID == cfg.Pipeline.ResearchV3ShadowCanaryScheduleID, nil
	}
	if verifier == nil {
		return false, errors.New("research V3 authority verifier is unavailable")
	}
	if err := verifier.VerifyEnabledResearchV3ActionAuthorization(
		ctx, identity.TenantID, identity.UserID, identity.TaskID, authorityToken,
	); err != nil {
		return false, err
	}
	return true, nil
}
