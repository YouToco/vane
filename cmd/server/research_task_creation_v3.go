package main

import (
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
	return cfg != nil && (cfg.Pipeline.ResearchV3ShadowCanaryScheduleID != "" ||
		cfg.Pipeline.ResearchV3AuthorityCanaryScheduleID != "" ||
		cfg.Agent.AgentFirstOwnerCanary)
}

func shouldEnableResearchV3Delivery(cfg *config.Config) bool {
	return cfg != nil && (cfg.Pipeline.ResearchV3AuthorityCanaryScheduleID != "" ||
		cfg.Agent.AgentFirstOwnerCanary)
}

// researchV3RuntimeAdmissionAllowed preserves both existing exact-task
// canaries and admits only tasks owned by the exact Agent-first canary user.
// Identity is the Store-sealed run identity; model arguments cannot select it.
func researchV3RuntimeAdmissionAllowed(
	cfg *config.Config,
	identity types.RunIdentity,
) bool {
	if cfg == nil || identity.TaskID == "" {
		return false
	}
	if identity.TaskID == cfg.Pipeline.ResearchV3ShadowCanaryScheduleID ||
		identity.TaskID == cfg.Pipeline.ResearchV3AuthorityCanaryScheduleID {
		return true
	}
	return cfg.Agent.AgentFirstOwnerCanary &&
		cfg.Agent.AgentFirstCanaryUserID > 0 &&
		identity.UserID == cfg.Agent.AgentFirstCanaryUserID
}
