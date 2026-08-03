package main

import (
	"testing"

	"github.com/YouToco/vane/config"
	"github.com/YouToco/vane/types"
)

func TestNativeResearchV3CreationPolicyIsBounded(t *testing.T) {
	policy := nativeResearchV3CreationPolicy()
	if err := policy.PlannerBudget.ValidateForMode(types.ExecutionModeDiscoverAtRun); err != nil {
		t.Fatalf("native V3 creation policy is invalid: %v", err)
	}
	if policy.PlannerBudget.MaxPlannerRounds != 8 ||
		policy.PlannerBudget.MaxToolCalls != 16 ||
		policy.PlannerBudget.MaxTokens != 32_768 ||
		policy.PlannerBudget.MaxCostMicroUSD != 1_000_000 ||
		policy.PlannerBudget.DurationMs != 300_000 {
		t.Fatalf("native V3 creation policy drifted: %+v", policy.PlannerBudget)
	}
}

func TestResearchV3RuntimeAdmissionIsExactOwnerCanaryOrRetainedTask(t *testing.T) {
	cfg := &config.Config{}
	cfg.Agent.AgentFirstOwnerCanary = true
	cfg.Agent.AgentFirstCanaryUserID = 42
	cfg.Pipeline.ResearchV3ShadowCanaryScheduleID = "task-shadow"
	cfg.Pipeline.ResearchV3AuthorityCanaryScheduleID = "task-authority"
	base := types.RunIdentity{
		TemporalWorkflowID: "workflow", TemporalRunID: "run",
		RunKind:  types.RunSnapshotKindScheduled,
		TenantID: 7, UserID: 42, TaskID: "task-new-native-v3",
	}
	if !shouldInitializeResearchV3Runtime(cfg) || !shouldEnableResearchV3Delivery(cfg) ||
		!researchV3RuntimeAdmissionAllowed(cfg, base) {
		t.Fatal("exact owner canary native V3 task was not admitted")
	}
	otherUser := base
	otherUser.UserID = 43
	if researchV3RuntimeAdmissionAllowed(cfg, otherUser) {
		t.Fatal("other user crossed exact owner canary admission")
	}
	missingTask := base
	missingTask.TaskID = ""
	if researchV3RuntimeAdmissionAllowed(cfg, missingTask) {
		t.Fatal("empty task identity crossed owner canary admission")
	}
	for _, retainedTask := range []string{"task-shadow", "task-authority"} {
		retained := otherUser
		retained.TaskID = retainedTask
		if !researchV3RuntimeAdmissionAllowed(cfg, retained) {
			t.Fatalf("retained exact-task canary %s stopped working", retainedTask)
		}
	}
	cfg.Agent.AgentFirstOwnerCanary = false
	if researchV3RuntimeAdmissionAllowed(cfg, base) {
		t.Fatal("disabled owner canary admitted a new task")
	}
	cfg.Pipeline.ResearchV3ShadowCanaryScheduleID = ""
	cfg.Pipeline.ResearchV3AuthorityCanaryScheduleID = ""
	if shouldInitializeResearchV3Runtime(cfg) || shouldEnableResearchV3Delivery(cfg) {
		t.Fatal("empty canary configuration expanded V3 runtime")
	}
}
