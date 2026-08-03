package main

import (
	"context"
	"errors"
	"strings"
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

func TestOwnerAgentStartupRequiresPersistentResearchV3Runtime(t *testing.T) {
	if err := requireOwnerAgentResearchV3Runtime(nil); err == nil {
		t.Fatal("nil configuration crossed owner Agent startup Gate")
	}

	dark := &config.Config{}
	// A retained exact authority selector is not a substitute for persistent
	// runtime capability. This also prevents already-enabled database authority
	// from being silently stranded after a dark restart.
	dark.Pipeline.ResearchV3AuthorityCanaryScheduleID = "task-existing"
	err := requireOwnerAgentResearchV3Runtime(dark)
	if err == nil || !strings.Contains(err.Error(),
		"pipeline.research_v3_runtime_enabled=true") {
		t.Fatalf("dark owner Agent startup error=%v", err)
	}

	enabled := &config.Config{}
	enabled.Pipeline.ResearchV3RuntimeEnabled = true
	if err := requireOwnerAgentResearchV3Runtime(enabled); err != nil {
		t.Fatalf("persistent owner Agent runtime rejected: %v", err)
	}
	if !shouldInitializeResearchV3Runtime(enabled) ||
		!shouldEnableResearchV3Delivery(enabled) {
		t.Fatal("enabled owner Agent runtime did not assemble worker and delivery")
	}
}

type fakeResearchV3AuthorityVerifier struct {
	allowed map[string]string
	calls   []types.RunIdentity
}

func (f *fakeResearchV3AuthorityVerifier) VerifyEnabledResearchV3ActionAuthorization(
	_ context.Context, tenantID, userID int64, taskID, token string,
) error {
	f.calls = append(f.calls, types.RunIdentity{
		TenantID: tenantID, UserID: userID, TaskID: taskID,
	})
	if f.allowed[taskID] != token {
		return errors.New("authority rejected")
	}
	return nil
}

func TestResearchV3RuntimeAdmissionUsesExactShadowOrPerTaskAuthority(t *testing.T) {
	cfg := &config.Config{}
	cfg.Pipeline.ResearchV3RuntimeEnabled = true
	cfg.Pipeline.ResearchV3ShadowCanaryScheduleID = "task-shadow"
	cfg.Pipeline.ResearchV3AuthorityCanaryScheduleID = "task-current-cutover"
	verifier := &fakeResearchV3AuthorityVerifier{allowed: map[string]string{
		"task-formal-a": "token-a", "task-formal-b": "token-b",
	}}
	base := types.RunIdentity{
		TemporalWorkflowID: "workflow", TemporalRunID: "run",
		RunKind:  types.RunSnapshotKindScheduled,
		TenantID: 7, UserID: 42, TaskID: "task-formal-a",
	}
	if !shouldInitializeResearchV3Runtime(cfg) || !shouldEnableResearchV3Delivery(cfg) {
		t.Fatal("persistent V3 runtime capability was not enabled")
	}
	for _, tc := range []struct {
		task, token string
	}{{"task-formal-a", "token-a"}, {"task-formal-b", "token-b"}} {
		identity := base
		identity.TaskID = tc.task
		allowed, err := authorizeResearchV3Runtime(
			context.Background(), cfg, verifier, identity, tc.token,
		)
		if err != nil || !allowed {
			t.Fatalf("formal task %s was not admitted: allowed=%v err=%v", tc.task, allowed, err)
		}
	}
	if len(verifier.calls) != 2 {
		t.Fatalf("authority verifier calls=%d, want 2", len(verifier.calls))
	}
	if allowed, err := authorizeResearchV3Runtime(
		context.Background(), cfg, verifier, base, "token-b",
	); err == nil || allowed {
		t.Fatal("cross-task token was admitted")
	}
	shadow := base
	shadow.TaskID = "task-shadow"
	if allowed, err := authorizeResearchV3Runtime(
		context.Background(), cfg, verifier, shadow, "",
	); err != nil || !allowed {
		t.Fatalf("exact tokenless shadow rejected: allowed=%v err=%v", allowed, err)
	}
	shadow.TaskID = "task-current-cutover"
	if allowed, err := authorizeResearchV3Runtime(
		context.Background(), cfg, verifier, shadow, "",
	); err != nil || allowed {
		t.Fatal("current cutover ID incorrectly authorized a tokenless formal run")
	}
	missingTask := base
	missingTask.TaskID = ""
	if allowed, err := authorizeResearchV3Runtime(
		context.Background(), cfg, verifier, missingTask, "token-a",
	); err != nil || allowed {
		t.Fatal("empty task identity crossed runtime admission")
	}

	cfg.Pipeline.ResearchV3RuntimeEnabled = false
	cfg.Pipeline.ResearchV3ShadowCanaryScheduleID = ""
	cfg.Pipeline.ResearchV3AuthorityCanaryScheduleID = ""
	if shouldInitializeResearchV3Runtime(cfg) || shouldEnableResearchV3Delivery(cfg) {
		t.Fatal("fully dark configuration expanded V3 runtime")
	}
}
