package types

import (
	"strings"
	"testing"
)

func TestResearchV3ShadowWorkflowIDIsExact(t *testing.T) {
	valid := ResearchV3ShadowWorkflowIDPrefix + strings.Repeat("a", 64)
	for _, tc := range []struct {
		value string
		want  bool
	}{
		{value: valid, want: true},
		{value: ResearchV3ShadowWorkflowIDPrefix + strings.Repeat("0", 64), want: true},
		{value: ResearchV3ShadowWorkflowIDPrefix + strings.Repeat("A", 64)},
		{value: ResearchV3ShadowWorkflowIDPrefix + strings.Repeat("g", 64)},
		{value: ResearchV3ShadowWorkflowIDPrefix + strings.Repeat("a", 63)},
		{value: ResearchV3ShadowWorkflowIDPrefix + strings.Repeat("a", 65)},
		{value: "scheduled-v3-" + strings.Repeat("a", 64)},
	} {
		if got := IsResearchV3ShadowWorkflowID(tc.value); got != tc.want {
			t.Fatalf("IsResearchV3ShadowWorkflowID(%q)=%v, want %v", tc.value, got, tc.want)
		}
	}
}

func TestResearchRunSnapshotRefV3SealsCutoffAndScope(t *testing.T) {
	digest := strings.Repeat("a", 64)
	identity := RunIdentity{
		TemporalWorkflowID: "workflow-v3", TemporalRunID: "run-v3",
		RunKind: RunSnapshotKindScheduled, TenantID: 7, UserID: 42,
		TaskID: "task-v3",
	}
	sealed, err := SealResearchRunSnapshotRefV3(ResearchRunSnapshotRefV3{
		SnapshotID: 9, TemporalWorkflowID: identity.TemporalWorkflowID,
		TemporalRunID: identity.TemporalRunID, RunKind: identity.RunKind,
		TenantID: identity.TenantID, UserID: identity.UserID, TaskID: identity.TaskID,
		DefinitionVersion: 3, DefinitionDigest: digest,
		CapabilityCatalogDigest: digest, ToolPolicyDigest: digest,
		PromptPolicyDigest: digest, ModelPolicyDigest: digest, QuotaPolicyDigest: digest,
		PlannerBudget: PlannerBudget{MaxPlannerRounds: 8, MaxToolCalls: 16,
			MaxTokens: 32768, MaxCostMicroUSD: 1_000_000, DurationMs: 300_000},
		HistoryThroughUTC: "2026-08-01T12:34:56.123Z", PayloadDigest: digest,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := sealed.ValidateFor(identity); err != nil {
		t.Fatal(err)
	}
	mutations := []func(*ResearchRunSnapshotRefV3){
		func(ref *ResearchRunSnapshotRefV3) { ref.UserID++ },
		func(ref *ResearchRunSnapshotRefV3) { ref.HistoryThroughUTC = "2026-08-01T12:34:56+00:00" },
		func(ref *ResearchRunSnapshotRefV3) { ref.ToolPolicyDigest = strings.Repeat("b", 64) },
		func(ref *ResearchRunSnapshotRefV3) { ref.ReferenceDigest = strings.Repeat("b", 64) },
	}
	for index, mutate := range mutations {
		candidate := sealed
		mutate(&candidate)
		if err := candidate.ValidateFor(identity); err == nil {
			t.Fatalf("mutation %d passed", index)
		}
	}
	invalid := sealed
	invalid.HistoryThroughUTC = "not-a-time"
	invalid.ReferenceDigest = ""
	if _, err := SealResearchRunSnapshotRefV3(invalid); err == nil {
		t.Fatal("invalid cutoff passed sealing")
	}
}

func TestResearchRunPlanRefV3SealsAllScopeFields(t *testing.T) {
	digest := strings.Repeat("a", 64)
	identity := RunIdentity{
		TemporalWorkflowID: "workflow-v3", TemporalRunID: "run-v3",
		RunKind: RunSnapshotKindScheduled, TenantID: 7, UserID: 42,
		TaskID: "task-v3",
	}
	sealed, err := SealResearchRunPlanRefV3(ResearchRunPlanRefV3{
		PlanID: 3, RunSnapshotID: 9,
		TemporalWorkflowID: identity.TemporalWorkflowID,
		TemporalRunID:      identity.TemporalRunID, TenantID: identity.TenantID,
		UserID: identity.UserID, TaskID: identity.TaskID,
		DefinitionDigest: digest, CapabilityCatalogDigest: digest, PlanDigest: digest,
		ToolPolicyDigest: digest,
		StepCount:        2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := sealed.ValidateFor(identity, 9); err != nil {
		t.Fatal(err)
	}
	mutations := []func(*ResearchRunPlanRefV3){
		func(ref *ResearchRunPlanRefV3) { ref.UserID++ },
		func(ref *ResearchRunPlanRefV3) { ref.TaskID += "-other" },
		func(ref *ResearchRunPlanRefV3) { ref.PlanDigest = strings.Repeat("b", 64) },
		func(ref *ResearchRunPlanRefV3) { ref.StepCount-- },
		func(ref *ResearchRunPlanRefV3) { ref.ReferenceDigest = strings.Repeat("b", 64) },
	}
	for index, mutate := range mutations {
		candidate := sealed
		mutate(&candidate)
		if err := candidate.ValidateFor(identity, 9); err == nil {
			t.Fatalf("mutation %d passed", index)
		}
	}
}
