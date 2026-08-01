package workflow

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/YouToco/vane/fetcher"
	storepkg "github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
)

func researchBridgeFixtureV3(t *testing.T) (
	types.RunIdentity, types.ResearchRunSnapshotRefV3,
	types.ResearchRunPlanRefV3, storepkg.ResearchRunStepExecutionV3,
) {
	t.Helper()
	identity := types.RunIdentity{
		TemporalWorkflowID: "workflow-bridge", TemporalRunID: "run-bridge",
		RunKind:  types.RunSnapshotKindScheduled,
		TenantID: 11, UserID: 22, TaskID: "task-bridge",
	}
	snapshot, err := types.SealResearchRunSnapshotRefV3(types.ResearchRunSnapshotRefV3{
		SnapshotID: 33, TemporalWorkflowID: identity.TemporalWorkflowID,
		TemporalRunID: identity.TemporalRunID, RunKind: identity.RunKind,
		TenantID: identity.TenantID, UserID: identity.UserID, TaskID: identity.TaskID,
		DefinitionVersion: 1, DefinitionDigest: strings.Repeat("a", 64),
		CapabilityCatalogDigest: strings.Repeat("b", 64),
		ToolPolicyDigest:        strings.Repeat("c", 64), PromptPolicyDigest: strings.Repeat("d", 64),
		ModelPolicyDigest: strings.Repeat("e", 64), QuotaPolicyDigest: strings.Repeat("f", 64),
		PlannerBudget: types.PlannerBudget{MaxPlannerRounds: 8, MaxToolCalls: 16,
			MaxTokens: 32768, MaxCostMicroUSD: 1_000_000, DurationMs: 300_000},
		HistoryThroughUTC: "2026-08-01T00:00:00Z",
		PayloadDigest:     strings.Repeat("1", 64),
	})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := types.SealResearchRunPlanRefV3(types.ResearchRunPlanRefV3{
		PlanID: 44, RunSnapshotID: snapshot.SnapshotID,
		TemporalWorkflowID: identity.TemporalWorkflowID, TemporalRunID: identity.TemporalRunID,
		TenantID: identity.TenantID, UserID: identity.UserID, TaskID: identity.TaskID,
		DefinitionDigest:        snapshot.DefinitionDigest,
		CapabilityCatalogDigest: snapshot.CapabilityCatalogDigest,
		ToolPolicyDigest:        snapshot.ToolPolicyDigest,
		PlanDigest:              strings.Repeat("2", 64), StepCount: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	return identity, snapshot, plan, storepkg.ResearchRunStepExecutionV3{
		StepID: 55, SpendReservationID: 66, FirstWriter: true,
		Ordinal: 0, InvocationID: "search-official", ToolName: "web_search",
		Arguments: []byte(`{"query":"Kimi"}`), RequestDigest: strings.Repeat("3", 64),
		ReservedQuotaUnits: 1, ReservedCostMicroUSD: 10_000,
	}
}

func TestMapResearchExecutionReceiptV3(t *testing.T) {
	identity, snapshot, plan, execution := researchBridgeFixtureV3(t)
	traceID, err := fetcher.ResearchExecutionTraceV3(
		identity, snapshot.SnapshotID, plan.PlanDigest,
		execution.Ordinal, execution.InvocationID)
	if err != nil {
		t.Fatal(err)
	}
	status := 200
	result := []byte(`{"state":"available"}`)
	sum := sha256.Sum256(result)
	success := fetcher.ResearchExecutionReceiptV3{
		Status:  fetcher.ResearchExecutionSuccessV3,
		TraceID: traceID, Provider: "exa", Attempted: true,
		UsageQuantity: 10, UsageKnown: true, CostMicroUSD: 4_000, CostKnown: true,
		HTTPStatus: &status, DurationMS: 25, Result: result,
		NormalizedResultSize: len(result), ResultDigest: hex.EncodeToString(sum[:]),
	}
	mapped, err := mapResearchExecutionReceiptV3(identity, snapshot, plan, execution, success)
	if err != nil || mapped.Evidence == nil || mapped.Terminal != nil ||
		mapped.Evidence.CostMicroUSD != 4_000 ||
		mapped.Evidence.ProviderCall.PricingStatus != "provider_reported" {
		t.Fatalf("success mapping=%+v err=%v", mapped, err)
	}

	unknown := fetcher.ResearchExecutionReceiptV3{
		Status:  fetcher.ResearchExecutionIndeterminateV3,
		TraceID: traceID, Provider: "exa", Attempted: true,
		HTTPStatus: &status, DurationMS: 25,
		ErrorCode: fetcher.ResearchExecutionProviderCostUnknownV3,
	}
	mapped, err = mapResearchExecutionReceiptV3(identity, snapshot, plan, execution, unknown)
	if err != nil || mapped.Terminal == nil || mapped.Evidence != nil ||
		mapped.Terminal.CostMicroUSD != execution.ReservedCostMicroUSD ||
		mapped.Terminal.ProviderCall.PricingStatus != "unpriced" {
		t.Fatalf("unknown mapping=%+v err=%v", mapped, err)
	}

	unattempted := fetcher.ResearchExecutionReceiptV3{
		Status:    fetcher.ResearchExecutionDefiniteFailureV3,
		ErrorCode: fetcher.ResearchExecutionRouteUnavailableV3,
	}
	mapped, err = mapResearchExecutionReceiptV3(identity, snapshot, plan, execution, unattempted)
	if err != nil || mapped.Terminal == nil || mapped.Terminal.CostMicroUSD != 0 ||
		mapped.Terminal.ProviderCall != (storepkg.ResearchProviderCallV3{}) {
		t.Fatalf("unattempted mapping=%+v err=%v", mapped, err)
	}

	over := success
	over.CostMicroUSD = 12_000
	if _, err := mapResearchExecutionReceiptV3(identity, snapshot, plan, execution, over); err == nil {
		t.Fatal("completed over-cap receipt mapped to evidence")
	}
}

func TestMapResearchExecutionReceiptV3RejectsSwappedClaims(t *testing.T) {
	identity, snapshot, plan, execution := researchBridgeFixtureV3(t)
	traceID, err := fetcher.ResearchExecutionTraceV3(
		identity, snapshot.SnapshotID, plan.PlanDigest,
		execution.Ordinal, execution.InvocationID)
	if err != nil {
		t.Fatal(err)
	}
	status := 503
	receipt := fetcher.ResearchExecutionReceiptV3{
		Status:  fetcher.ResearchExecutionIndeterminateV3,
		TraceID: traceID, Provider: "exa", Attempted: true,
		HTTPStatus: &status, DurationMS: 25,
		ErrorCode: fetcher.ResearchExecutionProviderCostUnknownV3,
	}

	otherOrdinal := execution
	otherOrdinal.Ordinal++
	if _, err := mapResearchExecutionReceiptV3(
		identity, snapshot, plan, otherOrdinal, receipt); err == nil {
		t.Fatal("receipt with a relabeled ordinal and retained invocation was accepted")
	}

	otherIdentity := identity
	otherIdentity.TemporalRunID = "run-bridge-other"
	otherSnapshot, err := types.SealResearchRunSnapshotRefV3(types.ResearchRunSnapshotRefV3{
		SnapshotID:         snapshot.SnapshotID + 1,
		TemporalWorkflowID: otherIdentity.TemporalWorkflowID,
		TemporalRunID:      otherIdentity.TemporalRunID, RunKind: otherIdentity.RunKind,
		TenantID: otherIdentity.TenantID, UserID: otherIdentity.UserID,
		TaskID: otherIdentity.TaskID, DefinitionVersion: snapshot.DefinitionVersion,
		DefinitionDigest:        snapshot.DefinitionDigest,
		CapabilityCatalogDigest: snapshot.CapabilityCatalogDigest,
		ToolPolicyDigest:        snapshot.ToolPolicyDigest,
		PromptPolicyDigest:      snapshot.PromptPolicyDigest,
		ModelPolicyDigest:       snapshot.ModelPolicyDigest,
		QuotaPolicyDigest:       snapshot.QuotaPolicyDigest,
		PlannerBudget:           snapshot.PlannerBudget,
		HistoryThroughUTC:       snapshot.HistoryThroughUTC,
		PayloadDigest:           snapshot.PayloadDigest,
	})
	if err != nil {
		t.Fatal(err)
	}
	otherPlan, err := types.SealResearchRunPlanRefV3(types.ResearchRunPlanRefV3{
		PlanID: plan.PlanID + 1, RunSnapshotID: otherSnapshot.SnapshotID,
		TemporalWorkflowID: otherIdentity.TemporalWorkflowID,
		TemporalRunID:      otherIdentity.TemporalRunID,
		TenantID:           otherIdentity.TenantID, UserID: otherIdentity.UserID,
		TaskID: otherIdentity.TaskID, DefinitionDigest: otherSnapshot.DefinitionDigest,
		CapabilityCatalogDigest: otherSnapshot.CapabilityCatalogDigest,
		ToolPolicyDigest:        otherSnapshot.ToolPolicyDigest,
		PlanDigest:              plan.PlanDigest, StepCount: plan.StepCount,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mapResearchExecutionReceiptV3(
		otherIdentity, otherSnapshot, otherPlan, execution, receipt); err == nil {
		t.Fatal("receipt from another run was accepted")
	}
}
