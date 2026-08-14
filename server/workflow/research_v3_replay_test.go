package workflow

import (
	"bytes"
	"strings"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/worker"
	sdkworkflow "go.temporal.io/sdk/workflow"

	"github.com/YouToco/vane/types"
)

// legacyResearchStepReceiptV3 is the exact pre-terminal-union Activity wire.
// It deliberately has no phase field; using the current Go struct here would
// not prove compatibility with already persisted Temporal payloads.
type legacyResearchStepReceiptV3 struct {
	StepID        int64  `json:"step_id"`
	Ordinal       int    `json:"ordinal"`
	InvocationID  string `json:"invocation_id"`
	ToolName      string `json:"tool_name"`
	RequestDigest string `json:"request_digest"`
	ResultDigest  string `json:"result_digest"`
	EvidenceID    int64  `json:"evidence_id"`
}

func TestResearchStepReceiptV3LegacyPhaseCompatibilityIsSuccessOnly(t *testing.T) {
	legacy := ResearchStepReceiptV3{
		StepID: 31, Ordinal: 0, InvocationID: "search-official",
		ToolName: "web_search", RequestDigest: strings.Repeat("c", 64),
		ResultDigest: strings.Repeat("d", 64), EvidenceID: 41,
	}
	if err := legacy.Validate(0); err != nil {
		t.Fatalf("exact legacy success rejected: %v", err)
	}
	for name, mutate := range map[string]func(*ResearchStepReceiptV3){
		"missing evidence":      func(value *ResearchStepReceiptV3) { value.EvidenceID = 0 },
		"missing result digest": func(value *ResearchStepReceiptV3) { value.ResultDigest = "" },
		"failure code":          func(value *ResearchStepReceiptV3) { value.ErrorCode = "provider_rejected" },
	} {
		t.Run(name, func(t *testing.T) {
			invalid := legacy
			mutate(&invalid)
			if err := invalid.Validate(0); err == nil {
				t.Fatal("empty-phase non-success receipt was accepted")
			}
		})
	}
}

func TestResearchShadowWorkflowV3ReplaysLegacyCompletedStepWithoutPhase(t *testing.T) {
	const (
		workflowID = "research-v3-shadow-legacy-receipt-replay"
		runID      = "00000000-0000-4000-8000-000000000107"
		traceID    = "9f1d6c5e-0000-4000-8000-researchv3legacy"
	)
	input := ResearchShadowInputV3{TenantID: 7, UserID: 9, TaskID: "task-v3-replay"}
	digest := strings.Repeat("a", 64)
	snapshot, err := types.SealResearchRunSnapshotRefV3(types.ResearchRunSnapshotRefV3{
		SnapshotID: 11, TemporalWorkflowID: workflowID, TemporalRunID: runID,
		RunKind: types.RunSnapshotKindScheduled, TenantID: input.TenantID,
		UserID: input.UserID, TaskID: input.TaskID, DefinitionVersion: 1,
		DefinitionDigest: digest, CapabilityCatalogDigest: digest,
		ToolPolicyDigest: digest, PromptPolicyDigest: digest,
		ModelPolicyDigest: digest, QuotaPolicyDigest: digest,
		PlannerBudget: types.PlannerBudget{MaxPlannerRounds: 8, MaxToolCalls: 16,
			MaxTokens: 32768, MaxCostMicroUSD: 1_000_000, DurationMs: 300_000},
		HistoryThroughUTC: "2026-08-01T12:34:56Z", PayloadDigest: digest,
	})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := types.SealResearchRunPlanRefV3(types.ResearchRunPlanRefV3{
		PlanID: 22, RunSnapshotID: snapshot.SnapshotID,
		TemporalWorkflowID: workflowID, TemporalRunID: runID,
		TenantID: input.TenantID, UserID: input.UserID, TaskID: input.TaskID,
		DefinitionDigest: digest, CapabilityCatalogDigest: digest,
		ToolPolicyDigest: digest, PlanDigest: strings.Repeat("b", 64), StepCount: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	run := ResearchRunV3Input{
		TenantID: input.TenantID, UserID: input.UserID, TaskID: input.TaskID,
		TraceID: traceID, Snapshot: snapshot,
	}
	b := newHistoryBuilder(t, input)
	started := b.events[0].GetWorkflowExecutionStartedEventAttributes()
	started.WorkflowType = &commonpb.WorkflowType{Name: "ResearchShadowWorkflowV3"}
	b.sideEffect(1, traceID)
	b.activity("PrepareResearchRunV3", ResearchScheduledInputV3{
		TenantID: input.TenantID, UserID: input.UserID, TaskID: input.TaskID,
	}, PrepareResearchRunV3Result{Authorized: true, Snapshot: snapshot})
	b.activity("PlanResearchRunV3", run, PlanResearchRunV3Result{Plan: plan})
	b.activity("ExecuteResearchStepV3", ExecuteResearchStepV3Input{
		ResearchRunV3Input: run, Plan: plan, Ordinal: 0,
	}, legacyResearchStepReceiptV3{
		StepID: 31, Ordinal: 0, InvocationID: "search-official",
		ToolName: "web_search", RequestDigest: strings.Repeat("c", 64),
		ResultDigest: strings.Repeat("d", 64), EvidenceID: 41,
	})
	brief, err := types.SealResearchBriefRefV3(types.ResearchBriefRefV3{
		BriefID: 51, RunSnapshotID: snapshot.SnapshotID, PlanID: plan.PlanID,
		TemporalWorkflowID: workflowID, TemporalRunID: runID,
		TenantID: input.TenantID, UserID: input.UserID, TaskID: input.TaskID,
		DefinitionDigest: digest, PlanDigest: plan.PlanDigest,
		RequestDigest: strings.Repeat("e", 64), BriefDigest: strings.Repeat("f", 64),
		EvidenceDigest: strings.Repeat("1", 64), HistoryDigest: strings.Repeat("2", 64),
		NotificationThreshold: "major_updates_only",
		Significance:          types.ResearchBriefSignificanceNoneV3,
		Decision:              types.ResearchBriefDecisionQuietV3, DeliveryRequired: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	b.activity("SynthesizeResearchBriefV3", SynthesizeResearchBriefV3Input{
		ResearchRunV3Input: run, Plan: plan,
	}, brief)
	history := b.complete()

	legacyPayloadFound := false
	for _, event := range history.Events {
		if event.EventType != enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED {
			continue
		}
		payloads := event.GetActivityTaskCompletedEventAttributes().GetResult().GetPayloads()
		if len(payloads) == 0 || !bytes.Contains(payloads[0].Data, []byte(`"step_id":31`)) {
			continue
		}
		legacyPayloadFound = true
		if bytes.Contains(payloads[0].Data, []byte(`"phase"`)) {
			t.Fatalf("legacy Activity fixture accidentally contains phase: %s", payloads[0].Data)
		}
	}
	if !legacyPayloadFound {
		t.Fatal("legacy completed-step Activity payload was not found")
	}

	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflow(ResearchShadowWorkflowV3)
	if err := replayer.ReplayWorkflowHistoryWithOptions(replayLogger(), history,
		worker.ReplayWorkflowHistoryOptions{OriginalExecution: sdkworkflow.Execution{
			ID: workflowID, RunID: runID,
		}}); err != nil {
		t.Fatalf("legacy Research V3 completed-step history must replay exactly: %v", err)
	}
}
