package store

import (
	"math"
	"testing"

	"github.com/google/uuid"

	"github.com/YouToco/vane/types"
)

// TestGetScheduleRunCostExactAttribution is a real-PostgreSQL proof for the
// two immutable accounting anchors. In particular, two snapshots sharing one
// workflow ID must not multiply one Tool row; a JOIN instead of EXISTS makes
// this test fail.
func TestGetScheduleRunCostExactAttribution(t *testing.T) {
	fixture := newTaskRunSnapshotFixture(t)
	taskID := fixture.taskID()
	fixture.createApprovedTask(t, taskID, 1)

	first := fixture.params(taskID, "run-"+uuid.NewString())
	first.TemporalWorkflowID = "workflow-cost-first-" + uuid.NewString()
	if _, err := fixture.st.createOrGetTaskRunSnapshot(
		t.Context(), first,
	); err != nil {
		t.Fatal(err)
	}
	duplicateWorkflow := first
	duplicateWorkflow.TemporalRunID = "run-" + uuid.NewString()
	if _, err := fixture.st.createOrGetTaskRunSnapshot(
		t.Context(), duplicateWorkflow,
	); err != nil {
		t.Fatal(err)
	}
	latest := fixture.params(taskID, "run-"+uuid.NewString())
	latest.TemporalWorkflowID = "workflow-cost-latest-" + uuid.NewString()
	if _, err := fixture.st.createOrGetTaskRunSnapshot(
		t.Context(), latest,
	); err != nil {
		t.Fatal(err)
	}

	trace := "pipeline-cost-" + uuid.NewString()
	if _, _, err := fixture.st.RecordEmptyPushBatch(
		t.Context(), fixture.userID, trace, taskID,
		types.BatchExitGateFetch, types.PipelineCounts{},
	); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.st.InsertLLMCall(t.Context(), &types.LLMCall{
		TenantID: &fixture.tenantID,
		UserID:   &fixture.userID,
		TraceID:  trace,
		SpanName: "score",
		CostUSD:  0.25,
	}); err != nil {
		t.Fatal(err)
	}

	firstCost, latestCost := 0.007, 0.01
	insertTool := func(
		traceID string,
		cost *float64,
		errorType string,
	) {
		t.Helper()
		if _, err := fixture.st.InsertToolCall(
			t.Context(),
			&types.ToolCall{
				TenantID:  &fixture.tenantID,
				UserID:    &fixture.userID,
				TraceID:   traceID,
				ToolName:  "exa:search",
				ToolKind:  types.ToolCallKindExaFetch,
				CostUSD:   cost,
				ErrorType: errorType,
			},
		); err != nil {
			t.Fatal(err)
		}
	}
	insertTool(first.TemporalWorkflowID, &firstCost, "")
	insertTool(latest.TemporalWorkflowID, &latestCost, types.ToolErrTimeout)
	insertTool(latest.TemporalWorkflowID, nil, types.ToolErrHTTP)

	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(ctx, t, fixture.st,
			`DELETE FROM llm_calls WHERE trace_id=$1`, trace)
		cleanupExec(ctx, t, fixture.st,
			`DELETE FROM tool_calls WHERE trace_id=ANY($1)`,
			[]string{
				first.TemporalWorkflowID,
				latest.TemporalWorkflowID,
			})
		cleanupExec(ctx, t, fixture.st,
			`DELETE FROM push_batches WHERE idempotency_key=$1`, trace)
	})

	got, err := fixture.st.GetScheduleRunCost(
		t.Context(), fixture.userID, taskID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(got.LLMCostUSD-0.25) > 1e-9 ||
		got.LLMCalls != 1 {
		t.Fatalf("LLM attribution=%#v", got)
	}
	if math.Abs(got.ToolCostUSD-0.017) > 1e-9 ||
		got.ToolCalls != 3 ||
		got.ToolPricedCalls != 2 {
		t.Fatalf("Tool attribution=%#v", got)
	}
	if got.LatestAcquisitionCalls != 2 ||
		got.LatestAcquisitionFailures != 2 ||
		got.LatestAcquisitionErrorType != types.ToolErrTimeout {
		t.Fatalf("latest acquisition=%#v", got)
	}

	foreign, err := fixture.st.GetScheduleRunCost(
		t.Context(), fixture.userID+999999, taskID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if foreign.LLMCalls != 0 || foreign.ToolCalls != 0 {
		t.Fatalf("foreign attribution leaked=%#v", foreign)
	}
}
