package store

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/YouToco/vane/server/types"
)

func TestAdminExecutionTrace_ExactScopePromptToolAndAudit(t *testing.T) {
	f := newTaskRunSnapshotFixture(t)
	taskID := f.taskID()
	f.createApprovedTask(t, taskID, 1)
	params := f.params(taskID, "run-"+uuid.NewString())
	snapshot, err := f.st.createOrGetTaskRunSnapshot(t.Context(), params)
	if err != nil {
		t.Fatal(err)
	}
	reusedWorkflow := params
	reusedWorkflow.TemporalRunID = "run-" + uuid.NewString()
	if _, err := f.st.createOrGetTaskRunSnapshot(
		t.Context(), reusedWorkflow,
	); err != nil {
		t.Fatal(err)
	}
	traceID := "admin-trace-" + uuid.NewString()
	if _, err := f.st.pool.Exec(t.Context(), `
		INSERT INTO push_batches (
		    tenant_id,user_id,status,exit_gate,stage_counts,idempotency_key,
		    schedule_id,run_snapshot_id
		) VALUES ($1,$2,$3,$4,'{}'::jsonb,$5,$6,$7)`,
		f.tenantID, f.userID, types.BatchStatusEmpty,
		types.BatchExitGateObservationNoMatch, traceID, taskID, snapshot.ID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := f.st.InsertLLMCall(t.Context(), &types.LLMCall{
		RunSnapshotID:    &snapshot.ID,
		TenantID:         &f.tenantID,
		UserID:           &f.userID,
		TraceID:          traceID,
		SpanName:         "score",
		Provider:         "deepseek",
		Model:            "test-model",
		SystemPrompt:     "system prompt exactly as sent",
		UserPrompt:       "user prompt exactly as sent",
		Completion:       "model output",
		PromptTokens:     11,
		CompletionTokens: 7,
		LatencyMs:        123,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.st.InsertToolCall(t.Context(), &types.ToolCall{
		RunSnapshotID: &snapshot.ID,
		TenantID:      &f.tenantID,
		UserID:        &f.userID,
		TraceID:       params.TemporalWorkflowID,
		ToolName:      "web_search",
		ToolKind:      types.ToolCallKindExaFetch,
		Provider:      "exa",
		EndpointPath:  "/search",
		Arguments:     json.RawMessage(`{"query":"vane"}`),
		ResultPreview: `{"items":[1]}`,
		ResultSize:    999,
		DurationMs:    88,
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(ctx, t, f.st,
			`DELETE FROM admin_trace_access_events WHERE run_snapshot_id=$1`,
			snapshot.ID)
		cleanupExec(ctx, t, f.st,
			`DELETE FROM llm_calls WHERE trace_id=$1`, traceID)
		cleanupExec(ctx, t, f.st,
			`DELETE FROM tool_calls WHERE trace_id=$1`, params.TemporalWorkflowID)
		cleanupExec(ctx, t, f.st,
			`DELETE FROM push_batches WHERE run_snapshot_id=$1`, snapshot.ID)
	})

	_, err = f.st.GetAdminExecutionTrace(
		t.Context(), f.tenantID, f.userID,
		f.tenantID, f.userID+999999, taskID, snapshot.ID,
	)
	if !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("foreign user must fail closed, got %v", err)
	}
	var auditCount int
	if err := f.st.pool.QueryRow(t.Context(),
		`SELECT count(*) FROM admin_trace_access_events
		  WHERE run_snapshot_id=$1`, snapshot.ID).Scan(&auditCount); err != nil {
		t.Fatal(err)
	}
	if auditCount != 0 {
		t.Fatalf("failed read wrote %d audit rows, want 0", auditCount)
	}

	got, err := f.st.GetAdminExecutionTrace(
		t.Context(), f.tenantID, f.userID,
		f.tenantID, f.userID, taskID, snapshot.ID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got.Run.SnapshotID != snapshot.ID || got.Run.ModelCalls != 1 ||
		got.Run.ToolCalls != 1 || len(got.Events) != 2 {
		t.Fatalf("trace summary/events=%#v", got)
	}
	var model, tool *AdminTraceEvent
	for i := range got.Events {
		switch got.Events[i].Kind {
		case "model":
			model = &got.Events[i]
		case "tool":
			tool = &got.Events[i]
		}
	}
	if model == nil ||
		model.SystemPrompt != "system prompt exactly as sent" ||
		model.UserPrompt != "user prompt exactly as sent" ||
		model.Completion != "model output" {
		t.Fatalf("model prompt/output not exact: %#v", model)
	}
	if tool == nil || string(tool.Arguments) != `{"query": "vane"}` ||
		!tool.ResultTruncated || tool.ResultSize != 999 {
		t.Fatalf("tool evidence/truncation=%#v", tool)
	}
	if err := f.st.pool.QueryRow(t.Context(),
		`SELECT count(*) FROM admin_trace_access_events
		  WHERE actor_tenant_id=$1 AND actor_user_id=$2
		    AND target_tenant_id=$1 AND target_user_id=$2
		    AND task_id=$3 AND run_snapshot_id=$4`,
		f.tenantID, f.userID, taskID, snapshot.ID,
	).Scan(&auditCount); err != nil {
		t.Fatal(err)
	}
	if auditCount != 1 {
		t.Fatalf("successful read audit rows=%d, want 1", auditCount)
	}
}
