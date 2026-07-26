package store

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/definitioneditwire"
	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

func TestTaskDefinitionEditCommitRebasesActiveSnapshotCutover(t *testing.T) {
	runFixture, taskID, baseRef := newTaskRunSnapshotCutoverControlFixture(t)
	ctx := t.Context()
	baseIdentity := baseRef.Identity()
	baseAudit, err := runFixture.st.AuditCompiledTaskRunSnapshotV2(
		ctx, baseIdentity, baseRef)
	if err != nil ||
		baseAudit.Status != CompiledRunSnapshotV2AuditMatch ||
		baseAudit.ShadowStatus != TaskRunSnapshotShadowMatch ||
		!baseAudit.TypedEqual {
		t.Fatalf("retained-v2 base audit=%+v err=%v", baseAudit, err)
	}
	activation, err := runFixture.st.ControlTaskRunSnapshotCutover(
		ctx,
		runFixture.tenantID,
		runFixture.userID,
		taskID,
		TaskRunSnapshotCutoverActivate,
	)
	if err != nil {
		t.Fatalf("activate retained-v2 authority: %v", err)
	}
	base, err := runFixture.st.GetCurrentApprovedDefinition(
		ctx, runFixture.tenantID, runFixture.userID, taskID)
	if err != nil {
		t.Fatalf("load active base definition: %v", err)
	}
	f := newTaskDefinitionEditCutoverFixture(
		t, runFixture, taskID, base)
	if activation.ApprovedDefinitionVersion != f.op.BaseDefinitionVersion {
		t.Fatalf(
			"activation definition version=%d, want base %d",
			activation.ApprovedDefinitionVersion,
			f.op.BaseDefinitionVersion,
		)
	}

	acquired, err := f.state.store.AcquireTaskDefinitionEditOperation(
		ctx,
		types.AcquireTaskDefinitionEditOperationParams{
			Scope:           f.op.Scope(),
			LeaseOwner:      "definition-edit-cutover-rebase-worker",
			LeaseDuration:   5 * time.Minute,
			ReceiptProvider: "feishu_card_patch:app-test",
			ReceiptTarget:   "message-cutover-rebase/card-cutover-rebase",
		},
	)
	if err != nil {
		t.Fatalf("acquire definition edit: %v", err)
	}
	lease := acquired.Lease()
	if err := f.state.store.QuiesceTaskDefinitionEdit(ctx, lease); err != nil {
		t.Fatalf("quiesce definition edit: %v", err)
	}
	if err := f.state.store.CheckpointTaskDefinitionEditBasePaused(
		ctx,
		lease,
		taskDefinitionEditSnapshotFixture(
			t, f, definitioneditwire.SnapshotPhaseBasePaused,
			"Aw",
		),
	); err != nil {
		t.Fatalf("checkpoint paused base: %v", err)
	}
	if err := f.state.store.CommitTaskDefinitionEditDefinition(ctx, lease); err != nil {
		t.Fatalf("commit target definition: %v", err)
	}

	var (
		eventCount         int
		pointerAction      string
		pointerID          int64
		pointerVersion     int64
		pointerDigest      string
		currentHeadVersion int64
		currentHeadDigest  string
	)
	if err := f.state.store.pool.QueryRow(ctx, `
		SELECT s.run_snapshot_cutover_event_id, e.action,
		       e.approved_definition_version,
		       e.approved_definition_digest,
		       s.approved_definition_version,
		       s.approved_definition_digest,
		       (SELECT count(*)
		          FROM task_run_snapshot_v2_cutover_events owned
		         WHERE owned.tenant_id=s.tenant_id
		           AND owned.user_id=s.user_id
		           AND owned.task_id=s.id)
		  FROM schedules s
		  JOIN task_run_snapshot_v2_cutover_events e
		    ON e.id=s.run_snapshot_cutover_event_id
		 WHERE s.tenant_id=$1 AND s.user_id=$2 AND s.id=$3`,
		f.op.TargetTenantID,
		f.op.TargetUserID,
		f.op.TaskID,
	).Scan(
		&pointerID,
		&pointerAction,
		&pointerVersion,
		&pointerDigest,
		&currentHeadVersion,
		&currentHeadDigest,
		&eventCount,
	); err != nil {
		t.Fatalf("load active cutover pointer and current head: %v", err)
	}
	if eventCount != 3 {
		t.Fatalf("cutover event count=%d, want base activate + edit pair", eventCount)
	}
	if pointerAction != string(TaskRunSnapshotCutoverActivate) {
		t.Errorf("cutover action=%q, want active", pointerAction)
	}
	if pointerVersion != currentHeadVersion || pointerDigest != currentHeadDigest {
		t.Errorf(
			"active cutover pin drifted from current head: pointer=%d/%s head=%d/%s",
			pointerVersion,
			pointerDigest,
			currentHeadVersion,
			currentHeadDigest,
		)
	}
	if currentHeadVersion != f.op.TargetDefinitionVersion ||
		currentHeadDigest != f.op.TargetDefinitionDigest {
		t.Fatalf(
			"definition commit did not reach target: head=%d/%s target=%d/%s",
			currentHeadVersion,
			currentHeadDigest,
			f.op.TargetDefinitionVersion,
			f.op.TargetDefinitionDigest,
		)
	}

	if err := f.state.store.CommitTaskDefinitionEditDefinition(ctx, lease); err != nil {
		t.Fatalf("exact definition commit replay: %v", err)
	}
	var replayPointerID int64
	var replayEventCount int
	if err := f.state.store.pool.QueryRow(ctx, `
		SELECT s.run_snapshot_cutover_event_id,
		       (SELECT count(*)
		          FROM task_run_snapshot_v2_cutover_events owned
		         WHERE owned.tenant_id=s.tenant_id
		           AND owned.user_id=s.user_id
		           AND owned.task_id=s.id)
		  FROM schedules s
		 WHERE s.tenant_id=$1 AND s.user_id=$2 AND s.id=$3`,
		f.op.TargetTenantID,
		f.op.TargetUserID,
		f.op.TaskID,
	).Scan(&replayPointerID, &replayEventCount); err != nil {
		t.Fatalf("load replayed cutover state: %v", err)
	}
	if replayPointerID != pointerID || replayEventCount != eventCount {
		t.Fatalf(
			"definition commit replay changed cutover: pointer %d->%d events %d->%d",
			pointerID,
			replayPointerID,
			eventCount,
			replayEventCount,
		)
	}

	if err := f.state.store.CheckpointTaskDefinitionEditTargetApplied(
		ctx,
		lease,
		taskDefinitionEditSnapshotFixture(
			t, f, definitioneditwire.SnapshotPhaseTargetPaused,
			"BQ",
		),
	); err != nil {
		t.Fatalf("checkpoint applied target: %v", err)
	}
	if err := f.state.store.CheckpointTaskDefinitionEditTargetRestored(
		ctx,
		lease,
		taskDefinitionEditSnapshotFixture(
			t, f, definitioneditwire.SnapshotPhaseTargetFinal,
			"Bw",
		),
	); err != nil {
		t.Fatalf("checkpoint restored target: %v", err)
	}
	if err := f.state.store.CompleteTaskDefinitionEditOperation(
		ctx,
		lease,
		json.RawMessage(`{"status":"edited"}`),
	); err != nil {
		t.Fatalf("complete definition edit: %v", err)
	}

	freshIdentity := types.RunIdentity{
		TemporalWorkflowID: scheduledTaskWorkflowID(f.op.TaskID),
		TemporalRunID:      "definition-edit-cutover-fresh-" + uuid.NewString(),
		RunKind:            types.RunSnapshotKindScheduled,
		TenantID:           f.op.TargetTenantID,
		UserID:             f.op.TargetUserID,
		TaskID:             f.op.TaskID,
	}
	freshRef, err := f.state.store.CreateOrGetCompiledTaskRunSnapshotV1(
		ctx,
		CreateOrGetCompiledTaskRunSnapshotV1Params{
			Identity: freshIdentity,
			Policy:   testCompiledRunPolicyV1(t),
		},
	)
	if err != nil {
		t.Fatalf("fresh PrepareRun snapshot after definition edit: %v", err)
	}
	_, authority, err := f.state.store.LoadAuthoritativeCompiledTaskRunSnapshot(
		ctx, freshIdentity, freshRef)
	if err != nil {
		t.Fatalf("load fresh authoritative snapshot: %v", err)
	}
	if authority != CompiledRunSnapshotAuthorityV2 {
		t.Errorf("fresh PrepareRun authority=%q, want %q",
			authority, CompiledRunSnapshotAuthorityV2)
	}
	authorized, err := f.state.store.AuthorizeTaskRunSideEffect(
		ctx, freshIdentity, freshRef)
	if err != nil {
		t.Fatalf("authorize fresh PrepareRun snapshot: %v", err)
	}
	if !authorized {
		t.Error("fresh PrepareRun snapshot was not authorized")
	}
}

func TestTaskDefinitionEditCommitPreservesInactiveSnapshotCutover(t *testing.T) {
	t.Run("null", func(t *testing.T) {
		f := newTaskDefinitionEditOperationFixture(t)
		lease := prepareTaskDefinitionEditCutoverCommit(
			t, f, "definition-edit-null-cutover-worker")

		if err := f.state.store.CommitTaskDefinitionEditDefinition(
			t.Context(), lease,
		); err != nil {
			t.Fatalf("commit definition without cutover pointer: %v", err)
		}
		assertTaskDefinitionEditCutoverUnchanged(
			t, f, nil, 0)
	})

	t.Run("rollback", func(t *testing.T) {
		runFixture, taskID, _ := newTaskRunSnapshotCutoverControlFixture(t)
		ctx := t.Context()
		if _, err := runFixture.st.ControlTaskRunSnapshotCutover(
			ctx,
			runFixture.tenantID,
			runFixture.userID,
			taskID,
			TaskRunSnapshotCutoverActivate,
		); err != nil {
			t.Fatalf("activate retained-v2 authority: %v", err)
		}
		rollback, err := runFixture.st.ControlTaskRunSnapshotCutover(
			ctx,
			runFixture.tenantID,
			runFixture.userID,
			taskID,
			TaskRunSnapshotCutoverRollback,
		)
		if err != nil {
			t.Fatalf("rollback retained-v2 authority: %v", err)
		}
		base, err := runFixture.st.GetCurrentApprovedDefinition(
			ctx, runFixture.tenantID, runFixture.userID, taskID)
		if err != nil {
			t.Fatalf("load rollback-basis definition: %v", err)
		}
		f := newTaskDefinitionEditCutoverFixture(
			t, runFixture, taskID, base)
		lease := prepareTaskDefinitionEditCutoverCommit(
			t, f, "definition-edit-rollback-cutover-worker")

		if err := f.state.store.CommitTaskDefinitionEditDefinition(
			ctx, lease,
		); err != nil {
			t.Fatalf("commit definition from rollback cutover: %v", err)
		}
		assertTaskDefinitionEditCutoverUnchanged(
			t, f, &rollback.EventID, 2)
	})
}

func TestSnapshotCutoverControlConflictsWithDefinitionEditMarker(t *testing.T) {
	runFixture, taskID, _ := newTaskRunSnapshotCutoverControlFixture(t)
	ctx := t.Context()
	activation, err := runFixture.st.ControlTaskRunSnapshotCutover(
		ctx,
		runFixture.tenantID,
		runFixture.userID,
		taskID,
		TaskRunSnapshotCutoverActivate,
	)
	if err != nil {
		t.Fatalf("activate retained-v2 authority: %v", err)
	}
	base, err := runFixture.st.GetCurrentApprovedDefinition(
		ctx, runFixture.tenantID, runFixture.userID, taskID)
	if err != nil {
		t.Fatalf("load active base definition: %v", err)
	}
	f := newTaskDefinitionEditCutoverFixture(
		t, runFixture, taskID, base)
	acquired, err := f.state.store.AcquireTaskDefinitionEditOperation(
		ctx,
		types.AcquireTaskDefinitionEditOperationParams{
			Scope:           f.op.Scope(),
			LeaseOwner:      "definition-edit-cutover-conflict-worker",
			LeaseDuration:   5 * time.Minute,
			ReceiptProvider: "feishu_card_patch:app-test",
			ReceiptTarget:   "message-cutover-conflict/card-cutover-conflict",
		},
	)
	if err != nil {
		t.Fatalf("acquire definition edit: %v", err)
	}
	if err := f.state.store.QuiesceTaskDefinitionEdit(
		ctx, acquired.Lease(),
	); err != nil {
		t.Fatalf("quiesce definition edit: %v", err)
	}

	if _, err := runFixture.st.ControlTaskRunSnapshotCutover(
		ctx,
		runFixture.tenantID,
		runFixture.userID,
		taskID,
		TaskRunSnapshotCutoverRollback,
	); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("generic cutover during definition edit error=%v, want conflict", err)
	}
	assertTaskDefinitionEditCutoverUnchanged(
		t, f, &activation.EventID, 1)
}

func TestTaskDefinitionEditCutoverCommitTakeoverExactReplay(t *testing.T) {
	runFixture, taskID, _ := newTaskRunSnapshotCutoverControlFixture(t)
	ctx := t.Context()
	if _, err := runFixture.st.ControlTaskRunSnapshotCutover(
		ctx,
		runFixture.tenantID,
		runFixture.userID,
		taskID,
		TaskRunSnapshotCutoverActivate,
	); err != nil {
		t.Fatalf("activate retained-v2 authority: %v", err)
	}
	base, err := runFixture.st.GetCurrentApprovedDefinition(
		ctx, runFixture.tenantID, runFixture.userID, taskID)
	if err != nil {
		t.Fatalf("load active base definition: %v", err)
	}
	f := newTaskDefinitionEditCutoverFixture(
		t, runFixture, taskID, base)
	const oldOwner = "definition-edit-cutover-takeover-old"
	oldLease := prepareTaskDefinitionEditCutoverCommit(t, f, oldOwner)
	if err := f.state.store.CommitTaskDefinitionEditDefinition(
		ctx, oldLease,
	); err != nil {
		t.Fatalf("commit definition before takeover: %v", err)
	}

	var (
		pointerBefore int64
		eventsBefore  int
	)
	if err := f.state.store.pool.QueryRow(ctx, `
		SELECT s.run_snapshot_cutover_event_id,
		       (SELECT count(*)
		          FROM task_run_snapshot_v2_cutover_events e
		         WHERE e.tenant_id=s.tenant_id
		           AND e.user_id=s.user_id
		           AND e.task_id=s.id)
		  FROM schedules s
		 WHERE s.tenant_id=$1 AND s.user_id=$2 AND s.id=$3`,
		f.op.TargetTenantID,
		f.op.TargetUserID,
		f.op.TaskID,
	).Scan(&pointerBefore, &eventsBefore); err != nil {
		t.Fatalf("load cutover before takeover: %v", err)
	}
	if eventsBefore != 3 {
		t.Fatalf("events before takeover=%d, want base activation + edit pair",
			eventsBefore)
	}

	if _, err := f.state.store.pool.Exec(ctx, `
		UPDATE task_definition_edit_operations
		   SET lease_until=clock_timestamp()-interval '2 minutes',
		       takeover_not_before=clock_timestamp()-interval '1 minute'
		 WHERE id=$1 AND fence=$2`,
		f.op.ID, oldLease.Fence,
	); err != nil {
		t.Fatalf("age committed operation lease: %v", err)
	}
	const newOwner = "definition-edit-cutover-takeover-new"
	taken, err := f.state.store.AcquireTaskDefinitionEditOperation(
		ctx,
		types.AcquireTaskDefinitionEditOperationParams{
			Scope:           f.op.Scope(),
			LeaseOwner:      newOwner,
			LeaseDuration:   5 * time.Minute,
			ReceiptProvider: "feishu_card_patch:app-test",
			ReceiptTarget:   "message-" + oldOwner + "/card-" + oldOwner,
		},
	)
	if err != nil {
		t.Fatalf("take over committed definition edit: %v", err)
	}
	if taken.Fence != oldLease.Fence+1 ||
		taken.Attempt != 2 ||
		taken.LeaseOwner != newOwner {
		t.Fatalf("definition edit takeover=%+v, old fence=%d",
			taken, oldLease.Fence)
	}
	newLease := taken.Lease()

	if err := f.state.store.CommitTaskDefinitionEditDefinition(
		ctx, oldLease,
	); !errors.Is(err, types.ErrTaskDefinitionEditLeaseLost) {
		t.Fatalf("stale commit error=%v, want lease lost", err)
	}
	if err := f.state.store.CommitTaskDefinitionEditDefinition(
		ctx, newLease,
	); err != nil {
		t.Fatalf("takeover definition commit exact replay: %v", err)
	}
	if err := f.state.store.CheckpointTaskDefinitionEditTargetApplied(
		ctx,
		newLease,
		taskDefinitionEditSnapshotFixture(
			t, f, definitioneditwire.SnapshotPhaseTargetPaused, "DQ"),
	); err != nil {
		t.Fatalf("checkpoint target after takeover: %v", err)
	}
	if err := f.state.store.CheckpointTaskDefinitionEditTargetRestored(
		ctx,
		newLease,
		taskDefinitionEditSnapshotFixture(
			t, f, definitioneditwire.SnapshotPhaseTargetFinal, "Dw"),
	); err != nil {
		t.Fatalf("restore target after takeover: %v", err)
	}
	if err := f.state.store.CompleteTaskDefinitionEditOperation(
		ctx,
		oldLease,
		json.RawMessage(`{"status":"stale"}`),
	); !errors.Is(err, types.ErrTaskDefinitionEditLeaseLost) {
		t.Fatalf("stale completion error=%v, want lease lost", err)
	}

	var (
		pointerAfter     int64
		eventsAfter      int
		markerFenceAfter int64
	)
	if err := f.state.store.pool.QueryRow(ctx, `
		SELECT s.run_snapshot_cutover_event_id,
		       s.definition_edit_fence,
		       (SELECT count(*)
		          FROM task_run_snapshot_v2_cutover_events e
		         WHERE e.tenant_id=s.tenant_id
		           AND e.user_id=s.user_id
		           AND e.task_id=s.id)
		  FROM schedules s
		 WHERE s.tenant_id=$1 AND s.user_id=$2 AND s.id=$3`,
		f.op.TargetTenantID,
		f.op.TargetUserID,
		f.op.TaskID,
	).Scan(
		&pointerAfter,
		&markerFenceAfter,
		&eventsAfter,
	); err != nil {
		t.Fatalf("load cutover after takeover replay: %v", err)
	}
	if pointerAfter != pointerBefore || eventsAfter != eventsBefore {
		t.Fatalf(
			"takeover replay changed cutover: pointer %d->%d events %d->%d",
			pointerBefore,
			pointerAfter,
			eventsBefore,
			eventsAfter,
		)
	}
	if markerFenceAfter != newLease.Fence {
		t.Fatalf("schedule marker fence=%d, want takeover fence %d",
			markerFenceAfter, newLease.Fence)
	}
}

func TestTaskDefinitionEditCutoverAcceptsLegacyTargetConvergence(t *testing.T) {
	runFixture, taskID, _ := newTaskRunSnapshotCutoverControlFixture(t)
	ctx := t.Context()
	if _, err := runFixture.st.ControlTaskRunSnapshotCutover(
		ctx,
		runFixture.tenantID,
		runFixture.userID,
		taskID,
		TaskRunSnapshotCutoverActivate,
	); err != nil {
		t.Fatalf("activate retained-v2 authority: %v", err)
	}
	base, err := runFixture.st.GetCurrentApprovedDefinition(
		ctx, runFixture.tenantID, runFixture.userID, taskID)
	if err != nil {
		t.Fatalf("load active base definition: %v", err)
	}
	f := newTaskDefinitionEditCutoverFixture(
		t, runFixture, taskID, base)
	lease := prepareTaskDefinitionEditCutoverCommit(
		t, f, "definition-edit-legacy-target-worker")
	if err := f.state.store.CommitTaskDefinitionEditDefinition(
		ctx, lease,
	); err != nil {
		t.Fatalf("commit definition before legacy convergence fixture: %v", err)
	}

	var pointerBefore int64
	if err := f.state.store.pool.QueryRow(ctx, `
		SELECT run_snapshot_cutover_event_id
		  FROM schedules
		 WHERE tenant_id=$1 AND user_id=$2 AND id=$3`,
		f.op.TargetTenantID,
		f.op.TargetUserID,
		f.op.TaskID,
	).Scan(&pointerBefore); err != nil {
		t.Fatal(err)
	}
	if _, err := f.state.store.pool.Exec(ctx, `
		UPDATE task_run_snapshot_v2_cutover_events
		   SET definition_edit_operation_id=NULL
		 WHERE definition_edit_operation_id=$1`,
		f.op.ID,
	); err != nil {
		t.Fatalf("simulate pre-057 target convergence provenance: %v", err)
	}

	if err := f.state.store.CommitTaskDefinitionEditDefinition(
		ctx, lease,
	); err != nil {
		t.Fatalf("accept exact legacy target convergence: %v", err)
	}
	var (
		pointerAfter int64
		eventCount   int
		ownedCount   int
	)
	if err := f.state.store.pool.QueryRow(ctx, `
		SELECT s.run_snapshot_cutover_event_id,
		       (SELECT count(*)
		          FROM task_run_snapshot_v2_cutover_events e
		         WHERE e.tenant_id=s.tenant_id
		           AND e.user_id=s.user_id
		           AND e.task_id=s.id),
		       (SELECT count(*)
		          FROM task_run_snapshot_v2_cutover_events e
		         WHERE e.definition_edit_operation_id=$4)
		  FROM schedules s
		 WHERE s.tenant_id=$1 AND s.user_id=$2 AND s.id=$3`,
		f.op.TargetTenantID,
		f.op.TargetUserID,
		f.op.TaskID,
		f.op.ID,
	).Scan(&pointerAfter, &eventCount, &ownedCount); err != nil {
		t.Fatal(err)
	}
	if pointerAfter != pointerBefore || eventCount != 3 || ownedCount != 0 {
		t.Fatalf(
			"legacy convergence replay pointer=%d->%d events=%d owned=%d",
			pointerBefore, pointerAfter, eventCount, ownedCount,
		)
	}
}

func prepareTaskDefinitionEditCutoverCommit(
	t *testing.T,
	f taskDefinitionEditOperationFixture,
	leaseOwner string,
) types.TaskDefinitionEditLease {
	t.Helper()
	ctx := t.Context()
	acquired, err := f.state.store.AcquireTaskDefinitionEditOperation(
		ctx,
		types.AcquireTaskDefinitionEditOperationParams{
			Scope:           f.op.Scope(),
			LeaseOwner:      leaseOwner,
			LeaseDuration:   5 * time.Minute,
			ReceiptProvider: "feishu_card_patch:app-test",
			ReceiptTarget:   "message-" + leaseOwner + "/card-" + leaseOwner,
		},
	)
	if err != nil {
		t.Fatalf("acquire definition edit: %v", err)
	}
	lease := acquired.Lease()
	if err := f.state.store.QuiesceTaskDefinitionEdit(ctx, lease); err != nil {
		t.Fatalf("quiesce definition edit: %v", err)
	}
	if err := f.state.store.CheckpointTaskDefinitionEditBasePaused(
		ctx,
		lease,
		taskDefinitionEditSnapshotFixture(
			t, f, definitioneditwire.SnapshotPhaseBasePaused,
			"Cw",
		),
	); err != nil {
		t.Fatalf("checkpoint paused base: %v", err)
	}
	return lease
}

func assertTaskDefinitionEditCutoverUnchanged(
	t *testing.T,
	f taskDefinitionEditOperationFixture,
	wantPointer *int64,
	wantEventCount int,
) {
	t.Helper()
	var pointer *int64
	var eventCount int
	var operationEventCount int
	if err := f.state.store.pool.QueryRow(t.Context(), `
		SELECT s.run_snapshot_cutover_event_id,
		       (SELECT count(*)
		          FROM task_run_snapshot_v2_cutover_events e
		         WHERE e.tenant_id=s.tenant_id
		           AND e.user_id=s.user_id
		           AND e.task_id=s.id),
		       (SELECT count(*)
		          FROM task_run_snapshot_v2_cutover_events e
		         WHERE e.definition_edit_operation_id=$4)
		  FROM schedules s
		 WHERE s.tenant_id=$1 AND s.user_id=$2 AND s.id=$3`,
		f.op.TargetTenantID,
		f.op.TargetUserID,
		f.op.TaskID,
		f.op.ID,
	).Scan(&pointer, &eventCount, &operationEventCount); err != nil {
		t.Fatalf("load preserved cutover state: %v", err)
	}
	if (pointer == nil) != (wantPointer == nil) ||
		(pointer != nil && *pointer != *wantPointer) {
		t.Fatalf("cutover pointer=%v, want %v", pointer, wantPointer)
	}
	if eventCount != wantEventCount {
		t.Fatalf("cutover event count=%d, want %d", eventCount, wantEventCount)
	}
	if operationEventCount != 0 {
		t.Fatalf("inactive cutover gained %d operation-owned events",
			operationEventCount)
	}
}

func newTaskDefinitionEditCutoverFixture(
	t *testing.T,
	runFixture *taskRunSnapshotFixture,
	taskID string,
	base ApprovedDefinitionVersionRecord,
) taskDefinitionEditOperationFixture {
	t.Helper()
	ctx := t.Context()
	var sourceID int64
	if err := runFixture.st.pool.QueryRow(ctx,
		`SELECT source_id
		   FROM schedule_sources
		  WHERE schedule_id=$1
		  ORDER BY source_id
		  LIMIT 1`,
		taskID,
	).Scan(&sourceID); err != nil {
		t.Fatalf("load cutover fixture source: %v", err)
	}
	state := taskDefinitionStateFixture{
		store:      runFixture.st,
		tenantID:   runFixture.tenantID,
		userID:     runFixture.userID,
		taskID:     taskID,
		sourceID:   sourceID,
		definition: base.Definition,
	}
	preparedBytes, prepared := loadTaskDefinitionEditPreparedFixture(t)
	target := base.Definition
	target.Intent = "cutover rebased target " + uuid.NewString()
	target.PlaybookContent = target.Intent
	target.NLDescription = "cutover rebased target"
	targetBytes, err := taskstate.EncodeApprovedDefinitionV1(target)
	if err != nil {
		t.Fatalf("encode cutover target definition: %v", err)
	}

	var sessionID int64
	if err := runFixture.st.pool.QueryRow(ctx,
		`INSERT INTO agent_sessions (tenant_id, user_id, status, messages)
		 VALUES ($1,$2,'active','[]'::jsonb)
		 RETURNING id`,
		runFixture.tenantID,
		runFixture.userID,
	).Scan(&sessionID); err != nil {
		t.Fatalf("insert cutover edit session: %v", err)
	}
	operationID := "definition-edit-cutover-rebase-" + uuid.NewString()
	approvalRef := "definition-edit-cutover-rebase-approval-" + uuid.NewString()
	proposal := []byte(`{"fixture":"cutover-rebase"}`)
	baseSnapshot := []byte(`{"fixture":"cutover-rebase-base"}`)
	var op types.TaskDefinitionEditOperation
	if err := scanTaskDefinitionEditOperation(
		runFixture.st.pool.QueryRow(ctx, `
			INSERT INTO task_definition_edit_operations (
				id, tenant_id, user_id, target_tenant_id, target_user_id,
				task_id, session_id, approval_ref, expires_at, original_status,
				base_definition_version, base_definition_digest, base_definition,
				target_definition_version, target_definition_digest, target_definition,
				canonical_proposal, proposal_digest, prepared_edit,
				prepared_edit_digest, base_snapshot, base_snapshot_digest
			) VALUES (
				$1,$2,$3,$2,$3,$4,$5,$6,clock_timestamp()+interval '1 hour',
				'active',$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18
			) RETURNING `+taskDefinitionEditOperationColumns,
			operationID,
			runFixture.tenantID,
			runFixture.userID,
			taskID,
			sessionID,
			approvalRef,
			base.Version,
			base.Digest,
			base.Payload,
			base.Version+1,
			sha256HexTaskDefinitionEdit(targetBytes),
			targetBytes,
			proposal,
			sha256HexTaskDefinitionEdit(proposal),
			preparedBytes,
			sha256HexTaskDefinitionEdit(preparedBytes),
			baseSnapshot,
			sha256HexTaskDefinitionEdit(baseSnapshot),
		),
		&op,
	); err != nil {
		t.Fatalf("insert cutover edit operation: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx := context.Background()
		_, _ = runFixture.st.pool.Exec(cleanupCtx,
			`UPDATE schedules
			    SET definition_edit_operation_id=NULL, definition_edit_fence=NULL
			  WHERE tenant_id=$1 AND user_id=$2 AND id=$3
			    AND definition_edit_operation_id=$4`,
			runFixture.tenantID, runFixture.userID, taskID, operationID)
		_, _ = runFixture.st.pool.Exec(cleanupCtx,
			`DELETE FROM task_definition_edit_receipts WHERE operation_id=$1`,
			operationID)
		_, _ = runFixture.st.pool.Exec(cleanupCtx,
			`DELETE FROM task_definition_edit_operations WHERE id=$1`,
			operationID)
		_, _ = runFixture.st.pool.Exec(cleanupCtx,
			`DELETE FROM agent_sessions WHERE id=$1`,
			sessionID)
	})
	return taskDefinitionEditOperationFixture{
		state:     state,
		base:      base,
		op:        op,
		sessionID: sessionID,
		prepared:  prepared,
	}
}
