package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/definitioneditwire"
	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

type taskDefinitionEditOperationFixture struct {
	state     taskDefinitionStateFixture
	base      ApprovedDefinitionVersionRecord
	op        types.TaskDefinitionEditOperation
	sessionID int64
	prepared  definitioneditwire.PreparedEditV1
}

type taskDefinitionEditDelayTerminalTx struct {
	pgx.Tx
	delayed bool
}

func (tx *taskDefinitionEditDelayTerminalTx) QueryRow(
	ctx context.Context,
	sql string,
	args ...any,
) pgx.Row {
	if !tx.delayed && strings.Contains(sql,
		"SET status=$7, result=$8, error_code=$9") {
		tx.delayed = true
		_, err := tx.Tx.Exec(ctx, `
			SELECT pg_sleep(
			    GREATEST(0, EXTRACT(EPOCH FROM (
			        lease_until - clock_timestamp()
			    ))) + 0.05
			  )
			  FROM task_definition_edit_operations
			 WHERE id=$1`, args[0])
		if err != nil {
			return compiledTaskErrorRow{err: err}
		}
	}
	return tx.Tx.QueryRow(ctx, sql, args...)
}

func loadTaskDefinitionEditPreparedFixture(
	t *testing.T,
) ([]byte, definitioneditwire.PreparedEditV1) {
	t.Helper()
	raw, err := os.ReadFile(
		"../task/testdata/definition_edit_proposal_components_v1.json")
	if err != nil {
		t.Fatalf("read retained prepared-edit fixture: %v", err)
	}
	var components struct {
		PreparedEdit json.RawMessage `json:"prepared_edit"`
	}
	if err := json.Unmarshal(raw, &components); err != nil {
		t.Fatalf("decode retained prepared-edit components: %v", err)
	}
	var prepared definitioneditwire.PreparedEditV1
	if err := json.Unmarshal(components.PreparedEdit, &prepared); err != nil {
		t.Fatalf("decode retained prepared edit: %v", err)
	}
	return bytes.Clone(components.PreparedEdit), prepared
}

func newTaskDefinitionEditOperationFixture(
	t *testing.T,
) taskDefinitionEditOperationFixture {
	t.Helper()
	state, base := newApprovedDefinitionEditFixture(t)
	ctx := t.Context()
	var status types.ScheduleStatus
	if err := state.store.pool.QueryRow(ctx,
		`SELECT status FROM schedules
		  WHERE tenant_id=$1 AND user_id=$2 AND id=$3`,
		state.tenantID, state.userID, state.taskID,
	).Scan(&status); err != nil {
		t.Fatalf("load schedule status: %v", err)
	}
	preparedBytes, prepared := loadTaskDefinitionEditPreparedFixture(t)
	if prepared.OriginalState == definitioneditwire.OriginalStatusActive &&
		status != types.ScheduleStatusActive {
		if _, err := state.store.pool.Exec(ctx,
			`UPDATE schedules SET status='active' WHERE tenant_id=$1 AND user_id=$2 AND id=$3`,
			state.tenantID, state.userID, state.taskID); err != nil {
			t.Fatalf("activate edit fixture schedule: %v", err)
		}
		status = types.ScheduleStatusActive
	}
	var sessionID int64
	if err := state.store.pool.QueryRow(ctx,
		`INSERT INTO agent_sessions (tenant_id, user_id, status, messages)
		 VALUES ($1,$2,'active','[]'::jsonb) RETURNING id`,
		state.tenantID, state.userID,
	).Scan(&sessionID); err != nil {
		t.Fatalf("insert edit session: %v", err)
	}
	target := state.definition
	target.Intent = "durable edit target " + uuid.NewString()
	target.PlaybookContent = target.Intent
	target.NLDescription = "durable edit target"
	targetBytes, err := taskstate.EncodeApprovedDefinitionV1(target)
	if err != nil {
		t.Fatalf("encode target definition: %v", err)
	}
	operationID := "definition-edit-test-" + uuid.NewString()
	approvalRef := "definition-edit-approval-" + uuid.NewString()
	proposal := []byte(`{"fixture":"proposal"}`)
	baseSnapshot := []byte(`{"fixture":"base-snapshot"}`)
	var op types.TaskDefinitionEditOperation
	err = scanTaskDefinitionEditOperation(state.store.pool.QueryRow(ctx,
		`INSERT INTO task_definition_edit_operations (
			id, tenant_id, user_id, target_tenant_id, target_user_id,
			task_id, session_id, approval_ref, expires_at, original_status,
			base_definition_version, base_definition_digest, base_definition,
			target_definition_version, target_definition_digest, target_definition,
			canonical_proposal, proposal_digest, prepared_edit,
			prepared_edit_digest, base_snapshot, base_snapshot_digest
		 ) VALUES (
			$1,$2,$3,$2,$3,$4,$5,$6,clock_timestamp()+interval '1 hour',$7,
			$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19
		 ) RETURNING `+taskDefinitionEditOperationColumns,
		operationID, state.tenantID, state.userID, state.taskID, sessionID,
		approvalRef, status, base.Version, base.Digest, base.Payload,
		base.Version+1, sha256HexTaskDefinitionEdit(targetBytes), targetBytes,
		proposal, sha256HexTaskDefinitionEdit(proposal), preparedBytes,
		sha256HexTaskDefinitionEdit(preparedBytes), baseSnapshot,
		sha256HexTaskDefinitionEdit(baseSnapshot),
	), &op)
	if err != nil {
		t.Fatalf("insert edit operation: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx := context.Background()
		_, _ = state.store.pool.Exec(cleanupCtx,
			`UPDATE schedules
			    SET definition_edit_operation_id=NULL, definition_edit_fence=NULL
			  WHERE tenant_id=$1 AND user_id=$2 AND id=$3
			    AND definition_edit_operation_id=$4`,
			state.tenantID, state.userID, state.taskID, operationID)
		_, _ = state.store.pool.Exec(cleanupCtx,
			`DELETE FROM task_definition_edit_receipts WHERE operation_id=$1`,
			operationID)
		_, _ = state.store.pool.Exec(cleanupCtx,
			`DELETE FROM task_definition_edit_operations WHERE id=$1`, operationID)
		_, _ = state.store.pool.Exec(cleanupCtx,
			`DELETE FROM agent_sessions WHERE id=$1`, sessionID)
	})
	return taskDefinitionEditOperationFixture{
		state: state, base: base, op: op, sessionID: sessionID,
		prepared: prepared,
	}
}

func taskDefinitionEditSnapshotFixture(
	t *testing.T,
	f taskDefinitionEditOperationFixture,
	phase definitioneditwire.SnapshotPhaseV1,
	revision string,
) []byte {
	t.Helper()
	var representation definitioneditwire.RepresentationV1
	switch phase {
	case definitioneditwire.SnapshotPhaseBaseOriginal:
		representation = f.prepared.BaseOriginal
	case definitioneditwire.SnapshotPhaseBasePaused:
		representation = f.prepared.BasePaused
	case definitioneditwire.SnapshotPhaseTargetPaused:
		representation = f.prepared.TargetPaused
	case definitioneditwire.SnapshotPhaseTargetFinal:
		representation = f.prepared.TargetFinal
	default:
		t.Fatalf("unsupported snapshot phase: %s", phase)
	}
	raw, err := json.Marshal(definitioneditwire.SnapshotV1{
		TaskID:               f.prepared.Creation.TaskID,
		RequestDigest:        f.prepared.RequestDigest,
		Phase:                phase,
		RepresentationDigest: representation.Digest,
		Revision:             revision,
	})
	if err != nil {
		t.Fatalf("encode snapshot fixture: %v", err)
	}
	if _, err := definitioneditwire.DecodePhaseSnapshotBytes(
		f.op.PreparedEdit, raw); err != nil {
		t.Fatalf("self-check snapshot fixture: %v", err)
	}
	return raw
}

func TestTaskDefinitionEditAcquireQuiesceTakeoverAndTerminalOutbox(
	t *testing.T,
) {
	f := newTaskDefinitionEditOperationFixture(t)
	ctx := t.Context()
	params := types.AcquireTaskDefinitionEditOperationParams{
		Scope: f.op.Scope(), LeaseOwner: "edit-worker-one",
		LeaseDuration:   time.Minute,
		ReceiptProvider: "feishu_card_patch:app-test",
		ReceiptTarget:   "message-test/card-test",
	}
	acquired, err := f.state.store.AcquireTaskDefinitionEditOperation(ctx, params)
	if err != nil {
		t.Fatalf("AcquireTaskDefinitionEditOperation: %v", err)
	}
	if acquired.Status != types.TaskDefinitionEditOperationStatusExecuting ||
		acquired.Phase != types.TaskDefinitionEditPhaseProposalSealed ||
		acquired.Fence != 1 || acquired.Attempt != 1 || acquired.ConfirmedAt == nil {
		t.Fatalf("first acquisition=%+v", acquired)
	}
	firstLeaseUntil := *acquired.LeaseUntil
	replayed, err := f.state.store.AcquireTaskDefinitionEditOperation(ctx, params)
	if err != nil || replayed.Fence != 1 || replayed.Attempt != 1 ||
		replayed.LeaseUntil == nil || !replayed.LeaseUntil.Equal(firstLeaseUntil) {
		t.Fatalf("same-owner response-loss replay=%+v err=%v", replayed, err)
	}
	busyParams := params
	busyParams.LeaseOwner = "edit-worker-two"
	if _, err := f.state.store.AcquireTaskDefinitionEditOperation(
		ctx, busyParams); !errors.Is(err, types.ErrTaskDefinitionEditBusy) {
		t.Fatalf("concurrent acquisition error=%v, want busy", err)
	}
	if err := f.state.store.QuiesceTaskDefinitionEdit(ctx, acquired.Lease()); err != nil {
		t.Fatalf("QuiesceTaskDefinitionEdit: %v", err)
	}
	if err := f.state.store.QuiesceTaskDefinitionEdit(ctx, acquired.Lease()); err != nil {
		t.Fatalf("quiesce response-loss replay: %v", err)
	}
	var (
		status      types.ScheduleStatus
		markerID    *string
		markerFence *int64
	)
	if err := f.state.store.pool.QueryRow(ctx,
		`SELECT status, definition_edit_operation_id, definition_edit_fence
		   FROM schedules WHERE tenant_id=$1 AND user_id=$2 AND id=$3`,
		f.op.TenantID, f.op.UserID, f.op.TaskID,
	).Scan(&status, &markerID, &markerFence); err != nil {
		t.Fatal(err)
	}
	if status != types.ScheduleStatusPaused || markerID == nil ||
		*markerID != f.op.ID || markerFence == nil || *markerFence != 1 {
		t.Fatalf("quiesced schedule status=%s marker=%v/%v",
			status, markerID, markerFence)
	}
	if _, err := f.state.store.pool.Exec(ctx,
		`UPDATE task_definition_edit_operations
		    SET lease_until=clock_timestamp()-interval '2 minutes',
		        takeover_not_before=clock_timestamp()-interval '1 minute'
		  WHERE id=$1`, f.op.ID); err != nil {
		t.Fatalf("age operation lease: %v", err)
	}
	taken, err := f.state.store.AcquireTaskDefinitionEditOperation(ctx, busyParams)
	if err != nil {
		t.Fatalf("take over operation: %v", err)
	}
	if taken.Fence != 2 || taken.Attempt != 2 ||
		taken.LeaseOwner != busyParams.LeaseOwner {
		t.Fatalf("takeover=%+v", taken)
	}
	if err := f.state.store.pool.QueryRow(ctx,
		`SELECT definition_edit_fence FROM schedules WHERE id=$1`, f.op.TaskID,
	).Scan(&markerFence); err != nil || markerFence == nil || *markerFence != 2 {
		t.Fatalf("marker fence after takeover=%v err=%v", markerFence, err)
	}
	if err := f.state.store.RenewTaskDefinitionEditLease(
		ctx, acquired.Lease(), time.Minute); !errors.Is(err,
		types.ErrTaskDefinitionEditLeaseLost) {
		t.Fatalf("stale lease renewal error=%v, want lease lost", err)
	}
	if err := f.state.store.BlockTaskDefinitionEditOperation(ctx, taken.Lease(),
		types.TaskDefinitionEditBlockCheckpointInvalid); err != nil {
		t.Fatalf("BlockTaskDefinitionEditOperation: %v", err)
	}
	if err := f.state.store.BlockTaskDefinitionEditOperation(ctx, taken.Lease(),
		types.TaskDefinitionEditBlockCheckpointInvalid); err != nil {
		t.Fatalf("block response-loss replay: %v", err)
	}
	var receiptCount int
	if err := f.state.store.pool.QueryRow(ctx,
		`SELECT count(*) FROM task_definition_edit_receipts
		  WHERE operation_id=$1 AND status='pending'`, f.op.ID,
	).Scan(&receiptCount); err != nil || receiptCount != 1 {
		t.Fatalf("terminal receipt count=%d err=%v", receiptCount, err)
	}
	if err := f.state.store.DeleteSchedule(ctx, f.op.TaskID, f.op.UserID); err != nil {
		t.Fatalf("delete blocked schedule after terminal commit: %v", err)
	}
	if err := f.state.store.BlockTaskDefinitionEditOperation(ctx, taken.Lease(),
		types.TaskDefinitionEditBlockCheckpointInvalid); err != nil {
		t.Fatalf("blocked replay after schedule deletion: %v", err)
	}
}

func TestTaskDefinitionEditCheckpointCommitAndCompleteAtomic(t *testing.T) {
	f := newTaskDefinitionEditOperationFixture(t)
	ctx := t.Context()
	acquired, err := f.state.store.AcquireTaskDefinitionEditOperation(ctx,
		types.AcquireTaskDefinitionEditOperationParams{
			Scope: f.op.Scope(), LeaseOwner: "edit-commit-worker",
			LeaseDuration:   5 * time.Minute,
			ReceiptProvider: "feishu_card_patch:app-test",
			ReceiptTarget:   "message-commit/card-commit",
		})
	if err != nil {
		t.Fatalf("acquire definition edit: %v", err)
	}
	lease := acquired.Lease()
	if err := f.state.store.QuiesceTaskDefinitionEdit(ctx, lease); err != nil {
		t.Fatalf("quiesce definition edit: %v", err)
	}

	basePaused := taskDefinitionEditSnapshotFixture(t, f,
		definitioneditwire.SnapshotPhaseBasePaused, "Aw")
	basePausedDifferent := taskDefinitionEditSnapshotFixture(t, f,
		definitioneditwire.SnapshotPhaseBasePaused, "BA")
	targetPaused := taskDefinitionEditSnapshotFixture(t, f,
		definitioneditwire.SnapshotPhaseTargetPaused, "BQ")
	targetPausedDifferent := taskDefinitionEditSnapshotFixture(t, f,
		definitioneditwire.SnapshotPhaseTargetPaused, "Bg")
	targetFinal := taskDefinitionEditSnapshotFixture(t, f,
		definitioneditwire.SnapshotPhaseTargetFinal, "Bw")
	targetFinalDifferent := taskDefinitionEditSnapshotFixture(t, f,
		definitioneditwire.SnapshotPhaseTargetFinal, "CA")

	if err := f.state.store.CheckpointTaskDefinitionEditTargetApplied(
		ctx, lease, targetPaused); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("out-of-order target checkpoint error=%v, want conflict", err)
	}
	if err := f.state.store.CheckpointTaskDefinitionEditBasePaused(
		ctx, lease, basePaused); err != nil {
		t.Fatalf("checkpoint base paused: %v", err)
	}
	if err := f.state.store.CheckpointTaskDefinitionEditBasePaused(
		ctx, lease, basePaused); err != nil {
		t.Fatalf("replay exact base checkpoint: %v", err)
	}
	if err := f.state.store.CheckpointTaskDefinitionEditBasePaused(
		ctx, lease, basePausedDifferent); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("different base checkpoint error=%v, want conflict", err)
	}

	faultStore := *f.state.store
	var wrapped *compiledTaskFaultTx
	faultStore.beginTx = func(
		ctx context.Context,
		opts pgx.TxOptions,
	) (pgx.Tx, error) {
		realTx, err := f.state.store.beginTx(ctx, opts)
		if err != nil {
			return nil, err
		}
		wrapped = &compiledTaskFaultTx{
			Tx: realTx, failContains: "UPDATE task_definition_edit_operations",
		}
		return wrapped, nil
	}
	if err := faultStore.CommitTaskDefinitionEditDefinition(
		ctx, lease); !errors.Is(err, types.ErrDatabase) {
		t.Fatalf("injected commit error=%v, want database", err)
	}
	if wrapped == nil || !wrapped.fired || wrapped.rollbackCalls != 1 ||
		wrapped.rollbackContextErr != nil {
		t.Fatalf("fault transaction was not cleanly rolled back: %+v", wrapped)
	}
	assertApprovedDefinitionEditProjection(t, f.state, f.base,
		[]int64{f.state.sourceID})
	assertApprovedDefinitionVersionCount(t, f.state, 1)
	afterFault, err := f.state.store.LoadTaskDefinitionEditOperation(ctx, f.op.Scope())
	if err != nil || afterFault.Phase !=
		types.TaskDefinitionEditPhaseTemporalBasePaused {
		t.Fatalf("operation after rolled-back commit=%+v err=%v", afterFault, err)
	}

	if err := f.state.store.CommitTaskDefinitionEditDefinition(ctx, lease); err != nil {
		t.Fatalf("commit approved definition: %v", err)
	}
	if err := f.state.store.CommitTaskDefinitionEditDefinition(ctx, lease); err != nil {
		t.Fatalf("replay approved definition commit: %v", err)
	}
	targetDefinition, err := taskstate.DecodeApprovedDefinitionV1(
		f.op.TargetDefinition)
	if err != nil {
		t.Fatalf("decode fixture target: %v", err)
	}
	targetRecord := ApprovedDefinitionVersionRecord{
		Definition:  targetDefinition,
		Version:     f.op.TargetDefinitionVersion,
		Digest:      f.op.TargetDefinitionDigest,
		Payload:     bytes.Clone(f.op.TargetDefinition),
		ApprovalRef: f.op.ApprovalRef,
	}
	assertApprovedDefinitionEditProjection(t, f.state, targetRecord,
		[]int64{f.state.sourceID})
	assertApprovedDefinitionVersionCount(t, f.state, 2)
	if err := f.state.store.CheckpointTaskDefinitionEditBasePaused(
		ctx, lease, basePaused); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("backward base checkpoint error=%v, want conflict", err)
	}

	if err := f.state.store.CheckpointTaskDefinitionEditTargetApplied(
		ctx, lease, targetPaused); err != nil {
		t.Fatalf("checkpoint target applied: %v", err)
	}
	if err := f.state.store.CheckpointTaskDefinitionEditTargetApplied(
		ctx, lease, targetPaused); err != nil {
		t.Fatalf("replay exact target-applied checkpoint: %v", err)
	}
	if err := f.state.store.CheckpointTaskDefinitionEditTargetApplied(
		ctx, lease, targetPausedDifferent); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("different target-applied checkpoint error=%v, want conflict", err)
	}
	if err := f.state.store.CheckpointTaskDefinitionEditTargetRestored(
		ctx, lease, targetFinal); err != nil {
		t.Fatalf("checkpoint target restored: %v", err)
	}
	if err := f.state.store.CheckpointTaskDefinitionEditTargetRestored(
		ctx, lease, targetFinal); err != nil {
		t.Fatalf("replay exact target-restored checkpoint: %v", err)
	}
	if err := f.state.store.CheckpointTaskDefinitionEditTargetRestored(
		ctx, lease, targetFinalDifferent); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("different target-restored checkpoint error=%v, want conflict", err)
	}

	completionFaultStore := *f.state.store
	var completionWrapped *compiledTaskFaultTx
	completionFaultStore.beginTx = func(
		ctx context.Context,
		opts pgx.TxOptions,
	) (pgx.Tx, error) {
		realTx, err := f.state.store.beginTx(ctx, opts)
		if err != nil {
			return nil, err
		}
		completionWrapped = &compiledTaskFaultTx{
			Tx: realTx, failContains: "UPDATE task_definition_edit_operations",
		}
		return completionWrapped, nil
	}
	if err := completionFaultStore.CompleteTaskDefinitionEditOperation(
		ctx, lease, json.RawMessage(`{"a":"ok","z":1}`)); !errors.Is(err, types.ErrDatabase) {
		t.Fatalf("injected completion error=%v, want database", err)
	}
	if completionWrapped == nil || !completionWrapped.fired ||
		completionWrapped.rollbackCalls != 1 ||
		completionWrapped.rollbackContextErr != nil {
		t.Fatalf("completion fault was not cleanly rolled back: %+v",
			completionWrapped)
	}
	beforeCompletion, err := f.state.store.LoadTaskDefinitionEditOperation(
		ctx, f.op.Scope())
	if err != nil || beforeCompletion.Status !=
		types.TaskDefinitionEditOperationStatusExecuting ||
		beforeCompletion.Phase != types.TaskDefinitionEditPhaseTemporalTargetRestored {
		t.Fatalf("operation after rolled-back completion=%+v err=%v",
			beforeCompletion, err)
	}
	var (
		beforeStatus  types.ScheduleStatus
		beforeMarker  *string
		beforeFence   *int64
		beforeReceipt int
	)
	if err := f.state.store.pool.QueryRow(ctx,
		`SELECT status, definition_edit_operation_id, definition_edit_fence
		   FROM schedules WHERE tenant_id=$1 AND user_id=$2 AND id=$3`,
		f.op.TenantID, f.op.UserID, f.op.TaskID,
	).Scan(&beforeStatus, &beforeMarker, &beforeFence); err != nil {
		t.Fatalf("load schedule after rolled-back completion: %v", err)
	}
	if beforeStatus != types.ScheduleStatusPaused || beforeMarker == nil ||
		*beforeMarker != f.op.ID || beforeFence == nil || *beforeFence != lease.Fence {
		t.Fatalf("schedule after rolled-back completion status=%s marker=%v/%v",
			beforeStatus, beforeMarker, beforeFence)
	}
	if err := f.state.store.pool.QueryRow(ctx,
		`SELECT count(*) FROM task_definition_edit_receipts WHERE operation_id=$1`,
		f.op.ID).Scan(&beforeReceipt); err != nil || beforeReceipt != 0 {
		t.Fatalf("receipt after rolled-back completion count=%d err=%v",
			beforeReceipt, err)
	}

	if err := f.state.store.CompleteTaskDefinitionEditOperation(
		ctx, lease, json.RawMessage(`{"z":1,"a":"ok"}`)); err != nil {
		t.Fatalf("complete definition edit: %v", err)
	}
	if err := f.state.store.CompleteTaskDefinitionEditOperation(
		ctx, lease, json.RawMessage(`{"a":"ok","z":1}`)); err != nil {
		t.Fatalf("replay completed definition edit: %v", err)
	}
	completed, err := f.state.store.LoadTaskDefinitionEditOperation(ctx, f.op.Scope())
	if err != nil {
		t.Fatalf("load completed operation: %v", err)
	}
	canonicalResult, resultErr := canonicalTaskDefinitionEditResult(completed.Result)
	if resultErr != nil || completed.Status !=
		types.TaskDefinitionEditOperationStatusCompleted ||
		completed.Phase != types.TaskDefinitionEditPhaseTemporalTargetRestored ||
		completed.TombstonedAt == nil || completed.LeaseOwner != "" ||
		completed.LeaseUntil != nil ||
		string(canonicalResult) != `{"a":"ok","z":1}` {
		t.Fatalf("completed operation=%+v canonical=%s err=%v",
			completed, canonicalResult, resultErr)
	}
	var (
		status       types.ScheduleStatus
		markerID     *string
		markerFence  *int64
		headVersion  int64
		headDigest   string
		receiptCount int
	)
	if err := f.state.store.pool.QueryRow(ctx,
		`SELECT status, definition_edit_operation_id, definition_edit_fence,
		        approved_definition_version, approved_definition_digest
		   FROM schedules WHERE tenant_id=$1 AND user_id=$2 AND id=$3`,
		f.op.TenantID, f.op.UserID, f.op.TaskID,
	).Scan(&status, &markerID, &markerFence, &headVersion, &headDigest); err != nil {
		t.Fatalf("load completed schedule: %v", err)
	}
	if status != f.op.OriginalStatus || markerID != nil || markerFence != nil ||
		headVersion != f.op.TargetDefinitionVersion ||
		headDigest != f.op.TargetDefinitionDigest {
		t.Fatalf("completed schedule status=%s marker=%v/%v head=%d/%s",
			status, markerID, markerFence, headVersion, headDigest)
	}
	if err := f.state.store.pool.QueryRow(ctx,
		`SELECT count(*) FROM task_definition_edit_receipts
		  WHERE operation_id=$1 AND status='pending'`, f.op.ID,
	).Scan(&receiptCount); err != nil || receiptCount != 1 {
		t.Fatalf("completion receipt count=%d err=%v", receiptCount, err)
	}
	if err := f.state.store.DeleteSchedule(ctx, f.op.TaskID, f.op.UserID); err != nil {
		t.Fatalf("delete completed schedule after terminal commit: %v", err)
	}
	if err := f.state.store.CompleteTaskDefinitionEditOperation(
		ctx, lease, json.RawMessage(`{"a":"ok","z":1}`)); err != nil {
		t.Fatalf("completed replay after schedule deletion: %v", err)
	}
}

func TestTaskDefinitionEditSupersededReplaySurvivesScheduleDeletion(t *testing.T) {
	f := newTaskDefinitionEditOperationFixture(t)
	ctx := t.Context()
	acquired, err := f.state.store.AcquireTaskDefinitionEditOperation(ctx,
		types.AcquireTaskDefinitionEditOperationParams{
			Scope: f.op.Scope(), LeaseOwner: "edit-supersede-worker",
			LeaseDuration:   5 * time.Minute,
			ReceiptProvider: "feishu_card_patch:app-test",
			ReceiptTarget:   "message-supersede/card-supersede",
		})
	if err != nil {
		t.Fatalf("acquire definition edit: %v", err)
	}
	lease := acquired.Lease()
	if err := f.state.store.QuiesceTaskDefinitionEdit(ctx, lease); err != nil {
		t.Fatalf("quiesce definition edit: %v", err)
	}
	advanceApprovedDefinitionForTest(t, f.state, 2,
		"intervening v2 definition")
	advanceApprovedDefinitionForTest(t, f.state, 3,
		"current v3 proves supersession")
	if err := f.state.store.SupersedeTaskDefinitionEditOperation(
		ctx, lease); err != nil {
		t.Fatalf("supersede definition edit: %v", err)
	}
	if err := f.state.store.DeleteSchedule(ctx, f.op.TaskID, f.op.UserID); err != nil {
		t.Fatalf("delete superseded schedule: %v", err)
	}
	if err := f.state.store.SupersedeTaskDefinitionEditOperation(
		ctx, lease); err != nil {
		t.Fatalf("superseded replay after schedule deletion: %v", err)
	}
	var receiptCount int
	if err := f.state.store.pool.QueryRow(ctx,
		`SELECT count(*) FROM task_definition_edit_receipts
		  WHERE operation_id=$1`, f.op.ID,
	).Scan(&receiptCount); err != nil || receiptCount != 1 {
		t.Fatalf("superseded receipt count=%d err=%v", receiptCount, err)
	}
}

func TestTaskDefinitionEditCompleteFinalCASRejectsExpiredLease(t *testing.T) {
	f := newTaskDefinitionEditOperationFixture(t)
	ctx := t.Context()
	acquired, err := f.state.store.AcquireTaskDefinitionEditOperation(ctx,
		types.AcquireTaskDefinitionEditOperationParams{
			Scope: f.op.Scope(), LeaseOwner: "edit-expiry-worker",
			LeaseDuration:   5 * time.Minute,
			ReceiptProvider: "feishu_card_patch:app-test",
			ReceiptTarget:   "message-expiry/card-expiry",
		})
	if err != nil {
		t.Fatalf("acquire definition edit: %v", err)
	}
	lease := acquired.Lease()
	if err := f.state.store.QuiesceTaskDefinitionEdit(ctx, lease); err != nil {
		t.Fatalf("quiesce definition edit: %v", err)
	}
	basePaused := taskDefinitionEditSnapshotFixture(t, f,
		definitioneditwire.SnapshotPhaseBasePaused, "Aw")
	targetPaused := taskDefinitionEditSnapshotFixture(t, f,
		definitioneditwire.SnapshotPhaseTargetPaused, "BQ")
	targetFinal := taskDefinitionEditSnapshotFixture(t, f,
		definitioneditwire.SnapshotPhaseTargetFinal, "Bw")
	if err := f.state.store.CheckpointTaskDefinitionEditBasePaused(
		ctx, lease, basePaused); err != nil {
		t.Fatal(err)
	}
	if err := f.state.store.CommitTaskDefinitionEditDefinition(ctx, lease); err != nil {
		t.Fatal(err)
	}
	if err := f.state.store.CheckpointTaskDefinitionEditTargetApplied(
		ctx, lease, targetPaused); err != nil {
		t.Fatal(err)
	}
	if err := f.state.store.CheckpointTaskDefinitionEditTargetRestored(
		ctx, lease, targetFinal); err != nil {
		t.Fatal(err)
	}
	if _, err := f.state.store.pool.Exec(ctx, `
		UPDATE task_definition_edit_operations
		   SET lease_until=clock_timestamp()+interval '1 second',
		       takeover_not_before=clock_timestamp()+interval '31 seconds'
		 WHERE id=$1 AND fence=$2`, f.op.ID, lease.Fence); err != nil {
		t.Fatalf("shorten completion lease: %v", err)
	}

	delayedStore := *f.state.store
	var wrapped *taskDefinitionEditDelayTerminalTx
	delayedStore.beginTx = func(
		ctx context.Context,
		opts pgx.TxOptions,
	) (pgx.Tx, error) {
		realTx, beginErr := f.state.store.beginTx(ctx, opts)
		if beginErr != nil {
			return nil, beginErr
		}
		wrapped = &taskDefinitionEditDelayTerminalTx{Tx: realTx}
		return wrapped, nil
	}
	if err := delayedStore.CompleteTaskDefinitionEditOperation(
		ctx, lease, json.RawMessage(`{"ok":true}`)); !errors.Is(
		err, types.ErrTaskDefinitionEditLeaseLost) {
		t.Fatalf("completion after final-CAS lease expiry error=%v, want lease lost", err)
	}
	if wrapped == nil || !wrapped.delayed {
		t.Fatal("test did not cross lease expiry immediately before terminal CAS")
	}
	operation, err := f.state.store.LoadTaskDefinitionEditOperation(ctx, f.op.Scope())
	if err != nil {
		t.Fatal(err)
	}
	if operation.Status != types.TaskDefinitionEditOperationStatusExecuting ||
		operation.Phase != types.TaskDefinitionEditPhaseTemporalTargetRestored ||
		operation.TombstonedAt != nil || len(operation.Result) != 0 {
		t.Fatalf("expired terminal CAS changed operation: %+v", operation)
	}
	var status types.ScheduleStatus
	var markerID *string
	var markerFence *int64
	var receiptCount int
	if err := f.state.store.pool.QueryRow(ctx, `
		SELECT status, definition_edit_operation_id, definition_edit_fence
		  FROM schedules WHERE id=$1`, f.op.TaskID,
	).Scan(&status, &markerID, &markerFence); err != nil {
		t.Fatal(err)
	}
	if status != types.ScheduleStatusPaused || markerID == nil ||
		*markerID != f.op.ID || markerFence == nil || *markerFence != lease.Fence {
		t.Fatalf("expired completion did not roll back schedule: %s %v/%v",
			status, markerID, markerFence)
	}
	if err := f.state.store.pool.QueryRow(ctx,
		`SELECT count(*) FROM task_definition_edit_receipts
		  WHERE operation_id=$1`, f.op.ID,
	).Scan(&receiptCount); err != nil || receiptCount != 0 {
		t.Fatalf("expired completion receipt count=%d err=%v", receiptCount, err)
	}
}

func TestTaskDefinitionEditCommitRejectsHistoricalApprovalReplay(t *testing.T) {
	f := newTaskDefinitionEditOperationFixture(t)
	ctx := t.Context()
	acquired, err := f.state.store.AcquireTaskDefinitionEditOperation(ctx,
		types.AcquireTaskDefinitionEditOperationParams{
			Scope: f.op.Scope(), LeaseOwner: "edit-history-worker",
			LeaseDuration:   time.Minute,
			ReceiptProvider: "feishu_card_patch:app-test",
			ReceiptTarget:   "message-history/card-history",
		})
	if err != nil {
		t.Fatalf("acquire historical replay fixture: %v", err)
	}
	lease := acquired.Lease()
	if err := f.state.store.QuiesceTaskDefinitionEdit(ctx, lease); err != nil {
		t.Fatalf("quiesce historical replay fixture: %v", err)
	}
	basePaused := taskDefinitionEditSnapshotFixture(t, f,
		definitioneditwire.SnapshotPhaseBasePaused, "CQ")
	if err := f.state.store.CheckpointTaskDefinitionEditBasePaused(
		ctx, lease, basePaused); err != nil {
		t.Fatalf("checkpoint historical replay fixture: %v", err)
	}
	target, err := taskstate.DecodeApprovedDefinitionV1(f.op.TargetDefinition)
	if err != nil {
		t.Fatalf("decode historical replay target: %v", err)
	}
	if _, err := f.state.store.pool.Exec(ctx,
		`INSERT INTO task_approved_definition_versions (
			tenant_id, user_id, task_id, version, schema_version,
			execution_mode, definition_digest, payload, approval_ref
		 ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
		f.op.TargetTenantID, f.op.TargetUserID, f.op.TaskID,
		f.op.TargetDefinitionVersion, target.SchemaVersion, target.ExecutionMode,
		f.op.TargetDefinitionDigest, f.op.TargetDefinition, f.op.ApprovalRef,
	); err != nil {
		t.Fatalf("seed historical approved row: %v", err)
	}
	if err := f.state.store.CommitTaskDefinitionEditDefinition(
		ctx, lease); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("historical approval replay error=%v, want conflict", err)
	}
	assertApprovedDefinitionEditProjection(t, f.state, f.base,
		[]int64{f.state.sourceID})
	assertApprovedDefinitionVersionCount(t, f.state, 2)
	operation, err := f.state.store.LoadTaskDefinitionEditOperation(ctx, f.op.Scope())
	if err != nil || operation.Phase !=
		types.TaskDefinitionEditPhaseTemporalBasePaused {
		t.Fatalf("operation after historical replay=%+v err=%v", operation, err)
	}
}

func TestTaskDefinitionEditAcquireScheduleOutcomeWins(t *testing.T) {
	tests := []struct {
		name       string
		change     func(*testing.T, taskDefinitionEditOperationFixture)
		wantStatus types.TaskDefinitionEditOperationStatus
		wantCode   string
	}{
		{
			name: "deleted schedule blocks",
			change: func(t *testing.T, f taskDefinitionEditOperationFixture) {
				t.Helper()
				if err := f.state.store.DeleteSchedule(
					t.Context(), f.op.TaskID, f.op.UserID); err != nil {
					t.Fatalf("delete schedule: %v", err)
				}
			},
			wantStatus: types.TaskDefinitionEditOperationStatusBlocked,
			wantCode:   string(types.TaskDefinitionEditBlockScheduleDeleted),
		},
		{
			name: "unsafe status blocks",
			change: func(t *testing.T, f taskDefinitionEditOperationFixture) {
				t.Helper()
				if _, err := f.state.store.pool.Exec(t.Context(),
					`UPDATE schedules SET status='paused'
					  WHERE tenant_id=$1 AND user_id=$2 AND id=$3`,
					f.op.TenantID, f.op.UserID, f.op.TaskID); err != nil {
					t.Fatalf("make schedule state unsafe: %v", err)
				}
			},
			wantStatus: types.TaskDefinitionEditOperationStatusBlocked,
			wantCode:   string(types.TaskDefinitionEditBlockUnsafeRemoteState),
		},
		{
			name: "newer current head supersedes",
			change: func(t *testing.T, f taskDefinitionEditOperationFixture) {
				t.Helper()
				advanceApprovedDefinitionForTest(t, f.state, 2,
					"historical v2 does not authorize the pending edit")
				advanceApprovedDefinitionForTest(t, f.state, 3,
					"current v3 proves supersession")
			},
			wantStatus: types.TaskDefinitionEditOperationStatusSuperseded,
			wantCode:   "definition_superseded",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newTaskDefinitionEditOperationFixture(t)
			tt.change(t, f)
			terminal, err := f.state.store.AcquireTaskDefinitionEditOperation(
				t.Context(), types.AcquireTaskDefinitionEditOperationParams{
					Scope: f.op.Scope(), LeaseOwner: "edit-outcome-worker",
					LeaseDuration:   time.Minute,
					ReceiptProvider: "feishu_card_patch:app-test",
					ReceiptTarget:   "message-outcome/card-outcome",
				})
			if !errors.Is(err, types.ErrTaskDefinitionEditTerminal) {
				t.Fatalf("acquire schedule outcome error=%v, want terminal", err)
			}
			if terminal == nil || terminal.Status != tt.wantStatus ||
				terminal.ErrorCode != tt.wantCode || terminal.TombstonedAt == nil ||
				terminal.ConfirmedAt == nil || terminal.Fence != 1 ||
				terminal.Attempt != 1 || terminal.LeaseOwner != "" {
				t.Fatalf("terminal schedule outcome=%+v", terminal)
			}
			var receiptCount int
			if err := f.state.store.pool.QueryRow(t.Context(),
				`SELECT count(*) FROM task_definition_edit_receipts
				  WHERE operation_id=$1 AND status='pending'`, f.op.ID,
			).Scan(&receiptCount); err != nil || receiptCount != 1 {
				t.Fatalf("terminal receipt count=%d err=%v", receiptCount, err)
			}
		})
	}
}

func TestTaskDefinitionEditStaleTenantShardRLS(t *testing.T) {
	first := newTaskDefinitionEditOperationFixture(t)
	second := newTaskDefinitionEditOperationFixture(t)
	ctx := t.Context()
	for _, fixture := range []taskDefinitionEditOperationFixture{first, second} {
		acquired, err := fixture.state.store.AcquireTaskDefinitionEditOperation(ctx,
			types.AcquireTaskDefinitionEditOperationParams{
				Scope: fixture.op.Scope(), LeaseOwner: "edit-stale-worker",
				LeaseDuration:   time.Minute,
				ReceiptProvider: "feishu_card_patch:app-test",
				ReceiptTarget:   "message-stale/card-stale",
			})
		if err != nil {
			t.Fatalf("acquire stale fixture %s: %v", fixture.op.ID, err)
		}
		if _, err := fixture.state.store.pool.Exec(ctx,
			`UPDATE task_definition_edit_operations
			    SET lease_until=clock_timestamp()-interval '2 minutes',
			        takeover_not_before=clock_timestamp()-interval '1 minute'
			  WHERE id=$1 AND fence=$2`, acquired.ID, acquired.Fence); err != nil {
			t.Fatalf("age stale fixture %s: %v", fixture.op.ID, err)
		}
	}

	tenantIDs, err := first.state.store.ListStaleTaskDefinitionEditTenantIDs(
		ctx, time.Now().Add(time.Hour), 0, 1000)
	if err != nil {
		t.Fatalf("list stale tenant shards: %v", err)
	}
	seenTenants := make(map[int64]bool, len(tenantIDs))
	for _, tenantID := range tenantIDs {
		seenTenants[tenantID] = true
	}
	if !seenTenants[first.op.TenantID] || !seenTenants[second.op.TenantID] {
		t.Fatalf("stale tenant shards=%v, missing fixture tenants %d/%d",
			tenantIDs, first.op.TenantID, second.op.TenantID)
	}
	for _, fixture := range []taskDefinitionEditOperationFixture{first, second} {
		operations, err := fixture.state.store.ListStaleTaskDefinitionEditOperations(
			ctx, fixture.op.TenantID, time.Now().Add(time.Hour), 1000)
		if err != nil {
			t.Fatalf("list tenant %d stale operations: %v", fixture.op.TenantID, err)
		}
		found := false
		for _, operation := range operations {
			if operation.TenantID != fixture.op.TenantID {
				t.Fatalf("tenant %d scan leaked operation %+v",
					fixture.op.TenantID, operation)
			}
			if operation.ID == fixture.op.ID {
				found = true
			}
		}
		if !found {
			t.Fatalf("tenant %d stale scan omitted %s: %+v",
				fixture.op.TenantID, fixture.op.ID, operations)
		}
	}

	tx, err := first.state.store.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatalf("begin direct RLS probe: %v", err)
	}
	defer rollbackTaskDefinitionEditTx(ctx, tx)
	if _, err := tx.Exec(ctx, `SELECT set_config('app.tenant_id',$1,true)`,
		strconv.FormatInt(first.op.TenantID, 10)); err != nil {
		t.Fatalf("set direct RLS tenant: %v", err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_edit_coordinator`); err != nil {
		t.Fatalf("enter coordinator role: %v", err)
	}
	rows, err := tx.Query(ctx,
		`SELECT id, tenant_id FROM task_definition_edit_operations
		  WHERE id=ANY($1::text[]) ORDER BY id`, []string{first.op.ID, second.op.ID})
	if err != nil {
		t.Fatalf("query direct RLS scope: %v", err)
	}
	defer rows.Close()
	var visible []string
	for rows.Next() {
		var id string
		var tenantID int64
		if err := rows.Scan(&id, &tenantID); err != nil {
			t.Fatalf("scan direct RLS scope: %v", err)
		}
		if tenantID != first.op.TenantID {
			t.Fatalf("direct RLS leaked tenant %d operation %s", tenantID, id)
		}
		visible = append(visible, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate direct RLS scope: %v", err)
	}
	if len(visible) != 1 || visible[0] != first.op.ID {
		t.Fatalf("direct RLS visible operations=%v, want only %s",
			visible, first.op.ID)
	}
}

func TestCanonicalTaskDefinitionEditResult(t *testing.T) {
	canonical, err := canonicalTaskDefinitionEditResult(
		json.RawMessage(`{"z":1,"a":"ok"}`))
	if err != nil || string(canonical) != `{"a":"ok","z":1}` {
		t.Fatalf("canonical result=%s err=%v", canonical, err)
	}
	for _, invalid := range []json.RawMessage{
		nil, json.RawMessage(`[]`), json.RawMessage(`{"a":1,"a":2}`),
	} {
		if _, err := canonicalTaskDefinitionEditResult(invalid); err == nil {
			t.Fatalf("invalid result accepted: %s", invalid)
		}
	}
}

func TestTaskDefinitionEditOperationInputBoundaries(t *testing.T) {
	base := types.AcquireTaskDefinitionEditOperationParams{
		Scope: types.TaskDefinitionEditScope{
			ID: "edit-id", TenantID: 1, UserID: 2,
			TargetTenantID: 1, TargetUserID: 2, TaskID: "task-id",
		},
		LeaseOwner: "worker", LeaseDuration: time.Second,
		ReceiptProvider: "provider", ReceiptTarget: "target",
	}
	for _, duration := range []time.Duration{time.Nanosecond, 999 * time.Nanosecond} {
		changed := base
		changed.LeaseDuration = duration
		if err := validateTaskDefinitionEditAcquire(changed); err == nil {
			t.Fatalf("sub-microsecond duration accepted: %s", duration)
		}
	}
	for _, owner := range []string{"worker\nforged", "worker\u200bhidden", "worker\u2060hidden"} {
		changed := base
		changed.LeaseOwner = owner
		if err := validateTaskDefinitionEditAcquire(changed); err == nil {
			t.Fatalf("control/format lease owner accepted: %q", owner)
		}
	}
	for name, mutate := range map[string]func(*types.TaskDefinitionEditScope){
		"cross tenant": func(scope *types.TaskDefinitionEditScope) {
			scope.TargetTenantID++
		},
		"cross user": func(scope *types.TaskDefinitionEditScope) {
			scope.TargetUserID++
		},
	} {
		t.Run(name, func(t *testing.T) {
			changed := base
			mutate(&changed.Scope)
			if err := validateTaskDefinitionEditAcquire(changed); err == nil {
				t.Fatal("v1 actor/target mismatch accepted")
			}
		})
	}
}
