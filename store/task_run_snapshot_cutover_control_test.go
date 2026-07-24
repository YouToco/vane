package store

import (
	"testing"

	"github.com/google/uuid"

	"github.com/YouToco/vane/types"
)

type cutoverControlResult struct {
	EventID                   int64
	Generation                int64
	Action                    string
	ApprovedDefinitionVersion int64
	ApprovedDefinitionDigest  string
	HighWatermark             int64
	AuditFrom                 int64
	AuditCount                int64
	AuditThrough              int64
}

func TestTaskRunSnapshotCutoverControlExactLifecycleAndReplay(t *testing.T) {
	f, taskID, canaryRef := newTaskRunSnapshotCutoverControlFixture(t)

	activation := callTaskRunSnapshotCutoverControl(
		t, f.st, f.tenantID, f.userID, taskID, "activate")
	if activation.Action != "activate" || activation.Generation != 1 ||
		activation.HighWatermark != canaryRef.SnapshotID ||
		activation.AuditFrom != canaryRef.SnapshotID ||
		activation.AuditCount != 1 ||
		activation.AuditThrough != canaryRef.SnapshotID {
		t.Fatalf("activation carrier = %+v", activation)
	}
	replayedActivation := callTaskRunSnapshotCutoverControl(
		t, f.st, f.tenantID, f.userID, taskID, "activate")
	if replayedActivation != activation {
		t.Fatalf("activation replay appended/drifted: first=%+v retry=%+v",
			activation, replayedActivation)
	}
	assertCutoverEventCount(t, f, taskID, 1)

	rollback := callTaskRunSnapshotCutoverControl(
		t, f.st, f.tenantID, f.userID, taskID, "rollback")
	if rollback.Action != "rollback" || rollback.Generation != 2 ||
		rollback.EventID == activation.EventID ||
		rollback.ApprovedDefinitionVersion !=
			activation.ApprovedDefinitionVersion ||
		rollback.ApprovedDefinitionDigest !=
			activation.ApprovedDefinitionDigest ||
		rollback.HighWatermark != activation.HighWatermark ||
		rollback.AuditFrom != activation.AuditFrom ||
		rollback.AuditCount != activation.AuditCount ||
		rollback.AuditThrough != activation.AuditThrough {
		t.Fatalf("rollback is not bound to activation: activate=%+v rollback=%+v",
			activation, rollback)
	}
	replayedRollback := callTaskRunSnapshotCutoverControl(
		t, f.st, f.tenantID, f.userID, taskID, "rollback")
	if replayedRollback != rollback {
		t.Fatalf("rollback replay appended/drifted: first=%+v retry=%+v",
			rollback, replayedRollback)
	}
	assertCutoverEventCount(t, f, taskID, 2)

	reactivation := callTaskRunSnapshotCutoverControl(
		t, f.st, f.tenantID, f.userID, taskID, "activate")
	if reactivation.Action != "activate" ||
		reactivation.Generation != 3 ||
		reactivation.EventID == activation.EventID ||
		reactivation.HighWatermark != canaryRef.SnapshotID ||
		reactivation.AuditCount != 1 {
		t.Fatalf("reactivation did not refreeze exact population: %+v",
			reactivation)
	}
	assertCutoverEventCount(t, f, taskID, 3)
}

func TestTaskRunSnapshotCutoverControlRejectsEmptyOrBadFullAudit(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		f := newTaskRunSnapshotFixture(t)
		taskID := f.taskID()
		f.createApprovedTask(t, taskID, 1)
		applyTaskDefinitionBaseline(t, f, taskID)
		_, err := callTaskRunSnapshotCutoverControlError(
			t, f.st, f.tenantID, f.userID, taskID, "activate")
		requireSQLState038(t, err, "23514")
	})

	t.Run("bad retained row", func(t *testing.T) {
		f, taskID, ref := newTaskRunSnapshotCutoverControlFixture(t)
		if _, err := f.st.pool.Exec(t.Context(),
			`DELETE FROM task_run_snapshot_v2_shadows
			  WHERE run_snapshot_id=$1`,
			ref.SnapshotID); err != nil {
			t.Fatal(err)
		}
		_, err := callTaskRunSnapshotCutoverControlError(
			t, f.st, f.tenantID, f.userID, taskID, "activate")
		requireSQLState038(t, err, "23514")
		assertCutoverEventCount(t, f, taskID, 0)
	})
}

func TestTaskRunSnapshotCutoverControlReplayFailsClosedOnActivePinDrift(
	t *testing.T,
) {
	f, taskID, _ := newTaskRunSnapshotCutoverControlFixture(t)
	activation := callTaskRunSnapshotCutoverControl(
		t, f.st, f.tenantID, f.userID, taskID, "activate")
	if _, err := f.st.pool.Exec(t.Context(),
		`UPDATE schedules
		    SET approved_definition_version=NULL,
		        approved_definition_digest=NULL
		  WHERE tenant_id=$1 AND user_id=$2 AND id=$3`,
		f.tenantID, f.userID, taskID); err != nil {
		t.Fatal(err)
	}
	_, err := callTaskRunSnapshotCutoverControlError(
		t, f.st, f.tenantID, f.userID, taskID, "activate")
	requireSQLState038(t, err, "23514")
	rollback := callTaskRunSnapshotCutoverControl(
		t, f.st, f.tenantID, f.userID, taskID, "rollback")
	if rollback.Action != "rollback" ||
		rollback.ApprovedDefinitionVersion !=
			activation.ApprovedDefinitionVersion {
		t.Fatalf("rollback after active pin drift = %+v", rollback)
	}
}

func TestTaskRunSnapshotCutoverControlAuditsHistoricalDefinitionPin(
	t *testing.T,
) {
	f, taskID, _ := newTaskRunSnapshotCutoverControlFixture(t)
	var digest string
	if err := f.st.pool.QueryRow(t.Context(), `
		INSERT INTO task_approved_definition_versions (
		    tenant_id,user_id,task_id,version,schema_version,
		    execution_mode,definition_digest,payload,approval_ref
		)
		SELECT tenant_id,user_id,task_id,2,schema_version,
		       execution_mode,definition_digest,payload,
		       'cutover-control-version-2'
		  FROM task_approved_definition_versions
		 WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3 AND version=1
		RETURNING definition_digest`,
		f.tenantID, f.userID, taskID).Scan(&digest); err != nil {
		t.Fatal(err)
	}
	if _, err := f.st.pool.Exec(t.Context(),
		`UPDATE schedules
		    SET approved_definition_version=2,
		        approved_definition_digest=$4
		  WHERE tenant_id=$1 AND user_id=$2 AND id=$3`,
		f.tenantID, f.userID, taskID, digest); err != nil {
		t.Fatal(err)
	}
	activation := callTaskRunSnapshotCutoverControl(
		t, f.st, f.tenantID, f.userID, taskID, "activate")
	if activation.ApprovedDefinitionVersion != 2 ||
		activation.ApprovedDefinitionDigest != digest {
		t.Fatalf("activation did not pin current v2 definition: %+v",
			activation)
	}
}

func newTaskRunSnapshotCutoverControlFixture(
	t *testing.T,
) (*taskRunSnapshotFixture, string, types.RunSnapshotRef) {
	t.Helper()
	f := newTaskRunSnapshotFixture(t)
	taskID := f.taskID()
	f.createApprovedTask(t, taskID, 1)
	applyTaskDefinitionBaseline(t, f, taskID)
	identity := types.RunIdentity{
		TemporalWorkflowID: scheduledTaskWorkflowID(taskID),
		TemporalRunID:      "cutover-control-" + uuid.NewString(),
		RunKind:            types.RunSnapshotKindScheduled,
		TenantID:           f.tenantID,
		UserID:             f.userID,
		TaskID:             taskID,
	}
	ref, err := f.st.CreateOrGetCompiledRunSnapshotShadowV2(
		t.Context(), identity, testCompiledRunPolicyV1(t))
	if err != nil {
		t.Fatalf("create control canary: %v", err)
	}
	return f, taskID, ref
}

func applyTaskDefinitionBaseline(
	t *testing.T,
	f *taskRunSnapshotFixture,
	taskID string,
) {
	t.Helper()
	result, err := f.st.reconcileTaskDefinitionBaseline(
		t.Context(), TaskDefinitionBaselineApply, TaskDefinitionBaselineCursor{
			TenantID: f.tenantID, UserID: f.userID, TaskID: taskID,
		})
	if err != nil || result.Status != TaskDefinitionBaselineApplied {
		t.Fatalf("apply control baseline = %+v, %v", result, err)
	}
}

func callTaskRunSnapshotCutoverControl(
	t *testing.T,
	st *Store,
	tenantID, userID int64,
	taskID, action string,
) cutoverControlResult {
	t.Helper()
	result, err := callTaskRunSnapshotCutoverControlError(
		t, st, tenantID, userID, taskID, action)
	if err != nil {
		t.Fatalf("snapshot cutover %s: %v", action, err)
	}
	return result
}

func callTaskRunSnapshotCutoverControlError(
	t *testing.T,
	st *Store,
	tenantID, userID int64,
	taskID, action string,
) (cutoverControlResult, error) {
	t.Helper()
	tx, err := st.pool.Begin(t.Context())
	if err != nil {
		return cutoverControlResult{}, err
	}
	defer rollbackCompiledTaskTx(t.Context(), tx)
	if _, err := tx.Exec(t.Context(),
		`SET LOCAL ROLE vane_snapshot_cutover_operator`); err != nil {
		return cutoverControlResult{}, err
	}
	var result cutoverControlResult
	err = tx.QueryRow(t.Context(),
		`SELECT event_id,generation,action,
		        approved_definition_version,approved_definition_digest,
		        snapshot_high_watermark,audit_from_snapshot_id,
		        audit_count,audit_through_id
		   FROM task_run_snapshot_v2_cutover_control($1,$2,$3,$4)`,
		tenantID, userID, taskID, action,
	).Scan(
		&result.EventID, &result.Generation, &result.Action,
		&result.ApprovedDefinitionVersion,
		&result.ApprovedDefinitionDigest,
		&result.HighWatermark, &result.AuditFrom,
		&result.AuditCount, &result.AuditThrough,
	)
	if err != nil {
		return cutoverControlResult{}, err
	}
	if err := tx.Commit(t.Context()); err != nil {
		return cutoverControlResult{}, err
	}
	return result, nil
}

func assertCutoverEventCount(
	t *testing.T,
	f *taskRunSnapshotFixture,
	taskID string,
	want int,
) {
	t.Helper()
	var got int
	if err := f.st.pool.QueryRow(t.Context(),
		`SELECT count(*) FROM task_run_snapshot_v2_cutover_events
		  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3`,
		f.tenantID, f.userID, taskID,
	).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("cutover event count = %d, want %d", got, want)
	}
}
