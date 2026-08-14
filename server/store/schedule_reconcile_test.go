package store

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/server/types"
)

func TestAcquireScheduleReconcileAuthorizesFreshActiveRowAndSerializesMutation(t *testing.T) {
	st := tenantTestStore(t)
	f := newCompiledTaskFixture(t, st)
	ctx := t.Context()
	taskID := f.taskID()
	if _, err := st.pool.Exec(ctx, `
		INSERT INTO schedules (
			id, tenant_id, user_id, nl_description, spec_json, scope_json,
			status, execution_mode
		) VALUES ($1,$2,$3,'reconcile fixture','{"cron":"0 8 * * *"}','{}',$4,$5)`,
		taskID, f.tenantID, f.userID,
		types.ScheduleStatusActive, types.ExecutionModeCompiled,
	); err != nil {
		t.Fatalf("insert schedule fixture: %v", err)
	}

	acquireCtx, cancelAcquire := context.WithCancel(ctx)
	sc, release, err := st.AcquireScheduleReconcile(
		acquireCtx, f.tenantID, taskID)
	if err != nil {
		t.Fatalf("AcquireScheduleReconcile: %v", err)
	}
	if sc == nil || sc.ID != taskID || sc.Status != types.ScheduleStatusActive {
		t.Fatalf("authorized schedule = %+v", sc)
	}
	cancelAcquire()

	waiting := make(chan error, 1)
	go func() {
		tx, err := st.beginTx(ctx, pgx.TxOptions{})
		if err != nil {
			waiting <- err
			return
		}
		defer rollbackTaskDefinitionEditTx(ctx, tx)
		waiting <- lockTaskScheduleMutation(ctx, tx, taskID)
	}()
	select {
	case err := <-waiting:
		t.Fatalf("second mutation lock completed before release: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	releaseCtx, cancelRelease := context.WithTimeout(context.Background(), time.Second)
	defer cancelRelease()
	if err := release(releaseCtx); err != nil {
		t.Fatalf("release reconcile gate: %v", err)
	}
	select {
	case err := <-waiting:
		if err != nil {
			t.Fatalf("second mutation lock: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("second mutation lock did not acquire after release")
	}

	if _, err := st.pool.Exec(ctx,
		`UPDATE schedules SET status=$2 WHERE id=$1`,
		taskID, types.ScheduleStatusPaused,
	); err != nil {
		t.Fatalf("pause fixture: %v", err)
	}
	sc, release, err = st.AcquireScheduleReconcile(ctx, f.tenantID, taskID)
	if err != nil {
		t.Fatalf("AcquireScheduleReconcile paused: %v", err)
	}
	if release == nil {
		t.Fatal("paused skip must still return a release for the held advisory lock")
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := release(releaseCtx); err != nil {
			t.Errorf("release paused reconcile gate: %v", err)
		}
	}()
	if sc != nil {
		t.Fatalf("paused schedule must not be authorized: %+v", sc)
	}
}

func TestScheduleReconcileRollbackContextIsDetachedAndBounded(t *testing.T) {
	parent, cancelParent := context.WithCancel(t.Context())
	cancelParent()
	rollbackCtx, cancelRollback := scheduleReconcileRollbackContext(parent)
	defer cancelRollback()

	if err := rollbackCtx.Err(); err != nil {
		t.Fatalf("rollback context inherited caller cancellation: %v", err)
	}
	deadline, ok := rollbackCtx.Deadline()
	if !ok {
		t.Fatal("rollback context has no deadline")
	}
	remaining := time.Until(deadline)
	if remaining <= 0 || remaining > scheduleReconcileRollbackTimeout {
		t.Fatalf("rollback deadline remaining = %s, want (0,%s]",
			remaining, scheduleReconcileRollbackTimeout)
	}
}

func TestBeginScheduleStatusChangeCommitsOrRollsBackUnderMutationLock(
	t *testing.T,
) {
	st := tenantTestStore(t)
	f := newCompiledTaskFixture(t, st)
	ctx := t.Context()
	taskID := f.taskID()
	if _, err := st.pool.Exec(ctx, `
		INSERT INTO schedules (
			id, tenant_id, user_id, nl_description, spec_json, scope_json,
			status, execution_mode
		) VALUES ($1,$2,$3,'web lifecycle','{"cron":"0 8 * * *"}','{}',$4,$5)`,
		taskID, f.tenantID, f.userID,
		types.ScheduleStatusActive, types.ExecutionModeCompiled,
	); err != nil {
		t.Fatalf("insert schedule fixture: %v", err)
	}

	commit, rollback, err := st.BeginScheduleStatusChange(
		ctx, taskID, f.userID,
		types.ScheduleStatusActive,
		types.ScheduleStatusPaused,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := rollback(ctx); err != nil {
		t.Fatal(err)
	}
	paused, err := st.GetSchedule(ctx, taskID, f.userID)
	if err != nil || paused.Status != types.ScheduleStatusPaused {
		t.Fatalf("paused=%+v err=%v", paused, err)
	}

	commit, rollback, err = st.BeginScheduleStatusChange(
		ctx, taskID, f.userID,
		types.ScheduleStatusPaused,
		types.ScheduleStatusActive,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := rollback(ctx); err != nil {
		t.Fatal(err)
	}
	rolledBack, err := st.GetSchedule(ctx, taskID, f.userID)
	if err != nil || rolledBack.Status != types.ScheduleStatusPaused {
		t.Fatalf("rolled back=%+v err=%v", rolledBack, err)
	}
	if commit == nil {
		t.Fatal("status change did not return commit callback")
	}
}
