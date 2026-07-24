package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type taskRunSnapshotCutoverFixture struct {
	base       *taskRunSnapshotFixture
	taskID     string
	baseline   TaskDefinitionBaselineResult
	canary     *taskRunSnapshot
	eventID    int64
	generation int64
}

func newTaskRunSnapshotCutoverFixture(
	t *testing.T,
) taskRunSnapshotCutoverFixture {
	t.Helper()
	base := newTaskRunSnapshotFixture(t)
	taskID := base.taskID()
	base.createApprovedTask(t, taskID, 1)
	baseline, err := base.st.reconcileTaskDefinitionBaseline(
		t.Context(), TaskDefinitionBaselineApply, TaskDefinitionBaselineCursor{
			TenantID: base.tenantID,
			UserID:   base.userID,
			TaskID:   taskID,
		})
	if err != nil || baseline.Status != TaskDefinitionBaselineApplied {
		t.Fatalf("apply cutover baseline = %+v, %v", baseline, err)
	}
	canary, err := base.st.createOrGetTaskRunSnapshotWithShadowV2(
		t.Context(), base.params(taskID, "cutover-canary-"+uuid.NewString()), true)
	if err != nil {
		t.Fatalf("create cutover canary: %v", err)
	}
	eventID := insertCutoverEventAndPointSchedule(
		t, base.st, base.tenantID, base.userID, taskID,
		baseline, canary.ID, 1)
	return taskRunSnapshotCutoverFixture{
		base: base, taskID: taskID, baseline: baseline, canary: canary,
		eventID: eventID, generation: 1,
	}
}

func insertCutoverEventAndPointSchedule(
	t *testing.T,
	st *Store,
	tenantID, userID int64,
	taskID string,
	baseline TaskDefinitionBaselineResult,
	highWatermark, generation int64,
) int64 {
	t.Helper()
	tx, err := st.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer rollbackCompiledTaskTx(t.Context(), tx)
	setCutoverTenantContext(t, tx, tenantID)
	lockCutoverSchedule(t, tx, tenantID, userID, taskID)
	eventID := insertCutoverEvent(
		t.Context(), t, tx, tenantID, userID, taskID, baseline,
		highWatermark, generation)
	tag, err := tx.Exec(t.Context(),
		`UPDATE schedules
		    SET run_snapshot_cutover_event_id=$4
		  WHERE tenant_id=$1 AND user_id=$2 AND id=$3`,
		tenantID, userID, taskID, eventID)
	if err != nil {
		t.Fatalf("point schedule at cutover event: %v", err)
	}
	if tag.RowsAffected() != 1 {
		t.Fatalf("point schedule at cutover event affected %d rows",
			tag.RowsAffected())
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatalf("commit cutover event: %v", err)
	}
	return eventID
}

func insertCutoverEvent(
	ctx context.Context,
	t *testing.T,
	tx pgx.Tx,
	tenantID, userID int64,
	taskID string,
	baseline TaskDefinitionBaselineResult,
	highWatermark, generation int64,
) int64 {
	t.Helper()
	var eventID int64
	if err := tx.QueryRow(ctx,
		`INSERT INTO task_run_snapshot_v2_cutover_events (
		    tenant_id, user_id, task_id, generation, action,
		    approved_definition_version, approved_definition_digest,
		    snapshot_high_watermark, audit_from_snapshot_id,
		    audit_count, audit_through_id
		 ) VALUES ($1,$2,$3,$4,'activate',$5,$6,$7,$7,1,$7)
		 RETURNING id`,
		tenantID, userID, taskID, generation,
		baseline.Version, baseline.Digest, highWatermark,
	).Scan(&eventID); err != nil {
		t.Fatalf("insert cutover event: %v", err)
	}
	return eventID
}

func TestTaskRunSnapshotCutoverFenceRejectsUnmarkedProductionWriter(t *testing.T) {
	f := newTaskRunSnapshotCutoverFixture(t)
	runID := "cutover-old-writer-" + uuid.NewString()
	_, err := f.base.st.createOrGetTaskRunSnapshotWithShadowV2(
		t.Context(), f.base.params(f.taskID, runID), true)
	if err == nil {
		t.Fatal("active cutover accepted a production writer with a NULL marker")
	}
	var parents, shadows int
	if err := f.base.st.pool.QueryRow(t.Context(),
		`SELECT
		   (SELECT count(*) FROM task_run_snapshots WHERE temporal_run_id=$1),
		   (SELECT count(*) FROM task_run_snapshot_v2_shadows
		     WHERE temporal_run_id=$1)`,
		runID).Scan(&parents, &shadows); err != nil {
		t.Fatal(err)
	}
	if parents != 0 || shadows != 0 {
		t.Fatalf("rejected old writer persisted parent/shadow = %d/%d",
			parents, shadows)
	}
}

func TestTaskRunSnapshotCutoverFenceAcceptsControlledMarkedWriter(t *testing.T) {
	f := newTaskRunSnapshotCutoverFixture(t)
	runID := "cutover-marked-" + uuid.NewString()
	eventID := f.eventID
	snapshot, err := f.base.st.createOrGetTaskRunSnapshotWithAuthorityV2(
		t.Context(), f.base.params(f.taskID, runID), true, &eventID)
	if err != nil {
		t.Fatalf("create controlled marked snapshot: %v", err)
	}
	if snapshot.V2CutoverEventID == nil ||
		*snapshot.V2CutoverEventID != eventID ||
		snapshot.ID <= f.canary.ID {
		t.Fatalf("marked snapshot = id %d event %v, want event %d above %d",
			snapshot.ID, snapshot.V2CutoverEventID, eventID, f.canary.ID)
	}
	var status string
	if err := f.base.st.pool.QueryRow(t.Context(),
		`SELECT status FROM task_run_snapshot_v2_shadows
		  WHERE run_snapshot_id=$1`,
		snapshot.ID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != string(TaskRunSnapshotShadowMatch) {
		t.Fatalf("marked snapshot sidecar status = %q", status)
	}
}

func TestTaskRunSnapshotCutoverAuditSurvivesScheduleDeletion(t *testing.T) {
	for _, marked := range []bool{false, true} {
		t.Run(fmt.Sprintf("marked=%v", marked), func(t *testing.T) {
			f := newTaskRunSnapshotCutoverFixture(t)
			wantParents := 1
			wantShadows := 1
			if marked {
				eventID := f.eventID
				if _, err := f.base.st.createOrGetTaskRunSnapshotWithAuthorityV2(
					t.Context(),
					f.base.params(f.taskID, "cutover-delete-"+uuid.NewString()),
					true, &eventID,
				); err != nil {
					t.Fatalf("create marked run: %v", err)
				}
				wantParents++
				wantShadows++
			}
			if err := f.base.st.DeleteSchedule(
				t.Context(), f.taskID, f.base.userID); err != nil {
				t.Fatalf("delete cutover schedule: %v", err)
			}
			var schedules, definitions, events, parents, shadows int
			if err := f.base.st.pool.QueryRow(t.Context(),
				`SELECT
				   (SELECT count(*) FROM schedules WHERE id=$1),
				   (SELECT count(*) FROM task_approved_definition_versions
				     WHERE task_id=$1),
				   (SELECT count(*) FROM task_run_snapshot_v2_cutover_events
				     WHERE task_id=$1),
				   (SELECT count(*) FROM task_run_snapshots WHERE task_id=$1),
				   (SELECT count(*) FROM task_run_snapshot_v2_shadows
				     WHERE task_id=$1)`,
				f.taskID,
			).Scan(&schedules, &definitions, &events, &parents, &shadows); err != nil {
				t.Fatal(err)
			}
			if schedules != 0 || definitions != 0 || events != 1 ||
				parents != wantParents || shadows != wantShadows {
				t.Fatalf("post-delete schedule/definition/event/parent/shadow = "+
					"%d/%d/%d/%d/%d, want 0/0/1/%d/%d",
					schedules, definitions, events, parents, shadows,
					wantParents, wantShadows)
			}
		})
	}
}

func TestTaskRunSnapshotCutoverRecreatedTaskUsesNextGeneration(t *testing.T) {
	f := newTaskRunSnapshotCutoverFixture(t)
	if err := f.base.st.DeleteSchedule(
		t.Context(), f.taskID, f.base.userID); err != nil {
		t.Fatalf("delete first task generation: %v", err)
	}
	oldPrefix := f.base.urlPrefix
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(ctx, t, f.base.st,
			`DELETE FROM sources WHERE url LIKE $1`, oldPrefix+"%")
	})
	f.base.urlPrefix += "/recreated"
	f.base.createApprovedTask(t, f.taskID, 1)
	baseline, err := f.base.st.reconcileTaskDefinitionBaseline(
		t.Context(), TaskDefinitionBaselineApply, TaskDefinitionBaselineCursor{
			TenantID: f.base.tenantID,
			UserID:   f.base.userID,
			TaskID:   f.taskID,
		})
	if err != nil || baseline.Status != TaskDefinitionBaselineApplied {
		t.Fatalf("baseline recreated task = %+v, %v", baseline, err)
	}
	canary, err := f.base.st.createOrGetTaskRunSnapshotWithShadowV2(
		t.Context(),
		f.base.params(f.taskID, "cutover-recreated-"+uuid.NewString()), true)
	if err != nil {
		t.Fatalf("recreated task canary: %v", err)
	}
	eventID := insertCutoverEventAndPointSchedule(
		t, f.base.st, f.base.tenantID, f.base.userID, f.taskID,
		baseline, canary.ID, 2)
	var generation int64
	if err := f.base.st.pool.QueryRow(t.Context(),
		`SELECT generation FROM task_run_snapshot_v2_cutover_events
		  WHERE id=$1`,
		eventID).Scan(&generation); err != nil {
		t.Fatal(err)
	}
	if generation != 2 {
		t.Fatalf("recreated task generation = %d, want 2", generation)
	}
}

func TestTaskRunSnapshotCutoverMarkerRequiresSidecarAtCommit(t *testing.T) {
	f := newTaskRunSnapshotCutoverFixture(t)
	tx, err := f.base.st.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer rollbackCompiledTaskTx(t.Context(), tx)
	setCutoverTenantContext(t, tx, f.base.tenantID)
	runID := "cutover-missing-sidecar-" + uuid.NewString()
	if _, err := tx.Exec(t.Context(), cloneTaskRunSnapshotSQL,
		f.canary.ID, runID, f.eventID); err != nil {
		t.Fatalf("insert marked parent before deferred check: %v", err)
	}
	if err := tx.Commit(t.Context()); err == nil {
		t.Fatal("marked parent committed without its exact sidecar")
	}
	var count int
	if err := f.base.st.pool.QueryRow(t.Context(),
		`SELECT count(*) FROM task_run_snapshots WHERE temporal_run_id=$1`,
		runID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("failed marked transaction left %d parent rows", count)
	}
}

func TestTaskRunSnapshotCutoverMarkerRejectsSidecarEmbeddingOtherParent(
	t *testing.T,
) {
	f := newTaskRunSnapshotCutoverFixture(t)
	otherParams := f.base.params(
		f.taskID, "cutover-other-parent-"+uuid.NewString())
	otherParams.PromptPolicyJSON = []byte(`{"score":"other-policy"}`)
	eventID := f.eventID
	other, err := f.base.st.createOrGetTaskRunSnapshotWithAuthorityV2(
		t.Context(), otherParams, true, &eventID)
	if err != nil {
		t.Fatalf("create other marked parent: %v", err)
	}

	tx, err := f.base.st.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer rollbackCompiledTaskTx(t.Context(), tx)
	setCutoverTenantContext(t, tx, f.base.tenantID)
	runID := "cutover-wrong-embedded-parent-" + uuid.NewString()
	var parentID int64
	var workflowID string
	if err := tx.QueryRow(t.Context(),
		cloneTaskRunSnapshotSQL+` RETURNING id, temporal_workflow_id`,
		f.canary.ID, runID, f.eventID,
	).Scan(&parentID, &workflowID); err != nil {
		t.Fatalf("insert marked parent: %v", err)
	}
	if _, err := tx.Exec(t.Context(), `
		WITH source AS (
		    SELECT tenant_id,user_id,task_id,status,
		           approved_definition_version,approved_definition_digest,
		           adaptive_version,adaptive_digest,
		           convert_from(payload,'UTF8')::jsonb AS body
		      FROM task_run_snapshot_v2_shadows
		     WHERE run_snapshot_id=$2
		), rewritten AS (
		    SELECT *,
		           jsonb_set(
		             jsonb_set(
		               jsonb_set(body,
		                 '{identity,temporal_workflow_id}',to_jsonb($3::text)),
		               '{identity,temporal_run_id}',to_jsonb($4::text)),
		             '{legacy,snapshot_id}',to_jsonb($1::bigint)
		           ) AS value
		      FROM source
		)
		INSERT INTO task_run_snapshot_v2_shadows (
		    run_snapshot_id,tenant_id,user_id,task_id,
		    temporal_workflow_id,temporal_run_id,status,
		    approved_definition_version,approved_definition_digest,
		    adaptive_version,adaptive_digest,payload,payload_digest
		)
		SELECT $1,tenant_id,user_id,task_id,$3,$4,status,
		       approved_definition_version,approved_definition_digest,
		       adaptive_version,adaptive_digest,
		       convert_to(value::text,'UTF8'),
		       encode(sha256(convert_to(value::text,'UTF8')),'hex')
		  FROM rewritten`,
		parentID, other.ID, workflowID, runID); err != nil {
		t.Fatalf("insert structurally valid wrong-parent sidecar: %v", err)
	}
	if err := tx.Commit(t.Context()); err == nil {
		t.Fatal("marked parent accepted a sidecar embedding another parent")
	}
}

func TestTaskRunSnapshotCutoverActivePinDriftFailsAllNewWriters(t *testing.T) {
	f := newTaskRunSnapshotCutoverFixture(t)
	if _, err := f.base.st.pool.Exec(t.Context(),
		`UPDATE schedules
		    SET approved_definition_version=NULL,
		        approved_definition_digest=NULL
		  WHERE tenant_id=$1 AND user_id=$2 AND id=$3`,
		f.base.tenantID, f.base.userID, f.taskID); err != nil {
		t.Fatalf("drift current Approved head: %v", err)
	}
	for _, marked := range []bool{false, true} {
		runID := fmt.Sprintf("cutover-pin-drift-%v-%s", marked, uuid.NewString())
		var eventID *int64
		if marked {
			value := f.eventID
			eventID = &value
		}
		_, err := f.base.st.createOrGetTaskRunSnapshotWithAuthorityV2(
			t.Context(), f.base.params(f.taskID, runID), true, eventID)
		if err == nil {
			t.Fatalf("active pin drift accepted marked=%v writer", marked)
		}
	}
}

func TestTaskRunSnapshotCutoverPointerStateMachine(t *testing.T) {
	f := newTaskRunSnapshotCutoverFixture(t)
	tx, err := f.base.st.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	setCutoverTenantContext(t, tx, f.base.tenantID)
	if _, err := tx.Exec(t.Context(),
		`UPDATE schedules SET run_snapshot_cutover_event_id=NULL
		  WHERE tenant_id=$1 AND user_id=$2 AND id=$3`,
		f.base.tenantID, f.base.userID, f.taskID); err == nil {
		t.Fatal("pointer state machine accepted direct clear")
	}
	_ = tx.Rollback(t.Context())

	rollbackID := insertRollbackEvent(t, f, f.eventID, 2)
	tx, err = f.base.st.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer rollbackCompiledTaskTx(t.Context(), tx)
	setCutoverTenantContext(t, tx, f.base.tenantID)
	if _, err := tx.Exec(t.Context(),
		`UPDATE schedules SET run_snapshot_cutover_event_id=$4
		  WHERE tenant_id=$1 AND user_id=$2 AND id=$3`,
		f.base.tenantID, f.base.userID, f.taskID, f.eventID); err == nil {
		t.Fatal("pointer state machine accepted historical activation backpoint")
	}
	_ = tx.Rollback(t.Context())

	tx, err = f.base.st.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer rollbackCompiledTaskTx(t.Context(), tx)
	setCutoverTenantContext(t, tx, f.base.tenantID)
	lockCutoverSchedule(
		t, tx, f.base.tenantID, f.base.userID, f.taskID)
	reactivationID := insertCutoverEvent(
		t.Context(), t, tx, f.base.tenantID, f.base.userID, f.taskID,
		f.baseline, f.canary.ID, 3)
	if _, err := tx.Exec(t.Context(),
		`UPDATE schedules SET run_snapshot_cutover_event_id=$4
		  WHERE tenant_id=$1 AND user_id=$2 AND id=$3`,
		f.base.tenantID, f.base.userID, f.taskID, reactivationID); err != nil {
		t.Fatalf("rollback->next activation: %v", err)
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	if rollbackID == reactivationID {
		t.Fatal("rollback and reactivation events collided")
	}
}

func TestTaskRunSnapshotCutoverOldBinaryWithoutTenantContext(
	t *testing.T,
) {
	base := newTaskRunSnapshotFixture(t)
	taskID := base.taskID()
	base.createApprovedTask(t, taskID, 1)
	runBefore := "cutover-old-inactive-" + uuid.NewString()
	if _, err := base.st.pool.Exec(t.Context(), oldTaskRunSnapshotInsertSQL,
		base.tenantID, base.userID, taskID, runBefore); err != nil {
		t.Fatalf("pre-037 old writer under NULL pointer: %v", err)
	}
	baseline, err := base.st.reconcileTaskDefinitionBaseline(
		t.Context(), TaskDefinitionBaselineApply, TaskDefinitionBaselineCursor{
			TenantID: base.tenantID, UserID: base.userID, TaskID: taskID,
		})
	if err != nil || baseline.Status != TaskDefinitionBaselineApplied {
		t.Fatalf("apply baseline = %+v, %v", baseline, err)
	}
	canary, err := base.st.createOrGetTaskRunSnapshotWithShadowV2(
		t.Context(), base.params(taskID, "cutover-old-canary-"+uuid.NewString()), true)
	if err != nil {
		t.Fatal(err)
	}
	insertCutoverEventAndPointSchedule(
		t, base.st, base.tenantID, base.userID, taskID,
		baseline, canary.ID, 1)
	runAfter := "cutover-old-active-" + uuid.NewString()
	if _, err := base.st.pool.Exec(t.Context(), oldTaskRunSnapshotInsertSQL,
		base.tenantID, base.userID, taskID, runAfter); err == nil {
		t.Fatal("old writer without tenant context crossed active admission")
	}
}

func TestTaskRunSnapshotCutoverFenceHidesCrossTenantStateFromVaneApp(
	t *testing.T,
) {
	f := newTaskRunSnapshotCutoverFixture(t)
	other := newTaskRunSnapshotFixture(t)
	var messages []string
	for name, tenantID := range map[string]*int64{
		"missing": nil,
		"cross":   &other.tenantID,
	} {
		t.Run(name, func(t *testing.T) {
			tx, err := f.base.st.pool.Begin(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer rollbackCompiledTaskTx(t.Context(), tx)
			if tenantID != nil {
				setCutoverTenantContext(t, tx, *tenantID)
			}
			if _, err := tx.Exec(t.Context(), `SET LOCAL ROLE vane_app`); err != nil {
				t.Fatal(err)
			}
			runID := "cutover-tenant-oracle-" + name + "-" + uuid.NewString()
			_, err = tx.Exec(t.Context(), oldTaskRunSnapshotInsertSQL,
				f.base.tenantID, f.base.userID, f.taskID, runID)
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) || pgErr.Code != "23514" {
				t.Fatalf("tenant oracle error = %v", err)
			}
			messages = append(messages, pgErr.Message)

			lockTx, err := f.base.st.pool.Begin(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer rollbackCompiledTaskTx(t.Context(), lockTx)
			if _, err := lockTx.Exec(t.Context(),
				`SELECT id FROM schedules
				  WHERE tenant_id=$1 AND user_id=$2 AND id=$3
				  FOR UPDATE NOWAIT`,
				f.base.tenantID, f.base.userID, f.taskID); err != nil {
				t.Fatalf("failed tenant oracle retained schedule lock: %v", err)
			}
		})
	}
	if len(messages) != 2 || messages[0] != messages[1] {
		t.Fatalf("missing/cross tenant errors differ: %v", messages)
	}
}

func TestTaskRunSnapshotCutoverAdmissionSerializesStaleRRWriter(t *testing.T) {
	base := newTaskRunSnapshotFixture(t)
	taskID := base.taskID()
	base.createApprovedTask(t, taskID, 1)
	baseline, err := base.st.reconcileTaskDefinitionBaseline(
		t.Context(), TaskDefinitionBaselineApply, TaskDefinitionBaselineCursor{
			TenantID: base.tenantID, UserID: base.userID, TaskID: taskID,
		})
	if err != nil || baseline.Status != TaskDefinitionBaselineApplied {
		t.Fatalf("apply baseline = %+v, %v", baseline, err)
	}
	canary, err := base.st.createOrGetTaskRunSnapshotWithShadowV2(
		t.Context(), base.params(taskID, "cutover-race-canary-"+uuid.NewString()), true)
	if err != nil {
		t.Fatal(err)
	}

	stale, err := base.st.pool.BeginTx(t.Context(),
		pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		t.Fatal(err)
	}
	defer rollbackCompiledTaskTx(t.Context(), stale)
	setCutoverTenantContext(t, stale, base.tenantID)

	activation, err := base.st.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer rollbackCompiledTaskTx(t.Context(), activation)
	setCutoverTenantContext(t, activation, base.tenantID)
	lockCutoverSchedule(
		t, activation, base.tenantID, base.userID, taskID)
	eventID := insertCutoverEvent(
		t.Context(), t, activation, base.tenantID, base.userID,
		taskID, baseline, canary.ID, 1)
	if _, err := activation.Exec(t.Context(),
		`UPDATE schedules SET run_snapshot_cutover_event_id=$4
		  WHERE tenant_id=$1 AND user_id=$2 AND id=$3`,
		base.tenantID, base.userID, taskID, eventID); err != nil {
		t.Fatal(err)
	}

	runID := "cutover-stale-writer-" + uuid.NewString()
	writerResult := make(chan error, 1)
	go func() {
		_, err := stale.Exec(context.Background(), cloneTaskRunSnapshotSQL,
			canary.ID, runID, nil)
		writerResult <- err
	}()
	select {
	case err := <-writerResult:
		t.Fatalf("stale writer did not wait for activation fence: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	if err := activation.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-writerResult:
		if err == nil {
			t.Fatal("stale RR writer committed after activation")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stale RR writer did not converge after activation commit")
	}
	_ = stale.Rollback(t.Context())
	var count int
	if err := base.st.pool.QueryRow(t.Context(),
		`SELECT count(*) FROM task_run_snapshots WHERE temporal_run_id=$1`,
		runID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("stale RR writer left %d parent rows", count)
	}
}

func TestTaskRunSnapshotCutoverEventRejectsRecursiveRollback(
	t *testing.T,
) {
	f := newTaskRunSnapshotCutoverFixture(t)
	rollbackID := insertRollbackEvent(t, f, f.eventID, 2)
	tx, err := f.base.st.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer rollbackCompiledTaskTx(t.Context(), tx)
	setCutoverTenantContext(t, tx, f.base.tenantID)
	_, err = tx.Exec(t.Context(),
		`INSERT INTO task_run_snapshot_v2_cutover_events (
		    tenant_id,user_id,task_id,generation,action,reverts_event_id,
		    approved_definition_version,approved_definition_digest,
		    snapshot_high_watermark,audit_from_snapshot_id,
		    audit_count,audit_through_id
		 ) SELECT tenant_id,user_id,task_id,3,'rollback',$1,
		          approved_definition_version,approved_definition_digest,
		          snapshot_high_watermark,audit_from_snapshot_id,
		          audit_count,audit_through_id
		     FROM task_run_snapshot_v2_cutover_events WHERE id=$1`,
		rollbackID)
	if err == nil {
		t.Fatal("cutover event accepted rollback of a rollback")
	}
}

func insertRollbackEvent(
	t *testing.T,
	f taskRunSnapshotCutoverFixture,
	activationID, generation int64,
) int64 {
	t.Helper()
	tx, err := f.base.st.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer rollbackCompiledTaskTx(t.Context(), tx)
	setCutoverTenantContext(t, tx, f.base.tenantID)
	lockCutoverSchedule(
		t, tx, f.base.tenantID, f.base.userID, f.taskID)
	var rollbackID int64
	if err := tx.QueryRow(t.Context(),
		`INSERT INTO task_run_snapshot_v2_cutover_events (
		    tenant_id,user_id,task_id,generation,action,reverts_event_id,
		    approved_definition_version,approved_definition_digest,
		    snapshot_high_watermark,audit_from_snapshot_id,
		    audit_count,audit_through_id
		 ) SELECT tenant_id,user_id,task_id,$2,'rollback',$1,
		          approved_definition_version,approved_definition_digest,
		          snapshot_high_watermark,audit_from_snapshot_id,
		          audit_count,audit_through_id
		     FROM task_run_snapshot_v2_cutover_events WHERE id=$1
		 RETURNING id`,
		activationID, generation).Scan(&rollbackID); err != nil {
		t.Fatalf("insert rollback event: %v", err)
	}
	if _, err := tx.Exec(t.Context(),
		`UPDATE schedules SET run_snapshot_cutover_event_id=$4
		  WHERE tenant_id=$1 AND user_id=$2 AND id=$3`,
		f.base.tenantID, f.base.userID, f.taskID, rollbackID); err != nil {
		t.Fatalf("point schedule at rollback event: %v", err)
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatalf("commit rollback event: %v", err)
	}
	return rollbackID
}

func setCutoverTenantContext(t *testing.T, tx pgx.Tx, tenantID int64) {
	t.Helper()
	if _, err := tx.Exec(t.Context(),
		`SELECT set_config('app.tenant_id', $1, true)`,
		fmt.Sprintf("%d", tenantID)); err != nil {
		t.Fatalf("set cutover tenant context: %v", err)
	}
}

func lockCutoverSchedule(
	t *testing.T,
	tx pgx.Tx,
	tenantID, userID int64,
	taskID string,
) {
	t.Helper()
	var lockedID string
	if err := tx.QueryRow(t.Context(),
		`SELECT id FROM schedules
		  WHERE tenant_id=$1 AND user_id=$2 AND id=$3
		  FOR UPDATE`,
		tenantID, userID, taskID).Scan(&lockedID); err != nil {
		t.Fatalf("lock cutover schedule: %v", err)
	}
	if lockedID != taskID {
		t.Fatalf("locked cutover schedule = %q, want %q", lockedID, taskID)
	}
}

const cloneTaskRunSnapshotSQL = `
INSERT INTO task_run_snapshots (
    id, tenant_id, user_id, task_id, temporal_workflow_id, temporal_run_id,
    run_kind, execution_mode, adaptive_version, capability_catalog_digest,
    tool_policy_digest, prompt_policy_digest, model_policy_digest,
    quota_policy_digest, definition_digest, plan_digest, payload_digest,
    reference_digest, reference_schema_version, payload, budget,
    v2_cutover_event_id
)
SELECT nextval('task_run_snapshots_id_seq'), tenant_id, user_id, task_id,
       temporal_workflow_id || '-clone-' || $2, $2,
       run_kind, execution_mode, adaptive_version, capability_catalog_digest,
       tool_policy_digest, prompt_policy_digest, model_policy_digest,
       quota_policy_digest, definition_digest, plan_digest, payload_digest,
       reference_digest, reference_schema_version, payload, budget, $3
 FROM task_run_snapshots
 WHERE id=$1`

const oldTaskRunSnapshotInsertSQL = `
INSERT INTO task_run_snapshots (
    tenant_id, user_id, task_id, temporal_workflow_id, temporal_run_id,
    run_kind, execution_mode, adaptive_version, capability_catalog_digest,
    tool_policy_digest, prompt_policy_digest, model_policy_digest,
    quota_policy_digest, definition_digest, plan_digest, payload_digest,
    reference_digest, reference_schema_version, payload, budget
) VALUES (
    $1,$2,$3,'old-workflow-' || $4,$4,
    'scheduled','compiled',0,repeat('0',64),repeat('1',64),
    repeat('2',64),repeat('3',64),repeat('4',64),repeat('5',64),
    repeat('6',64),encode(sha256(convert_to('{}','UTF8')),'hex'),
    repeat('8',64),'old-worker/v1',convert_to('{}','UTF8'),'{}'::jsonb
)`

func TestTaskRunSnapshotCutoverMigrationPrivileges(t *testing.T) {
	f := newTaskRunSnapshotFixture(t)
	var selectOK, insertOK, updateOK, deleteOK, sequenceOK bool
	if err := f.st.pool.QueryRow(t.Context(),
		`SELECT
		    has_table_privilege('vane_app',
		        'task_run_snapshot_v2_cutover_events','SELECT'),
		    has_table_privilege('vane_app',
		        'task_run_snapshot_v2_cutover_events','INSERT'),
		    has_table_privilege('vane_app',
		        'task_run_snapshot_v2_cutover_events','UPDATE'),
		    has_table_privilege('vane_app',
		        'task_run_snapshot_v2_cutover_events','DELETE'),
		    has_sequence_privilege('vane_app',
		        'task_run_snapshot_v2_cutover_events_id_seq','USAGE')`,
	).Scan(&selectOK, &insertOK, &updateOK, &deleteOK, &sequenceOK); err != nil {
		t.Fatal(err)
	}
	if !selectOK || insertOK || updateOK || deleteOK || sequenceOK {
		t.Fatalf("vane_app cutover privileges select/insert/update/delete/seq = "+
			"%v/%v/%v/%v/%v",
			selectOK, insertOK, updateOK, deleteOK, sequenceOK)
	}
	rows, err := f.st.pool.Query(t.Context(),
		`SELECT p.proname, p.prosecdef,
		        COALESCE(array_to_string(p.proconfig, ','), ''),
		        has_function_privilege(
		            'vane_app', p.oid, 'EXECUTE')
		   FROM pg_proc p
		  WHERE p.proname = ANY($1::text[])
		  ORDER BY p.proname`,
		[]string{
			"task_run_snapshot_v2_admission_fence",
			"task_run_snapshot_v2_cutover_event_integrity",
			"task_run_snapshot_v2_cutover_pointer_transition",
			"task_run_snapshot_v2_marker_integrity",
		})
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	wantSecurityDefiner := map[string]bool{
		"task_run_snapshot_v2_admission_fence":            false,
		"task_run_snapshot_v2_cutover_event_integrity":    true,
		"task_run_snapshot_v2_cutover_pointer_transition": true,
		"task_run_snapshot_v2_marker_integrity":           false,
	}
	count := 0
	for rows.Next() {
		var name, config string
		var securityDefiner, executable bool
		if err := rows.Scan(
			&name, &securityDefiner, &config, &executable); err != nil {
			t.Fatal(err)
		}
		count++
		if securityDefiner != wantSecurityDefiner[name] ||
			!strings.Contains(config, "search_path=pg_catalog, public") ||
			executable {
			t.Fatalf("%s security-definer/config/app-execute = %v/%q/%v",
				name, securityDefiner, config, executable)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if count != 4 {
		t.Fatalf("cutover security functions = %d, want 4", count)
	}
}

func TestTaskRunSnapshotCutoverTenantPurgeReportsAndDeletesFence(t *testing.T) {
	f := newTaskRunSnapshotCutoverFixture(t)
	eventID := f.eventID
	if _, err := f.base.st.createOrGetTaskRunSnapshotWithAuthorityV2(
		t.Context(),
		f.base.params(f.taskID, "cutover-purge-"+uuid.NewString()),
		true, &eventID,
	); err != nil {
		t.Fatalf("create marked purge run: %v", err)
	}
	dry, err := f.base.st.PurgeTenant(t.Context(), f.base.tenantID, true)
	if err != nil {
		t.Fatalf("dry-run cutover purge: %v", err)
	}
	if dry.Rows["task_run_snapshot_v2_shadows"] != 2 ||
		dry.Rows["task_run_snapshots"] != 2 ||
		dry.Rows["task_run_snapshot_v2_cutover_events"] != 1 {
		t.Fatalf("dry-run cutover rows shadows/parents/events = %d/%d/%d",
			dry.Rows["task_run_snapshot_v2_shadows"],
			dry.Rows["task_run_snapshots"],
			dry.Rows["task_run_snapshot_v2_cutover_events"])
	}
	var events int
	if err := f.base.st.pool.QueryRow(t.Context(),
		`SELECT count(*) FROM task_run_snapshot_v2_cutover_events
		  WHERE tenant_id=$1`,
		f.base.tenantID).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if events != 1 {
		t.Fatalf("dry-run removed %d cutover events", 1-events)
	}
	real, err := f.base.st.PurgeTenant(t.Context(), f.base.tenantID, false)
	if err != nil {
		t.Fatalf("real cutover purge: %v", err)
	}
	if real.Rows["task_run_snapshot_v2_shadows"] != 2 ||
		real.Rows["task_run_snapshots"] != 2 ||
		real.Rows["task_run_snapshot_v2_cutover_events"] != 1 {
		t.Fatalf("real cutover rows shadows/parents/events = %d/%d/%d",
			real.Rows["task_run_snapshot_v2_shadows"],
			real.Rows["task_run_snapshots"],
			real.Rows["task_run_snapshot_v2_cutover_events"])
	}
	if err := f.base.st.pool.QueryRow(t.Context(),
		`SELECT count(*) FROM task_run_snapshot_v2_cutover_events
		  WHERE tenant_id=$1`,
		f.base.tenantID).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if events != 0 {
		t.Fatalf("real purge retained %d cutover events", events)
	}
}

func TestTaskRunSnapshotCutoverErrorDoesNotExposeDatabaseDetail(t *testing.T) {
	f := newTaskRunSnapshotCutoverFixture(t)
	runID := "cutover-safe-error-" + uuid.NewString()
	_, err := f.base.st.createOrGetTaskRunSnapshotWithShadowV2(
		t.Context(), f.base.params(f.taskID, runID), true)
	if err == nil {
		t.Fatal("expected admission error")
	}
	if errors.Is(err, context.Canceled) ||
		fmt.Sprint(err) == "" {
		t.Fatalf("unexpected admission error: %v", err)
	}
}

func TestTaskRunSnapshotCutoverHasNoProductionAdmissionWriter(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve cutover test path")
	}
	root := filepath.Dir(filepath.Dir(file))
	fset := token.NewFileSet()
	var authorityCalls []string
	for _, dir := range []string{"store", "workflow", filepath.Join("cmd", "runtimeadmin")} {
		err := filepath.WalkDir(filepath.Join(root, dir),
			func(path string, entry os.DirEntry, walkErr error) error {
				if walkErr != nil {
					return walkErr
				}
				if entry.IsDir() || filepath.Ext(path) != ".go" ||
					strings.HasSuffix(path, "_test.go") {
					return nil
				}
				raw, err := os.ReadFile(path)
				if err != nil {
					return err
				}
				if bytes.Contains(raw, []byte(
					"INSERT INTO task_run_snapshot_v2_cutover_events")) ||
					bytes.Contains(raw, []byte(
						"SET run_snapshot_cutover_event_id")) {
					t.Errorf("production cutover mutation SQL in %s",
						filepath.ToSlash(path))
				}
				parsed, err := parser.ParseFile(fset, path, raw, 0)
				if err != nil {
					return err
				}
				ast.Inspect(parsed, func(node ast.Node) bool {
					call, ok := node.(*ast.CallExpr)
					if !ok {
						return true
					}
					selector, ok := call.Fun.(*ast.SelectorExpr)
					if !ok ||
						selector.Sel.Name !=
							"createOrGetTaskRunSnapshotWithAuthorityV2" {
						return true
					}
					position := fset.Position(call.Pos())
					authorityCalls = append(authorityCalls,
						fmt.Sprintf("%s:%d", filepath.ToSlash(path), position.Line))
					if len(call.Args) != 4 {
						t.Errorf("authority writer call args = %d in %s",
							len(call.Args), filepath.ToSlash(path))
						return true
					}
					identifier, nilArg := call.Args[3].(*ast.Ident)
					if !nilArg || identifier.Name != "nil" {
						t.Errorf("production authority writer has non-nil marker in %s",
							filepath.ToSlash(path))
					}
					return true
				})
				return nil
			})
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(authorityCalls) != 1 ||
		!strings.Contains(authorityCalls[0], "store/task_run_snapshots.go") {
		t.Fatalf("production authority-writer calls = %v, want one nil wrapper",
			authorityCalls)
	}
}
