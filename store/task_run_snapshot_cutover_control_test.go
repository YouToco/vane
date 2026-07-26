package store

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

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
	forceTaskRunSnapshotCutoverActivePinDrift(
		t, f.st, f.tenantID, f.userID, taskID)
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

func TestTaskRunSnapshotCutoverControlRecreatedTaskAuditsRetainedHistory(
	t *testing.T,
) {
	f, taskID, firstRef := newTaskRunSnapshotCutoverControlFixture(t)
	first := callTaskRunSnapshotCutoverControl(
		t, f.st, f.tenantID, f.userID, taskID, "activate")
	if err := f.st.DeleteSchedule(
		t.Context(), taskID, f.userID); err != nil {
		t.Fatalf("delete first task generation: %v", err)
	}

	f.urlPrefix += "/recreated"
	f.createApprovedTask(t, taskID, 1)
	applyTaskDefinitionBaseline(t, f, taskID)
	identity := types.RunIdentity{
		TemporalWorkflowID: scheduledTaskWorkflowID(taskID),
		TemporalRunID:      "cutover-control-recreated-" + uuid.NewString(),
		RunKind:            types.RunSnapshotKindScheduled,
		TenantID:           f.tenantID,
		UserID:             f.userID,
		TaskID:             taskID,
	}
	secondRef, err := f.st.CreateOrGetCompiledRunSnapshotShadowV2(
		t.Context(), identity, testCompiledRunPolicyV1(t))
	if err != nil {
		t.Fatalf("create recreated task canary: %v", err)
	}
	rows, err := f.st.pool.Query(t.Context(),
		`SELECT id,task_run_snapshot_v2_cutover_row_exact(id)
		   FROM task_run_snapshots
		  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3
		  ORDER BY id`,
		f.tenantID, f.userID, taskID)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id int64
		var exact bool
		if err := rows.Scan(&id, &exact); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		if !exact {
			rows.Close()
			t.Fatalf("retained row %d is not DB-exact", id)
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		t.Fatal(err)
	}
	rows.Close()
	second := callTaskRunSnapshotCutoverControl(
		t, f.st, f.tenantID, f.userID, taskID, "activate")
	if second.Generation != first.Generation+1 ||
		second.AuditCount != 2 ||
		second.AuditFrom != firstRef.SnapshotID ||
		second.HighWatermark != secondRef.SnapshotID {
		t.Fatalf("recreated activation did not audit retained history: "+
			"first=%+v second=%+v refs=%d/%d",
			first, second, firstRef.SnapshotID, secondRef.SnapshotID)
	}
}

func TestTaskRunSnapshotCutoverProductionAdmissionLifecycle(t *testing.T) {
	f, taskID, canaryRef := newTaskRunSnapshotCutoverControlFixture(t)
	activation := callTaskRunSnapshotCutoverControl(
		t, f.st, f.tenantID, f.userID, taskID, "activate")

	// Exact historical replay returns before current-pointer admission and
	// retains its original NULL marker/v1 authority.
	replayedCanary, err := f.st.CreateOrGetCompiledTaskRunSnapshotV1(
		t.Context(), CreateOrGetCompiledTaskRunSnapshotV1Params{
			Identity: canaryRef.Identity(),
			Policy:   testCompiledRunPolicyV1(t),
		})
	if err != nil || replayedCanary != canaryRef {
		t.Fatalf("historical replay = %+v, %v; want %+v",
			replayedCanary, err, canaryRef)
	}
	assertProductionCutoverRun(
		t, f.st, canaryRef, nil, CompiledRunSnapshotAuthorityV1)

	activeRef := createProductionCutoverRun(t, f, taskID, "active")
	assertProductionCutoverRun(
		t, f.st, activeRef, &activation.EventID,
		CompiledRunSnapshotAuthorityV2)

	callTaskRunSnapshotCutoverControl(
		t, f.st, f.tenantID, f.userID, taskID, "rollback")
	rollbackRef := createProductionCutoverRun(t, f, taskID, "rollback")
	assertProductionCutoverRun(
		t, f.st, rollbackRef, nil, CompiledRunSnapshotAuthorityV1)

	reactivation := callTaskRunSnapshotCutoverControl(
		t, f.st, f.tenantID, f.userID, taskID, "activate")
	if reactivation.AuditCount != 3 {
		t.Fatalf("reactivation audited %d rows, want 3",
			reactivation.AuditCount)
	}
	reactivatedRef := createProductionCutoverRun(
		t, f, taskID, "reactivated")
	assertProductionCutoverRun(
		t, f.st, reactivatedRef, &reactivation.EventID,
		CompiledRunSnapshotAuthorityV2)
}

func TestTaskRunSnapshotCutoverConcurrentControlAndAdmissionConverge(
	t *testing.T,
) {
	f, taskID, _ := newTaskRunSnapshotCutoverControlFixture(t)
	identity := types.RunIdentity{
		TemporalWorkflowID: scheduledTaskWorkflowID(taskID),
		TemporalRunID:      "cutover-control-race-" + uuid.NewString(),
		RunKind:            types.RunSnapshotKindScheduled,
		TenantID:           f.tenantID,
		UserID:             f.userID,
		TaskID:             taskID,
	}
	start := make(chan struct{})
	policy := testCompiledRunPolicyV1(t)
	var result TaskRunSnapshotCutoverResult
	var ref types.RunSnapshotRef
	var controlErr, snapshotErr error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		result, controlErr = f.st.ControlTaskRunSnapshotCutover(
			t.Context(), f.tenantID, f.userID, taskID,
			TaskRunSnapshotCutoverActivate)
	}()
	go func() {
		defer wg.Done()
		<-start
		ref, snapshotErr = f.st.CreateOrGetCompiledRunSnapshotShadowV2(
			t.Context(), identity, policy)
	}()
	close(start)
	waitCutoverRace(t, &wg)
	if controlErr != nil || snapshotErr != nil {
		t.Fatalf("control/admission race errors = %v/%v",
			controlErr, snapshotErr)
	}
	var marker *int64
	if err := f.st.pool.QueryRow(t.Context(),
		`SELECT v2_cutover_event_id FROM task_run_snapshots WHERE id=$1`,
		ref.SnapshotID).Scan(&marker); err != nil {
		t.Fatal(err)
	}
	wantAuthority := CompiledRunSnapshotAuthorityV1
	if marker != nil {
		if *marker != result.EventID {
			t.Fatalf("racing marker = %d, want event %d",
				*marker, result.EventID)
		}
		wantAuthority = CompiledRunSnapshotAuthorityV2
	} else if result.SnapshotHighWatermark < ref.SnapshotID ||
		result.AuditCount != 2 {
		t.Fatalf("admission won but activation carrier missed run %d: %+v",
			ref.SnapshotID, result)
	}
	_, authority, err := f.st.LoadAuthoritativeCompiledTaskRunSnapshot(
		t.Context(), identity, ref)
	if err != nil || authority != wantAuthority {
		t.Fatalf("racing authority = %q, %v; want %q",
			authority, err, wantAuthority)
	}
}

func TestTaskRunSnapshotCutoverControlAndAdmissionDeterministicOrdering(
	t *testing.T,
) {
	t.Run("admission commits first", func(t *testing.T) {
		f, taskID, oldRef := newTaskRunSnapshotCutoverControlFixture(t)
		identity := types.RunIdentity{
			TemporalWorkflowID: scheduledTaskWorkflowID(taskID),
			TemporalRunID:      "cutover-admission-first-" + uuid.NewString(),
			RunKind:            types.RunSnapshotKindScheduled,
			TenantID:           f.tenantID,
			UserID:             f.userID,
			TaskID:             taskID,
		}
		policy := testCompiledRunPolicyV1(t)
		admissionDone := make(chan struct{})
		var result TaskRunSnapshotCutoverResult
		var ref types.RunSnapshotRef
		var controlErr, snapshotErr error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			ref, snapshotErr = f.st.CreateOrGetCompiledRunSnapshotShadowV2(
				t.Context(), identity, policy)
			close(admissionDone)
		}()
		go func() {
			defer wg.Done()
			<-admissionDone
			if snapshotErr != nil {
				return
			}
			result, controlErr = f.st.ControlTaskRunSnapshotCutover(
				t.Context(), f.tenantID, f.userID, taskID,
				TaskRunSnapshotCutoverActivate)
		}()
		waitCutoverRace(t, &wg)
		if snapshotErr != nil || controlErr != nil {
			t.Fatalf("admission-first errors = %v/%v",
				snapshotErr, controlErr)
		}
		if result.SnapshotHighWatermark != ref.SnapshotID ||
			result.AuditThroughID != ref.SnapshotID ||
			result.AuditFromSnapshotID != oldRef.SnapshotID ||
			result.AuditCount != 2 {
			t.Fatalf("admission-first frozen carrier = %+v, old/new=%d/%d",
				result, oldRef.SnapshotID, ref.SnapshotID)
		}
		assertProductionCutoverRun(
			t, f.st, ref, nil, CompiledRunSnapshotAuthorityV1)
	})

	t.Run("control commits first", func(t *testing.T) {
		f, taskID, oldRef := newTaskRunSnapshotCutoverControlFixture(t)
		identity := types.RunIdentity{
			TemporalWorkflowID: scheduledTaskWorkflowID(taskID),
			TemporalRunID:      "cutover-control-first-" + uuid.NewString(),
			RunKind:            types.RunSnapshotKindScheduled,
			TenantID:           f.tenantID,
			UserID:             f.userID,
			TaskID:             taskID,
		}
		policy := testCompiledRunPolicyV1(t)
		controlDone := make(chan struct{})
		var result TaskRunSnapshotCutoverResult
		var ref types.RunSnapshotRef
		var controlErr, snapshotErr error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			result, controlErr = f.st.ControlTaskRunSnapshotCutover(
				t.Context(), f.tenantID, f.userID, taskID,
				TaskRunSnapshotCutoverActivate)
			close(controlDone)
		}()
		go func() {
			defer wg.Done()
			<-controlDone
			if controlErr != nil {
				return
			}
			ref, snapshotErr = f.st.CreateOrGetCompiledTaskRunSnapshotV1(
				t.Context(), CreateOrGetCompiledTaskRunSnapshotV1Params{
					Identity: identity, Policy: policy,
				})
		}()
		waitCutoverRace(t, &wg)
		if controlErr != nil || snapshotErr != nil {
			t.Fatalf("control-first errors = %v/%v",
				controlErr, snapshotErr)
		}
		if result.SnapshotHighWatermark != oldRef.SnapshotID ||
			ref.SnapshotID <= result.SnapshotHighWatermark {
			t.Fatalf("control-first carrier/new run = %+v/%d",
				result, ref.SnapshotID)
		}
		assertProductionCutoverRun(
			t, f.st, ref, &result.EventID,
			CompiledRunSnapshotAuthorityV2)
	})
}

func TestTaskRunSnapshotCutoverConcurrentDesiredStateIsExactReplay(
	t *testing.T,
) {
	f, taskID, _ := newTaskRunSnapshotCutoverControlFixture(t)
	start := make(chan struct{})
	results := make([]TaskRunSnapshotCutoverResult, 2)
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i := range 2 {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			results[index], errs[index] =
				f.st.ControlTaskRunSnapshotCutover(
					t.Context(), f.tenantID, f.userID, taskID,
					TaskRunSnapshotCutoverActivate)
		}(i)
	}
	close(start)
	waitCutoverRace(t, &wg)
	if errs[0] != nil || errs[1] != nil || results[0] != results[1] {
		t.Fatalf("concurrent activate results/errors = %+v/%+v %v/%v",
			results[0], results[1], errs[0], errs[1])
	}
	assertCutoverEventCount(t, f, taskID, 1)
}

func TestTaskRunSnapshotCutoverControlCASFailureLeavesNoOrphan(
	t *testing.T,
) {
	f, taskID, _ := newTaskRunSnapshotCutoverControlFixture(t)
	if _, err := f.st.pool.Exec(t.Context(), `
		CREATE FUNCTION zzz_test_cancel_cutover_pointer()
		RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
		    RETURN NULL;
		END
		$$;
		CREATE TRIGGER zzz_test_cancel_cutover_pointer
		BEFORE UPDATE OF run_snapshot_cutover_event_id ON schedules
		FOR EACH ROW EXECUTE FUNCTION zzz_test_cancel_cutover_pointer()`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = f.st.pool.Exec(context.Background(), `
			DROP TRIGGER IF EXISTS zzz_test_cancel_cutover_pointer ON schedules;
			DROP FUNCTION IF EXISTS zzz_test_cancel_cutover_pointer()`)
	})
	_, err := f.st.ControlTaskRunSnapshotCutover(
		t.Context(), f.tenantID, f.userID, taskID,
		TaskRunSnapshotCutoverActivate)
	assertAppCode(t, err, types.CodeDatabase)
	if !strings.Contains(err.Error(), "sqlstate 40001") {
		t.Fatalf("CAS failure did not retain safe SQLSTATE: %v", err)
	}
	assertCutoverEventCount(t, f, taskID, 0)
	var pointer *int64
	if err := f.st.pool.QueryRow(t.Context(),
		`SELECT run_snapshot_cutover_event_id FROM schedules
		  WHERE tenant_id=$1 AND user_id=$2 AND id=$3`,
		f.tenantID, f.userID, taskID).Scan(&pointer); err != nil {
		t.Fatal(err)
	}
	if pointer != nil {
		t.Fatalf("failed CAS left pointer %v", pointer)
	}
}

func TestTaskRunSnapshotCutoverControlFailsClosedOnOperatorRoleDrift(
	t *testing.T,
) {
	tests := []struct {
		name, mutate, restore string
	}{
		{
			name: "attributes",
			mutate: `ALTER ROLE vane_snapshot_cutover_operator
				LOGIN SUPERUSER CREATEDB CREATEROLE REPLICATION
				INHERIT BYPASSRLS`,
			restore: `ALTER ROLE vane_snapshot_cutover_operator
				NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
				NOINHERIT NOBYPASSRLS`,
		},
		{
			name: "raw table grant",
			mutate: `GRANT SELECT ON schedules
				TO vane_snapshot_cutover_operator`,
			restore: `REVOKE SELECT ON schedules
				FROM vane_snapshot_cutover_operator`,
		},
		{
			name: "raw sequence grant",
			mutate: `GRANT USAGE ON task_run_snapshot_v2_cutover_events_id_seq
				TO vane_snapshot_cutover_operator`,
			restore: `REVOKE USAGE ON task_run_snapshot_v2_cutover_events_id_seq
				FROM vane_snapshot_cutover_operator`,
		},
		{
			name: "helper execute",
			mutate: `GRANT EXECUTE ON FUNCTION
				task_run_snapshot_v2_cutover_row_exact(BIGINT)
				TO vane_snapshot_cutover_operator`,
			restore: `REVOKE EXECUTE ON FUNCTION
				task_run_snapshot_v2_cutover_row_exact(BIGINT)
				FROM vane_snapshot_cutover_operator`,
		},
		{
			name: "control function invoker drift",
			mutate: `ALTER FUNCTION
				task_run_snapshot_v2_cutover_control(BIGINT,BIGINT,TEXT,TEXT)
				SECURITY INVOKER`,
			restore: `ALTER FUNCTION
				task_run_snapshot_v2_cutover_control(BIGINT,BIGINT,TEXT,TEXT)
				SECURITY DEFINER`,
		},
		{
			name: "control search path drift",
			mutate: `ALTER FUNCTION
				task_run_snapshot_v2_cutover_control(BIGINT,BIGINT,TEXT,TEXT)
				RESET search_path`,
			restore: `ALTER FUNCTION
				task_run_snapshot_v2_cutover_control(BIGINT,BIGINT,TEXT,TEXT)
				SET search_path = pg_catalog, public`,
		},
		{
			name: "runtime app function grant",
			mutate: `GRANT EXECUTE ON FUNCTION
				task_run_snapshot_v2_cutover_control(BIGINT,BIGINT,TEXT,TEXT)
				TO vane_app`,
			restore: `REVOKE EXECUTE ON FUNCTION
				task_run_snapshot_v2_cutover_control(BIGINT,BIGINT,TEXT,TEXT)
				FROM vane_app`,
		},
		{
			name:    "runtime app enters operator",
			mutate:  `GRANT vane_snapshot_cutover_operator TO vane_app`,
			restore: `REVOKE vane_snapshot_cutover_operator FROM vane_app`,
		},
		{
			name:    "operator enters runtime app",
			mutate:  `GRANT vane_app TO vane_snapshot_cutover_operator`,
			restore: `REVOKE vane_app FROM vane_snapshot_cutover_operator`,
		},
		{
			name: "control execute revoked",
			mutate: `REVOKE EXECUTE ON FUNCTION
				task_run_snapshot_v2_cutover_control(BIGINT,BIGINT,TEXT,TEXT)
				FROM vane_snapshot_cutover_operator`,
			restore: `GRANT EXECUTE ON FUNCTION
				task_run_snapshot_v2_cutover_control(BIGINT,BIGINT,TEXT,TEXT)
				TO vane_snapshot_cutover_operator`,
		},
		{
			name:    "owner cannot set",
			mutate:  `REVOKE vane_snapshot_cutover_operator FROM CURRENT_USER`,
			restore: `GRANT vane_snapshot_cutover_operator TO CURRENT_USER`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f, taskID, _ := newTaskRunSnapshotCutoverControlFixture(t)
			if _, err := f.st.pool.Exec(t.Context(), test.mutate); err != nil {
				t.Fatalf("mutate role boundary: %v", err)
			}
			restored := false
			t.Cleanup(func() {
				if !restored {
					_, _ = f.st.pool.Exec(context.Background(), test.restore)
				}
			})
			_, err := f.st.ControlTaskRunSnapshotCutover(
				t.Context(), f.tenantID, f.userID, taskID,
				TaskRunSnapshotCutoverActivate)
			assertAppCode(t, err, types.CodeInternal)
			if _, restoreErr := f.st.pool.Exec(
				t.Context(), test.restore); restoreErr != nil {
				t.Fatalf("restore role boundary: %v", restoreErr)
			}
			restored = true
			assertCutoverEventCount(t, f, taskID, 0)
			var pointer *int64
			if err := f.st.pool.QueryRow(t.Context(),
				`SELECT run_snapshot_cutover_event_id
				   FROM schedules
				  WHERE tenant_id=$1 AND user_id=$2 AND id=$3`,
				f.tenantID, f.userID, taskID,
			).Scan(&pointer); err != nil {
				t.Fatal(err)
			}
			if pointer != nil {
				t.Fatalf("role drift left cutover pointer %v", pointer)
			}
		})
	}
}

func TestTaskRunSnapshotCutoverControlRacesPurgeWithoutDeadlock(
	t *testing.T,
) {
	f, taskID, _ := newTaskRunSnapshotCutoverControlFixture(t)
	start := make(chan struct{})
	var controlErr, purgeErr error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		_, controlErr = f.st.ControlTaskRunSnapshotCutover(
			t.Context(), f.tenantID, f.userID, taskID,
			TaskRunSnapshotCutoverActivate)
	}()
	go func() {
		defer wg.Done()
		<-start
		_, purgeErr = f.st.PurgeTenant(
			t.Context(), f.tenantID, false)
	}()
	close(start)
	waitCutoverRace(t, &wg)
	if purgeErr != nil {
		t.Fatalf("purge race failed: control=%v purge=%v",
			controlErr, purgeErr)
	}
	var tenants, events int
	if err := f.st.pool.QueryRow(t.Context(), `
		SELECT
		  (SELECT count(*) FROM tenants WHERE id=$1),
		  (SELECT count(*) FROM task_run_snapshot_v2_cutover_events
		    WHERE tenant_id=$1)`,
		f.tenantID).Scan(&tenants, &events); err != nil {
		t.Fatal(err)
	}
	if tenants != 0 || events != 0 {
		t.Fatalf("purge race retained tenant/events = %d/%d",
			tenants, events)
	}
}

func TestTaskRunSnapshotCutoverControlRacesDeleteWithoutDeadlock(
	t *testing.T,
) {
	f, taskID, _ := newTaskRunSnapshotCutoverControlFixture(t)
	start := make(chan struct{})
	var controlErr, deleteErr error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		_, controlErr = f.st.ControlTaskRunSnapshotCutover(
			t.Context(), f.tenantID, f.userID, taskID,
			TaskRunSnapshotCutoverActivate)
	}()
	go func() {
		defer wg.Done()
		<-start
		deleteErr = f.st.DeleteSchedule(
			t.Context(), taskID, f.userID)
	}()
	close(start)
	waitCutoverRace(t, &wg)
	if deleteErr != nil {
		t.Fatalf("delete race failed: control=%v delete=%v",
			controlErr, deleteErr)
	}
	var schedules, pointed int
	if err := f.st.pool.QueryRow(t.Context(), `
		SELECT
		  (SELECT count(*) FROM schedules
		    WHERE tenant_id=$1 AND user_id=$2 AND id=$3),
		  (SELECT count(*) FROM schedules
		    WHERE tenant_id=$1 AND user_id=$2 AND id=$3
		      AND run_snapshot_cutover_event_id IS NOT NULL)`,
		f.tenantID, f.userID, taskID).Scan(&schedules, &pointed); err != nil {
		t.Fatal(err)
	}
	if schedules != 0 || pointed != 0 {
		t.Fatalf("delete race retained schedule/pointer = %d/%d",
			schedules, pointed)
	}
	var completeEvents int
	if err := f.st.pool.QueryRow(t.Context(), `
		SELECT count(*)
		  FROM task_run_snapshot_v2_cutover_events
		 WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3
		   AND action='activate'
		   AND generation=1
		   AND approved_definition_version > 0
		   AND approved_definition_digest ~ '^[0-9a-f]{64}$'
		   AND snapshot_high_watermark > 0
		   AND audit_from_snapshot_id > 0
		   AND audit_count > 0
		   AND audit_through_id = snapshot_high_watermark`,
		f.tenantID, f.userID, taskID,
	).Scan(&completeEvents); err != nil {
		t.Fatal(err)
	}
	switch {
	case controlErr == nil && completeEvents != 1:
		t.Fatalf("control won delete race but complete events = %d, want 1",
			completeEvents)
	case errors.Is(controlErr, types.ErrNotFound) && completeEvents != 0:
		t.Fatalf("delete won race but events = %d, want 0", completeEvents)
	case controlErr != nil && !errors.Is(controlErr, types.ErrNotFound):
		t.Fatalf("delete race control failed unexpectedly: %v", controlErr)
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

func createProductionCutoverRun(
	t *testing.T,
	f *taskRunSnapshotFixture,
	taskID, suffix string,
) types.RunSnapshotRef {
	t.Helper()
	identity := types.RunIdentity{
		TemporalWorkflowID: scheduledTaskWorkflowID(taskID),
		TemporalRunID:      "cutover-production-" + suffix + "-" + uuid.NewString(),
		RunKind:            types.RunSnapshotKindScheduled,
		TenantID:           f.tenantID,
		UserID:             f.userID,
		TaskID:             taskID,
	}
	ref, err := f.st.CreateOrGetCompiledTaskRunSnapshotV1(
		t.Context(), CreateOrGetCompiledTaskRunSnapshotV1Params{
			Identity: identity, Policy: testCompiledRunPolicyV1(t),
		})
	if err != nil {
		t.Fatalf("create production %s run: %v", suffix, err)
	}
	return ref
}

func assertProductionCutoverRun(
	t *testing.T,
	st *Store,
	ref types.RunSnapshotRef,
	wantMarker *int64,
	wantAuthority CompiledRunSnapshotAuthority,
) {
	t.Helper()
	var marker *int64
	var shadows int
	if err := st.pool.QueryRow(t.Context(), `
		SELECT p.v2_cutover_event_id,
		       (SELECT count(*) FROM task_run_snapshot_v2_shadows sh
		         WHERE sh.run_snapshot_id=p.id)
		  FROM task_run_snapshots p
		 WHERE p.id=$1`,
		ref.SnapshotID).Scan(&marker, &shadows); err != nil {
		t.Fatal(err)
	}
	if (marker == nil) != (wantMarker == nil) ||
		(marker != nil && *marker != *wantMarker) ||
		shadows != 1 {
		t.Fatalf("run %d marker/shadows = %v/%d, want %v/1",
			ref.SnapshotID, marker, shadows, wantMarker)
	}
	_, authority, err := st.LoadAuthoritativeCompiledTaskRunSnapshot(
		t.Context(), ref.Identity(), ref)
	if err != nil {
		t.Fatalf("load run %d authority: %v", ref.SnapshotID, err)
	}
	if authority != wantAuthority {
		t.Fatalf("run %d authority = %q, want %q",
			ref.SnapshotID, authority, wantAuthority)
	}
}

func waitCutoverRace(t *testing.T, wg *sync.WaitGroup) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("snapshot cutover race deadlocked")
	}
}
