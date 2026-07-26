package store

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/types"
)

type TaskRunSnapshotCutoverAction string

const (
	TaskRunSnapshotCutoverActivate TaskRunSnapshotCutoverAction = "activate"
	TaskRunSnapshotCutoverRollback TaskRunSnapshotCutoverAction = "rollback"
)

func (a TaskRunSnapshotCutoverAction) valid() bool {
	return a == TaskRunSnapshotCutoverActivate ||
		a == TaskRunSnapshotCutoverRollback
}

type TaskRunSnapshotCutoverResult struct {
	TenantID                  int64                        `json:"tenant_id"`
	UserID                    int64                        `json:"user_id"`
	TaskID                    string                       `json:"task_id"`
	EventID                   int64                        `json:"event_id"`
	Generation                int64                        `json:"generation"`
	Action                    TaskRunSnapshotCutoverAction `json:"action"`
	ApprovedDefinitionVersion int64                        `json:"approved_definition_version"`
	SnapshotHighWatermark     int64                        `json:"snapshot_high_watermark"`
	AuditFromSnapshotID       int64                        `json:"audit_from_snapshot_id"`
	AuditCount                int64                        `json:"audit_count"`
	AuditThroughID            int64                        `json:"audit_through_id"`
}

type TaskRunSnapshotCutoverStatus struct {
	TenantID int64                         `json:"tenant_id"`
	UserID   int64                         `json:"user_id"`
	TaskID   string                        `json:"task_id"`
	State    string                        `json:"state"`
	Current  *TaskRunSnapshotCutoverResult `json:"current,omitempty"`
}

// ControlTaskRunSnapshotCutover is the Store half of the operator boundary.
// It takes the schedule lock before reading any event/snapshot/definition,
// runs the frozen Go typed materializer over the complete retained population
// for a new activation, then enters the NOINHERIT operator role solely to call
// the database-derived definer primitive in the same transaction.
func (s *Store) ControlTaskRunSnapshotCutover(
	ctx context.Context,
	tenantID, userID int64,
	taskID string,
	action TaskRunSnapshotCutoverAction,
) (TaskRunSnapshotCutoverResult, error) {
	if tenantID <= 0 || userID <= 0 || !validTaskRunTaskID(taskID) ||
		!action.valid() {
		return TaskRunSnapshotCutoverResult{},
			taskRunValidationError("snapshot cutover request is invalid")
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.ReadCommitted,
	})
	if err != nil {
		return TaskRunSnapshotCutoverResult{},
			taskRunDatabaseError("begin snapshot cutover control", err)
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	if err := setTaskRunTenantContext(ctx, tx, tenantID); err != nil {
		return TaskRunSnapshotCutoverResult{}, err
	}

	var (
		pointer       *int64
		editOperation *string
		editFence     *int64
	)
	if err := tx.QueryRow(ctx,
		`SELECT run_snapshot_cutover_event_id,
		        definition_edit_operation_id,definition_edit_fence
		   FROM schedules
		  WHERE tenant_id=$1 AND user_id=$2 AND id=$3
		  FOR UPDATE`,
		tenantID, userID, taskID,
	).Scan(&pointer, &editOperation, &editFence); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return TaskRunSnapshotCutoverResult{}, taskRunNotFound()
		}
		return TaskRunSnapshotCutoverResult{},
			taskRunDatabaseError("lock snapshot cutover schedule", err)
	}
	if (editOperation == nil) != (editFence == nil) {
		return TaskRunSnapshotCutoverResult{}, taskRunIntegrityError()
	}
	if editOperation != nil {
		return TaskRunSnapshotCutoverResult{}, types.NewAppError(
			types.CodeConflict,
			"task run snapshot cutover conflicts with a definition edit",
			nil,
		)
	}

	var currentAction *string
	if pointer != nil {
		var loadedAction string
		if err := tx.QueryRow(ctx,
			`SELECT action
			   FROM task_run_snapshot_v2_cutover_events
			  WHERE id=$1 AND tenant_id=$2 AND user_id=$3 AND task_id=$4`,
			*pointer, tenantID, userID, taskID,
		).Scan(&loadedAction); err != nil {
			return TaskRunSnapshotCutoverResult{}, taskRunIntegrityError()
		}
		currentAction = &loadedAction
	}

	if err := validateTaskRunSnapshotCutoverOperator(ctx, tx); err != nil {
		return TaskRunSnapshotCutoverResult{}, err
	}

	needsActivationAudit := action == TaskRunSnapshotCutoverActivate &&
		(currentAction == nil || *currentAction == string(TaskRunSnapshotCutoverRollback))
	if needsActivationAudit {
		if err := strictAuditTaskRunSnapshotCutoverV2(
			ctx, tx, tenantID, userID, taskID); err != nil {
			return TaskRunSnapshotCutoverResult{}, err
		}
	}

	if _, err := tx.Exec(ctx,
		`SET LOCAL ROLE vane_snapshot_cutover_operator`); err != nil {
		return TaskRunSnapshotCutoverResult{},
			taskRunDatabaseError("enter snapshot cutover operator", err)
	}
	result := TaskRunSnapshotCutoverResult{
		TenantID: tenantID, UserID: userID, TaskID: taskID,
	}
	var rawAction string
	var definitionDigest string
	if err := tx.QueryRow(ctx,
		`SELECT event_id,generation,action,
		        approved_definition_version,approved_definition_digest,
		        snapshot_high_watermark,audit_from_snapshot_id,
		        audit_count,audit_through_id
		   FROM task_run_snapshot_v2_cutover_control($1,$2,$3,$4)`,
		tenantID, userID, taskID, string(action),
	).Scan(
		&result.EventID, &result.Generation, &rawAction,
		&result.ApprovedDefinitionVersion, &definitionDigest,
		&result.SnapshotHighWatermark, &result.AuditFromSnapshotID,
		&result.AuditCount, &result.AuditThroughID,
	); err != nil {
		return TaskRunSnapshotCutoverResult{},
			taskRunDatabaseError("apply snapshot cutover control", err)
	}
	result.Action = TaskRunSnapshotCutoverAction(rawAction)
	if !result.Action.valid() || result.EventID <= 0 ||
		result.Generation <= 0 || result.ApprovedDefinitionVersion <= 0 ||
		!validTaskStateDigest(definitionDigest) ||
		result.SnapshotHighWatermark <= 0 ||
		result.AuditFromSnapshotID <= 0 || result.AuditCount <= 0 ||
		result.AuditThroughID != result.SnapshotHighWatermark {
		return TaskRunSnapshotCutoverResult{}, taskRunIntegrityError()
	}
	if err := tx.Commit(ctx); err != nil {
		return TaskRunSnapshotCutoverResult{},
			taskRunDatabaseError("commit snapshot cutover control", err)
	}
	return result, nil
}

// validateTaskRunSnapshotCutoverOperator re-proves the complete database role
// boundary on every mutation while the exact schedule is locked. Migration
// time checks are not sufficient because cluster roles and ACLs can drift.
func validateTaskRunSnapshotCutoverOperator(
	ctx context.Context,
	tx pgx.Tx,
) error {
	var valid bool
	err := tx.QueryRow(ctx, `
		SELECT
		  NOT op.rolsuper AND NOT op.rolcreatedb AND NOT op.rolcreaterole AND
		  NOT op.rolcanlogin AND NOT op.rolinherit AND NOT op.rolreplication AND
		  NOT op.rolbypassrls AND
		  control.prosecdef AND helper.prosecdef AND
		  control.proowner = owner_role.oid AND
		  helper.proowner = owner_role.oid AND
		  control.proconfig =
		    ARRAY['search_path=pg_catalog, public']::TEXT[] AND
		  helper.proconfig =
		    ARRAY['search_path=pg_catalog, public']::TEXT[] AND
		  pg_has_role(CURRENT_USER, op.oid, 'SET') AND
		  EXISTS (
		    SELECT 1
		      FROM pg_auth_members am
		     WHERE am.roleid = op.oid
		       AND am.member = owner_role.oid
		  ) AND
		  NOT pg_has_role('vane_app', op.oid, 'MEMBER') AND
		  NOT pg_has_role(op.oid, 'vane_app', 'MEMBER') AND
		  NOT EXISTS (
		    SELECT 1
		      FROM pg_auth_members am
		     WHERE am.roleid = op.oid
		       AND am.member <> (SELECT oid FROM pg_roles
		                           WHERE rolname = CURRENT_USER)
		  ) AND
		  NOT EXISTS (
		    SELECT 1 FROM pg_auth_members am WHERE am.member = op.oid
		  ) AND
		  NOT EXISTS (
		    SELECT 1
		      FROM pg_class c
		      JOIN pg_namespace n ON n.oid = c.relnamespace
		     WHERE n.nspname = 'public'
		       AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
		       AND has_table_privilege(
		             op.oid, c.oid,
		             'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'
		           )
		  ) AND
		  NOT EXISTS (
		    SELECT 1
		      FROM pg_class c
		      JOIN pg_namespace n ON n.oid = c.relnamespace
		     WHERE n.nspname = 'public'
		       AND CASE
		             WHEN c.relkind = 'S' THEN has_sequence_privilege(
		               op.oid, c.oid, 'USAGE,SELECT,UPDATE'
		             )
		             ELSE FALSE
		           END
		  ) AND
		  has_function_privilege(
		    op.oid,
		    'public.task_run_snapshot_v2_cutover_control(bigint,bigint,text,text)',
		    'EXECUTE'
		  ) AND
		  NOT has_function_privilege(
		    'vane_app',
		    'public.task_run_snapshot_v2_cutover_control(bigint,bigint,text,text)',
		    'EXECUTE'
		  ) AND
		  NOT has_function_privilege(
		    'vane_app',
		    'public.task_run_snapshot_v2_cutover_row_exact(bigint)',
		    'EXECUTE'
		  ) AND
		  NOT has_function_privilege(
		    op.oid,
		    'public.task_run_snapshot_v2_cutover_row_exact(bigint)',
		    'EXECUTE'
		  ) AND
		  NOT EXISTS (
		    SELECT 1
		      FROM aclexplode(control.proacl) acl
		     WHERE acl.privilege_type <> 'EXECUTE'
		        OR acl.grantee NOT IN (owner_role.oid, op.oid)
		        OR (acl.grantee = op.oid AND acl.is_grantable)
		  ) AND
		  NOT EXISTS (
		    SELECT 1
		      FROM aclexplode(helper.proacl) acl
		     WHERE acl.privilege_type <> 'EXECUTE'
		        OR acl.grantee <> owner_role.oid
		  ) AND
		  NOT EXISTS (
		    SELECT 1
		      FROM pg_proc p
		      JOIN pg_namespace n ON n.oid = p.pronamespace
		     WHERE n.nspname = 'public'
		       AND p.prosecdef
		       AND p.oid <>
		           'public.task_run_snapshot_v2_cutover_control(bigint,bigint,text,text)'::regprocedure
		       AND has_function_privilege(op.oid, p.oid, 'EXECUTE')
		  )
		  FROM pg_roles op
		  JOIN pg_roles owner_role ON owner_role.rolname = CURRENT_USER
		  JOIN pg_proc control ON control.oid =
		    'public.task_run_snapshot_v2_cutover_control(bigint,bigint,text,text)'::regprocedure
		  JOIN pg_proc helper ON helper.oid =
		    'public.task_run_snapshot_v2_cutover_row_exact(bigint)'::regprocedure
		 WHERE op.rolname = 'vane_snapshot_cutover_operator'`,
	).Scan(&valid)
	if err != nil || !valid {
		return taskRunIntegrityError()
	}
	return nil
}

func strictAuditTaskRunSnapshotCutoverV2(
	ctx context.Context,
	tx pgx.Tx,
	tenantID, userID int64,
	taskID string,
) error {
	rows, err := tx.Query(ctx,
		`SELECT id,temporal_workflow_id,temporal_run_id
		   FROM task_run_snapshots
		  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3
		  ORDER BY id`,
		tenantID, userID, taskID)
	if err != nil {
		return taskRunDatabaseError(
			"freeze snapshot cutover typed audit", err)
	}
	type scope struct {
		id                int64
		workflowID, runID string
	}
	var scopes []scope
	for rows.Next() {
		var item scope
		if err := rows.Scan(
			&item.id, &item.workflowID, &item.runID); err != nil {
			rows.Close()
			return taskRunDatabaseError(
				"scan snapshot cutover typed audit", err)
		}
		scopes = append(scopes, item)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return taskRunDatabaseError(
			"iterate snapshot cutover typed audit", err)
	}
	if len(scopes) == 0 {
		return taskRunIntegrityError()
	}
	for _, item := range scopes {
		lookup := CreateOrGetTaskRunSnapshotParams{
			TenantID: tenantID, UserID: userID, TaskID: taskID,
			TemporalWorkflowID: item.workflowID,
			TemporalRunID:      item.runID,
		}
		parent, found, err := loadTaskRunSnapshot(ctx, tx, lookup)
		if err != nil || !found || parent.ID != item.id {
			if err != nil {
				return err
			}
			return taskRunIntegrityError()
		}
		ref, err := parent.safeRef()
		if err != nil {
			return taskRunIntegrityError()
		}
		expected := types.RunIdentity{
			TemporalWorkflowID: parent.TemporalWorkflowID,
			TemporalRunID:      parent.TemporalRunID,
			RunKind:            types.RunSnapshotKindScheduled,
			TenantID:           tenantID,
			UserID:             userID,
			TaskID:             taskID,
		}
		_, audit, err := auditCompiledTaskRunSnapshotV2(
			ctx, tx, expected, ref)
		if err != nil ||
			audit.Status != CompiledRunSnapshotV2AuditMatch ||
			audit.ShadowStatus != TaskRunSnapshotShadowMatch ||
			!audit.TypedEqual {
			if err != nil {
				return err
			}
			return taskRunIntegrityError()
		}
	}
	return nil
}

func (s *Store) GetTaskRunSnapshotCutoverStatus(
	ctx context.Context,
	tenantID, userID int64,
	taskID string,
) (TaskRunSnapshotCutoverStatus, error) {
	if tenantID <= 0 || userID <= 0 || !validTaskRunTaskID(taskID) {
		return TaskRunSnapshotCutoverStatus{},
			taskRunValidationError("snapshot cutover status request is invalid")
	}
	status := TaskRunSnapshotCutoverStatus{
		TenantID: tenantID, UserID: userID, TaskID: taskID, State: "inactive",
	}
	var pointer *int64
	if err := s.pool.QueryRow(ctx,
		`SELECT run_snapshot_cutover_event_id
		   FROM schedules
		  WHERE tenant_id=$1 AND user_id=$2 AND id=$3`,
		tenantID, userID, taskID).Scan(&pointer); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return TaskRunSnapshotCutoverStatus{}, taskRunNotFound()
		}
		return TaskRunSnapshotCutoverStatus{},
			taskRunDatabaseError("load snapshot cutover status", err)
	}
	if pointer == nil {
		return status, nil
	}
	current := &TaskRunSnapshotCutoverResult{
		TenantID: tenantID, UserID: userID, TaskID: taskID,
	}
	var rawAction, definitionDigest string
	if err := s.pool.QueryRow(ctx,
		`SELECT id,generation,action,
		        approved_definition_version,approved_definition_digest,
		        snapshot_high_watermark,audit_from_snapshot_id,
		        audit_count,audit_through_id
		   FROM task_run_snapshot_v2_cutover_events
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3 AND task_id=$4`,
		*pointer, tenantID, userID, taskID,
	).Scan(
		&current.EventID, &current.Generation, &rawAction,
		&current.ApprovedDefinitionVersion, &definitionDigest,
		&current.SnapshotHighWatermark, &current.AuditFromSnapshotID,
		&current.AuditCount, &current.AuditThroughID,
	); err != nil {
		return TaskRunSnapshotCutoverStatus{}, taskRunIntegrityError()
	}
	current.Action = TaskRunSnapshotCutoverAction(rawAction)
	if !current.Action.valid() || !validTaskStateDigest(definitionDigest) {
		return TaskRunSnapshotCutoverStatus{}, taskRunIntegrityError()
	}
	status.State = rawAction
	status.Current = current
	return status, nil
}
