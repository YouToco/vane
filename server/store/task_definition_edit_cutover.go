package store

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/types"
)

func validateTaskDefinitionEditCutoverBase(
	op *types.TaskDefinitionEditOperation,
	schedule *taskDefinitionEditScheduleRow,
) error {
	if op == nil || schedule == nil {
		return taskDefinitionEditIntegrity()
	}
	if schedule.CutoverEventID == nil {
		return nil
	}
	if schedule.CutoverAction == nil {
		return taskDefinitionEditIntegrity()
	}
	if *schedule.CutoverAction == TaskRunSnapshotCutoverRollback {
		return nil
	}
	if *schedule.CutoverAction != TaskRunSnapshotCutoverActivate ||
		schedule.CutoverDefinitionVersion == nil ||
		schedule.CutoverDefinitionDigest == nil ||
		*schedule.CutoverDefinitionVersion != op.BaseDefinitionVersion ||
		!constantTimeDigestEqual(
			*schedule.CutoverDefinitionDigest,
			op.BaseDefinitionDigest,
		) {
		return taskDefinitionEditIntegrity()
	}
	return nil
}

func ensureTaskDefinitionEditCutoverTx(
	ctx context.Context,
	tx pgx.Tx,
	op *types.TaskDefinitionEditOperation,
	lease types.TaskDefinitionEditLease,
	schedule *taskDefinitionEditScheduleRow,
) error {
	if op == nil || schedule == nil {
		return taskDefinitionEditIntegrity()
	}
	if schedule.CutoverEventID != nil {
		if schedule.CutoverAction == nil {
			return taskDefinitionEditIntegrity()
		}
		if *schedule.CutoverAction == TaskRunSnapshotCutoverActivate {
			if err := strictAuditTaskRunSnapshotCutoverV2(
				ctx, tx, op.TargetTenantID, op.TargetUserID, op.TaskID,
			); err != nil {
				return err
			}
		}
	}
	if err := validateTaskDefinitionEditCutoverRebaseRole(ctx, tx); err != nil {
		return err
	}
	var (
		rebased                  bool
		rollbackID, activationID *int64
	)
	if err := tx.QueryRow(ctx,
		`SELECT rebased,rollback_event_id,activate_event_id
		   FROM task_run_snapshot_v2_rebase_definition_edit($1,$2,$3)`,
		op.ID, lease.Fence, lease.LeaseOwner,
	).Scan(&rebased, &rollbackID, &activationID); err != nil {
		return taskDefinitionEditDatabaseError(
			"rebase snapshot cutover authority", err)
	}
	if rebased != (rollbackID != nil && activationID != nil) ||
		(rollbackID == nil) != (activationID == nil) {
		return taskDefinitionEditIntegrity()
	}
	return nil
}

func validateTaskDefinitionEditCutoverRebaseRole(
	ctx context.Context,
	tx pgx.Tx,
) error {
	var valid bool
	err := tx.QueryRow(ctx, `
		SELECT
		  current_user = 'vane_edit_coordinator' AND
		  p.prosecdef AND
		  p.proowner = (SELECT oid FROM pg_roles WHERE rolname=session_user) AND
		  p.proconfig =
		    ARRAY['search_path=pg_catalog, public']::TEXT[] AND
		  has_function_privilege(
		    current_user,
		    'public.task_run_snapshot_v2_rebase_definition_edit(text,bigint,text)',
		    'EXECUTE'
		  ) AND
		  NOT has_function_privilege(
		    current_user,
		    'public.task_run_snapshot_v2_cutover_control(bigint,bigint,text,text)',
		    'EXECUTE'
		  ) AND
		  NOT has_function_privilege(
		    'vane_app',
		    'public.task_run_snapshot_v2_rebase_definition_edit(text,bigint,text)',
		    'EXECUTE'
		  ) AND
		  NOT has_function_privilege(
		    'vane_snapshot_cutover_operator',
		    'public.task_run_snapshot_v2_rebase_definition_edit(text,bigint,text)',
		    'EXECUTE'
		  ) AND
		  NOT has_table_privilege(
		    current_user,
		    'task_run_snapshot_v2_cutover_events',
		    'INSERT,UPDATE,DELETE,TRUNCATE'
		  ) AND
		  NOT has_sequence_privilege(
		    current_user,
		    'task_run_snapshot_v2_cutover_events_id_seq',
		    'USAGE,SELECT,UPDATE'
		  ) AND
		  NOT has_column_privilege(
		    current_user, 'schedules',
		    'run_snapshot_cutover_event_id', 'UPDATE'
		  ) AND
		  NOT EXISTS (
		    SELECT 1
		      FROM aclexplode(p.proacl) acl
		     WHERE acl.privilege_type <> 'EXECUTE'
		        OR acl.grantee NOT IN (
		            (SELECT oid FROM pg_roles WHERE rolname=session_user),
		            (SELECT oid FROM pg_roles
		              WHERE rolname='vane_edit_coordinator')
		        )
		        OR (
		            acl.grantee = (
		                SELECT oid FROM pg_roles
		                 WHERE rolname='vane_edit_coordinator'
		            ) AND acl.is_grantable
		        )
		  )
		  FROM pg_proc p
		 WHERE p.oid =
		  'public.task_run_snapshot_v2_rebase_definition_edit(text,bigint,text)'::regprocedure`,
	).Scan(&valid)
	if err != nil || !valid {
		return taskDefinitionEditIntegrity()
	}
	return nil
}
