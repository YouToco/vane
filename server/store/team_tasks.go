package store

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/server/types"
)

// TaskMutation is an auditable product action. Authorization is always read
// from the current membership row; a role cached in a session is never trusted.
type TaskMutation string

const (
	TaskMutationRun    TaskMutation = "run"
	TaskMutationPause  TaskMutation = "pause"
	TaskMutationResume TaskMutation = "resume"
	TaskMutationEdit   TaskMutation = "edit"
	TaskMutationDelete TaskMutation = "delete"
)

const teamScheduleColumns = `
    s.id,s.tenant_id,s.user_id,a.assignee_user_id,a.creator_user_id,
    a.task_visibility,s.nl_description,s.spec_json,s.scope_json,s.status,
    s.execution_mode,s.created_at,GREATEST(s.updated_at,a.updated_at)`

func scanTeamSchedule(row pgx.Row, out *types.Schedule) error {
	var rawMode string
	if err := row.Scan(
		&out.ID, &out.TenantID, &out.UserID, &out.AssigneeUserID,
		&out.CreatorUserID, &out.Visibility, &out.NLDescription,
		&out.SpecJSON, &out.ScopeJSON, &out.Status, &rawMode,
		&out.CreatedAt, &out.UpdatedAt,
	); err != nil {
		return err
	}
	mode, err := types.ParseExecutionMode(rawMode)
	if err != nil {
		return fmt.Errorf("store: schedule %q has invalid execution mode: %w", out.ID, err)
	}
	out.ExecutionMode = mode
	return nil
}

func validTaskMutation(action TaskMutation) bool {
	switch action {
	case TaskMutationRun, TaskMutationPause, TaskMutationResume,
		TaskMutationEdit, TaskMutationDelete:
		return true
	default:
		return false
	}
}

func taskMutationEvent(action TaskMutation) string {
	return "task." + string(action) + "_requested"
}

func (s *Store) beginTeamTaskTx(
	ctx context.Context, tenantID, actorUserID int64, options pgx.TxOptions,
) (pgx.Tx, error) {
	if tenantID <= 0 || actorUserID <= 0 {
		return nil, types.NewAppError(types.CodeValidation,
			"团队任务身份无效", types.ErrValidation)
	}
	tx, err := s.beginTx(ctx, options)
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "开始团队任务事务", err)
	}
	fail := func(cause error) (pgx.Tx, error) {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return nil, cause
	}
	if err := setWorkspaceControlScope(ctx, tx, tenantID, actorUserID); err != nil {
		return fail(err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_app`); err != nil {
		return fail(types.NewAppError(types.CodeDatabase,
			"进入团队任务最小权限角色", err))
	}
	return tx, nil
}

func currentTeamTaskRole(
	ctx context.Context, tx pgx.Tx, tenantID, actorUserID int64,
) (types.MembershipRole, error) {
	var role types.MembershipRole
	err := tx.QueryRow(ctx, `
        SELECT m.role
          FROM memberships m JOIN tenants t ON t.id=m.tenant_id
         WHERE m.tenant_id=$1 AND m.user_id=$2
           AND t.status='active' AND t.deleted_at IS NULL`,
		tenantID, actorUserID).Scan(&role)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", types.NewAppError(types.CodeNotFound,
			"工作区不存在或无权访问任务", err)
	}
	if err != nil {
		return "", types.NewAppError(types.CodeDatabase, "校验团队任务成员", err)
	}
	if !role.Valid() {
		return "", types.NewAppError(types.CodeConflict, "工作区成员角色无效", nil)
	}
	return role, nil
}

// ListSchedulesForMember returns every workspace-visible team task, while a
// personal task remains visible only to its creator/assignee. The execution
// identity stays internal and never determines visibility.
func (s *Store) ListSchedulesForMember(
	ctx context.Context, tenantID, actorUserID int64,
) ([]types.Schedule, error) {
	tx, err := s.beginTeamTaskTx(ctx, tenantID, actorUserID,
		pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := currentTeamTaskRole(ctx, tx, tenantID, actorUserID); err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, `SELECT `+teamScheduleColumns+`
          FROM schedules s
          JOIN task_workspace_access a ON a.tenant_id=s.tenant_id
           AND a.execution_user_id=s.user_id AND a.schedule_id=s.id
         WHERE s.tenant_id=$1
           AND (a.task_visibility='workspace' OR
                a.creator_user_id=$2 OR a.assignee_user_id=$2)
           AND `+matureSchedulePredicate+`
         ORDER BY s.created_at DESC,s.id`, tenantID, actorUserID)
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "查询工作区任务", err)
	}
	defer rows.Close()
	var out []types.Schedule
	for rows.Next() {
		var item types.Schedule
		if err := scanTeamSchedule(rows, &item); err != nil {
			return nil, types.NewAppError(types.CodeDatabase, "扫描工作区任务", err)
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "遍历工作区任务", err)
	}
	rows.Close()
	if err := tx.Commit(ctx); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "提交工作区任务读取", err)
	}
	return out, nil
}

// GetScheduleForMember resolves product visibility but returns the frozen
// execution UserID for internal history/snapshot readers.
func (s *Store) GetScheduleForMember(
	ctx context.Context, tenantID, actorUserID int64, taskID string,
) (*types.Schedule, error) {
	if taskID == "" || taskID != strings.TrimSpace(taskID) || len(taskID) > 255 {
		return nil, types.NewAppError(types.CodeValidation,
			"任务标识无效", types.ErrValidation)
	}
	tx, err := s.beginTeamTaskTx(ctx, tenantID, actorUserID,
		pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := currentTeamTaskRole(ctx, tx, tenantID, actorUserID); err != nil {
		return nil, err
	}
	var out types.Schedule
	err = scanTeamSchedule(tx.QueryRow(ctx, `SELECT `+teamScheduleColumns+`
          FROM schedules s
          JOIN task_workspace_access a ON a.tenant_id=s.tenant_id
           AND a.execution_user_id=s.user_id AND a.schedule_id=s.id
         WHERE s.tenant_id=$1 AND s.id=$2
           AND (a.task_visibility='workspace' OR
                a.creator_user_id=$3 OR a.assignee_user_id=$3)
           AND `+matureSchedulePredicate,
		tenantID, taskID, actorUserID), &out)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, types.NewAppError(types.CodeNotFound, "任务不存在或无权访问", err)
	}
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "查询工作区任务", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "提交工作区任务读取", err)
	}
	return &out, nil
}

// AuthorizeScheduleMutation authorizes against the current membership and
// appends an immutable request audit before any external Temporal mutation.
// A creator may mutate their task; Admin and Owner may mutate all team tasks.
func (s *Store) AuthorizeScheduleMutation(
	ctx context.Context, tenantID, actorUserID int64, taskID string,
	action TaskMutation,
) (*types.Schedule, error) {
	if !validTaskMutation(action) || taskID == "" ||
		taskID != strings.TrimSpace(taskID) || len(taskID) > 255 {
		return nil, types.NewAppError(types.CodeValidation,
			"团队任务操作参数无效", types.ErrValidation)
	}
	tx, err := s.beginTeamTaskTx(ctx, tenantID, actorUserID, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	role, err := currentTeamTaskRole(ctx, tx, tenantID, actorUserID)
	if err != nil {
		return nil, err
	}
	var out types.Schedule
	err = scanTeamSchedule(tx.QueryRow(ctx, `SELECT `+teamScheduleColumns+`
          FROM schedules s
          JOIN task_workspace_access a ON a.tenant_id=s.tenant_id
           AND a.execution_user_id=s.user_id AND a.schedule_id=s.id
         WHERE s.tenant_id=$1 AND s.id=$2
           AND (a.task_visibility='workspace' OR
                a.creator_user_id=$3 OR a.assignee_user_id=$3)
           AND `+matureSchedulePredicate+`
         FOR UPDATE OF s,a`, tenantID, taskID, actorUserID), &out)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, types.NewAppError(types.CodeNotFound, "任务不存在或无权访问", err)
	}
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "锁定工作区任务", err)
	}
	if role != types.MembershipRoleOwner && role != types.MembershipRoleAdmin &&
		out.CreatorUserID != actorUserID {
		return nil, types.NewAppError(types.CodeForbidden,
			"只有任务创建者或工作区管理员可以执行此操作", types.ErrForbidden)
	}
	if _, err := tx.Exec(ctx, `
        INSERT INTO task_access_audit_events(
            tenant_id,task_id,actor_user_id,creator_user_id,
            execution_user_id,assignee_user_id,event_kind)
        VALUES($1,$2,$3,$4,$5,$6,$7)`,
		tenantID, taskID, actorUserID, out.CreatorUserID,
		out.UserID, out.AssigneeUserID, taskMutationEvent(action)); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "记录团队任务操作审计", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "提交团队任务操作授权", err)
	}
	return &out, nil
}

// TransferScheduleAssignee changes only the product responsibility. Frozen
// schedules.user_id and all historical snapshots remain byte-for-byte stable.
func (s *Store) TransferScheduleAssignee(
	ctx context.Context, tenantID, actorUserID int64, taskID string,
	targetUserID int64,
) (*types.Schedule, error) {
	if targetUserID <= 0 || taskID == "" ||
		taskID != strings.TrimSpace(taskID) || len(taskID) > 255 {
		return nil, types.NewAppError(types.CodeValidation,
			"任务负责人转移参数无效", types.ErrValidation)
	}
	tx, err := s.beginTeamTaskTx(ctx, tenantID, actorUserID, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	role, err := currentTeamTaskRole(ctx, tx, tenantID, actorUserID)
	if err != nil {
		return nil, err
	}
	if role != types.MembershipRoleOwner && role != types.MembershipRoleAdmin {
		return nil, types.NewAppError(types.CodeForbidden,
			"只有工作区管理员可以转移任务负责人", types.ErrForbidden)
	}
	var targetExists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(
        SELECT 1 FROM memberships m JOIN tenants t ON t.id=m.tenant_id
         WHERE m.tenant_id=$1 AND m.user_id=$2
           AND t.status='active' AND t.deleted_at IS NULL)`,
		tenantID, targetUserID).Scan(&targetExists); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "校验新任务负责人", err)
	}
	if !targetExists {
		return nil, types.NewAppError(types.CodeNotFound, "新负责人不是当前工作区成员", nil)
	}
	var out types.Schedule
	err = scanTeamSchedule(tx.QueryRow(ctx, `SELECT `+teamScheduleColumns+`
          FROM schedules s
          JOIN task_workspace_access a ON a.tenant_id=s.tenant_id
           AND a.execution_user_id=s.user_id AND a.schedule_id=s.id
         WHERE s.tenant_id=$1 AND s.id=$2
           AND a.task_visibility='workspace'
           AND `+matureSchedulePredicate+`
         FOR UPDATE OF s,a`, tenantID, taskID), &out)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, types.NewAppError(types.CodeNotFound, "团队任务不存在", err)
	}
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "锁定负责人转移任务", err)
	}
	previous := out.AssigneeUserID
	if previous == targetUserID {
		return nil, types.NewAppError(types.CodeConflict, "目标成员已经是任务负责人", nil)
	}
	if err := tx.QueryRow(ctx, `UPDATE task_workspace_access
        SET assignee_user_id=$3,updated_at=clock_timestamp()
        WHERE tenant_id=$1 AND schedule_id=$2
        RETURNING updated_at`, tenantID, taskID, targetUserID).Scan(&out.UpdatedAt); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "转移任务负责人", err)
	}
	if _, err := tx.Exec(ctx, `
        INSERT INTO task_access_audit_events(
            tenant_id,task_id,actor_user_id,creator_user_id,
            execution_user_id,assignee_user_id,target_user_id,event_kind,details)
        VALUES($1,$2,$3,$4,$5,$6,$7,'task.assignee_changed',
               jsonb_build_object('previous_assignee_user_id',$8::bigint))`,
		tenantID, taskID, actorUserID, out.CreatorUserID, out.UserID,
		targetUserID, targetUserID, previous); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "记录负责人转移审计", err)
	}
	out.AssigneeUserID = targetUserID
	if err := tx.Commit(ctx); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "提交任务负责人转移", err)
	}
	return &out, nil
}
