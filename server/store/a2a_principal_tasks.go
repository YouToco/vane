package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/server/types"
)

const a2aPrincipalTaskColumns = `tenant_id,principal_user_id,actor_type,
	created_by_token_id,id,context_id,status,task,version,created_at,updated_at`

func scanA2APrincipalTask(row pgx.Row, task *types.A2ATask) error {
	return row.Scan(&task.TenantID, &task.PrincipalUserID, &task.ActorType,
		&task.CreatedByToken, &task.ID, &task.ContextID, &task.Status,
		&task.Task, &task.Version, &task.CreatedAt, &task.UpdatedAt)
}

func validateA2AExecutionScope(scope types.A2AExecutionScope) error {
	if scope.TenantID <= 0 || scope.UserID <= 0 || !scope.ActorType.Valid() {
		return types.NewAppError(types.CodeForbidden, "invalid A2A execution scope", nil)
	}
	if _, err := uuid.Parse(scope.TokenID); err != nil {
		return types.NewAppError(types.CodeForbidden, "invalid A2A execution scope", err)
	}
	return nil
}

// beginA2APrincipalTx revalidates the complete live credential authority for
// every task/content operation. The detached SDK execution keeps the captured
// scope value, but revocation, expiry, membership removal or a new membership
// generation immediately makes subsequent database work fail closed.
func (s *Store) beginA2APrincipalTx(
	ctx context.Context,
	scope types.A2AExecutionScope,
) (pgx.Tx, error) {
	if err := validateA2AExecutionScope(scope); err != nil {
		return nil, err
	}
	// Reads intentionally use a read-write transaction because FOR KEY SHARE is
	// the linearization point against membership removal and tenant erasure.
	tx, err := s.beginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted, AccessMode: pgx.ReadWrite})
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "begin scoped A2A transaction", err)
	}
	fail := func(message string, cause error) (pgx.Tx, error) {
		_ = tx.Rollback(ctx)
		return nil, types.NewAppError(types.CodeDatabase, message, cause)
	}
	exists, err := lockTenantAdmissionRootShared(ctx, tx, scope.TenantID)
	if err != nil {
		return fail("lock A2A workspace admission", err)
	}
	if !exists {
		_ = tx.Rollback(ctx)
		return nil, types.NewAppError(types.CodeForbidden, "A2A workspace is unavailable", nil)
	}
	var liveRole types.MembershipRole
	var liveActor types.ActorType
	var liveScopes []string
	err = tx.QueryRow(ctx, `
		SELECT CASE WHEN token.actor_type='service_account' THEN 'member' ELSE membership.role END,
		       token.actor_type,token.scopes
		FROM memberships membership
		JOIN tenants workspace ON workspace.id=membership.tenant_id
		JOIN a2a_access_tokens token
		  ON token.tenant_id=membership.tenant_id
		 AND token.principal_user_id=membership.user_id
		 AND token.membership_generation=membership.authorization_generation
		WHERE membership.tenant_id=$1 AND membership.user_id=$2
		  AND token.id=$3 AND token.revoked_at IS NULL
		  AND token.expires_at>clock_timestamp()
		  AND workspace.status='active' AND workspace.deleted_at IS NULL
		FOR KEY SHARE OF membership,workspace,token`,
		scope.TenantID, scope.UserID, scope.TokenID).Scan(&liveRole, &liveActor, &liveScopes)
	if err != nil {
		_ = tx.Rollback(ctx)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, types.NewAppError(types.CodeForbidden, "A2A authority is no longer active", err)
		}
		return nil, types.NewAppError(types.CodeDatabase, "revalidate A2A authority", err)
	}
	if liveActor != scope.ActorType || liveRole != scope.Role || !a2aScopeSetEqual(scope.Scopes, liveScopes) {
		_ = tx.Rollback(ctx)
		return nil, types.NewAppError(types.CodeForbidden, "A2A execution authority drifted", nil)
	}
	for key, value := range map[string]string{
		"app.tenant_id":    fmt.Sprint(scope.TenantID),
		"app.user_id":      fmt.Sprint(scope.UserID),
		"app.actor_type":   string(scope.ActorType),
		"app.a2a_token_id": scope.TokenID,
	} {
		if _, err := tx.Exec(ctx, `SELECT set_config($1,$2,true)`, key, value); err != nil {
			return fail("set scoped A2A database authority", err)
		}
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_app`); err != nil {
		return fail("enter scoped A2A database role", err)
	}
	return tx, nil
}

func a2aScopeSetEqual(captured []types.A2AScope, live []string) bool {
	if len(captured) != len(live) {
		return false
	}
	for i := range captured {
		if string(captured[i]) != live[i] {
			return false
		}
	}
	return true
}

func (s *Store) CreateA2APrincipalTask(ctx context.Context, scope types.A2AExecutionScope, task *types.A2ATask) error {
	tx, err := s.beginA2APrincipalTx(ctx, scope)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	task.TenantID, task.PrincipalUserID = scope.TenantID, scope.UserID
	task.ActorType, task.CreatedByToken = scope.ActorType, scope.TokenID
	err = scanA2APrincipalTask(tx.QueryRow(ctx, `
		INSERT INTO a2a_principal_tasks
		 (tenant_id,principal_user_id,actor_type,created_by_token_id,id,context_id,status,task)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
		RETURNING `+a2aPrincipalTaskColumns,
		task.TenantID, task.PrincipalUserID, task.ActorType, task.CreatedByToken,
		task.ID, task.ContextID, task.Status, task.Task), task)
	if err != nil {
		if isUniqueViolation(err) {
			return types.NewAppError(types.CodeConflict, "A2A task already exists", err)
		}
		return types.NewAppError(types.CodeDatabase, "create scoped A2A task", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return types.NewAppError(types.CodeDatabase, "commit scoped A2A task", err)
	}
	return nil
}

func (s *Store) GetA2APrincipalTask(ctx context.Context, scope types.A2AExecutionScope, id string) (*types.A2ATask, error) {
	tx, err := s.beginA2APrincipalTx(ctx, scope)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var task types.A2ATask
	err = scanA2APrincipalTask(tx.QueryRow(ctx, `SELECT `+a2aPrincipalTaskColumns+`
		FROM a2a_principal_tasks WHERE tenant_id=$1 AND principal_user_id=$2 AND id=$3`,
		scope.TenantID, scope.UserID, id), &task)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, types.NewAppError(types.CodeNotFound, "A2A task not found", err)
		}
		return nil, types.NewAppError(types.CodeDatabase, "read scoped A2A task", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "commit scoped A2A task read", err)
	}
	return &task, nil
}

func (s *Store) UpdateA2APrincipalTask(ctx context.Context, scope types.A2AExecutionScope, id string, expectedVersion int64, status string, payload json.RawMessage) error {
	tx, err := s.beginA2APrincipalTx(ctx, scope)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	tag, err := tx.Exec(ctx, `UPDATE a2a_principal_tasks
		SET status=$4,task=$5,version=version+1,updated_at=clock_timestamp()
		WHERE tenant_id=$1 AND principal_user_id=$2 AND id=$3 AND version=$6`,
		scope.TenantID, scope.UserID, id, status, payload, expectedVersion)
	if err != nil {
		return types.NewAppError(types.CodeDatabase, "update scoped A2A task", err)
	}
	if tag.RowsAffected() == 0 {
		var exists bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM a2a_principal_tasks
			WHERE tenant_id=$1 AND principal_user_id=$2 AND id=$3)`,
			scope.TenantID, scope.UserID, id).Scan(&exists); err != nil {
			return types.NewAppError(types.CodeDatabase, "check scoped A2A task version", err)
		}
		if !exists {
			return types.NewAppError(types.CodeNotFound, "A2A task not found", nil)
		}
		return types.NewAppError(types.CodeConflict, "A2A task version advanced", nil)
	}
	if err := tx.Commit(ctx); err != nil {
		return types.NewAppError(types.CodeDatabase, "commit scoped A2A task update", err)
	}
	return nil
}

func (s *Store) ListA2APrincipalTasks(ctx context.Context, scope types.A2AExecutionScope, q types.A2ATaskQuery) ([]types.A2ATask, int64, string, error) {
	tx, err := s.beginA2APrincipalTx(ctx, scope)
	if err != nil {
		return nil, 0, "", err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	pageSize := clampA2APageSize(q.PageSize)
	conds := []string{"tenant_id=$1", "principal_user_id=$2"}
	args := []any{scope.TenantID, scope.UserID}
	if q.ContextID != "" {
		args = append(args, q.ContextID)
		conds = append(conds, fmt.Sprintf("context_id=$%d", len(args)))
	}
	if q.Status != "" {
		args = append(args, q.Status)
		conds = append(conds, fmt.Sprintf("status=$%d", len(args)))
	}
	if !q.StatusTimestampAfter.IsZero() {
		args = append(args, q.StatusTimestampAfter)
		conds = append(conds, fmt.Sprintf("updated_at>$%d", len(args)))
	}
	baseConds := append([]string(nil), conds...)
	baseArgs := append([]any(nil), args...)
	var total int64
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM a2a_principal_tasks WHERE `+
		strings.Join(baseConds, " AND "), baseArgs...).Scan(&total); err != nil {
		return nil, 0, "", types.NewAppError(types.CodeDatabase, "count scoped A2A tasks", err)
	}
	if q.PageToken != "" {
		cursorAt, cursorID, err := decodeA2ACursor(q.PageToken)
		if err != nil {
			return nil, 0, "", err
		}
		args = append(args, cursorAt, cursorID)
		conds = append(conds, fmt.Sprintf("(created_at,id)<($%d,$%d)", len(args)-1, len(args)))
	}
	args = append(args, pageSize)
	rows, err := tx.Query(ctx, `SELECT `+a2aPrincipalTaskColumns+`
		FROM a2a_principal_tasks WHERE `+strings.Join(conds, " AND ")+
		fmt.Sprintf(" ORDER BY created_at DESC,id DESC LIMIT $%d", len(args)), args...)
	if err != nil {
		return nil, 0, "", types.NewAppError(types.CodeDatabase, "list scoped A2A tasks", err)
	}
	defer rows.Close()
	items := make([]types.A2ATask, 0)
	for rows.Next() {
		var item types.A2ATask
		if err := scanA2APrincipalTask(rows, &item); err != nil {
			return nil, 0, "", types.NewAppError(types.CodeDatabase, "scan scoped A2A task", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, "", types.NewAppError(types.CodeDatabase, "iterate scoped A2A tasks", err)
	}
	next := ""
	if len(items) == pageSize {
		last := items[len(items)-1]
		next = encodeA2ACursor(last.CreatedAt, last.ID)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, 0, "", types.NewAppError(types.CodeDatabase, "commit scoped A2A task list", err)
	}
	return items, total, next, nil
}

// FailStaleA2APrincipalTasks is startup-only schema-owner maintenance. It does
// not create an interactive authorization path and only seals orphaned work
// left by the previous process generation.
func (s *Store) FailStaleA2APrincipalTasks(ctx context.Context, olderThan time.Time) (int64, error) {
	tag, err := s.pool.Exec(ctx, `UPDATE a2a_principal_tasks
		SET status='TASK_STATE_FAILED',
		    task=jsonb_set(task,'{status}',
		      (CASE WHEN jsonb_typeof(task->'status')='object' THEN task->'status' ELSE '{}'::jsonb END)
		      || jsonb_build_object('state','TASK_STATE_FAILED','timestamp',clock_timestamp()),true),
		    version=version+1,updated_at=clock_timestamp()
		WHERE status IN ('TASK_STATE_SUBMITTED','TASK_STATE_WORKING') AND updated_at<$1`, olderThan)
	if err != nil {
		return 0, types.NewAppError(types.CodeDatabase, "seal stale scoped A2A tasks", err)
	}
	return tag.RowsAffected(), nil
}
