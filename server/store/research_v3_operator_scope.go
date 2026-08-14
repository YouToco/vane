package store

import (
	"context"
	"errors"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/server/types"
)

// ResolveResearchV3OperatorScope resolves the exact owner tuple from database
// truth. It is intentionally callable only by the direct migration owner that
// can enter the NOLOGIN cutover role created by migration 101.
func (s *Store) ResolveResearchV3OperatorScope(
	ctx context.Context, taskID string,
) (types.ResearchV3OperatorScope, error) {
	if taskID == "" || strings.TrimSpace(taskID) != taskID || len(taskID) > 255 {
		return types.ResearchV3OperatorScope{}, types.NewAppError(
			types.CodeValidation, "Research V3 operator task is invalid", types.ErrValidation)
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return types.ResearchV3OperatorScope{}, taskStateDatabaseError(
			"begin Research V3 operator scope resolution", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var directOwner bool
	if err := tx.QueryRow(ctx, `
		SELECT session_user=current_user AND
		       pg_has_role(current_user,'vane_research_v3_cutover_operator','MEMBER')
	`).Scan(&directOwner); err != nil || !directOwner {
		if err == nil {
			err = errors.New("migration-owner cutover role is unavailable")
		}
		return types.ResearchV3OperatorScope{}, types.NewAppError(
			types.CodeConflict, "Research V3 operator database authority is unavailable", err)
	}
	var (
		scope   types.ResearchV3OperatorScope
		rawMode string
		version *int64
		digest  *string
	)
	err = tx.QueryRow(ctx, `
		SELECT schedule.tenant_id,schedule.user_id,schedule.id,schedule.status,
		       schedule.execution_mode,schedule.spec_json,
		       schedule.approved_definition_version,
		       schedule.approved_definition_digest
		  FROM schedules schedule
		  JOIN tenants tenant ON tenant.id=schedule.tenant_id
		  JOIN memberships membership
		    ON membership.tenant_id=schedule.tenant_id
		   AND membership.user_id=schedule.user_id
		 WHERE schedule.id=$1
		   AND schedule.status IN ('active','paused')
		   AND membership.role='owner'
		   AND tenant.status='active' AND tenant.deleted_at IS NULL
		   AND NOT EXISTS (
		       SELECT 1 FROM task_creation_operations operation
		        WHERE operation.tenant_id=schedule.tenant_id
		          AND operation.user_id=schedule.user_id
		          AND operation.task_id=schedule.id
		          AND ((operation.execution_version=1 AND operation.tool_name='create_schedule') OR
		               (operation.execution_version=2 AND operation.tool_name='manage_tasks'))
		          AND NOT (operation.status='executed' AND operation.phase='completed'))
	`, taskID).Scan(&scope.TenantID, &scope.UserID, &scope.TaskID, &scope.Status,
		&rawMode, &scope.SpecJSON, &version, &digest)
	if errors.Is(err, pgx.ErrNoRows) {
		return types.ResearchV3OperatorScope{}, types.NewAppError(
			types.CodeNotFound, "Research V3 operator task is unavailable", types.ErrNotFound)
	}
	if err != nil {
		return types.ResearchV3OperatorScope{}, taskStateDatabaseError(
			"resolve Research V3 operator scope", err)
	}
	scope.ExecutionMode, err = types.ParseExecutionMode(rawMode)
	if err != nil || (version == nil) != (digest == nil) {
		return types.ResearchV3OperatorScope{}, taskStateIntegrity()
	}
	if version != nil {
		if *version <= 0 || !validDigestSyntaxV3(*digest) {
			return types.ResearchV3OperatorScope{}, taskStateIntegrity()
		}
		scope.ProductionHead = &types.ResearchV3DefinitionHead{
			Version: *version, Digest: *digest,
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return types.ResearchV3OperatorScope{}, taskStateDatabaseError(
			"commit Research V3 operator scope resolution", err)
	}
	return scope, nil
}
