package store

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/server/types"
)

type taskRunAuthorizationQueryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

// ResolveScheduledRunIdentity resolves the complete tenant scope for a live
// scheduled task. Scope misses and revoked state deliberately collapse to one
// NotFound result: callers must not be able to distinguish another user's task
// from a paused, deleted, immature, or otherwise unavailable task.
//
// This resolver intentionally does not recover identity from a run snapshot.
// Response-lost recovery after task deletion is handled by the exact snapshot
// key; guessing from partial live state would weaken the tenant boundary.
func (s *Store) ResolveScheduledRunIdentity(
	ctx context.Context,
	taskID string,
	userID int64,
	workflowID string,
	runID string,
) (types.RunIdentity, error) {
	return resolveScheduledRunIdentity(ctx, s.pool, taskID, userID, workflowID, runID)
}

func resolveScheduledRunIdentity(
	ctx context.Context,
	q taskRunAuthorizationQueryer,
	taskID string,
	userID int64,
	workflowID string,
	runID string,
) (types.RunIdentity, error) {
	identity := types.RunIdentity{
		TemporalWorkflowID: workflowID,
		TemporalRunID:      runID,
		RunKind:            types.RunSnapshotKindScheduled,
		// TenantID is resolved below. A positive sentinel lets the shared
		// validator reject every other malformed caller-controlled field
		// before any database access.
		TenantID: 1,
		UserID:   userID,
		TaskID:   taskID,
	}
	if err := identity.Validate(); err != nil {
		return types.RunIdentity{}, err
	}
	if !validScheduledTaskWorkflowExecutionIDV1(taskID, workflowID) {
		return types.RunIdentity{}, taskRunNotFound()
	}

	var tenantID int64
	err := q.QueryRow(ctx,
		`SELECT s.tenant_id
		   FROM schedules s
		   JOIN tenants t ON t.id = s.tenant_id
		   JOIN memberships m
		     ON m.tenant_id = s.tenant_id AND m.user_id = s.user_id
		  WHERE s.id = $1 AND s.user_id = $2
		    AND s.status = $3
		    AND t.status = $4 AND t.deleted_at IS NULL
		    AND `+matureSchedulePredicate,
		taskID, userID, types.ScheduleStatusActive, types.TenantStatusActive,
	).Scan(&tenantID)
	if errors.Is(err, pgx.ErrNoRows) {
		return types.RunIdentity{}, taskRunNotFound()
	}
	if err != nil {
		return types.RunIdentity{}, taskRunDatabaseError(
			"resolve scheduled run identity", err)
	}

	identity.TenantID = tenantID
	if err := identity.Validate(); err != nil {
		return types.RunIdentity{}, taskRunDatabaseError(
			"validate resolved scheduled run identity", nil)
	}
	return identity, nil
}

// AuthorizeTaskRunSideEffect is the final live-state gate before a prepared
// scheduled run may spend money, call an external service, or write data. The
// sealed ref must name an exact, integrity-checked persisted snapshot before
// current tenant/member/task state is considered. expected must be assembled
// by the Activity from Temporal ActivityInfo plus its trusted task input; this
// prevents a valid snapshot reference from becoming a cross-run bearer token.
// A revoked or missing run returns false; malformed refs, identity mismatches,
// integrity failures, and database failures return errors and fail closed.
func (s *Store) AuthorizeTaskRunSideEffect(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
) (bool, error) {
	callerReference, err := validateTaskRunSnapshotReferenceForExpectedV1(ref, expected)
	if err != nil {
		return false, taskRunValidationError("task run snapshot reference is invalid")
	}
	lookup := CreateOrGetTaskRunSnapshotParams{
		TenantID: expected.TenantID, UserID: expected.UserID, TaskID: expected.TaskID,
		TemporalWorkflowID: expected.TemporalWorkflowID,
		TemporalRunID:      expected.TemporalRunID,
	}
	snapshot, found, err := loadTaskRunSnapshot(ctx, s.pool, lookup)
	if err != nil {
		return false, err
	}
	if !found {
		return false, nil
	}
	storedRef, err := snapshot.safeRef()
	if err != nil {
		return false, taskRunIntegrityError()
	}
	storedReference, err := validateTaskRunSnapshotReferenceForExpectedV1(
		storedRef, expected)
	if err != nil || storedReference != callerReference {
		return false, taskRunIntegrityError()
	}
	return authorizeLiveTaskRunSideEffectV1(ctx, s.pool, expected)
}

// AuthorizeTaskRunSideEffectV2 is the Source-free Tool runtime's final
// live-state gate. Its distinct reference type prevents retained V1 effects
// from accepting a Tool snapshot (and vice versa).
func (s *Store) AuthorizeTaskRunSideEffectV2(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRefV2,
) (bool, error) {
	if err := validateTaskRunSnapshotReferenceForExpectedV2(ref, expected); err != nil {
		return false, taskRunValidationError(
			"task run v2 snapshot reference is invalid")
	}
	lookup := taskRunLookupFromIdentity(expected)
	snapshot, found, err := loadTaskRunSnapshot(ctx, s.pool, lookup)
	if err != nil {
		return false, err
	}
	if !found {
		return false, nil
	}
	storedRef, err := snapshot.safeRefV2()
	if err != nil || storedRef != ref {
		return false, taskRunIntegrityError()
	}
	return authorizeLiveTaskRunSideEffectV1(ctx, s.pool, expected)
}

func authorizeLiveTaskRunSideEffectV1(
	ctx context.Context,
	q taskRunAuthorizationQueryer,
	identity types.RunIdentity,
) (bool, error) {
	if err := validateTaskRunExpectedIdentityV1(identity); err != nil {
		return false, taskRunValidationError(
			"scheduled v1 run identity is invalid")
	}

	var authorized bool
	if err := q.QueryRow(ctx,
		`SELECT EXISTS (
		    SELECT 1
		      FROM schedules s
		      JOIN tenants t ON t.id = s.tenant_id
		      JOIN memberships m
		        ON m.tenant_id = s.tenant_id AND m.user_id = s.user_id
		     WHERE s.id = $1
		       AND s.tenant_id = $2
		       AND s.user_id = $3
		       AND (
		         s.status = $4 OR (
		           s.status = $6 AND authorize_manual_task_run_v1(
		             s.tenant_id, s.user_id, s.id, $7
		           )
		         )
		       )
		       AND t.status = $5 AND t.deleted_at IS NULL
		       AND `+matureSchedulePredicate+`
		)`,
		identity.TaskID, identity.TenantID, identity.UserID,
		types.ScheduleStatusActive, types.TenantStatusActive,
		types.ScheduleStatusPaused, identity.TemporalWorkflowID,
	).Scan(&authorized); err != nil {
		return false, taskRunDatabaseError("authorize task run side effect", err)
	}
	return authorized, nil
}

func scheduledTaskWorkflowID(taskID string) string {
	return taskRunScheduledWorkflowPrefixV1 + taskID
}
