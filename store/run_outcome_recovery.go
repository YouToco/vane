package store

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/types"
)

const maxRunOutcomeRecoveryPageV1 = 100

// RunOutcomeRecoveryCursorV1 is the stable (created_at,id) keyset position
// returned by the constrained recovery reader.
type RunOutcomeRecoveryCursorV1 struct {
	CreatedAt time.Time
	ID        int64
}

// RunOutcomeRecoveryCandidateV1 is the complete capability returned to the
// recovery runner: one marker, its exact Temporal execution, and its cursor.
type RunOutcomeRecoveryCandidateV1 struct {
	Marker    types.RunOutcomeMarkerV1
	Identity  types.RunIdentity
	CreatedAt time.Time
}

// ListStaleRunOutcomeCandidatesV1 reads one bounded keyset page through the
// recovery-only SECURITY DEFINER function. The role has no table privileges.
func (s *Store) ListStaleRunOutcomeCandidatesV1(
	ctx context.Context,
	after *RunOutcomeRecoveryCursorV1,
	limit int,
) ([]RunOutcomeRecoveryCandidateV1, error) {
	if limit <= 0 || limit > maxRunOutcomeRecoveryPageV1 {
		return nil, canonicalBriefValidationError(
			"run outcome recovery page limit is invalid")
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, canonicalBriefDatabaseError(
			"begin run outcome recovery read", err)
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	if _, err := tx.Exec(
		ctx, `SET LOCAL search_path=pg_catalog,public,pg_temp`); err != nil {
		return nil, canonicalBriefDatabaseError(
			"pin run outcome recovery search path", err)
	}
	if _, err := tx.Exec(
		ctx, `SET LOCAL ROLE vane_run_outcome_recovery`); err != nil {
		return nil, canonicalBriefDatabaseError(
			"enter run outcome recovery role", err)
	}
	var afterCreated any
	var afterID int64
	if after != nil {
		if after.CreatedAt.IsZero() || after.ID <= 0 {
			return nil, canonicalBriefValidationError(
				"run outcome recovery cursor is invalid")
		}
		afterCreated = after.CreatedAt
		afterID = after.ID
	}
	rows, err := tx.Query(ctx,
		`SELECT outcome_id,schema_version,run_snapshot_id,
		        tenant_id,user_id,task_id,temporal_workflow_id,
		        temporal_run_id,created_at
		   FROM read_stale_run_outcomes_v1($1,$2,$3)`,
		afterCreated, afterID, limit,
	)
	if err != nil {
		return nil, canonicalBriefDatabaseError(
			"list stale run outcomes", err)
	}
	defer rows.Close()
	result := make([]RunOutcomeRecoveryCandidateV1, 0, limit)
	for rows.Next() {
		var candidate RunOutcomeRecoveryCandidateV1
		if err := rows.Scan(
			&candidate.Marker.ID, &candidate.Marker.SchemaVersion,
			&candidate.Marker.RunSnapshotID, &candidate.Marker.TenantID,
			&candidate.Marker.UserID, &candidate.Marker.TaskID,
			&candidate.Identity.TemporalWorkflowID,
			&candidate.Identity.TemporalRunID, &candidate.CreatedAt,
		); err != nil {
			return nil, canonicalBriefDatabaseError(
				"scan stale run outcome", err)
		}
		candidate.Identity.RunKind = types.RunSnapshotKindScheduled
		candidate.Identity.TenantID = candidate.Marker.TenantID
		candidate.Identity.UserID = candidate.Marker.UserID
		candidate.Identity.TaskID = candidate.Marker.TaskID
		candidate.CreatedAt = candidate.CreatedAt.Round(0).UTC().
			Truncate(time.Microsecond)
		if err := candidate.Marker.Validate(); err != nil ||
			candidate.Identity.Validate() != nil ||
			candidate.CreatedAt.IsZero() {
			return nil, canonicalBriefIntegrityError()
		}
		result = append(result, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, canonicalBriefDatabaseError(
			"iterate stale run outcomes", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, canonicalBriefDatabaseError(
			"commit run outcome recovery read", err)
	}
	return result, nil
}

// FinalizeRecoveredRunOutcomeClaimV1 accepts only a candidate previously
// obtainable from the constrained reader. It rechecks exact snapshot/Temporal
// identity before entering the same writer CAS used by workflow finalization.
func (s *Store) FinalizeRecoveredRunOutcomeClaimV1(
	ctx context.Context,
	expected types.RunIdentity,
	claim types.RunOutcomeClaimV1,
) (types.RunOutcomeV1, error) {
	if err := expected.Validate(); err != nil ||
		claim.Validate() != nil ||
		claim.TenantID != expected.TenantID ||
		claim.UserID != expected.UserID ||
		claim.TaskID != expected.TaskID {
		return types.RunOutcomeV1{}, canonicalBriefValidationError(
			"recovered run outcome claim is invalid")
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return types.RunOutcomeV1{}, canonicalBriefDatabaseError(
			"begin recovered outcome transaction", err)
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	if err := lockPushEffectSchemaWriter(ctx, tx); err != nil {
		return types.RunOutcomeV1{}, canonicalBriefDatabaseError(
			"lock recovered outcome schema admission", err)
	}
	if _, err := tx.Exec(
		ctx, `SET LOCAL search_path=pg_catalog,public,pg_temp`); err != nil {
		return types.RunOutcomeV1{}, canonicalBriefDatabaseError(
			"pin recovered outcome search path", err)
	}
	exists, err := lockTenantAdmissionRoot(ctx, tx, expected.TenantID)
	if err != nil {
		return types.RunOutcomeV1{}, canonicalBriefDatabaseError(
			"lock recovered outcome tenant admission", err)
	}
	if !exists {
		return types.RunOutcomeV1{}, canonicalBriefNotFoundError(
			"recovered outcome tenant is unavailable")
	}
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.tenant_id',$1,true),
		        set_config('app.user_id',$2,true)`,
		strconv.FormatInt(expected.TenantID, 10),
		strconv.FormatInt(expected.UserID, 10)); err != nil {
		return types.RunOutcomeV1{}, canonicalBriefDatabaseError(
			"set recovered outcome identity", err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_brief_writer`); err != nil {
		return types.RunOutcomeV1{}, canonicalBriefDatabaseError(
			"enter recovered outcome writer role", err)
	}
	var exact bool
	err = tx.QueryRow(ctx,
		`SELECT true
		   FROM read_canonical_brief_run_identity_v1($1)
		  WHERE task_id=$2
		    AND temporal_workflow_id=$3 AND temporal_run_id=$4`,
		claim.RunSnapshotID, expected.TaskID,
		expected.TemporalWorkflowID, expected.TemporalRunID,
	).Scan(&exact)
	if errors.Is(err, pgx.ErrNoRows) || !exact {
		return types.RunOutcomeV1{}, canonicalBriefNotFoundError(
			"recovered outcome snapshot is unavailable")
	}
	if err != nil {
		return types.RunOutcomeV1{}, canonicalBriefDatabaseError(
			"read recovered outcome identity", err)
	}
	outcome, err := finalizeRunOutcomeClaimTxV1(ctx, tx, claim)
	if err != nil {
		return types.RunOutcomeV1{}, err
	}
	if err := commitCanonicalBriefTxV1(
		ctx, tx, "commit recovered run outcome claim"); err != nil {
		return types.RunOutcomeV1{}, err
	}
	return outcome, nil
}
