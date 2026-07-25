package store

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/pusheffect"
)

// AuthorizePushEffectRunSideEffect re-enters the existing task-run
// authorization chain from one explicitly scoped durable Push effect.
//
// Recovery cannot reconstruct TemporalWorkflowID or a sealed RunSnapshotRef
// from the effect's partial run fields. This method therefore loads the effect
// through its tenant-scoped restricted role, resolves the exact immutable
// snapshot by the persisted snapshot/run tuple, rebuilds its integrity-checked
// safe reference, and only then calls AuthorizeTaskRunSideEffect.
//
// This is a live-authority preflight, not a send claim. A future recovery
// sender must still use one atomic authorized-claim Store transition before it
// crosses the provider boundary.
func (s *Store) AuthorizePushEffectRunSideEffect(
	ctx context.Context,
	scope pusheffect.Scope,
) (bool, error) {
	effect, err := s.LoadPushEffect(ctx, scope)
	if err != nil {
		return false, err
	}

	tx, err := s.beginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.ReadCommitted,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return false, pushEffectDatabaseError(
			"begin run authorization snapshot transaction", err)
	}
	defer rollbackPushEffectTx(ctx, tx)
	if err := setTaskRunTenantContext(ctx, tx, effect.TenantID); err != nil {
		return false, err
	}

	snapshot, err := scanTaskRunSnapshot(tx.QueryRow(ctx,
		`SELECT `+taskRunSnapshotColumns+`
		   FROM task_run_snapshots
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		    AND task_id=$4 AND temporal_run_id=$5`,
		effect.RunSnapshotID,
		effect.TenantID,
		effect.UserID,
		effect.TaskID,
		effect.RunID,
	))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, pushEffectIntegrity()
		}
		return false, pushEffectDatabaseError(
			"load run authorization snapshot", err)
	}
	ref, err := snapshot.safeRef()
	if err != nil ||
		ref.SnapshotID != effect.RunSnapshotID ||
		ref.TenantID != effect.TenantID ||
		ref.UserID != effect.UserID ||
		ref.TaskID != effect.TaskID ||
		ref.TemporalRunID != effect.RunID {
		return false, pushEffectIntegrity()
	}
	if err := tx.Commit(ctx); err != nil {
		return false, pushEffectDatabaseError(
			"commit run authorization snapshot transaction", err)
	}
	return s.AuthorizeTaskRunSideEffect(ctx, ref.Identity(), ref)
}
