package store

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/runcontext"
	"github.com/YouToco/vane/types"
)

// LoadCompiledRunSnapshotRefV2 recovers only an already-committed Source-free
// reference. It does not read the current task head or rebuild live policy.
func (s *Store) LoadCompiledRunSnapshotRefV2(
	ctx context.Context,
	expected types.RunIdentity,
) (types.RunSnapshotRefV2, bool, error) {
	if validateTaskRunExpectedIdentityV1(expected) != nil {
		return types.RunSnapshotRefV2{}, false, taskRunValidationError(
			"scheduled v2 run identity is invalid")
	}
	lookup := taskRunLookupFromIdentity(expected)
	snapshot, found, err := s.loadTaskRunSnapshotScopedV2(ctx, lookup)
	if err != nil || !found {
		return types.RunSnapshotRefV2{}, found, err
	}
	if snapshot.ReferenceSchemaVersion != types.RunSnapshotSchemaVersionV2 {
		return types.RunSnapshotRefV2{}, false, taskRunIntegrityError()
	}
	ref, err := snapshot.safeRefV2()
	if err != nil ||
		validateTaskRunSnapshotReferenceForExpectedV2(ref, expected) != nil {
		return types.RunSnapshotRefV2{}, false, taskRunIntegrityError()
	}
	return ref, true, nil
}

// LoadCompiledTaskRunSnapshotV2 returns the immutable Activity-only execution
// view. It validates the caller's full Temporal identity and every persisted
// digest, then decodes only the frozen payload; no live task/policy/config is
// consulted.
func (s *Store) LoadCompiledTaskRunSnapshotV2(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRefV2,
) (runcontext.CompiledSnapshotV2, error) {
	if validateTaskRunSnapshotReferenceForExpectedV2(ref, expected) != nil {
		return runcontext.CompiledSnapshotV2{}, taskRunValidationError(
			"task run v2 snapshot reference is invalid")
	}
	lookup := taskRunLookupFromIdentity(expected)
	snapshot, found, err := s.loadTaskRunSnapshotScopedV2(ctx, lookup)
	if err != nil {
		return runcontext.CompiledSnapshotV2{}, err
	}
	if !found {
		return runcontext.CompiledSnapshotV2{}, taskRunNotFound()
	}
	if snapshot.ReferenceSchemaVersion != types.RunSnapshotSchemaVersionV2 {
		return runcontext.CompiledSnapshotV2{}, taskRunIntegrityError()
	}
	storedRef, err := snapshot.safeRefV2()
	if err != nil || storedRef != ref {
		return runcontext.CompiledSnapshotV2{}, taskRunIntegrityError()
	}
	decoded, err := readTaskRunSnapshotPayloadV2(snapshot.Payload)
	if err != nil {
		return runcontext.CompiledSnapshotV2{}, taskRunIntegrityError()
	}
	payload := decoded.Payload
	return runcontext.CompiledSnapshotV2{
		Ref:                            storedRef,
		Mode:                           payload.Mode,
		DefinitionVersion:              payload.DefinitionVersion,
		AdaptiveVersion:                payload.AdaptiveVersion,
		AdaptiveBasisDefinitionVersion: payload.AdaptiveBasisDefinitionVersion,
		AdaptiveBasisDefinitionDigest:  payload.AdaptiveBasisDefinitionDigest,
		ObservationRollout:             payload.ObservationRollout,
		Budget:                         types.PlannerBudget{},
		Definition:                     payload.Definition,
		Adaptive:                       payload.Adaptive,
		ToolBindings:                   payload.ToolBindings,
		Policy:                         decoded.Policy,
	}, nil
}

func (s *Store) loadTaskRunSnapshotScopedV2(
	ctx context.Context,
	lookup CreateOrGetTaskRunSnapshotParams,
) (*taskRunSnapshot, bool, error) {
	tx, err := s.beginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, false, taskRunDatabaseError(
			"begin task run v2 read transaction", err)
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	if err := setTaskRunTenantContext(ctx, tx, lookup.TenantID); err != nil {
		return nil, false, err
	}
	snapshot, found, err := loadTaskRunSnapshot(ctx, tx, lookup)
	if err != nil {
		return nil, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, false, taskRunDatabaseError(
			"commit task run v2 read transaction", err)
	}
	return snapshot, found, nil
}
