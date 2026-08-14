package store

import (
	"cmp"
	"context"
	"fmt"
	"slices"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/server/types"
)

// beginObservedEventAdmissionV1 joins observation writes to the same schema,
// tenant, and exact-batch lock order as effect creation and settlement.
func (s *Store) beginObservedEventAdmissionV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	batchScopes []types.PushBatchScope,
	snapshotByBatch map[int64]int64,
	requireClaimedAuthority bool,
) (pgx.Tx, map[int64]types.BatchStatus, error) {
	callerReference, err :=
		validateTaskRunSnapshotReferenceForExpectedV1(ref, expected)
	if err != nil || len(batchScopes) == 0 {
		return nil, nil, taskRunValidationError(
			"observed event admission reference is invalid")
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, nil, taskRunDatabaseError(
			"begin observed event admission", err)
	}
	if err := lockPushEffectSchemaWriter(ctx, tx); err != nil {
		rollbackCompiledTaskTx(ctx, tx)
		return nil, nil, taskRunDatabaseError(
			"lock observed event schema admission", err)
	}
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.tenant_id',$1,true)`,
		fmt.Sprintf("%d", expected.TenantID),
	); err != nil {
		rollbackCompiledTaskTx(ctx, tx)
		return nil, nil, taskRunDatabaseError(
			"set observed event tenant context", err)
	}
	tenantExists, err := lockTenantAdmissionRoot(
		ctx, tx, expected.TenantID)
	if err != nil {
		rollbackCompiledTaskTx(ctx, tx)
		return nil, nil, taskRunDatabaseError(
			"lock observed event tenant admission", err)
	}
	if !tenantExists {
		rollbackCompiledTaskTx(ctx, tx)
		return nil, nil, taskRunNotFound()
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_app`); err != nil {
		rollbackCompiledTaskTx(ctx, tx)
		return nil, nil, taskRunDatabaseError(
			"enter observed event application role", err)
	}
	statusByBatch := make(map[int64]types.BatchStatus, len(batchScopes))
	slices.SortFunc(batchScopes, func(a, b types.PushBatchScope) int {
		return cmp.Compare(a.BatchID, b.BatchID)
	})
	for _, scope := range batchScopes {
		if _, exists := statusByBatch[scope.BatchID]; exists {
			continue
		}
		requiredAuthority := types.PushBatchDeliveryAuthority("")
		if requireClaimedAuthority {
			requiredAuthority = "*"
		}
		status, lockErr := lockPushEffectBatchAdmission(
			ctx,
			tx,
			scope,
			snapshotByBatch[scope.BatchID],
			requiredAuthority,
		)
		if lockErr != nil {
			rollbackCompiledTaskTx(ctx, tx)
			return nil, nil, lockErr
		}
		statusByBatch[scope.BatchID] = status
	}

	lookup := CreateOrGetTaskRunSnapshotParams{
		TenantID:           expected.TenantID,
		UserID:             expected.UserID,
		TaskID:             expected.TaskID,
		TemporalWorkflowID: expected.TemporalWorkflowID,
		TemporalRunID:      expected.TemporalRunID,
	}
	snapshot, found, err := loadTaskRunSnapshot(ctx, tx, lookup)
	if err != nil {
		rollbackCompiledTaskTx(ctx, tx)
		return nil, nil, err
	}
	if !found {
		rollbackCompiledTaskTx(ctx, tx)
		return nil, nil, taskRunNotFound()
	}
	storedRef, err := snapshot.safeRef()
	if err != nil {
		rollbackCompiledTaskTx(ctx, tx)
		return nil, nil, taskRunIntegrityError()
	}
	storedReference, err :=
		validateTaskRunSnapshotReferenceForExpectedV1(storedRef, expected)
	if err != nil || storedReference != callerReference {
		rollbackCompiledTaskTx(ctx, tx)
		return nil, nil, taskRunIntegrityError()
	}
	if err := lockLiveCompiledRunWriteV1(ctx, tx, expected); err != nil {
		rollbackCompiledTaskTx(ctx, tx)
		return nil, nil, err
	}
	return tx, statusByBatch, nil
}
