package store

import (
	"context"
	"errors"
	"strconv"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/server/types"
)

const (
	pushEffectBatchLockNamespace = "vane/push-effect-batch/v1/"
	pushEffectBatchLockSeed      = int64(0x50555348)
)

// lockPushEffectBatchAdmission serializes every legacy/effect/observation
// writer for one immutable batch scope.
func lockPushEffectBatchAdmission(
	ctx context.Context,
	tx pgx.Tx,
	scope types.PushBatchScope,
	runSnapshotID int64,
	requiredAuthority types.PushBatchDeliveryAuthority,
) (types.BatchStatus, error) {
	if scope.TenantID <= 0 || scope.UserID <= 0 || scope.BatchID <= 0 ||
		runSnapshotID <= 0 {
		return "", pushEffectValidation("push effect batch scope is invalid")
	}
	if err := lockPushEffectSchemaWriter(ctx, tx); err != nil {
		return "", pushEffectDatabaseError(
			"lock push effect schema admission", err)
	}
	key := pushEffectBatchLockNamespace +
		strconv.FormatInt(scope.TenantID, 10) + "/" +
		strconv.FormatInt(scope.UserID, 10) + "/" +
		strconv.FormatInt(scope.BatchID, 10)
	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1,$2))`,
		key, pushEffectBatchLockSeed,
	); err != nil {
		return "", pushEffectDatabaseError("lock push effect batch fence", err)
	}
	var status types.BatchStatus
	var authority types.PushBatchDeliveryAuthority
	err := tx.QueryRow(ctx, `
		SELECT batch_status,batch_authority
		  FROM lock_push_effect_batch_v1($1,$2,$3,$4,$5)`,
		scope.BatchID,
		scope.TenantID,
		scope.UserID,
		runSnapshotID,
		string(requiredAuthority),
	).Scan(&status, &authority)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", pushEffectConflict("push effect batch is unavailable")
	}
	if err != nil {
		return "", pushEffectDatabaseError("lock push effect batch row", err)
	}
	if requiredAuthority != "" && requiredAuthority != "*" &&
		authority != requiredAuthority {
		return "", pushEffectConflict("push effect batch authority differs")
	}
	if requiredAuthority != "" && !authority.Valid() {
		return "", pushEffectConflict("push effect batch authority is unavailable")
	}
	return status, nil
}
