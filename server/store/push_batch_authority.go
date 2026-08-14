package store

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/server/types"
)

// pushEffectSchemaFenceKey is the stable, process-independent admission fence
// for transactions that can race push-effect schema evolution. Writers take a
// shared transaction lock before touching the schema; migrations take the
// corresponding exclusive transaction lock before any table lock or DDL.
const pushEffectSchemaFenceKey int64 = 6215335020355474248 // "VANEPUSH"

// ClaimPushBatchDeliveryAuthority atomically elects the one provider protocol
// allowed to deliver a batch. A losing caller observes the durable winner; it
// never rewrites it.
func (s *Store) ClaimPushBatchDeliveryAuthority(
	ctx context.Context,
	scope types.PushBatchScope,
	desired types.PushBatchDeliveryAuthority,
) (types.PushBatchDeliveryAuthority, error) {
	if scope.TenantID <= 0 || scope.UserID <= 0 || scope.BatchID <= 0 ||
		!desired.Valid() {
		return "", types.NewAppError(
			types.CodeValidation,
			"push batch delivery authority scope is invalid",
			nil,
		)
	}
	tx, err := s.beginPushBatchAuthorityTx(ctx, scope.TenantID)
	if err != nil {
		if errors.Is(err, types.ErrNotFound) {
			return "", types.NewAppError(
				types.CodeNotFound,
				"push batch delivery authority target is unavailable",
				nil,
			)
		}
		return "", pushBatchAuthorityDatabaseError(
			"begin authority transaction",
			err,
		)
	}
	defer rollbackPushEffectTx(ctx, tx)

	var winner types.PushBatchDeliveryAuthority
	err = tx.QueryRow(ctx, `
		UPDATE push_batches
		   SET delivery_authority=COALESCE(delivery_authority,$4)
		 WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		 RETURNING delivery_authority`,
		scope.BatchID,
		scope.TenantID,
		scope.UserID,
		desired,
	).Scan(&winner)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", types.NewAppError(
			types.CodeNotFound,
			"push batch delivery authority target is unavailable",
			nil,
		)
	}
	if err != nil {
		return "", pushBatchAuthorityDatabaseError(
			"claim delivery authority",
			err,
		)
	}
	if !winner.Valid() {
		return "", types.NewAppError(
			types.CodeInternal,
			"push batch delivery authority is invalid",
			nil,
		)
	}
	if err := tx.Commit(ctx); err != nil {
		return "", pushBatchAuthorityDatabaseError(
			"commit authority transaction",
			err,
		)
	}
	return winner, nil
}

func (s *Store) beginPushBatchAuthorityTx(
	ctx context.Context,
	tenantID int64,
) (pgx.Tx, error) {
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	if err := lockPushEffectSchemaWriter(ctx, tx); err != nil {
		rollbackPushEffectTx(ctx, tx)
		return nil, fmt.Errorf("lock push effect schema admission: %w", err)
	}
	if _, err := tx.Exec(
		ctx,
		`SELECT set_config('app.tenant_id',$1,true)`,
		strconv.FormatInt(tenantID, 10),
	); err != nil {
		rollbackPushEffectTx(ctx, tx)
		return nil, fmt.Errorf("set tenant context: %w", err)
	}
	tenantExists, err := lockTenantAdmissionRoot(ctx, tx, tenantID)
	if err != nil {
		rollbackPushEffectTx(ctx, tx)
		return nil, fmt.Errorf("lock tenant admission: %w", err)
	}
	if !tenantExists {
		rollbackPushEffectTx(ctx, tx)
		return nil, types.ErrNotFound
	}
	if _, err := tx.Exec(
		ctx,
		`SET LOCAL ROLE vane_push_batch_authority`,
	); err != nil {
		rollbackPushEffectTx(ctx, tx)
		return nil, fmt.Errorf("set authority role: %w", err)
	}
	return tx, nil
}

func lockPushEffectSchemaWriter(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(
		ctx,
		`SELECT pg_advisory_xact_lock_shared($1)`,
		pushEffectSchemaFenceKey,
	)
	return err
}

func pushBatchAuthorityDatabaseError(action string, cause error) error {
	if !errors.Is(cause, context.Canceled) &&
		!errors.Is(cause, context.DeadlineExceeded) {
		cause = errors.New("database operation failed")
	}
	return types.NewAppError(
		types.CodeDatabase,
		"push batch authority: "+action,
		cause,
	)
}
