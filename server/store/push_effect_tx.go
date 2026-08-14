package store

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/server/types"
)

const pushEffectRollbackTimeout = 2 * time.Second

func (s *Store) beginPushEffectCoordinatorTx(
	ctx context.Context,
	tenantID int64,
) (pgx.Tx, error) {
	return s.beginPushEffectRoleTx(ctx, tenantID, "vane_push_effect_coordinator")
}

func (s *Store) beginPushEffectReceiptTx(
	ctx context.Context,
	tenantID int64,
) (pgx.Tx, error) {
	return s.beginPushEffectRoleTx(ctx, tenantID, "vane_push_effect_receipt")
}

func (s *Store) beginPushEffectOperatorTx(
	ctx context.Context,
	tenantID int64,
) (pgx.Tx, error) {
	return s.beginPushEffectRoleTx(ctx, tenantID, "vane_push_effect_operator")
}

func (s *Store) beginPushEffectRoleTx(
	ctx context.Context,
	tenantID int64,
	role string,
) (pgx.Tx, error) {
	if tenantID <= 0 {
		return nil, fmt.Errorf("begin push effect transaction: tenant id is not positive")
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin push effect transaction: %w", err)
	}
	if err := lockPushEffectSchemaWriter(ctx, tx); err != nil {
		rollbackPushEffectTx(ctx, tx)
		return nil, fmt.Errorf("lock push effect schema admission: %w", err)
	}
	if _, err := tx.Exec(ctx, `SELECT set_config('app.tenant_id',$1,true)`,
		strconv.FormatInt(tenantID, 10)); err != nil {
		rollbackPushEffectTx(ctx, tx)
		return nil, fmt.Errorf("set push effect tenant context: %w", err)
	}
	tenantExists, err := lockTenantAdmissionRoot(ctx, tx, tenantID)
	if err != nil {
		rollbackPushEffectTx(ctx, tx)
		return nil, fmt.Errorf("lock push effect tenant admission: %w", err)
	}
	if !tenantExists {
		rollbackPushEffectTx(ctx, tx)
		return nil, types.ErrNotFound
	}
	var setRole string
	switch role {
	case "vane_push_effect_coordinator":
		setRole = `SET LOCAL ROLE vane_push_effect_coordinator`
	case "vane_push_effect_receipt":
		setRole = `SET LOCAL ROLE vane_push_effect_receipt`
	case "vane_push_effect_operator":
		setRole = `SET LOCAL ROLE vane_push_effect_operator`
	default:
		rollbackPushEffectTx(ctx, tx)
		return nil, fmt.Errorf("set push effect role: unknown role")
	}
	if _, err := tx.Exec(ctx, setRole); err != nil {
		rollbackPushEffectTx(ctx, tx)
		return nil, fmt.Errorf("set push effect role: %w", err)
	}
	return tx, nil
}

func rollbackPushEffectTx(parent context.Context, tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(
		context.WithoutCancel(parent), pushEffectRollbackTimeout)
	defer cancel()
	_ = tx.Rollback(ctx)
}
