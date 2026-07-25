package store

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
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
	var setRole string
	switch role {
	case "vane_push_effect_coordinator":
		setRole = `SET LOCAL ROLE vane_push_effect_coordinator`
	case "vane_push_effect_receipt":
		setRole = `SET LOCAL ROLE vane_push_effect_receipt`
	case "vane_push_effect_operator":
		setRole = `SET LOCAL ROLE vane_push_effect_operator`
	default:
		return nil, fmt.Errorf("set push effect role: unknown role")
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin push effect transaction: %w", err)
	}
	if err := lockPushEffectSchemaWriter(ctx, tx); err != nil {
		rollbackPushEffectTx(ctx, tx)
		return nil, fmt.Errorf("lock push effect schema admission: %w", err)
	}
	// Check the exact membership edge rather than pg_has_role. A database
	// superuser can SET ROLE without membership, but must not bypass the durable
	// 047 live-protocol admission boundary.
	var admitted bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			  FROM pg_auth_members am
			  JOIN pg_roles granted ON granted.oid=am.roleid
			  JOIN pg_roles member ON member.oid=am.member
			 WHERE granted.rolname=$1
			   AND member.rolname=current_user
		)`, role,
	).Scan(&admitted); err != nil {
		rollbackPushEffectTx(ctx, tx)
		return nil, fmt.Errorf("check push effect role admission: %w", err)
	}
	if !admitted {
		rollbackPushEffectTx(ctx, tx)
		return nil, fmt.Errorf("push effect runtime role is not admitted")
	}
	if _, err := tx.Exec(ctx, `SELECT set_config('app.tenant_id',$1,true)`,
		strconv.FormatInt(tenantID, 10)); err != nil {
		rollbackPushEffectTx(ctx, tx)
		return nil, fmt.Errorf("set push effect tenant context: %w", err)
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
