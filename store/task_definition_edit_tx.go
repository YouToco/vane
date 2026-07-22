package store

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
)

const taskDefinitionEditRollbackTimeout = 2 * time.Second

// beginTaskDefinitionEditTx starts a tenant-scoped transaction under the
// restricted edit-coordinator role. Cross-tenant discovery is deliberately
// unavailable here; the separately audited owner READ ONLY due-index discovery
// exception does not use this helper.
func (s *Store) beginTaskDefinitionEditTx(
	ctx context.Context,
	tenantID int64,
) (pgx.Tx, error) {
	return s.beginTaskDefinitionEditRoleTx(ctx, tenantID, "vane_edit_coordinator")
}

// beginTaskDefinitionEditReceiptTx is the receipt-dispatch counterpart of
// beginTaskDefinitionEditTx. It cannot update operations, schedules, or
// Approved Definition state at the database privilege boundary.
func (s *Store) beginTaskDefinitionEditReceiptTx(
	ctx context.Context,
	tenantID int64,
) (pgx.Tx, error) {
	return s.beginTaskDefinitionEditRoleTx(ctx, tenantID, "vane_edit_receipt")
}

func (s *Store) beginTaskDefinitionEditRoleTx(
	ctx context.Context,
	tenantID int64,
	role string,
) (pgx.Tx, error) {
	if tenantID <= 0 {
		return nil, fmt.Errorf("begin task definition edit transaction: tenant id is not positive")
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin task definition edit transaction: %w", err)
	}

	tenantContext := strconv.FormatInt(tenantID, 10)
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.tenant_id', $1, true)`, tenantContext); err != nil {
		rollbackTaskDefinitionEditTx(ctx, tx)
		return nil, fmt.Errorf("set task definition edit tenant context: %w", err)
	}

	var setRoleSQL string
	switch role {
	case "vane_edit_coordinator":
		setRoleSQL = `SET LOCAL ROLE vane_edit_coordinator`
	case "vane_edit_receipt":
		setRoleSQL = `SET LOCAL ROLE vane_edit_receipt`
	default:
		rollbackTaskDefinitionEditTx(ctx, tx)
		return nil, fmt.Errorf("set task definition edit role: unknown role")
	}
	if _, err := tx.Exec(ctx, setRoleSQL); err != nil {
		rollbackTaskDefinitionEditTx(ctx, tx)
		return nil, fmt.Errorf("set task definition edit role: %w", err)
	}
	return tx, nil
}

func rollbackTaskDefinitionEditTx(parent context.Context, tx pgx.Tx) {
	rollbackCtx, cancel := context.WithTimeout(
		context.WithoutCancel(parent), taskDefinitionEditRollbackTimeout)
	defer cancel()
	_ = tx.Rollback(rollbackCtx)
}
