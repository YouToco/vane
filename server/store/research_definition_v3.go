package store

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/types"
)

// HasCurrentResearchApprovedDefinitionV3 is the shadow/authority preflight.
// It verifies the current immutable head and canonical V3 payload without
// exposing the task manual across the scheduler boundary.
func (s *Store) HasCurrentResearchApprovedDefinitionV3(
	ctx context.Context,
	tenantID, userID int64,
	taskID string,
) (bool, error) {
	if err := validateTaskStateScope(tenantID, userID, taskID); err != nil {
		return false, err
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return false, taskStateDatabaseError("begin research V3 preflight", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := bindResearchV3AppScopeTx(ctx, tx, tenantID, userID); err != nil {
		return false, err
	}
	head, err := loadPreparedResearchV3HeadTx(ctx, tx, tenantID, userID, taskID, false)
	if types.CodeOf(err) == types.CodeNotFound || errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, taskStateDatabaseError("commit research V3 preflight", err)
	}
	return head.Version > 0, nil
}
