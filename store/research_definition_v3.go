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
	head, err := loadPreparedResearchV3HeadTx(ctx, s.pool, tenantID, userID, taskID, false)
	if types.CodeOf(err) == types.CodeNotFound || errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return head.Version > 0, nil
}
