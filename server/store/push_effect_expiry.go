package store

import (
	"context"

	"github.com/YouToco/vane/pusheffect"
)

// BlockExpiredUnclaimedPushEffect makes one database-clock decision for an
// exact prepared/definite-failed fence. It returns false without mutation while
// a complete provider window still fits, and terminally blocks once it cannot.
func (s *Store) BlockExpiredUnclaimedPushEffect(
	ctx context.Context,
	resolution pusheffect.ExpiryResolution,
) (bool, error) {
	if err := validatePushEffectScope(resolution.Scope); err != nil ||
		resolution.ExpectedFence < 0 ||
		resolution.ExpectedTaskID == "" ||
		resolution.RequiredWindow <= 0 ||
		resolution.RequiredWindow > maxPushEffectLease ||
		resolution.RequiredWindow.Microseconds() <= 0 {
		return false, pushEffectValidation(
			"push effect expiry resolution is invalid")
	}
	tx, err := s.beginPushEffectCoordinatorTx(ctx, resolution.TenantID)
	if err != nil {
		return false, pushEffectDatabaseError(
			"begin expired effect resolution transaction", err)
	}
	defer rollbackPushEffectTx(ctx, tx)
	if err := lockPushEffectBatchForScope(ctx, tx, resolution.Scope); err != nil {
		return false, err
	}
	var raw string
	if err := tx.QueryRow(ctx, `
		SELECT block_expired_unclaimed_push_effect_v1(
			$1,$2,$3,$4,$5,$6
		)`,
		resolution.ID, resolution.TenantID, resolution.UserID,
		resolution.ExpectedFence, resolution.ExpectedTaskID,
		resolution.RequiredWindow.Microseconds(),
	).Scan(&raw); err != nil {
		return false, pushEffectDatabaseError(
			"block expired unclaimed push effect", err)
	}
	switch raw {
	case "blocked":
		if err := tx.Commit(ctx); err != nil {
			return false, pushEffectDatabaseError(
				"commit expired effect resolution transaction", err)
		}
		return true, nil
	case "open":
		if err := tx.Commit(ctx); err != nil {
			return false, pushEffectDatabaseError(
				"commit open effect resolution transaction", err)
		}
		return false, nil
	default:
		return false, pushEffectLeaseLost()
	}
}
