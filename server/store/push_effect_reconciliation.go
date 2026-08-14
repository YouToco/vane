package store

import (
	"context"

	"github.com/YouToco/vane/server/pusheffect"
)

// DeferOrBlockPushEffectReconciliation makes one database-clock decision for
// an exact ambiguous fence. The SQL primitive fixes both failure classes:
// provider_history_inconclusive while open, provider_window_expired at expiry.
func (s *Store) DeferOrBlockPushEffectReconciliation(
	ctx context.Context,
	schedule pusheffect.ReconciliationSchedule,
) (pusheffect.ReconciliationDecision, error) {
	if err := validatePushEffectScope(schedule.Scope); err != nil ||
		schedule.ExpectedFence <= 0 ||
		schedule.RetryAfter <= 0 ||
		schedule.RetryAfter > maxPushEffectRetryWindow ||
		schedule.RetryAfter.Microseconds() <= 0 {
		return "", pushEffectValidation(
			"push effect reconciliation schedule is invalid")
	}
	tx, err := s.beginPushEffectCoordinatorTx(ctx, schedule.TenantID)
	if err != nil {
		return "", pushEffectDatabaseError(
			"begin reconciliation schedule transaction", err)
	}
	defer rollbackPushEffectTx(ctx, tx)
	if err := lockPushEffectBatchForScope(ctx, tx, schedule.Scope); err != nil {
		return "", err
	}
	var raw string
	if err := tx.QueryRow(ctx, `
		SELECT defer_or_block_push_effect_reconciliation_v1(
			$1,$2,$3,$4,$5,$6
		)`,
		schedule.ID, schedule.TenantID, schedule.UserID,
		schedule.ExpectedFence, schedule.RetryAfter.Microseconds(),
		schedule.UntilExpiry,
	).Scan(&raw); err != nil {
		return "", pushEffectDatabaseError(
			"defer or block push effect reconciliation", err)
	}
	decision := pusheffect.ReconciliationDecision(raw)
	if decision != pusheffect.ReconciliationDeferred &&
		decision != pusheffect.ReconciliationBlocked {
		return "", pushEffectLeaseLost()
	}
	if err := tx.Commit(ctx); err != nil {
		return "", pushEffectDatabaseError(
			"commit reconciliation schedule transaction", err)
	}
	return decision, nil
}

// BlockConflictingPushEffectHistory applies the only automatic history block:
// multiple exact positive matches for the same frozen effect.
func (s *Store) BlockConflictingPushEffectHistory(
	ctx context.Context,
	resolution pusheffect.HistoryResolution,
) error {
	if err := validatePushEffectScope(resolution.Scope); err != nil ||
		resolution.ExpectedFence <= 0 {
		return pushEffectValidation(
			"push effect history resolution is invalid")
	}
	tx, err := s.beginPushEffectCoordinatorTx(ctx, resolution.TenantID)
	if err != nil {
		return pushEffectDatabaseError(
			"begin history conflict transaction", err)
	}
	defer rollbackPushEffectTx(ctx, tx)
	if err := lockPushEffectBatchForScope(ctx, tx, resolution.Scope); err != nil {
		return err
	}
	var changed bool
	if err := tx.QueryRow(ctx, `
		SELECT block_conflicting_push_effect_history_v1($1,$2,$3,$4)`,
		resolution.ID, resolution.TenantID, resolution.UserID,
		resolution.ExpectedFence,
	).Scan(&changed); err != nil {
		return pushEffectDatabaseError("block conflicting push effect history", err)
	}
	if !changed {
		return pushEffectLeaseLost()
	}
	if err := tx.Commit(ctx); err != nil {
		return pushEffectDatabaseError(
			"commit history conflict transaction", err)
	}
	return nil
}

// BlockExhaustedPushEffectAttempts terminally removes a deterministic
// prepared/definite-failed row from the due set once its exact-task attempt
// budget is exhausted. Provider-ambiguous rows are deliberately ineligible.
func (s *Store) BlockExhaustedPushEffectAttempts(
	ctx context.Context,
	resolution pusheffect.ExhaustedResolution,
) error {
	if err := validatePushEffectScope(resolution.Scope); err != nil ||
		resolution.ExpectedFence <= 0 ||
		resolution.ExpectedTaskID == "" {
		return pushEffectValidation(
			"push effect exhausted resolution is invalid")
	}
	tx, err := s.beginPushEffectCoordinatorTx(ctx, resolution.TenantID)
	if err != nil {
		return pushEffectDatabaseError(
			"begin exhausted resolution transaction", err)
	}
	defer rollbackPushEffectTx(ctx, tx)
	if err := lockPushEffectBatchForScope(ctx, tx, resolution.Scope); err != nil {
		return err
	}
	var changed bool
	if err := tx.QueryRow(ctx, `
		SELECT block_exhausted_push_effect_attempts_v1(
			$1,$2,$3,$4,$5
		)`,
		resolution.ID, resolution.TenantID, resolution.UserID,
		resolution.ExpectedFence, resolution.ExpectedTaskID,
	).Scan(&changed); err != nil {
		return pushEffectDatabaseError("block exhausted push effect", err)
	}
	if !changed {
		return pushEffectLeaseLost()
	}
	if err := tx.Commit(ctx); err != nil {
		return pushEffectDatabaseError(
			"commit exhausted resolution transaction", err)
	}
	return nil
}
