package store

import (
	"testing"
	"time"

	"github.com/YouToco/vane/pusheffect"
)

func TestDeferOrBlockPushEffectReconciliationUsesOneDatabaseClockDecision(
	t *testing.T,
) {
	t.Run("open window defers ambiguous effect", func(t *testing.T) {
		f, effect := ambiguousPushEffectFixture(t)
		const retryAfter = 2 * time.Minute
		var before time.Time
		if err := f.db.QueryRowContext(
			t.Context(), `SELECT clock_timestamp()`,
		).Scan(&before); err != nil {
			t.Fatal(err)
		}
		decision, err := f.store.DeferOrBlockPushEffectReconciliation(
			t.Context(),
			pusheffect.ReconciliationSchedule{
				Scope: effect.Scope(), ExpectedFence: effect.Fence,
				RetryAfter: retryAfter,
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		if decision != pusheffect.ReconciliationDeferred {
			t.Fatalf("decision=%q, want deferred", decision)
		}
		var (
			status, class string
			nextAttempt   time.Time
			blockedAt     *time.Time
			after         time.Time
		)
		if err := f.db.QueryRowContext(t.Context(), `
			SELECT status,failure_class,next_attempt_at,blocked_at,
			       clock_timestamp()
			  FROM push_effects WHERE id=$1`,
			effect.ID,
		).Scan(&status, &class, &nextAttempt, &blockedAt, &after); err != nil {
			t.Fatal(err)
		}
		if status != string(pusheffect.StatusAmbiguous) ||
			class != "provider_history_inconclusive" ||
			blockedAt != nil {
			t.Fatalf("deferred row=%q/%q/blocked:%v", status, class, blockedAt)
		}
		if nextAttempt.Before(before.Add(retryAfter)) ||
			nextAttempt.After(after.Add(retryAfter)) {
			t.Fatalf(
				"next_attempt_at=%s outside database-clock bounds [%s,%s]",
				nextAttempt, before.Add(retryAfter), after.Add(retryAfter),
			)
		}
	})

	t.Run("expired window blocks ambiguous effect", func(t *testing.T) {
		f, effect := ambiguousPushEffectFixture(t)
		if _, err := f.db.ExecContext(t.Context(), `
			UPDATE push_effects
			   SET created_at=clock_timestamp()-interval '2 hours',
			       idempotency_expires_at=
			           clock_timestamp()-interval '90 minutes'
			 WHERE id=$1`,
			effect.ID,
		); err != nil {
			t.Fatal(err)
		}
		decision, err := f.store.DeferOrBlockPushEffectReconciliation(
			t.Context(),
			pusheffect.ReconciliationSchedule{
				Scope: effect.Scope(), ExpectedFence: effect.Fence,
				RetryAfter: time.Minute,
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		if decision != pusheffect.ReconciliationBlocked {
			t.Fatalf("decision=%q, want blocked", decision)
		}
		var (
			status, class string
			blockedAt     *time.Time
		)
		if err := f.db.QueryRowContext(t.Context(), `
			SELECT status,failure_class,blocked_at
			  FROM push_effects WHERE id=$1`,
			effect.ID,
		).Scan(&status, &class, &blockedAt); err != nil {
			t.Fatal(err)
		}
		if status != string(pusheffect.StatusBlocked) ||
			class != "provider_window_expired" || blockedAt == nil {
			t.Fatalf("blocked row=%q/%q/blocked:%v", status, class, blockedAt)
		}
	})

	t.Run("sub-microsecond retry is rejected before SQL", func(t *testing.T) {
		f, effect := ambiguousPushEffectFixture(t)
		_, err := f.store.DeferOrBlockPushEffectReconciliation(
			t.Context(),
			pusheffect.ReconciliationSchedule{
				Scope: effect.Scope(), ExpectedFence: effect.Fence,
				RetryAfter: time.Nanosecond,
			},
		)
		if err == nil {
			t.Fatal("sub-microsecond reconciliation retry was accepted")
		}
	})
}

func TestBlockExhaustedPushEffectAttemptsUsesFixedThresholdAndState(t *testing.T) {
	t.Run("attempt seven is not exhausted", func(t *testing.T) {
		f, effect := deterministicFailedPushEffectFixture(t, 7)
		err := f.store.BlockExhaustedPushEffectAttempts(
			t.Context(),
			pusheffect.ExhaustedResolution{
				Scope: effect.Scope(), ExpectedFence: effect.Fence,
				ExpectedTaskID: effect.TaskID,
			},
		)
		if err == nil {
			t.Fatal("attempt seven was blocked")
		}
	})

	t.Run("attempt eight is blocked", func(t *testing.T) {
		f, effect := deterministicFailedPushEffectFixture(
			t, pusheffect.RecoveryMaxAttempts)
		if err := f.store.BlockExhaustedPushEffectAttempts(
			t.Context(),
			pusheffect.ExhaustedResolution{
				Scope: effect.Scope(), ExpectedFence: effect.Fence,
				ExpectedTaskID: effect.TaskID,
			},
		); err != nil {
			t.Fatal(err)
		}
		var status, class string
		if err := f.db.QueryRowContext(t.Context(), `
			SELECT status,failure_class FROM push_effects WHERE id=$1`,
			effect.ID,
		).Scan(&status, &class); err != nil {
			t.Fatal(err)
		}
		if status != string(pusheffect.StatusBlocked) ||
			class != "attempt_budget_exhausted" {
			t.Fatalf("exhausted row=%q/%q", status, class)
		}
	})

	t.Run("ambiguous state is never deterministically exhausted", func(t *testing.T) {
		f, effect := ambiguousPushEffectFixture(t)
		if _, err := f.db.ExecContext(t.Context(), `
			UPDATE push_effects SET attempt=$2 WHERE id=$1`,
			effect.ID, pusheffect.RecoveryMaxAttempts,
		); err != nil {
			t.Fatal(err)
		}
		effect.Attempt = pusheffect.RecoveryMaxAttempts
		err := f.store.BlockExhaustedPushEffectAttempts(
			t.Context(),
			pusheffect.ExhaustedResolution{
				Scope: effect.Scope(), ExpectedFence: effect.Fence,
				ExpectedTaskID: effect.TaskID,
			},
		)
		if err == nil {
			t.Fatal("ambiguous provider outcome was deterministically blocked")
		}
	})
}

func TestBlockConflictingPushEffectHistoryUsesFixedTransitionAndReplay(
	t *testing.T,
) {
	f, effect := ambiguousPushEffectFixture(t)
	wrong := pusheffect.HistoryResolution{
		Scope: effect.Scope(), ExpectedFence: effect.Fence + 1,
	}
	if err := f.store.BlockConflictingPushEffectHistory(
		t.Context(), wrong,
	); err == nil {
		t.Fatal("wrong history fence was accepted")
	}
	unchanged, err := f.store.LoadPushEffect(t.Context(), effect.Scope())
	if err != nil {
		t.Fatal(err)
	}
	if unchanged.Status != pusheffect.StatusAmbiguous ||
		unchanged.Fence != effect.Fence {
		t.Fatalf("wrong fence mutated effect=%+v", unchanged)
	}
	exact := pusheffect.HistoryResolution{
		Scope: effect.Scope(), ExpectedFence: effect.Fence,
	}
	if err := f.store.BlockConflictingPushEffectHistory(
		t.Context(), exact,
	); err != nil {
		t.Fatal(err)
	}
	if err := f.store.BlockConflictingPushEffectHistory(
		t.Context(), exact,
	); err != nil {
		t.Fatalf("exact history block replay: %v", err)
	}
	var (
		status, class string
		blockedAt     *time.Time
	)
	if err := f.db.QueryRowContext(t.Context(), `
		SELECT status,failure_class,blocked_at
		  FROM push_effects WHERE id=$1`,
		effect.ID,
	).Scan(&status, &class, &blockedAt); err != nil {
		t.Fatal(err)
	}
	if status != string(pusheffect.StatusBlocked) ||
		class != "provider_history_conflict" || blockedAt == nil {
		t.Fatalf("history block=%q/%q/blocked:%v", status, class, blockedAt)
	}
}

func deterministicFailedPushEffectFixture(
	t *testing.T,
	attempt int,
) (pushEffectFixture, *pusheffect.Effect) {
	t.Helper()
	f, effect := ambiguousPushEffectFixture(t)
	if _, err := f.db.ExecContext(t.Context(), `
		UPDATE push_effects
		   SET status='definite_failed', attempt=$2,
		       failure_class='provider_definite_rejection',
		       ambiguous_since=NULL
		 WHERE id=$1`,
		effect.ID, attempt,
	); err != nil {
		t.Fatal(err)
	}
	effect, err := f.store.LoadPushEffect(t.Context(), effect.Scope())
	if err != nil {
		t.Fatal(err)
	}
	return f, effect
}

func ambiguousPushEffectFixture(
	t *testing.T,
) (pushEffectFixture, *pusheffect.Effect) {
	t.Helper()
	f := newPushEffectFixtureAt(t, 49)
	effect, err := f.store.CreatePushEffect(t.Context(), f.prepared)
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := f.store.ClaimPushEffect(
		t.Context(),
		pusheffect.ClaimParams{
			Scope: effect.Scope(), LeaseOwner: "reconciliation-test",
			LeaseDuration: time.Minute,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.store.RecordPushEffectAmbiguous(
		t.Context(),
		pusheffect.FailureParams{
			Lease: pusheffect.Lease{
				Scope: claimed.Scope(), LeaseOwner: claimed.LeaseOwner,
				Fence: claimed.Fence,
			},
			Class: "provider_response_unknown",
		},
	); err != nil {
		t.Fatal(err)
	}
	ambiguous, err := f.store.LoadPushEffect(t.Context(), effect.Scope())
	if err != nil {
		t.Fatal(err)
	}
	return f, ambiguous
}
