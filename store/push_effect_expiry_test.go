package store

import (
	"sync"
	"testing"
	"time"

	"github.com/YouToco/vane/pusheffect"
)

func TestBlockExpiredUnclaimedPushEffectPreparedAndReplay(t *testing.T) {
	f, effect := shortWindowPreparedPushEffect(t)
	resolution := pusheffect.ExpiryResolution{
		Scope: effect.Scope(), ExpectedFence: effect.Fence,
		ExpectedTaskID: effect.TaskID, RequiredWindow: time.Minute,
	}
	blocked, err := f.store.BlockExpiredUnclaimedPushEffect(
		t.Context(), resolution)
	if err != nil || !blocked {
		t.Fatalf("first block=%v/%v", blocked, err)
	}
	blocked, err = f.store.BlockExpiredUnclaimedPushEffect(
		t.Context(), resolution)
	if err != nil || !blocked {
		t.Fatalf("block replay=%v/%v", blocked, err)
	}
	var (
		status, class string
		fence         int64
		attempt       int
		blockedAt     *time.Time
	)
	if err := f.db.QueryRowContext(t.Context(), `
		SELECT status,failure_class,fence,attempt,blocked_at
		  FROM push_effects WHERE id=$1`,
		effect.ID,
	).Scan(&status, &class, &fence, &attempt, &blockedAt); err != nil {
		t.Fatal(err)
	}
	if status != string(pusheffect.StatusBlocked) ||
		class != "provider_window_expired_no_send" ||
		fence != 1 || attempt != 1 || blockedAt == nil {
		t.Fatalf("blocked row=%q/%q fence=%d attempt=%d at=%v",
			status, class, fence, attempt, blockedAt)
	}
}

func TestBlockExpiredUnclaimedPushEffectDefiniteFailure(t *testing.T) {
	f, effect := shortWindowPreparedPushEffect(t)
	claimed, err := f.store.ClaimPushEffect(t.Context(), pusheffect.ClaimParams{
		Scope: effect.Scope(), LeaseOwner: "expiry-definite",
		LeaseDuration: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.store.RecordPushEffectDefiniteFailure(
		t.Context(),
		pusheffect.FailureParams{
			Lease: pusheffect.Lease{
				Scope: claimed.Scope(), LeaseOwner: claimed.LeaseOwner,
				Fence: claimed.Fence,
			},
			Class: "provider_definite_rejection", RetryAfter: time.Second,
		},
	); err != nil {
		t.Fatal(err)
	}
	blocked, err := f.store.BlockExpiredUnclaimedPushEffect(
		t.Context(),
		pusheffect.ExpiryResolution{
			Scope: claimed.Scope(), ExpectedFence: claimed.Fence,
			ExpectedTaskID: claimed.TaskID, RequiredWindow: time.Minute,
		},
	)
	if err != nil || !blocked {
		t.Fatalf("definite block=%v/%v", blocked, err)
	}
	var status, class string
	if err := f.db.QueryRowContext(t.Context(), `
		SELECT status,failure_class FROM push_effects WHERE id=$1`,
		effect.ID,
	).Scan(&status, &class); err != nil {
		t.Fatal(err)
	}
	if status != string(pusheffect.StatusBlocked) ||
		class != "provider_window_expired_no_send" {
		t.Fatalf("definite terminal=%q/%q", status, class)
	}
}

func TestBlockExpiredUnclaimedPushEffectAndClaimHaveOneWinner(t *testing.T) {
	f, effect := authorizedPushEffectFixtureWithExpiry(t, "", 10*time.Second)
	start := make(chan struct{})
	var (
		wg                 sync.WaitGroup
		blocked            bool
		blockErr, claimErr error
		claimed            *pusheffect.Effect
		claimDecision      pusheffect.AuthorizedClaimDecision
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		blocked, blockErr = f.st.BlockExpiredUnclaimedPushEffect(
			t.Context(),
			pusheffect.ExpiryResolution{
				Scope: effect.Scope(), ExpectedFence: effect.Fence,
				ExpectedTaskID: effect.TaskID, RequiredWindow: time.Minute,
			},
		)
	}()
	go func() {
		defer wg.Done()
		<-start
		claimed, claimDecision, claimErr = f.st.ClaimAuthorizedPushEffect(
			t.Context(),
			pusheffect.AuthorizedClaimParams{
				ClaimParams: pusheffect.ClaimParams{
					Scope: effect.Scope(), LeaseOwner: "expiry-race",
					LeaseDuration: time.Minute,
				},
				ExpectedTaskID:   effect.TaskID,
				DenialRetryAfter: time.Minute,
			},
		)
	}()
	close(start)
	wg.Wait()
	if blockErr != nil || !blocked {
		t.Fatalf("expiry race block=%v/%v", blocked, blockErr)
	}
	if claimErr == nil || claimed != nil || claimDecision != "" {
		t.Fatalf("expiry race claim=%+v/%q/%v",
			claimed, claimDecision, claimErr)
	}
	var status string
	if err := f.st.pool.QueryRow(t.Context(),
		`SELECT status FROM push_effects WHERE id=$1`, effect.ID,
	).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != string(pusheffect.StatusBlocked) {
		t.Fatalf("expiry race final status=%q", status)
	}
}

func TestBlockExpiredUnclaimedPushEffectKeepsOpenWindow(t *testing.T) {
	f := newPushEffectFixtureAt(t, 51)
	effect, err := f.store.CreatePushEffect(t.Context(), f.prepared)
	if err != nil {
		t.Fatal(err)
	}
	blocked, err := f.store.BlockExpiredUnclaimedPushEffect(
		t.Context(),
		pusheffect.ExpiryResolution{
			Scope: effect.Scope(), ExpectedFence: effect.Fence,
			ExpectedTaskID: effect.TaskID, RequiredWindow: time.Minute,
		},
	)
	if err != nil || blocked {
		t.Fatalf("open window block=%v/%v", blocked, err)
	}
	unchanged, err := f.store.LoadPushEffect(t.Context(), effect.Scope())
	if err != nil {
		t.Fatal(err)
	}
	if unchanged.Status != pusheffect.StatusPrepared ||
		unchanged.Fence != 0 || unchanged.Attempt != 0 {
		t.Fatalf("open window mutated effect=%+v", unchanged)
	}
}

func TestBlockExpiredUnclaimedPushEffectRejectsOtherStates(t *testing.T) {
	tests := []struct {
		name   string
		status pusheffect.Status
	}{
		{name: "sending", status: pusheffect.StatusSending},
		{name: "ambiguous", status: pusheffect.StatusAmbiguous},
		{name: "sent", status: pusheffect.StatusSent},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := newPushEffectFixtureAt(t, 51)
			effect, err := f.store.CreatePushEffect(t.Context(), f.prepared)
			if err != nil {
				t.Fatal(err)
			}
			claimed, err := f.store.ClaimPushEffect(
				t.Context(),
				pusheffect.ClaimParams{
					Scope: effect.Scope(), LeaseOwner: "expiry-other-state",
					LeaseDuration: time.Minute,
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			switch test.status {
			case pusheffect.StatusSending:
				// The claim already materialized this state.
			case pusheffect.StatusAmbiguous:
				if err := f.store.RecordPushEffectAmbiguous(
					t.Context(),
					pusheffect.FailureParams{
						Lease: pusheffect.Lease{
							Scope:      claimed.Scope(),
							LeaseOwner: claimed.LeaseOwner,
							Fence:      claimed.Fence,
						},
						Class: "provider_response_unknown",
					},
				); err != nil {
					t.Fatal(err)
				}
			case pusheffect.StatusSent:
				if _, err := f.db.ExecContext(t.Context(), `
					UPDATE push_effects
					   SET status='sent',lease_owner='',lease_until=NULL,
					       takeover_not_before=NULL,failure_class='',
					       ambiguous_since=NULL,sent_at=clock_timestamp(),
					       provider_message_id='om_expiry_other',
					       updated_at=clock_timestamp()
					 WHERE id=$1`,
					effect.ID,
				); err != nil {
					t.Fatal(err)
				}
			}
			blocked, err := f.store.BlockExpiredUnclaimedPushEffect(
				t.Context(),
				pusheffect.ExpiryResolution{
					Scope: effect.Scope(), ExpectedFence: claimed.Fence,
					ExpectedTaskID: effect.TaskID,
					RequiredWindow: time.Hour,
				},
			)
			if err == nil || blocked {
				t.Fatalf("%s block=%v/%v", test.status, blocked, err)
			}
			var status string
			if err := f.db.QueryRowContext(t.Context(),
				`SELECT status FROM push_effects WHERE id=$1`, effect.ID,
			).Scan(&status); err != nil {
				t.Fatal(err)
			}
			if status != string(test.status) {
				t.Fatalf("state changed=%q want %q", status, test.status)
			}
		})
	}
}

func shortWindowPreparedPushEffect(
	t *testing.T,
) (pushEffectFixture, *pusheffect.Effect) {
	t.Helper()
	f := newPushEffectFixtureAt(t, 51)
	f.prepared.IdempotencyExpiresAt = time.Now().UTC().
		Truncate(time.Microsecond).Add(10 * time.Second)
	effect, err := f.store.CreatePushEffect(t.Context(), f.prepared)
	if err != nil {
		t.Fatal(err)
	}
	return f, effect
}
