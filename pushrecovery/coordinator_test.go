package pushrecovery

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/pusheffect"
)

func TestCoordinatorPreparedEffectUsesAuthorizedClaimAndFrozenRequest(
	t *testing.T,
) {
	now := time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)
	effect := testEffect(now, pusheffect.StatusPrepared)
	store := newFakeStore(effect)
	provider := &fakeProvider{
		sendObservation: pusheffect.ProviderObservation{
			Disposition: pusheffect.AttemptSent,
			AppIdentity: effect.AppIdentity,
			MessageID:   "om_sent",
			ChatID:      effect.ProviderChatID,
		},
	}
	coordinator := newTestCoordinator(t, store, provider, now)

	outcome, err := coordinator.recoverEffect(t.Context(), effect)
	if err != nil || outcome != OutcomeSent {
		t.Fatalf("recover prepared effect = %q, err=%v", outcome, err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.authorizeCalls != 1 || store.freshClaimCalls != 1 ||
		store.reconciliationClaimCalls != 0 || store.sentCalls != 1 {
		t.Fatalf(
			"store calls authorize/fresh/reconcile/sent=%d/%d/%d/%d",
			store.authorizeCalls,
			store.freshClaimCalls,
			store.reconciliationClaimCalls,
			store.sentCalls,
		)
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	if provider.sendCalls != 1 ||
		provider.gotAppIdentity != effect.AppIdentity ||
		provider.gotTarget != effect.Target ||
		provider.gotCard != string(effect.Card) ||
		provider.gotUUID != effect.ProviderUUID {
		t.Fatalf("provider request drifted: %+v", provider)
	}
}

func TestCoordinatorDoesNotClaimWhenRunIsRevoked(t *testing.T) {
	now := time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)
	effect := testEffect(now, pusheffect.StatusDefiniteFailed)
	effect.Fence = 1
	effect.Attempt = 1
	store := newFakeStore(effect)
	store.authorized = false
	provider := &fakeProvider{}
	coordinator := newTestCoordinator(t, store, provider, now)

	outcome, err := coordinator.recoverEffect(t.Context(), effect)
	if err != nil || outcome != OutcomeNotAuthorized {
		t.Fatalf("recover revoked effect = %q, err=%v", outcome, err)
	}
	store.mu.Lock()
	if store.freshClaimCalls != 0 {
		t.Fatalf("revoked effect claims=%d, want 0", store.freshClaimCalls)
	}
	store.mu.Unlock()
	provider.mu.Lock()
	if provider.sendCalls != 0 {
		t.Fatalf("revoked effect sends=%d, want 0", provider.sendCalls)
	}
	provider.mu.Unlock()
}

func TestCoordinatorPersistsProviderBoundaryClassification(t *testing.T) {
	now := time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name          string
		observation   pusheffect.ProviderObservation
		sendErr       error
		wantOutcome   Outcome
		wantDefinite  int
		wantAmbiguous int
		wantSent      int
	}{
		{
			name: "explicit pre-boundary rejection",
			observation: pusheffect.ProviderObservation{
				Disposition: pusheffect.AttemptDefiniteNotSent,
				AppIdentity: "cli_push_recovery",
			},
			sendErr:      errors.New("provider rejected request"),
			wantOutcome:  OutcomeDefiniteFail,
			wantDefinite: 1,
		},
		{
			name: "transport response loss",
			observation: pusheffect.ProviderObservation{
				Disposition: pusheffect.AttemptAmbiguous,
				AppIdentity: "cli_push_recovery",
			},
			sendErr:       errors.New("provider response lost"),
			wantOutcome:   OutcomeAmbiguous,
			wantAmbiguous: 1,
		},
		{
			name: "sent-shaped observation with transport error",
			observation: pusheffect.ProviderObservation{
				Disposition: pusheffect.AttemptSent,
				AppIdentity: "cli_push_recovery",
				MessageID:   "om_untrusted",
				ChatID:      "oc_push_recovery",
			},
			sendErr:       errors.New("transport reported failure"),
			wantOutcome:   OutcomeAmbiguous,
			wantAmbiguous: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			effect := testEffect(now, pusheffect.StatusPrepared)
			store := newFakeStore(effect)
			provider := &fakeProvider{
				sendObservation: tt.observation,
				sendErr:         tt.sendErr,
			}
			coordinator := newTestCoordinator(t, store, provider, now)

			outcome, err := coordinator.recoverEffect(t.Context(), effect)
			if outcome != tt.wantOutcome || !errors.Is(err, tt.sendErr) {
				t.Fatalf("provider classification = %q, err=%v", outcome, err)
			}
			store.mu.Lock()
			defer store.mu.Unlock()
			if store.definiteFailureCalls != tt.wantDefinite ||
				store.ambiguousCalls != tt.wantAmbiguous ||
				store.sentCalls != tt.wantSent {
				t.Fatalf(
					"persisted definite/ambiguous/sent=%d/%d/%d, want %d/%d/%d",
					store.definiteFailureCalls,
					store.ambiguousCalls,
					store.sentCalls,
					tt.wantDefinite,
					tt.wantAmbiguous,
					tt.wantSent,
				)
			}
		})
	}
}

func TestCoordinatorAmbiguousPositiveHistoryNeverCreates(t *testing.T) {
	now := time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)
	effect := testEffect(now, pusheffect.StatusAmbiguous)
	effect.Fence = 3
	effect.Attempt = 2
	ambiguousSince := now.Add(-time.Minute)
	effect.AmbiguousSince = &ambiguousSince
	effect.FailureClass = "response_unknown"
	store := newFakeStore(effect)
	provider := &fakeProvider{
		historyObservation: pusheffect.HistoryObservation{
			MatchCount: 1,
			MessageID:  "om_history",
		},
	}
	coordinator := newTestCoordinator(t, store, provider, now)

	outcome, err := coordinator.recoverEffect(t.Context(), effect)
	if err != nil || outcome != OutcomeSent {
		t.Fatalf("recover history-positive effect = %q, err=%v", outcome, err)
	}
	store.mu.Lock()
	if store.authorizeCalls != 0 || store.reconciliationClaimCalls != 0 ||
		store.sentCalls != 1 {
		t.Fatalf(
			"history-positive authorize/reconcile/sent=%d/%d/%d",
			store.authorizeCalls,
			store.reconciliationClaimCalls,
			store.sentCalls,
		)
	}
	store.mu.Unlock()
	provider.mu.Lock()
	defer provider.mu.Unlock()
	if provider.sendCalls != 0 || provider.historyCalls != 1 {
		t.Fatalf(
			"history-positive provider send/history=%d/%d",
			provider.sendCalls,
			provider.historyCalls,
		)
	}
	if provider.historyQuery.CardDigest != effect.CardDigest ||
		provider.historyQuery.EffectID != effect.ID ||
		provider.historyQuery.ProviderChatID != effect.ProviderChatID ||
		provider.historyQuery.AppIdentity != effect.AppIdentity {
		t.Fatalf("history query lost frozen evidence: %+v", provider.historyQuery)
	}
}

func TestCoordinatorAmbiguousRetryNeverDowngradesToDefiniteFailure(
	t *testing.T,
) {
	now := time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)
	effect := testEffect(now, pusheffect.StatusAmbiguous)
	effect.Fence = 4
	effect.Attempt = 2
	ambiguousSince := now.Add(-time.Minute)
	effect.AmbiguousSince = &ambiguousSince
	effect.FailureClass = "provider_response_unknown"
	store := newFakeStore(effect)
	provider := &fakeProvider{
		sendObservation: pusheffect.ProviderObservation{
			Disposition: pusheffect.AttemptDefiniteNotSent,
			AppIdentity: effect.AppIdentity,
		},
		sendErr: errors.New("provider rejected reconciliation"),
	}
	coordinator := newTestCoordinator(t, store, provider, now)

	outcome, err := coordinator.recoverEffect(t.Context(), effect)
	if outcome != OutcomeAmbiguous ||
		!errors.Is(err, provider.sendErr) {
		t.Fatalf("ambiguous retry = %q, err=%v", outcome, err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.reconciliationClaimCalls != 1 ||
		store.ambiguousCalls != 1 ||
		store.definiteFailureCalls != 0 {
		t.Fatalf(
			"reconcile/ambiguous/definite=%d/%d/%d",
			store.reconciliationClaimCalls,
			store.ambiguousCalls,
			store.definiteFailureCalls,
		)
	}
}

func TestCoordinatorAmbiguousExpiryAndConflictBlockWithoutCreate(
	t *testing.T,
) {
	now := time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name       string
		matches    int
		expiry     time.Time
		wantMethod string
	}{
		{
			name:       "provider window expired",
			expiry:     now.Add(-time.Second),
			wantMethod: "expired",
		},
		{
			name:       "multiple exact history matches",
			matches:    2,
			expiry:     now.Add(time.Minute),
			wantMethod: "conflict",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			effect := testEffect(now, pusheffect.StatusAmbiguous)
			effect.Fence = 2
			effect.Attempt = 1
			effect.IdempotencyExpiresAt = tt.expiry
			ambiguousSince := now.Add(-time.Minute)
			effect.AmbiguousSince = &ambiguousSince
			effect.FailureClass = "response_unknown"
			store := newFakeStore(effect)
			provider := &fakeProvider{
				historyObservation: pusheffect.HistoryObservation{
					MatchCount: tt.matches,
				},
			}
			coordinator := newTestCoordinator(t, store, provider, now)

			outcome, err := coordinator.recoverEffect(t.Context(), effect)
			if err != nil || outcome != OutcomeBlocked {
				t.Fatalf("recover blocked effect = %q, err=%v", outcome, err)
			}
			store.mu.Lock()
			if tt.wantMethod == "expired" && store.expiredBlockCalls != 1 {
				t.Fatalf("expired blocks=%d, want 1", store.expiredBlockCalls)
			}
			if tt.wantMethod == "conflict" && store.conflictBlockCalls != 1 {
				t.Fatalf("conflict blocks=%d, want 1", store.conflictBlockCalls)
			}
			store.mu.Unlock()
			provider.mu.Lock()
			if provider.sendCalls != 0 {
				t.Fatalf("blocked effect sends=%d, want 0", provider.sendCalls)
			}
			provider.mu.Unlock()
		})
	}
}

func TestCoordinatorNeverUsesProcessClockForExpiryAuthority(t *testing.T) {
	now := time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)
	effect := testEffect(now, pusheffect.StatusAmbiguous)
	effect.Fence = 1
	effect.Attempt = defaultMaxAttempts
	effect.FailureClass = "response_unknown"
	ambiguousSince := now.Add(-time.Minute)
	effect.AmbiguousSince = &ambiguousSince

	store := newFakeStore(effect)
	coordinator := newTestCoordinator(t, store, &fakeProvider{}, now)
	coordinator.now = func() time.Time { return now.Add(24 * time.Hour) }
	outcome, err := coordinator.recoverEffect(t.Context(), effect)
	if err != nil || outcome != OutcomeDeferred {
		t.Fatalf(
			"ahead process clock outcome=%q err=%v, want deferred",
			outcome,
			err,
		)
	}

	store.dbNow = effect.IdempotencyExpiresAt
	coordinator.now = func() time.Time { return now.Add(-24 * time.Hour) }
	outcome, err = coordinator.recoverEffect(t.Context(), effect)
	if err != nil || outcome != OutcomeBlocked {
		t.Fatalf(
			"behind process clock outcome=%q err=%v, want blocked",
			outcome,
			err,
		)
	}
}

func TestCoordinatorRecoverOnceIsTenantShardedAndBounded(t *testing.T) {
	now := time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)
	store := newFakeStore(testEffect(now, pusheffect.StatusAmbiguous))
	store.dbNow = now
	store.tenants = []int64{1, 2, 3, 4}
	store.shards = make(map[int64][]pusheffect.Effect)
	for _, tenantID := range store.tenants {
		for range 4 {
			effect := testEffect(now, pusheffect.StatusAmbiguous)
			effect.TenantID = tenantID
			effect.ID = uuid.NewString()
			effect.Fence = 1
			effect.Attempt = defaultMaxAttempts
			effect.FailureClass = "response_unknown"
			ambiguousSince := now.Add(-time.Minute)
			effect.AmbiguousSince = &ambiguousSince
			store.shards[tenantID] = append(store.shards[tenantID], effect)
		}
	}
	coordinator := newTestCoordinator(t, store, &fakeProvider{}, now)
	coordinator.config.TenantLimit = 3
	coordinator.config.PerTenantLimit = 2
	coordinator.config.PassLimit = 5
	coordinator.config.Concurrency = 2

	if err := coordinator.RecoverOnce(t.Context()); err != nil {
		t.Fatalf("bounded recovery pass: %v", err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.listedTenants) != 3 {
		t.Fatalf("listed tenant shards=%v, want 3", store.listedTenants)
	}
	total := 0
	for _, limit := range store.effectListLimits {
		if limit > 2 {
			t.Fatalf("per-tenant limit=%d, max 2", limit)
		}
		total += limit
	}
	if total != 5 {
		t.Fatalf("global requested effect capacity=%d, want 5", total)
	}
}

func TestCoordinatorPersistentDeferralPreventsSameTenantStarvationAcrossRestart(
	t *testing.T,
) {
	now := time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)
	exhausted := testEffect(now, pusheffect.StatusAmbiguous)
	exhausted.ID = "00000000-0000-0000-0000-000000000001"
	exhausted.Fence = 1
	exhausted.Attempt = defaultMaxAttempts
	exhausted.FailureClass = "response_unknown"
	exhaustedSince := now.Add(-time.Minute)
	exhausted.AmbiguousSince = &exhaustedSince

	second := testEffect(now, pusheffect.StatusAmbiguous)
	second.ID = "00000000-0000-0000-0000-000000000002"
	second.Fence = 1
	second.Attempt = 1
	second.FailureClass = "response_unknown"
	secondSince := now.Add(-time.Minute)
	second.AmbiguousSince = &secondSince

	third := testEffect(now, pusheffect.StatusAmbiguous)
	third.ID = "00000000-0000-0000-0000-000000000003"
	third.Fence = 1
	third.Attempt = 1
	third.FailureClass = "response_unknown"
	thirdSince := now.Add(-time.Minute)
	third.AmbiguousSince = &thirdSince

	store := newFakeStore(exhausted)
	store.dbNow = now
	store.tenants = []int64{1}
	store.shards = map[int64][]pusheffect.Effect{
		1: {exhausted, second, third},
	}
	provider := &fakeProvider{
		historyByEffect: map[string]pusheffect.HistoryObservation{
			exhausted.ID: {},
			second.ID: {
				MatchCount: 1,
				MessageID:  "om_second",
			},
			third.ID: {
				MatchCount: 1,
				MessageID:  "om_third",
			},
		},
	}
	first := newTestCoordinator(t, store, provider, now)
	first.config.TenantLimit = 1
	first.config.PerTenantLimit = 1
	first.config.PassLimit = 1
	first.config.Concurrency = 1

	if err := first.RecoverOnce(t.Context()); err != nil {
		t.Fatalf("first recovery pass: %v", err)
	}
	if err := first.RecoverOnce(t.Context()); err != nil {
		t.Fatalf("second recovery pass: %v", err)
	}

	// Simulate restart: the tenant cursor resets, but next_attempt_at remains
	// in the shared durable Store.
	restarted := newTestCoordinator(t, store, provider, now)
	restarted.config.TenantLimit = 1
	restarted.config.PerTenantLimit = 1
	restarted.config.PassLimit = 1
	restarted.config.Concurrency = 1
	if err := restarted.RecoverOnce(t.Context()); err != nil {
		t.Fatalf("post-restart recovery pass: %v", err)
	}

	provider.mu.Lock()
	if provider.historyCallsByEffect[exhausted.ID] != 1 ||
		provider.historyCallsByEffect[second.ID] != 1 ||
		provider.historyCallsByEffect[third.ID] != 1 {
		t.Fatalf(
			"history calls exhausted/second/third=%d/%d/%d, want 1/1/1",
			provider.historyCallsByEffect[exhausted.ID],
			provider.historyCallsByEffect[second.ID],
			provider.historyCallsByEffect[third.ID],
		)
	}
	provider.mu.Unlock()
	store.mu.Lock()
	defer store.mu.Unlock()
	deferred, ok := store.effectByIDLocked(exhausted.ID)
	if !ok ||
		!deferred.NextAttemptAt.Equal(deferred.IdempotencyExpiresAt) ||
		store.expiryDeferralCalls != 1 {
		t.Fatalf(
			"persistent exhausted deferral=%+v found=%v calls=%d",
			deferred,
			ok,
			store.expiryDeferralCalls,
		)
	}
}

func TestCoordinatorProviderTimeoutCheckpointsAcrossRestartWithoutStarvation(
	t *testing.T,
) {
	now := time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)
	timedOut := testEffect(now, pusheffect.StatusAmbiguous)
	timedOut.ID = "00000000-0000-0000-0000-000000000011"
	timedOut.Fence = 1
	timedOut.Attempt = 1
	timedOut.FailureClass = "response_unknown"
	timedOutSince := now.Add(-time.Minute)
	timedOut.AmbiguousSince = &timedOutSince

	next := testEffect(now, pusheffect.StatusAmbiguous)
	next.ID = "00000000-0000-0000-0000-000000000012"
	next.Fence = 1
	next.Attempt = 1
	next.FailureClass = "response_unknown"
	nextSince := now.Add(-time.Minute)
	next.AmbiguousSince = &nextSince

	store := newFakeStore(timedOut)
	store.tenants = []int64{1}
	store.shards = map[int64][]pusheffect.Effect{1: {timedOut, next}}
	provider := &fakeProvider{
		historyWaitForContext: map[string]bool{timedOut.ID: true},
		historyByEffect: map[string]pusheffect.HistoryObservation{
			next.ID: {MatchCount: 1, MessageID: "om_after_timeout"},
		},
	}
	first := newTestCoordinator(t, store, provider, now)
	first.config.TenantLimit = 1
	first.config.PerTenantLimit = 1
	first.config.PassLimit = 1
	first.config.Concurrency = 1
	first.config.AttemptTimeout = 10 * time.Millisecond
	first.config.CheckpointTimeout = 100 * time.Millisecond
	first.config.PassTimeout = time.Second

	err := first.RecoverOnce(t.Context())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timeout recovery error=%v, want deadline exceeded", err)
	}

	store.mu.Lock()
	deferred, found := store.effectByIDLocked(timedOut.ID)
	if !found || !deferred.NextAttemptAt.After(store.dbNow) ||
		store.deferralCalls != 1 {
		store.mu.Unlock()
		t.Fatalf(
			"timeout checkpoint=%+v found=%v deferrals=%d",
			deferred,
			found,
			store.deferralCalls,
		)
	}
	store.mu.Unlock()

	// A fresh coordinator resets its cursor, but the durable backoff makes the
	// next same-tenant effect visible despite a per-tenant limit of one.
	restarted := newTestCoordinator(t, store, provider, now)
	restarted.config.TenantLimit = 1
	restarted.config.PerTenantLimit = 1
	restarted.config.PassLimit = 1
	restarted.config.Concurrency = 1
	if err := restarted.RecoverOnce(t.Context()); err != nil {
		t.Fatalf("post-timeout restart recovery: %v", err)
	}
	if err := restarted.RecoverOnce(t.Context()); err != nil {
		t.Fatalf("immediate history throttle pass: %v", err)
	}

	provider.mu.Lock()
	defer provider.mu.Unlock()
	if provider.historyCallsByEffect[timedOut.ID] != 1 ||
		provider.historyCallsByEffect[next.ID] != 1 {
		t.Fatalf(
			"history calls timed-out/next=%d/%d, want 1/1",
			provider.historyCallsByEffect[timedOut.ID],
			provider.historyCallsByEffect[next.ID],
		)
	}
}

func TestCoordinatorCheckpointHonorsPassCancellation(t *testing.T) {
	now := time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)
	effect := testEffect(now, pusheffect.StatusAmbiguous)
	effect.Fence = 1
	effect.Attempt = 1
	effect.FailureClass = "response_unknown"
	ambiguousSince := now.Add(-time.Minute)
	effect.AmbiguousSince = &ambiguousSince
	store := newFakeStore(effect)
	provider := &fakeProvider{historyErr: context.DeadlineExceeded}
	coordinator := newTestCoordinator(t, store, provider, now)

	passCtx, cancelPass := context.WithCancel(t.Context())
	cancelPass()
	_, err := coordinator.recoverEffectWithCheckpoint(
		t.Context(),
		passCtx,
		effect,
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("shutdown checkpoint error=%v, want canceled", err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.deferralCalls != 0 {
		t.Fatalf("shutdown deferrals=%d, want 0", store.deferralCalls)
	}
}

func newTestCoordinator(
	t *testing.T,
	store Store,
	provider Provider,
	now time.Time,
) *Coordinator {
	t.Helper()
	coordinator, err := New(Deps{
		Store:    store,
		Provider: provider,
		Config: Config{
			MaxAttempts: 8,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	coordinator.now = func() time.Time { return now }
	return coordinator
}

func testEffect(now time.Time, status pusheffect.Status) pusheffect.Effect {
	card := []byte(`{"card":"frozen"}`)
	return pusheffect.Effect{
		Prepared: pusheffect.Prepared{
			ID:                   uuid.NewString(),
			TenantID:             1,
			UserID:               2,
			TaskID:               "task-push-recovery",
			RunSnapshotID:        3,
			RunID:                "run-push-recovery",
			StepID:               "push",
			ChunkIndex:           0,
			ChunkCount:           1,
			BatchID:              4,
			DeliveryIDs:          []int64{5},
			Provider:             "feishu",
			AppIdentity:          "cli_push_recovery",
			ProviderChatID:       "oc_push_recovery",
			Target:               "ou_push_recovery",
			Card:                 card,
			ProviderUUID:         uuid.NewString(),
			IdempotencyExpiresAt: now.Add(time.Hour),
		},
		SchemaVersion: pusheffect.SchemaVersion,
		CardDigest:    pusheffect.CardDigest(card),
		Status:        status,
		NextAttemptAt: now,
		CreatedAt:     now.Add(-time.Minute),
		UpdatedAt:     now,
	}
}

type fakeStore struct {
	mu sync.Mutex

	effect     pusheffect.Effect
	authorized bool
	dbNow      time.Time
	tenants    []int64
	shards     map[int64][]pusheffect.Effect

	listedTenants            []int64
	effectListLimits         []int
	authorizeCalls           int
	freshClaimCalls          int
	reconciliationClaimCalls int
	definiteFailureCalls     int
	ambiguousCalls           int
	sentCalls                int
	deferralCalls            int
	expiryDeferralCalls      int
	expiredBlockCalls        int
	conflictBlockCalls       int
}

func newFakeStore(effect pusheffect.Effect) *fakeStore {
	return &fakeStore{
		effect:     effect,
		authorized: true,
		dbNow:      effect.UpdatedAt,
		shards: map[int64][]pusheffect.Effect{
			effect.TenantID: {effect},
		},
	}
}

func (s *fakeStore) ListRecoverablePushEffectTenantIDs(
	_ context.Context,
	_ time.Time,
	after int64,
	limit int,
) ([]int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var result []int64
	for _, tenantID := range s.tenants {
		if tenantID > after && len(result) < limit {
			result = append(result, tenantID)
		}
	}
	s.listedTenants = append([]int64(nil), result...)
	return result, nil
}

func (s *fakeStore) ListRecoverablePushEffects(
	_ context.Context,
	tenantID int64,
	_ time.Time,
	limit int,
) ([]pusheffect.Effect, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.effectListLimits = append(s.effectListLimits, limit)
	shard := s.shards[tenantID]
	due := make([]pusheffect.Effect, 0, min(limit, len(shard)))
	for _, effect := range shard {
		if len(due) >= limit {
			break
		}
		switch effect.Status {
		case pusheffect.StatusPrepared,
			pusheffect.StatusDefiniteFailed,
			pusheffect.StatusAmbiguous:
			if !s.dbNow.IsZero() && effect.NextAttemptAt.After(s.dbNow) {
				continue
			}
		case pusheffect.StatusSending:
			if effect.TakeoverNotBefore == nil ||
				(!s.dbNow.IsZero() &&
					effect.TakeoverNotBefore.After(s.dbNow)) {
				continue
			}
		default:
			continue
		}
		due = append(due, effect)
	}
	return due, nil
}

func (s *fakeStore) TakeOverStalePushEffect(
	_ context.Context,
	_ pusheffect.Scope,
) (*pusheffect.Effect, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.effect.Status = pusheffect.StatusAmbiguous
	s.effect.Fence++
	s.effect.Attempt++
	s.effect.LeaseOwner = ""
	s.effect.LeaseUntil = nil
	s.effect.TakeoverNotBefore = nil
	s.effect.FailureClass = "response_unknown"
	now := s.effect.UpdatedAt.Add(time.Second)
	s.effect.AmbiguousSince = &now
	copy := s.effect
	return &copy, nil
}

func (s *fakeStore) AuthorizePushEffectRunSideEffect(
	_ context.Context,
	_ pusheffect.Scope,
) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.authorizeCalls++
	return s.authorized, nil
}

func (s *fakeStore) ClaimAuthorizedPushEffect(
	_ context.Context,
	params pusheffect.ClaimParams,
) (*pusheffect.Effect, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.freshClaimCalls++
	return s.claim(params), nil
}

func (s *fakeStore) ClaimAuthorizedPushEffectReconciliation(
	_ context.Context,
	params pusheffect.ClaimParams,
) (*pusheffect.Effect, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reconciliationClaimCalls++
	return s.claim(params), nil
}

func (s *fakeStore) claim(
	params pusheffect.ClaimParams,
) *pusheffect.Effect {
	s.effect.Status = pusheffect.StatusSending
	s.effect.LeaseOwner = params.LeaseOwner
	until := s.effect.UpdatedAt.Add(params.LeaseDuration)
	s.effect.LeaseUntil = &until
	s.effect.Fence++
	s.effect.Attempt++
	copy := s.effect
	return &copy
}

func (s *fakeStore) RecordPushEffectDefiniteFailure(
	_ context.Context,
	_ pusheffect.FailureParams,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.definiteFailureCalls++
	s.effect.Status = pusheffect.StatusDefiniteFailed
	return nil
}

func (s *fakeStore) RecordPushEffectAmbiguous(
	_ context.Context,
	_ pusheffect.FailureParams,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ambiguousCalls++
	s.effect.Status = pusheffect.StatusAmbiguous
	return nil
}

func (s *fakeStore) RecordPushEffectSentWithDeliveries(
	_ context.Context,
	receipt pusheffect.SentReceipt,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sentCalls++
	s.updateEffectLocked(receipt.ID, func(effect *pusheffect.Effect) {
		effect.Status = pusheffect.StatusSent
	})
	return nil
}

func (s *fakeStore) DeferPushEffectReconciliation(
	_ context.Context,
	resolution pusheffect.Resolution,
	retryAfter time.Duration,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deferralCalls++
	s.updateEffectLocked(resolution.ID, func(effect *pusheffect.Effect) {
		effect.NextAttemptAt = s.dbNow.Add(retryAfter)
		if effect.NextAttemptAt.After(effect.IdempotencyExpiresAt) {
			effect.NextAttemptAt = effect.IdempotencyExpiresAt
		}
		effect.FailureClass = resolution.Class
	})
	return nil
}

func (s *fakeStore) DeferPushEffectReconciliationUntilExpiry(
	_ context.Context,
	resolution pusheffect.Resolution,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expiryDeferralCalls++
	s.updateEffectLocked(resolution.ID, func(effect *pusheffect.Effect) {
		effect.NextAttemptAt = effect.IdempotencyExpiresAt
		effect.FailureClass = resolution.Class
	})
	return nil
}

func (s *fakeStore) DeferOrBlockPushEffectReconciliation(
	ctx context.Context,
	schedule pusheffect.ReconciliationSchedule,
) (pusheffect.ReconciliationDecision, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var decision pusheffect.ReconciliationDecision
	target, found := s.effectByIDLocked(schedule.ID)
	if !found && s.effect.ID == schedule.ID {
		target = s.effect
		found = true
	}
	if !found {
		return "", errors.New("fake reconciliation target not found")
	}
	expired := !s.dbNow.Before(target.IdempotencyExpiresAt)
	if expired {
		s.expiredBlockCalls++
	} else if schedule.UntilExpiry {
		s.expiryDeferralCalls++
	} else {
		s.deferralCalls++
	}
	s.updateEffectLocked(schedule.ID, func(effect *pusheffect.Effect) {
		if !s.dbNow.Before(effect.IdempotencyExpiresAt) {
			effect.Status = pusheffect.StatusBlocked
			effect.FailureClass = schedule.ExpiredClass
			blockedAt := s.dbNow
			effect.BlockedAt = &blockedAt
			decision = pusheffect.ReconciliationBlocked
			return
		}
		if schedule.UntilExpiry {
			effect.NextAttemptAt = effect.IdempotencyExpiresAt
		} else {
			effect.NextAttemptAt = s.dbNow.Add(schedule.RetryAfter)
			if effect.NextAttemptAt.After(effect.IdempotencyExpiresAt) {
				effect.NextAttemptAt = effect.IdempotencyExpiresAt
			}
		}
		effect.FailureClass = schedule.Class
		decision = pusheffect.ReconciliationDeferred
	})
	return decision, nil
}

func (s *fakeStore) BlockExpiredPushEffect(
	_ context.Context,
	resolution pusheffect.Resolution,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expiredBlockCalls++
	s.updateEffectLocked(resolution.ID, func(effect *pusheffect.Effect) {
		effect.Status = pusheffect.StatusBlocked
	})
	return nil
}

func (s *fakeStore) BlockConflictingPushEffectHistory(
	_ context.Context,
	resolution pusheffect.Resolution,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.conflictBlockCalls++
	s.updateEffectLocked(resolution.ID, func(effect *pusheffect.Effect) {
		effect.Status = pusheffect.StatusBlocked
	})
	return nil
}

func (s *fakeStore) updateEffectLocked(
	effectID string,
	update func(*pusheffect.Effect),
) {
	if s.effect.ID == effectID {
		update(&s.effect)
	}
	for tenantID, shard := range s.shards {
		for i := range shard {
			if shard[i].ID == effectID {
				update(&shard[i])
			}
		}
		s.shards[tenantID] = shard
	}
}

func (s *fakeStore) effectByIDLocked(
	effectID string,
) (pusheffect.Effect, bool) {
	for _, shard := range s.shards {
		for _, effect := range shard {
			if effect.ID == effectID {
				return effect, true
			}
		}
	}
	return pusheffect.Effect{}, false
}

type fakeProvider struct {
	mu sync.Mutex

	historyObservation    pusheffect.HistoryObservation
	historyErr            error
	historyByEffect       map[string]pusheffect.HistoryObservation
	historyWaitForContext map[string]bool
	sendObservation       pusheffect.ProviderObservation
	sendErr               error

	historyCalls         int
	historyCallsByEffect map[string]int
	sendCalls            int
	historyQuery         pusheffect.HistoryQuery
	gotAppIdentity       string
	gotTarget            string
	gotCard              string
	gotUUID              string
}

func (p *fakeProvider) ResolvePushEffectMessage(
	ctx context.Context,
	query pusheffect.HistoryQuery,
) (pusheffect.HistoryObservation, error) {
	p.mu.Lock()
	p.historyCalls++
	p.historyQuery = query
	if p.historyCallsByEffect == nil {
		p.historyCallsByEffect = make(map[string]int)
	}
	p.historyCallsByEffect[query.EffectID]++
	observation, hasObservation := p.historyByEffect[query.EffectID]
	defaultObservation := p.historyObservation
	waitForContext := p.historyWaitForContext[query.EffectID]
	historyErr := p.historyErr
	p.mu.Unlock()
	if waitForContext {
		<-ctx.Done()
		return observation, ctx.Err()
	}
	if hasObservation {
		return observation, historyErr
	}
	return defaultObservation, historyErr
}

func (p *fakeProvider) PushWithUUID(
	_ context.Context,
	appIdentity string,
	target string,
	card string,
	providerUUID string,
) (pusheffect.ProviderObservation, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sendCalls++
	p.gotAppIdentity = appIdentity
	p.gotTarget = target
	p.gotCard = card
	p.gotUUID = providerUUID
	return p.sendObservation, p.sendErr
}
