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

func TestCoordinatorRecoverOnceIsTenantShardedAndBounded(t *testing.T) {
	now := time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)
	store := newFakeStore(testEffect(now, pusheffect.StatusSent))
	store.tenants = []int64{1, 2, 3, 4}
	store.shards = make(map[int64][]pusheffect.Effect)
	for _, tenantID := range store.tenants {
		for range 4 {
			effect := testEffect(now, pusheffect.StatusSent)
			effect.TenantID = tenantID
			effect.ID = uuid.NewString()
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
	expiredBlockCalls        int
	conflictBlockCalls       int
}

func newFakeStore(effect pusheffect.Effect) *fakeStore {
	return &fakeStore{
		effect:     effect,
		authorized: true,
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
	if len(shard) > limit {
		shard = shard[:limit]
	}
	return append([]pusheffect.Effect(nil), shard...), nil
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
	_ pusheffect.SentReceipt,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sentCalls++
	s.effect.Status = pusheffect.StatusSent
	return nil
}

func (s *fakeStore) BlockExpiredPushEffect(
	_ context.Context,
	_ pusheffect.Resolution,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expiredBlockCalls++
	s.effect.Status = pusheffect.StatusBlocked
	return nil
}

func (s *fakeStore) BlockConflictingPushEffectHistory(
	_ context.Context,
	_ pusheffect.Resolution,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.conflictBlockCalls++
	s.effect.Status = pusheffect.StatusBlocked
	return nil
}

type fakeProvider struct {
	mu sync.Mutex

	historyObservation pusheffect.HistoryObservation
	historyErr         error
	sendObservation    pusheffect.ProviderObservation
	sendErr            error

	historyCalls   int
	sendCalls      int
	historyQuery   pusheffect.HistoryQuery
	gotAppIdentity string
	gotTarget      string
	gotCard        string
	gotUUID        string
}

func (p *fakeProvider) ResolvePushEffectMessage(
	_ context.Context,
	query pusheffect.HistoryQuery,
) (pusheffect.HistoryObservation, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.historyCalls++
	p.historyQuery = query
	return p.historyObservation, p.historyErr
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
