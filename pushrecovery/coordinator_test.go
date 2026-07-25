package pushrecovery

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/YouToco/vane/pusheffect"
)

type fakeStore struct {
	effect             pusheffect.Effect
	authorizedClaim    pusheffect.AuthorizedClaimParams
	receipt            *pusheffect.SentReceipt
	ambiguous          *pusheffect.FailureParams
	definite           *pusheffect.FailureParams
	deferred           *pusheffect.ReconciliationSchedule
	historyConflict    *pusheffect.HistoryResolution
	exhausted          *pusheffect.ExhaustedResolution
	checkpointCanceled bool
	claimDecision      pusheffect.AuthorizedClaimDecision
	claimErr           error
	deferDecision      pusheffect.ReconciliationDecision
	deferErr           error
	claimCalls         int
}

func (f *fakeStore) LoadPushEffect(
	_ context.Context,
	_ pusheffect.Scope,
) (*pusheffect.Effect, error) {
	effect := f.effect
	return &effect, nil
}

func (f *fakeStore) TakeOverStalePushEffect(
	_ context.Context,
	_ pusheffect.Scope,
) (*pusheffect.Effect, error) {
	effect := f.effect
	effect.Status = pusheffect.StatusAmbiguous
	effect.LeaseOwner = ""
	effect.LeaseUntil = nil
	effect.Fence++
	effect.Attempt++
	return &effect, nil
}

func (f *fakeStore) ClaimAuthorizedPushEffect(
	_ context.Context,
	params pusheffect.AuthorizedClaimParams,
) (*pusheffect.Effect, pusheffect.AuthorizedClaimDecision, error) {
	f.authorizedClaim = params
	f.claimCalls++
	if f.claimErr != nil {
		return nil, "", f.claimErr
	}
	if f.claimDecision == pusheffect.AuthorizedClaimNotDue ||
		f.claimDecision == pusheffect.AuthorizedClaimDenied {
		return nil, f.claimDecision, nil
	}
	effect := f.effect
	effect.Status = pusheffect.StatusSending
	effect.LeaseOwner = params.LeaseOwner
	effect.Fence++
	effect.Attempt++
	return &effect, pusheffect.AuthorizedClaimed, nil
}

func (f *fakeStore) ClaimAuthorizedPushEffectReconciliation(
	ctx context.Context,
	params pusheffect.AuthorizedClaimParams,
) (*pusheffect.Effect, pusheffect.AuthorizedClaimDecision, error) {
	return f.ClaimAuthorizedPushEffect(ctx, params)
}

func (f *fakeStore) RecordPushEffectDefiniteFailure(
	ctx context.Context,
	params pusheffect.FailureParams,
) error {
	f.checkpointCanceled = f.checkpointCanceled || ctx.Err() != nil
	f.definite = &params
	return nil
}

func (f *fakeStore) RecordPushEffectAmbiguous(
	ctx context.Context,
	params pusheffect.FailureParams,
) error {
	f.checkpointCanceled = f.checkpointCanceled || ctx.Err() != nil
	f.ambiguous = &params
	return nil
}

func (f *fakeStore) RecordPushEffectSentWithDeliveries(
	ctx context.Context,
	receipt pusheffect.SentReceipt,
) error {
	f.checkpointCanceled = f.checkpointCanceled || ctx.Err() != nil
	f.receipt = &receipt
	return nil
}

func (f *fakeStore) DeferOrBlockPushEffectReconciliation(
	ctx context.Context,
	schedule pusheffect.ReconciliationSchedule,
) (pusheffect.ReconciliationDecision, error) {
	f.checkpointCanceled = f.checkpointCanceled || ctx.Err() != nil
	f.deferred = &schedule
	if f.deferErr != nil {
		return "", f.deferErr
	}
	if f.deferDecision != "" {
		return f.deferDecision, nil
	}
	return pusheffect.ReconciliationDeferred, nil
}

func (f *fakeStore) BlockConflictingPushEffectHistory(
	ctx context.Context,
	resolution pusheffect.HistoryResolution,
) error {
	f.checkpointCanceled = f.checkpointCanceled || ctx.Err() != nil
	f.historyConflict = &resolution
	return nil
}

func (f *fakeStore) BlockExhaustedPushEffectAttempts(
	ctx context.Context,
	resolution pusheffect.ExhaustedResolution,
) error {
	f.checkpointCanceled = f.checkpointCanceled || ctx.Err() != nil
	f.exhausted = &resolution
	return nil
}

type fakeSender struct {
	send    func(context.Context) (pusheffect.ProviderObservation, error)
	observe func(appIdentity, target, card, providerUUID string)
}

func (f fakeSender) PushWithUUID(
	ctx context.Context,
	appIdentity, target, card, providerUUID string,
) (pusheffect.ProviderObservation, error) {
	if f.observe != nil {
		f.observe(appIdentity, target, card, providerUUID)
	}
	return f.send(ctx)
}

type fakeHistoryResolver struct {
	resolve func(context.Context) (pusheffect.HistoryObservation, error)
	observe func(pusheffect.HistoryQuery)
}

func (f fakeHistoryResolver) ResolvePushEffectMessage(
	ctx context.Context,
	query pusheffect.HistoryQuery,
) (pusheffect.HistoryObservation, error) {
	if f.observe != nil {
		f.observe(query)
	}
	return f.resolve(ctx)
}

func TestAttemptClaimsOnlyConfiguredTaskAndSettlesObservationKeys(t *testing.T) {
	t.Parallel()

	effect := testEffect()
	st := &fakeStore{effect: effect}
	coordinator, err := New(Deps{
		Store: st,
		Sender: fakeSender{
			observe: func(appIdentity, target, card, providerUUID string) {
				if appIdentity != effect.AppIdentity ||
					target != effect.Target ||
					card != string(effect.Card) ||
					providerUUID != effect.ProviderUUID {
					t.Fatalf(
						"sender args=%q/%q/%q/%q",
						appIdentity, target, card, providerUUID,
					)
				}
			},
			send: func(context.Context) (
				pusheffect.ProviderObservation, error,
			) {
				return pusheffect.ProviderObservation{
					Disposition: pusheffect.AttemptSent,
					AppIdentity: effect.AppIdentity,
					MessageID:   "om_exact",
					ChatID:      effect.ProviderChatID,
				}, nil
			},
		},
		HistoryResolver: fakeHistoryResolver{
			resolve: func(context.Context) (
				pusheffect.HistoryObservation, error,
			) {
				return pusheffect.HistoryObservation{}, nil
			},
		},
		Config: Config{ExactTaskID: effect.TaskID},
	})
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := coordinator.Attempt(t.Context(), effect.Scope())
	if err != nil || outcome != OutcomeSent {
		t.Fatalf("Attempt()=%q/%v", outcome, err)
	}
	if st.authorizedClaim.ExpectedTaskID != effect.TaskID {
		t.Fatalf("authorized task=%q", st.authorizedClaim.ExpectedTaskID)
	}
	if st.receipt == nil ||
		len(st.receipt.ObservationEventKeys) != 1 ||
		st.receipt.ObservationEventKeys[0] !=
			effect.ObservationEventKeys[0] {
		t.Fatalf("sent receipt lost observation keys: %+v", st.receipt)
	}
}

func TestAttemptStaleTakeoverAdoptsExactHistoryWithObservationKeys(
	t *testing.T,
) {
	t.Parallel()

	effect := testEffect()
	effect.Status = pusheffect.StatusSending
	effect.LeaseOwner = "stale-worker"
	expiredLease := effect.CreatedAt.Add(-time.Minute)
	effect.LeaseUntil = &expiredLease
	effect.Fence = 2
	effect.Attempt = 2
	st := &fakeStore{effect: effect}
	coordinator, err := New(Deps{
		Store: st,
		Sender: fakeSender{send: func(context.Context) (
			pusheffect.ProviderObservation, error,
		) {
			t.Fatal("positive history called provider Create")
			return pusheffect.ProviderObservation{}, nil
		}},
		HistoryResolver: fakeHistoryResolver{
			observe: func(query pusheffect.HistoryQuery) {
				if query.EffectID != effect.ID ||
					query.ProviderChatID != effect.ProviderChatID ||
					query.AppIdentity != effect.AppIdentity ||
					query.CardDigest != effect.CardDigest ||
					!query.StartTime.Equal(
						effect.CreatedAt.Add(-defaultHistorySkew)) ||
					!query.EndTime.Equal(
						effect.IdempotencyExpiresAt.Add(defaultHistorySkew)) {
					t.Fatalf("history query=%+v", query)
				}
			},
			resolve: func(context.Context) (
				pusheffect.HistoryObservation, error,
			) {
				return pusheffect.HistoryObservation{
					MatchCount: 1, MessageID: "om_history_exact",
				}, nil
			},
		},
		Config: Config{ExactTaskID: effect.TaskID},
	})
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := coordinator.Attempt(t.Context(), effect.Scope())
	if err != nil || outcome != OutcomeSent {
		t.Fatalf("Attempt()=%q/%v", outcome, err)
	}
	if st.receipt == nil ||
		st.receipt.ProviderMessageID != "om_history_exact" ||
		len(st.receipt.ObservationEventKeys) != 1 ||
		st.receipt.ObservationEventKeys[0] !=
			effect.ObservationEventKeys[0] {
		t.Fatalf("history receipt=%+v", st.receipt)
	}
}

func TestAttemptProviderTimeoutStillPersistsAmbiguousAndBackoff(t *testing.T) {
	t.Parallel()

	effect := testEffect()
	st := &fakeStore{effect: effect}
	coordinator, err := New(Deps{
		Store: st,
		Sender: fakeSender{send: func(ctx context.Context) (
			pusheffect.ProviderObservation, error,
		) {
			<-ctx.Done()
			return pusheffect.ProviderObservation{}, ctx.Err()
		}},
		HistoryResolver: fakeHistoryResolver{
			resolve: func(context.Context) (
				pusheffect.HistoryObservation, error,
			) {
				return pusheffect.HistoryObservation{}, nil
			},
		},
		Config: Config{
			ExactTaskID: effect.TaskID, ProviderTimeout: time.Millisecond,
			CheckpointTimeout: time.Second,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := coordinator.Attempt(t.Context(), effect.Scope())
	if outcome != OutcomeAmbiguous ||
		!errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Attempt()=%q/%v", outcome, err)
	}
	if st.ambiguous == nil || st.deferred == nil {
		t.Fatalf(
			"timeout checkpoints ambiguous=%+v deferred=%+v",
			st.ambiguous, st.deferred,
		)
	}
	if st.checkpointCanceled {
		t.Fatal("provider child deadline leaked into durable checkpoint context")
	}
}

func TestAttemptHistoryErrorPreservesDurableBlockAndCheckpointFailureIsUnknown(
	t *testing.T,
) {
	t.Parallel()
	historyErr := errors.New("history unavailable")
	checkpointErr := errors.New("checkpoint unavailable")
	for _, test := range []struct {
		name        string
		store       *fakeStore
		wantOutcome Outcome
		wantErr     error
	}{
		{
			name: "expired window blocks",
			store: &fakeStore{
				deferDecision: pusheffect.ReconciliationBlocked,
			},
			wantOutcome: OutcomeBlocked,
			wantErr:     historyErr,
		},
		{
			name:    "checkpoint failure has no durable outcome",
			store:   &fakeStore{deferErr: checkpointErr},
			wantErr: checkpointErr,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			effect := testEffect()
			effect.Status = pusheffect.StatusAmbiguous
			effect.Fence = 1
			effect.Attempt = 1
			test.store.effect = effect
			coordinator, err := New(Deps{
				Store: test.store,
				Sender: fakeSender{send: func(context.Context) (
					pusheffect.ProviderObservation, error,
				) {
					t.Fatal("history error called provider")
					return pusheffect.ProviderObservation{}, nil
				}},
				HistoryResolver: fakeHistoryResolver{
					resolve: func(context.Context) (
						pusheffect.HistoryObservation, error,
					) {
						return pusheffect.HistoryObservation{}, historyErr
					},
				},
				Config: Config{ExactTaskID: effect.TaskID},
			})
			if err != nil {
				t.Fatal(err)
			}
			outcome, err := coordinator.Attempt(
				t.Context(), effect.Scope())
			if outcome != test.wantOutcome ||
				!errors.Is(err, test.wantErr) {
				t.Fatalf("Attempt()=%q/%v", outcome, err)
			}
		})
	}
}

func TestAttemptLifecycleCancellationCannotEraseProviderReceipt(t *testing.T) {
	t.Parallel()
	effect := testEffect()
	st := &fakeStore{effect: effect}
	attemptCtx, cancelAttempt := context.WithCancel(t.Context())
	coordinator, err := New(Deps{
		Store: st,
		Sender: fakeSender{send: func(context.Context) (
			pusheffect.ProviderObservation, error,
		) {
			cancelAttempt()
			return pusheffect.ProviderObservation{
				Disposition: pusheffect.AttemptSent,
				AppIdentity: effect.AppIdentity,
				MessageID:   "om_after_cancel",
				ChatID:      effect.ProviderChatID,
			}, nil
		}},
		HistoryResolver: fakeHistoryResolver{
			resolve: func(context.Context) (
				pusheffect.HistoryObservation, error,
			) {
				return pusheffect.HistoryObservation{}, nil
			},
		},
		Config: Config{
			ExactTaskID:       effect.TaskID,
			CheckpointTimeout: time.Second,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := coordinator.Attempt(attemptCtx, effect.Scope())
	if err != nil || outcome != OutcomeSent {
		t.Fatalf("Attempt()=%q/%v", outcome, err)
	}
	if st.receipt == nil || st.checkpointCanceled {
		t.Fatalf("receipt=%+v checkpointCanceled=%v",
			st.receipt, st.checkpointCanceled)
	}
}

func TestAttemptAmbiguousNotDueNeverBurnsReconciliationAttempt(t *testing.T) {
	t.Parallel()

	effect := testEffect()
	effect.Status = pusheffect.StatusAmbiguous
	effect.Fence = 2
	effect.Attempt = 2
	st := &fakeStore{
		effect: effect, claimDecision: pusheffect.AuthorizedClaimNotDue,
	}
	sendCalls := 0
	coordinator, err := New(Deps{
		Store: st,
		Sender: fakeSender{send: func(context.Context) (
			pusheffect.ProviderObservation, error,
		) {
			sendCalls++
			return pusheffect.ProviderObservation{}, nil
		}},
		HistoryResolver: fakeHistoryResolver{
			resolve: func(context.Context) (
				pusheffect.HistoryObservation, error,
			) {
				return pusheffect.HistoryObservation{}, nil
			},
		},
		Config: Config{ExactTaskID: effect.TaskID},
	})
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		outcome, err := coordinator.Attempt(t.Context(), effect.Scope())
		if err != nil || outcome != OutcomeDeferred {
			t.Fatalf("Attempt()=%q/%v", outcome, err)
		}
	}
	if sendCalls != 0 || st.deferred != nil || st.claimCalls != 2 {
		t.Fatalf(
			"not-due attempts send=%d deferred=%+v claims=%d",
			sendCalls, st.deferred, st.claimCalls,
		)
	}
}

func TestAttemptBlocksDeterministicExhaustedEffect(t *testing.T) {
	t.Parallel()

	effect := testEffect()
	effect.Status = pusheffect.StatusDefiniteFailed
	effect.Fence = 8
	effect.Attempt = 8
	st := &fakeStore{effect: effect}
	coordinator, err := New(Deps{
		Store: st,
		Sender: fakeSender{send: func(context.Context) (
			pusheffect.ProviderObservation, error,
		) {
			t.Fatal("exhausted effect called provider")
			return pusheffect.ProviderObservation{}, nil
		}},
		HistoryResolver: fakeHistoryResolver{
			resolve: func(context.Context) (
				pusheffect.HistoryObservation, error,
			) {
				t.Fatal("deterministic exhausted effect called history")
				return pusheffect.HistoryObservation{}, nil
			},
		},
		Config: Config{ExactTaskID: effect.TaskID},
	})
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := coordinator.Attempt(t.Context(), effect.Scope())
	if err != nil || outcome != OutcomeBlocked {
		t.Fatalf("Attempt()=%q/%v", outcome, err)
	}
	if st.exhausted == nil ||
		st.exhausted.ExpectedTaskID != effect.TaskID ||
		st.exhausted.ExpectedFence != effect.Fence {
		t.Fatalf("exhausted resolution=%+v", st.exhausted)
	}
}

func TestAttemptHistoryTimeoutStillPersistsDefer(t *testing.T) {
	t.Parallel()

	effect := testEffect()
	effect.Status = pusheffect.StatusAmbiguous
	effect.Fence = 1
	effect.Attempt = 1
	st := &fakeStore{effect: effect}
	coordinator, err := New(Deps{
		Store: st,
		Sender: fakeSender{send: func(context.Context) (
			pusheffect.ProviderObservation, error,
		) {
			t.Fatal("history timeout called provider")
			return pusheffect.ProviderObservation{}, nil
		}},
		HistoryResolver: fakeHistoryResolver{
			resolve: func(ctx context.Context) (
				pusheffect.HistoryObservation, error,
			) {
				<-ctx.Done()
				return pusheffect.HistoryObservation{}, ctx.Err()
			},
		},
		Config: Config{
			ExactTaskID: effect.TaskID, HistoryTimeout: time.Millisecond,
			CheckpointTimeout: time.Second,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := coordinator.Attempt(t.Context(), effect.Scope())
	if outcome != OutcomeDeferred ||
		!errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Attempt()=%q/%v", outcome, err)
	}
	if st.deferred == nil || st.checkpointCanceled {
		t.Fatalf(
			"history timeout defer=%+v canceled=%v",
			st.deferred, st.checkpointCanceled,
		)
	}
}

func TestAttemptDecisionMatrix(t *testing.T) {
	t.Parallel()

	rejected := errors.New("provider rejected request")
	tests := []struct {
		name          string
		effect        func() pusheffect.Effect
		claimDecision pusheffect.AuthorizedClaimDecision
		history       pusheffect.HistoryObservation
		observation   pusheffect.ProviderObservation
		sendErr       error
		wantOutcome   Outcome
		wantErr       error
		wantSends     int
		verify        func(*testing.T, *fakeStore)
	}{
		{
			name: "multiple exact history matches block without send",
			effect: func() pusheffect.Effect {
				effect := testEffect()
				effect.Status, effect.Fence = pusheffect.StatusAmbiguous, 2
				return effect
			},
			history:     pusheffect.HistoryObservation{MatchCount: 2},
			wantOutcome: OutcomeBlocked,
			verify: func(t *testing.T, store *fakeStore) {
				if store.historyConflict == nil ||
					store.historyConflict.ExpectedFence != 2 {
					t.Fatalf("history block=%+v", store.historyConflict)
				}
			},
		},
		{
			name:          "fresh live denial never calls provider",
			effect:        testEffect,
			claimDecision: pusheffect.AuthorizedClaimDenied,
			wantOutcome:   OutcomeNotAuthorized,
		},
		{
			name: "ambiguous live denial durably defers without send",
			effect: func() pusheffect.Effect {
				effect := testEffect()
				effect.Status, effect.Fence = pusheffect.StatusAmbiguous, 2
				return effect
			},
			claimDecision: pusheffect.AuthorizedClaimDenied,
			wantOutcome:   OutcomeNotAuthorized,
			verify: func(t *testing.T, store *fakeStore) {
				if store.deferred == nil {
					t.Fatal("ambiguous denial did not persist defer")
				}
			},
		},
		{
			name:        "definite provider rejection records retry",
			effect:      testEffect,
			observation: pusheffect.ProviderObservation{Disposition: pusheffect.AttemptDefiniteNotSent},
			sendErr:     rejected,
			wantOutcome: OutcomeDefiniteFail,
			wantErr:     rejected,
			wantSends:   1,
			verify: func(t *testing.T, store *fakeStore) {
				if store.definite == nil ||
					store.definite.Class != "provider_definite_rejection" ||
					store.definite.RetryAfter != defaultRetryAfter {
					t.Fatalf("definite checkpoint=%+v", store.definite)
				}
			},
		},
		{
			name:   "mismatched sent receipt becomes ambiguous",
			effect: testEffect,
			observation: pusheffect.ProviderObservation{
				Disposition: pusheffect.AttemptSent,
				AppIdentity: "wrong-app",
				MessageID:   "om_untrusted",
			},
			wantOutcome: OutcomeAmbiguous,
			wantSends:   1,
			verify: func(t *testing.T, store *fakeStore) {
				if store.receipt != nil || store.ambiguous == nil ||
					store.ambiguous.Class != "provider_receipt_invalid" ||
					store.deferred == nil {
					t.Fatalf(
						"invalid receipt sent=%+v ambiguous=%+v deferred=%+v",
						store.receipt, store.ambiguous, store.deferred,
					)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			effect := test.effect()
			store := &fakeStore{
				effect: effect, claimDecision: test.claimDecision,
			}
			sendCalls := 0
			coordinator, err := New(Deps{
				Store: store,
				Sender: fakeSender{send: func(context.Context) (
					pusheffect.ProviderObservation, error,
				) {
					sendCalls++
					return test.observation, test.sendErr
				}},
				HistoryResolver: fakeHistoryResolver{
					resolve: func(context.Context) (
						pusheffect.HistoryObservation, error,
					) {
						return test.history, nil
					},
				},
				Config: Config{ExactTaskID: effect.TaskID},
			})
			if err != nil {
				t.Fatal(err)
			}
			outcome, gotErr := coordinator.Attempt(
				t.Context(), effect.Scope())
			if outcome != test.wantOutcome ||
				!errors.Is(gotErr, test.wantErr) {
				t.Fatalf(
					"Attempt()=%q/%v, want %q/%v",
					outcome, gotErr, test.wantOutcome, test.wantErr,
				)
			}
			if sendCalls != test.wantSends {
				t.Fatalf("provider calls=%d want=%d", sendCalls, test.wantSends)
			}
			if test.verify != nil {
				test.verify(t, store)
			}
		})
	}
}

func TestNewRejectsUnsafeRecoveryTimingBoundaries(t *testing.T) {
	t.Parallel()

	effect := testEffect()
	validDeps := func() Deps {
		return Deps{
			Store: &fakeStore{effect: effect},
			Sender: fakeSender{send: func(context.Context) (
				pusheffect.ProviderObservation, error,
			) {
				return pusheffect.ProviderObservation{}, nil
			}},
			HistoryResolver: fakeHistoryResolver{
				resolve: func(context.Context) (
					pusheffect.HistoryObservation, error,
				) {
					return pusheffect.HistoryObservation{}, nil
				},
			},
			Config: Config{ExactTaskID: effect.TaskID},
		}
	}
	tests := []struct {
		name   string
		change func(*Config)
	}{
		{
			name: "sub-microsecond lease",
			change: func(config *Config) {
				config.LeaseDuration = time.Nanosecond
			},
		},
		{
			name: "lease above protocol maximum",
			change: func(config *Config) {
				config.LeaseDuration = pusheffect.MaxLeaseDuration +
					time.Microsecond
			},
		},
		{
			name: "sub-microsecond history skew",
			change: func(config *Config) {
				config.HistorySkew = time.Nanosecond
			},
		},
		{
			name: "history window above two hours",
			change: func(config *Config) {
				config.HistorySkew = maxHistorySkew + time.Microsecond
			},
		},
		{
			name: "sub-microsecond provider timeout",
			change: func(config *Config) {
				config.ProviderTimeout = time.Nanosecond
			},
		},
		{
			name: "sub-microsecond history timeout",
			change: func(config *Config) {
				config.HistoryTimeout = time.Nanosecond
			},
		},
		{
			name: "sub-microsecond checkpoint timeout",
			change: func(config *Config) {
				config.CheckpointTimeout = time.Nanosecond
			},
		},
		{
			name: "provider and checkpoint consume lease",
			change: func(config *Config) {
				config.LeaseDuration = 35 * time.Second
			},
		},
		{
			name: "explicit provider and checkpoint equal lease",
			change: func(config *Config) {
				config.LeaseDuration = 20 * time.Second
				config.ProviderTimeout = 15 * time.Second
				config.CheckpointTimeout = 5 * time.Second
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			deps := validDeps()
			test.change(&deps.Config)
			if _, err := New(deps); !errors.Is(err, ErrDependencies) {
				t.Fatalf("New() error=%v", err)
			}
		})
	}
}

func TestNewAcceptsExactRecoveryTimingBoundaries(t *testing.T) {
	t.Parallel()

	effect := testEffect()
	validDeps := func(config Config) Deps {
		return Deps{
			Store: &fakeStore{effect: effect},
			Sender: fakeSender{send: func(context.Context) (
				pusheffect.ProviderObservation, error,
			) {
				return pusheffect.ProviderObservation{}, nil
			}},
			HistoryResolver: fakeHistoryResolver{
				resolve: func(context.Context) (
					pusheffect.HistoryObservation, error,
				) {
					return pusheffect.HistoryObservation{}, nil
				},
			},
			Config: config,
		}
	}
	configs := []Config{
		{
			ExactTaskID:       effect.TaskID,
			LeaseDuration:     pusheffect.MaxLeaseDuration,
			HistorySkew:       maxHistorySkew,
			ProviderTimeout:   maxExternalTimeout,
			CheckpointTimeout: maxCheckpointTimeout,
		},
		{
			ExactTaskID:       effect.TaskID,
			LeaseDuration:     3 * time.Microsecond,
			RetryAfter:        time.Microsecond,
			HistorySkew:       time.Microsecond,
			ProviderTimeout:   time.Microsecond,
			HistoryTimeout:    time.Microsecond,
			CheckpointTimeout: time.Microsecond,
		},
	}
	for _, config := range configs {
		if _, err := New(validDeps(config)); err != nil {
			t.Fatalf("New() boundary %+v error=%v", config, err)
		}
	}
}

func testEffect() pusheffect.Effect {
	now := time.Now().UTC()
	return pusheffect.Effect{
		Prepared: pusheffect.Prepared{
			ID: "effect-exact", TenantID: 1, UserID: 2,
			TaskID: "task-exact", RunSnapshotID: 3, RunID: "run-exact",
			AppIdentity: "cli_exact", ProviderChatID: "oc_exact",
			Target: "ou_exact", Card: []byte(`{"card":"exact"}`),
			ProviderUUID: "be68a7d2-5535-44cb-a7cb-2d3dbd88a479",
			ObservationEventKeys: []string{
				"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			},
			IdempotencyExpiresAt: now.Add(time.Hour),
		},
		Status: pusheffect.StatusPrepared, Fence: 0, Attempt: 0,
		CreatedAt:  now,
		CardDigest: pusheffect.CardDigest([]byte(`{"card":"exact"}`)),
	}
}
