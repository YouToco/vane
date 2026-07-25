package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	"go.temporal.io/sdk/testsuite"

	"github.com/YouToco/vane/feedback"
	"github.com/YouToco/vane/observation"
	"github.com/YouToco/vane/pusheffect"
	"github.com/YouToco/vane/runtimepolicy"
	"github.com/YouToco/vane/types"
)

type pushEffectObservationStoreFake struct {
	mu sync.Mutex

	reserved      int
	boundKey      string
	boundDelivery int64
	marked        int
}

func (*pushEffectObservationStoreFake) PrepareObservationQualificationStep(
	context.Context,
	types.RunIdentity,
	types.RunSnapshotRef,
	string,
	string,
) (string, json.RawMessage, error) {
	return "", nil, errors.New("unexpected qualification preparation")
}

func (*pushEffectObservationStoreFake) MarkObservationQualificationSending(
	context.Context,
	types.RunIdentity,
	types.RunSnapshotRef,
	string,
	string,
) error {
	return errors.New("unexpected qualification send")
}

func (*pushEffectObservationStoreFake) CompleteObservationQualificationStep(
	context.Context,
	types.RunIdentity,
	types.RunSnapshotRef,
	string,
	string,
	json.RawMessage,
) error {
	return errors.New("unexpected qualification completion")
}

func (*pushEffectObservationStoreFake) MarkObservationQualificationUncertain(
	context.Context,
	types.RunIdentity,
	types.RunSnapshotRef,
	string,
	string,
) error {
	return errors.New("unexpected qualification uncertainty")
}

func (f *pushEffectObservationStoreFake) ReserveObservedEventV1(
	_ context.Context,
	_ types.RunIdentity,
	_ types.RunSnapshotRef,
	_ int64,
	_ observation.QualifiedEvent,
) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reserved++
	return true, nil
}

func (f *pushEffectObservationStoreFake) BindObservedEventDeliveryV1(
	_ context.Context,
	_ types.RunIdentity,
	_ types.RunSnapshotRef,
	_ string,
	eventKey string,
	_ int64,
	deliveryID int64,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.boundKey = eventKey
	f.boundDelivery = deliveryID
	return nil
}

func (f *pushEffectObservationStoreFake) MarkObservedEventDeliveredV1(
	context.Context,
	types.RunIdentity,
	types.RunSnapshotRef,
	int64,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.marked++
	return nil
}

type pushEffectStoreFake struct {
	mu sync.Mutex

	initialStatus   pusheffect.Status
	batchStarted    bool
	authorityWinner types.PushBatchDeliveryAuthority
	authorityErr    error
	authorityClaims []types.PushBatchDeliveryAuthority
	prepared        []pusheffect.Prepared
	claims          int
	reconciles      int
	definite        int
	ambiguous       int
	definiteParams  []pusheffect.FailureParams
	ambiguousParams []pusheffect.FailureParams
	receipts        []pusheffect.SentReceipt
}

func (f *pushEffectStoreFake) ClaimPushBatchDeliveryAuthority(
	_ context.Context,
	_ types.PushBatchScope,
	desired types.PushBatchDeliveryAuthority,
) (types.PushBatchDeliveryAuthority, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.authorityClaims = append(f.authorityClaims, desired)
	if f.authorityErr != nil {
		return "", f.authorityErr
	}
	if f.authorityWinner.Valid() {
		return f.authorityWinner, nil
	}
	return desired, nil
}

func (f *pushEffectStoreFake) PushEffectBatchStarted(
	_ context.Context,
	_ int64,
	_ int64,
	_ int64,
) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.batchStarted || len(f.prepared) > 0, nil
}

func (f *pushEffectStoreFake) CreatePushEffect(
	_ context.Context,
	prepared pusheffect.Prepared,
) (*pusheffect.Effect, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.prepared = append(f.prepared, prepared)
	status := f.initialStatus
	if status == "" {
		status = pusheffect.StatusPrepared
	}
	return &pusheffect.Effect{Prepared: prepared, Status: status, Fence: 1}, nil
}

func (f *pushEffectStoreFake) ClaimPushEffect(
	_ context.Context,
	params pusheffect.ClaimParams,
) (*pusheffect.Effect, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.claims++
	prepared := f.prepared[len(f.prepared)-1]
	return &pusheffect.Effect{
		Prepared: prepared, Status: pusheffect.StatusSending,
		LeaseOwner: params.LeaseOwner, Fence: 2,
	}, nil
}

func (f *pushEffectStoreFake) ClaimPushEffectReconciliation(
	_ context.Context,
	params pusheffect.ClaimParams,
) (*pusheffect.Effect, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reconciles++
	prepared := f.prepared[len(f.prepared)-1]
	return &pusheffect.Effect{
		Prepared: prepared, Status: pusheffect.StatusSending,
		LeaseOwner: params.LeaseOwner, Fence: 2,
	}, nil
}

func (f *pushEffectStoreFake) RecordPushEffectDefiniteFailure(
	_ context.Context,
	params pusheffect.FailureParams,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if params.RetryAfter <= 0 {
		return errors.New("store contract: definite failure requires retry backoff")
	}
	f.definite++
	f.definiteParams = append(f.definiteParams, params)
	return nil
}

func (f *pushEffectStoreFake) RecordPushEffectAmbiguous(
	_ context.Context,
	params pusheffect.FailureParams,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if params.RetryAfter != 0 {
		return errors.New("store contract: ambiguous failure forbids retry backoff")
	}
	f.ambiguous++
	f.ambiguousParams = append(f.ambiguousParams, params)
	return nil
}

func (f *pushEffectStoreFake) RecordPushEffectSentWithDeliveries(
	_ context.Context,
	receipt pusheffect.SentReceipt,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.receipts = append(f.receipts, receipt)
	return nil
}

func TestPush_CompiledCanaryUsesDurableEffectAndAtomicReceipt(t *testing.T) {
	identity, ref, snapshot := compiledActivityFixture("Frozen Task")
	compiledStore := &compiledRunStoreFake{
		snapshot: snapshot, authorize: true,
	}
	effects := new(pushEffectStoreFake)
	observed := new(pushEffectObservationStoreFake)
	legacyStore := &effectCountingStore{fakeStore: new(fakeStore)}
	observation := pusheffect.ProviderObservation{
		Disposition: pusheffect.AttemptSent,
		AppIdentity: "cli_test",
		MessageID:   "om_effect",
		ChatID:      "oc_owner",
	}
	pusher := &fakePusher{durableObservation: &observation}
	var marker string
	a := NewActivities(
		fakeFetcher{}, fakeScorer{}, fakeCardGen{}, pusher,
		legacyStore, fakeFeishu{}, nil, nil,
		func(in feedback.AggregateCardInput) string {
			marker = in.EffectID
			return `{"effect_id":"` + in.EffectID + `"}`
		},
		func(string, int) (string, string) { return "title", "blue" },
		WithCompiledRuntimeV1(
			compiledStore,
			func(context.Context, int64, bool) (runtimepolicy.BundleV1, error) {
				return runtimepolicy.BundleV1{}, nil
			},
			new(compiledModelResolverFake),
		),
		WithObservationRuntime(observed, nil, "", ""),
		WithPushEffectCanary(effects, identity.TaskID),
	)
	eventKey := strings.Repeat("b", 64)
	policyDigest := strings.Repeat("c", 64)
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(a.Push)
	err := executePushActivity(t, env, a, PushIn{
		UserID: identity.UserID, ScheduleID: identity.TaskID,
		TraceID: "trace-effect",
		Run: &CompiledRunInputV1{
			TenantID: identity.TenantID, TaskID: identity.TaskID, Snapshot: ref,
		},
		Cards: []GeneratedCard{{
			Scored: types.ScoredItem{
				Item: types.ContentItem{
					ID: 501, SourceID: 10, Title: "item",
					ObservationEventKey:     eventKey,
					ObservationPolicyDigest: policyDigest,
					ObservationEventJSON: json.RawMessage(
						`{"event_type":"release","subject":"item",` +
							`"occurred_at":"2026-07-25T00:00:00Z"}`,
					),
				},
				Score: 88,
			},
			BodyMD: "body",
		}},
	})
	if err != nil {
		t.Fatalf("durable effect Push failed: %v", err)
	}
	effects.mu.Lock()
	defer effects.mu.Unlock()
	if len(effects.prepared) != 1 || effects.claims != 1 ||
		effects.reconciles != 0 || len(effects.receipts) != 1 ||
		len(effects.authorityClaims) != 1 ||
		effects.authorityClaims[0] != types.PushBatchDeliveryAuthorityEffect {
		t.Fatalf("effect calls authority=%v prepared=%d claim=%d reconcile=%d receipts=%d",
			effects.authorityClaims,
			len(effects.prepared), effects.claims,
			effects.reconciles, len(effects.receipts))
	}
	prepared := effects.prepared[0]
	if marker == "" || marker != prepared.ID ||
		prepared.ProviderUUID != marker ||
		prepared.ProviderChatID != "oc_owner" ||
		prepared.AppIdentity != "cli_test" ||
		prepared.RunSnapshotID != ref.SnapshotID ||
		len(prepared.DeliveryIDs) != 1 ||
		len(prepared.ObservationEventKeys) != 1 ||
		prepared.ObservationEventKeys[0] != eventKey ||
		string(prepared.Card) != `{"effect_id":"`+marker+`"}` {
		t.Fatalf("prepared checkpoint drift: %+v marker=%q", prepared, marker)
	}
	if effects.receipts[0].ProviderMessageID != "om_effect" ||
		len(effects.receipts[0].ObservationEventKeys) != 1 ||
		effects.receipts[0].ObservationEventKeys[0] != eventKey {
		t.Fatalf("receipt = %+v", effects.receipts[0])
	}
	observed.mu.Lock()
	reserved := observed.reserved
	boundKey := observed.boundKey
	boundDelivery := observed.boundDelivery
	marked := observed.marked
	observed.mu.Unlock()
	if reserved != 1 || boundKey != eventKey ||
		boundDelivery != prepared.DeliveryIDs[0] || marked != 0 {
		t.Fatalf(
			"observation reserve/bind/legacy-mark=%d/%q/%d/%d",
			reserved,
			boundKey,
			boundDelivery,
			marked,
		)
	}
	compiledStore.mu.Lock()
	deliveryReceipts := compiledStore.deliveryReceipts
	batchStatuses := compiledStore.batchStatuses
	compiledStore.mu.Unlock()
	if deliveryReceipts != 0 || batchStatuses != 1 {
		t.Fatalf("legacy compiled receipts=%d batch statuses=%d",
			deliveryReceipts, batchStatuses)
	}
}

func TestPushBatchAuthorityWinnerOverridesLocalRouting(t *testing.T) {
	tests := []struct {
		name            string
		canarySelected  bool
		winner          types.PushBatchDeliveryAuthority
		wantDesired     types.PushBatchDeliveryAuthority
		wantEffects     int
		wantLegacyMarks int
	}{
		{
			name:            "legacy winner overrides canary",
			canarySelected:  true,
			winner:          types.PushBatchDeliveryAuthorityLegacy,
			wantDesired:     types.PushBatchDeliveryAuthorityEffect,
			wantLegacyMarks: 1,
		},
		{
			name:        "effect winner overrides rollback",
			winner:      types.PushBatchDeliveryAuthorityEffect,
			wantDesired: types.PushBatchDeliveryAuthorityLegacy,
			wantEffects: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			identity, ref, snapshot := compiledActivityFixture("Frozen Task")
			compiledStore := &compiledRunStoreFake{
				snapshot: snapshot, authorize: true,
			}
			effects := &pushEffectStoreFake{
				authorityWinner: test.winner,
			}
			observation := pusheffect.ProviderObservation{
				Disposition: pusheffect.AttemptSent,
				AppIdentity: "cli_test",
				MessageID:   "om_authority",
				ChatID:      "oc_owner",
			}
			pusher := &fakePusher{durableObservation: &observation}
			canaryTaskID := ""
			if test.canarySelected {
				canaryTaskID = identity.TaskID
			}
			a := NewActivities(
				fakeFetcher{},
				fakeScorer{},
				fakeCardGen{},
				pusher,
				&effectCountingStore{fakeStore: new(fakeStore)},
				fakeFeishu{},
				nil,
				nil,
				func(in feedback.AggregateCardInput) string {
					return `{"effect_id":"` + in.EffectID + `"}`
				},
				func(string, int) (string, string) {
					return "title", "blue"
				},
				WithCompiledRuntimeV1(
					compiledStore,
					func(
						context.Context,
						int64,
						bool,
					) (runtimepolicy.BundleV1, error) {
						return runtimepolicy.BundleV1{}, nil
					},
					new(compiledModelResolverFake),
				),
				WithPushEffectCanary(effects, canaryTaskID),
			)
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestActivityEnvironment()
			env.RegisterActivity(a.Push)
			err := executePushActivity(t, env, a, PushIn{
				UserID:     identity.UserID,
				ScheduleID: identity.TaskID,
				TraceID:    "trace-authority-" + test.name,
				Run: &CompiledRunInputV1{
					TenantID: identity.TenantID,
					TaskID:   identity.TaskID,
					Snapshot: ref,
				},
				Cards: []GeneratedCard{{
					Scored: types.ScoredItem{
						Item: types.ContentItem{
							ID: 501, SourceID: 10, Title: "item",
						},
						Score: 88,
					},
					BodyMD: "body",
				}},
			})
			if err != nil {
				t.Fatalf("Push failed: %v", err)
			}
			effects.mu.Lock()
			authorityClaims := append(
				[]types.PushBatchDeliveryAuthority(nil),
				effects.authorityClaims...,
			)
			prepared := len(effects.prepared)
			effects.mu.Unlock()
			if len(authorityClaims) != 1 ||
				authorityClaims[0] != test.wantDesired ||
				prepared != test.wantEffects {
				t.Fatalf(
					"authority/prepared=%v/%d, want %q/%d",
					authorityClaims,
					prepared,
					test.wantDesired,
					test.wantEffects,
				)
			}
			compiledStore.mu.Lock()
			legacyMarks := compiledStore.deliveryReceipts
			compiledStore.mu.Unlock()
			if legacyMarks != test.wantLegacyMarks {
				t.Fatalf(
					"legacy delivery receipts=%d, want %d",
					legacyMarks,
					test.wantLegacyMarks,
				)
			}
		})
	}
}

func TestPushEffectReconciliationNeverDowngradesAmbiguous(t *testing.T) {
	identity, ref, snapshot := compiledActivityFixture("Frozen Task")
	compiledStore := &compiledRunStoreFake{
		snapshot: snapshot, authorize: true,
	}
	effects := &pushEffectStoreFake{
		initialStatus: pusheffect.StatusAmbiguous,
		batchStarted:  true,
	}
	rejection := pusheffect.ProviderObservation{
		Disposition: pusheffect.AttemptDefiniteNotSent,
		AppIdentity: "cli_test",
	}
	pusher := &fakePusher{
		durableObservation: &rejection,
		durableErr: types.NewAppError(
			types.CodePushFailed, "provider rejected replay", errors.New("rejected"),
		),
	}
	a := NewActivities(
		fakeFetcher{}, fakeScorer{}, fakeCardGen{}, pusher,
		&effectCountingStore{fakeStore: new(fakeStore)},
		fakeFeishu{}, nil, nil,
		func(in feedback.AggregateCardInput) string {
			return `{"effect_id":"` + in.EffectID + `"}`
		},
		func(string, int) (string, string) { return "title", "blue" },
		WithCompiledRuntimeV1(
			compiledStore,
			func(context.Context, int64, bool) (runtimepolicy.BundleV1, error) {
				return runtimepolicy.BundleV1{}, nil
			},
			new(compiledModelResolverFake),
		),
		// Configuration rollback stops admitting new batches. The durable
		// batch latch must still resume this already-started effect and must
		// never fall through to legacy no-UUID Push.
		WithPushEffectCanary(effects, ""),
	)
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(a.Push)
	err := executePushActivity(t, env, a, PushIn{
		UserID: identity.UserID, ScheduleID: identity.TaskID,
		TraceID: "trace-effect-reconcile",
		Run: &CompiledRunInputV1{
			TenantID: identity.TenantID, TaskID: identity.TaskID, Snapshot: ref,
		},
		Cards: []GeneratedCard{{
			Scored: types.ScoredItem{
				Item: types.ContentItem{ID: 501, SourceID: 10}, Score: 88,
			},
			BodyMD: "body",
		}},
	})
	if err == nil {
		t.Fatal("provider rejection should fail this Activity attempt")
	}
	effects.mu.Lock()
	defer effects.mu.Unlock()
	if effects.reconciles != 1 || effects.ambiguous != 1 ||
		effects.definite != 0 {
		t.Fatalf("ambiguous downgrade: reconcile=%d ambiguous=%d definite=%d",
			effects.reconciles, effects.ambiguous, effects.definite)
	}
	if len(effects.ambiguousParams) != 1 ||
		effects.ambiguousParams[0].RetryAfter != 0 {
		t.Fatalf("ambiguous failure retry boundary=%v, want zero",
			effects.ambiguousParams)
	}
}

func TestPushEffectDefiniteFailureRetainsRetryBackoff(t *testing.T) {
	identity, ref, snapshot := compiledActivityFixture("Frozen Task")
	compiledStore := &compiledRunStoreFake{
		snapshot: snapshot, authorize: true,
	}
	effects := new(pushEffectStoreFake)
	rejection := pusheffect.ProviderObservation{
		Disposition: pusheffect.AttemptDefiniteNotSent,
		AppIdentity: "cli_test",
	}
	pusher := &fakePusher{
		durableObservation: &rejection,
		durableErr: types.NewAppError(
			types.CodePushFailed, "provider rejected request", errors.New("rejected"),
		),
	}
	a := NewActivities(
		fakeFetcher{}, fakeScorer{}, fakeCardGen{}, pusher,
		&effectCountingStore{fakeStore: new(fakeStore)},
		fakeFeishu{}, nil, nil,
		func(in feedback.AggregateCardInput) string {
			return `{"effect_id":"` + in.EffectID + `"}`
		},
		func(string, int) (string, string) { return "title", "blue" },
		WithCompiledRuntimeV1(
			compiledStore,
			func(context.Context, int64, bool) (runtimepolicy.BundleV1, error) {
				return runtimepolicy.BundleV1{}, nil
			},
			new(compiledModelResolverFake),
		),
		WithPushEffectCanary(effects, identity.TaskID),
	)
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(a.Push)
	err := executePushActivity(t, env, a, PushIn{
		UserID: identity.UserID, ScheduleID: identity.TaskID,
		TraceID: "trace-effect-definite-failure",
		Run: &CompiledRunInputV1{
			TenantID: identity.TenantID, TaskID: identity.TaskID, Snapshot: ref,
		},
		Cards: []GeneratedCard{{
			Scored: types.ScoredItem{
				Item: types.ContentItem{ID: 501, SourceID: 10}, Score: 88,
			},
			BodyMD: "body",
		}},
	})
	if err == nil {
		t.Fatal("provider rejection should fail this Activity attempt")
	}
	effects.mu.Lock()
	defer effects.mu.Unlock()
	if effects.definite != 1 || effects.ambiguous != 0 ||
		len(effects.definiteParams) != 1 ||
		effects.definiteParams[0].RetryAfter != pushEffectRetryAfter {
		t.Fatalf(
			"definite failure boundary definite=%d ambiguous=%d params=%+v",
			effects.definite, effects.ambiguous, effects.definiteParams,
		)
	}
}

func TestValidPushEffectSentObservationBindsFrozenProviderIdentity(t *testing.T) {
	effect := pusheffect.Effect{Prepared: pusheffect.Prepared{
		AppIdentity:    "cli_frozen",
		ProviderChatID: "oc_frozen",
	}}
	base := pusheffect.ProviderObservation{
		Disposition: pusheffect.AttemptSent,
		AppIdentity: "cli_frozen",
		MessageID:   "om_receipt",
		ChatID:      "oc_frozen",
	}
	if !validPushEffectSentObservation(&effect, base) {
		t.Fatal("exact provider observation was rejected")
	}
	tests := []struct {
		name   string
		mutate func(*pusheffect.ProviderObservation)
	}{
		{
			name: "missing app identity",
			mutate: func(observation *pusheffect.ProviderObservation) {
				observation.AppIdentity = ""
			},
		},
		{
			name: "wrong app identity",
			mutate: func(observation *pusheffect.ProviderObservation) {
				observation.AppIdentity = "cli_other"
			},
		},
		{
			name: "wrong chat",
			mutate: func(observation *pusheffect.ProviderObservation) {
				observation.ChatID = "oc_other"
			},
		},
		{
			name: "missing message receipt",
			mutate: func(observation *pusheffect.ProviderObservation) {
				observation.MessageID = ""
			},
		},
		{
			name: "non-sent disposition",
			mutate: func(observation *pusheffect.ProviderObservation) {
				observation.Disposition = pusheffect.AttemptAmbiguous
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			observation := base
			test.mutate(&observation)
			if validPushEffectSentObservation(&effect, observation) {
				t.Fatalf("non-exact observation accepted: %+v", observation)
			}
		})
	}
	base.ChatID = ""
	if !validPushEffectSentObservation(&effect, base) {
		t.Fatal("provider receipt without supplementary ChatID was rejected")
	}
}
