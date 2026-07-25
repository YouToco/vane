package workflow

import (
	"context"
	"errors"
	"sync"
	"testing"

	"go.temporal.io/sdk/testsuite"

	"github.com/YouToco/vane/feedback"
	"github.com/YouToco/vane/pusheffect"
	"github.com/YouToco/vane/runtimepolicy"
	"github.com/YouToco/vane/types"
)

type pushEffectStoreFake struct {
	mu sync.Mutex

	initialStatus pusheffect.Status
	prepared      []pusheffect.Prepared
	claims        int
	reconciles    int
	definite      int
	ambiguous     int
	receipts      []pusheffect.SentReceipt
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
	_ pusheffect.FailureParams,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.definite++
	return nil
}

func (f *pushEffectStoreFake) RecordPushEffectAmbiguous(
	_ context.Context,
	_ pusheffect.FailureParams,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ambiguous++
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
	legacyStore := &effectCountingStore{fakeStore: new(fakeStore)}
	observation := pusheffect.ProviderObservation{
		Disposition: pusheffect.AttemptSent,
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
		WithPushEffectCanary(effects, identity.TaskID),
	)
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
		effects.reconciles != 0 || len(effects.receipts) != 1 {
		t.Fatalf("effect calls prepared=%d claim=%d reconcile=%d receipts=%d",
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
		string(prepared.Card) != `{"effect_id":"`+marker+`"}` {
		t.Fatalf("prepared checkpoint drift: %+v marker=%q", prepared, marker)
	}
	if effects.receipts[0].ProviderMessageID != "om_effect" {
		t.Fatalf("receipt = %+v", effects.receipts[0])
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

func TestPushEffectReconciliationNeverDowngradesAmbiguous(t *testing.T) {
	identity, ref, snapshot := compiledActivityFixture("Frozen Task")
	compiledStore := &compiledRunStoreFake{
		snapshot: snapshot, authorize: true,
	}
	effects := &pushEffectStoreFake{initialStatus: pusheffect.StatusAmbiguous}
	rejection := pusheffect.ProviderObservation{
		Disposition: pusheffect.AttemptDefiniteNotSent,
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
		WithPushEffectCanary(effects, identity.TaskID),
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
}
