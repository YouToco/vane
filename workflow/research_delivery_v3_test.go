package workflow

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/YouToco/vane/pusheffect"
	storepkg "github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
)

type researchDeliveryStoreFakeV3 struct {
	researchDeliveryStoreV3
	anchor       storepkg.ResearchBriefDeliveryV3
	effect       *pusheffect.Effect
	prepareCalls int
	claimCalls   int
	settleCalls  int
	ambiguous    bool
}

func (f *researchDeliveryStoreFakeV3) LoadResearchBriefPayloadForDeliveryV3(
	context.Context, types.RunIdentity, types.ResearchRunSnapshotRefV3,
	types.ResearchRunPlanRefV3, types.ResearchBriefRefV3,
) (types.ResearchBriefPayloadV3, error) {
	return types.ResearchBriefPayloadV3{
		SchemaVersion: types.ResearchBriefPayloadSchemaV3,
		Headline:      "Kimi update", Summary: "Kimi now has a material pricing update.",
		Significance: types.ResearchBriefSignificanceMajorV3,
		Citations: []types.ResearchBriefCitationV3{{
			Kind: types.ResearchBriefCitationCurrentEvidenceV3, Ref: "1",
		}},
	}, nil
}

func (f *researchDeliveryStoreFakeV3) PrepareOrGetResearchBriefDeliveryV3(
	_ context.Context, params storepkg.PrepareResearchBriefDeliveryV3Params,
) (storepkg.ResearchBriefDeliveryV3, *pusheffect.Effect, error) {
	f.prepareCalls++
	if len(params.Card) == 0 || params.Target == "" {
		return storepkg.ResearchBriefDeliveryV3{}, nil, errors.New("invalid prepare")
	}
	return f.anchor, f.effect, nil
}

func (f *researchDeliveryStoreFakeV3) ClaimResearchBriefDeliveryV3(
	_ context.Context, _ types.RunIdentity, _ types.ResearchRunSnapshotRefV3,
	_ types.ResearchRunPlanRefV3, _ types.ResearchBriefRefV3,
	leaseOwner string, _ time.Duration,
) (*pusheffect.Effect, error) {
	f.claimCalls++
	copy := *f.effect
	copy.Status = pusheffect.StatusSending
	copy.LeaseOwner = leaseOwner
	copy.Fence++
	f.effect = &copy
	return f.effect, nil
}

func (f *researchDeliveryStoreFakeV3) LoadResearchBriefDeliveryV3(
	context.Context, types.RunIdentity, types.ResearchRunSnapshotRefV3,
	types.ResearchRunPlanRefV3, types.ResearchBriefRefV3,
) (storepkg.ResearchBriefDeliveryV3, error) {
	return f.anchor, nil
}

func (f *researchDeliveryStoreFakeV3) RecordPushEffectSentWithDeliveries(
	_ context.Context, receipt pusheffect.SentReceipt,
) error {
	f.settleCalls++
	copy := *f.effect
	copy.Status = pusheffect.StatusSent
	copy.ProviderMessageID = receipt.ProviderMessageID
	copy.LeaseOwner = ""
	f.effect = &copy
	now := time.Now()
	f.anchor.Status = "sent"
	f.anchor.ProviderMessageID = receipt.ProviderMessageID
	f.anchor.ReceiptDigest = strings.Repeat("9", 64)
	f.anchor.SentAt = &now
	return nil
}

func (f *researchDeliveryStoreFakeV3) RecordPushEffectDefiniteFailure(
	context.Context, pusheffect.FailureParams,
) error {
	copy := *f.effect
	copy.Status = pusheffect.StatusDefiniteFailed
	f.effect = &copy
	return nil
}

func (f *researchDeliveryStoreFakeV3) RecordPushEffectAmbiguous(
	context.Context, pusheffect.FailureParams,
) error {
	f.ambiguous = true
	copy := *f.effect
	copy.Status = pusheffect.StatusAmbiguous
	f.effect = &copy
	return nil
}

type researchDeliverySenderFakeV3 struct {
	calls       int
	observation pusheffect.ProviderObservation
	err         error
}

func (f *researchDeliverySenderFakeV3) PushWithUUID(
	context.Context, string, string, string, string,
) (pusheffect.ProviderObservation, error) {
	f.calls++
	return f.observation, f.err
}

type researchDeliveryTargetFakeV3 struct{}

func (researchDeliveryTargetFakeV3) ResearchDeliveryTargetV3() ResearchDeliveryTargetV3 {
	return ResearchDeliveryTargetV3{
		Provider: "feishu", AppIdentity: "cli_a:gen_1",
		ProviderChatID: "oc_owner", Target: "ou_owner",
	}
}

func researchDeliveryFixtureV3(t *testing.T) (
	types.RunIdentity, types.ResearchRunSnapshotRefV3,
	types.ResearchRunPlanRefV3, types.ResearchBriefRefV3,
) {
	t.Helper()
	identity, snapshot, plan, _ := researchBridgeFixtureV3(t)
	brief, err := types.SealResearchBriefRefV3(types.ResearchBriefRefV3{
		BriefID: 31, RunSnapshotID: snapshot.SnapshotID, PlanID: plan.PlanID,
		TemporalWorkflowID: identity.TemporalWorkflowID,
		TemporalRunID:      identity.TemporalRunID, TenantID: identity.TenantID,
		UserID: identity.UserID, TaskID: identity.TaskID,
		DefinitionDigest: snapshot.DefinitionDigest, PlanDigest: plan.PlanDigest,
		RequestDigest: strings.Repeat("1", 64), BriefDigest: strings.Repeat("2", 64),
		EvidenceDigest: strings.Repeat("3", 64), HistoryDigest: strings.Repeat("4", 64),
		NotificationThreshold: "major_updates_only",
		Significance:          types.ResearchBriefSignificanceMajorV3,
		Decision:              types.ResearchBriefDecisionDeliverV3, DeliveryRequired: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return identity, snapshot, plan, brief
}

func newResearchDeliveryFakeV3(t *testing.T) (
	*researchDeliveryStoreFakeV3, types.RunIdentity,
	types.ResearchRunSnapshotRefV3, types.ResearchRunPlanRefV3,
	types.ResearchBriefRefV3,
) {
	t.Helper()
	identity, snapshot, plan, brief := researchDeliveryFixtureV3(t)
	effect := &pusheffect.Effect{Prepared: pusheffect.Prepared{
		ID: "effect-v3", TenantID: identity.TenantID, UserID: identity.UserID,
		TaskID: identity.TaskID, RunSnapshotID: snapshot.SnapshotID,
		RunID: identity.TemporalRunID, AppIdentity: "cli_a:gen_1",
		ProviderChatID: "oc_owner", Target: "ou_owner", Card: []byte(`{"schema":"2.0"}`),
		ProviderUUID: "provider-uuid-v3",
	}, Status: pusheffect.StatusPrepared}
	store := &researchDeliveryStoreFakeV3{
		anchor: storepkg.ResearchBriefDeliveryV3{
			ID: 41, Identity: identity, RunSnapshotID: snapshot.SnapshotID,
			PlanID: plan.PlanID, BriefID: brief.BriefID, EffectID: effect.ID,
			Status: "prepared",
		},
		effect: effect,
	}
	return store, identity, snapshot, plan, brief
}

func TestReceiptBackedResearchDeliveryV3ResponseLostReplayDoesNotResend(t *testing.T) {
	store, identity, snapshot, plan, brief := newResearchDeliveryFakeV3(t)
	sender := &researchDeliverySenderFakeV3{observation: pusheffect.ProviderObservation{
		Disposition: pusheffect.AttemptSent, AppIdentity: "cli_a:gen_1",
		MessageID: "om_delivery", ChatID: "oc_owner",
	}}
	delivery, err := NewReceiptBackedResearchDeliveryV3(store, sender,
		researchDeliveryTargetFakeV3{}, func(types.ResearchBriefPayloadV3) ([]byte, error) {
			return []byte(`{"schema":"2.0"}`), nil
		})
	if err != nil {
		t.Fatal(err)
	}
	store.anchor.DeliveryID = 42
	first, err := delivery.Deliver(t.Context(), identity, snapshot, plan, brief, "trace-v3")
	if err != nil || first.DeliveryID != store.anchor.DeliveryID || sender.calls != 1 ||
		store.claimCalls != 1 || store.settleCalls != 1 {
		t.Fatalf("first receipt=%+v sender=%d claim=%d settle=%d err=%v",
			first, sender.calls, store.claimCalls, store.settleCalls, err)
	}
	second, err := delivery.Deliver(t.Context(), identity, snapshot, plan, brief, "trace-v3")
	if err != nil || second != first || sender.calls != 1 || store.claimCalls != 1 {
		t.Fatalf("response-lost replay=%+v sender=%d claim=%d err=%v",
			second, sender.calls, store.claimCalls, err)
	}
}

func TestReceiptBackedResearchDeliveryV3AmbiguousNeverRetriesProvider(t *testing.T) {
	store, identity, snapshot, plan, brief := newResearchDeliveryFakeV3(t)
	sender := &researchDeliverySenderFakeV3{err: errors.New("response lost")}
	delivery, err := NewReceiptBackedResearchDeliveryV3(store, sender,
		researchDeliveryTargetFakeV3{}, func(types.ResearchBriefPayloadV3) ([]byte, error) {
			return []byte(`{"schema":"2.0"}`), nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := delivery.Deliver(t.Context(), identity, snapshot, plan, brief, "trace-v3"); err == nil ||
		!store.ambiguous || sender.calls != 1 {
		t.Fatalf("ambiguous first call sender=%d marked=%v err=%v", sender.calls, store.ambiguous, err)
	}
	if _, err := delivery.Deliver(t.Context(), identity, snapshot, plan, brief, "trace-v3"); err == nil ||
		sender.calls != 1 {
		t.Fatalf("ambiguous retry sender=%d err=%v", sender.calls, err)
	}
}

func TestReceiptBackedResearchDeliveryV3QuietHasZeroEffects(t *testing.T) {
	store, identity, snapshot, plan, brief := newResearchDeliveryFakeV3(t)
	brief.DeliveryRequired = false
	sender := &researchDeliverySenderFakeV3{}
	delivery, err := NewReceiptBackedResearchDeliveryV3(store, sender,
		researchDeliveryTargetFakeV3{}, func(types.ResearchBriefPayloadV3) ([]byte, error) {
			return []byte(`{"schema":"2.0"}`), nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := delivery.Deliver(t.Context(), identity, snapshot, plan, brief, "trace-v3"); err == nil {
		t.Fatal("quiet Brief unexpectedly reached delivery")
	}
	if store.prepareCalls != 0 || store.claimCalls != 0 || sender.calls != 0 {
		t.Fatalf("quiet Brief effects prepare=%d claim=%d send=%d",
			store.prepareCalls, store.claimCalls, sender.calls)
	}
}
