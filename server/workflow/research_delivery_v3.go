package workflow

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/server/pusheffect"
	storepkg "github.com/YouToco/vane/server/store"
	"github.com/YouToco/vane/server/types"
)

const (
	researchDeliveryLeaseV3      = 2 * time.Minute
	researchDeliveryCheckpointV3 = 10 * time.Second
)

var researchDeliveryLeaseNamespaceV3 = uuid.MustParse("8ab89a24-e98e-48dc-81de-e61c86f9009e")

type ResearchDeliveryTargetV3 struct {
	Provider       string
	AppIdentity    string
	ProviderChatID string
	Target         string
}

type ResearchDeliveryTargetProviderV3 interface {
	ResearchDeliveryTargetV3(context.Context, types.RunIdentity) (ResearchDeliveryTargetV3, error)
}

type ResearchDeliveryTargetResolverV3 func(
	context.Context, types.RunIdentity,
) (ResearchDeliveryTargetV3, error)

func (f ResearchDeliveryTargetResolverV3) ResearchDeliveryTargetV3(
	ctx context.Context, identity types.RunIdentity,
) (ResearchDeliveryTargetV3, error) {
	if f == nil {
		return ResearchDeliveryTargetV3{}, researchCoordinatorValidationV3(
			"research V3 delivery target resolver is unavailable")
	}
	return f(ctx, identity)
}

type ResearchDeliveryRendererV3 func(types.ResearchBriefPayloadV3) ([]byte, error)

type ResearchDeliverySenderV3 interface {
	PushWithUUID(context.Context, string, string, string, string) (pusheffect.ProviderObservation, error)
}

type ResearchTelegramSenderV3 interface {
	SendTextEffect(
		context.Context, int64, int64, int64, string, string, string,
	) error
}

type ResearchTelegramRendererV3 func(types.ResearchBriefPayloadV3) (string, error)

type researchDeliveryStoreV3 interface {
	ResolveDeliveryChannelPreference(context.Context, int64, int64, string) (storepkg.DeliveryChannelPreference, error)
	PrepareArtifactDeliveryPlan(context.Context, int64, int64, string, string, string,
		storepkg.DeliveryChannelPreference) (storepkg.ArtifactDeliveryPlan, error)
	PrepareTelegramOutbound(context.Context, int64, int64, int64, string, string, string) (storepkg.ChannelOutboundEffect, error)
	LoadResearchBriefPayloadForDeliveryV3(context.Context, types.RunIdentity,
		types.ResearchRunSnapshotRefV3, types.ResearchRunPlanRefV3,
		types.ResearchBriefRefV3) (types.ResearchBriefPayloadV3, error)
	PrepareOrGetResearchBriefDeliveryV3(context.Context,
		storepkg.PrepareResearchBriefDeliveryV3Params) (storepkg.ResearchBriefDeliveryV3, *pusheffect.Effect, error)
	ClaimResearchBriefDeliveryV3(context.Context, types.RunIdentity,
		types.ResearchRunSnapshotRefV3, types.ResearchRunPlanRefV3,
		types.ResearchBriefRefV3, string, time.Duration) (*pusheffect.Effect, error)
	LoadResearchBriefDeliveryV3(context.Context, types.RunIdentity,
		types.ResearchRunSnapshotRefV3, types.ResearchRunPlanRefV3,
		types.ResearchBriefRefV3) (storepkg.ResearchBriefDeliveryV3, error)
	RecordPushEffectSentWithDeliveries(context.Context, pusheffect.SentReceipt) error
	RecordPushEffectDefiniteFailure(context.Context, pusheffect.FailureParams) error
	RecordPushEffectAmbiguous(context.Context, pusheffect.FailureParams) error
}

// ReceiptBackedResearchDeliveryV3 owns the provider boundary only. Scheduler
// cutover remains an independent, default-off authority: constructing this
// coordinator does not make any Workflow eligible to call it.
type ReceiptBackedResearchDeliveryV3 struct {
	store          researchDeliveryStoreV3
	sender         ResearchDeliverySenderV3
	targets        ResearchDeliveryTargetProviderV3
	render         ResearchDeliveryRendererV3
	telegramMu     sync.RWMutex
	telegramSender ResearchTelegramSenderV3
	telegramRender ResearchTelegramRendererV3
}

// SetTelegramDelivery is wired before the Temporal worker starts. Keeping it
// late-bound avoids starting a public Bot ingress before the Agent and task
// control plane have completed composition.
func (d *ReceiptBackedResearchDeliveryV3) SetTelegramDelivery(
	sender ResearchTelegramSenderV3, render ResearchTelegramRendererV3,
) {
	d.telegramMu.Lock()
	defer d.telegramMu.Unlock()
	d.telegramSender, d.telegramRender = sender, render
}

func (d *ReceiptBackedResearchDeliveryV3) telegramDelivery() (
	ResearchTelegramSenderV3, ResearchTelegramRendererV3,
) {
	d.telegramMu.RLock()
	defer d.telegramMu.RUnlock()
	return d.telegramSender, d.telegramRender
}

func NewReceiptBackedResearchDeliveryV3(
	store researchDeliveryStoreV3, sender ResearchDeliverySenderV3,
	targets ResearchDeliveryTargetProviderV3, render ResearchDeliveryRendererV3,
) (*ReceiptBackedResearchDeliveryV3, error) {
	if researchDependencyNilV3(store) || researchDependencyNilV3(sender) ||
		researchDependencyNilV3(targets) || render == nil {
		return nil, errors.New("research V3 delivery dependencies are unavailable")
	}
	return &ReceiptBackedResearchDeliveryV3{
		store: store, sender: sender, targets: targets, render: render,
	}, nil
}

func (d *ReceiptBackedResearchDeliveryV3) Deliver(
	ctx context.Context, identity types.RunIdentity,
	snapshot types.ResearchRunSnapshotRefV3, plan types.ResearchRunPlanRefV3,
	brief ResearchBriefRefV3, traceID string,
) (ResearchDeliveryReceiptV3, error) {
	if d == nil || !brief.DeliveryRequired || strings.TrimSpace(traceID) != traceID ||
		traceID == "" || snapshot.ValidateFor(identity) != nil ||
		plan.ValidateFor(identity, snapshot.SnapshotID) != nil ||
		brief.ValidateFor(identity, snapshot.SnapshotID, plan.PlanID) != nil {
		return ResearchDeliveryReceiptV3{}, researchCoordinatorValidationV3(
			"research V3 delivery scope is invalid")
	}
	payload, err := d.store.LoadResearchBriefPayloadForDeliveryV3(
		ctx, identity, snapshot, plan, brief)
	if err != nil {
		return ResearchDeliveryReceiptV3{}, err
	}
	preference, err := d.store.ResolveDeliveryChannelPreference(
		ctx, identity.TenantID, identity.UserID, identity.TaskID)
	if err != nil {
		return ResearchDeliveryReceiptV3{}, err
	}
	frozen, err := d.store.PrepareArtifactDeliveryPlan(
		ctx, identity.TenantID, identity.UserID, identity.TaskID,
		storepkg.ArtifactDeliveryResearchV3,
		strconv.FormatInt(brief.BriefID, 10)+":"+brief.ReferenceDigest,
		preference)
	if err != nil {
		return ResearchDeliveryReceiptV3{}, err
	}
	if frozen.Selection.Includes("telegram") {
		if frozen.TelegramRouteID == nil {
			return ResearchDeliveryReceiptV3{}, researchCoordinatorValidationV3(
				"research V3 Telegram route is unavailable")
		}
		telegramSender, telegramRender := d.telegramDelivery()
		if telegramSender == nil || telegramRender == nil {
			return ResearchDeliveryReceiptV3{}, types.NewAppError(types.CodeConflict,
				"research V3 Telegram sender is unavailable", types.ErrConflict)
		}
		text, renderErr := telegramRender(payload)
		if renderErr != nil || strings.TrimSpace(text) == "" {
			return ResearchDeliveryReceiptV3{}, researchCoordinatorValidationV3(
				"research V3 Telegram rendering failed")
		}
		effectID := uuid.NewSHA1(uuid.NameSpaceURL,
			[]byte("vane.research-v3-telegram/v1:"+frozen.ID)).String()
		if _, err := d.store.PrepareTelegramOutbound(
			ctx, identity.TenantID, identity.UserID, *frozen.TelegramRouteID,
			effectID, "research_v3", text); err != nil {
			return ResearchDeliveryReceiptV3{}, err
		}
		if err := telegramSender.SendTextEffect(
			ctx, identity.TenantID, identity.UserID, *frozen.TelegramRouteID,
			effectID, "research_v3", text); err != nil {
			return ResearchDeliveryReceiptV3{}, err
		}
		if !frozen.Selection.Includes("feishu") {
			return researchTelegramReceiptV3(frozen, brief)
		}
	}
	if !frozen.Selection.Includes("feishu") {
		return ResearchDeliveryReceiptV3{}, researchCoordinatorValidationV3(
			"research V3 frozen provider set is invalid")
	}
	card, err := d.render(payload)
	if err != nil || len(card) == 0 {
		return ResearchDeliveryReceiptV3{}, researchCoordinatorValidationV3(
			"research V3 delivery rendering failed")
	}
	target, err := d.targets.ResearchDeliveryTargetV3(ctx, identity)
	if err != nil {
		return ResearchDeliveryReceiptV3{}, err
	}
	if target.Provider != "feishu" || target.AppIdentity == "" ||
		target.ProviderChatID == "" || target.Target == "" {
		return ResearchDeliveryReceiptV3{}, researchCoordinatorValidationV3(
			"research V3 delivery target is invalid")
	}
	prepared, effect, err := d.store.PrepareOrGetResearchBriefDeliveryV3(ctx,
		storepkg.PrepareResearchBriefDeliveryV3Params{
			Identity: identity, SnapshotRef: snapshot, PlanRef: plan, BriefRef: brief,
			Provider: target.Provider, AppIdentity: target.AppIdentity,
			ProviderChatID: target.ProviderChatID, Target: target.Target, Card: card,
		})
	if err != nil {
		return ResearchDeliveryReceiptV3{}, err
	}
	if prepared.Status == "sent" {
		return researchDeliveryReceiptFromStoreV3(prepared, brief.BriefID)
	}
	if effect == nil || effect.ID != prepared.EffectID ||
		effect.RunSnapshotID != snapshot.SnapshotID || effect.RunID != identity.TemporalRunID {
		return ResearchDeliveryReceiptV3{}, researchCoordinatorValidationV3(
			"research V3 delivery effect is invalid")
	}
	if effect.Status == pusheffect.StatusSent {
		if err := d.settleSentV3(ctx, effect); err != nil {
			return ResearchDeliveryReceiptV3{}, err
		}
		return d.loadReceiptV3(ctx, identity, snapshot, plan, brief)
	}
	if effect.Status != pusheffect.StatusPrepared &&
		effect.Status != pusheffect.StatusDefiniteFailed {
		// sending/ambiguous can only progress through durable recovery. Never
		// infer not-sent and never issue a second provider request here.
		return ResearchDeliveryReceiptV3{}, types.NewAppError(types.CodeConflict,
			"research V3 delivery requires durable recovery", types.ErrConflict)
	}
	leaseMaterial := identity.TemporalWorkflowID + "\n" + identity.TemporalRunID +
		"\n" + traceID + "\n" + effect.ID
	leaseOwner := "research-delivery/" + uuid.NewSHA1(
		researchDeliveryLeaseNamespaceV3, []byte(leaseMaterial)).String()
	claimed, err := d.store.ClaimResearchBriefDeliveryV3(ctx, identity,
		snapshot, plan, brief, leaseOwner, researchDeliveryLeaseV3)
	if err != nil {
		return ResearchDeliveryReceiptV3{}, err
	}
	if claimed == nil || claimed.Status != pusheffect.StatusSending ||
		claimed.LeaseOwner != leaseOwner || claimed.Fence <= effect.Fence {
		return ResearchDeliveryReceiptV3{}, researchCoordinatorValidationV3(
			"research V3 delivery claim is invalid")
	}
	observation, sendErr := d.sender.PushWithUUID(ctx, claimed.AppIdentity,
		claimed.Target, string(claimed.Card), claimed.ProviderUUID)
	lease := pusheffect.Lease{
		Scope: claimed.Scope(), LeaseOwner: claimed.LeaseOwner, Fence: claimed.Fence,
	}
	checkpointCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx), researchDeliveryCheckpointV3)
	defer cancel()
	if sendErr == nil && validResearchDeliveryObservationV3(claimed, observation) {
		if err := d.store.RecordPushEffectSentWithDeliveries(checkpointCtx,
			pusheffect.SentReceipt{
				Scope: claimed.Scope(), ExpectedFence: claimed.Fence,
				LeaseOwner: claimed.LeaseOwner, ProviderMessageID: observation.MessageID,
			}); err != nil {
			return ResearchDeliveryReceiptV3{}, err
		}
		return d.loadReceiptV3(checkpointCtx, identity, snapshot, plan, brief)
	}
	failure := pusheffect.FailureParams{Lease: lease, Class: "provider_response_unknown"}
	definite := observation.Disposition == pusheffect.AttemptDefiniteNotSent
	if observation.Disposition == pusheffect.AttemptSent {
		failure.Class = "provider_receipt_invalid"
	} else if definite {
		failure.Class = "provider_definite_rejection"
		failure.RetryAfter = 30 * time.Second
	}
	if definite {
		err = d.store.RecordPushEffectDefiniteFailure(checkpointCtx, failure)
	} else {
		err = d.store.RecordPushEffectAmbiguous(checkpointCtx, failure)
	}
	if err != nil {
		return ResearchDeliveryReceiptV3{}, errors.Join(sendErr, err)
	}
	if sendErr != nil {
		return ResearchDeliveryReceiptV3{}, sendErr
	}
	return ResearchDeliveryReceiptV3{}, types.NewAppError(types.CodePushFailed,
		"research V3 provider attempt has no valid receipt", types.ErrConflict)
}

func researchTelegramReceiptV3(
	plan storepkg.ArtifactDeliveryPlan, brief types.ResearchBriefRefV3,
) (ResearchDeliveryReceiptV3, error) {
	material := strings.Join([]string{
		"vane.research-v3-telegram-receipt/v1", plan.ID,
		strconv.FormatInt(brief.BriefID, 10), brief.ReferenceDigest,
	}, "\n")
	digest := sha256.Sum256([]byte(material))
	receipt := ResearchDeliveryReceiptV3{
		PlanID: plan.ID, BriefID: brief.BriefID,
		ReceiptDigest: hex.EncodeToString(digest[:]),
	}
	if err := receipt.Validate(brief.BriefID); err != nil {
		return ResearchDeliveryReceiptV3{}, err
	}
	return receipt, nil
}

func (d *ReceiptBackedResearchDeliveryV3) settleSentV3(
	ctx context.Context, effect *pusheffect.Effect,
) error {
	return d.store.RecordPushEffectSentWithDeliveries(ctx, pusheffect.SentReceipt{
		Scope: effect.Scope(), ExpectedFence: effect.Fence,
		ProviderMessageID: effect.ProviderMessageID,
	})
}

func (d *ReceiptBackedResearchDeliveryV3) loadReceiptV3(
	ctx context.Context, identity types.RunIdentity,
	snapshot types.ResearchRunSnapshotRefV3, plan types.ResearchRunPlanRefV3,
	brief types.ResearchBriefRefV3,
) (ResearchDeliveryReceiptV3, error) {
	row, err := d.store.LoadResearchBriefDeliveryV3(ctx, identity, snapshot, plan, brief)
	if err != nil {
		return ResearchDeliveryReceiptV3{}, err
	}
	return researchDeliveryReceiptFromStoreV3(row, brief.BriefID)
}

func researchDeliveryReceiptFromStoreV3(
	row storepkg.ResearchBriefDeliveryV3, briefID int64,
) (ResearchDeliveryReceiptV3, error) {
	receipt := ResearchDeliveryReceiptV3{
		DeliveryID: row.DeliveryID, BriefID: row.BriefID, ReceiptDigest: row.ReceiptDigest,
	}
	if row.Status != "sent" || row.ID <= 0 || row.DeliveryID <= 0 ||
		row.BriefID != briefID || row.ProviderMessageID == "" ||
		row.SentAt == nil || receipt.Validate(briefID) != nil {
		return ResearchDeliveryReceiptV3{}, researchCoordinatorValidationV3(
			"research V3 delivery receipt is unavailable")
	}
	return receipt, nil
}

func validResearchDeliveryObservationV3(
	effect *pusheffect.Effect, observation pusheffect.ProviderObservation,
) bool {
	return effect != nil && observation.Disposition == pusheffect.AttemptSent &&
		observation.AppIdentity == effect.AppIdentity && observation.MessageID != "" &&
		(observation.ChatID == "" || observation.ChatID == effect.ProviderChatID)
}

func researchDeliveryActionDigestV3(
	brief types.ResearchBriefRefV3, card []byte, target ResearchDeliveryTargetV3,
	effectID string,
) string {
	cardSum := sha256.Sum256(card)
	material := strings.Join([]string{
		"vane.research-delivery-action/v3", brief.ReferenceDigest,
		hex.EncodeToString(cardSum[:]), target.Provider, target.AppIdentity,
		target.ProviderChatID, target.Target, effectID,
	}, "\n")
	sum := sha256.Sum256([]byte(material))
	return hex.EncodeToString(sum[:])
}
