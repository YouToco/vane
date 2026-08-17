package periodicbrief

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/YouToco/vane/server/channelruntime"
	"github.com/YouToco/vane/server/pusheffect"
	"github.com/YouToco/vane/server/store"
	"github.com/YouToco/vane/server/types"
)

type periodicDeliveryStoreFake struct {
	settings      store.BriefReportSettingsV1
	preference    store.DeliveryChannelPreference
	plan          store.ArtifactDeliveryPlan
	prepared      store.PeriodicReportDeliveryV1
	shouldSend    bool
	outboundCalls int
	finalizeCalls int
	finalStatus   store.PeriodicReportDeliveryStatusV1
	outboundBody  string
}

func (f *periodicDeliveryStoreFake) ResolveDeliveryChannelPreference(
	context.Context, int64, int64, string,
) (store.DeliveryChannelPreference, error) {
	if f.preference.Selection.Valid() {
		return f.preference, nil
	}
	return store.DeliveryChannelPreference{Selection: store.DeliveryChannelFeishu}, nil
}
func (f *periodicDeliveryStoreFake) PrepareArtifactDeliveryPlan(
	_ context.Context, tenantID, userID int64, taskID, kind, key string,
	preference store.DeliveryChannelPreference,
) (store.ArtifactDeliveryPlan, error) {
	if f.plan.ID != "" {
		return f.plan, nil
	}
	return store.ArtifactDeliveryPlan{
		ID:       "8c925552-f062-5c77-a127-413240cc2604",
		TenantID: tenantID, UserID: userID, TaskID: taskID,
		ArtifactKind: kind, ArtifactKey: key,
		Selection: preference.Selection,
	}, nil
}
func (f *periodicDeliveryStoreFake) PrepareTelegramSendPermit(
	_ context.Context, tenantID, userID, routeID int64,
	effectID, kind, body string,
) (channelruntime.SendPermit, error) {
	f.outboundCalls++
	f.outboundBody = body
	digest := sha256.Sum256([]byte(body))
	return channelruntime.BindDurableSend(channelruntime.ProviderTelegram,
		tenantID, userID, routeID, effectID, kind, hex.EncodeToString(digest[:]))
}

func (f *periodicDeliveryStoreFake) GetBriefReportSettingsV1(
	context.Context, int64, int64, string,
) (store.BriefReportSettingsV1, error) {
	return f.settings, nil
}
func (*periodicDeliveryStoreFake) LoadGroundedBriefContextV1(
	context.Context, int64, int64, string,
	store.GroundedBriefKindV1, int64,
) (store.GroundedBriefContextV1, error) {
	return store.GroundedBriefContextV1{}, nil
}
func (f *periodicDeliveryStoreFake) PreparePeriodicReportDeliveryV1(
	_ context.Context, report types.PeriodicBriefReportV1,
	_ store.BriefReportDeliveryV1, card []byte,
	providerUUID, appIdentity, targetOpenID, providerChatID string,
	shouldSend bool,
) (store.PeriodicReportDeliveryV1, error) {
	f.shouldSend = shouldSend
	status := store.PeriodicReportDeliverySkipped
	if shouldSend {
		status = store.PeriodicReportDeliveryPrepared
	}
	f.prepared = store.PeriodicReportDeliveryV1{
		ReportID: report.ID, TenantID: report.TenantID,
		UserID: report.UserID, TaskID: report.TaskID,
		CardPayload: card, ProviderUUID: providerUUID,
		AppIdentity: appIdentity, TargetOpenID: targetOpenID,
		ProviderChatID: providerChatID, Status: status,
	}
	return f.prepared, nil
}
func (f *periodicDeliveryStoreFake) ClaimPeriodicReportDeliveryV1(
	context.Context, int64, int64, int64,
) (store.PeriodicReportDeliveryV1, bool, error) {
	f.prepared.Status = store.PeriodicReportDeliverySending
	return f.prepared, true, nil
}
func (f *periodicDeliveryStoreFake) FinalizePeriodicReportDeliveryV1(
	_ context.Context, _, _, _ int64,
	status store.PeriodicReportDeliveryStatusV1, _ string,
) error {
	f.finalizeCalls++
	f.finalStatus = status
	return nil
}

type periodicDeliverySenderFake struct {
	observation pusheffect.ProviderObservation
	err         error
	sendCalls   int
}

type periodicTelegramSenderFake struct {
	sendCalls int
	routeID   int64
	kind      string
	body      string
}

func (f *periodicTelegramSenderFake) Send(
	_ context.Context, permit channelruntime.SendPermit,
) (channelruntime.ProviderObservation, error) {
	f.sendCalls++
	f.routeID, f.kind = permit.RouteID(), permit.EffectKind()
	return channelruntime.ProviderObservation{
		Disposition: pusheffect.AttemptSent, AppIdentity: "telegram-test",
		MessageID: "1",
	}, nil
}

func (*periodicDeliverySenderFake) OwnerOpenID() string { return "ou_1" }
func (*periodicDeliverySenderFake) OwnerChatID() string { return "oc_1" }
func (*periodicDeliverySenderFake) AppIdentity() string { return "app_1" }
func (f *periodicDeliverySenderFake) SendCardWithUUIDResult(
	context.Context, string, string, string, string,
) (pusheffect.ProviderObservation, error) {
	f.sendCalls++
	return f.observation, f.err
}

func periodicDeliveryReportFixture(t *testing.T) types.PeriodicBriefReportV1 {
	t.Helper()
	at := time.Date(2026, 7, 28, 0, 0, 0, 0, time.UTC)
	report, err := (types.PeriodicBriefReportDraftV1{
		SchemaVersion:  types.PeriodicBriefSchemaVersionV1,
		TenantID:       4,
		UserID:         5,
		TaskID:         "task-v1",
		Cadence:        "daily",
		Timezone:       "UTC",
		PeriodStart:    at.AddDate(0, 0, -1),
		PeriodEnd:      at,
		GeneratedAt:    at,
		ProfileDigest:  strings.Repeat("a", 64),
		InputDigest:    strings.Repeat("b", 64),
		OutcomeDigest:  strings.Repeat("c", 64),
		GenerationMode: types.ExecutiveGenerationFallback,
		SourceCoverage: types.RunCompletenessComplete,
		Processing:     types.RunCompletenessComplete,
		Content:        quietContentV1(),
	}).Seal(77)
	if err != nil {
		t.Fatal(err)
	}
	return report
}

func TestImportantPeriodicSignalScopesSourceRefsByBrief(t *testing.T) {
	report := types.PeriodicBriefReportV1{
		PeriodicBriefReportDraftV1: types.PeriodicBriefReportDraftV1{
			Content: types.ExecutiveBriefContentV1{
				DecisionState: types.ExecutiveDecisionWatch,
				Signals: []types.ExecutiveSignalV1{{
					Kind:      types.ExecutiveSignalRisk,
					Lifecycle: types.ExecutiveSignalPersistent,
					EvidenceRefs: []types.ExecutiveEvidenceRefV1{
						{BriefID: 11, InsightID: 101, ClaimIndexes: []int{0}},
						{BriefID: 12, InsightID: 102, ClaimIndexes: []int{0}},
					},
				}},
			},
		},
	}
	grounding := store.GroundedBriefContextV1{
		Evidence: []store.GroundedEvidenceBriefV1{
			{
				BriefID: 11,
				Insights: []store.TaskBriefInsightV1{{
					ID: 101,
					Structured: &store.TaskBriefStructuredInsightV1{
						Claims: []types.StructuredClaimV1{{
							SourceRefs: []string{"source-1"},
						}},
					},
				}},
			},
			{
				BriefID: 12,
				Insights: []store.TaskBriefInsightV1{{
					ID: 102,
					Structured: &store.TaskBriefStructuredInsightV1{
						Claims: []types.StructuredClaimV1{{
							SourceRefs: []string{"source-1"},
						}},
					},
				}},
			},
		},
	}
	if !importantPeriodicSignalV1(report, grounding) {
		t.Fatal("two Brief-scoped sources must satisfy important delivery")
	}
}

func TestDeliveryAlwaysSendsQuietReport(t *testing.T) {
	report := periodicDeliveryReportFixture(t)
	deliveryStore := &periodicDeliveryStoreFake{
		settings: store.BriefReportSettingsV1{
			Mode:     store.BriefReportModeAuto,
			Cadence:  store.BriefReportCadenceDaily,
			Delivery: store.BriefReportDeliveryAlways,
			Timezone: "UTC",
		},
	}
	sender := &periodicDeliverySenderFake{
		observation: pusheffect.ProviderObservation{
			Disposition: pusheffect.AttemptSent,
			MessageID:   "om_quiet",
		},
	}
	if err := deliverPeriodicBriefV1(
		t.Context(), report, deliveryStore, sender,
		nil, "https://vane.example", true,
	); err != nil {
		t.Fatal(err)
	}
	if !deliveryStore.shouldSend || sender.sendCalls != 1 ||
		deliveryStore.finalStatus != store.PeriodicReportDeliverySent {
		t.Fatalf("should_send=%v sends=%d final=%s",
			deliveryStore.shouldSend, sender.sendCalls,
			deliveryStore.finalStatus)
	}
}

func TestDeliveryUnknownStaysSendingForHistoryRecovery(t *testing.T) {
	report := periodicDeliveryReportFixture(t)
	deliveryStore := &periodicDeliveryStoreFake{
		settings: store.BriefReportSettingsV1{
			Mode:     store.BriefReportModeAuto,
			Cadence:  store.BriefReportCadenceDaily,
			Delivery: store.BriefReportDeliveryAlways,
			Timezone: "UTC",
		},
	}
	sender := &periodicDeliverySenderFake{
		observation: pusheffect.ProviderObservation{
			Disposition: pusheffect.AttemptAmbiguous,
		},
		err: errors.New("provider boundary unknown"),
	}
	if err := deliverPeriodicBriefV1(
		t.Context(), report, deliveryStore, sender,
		nil, "https://vane.example", true,
	); err == nil {
		t.Fatal("ambiguous provider send unexpectedly succeeded")
	}
	if deliveryStore.finalizeCalls != 0 {
		t.Fatalf("unknown send finalized %d times as %s",
			deliveryStore.finalizeCalls, deliveryStore.finalStatus)
	}
}

func TestDeliveryBothFreezesAndSendsBothProviders(t *testing.T) {
	report := periodicDeliveryReportFixture(t)
	routeID := int64(91)
	deliveryStore := &periodicDeliveryStoreFake{
		settings: store.BriefReportSettingsV1{
			Mode: store.BriefReportModeAuto, Cadence: store.BriefReportCadenceDaily,
			Delivery: store.BriefReportDeliveryAlways, Timezone: "UTC",
		},
		preference: store.DeliveryChannelPreference{
			Selection: store.DeliveryChannelBoth, TelegramRouteID: &routeID,
		},
		plan: store.ArtifactDeliveryPlan{
			ID:        "f02daee4-cbc0-5571-a079-b58760264911",
			Selection: store.DeliveryChannelBoth, TelegramRouteID: &routeID,
		},
	}
	feishuSender := &periodicDeliverySenderFake{
		observation: pusheffect.ProviderObservation{
			Disposition: pusheffect.AttemptSent, MessageID: "om_both",
		},
	}
	telegramSender := &periodicTelegramSenderFake{}
	if err := deliverPeriodicBriefV1(t.Context(), report, deliveryStore,
		feishuSender, telegramSender, "https://vane.example", true); err != nil {
		t.Fatal(err)
	}
	if feishuSender.sendCalls != 1 || telegramSender.sendCalls != 1 ||
		deliveryStore.outboundCalls != 1 || telegramSender.routeID != routeID ||
		telegramSender.kind != "periodic_report" ||
		!strings.Contains(deliveryStore.outboundBody, "https://vane.example/") {
		t.Fatalf("feishu=%d telegram=%+v outbound=%d",
			feishuSender.sendCalls, telegramSender, deliveryStore.outboundCalls)
	}
}

func TestDeliveryTelegramRequiresFrozenRoute(t *testing.T) {
	report := periodicDeliveryReportFixture(t)
	deliveryStore := &periodicDeliveryStoreFake{
		settings: store.BriefReportSettingsV1{
			Mode: store.BriefReportModeAuto, Cadence: store.BriefReportCadenceDaily,
			Delivery: store.BriefReportDeliveryAlways, Timezone: "UTC",
		},
		preference: store.DeliveryChannelPreference{Selection: store.DeliveryChannelTelegram},
		plan: store.ArtifactDeliveryPlan{
			ID: "f02daee4-cbc0-5571-a079-b58760264911", Selection: store.DeliveryChannelTelegram,
		},
	}
	err := deliverPeriodicBriefV1(t.Context(), report, deliveryStore,
		&periodicDeliverySenderFake{}, &periodicTelegramSenderFake{},
		"https://vane.example", true)
	if types.CodeOf(err) != types.CodeConflict {
		t.Fatalf("err=%v code=%s", err, types.CodeOf(err))
	}
}

func TestDeliveryNilTelegramAdapterLeavesDurableEffectAndFailsPartial(t *testing.T) {
	report := periodicDeliveryReportFixture(t)
	routeID := int64(93)
	deliveryStore := &periodicDeliveryStoreFake{
		settings: store.BriefReportSettingsV1{
			Mode: store.BriefReportModeAuto, Cadence: store.BriefReportCadenceDaily,
			Delivery: store.BriefReportDeliveryAlways, Timezone: "UTC",
		},
		preference: store.DeliveryChannelPreference{
			Selection: store.DeliveryChannelTelegram, TelegramRouteID: &routeID,
		},
		plan: store.ArtifactDeliveryPlan{
			ID:        "f02daee4-cbc0-5571-a079-b58760264911",
			Selection: store.DeliveryChannelTelegram, TelegramRouteID: &routeID,
		},
	}
	err := deliverPeriodicBriefV1(t.Context(), report, deliveryStore,
		&periodicDeliverySenderFake{}, nil, "https://vane.example", true)
	if types.CodeOf(err) != types.CodeConflict {
		t.Fatalf("err=%v code=%s", err, types.CodeOf(err))
	}
	if deliveryStore.outboundCalls != 1 || deliveryStore.outboundBody == "" {
		t.Fatalf("nil adapter escaped before durable effect: calls=%d body=%q",
			deliveryStore.outboundCalls, deliveryStore.outboundBody)
	}
}

func TestPeriodicTelegramRendererIncludesDecisionSections(t *testing.T) {
	report := periodicDeliveryReportFixture(t)
	report.Content.WhyForYou = "会影响下一季度预算"
	report.Content.Signals = []types.ExecutiveSignalV1{{
		Title: "价格变化", Summary: "官方价格页已更新",
	}}
	report.Content.NextSteps = []types.ExecutiveNextStepV1{{
		Label: "复核预算", Rationale: "避免超支",
	}}
	body := renderPeriodicTelegramV1(report, "https://vane.example/report")
	for _, want := range []string{
		report.Content.Headline, report.Content.ExecutiveSummary,
		"为什么值得关注", "价格变化：官方价格页已更新",
		"复核预算：避免超支", "https://vane.example/report",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("message missing %q: %s", want, body)
		}
	}
}

func TestActivitiesChannelDispatcherIsRaceSafe(t *testing.T) {
	a := &Activities{}
	sender := &periodicTelegramSenderFake{}
	a.SetChannelDispatcher(sender)
	if got := a.getChannelDispatcher(); got != sender {
		t.Fatalf("sender=%T want %T", got, sender)
	}
}
