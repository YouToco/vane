package periodicbrief

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/YouToco/vane/server/pusheffect"
	"github.com/YouToco/vane/server/store"
	"github.com/YouToco/vane/server/types"
)

type periodicDeliveryStoreFake struct {
	settings      store.BriefReportSettingsV1
	prepared      store.PeriodicReportDeliveryV1
	shouldSend    bool
	finalizeCalls int
	finalStatus   store.PeriodicReportDeliveryStatusV1
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
		"https://vane.example", true,
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
		"https://vane.example", true,
	); err == nil {
		t.Fatal("ambiguous provider send unexpectedly succeeded")
	}
	if deliveryStore.finalizeCalls != 0 {
		t.Fatalf("unknown send finalized %d times as %s",
			deliveryStore.finalizeCalls, deliveryStore.finalStatus)
	}
}
