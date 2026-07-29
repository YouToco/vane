package periodicbrief

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"

	"github.com/YouToco/vane/executivebrief"
	"github.com/YouToco/vane/pusheffect"
	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
)

type periodicRecoveryStoreFake struct {
	loaded       store.PeriodicBriefIntentInputsV1
	loadCalls    int
	recovered    *types.PeriodicBriefReportDraftV1
	missing      []types.PeriodicBriefReportV1
	prepared     bool
	settings     store.BriefReportSettingsV1
	claimed      store.PeriodicReportDeliveryV1
	finalStatus  store.PeriodicReportDeliveryStatusV1
	finalMessage string
	policyCalls  int
	claimDigest  string
	profile      *types.Profile
}

func (f *periodicRecoveryStoreFake) ListPeriodicSynthesisRecoveryCandidatesV1(
	context.Context, *store.PeriodicSynthesisRecoveryCursorV1, int,
) ([]store.PeriodicSynthesisRecoveryCandidateV1, error) {
	return nil, nil
}
func (f *periodicRecoveryStoreFake) LoadPeriodicBriefIntentInputsForRecoveryV1(
	context.Context, int64, int64, int64,
) (store.PeriodicBriefIntentInputsV1, error) {
	f.loadCalls++
	return f.loaded, nil
}
func (f *periodicRecoveryStoreFake) GetProfileForTenant(
	context.Context, int64, int64,
) (*types.Profile, error) {
	if f.profile != nil {
		return f.profile, nil
	}
	return nil, types.NewAppError(types.CodeNotFound, "none", nil)
}
func (f *periodicRecoveryStoreFake) LoadPeriodicSynthesisPolicyV1(
	context.Context, int64, int64, string, int64,
) (store.PeriodicSynthesisPolicyV1, error) {
	f.policyCalls++
	return store.PeriodicSynthesisPolicyV1{},
		types.NewAppError(types.CodeNotFound, "none", nil)
}
func (f *periodicRecoveryStoreFake) ClaimPeriodicSynthesisSpendV1(
	_ context.Context, _, _, _ int64, requestDigest string,
	_ int64, _ int64, _ string, _ string,
) (store.PeriodicSynthesisReceiptV1, bool, error) {
	f.claimDigest = requestDigest
	return store.PeriodicSynthesisReceiptV1{}, true, nil
}
func (f *periodicRecoveryStoreFake) RecoverPeriodicBriefReportV1(
	_ context.Context, _, _ int64, _ int64, _ string,
	draft types.PeriodicBriefReportDraftV1,
) (types.PeriodicBriefReportV1, error) {
	f.recovered = &draft
	return draft.Seal(77)
}
func (f *periodicRecoveryStoreFake) ListPeriodicDeliveryRecoveryCandidatesV1(
	context.Context, *store.PeriodicDeliveryRecoveryCursorV1, int,
) ([]store.PeriodicDeliveryRecoveryCandidateV1, error) {
	return nil, nil
}
func (f *periodicRecoveryStoreFake) ListPeriodicMissingDeliveryReportsV1(
	_ context.Context, after int64, _ int,
) ([]types.PeriodicBriefReportV1, error) {
	if len(f.missing) == 0 ||
		after >= f.missing[len(f.missing)-1].ID {
		return nil, nil
	}
	return append([]types.PeriodicBriefReportV1(nil), f.missing...), nil
}
func (f *periodicRecoveryStoreFake) GetBriefReportSettingsV1(
	context.Context, int64, int64, string,
) (store.BriefReportSettingsV1, error) {
	return f.settings, nil
}
func (f *periodicRecoveryStoreFake) LoadGroundedBriefContextV1(
	context.Context, int64, int64, string,
	store.GroundedBriefKindV1, int64,
) (store.GroundedBriefContextV1, error) {
	return store.GroundedBriefContextV1{}, nil
}
func (f *periodicRecoveryStoreFake) PreparePeriodicReportDeliveryV1(
	_ context.Context, report types.PeriodicBriefReportV1,
	_ store.BriefReportDeliveryV1, card []byte,
	providerUUID, appIdentity, targetOpenID, _ string,
	shouldSend bool,
) (store.PeriodicReportDeliveryV1, error) {
	f.prepared = true
	status := store.PeriodicReportDeliverySkipped
	if shouldSend {
		status = store.PeriodicReportDeliveryPrepared
	}
	f.claimed = store.PeriodicReportDeliveryV1{
		ReportID: report.ID, TenantID: report.TenantID,
		UserID: report.UserID, TaskID: report.TaskID,
		CardPayload: card, ProviderUUID: providerUUID,
		AppIdentity: appIdentity, TargetOpenID: targetOpenID,
		Status: status,
	}
	return f.claimed, nil
}
func (f *periodicRecoveryStoreFake) ClaimPeriodicReportDeliveryV1(
	context.Context, int64, int64, int64,
) (store.PeriodicReportDeliveryV1, bool, error) {
	return f.claimed, true, nil
}
func (f *periodicRecoveryStoreFake) FinalizePeriodicReportDeliveryV1(
	_ context.Context, _, _, _ int64,
	status store.PeriodicReportDeliveryStatusV1,
	message string,
) error {
	f.finalStatus, f.finalMessage = status, message
	return nil
}

type periodicDescriberFake struct {
	response *workflowservice.DescribeWorkflowExecutionResponse
	err      error
}

func (f periodicDescriberFake) DescribeWorkflowExecution(
	context.Context, string, string,
) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	return f.response, f.err
}

type periodicRecoverySenderFake struct {
	sendCalls  int
	send       pusheffect.ProviderObservation
	sendErr    error
	history    pusheffect.HistoryObservation
	historyErr error
}

func (*periodicRecoverySenderFake) OwnerOpenID() string { return "ou_1" }
func (*periodicRecoverySenderFake) OwnerChatID() string { return "oc_1" }
func (*periodicRecoverySenderFake) AppIdentity() string { return "app_1" }
func (f *periodicRecoverySenderFake) SendCardWithUUIDResult(
	context.Context, string, string, string, string,
) (pusheffect.ProviderObservation, error) {
	f.sendCalls++
	return f.send, f.sendErr
}
func (f *periodicRecoverySenderFake) ResolvePeriodicReportMessage(
	context.Context, pusheffect.HistoryQuery,
) (pusheffect.HistoryObservation, error) {
	return f.history, f.historyErr
}

func periodicRecoveryBriefFixture(t *testing.T) types.BriefV1 {
	t.Helper()
	at := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	returnValue, err := (types.BriefDraftV1{
		SchemaVersion: types.BriefSchemaVersionV1,
		RunOutcomeID:  1,
		RunSnapshotID: 2,
		PushBatchID:   3,
		TenantID:      4,
		UserID:        5,
		TaskID:        "task-v1",
		GeneratedAt:   at,
		Insights: []types.InsightV1{{
			ID: 6, RankPosition: 1, Title: "供应变化",
			BodyMD: "交期延长", SourceTitle: "Vendor",
			SourceURL: "https://example.com/update", DiscoveredAt: at,
			Structured: &types.StructuredInsightV1{
				SchemaVersion:    types.StructuredInsightSchemaVersionV1,
				BodyMD:           "交期延长",
				WhatChanged:      "交期延长两周",
				WhyItMatters:     "影响采购窗口",
				ImportanceReason: "供应商公告",
				Claims: []types.StructuredClaimV1{{
					Text: "交期延长两周", Excerpt: "delivery plus two weeks",
					SourceRefs: []string{"source-1"},
				}},
			},
		}},
	}).Seal(11)
	if err != nil {
		t.Fatal(err)
	}
	return returnValue
}

func TestRecoveryConvergesStaleSpendingToFallbackWithoutProvider(t *testing.T) {
	brief := periodicRecoveryBriefFixture(t)
	fakeStore := &periodicRecoveryStoreFake{
		loaded: store.PeriodicBriefIntentInputsV1{
			Intent: store.PeriodicBriefIntentV1{
				ID: 9, TenantID: 4, UserID: 5, TaskID: "task-v1",
				Cadence: "weekly", Timezone: "UTC",
				PeriodStart:    brief.GeneratedAt.AddDate(0, 0, -1),
				PeriodEnd:      brief.GeneratedAt.AddDate(0, 0, 1),
				InputDigest:    strings.Repeat("a", 64),
				RunOutcomeIDs:  []int64{8},
				OutcomeDigest:  strings.Repeat("d", 64),
				SourceCoverage: types.RunCompletenessComplete,
				Processing:     types.RunCompletenessComplete,
			},
			Briefs: []types.BriefV1{brief},
		},
	}
	sender := &periodicRecoverySenderFake{}
	runner, err := NewRecoveryRunner(
		fakeStore, periodicDescriberFake{}, sender,
		"https://vane.example", "task-v1", nil)
	if err != nil {
		t.Fatal(err)
	}
	err = runner.recoverOne(t.Context(),
		store.PeriodicSynthesisRecoveryCandidateV1{
			Kind: "spending", IntentID: 9, TenantID: 4, UserID: 5,
			RequestDigest: strings.Repeat("b", 64),
			ProfileDigest: strings.Repeat("c", 64),
			InputDigest:   strings.Repeat("a", 64),
		})
	if err != nil {
		t.Fatal(err)
	}
	if fakeStore.recovered == nil ||
		fakeStore.recovered.GenerationMode !=
			types.ExecutiveGenerationFallback ||
		fakeStore.recovered.Processing != types.RunCompletenessPartial {
		t.Fatalf("recovered draft = %+v", fakeStore.recovered)
	}
	if sender.sendCalls != 0 {
		t.Fatalf("synthesis recovery made %d provider sends", sender.sendCalls)
	}
}

func TestRecoveryConvergesClaimlessSpendingWithExactBriefInput(t *testing.T) {
	brief := periodicRecoveryBriefFixture(t)
	draft := brief.BriefDraftV1
	draft.Insights = append([]types.InsightV1(nil), brief.Insights...)
	draft.Insights[0].Structured = nil
	var err error
	brief, err = draft.Seal(brief.ID)
	if err != nil {
		t.Fatal(err)
	}
	fakeStore := &periodicRecoveryStoreFake{
		loaded: store.PeriodicBriefIntentInputsV1{
			Intent: store.PeriodicBriefIntentV1{
				ID: 9, TenantID: 4, UserID: 5, TaskID: "task-v1",
				Cadence: "weekly", Timezone: "UTC",
				PeriodStart:    brief.GeneratedAt.AddDate(0, 0, -1),
				PeriodEnd:      brief.GeneratedAt.AddDate(0, 0, 1),
				InputDigest:    strings.Repeat("a", 64),
				RunOutcomeIDs:  []int64{8},
				OutcomeDigest:  strings.Repeat("d", 64),
				SourceCoverage: types.RunCompletenessComplete,
				Processing:     types.RunCompletenessComplete,
			},
			Briefs: []types.BriefV1{brief},
		},
	}
	sender := &periodicRecoverySenderFake{}
	runner, err := NewRecoveryRunner(
		fakeStore, periodicDescriberFake{}, sender,
		"https://vane.example", "task-v1", nil)
	if err != nil {
		t.Fatal(err)
	}
	err = runner.recoverOne(t.Context(),
		store.PeriodicSynthesisRecoveryCandidateV1{
			Kind: "spending", IntentID: 9, TenantID: 4, UserID: 5,
			RequestDigest: strings.Repeat("b", 64),
			ProfileDigest: strings.Repeat("c", 64),
			InputDigest:   strings.Repeat("a", 64),
		})
	if err != nil {
		t.Fatal(err)
	}
	recovered := fakeStore.recovered
	if recovered == nil ||
		recovered.GenerationMode != types.ExecutiveGenerationFallback ||
		recovered.Processing != types.RunCompletenessPartial ||
		len(recovered.Inputs) != 1 ||
		recovered.Inputs[0].BriefID != brief.ID ||
		recovered.Inputs[0].Digest != brief.Digest ||
		recovered.Content.DecisionState !=
			types.ExecutiveDecisionInsufficientEvidence ||
		len(recovered.Content.Signals) != 0 ||
		len(recovered.Content.NextSteps) != 0 {
		t.Fatalf("claimless recovered draft = %+v", recovered)
	}
	if sender.sendCalls != 0 {
		t.Fatalf("claimless recovery made %d provider sends", sender.sendCalls)
	}
}

func TestRecoveryConvergesClaimlessPreparedWithWorkflowDigestSemantics(
	t *testing.T,
) {
	brief := periodicRecoveryBriefFixture(t)
	draft := brief.BriefDraftV1
	draft.Insights = append([]types.InsightV1(nil), brief.Insights...)
	structured := *draft.Insights[0].Structured
	structured.Claims = nil
	structured.WhatChanged = ""
	structured.WhyItMatters = ""
	structured.ImportanceReason = ""
	draft.Insights[0].Structured = &structured
	var err error
	brief, err = draft.Seal(brief.ID)
	if err != nil {
		t.Fatal(err)
	}
	inputDigest := strings.Repeat("a", 64)
	fakeStore := &periodicRecoveryStoreFake{
		profile: &types.Profile{
			Industry:       "工业软件",
			Occupation:     "产品负责人",
			Tags:           []string{"AI"},
			Summary:        "关注可信自动化",
			ProfileEpoch:   3,
			ProfileVersion: 5,
		},
		loaded: store.PeriodicBriefIntentInputsV1{
			Intent: store.PeriodicBriefIntentV1{
				ID: 9, TenantID: 4, UserID: 5, TaskID: "task-v1",
				Cadence: "weekly", Timezone: "UTC",
				PeriodStart:    brief.GeneratedAt.AddDate(0, 0, -1),
				PeriodEnd:      brief.GeneratedAt.AddDate(0, 0, 1),
				InputDigest:    inputDigest,
				RunOutcomeIDs:  []int64{8},
				OutcomeDigest:  strings.Repeat("d", 64),
				SourceCoverage: types.RunCompletenessComplete,
				Processing:     types.RunCompletenessComplete,
			},
			Briefs: []types.BriefV1{brief},
		},
	}
	runner, err := NewRecoveryRunner(
		fakeStore,
		periodicDescriberFake{
			response: &workflowservice.DescribeWorkflowExecutionResponse{
				WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
					Execution: &commonpb.WorkflowExecution{
						WorkflowId: "wf", RunId: "run"},
					Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
				},
			},
		},
		&periodicRecoverySenderFake{},
		"https://vane.example", "task-v1", nil)
	if err != nil {
		t.Fatal(err)
	}
	err = runner.recoverOne(t.Context(),
		store.PeriodicSynthesisRecoveryCandidateV1{
			Kind: "prepared", IntentID: 9, TenantID: 4, UserID: 5,
			WorkflowID: "wf", TemporalRunID: "run",
		})
	if err != nil {
		t.Fatal(err)
	}
	profile := executivebrief.ProfileContextV1{
		Epoch: 3, Version: 5,
		Industry: "工业软件", Occupation: "产品负责人",
		Tags: []string{"AI"}, Summary: "关注可信自动化",
	}
	profileDigest, err := executivebrief.ProfileDigestV1(profile)
	if err != nil {
		t.Fatal(err)
	}
	expectedDigest, err := synthesisRequestDigestV1(
		inputDigest, profileDigest, store.PeriodicSynthesisPolicyV1{})
	if err != nil {
		t.Fatal(err)
	}
	if fakeStore.recovered == nil ||
		len(fakeStore.recovered.Inputs) != 1 ||
		fakeStore.recovered.Inputs[0].BriefID != brief.ID ||
		fakeStore.recovered.ProfileEpoch != profile.Epoch ||
		fakeStore.recovered.ProfileVersion != profile.Version ||
		fakeStore.recovered.ProfileDigest != profileDigest ||
		fakeStore.policyCalls != 0 ||
		fakeStore.claimDigest != expectedDigest {
		t.Fatalf(
			"prepared claimless recovery draft=%+v policyCalls=%d digest=%q want=%q",
			fakeStore.recovered, fakeStore.policyCalls,
			fakeStore.claimDigest, expectedDigest)
	}
}

func TestRecoverySkipsRunningExactTemporalExecution(t *testing.T) {
	fakeStore := &periodicRecoveryStoreFake{}
	sender := &periodicRecoverySenderFake{}
	runner, err := NewRecoveryRunner(
		fakeStore,
		periodicDescriberFake{response: &workflowservice.DescribeWorkflowExecutionResponse{
			WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: "wf", RunId: "run"},
				Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			},
		}},
		sender, "https://vane.example", "task-v1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.recoverOne(t.Context(),
		store.PeriodicSynthesisRecoveryCandidateV1{
			Kind: "prepared", IntentID: 9, TenantID: 4, UserID: 5,
			WorkflowID: "wf", TemporalRunID: "run",
		}); err != nil {
		t.Fatal(err)
	}
	if fakeStore.loadCalls != 0 {
		t.Fatal("running execution was recovered")
	}
}

func TestDeliveryRecoveryReusesUUIDAndFinalizesSent(t *testing.T) {
	at := time.Now().Add(-3 * time.Minute)
	fakeStore := &periodicRecoveryStoreFake{
		claimed: store.PeriodicReportDeliveryV1{
			ReportID: 7, TenantID: 4, UserID: 5,
			AppIdentity: "app", TargetOpenID: "open",
			ProviderUUID: "3f2504e0-4f89-41d3-9a0c-0305e82c3301",
			CardPayload:  []byte(`{"schema":"2.0"}`),
		},
	}
	sender := &periodicRecoverySenderFake{
		send: pusheffect.ProviderObservation{
			Disposition: pusheffect.AttemptSent, MessageID: "om_1",
		},
	}
	runner, err := NewRecoveryRunner(
		fakeStore, periodicDescriberFake{err: errors.New("unused")},
		sender, "https://vane.example", "task-v1", nil)
	if err != nil {
		t.Fatal(err)
	}
	err = runner.recoverDeliveryOne(t.Context(),
		store.PeriodicDeliveryRecoveryCandidateV1{
			UpdatedAt: at,
			PeriodicReportDeliveryV1: store.PeriodicReportDeliveryV1{
				ReportID: 7, TenantID: 4, UserID: 5,
				Status: store.PeriodicReportDeliveryPrepared,
			},
		})
	if err != nil {
		t.Fatal(err)
	}
	if sender.sendCalls != 1 ||
		fakeStore.finalStatus != store.PeriodicReportDeliverySent ||
		fakeStore.finalMessage != "om_1" {
		t.Fatalf("delivery recovery send=%d status=%s message=%q",
			sender.sendCalls, fakeStore.finalStatus,
			fakeStore.finalMessage)
	}
}

func TestRecoveryCreatesMissingDeliveryReceiptAndSends(t *testing.T) {
	at := time.Now().Round(0).UTC().Truncate(time.Microsecond)
	report, err := (types.PeriodicBriefReportDraftV1{
		SchemaVersion:  types.PeriodicBriefSchemaVersionV1,
		TenantID:       4,
		UserID:         5,
		TaskID:         "task-v1",
		Cadence:        "weekly",
		Timezone:       "UTC",
		PeriodStart:    at.AddDate(0, 0, -7),
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
	fakeStore := &periodicRecoveryStoreFake{
		missing: []types.PeriodicBriefReportV1{report},
		settings: store.BriefReportSettingsV1{
			Mode:     store.BriefReportModeAuto,
			Cadence:  store.BriefReportCadenceWeekly,
			Delivery: store.BriefReportDeliveryAlways,
			Timezone: "UTC",
		},
	}
	sender := &periodicRecoverySenderFake{
		send: pusheffect.ProviderObservation{
			Disposition: pusheffect.AttemptSent,
			MessageID:   "om_recovered",
		},
	}
	runner, err := NewRecoveryRunner(
		fakeStore, periodicDescriberFake{}, sender,
		"https://vane.example", "task-v1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.runMissingDeliveryPass(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !fakeStore.prepared || sender.sendCalls != 1 ||
		fakeStore.finalStatus != store.PeriodicReportDeliverySent ||
		fakeStore.finalMessage != "om_recovered" {
		t.Fatalf("prepared=%v sends=%d status=%s message=%q",
			fakeStore.prepared, sender.sendCalls,
			fakeStore.finalStatus, fakeStore.finalMessage)
	}
}

func TestRecoveryKeepsMissingDeliveryDarkOutsideRendererCanary(
	t *testing.T,
) {
	fakeStore := &periodicRecoveryStoreFake{
		missing: []types.PeriodicBriefReportV1{
			periodicDeliveryReportFixture(t),
		},
		settings: store.BriefReportSettingsV1{
			Mode:     store.BriefReportModeAuto,
			Cadence:  store.BriefReportCadenceDaily,
			Delivery: store.BriefReportDeliveryAlways,
			Timezone: "UTC",
		},
	}
	sender := &periodicRecoverySenderFake{}
	runner, err := NewRecoveryRunner(
		fakeStore, periodicDescriberFake{}, sender,
		"https://vane.example", "task-other", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.runMissingDeliveryPass(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !fakeStore.prepared ||
		fakeStore.claimed.Status != store.PeriodicReportDeliverySkipped ||
		sender.sendCalls != 0 {
		t.Fatalf("dark report receipt=%v status=%s sends=%d",
			fakeStore.prepared, fakeStore.claimed.Status,
			sender.sendCalls)
	}
}

func TestDeliveryRecoveryLeavesUnknownSendingWhenHistoryUnavailable(
	t *testing.T,
) {
	at := time.Now().Add(-3 * time.Minute)
	fakeStore := &periodicRecoveryStoreFake{}
	sender := &periodicRecoverySenderFake{
		historyErr: errors.New("provider history unavailable"),
	}
	runner, err := NewRecoveryRunner(
		fakeStore, periodicDescriberFake{}, sender,
		"https://vane.example", "task-v1", nil)
	if err != nil {
		t.Fatal(err)
	}
	err = runner.recoverDeliveryOne(
		t.Context(),
		store.PeriodicDeliveryRecoveryCandidateV1{
			UpdatedAt: at,
			PeriodicReportDeliveryV1: store.PeriodicReportDeliveryV1{
				ReportID: 7, TenantID: 4, UserID: 5,
				Status:           store.PeriodicReportDeliverySending,
				AttemptStartedAt: &at,
				ProviderChatID:   "oc_1",
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if fakeStore.finalStatus != "" {
		t.Fatalf("history outage finalized unknown send as %s",
			fakeStore.finalStatus)
	}
}

func TestPreparedDeliveryRecoveryLeavesUnknownSendInSending(
	t *testing.T,
) {
	fakeStore := &periodicRecoveryStoreFake{
		claimed: store.PeriodicReportDeliveryV1{
			ReportID: 7, TenantID: 4, UserID: 5,
			AppIdentity: "app", TargetOpenID: "open",
			ProviderUUID: "3f2504e0-4f89-41d3-9a0c-0305e82c3301",
			CardPayload:  []byte(`{"schema":"2.0"}`),
		},
	}
	sender := &periodicRecoverySenderFake{
		send: pusheffect.ProviderObservation{
			Disposition: pusheffect.AttemptAmbiguous,
		},
		sendErr: errors.New("provider boundary unknown"),
	}
	runner, err := NewRecoveryRunner(
		fakeStore, periodicDescriberFake{}, sender,
		"https://vane.example", "task-v1", nil)
	if err != nil {
		t.Fatal(err)
	}
	err = runner.recoverDeliveryOne(
		t.Context(),
		store.PeriodicDeliveryRecoveryCandidateV1{
			UpdatedAt: time.Now().Add(-3 * time.Minute),
			PeriodicReportDeliveryV1: store.PeriodicReportDeliveryV1{
				ReportID: 7, TenantID: 4, UserID: 5,
				Status: store.PeriodicReportDeliveryPrepared,
			},
		},
	)
	if types.CodeOf(err) == "" {
		t.Fatal("unknown recovery send unexpectedly succeeded")
	}
	if fakeStore.finalStatus != "" {
		t.Fatalf("unknown prepared send finalized as %s",
			fakeStore.finalStatus)
	}
}

func TestDeliveryRecoveryLeavesSendingAfterEmptyHistory(
	t *testing.T,
) {
	at := time.Now().Add(-3 * time.Minute)
	fakeStore := &periodicRecoveryStoreFake{}
	runner, err := NewRecoveryRunner(
		fakeStore, periodicDescriberFake{},
		&periodicRecoverySenderFake{},
		"https://vane.example", "task-v1", nil)
	if err != nil {
		t.Fatal(err)
	}
	err = runner.recoverDeliveryOne(
		t.Context(),
		store.PeriodicDeliveryRecoveryCandidateV1{
			UpdatedAt: at,
			PeriodicReportDeliveryV1: store.PeriodicReportDeliveryV1{
				ReportID: 7, TenantID: 4, UserID: 5,
				Status:           store.PeriodicReportDeliverySending,
				AttemptStartedAt: &at,
				ProviderChatID:   "oc_1",
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if fakeStore.finalStatus != "" {
		t.Fatalf("empty history finalized unknown send as %s",
			fakeStore.finalStatus)
	}
}
