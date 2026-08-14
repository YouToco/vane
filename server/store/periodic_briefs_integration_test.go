package store

import (
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/runtimepolicy"
	"github.com/YouToco/vane/types"
)

func TestPeriodicSynthesisPolicyLoadsExactSnapshotAndRevocationFailsClosed(
	t *testing.T,
) {
	f := newTaskRunSnapshotFixture(t)
	taskID := f.taskID()
	f.createApprovedTask(t, taskID, 1)
	policy := testCompiledRunPolicyV1(t)
	prompt := runtimepolicy.PromptStageV1{
		SystemPrompt:    "frozen periodic prompt",
		RendererVersion: "periodic.render/frozen-v1",
	}
	policy.PromptPolicy.PeriodicSynthesis = &prompt
	policy.ModelPolicy.Calls = append(
		policy.ModelPolicy.Calls,
		runtimepolicy.ModelCallV1{
			Stage: runtimepolicy.ModelStagePeriodicSynthesis,
			Model: "periodic-model-frozen", Temperature: 0.3,
			MaxTokens: 1234, DisableThinking: true,
		})
	if err := policy.Validate(); err != nil {
		t.Fatal(err)
	}
	identity := types.RunIdentity{
		TemporalWorkflowID: "wf-" + taskID,
		TemporalRunID:      "run-periodic-policy",
		RunKind:            types.RunSnapshotKindScheduled,
		TenantID:           f.tenantID,
		UserID:             f.userID,
		TaskID:             taskID,
	}
	ref, err := f.st.CreateOrGetCompiledTaskRunSnapshotV1(
		t.Context(), CreateOrGetCompiledTaskRunSnapshotV1Params{
			Identity: identity, Policy: policy,
		})
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := f.st.LoadPeriodicSynthesisPolicyV1(
		t.Context(), f.tenantID, f.userID, taskID, ref.SnapshotID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.SystemPrompt != prompt.SystemPrompt ||
		loaded.Renderer != prompt.RendererVersion ||
		loaded.ModelCall.Model != "periodic-model-frozen" ||
		!validStoreDigestV1(loaded.PolicyDigest) {
		t.Fatalf("periodic frozen policy = %+v", loaded)
	}
	if _, err := f.st.pool.Exec(t.Context(),
		`DELETE FROM memberships WHERE tenant_id=$1 AND user_id=$2`,
		f.tenantID, f.userID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.st.LoadPeriodicSynthesisPolicyV1(
		t.Context(), f.tenantID, f.userID, taskID,
		ref.SnapshotID); types.CodeOf(err) != types.CodeNotFound {
		t.Fatalf("revoked policy error=%v, want not found", err)
	}
}

func TestPeriodicReportRecoveryFreezesExactCanonicalInputs(t *testing.T) {
	f, marker, briefDraft := executiveSynthesisFixtureV1(t, true)
	prepare := prepareExecutiveSynthesisReceiptV1(
		t, f, marker, briefDraft)
	issueContent := executiveSynthesisContentV1(f.deliveryID[0])
	issueContent.DecisionState =
		types.ExecutiveDecisionInsufficientEvidence
	receipt, err := f.base.st.FinalizeExecutiveSynthesisFallbackV1(
		t.Context(), f.identity, f.ref, marker, issueContent)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.FinalizedAt == nil {
		t.Fatal("executive synthesis receipt is not finalized")
	}
	outcome, err := f.base.st.FinalizeRunOutcomeClaimV1(
		t.Context(), f.identity, f.ref, types.RunOutcomeClaimV1{
			RunOutcomeMarkerV1: marker,
			Result:             types.RunResultContent,
			SourceCoverage:     types.RunCompletenessComplete,
			Processing:         types.RunCompletenessPartial,
		})
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := f.base.st.FreezeExecutiveBriefArtifactRecoveryV1(
		t.Context(), f.identity, f.ref,
		types.ExecutiveBriefArtifactDraftV1{
			SchemaVersion: types.ExecutiveBriefSchemaVersionV1,
			RunOutcomeID:  marker.ID, RunSnapshotID: f.ref.SnapshotID,
			PushBatchID: f.batchID, TenantID: f.identity.TenantID,
			UserID: f.identity.UserID, TaskID: f.identity.TaskID,
			ProfileEpoch:   prepare.ProfileEpoch,
			ProfileVersion: prepare.ProfileVersion,
			ProfileDigest:  prepare.ProfileDigest,
			InputDigest:    prepare.InputDigest,
			GenerationMode: types.ExecutiveGenerationFallback,
			Processing:     types.RunCompletenessPartial,
			GeneratedAt:    *receipt.FinalizedAt,
			Content:        issueContent,
		})
	if err != nil {
		t.Fatal(err)
	}
	var brief types.BriefV1
	var briefPayload []byte
	if err := f.base.st.pool.QueryRow(t.Context(),
		`SELECT payload FROM brief_snapshots WHERE id=$1`,
		artifact.BriefSnapshotID).Scan(&briefPayload); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(briefPayload, &brief); err != nil {
		t.Fatal(err)
	}
	// Period membership is defined by the run outcome's database-clock
	// finalized_at, not the Brief fixture's intentionally frozen GeneratedAt.
	if outcome.FinalizedAt.IsZero() {
		t.Fatal("run outcome is not finalized")
	}
	periodStart := outcome.FinalizedAt
	periodEnd := outcome.FinalizedAt.Add(time.Microsecond)
	if !brief.GeneratedAt.Before(periodStart) &&
		brief.GeneratedAt.Before(periodEnd) {
		t.Fatalf(
			"fixed Brief time %s unexpectedly overlaps outcome window [%s,%s)",
			brief.GeneratedAt, periodStart, periodEnd,
		)
	}
	intent, err := f.base.st.PreparePeriodicBriefIntentV1(
		t.Context(), f.identity.TenantID, f.identity.UserID,
		f.identity.TaskID, BriefReportCadenceWeekly,
		periodStart, periodEnd)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		if _, err := f.base.st.pool.Exec(ctx,
			`UPDATE tenants
			    SET status='deleting',purge_after=clock_timestamp()-interval '1 second'
			  WHERE id=$1`, f.identity.TenantID); err != nil {
			t.Errorf("mark periodic fixture tenant deleting: %v", err)
			return
		}
		if _, err := f.base.st.PurgeTenant(
			ctx, f.identity.TenantID, false); err != nil {
			t.Errorf("purge periodic fixture tenant: %v", err)
		}
	})
	if len(intent.InputBriefIDs) != 1 ||
		intent.InputBriefIDs[0] != brief.ID {
		t.Fatalf("period input Briefs = %v", intent.InputBriefIDs)
	}
	if err := f.base.st.BindPeriodicBriefIntentRunV1(
		t.Context(), f.identity.TenantID, f.identity.UserID,
		intent.ID, "periodic-run-1"); err != nil {
		t.Fatal(err)
	}
	if err := f.base.st.AdoptExistingPeriodicBriefIntentRunV1(
		t.Context(), f.identity.TenantID, f.identity.UserID,
		intent.ID, "periodic-run-from-restart"); err != nil {
		t.Fatal(err)
	}
	sealedIntent, err := f.base.st.PreparePeriodicBriefIntentV1(
		t.Context(), f.identity.TenantID, f.identity.UserID,
		f.identity.TaskID, BriefReportCadenceWeekly,
		periodStart, periodEnd)
	if err != nil {
		t.Fatal(err)
	}
	if sealedIntent.TemporalRunID != "periodic-run-1" {
		t.Fatalf("adopt rewrote sealed run ID: %q",
			sealedIntent.TemporalRunID)
	}
	requestDigest := strings.Repeat("e", 64)
	profileDigest := strings.Repeat("f", 64)
	_, claimed, err := f.base.st.ClaimPeriodicSynthesisSpendV1(
		t.Context(), f.identity.TenantID, f.identity.UserID,
		intent.ID, requestDigest, 0, 0, profileDigest,
		intent.InputDigest)
	if err != nil || !claimed {
		t.Fatalf("periodic claim=%t err=%v", claimed, err)
	}
	content := types.ExecutiveBriefContentV1{
		Headline:         "本周供应变化仍需观察",
		ExecutiveSummary: "已冻结信号显示交期延长。",
		DecisionState:    types.ExecutiveDecisionWatch,
		WhyForYou:        "这影响采购窗口。",
		Signals: []types.ExecutiveSignalV1{{
			Kind:      types.ExecutiveSignalRisk,
			Lifecycle: types.ExecutiveSignalPersistent,
			Title:     "交期风险持续",
			Summary:   "本周仍有可靠证据。",
			EvidenceRefs: []types.ExecutiveEvidenceRefV1{{
				BriefID: brief.ID, InsightID: f.deliveryID[0],
				ClaimIndexes: []int{0},
			}},
		}},
	}
	reportDraft := types.PeriodicBriefReportDraftV1{
		SchemaVersion: types.PeriodicBriefSchemaVersionV1,
		TenantID:      f.identity.TenantID, UserID: f.identity.UserID,
		TaskID: f.identity.TaskID, Cadence: "weekly",
		Timezone: "Asia/Shanghai", PeriodStart: periodStart,
		PeriodEnd: periodEnd, GeneratedAt: time.Now(),
		ProfileDigest: profileDigest, InputDigest: intent.InputDigest,
		Inputs: []types.PeriodicBriefInputV1{{
			BriefID: brief.ID, Digest: brief.Digest,
		}},
		RunOutcomeIDs:  append([]int64(nil), intent.RunOutcomeIDs...),
		OutcomeDigest:  intent.OutcomeDigest,
		GenerationMode: types.ExecutiveGenerationFallback,
		SourceCoverage: types.RunCompletenessComplete,
		Processing:     types.RunCompletenessPartial,
		Content:        content,
	}
	reportDraft, err = reportDraft.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.base.st.FinalizePeriodicBriefReportV1(
		t.Context(), f.identity.TenantID, f.identity.UserID,
		intent.ID, requestDigest, reportDraft, false,
	); types.CodeOf(err) != types.CodeValidation {
		t.Fatalf("fallback draft with model receipt error=%v", err)
	}
	modelDraft := reportDraft
	modelDraft.GenerationMode = types.ExecutiveGenerationModel
	modelDraft.Processing = types.RunCompletenessComplete
	if _, err := f.base.st.RecoverPeriodicBriefReportV1(
		t.Context(), f.identity.TenantID, f.identity.UserID,
		intent.ID, requestDigest, modelDraft,
	); types.CodeOf(err) != types.CodeValidation {
		t.Fatalf("model draft with fallback receipt error=%v", err)
	}
	driftedOutcomeDraft := reportDraft
	driftedOutcomeDraft.RunOutcomeIDs = []int64{
		reportDraft.RunOutcomeIDs[0] + 1,
	}
	driftedOutcomeDraft.OutcomeDigest = strings.Repeat("9", 64)
	if _, err := f.base.st.RecoverPeriodicBriefReportV1(
		t.Context(), f.identity.TenantID, f.identity.UserID,
		intent.ID, requestDigest, driftedOutcomeDraft,
	); types.CodeOf(err) != types.CodeConflict {
		t.Fatalf("drifted periodic outcome set error=%v", err)
	}
	report, err := f.base.st.RecoverPeriodicBriefReportV1(
		t.Context(), f.identity.TenantID, f.identity.UserID,
		intent.ID, requestDigest, reportDraft)
	if err != nil {
		t.Fatal(err)
	}
	if report.Validate() != nil || report.ID <= 0 ||
		report.Inputs[0].BriefID != brief.ID {
		t.Fatalf("periodic report = %+v", report)
	}
	duplicateIntent, err := f.base.st.PreparePeriodicBriefIntentV1(
		t.Context(), f.identity.TenantID, f.identity.UserID,
		f.identity.TaskID, BriefReportCadenceWeekly,
		periodStart.Add(-time.Minute), periodEnd.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if duplicateIntent.InputDigest != intent.InputDigest {
		t.Fatalf("duplicate input digest=%q want=%q",
			duplicateIntent.InputDigest, intent.InputDigest)
	}
	if err := f.base.st.BindPeriodicBriefIntentRunV1(
		t.Context(), f.identity.TenantID, f.identity.UserID,
		duplicateIntent.ID, "periodic-run-duplicate-request"); err != nil {
		t.Fatal(err)
	}
	if _, claimed, claimErr := f.base.st.ClaimPeriodicSynthesisSpendV1(
		t.Context(), f.identity.TenantID, f.identity.UserID,
		duplicateIntent.ID, requestDigest, 0, 0, profileDigest,
		duplicateIntent.InputDigest,
	); claimed || !errors.Is(claimErr, types.ErrConflict) ||
		types.CodeOf(claimErr) != types.CodeConflict {
		t.Fatalf("duplicate request claimed=%t err=%v", claimed, claimErr)
	}
	replayed, err := f.base.st.RecoverPeriodicBriefReportV1(
		t.Context(), f.identity.TenantID, f.identity.UserID,
		intent.ID, requestDigest, reportDraft)
	if err != nil || replayed.Digest != report.Digest {
		t.Fatalf("periodic replay=%+v err=%v", replayed, err)
	}
	card := []byte(`{"type":"template","data":{"template_id":"p2d"}}`)
	providerUUID := uuid.NewString()
	delivery, err := f.base.st.PreparePeriodicReportDeliveryV1(
		t.Context(), report, BriefReportDeliveryImportant, card,
		providerUUID, "app-v1", "open-user-v1", "chat-v1", true)
	if err != nil {
		t.Fatal(err)
	}
	if delivery.Status != PeriodicReportDeliveryPrepared {
		t.Fatalf("periodic delivery status=%s", delivery.Status)
	}
	replayedDelivery, err :=
		f.base.st.PreparePeriodicReportDeliveryV1(
			t.Context(), report, BriefReportDeliveryImportant, card,
			providerUUID, "app-v1", "open-user-v1", "chat-v1", true)
	if err != nil ||
		replayedDelivery.ProviderUUID != delivery.ProviderUUID {
		t.Fatalf("periodic delivery replay=%+v err=%v",
			replayedDelivery, err)
	}
	if _, _, err := f.base.st.ClaimPeriodicReportDeliveryV1(
		t.Context(), report.TenantID, report.UserID+1,
		report.ID); types.CodeOf(err) != types.CodeNotFound {
		t.Fatalf("cross-user periodic delivery claim error=%v", err)
	}

	type claimResult struct {
		authority bool
		err       error
	}
	startClaims := make(chan struct{})
	results := make(chan claimResult, 2)
	var claims sync.WaitGroup
	for range 2 {
		claims.Add(1)
		go func() {
			defer claims.Done()
			<-startClaims
			_, authority, claimErr :=
				f.base.st.ClaimPeriodicReportDeliveryV1(
					t.Context(), report.TenantID, report.UserID,
					report.ID)
			results <- claimResult{authority: authority, err: claimErr}
		}()
	}
	close(startClaims)
	claims.Wait()
	close(results)
	authorities := 0
	for result := range results {
		if result.err != nil {
			t.Fatalf("concurrent periodic delivery claim: %v", result.err)
		}
		if result.authority {
			authorities++
		}
	}
	if authorities != 1 {
		t.Fatalf("periodic delivery authorities=%d, want 1", authorities)
	}
	if err := f.base.st.FinalizePeriodicReportDeliveryV1(
		t.Context(), report.TenantID, report.UserID, report.ID,
		PeriodicReportDeliverySent, "message-v1"); err != nil {
		t.Fatal(err)
	}
	advanced, err := f.base.st.PreparePeriodicReportDeliveryV1(
		t.Context(), report, BriefReportDeliveryImportant, card,
		providerUUID, "app-v1", "open-user-v1", "chat-v1", true)
	if err != nil || advanced.Status != PeriodicReportDeliverySent ||
		advanced.ProviderMessageID != "message-v1" {
		t.Fatalf("advanced periodic delivery replay=%+v err=%v",
			advanced, err)
	}
	if _, err := f.base.st.PreparePeriodicReportDeliveryV1(
		t.Context(), report, BriefReportDeliveryImportant,
		[]byte(`{"different":true}`), providerUUID,
		"app-v1", "open-user-v1", "chat-v1", true,
	); types.CodeOf(err) != types.CodeConflict {
		t.Fatalf("drifted periodic delivery error=%v", err)
	}
}
