package store

import (
	"strings"
	"testing"
	"time"

	"github.com/YouToco/vane/types"
)

func executiveSynthesisFixtureV1(
	t *testing.T,
) (*canonicalBriefFixture, types.RunOutcomeMarkerV1, types.BriefDraftV1) {
	t.Helper()
	f := newCanonicalBriefFixture(t, 1)
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(ctx, t, f.base.st,
			`DELETE FROM executive_brief_artifacts WHERE tenant_id=$1`,
			f.identity.TenantID)
		cleanupExec(ctx, t, f.base.st,
			`DELETE FROM executive_brief_synthesis_receipts
			  WHERE tenant_id=$1`, f.identity.TenantID)
	})
	const excerpt = "verified source excerpt"
	if _, err := f.base.st.pool.Exec(t.Context(),
		`UPDATE content_items SET content=$2 WHERE id=$1`,
		f.contentID[0], excerpt); err != nil {
		t.Fatal(err)
	}
	marker, err := f.base.st.CreatePendingRunOutcomeV1(
		t.Context(), f.identity, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	structured, err := types.SealStructuredInsightEvidenceV1(
		types.StructuredInsightV1{
			SchemaVersion:    types.StructuredInsightSchemaVersionV1,
			BodyMD:           f.bodyMD[0],
			WhatChanged:      "A verified change occurred.",
			WhyItMatters:     "It affects the monitored decision.",
			ImportanceReason: "The source directly supports the claim.",
			Claims: []types.StructuredClaimV1{{
				Text: "A verified change occurred.", Excerpt: excerpt,
				SourceRefs: []string{"source-1"},
			}},
		},
		map[string]string{"source-1": excerpt},
	)
	if err != nil {
		t.Fatal(err)
	}
	generatedAt := time.Date(
		2026, 7, 28, 12, 0, 0, 123456000, time.UTC)
	draft, err := f.base.st.PrepareBriefDraftV2(
		t.Context(), f.identity, f.ref, marker, f.batchID, generatedAt,
		[]int64{f.deliveryID[0]},
		map[int64]types.StructuredInsightV1{f.deliveryID[0]: structured},
		map[int64]map[string]string{
			f.deliveryID[0]: {"source-1": excerpt},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	return f, marker, draft
}

func executiveSynthesisContentV1(
	insightID int64,
) types.ExecutiveBriefContentV1 {
	ref := types.ExecutiveEvidenceRefV1{
		InsightID: insightID, ClaimIndexes: []int{0},
	}
	return types.ExecutiveBriefContentV1{
		Headline:         "The monitored change needs review.",
		ExecutiveSummary: "One verified signal may affect the decision.",
		DecisionState:    types.ExecutiveDecisionWatch,
		WhyForYou:        "It matches the monitored scope.",
		Signals: []types.ExecutiveSignalV1{{
			Kind:  types.ExecutiveSignalChange,
			Title: "Verified change", Summary: "Review the source-backed change.",
			EvidenceRefs: []types.ExecutiveEvidenceRefV1{ref},
		}},
		NextSteps: []types.ExecutiveNextStepV1{{
			Kind:  types.ExecutiveNextStepDeepDive,
			Label: "深入了解", Rationale: "Inspect the verified evidence.",
			EvidenceRefs: []types.ExecutiveEvidenceRefV1{ref},
		}},
	}
}

func prepareExecutiveSynthesisReceiptV1(
	t *testing.T,
	f *canonicalBriefFixture,
	marker types.RunOutcomeMarkerV1,
	draft types.BriefDraftV1,
) ExecutiveSynthesisPrepareV1 {
	t.Helper()
	inputDigest, err := ExecutiveSynthesisInputDigestV1(draft)
	if err != nil {
		t.Fatal(err)
	}
	profileDigest := strings.Repeat("a", 64)
	requestDigest, err := ExecutiveSynthesisRequestDigestV1(
		profileDigest, inputDigest, "issue-synthesis/v1")
	if err != nil {
		t.Fatal(err)
	}
	prepare := ExecutiveSynthesisPrepareV1{
		Marker: marker, PushBatchID: f.batchID,
		ProfileEpoch: 2, ProfileVersion: 7,
		ProfileDigest: profileDigest, InputDigest: inputDigest,
		RequestDigest: requestDigest,
	}
	prepared, err := f.base.st.PrepareExecutiveSynthesisV1(
		t.Context(), f.identity, f.ref, prepare)
	if err != nil || prepared.Status != ExecutiveSynthesisPrepared {
		t.Fatalf("prepare = %+v, err=%v", prepared, err)
	}
	replayed, err := f.base.st.PrepareExecutiveSynthesisV1(
		t.Context(), f.identity, f.ref, prepare)
	if err != nil || replayed.ExecutiveSynthesisPrepareV1 != prepare {
		t.Fatalf("prepare replay = %+v, err=%v", replayed, err)
	}
	return prepare
}

func TestExecutiveSynthesisAtMostOnceLifecycleAndArtifactReplay(
	t *testing.T,
) {
	f, marker, briefDraft := executiveSynthesisFixtureV1(t)
	prepare := prepareExecutiveSynthesisReceiptV1(
		t, f, marker, briefDraft)
	spending, claimed, err := f.base.st.ClaimExecutiveSynthesisSpendV1(
		t.Context(), f.identity, f.ref, marker)
	if err != nil || !claimed ||
		spending.Status != ExecutiveSynthesisSpending {
		t.Fatalf("claim = %+v/%t, err=%v", spending, claimed, err)
	}
	replayedSpend, claimedAgain, err :=
		f.base.st.ClaimExecutiveSynthesisSpendV1(
			t.Context(), f.identity, f.ref, marker)
	if err != nil || claimedAgain ||
		replayedSpend.Status != ExecutiveSynthesisSpending {
		t.Fatalf("claim replay = %+v/%t, err=%v",
			replayedSpend, claimedAgain, err)
	}
	content := executiveSynthesisContentV1(f.deliveryID[0])
	forged := content
	forged.Signals = append([]types.ExecutiveSignalV1(nil), content.Signals...)
	forged.Signals[0].EvidenceRefs = []types.ExecutiveEvidenceRefV1{{
		InsightID: f.deliveryID[0], ClaimIndexes: []int{1},
	}}
	if _, err := f.base.st.FinalizeExecutiveSynthesisV1(
		t.Context(), f.identity, f.ref, marker, forged); err == nil {
		t.Fatal("finalize admitted an out-of-range frozen claim reference")
	}
	finalized, err := f.base.st.FinalizeExecutiveSynthesisV1(
		t.Context(), f.identity, f.ref, marker, content)
	if err != nil || finalized.Status != ExecutiveSynthesisFinalized ||
		finalized.FinalizedAt == nil {
		t.Fatalf("finalize = %+v, err=%v", finalized, err)
	}
	replayedFinal, err := f.base.st.FinalizeExecutiveSynthesisV1(
		t.Context(), f.identity, f.ref, marker, content)
	if err != nil || replayedFinal.FinalizedAt == nil ||
		!replayedFinal.FinalizedAt.Equal(*finalized.FinalizedAt) {
		t.Fatalf("finalize replay = %+v, err=%v", replayedFinal, err)
	}
	claim := types.RunOutcomeClaimV1{
		RunOutcomeMarkerV1: marker,
		Result:             types.RunResultContent,
		SourceCoverage:     types.RunCompletenessComplete,
		Processing:         types.RunCompletenessComplete,
	}
	if _, err := f.base.st.FinalizeRunOutcomeClaimV1(
		t.Context(), f.identity, f.ref, claim); err != nil {
		t.Fatal(err)
	}
	artifactDraft := types.ExecutiveBriefArtifactDraftV1{
		SchemaVersion: types.ExecutiveBriefSchemaVersionV1,
		RunOutcomeID:  marker.ID, RunSnapshotID: f.ref.SnapshotID,
		PushBatchID: f.batchID, TenantID: f.identity.TenantID,
		UserID: f.identity.UserID, TaskID: f.identity.TaskID,
		ProfileEpoch: prepare.ProfileEpoch, ProfileVersion: prepare.ProfileVersion,
		ProfileDigest: prepare.ProfileDigest, InputDigest: prepare.InputDigest,
		GenerationMode: types.ExecutiveGenerationModel,
		Processing:     types.RunCompletenessComplete,
		GeneratedAt:    *finalized.FinalizedAt, Content: content,
	}
	artifact, err := f.base.st.FreezeExecutiveBriefArtifactRecoveryV1(
		t.Context(), f.identity, f.ref, artifactDraft)
	if err != nil {
		t.Fatal(err)
	}
	if artifact.BriefSnapshotID <= 0 ||
		artifact.Content.Signals[0].EvidenceRefs[0].BriefID !=
			artifact.BriefSnapshotID {
		t.Fatalf("artifact lacks canonical Brief binding: %+v", artifact)
	}
	replayedArtifact, err := f.base.st.FreezeExecutiveBriefArtifactV1(
		t.Context(), f.identity, f.ref, artifactDraft)
	if err != nil || replayedArtifact.Digest != artifact.Digest {
		t.Fatalf("artifact replay = %+v, err=%v", replayedArtifact, err)
	}
}

func TestExecutiveSynthesisFallbackDoesNotRequireSpendClaim(t *testing.T) {
	f, marker, draft := executiveSynthesisFixtureV1(t)
	prepareExecutiveSynthesisReceiptV1(t, f, marker, draft)
	fallback := executiveSynthesisContentV1(f.deliveryID[0])
	fallback.DecisionState = types.ExecutiveDecisionInsufficientEvidence
	fallback.Headline = "已有可靠情报，综合暂不完整"
	fallback.ExecutiveSummary = "保留已验证内容，稍后可继续查看。"
	fallback.WhyForYou = "当前画像依据有限。"
	receipt, err := f.base.st.FinalizeExecutiveSynthesisFallbackV1(
		t.Context(), f.identity, f.ref, marker, fallback)
	if err != nil || receipt.Status != ExecutiveSynthesisFallback ||
		receipt.SpendingStartedAt != nil {
		t.Fatalf("fallback = %+v, err=%v", receipt, err)
	}
}
