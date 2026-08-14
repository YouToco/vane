package executivebriefrecovery

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/YouToco/vane/server/store"
	"github.com/YouToco/vane/server/types"
)

type recoveryStoreFake struct {
	mu         sync.Mutex
	candidates []store.ExecutiveSynthesisRecoveryCandidateV1
	draft      types.BriefDraftV1
	receipt    store.ExecutiveSynthesisReceiptV1
	recovered  int
	frozen     int
}

func (f *recoveryStoreFake) ListExecutiveSynthesisRecoveryCandidatesV1(
	_ context.Context,
	cursor *store.ExecutiveSynthesisRecoveryCursorV1,
	_ int,
) ([]store.ExecutiveSynthesisRecoveryCandidateV1, error) {
	if cursor != nil {
		return nil, nil
	}
	return append(
		[]store.ExecutiveSynthesisRecoveryCandidateV1(nil),
		f.candidates...), nil
}

func (f *recoveryStoreFake) LoadPreparedBriefDraftV1(
	context.Context, types.RunIdentity, types.RunSnapshotRef,
	types.RunOutcomeMarkerV1,
) (types.BriefDraftV1, bool, error) {
	return f.draft, true, nil
}

func (f *recoveryStoreFake) LoadExecutiveSynthesisReceiptV1(
	context.Context, types.RunIdentity, types.RunSnapshotRef,
	types.RunOutcomeMarkerV1,
) (store.ExecutiveSynthesisReceiptV1, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.receipt, nil
}

func (f *recoveryStoreFake) GetProfileForTenant(
	context.Context, int64, int64,
) (*types.Profile, error) {
	return nil, types.NewAppError(types.CodeNotFound, "missing", nil)
}

func (f *recoveryStoreFake) PrepareExecutiveSynthesisRecoveryV1(
	_ context.Context, _ types.RunIdentity, _ types.RunSnapshotRef,
	prepare store.ExecutiveSynthesisPrepareV1,
) (store.ExecutiveSynthesisReceiptV1, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.receipt = store.ExecutiveSynthesisReceiptV1{
		ExecutiveSynthesisPrepareV1: prepare,
		Status:                      store.ExecutiveSynthesisPrepared,
	}
	return f.receipt, nil
}

func (f *recoveryStoreFake) RecoverExecutiveSynthesisFallbackV1(
	_ context.Context, _ types.RunIdentity, _ types.RunSnapshotRef,
	_ types.RunOutcomeMarkerV1, content types.ExecutiveBriefContentV1,
) (store.ExecutiveSynthesisReceiptV1, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := time.Unix(20, 0).UTC()
	f.recovered++
	f.receipt.Status = store.ExecutiveSynthesisFallback
	f.receipt.GenerationMode = types.ExecutiveGenerationFallback
	f.receipt.Processing = types.RunCompletenessPartial
	f.receipt.Content = &content
	f.receipt.FinalizedAt = &now
	return f.receipt, nil
}

func (f *recoveryStoreFake) FreezeExecutiveBriefArtifactRecoveryV1(
	_ context.Context, _ types.RunIdentity, _ types.RunSnapshotRef,
	draft types.ExecutiveBriefArtifactDraftV1,
) (types.ExecutiveBriefArtifactV1, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.frozen++
	return draft.Seal(1, 2)
}

func TestRunnerConvergesClaimlessStaleSpendToFallbackAndFreeze(t *testing.T) {
	marker := types.RunOutcomeMarkerV1{
		ID: 1, SchemaVersion: types.RunOutcomeSchemaVersionV1,
		RunSnapshotID: 2, TenantID: 3, UserID: 4, TaskID: "task-a",
	}
	ref := types.RunSnapshotRef{
		SchemaVersion: types.RunSnapshotSchemaVersion, SnapshotID: 2,
		ReferenceDigest: strings.Repeat("a", 64),
		PayloadDigest:   strings.Repeat("b", 64),
	}
	identity := types.RunIdentity{
		TemporalWorkflowID: "wf", TemporalRunID: "run",
		RunKind:  types.RunSnapshotKindScheduled,
		TenantID: 3, UserID: 4, TaskID: "task-a",
	}
	structured, err := types.SealStructuredInsightEvidenceV1(
		types.StructuredInsightV1{
			SchemaVersion: types.StructuredInsightSchemaVersionV1,
			BodyMD:        "body",
			WhatChanged:   "", WhyItMatters: "",
			ImportanceReason: "", Claims: nil,
		}, map[string]string{"source-1": "evidence"})
	if err != nil {
		t.Fatal(err)
	}
	draft := types.BriefDraftV1{
		SchemaVersion: types.BriefSchemaVersionV1,
		RunOutcomeID:  1, RunSnapshotID: 2, PushBatchID: 5,
		TenantID: 3, UserID: 4, TaskID: "task-a",
		GeneratedAt: time.Unix(10, 0).UTC(),
		Insights: []types.InsightV1{{
			ID: 6, RankPosition: 1, Title: "title",
			BodyMD: "body", SourceTitle: "source",
			SourceURL:    "https://example.com",
			DiscoveredAt: time.Unix(9, 0).UTC(),
			Structured:   &structured,
		}},
	}
	fake := &recoveryStoreFake{
		candidates: []store.ExecutiveSynthesisRecoveryCandidateV1{{
			CandidateAt: time.Unix(30, 0).UTC(), Kind: "fallback",
			Identity: identity, Ref: ref, Marker: marker,
			PushBatchID: 5, Status: store.ExecutiveSynthesisSpending,
		}},
		draft: draft,
		receipt: store.ExecutiveSynthesisReceiptV1{
			ExecutiveSynthesisPrepareV1: store.ExecutiveSynthesisPrepareV1{
				Marker: marker, PushBatchID: 5,
				ProfileDigest: strings.Repeat("d", 64),
				InputDigest:   strings.Repeat("e", 64),
				RequestDigest: strings.Repeat("f", 64),
			},
			Status: store.ExecutiveSynthesisSpending,
		},
	}
	runner, err := NewRunner(fake, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.RunStartup(t.Context()); err != nil {
		t.Fatal(err)
	}
	if fake.recovered != 1 || fake.frozen != 1 {
		t.Fatalf("recovered=%d frozen=%d",
			fake.recovered, fake.frozen)
	}
	if fake.receipt.Content == nil ||
		fake.receipt.Content.DecisionState !=
			types.ExecutiveDecisionInsufficientEvidence ||
		len(fake.receipt.Content.Signals) != 0 ||
		len(fake.receipt.Content.NextSteps) != 0 {
		t.Fatalf("claimless recovery content=%+v", fake.receipt.Content)
	}
}
