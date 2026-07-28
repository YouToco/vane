package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/YouToco/vane/observation"
	"github.com/YouToco/vane/runcontext"
	"github.com/YouToco/vane/types"
)

func TestCanonicalBriefStagePromotesAtomicallyAndReplays(t *testing.T) {
	f := newCanonicalBriefFixture(t, 2)
	marker, err := f.base.st.CreatePendingRunOutcomeV1(
		t.Context(), f.identity, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	generatedAt := time.Date(
		2026, 7, 27, 12, 34, 56, 123456000, time.UTC)
	order := []int64{f.deliveryID[1], f.deliveryID[0]}
	draft, err := f.base.st.PrepareBriefDraftV1(
		t.Context(), f.identity, f.ref, marker,
		f.batchID, generatedAt, order)
	if err != nil {
		t.Fatal(err)
	}
	if draft.GeneratedAt != generatedAt ||
		len(draft.Insights) != 2 ||
		draft.Insights[0].ID != order[0] ||
		draft.Insights[0].RankPosition != 1 ||
		draft.Insights[1].ID != order[1] ||
		draft.Insights[1].RankPosition != 2 {
		t.Fatalf("staged draft = %+v", draft)
	}
	replayed, found, err := f.base.st.LoadPreparedBriefDraftV1(
		t.Context(), f.identity, f.ref, marker)
	if err != nil || !found {
		t.Fatalf("load staged draft: found=%t err=%v", found, err)
	}
	firstDigest, _ := draft.RequestDigest()
	replayDigest, _ := replayed.RequestDigest()
	if firstDigest != replayDigest {
		t.Fatalf("stage replay digest = %q, want %q",
			replayDigest, firstDigest)
	}
	var batchState string
	if err := f.base.st.pool.QueryRow(t.Context(),
		`SELECT brief_state FROM push_batches WHERE id=$1`,
		f.batchID).Scan(&batchState); err != nil {
		t.Fatal(err)
	}
	if batchState != "sealed" {
		t.Fatalf("batch state = %q, want sealed", batchState)
	}
	if _, err := f.base.st.PrepareBriefDraftV1(
		t.Context(), f.identity, f.ref, marker,
		f.batchID, generatedAt, order); err != nil {
		t.Fatalf("exact stage replay failed: %v", err)
	}
	if _, err := f.base.st.PrepareBriefDraftV1(
		t.Context(), f.identity, f.ref, marker,
		f.batchID, generatedAt.Add(time.Second), order,
	); err == nil {
		t.Fatal("stage replay admitted a different deterministic time")
	}
	reversed := []int64{order[1], order[0]}
	if _, err := f.base.st.PrepareBriefDraftV1(
		t.Context(), f.identity, f.ref, marker,
		f.batchID, generatedAt, reversed,
	); err == nil {
		t.Fatal("stage replay admitted a different delivery order")
	}

	// Promotion must depend only on the frozen bytes. Mutating live evidence
	// after stage commit cannot make finalization fail or alter the Brief.
	if _, err := f.base.st.pool.Exec(t.Context(),
		`UPDATE content_items SET title='drift after freeze' WHERE id=$1`,
		f.contentID[0]); err != nil {
		t.Fatal(err)
	}
	claim := types.RunOutcomeClaimV1{
		RunOutcomeMarkerV1: marker,
		Result:             types.RunResultContent,
		SourceCoverage:     types.RunCompletenessComplete,
		Processing:         types.RunCompletenessPartial,
	}
	outcome, err := f.base.st.FinalizeRunOutcomeClaimV1(
		t.Context(), f.identity, f.ref, claim)
	if err != nil {
		t.Fatal(err)
	}
	brief, found, err := f.base.st.LoadBriefV1(
		t.Context(), f.identity, f.ref)
	if err != nil || !found {
		t.Fatalf("load promoted Brief: found=%t err=%v", found, err)
	}
	if brief.RunOutcomeID != outcome.ID ||
		brief.Insights[1].Title == "drift after freeze" {
		t.Fatalf("promoted Brief drifted: %+v", brief)
	}
	replayedOutcome, err := f.base.st.FinalizeRunOutcomeClaimV1(
		t.Context(), f.identity, f.ref, claim)
	if err != nil || replayedOutcome.Digest != outcome.Digest ||
		!replayedOutcome.FinalizedAt.Equal(outcome.FinalizedAt) {
		t.Fatalf("finalization replay = %+v err=%v",
			replayedOutcome, err)
	}
	var status string
	var briefID int64
	var resolvedAt time.Time
	if err := f.base.st.pool.QueryRow(t.Context(),
		`SELECT status,brief_snapshot_id,resolved_at
		   FROM canonical_brief_stages WHERE run_outcome_id=$1`,
		marker.ID).Scan(&status, &briefID, &resolvedAt); err != nil {
		t.Fatal(err)
	}
	if status != "promoted" || briefID != brief.ID ||
		!resolvedAt.Equal(outcome.FinalizedAt) {
		t.Fatalf("stage terminal = %q/%d/%v, outcome=%v",
			status, briefID, resolvedAt, outcome.FinalizedAt)
	}
}

func TestStructuredBriefStageFirstWriteReplayAndConflict(t *testing.T) {
	f := newCanonicalBriefFixture(t, 2)
	marker, err := f.base.st.CreatePendingRunOutcomeV1(
		t.Context(), f.identity, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	generatedAt := time.Date(
		2026, 7, 28, 1, 2, 3, 456000, time.UTC)
	order := []int64{f.deliveryID[1], f.deliveryID[0]}
	structured := make(map[int64]types.StructuredInsightV1, len(order))
	evidence := make(map[int64]map[string]string, len(order))
	for index, deliveryID := range f.deliveryID {
		sourceText := fmt.Sprintf("verified excerpt %d", index)
		if _, err := f.base.st.pool.Exec(t.Context(),
			`UPDATE content_items SET content=$1 WHERE id=$2`,
			sourceText, f.contentID[index]); err != nil {
			t.Fatal(err)
		}
		evidence[deliveryID] = map[string]string{"source-1": sourceText}
		value, err := types.SealStructuredInsightEvidenceV1(
			types.StructuredInsightV1{
				SchemaVersion: types.StructuredInsightSchemaVersionV1,
				BodyMD:        f.bodyMD[index], WhatChanged: "change",
				WhyItMatters: "reason", ImportanceReason: "evidence",
				Claims: []types.StructuredClaimV1{{
					Text: "claim", Excerpt: sourceText,
					SourceRefs: []string{"source-1"},
				}},
			}, evidence[deliveryID])
		if err != nil {
			t.Fatal(err)
		}
		structured[deliveryID] = value
	}
	draft, err := f.base.st.PrepareBriefDraftV2(
		t.Context(), f.identity, f.ref, marker,
		f.batchID, generatedAt, order, structured, evidence)
	if err != nil {
		t.Fatal(err)
	}
	for _, insight := range draft.Insights {
		if insight.Structured == nil ||
			insight.Structured.BodyMD != insight.BodyMD {
			t.Fatalf("structured draft lost projection: %+v", insight)
		}
	}
	replayed, err := f.base.st.PrepareBriefDraftV2(
		t.Context(), f.identity, f.ref, marker,
		f.batchID, generatedAt, order, structured, evidence)
	if err != nil {
		t.Fatalf("exact structured replay failed: %v", err)
	}
	firstDigest, _ := draft.RequestDigest()
	replayDigest, _ := replayed.RequestDigest()
	if firstDigest != replayDigest {
		t.Fatalf("structured replay digest = %q, want %q",
			replayDigest, firstDigest)
	}
	changed := make(map[int64]types.StructuredInsightV1, len(structured))
	for deliveryID, insight := range structured {
		changed[deliveryID] = insight
	}
	mutated := changed[order[0]]
	mutated.WhatChanged = "different terminal claim"
	changed[order[0]] = mutated
	if _, err := f.base.st.PrepareBriefDraftV2(
		t.Context(), f.identity, f.ref, marker,
		f.batchID, generatedAt, order, changed, evidence,
	); err == nil {
		t.Fatal("structured stage replay admitted different semantics")
	}
	forged := make(map[int64]types.StructuredInsightV1, len(structured))
	for deliveryID, insight := range structured {
		insight.Claims = append([]types.StructuredClaimV1(nil), insight.Claims...)
		for index := range insight.Claims {
			insight.Claims[index].SourceRefs = append(
				[]string(nil), insight.Claims[index].SourceRefs...)
		}
		forged[deliveryID] = insight
	}
	target := order[0]
	forgedValue := forged[target]
	forgedValue.Claims[0].SourceRefs = []string{"forged"}
	forgedValue, err = types.SealStructuredInsightEvidenceV1(
		forgedValue, map[string]string{
			"forged": forgedValue.Claims[0].Excerpt,
		})
	if err != nil {
		t.Fatal(err)
	}
	forged[target] = forgedValue
	if _, err := f.base.st.PrepareBriefDraftV2(
		t.Context(), f.identity, f.ref, marker,
		f.batchID, generatedAt, order, forged, evidence,
	); err == nil {
		t.Fatal("structured stage admitted forged source provenance")
	}
	if _, err := f.base.st.PrepareBriefDraftV1(
		t.Context(), f.identity, f.ref, marker,
		f.batchID, generatedAt, order,
	); err == nil {
		t.Fatal("legacy replay erased structured semantics")
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
	brief, found, err := f.base.st.LoadBriefV1(
		t.Context(), f.identity, f.ref)
	if err != nil || !found {
		t.Fatalf("load structured Brief: found=%t err=%v", found, err)
	}
	if brief.Insights[0].Structured == nil ||
		brief.Insights[0].Structured.WhatChanged != "change" {
		t.Fatalf("promoted structured Brief = %+v", brief)
	}
}

func TestCanonicalBriefStageFreezesStructuredEventEvidence(t *testing.T) {
	f := newCanonicalBriefFixture(t, 1)
	if _, err := f.base.st.pool.Exec(t.Context(),
		`UPDATE content_items
		    SET content='frozen evidence body',content_hash=$2
		  WHERE id=$1`,
		f.contentID[0], "hash-event-"+f.traceID,
	); err != nil {
		t.Fatal(err)
	}
	var item types.ContentItem
	if err := f.base.st.pool.QueryRow(t.Context(),
		`SELECT id,url,title,content,published_at,created_at
		   FROM content_items WHERE id=$1`,
		f.contentID[0],
	).Scan(
		&item.ID, &item.URL, &item.Title, &item.Content,
		&item.PublishedAt, &item.CreatedAt,
	); err != nil {
		t.Fatal(err)
	}
	metadata, err := runcontext.StructuredEventEvidenceMetadataV1(
		0, item, runcontext.SourceV1{
			SourceID: f.sourceID, Platform: types.PlatformWeb,
			Title: f.sourceName,
		})
	if err != nil {
		t.Fatal(err)
	}
	marker, err := f.base.st.CreatePendingRunOutcomeV1(
		t.Context(), f.identity, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	corpus := map[string]string{
		"source-1": runcontext.StructuredEventEvidenceTextV1(item.Content),
	}
	structured, err := types.SealStructuredInsightEvidenceV1(
		types.StructuredInsightV1{
			SchemaVersion: types.StructuredInsightSchemaVersionV1,
			BodyMD:        f.bodyMD[0],
		},
		corpus,
	)
	if err != nil {
		t.Fatal(err)
	}
	eventJSON := json.RawMessage(fmt.Sprintf(
		`{"evidence_content_ids":[%d]}`, f.contentID[0]))
	event := observation.QualifiedEvent{
		PolicyDigest: strings.Repeat("a", 64),
		EventKey:     strings.Repeat("b", 64),
		EventType:    "model_release",
		Subject:      "OpenAI models",
		OccurredAt: time.Date(
			2026, 7, 28, 12, 0, 0, 0, time.UTC),
		EvidenceJSON: eventJSON,
	}
	authority, err := f.base.st.ClaimPushBatchDeliveryAuthority(
		t.Context(),
		types.PushBatchScope{
			TenantID: f.identity.TenantID,
			UserID:   f.identity.UserID,
			BatchID:  f.batchID,
		},
		types.PushBatchDeliveryAuthorityEffect,
	)
	if err != nil ||
		authority != types.PushBatchDeliveryAuthorityEffect {
		t.Fatalf("claim push authority: authority=%q err=%v",
			authority, err)
	}
	provenance, accepted, err :=
		f.base.st.ReserveObservedEventProvenanceV1(
			t.Context(), f.identity, f.ref, f.batchID, event)
	if err != nil || !accepted {
		t.Fatalf("reserve observed event: accepted=%t err=%v",
			accepted, err)
	}
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(ctx, t, f.base.st,
			`DELETE FROM task_observed_events WHERE id=$1`, provenance.ID)
	})
	if err := f.base.st.BindObservedEventDeliveryV1(
		t.Context(), f.identity, f.ref,
		event.PolicyDigest, event.EventKey, f.batchID, f.deliveryID[0],
	); err != nil {
		t.Fatal(err)
	}
	generatedAt := time.Date(
		2026, 7, 28, 13, 0, 0, 0, time.UTC)
	structuredByDelivery := map[int64]types.StructuredInsightV1{
		f.deliveryID[0]: structured,
	}
	corpusByDelivery := map[int64]map[string]string{
		f.deliveryID[0]: corpus,
	}
	eventByDelivery := map[int64]StructuredEventEvidenceStageV1{
		f.deliveryID[0]: {
			Provenance: provenance,
			Sources: []StructuredEventEvidenceStageSourceV1{{
				ContentItemID: f.contentID[0],
				Metadata:      metadata,
				EvidenceText:  corpus["source-1"],
			}},
		},
	}
	assertConflict := func(
		name string,
		structuredInput map[int64]types.StructuredInsightV1,
		corpusInput map[int64]map[string]string,
		eventInput map[int64]StructuredEventEvidenceStageV1,
	) {
		t.Helper()
		if _, err := f.base.st.PrepareBriefDraftV3(
			t.Context(), f.identity, f.ref, marker, f.batchID, generatedAt,
			f.deliveryID, structuredInput, corpusInput, eventInput,
		); !errors.Is(err, types.ErrConflict) &&
			!errors.Is(err, types.ErrValidation) {
			t.Fatalf("%s error=%v want conflict/validation", name, err)
		}
	}
	missingProvenance := eventByDelivery[f.deliveryID[0]]
	missingProvenance.Provenance.ID++
	assertConflict(
		"missing provenance", structuredByDelivery, corpusByDelivery,
		map[int64]StructuredEventEvidenceStageV1{
			f.deliveryID[0]: missingProvenance,
		})

	forgedBody := "forged body from another content item"
	forgedCorpus := map[string]string{"source-1": forgedBody}
	forgedStructured, err := types.SealStructuredInsightEvidenceV1(
		types.StructuredInsightV1{
			SchemaVersion: types.StructuredInsightSchemaVersionV1,
			BodyMD:        f.bodyMD[0],
		},
		forgedCorpus,
	)
	if err != nil {
		t.Fatal(err)
	}
	forgedStage := eventByDelivery[f.deliveryID[0]]
	forgedStage.Sources = append(
		[]StructuredEventEvidenceStageSourceV1(nil),
		forgedStage.Sources...)
	forgedStage.Sources[0].EvidenceText = forgedBody
	assertConflict(
		"forged body",
		map[int64]types.StructuredInsightV1{
			f.deliveryID[0]: forgedStructured,
		},
		map[int64]map[string]string{
			f.deliveryID[0]: forgedCorpus,
		},
		map[int64]StructuredEventEvidenceStageV1{
			f.deliveryID[0]: forgedStage,
		})

	forgedMetadata := eventByDelivery[f.deliveryID[0]]
	forgedMetadata.Sources = append(
		[]StructuredEventEvidenceStageSourceV1(nil),
		forgedMetadata.Sources...)
	forgedMetadata.Sources[0].Metadata.Title = "forged title"
	assertConflict(
		"forged metadata", structuredByDelivery, corpusByDelivery,
		map[int64]StructuredEventEvidenceStageV1{
			f.deliveryID[0]: forgedMetadata,
		})

	forgedContentID := eventByDelivery[f.deliveryID[0]]
	forgedContentID.Sources = append(
		[]StructuredEventEvidenceStageSourceV1(nil),
		forgedContentID.Sources...)
	forgedContentID.Sources[0].ContentItemID++
	assertConflict(
		"forged content id", structuredByDelivery, corpusByDelivery,
		map[int64]StructuredEventEvidenceStageV1{
			f.deliveryID[0]: forgedContentID,
		})

	extraRef := eventByDelivery[f.deliveryID[0]]
	extraSource := extraRef.Sources[0]
	extraSource.ContentItemID++
	extraSource.Metadata.Ref = "source-2"
	extraRef.Sources = append(
		append([]StructuredEventEvidenceStageSourceV1(nil),
			extraRef.Sources...),
		extraSource,
	)
	assertConflict(
		"metadata ref without corpus", structuredByDelivery, corpusByDelivery,
		map[int64]StructuredEventEvidenceStageV1{
			f.deliveryID[0]: extraRef,
		})

	draft, err := f.base.st.PrepareBriefDraftV3(
		t.Context(), f.identity, f.ref, marker, f.batchID, generatedAt,
		f.deliveryID, structuredByDelivery, corpusByDelivery,
		eventByDelivery,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(draft.Insights) != 1 ||
		draft.Insights[0].EventEvidence == nil ||
		draft.Insights[0].EventEvidence.Provenance.ID != provenance.ID {
		t.Fatalf("staged event evidence = %+v", draft)
	}
	payload, err := json.Marshal(draft)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(payload), "content_item_id") ||
		strings.Contains(string(payload), "frozen evidence body") {
		t.Fatalf("Brief leaked raw evidence inventory: %s", payload)
	}
	if _, err := f.base.st.PrepareBriefDraftV3(
		t.Context(), f.identity, f.ref, marker, f.batchID, generatedAt,
		f.deliveryID, structuredByDelivery, corpusByDelivery,
		eventByDelivery,
	); err != nil {
		t.Fatalf("exact event evidence replay failed: %v", err)
	}
	drifted := eventByDelivery[f.deliveryID[0]]
	drifted.Provenance.ID++
	eventByDelivery[f.deliveryID[0]] = drifted
	if _, err := f.base.st.PrepareBriefDraftV3(
		t.Context(), f.identity, f.ref, marker, f.batchID, generatedAt,
		f.deliveryID, structuredByDelivery, corpusByDelivery,
		eventByDelivery,
	); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("provenance drift error=%v want conflict", err)
	}
}

func TestCanonicalBriefStageAbortsWithNonContentOutcome(t *testing.T) {
	f := newCanonicalBriefFixture(t, 1)
	marker, err := f.base.st.CreatePendingRunOutcomeV1(
		t.Context(), f.identity, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.base.st.PrepareBriefDraftV1(
		t.Context(), f.identity, f.ref, marker, f.batchID,
		time.Date(2026, 7, 27, 1, 2, 3, 0, time.UTC),
		f.deliveryID,
	); err != nil {
		t.Fatal(err)
	}
	claim := types.RunOutcomeClaimV1{
		RunOutcomeMarkerV1: marker,
		Result:             types.RunResultInterrupted,
		SourceCoverage:     types.RunCompletenessPartial,
		Processing:         types.RunCompletenessPartial,
		FailureCode:        "workflow_canceled",
		FailureMessage:     "workflow was canceled",
	}
	outcome, err := f.base.st.FinalizeRunOutcomeClaimV1(
		t.Context(), f.identity, f.ref, claim)
	if err != nil {
		t.Fatal(err)
	}
	if _, found, err := f.base.st.LoadBriefV1(
		t.Context(), f.identity, f.ref); err != nil || found {
		t.Fatalf("aborted stage produced Brief: found=%t err=%v", found, err)
	}
	var status string
	var resolvedAt time.Time
	if err := f.base.st.pool.QueryRow(t.Context(),
		`SELECT status,resolved_at
		   FROM canonical_brief_stages WHERE run_outcome_id=$1`,
		marker.ID).Scan(&status, &resolvedAt); err != nil {
		t.Fatal(err)
	}
	if status != "aborted" || !resolvedAt.Equal(outcome.FinalizedAt) {
		t.Fatalf("aborted stage = %q/%v", status, resolvedAt)
	}
}

func TestCanonicalBriefSealedEmptyReceiptRecoversQuiet(t *testing.T) {
	f := newCanonicalBriefFixture(t, 0)
	marker, err := f.base.st.CreatePendingRunOutcomeV1(
		t.Context(), f.identity, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	winner, err := f.base.st.ClaimPushBatchDeliveryAuthority(
		t.Context(),
		types.PushBatchScope{
			TenantID: f.identity.TenantID,
			UserID:   f.identity.UserID,
			BatchID:  f.batchID,
		},
		types.PushBatchDeliveryAuthorityEffect,
	)
	if err != nil || winner != types.PushBatchDeliveryAuthorityEffect {
		t.Fatalf("effect authority=%q err=%v", winner, err)
	}
	if err := f.base.st.SealEmptyBriefBatchV1(
		t.Context(), f.identity, f.ref, marker, f.batchID,
	); err != nil {
		t.Fatal(err)
	}
	batchID, found, err := f.base.st.LoadSealedEmptyBriefBatchV1(
		t.Context(), f.identity, f.ref, marker, f.traceID)
	if err != nil || !found || batchID != f.batchID {
		t.Fatalf("empty receipt batch=%d found=%t err=%v",
			batchID, found, err)
	}
	if _, found, err := f.base.st.LoadSealedEmptyBriefBatchV1(
		t.Context(), f.identity, f.ref, marker, f.traceID+"-other",
	); err != nil || found {
		t.Fatalf("wrong trace empty receipt found=%t err=%v", found, err)
	}
	if err := f.base.st.CompleteEmptyPushEffectBatch(
		t.Context(),
		types.PushBatchScope{
			TenantID: f.identity.TenantID,
			UserID:   f.identity.UserID,
			BatchID:  f.batchID,
		},
		f.ref.SnapshotID,
	); err != nil {
		t.Fatal(err)
	}
	failed := types.RunOutcomeClaimV1{
		RunOutcomeMarkerV1: marker,
		Result:             types.RunResultFailed,
		SourceCoverage:     types.RunCompletenessPartial,
		Processing:         types.RunCompletenessPartial,
		FailureCode:        "workflow_failed",
		FailureMessage:     "finalizer failed",
	}
	outcome, err := f.base.st.FinalizeRunOutcomeClaimV1(
		t.Context(), f.identity, f.ref, failed)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Result != types.RunResultQuiet ||
		outcome.Processing != types.RunCompletenessPartial {
		t.Fatalf("empty recovered outcome = %+v", outcome)
	}
	var status, briefState string
	if err := f.base.st.pool.QueryRow(t.Context(),
		`SELECT status,brief_state FROM push_batches WHERE id=$1`,
		f.batchID).Scan(&status, &briefState); err != nil {
		t.Fatal(err)
	}
	if status != string(types.BatchStatusDone) || briefState != "sealed" {
		t.Fatalf("empty batch terminal=%q/%q", status, briefState)
	}
}

func TestCanonicalEmptyLegacyRaceFailsClosedAfterConcurrentSeal(t *testing.T) {
	f := newCanonicalBriefFixture(t, 0)
	_, err := f.base.st.CreatePendingRunOutcomeV1(
		t.Context(), f.identity, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	winner, err := f.base.st.ClaimPushBatchDeliveryAuthority(
		t.Context(),
		types.PushBatchScope{
			TenantID: f.identity.TenantID,
			UserID:   f.identity.UserID,
			BatchID:  f.batchID,
		},
		types.PushBatchDeliveryAuthorityEffect,
	)
	if err != nil || winner != types.PushBatchDeliveryAuthorityEffect {
		t.Fatalf("effect authority=%q err=%v", winner, err)
	}

	// Leave an uncommitted canonical seal in front of the capability
	// function's open-state recheck. The first plain read can still observe
	// open, but FOR UPDATE must wait and then deny the legacy handoff.
	sealer, err := f.base.st.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sealer.Rollback(t.Context()) }()
	if _, err := sealer.Exec(t.Context(),
		`SELECT set_config('app.tenant_id',$1,true),
		        set_config('app.user_id',$2,true)`,
		fmt.Sprint(f.identity.TenantID),
		fmt.Sprint(f.identity.UserID)); err != nil {
		t.Fatal(err)
	}
	if _, err := sealer.Exec(
		t.Context(), `SET LOCAL ROLE vane_brief_writer`); err != nil {
		t.Fatal(err)
	}
	if _, err := sealer.Exec(t.Context(),
		`UPDATE push_batches SET brief_state='sealed' WHERE id=$1`,
		f.batchID); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- f.base.st.CompleteEmptyPushEffectBatch(
			t.Context(),
			types.PushBatchScope{
				TenantID: f.identity.TenantID,
				UserID:   f.identity.UserID,
				BatchID:  f.batchID,
			},
			f.ref.SnapshotID,
		)
	}()
	waitDeadline := time.Now().Add(5 * time.Second)
	for {
		var waiting bool
		if err := f.base.st.pool.QueryRow(t.Context(), `
			SELECT EXISTS (
			    SELECT 1 FROM pg_locks
			     WHERE locktype IN ('transactionid','tuple')
			       AND NOT granted
			)`,
		).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		if time.Now().After(waitDeadline) {
			t.Fatal("legacy empty completion did not wait for seal")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := sealer.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, types.ErrConflict) {
			t.Fatalf("legacy race error=%v, want conflict", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("legacy empty completion did not resume")
	}
	var status, briefState string
	if err := f.base.st.pool.QueryRow(t.Context(),
		`SELECT status,brief_state FROM push_batches WHERE id=$1`,
		f.batchID,
	).Scan(&status, &briefState); err != nil {
		t.Fatal(err)
	}
	if status != string(types.BatchStatusPending) || briefState != "sealed" {
		t.Fatalf("legacy race terminal=%q/%q", status, briefState)
	}
}

func TestCanonicalBriefStagedEvidenceOverridesFailedRecovery(t *testing.T) {
	f := newCanonicalBriefFixture(t, 1)
	marker, err := f.base.st.CreatePendingRunOutcomeV1(
		t.Context(), f.identity, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.base.st.PrepareBriefDraftV1(
		t.Context(), f.identity, f.ref, marker, f.batchID,
		time.Date(2026, 7, 27, 8, 9, 10, 0, time.UTC),
		f.deliveryID,
	); err != nil {
		t.Fatal(err)
	}
	failed := types.RunOutcomeClaimV1{
		RunOutcomeMarkerV1: marker,
		Result:             types.RunResultFailed,
		SourceCoverage:     types.RunCompletenessPartial,
		Processing:         types.RunCompletenessPartial,
		FailureCode:        "workflow_failed",
		FailureMessage:     "finalizer failed",
	}
	outcome, err := f.base.st.FinalizeRecoveredRunOutcomeClaimV1(
		t.Context(), f.identity, failed)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Result != types.RunResultContent ||
		outcome.Processing != types.RunCompletenessPartial {
		t.Fatalf("staged recovered outcome = %+v", outcome)
	}
	replayed, err := f.base.st.FinalizeRecoveredRunOutcomeClaimV1(
		t.Context(), f.identity, failed)
	if err != nil || replayed.Digest != outcome.Digest ||
		!replayed.FinalizedAt.Equal(outcome.FinalizedAt) {
		t.Fatalf("normalized failed replay=%+v err=%v", replayed, err)
	}
	if _, found, err := f.base.st.LoadBriefV1(
		t.Context(), f.identity, f.ref); err != nil || !found {
		t.Fatalf("staged recovery Brief found=%t err=%v", found, err)
	}
}

func TestCanonicalBriefFinalizedFailedAbortedStageReplaysAfterResponseLoss(
	t *testing.T,
) {
	f := newCanonicalBriefFixture(t, 1)
	marker, err := f.base.st.CreatePendingRunOutcomeV1(
		t.Context(), f.identity, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.base.st.PrepareBriefDraftV1(
		t.Context(), f.identity, f.ref, marker, f.batchID,
		time.Date(2026, 7, 27, 8, 9, 10, 0, time.UTC),
		f.deliveryID,
	); err != nil {
		t.Fatal(err)
	}
	claim := types.RunOutcomeClaimV1{
		RunOutcomeMarkerV1: marker,
		Result:             types.RunResultFailed,
		SourceCoverage:     types.RunCompletenessPartial,
		Processing:         types.RunCompletenessPartial,
		FailureCode:        "workflow_failed",
		FailureMessage:     "old finalizer response was lost",
	}
	finalizedAt := time.Date(
		2026, 7, 27, 9, 10, 11, 123456000, time.UTC)
	terminal, err := claim.SealAt(finalizedAt)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := f.base.st.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(t.Context()) }()
	if _, err := tx.Exec(t.Context(),
		`SELECT set_config('app.tenant_id',$1,true),
		        set_config('app.user_id',$2,true)`,
		fmt.Sprint(f.identity.TenantID),
		fmt.Sprint(f.identity.UserID)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(
		t.Context(), `SET LOCAL ROLE vane_brief_writer`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(t.Context(), `
		UPDATE task_run_outcomes
		   SET status='finalized',result=$2,source_coverage=$3,
		       processing=$4,failure_code=$5,failure_message=$6,
		       finalized_at=$7,outcome_digest=$8
		 WHERE id=$1 AND status='pending'`,
		terminal.ID, terminal.Result, terminal.SourceCoverage,
		terminal.Processing, terminal.FailureCode, terminal.FailureMessage,
		terminal.FinalizedAt, terminal.Digest,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(t.Context(), `
		UPDATE canonical_brief_stages
		   SET status='aborted',resolved_at=$2
		 WHERE run_outcome_id=$1 AND status='staged'`,
		terminal.ID, terminal.FinalizedAt,
	); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	replayed, err := f.base.st.FinalizeRunOutcomeClaimV1(
		t.Context(), f.identity, f.ref, claim)
	if err != nil {
		t.Fatalf("exact old failed/aborted replay: %v", err)
	}
	if replayed.Digest != terminal.Digest ||
		!replayed.FinalizedAt.Equal(terminal.FinalizedAt) {
		t.Fatalf("failed/aborted replay=%+v want=%+v", replayed, terminal)
	}
	different := claim
	different.FailureMessage = "different terminal receipt"
	if _, err := f.base.st.FinalizeRunOutcomeClaimV1(
		t.Context(), f.identity, f.ref, different,
	); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("different failed/aborted replay error=%v, want conflict", err)
	}
}

func TestMigration064StageInsertSerializesOutcomeFinalization(t *testing.T) {
	f := newCanonicalBriefFixture(t, 1)
	marker, err := f.base.st.CreatePendingRunOutcomeV1(
		t.Context(), f.identity, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	sealCanonicalTestBatch(t, f)
	draft, err := (types.BriefDraftV1{
		SchemaVersion: types.BriefSchemaVersionV1,
		RunOutcomeID:  marker.ID,
		RunSnapshotID: marker.RunSnapshotID,
		PushBatchID:   f.batchID,
		TenantID:      marker.TenantID,
		UserID:        marker.UserID,
		TaskID:        marker.TaskID,
		GeneratedAt:   time.Date(2026, 7, 27, 9, 10, 11, 0, time.UTC),
		Insights: []types.InsightV1{{
			ID: f.deliveryID[0], RankPosition: 1,
			Title: f.itemTitle[0], BodyMD: f.bodyMD[0],
			SourceTitle: f.sourceName, SourceURL: f.itemURL[0],
			PublishedAt: f.published[0], DiscoveredAt: f.deliveryAt[0],
		}},
	}).Canonical()
	if err != nil {
		t.Fatal(err)
	}
	digest, err := draft.RequestDigest()
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(draft)
	if err != nil {
		t.Fatal(err)
	}
	stageTx, err := f.base.st.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stageTx.Rollback(t.Context()) }()
	if _, err := stageTx.Exec(t.Context(),
		`SELECT set_config('app.tenant_id',$1,true),
		        set_config('app.user_id',$2,true)`,
		fmt.Sprint(f.identity.TenantID),
		fmt.Sprint(f.identity.UserID)); err != nil {
		t.Fatal(err)
	}
	if _, err := stageTx.Exec(
		t.Context(), `SET LOCAL ROLE vane_brief_writer`); err != nil {
		t.Fatal(err)
	}
	if _, err := stageTx.Exec(t.Context(), `
		INSERT INTO canonical_brief_stages (
		    run_outcome_id,tenant_id,user_id,task_id,run_snapshot_id,
		    push_batch_id,schema_version,request_digest,payload,
		    insight_count,generated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
		draft.RunOutcomeID, draft.TenantID, draft.UserID, draft.TaskID,
		draft.RunSnapshotID, draft.PushBatchID, draft.SchemaVersion,
		digest, payload, len(draft.Insights), draft.GeneratedAt,
	); err != nil {
		t.Fatal(err)
	}
	claim := types.RunOutcomeClaimV1{
		RunOutcomeMarkerV1: marker,
		Result:             types.RunResultContent,
		SourceCoverage:     types.RunCompletenessComplete,
		Processing:         types.RunCompletenessComplete,
	}
	done := make(chan error, 1)
	go func() {
		_, finalizeErr := f.base.st.FinalizeRunOutcomeClaimV1(
			t.Context(), f.identity, f.ref, claim)
		done <- finalizeErr
	}()
	select {
	case err := <-done:
		t.Fatalf("finalizer crossed uncommitted stage admission: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	if err := stageTx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("finalizer did not resume after stage commit")
	}
	var outcomeStatus, stageStatus string
	if err := f.base.st.pool.QueryRow(t.Context(), `
		SELECT o.status,s.status
		  FROM task_run_outcomes o
		  JOIN canonical_brief_stages s ON s.run_outcome_id=o.id
		 WHERE o.id=$1`,
		marker.ID,
	).Scan(&outcomeStatus, &stageStatus); err != nil {
		t.Fatal(err)
	}
	if outcomeStatus != "finalized" || stageStatus != "promoted" {
		t.Fatalf("serialized terminal=%q/%q", outcomeStatus, stageStatus)
	}
}

func TestMigration064StageAdmissionRequiresWriterAndExactSessionScope(
	t *testing.T,
) {
	f := newCanonicalBriefFixture(t, 1)
	marker, err := f.base.st.CreatePendingRunOutcomeV1(
		t.Context(), f.identity, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	sealCanonicalTestBatch(t, f)
	draft, err := (types.BriefDraftV1{
		SchemaVersion: types.BriefSchemaVersionV1,
		RunOutcomeID:  marker.ID,
		RunSnapshotID: marker.RunSnapshotID,
		PushBatchID:   f.batchID,
		TenantID:      marker.TenantID,
		UserID:        marker.UserID,
		TaskID:        marker.TaskID,
		GeneratedAt:   time.Date(2026, 7, 27, 9, 10, 11, 0, time.UTC),
		Insights: []types.InsightV1{{
			ID: f.deliveryID[0], RankPosition: 1,
			Title: f.itemTitle[0], BodyMD: f.bodyMD[0],
			SourceTitle: f.sourceName, SourceURL: f.itemURL[0],
			PublishedAt: f.published[0], DiscoveredAt: f.deliveryAt[0],
		}},
	}).Canonical()
	if err != nil {
		t.Fatal(err)
	}
	digest, err := draft.RequestDigest()
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(draft)
	if err != nil {
		t.Fatal(err)
	}
	insert := func(
		t *testing.T,
		asWriter bool,
		tenantSetting, userSetting *int64,
	) error {
		t.Helper()
		tx, err := f.base.st.pool.Begin(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(t.Context()) }()
		if tenantSetting != nil {
			if _, err := tx.Exec(t.Context(),
				`SELECT set_config('app.tenant_id',$1,true)`,
				fmt.Sprint(*tenantSetting)); err != nil {
				t.Fatal(err)
			}
		}
		if userSetting != nil {
			if _, err := tx.Exec(t.Context(),
				`SELECT set_config('app.user_id',$1,true)`,
				fmt.Sprint(*userSetting)); err != nil {
				t.Fatal(err)
			}
		}
		if asWriter {
			if _, err := tx.Exec(
				t.Context(), `SET LOCAL ROLE vane_brief_writer`); err != nil {
				t.Fatal(err)
			}
		}
		_, err = tx.Exec(t.Context(), `
			INSERT INTO canonical_brief_stages (
			    run_outcome_id,tenant_id,user_id,task_id,run_snapshot_id,
			    push_batch_id,schema_version,request_digest,payload,
			    insight_count,generated_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
			draft.RunOutcomeID, draft.TenantID, draft.UserID, draft.TaskID,
			draft.RunSnapshotID, draft.PushBatchID, draft.SchemaVersion,
			digest, payload, len(draft.Insights), draft.GeneratedAt,
		)
		return err
	}
	wrongTenant := f.identity.TenantID + 1000
	wrongUser := f.identity.UserID + 1000
	tests := []struct {
		name                       string
		asWriter                   bool
		tenantSetting, userSetting *int64
	}{
		{name: "wrong current user"},
		{name: "missing scope", asWriter: true},
		{
			name: "wrong tenant", asWriter: true,
			tenantSetting: &wrongTenant, userSetting: &f.identity.UserID,
		},
		{
			name: "wrong user", asWriter: true,
			tenantSetting: &f.identity.TenantID, userSetting: &wrongUser,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := insert(
				t, test.asWriter, test.tenantSetting, test.userSetting,
			); err == nil {
				t.Fatal("stage admission crossed role/session boundary")
			}
		})
	}
}

func TestCanonicalEmptyCompletionSerializesFailedFinalizer(t *testing.T) {
	f := newCanonicalBriefFixture(t, 0)
	marker, err := f.base.st.CreatePendingRunOutcomeV1(
		t.Context(), f.identity, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	winner, err := f.base.st.ClaimPushBatchDeliveryAuthority(
		t.Context(),
		types.PushBatchScope{
			TenantID: f.identity.TenantID,
			UserID:   f.identity.UserID,
			BatchID:  f.batchID,
		},
		types.PushBatchDeliveryAuthorityEffect,
	)
	if err != nil || winner != types.PushBatchDeliveryAuthorityEffect {
		t.Fatalf("effect authority=%q err=%v", winner, err)
	}
	if err := f.base.st.SealEmptyBriefBatchV1(
		t.Context(), f.identity, f.ref, marker, f.batchID,
	); err != nil {
		t.Fatal(err)
	}
	receiptTx, err := f.base.st.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = receiptTx.Rollback(t.Context()) }()
	if _, err := receiptTx.Exec(t.Context(),
		`SELECT set_config('app.tenant_id',$1,true)`,
		fmt.Sprint(f.identity.TenantID)); err != nil {
		t.Fatal(err)
	}
	if _, err := receiptTx.Exec(
		t.Context(), `SET LOCAL ROLE vane_push_effect_receipt`); err != nil {
		t.Fatal(err)
	}
	var decision string
	if err := receiptTx.QueryRow(t.Context(), `
		SELECT complete_canonical_empty_push_batch_v1($1,$2,$3,$4)`,
		f.identity.TenantID, f.identity.UserID,
		f.batchID, f.ref.SnapshotID,
	).Scan(&decision); err != nil {
		t.Fatal(err)
	}
	if decision != "done" {
		t.Fatalf("empty receipt decision = %q", decision)
	}
	failed := types.RunOutcomeClaimV1{
		RunOutcomeMarkerV1: marker,
		Result:             types.RunResultFailed,
		SourceCoverage:     types.RunCompletenessPartial,
		Processing:         types.RunCompletenessPartial,
		FailureCode:        "workflow_failed",
		FailureMessage:     "push response was lost",
	}
	type finalizeResult struct {
		outcome types.RunOutcomeV1
		err     error
	}
	done := make(chan finalizeResult, 1)
	go func() {
		outcome, finalizeErr := f.base.st.FinalizeRunOutcomeClaimV1(
			t.Context(), f.identity, f.ref, failed)
		done <- finalizeResult{outcome: outcome, err: finalizeErr}
	}()
	select {
	case result := <-done:
		t.Fatalf("finalizer crossed uncommitted empty receipt: %+v", result)
	case <-time.After(200 * time.Millisecond):
	}
	if err := receiptTx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-done:
		if result.err != nil {
			t.Fatal(result.err)
		}
		if result.outcome.Result != types.RunResultQuiet {
			t.Fatalf("serialized empty outcome = %+v", result.outcome)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("finalizer did not resume after empty receipt commit")
	}
}

func sealCanonicalTestBatch(t *testing.T, f *canonicalBriefFixture) {
	t.Helper()
	tx, err := f.base.st.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(t.Context()) }()
	if _, err := tx.Exec(t.Context(),
		`SELECT set_config('app.tenant_id',$1,true),
		        set_config('app.user_id',$2,true)`,
		fmt.Sprint(f.identity.TenantID),
		fmt.Sprint(f.identity.UserID)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(
		t.Context(), `SET LOCAL ROLE vane_brief_writer`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(t.Context(),
		`UPDATE push_batches SET brief_state='sealed' WHERE id=$1`,
		f.batchID); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func directCanonicalBriefInsertError(
	t *testing.T,
	f *canonicalBriefFixture,
	marker types.RunOutcomeMarkerV1,
) error {
	t.Helper()
	tx, err := f.base.st.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(t.Context()) }()
	if _, err := tx.Exec(t.Context(),
		`SELECT set_config('app.tenant_id',$1,true),
		        set_config('app.user_id',$2,true)`,
		fmt.Sprint(f.identity.TenantID),
		fmt.Sprint(f.identity.UserID)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(
		t.Context(), `SET LOCAL ROLE vane_brief_writer`); err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(t.Context(),
		`INSERT INTO brief_snapshots (
		    id,tenant_id,user_id,task_id,run_outcome_id,run_snapshot_id,
		    push_batch_id,schema_version,request_digest,payload_digest,
		    payload,insight_count,generated_at
		 ) VALUES (
		    nextval('brief_snapshots_id_seq'),$1,$2,$3,$4,$5,$6,
		    'vane.brief/v1',$7,$7,$8,1,$9
		 )`,
		f.identity.TenantID, f.identity.UserID, f.identity.TaskID,
		marker.ID, f.ref.SnapshotID, f.batchID,
		strings.Repeat("0", 64), []byte("{}"), time.Now().UTC())
	return err
}

func TestMigration064BriefSnapshotAdmissionRejectsInvalidBusinessState(
	t *testing.T,
) {
	tests := []struct {
		name   string
		result types.RunResultV1
		seal   bool
	}{
		{name: "pending sealed", seal: true},
		{name: "content open", result: types.RunResultContent},
		{name: "quiet sealed", result: types.RunResultQuiet, seal: true},
		{name: "failed sealed", result: types.RunResultFailed, seal: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := newCanonicalBriefFixture(t, 0)
			marker, err := f.base.st.CreatePendingRunOutcomeV1(
				t.Context(), f.identity, f.ref)
			if err != nil {
				t.Fatal(err)
			}
			if test.result != "" {
				claim := types.RunOutcomeClaimV1{
					RunOutcomeMarkerV1: marker,
					Result:             test.result,
					SourceCoverage:     types.RunCompletenessPartial,
					Processing:         types.RunCompletenessPartial,
				}
				if test.result == types.RunResultFailed {
					claim.FailureCode = "activity_failed"
					claim.FailureMessage = "activity failed"
				}
				if _, err := f.base.st.FinalizeRunOutcomeClaimV1(
					t.Context(), f.identity, f.ref, claim); err != nil {
					t.Fatal(err)
				}
			}
			if test.seal {
				sealCanonicalTestBatch(t, f)
			}
			err = directCanonicalBriefInsertError(t, f, marker)
			if err == nil ||
				!strings.Contains(err.Error(),
					"canonical Brief snapshot admission denied") {
				t.Fatalf("direct invalid Brief insert error = %v", err)
			}
		})
	}
}

func TestCanonicalBriefStageWorkflowRecoveryFinalizersConverge(t *testing.T) {
	f := newCanonicalBriefFixture(t, 1)
	marker, err := f.base.st.CreatePendingRunOutcomeV1(
		t.Context(), f.identity, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.base.st.PrepareBriefDraftV1(
		t.Context(), f.identity, f.ref, marker, f.batchID,
		time.Date(2026, 7, 27, 4, 5, 6, 0, time.UTC),
		f.deliveryID,
	); err != nil {
		t.Fatal(err)
	}
	claim := types.RunOutcomeClaimV1{
		RunOutcomeMarkerV1: marker,
		Result:             types.RunResultContent,
		SourceCoverage:     types.RunCompletenessComplete,
		Processing:         types.RunCompletenessPartial,
	}
	var wg sync.WaitGroup
	wg.Add(2)
	outcomes := make([]types.RunOutcomeV1, 2)
	errs := make([]error, 2)
	go func() {
		defer wg.Done()
		outcomes[0], errs[0] = f.base.st.FinalizeRunOutcomeClaimV1(
			t.Context(), f.identity, f.ref, claim)
	}()
	go func() {
		defer wg.Done()
		outcomes[1], errs[1] =
			f.base.st.FinalizeRecoveredRunOutcomeClaimV1(
				t.Context(), f.identity, claim)
	}()
	wg.Wait()
	for index, err := range errs {
		if err != nil {
			t.Fatalf("finalizer %d: %v", index, err)
		}
	}
	if outcomes[0].Digest != outcomes[1].Digest ||
		!outcomes[0].FinalizedAt.Equal(outcomes[1].FinalizedAt) {
		t.Fatalf("finalizers diverged: %+v / %+v", outcomes[0], outcomes[1])
	}
	var briefCount, promotedCount int
	if err := f.base.st.pool.QueryRow(t.Context(),
		`SELECT
		    (SELECT count(*) FROM brief_snapshots WHERE run_outcome_id=$1),
		    (SELECT count(*) FROM canonical_brief_stages
		      WHERE run_outcome_id=$1 AND status='promoted')`,
		marker.ID).Scan(&briefCount, &promotedCount); err != nil {
		t.Fatal(err)
	}
	if briefCount != 1 || promotedCount != 1 {
		t.Fatalf("converged rows Brief=%d promoted=%d",
			briefCount, promotedCount)
	}
}

func TestMigration064RejectsTerminalOutcomeWithUnresolvedStage(t *testing.T) {
	f := newCanonicalBriefFixture(t, 1)
	marker, err := f.base.st.CreatePendingRunOutcomeV1(
		t.Context(), f.identity, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.base.st.PrepareBriefDraftV1(
		t.Context(), f.identity, f.ref, marker, f.batchID,
		time.Date(2026, 7, 27, 10, 11, 12, 0, time.UTC),
		f.deliveryID,
	); err != nil {
		t.Fatal(err)
	}
	tx, err := f.base.st.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(t.Context()) }()
	if _, err := tx.Exec(t.Context(),
		`SELECT set_config('app.tenant_id',$1,true),
		        set_config('app.user_id',$2,true)`,
		fmt.Sprint(f.identity.TenantID),
		fmt.Sprint(f.identity.UserID)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(
		t.Context(), `SET LOCAL ROLE vane_brief_writer`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(t.Context(),
		`UPDATE task_run_outcomes
		    SET status='finalized',result='content',
		        source_coverage='complete',processing='complete',
		        finalized_at=clock_timestamp(),outcome_digest=$2
		  WHERE id=$1`,
		marker.ID, strings.Repeat("0", 64)); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(t.Context()); err == nil ||
		!strings.Contains(err.Error(),
			"finalized RunOutcome has unresolved canonical Brief stage") {
		t.Fatalf("unresolved terminal outcome commit error = %v", err)
	}
}
