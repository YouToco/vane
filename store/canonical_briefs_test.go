package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/types"
)

type canonicalBriefFixture struct {
	base       *taskRunSnapshotFixture
	identity   types.RunIdentity
	ref        types.RunSnapshotRef
	batchID    int64
	sourceID   int64
	deliveryID []int64
	deliveryAt []time.Time
	bodyMD     []string
	contentID  []int64
	itemTitle  []string
	itemURL    []string
	published  []*time.Time
	sourceName string
}

func newCanonicalBriefFixture(t *testing.T, deliveries int) *canonicalBriefFixture {
	t.Helper()
	base := newTaskRunSnapshotFixture(t)
	taskID := base.taskID()
	sourceIDs := base.createApprovedTask(t, taskID, 1)
	identity := types.RunIdentity{
		TemporalWorkflowID: "wf-" + taskID,
		TemporalRunID:      uuid.NewString(),
		RunKind:            types.RunSnapshotKindScheduled,
		TenantID:           base.tenantID,
		UserID:             base.userID,
		TaskID:             taskID,
	}
	ref, err := base.st.CreateOrGetCompiledTaskRunSnapshotV1(
		t.Context(), CreateOrGetCompiledTaskRunSnapshotV1Params{
			Identity: identity, Policy: testCompiledRunPolicyV1(t),
		})
	if err != nil {
		t.Fatalf("create compiled snapshot: %v", err)
	}
	var storedIdentity struct {
		tenantID, userID                 int64
		taskID, workflowID, runID        string
		schema, reference, payloadDigest string
	}
	if err := base.st.pool.QueryRow(t.Context(),
		`SELECT tenant_id,user_id,task_id,temporal_workflow_id,temporal_run_id,
		        reference_schema_version,reference_digest,payload_digest
		   FROM task_run_snapshots WHERE id=$1`,
		ref.SnapshotID,
	).Scan(
		&storedIdentity.tenantID, &storedIdentity.userID,
		&storedIdentity.taskID, &storedIdentity.workflowID,
		&storedIdentity.runID, &storedIdentity.schema,
		&storedIdentity.reference, &storedIdentity.payloadDigest,
	); err != nil {
		t.Fatalf("load compiled snapshot identity: %v", err)
	}
	if storedIdentity.tenantID != identity.TenantID ||
		storedIdentity.userID != identity.UserID ||
		storedIdentity.taskID != identity.TaskID ||
		storedIdentity.workflowID != identity.TemporalWorkflowID ||
		storedIdentity.runID != identity.TemporalRunID ||
		storedIdentity.schema != ref.SchemaVersion ||
		storedIdentity.reference != ref.ReferenceDigest ||
		storedIdentity.payloadDigest != ref.PayloadDigest {
		t.Fatalf("stored snapshot identity drift: %+v ref=%+v",
			storedIdentity, ref)
	}
	tx, err := base.st.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(t.Context()) }()
	if _, err := tx.Exec(t.Context(),
		`SELECT set_config('app.tenant_id',$1,true),
		        set_config('app.user_id',$2,true)`,
		fmt.Sprint(identity.TenantID), fmt.Sprint(identity.UserID)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(t.Context(), `SET LOCAL ROLE vane_brief_writer`); err != nil {
		t.Fatal(err)
	}
	var visible bool
	if err := tx.QueryRow(t.Context(),
		`SELECT true
		   FROM read_canonical_brief_run_identity_v1($1)
		  WHERE task_id=$2
		    AND temporal_workflow_id=$3 AND temporal_run_id=$4
		    AND reference_schema_version=$5
		    AND reference_digest=$6 AND payload_digest=$7`,
		ref.SnapshotID, identity.TaskID,
		identity.TemporalWorkflowID, identity.TemporalRunID,
		ref.SchemaVersion, ref.ReferenceDigest, ref.PayloadDigest,
	).Scan(&visible); err != nil {
		t.Fatal(err)
	}
	if !visible {
		var tenantSetting, userSetting string
		_ = tx.QueryRow(t.Context(),
			`SELECT current_setting('app.tenant_id',true),
			        current_setting('app.user_id',true)`,
		).Scan(&tenantSetting, &userSetting)
		t.Fatalf("brief writer cannot see exact snapshot: tenant=%q user=%q",
			tenantSetting, userSetting)
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	batchID, err := base.st.CreatePushBatchForTaskRunV1(
		t.Context(), identity, ref, "brief-"+uuid.NewString())
	if err != nil {
		t.Fatalf("create exact batch: %v", err)
	}
	f := &canonicalBriefFixture{
		base: base, identity: identity, ref: ref, batchID: batchID,
		sourceID: sourceIDs[0], sourceName: "approved 0",
	}
	for i := range deliveries {
		itemURL := fmt.Sprintf("https://brief.test/%s/%d", uuid.NewString(), i)
		itemTitle := fmt.Sprintf("brief item %d", i+1)
		var publishedAt *time.Time
		if i%2 == 0 {
			value := time.Now().Add(-time.Duration(i+1) * time.Hour).
				Round(0).UTC().Truncate(time.Microsecond)
			publishedAt = &value
		}
		contentID, created, err := base.st.UpsertContentItem(
			t.Context(), &types.ContentItem{
				SourceID: sourceIDs[0], ExternalID: uuid.NewString(),
				CanonicalKey: itemURL, URL: itemURL,
				Title: itemTitle, PublishedAt: publishedAt,
				ContentHash: "hash-" + uuid.NewString(),
			})
		if err != nil || !created {
			t.Fatalf("create content %d: id=%d created=%v err=%v",
				i, contentID, created, err)
		}
		bodyMD := fmt.Sprintf("What changed %d\n\nWhy it matters", i+1)
		deliveryID, _, _, err := base.st.InsertDeliveryIdempotent(
			t.Context(), &types.Delivery{
				BatchID: batchID, UserID: base.userID,
				ContentItemID: &contentID, Score: float64(90 - i),
				BodyMD:   bodyMD,
				CardJSON: json.RawMessage(`{}`),
			})
		if err != nil {
			t.Fatalf("create delivery %d: %v", i, err)
		}
		f.contentID = append(f.contentID, contentID)
		f.deliveryID = append(f.deliveryID, deliveryID)
		f.bodyMD = append(f.bodyMD, bodyMD)
		f.itemTitle = append(f.itemTitle, itemTitle)
		f.itemURL = append(f.itemURL, itemURL)
		f.published = append(f.published, publishedAt)
		var createdAt time.Time
		if err := base.st.pool.QueryRow(t.Context(),
			`SELECT created_at FROM deliveries WHERE id=$1`,
			deliveryID).Scan(&createdAt); err != nil {
			t.Fatal(err)
		}
		f.deliveryAt = append(f.deliveryAt,
			createdAt.Round(0).UTC().Truncate(time.Microsecond))
	}
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		var stageTableExists bool
		if err := base.st.pool.QueryRow(ctx,
			`SELECT to_regclass('public.canonical_brief_stages') IS NOT NULL`,
		).Scan(&stageTableExists); err == nil && stageTableExists {
			cleanupExec(ctx, t, base.st,
				`DELETE FROM canonical_brief_stages WHERE tenant_id=$1`,
				base.tenantID)
		}
		cleanupExec(ctx, t, base.st,
			`DELETE FROM brief_snapshots WHERE tenant_id=$1`, base.tenantID)
		cleanupExec(ctx, t, base.st,
			`DELETE FROM task_run_outcomes WHERE tenant_id=$1`, base.tenantID)
		cleanupExec(ctx, t, base.st,
			`DELETE FROM deliveries WHERE batch_id=$1`, batchID)
		cleanupExec(ctx, t, base.st,
			`DELETE FROM push_batches WHERE id=$1`, batchID)
		for _, contentID := range f.contentID {
			cleanupExec(ctx, t, base.st,
				`DELETE FROM content_sources WHERE content_item_id=$1`, contentID)
			cleanupExec(ctx, t, base.st,
				`DELETE FROM content_items WHERE id=$1`, contentID)
		}
	})
	return f
}

func (f *canonicalBriefFixture) finalizedContentOutcome(
	t *testing.T,
) types.RunOutcomeV1 {
	t.Helper()
	marker, err := f.base.st.CreatePendingRunOutcomeV1(
		t.Context(), f.identity, f.ref)
	if err != nil {
		t.Fatalf("create pending outcome: %v", err)
	}
	outcome, err := (types.RunOutcomeV1{
		RunOutcomeMarkerV1: marker,
		Result:             types.RunResultContent,
		SourceCoverage:     types.RunCompletenessComplete,
		Processing:         types.RunCompletenessComplete,
		FinalizedAt:        time.Now(),
	}).Seal()
	if err != nil {
		t.Fatal(err)
	}
	finalized, err := f.base.st.FinalizeRunOutcomeV1(
		t.Context(), f.identity, f.ref, outcome)
	if err != nil {
		t.Fatalf("finalize outcome: %v", err)
	}
	return finalized
}

func (f *canonicalBriefFixture) draft(
	t *testing.T, outcome types.RunOutcomeV1,
) types.BriefDraftV1 {
	t.Helper()
	insights := make([]types.InsightV1, 0, len(f.deliveryID))
	// Reverse delivery ID order deliberately: rank is the frozen caller order,
	// never a read-time sort by score or database identity.
	for i := len(f.deliveryID) - 1; i >= 0; i-- {
		insights = append(insights, types.InsightV1{
			ID: f.deliveryID[i], RankPosition: len(insights) + 1,
			Title:        f.itemTitle[i],
			BodyMD:       f.bodyMD[i],
			SourceTitle:  f.sourceName,
			SourceURL:    f.itemURL[i],
			PublishedAt:  f.published[i],
			DiscoveredAt: f.deliveryAt[i],
		})
	}
	return types.BriefDraftV1{
		SchemaVersion: types.BriefSchemaVersionV1,
		RunOutcomeID:  outcome.ID, RunSnapshotID: f.ref.SnapshotID,
		PushBatchID: f.batchID, TenantID: f.identity.TenantID,
		UserID: f.identity.UserID, TaskID: f.identity.TaskID,
		GeneratedAt: time.Now(), Insights: insights,
	}
}

func TestCanonicalBriefDarkStoreLifecycleAndExactReplay(t *testing.T) {
	f := newCanonicalBriefFixture(t, 2)
	firstMarker, err := f.base.st.CreatePendingRunOutcomeV1(
		t.Context(), f.identity, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	replayedMarker, err := f.base.st.CreatePendingRunOutcomeV1(
		t.Context(), f.identity, f.ref)
	if err != nil || replayedMarker != firstMarker {
		t.Fatalf("marker replay=%+v want=%+v err=%v",
			replayedMarker, firstMarker, err)
	}
	outcome, err := (types.RunOutcomeV1{
		RunOutcomeMarkerV1: firstMarker,
		Result:             types.RunResultContent,
		SourceCoverage:     types.RunCompletenessPartial,
		Processing:         types.RunCompletenessComplete,
		FinalizedAt:        time.Now(),
	}).Seal()
	if err != nil {
		t.Fatal(err)
	}
	firstOutcome, err := f.base.st.FinalizeRunOutcomeV1(
		t.Context(), f.identity, f.ref, outcome)
	if err != nil {
		t.Fatal(err)
	}
	replayedOutcome, err := f.base.st.FinalizeRunOutcomeV1(
		t.Context(), f.identity, f.ref, outcome)
	if err != nil || replayedOutcome.Digest != firstOutcome.Digest {
		t.Fatalf("outcome replay digest=%q want=%q err=%v",
			replayedOutcome.Digest, firstOutcome.Digest, err)
	}

	draft := f.draft(t, firstOutcome)
	firstBrief, err := f.base.st.FreezeBriefV1(
		t.Context(), f.identity, f.ref, draft)
	if err != nil {
		t.Fatal(err)
	}
	replayedBrief, err := f.base.st.FreezeBriefV1(
		t.Context(), f.identity, f.ref, draft)
	if err != nil || replayedBrief.ID != firstBrief.ID ||
		replayedBrief.Digest != firstBrief.Digest {
		t.Fatalf("brief replay=%+v want id/digest=%d/%s err=%v",
			replayedBrief, firstBrief.ID, firstBrief.Digest, err)
	}
	if firstBrief.Insights[0].ID != f.deliveryID[1] ||
		firstBrief.Insights[1].ID != f.deliveryID[0] {
		t.Fatalf("frozen rank was reordered: %+v", firstBrief.Insights)
	}
	existingContentID := f.contentID[0]
	replayedDeliveryID, existed, _, err := f.base.st.InsertDeliveryIdempotent(
		t.Context(), &types.Delivery{
			BatchID: f.batchID, UserID: f.identity.UserID,
			ContentItemID: &existingContentID, BodyMD: "ignored replay",
		})
	if err != nil || !existed || replayedDeliveryID != f.deliveryID[0] {
		t.Fatalf("sealed exact delivery replay id=%d existed=%v err=%v",
			replayedDeliveryID, existed, err)
	}
	lateURL := "https://brief.test/late/" + uuid.NewString()
	lateContentID, created, err := f.base.st.UpsertContentItem(
		t.Context(), &types.ContentItem{
			SourceID: f.sourceID, ExternalID: uuid.NewString(),
			CanonicalKey: lateURL, URL: lateURL, Title: "late",
			ContentHash: "hash-" + uuid.NewString(),
		})
	if err != nil || !created {
		t.Fatalf("create late content: id=%d created=%v err=%v",
			lateContentID, created, err)
	}
	f.contentID = append(f.contentID, lateContentID)
	if _, _, _, err := f.base.st.InsertDeliveryIdempotent(
		t.Context(), &types.Delivery{
			BatchID: f.batchID, UserID: f.identity.UserID,
			ContentItemID: &lateContentID, BodyMD: "must be rejected",
		}); err == nil {
		t.Fatal("sealed batch accepted a new delivery")
	}
	loaded, found, err := f.base.st.LoadBriefV1(
		t.Context(), f.identity, f.ref)
	if err != nil || !found || loaded.Digest != firstBrief.Digest {
		t.Fatalf("load found=%v digest=%q err=%v", found, loaded.Digest, err)
	}

	conflict := draft
	conflict.Insights = append([]types.InsightV1(nil), draft.Insights...)
	conflict.Insights[0].BodyMD = "different frozen content"
	if _, err := f.base.st.FreezeBriefV1(
		t.Context(), f.identity, f.ref, conflict); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("different replay error=%v want conflict", err)
	}
}

func TestRunOutcomeClaimUsesDatabaseTimeAndSemanticReplay(t *testing.T) {
	f := newCanonicalBriefFixture(t, 0)
	marker, err := f.base.st.CreatePendingRunOutcomeV1(
		t.Context(), f.identity, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	claim := types.RunOutcomeClaimV1{
		RunOutcomeMarkerV1: marker,
		Result:             types.RunResultQuiet,
		SourceCoverage:     types.RunCompletenessComplete,
		Processing:         types.RunCompletenessComplete,
	}
	before := time.Now().UTC().Add(-time.Second)
	first, err := f.base.st.FinalizeRunOutcomeClaimV1(
		t.Context(), f.identity, f.ref, claim)
	if err != nil {
		t.Fatal(err)
	}
	if first.FinalizedAt.Before(before) || first.Digest == "" {
		t.Fatalf("database-sealed outcome = %+v", first)
	}
	replayed, err := f.base.st.FinalizeRunOutcomeClaimV1(
		t.Context(), f.identity, f.ref, claim)
	if err != nil || replayed != first {
		t.Fatalf("semantic replay = %+v, %v; want %+v", replayed, err, first)
	}
	different := claim
	different.Processing = types.RunCompletenessPartial
	if _, err := f.base.st.FinalizeRunOutcomeClaimV1(
		t.Context(), f.identity, f.ref, different,
	); err == nil || !errors.Is(err, types.ErrConflict) {
		t.Fatalf("different terminal claim error = %v, want conflict", err)
	}
}

func TestRunOutcomeWorkflowRecoveryConcurrentCAS(t *testing.T) {
	t.Run("same semantic converges", func(t *testing.T) {
		f := newCanonicalBriefFixture(t, 0)
		marker, err := f.base.st.CreatePendingRunOutcomeV1(
			t.Context(), f.identity, f.ref)
		if err != nil {
			t.Fatal(err)
		}
		claim := types.RunOutcomeClaimV1{
			RunOutcomeMarkerV1: marker,
			Result:             types.RunResultQuiet,
			SourceCoverage:     types.RunCompletenessComplete,
			Processing:         types.RunCompletenessComplete,
		}
		results := make(chan types.RunOutcomeV1, 2)
		errs := make(chan error, 2)
		go func() {
			outcome, err := f.base.st.FinalizeRunOutcomeClaimV1(
				t.Context(), f.identity, f.ref, claim)
			results <- outcome
			errs <- err
		}()
		go func() {
			outcome, err := f.base.st.FinalizeRecoveredRunOutcomeClaimV1(
				t.Context(), f.identity, claim)
			results <- outcome
			errs <- err
		}()
		first, second := <-results, <-results
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
		if first.Digest == "" || first != second {
			t.Fatalf("concurrent same claim outcomes = %+v / %+v",
				first, second)
		}
	})

	t.Run("different semantic conflicts", func(t *testing.T) {
		f := newCanonicalBriefFixture(t, 0)
		marker, err := f.base.st.CreatePendingRunOutcomeV1(
			t.Context(), f.identity, f.ref)
		if err != nil {
			t.Fatal(err)
		}
		quiet := types.RunOutcomeClaimV1{
			RunOutcomeMarkerV1: marker,
			Result:             types.RunResultQuiet,
			SourceCoverage:     types.RunCompletenessComplete,
			Processing:         types.RunCompletenessComplete,
		}
		failed := types.RunOutcomeClaimV1{
			RunOutcomeMarkerV1: marker,
			Result:             types.RunResultFailed,
			SourceCoverage:     types.RunCompletenessPartial,
			Processing:         types.RunCompletenessPartial,
			FailureCode:        "workflow_failed",
			FailureMessage:     "workflow failed before a reliable terminal result",
		}
		errs := make(chan error, 2)
		go func() {
			_, err := f.base.st.FinalizeRunOutcomeClaimV1(
				t.Context(), f.identity, f.ref, quiet)
			errs <- err
		}()
		go func() {
			_, err := f.base.st.FinalizeRecoveredRunOutcomeClaimV1(
				t.Context(), f.identity, failed)
			errs <- err
		}()
		first, second := <-errs, <-errs
		conflicts := 0
		for _, err := range []error{first, second} {
			if errors.Is(err, types.ErrConflict) {
				conflicts++
			} else if err != nil {
				t.Fatalf("unexpected CAS error = %v", err)
			}
		}
		if conflicts != 1 {
			t.Fatalf("conflicts = %d, errors=%v/%v", conflicts, first, second)
		}
	})
}

func TestCanonicalBriefDarkStoreRequiresContentAndCompleteDeliverySet(t *testing.T) {
	t.Run("quiet outcome", func(t *testing.T) {
		f := newCanonicalBriefFixture(t, 1)
		marker, err := f.base.st.CreatePendingRunOutcomeV1(
			t.Context(), f.identity, f.ref)
		if err != nil {
			t.Fatal(err)
		}
		outcome, err := (types.RunOutcomeV1{
			RunOutcomeMarkerV1: marker,
			Result:             types.RunResultQuiet,
			SourceCoverage:     types.RunCompletenessComplete,
			Processing:         types.RunCompletenessComplete,
			FinalizedAt:        time.Now(),
		}).Seal()
		if err != nil {
			t.Fatal(err)
		}
		outcome, err = f.base.st.FinalizeRunOutcomeV1(
			t.Context(), f.identity, f.ref, outcome)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.base.st.FreezeBriefV1(
			t.Context(), f.identity, f.ref, f.draft(t, outcome),
		); !errors.Is(err, types.ErrConflict) {
			t.Fatalf("quiet brief error=%v want conflict", err)
		}
	})

	t.Run("omitted delivery", func(t *testing.T) {
		f := newCanonicalBriefFixture(t, 2)
		outcome := f.finalizedContentOutcome(t)
		draft := f.draft(t, outcome)
		draft.Insights = draft.Insights[:1]
		if _, err := f.base.st.FreezeBriefV1(
			t.Context(), f.identity, f.ref, draft,
		); !errors.Is(err, types.ErrConflict) {
			t.Fatalf("partial batch error=%v want conflict", err)
		}
	})
}

func TestCanonicalBriefDarkStoreRejectsUndurableSourceClaims(t *testing.T) {
	f := newCanonicalBriefFixture(t, 1)
	outcome := f.finalizedContentOutcome(t)
	valid := f.draft(t, outcome)
	cases := map[string]func(*types.BriefDraftV1){
		"item title": func(d *types.BriefDraftV1) {
			d.Insights[0].Title = "caller invented title"
		},
		"source URL": func(d *types.BriefDraftV1) {
			d.Insights[0].SourceURL = "https://phishing.example/claim"
		},
		"source title": func(d *types.BriefDraftV1) {
			d.Insights[0].SourceTitle = "Impersonated source"
		},
		"publication time": func(d *types.BriefDraftV1) {
			value := time.Now().Add(24 * time.Hour)
			d.Insights[0].PublishedAt = &value
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			candidate := valid
			candidate.Insights = append(
				[]types.InsightV1(nil), valid.Insights...)
			mutate(&candidate)
			if _, err := f.base.st.FreezeBriefV1(
				t.Context(), f.identity, f.ref, candidate,
			); !errors.Is(err, types.ErrConflict) {
				t.Fatalf("undurable claim error=%v want conflict", err)
			}
		})
	}
	if _, err := f.base.st.FreezeBriefV1(
		t.Context(), f.identity, f.ref, valid); err != nil {
		t.Fatalf("durable source evidence was rejected: %v", err)
	}
}

func TestCanonicalBriefDarkStoreRejectsDeliveryWithoutSourceEvidence(
	t *testing.T,
) {
	f := newCanonicalBriefFixture(t, 1)
	if _, _, _, err := f.base.st.InsertDeliveryIdempotent(
		t.Context(), &types.Delivery{
			BatchID: f.batchID, UserID: f.identity.UserID,
			BodyMD: "delivery without a durable content item",
		},
	); err != nil {
		t.Fatal(err)
	}
	outcome := f.finalizedContentOutcome(t)
	if _, err := f.base.st.FreezeBriefV1(
		t.Context(), f.identity, f.ref, f.draft(t, outcome),
	); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("incomplete delivery evidence error=%v want conflict", err)
	}
}

func TestCanonicalBriefDarkStoreSealsDeliveryEvidence(t *testing.T) {
	f := newCanonicalBriefFixture(t, 1)
	f.bodyMD[0] = "corrected before canonical seal"
	if _, err := f.base.st.pool.Exec(t.Context(), `
		UPDATE deliveries SET body_md=$2 WHERE id=$1`,
		f.deliveryID[0], f.bodyMD[0],
	); err != nil {
		t.Fatalf("open delivery evidence correction was rejected: %v", err)
	}
	outcome := f.finalizedContentOutcome(t)
	if _, err := f.base.st.FreezeBriefV1(
		t.Context(), f.identity, f.ref, f.draft(t, outcome),
	); err != nil {
		t.Fatal(err)
	}
	var openBatchID int64
	if err := f.base.st.pool.QueryRow(t.Context(), `
		INSERT INTO push_batches (tenant_id,user_id)
		VALUES ($1,$2) RETURNING id`,
		f.identity.TenantID, f.identity.UserID,
	).Scan(&openBatchID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(ctx, t, f.base.st,
			`DELETE FROM push_batches WHERE id=$1`, openBatchID)
	})
	for name, mutation := range map[string]struct {
		statement string
		value     any
		want      string
	}{
		"body": {
			statement: `UPDATE deliveries SET body_md=$2 WHERE id=$1`,
			value:     "mutated",
			want:      "canonical delivery evidence is immutable",
		},
		"batch": {
			statement: `UPDATE deliveries SET batch_id=$2 WHERE id=$1`,
			value:     openBatchID,
			want:      "canonical delivery scope is immutable",
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := f.base.st.pool.Exec(
				t.Context(), mutation.statement,
				f.deliveryID[0], mutation.value,
			); err == nil || !strings.Contains(
				err.Error(), mutation.want) {
				t.Fatalf("sealed delivery evidence mutation=%v", err)
			}
		})
	}
	if _, err := f.base.st.pool.Exec(t.Context(), `
		UPDATE deliveries
		   SET status='sent',feishu_message_id='receipt-after-seal',
		       sent_at=clock_timestamp()
		 WHERE id=$1`, f.deliveryID[0],
	); err != nil {
		t.Fatalf("receipt update after seal was rejected: %v", err)
	}
}

func TestCanonicalBriefPendingMarkerConvergesUnderRace(t *testing.T) {
	f := newCanonicalBriefFixture(t, 0)
	const workers = 12
	ids := make([]int64, workers)
	errs := make([]error, workers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			marker, err := f.base.st.CreatePendingRunOutcomeV1(
				t.Context(), f.identity, f.ref)
			ids[i], errs[i] = marker.ID, err
		}()
	}
	close(start)
	wg.Wait()
	for i := range workers {
		if errs[i] != nil {
			t.Fatalf("worker %d: %v", i, errs[i])
		}
		if ids[i] != ids[0] || ids[i] <= 0 {
			t.Fatalf("marker ids diverged: %v", ids)
		}
	}
}
