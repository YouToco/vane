package store

import (
	"encoding/json"
	"errors"
	"fmt"
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
		   FROM task_run_snapshots
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3 AND task_id=$4
		    AND temporal_workflow_id=$5 AND temporal_run_id=$6
		    AND reference_schema_version=$7
		    AND reference_digest=$8 AND payload_digest=$9`,
		ref.SnapshotID, identity.TenantID, identity.UserID, identity.TaskID,
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
		sourceID: sourceIDs[0],
	}
	for i := range deliveries {
		itemURL := fmt.Sprintf("https://brief.test/%s/%d", uuid.NewString(), i)
		contentID, created, err := base.st.UpsertContentItem(
			t.Context(), &types.ContentItem{
				SourceID: sourceIDs[0], ExternalID: uuid.NewString(),
				CanonicalKey: itemURL, URL: itemURL,
				Title:       fmt.Sprintf("brief item %d", i+1),
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
			Title:        fmt.Sprintf("Insight %d", i+1),
			BodyMD:       f.bodyMD[i],
			SourceTitle:  "Official",
			SourceURL:    fmt.Sprintf("https://example.com/%d", i+1),
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
