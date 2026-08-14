package store

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/server/types"
)

type compiledContentWriteResult struct {
	id    int64
	isNew bool
	err   error
}

func TestCompiledRunFetchWrites_ExactSourceAndAtomicHealth(t *testing.T) {
	f := newCompiledRunWriteFixture(t)
	ctx := t.Context()

	item := compiledFetchTestItem(f.sourceA, "happy")
	id, isNew, err := f.base.st.UpsertContentItemForTaskRunV1(
		ctx, f.idA, f.refA, f.sourceA, &item)
	if err != nil {
		t.Fatalf("exact content upsert: %v", err)
	}
	if !isNew || id <= 0 {
		t.Fatalf("exact content upsert = (%d,%v), want new row", id, isNew)
	}
	f.content = append(f.content, id)
	assertCompiledContentAppearance(t, f.base.st, id, f.sourceA, 1)

	// A retry reuses the global row and appearance rather than creating either
	// twice. The longer body update remains inside the same authorized tx.
	item.Content = "a much longer replacement body"
	retryID, retryNew, err := f.base.st.UpsertContentItemForTaskRunV1(
		ctx, f.idA, f.refA, f.sourceA, &item)
	if err != nil {
		t.Fatalf("retry exact content upsert: %v", err)
	}
	if retryNew || retryID != id {
		t.Fatalf("retry = (%d,%v), want existing %d", retryID, retryNew, id)
	}
	assertCompiledContentAppearance(t, f.base.st, id, f.sourceA, 1)

	lastFetched := time.Now().UTC().Truncate(time.Microsecond)
	nextFetch := lastFetched.Add(17 * time.Minute)
	updated, err := f.base.st.UpdateFetchTargetStateForTaskRunV1(
		ctx, f.idA, f.refA, f.sourceA, lastFetched, nextFetch, 3)
	if err != nil {
		t.Fatalf("exact source health update: %v", err)
	}
	if !updated {
		t.Fatal("exact source health update unexpectedly reported stale source")
	}
	var gotFailCount int
	if err := f.base.st.pool.QueryRow(ctx,
		`SELECT fail_count FROM fetch_targets WHERE id = $1`, f.sourceA,
	).Scan(&gotFailCount); err != nil {
		t.Fatal(err)
	}
	if gotFailCount != 3 {
		t.Fatalf("fail_count = %d, want 3", gotFailCount)
	}

	outside := compiledFetchTestItem(f.sourceB, "outside-snapshot")
	cleanupCompiledTestContentByCanonical(t, f.base.st, outside.CanonicalKey)
	if _, _, err := f.base.st.UpsertContentItemForTaskRunV1(
		ctx, f.idA, f.refA, f.sourceB, &outside,
	); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("source outside frozen snapshot error = %v, want validation", err)
	}
	assertNoContentForCanonicalKey(t, f.base.st, outside.CanonicalKey)
}

func TestCompiledRunFetchWrites_AdoptsSourceFreeCanonicalForV1(
	t *testing.T,
) {
	f := newCompiledRunWriteFixture(t)
	ctx := t.Context()
	item := compiledFetchTestItem(f.sourceA, "source-free-first")
	cleanupCompiledTestContentByCanonical(t, f.base.st, item.CanonicalKey)

	var sourceFreeID int64
	if err := f.base.st.pool.QueryRow(ctx,
		`INSERT INTO content_items (
		    source_id,external_id,canonical_key,url,title,content,
		    content_hash,kind
		 ) VALUES (NULL,$1,$2,$3,$4,$5,$6,$7)
		 RETURNING id`,
		item.ExternalID, item.CanonicalKey, item.URL, item.Title,
		"short", item.ContentHash, item.Kind,
	).Scan(&sourceFreeID); err != nil {
		t.Fatal(err)
	}
	f.content = append(f.content, sourceFreeID)

	gotID, isNew, err := f.base.st.UpsertContentItemForTaskRunV1(
		ctx, f.idA, f.refA, f.sourceA, &item)
	if err != nil || isNew || gotID != sourceFreeID {
		t.Fatalf("V1 adoption=(id=%d new=%v err=%v), want existing %d",
			gotID, isNew, err, sourceFreeID)
	}
	var legacySourceID int64
	if err := f.base.st.pool.QueryRow(ctx,
		`SELECT source_id FROM content_items WHERE id=$1`, sourceFreeID,
	).Scan(&legacySourceID); err != nil {
		t.Fatalf("V1 reader still sees nullable source_id: %v", err)
	}
	if legacySourceID != f.sourceA {
		t.Fatalf("adopted legacy source=%d, want %d",
			legacySourceID, f.sourceA)
	}
	assertCompiledContentAppearance(
		t, f.base.st, sourceFreeID, f.sourceA, 1)
}

func TestCompiledRunFetchWrites_ConfigDriftWinningRaceIsZeroWrite(t *testing.T) {
	f := newCompiledRunWriteFixture(t)
	ctx := t.Context()
	item := compiledFetchTestItem(f.sourceA, "config-drift")
	cleanupCompiledTestContentByCanonical(t, f.base.st, item.CanonicalKey)

	// Win the source-row race with a reconfiguration and retain the row lock.
	// The stale network response must wait, re-evaluate the exact predicate, and
	// then commit neither content nor an appearance for the reused source ID.
	locker, err := f.base.st.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer locker.Rollback(ctx) //nolint:errcheck -- cleanup fallback
	if _, err := locker.Exec(ctx,
		`UPDATE fetch_targets SET config = $2::jsonb WHERE id = $1`,
		f.sourceA, json.RawMessage(`{"query":"reconfigured-B"}`),
	); err != nil {
		t.Fatal(err)
	}

	callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	result := make(chan compiledContentWriteResult, 1)
	go func() {
		id, isNew, callErr := f.base.st.UpsertContentItemForTaskRunV1(
			callCtx, f.idA, f.refA, f.sourceA, &item)
		result <- compiledContentWriteResult{id: id, isNew: isNew, err: callErr}
	}()
	assertCompiledCallWaitsForLock(t, result)
	if err := locker.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	got := <-result
	if !errors.Is(got.err, types.ErrNotFound) {
		t.Fatalf("stale content result = %+v, want not found", got)
	}
	assertNoContentForCanonicalKey(t, f.base.st, item.CanonicalKey)

	updated, err := f.base.st.UpdateFetchTargetStateForTaskRunV1(
		ctx, f.idA, f.refA, f.sourceA, time.Now().UTC(), time.Now().UTC().Add(time.Hour), 9)
	if err != nil {
		t.Fatalf("stale health update: %v", err)
	}
	if updated {
		t.Fatal("stale snapshot advanced reconfigured source health")
	}
	var failCount int
	var status types.FetchTargetStatus
	if err := f.base.st.pool.QueryRow(ctx,
		`SELECT fail_count, status FROM fetch_targets WHERE id = $1`, f.sourceA,
	).Scan(&failCount, &status); err != nil {
		t.Fatal(err)
	}
	if failCount != 0 || status != types.FetchTargetStatusActive {
		t.Fatalf("stale health mutated source: fail_count=%d status=%q", failCount, status)
	}
	disabled, err := f.base.st.DisableFetchTargetIfActiveForTaskRunV1(
		ctx, f.idA, f.refA, f.sourceA)
	if err != nil {
		t.Fatalf("stale disable: %v", err)
	}
	if disabled {
		t.Fatal("stale snapshot disabled reconfigured source")
	}
}

func TestCompiledRunFetchWrites_RevocationWinningRaceIsZeroWrite(t *testing.T) {
	f := newCompiledRunWriteFixture(t)
	ctx := t.Context()
	item := compiledFetchTestItem(f.sourceA, "revoked")
	cleanupCompiledTestContentByCanonical(t, f.base.st, item.CanonicalKey)

	locker, err := f.base.st.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer locker.Rollback(ctx) //nolint:errcheck -- cleanup fallback
	if _, err := locker.Exec(ctx,
		`UPDATE schedules SET status = $2 WHERE id = $1`,
		f.taskA, types.ScheduleStatusPaused,
	); err != nil {
		t.Fatal(err)
	}

	callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	result := make(chan compiledContentWriteResult, 1)
	go func() {
		id, isNew, callErr := f.base.st.UpsertContentItemForTaskRunV1(
			callCtx, f.idA, f.refA, f.sourceA, &item)
		result <- compiledContentWriteResult{id: id, isNew: isNew, err: callErr}
	}()
	assertCompiledCallWaitsForLock(t, result)
	if err := locker.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	got := <-result
	if !errors.Is(got.err, types.ErrNotFound) {
		t.Fatalf("revoked content result = %+v, want not found", got)
	}
	assertNoContentForCanonicalKey(t, f.base.st, item.CanonicalKey)
}

func TestCompiledRunDedup_TenantUserHistoryAcrossPrivateTasks(t *testing.T) {
	f := newCompiledRunWriteFixture(t)
	ctx := t.Context()
	now := time.Now().UTC()

	// A2 compiled tasks intentionally have no subscriptions. Both the current
	// task and another private task for tenant A must still contribute history.
	otherTaskA := "push-dedup-private-a-" + uuid.NewString()
	otherSourceA := f.base.createApprovedTask(t, otherTaskA, 1)[0]
	currentA := f.createContent(t, f.sourceA, "tenant-a-current")
	otherA := f.createContent(t, otherSourceA, "tenant-a-other-task")
	tenantB := f.createContent(t, f.sourceB, "tenant-b-private")
	setCompiledContentSimhash(t, f.base.st, currentA, 101, now)
	setCompiledContentSimhash(t, f.base.st, otherA, 202, now.Add(-time.Minute))
	setCompiledContentSimhash(t, f.base.st, tenantB, 303, now.Add(-2*time.Minute))

	// A pushed item whose source is no longer linked remains history through the
	// tenant+user delivery ledger. This also avoids expanding via memberships.
	deliveryOnlySource := insertCompiledTestSource(t, f.base, "delivery-only")
	deliveryOnly := f.createContent(t, deliveryOnlySource, "tenant-a-delivery-only")
	setCompiledContentSimhash(t, f.base.st, deliveryOnly, 404, now.Add(-3*time.Minute))
	insertCompiledTestDelivery(t, f.base.st, f.idA.TenantID, f.idA.UserID,
		"deleted-task-"+uuid.NewString(), deliveryOnly)

	historyA, err := f.base.st.ListRecentSimhashesForTaskRunV1(
		ctx, f.idA, f.refA, now.Add(-time.Hour), nil)
	if err != nil {
		t.Fatalf("tenant A simhash history: %v", err)
	}
	assertInt64Set(t, historyA, []int64{101, 202, 404})

	historyB, err := f.base.st.ListRecentSimhashesForTaskRunV1(
		ctx, f.idB, f.refB, now.Add(-time.Hour), nil)
	if err != nil {
		t.Fatalf("tenant B simhash history: %v", err)
	}
	assertInt64Set(t, historyB, []int64{303})

	excluded, err := f.base.st.ListRecentSimhashesForTaskRunV1(
		ctx, f.idA, f.refA, now.Add(-time.Hour), []int64{currentA})
	if err != nil {
		t.Fatalf("tenant A excluded simhash history: %v", err)
	}
	assertInt64Set(t, excluded, []int64{202, 404})
}

func TestCompiledRunCandidates_DeliveryScopeIsTenantUserAcrossTasks(t *testing.T) {
	f := newCompiledRunWriteFixture(t)
	ctx := t.Context()
	contentA := f.createContent(t, f.sourceA, "tenant-a-outside-candidate")
	contentB := f.createContent(t, f.sourceB, "tenant-b-candidate")
	setCompiledContentFetchedAt(t, f.base.st, contentA, time.Now().UTC().Add(time.Minute))

	// The same user's delivery in tenant A must not suppress tenant B.
	insertCompiledTestDelivery(t, f.base.st, f.idA.TenantID, f.idA.UserID,
		"another-a-task-"+uuid.NewString(), contentB)
	candidates, err := f.base.st.ListUnpushedForTaskRunV1(
		ctx, f.idB, f.refB, []int64{f.sourceB}, 10, 10)
	if err != nil {
		t.Fatalf("tenant B candidates after tenant A delivery: %v", err)
	}
	assertContentIDs(t, candidates, []int64{contentB})

	// A delivery from any other task in tenant B suppresses it for this user.
	insertCompiledTestDelivery(t, f.base.st, f.idB.TenantID, f.idB.UserID,
		"another-b-task-"+uuid.NewString(), contentB)
	candidates, err = f.base.st.ListUnpushedForTaskRunV1(
		ctx, f.idB, f.refB, []int64{f.sourceB}, 10, 10)
	if err != nil {
		t.Fatalf("tenant B candidates after tenant B delivery: %v", err)
	}
	assertContentIDs(t, candidates, nil)
}

func TestCompiledRunCandidates_ConfigDriftFiltersWholeSourceButKeepsSibling(t *testing.T) {
	f := newTaskRunSnapshotFixture(t)
	ctx := t.Context()
	taskID := f.taskID()
	frozen := f.createApprovedTask(t, taskID, 2)
	identity := scheduledRunIdentity(taskID, f.tenantID, f.userID,
		"run-candidate-drift-"+uuid.NewString())
	ref, err := f.st.CreateOrGetCompiledTaskRunSnapshotV1(ctx,
		CreateOrGetCompiledTaskRunSnapshotV1Params{
			Identity: identity,
			Policy:   testCompiledRunPolicyV1(t),
		})
	if err != nil {
		t.Fatal(err)
	}

	oldDrifted := compiledFetchTestItem(frozen[0], "drifted-old-a")
	oldDriftedID, _, err := f.st.UpsertContentItemForTaskRunV1(
		ctx, identity, ref, frozen[0], &oldDrifted)
	if err != nil {
		t.Fatal(err)
	}
	stable := compiledFetchTestItem(frozen[1], "stable-sibling")
	stableID, _, err := f.st.UpsertContentItemForTaskRunV1(
		ctx, identity, ref, frozen[1], &stable)
	if err != nil {
		t.Fatal(err)
	}
	contentIDs := []int64{oldDriftedID, stableID}
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(cleanupCtx, t, f.st,
			`DELETE FROM content_sources WHERE content_item_id = ANY($1)`, contentIDs)
		cleanupExec(cleanupCtx, t, f.st,
			`DELETE FROM content_items WHERE id = ANY($1)`, contentIDs)
	})

	if _, err := f.st.pool.Exec(ctx,
		`UPDATE fetch_targets SET config = '{"query":"new-B"}'::jsonb WHERE id = $1`,
		frozen[0]); err != nil {
		t.Fatal(err)
	}
	// Content written after reconfiguration shares the global source ID but is
	// semantically B, so neither this nor the old A content may enter snapshot A.
	newDrifted := compiledFetchTestItem(frozen[0], "drifted-new-b")
	newDriftedID, isNew, err := f.st.UpsertContentItem(ctx, &newDrifted)
	if err != nil || !isNew {
		t.Fatalf("create post-drift content: id=%d new=%v err=%v",
			newDriftedID, isNew, err)
	}
	contentIDs = append(contentIDs, newDriftedID)

	// Status is health, not semantic identity. A disabled but otherwise exact
	// sibling still contributes content already fetched before disablement.
	if _, err := f.st.pool.Exec(ctx,
		`UPDATE fetch_targets SET status = $2 WHERE id = $1`,
		frozen[1], types.FetchTargetStatusDisabled); err != nil {
		t.Fatal(err)
	}

	candidates, err := f.st.ListUnpushedForTaskRunV1(
		ctx, identity, ref, frozen, 10, 10)
	if err != nil {
		t.Fatal(err)
	}
	assertContentIDs(t, candidates, []int64{stableID})

	empty, err := f.st.ListUnpushedForTaskRunV1(
		ctx, identity, ref, []int64{frozen[0]}, 10, 10)
	if err != nil {
		t.Fatal(err)
	}
	if empty == nil || len(empty) != 0 {
		t.Fatalf("all-drifted candidates = %+v, want non-nil empty slice", empty)
	}
}

func TestCompiledRunCandidates_PerSourceCapUsesFrozenAppearance(t *testing.T) {
	f := newTaskRunSnapshotFixture(t)
	ctx := t.Context()
	taskID := f.taskID()
	frozen := f.createApprovedTask(t, taskID, 2)
	identity := scheduledRunIdentity(taskID, f.tenantID, f.userID,
		"run-cap-"+uuid.NewString())
	ref, err := f.st.CreateOrGetCompiledTaskRunSnapshotV1(ctx,
		CreateOrGetCompiledTaskRunSnapshotV1Params{
			Identity: identity,
			Policy:   testCompiledRunPolicyV1(t),
		})
	if err != nil {
		t.Fatal(err)
	}

	outsideA := insertCompiledTestSource(t, f, "outside-a")
	outsideB := insertCompiledTestSource(t, f, "outside-b")
	var contentIDs []int64
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		if len(contentIDs) > 0 {
			cleanupExec(cleanupCtx, t, f.st,
				`DELETE FROM content_sources WHERE content_item_id = ANY($1)`, contentIDs)
			cleanupExec(cleanupCtx, t, f.st,
				`DELETE FROM content_items WHERE id = ANY($1)`, contentIDs)
		}
	})

	// Two globally distinct first-observer sources both appear through frozen
	// source A. A cap partitioned by ci.source_id would admit both; the actual
	// frozen appearance partition must admit only the newer one.
	oldA := insertGlobalThenCompiledAppearance(
		t, f.st, identity, ref, outsideA, frozen[0], "old-a")
	contentIDs = append(contentIDs, oldA)
	newA := insertGlobalThenCompiledAppearance(
		t, f.st, identity, ref, outsideB, frozen[0], "new-a")
	contentIDs = append(contentIDs, newA)
	bItem := compiledFetchTestItem(frozen[1], "source-b")
	bID, _, err := f.st.UpsertContentItemForTaskRunV1(
		ctx, identity, ref, frozen[1], &bItem)
	if err != nil {
		t.Fatal(err)
	}
	contentIDs = append(contentIDs, bID)

	base := time.Now().UTC().Add(-time.Hour)
	setCompiledContentFetchedAt(t, f.st, oldA, base)
	setCompiledContentFetchedAt(t, f.st, newA, base.Add(2*time.Minute))
	setCompiledContentFetchedAt(t, f.st, bID, base.Add(time.Minute))

	candidates, err := f.st.ListUnpushedForTaskRunV1(
		ctx, identity, ref, frozen, 10, 1)
	if err != nil {
		t.Fatal(err)
	}
	assertContentIDs(t, candidates, []int64{newA, bID})
}

func compiledFetchTestItem(sourceID int64, suffix string) types.ContentItem {
	key := "https://compiled-fetch.test/" + suffix + "/" + uuid.NewString()
	simhash := int64(len(suffix) + 1)
	return types.ContentItem{
		SourceID: sourceID, ExternalID: uuid.NewString(), CanonicalKey: key,
		URL: key, Title: suffix, Content: "body-" + suffix,
		ContentHash: uuid.NewString(), Simhash: &simhash,
	}
}

func assertCompiledContentAppearance(
	t *testing.T,
	st *Store,
	contentID int64,
	sourceID int64,
	want int,
) {
	t.Helper()
	var count int
	if err := st.pool.QueryRow(t.Context(),
		`SELECT count(*) FROM content_sources
		  WHERE content_item_id = $1 AND source_id = $2`,
		contentID, sourceID,
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != want {
		t.Fatalf("content appearance count = %d, want %d", count, want)
	}
}

func assertNoContentForCanonicalKey(t *testing.T, st *Store, canonicalKey string) {
	t.Helper()
	var contentCount, appearanceCount int
	if err := st.pool.QueryRow(t.Context(),
		`SELECT count(*),
		        (SELECT count(*) FROM content_sources cs
		          JOIN content_items ci2 ON ci2.id = cs.content_item_id
		         WHERE ci2.canonical_key = $1)
		   FROM content_items ci
		  WHERE ci.canonical_key = $1`, canonicalKey,
	).Scan(&contentCount, &appearanceCount); err != nil {
		t.Fatal(err)
	}
	if contentCount != 0 || appearanceCount != 0 {
		t.Fatalf("stale write persisted content=%d appearances=%d",
			contentCount, appearanceCount)
	}
}

func cleanupCompiledTestContentByCanonical(
	t *testing.T,
	st *Store,
	canonicalKey string,
) {
	t.Helper()
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM content_sources
			  WHERE content_item_id IN (
			      SELECT id FROM content_items WHERE canonical_key = $1
			  )`, canonicalKey)
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM content_items WHERE canonical_key = $1`, canonicalKey)
	})
}

func assertCompiledCallWaitsForLock(
	t *testing.T,
	result <-chan compiledContentWriteResult,
) {
	t.Helper()
	select {
	case got := <-result:
		t.Fatalf("compiled write escaped row lock before winning tx committed: %+v", got)
	case <-time.After(100 * time.Millisecond):
	}
}

func setCompiledContentSimhash(
	t *testing.T,
	st *Store,
	contentID int64,
	simhash int64,
	fetchedAt time.Time,
) {
	t.Helper()
	if _, err := st.pool.Exec(t.Context(),
		`UPDATE content_items SET simhash = $2, fetched_at = $3 WHERE id = $1`,
		contentID, simhash, fetchedAt,
	); err != nil {
		t.Fatal(err)
	}
}

func setCompiledContentFetchedAt(
	t *testing.T,
	st *Store,
	contentID int64,
	fetchedAt time.Time,
) {
	t.Helper()
	if _, err := st.pool.Exec(t.Context(),
		`UPDATE content_items SET fetched_at = $2 WHERE id = $1`,
		contentID, fetchedAt,
	); err != nil {
		t.Fatal(err)
	}
}

func insertCompiledTestSource(
	t *testing.T,
	f *taskRunSnapshotFixture,
	suffix string,
) int64 {
	t.Helper()
	var sourceID int64
	if err := f.st.pool.QueryRow(t.Context(),
		`INSERT INTO fetch_targets (platform, capability, url, title, config, status)
		 VALUES ('web', 'search', $1, $2, '{}', 'active') RETURNING id`,
		f.urlPrefix+"/"+suffix+"/"+uuid.NewString(), suffix,
	).Scan(&sourceID); err != nil {
		t.Fatal(err)
	}
	return sourceID
}

func insertCompiledTestDelivery(
	t *testing.T,
	st *Store,
	tenantID int64,
	userID int64,
	scheduleID string,
	contentID int64,
) {
	t.Helper()
	var batchID int64
	if err := st.pool.QueryRow(t.Context(),
		`INSERT INTO push_batches
		    (tenant_id, user_id, idempotency_key, schedule_id, status)
		 VALUES ($1, $2, $3, $4, 'done') RETURNING id`,
		tenantID, userID, "compiled-test-"+uuid.NewString(), scheduleID,
	).Scan(&batchID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(t.Context(),
		`INSERT INTO deliveries
		    (tenant_id, batch_id, user_id, content_item_id, status)
		 VALUES ($1, $2, $3, $4, 'sent')`,
		tenantID, batchID, userID, contentID,
	); err != nil {
		t.Fatal(err)
	}
}

func insertGlobalThenCompiledAppearance(
	t *testing.T,
	st *Store,
	identity types.RunIdentity,
	ref types.RunSnapshotRef,
	globalSourceID int64,
	frozenSourceID int64,
	suffix string,
) int64 {
	t.Helper()
	global := compiledFetchTestItem(globalSourceID, suffix)
	id, isNew, err := st.UpsertContentItem(t.Context(), &global)
	if err != nil || !isNew {
		t.Fatalf("create global first observer: id=%d new=%v err=%v", id, isNew, err)
	}
	appearance := global
	appearance.SourceID = frozenSourceID
	appearance.ExternalID = "frozen-" + uuid.NewString()
	appearance.ContentHash = "frozen-" + uuid.NewString()
	gotID, gotNew, err := st.UpsertContentItemForTaskRunV1(
		t.Context(), identity, ref, frozenSourceID, &appearance)
	if err != nil {
		t.Fatalf("add frozen appearance: %v", err)
	}
	if gotNew || gotID != id {
		t.Fatalf("frozen appearance = (%d,%v), want existing %d", gotID, gotNew, id)
	}
	return id
}

func assertInt64Set(t *testing.T, got, want []int64) {
	t.Helper()
	gotSet := make(map[int64]int, len(got))
	for _, value := range got {
		gotSet[value]++
	}
	wantSet := make(map[int64]int, len(want))
	for _, value := range want {
		wantSet[value]++
	}
	if len(gotSet) != len(wantSet) {
		t.Fatalf("set cardinality = %v, want %v", got, want)
	}
	for value, count := range wantSet {
		if gotSet[value] != count {
			t.Fatalf("set = %v, want %v", got, want)
		}
	}
}
