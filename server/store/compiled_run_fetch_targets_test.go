package store

import (
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/server/types"
)

func TestCompiledRunSourceQueriesRejectInvalidInputs(t *testing.T) {
	tests := []struct {
		name string
		call func(*Store) error
	}{
		{
			name: "due source set is nil",
			call: func(st *Store) error {
				_, err := st.ListDueFetchTargetsByIDs(t.Context(), nil)
				return err
			},
		},
		{
			name: "due source set contains zero",
			call: func(st *Store) error {
				_, err := st.ListDueFetchTargetsByIDs(t.Context(), []int64{1, 0})
				return err
			},
		},
		{
			name: "due source set contains negative id",
			call: func(st *Store) error {
				_, err := st.ListDueFetchTargetsByIDs(t.Context(), []int64{-1})
				return err
			},
		},
		{
			name: "due source set contains duplicate ids",
			call: func(st *Store) error {
				_, err := st.ListDueFetchTargetsByIDs(t.Context(), []int64{2, 2})
				return err
			},
		},
		{
			name: "candidate user id is zero",
			call: func(st *Store) error {
				_, err := st.ListUnpushedForTaskRunV1(
					t.Context(), types.RunIdentity{}, types.RunSnapshotRef{},
					[]int64{1}, 10, 2)
				return err
			},
		},
		{
			name: "candidate limit is zero",
			call: func(st *Store) error {
				_, err := st.ListUnpushedForTaskRunV1(
					t.Context(), types.RunIdentity{}, types.RunSnapshotRef{},
					[]int64{1}, 0, 2)
				return err
			},
		},
		{
			name: "candidate source cap is zero",
			call: func(st *Store) error {
				_, err := st.ListUnpushedForTaskRunV1(
					t.Context(), types.RunIdentity{}, types.RunSnapshotRef{},
					[]int64{1}, 10, 0)
				return err
			},
		},
		{
			name: "candidate source set is empty",
			call: func(st *Store) error {
				_, err := st.ListUnpushedForTaskRunV1(
					t.Context(), types.RunIdentity{}, types.RunSnapshotRef{},
					[]int64{}, 10, 2)
				return err
			},
		},
		{
			name: "candidate source set contains duplicate ids",
			call: func(st *Store) error {
				_, err := st.ListUnpushedForTaskRunV1(
					t.Context(), types.RunIdentity{}, types.RunSnapshotRef{},
					[]int64{1, 1}, 10, 2)
				return err
			},
		},
		{
			name: "display content id is zero",
			call: func(st *Store) error {
				_, _, err := st.FetchTargetForContentFromIDs(t.Context(), 0, []int64{1})
				return err
			},
		},
		{
			name: "display source set is empty",
			call: func(st *Store) error {
				_, _, err := st.FetchTargetForContentFromIDs(t.Context(), 1, nil)
				return err
			},
		},
		{
			name: "display source set contains duplicate ids",
			call: func(st *Store) error {
				_, _, err := st.FetchTargetForContentFromIDs(t.Context(), 1, []int64{3, 3})
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.call(new(Store))
			if err == nil {
				t.Fatal("expected validation error")
			}
			if !errors.Is(err, types.ErrValidation) {
				t.Fatalf("error = %v, want validation", err)
			}
			if got := types.CodeOf(err); got != types.CodeValidation {
				t.Fatalf("code = %q, want %q", got, types.CodeValidation)
			}
		})
	}
}

func TestCompiledRunSourceQueriesDoNotExpandMutableScope(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "compiled_run_fetch_targets.go", nil, 0)
	if err != nil {
		t.Fatalf("parse compiled query source: %v", err)
	}

	forbidden := []string{"task_fetch_targets", "subscriptions"}
	ast.Inspect(file, func(node ast.Node) bool {
		literal, ok := node.(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			return true
		}
		value, err := strconv.Unquote(literal.Value)
		if err != nil {
			t.Fatalf("unquote source literal: %v", err)
		}
		for _, table := range forbidden {
			if strings.Contains(strings.ToLower(value), table) {
				t.Errorf("compiled query must not depend on mutable scope table %q", table)
			}
		}
		return true
	})
}

func TestCompiledRunSourceQueries(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("DATABASE_URL is not set; skipping compiled run source integration test")
	}
	ctx := t.Context()

	if err := Migrate(ctx, dbURL); err != nil {
		t.Fatalf("Migrate() failed: %v", err)
	}
	st, err := New(ctx, dbURL)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	registerStoreClose(t, st)

	owner, err := st.UpsertUserByOpenID(ctx, "ou_compiled_owner_"+uuid.NewString(), "compiled-owner")
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	userIDs := []int64{owner.ID}
	var sourceIDs []int64
	var contentIDs []int64
	var taskID string
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(cleanupCtx, t, st, `DELETE FROM deliveries WHERE user_id = ANY($1)`, userIDs)
		cleanupExec(cleanupCtx, t, st, `DELETE FROM push_batches WHERE user_id = ANY($1)`, userIDs)
		if taskID != "" {
			cleanupExec(cleanupCtx, t, st,
				`DELETE FROM task_run_snapshots WHERE task_id = $1`, taskID)
			cleanupExec(cleanupCtx, t, st,
				`DELETE FROM schedules WHERE id = $1`, taskID)
		}
		if len(contentIDs) > 0 {
			cleanupExec(cleanupCtx, t, st, `DELETE FROM content_sources WHERE content_item_id = ANY($1)`, contentIDs)
			cleanupExec(cleanupCtx, t, st, `DELETE FROM content_items WHERE id = ANY($1)`, contentIDs)
		}
		if len(sourceIDs) > 0 {
			cleanupExec(cleanupCtx, t, st, `DELETE FROM fetch_targets WHERE id = ANY($1)`, sourceIDs)
		}
		cleanupExec(cleanupCtx, t, st, `DELETE FROM memberships WHERE user_id = ANY($1)`, userIDs)
		cleanupExec(cleanupCtx, t, st, `DELETE FROM users WHERE id = ANY($1)`, userIDs)
	})

	other, err := st.UpsertUserByOpenID(ctx, "ou_compiled_other_"+uuid.NewString(), "compiled-other")
	if err != nil {
		t.Fatalf("create other user: %v", err)
	}
	userIDs = append(userIDs, other.ID)
	attachTenant(t, st, owner.ID)
	attachTenant(t, st, other.ID)

	createdSources := make(map[int64]types.FetchTarget)
	newSource := func(name string) int64 {
		t.Helper()
		source := types.FetchTarget{
			Platform:   types.PlatformWeb,
			Capability: types.CapSearch,
			URL:        "https://example.com/compiled-" + name + "-" + uuid.NewString(),
			Title:      name,
			Config:     json.RawMessage(`{}`),
		}
		id, _, err := st.GetOrCreateFetchTarget(ctx, &source)
		if err != nil {
			t.Fatalf("create source %s: %v", name, err)
		}
		source.ID = id
		createdSources[id] = source
		sourceIDs = append(sourceIDs, id)
		return id
	}

	sourceA := newSource("a")
	sourceB := newSource("b")
	disabledSource := newSource("disabled")
	futureSource := newSource("future")
	outsideSource := newSource("outside")
	displaySource := newSource("display")

	taskID = "push-compiled-source-query-" + uuid.NewString()
	planned := make([]compiledPlanTarget, 0, 2)
	for _, sourceID := range []int64{sourceA, sourceB} {
		source := createdSources[sourceID]
		planned = append(planned, compiledPlanTarget{
			Platform: string(source.Platform), Capability: string(source.Capability),
			Title: source.Title, URL: source.URL, Config: source.Config,
		})
	}
	plan, err := json.Marshal(compiledFetchPlan{Targets: planned})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(ctx,
		`INSERT INTO schedules
		    (id, tenant_id, user_id, nl_description, spec_json, scope_json,
		     status, push_strictness)
		 VALUES ($1, 1, $2, 'compiled source query', '{}', '{}', 'active', 'normal')`,
		taskID, owner.ID); err != nil {
		t.Fatalf("create compiled source query schedule: %v", err)
	}
	if _, err := st.pool.Exec(ctx,
		`INSERT INTO schedule_playbooks (schedule_id, content, fetch_plan)
		 VALUES ($1, 'compiled source query', $2)`, taskID, plan); err != nil {
		t.Fatalf("create compiled source query playbook: %v", err)
	}
	for _, sourceID := range []int64{sourceA, sourceB} {
		if _, err := st.pool.Exec(ctx,
			`INSERT INTO task_fetch_targets (schedule_id, fetch_target_id) VALUES ($1, $2)`,
			taskID, sourceID); err != nil {
			t.Fatalf("link compiled source query source: %v", err)
		}
	}
	identity := scheduledRunIdentity(
		taskID, 1, owner.ID, "run-source-query-"+uuid.NewString())
	ref, err := st.CreateOrGetCompiledTaskRunSnapshotV1(ctx,
		CreateOrGetCompiledTaskRunSnapshotV1Params{
			Identity: identity,
			Policy:   testCompiledRunPolicyV1(t),
		})
	if err != nil {
		t.Fatalf("create compiled source query snapshot: %v", err)
	}

	newContent := func(sourceID int64, name string, fetchedAt time.Time) int64 {
		t.Helper()
		key := "https://example.com/compiled-item-" + name + "-" + uuid.NewString()
		id, created, err := st.UpsertContentItem(ctx, &types.ContentItem{
			SourceID:     sourceID,
			ExternalID:   "external-" + uuid.NewString(),
			CanonicalKey: key,
			URL:          key,
			Title:        name,
			Content:      "body-" + name,
			ContentHash:  "hash-" + uuid.NewString(),
		})
		if err != nil {
			t.Fatalf("create content %s: %v", name, err)
		}
		if !created {
			t.Fatalf("content %s unexpectedly existed", name)
		}
		if _, err := st.pool.Exec(ctx,
			`UPDATE content_items SET fetched_at = $2 WHERE id = $1`, id, fetchedAt); err != nil {
			t.Fatalf("set fetched_at for %s: %v", name, err)
		}
		contentIDs = append(contentIDs, id)
		return id
	}

	if _, err := st.pool.Exec(ctx,
		`UPDATE fetch_targets
		    SET status = $2, next_fetch_at = now() - interval '1 minute', fail_count = $3
		  WHERE id = $1`, sourceA, types.FetchTargetStatusActive, 4); err != nil {
		t.Fatalf("prepare source A health: %v", err)
	}
	if _, err := st.pool.Exec(ctx,
		`UPDATE fetch_targets
		    SET status = $2, next_fetch_at = now() - interval '1 minute', fail_count = $3
		  WHERE id = $1`, sourceB, types.FetchTargetStatusActive, 8); err != nil {
		t.Fatalf("prepare source B health: %v", err)
	}
	if _, err := st.pool.Exec(ctx,
		`UPDATE fetch_targets SET status = $2, next_fetch_at = now() - interval '1 minute'
		  WHERE id = $1`, disabledSource, types.FetchTargetStatusDisabled); err != nil {
		t.Fatalf("prepare disabled source: %v", err)
	}
	if _, err := st.pool.Exec(ctx,
		`UPDATE fetch_targets SET status = $2, next_fetch_at = now() + interval '1 hour'
		  WHERE id = $1`, futureSource, types.FetchTargetStatusActive); err != nil {
		t.Fatalf("prepare future source: %v", err)
	}
	if _, err := st.pool.Exec(ctx,
		`UPDATE fetch_targets SET status = $2, next_fetch_at = now() - interval '1 minute'
		  WHERE id = $1`, outsideSource, types.FetchTargetStatusActive); err != nil {
		t.Fatalf("prepare outside source: %v", err)
	}

	due, err := st.ListDueFetchTargetsByIDs(ctx, []int64{sourceB, futureSource, sourceA, disabledSource})
	if err != nil {
		t.Fatalf("ListDueFetchTargetsByIDs(): %v", err)
	}
	if len(due) != 2 {
		t.Fatalf("due source count = %d, want 2: %+v", len(due), due)
	}
	if due[0].ID != sourceA || due[1].ID != sourceB {
		t.Fatalf("due source ids = [%d %d], want deterministic [%d %d]", due[0].ID, due[1].ID, sourceA, sourceB)
	}
	if due[0].FailCount != 4 || due[1].FailCount != 8 {
		t.Fatalf("due live fail counts = [%d %d], want [4 8]", due[0].FailCount, due[1].FailCount)
	}
	for _, src := range due {
		if src.ID == outsideSource {
			t.Fatalf("source outside frozen set leaked into due rows: %d", outsideSource)
		}
	}

	base := time.Now().UTC().Add(-time.Hour)
	aOld := newContent(sourceA, "a-old", base.Add(time.Minute))
	aNew := newContent(sourceA, "a-new", base.Add(4*time.Minute))
	_ = newContent(sourceB, "b-old", base)
	bNew := newContent(sourceB, "b-new", base.Add(3*time.Minute))
	outsideItem := newContent(outsideSource, "outside", base.Add(5*time.Minute))

	candidates, err := st.ListUnpushedForTaskRunV1(
		ctx, identity, ref, []int64{sourceB, sourceA}, 10, 1)
	if err != nil {
		t.Fatalf("ListUnpushedForTaskRunV1() fairness: %v", err)
	}
	wantCandidateIDs := []int64{aNew, bNew}
	assertContentIDs(t, candidates, wantCandidateIDs)
	for _, item := range candidates {
		if item.ID == outsideItem {
			t.Fatalf("content outside frozen set leaked into candidates: %d", outsideItem)
		}
	}

	batchID, err := st.CreatePushBatchIdempotent(ctx, owner.ID, "compiled-delivery-"+uuid.NewString(), "")
	if err != nil {
		t.Fatalf("create delivery batch: %v", err)
	}
	if _, _, _, err := st.InsertDeliveryIdempotent(ctx, &types.Delivery{
		BatchID:       batchID,
		UserID:        owner.ID,
		ContentItemID: &aNew,
		Score:         80,
		BodyMD:        "delivered",
	}); err != nil {
		t.Fatalf("insert delivered candidate: %v", err)
	}

	ownerCandidates, err := st.ListUnpushedForTaskRunV1(
		ctx, identity, ref, []int64{sourceA, sourceB}, 10, 1)
	if err != nil {
		t.Fatalf("ListUnpushedForTaskRunV1() delivered exclusion: %v", err)
	}
	assertContentIDs(t, ownerCandidates, []int64{bNew, aOld})

	limited, err := st.ListUnpushedForTaskRunV1(
		ctx, identity, ref, []int64{sourceA, sourceB}, 1, 10)
	if err != nil {
		t.Fatalf("ListUnpushedForTaskRunV1() global limit: %v", err)
	}
	assertContentIDs(t, limited, []int64{bNew})

	displayKey := "https://example.com/compiled-display-" + uuid.NewString()
	displayItem, created, err := st.UpsertContentItem(ctx, &types.ContentItem{
		SourceID:     outsideSource,
		ExternalID:   "outside-" + uuid.NewString(),
		CanonicalKey: displayKey,
		URL:          displayKey,
		Title:        "display attribution",
		ContentHash:  "hash-" + uuid.NewString(),
	})
	if err != nil || !created {
		t.Fatalf("create display item: id=%d created=%v err=%v", displayItem, created, err)
	}
	contentIDs = append(contentIDs, displayItem)
	for _, sourceID := range []int64{displaySource, sourceA} {
		secondID, secondCreated, err := st.UpsertContentItem(ctx, &types.ContentItem{
			SourceID:     sourceID,
			ExternalID:   "appearance-" + uuid.NewString(),
			CanonicalKey: displayKey,
			URL:          displayKey,
			Title:        "display attribution",
			ContentHash:  "hash-" + uuid.NewString(),
		})
		if err != nil {
			t.Fatalf("add display appearance for source %d: %v", sourceID, err)
		}
		if secondCreated || secondID != displayItem {
			t.Fatalf("display appearance created=%v id=%d, want existing %d", secondCreated, secondID, displayItem)
		}
	}

	displayID, ok, err := st.FetchTargetForContentFromIDs(ctx, displayItem, []int64{displaySource})
	if err != nil {
		t.Fatalf("FetchTargetForContentFromIDs(): %v", err)
	}
	if !ok || displayID != displaySource {
		t.Fatalf("display source = (%d,%v), want frozen source (%d,true)", displayID, ok, displaySource)
	}

	deterministicID, ok, err := st.FetchTargetForContentFromIDs(ctx, displayItem, []int64{displaySource, sourceA})
	if err != nil {
		t.Fatalf("FetchTargetForContentFromIDs() deterministic: %v", err)
	}
	wantLowest := min(displaySource, sourceA)
	if !ok || deterministicID != wantLowest {
		t.Fatalf("display source = (%d,%v), want lowest matching source (%d,true)", deterministicID, ok, wantLowest)
	}

	missingID, ok, err := st.FetchTargetForContentFromIDs(ctx, displayItem, []int64{sourceB})
	if err != nil {
		t.Fatalf("FetchTargetForContentFromIDs() empty intersection: %v", err)
	}
	if ok || missingID != 0 {
		t.Fatalf("empty intersection = (%d,%v), want (0,false)", missingID, ok)
	}
}

func assertContentIDs(t *testing.T, items []types.ContentItem, want []int64) {
	t.Helper()
	if len(items) != len(want) {
		t.Fatalf("candidate count = %d, want %d; items=%+v", len(items), len(want), items)
	}
	for i, item := range items {
		if item.ID != want[i] {
			t.Fatalf("candidate[%d].ID = %d, want %d", i, item.ID, want[i])
		}
	}
}
