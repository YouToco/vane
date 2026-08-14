package store

import (
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/YouToco/vane/types"
)

func TestStructuredEventEvidenceRejectsInvalidContentSet(t *testing.T) {
	for name, ids := range map[string][]int64{
		"empty":     nil,
		"zero":      {0},
		"duplicate": {1, 1},
		"over limit": {
			1, 2, 3, 4, 5, 6, 7, 8, 9,
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := new(Store).
				LoadStructuredEventEvidenceForTaskRunV1(
					t.Context(), types.RunIdentity{},
					types.RunSnapshotRef{}, ids)
			if !errors.Is(err, types.ErrValidation) {
				t.Fatalf("error=%v want validation", err)
			}
		})
	}
}

func TestStructuredEventEvidenceLoadsOnlyExactSnapshotInventory(t *testing.T) {
	f := newCanonicalBriefFixture(t, 2)
	got, err := f.base.st.LoadStructuredEventEvidenceForTaskRunV1(
		t.Context(), f.identity, f.ref,
		[]int64{f.contentID[1], f.contentID[0]})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 ||
		got[0].Item.ID != f.contentID[1] ||
		got[1].Item.ID != f.contentID[0] ||
		got[0].Source.SourceID != f.sourceID ||
		got[0].Source.Title != f.sourceName {
		t.Fatalf("ordered evidence inventory=%+v", got)
	}

	foreignTaskID := "task-evidence-foreign-" + uuid.NewString()
	foreignSourceIDs := f.base.createApprovedTask(t, foreignTaskID, 1)
	foreignURL := "https://foreign-evidence.test/" + uuid.NewString()
	foreignContentID, created, err := f.base.st.UpsertContentItem(
		t.Context(), &types.ContentItem{
			SourceID:   foreignSourceIDs[0],
			ExternalID: uuid.NewString(), CanonicalKey: foreignURL,
			URL: foreignURL, Title: "foreign evidence",
			Content:     "must stay outside the exact snapshot",
			ContentHash: "hash-" + uuid.NewString(),
		})
	if err != nil || !created {
		t.Fatalf("create foreign content: id=%d created=%t err=%v",
			foreignContentID, created, err)
	}
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(ctx, t, f.base.st,
			`DELETE FROM content_sources WHERE content_item_id=$1`,
			foreignContentID)
		cleanupExec(ctx, t, f.base.st,
			`DELETE FROM content_items WHERE id=$1`, foreignContentID)
	})
	if _, err := f.base.st.LoadStructuredEventEvidenceForTaskRunV1(
		t.Context(), f.identity, f.ref,
		[]int64{foreignContentID},
	); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("out-of-snapshot evidence error=%v want conflict", err)
	}
	if _, err := f.base.st.LoadStructuredEventEvidenceForTaskRunV1(
		t.Context(), f.identity, f.ref,
		[]int64{foreignContentID + 1_000_000},
	); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("missing evidence error=%v want conflict", err)
	}

	wrongUser := f.identity
	wrongUser.UserID++
	if _, err := f.base.st.LoadStructuredEventEvidenceForTaskRunV1(
		t.Context(), wrongUser, f.ref, []int64{f.contentID[0]},
	); !errors.Is(err, types.ErrValidation) &&
		!errors.Is(err, types.ErrNotFound) {
		t.Fatalf("cross-user evidence error=%v", err)
	}
	wrongTenant := f.identity
	wrongTenant.TenantID++
	if _, err := f.base.st.LoadStructuredEventEvidenceForTaskRunV1(
		t.Context(), wrongTenant, f.ref, []int64{f.contentID[0]},
	); !errors.Is(err, types.ErrValidation) &&
		!errors.Is(err, types.ErrNotFound) {
		t.Fatalf("cross-tenant evidence error=%v", err)
	}
	wrongRun := f.identity
	wrongRun.TemporalRunID = uuid.NewString()
	if _, err := f.base.st.LoadStructuredEventEvidenceForTaskRunV1(
		t.Context(), wrongRun, f.ref, []int64{f.contentID[0]},
	); !errors.Is(err, types.ErrValidation) &&
		!errors.Is(err, types.ErrNotFound) {
		t.Fatalf("cross-run evidence error=%v", err)
	}
}

func TestStructuredEventEvidenceUsesLowestFrozenAttributionAndIgnoresLiveDrift(
	t *testing.T,
) {
	f := newTaskRunSnapshotFixture(t)
	taskID := f.taskID()
	sourceIDs := f.createApprovedTask(t, taskID, 2)
	identity := types.RunIdentity{
		TemporalWorkflowID: "wf-" + taskID,
		TemporalRunID:      uuid.NewString(),
		RunKind:            types.RunSnapshotKindScheduled,
		TenantID:           f.tenantID,
		UserID:             f.userID,
		TaskID:             taskID,
	}
	ref, err := f.st.CreateOrGetCompiledTaskRunSnapshotV1(
		t.Context(), CreateOrGetCompiledTaskRunSnapshotV1Params{
			Identity: identity, Policy: testCompiledRunPolicyV1(t),
		})
	if err != nil {
		t.Fatal(err)
	}
	contentURL := "https://evidence-attribution.test/" + uuid.NewString()
	contentID, created, err := f.st.UpsertContentItem(
		t.Context(), &types.ContentItem{
			SourceID: sourceIDs[1], ExternalID: uuid.NewString(),
			CanonicalKey: contentURL, URL: contentURL,
			Title: "two-source evidence", Content: "bounded evidence body",
			ContentHash: "hash-" + uuid.NewString(),
		})
	if err != nil || !created {
		t.Fatalf("create attributed content: id=%d created=%t err=%v",
			contentID, created, err)
	}
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(ctx, t, f.st,
			`DELETE FROM content_sources WHERE content_item_id=$1`, contentID)
		cleanupExec(ctx, t, f.st,
			`DELETE FROM content_items WHERE id=$1`, contentID)
	})
	if _, err := f.st.pool.Exec(t.Context(),
		`INSERT INTO content_sources (
		     content_item_id,source_id,external_id,url
		 ) VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING`,
		contentID, sourceIDs[0], "secondary-"+uuid.NewString(), contentURL,
	); err != nil {
		t.Fatal(err)
	}

	// Live mutable source presentation must not replace the snapshot's frozen
	// title, even though the deterministic lowest source ID remains selected.
	if _, err := f.st.pool.Exec(t.Context(),
		`UPDATE fetch_targets SET title='live drift' WHERE id=$1`, sourceIDs[0],
	); err != nil {
		t.Fatal(err)
	}
	got, err := f.st.LoadStructuredEventEvidenceForTaskRunV1(
		t.Context(), identity, ref, []int64{contentID})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 ||
		got[0].Source.SourceID != sourceIDs[0] ||
		got[0].Source.Title != "approved 0" {
		t.Fatalf("deterministic frozen attribution=%+v", got)
	}
}
