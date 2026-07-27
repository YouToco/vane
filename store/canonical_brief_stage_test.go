package store

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

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
