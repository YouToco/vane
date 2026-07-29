package store

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/types"
)

func TestProfileEpochResetRestoreRawReplayAndAuthority(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL，跳过画像 epoch 真 PostgreSQL 测试")
	}
	if err := Migrate(t.Context(), dbURL); err != nil {
		t.Fatal(err)
	}
	st, err := New(t.Context(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	registerStoreClose(t, st)
	u, err := st.UpsertUserByOpenID(
		t.Context(), "profile_epoch_"+uuid.NewString(), "epoch")
	if err != nil {
		t.Fatal(err)
	}
	attachTenant(t, st, u.ID)
	db, err := sql.Open("pgx", dbURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	feedbackSuffix := "phase-b-" + uuid.NewString()
	feedbackURL := "https://migration-066.example/" + feedbackSuffix
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(ctx, t, st, `DELETE FROM feedbacks WHERE user_id=$1`, u.ID)
		cleanupExec(ctx, t, st, `DELETE FROM deliveries WHERE user_id=$1`, u.ID)
		cleanupExec(ctx, t, st, `DELETE FROM push_batches WHERE user_id=$1`, u.ID)
		cleanupExec(ctx, t, st, `
			DELETE FROM content_items
			 WHERE source_id IN (SELECT id FROM fetch_targets WHERE url=$1)`,
			feedbackURL)
		cleanupExec(ctx, t, st, `DELETE FROM fetch_targets WHERE url=$1`, feedbackURL)
		for _, table := range []string{
			"profile_epoch_receipts", "profile_epoch_events",
			"profile_epoch_checkpoints", "profile_claim_receipts",
			"profile_claim_events", "profile_claims", "profile_claim_states",
			"profile_epochs", "profile_edit_receipts", "profile_edit_revisions",
			"profiles", "profile_feedback_epoch_fences",
		} {
			cleanupExec(ctx, t, st, "DELETE FROM "+table+" WHERE user_id=$1", u.ID)
		}
		cleanupExec(ctx, t, st, `DELETE FROM memberships WHERE user_id=$1`, u.ID)
		cleanupExec(ctx, t, st, `DELETE FROM users WHERE id=$1`, u.ID)
	})

	industry := "AI"
	created, err := st.PatchProfile(
		t.Context(), 1, u.ID, nil,
		types.ProfileEditPatch{
			Industry: &industry,
			Tags:     ptrStrings([]string{"manual"}),
		},
		"epoch-create-"+uuid.NewString(), strings.Repeat("1", 64))
	if err != nil {
		t.Fatal(err)
	}
	p, err := st.GetProfile(t.Context(), u.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.EvolveProfile(
		t.Context(), u.ID, "evidence summary", []string{"evidence"},
		10, created.UpdatedAt, 0, p.ProfileEpoch, p.ProfileVersion,
	); err != nil {
		t.Fatal(err)
	}
	before, err := st.GetProfile(t.Context(), u.ID)
	if err != nil {
		t.Fatal(err)
	}
	claims, err := st.ListProfileClaims(t.Context(), 1, u.ID)
	if err != nil {
		t.Fatal(err)
	}
	resetKey := "reset-" + uuid.NewString()
	resetDigest := strings.Repeat("2", 64)
	reset, err := st.ApplyProfileEpochAction(
		t.Context(), 1, u.ID,
		types.ProfileEpochAction{
			ExpectedEpoch: claims.ProfileEpoch, ExpectedVersion: claims.Version,
			Action: "reset", Scope: "history_learning",
		},
		resetKey, resetDigest)
	if err != nil {
		t.Fatal(err)
	}
	if reset.ProfileEpoch != 1 || reset.Version != claims.Version+1 ||
		!reset.RestoreAllowed {
		t.Fatalf("reset result=%+v", reset)
	}
	if reset.Profile.Summary != "" || containsString(reset.Profile.Tags, "evidence") {
		t.Fatalf("reset carried ordinary evidence: %+v", reset.Profile)
	}
	afterReset, err := st.ListProfileClaims(t.Context(), 1, u.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !afterReset.RestoreAllowed {
		t.Fatal("GET claims must expose pristine restore_allowed")
	}

	// Replay is lifetime-scoped and precedes stale CAS. The original authority
	// is stale after reset, but the exact retry still returns the first response.
	replayed, err := st.ApplyProfileEpochAction(
		t.Context(), 1, u.ID,
		types.ProfileEpochAction{
			ExpectedEpoch: claims.ProfileEpoch, ExpectedVersion: claims.Version,
			Action: "reset", Scope: "history_learning",
		},
		resetKey, resetDigest)
	if err != nil {
		t.Fatal(err)
	}
	firstJSON, err := json.Marshal(reset)
	if err != nil {
		t.Fatal(err)
	}
	replayJSON, err := json.Marshal(replayed)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(firstJSON, replayJSON) {
		t.Fatalf("receipt replay changed response:\nfirst=%s\nreplay=%s",
			firstJSON, replayJSON)
	}
	if _, err := st.ApplyProfileEpochAction(
		t.Context(), 1, u.ID,
		types.ProfileEpochAction{
			ExpectedEpoch: claims.ProfileEpoch, ExpectedVersion: claims.Version,
			Action: "reset", Scope: "history_learning",
		},
		resetKey, strings.Repeat("9", 64),
	); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("same key with another digest must conflict, got %v", err)
	}

	// Pre-reset Evolver and claim tokens cannot write into the new epoch.
	if err := st.EvolveProfile(
		t.Context(), u.ID, "stale output", []string{"stale"},
		before.LastEvolvedFeedbackID, before.UpdatedAt,
		before.LastEvolvedFeedbackID, claims.ProfileEpoch, claims.Version,
	); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("stale Evolver authority must conflict, got %v", err)
	}
	var staleClaimID int64
	for _, claim := range claims.Claims {
		if claim.Active {
			staleClaimID, _ = strconv.ParseInt(claim.ID, 10, 64)
			break
		}
	}
	if staleClaimID == 0 {
		t.Fatal("fixture needs an active predecessor claim")
	}
	if _, err := st.ApplyProfileClaimAction(
		t.Context(), 1, u.ID,
		types.ProfileClaimAction{
			ExpectedEpoch: claims.ProfileEpoch, ExpectedVersion: claims.Version,
			Action: "pin", ClaimID: staleClaimID,
		},
		"stale-claim-"+uuid.NewString(), strings.Repeat("8", 64),
	); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("stale claim authority must conflict, got %v", err)
	}

	// Damage the acceleration cache. Restore must ignore it and replay the
	// immutable predecessor ledger plus transition identity.
	tx, err := st.beginTx(t.Context(), pgxTxOptionsForTest())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(t.Context()) }()
	if _, err := tx.Exec(t.Context(),
		`SELECT set_config('app.tenant_id','1',true),
		        set_config('app.user_id',$1,true)`,
		strconv.FormatInt(u.ID, 10)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(t.Context(),
		`UPDATE profile_epoch_checkpoints
		    SET canonical_payload='corrupt'::bytea
		  WHERE tenant_id=1 AND user_id=$1`, u.ID); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}

	restored, err := st.ApplyProfileEpochAction(
		t.Context(), 1, u.ID,
		types.ProfileEpochAction{
			ExpectedEpoch:   afterReset.ProfileEpoch,
			ExpectedVersion: afterReset.Version, Action: "restore",
		},
		"restore-"+uuid.NewString(), strings.Repeat("3", 64))
	if err != nil {
		t.Fatal(err)
	}
	if restored.ProfileEpoch != 2 || restored.RestoreAllowed {
		t.Fatalf("restore result=%+v", restored)
	}
	if restored.Profile.Industry != before.Industry ||
		restored.Profile.Summary != before.Summary ||
		!containsString(restored.Profile.Tags, "evidence") {
		t.Fatalf("raw replay differs: before=%+v restored=%+v", before, restored.Profile)
	}
	restoredClaims, err := st.ListProfileClaims(t.Context(), 1, u.ID)
	if err != nil {
		t.Fatal(err)
	}
	foundEvidence := false
	for _, claim := range restoredClaims.Claims {
		if claim.Value == "evidence" && claim.Source.State == "evidence" {
			foundEvidence = true
		}
	}
	if !foundEvidence {
		t.Fatalf("restore upgraded or lost evidence authority: %+v", restoredClaims.Claims)
	}

	secondReset, err := st.ApplyProfileEpochAction(
		t.Context(), 1, u.ID,
		types.ProfileEpochAction{
			ExpectedEpoch:   restoredClaims.ProfileEpoch,
			ExpectedVersion: restoredClaims.Version,
			Action:          "reset", Scope: "history_learning",
		},
		"reset-2-"+uuid.NewString(), strings.Repeat("4", 64))
	if err != nil {
		t.Fatal(err)
	}
	if containsString(secondReset.Profile.Tags, "evidence") ||
		secondReset.Profile.Summary != "" {
		t.Fatalf("restore upgraded evidence; next reset carried it: %+v",
			secondReset.Profile)
	}

	// A committed feedback fact in the reset-created epoch closes restore even
	// before Evolver consumes it.
	deliveryID := seedMigration066DeliveryForUser(
		t, db, u.ID, feedbackSuffix)
	if _, err := st.InsertFeedback(t.Context(), &types.Feedback{
		UserID: u.ID, DeliveryID: deliveryID,
		Action: types.FeedbackActionInterested,
	}); err != nil {
		t.Fatal(err)
	}
	dirty, err := st.ListProfileClaims(t.Context(), 1, u.ID)
	if err != nil {
		t.Fatal(err)
	}
	if dirty.RestoreAllowed {
		t.Fatal("current-epoch feedback must close restore_allowed")
	}
	if _, err := st.ApplyProfileEpochAction(
		t.Context(), 1, u.ID,
		types.ProfileEpochAction{
			ExpectedEpoch: dirty.ProfileEpoch, ExpectedVersion: dirty.Version,
			Action: "restore",
		},
		"dirty-restore-"+uuid.NewString(), strings.Repeat("7", 64),
	); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("dirty reset epoch restore must conflict, got %v", err)
	}
}

func TestProfileEpochFeedbackIsolationAndPerEpochIdempotency(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL，跳过画像 epoch 反馈隔离测试")
	}
	if err := Migrate(t.Context(), dbURL); err != nil {
		t.Fatal(err)
	}
	st, err := New(t.Context(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	registerStoreClose(t, st)
	u, err := st.UpsertUserByOpenID(
		t.Context(), "profile_epoch_feedback_"+uuid.NewString(), "epoch-feedback")
	if err != nil {
		t.Fatal(err)
	}
	attachTenant(t, st, u.ID)
	db, err := sql.Open("pgx", dbURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	suffix := "phase-b-feedback-" + uuid.NewString()
	sourceURL := "https://migration-066.example/" + suffix
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		for _, table := range []string{
			"profile_epoch_receipts", "profile_epoch_events",
			"profile_epoch_checkpoints", "profile_claim_receipts",
			"profile_claim_events", "profile_claims", "profile_claim_states",
			"profile_epochs", "profile_edit_receipts", "profile_edit_revisions",
			"profiles", "profile_feedback_epoch_fences",
		} {
			cleanupExec(ctx, t, st, "DELETE FROM "+table+" WHERE user_id=$1", u.ID)
		}
		cleanupExec(ctx, t, st, `DELETE FROM feedbacks WHERE user_id=$1`, u.ID)
		cleanupExec(ctx, t, st, `DELETE FROM deliveries WHERE user_id=$1`, u.ID)
		cleanupExec(ctx, t, st, `DELETE FROM push_batches WHERE user_id=$1`, u.ID)
		cleanupExec(ctx, t, st, `
			DELETE FROM content_items
			 WHERE source_id IN (SELECT id FROM fetch_targets WHERE url=$1)`, sourceURL)
		cleanupExec(ctx, t, st, `DELETE FROM fetch_targets WHERE url=$1`, sourceURL)
		cleanupExec(ctx, t, st, `DELETE FROM memberships WHERE user_id=$1`, u.ID)
		cleanupExec(ctx, t, st, `DELETE FROM users WHERE id=$1`, u.ID)
	})

	occupation := "epoch feedback tester"
	if _, err := st.PatchProfile(
		t.Context(), 1, u.ID, nil,
		types.ProfileEditPatch{Occupation: &occupation},
		"epoch-feedback-profile-"+uuid.NewString(), strings.Repeat("a", 64),
	); err != nil {
		t.Fatal(err)
	}
	deliveryID := seedMigration066DeliveryForUser(t, db, u.ID, suffix)
	deep0, _, existed, err := st.InsertDeepDiveFeedback(
		t.Context(), &types.Feedback{
			UserID: u.ID, DeliveryID: deliveryID,
			Action: types.FeedbackActionDeepDive, Detail: "epoch zero deep dive",
		},
	)
	if err != nil || existed {
		t.Fatalf("epoch-0 deep dive id=%d existed=%v err=%v", deep0, existed, err)
	}
	misjudged0, err := st.InsertFeedback(t.Context(), &types.Feedback{
		UserID: u.ID, DeliveryID: deliveryID,
		Action:     types.FeedbackActionMisjudged,
		ReasonCode: types.FeedbackReasonOutdated, Detail: "epoch zero problem",
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, action := range []types.FeedbackAction{
		types.FeedbackActionDeepDive, types.FeedbackActionMisjudged,
	} {
		has, err := st.HasFeedback(t.Context(), deliveryID, action)
		if err != nil || !has {
			t.Fatalf("epoch-0 HasFeedback(%s)=%v err=%v", action, has, err)
		}
	}

	claims, err := st.ListProfileClaims(t.Context(), 1, u.ID)
	if err != nil {
		t.Fatal(err)
	}
	reset, err := st.ApplyProfileEpochAction(
		t.Context(), 1, u.ID,
		types.ProfileEpochAction{
			ExpectedEpoch: claims.ProfileEpoch, ExpectedVersion: claims.Version,
			Action: "reset", Scope: "history_learning",
		},
		"epoch-feedback-reset-"+uuid.NewString(), strings.Repeat("b", 64),
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, action := range []types.FeedbackAction{
		types.FeedbackActionDeepDive, types.FeedbackActionMisjudged,
	} {
		has, err := st.HasFeedback(t.Context(), deliveryID, action)
		if err != nil || has {
			t.Fatalf("old-epoch HasFeedback(%s) leaked=%v err=%v",
				action, has, err)
		}
		if _, err := st.GetFeedbackDetail(
			t.Context(), deliveryID, action,
		); !errors.Is(err, types.ErrNotFound) {
			t.Fatalf("old-epoch detail(%s) leaked: %v", action, err)
		}
	}
	if _, err := st.LatestFeedbackAction(
		t.Context(), deliveryID,
		[]types.FeedbackAction{
			types.FeedbackActionInterested, types.FeedbackActionNotInterested,
		},
	); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("old-epoch latest action leaked: %v", err)
	}
	rows, err := st.ListFeedbacksForEvolutionForTenant(
		t.Context(), 1, u.ID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("old epoch entered evolution input: %+v", rows)
	}

	deep1, _, existed, err := st.InsertDeepDiveFeedback(
		t.Context(), &types.Feedback{
			UserID: u.ID, DeliveryID: deliveryID,
			Action: types.FeedbackActionDeepDive, Detail: "epoch one deep dive",
		},
	)
	if err != nil || existed || deep1 == deep0 {
		t.Fatalf("epoch-1 deep dive id=%d existed=%v old=%d err=%v",
			deep1, existed, deep0, err)
	}
	replayedDeep, frozenDeep, existed, err := st.InsertDeepDiveFeedback(
		t.Context(), &types.Feedback{
			UserID: u.ID, DeliveryID: deliveryID,
			Action: types.FeedbackActionDeepDive, Detail: "must not replace",
		},
	)
	if err != nil || !existed || replayedDeep != deep1 ||
		frozenDeep != "epoch one deep dive" {
		t.Fatalf("epoch-1 deep replay id=%d existed=%v detail=%q err=%v",
			replayedDeep, existed, frozenDeep, err)
	}
	misjudged1, err := st.InsertFeedback(t.Context(), &types.Feedback{
		UserID: u.ID, DeliveryID: deliveryID,
		Action:     types.FeedbackActionMisjudged,
		ReasonCode: types.FeedbackReasonNotRelevant, Detail: "epoch one problem",
	})
	if err != nil || misjudged1 == misjudged0 {
		t.Fatalf("epoch-1 misjudged id=%d old=%d err=%v",
			misjudged1, misjudged0, err)
	}
	replayedMisjudged, err := st.InsertFeedback(t.Context(), &types.Feedback{
		UserID: u.ID, DeliveryID: deliveryID,
		Action:     types.FeedbackActionMisjudged,
		ReasonCode: types.FeedbackReasonOther, Detail: "must not replace",
	})
	if err != nil || replayedMisjudged != misjudged1 {
		t.Fatalf("epoch-1 misjudged replay id=%d want=%d err=%v",
			replayedMisjudged, misjudged1, err)
	}
	rows, err = st.ListFeedbacksForEvolutionForTenant(
		t.Context(), 1, u.ID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("active epoch evolution rows=%d want=2: %+v", len(rows), rows)
	}

	// A deep-dive idempotency winner lookup must remain in the same admission
	// and feedback-fence transaction as the conflicting insert. Otherwise a
	// reset between those two statements can redirect the lookup to a new
	// epoch and turn an already-paid result into a 500/re-generation.
	deepTx, err := st.beginTx(
		t.Context(), pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		t.Fatal(err)
	}
	defer deepTx.Rollback(t.Context())
	if _, err := deepTx.Exec(t.Context(),
		`SELECT pg_advisory_xact_lock($1,$2)`,
		agentSessionFactAdmissionClass, agentSessionFactAdmissionKey,
	); err != nil {
		t.Fatal(err)
	}
	if err := setFeedbackRuntimeContext(
		t.Context(), deepTx, 1, u.ID,
	); err != nil {
		t.Fatal(err)
	}
	conflictReached := make(chan struct{})
	releaseLookup := make(chan struct{})
	type deepResult struct {
		id      int64
		detail  string
		existed bool
		err     error
	}
	deepDone := make(chan deepResult, 1)
	go func() {
		id, detail, existed, runErr := insertDeepDiveFeedbackFact(
			t.Context(), deepTx, 1,
			&types.Feedback{
				UserID: u.ID, DeliveryID: deliveryID,
				Action: types.FeedbackActionDeepDive, Detail: "must not replace",
			},
			func() {
				close(conflictReached)
				<-releaseLookup
			},
		)
		if runErr == nil {
			runErr = deepTx.Commit(t.Context())
		}
		deepDone <- deepResult{id: id, detail: detail, existed: existed, err: runErr}
	}()
	select {
	case <-conflictReached:
	case <-time.After(3 * time.Second):
		close(releaseLookup)
		t.Fatal("deep-dive retry did not reach its fenced winner lookup")
	}
	currentClaims, err := st.ListProfileClaims(t.Context(), 1, u.ID)
	if err != nil {
		close(releaseLookup)
		t.Fatal(err)
	}
	resetDone := make(chan error, 1)
	go func() {
		_, resetErr := st.ApplyProfileEpochAction(
			t.Context(), 1, u.ID,
			types.ProfileEpochAction{
				ExpectedEpoch:   currentClaims.ProfileEpoch,
				ExpectedVersion: currentClaims.Version,
				Action:          "reset", Scope: "history_learning",
			},
			"epoch-feedback-reset-race-"+uuid.NewString(),
			strings.Repeat("c", 64),
		)
		resetDone <- resetErr
	}()
	select {
	case resetErr := <-resetDone:
		close(releaseLookup)
		t.Fatalf("reset crossed deep-dive winner lookup: %v", resetErr)
	case <-time.After(200 * time.Millisecond):
	}
	close(releaseLookup)
	select {
	case result := <-deepDone:
		if result.err != nil || !result.existed || result.id != deep1 ||
			result.detail != "epoch one deep dive" {
			t.Fatalf("fenced deep-dive retry=%+v want winner %d", result, deep1)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("deep-dive retry did not finish after releasing lookup")
	}
	select {
	case resetErr := <-resetDone:
		if resetErr != nil {
			t.Fatalf("reset after deep-dive winner lookup failed: %v", resetErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("reset did not resume after deep-dive winner lookup committed")
	}

	var epoch0Rows, epoch1Rows int
	if err := db.QueryRowContext(t.Context(), `
		SELECT count(*) FILTER (WHERE profile_epoch=0),
		       count(*) FILTER (WHERE profile_epoch=$2)
		  FROM feedbacks
		 WHERE id=ANY($1)`,
		[]int64{deep0, misjudged0, deep1, misjudged1}, reset.ProfileEpoch,
	).Scan(&epoch0Rows, &epoch1Rows); err != nil {
		t.Fatal(err)
	}
	if epoch0Rows != 2 || epoch1Rows != 2 {
		t.Fatalf("feedback epoch split=%d/%d want=2/2", epoch0Rows, epoch1Rows)
	}
}

func TestProfileEpochRestoreRejectsMissingCancelledLedgerEvents(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL，跳过画像 epoch 账本完整性测试")
	}
	if err := Migrate(t.Context(), dbURL); err != nil {
		t.Fatal(err)
	}
	st, err := New(t.Context(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	registerStoreClose(t, st)
	u, err := st.UpsertUserByOpenID(
		t.Context(), "profile_epoch_ledger_"+uuid.NewString(), "epoch-ledger")
	if err != nil {
		t.Fatal(err)
	}
	attachTenant(t, st, u.ID)
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		for _, table := range []string{
			"profile_epoch_receipts", "profile_epoch_events",
			"profile_epoch_checkpoints", "profile_claim_receipts",
			"profile_claim_events", "profile_claims", "profile_claim_states",
			"profile_epochs", "profile_edit_receipts", "profile_edit_revisions",
			"profiles", "profile_feedback_epoch_fences",
		} {
			cleanupExec(ctx, t, st, "DELETE FROM "+table+" WHERE user_id=$1", u.ID)
		}
		cleanupExec(ctx, t, st, `DELETE FROM memberships WHERE user_id=$1`, u.ID)
		cleanupExec(ctx, t, st, `DELETE FROM users WHERE id=$1`, u.ID)
	})
	tag := []string{"manual-ledger"}
	if _, err := st.PatchProfile(
		t.Context(), 1, u.ID, nil,
		types.ProfileEditPatch{Tags: &tag},
		"ledger-profile-"+uuid.NewString(), strings.Repeat("d", 64),
	); err != nil {
		t.Fatal(err)
	}
	claims, err := st.ListProfileClaims(t.Context(), 1, u.ID)
	if err != nil {
		t.Fatal(err)
	}
	var claimID int64
	for _, claim := range claims.Claims {
		if claim.Active && claim.Field == "tag" {
			claimID = parseTestID(t, claim.ID)
			break
		}
	}
	if claimID == 0 {
		t.Fatal("missing tag claim for ledger test")
	}
	pin, err := st.ApplyProfileClaimAction(
		t.Context(), 1, u.ID,
		types.ProfileClaimAction{
			ExpectedEpoch: claims.ProfileEpoch, ExpectedVersion: claims.Version,
			Action: "pin", ClaimID: claimID,
		},
		"ledger-pin-"+uuid.NewString(), strings.Repeat("e", 64),
	)
	if err != nil {
		t.Fatal(err)
	}
	pinEventID := parseTestID(t, pin.EventID)
	revoke, err := st.ApplyProfileClaimAction(
		t.Context(), 1, u.ID,
		types.ProfileClaimAction{
			ExpectedEpoch: claims.ProfileEpoch, ExpectedVersion: pin.Version,
			Action: "revoke", EventID: pinEventID,
		},
		"ledger-revoke-"+uuid.NewString(), strings.Repeat("f", 64),
	)
	if err != nil {
		t.Fatal(err)
	}
	revokeEventID := parseTestID(t, revoke.EventID)
	if _, err := st.ApplyProfileClaimAction(
		t.Context(), 1, u.ID,
		types.ProfileClaimAction{
			ExpectedEpoch: claims.ProfileEpoch, ExpectedVersion: revoke.Version,
			Action: "pin", ClaimID: claimID,
		},
		"ledger-later-pin-"+uuid.NewString(), strings.Repeat("0", 64),
	); err != nil {
		t.Fatal(err)
	}
	afterRevoke, err := st.ListProfileClaims(t.Context(), 1, u.ID)
	if err != nil {
		t.Fatal(err)
	}
	reset, err := st.ApplyProfileEpochAction(
		t.Context(), 1, u.ID,
		types.ProfileEpochAction{
			ExpectedEpoch:   afterRevoke.ProfileEpoch,
			ExpectedVersion: afterRevoke.Version,
			Action:          "reset", Scope: "history_learning",
		},
		"ledger-reset-"+uuid.NewString(), strings.Repeat("1", 64),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(t.Context(), `
		DELETE FROM profile_claim_receipts
		 WHERE tenant_id=1 AND user_id=$1 AND event_id=ANY($2)`,
		u.ID, []int64{pinEventID, revokeEventID},
	); err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(t.Context(), `
		DELETE FROM profile_claim_events
		 WHERE tenant_id=1 AND user_id=$1 AND id=$2`,
		u.ID, revokeEventID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(t.Context(), `
		DELETE FROM profile_claim_events
		 WHERE tenant_id=1 AND user_id=$1 AND id=$2`,
		u.ID, pinEventID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := st.ApplyProfileEpochAction(
		t.Context(), 1, u.ID,
		types.ProfileEpochAction{
			ExpectedEpoch: reset.ProfileEpoch, ExpectedVersion: reset.Version,
			Action: "restore",
		},
		"ledger-restore-"+uuid.NewString(), strings.Repeat("2", 64),
	); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("restore accepted incomplete raw ledger: %v", err)
	}
}

func TestProfileEpochResetRejectsOversizedCarriedClaim(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL，跳过画像 epoch 单条上限测试")
	}
	if err := Migrate(t.Context(), dbURL); err != nil {
		t.Fatal(err)
	}
	st, err := New(t.Context(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	registerStoreClose(t, st)
	u, err := st.UpsertUserByOpenID(
		t.Context(), "profile_epoch_limit_"+uuid.NewString(), "epoch-limit")
	if err != nil {
		t.Fatal(err)
	}
	attachTenant(t, st, u.ID)
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		for _, table := range []string{
			"profile_epoch_receipts", "profile_epoch_events",
			"profile_epoch_checkpoints", "profile_claim_receipts",
			"profile_claim_events", "profile_claims", "profile_claim_states",
			"profile_epochs", "profile_edit_receipts", "profile_edit_revisions",
			"profiles", "profile_feedback_epoch_fences",
		} {
			cleanupExec(ctx, t, st, "DELETE FROM "+table+" WHERE user_id=$1", u.ID)
		}
		cleanupExec(ctx, t, st, `DELETE FROM memberships WHERE user_id=$1`, u.ID)
		cleanupExec(ctx, t, st, `DELETE FROM users WHERE id=$1`, u.ID)
	})
	tags := []string{"short"}
	if _, err := st.PatchProfile(
		t.Context(), 1, u.ID, nil,
		types.ProfileEditPatch{Tags: &tags},
		"limit-profile-"+uuid.NewString(), strings.Repeat("3", 64),
	); err != nil {
		t.Fatal(err)
	}
	claims, err := st.ListProfileClaims(t.Context(), 1, u.ID)
	if err != nil {
		t.Fatal(err)
	}
	oversized := strings.Repeat("界", 21)
	tx, err := st.beginTx(t.Context(), pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(t.Context())
	if _, err := tx.Exec(t.Context(), `
		SELECT set_config('app.tenant_id','1',true),
		       set_config('app.user_id',$1,true),
		       set_config('app.profile_epoch',$2,true)`,
		strconv.FormatInt(u.ID, 10),
		strconv.FormatInt(claims.ProfileEpoch, 10),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(
		t.Context(), `SET LOCAL ROLE vane_profile_claim_editor`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(t.Context(), `
		INSERT INTO profile_claims (
		  tenant_id,user_id,profile_epoch,field_name,claim_value,source_state
		) VALUES (1,$1,$2,'tag',$3,'manual')`,
		u.ID, claims.ProfileEpoch, oversized,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(t.Context(), `
		UPDATE profiles SET tags=ARRAY['short',$1]::text[]
		 WHERE tenant_id=1 AND user_id=$2`,
		oversized, u.ID,
	); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := st.ApplyProfileEpochAction(
		t.Context(), 1, u.ID,
		types.ProfileEpochAction{
			ExpectedEpoch: claims.ProfileEpoch, ExpectedVersion: claims.Version,
			Action: "reset", Scope: "history_learning",
		},
		"limit-reset-"+uuid.NewString(), strings.Repeat("4", 64),
	); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("reset accepted oversized carried claim: %v", err)
	}
}

func TestProfileEpochTwoEpochTenantPurge(t *testing.T) {
	if os.Getenv("DATABASE_URL") == "" {
		t.Skip("未设置 DATABASE_URL，跳过画像 epoch 租户清理测试")
	}
	st := purgeStore(t)
	tenantID := seedPurgeTenant(t, st)
	var userID int64
	if err := st.pool.QueryRow(t.Context(), `
		SELECT user_id
		  FROM memberships
		 WHERE tenant_id=$1
		 ORDER BY user_id
		 LIMIT 1`, tenantID).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	claims, err := st.ListProfileClaims(t.Context(), tenantID, userID)
	if err != nil {
		t.Fatal(err)
	}
	reset, err := st.ApplyProfileEpochAction(
		t.Context(), tenantID, userID,
		types.ProfileEpochAction{
			ExpectedEpoch: claims.ProfileEpoch, ExpectedVersion: claims.Version,
			Action: "reset", Scope: "history_learning",
		},
		"purge-reset-"+uuid.NewString(), strings.Repeat("5", 64),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.ApplyProfileEpochAction(
		t.Context(), tenantID, userID,
		types.ProfileEpochAction{
			ExpectedEpoch: reset.ProfileEpoch, ExpectedVersion: reset.Version,
			Action: "restore",
		},
		"purge-restore-"+uuid.NewString(), strings.Repeat("6", 64),
	); err != nil {
		t.Fatal(err)
	}

	report, err := st.PurgeTenant(t.Context(), tenantID, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{
		"profile_feedback_epoch_fences", "profile_epoch_checkpoints",
		"profile_epoch_events", "profile_epoch_receipts", "profile_epochs",
	} {
		if report.Rows[table] == 0 {
			t.Errorf("purge report omitted populated Phase B table %s", table)
		}
		var rows int
		if err := st.pool.QueryRow(
			t.Context(), "SELECT count(*) FROM "+table+" WHERE tenant_id=$1",
			tenantID,
		).Scan(&rows); err != nil {
			t.Fatal(err)
		}
		if rows != 0 {
			t.Errorf("tenant purge left %d rows in %s", rows, table)
		}
	}
}

func pgxTxOptionsForTest() pgx.TxOptions {
	return pgx.TxOptions{}
}
