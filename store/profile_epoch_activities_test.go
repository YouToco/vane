package store

import (
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/YouToco/vane/types"
)

func TestAggregateQuestionActivityClosesRestoreWithoutLearning(
	t *testing.T,
) {
	dbURL, db, provider := openMigration066Database(
		t, "vane_profile_activity_behavior")
	if _, err := provider.UpTo(t.Context(), 69); err != nil {
		t.Fatal(err)
	}
	st, err := New(t.Context(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	registerStoreClose(t, st)

	user, err := st.UpsertUserByOpenID(
		t.Context(), "profile_activity_"+uuid.NewString(), "activity")
	if err != nil {
		t.Fatal(err)
	}
	attachTenant(t, st, user.ID)
	suffixA := "activity-a-" + uuid.NewString()
	suffixB := "activity-b-" + uuid.NewString()
	messageID := "om_aggregate_" + uuid.NewString()
	var tenant2ID int64
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(ctx, t, st,
			`DELETE FROM profile_epoch_activities WHERE user_id=$1`, user.ID)
		cleanupExec(ctx, t, st,
			`DELETE FROM feedbacks WHERE user_id=$1`, user.ID)
		cleanupExec(ctx, t, st,
			`DELETE FROM deliveries WHERE user_id=$1`, user.ID)
		cleanupExec(ctx, t, st,
			`DELETE FROM push_batches WHERE user_id=$1`, user.ID)
		for _, suffix := range []string{suffixA, suffixB} {
			url := "https://migration-066.example/" + suffix
			cleanupExec(ctx, t, st, `
				DELETE FROM content_items
				 WHERE source_id IN (SELECT id FROM sources WHERE url=$1)`,
				url)
			cleanupExec(ctx, t, st,
				`DELETE FROM sources WHERE url=$1`, url)
		}
		for _, table := range []string{
			"profile_epoch_receipts", "profile_epoch_events",
			"profile_epoch_checkpoints", "profile_claim_receipts",
			"profile_claim_events", "profile_claims", "profile_claim_states",
			"profile_epochs", "profile_edit_receipts", "profile_edit_revisions",
			"profiles", "profile_feedback_epoch_fences",
		} {
			cleanupExec(ctx, t, st,
				"DELETE FROM "+table+" WHERE user_id=$1", user.ID)
		}
		cleanupExec(ctx, t, st,
			`DELETE FROM memberships WHERE user_id=$1`, user.ID)
		cleanupExec(ctx, t, st,
			`DELETE FROM users WHERE id=$1`, user.ID)
		if tenant2ID > 0 {
			cleanupExec(ctx, t, st,
				`DELETE FROM tenants WHERE id=$1`, tenant2ID)
		}
	})

	industry := "AI"
	if _, err := st.PatchProfile(
		t.Context(), 1, user.ID, nil,
		types.ProfileEditPatch{Industry: &industry},
		"activity-profile-"+uuid.NewString(), strings.Repeat("1", 64),
	); err != nil {
		t.Fatal(err)
	}
	beforeReset, err := st.ListProfileClaims(t.Context(), 1, user.ID)
	if err != nil {
		t.Fatal(err)
	}
	reset, err := st.ApplyProfileEpochAction(
		t.Context(), 1, user.ID,
		types.ProfileEpochAction{
			ExpectedEpoch:   beforeReset.ProfileEpoch,
			ExpectedVersion: beforeReset.Version,
			Action:          "reset", Scope: "history_learning",
		},
		"activity-reset-"+uuid.NewString(), strings.Repeat("2", 64),
	)
	if err != nil {
		t.Fatal(err)
	}
	if reset.ProfileEpoch != 1 || !reset.RestoreAllowed {
		t.Fatalf("reset=%+v", reset)
	}

	deliveryA := seedMigration066DeliveryForUser(
		t, db, user.ID, suffixA)
	deliveryB := seedMigration066DeliveryForUser(
		t, db, user.ID, suffixB)
	if _, err := db.ExecContext(t.Context(),
		`UPDATE deliveries SET feishu_message_id=$1,status='sent',
		        sent_at=now()
		  WHERE id=ANY($2)`,
		messageID, []int64{deliveryA, deliveryB},
	); err != nil {
		t.Fatal(err)
	}

	const appIdentity = "cli_profile_activity_test"
	inboundKey := "om_inbound_" + uuid.NewString()
	requestDigest := strings.Repeat("a", 64)
	const firstWrappedContext = "[追问上下文] durable aggregate receipt"
	storedContext, err := st.RecordAggregateQuestionActivity(
		t.Context(), user.ID, appIdentity, inboundKey,
		messageID, requestDigest,
		[]int64{deliveryA, deliveryB},
		firstWrappedContext,
	)
	if err != nil {
		t.Fatal(err)
	}
	if storedContext != firstWrappedContext {
		t.Fatalf("stored context=%q want %q",
			storedContext, firstWrappedContext)
	}

	var (
		activityCount, feedbackCount int
		activityEpoch                int64
		storedSetDigest              string
	)
	if err := db.QueryRowContext(t.Context(), `
		SELECT count(*),min(profile_epoch),min(delivery_set_digest),
		       (SELECT count(*) FROM feedbacks WHERE user_id=$1)
		  FROM profile_epoch_activities
		 WHERE tenant_id=1 AND user_id=$1`,
		user.ID,
	).Scan(
		&activityCount, &activityEpoch, &storedSetDigest, &feedbackCount,
	); err != nil {
		t.Fatal(err)
	}
	wantSetDigest := aggregateQuestionDeliverySetDigest(
		[]int64{deliveryA, deliveryB})
	if activityCount != 1 || activityEpoch != reset.ProfileEpoch ||
		storedSetDigest != wantSetDigest || feedbackCount != 0 {
		t.Fatalf(
			"activity=%d epoch=%d digest=%q feedback=%d want epoch=%d digest=%q",
			activityCount, activityEpoch, storedSetDigest, feedbackCount,
			reset.ProfileEpoch, wantSetDigest,
		)
	}
	rows, err := st.ListFeedbacksForEvolution(
		t.Context(), user.ID, 0, 50)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("non-learning activity leaked into Evolver: %+v", rows)
	}
	dirty, err := st.ListProfileClaims(t.Context(), 1, user.ID)
	if err != nil {
		t.Fatal(err)
	}
	if dirty.RestoreAllowed {
		t.Fatal("current-epoch aggregate question activity must close restore")
	}
	if _, err := st.ApplyProfileEpochAction(
		t.Context(), 1, user.ID,
		types.ProfileEpochAction{
			ExpectedEpoch: dirty.ProfileEpoch, ExpectedVersion: dirty.Version,
			Action: "restore",
		},
		"activity-restore-rejected-"+uuid.NewString(),
		strings.Repeat("f", 64),
	); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("activity-dirty restore must conflict: %v", err)
	}

	// Same inbound event and digest is an exact lifetime replay.
	if replayContext, err := st.RecordAggregateQuestionActivity(
		t.Context(), user.ID, appIdentity, inboundKey,
		messageID, requestDigest,
		[]int64{deliveryA, deliveryB},
		"later rebuilt context must not replace the receipt",
	); err != nil {
		t.Fatalf("exact replay: %v", err)
	} else if replayContext != firstWrappedContext {
		t.Fatalf("exact replay context=%q want first=%q",
			replayContext, firstWrappedContext)
	}
	if err := db.QueryRowContext(t.Context(), `
		SELECT count(*) FROM profile_epoch_activities
		 WHERE tenant_id=1 AND user_id=$1`,
		user.ID,
	).Scan(&activityCount); err != nil {
		t.Fatal(err)
	}
	if activityCount != 1 {
		t.Fatalf("exact replay duplicated activity: %d", activityCount)
	}
	// Lifetime replay is the stored inbound fact, not a re-derivation from a
	// later repaired delivery set.
	if _, err := db.ExecContext(t.Context(),
		`UPDATE deliveries SET feishu_message_id=$1 WHERE id=ANY($2)`,
		"om_repaired_"+uuid.NewString(), []int64{deliveryA, deliveryB},
	); err != nil {
		t.Fatal(err)
	}
	if replayContext, found, err := st.LookupAggregateQuestionActivity(
		t.Context(), user.ID, appIdentity, inboundKey, requestDigest,
	); err != nil {
		t.Fatalf("lookup lifetime receipt after total mapping drift: %v", err)
	} else if !found || replayContext != firstWrappedContext {
		t.Fatalf("lookup after drift found/context=%v/%q",
			found, replayContext)
	}
	if replayContext, err := st.RecordAggregateQuestionActivity(
		t.Context(), user.ID, appIdentity, inboundKey,
		messageID, requestDigest,
		[]int64{deliveryA, deliveryB},
		"delivery drift must not rebuild context",
	); err != nil {
		t.Fatalf("delivery-set drift must not break lifetime replay: %v", err)
	} else if replayContext != firstWrappedContext {
		t.Fatalf("delivery drift replay context=%q want first=%q",
			replayContext, firstWrappedContext)
	}
	if _, err := db.ExecContext(t.Context(),
		`UPDATE deliveries SET feishu_message_id=$1 WHERE id=ANY($2)`,
		messageID, []int64{deliveryA, deliveryB},
	); err != nil {
		t.Fatal(err)
	}
	if _, err := st.RecordAggregateQuestionActivity(
		t.Context(), user.ID, appIdentity, inboundKey,
		messageID, strings.Repeat("b", 64),
		[]int64{deliveryA, deliveryB},
		firstWrappedContext,
	); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("same inbound key with another digest must conflict: %v", err)
	}
	if _, err := st.RecordAggregateQuestionActivity(
		t.Context(), user.ID, appIdentity,
		"om_inbound_mismatch_"+uuid.NewString(),
		messageID, strings.Repeat("d", 64),
		[]int64{deliveryA, deliveryB, deliveryB + 999999},
		firstWrappedContext,
	); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("caller/store delivery-set mismatch must conflict: %v", err)
	}

	if err := db.QueryRowContext(t.Context(), `
		INSERT INTO tenants DEFAULT VALUES RETURNING id`,
	).Scan(&tenant2ID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `
		INSERT INTO memberships(tenant_id,user_id,role)
		VALUES($1,$2,'member')`, tenant2ID, user.ID,
	); err != nil {
		t.Fatal(err)
	}
	var contentA, contentB, tenant2Batch int64
	if err := db.QueryRowContext(t.Context(), `
		SELECT min(content_item_id),max(content_item_id)
		  FROM deliveries WHERE id=ANY($1)`,
		[]int64{deliveryA, deliveryB},
	).Scan(&contentA, &contentB); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(t.Context(), `
		INSERT INTO push_batches(tenant_id,user_id)
		VALUES($1,$2) RETURNING id`,
		tenant2ID, user.ID,
	).Scan(&tenant2Batch); err != nil {
		t.Fatal(err)
	}
	var tenant2DeliveryA, tenant2DeliveryB int64
	if err := db.QueryRowContext(t.Context(), `
		INSERT INTO deliveries
		  (tenant_id,batch_id,user_id,content_item_id,body_md,
		   feishu_message_id,status,sent_at)
		VALUES($1,$2,$3,$4,'tenant-2-a',$5,'sent',now())
		RETURNING id`,
		tenant2ID, tenant2Batch, user.ID, contentA,
		"om_cross_tenant_"+uuid.NewString(),
	).Scan(&tenant2DeliveryA); err != nil {
		t.Fatal(err)
	}
	crossTenantMessageID := "om_cross_tenant_" + uuid.NewString()
	if _, err := db.ExecContext(t.Context(), `
		UPDATE deliveries SET feishu_message_id=$1 WHERE id=$2`,
		crossTenantMessageID, tenant2DeliveryA,
	); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(t.Context(), `
		INSERT INTO deliveries
		  (tenant_id,batch_id,user_id,content_item_id,body_md,
		   feishu_message_id,status,sent_at)
		VALUES($1,$2,$3,$4,'tenant-2-b',$5,'sent',now())
		RETURNING id`,
		tenant2ID, tenant2Batch, user.ID, contentB,
		crossTenantMessageID,
	).Scan(&tenant2DeliveryB); err != nil {
		t.Fatal(err)
	}
	if _, err := st.RecordAggregateQuestionActivity(
		t.Context(), user.ID, appIdentity, inboundKey,
		crossTenantMessageID, requestDigest,
		[]int64{tenant2DeliveryA, tenant2DeliveryB},
		"cross tenant must not replay",
	); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("cross-tenant lifetime key reuse must conflict: %v", err)
	}

	concurrentKey := "om_inbound_concurrent_" + uuid.NewString()
	concurrentDigest := strings.Repeat("e", 64)
	start := make(chan struct{})
	type recordResult struct {
		context string
		err     error
	}
	results := make(chan recordResult, 2)
	for candidate := range 2 {
		go func(candidate int) {
			<-start
			stored, err := st.RecordAggregateQuestionActivity(
				t.Context(), user.ID, appIdentity, concurrentKey,
				messageID, concurrentDigest,
				[]int64{deliveryA, deliveryB},
				"[追问上下文] concurrent candidate "+
					strconv.Itoa(candidate),
			)
			results <- recordResult{context: stored, err: err}
		}(candidate)
	}
	close(start)
	var concurrentContexts []string
	for range 2 {
		result := <-results
		if result.err != nil {
			t.Fatalf("same-key concurrent replay: %v", result.err)
		}
		concurrentContexts = append(concurrentContexts, result.context)
	}
	if concurrentContexts[0] != concurrentContexts[1] ||
		(concurrentContexts[0] !=
			"[追问上下文] concurrent candidate 0" &&
			concurrentContexts[0] !=
				"[追问上下文] concurrent candidate 1") {
		t.Fatalf("concurrent callers did not replay first context: %q",
			concurrentContexts)
	}
	if err := db.QueryRowContext(t.Context(), `
		SELECT count(*) FROM profile_epoch_activities
		 WHERE tenant_id=1 AND user_id=$1
		   AND app_identity=$2 AND inbound_key=$3`,
		user.ID, appIdentity, concurrentKey,
	).Scan(&activityCount); err != nil {
		t.Fatal(err)
	}
	if activityCount != 1 {
		t.Fatalf("same-key concurrent replay rows=%d want 1", activityCount)
	}

	// A later reset creates a pristine epoch. Replaying the old inbound event
	// must remain the old fact rather than dirtying the new epoch.
	secondReset, err := st.ApplyProfileEpochAction(
		t.Context(), 1, user.ID,
		types.ProfileEpochAction{
			ExpectedEpoch:   dirty.ProfileEpoch,
			ExpectedVersion: dirty.Version,
			Action:          "reset", Scope: "history_learning",
		},
		"activity-reset-2-"+uuid.NewString(), strings.Repeat("3", 64),
	)
	if err != nil {
		t.Fatal(err)
	}
	if replayContext, err := st.RecordAggregateQuestionActivity(
		t.Context(), user.ID, appIdentity, inboundKey,
		messageID, requestDigest,
		[]int64{deliveryA, deliveryB},
		"cross epoch must replay first context",
	); err != nil {
		t.Fatalf("cross-epoch exact replay: %v", err)
	} else if replayContext != firstWrappedContext {
		t.Fatalf("cross-epoch replay context=%q want first=%q",
			replayContext, firstWrappedContext)
	}
	pristine, err := st.ListProfileClaims(t.Context(), 1, user.ID)
	if err != nil {
		t.Fatal(err)
	}
	if pristine.ProfileEpoch != secondReset.ProfileEpoch ||
		!pristine.RestoreAllowed {
		t.Fatalf("old inbound replay dirtied new epoch: %+v", pristine)
	}

	if _, err := st.RecordAggregateQuestionActivity(
		t.Context(), user.ID, appIdentity,
		"om_inbound_new_"+uuid.NewString(),
		messageID, strings.Repeat("c", 64),
		[]int64{deliveryA, deliveryB},
		"[追问上下文] new epoch receipt",
	); err != nil {
		t.Fatal(err)
	}
	closedAgain, err := st.ListProfileClaims(t.Context(), 1, user.ID)
	if err != nil {
		t.Fatal(err)
	}
	if closedAgain.RestoreAllowed {
		t.Fatal("new inbound event must close restore in the new epoch")
	}
}
