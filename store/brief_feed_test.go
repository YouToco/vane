package store

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/types"
)

func TestTaskBriefFeedSeparatesLatestCheckAndProjectsFeedback(t *testing.T) {
	f := newCanonicalBriefFixture(t, 2)
	contentOutcome := f.finalizedContentOutcome(t)
	brief, err := f.base.st.FreezeBriefV1(
		t.Context(), f.identity, f.ref, f.draft(t, contentOutcome))
	if err != nil {
		t.Fatal(err)
	}
	feedbackID, err := f.base.st.InsertFeedback(t.Context(), &types.Feedback{
		UserID: f.identity.UserID, DeliveryID: brief.Insights[0].ID,
		Action: types.FeedbackActionInterested,
		Detail: "current feedback",
	})
	if err != nil {
		t.Fatal(err)
	}
	latestFeedbackID, err := f.base.st.InsertFeedback(
		t.Context(), &types.Feedback{
			UserID: f.identity.UserID, DeliveryID: brief.Insights[0].ID,
			Action: types.FeedbackActionNotInterested,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(ctx, t, f.base.st,
			`DELETE FROM feedbacks WHERE id=ANY($1)`,
			[]int64{feedbackID, latestFeedbackID})
	})

	quietIdentity := f.identity
	quietIdentity.TemporalRunID = uuid.NewString()
	quietRef, err := f.base.st.CreateOrGetCompiledTaskRunSnapshotV1(
		t.Context(), CreateOrGetCompiledTaskRunSnapshotV1Params{
			Identity: quietIdentity, Policy: testCompiledRunPolicyV1(t),
		})
	if err != nil {
		t.Fatal(err)
	}
	quietMarker, err := f.base.st.CreatePendingRunOutcomeV1(
		t.Context(), quietIdentity, quietRef)
	if err != nil {
		t.Fatal(err)
	}
	quietOutcome, err := (types.RunOutcomeV1{
		RunOutcomeMarkerV1: quietMarker,
		Result:             types.RunResultQuiet,
		SourceCoverage:     types.RunCompletenessComplete,
		Processing:         types.RunCompletenessComplete,
		FinalizedAt:        time.Now(),
	}).Seal()
	if err != nil {
		t.Fatal(err)
	}
	quietOutcome, err = f.base.st.FinalizeRunOutcomeV1(
		t.Context(), quietIdentity, quietRef, quietOutcome)
	if err != nil {
		t.Fatal(err)
	}

	page, err := f.base.st.ListTaskBriefsV1(
		t.Context(), f.identity.TenantID, f.identity.UserID,
		f.identity.TaskID, TaskBriefQuery{PageSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	if page.Total != 1 || len(page.Items) != 1 ||
		page.Items[0].ID != brief.ID ||
		page.Items[0].PushBatchID != f.batchID ||
		len(page.Items[0].Insights) != 2 {
		t.Fatalf("brief page = %+v", page)
	}
	if page.NextPageToken != "" {
		t.Fatalf("single complete page returned cursor %q", page.NextPageToken)
	}
	if page.LatestCheck == nil ||
		page.LatestCheck.Result != types.RunResultQuiet ||
		!page.LatestCheck.FinalizedAt.Equal(quietOutcome.FinalizedAt) {
		t.Fatalf("latest check = %+v, quiet outcome = %+v",
			page.LatestCheck, quietOutcome)
	}
	if feedback := page.Items[0].Insights[0].Feedback; feedback.Preference != string(types.FeedbackActionNotInterested) ||
		feedback.Misjudged || feedback.DeepDiveRequested {
		t.Fatalf("current feedback projection = %+v", feedback)
	}
	if feedback := page.Items[0].Insights[1].Feedback; feedback.Preference != "" || feedback.Misjudged ||
		feedback.DeepDiveRequested {
		t.Fatalf("feedback leaked across insight: %+v",
			feedback)
	}
}

func TestTaskBriefFeedRejectsCrossScopeAndCursorReplay(t *testing.T) {
	f := newCanonicalBriefFixture(t, 1)
	outcome := f.finalizedContentOutcome(t)
	if _, err := f.base.st.FreezeBriefV1(
		t.Context(), f.identity, f.ref, f.draft(t, outcome)); err != nil {
		t.Fatal(err)
	}

	if _, err := f.base.st.ListTaskBriefsV1(
		t.Context(), f.identity.TenantID+999, f.identity.UserID,
		f.identity.TaskID, TaskBriefQuery{}); err == nil {
		t.Fatal("cross-tenant Brief read was admitted")
	}
	if _, err := f.base.st.ListTaskBriefsV1(
		t.Context(), f.identity.TenantID, f.identity.UserID+999,
		f.identity.TaskID, TaskBriefQuery{}); err == nil {
		t.Fatal("cross-user Brief read was admitted")
	}

	token, err := encodeBriefFeedCursorV1(
		f.identity.TaskID,
		time.Now().Round(0).UTC().Truncate(time.Microsecond),
		123,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeBriefFeedCursorV1(token, "another-task"); err == nil {
		t.Fatal("task-bound cursor replay was admitted")
	}
	if _, err := decodeBriefFeedCursorV1(
		token+base64URLGarbageForTest, f.identity.TaskID); err == nil {
		t.Fatal("cursor with trailing bytes was admitted")
	}
	if _, err := f.base.st.ListTaskBriefsV1(
		t.Context(), f.identity.TenantID, f.identity.UserID,
		f.identity.TaskID, TaskBriefQuery{
			PageToken: strings.Repeat("x", 2049),
		}); err == nil {
		t.Fatal("oversized cursor was admitted")
	}
}

const base64URLGarbageForTest = "x"

func TestTaskBriefFeedPaginatesWholeBriefs(t *testing.T) {
	f := newCanonicalBriefFixture(t, 1)
	firstOutcome := f.finalizedContentOutcome(t)
	firstDraft := f.draft(t, firstOutcome)
	firstDraft.GeneratedAt = time.Date(
		2026, 7, 27, 1, 0, 0, 0, time.UTC)
	firstBrief, err := f.base.st.FreezeBriefV1(
		t.Context(), f.identity, f.ref, firstDraft)
	if err != nil {
		t.Fatal(err)
	}

	secondIdentity := f.identity
	secondIdentity.TemporalRunID = uuid.NewString()
	secondRef, err := f.base.st.CreateOrGetCompiledTaskRunSnapshotV1(
		t.Context(), CreateOrGetCompiledTaskRunSnapshotV1Params{
			Identity: secondIdentity, Policy: testCompiledRunPolicyV1(t),
		})
	if err != nil {
		t.Fatal(err)
	}
	secondBatchID, err := f.base.st.CreatePushBatchForTaskRunV1(
		t.Context(), secondIdentity, secondRef, "brief-page-"+uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	secondURL := "https://brief.test/page/" + uuid.NewString()
	secondContentID, created, err := f.base.st.UpsertContentItem(
		t.Context(), &types.ContentItem{
			SourceID: f.sourceID, ExternalID: uuid.NewString(),
			CanonicalKey: secondURL, URL: secondURL, Title: "second Brief",
			ContentHash: "hash-" + uuid.NewString(),
		})
	if err != nil || !created {
		t.Fatalf("create second content: created=%t err=%v", created, err)
	}
	secondDeliveryID, _, _, err := f.base.st.InsertDeliveryIdempotent(
		t.Context(), &types.Delivery{
			BatchID: secondBatchID, UserID: f.identity.UserID,
			ContentItemID: &secondContentID, Score: 90,
			BodyMD: "second body", CardJSON: json.RawMessage(`{}`),
		})
	if err != nil {
		t.Fatal(err)
	}
	var discoveredAt time.Time
	if err := f.base.st.pool.QueryRow(t.Context(),
		`SELECT created_at FROM deliveries WHERE id=$1`,
		secondDeliveryID,
	).Scan(&discoveredAt); err != nil {
		t.Fatal(err)
	}
	secondMarker, err := f.base.st.CreatePendingRunOutcomeV1(
		t.Context(), secondIdentity, secondRef)
	if err != nil {
		t.Fatal(err)
	}
	secondOutcome, err := (types.RunOutcomeV1{
		RunOutcomeMarkerV1: secondMarker,
		Result:             types.RunResultContent,
		SourceCoverage:     types.RunCompletenessComplete,
		Processing:         types.RunCompletenessComplete,
		FinalizedAt:        time.Now(),
	}).Seal()
	if err != nil {
		t.Fatal(err)
	}
	secondOutcome, err = f.base.st.FinalizeRunOutcomeV1(
		t.Context(), secondIdentity, secondRef, secondOutcome)
	if err != nil {
		t.Fatal(err)
	}
	secondBrief, err := f.base.st.FreezeBriefV1(
		t.Context(), secondIdentity, secondRef, types.BriefDraftV1{
			SchemaVersion: types.BriefSchemaVersionV1,
			RunOutcomeID:  secondOutcome.ID,
			RunSnapshotID: secondRef.SnapshotID,
			PushBatchID:   secondBatchID,
			TenantID:      f.identity.TenantID,
			UserID:        f.identity.UserID,
			TaskID:        f.identity.TaskID,
			GeneratedAt: time.Date(
				2026, 7, 27, 2, 0, 0, 0, time.UTC),
			Insights: []types.InsightV1{{
				ID: secondDeliveryID, RankPosition: 1,
				Title: "second Brief", BodyMD: "second body",
				SourceTitle: f.sourceName, SourceURL: secondURL,
				DiscoveredAt: discoveredAt.Round(0).UTC().
					Truncate(time.Microsecond),
				Structured: &types.StructuredInsightV1{
					SchemaVersion:    types.StructuredInsightSchemaVersionV1,
					BodyMD:           "second body",
					WhatChanged:      "second change",
					WhyItMatters:     "second relevance",
					ImportanceReason: "second evidence",
					Claims: []types.StructuredClaimV1{{
						Text:       "second claim",
						Excerpt:    "second excerpt",
						SourceRefs: []string{"source-1"},
					}},
				},
			}},
		})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(ctx, t, f.base.st,
			`DELETE FROM brief_snapshots WHERE id=$1`, secondBrief.ID)
		cleanupExec(ctx, t, f.base.st,
			`DELETE FROM task_run_outcomes WHERE id=$1`, secondOutcome.ID)
		cleanupExec(ctx, t, f.base.st,
			`DELETE FROM deliveries WHERE id=$1`, secondDeliveryID)
		cleanupExec(ctx, t, f.base.st,
			`DELETE FROM push_batches WHERE id=$1`, secondBatchID)
		cleanupExec(ctx, t, f.base.st,
			`DELETE FROM content_sources WHERE content_item_id=$1`,
			secondContentID)
		cleanupExec(ctx, t, f.base.st,
			`DELETE FROM content_items WHERE id=$1`, secondContentID)
	})

	firstPage, err := f.base.st.ListTaskBriefsV1(
		t.Context(), f.identity.TenantID, f.identity.UserID,
		f.identity.TaskID, TaskBriefQuery{PageSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	if firstPage.Total != 2 || len(firstPage.Items) != 1 ||
		firstPage.Items[0].ID != secondBrief.ID ||
		firstPage.NextPageToken == "" {
		t.Fatalf("first page = %+v", firstPage)
	}
	if structured := firstPage.Items[0].Insights[0].Structured; structured == nil ||
		structured.WhatChanged != "second change" ||
		structured.WhyItMatters != "second relevance" ||
		structured.ImportanceReason != "second evidence" ||
		len(structured.Claims) != 1 ||
		structured.Claims[0].SourceRefs[0] != "source-1" {
		t.Fatalf("structured Web projection = %+v", structured)
	}
	secondPage, err := f.base.st.ListTaskBriefsV1(
		t.Context(), f.identity.TenantID, f.identity.UserID,
		f.identity.TaskID, TaskBriefQuery{
			PageSize: 1, PageToken: firstPage.NextPageToken,
		})
	if err != nil {
		t.Fatal(err)
	}
	if secondPage.Total != 2 || len(secondPage.Items) != 1 ||
		secondPage.Items[0].ID != firstBrief.ID ||
		secondPage.NextPageToken != "" {
		t.Fatalf("second page = %+v", secondPage)
	}
}

func TestBriefFeedPageSizeIsBounded(t *testing.T) {
	if got := clampBriefFeedPageSizeV1(0); got != 10 {
		t.Fatalf("default page size = %d", got)
	}
	if got := clampBriefFeedPageSizeV1(999); got != maxBriefFeedPageSizeV1 {
		t.Fatalf("bounded page size = %d", got)
	}
}

func TestBriefFeedByteCapKeepsShortPageAndCursorAdmission(t *testing.T) {
	items := []TaskBriefItemV1{{
		ID: 7, GeneratedAt: time.Date(
			2026, 7, 27, 3, 0, 0, 0, time.UTC),
	}}
	trimmed, hasMore := trimBriefFeedPageV1(items, 10, true)
	if !hasMore || len(trimmed) != 1 || trimmed[0].ID != 7 {
		t.Fatalf("byte-capped short page = %+v hasMore=%t",
			trimmed, hasMore)
	}
	if _, err := encodeBriefFeedCursorV1(
		"task-1", trimmed[0].GeneratedAt, trimmed[0].ID,
	); err != nil {
		t.Fatalf("short byte-capped page cannot advance: %v", err)
	}
}

func TestMigration065BriefReaderACL(t *testing.T) {
	f := newCanonicalBriefFixture(t, 0)
	var (
		noLogin, noInherit, noBypass, noSuper bool
		ownerCanSet, appCanSet                bool
		canBrief, canOutcome, canFeedback     bool
		canFeedbackReason, canFeedbackDetail  bool
		canDelivery, canContent, canWrite     bool
	)
	if err := f.base.st.pool.QueryRow(t.Context(), `
		SELECT NOT rolcanlogin,NOT rolinherit,NOT rolbypassrls,NOT rolsuper,
		       pg_has_role(current_user,oid,'SET'),
		       pg_has_role('vane_app',oid,'SET'),
		       has_column_privilege(
		           'vane_brief_reader','brief_snapshots','payload','SELECT'),
		       has_column_privilege(
		           'vane_brief_reader','task_run_outcomes','result','SELECT'),
		       has_column_privilege(
		           'vane_brief_reader','feedbacks','action','SELECT'),
		       has_column_privilege(
		           'vane_brief_reader','feedbacks','reason_code','SELECT'),
		       has_column_privilege(
		           'vane_brief_reader','feedbacks','detail','SELECT'),
		       has_column_privilege(
		           'vane_brief_reader','deliveries','id','SELECT'),
		       has_column_privilege(
		           'vane_brief_reader','content_items','id','SELECT'),
		       has_column_privilege(
		           'vane_brief_reader','brief_snapshots','payload','UPDATE')
		  FROM pg_roles WHERE rolname='vane_brief_reader'`,
	).Scan(
		&noLogin, &noInherit, &noBypass, &noSuper,
		&ownerCanSet, &appCanSet,
		&canBrief, &canOutcome, &canFeedback,
		&canFeedbackReason, &canFeedbackDetail,
		&canDelivery, &canContent, &canWrite,
	); err != nil {
		t.Fatal(err)
	}
	if !noLogin || !noInherit || !noBypass || !noSuper ||
		!ownerCanSet || appCanSet ||
		!canBrief || !canOutcome || !canFeedback ||
		canFeedbackReason || canFeedbackDetail ||
		canDelivery || canContent || canWrite {
		t.Fatalf(
			"unsafe reader ACL attrs=%t/%t/%t/%t set=%t/%t reads=%t/%t/%t sensitive_feedback=%t/%t unrelated=%t/%t write=%t",
			noLogin, noInherit, noBypass, noSuper,
			ownerCanSet, appCanSet, canBrief, canOutcome, canFeedback,
			canFeedbackReason, canFeedbackDetail,
			canDelivery, canContent, canWrite,
		)
	}
}

func TestMigration065BriefReaderRLSRejectsSameTenantOtherUser(t *testing.T) {
	f := newCanonicalBriefFixture(t, 1)
	otherUserID := testUser(t, f.base.st)
	if err := f.base.st.AddMembership(
		t.Context(), f.identity.TenantID, otherUserID,
		types.MembershipRoleMember,
	); err != nil {
		t.Fatal(err)
	}
	ownFeedbackID, err := f.base.st.InsertFeedback(
		t.Context(), &types.Feedback{
			UserID: f.identity.UserID, DeliveryID: f.deliveryID[0],
			Action: types.FeedbackActionInterested,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	var otherBatchID, otherDeliveryID, otherFeedbackID int64
	if err := f.base.st.pool.QueryRow(t.Context(), `
		INSERT INTO push_batches (tenant_id,user_id)
		VALUES ($1,$2) RETURNING id`,
		f.identity.TenantID, otherUserID,
	).Scan(&otherBatchID); err != nil {
		t.Fatal(err)
	}
	if err := f.base.st.pool.QueryRow(t.Context(), `
		INSERT INTO deliveries (tenant_id,batch_id,user_id)
		VALUES ($1,$2,$3) RETURNING id`,
		f.identity.TenantID, otherBatchID, otherUserID,
	).Scan(&otherDeliveryID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(ctx, t, f.base.st,
			`DELETE FROM feedbacks WHERE id=ANY($1)`,
			[]int64{ownFeedbackID, otherFeedbackID})
		cleanupExec(ctx, t, f.base.st,
			`DELETE FROM deliveries WHERE id=$1`, otherDeliveryID)
		cleanupExec(ctx, t, f.base.st,
			`DELETE FROM push_batches WHERE id=$1`, otherBatchID)
		cleanupExec(ctx, t, f.base.st,
			`DELETE FROM memberships WHERE tenant_id=$1 AND user_id=$2`,
			f.identity.TenantID, otherUserID)
		cleanupExec(ctx, t, f.base.st,
			`DELETE FROM users WHERE id=$1`, otherUserID)
	})
	if err := f.base.st.pool.QueryRow(t.Context(),
		`INSERT INTO feedbacks (
		     tenant_id,user_id,delivery_id,action
		 ) VALUES ($1,$2,$3,'not_interested')
		 RETURNING id`,
		f.identity.TenantID, otherUserID, otherDeliveryID,
	).Scan(&otherFeedbackID); err != nil {
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
		fmt.Sprint(f.identity.UserID),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(
		t.Context(), `SET LOCAL ROLE vane_brief_reader`); err != nil {
		t.Fatal(err)
	}
	var visible []int64
	if err := tx.QueryRow(t.Context(),
		`SELECT COALESCE(array_agg(id ORDER BY id),'{}'::bigint[])
		   FROM feedbacks WHERE id=ANY($1)`,
		[]int64{ownFeedbackID, otherFeedbackID},
	).Scan(&visible); err != nil {
		t.Fatal(err)
	}
	if len(visible) != 1 || visible[0] != ownFeedbackID {
		t.Fatalf("reader same-tenant cross-user visibility=%v, want [%d]",
			visible, ownFeedbackID)
	}
}

func TestTaskBriefFeedLockOrderMatchesTenantPurge(t *testing.T) {
	f := newCanonicalBriefFixture(t, 0)
	blocker, err := f.base.st.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = blocker.Rollback(t.Context()) }()
	if exists, err := lockTenantAdmissionRoot(
		t.Context(), blocker, f.identity.TenantID,
	); err != nil || !exists {
		t.Fatalf("lock purge tenant root exists=%t: %v", exists, err)
	}
	if _, err := blocker.Exec(
		t.Context(), `SET LOCAL lock_timeout='500ms'`); err != nil {
		t.Fatal(err)
	}

	readDone := make(chan error, 1)
	go func() {
		_, readErr := f.base.st.ListTaskBriefsV1(
			t.Context(), f.identity.TenantID, f.identity.UserID,
			f.identity.TaskID, TaskBriefQuery{},
		)
		readDone <- readErr
	}()

	deadline := time.Now().Add(3 * time.Second)
	for {
		var waiting bool
		if err := f.base.st.pool.QueryRow(t.Context(),
			`SELECT EXISTS (
			     SELECT 1 FROM pg_stat_activity
			      WHERE datname=current_database()
			        AND pid<>pg_backend_pid()
			        AND wait_event_type='Lock'
			        AND query LIKE '%pg_advisory_xact_lock_shared%'
			 )`,
		).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("brief reader did not wait at the tenant admission root")
		}
		time.Sleep(10 * time.Millisecond)
	}

	var scheduleLocked, membershipLocked bool
	if err := blocker.QueryRow(t.Context(),
		`SELECT true FROM schedules
		  WHERE tenant_id=$1 AND user_id=$2 AND id=$3
		  FOR UPDATE`,
		f.identity.TenantID, f.identity.UserID, f.identity.TaskID,
	).Scan(&scheduleLocked); err != nil {
		t.Fatalf("purge could not acquire schedule behind root: %v", err)
	}
	if err := blocker.QueryRow(t.Context(),
		`SELECT true FROM memberships
		  WHERE tenant_id=$1 AND user_id=$2
		  FOR UPDATE`,
		f.identity.TenantID, f.identity.UserID,
	).Scan(&membershipLocked); err != nil {
		t.Fatalf("schedule-first purge lock could not acquire membership: %v", err)
	}
	if !membershipLocked {
		t.Fatal("membership lock was not acquired")
	}
	if !scheduleLocked {
		t.Fatal("schedule lock was not acquired")
	}
	if err := blocker.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("brief reader did not drain after tenant root release")
	}
}

func TestTaskBriefFeedLockOrderMatchesTaskCreation(t *testing.T) {
	f := newCanonicalBriefFixture(t, 0)
	creator, err := f.base.st.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = creator.Rollback(t.Context()) }()
	if _, err := creator.Exec(
		t.Context(), `SET LOCAL lock_timeout='500ms'`); err != nil {
		t.Fatal(err)
	}
	var membershipLocked bool
	if err := creator.QueryRow(t.Context(),
		`SELECT true FROM memberships
		  WHERE tenant_id=$1 AND user_id=$2
		  FOR UPDATE`,
		f.identity.TenantID, f.identity.UserID,
	).Scan(&membershipLocked); err != nil || !membershipLocked {
		t.Fatalf("task creation membership lock=%t: %v",
			membershipLocked, err)
	}

	readDone := make(chan error, 1)
	go func() {
		_, readErr := f.base.st.ListTaskBriefsV1(
			t.Context(), f.identity.TenantID, f.identity.UserID,
			f.identity.TaskID, TaskBriefQuery{},
		)
		readDone <- readErr
	}()

	deadline := time.Now().Add(3 * time.Second)
	for {
		var waiting bool
		if err := f.base.st.pool.QueryRow(t.Context(),
			`SELECT EXISTS (
			     SELECT 1 FROM pg_stat_activity
			      WHERE datname=current_database()
			        AND pid<>pg_backend_pid()
			        AND wait_event_type='Lock'
			        AND query LIKE '%FROM memberships%'
			        AND query LIKE '%FOR KEY SHARE%'
			 )`,
		).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("brief reader did not wait at the membership lock")
		}
		time.Sleep(10 * time.Millisecond)
	}

	var scheduleLocked bool
	if err := creator.QueryRow(t.Context(),
		`SELECT true FROM schedules
		  WHERE tenant_id=$1 AND user_id=$2 AND id=$3
		  FOR UPDATE`,
		f.identity.TenantID, f.identity.UserID, f.identity.TaskID,
	).Scan(&scheduleLocked); err != nil || !scheduleLocked {
		t.Fatalf("membership-first task creation schedule lock=%t: %v",
			scheduleLocked, err)
	}
	if err := creator.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("brief reader did not drain after task creation lock release")
	}
}
