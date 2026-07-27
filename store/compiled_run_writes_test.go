package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/types"
)

type compiledRunWriteFixture struct {
	base *taskRunSnapshotFixture

	tenantB int64
	taskA   string
	taskB   string
	refA    types.RunSnapshotRef
	refB    types.RunSnapshotRef
	idA     types.RunIdentity
	idB     types.RunIdentity
	sourceA int64
	sourceB int64
	content []int64
}

func newCompiledRunWriteFixture(t *testing.T) *compiledRunWriteFixture {
	t.Helper()
	base := newTaskRunSnapshotFixture(t)
	ctx := t.Context()
	f := &compiledRunWriteFixture{base: base}
	f.taskA = "push-write-a-" + uuid.NewString()
	f.sourceA = base.createApprovedTask(t, f.taskA, 1)[0]
	f.idA = scheduledRunIdentity(
		f.taskA, base.tenantID, base.userID, "run-write-a-"+uuid.NewString())

	if err := base.st.pool.QueryRow(ctx,
		`INSERT INTO tenants (status, plan) VALUES ('active', 'free') RETURNING id`,
	).Scan(&f.tenantB); err != nil {
		t.Fatalf("create tenant B: %v", err)
	}
	if _, err := base.st.pool.Exec(ctx,
		`INSERT INTO memberships (tenant_id, user_id, role)
		 VALUES ($1, $2, 'owner')`, f.tenantB, base.userID); err != nil {
		t.Fatalf("attach same user to tenant B: %v", err)
	}
	f.taskB = "push-write-b-" + uuid.NewString()
	f.sourceB = createApprovedTaskForScope(
		t, base.st, f.tenantB, base.userID, f.taskB, base.urlPrefix+"/tenant-b")
	f.idB = scheduledRunIdentity(
		f.taskB, f.tenantB, base.userID, "run-write-b-"+uuid.NewString())

	policy := testCompiledRunPolicyV1(t)
	var err error
	f.refA, err = base.st.CreateOrGetCompiledTaskRunSnapshotV1(ctx,
		CreateOrGetCompiledTaskRunSnapshotV1Params{Identity: f.idA, Policy: policy})
	if err != nil {
		t.Fatalf("create tenant A snapshot: %v", err)
	}
	f.refB, err = base.st.CreateOrGetCompiledTaskRunSnapshotV1(ctx,
		CreateOrGetCompiledTaskRunSnapshotV1Params{Identity: f.idB, Policy: policy})
	if err != nil {
		t.Fatalf("create tenant B snapshot: %v", err)
	}

	// Registered after base's cleanup, so LIFO removes our dependent rows first.
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(cleanupCtx, t, base.st,
			`DELETE FROM deliveries WHERE tenant_id IN ($1, $2)`, base.tenantID, f.tenantB)
		cleanupExec(cleanupCtx, t, base.st,
			`DELETE FROM push_batches WHERE tenant_id IN ($1, $2)`, base.tenantID, f.tenantB)
		for _, table := range []string{
			"profile_claim_receipts", "profile_claim_events",
			"profile_claims", "profile_claim_states",
		} {
			cleanupExec(cleanupCtx, t, base.st,
				"DELETE FROM "+table+" WHERE user_id=$1", base.userID)
		}
		cleanupExec(cleanupCtx, t, base.st,
			`DELETE FROM profiles WHERE user_id = $1`, base.userID)
		for _, contentID := range f.content {
			cleanupExec(cleanupCtx, t, base.st,
				`DELETE FROM content_sources WHERE content_item_id = $1`, contentID)
			cleanupExec(cleanupCtx, t, base.st,
				`DELETE FROM content_items WHERE id = $1`, contentID)
		}
		cleanupExec(cleanupCtx, t, base.st,
			`DELETE FROM task_run_snapshots WHERE tenant_id = $1`, f.tenantB)
		cleanupExec(cleanupCtx, t, base.st,
			`DELETE FROM schedules WHERE tenant_id = $1`, f.tenantB)
		cleanupExec(cleanupCtx, t, base.st,
			`DELETE FROM memberships WHERE tenant_id = $1 AND user_id = $2`,
			f.tenantB, base.userID)
		cleanupExec(cleanupCtx, t, base.st,
			`DELETE FROM tenants WHERE id = $1`, f.tenantB)
	})
	return f
}

func (f *compiledRunWriteFixture) createClaimProfile(
	t *testing.T, industry, summary string, tags []string,
) {
	t.Helper()
	if tags == nil {
		tags = []string{}
	}
	ctx := t.Context()
	tx, err := f.base.st.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `
		SELECT set_config('app.tenant_id',$1,true),
		       set_config('app.user_id',$2,true)`,
		strconv.FormatInt(f.idA.TenantID, 10),
		strconv.FormatInt(f.idA.UserID, 10)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_profile_claim_editor`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO profiles(tenant_id,user_id,industry,tags)
		VALUES($1,$2,$3,$4)`,
		f.idA.TenantID, f.idA.UserID, industry, tags); err != nil {
		t.Fatal(err)
	}
	if summary != "" {
		if _, err := tx.Exec(ctx, `
			UPDATE profiles SET summary=$3
			 WHERE tenant_id=$1 AND user_id=$2`,
			f.idA.TenantID, f.idA.UserID, summary); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO profile_claim_states(tenant_id,user_id)
		VALUES($1,$2)`, f.idA.TenantID, f.idA.UserID); err != nil {
		t.Fatal(err)
	}
	insertClaim := func(field, value string) {
		t.Helper()
		if value == "" {
			return
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO profile_claims
			    (tenant_id,user_id,field_name,claim_value,source_state)
			VALUES($1,$2,$3,$4,'source_unavailable')`,
			f.idA.TenantID, f.idA.UserID, field, value); err != nil {
			t.Fatal(err)
		}
	}
	insertClaim("industry", industry)
	for _, value := range splitSummaryClaims(summary) {
		insertClaim("summary", value)
	}
	for _, value := range tags {
		insertClaim("tag", value)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

func createApprovedTaskForScope(
	t *testing.T,
	st *Store,
	tenantID int64,
	userID int64,
	taskID string,
	urlPrefix string,
) int64 {
	t.Helper()
	ctx := t.Context()
	url := fmt.Sprintf("%s/%s", urlPrefix, uuid.NewString())
	config := json.RawMessage(`{"query":"tenant-b"}`)
	var sourceID int64
	if err := st.pool.QueryRow(ctx,
		`INSERT INTO sources (platform, capability, url, title, config, status)
		 VALUES ('web', 'search', $1, 'tenant B source', $2, 'active')
		 RETURNING id`, url, config).Scan(&sourceID); err != nil {
		t.Fatalf("create tenant B source: %v", err)
	}
	plan, err := json.Marshal(compiledFetchPlan{Sources: []compiledPlanSource{{
		Platform: "web", Capability: "search", Title: "tenant B source",
		URL: url, Config: config,
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(ctx,
		`INSERT INTO schedules
		    (id, tenant_id, user_id, nl_description, spec_json, scope_json,
		     status, push_strictness)
		 VALUES ($1, $2, $3, 'tenant B task', '{}', '{}', 'active', 'normal')`,
		taskID, tenantID, userID); err != nil {
		t.Fatalf("create tenant B schedule: %v", err)
	}
	if _, err := st.pool.Exec(ctx,
		`INSERT INTO schedule_playbooks (schedule_id, content, fetch_plan)
		 VALUES ($1, 'tenant B playbook', $2)`, taskID, plan); err != nil {
		t.Fatalf("create tenant B playbook: %v", err)
	}
	if _, err := st.pool.Exec(ctx,
		`INSERT INTO schedule_sources (schedule_id, source_id) VALUES ($1, $2)`,
		taskID, sourceID); err != nil {
		t.Fatalf("create tenant B source link: %v", err)
	}
	return sourceID
}

func (f *compiledRunWriteFixture) createContent(t *testing.T, sourceID int64, suffix string) int64 {
	t.Helper()
	key := "https://compiled-write.test/" + suffix + "/" + uuid.NewString()
	id, _, err := f.base.st.UpsertContentItem(t.Context(), &types.ContentItem{
		SourceID: sourceID, ExternalID: uuid.NewString(), CanonicalKey: key,
		URL: key, Title: suffix, ContentHash: uuid.NewString(),
	})
	if err != nil {
		t.Fatalf("create content %s: %v", suffix, err)
	}
	f.content = append(f.content, id)
	return id
}

func TestCompiledRunWrites_ExactTenantRevocationAndDurableReceipt(t *testing.T) {
	f := newCompiledRunWriteFixture(t)
	ctx := t.Context()
	sharedKey := "compiled-shared-" + uuid.NewString()

	batchA, recoveryOnly, err := f.base.st.CreateOrRecoverPushBatchForTaskRunV1(
		ctx, f.idA, f.refA, sharedKey)
	if err != nil {
		t.Fatalf("create tenant A batch: %v", err)
	}
	if recoveryOnly {
		t.Fatal("live tenant A batch unexpectedly entered receipt-only recovery")
	}
	var tenantID, userID, snapshotID int64
	var scheduleID, physicalKey string
	if err := f.base.st.pool.QueryRow(ctx,
		`SELECT tenant_id, user_id, schedule_id, run_snapshot_id, idempotency_key
		   FROM push_batches WHERE id = $1`, batchA,
	).Scan(&tenantID, &userID, &scheduleID, &snapshotID, &physicalKey); err != nil {
		t.Fatal(err)
	}
	if tenantID != f.idA.TenantID || userID != f.idA.UserID ||
		scheduleID != f.idA.TaskID || snapshotID != f.refA.SnapshotID ||
		physicalKey != compiledPushBatchPhysicalKeyV1(f.refA.SnapshotID, sharedKey) {
		t.Fatalf("tenant A batch scope = (%d,%d,%q,%d,%q), want (%d,%d,%q,%d,%q)",
			tenantID, userID, scheduleID, snapshotID, physicalKey,
			f.idA.TenantID, f.idA.UserID, f.idA.TaskID, f.refA.SnapshotID,
			compiledPushBatchPhysicalKeyV1(f.refA.SnapshotID, sharedKey))
	}
	summaries, err := f.base.st.ListPushBatchSummaries(
		ctx, f.idA.UserID, time.Now().Add(-time.Hour), 100)
	if err != nil {
		t.Fatalf("list compiled batch summaries: %v", err)
	}
	var observedLogicalKey string
	for _, summary := range summaries {
		if summary.ID == batchA {
			observedLogicalKey = summary.IdempotencyKey
			break
		}
	}
	if observedLogicalKey != sharedKey {
		t.Fatalf("compiled summary idempotency key = %q, want logical trace %q",
			observedLogicalKey, sharedKey)
	}

	batchB, err := f.base.st.CreatePushBatchForTaskRunV1(
		ctx, f.idB, f.refB, sharedKey)
	if err != nil {
		t.Fatalf("tenant B same raw trace should get its own snapshot batch: %v", err)
	}
	if batchB == batchA {
		t.Fatalf("different snapshots adopted the same batch id %d", batchA)
	}
	var tenantBBatches, tenantBSnapshot int64
	if err := f.base.st.pool.QueryRow(ctx,
		`SELECT count(*), max(run_snapshot_id) FROM push_batches
		  WHERE tenant_id = $1 AND idempotency_key = $2`, f.tenantB,
		compiledPushBatchPhysicalKeyV1(f.refB.SnapshotID, sharedKey),
	).Scan(&tenantBBatches, &tenantBSnapshot); err != nil {
		t.Fatal(err)
	}
	if tenantBBatches != 1 || tenantBSnapshot != f.refB.SnapshotID {
		t.Fatalf("tenant B snapshot batches=%d snapshot=%d, want 1/%d",
			tenantBBatches, tenantBSnapshot, f.refB.SnapshotID)
	}

	contentA := f.createContent(t, f.sourceA, "tenant-a-delivery")
	deliveryID, existed, sentAlready, err := f.base.st.InsertDeliveryForTaskRunV1(
		ctx, f.idA, f.refA, sharedKey, &types.Delivery{
			BatchID: batchA, UserID: f.idA.UserID, ContentItemID: &contentA,
			Score: 88, BodyMD: "tenant A body",
		})
	if err != nil || existed || sentAlready {
		t.Fatalf("insert tenant A delivery = id:%d existed:%v sent:%v err:%v",
			deliveryID, existed, sentAlready, err)
	}

	// Prepare a second batch with one sent and one pending delivery. After
	// revocation it must not qualify for receipt-only recovery: rebuilding or
	// resending the pending entry requires live authority.
	mixedKey := "compiled-mixed-recovery-" + uuid.NewString()
	mixedBatch, err := f.base.st.CreatePushBatchForTaskRunV1(
		ctx, f.idA, f.refA, mixedKey)
	if err != nil {
		t.Fatalf("create mixed recovery batch: %v", err)
	}
	mixedSentContent := f.createContent(t, f.sourceA, "mixed-sent")
	mixedSentDelivery, existed, sentAlready, err := f.base.st.InsertDeliveryForTaskRunV1(
		ctx, f.idA, f.refA, mixedKey, &types.Delivery{
			BatchID: mixedBatch, UserID: f.idA.UserID,
			ContentItemID: &mixedSentContent, BodyMD: "already sent",
		})
	if err != nil || existed || sentAlready {
		t.Fatalf("insert mixed sent delivery = id:%d existed:%v sent:%v err:%v",
			mixedSentDelivery, existed, sentAlready, err)
	}
	mixedPendingContent := f.createContent(t, f.sourceA, "mixed-pending")
	if _, existed, sentAlready, err := f.base.st.InsertDeliveryForTaskRunV1(
		ctx, f.idA, f.refA, mixedKey, &types.Delivery{
			BatchID: mixedBatch, UserID: f.idA.UserID,
			ContentItemID: &mixedPendingContent, BodyMD: "still pending",
		}); err != nil || existed || sentAlready {
		t.Fatalf("insert mixed pending delivery = existed:%v sent:%v err:%v",
			existed, sentAlready, err)
	}
	emptyReceiptKey := "compiled-empty-receipt-" + uuid.NewString()
	emptyReceiptBatch, err := f.base.st.CreatePushBatchForTaskRunV1(
		ctx, f.idA, f.refA, emptyReceiptKey)
	if err != nil {
		t.Fatalf("create delivery-free receipt batch: %v", err)
	}
	for _, batchID := range []int64{batchA, mixedBatch, emptyReceiptBatch} {
		winner, claimErr := f.base.st.ClaimPushBatchDeliveryAuthority(
			ctx,
			types.PushBatchScope{
				TenantID: f.idA.TenantID,
				UserID:   f.idA.UserID,
				BatchID:  batchID,
			},
			types.PushBatchDeliveryAuthorityLegacy,
		)
		if claimErr != nil || winner != types.PushBatchDeliveryAuthorityLegacy {
			t.Fatalf("claim batch %d legacy authority = %q err=%v",
				batchID, winner, claimErr)
		}
	}

	f.createClaimProfile(t, "", "before", []string{"old"})
	profile, err := f.base.st.GetProfile(ctx, f.idA.UserID)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.base.st.EvolveProfileForTaskRunV1(
		ctx, f.idB, f.refB, "must not cross", []string{"bad"}, 1,
		profile.UpdatedAt, profile.LastEvolvedFeedbackID,
	); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("tenant B profile write error = %v, want conflict", err)
	}
	unchanged, err := f.base.st.GetProfile(ctx, f.idA.UserID)
	if err != nil || unchanged.Summary != "before" || unchanged.LastEvolvedFeedbackID != 0 {
		t.Fatalf("tenant B changed tenant A profile: profile=%+v err=%v", unchanged, err)
	}

	// Revoke only tenant A. The same user remains an active member of tenant B,
	// which reproduces the exact shape that made user->tenant derivation unsafe.
	if _, err := f.base.st.pool.Exec(ctx,
		`DELETE FROM memberships WHERE tenant_id = $1 AND user_id = $2`,
		f.idA.TenantID, f.idA.UserID); err != nil {
		t.Fatal(err)
	}

	emptyKey := "compiled-empty-revoked-" + uuid.NewString()
	if _, _, err := f.base.st.RecordEmptyPushBatchForTaskRunV1(
		ctx, f.idA, f.refA, emptyKey, types.BatchExitGateFetch,
		types.PipelineCounts{}.WithFetched(0),
	); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("revoked tenant A empty write error = %v, want not found", err)
	}
	var emptyRows int
	if err := f.base.st.pool.QueryRow(ctx,
		`SELECT count(*) FROM push_batches WHERE idempotency_key = $1`,
		compiledPushBatchPhysicalKeyV1(f.refA.SnapshotID, emptyKey),
	).Scan(&emptyRows); err != nil {
		t.Fatal(err)
	}
	if emptyRows != 0 {
		t.Fatalf("revoked tenant A wrote %d empty batches", emptyRows)
	}
	createAfterRevokeKey := "compiled-create-revoked-" + uuid.NewString()
	if _, err := f.base.st.CreatePushBatchForTaskRunV1(
		ctx, f.idA, f.refA, createAfterRevokeKey,
	); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("revoked tenant A batch write error = %v, want not found", err)
	}
	if err := f.base.st.pool.QueryRow(ctx,
		`SELECT count(*) FROM push_batches
		  WHERE idempotency_key IN ($1, $2)`,
		compiledPushBatchPhysicalKeyV1(f.refA.SnapshotID, emptyKey),
		compiledPushBatchPhysicalKeyV1(f.refA.SnapshotID, createAfterRevokeKey),
	).Scan(&emptyRows); err != nil {
		t.Fatal(err)
	}
	if emptyRows != 0 {
		t.Fatalf("revoked tenant A left %d pre-write batch rows", emptyRows)
	}

	contentAfterRevoke := f.createContent(t, f.sourceA, "after-revoke")
	if _, _, _, err := f.base.st.InsertDeliveryForTaskRunV1(
		ctx, f.idA, f.refA, sharedKey, &types.Delivery{
			BatchID: batchA, UserID: f.idA.UserID,
			ContentItemID: &contentAfterRevoke, BodyMD: "must not write",
		}); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("revoked tenant A delivery write error = %v, want not found", err)
	}
	var revokedDeliveries int
	if err := f.base.st.pool.QueryRow(ctx,
		`SELECT count(*) FROM deliveries WHERE content_item_id = $1`, contentAfterRevoke,
	).Scan(&revokedDeliveries); err != nil {
		t.Fatal(err)
	}
	if revokedDeliveries != 0 {
		t.Fatalf("revoked tenant A wrote %d deliveries", revokedDeliveries)
	}

	if err := f.base.st.AdvanceProfileCursorForTaskRunV1(
		ctx, f.idA, f.refA, 1, profile.UpdatedAt, profile.LastEvolvedFeedbackID,
	); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("revoked tenant A profile write error = %v, want not found", err)
	}
	if err := f.base.st.UpdatePushBatchStatusForTaskRunV1(
		ctx, f.idA, f.refA, sharedKey, batchA, types.BatchStatusFailed,
	); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("revoked tenant A live batch status error = %v, want not found", err)
	}
	if err := f.base.st.MarkPushBatchDoneReceiptV1(
		ctx, f.idA, f.refA, emptyReceiptKey, emptyReceiptBatch,
	); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("delivery-free batch receipt error = %v, want not found", err)
	}

	sentAt := time.Now().UTC()
	if err := f.base.st.MarkDeliverySentForTaskRunV1(
		ctx, f.idA, f.refA, mixedKey, mixedBatch, mixedSentDelivery,
		"om-mixed", json.RawMessage(`{"sent":true}`), sentAt,
	); err != nil {
		t.Fatalf("post-revoke mixed delivery receipt: %v", err)
	}
	if _, recoveryOnly, err := f.base.st.CreateOrRecoverPushBatchForTaskRunV1(
		ctx, f.idA, f.refA, mixedKey,
	); !errors.Is(err, types.ErrNotFound) || recoveryOnly {
		t.Fatalf("mixed sent/pending recovery = recovery:%v err:%v, want not found",
			recoveryOnly, err)
	}
	if err := f.base.st.MarkPushBatchDoneReceiptV1(
		ctx, f.idA, f.refA, mixedKey, mixedBatch,
	); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("mixed sent/pending batch receipt error = %v, want not found", err)
	}
	var mixedStatus, emptyReceiptStatus string
	if err := f.base.st.pool.QueryRow(ctx,
		`SELECT m.status, e.status
		   FROM push_batches m, push_batches e
		  WHERE m.id = $1 AND e.id = $2`, mixedBatch, emptyReceiptBatch,
	).Scan(&mixedStatus, &emptyReceiptStatus); err != nil {
		t.Fatal(err)
	}
	if mixedStatus != string(types.BatchStatusPending) ||
		emptyReceiptStatus != string(types.BatchStatusPending) {
		t.Fatalf("unproven receipts changed batch status: mixed=%q empty=%q",
			mixedStatus, emptyReceiptStatus)
	}

	if err := f.base.st.MarkDeliverySentForTaskRunV1(
		ctx, f.idA, f.refA, sharedKey, batchA, deliveryID,
		"om-tenant-a", json.RawMessage(`{"sent":true}`), sentAt,
	); err != nil {
		t.Fatalf("post-revoke tenant A durable delivery receipt: %v", err)
	}
	recoveredBatch, recoveryOnly, err := f.base.st.CreateOrRecoverPushBatchForTaskRunV1(
		ctx, f.idA, f.refA, sharedKey)
	if err != nil || !recoveryOnly || recoveredBatch != batchA {
		t.Fatalf("post-revoke batch recovery = id:%d recovery:%v err:%v, want %d/true/nil",
			recoveredBatch, recoveryOnly, err, batchA)
	}
	if err := f.base.st.MarkPushBatchDoneReceiptV1(
		ctx, f.idA, f.refA, sharedKey, batchA); err != nil {
		t.Fatalf("post-revoke tenant A durable batch receipt: %v", err)
	}

	if err := f.base.st.MarkDeliverySentForTaskRunV1(
		ctx, f.idB, f.refB, sharedKey, batchA, deliveryID,
		"om-tenant-b", json.RawMessage(`{"wrong":true}`), sentAt.Add(time.Second),
	); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("tenant B receipt against tenant A row error = %v, want not found", err)
	}
	var gotTenant int64
	var gotMessage, gotDeliveryStatus, gotBatchStatus string
	if err := f.base.st.pool.QueryRow(ctx,
		`SELECT d.tenant_id, d.feishu_message_id, d.status, b.status
		   FROM deliveries d JOIN push_batches b ON b.id = d.batch_id
		  WHERE d.id = $1`, deliveryID,
	).Scan(&gotTenant, &gotMessage, &gotDeliveryStatus, &gotBatchStatus); err != nil {
		t.Fatal(err)
	}
	if gotTenant != f.idA.TenantID || gotMessage != "om-tenant-a" ||
		gotDeliveryStatus != string(types.DeliveryStatusSent) ||
		gotBatchStatus != string(types.BatchStatusDone) {
		t.Fatalf("tenant A receipt drifted: tenant=%d message=%q delivery=%q batch=%q",
			gotTenant, gotMessage, gotDeliveryStatus, gotBatchStatus)
	}
}

func TestCompiledRunWrites_SameTraceDifferentRunSnapshotsStayIsolated(t *testing.T) {
	f := newCompiledRunWriteFixture(t)
	ctx := t.Context()
	resetIdentity := f.idA
	resetIdentity.TemporalRunID = "run-write-a-reset-" + uuid.NewString()
	resetRef, err := f.base.st.CreateOrGetCompiledTaskRunSnapshotV1(ctx,
		CreateOrGetCompiledTaskRunSnapshotV1Params{
			Identity: resetIdentity,
			Policy:   testCompiledRunPolicyV1(t),
		})
	if err != nil {
		t.Fatalf("create reset run snapshot: %v", err)
	}

	traceID := "compiled-reset-trace-" + uuid.NewString()
	firstBatch, err := f.base.st.CreatePushBatchForTaskRunV1(
		ctx, f.idA, f.refA, traceID)
	if err != nil {
		t.Fatalf("create original run batch: %v", err)
	}
	firstRetry, err := f.base.st.CreatePushBatchForTaskRunV1(
		ctx, f.idA, f.refA, traceID)
	if err != nil || firstRetry != firstBatch {
		t.Fatalf("original run retry = id:%d err:%v, want %d/nil",
			firstRetry, err, firstBatch)
	}
	resetBatch, err := f.base.st.CreatePushBatchForTaskRunV1(
		ctx, resetIdentity, resetRef, traceID)
	if err != nil {
		t.Fatalf("create reset run batch with replayed trace: %v", err)
	}
	if firstBatch == resetBatch {
		t.Fatalf("reset run adopted original batch %d", firstBatch)
	}
	resetRetry, err := f.base.st.CreatePushBatchForTaskRunV1(
		ctx, resetIdentity, resetRef, traceID)
	if err != nil || resetRetry != resetBatch {
		t.Fatalf("reset run retry = id:%d err:%v, want %d/nil",
			resetRetry, err, resetBatch)
	}
	var rows, distinctSnapshots int
	if err := f.base.st.pool.QueryRow(ctx,
		`SELECT count(*), count(DISTINCT run_snapshot_id)
		   FROM push_batches
		  WHERE (idempotency_key = $1 AND run_snapshot_id = $2)
		     OR (idempotency_key = $3 AND run_snapshot_id = $4)`,
		compiledPushBatchPhysicalKeyV1(f.refA.SnapshotID, traceID), f.refA.SnapshotID,
		compiledPushBatchPhysicalKeyV1(resetRef.SnapshotID, traceID), resetRef.SnapshotID,
	).Scan(&rows, &distinctSnapshots); err != nil {
		t.Fatal(err)
	}
	if rows != 2 || distinctSnapshots != 2 {
		t.Fatalf("replayed trace rows=%d distinct snapshots=%d, want 2/2",
			rows, distinctSnapshots)
	}

	contentID := f.createContent(t, f.sourceA, "reset-isolation")
	firstDelivery, existed, sentAlready, err := f.base.st.InsertDeliveryForTaskRunV1(
		ctx, f.idA, f.refA, traceID, &types.Delivery{
			BatchID: firstBatch, UserID: f.idA.UserID,
			ContentItemID: &contentID, BodyMD: "original run",
		})
	if err != nil || existed || sentAlready {
		t.Fatalf("insert original delivery = id:%d existed:%v sent:%v err:%v",
			firstDelivery, existed, sentAlready, err)
	}
	if _, _, _, err := f.base.st.InsertDeliveryForTaskRunV1(
		ctx, resetIdentity, resetRef, traceID, &types.Delivery{
			BatchID: firstBatch, UserID: resetIdentity.UserID,
			ContentItemID: &contentID, BodyMD: "must not adopt",
		}); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("reset run write through original batch error = %v, want not found", err)
	}
	resetDelivery, existed, sentAlready, err := f.base.st.InsertDeliveryForTaskRunV1(
		ctx, resetIdentity, resetRef, traceID, &types.Delivery{
			BatchID: resetBatch, UserID: resetIdentity.UserID,
			ContentItemID: &contentID, BodyMD: "reset run",
		})
	if err != nil || existed || sentAlready || resetDelivery == firstDelivery {
		t.Fatalf("insert reset delivery = id:%d existed:%v sent:%v err:%v",
			resetDelivery, existed, sentAlready, err)
	}
	if err := f.base.st.MarkDeliverySentForTaskRunV1(
		ctx, resetIdentity, resetRef, traceID, firstBatch, firstDelivery,
		"om-wrong-run", json.RawMessage(`{"wrong":true}`), time.Now().UTC(),
	); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("reset run receipt through original batch error = %v, want not found", err)
	}
	var firstAuthority *string
	if err := f.base.st.pool.QueryRow(ctx,
		`SELECT delivery_authority FROM push_batches WHERE id=$1`,
		firstBatch,
	).Scan(&firstAuthority); err != nil {
		t.Fatal(err)
	}
	if firstAuthority != nil {
		t.Fatalf("wrong-snapshot receipt claimed authority %q", *firstAuthority)
	}
}

func TestCompiledRunWrites_RejectMutatedReferenceBeforeWrite(t *testing.T) {
	f := newCompiledRunWriteFixture(t)
	mutated := f.refA
	mutated.ReferenceDigest = f.refB.ReferenceDigest
	key := "compiled-mutated-ref-" + uuid.NewString()
	if _, err := f.base.st.CreatePushBatchForTaskRunV1(
		t.Context(), f.idA, mutated, key); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("mutated reference error = %v, want validation", err)
	}
	var rows int
	if err := f.base.st.pool.QueryRow(t.Context(),
		`SELECT count(*) FROM push_batches WHERE idempotency_key = $1`,
		compiledPushBatchPhysicalKeyV1(f.refA.SnapshotID, key),
	).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 0 {
		t.Fatalf("mutated reference wrote %d rows", rows)
	}
}
