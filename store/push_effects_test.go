package store

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"

	"github.com/YouToco/vane/pusheffect"
	"github.com/YouToco/vane/types"
)

type pushEffectFixture struct {
	store    *Store
	db       *sql.DB
	provider *goose.Provider
	prepared pusheffect.Prepared
}

func newPushEffectFixture(t *testing.T) pushEffectFixture {
	t.Helper()
	dbURL, db, provider := migration039Scratch(t)
	ctx := t.Context()
	var userID, snapshotID, batchID, deliveryA, deliveryB int64
	openID := "push-effect-" + uuid.NewString()
	if err := db.QueryRowContext(ctx, `
		INSERT INTO users (feishu_open_id,name)
		VALUES ($1,'push effect fixture') RETURNING id`, openID,
	).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO memberships (tenant_id,user_id,role)
		VALUES (1,$1,'owner')`, userID); err != nil {
		t.Fatal(err)
	}
	runID := uuid.NewString()
	taskID := "push-effect-task-" + uuid.NewString()
	digest := strings.Repeat("a", 64)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO schedules (
			id,tenant_id,user_id,nl_description,spec_json,scope_json,
			status,execution_mode
		) VALUES ($1,1,$2,'push effect fixture','{}'::jsonb,'{}'::jsonb,
		          'active','compiled')`,
		taskID, userID); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `
		INSERT INTO task_run_snapshots (
			tenant_id,user_id,task_id,temporal_workflow_id,temporal_run_id,
			run_kind,execution_mode,adaptive_version,
			capability_catalog_digest,tool_policy_digest,prompt_policy_digest,
			model_policy_digest,quota_policy_digest,definition_digest,plan_digest,
			payload_digest,reference_digest,reference_schema_version,payload,budget
		) VALUES (
			1,$1,$2,$3,$4,'scheduled','compiled',0,
			$5,$5,$5,$5,$5,$5,$5,$5,$5,'fixture/v1','{}','{}'::jsonb
		) RETURNING id`,
		userID, taskID, "workflow-"+runID, runID, digest,
	).Scan(&snapshotID); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `
		INSERT INTO push_batches (
			tenant_id,user_id,status,idempotency_key,run_snapshot_id
		) VALUES (1,$1,'pending',$2,$3) RETURNING id`,
		userID, "batch-"+runID, snapshotID,
	).Scan(&batchID); err != nil {
		t.Fatal(err)
	}
	for _, destination := range []*int64{&deliveryA, &deliveryB} {
		if err := db.QueryRowContext(ctx, `
			INSERT INTO deliveries (
				tenant_id,batch_id,user_id,score,card_json,status
			) VALUES (1,$1,$2,80,'{}'::jsonb,'pending') RETURNING id`,
			batchID, userID,
		).Scan(destination); err != nil {
			t.Fatal(err)
		}
	}
	st, err := New(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	return pushEffectFixture{
		store: st, db: db, provider: provider,
		prepared: pusheffect.Prepared{
			ID: "effect-" + uuid.NewString(), TenantID: 1, UserID: userID,
			TaskID: taskID, RunSnapshotID: snapshotID, RunID: runID,
			StepID: "push", ChunkIndex: 0, ChunkCount: 1,
			BatchID: batchID, DeliveryIDs: []int64{deliveryA, deliveryB},
			Provider: "feishu", AppIdentity: "app-fingerprint",
			ProviderChatID: "oc_owner_p2p", Target: "ou_target",
			Card:         []byte(`{"card":"frozen"}`),
			ProviderUUID: uuid.NewString(),
			IdempotencyExpiresAt: time.Now().UTC().
				Truncate(time.Microsecond).Add(time.Hour),
		},
	}
}

func TestMigration039RefusesDurablePushEffectDowngrade(t *testing.T) {
	f := newPushEffectFixture(t)
	if _, err := f.store.CreatePushEffect(t.Context(), f.prepared); err != nil {
		t.Fatal(err)
	}
	if _, err := f.provider.Down(t.Context()); err == nil ||
		!strings.Contains(err.Error(), "refusing downgrade") {
		t.Fatalf("039 downgrade accepted durable effect: %v", err)
	}
	var version, effects int
	if err := f.db.QueryRowContext(t.Context(), `
		SELECT
		  (SELECT COALESCE(max(version_id),0)
		     FROM goose_db_version WHERE is_applied),
		  (SELECT count(*) FROM push_effects)`,
	).Scan(&version, &effects); err != nil {
		t.Fatal(err)
	}
	if version != 39 || effects != 1 {
		t.Fatalf("failed down version/effects=%d/%d", version, effects)
	}
}

func TestMigration039DownSerializesConcurrentPushEffectInsert(t *testing.T) {
	f := newPushEffectFixture(t)
	canonical, err := pusheffect.Canonicalize(f.prepared)
	if err != nil {
		t.Fatal(err)
	}
	insertTx, err := f.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	insertDone := false
	defer func() {
		if !insertDone {
			_ = insertTx.Rollback()
		}
	}()
	if _, err := insertTx.ExecContext(t.Context(), `
		INSERT INTO push_effects (
			id,tenant_id,user_id,task_id,run_snapshot_id,run_id,step_id,
			chunk_index,chunk_count,batch_id,delivery_ids,provider,app_identity,
			provider_chat_id,target,card_payload,card_digest,provider_uuid,
			idempotency_expires_at,schema_version,canonical_payload,payload_digest
		) VALUES (
			$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,
			$17,$18::uuid,$19,$20,$21,$22
		)`,
		f.prepared.ID, f.prepared.TenantID, f.prepared.UserID,
		f.prepared.TaskID, f.prepared.RunSnapshotID, f.prepared.RunID,
		f.prepared.StepID, f.prepared.ChunkIndex, f.prepared.ChunkCount,
		f.prepared.BatchID, f.prepared.DeliveryIDs, f.prepared.Provider,
		f.prepared.AppIdentity, f.prepared.ProviderChatID, f.prepared.Target,
		f.prepared.Card, canonical.CardDigest(), f.prepared.ProviderUUID,
		f.prepared.IdempotencyExpiresAt,
		pusheffect.SchemaVersion, canonical.Payload(), canonical.Digest(),
	); err != nil {
		t.Fatal(err)
	}
	downDone := make(chan error, 1)
	go func() {
		_, downErr := f.provider.Down(t.Context())
		downDone <- downErr
	}()
	if !waitForMigration039DowngradeFence(
		t.Context(), f.db, 5*time.Second) {
		t.Fatal("039 Down did not wait at its pre-check lock")
	}
	if err := insertTx.Commit(); err != nil {
		t.Fatal(err)
	}
	insertDone = true
	select {
	case downErr := <-downDone:
		if downErr == nil || !strings.Contains(downErr.Error(), "refusing downgrade") {
			t.Fatalf("039 Down accepted concurrent insert: %v", downErr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("039 Down did not converge")
	}
}

func waitForMigration039DowngradeFence(
	ctx context.Context,
	db *sql.DB,
	timeout time.Duration,
) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var waiting bool
		err := db.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM pg_stat_activity
				 WHERE datname=current_database()
				   AND pid<>pg_backend_pid()
				   AND wait_event_type='Lock'
				   AND query LIKE '%migration 039 downgrade fence%'
			)`,
		).Scan(&waiting)
		if err == nil && waiting {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

func TestPushEffectCreateClaimFailureAndReceiptConverge(t *testing.T) {
	f := newPushEffectFixture(t)
	ctx := t.Context()
	created, err := f.store.CreatePushEffect(ctx, f.prepared)
	if err != nil {
		t.Fatal(err)
	}
	started, err := f.store.PushEffectBatchStarted(
		ctx, f.prepared.TenantID, f.prepared.UserID, f.prepared.BatchID)
	if err != nil || !started {
		t.Fatalf("created batch latch started=%v err=%v", started, err)
	}
	for _, scope := range []struct {
		tenantID int64
		userID   int64
		batchID  int64
	}{
		{f.prepared.TenantID + 1, f.prepared.UserID, f.prepared.BatchID},
		{f.prepared.TenantID, f.prepared.UserID + 1, f.prepared.BatchID},
		{f.prepared.TenantID, f.prepared.UserID, f.prepared.BatchID + 1},
	} {
		started, err := f.store.PushEffectBatchStarted(
			ctx, scope.tenantID, scope.userID, scope.batchID)
		if err != nil || started {
			t.Fatalf("foreign batch latch scope=%+v started=%v err=%v",
				scope, started, err)
		}
	}
	tenantIDs, err := f.store.ListRecoverablePushEffectTenantIDs(
		ctx, time.Now().Add(time.Minute), 0, 10)
	if err != nil || len(tenantIDs) != 1 || tenantIDs[0] != f.prepared.TenantID {
		t.Fatalf("recoverable tenant shards=%v err=%v", tenantIDs, err)
	}
	recoverable, err := f.store.ListRecoverablePushEffects(
		ctx, f.prepared.TenantID, time.Now().Add(time.Minute), 10)
	if err != nil || len(recoverable) != 1 ||
		recoverable[0].ID != f.prepared.ID ||
		recoverable[0].Status != pusheffect.StatusPrepared {
		t.Fatalf("recoverable effects=%+v err=%v", recoverable, err)
	}
	replayed, err := f.store.CreatePushEffect(ctx, f.prepared)
	if err != nil || replayed.ID != created.ID ||
		replayed.PayloadDigest != created.PayloadDigest {
		t.Fatalf("create response-lost replay=%+v err=%v", replayed, err)
	}
	drifted := f.prepared
	drifted.Card = []byte(`{"card":"drift"}`)
	if _, err := f.store.CreatePushEffect(ctx, drifted); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("digest drift error=%v, want conflict", err)
	}
	rebound := f.prepared
	rebound.ID = "effect-" + uuid.NewString()
	rebound.StepID = "push-rebound"
	if _, err := f.store.CreatePushEffect(ctx, rebound); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("provider uuid rebind error=%v, want conflict", err)
	}

	first, err := f.store.ClaimPushEffect(ctx, pusheffect.ClaimParams{
		Scope: f.prepared.Scope(), LeaseOwner: "worker-one",
		LeaseDuration: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	replayedClaim, err := f.store.ClaimPushEffect(ctx, pusheffect.ClaimParams{
		Scope: f.prepared.Scope(), LeaseOwner: "worker-one",
		LeaseDuration: time.Minute,
	})
	if err != nil || replayedClaim.Fence != first.Fence ||
		replayedClaim.Attempt != first.Attempt {
		t.Fatalf("claim replay=%+v err=%v", replayedClaim, err)
	}
	if _, err := f.store.ClaimPushEffect(ctx, pusheffect.ClaimParams{
		Scope: f.prepared.Scope(), LeaseOwner: "worker-two",
		LeaseDuration: time.Minute,
	}); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("concurrent claim error=%v", err)
	}
	firstLease := pusheffect.Lease{
		Scope: f.prepared.Scope(), LeaseOwner: first.LeaseOwner, Fence: first.Fence,
	}
	if err := f.store.RecordPushEffectDefiniteFailure(ctx,
		pusheffect.FailureParams{
			Lease: firstLease, Class: "client_disconnected",
			RetryAfter: time.Second,
		}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.pool.Exec(ctx, `
		UPDATE push_effects SET next_attempt_at=clock_timestamp()-interval '1 second'
		 WHERE id=$1`, f.prepared.ID); err != nil {
		t.Fatal(err)
	}
	second, err := f.store.ClaimPushEffect(ctx, pusheffect.ClaimParams{
		Scope: f.prepared.Scope(), LeaseOwner: "worker-two",
		LeaseDuration: time.Minute,
	})
	if err != nil || second.Fence != first.Fence+1 ||
		second.Attempt != first.Attempt+1 {
		t.Fatalf("retry claim=%+v err=%v", second, err)
	}
	if err := f.store.RecordPushEffectAmbiguous(ctx,
		pusheffect.FailureParams{
			Lease: firstLease, Class: "response_lost",
		}); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("old fence error=%v, want conflict", err)
	}
	secondLease := pusheffect.Lease{
		Scope: f.prepared.Scope(), LeaseOwner: second.LeaseOwner, Fence: second.Fence,
	}
	if err := f.store.RecordPushEffectAmbiguous(ctx,
		pusheffect.FailureParams{
			Lease: secondLease, Class: "response_lost",
		}); err != nil {
		t.Fatal(err)
	}
	if err := f.store.RecordPushEffectSent(ctx, pusheffect.SentReceipt{
		Scope: f.prepared.Scope(), ExpectedFence: second.Fence,
		ProviderMessageID: "om_message",
	}); err != nil {
		t.Fatal(err)
	}
	if err := f.store.RecordPushEffectSent(ctx, pusheffect.SentReceipt{
		Scope: f.prepared.Scope(), ExpectedFence: second.Fence,
		ProviderMessageID: "om_message",
	}); err != nil {
		t.Fatalf("sent response-lost replay: %v", err)
	}
	final, err := f.store.LoadPushEffect(ctx, f.prepared.Scope())
	if err != nil || final.Status != pusheffect.StatusSent ||
		final.ProviderMessageID != "om_message" {
		t.Fatalf("final=%+v err=%v", final, err)
	}
}

func TestPushEffectStaleSendingBecomesAmbiguousWithoutSendLease(t *testing.T) {
	f := newPushEffectFixture(t)
	ctx := t.Context()
	if _, err := f.store.CreatePushEffect(ctx, f.prepared); err != nil {
		t.Fatal(err)
	}
	claimed, err := f.store.ClaimPushEffect(ctx, pusheffect.ClaimParams{
		Scope: f.prepared.Scope(), LeaseOwner: "dead-worker",
		LeaseDuration: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.pool.Exec(ctx, `
		UPDATE push_effects
		   SET lease_until=clock_timestamp()-interval '2 minutes',
		       takeover_not_before=clock_timestamp()-interval '1 minute'
		 WHERE id=$1`, f.prepared.ID); err != nil {
		t.Fatal(err)
	}
	type takeoverResult struct {
		effect *pusheffect.Effect
		err    error
	}
	start := make(chan struct{})
	results := make(chan takeoverResult, 2)
	for range 2 {
		go func() {
			<-start
			effect, err := f.store.TakeOverStalePushEffect(
				context.Background(), f.prepared.Scope())
			results <- takeoverResult{effect: effect, err: err}
		}()
	}
	close(start)
	var taken *pusheffect.Effect
	for range 2 {
		result := <-results
		if result.err != nil {
			t.Fatalf("concurrent takeover: %v", result.err)
		}
		if taken == nil {
			taken = result.effect
			continue
		}
		if result.effect.Status != taken.Status ||
			result.effect.Fence != taken.Fence ||
			result.effect.Attempt != taken.Attempt {
			t.Fatalf("takeover replay drifted: first=%+v second=%+v",
				taken, result.effect)
		}
	}
	if taken.Status != pusheffect.StatusAmbiguous ||
		taken.LeaseOwner != "" || taken.LeaseUntil != nil ||
		taken.Fence != claimed.Fence+1 {
		t.Fatalf("unsafe takeover result=%+v", taken)
	}
	tenantIDs, err := f.store.ListRecoverablePushEffectTenantIDs(
		ctx, time.Now().Add(time.Minute), 0, 10)
	if err != nil || len(tenantIDs) != 1 ||
		tenantIDs[0] != f.prepared.TenantID {
		t.Fatalf("ambiguous recoverable tenant shards=%v err=%v", tenantIDs, err)
	}
	recoverable, err := f.store.ListRecoverablePushEffects(
		ctx, f.prepared.TenantID, time.Now().Add(time.Minute), 10)
	if err != nil || len(recoverable) != 1 ||
		recoverable[0].ID != f.prepared.ID ||
		recoverable[0].Status != pusheffect.StatusAmbiguous {
		t.Fatalf("ambiguous recoverable effects=%+v err=%v", recoverable, err)
	}
	if _, err := f.store.ClaimPushEffect(ctx, pusheffect.ClaimParams{
		Scope: f.prepared.Scope(), LeaseOwner: "new-worker",
		LeaseDuration: time.Minute,
	}); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("ambiguous effect became sendable: %v", err)
	}
	if err := f.store.RecordPushEffectSent(ctx, pusheffect.SentReceipt{
		Scope: f.prepared.Scope(), ExpectedFence: claimed.Fence,
		LeaseOwner: claimed.LeaseOwner, ProviderMessageID: "late-old",
	}); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("old receipt fence error=%v, want conflict", err)
	}
	reconciliation, err := f.store.ClaimPushEffectReconciliation(
		ctx, pusheffect.ClaimParams{
			Scope: f.prepared.Scope(), LeaseOwner: "reconcile-worker",
			LeaseDuration: time.Minute,
		})
	if err != nil || reconciliation.Status != pusheffect.StatusSending ||
		reconciliation.Fence != taken.Fence+1 ||
		reconciliation.ProviderChatID != f.prepared.ProviderChatID {
		t.Fatalf("reconciliation claim=%+v err=%v", reconciliation, err)
	}
	reconciliationLease := pusheffect.Lease{
		Scope: f.prepared.Scope(), LeaseOwner: reconciliation.LeaseOwner,
		Fence: reconciliation.Fence,
	}
	if err := f.store.RecordPushEffectAmbiguous(ctx,
		pusheffect.FailureParams{
			Lease: reconciliationLease, Class: "history_miss_not_proof",
		}); err != nil {
		t.Fatal(err)
	}
	if err := f.store.BlockPushEffect(ctx, pusheffect.Resolution{
		Scope: f.prepared.Scope(), ExpectedFence: reconciliation.Fence,
		Class: "reconciliation_unavailable",
	}); err != nil {
		t.Fatal(err)
	}
	blocked, err := f.store.LoadPushEffect(ctx, f.prepared.Scope())
	if err != nil || blocked.Status != pusheffect.StatusBlocked ||
		blocked.BlockedAt == nil {
		t.Fatalf("blocked=%+v err=%v", blocked, err)
	}
}

func TestPushEffectReconciliationStopsAtProviderWindow(t *testing.T) {
	f := newPushEffectFixture(t)
	ctx := t.Context()
	f.prepared.IdempotencyExpiresAt = time.Now().UTC().
		Truncate(time.Microsecond).Add(2 * time.Second)
	if _, err := f.store.CreatePushEffect(ctx, f.prepared); err != nil {
		t.Fatal(err)
	}
	claimed, err := f.store.ClaimPushEffect(ctx, pusheffect.ClaimParams{
		Scope: f.prepared.Scope(), LeaseOwner: "window-worker",
		LeaseDuration: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.store.RecordPushEffectAmbiguous(ctx,
		pusheffect.FailureParams{
			Lease: pusheffect.Lease{
				Scope: f.prepared.Scope(), LeaseOwner: claimed.LeaseOwner,
				Fence: claimed.Fence,
			},
			Class: "history_miss_not_proof",
		}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.pool.Exec(ctx, `
		SELECT pg_sleep(
			GREATEST(0,EXTRACT(EPOCH FROM (
				idempotency_expires_at-clock_timestamp()
			)))+0.02
		) FROM push_effects WHERE id=$1`, f.prepared.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.ClaimPushEffectReconciliation(
		ctx, pusheffect.ClaimParams{
			Scope: f.prepared.Scope(), LeaseOwner: "late-reconcile-worker",
			LeaseDuration: time.Minute,
		}); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("expired provider window claim error=%v, want conflict", err)
	}
	current, err := f.store.LoadPushEffect(ctx, f.prepared.Scope())
	if err != nil || current.Status != pusheffect.StatusAmbiguous {
		t.Fatalf("expired reconciliation mutated effect=%+v err=%v", current, err)
	}
}

func TestPushEffectAmbiguousDeferralIsFencedAndDueByDatabaseClock(
	t *testing.T,
) {
	f := newPushEffectFixture(t)
	ctx := t.Context()
	if _, err := f.store.CreatePushEffect(ctx, f.prepared); err != nil {
		t.Fatal(err)
	}
	claimed, err := f.store.ClaimPushEffect(
		ctx,
		pusheffect.ClaimParams{
			Scope:         f.prepared.Scope(),
			LeaseOwner:    "defer-" + uuid.NewString(),
			LeaseDuration: time.Minute,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.store.RecordPushEffectAmbiguous(
		ctx,
		pusheffect.FailureParams{
			Lease: pusheffect.Lease{
				Scope:      claimed.Scope(),
				LeaseOwner: claimed.LeaseOwner,
				Fence:      claimed.Fence,
			},
			Class: "response_unknown",
		},
	); err != nil {
		t.Fatal(err)
	}

	resolution := pusheffect.Resolution{
		Scope:         claimed.Scope(),
		ExpectedFence: claimed.Fence,
		Class:         "provider_history_miss",
	}
	if err := f.store.DeferPushEffectReconciliation(
		ctx,
		resolution,
		5*time.Minute,
	); err != nil {
		t.Fatal(err)
	}
	recoverable, err := f.store.ListRecoverablePushEffects(
		ctx,
		f.prepared.TenantID,
		time.Now().Add(24*time.Hour),
		10,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(recoverable) != 0 {
		t.Fatalf("deferred ambiguous effects=%d, want 0", len(recoverable))
	}
	tenantIDs, err := f.store.ListRecoverablePushEffectTenantIDs(
		ctx,
		time.Now().Add(24*time.Hour),
		0,
		10,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(tenantIDs) != 0 {
		t.Fatalf("deferred ambiguous tenant shards=%v, want none", tenantIDs)
	}

	if err := f.store.DeferPushEffectReconciliation(
		ctx,
		pusheffect.Resolution{
			Scope:         claimed.Scope(),
			ExpectedFence: claimed.Fence + 1,
			Class:         "stale_fence",
		},
		time.Minute,
	); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("stale defer fence error=%v, want conflict", err)
	}

	if _, err := f.db.ExecContext(ctx, `
		UPDATE push_effects
		   SET next_attempt_at=clock_timestamp()-interval '1 second'
		 WHERE id=$1`,
		f.prepared.ID,
	); err != nil {
		t.Fatal(err)
	}
	recoverable, err = f.store.ListRecoverablePushEffects(
		ctx,
		f.prepared.TenantID,
		time.Now().Add(24*time.Hour),
		10,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(recoverable) != 1 ||
		recoverable[0].ID != f.prepared.ID {
		t.Fatalf("due ambiguous effects=%+v, want exact deferred effect", recoverable)
	}
	tenantIDs, err = f.store.ListRecoverablePushEffectTenantIDs(
		ctx,
		time.Now().Add(24*time.Hour),
		0,
		10,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(tenantIDs) != 1 ||
		tenantIDs[0] != f.prepared.TenantID {
		t.Fatalf("due ambiguous tenant shards=%v, want exact tenant", tenantIDs)
	}

	if err := f.store.DeferPushEffectReconciliationUntilExpiry(
		ctx,
		pusheffect.Resolution{
			Scope:         claimed.Scope(),
			ExpectedFence: claimed.Fence,
			Class:         "attempt_budget_exhausted",
		},
	); err != nil {
		t.Fatal(err)
	}
	current, err := f.store.LoadPushEffect(ctx, claimed.Scope())
	if err != nil {
		t.Fatal(err)
	}
	if !current.NextAttemptAt.Equal(current.IdempotencyExpiresAt) ||
		current.FailureClass != "attempt_budget_exhausted" {
		t.Fatalf(
			"expiry deferral next/class=%s/%q, want %s/attempt_budget_exhausted",
			current.NextAttemptAt,
			current.FailureClass,
			current.IdempotencyExpiresAt,
		)
	}
}

func TestPushEffectReconciliationScheduleUsesDatabaseClockAtExpiryBoundary(
	t *testing.T,
) {
	f := newPushEffectFixture(t)
	ctx := t.Context()
	f.prepared.IdempotencyExpiresAt = time.Now().UTC().
		Truncate(time.Microsecond).Add(2 * time.Second)
	if _, err := f.store.CreatePushEffect(ctx, f.prepared); err != nil {
		t.Fatal(err)
	}
	claimed, err := f.store.ClaimPushEffect(
		ctx,
		pusheffect.ClaimParams{
			Scope:         f.prepared.Scope(),
			LeaseOwner:    "db-clock-" + uuid.NewString(),
			LeaseDuration: time.Minute,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.store.RecordPushEffectAmbiguous(
		ctx,
		pusheffect.FailureParams{
			Lease: pusheffect.Lease{
				Scope:      claimed.Scope(),
				LeaseOwner: claimed.LeaseOwner,
				Fence:      claimed.Fence,
			},
			Class: "response_unknown",
		},
	); err != nil {
		t.Fatal(err)
	}
	schedule := pusheffect.ReconciliationSchedule{
		Resolution: pusheffect.Resolution{
			Scope:         claimed.Scope(),
			ExpectedFence: claimed.Fence,
			Class:         "provider_history_unavailable",
		},
		RetryAfter:   5 * time.Minute,
		ExpiredClass: "provider_window_expired",
	}
	decision, err := f.store.DeferOrBlockPushEffectReconciliation(
		ctx,
		schedule,
	)
	if err != nil || decision != pusheffect.ReconciliationDeferred {
		t.Fatalf("open-window decision=%q err=%v, want deferred", decision, err)
	}
	current, err := f.store.LoadPushEffect(ctx, claimed.Scope())
	if err != nil {
		t.Fatal(err)
	}
	if !current.NextAttemptAt.Equal(current.IdempotencyExpiresAt) ||
		current.Status != pusheffect.StatusAmbiguous {
		t.Fatalf("open-window checkpoint=%+v", current)
	}

	// No process timestamp is accepted by the transition. PostgreSQL waits
	// through the exact boundary and then makes the opposite decision.
	if _, err := f.db.ExecContext(ctx, `
		SELECT pg_sleep(
			GREATEST(0,EXTRACT(EPOCH FROM (
				idempotency_expires_at-clock_timestamp()
			)))+0.02
		) FROM push_effects WHERE id=$1`,
		f.prepared.ID,
	); err != nil {
		t.Fatal(err)
	}
	decision, err = f.store.DeferOrBlockPushEffectReconciliation(
		ctx,
		schedule,
	)
	if err != nil || decision != pusheffect.ReconciliationBlocked {
		t.Fatalf("expired-window decision=%q err=%v, want blocked", decision, err)
	}
	current, err = f.store.LoadPushEffect(ctx, claimed.Scope())
	if err != nil {
		t.Fatal(err)
	}
	if current.Status != pusheffect.StatusBlocked ||
		current.BlockedAt == nil ||
		current.FailureClass != "provider_window_expired" {
		t.Fatalf("expired-window checkpoint=%+v", current)
	}
}

func TestPushEffectRLSAndStoredIntegrity(t *testing.T) {
	f := newPushEffectFixture(t)
	ctx := t.Context()
	if _, err := f.store.CreatePushEffect(ctx, f.prepared); err != nil {
		t.Fatal(err)
	}
	cross := f.prepared.Scope()
	cross.TenantID++
	if _, err := f.store.LoadPushEffect(ctx, cross); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("cross-tenant load error=%v, want not found", err)
	}
	if _, err := f.store.pool.Exec(ctx, `
		UPDATE push_effects
		   SET card_payload='{"card":"tampered"}',
		       card_digest=encode(sha256(convert_to('{"card":"tampered"}','UTF8')),'hex')
		 WHERE id=$1`, f.prepared.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.LoadPushEffect(ctx, f.prepared.Scope()); !errors.Is(err, types.ErrInternal) {
		t.Fatalf("stored drift error=%v, want internal integrity failure", err)
	}
}

func TestPushEffectConcurrentClaimHasOneFence(t *testing.T) {
	f := newPushEffectFixture(t)
	ctx := t.Context()
	if _, err := f.store.CreatePushEffect(ctx, f.prepared); err != nil {
		t.Fatal(err)
	}
	type result struct {
		effect *pusheffect.Effect
		err    error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	for _, owner := range []string{"worker-a", "worker-b"} {
		go func(owner string) {
			<-start
			effect, err := f.store.ClaimPushEffect(context.Background(),
				pusheffect.ClaimParams{
					Scope: f.prepared.Scope(), LeaseOwner: owner,
					LeaseDuration: time.Minute,
				})
			results <- result{effect: effect, err: err}
		}(owner)
	}
	close(start)
	successes := 0
	conflicts := 0
	for range 2 {
		got := <-results
		if got.err == nil {
			successes++
			if got.effect.Fence != 1 || got.effect.Attempt != 1 {
				t.Fatalf("winning claim=%+v", got.effect)
			}
		} else if errors.Is(got.err, types.ErrConflict) {
			conflicts++
		} else {
			t.Fatalf("claim error=%v", got.err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("claim results success/conflict=%d/%d", successes, conflicts)
	}
}

func TestPushEffectCreateFirstSerializesTenantPurge(t *testing.T) {
	f := newPushEffectFixture(t)
	ctx := t.Context()
	if _, err := f.provider.Up(ctx); err != nil {
		t.Fatalf("migrate purge fixture to latest: %v", err)
	}

	parentTx, err := f.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	parentReleased := false
	defer func() {
		if !parentReleased {
			_ = parentTx.Rollback()
		}
	}()
	var batchID int64
	if err := parentTx.QueryRowContext(ctx, `
		SELECT id FROM push_batches
		 WHERE id=$1 AND tenant_id=$2
		 FOR UPDATE /* push effect create-first parent gate */`,
		f.prepared.BatchID, f.prepared.TenantID).Scan(&batchID); err != nil {
		t.Fatal(err)
	}

	createDone := make(chan error, 1)
	go func() {
		_, createErr := f.store.CreatePushEffect(ctx, f.prepared)
		createDone <- createErr
	}()
	waitForTenantAdmissionRootHolder(t, f.db, f.prepared.TenantID)

	type purgeResult struct {
		report *PurgeReport
		err    error
	}
	purgeDone := make(chan purgeResult, 1)
	go func() {
		report, purgeErr := f.store.PurgeTenant(
			ctx, f.prepared.TenantID, false)
		purgeDone <- purgeResult{report: report, err: purgeErr}
	}()
	waitForTenantAdmissionRootWaiter(t, f.db)

	if err := parentTx.Rollback(); err != nil {
		t.Fatal(err)
	}
	parentReleased = true

	select {
	case createErr := <-createDone:
		if createErr != nil {
			t.Fatalf("create-first effect failed: %v", createErr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("create-first effect did not converge")
	}

	var purged purgeResult
	select {
	case purged = <-purgeDone:
	case <-time.After(10 * time.Second):
		t.Fatal("create-first purge did not converge")
	}
	if purged.err != nil {
		t.Fatalf("create-first purge failed: %v", purged.err)
	}
	if purged.report == nil ||
		purged.report.Rows["push_effects"] != 1 ||
		purged.report.Rows["deliveries"] != 2 ||
		purged.report.Rows["push_batches"] != 1 ||
		purged.report.Rows["task_run_snapshots"] != 1 ||
		purged.report.Rows["tenants"] != 1 {
		t.Fatalf("create-first purge report = %+v", purged.report)
	}
	assertPushEffectAggregatePurged(t, f)
}

func TestPushEffectPurgeFirstRejectsWaitingCreate(t *testing.T) {
	f := newPushEffectFixture(t)
	ctx := t.Context()
	if _, err := f.provider.Up(ctx); err != nil {
		t.Fatalf("migrate purge fixture to latest: %v", err)
	}

	scheduleTx, err := f.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	scheduleReleased := false
	defer func() {
		if !scheduleReleased {
			_ = scheduleTx.Rollback()
		}
	}()
	var taskID string
	if err := scheduleTx.QueryRowContext(ctx, `
		SELECT id FROM schedules
		 WHERE id=$1 AND tenant_id=$2
		 FOR UPDATE /* push effect purge-first schedule gate */`,
		f.prepared.TaskID, f.prepared.TenantID).Scan(&taskID); err != nil {
		t.Fatal(err)
	}

	type purgeResult struct {
		report *PurgeReport
		err    error
	}
	purgeDone := make(chan purgeResult, 1)
	go func() {
		report, purgeErr := f.store.PurgeTenant(
			ctx, f.prepared.TenantID, false)
		purgeDone <- purgeResult{report: report, err: purgeErr}
	}()
	waitForTenantAdmissionRootHolder(t, f.db, f.prepared.TenantID)
	waitForTenantPurgeDefinitionEditLock(t, f.store)

	createDone := make(chan error, 1)
	go func() {
		_, createErr := f.store.CreatePushEffect(ctx, f.prepared)
		createDone <- createErr
	}()
	waitForTenantAdmissionRootWaiter(t, f.db)

	if err := scheduleTx.Rollback(); err != nil {
		t.Fatal(err)
	}
	scheduleReleased = true

	var purged purgeResult
	select {
	case purged = <-purgeDone:
	case <-time.After(10 * time.Second):
		t.Fatal("purge-first purge did not converge")
	}
	if purged.err != nil {
		t.Fatalf("purge-first purge failed: %v", purged.err)
	}
	if purged.report == nil ||
		purged.report.Rows["push_effects"] != 0 ||
		purged.report.Rows["deliveries"] != 2 ||
		purged.report.Rows["push_batches"] != 1 ||
		purged.report.Rows["task_run_snapshots"] != 1 ||
		purged.report.Rows["tenants"] != 1 {
		t.Fatalf("purge-first purge report = %+v", purged.report)
	}

	select {
	case createErr := <-createDone:
		if !errors.Is(createErr, types.ErrConflict) {
			t.Fatalf("purge-first create error = %v, want conflict", createErr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("purge-first create did not converge")
	}
	assertPushEffectAggregatePurged(t, f)
}

func waitForTenantAdmissionRootHolder(
	t *testing.T,
	db *sql.DB,
	tenantID int64,
) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		tx, err := db.BeginTx(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}
		var acquired bool
		key := tenantAdmissionRootLockNamespace +
			strconv.FormatInt(tenantID, 10)
		queryErr := tx.QueryRowContext(t.Context(), `
			SELECT pg_try_advisory_xact_lock(hashtextextended($1, $2))`,
			key, tenantAdmissionRootLockSeed).Scan(&acquired)
		rollbackErr := tx.Rollback()
		if queryErr != nil {
			t.Fatal(queryErr)
		}
		if rollbackErr != nil {
			t.Fatal(rollbackErr)
		}
		if !acquired {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("tenant admission root was not held")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func waitForTenantAdmissionRootWaiter(t *testing.T, db *sql.DB) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var waiting bool
		if err := db.QueryRowContext(t.Context(), `
			SELECT EXISTS (
				SELECT 1
				  FROM pg_stat_activity
				 WHERE datname=current_database()
				   AND pid<>pg_backend_pid()
				   AND wait_event_type='Lock'
				   AND query LIKE
				       '%pg_advisory_xact_lock(hashtextextended%'
			)`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("tenant admission root waiter was not observed")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func assertPushEffectAggregatePurged(t *testing.T, f pushEffectFixture) {
	t.Helper()
	var effects, deliveries, batches, snapshots, tenants int
	if err := f.db.QueryRowContext(t.Context(), `
		SELECT
		  (SELECT count(*) FROM push_effects WHERE tenant_id=$1),
		  (SELECT count(*) FROM deliveries WHERE tenant_id=$1),
		  (SELECT count(*) FROM push_batches WHERE tenant_id=$1),
		  (SELECT count(*) FROM task_run_snapshots WHERE tenant_id=$1),
		  (SELECT count(*) FROM tenants WHERE id=$1)`,
		f.prepared.TenantID,
	).Scan(&effects, &deliveries, &batches, &snapshots, &tenants); err != nil {
		t.Fatal(err)
	}
	if effects != 0 || deliveries != 0 || batches != 0 ||
		snapshots != 0 || tenants != 0 {
		t.Fatalf(
			"purged aggregate effects/deliveries/batches/snapshots/tenants=%d/%d/%d/%d/%d",
			effects, deliveries, batches, snapshots, tenants)
	}
}
