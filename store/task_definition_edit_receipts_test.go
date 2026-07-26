package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/types"
)

type taskDefinitionEditReceiptFixture struct {
	store       *Store
	tenantID    int64
	userID      int64
	sessionID   int64
	operationID string
	taskID      string
	receipt     *types.TaskDefinitionEditReceipt
}

func TestTaskDefinitionEditReceipt_TerminalInsertReplaySuppressionAndDiscovery(
	t *testing.T,
) {
	st := tenantTestStore(t)
	bound := newTaskDefinitionEditReceiptFixture(
		t, st, "feishu_card_patch:test", "om_"+uuid.NewString())
	suppressed := newTaskDefinitionEditReceiptFixture(t, st, "", "")
	ctx := t.Context()

	if bound.receipt.Status != types.TaskDefinitionEditReceiptStatusPending ||
		bound.receipt.Provider == "" || bound.receipt.Target == "" ||
		bound.receipt.NextAttemptAt.Sub(bound.receipt.CreatedAt) < 3*time.Second ||
		bound.receipt.OperationStatus != types.TaskDefinitionEditOperationStatusCancelled ||
		bound.receipt.OperationPhase != types.TaskDefinitionEditPhaseProposalSealed ||
		bound.receipt.TaskID != bound.taskID {
		t.Fatalf("bound terminal receipt mismatch: %+v", bound.receipt)
	}
	if suppressed.receipt.Status != types.TaskDefinitionEditReceiptStatusSuppressed ||
		suppressed.receipt.FailureClass !=
			types.TaskDefinitionEditReceiptFailureTargetUnbound ||
		suppressed.receipt.Provider != "" || suppressed.receipt.Target != "" ||
		suppressed.receipt.SentAt == nil ||
		suppressed.receipt.ProviderMessageID != "target-unbound-suppressed" {
		t.Fatalf("suppressed terminal receipt mismatch: %+v", suppressed.receipt)
	}
	if bound.receipt.ProviderKey !=
		taskDefinitionEditReceiptExpectedProviderKey(bound.operationID) {
		t.Fatalf("bound provider key=%q want SHA-256-derived key",
			bound.receipt.ProviderKey)
	}
	if suppressed.receipt.ProviderKey !=
		taskDefinitionEditReceiptExpectedProviderKey(suppressed.operationID) {
		t.Fatalf("suppressed provider key=%q want SHA-256-derived key",
			suppressed.receipt.ProviderKey)
	}

	replayTaskDefinitionEditTerminalReceipt(t, bound)
	replayTaskDefinitionEditTerminalReceipt(t, suppressed)
	assertTaskDefinitionEditReceiptCount(t, st, bound.operationID, 1)
	assertTaskDefinitionEditReceiptCount(t, st, suppressed.operationID, 1)

	makeTaskDefinitionEditReceiptDue(t, bound)
	makeTaskDefinitionEditReceiptDue(t, suppressed)
	discoveryStore := *st
	var discoveryOptions pgx.TxOptions
	discoveryStore.beginTx = func(
		ctx context.Context,
		options pgx.TxOptions,
	) (pgx.Tx, error) {
		discoveryOptions = options
		return st.pool.BeginTx(ctx, options)
	}
	tenantIDs, err := discoveryStore.ListDueTaskDefinitionEditReceiptTenantIDs(
		ctx, time.Now().Add(time.Hour), 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if discoveryOptions.AccessMode != pgx.ReadOnly {
		t.Fatalf("cross-tenant discovery access mode=%v want READ ONLY",
			discoveryOptions.AccessMode)
	}
	if !taskDefinitionEditReceiptContainsTenantID(tenantIDs, bound.tenantID) {
		t.Fatalf("bound receipt tenant missing from discovery: %+v", tenantIDs)
	}
	if taskDefinitionEditReceiptContainsTenantID(tenantIDs, suppressed.tenantID) {
		t.Fatalf("suppressed receipt tenant must not be discovered: %+v", tenantIDs)
	}
	due, err := st.ListDueTaskDefinitionEditReceipts(
		ctx, bound.tenantID, time.Now().Add(time.Hour), 100)
	if err != nil {
		t.Fatal(err)
	}
	if !containsTaskDefinitionEditReceipt(due, bound.receipt.ID) ||
		containsTaskDefinitionEditReceipt(due, suppressed.receipt.ID) {
		t.Fatalf("tenant due list mismatch: %+v", due)
	}

	if _, err := st.pool.Exec(ctx, `
		UPDATE task_definition_edit_receipts
		   SET provider_key = $2::uuid
		 WHERE operation_id = $1`, bound.operationID, uuid.NewString()); err != nil {
		t.Fatal(err)
	}
	tx, err := st.beginTaskDefinitionEditTx(ctx, bound.tenantID)
	if err != nil {
		t.Fatal(err)
	}
	defer rollbackTaskDefinitionEditTx(ctx, tx)
	if err := insertTaskDefinitionEditReceiptForTerminal(
		ctx, tx, bound.operationID, bound.tenantID, bound.userID,
	); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("operation conflict must not adopt a different provider key: %v", err)
	}
}

func TestTaskDefinitionEditReceipt_LeasePayloadSessionAndDeliveryLifecycle(
	t *testing.T,
) {
	st := tenantTestStore(t)
	f := newTaskDefinitionEditReceiptFixture(
		t, st, "feishu_card_patch:test", "om_"+uuid.NewString())
	ctx := t.Context()
	makeTaskDefinitionEditReceiptDue(t, f)

	claim := raceTaskDefinitionEditReceiptAcquireForTest(t, f)
	if claim.Fence != 1 || claim.Attempt != 1 || claim.LeaseUntil == nil {
		t.Fatalf("first receipt claim mismatch: %+v", claim)
	}
	originalLeaseUntil := *claim.LeaseUntil
	replayed, err := st.AcquireTaskDefinitionEditReceipt(
		ctx, types.AcquireTaskDefinitionEditReceiptParams{
			ID: f.receipt.ID, TenantID: f.tenantID, UserID: f.userID,
			LeaseOwner: claim.LeaseOwner, LeaseDuration: time.Minute,
		})
	if err != nil {
		t.Fatalf("same-owner active replay must adopt: %v", err)
	}
	if replayed.Fence != claim.Fence || replayed.Attempt != claim.Attempt ||
		replayed.LeaseUntil == nil || !replayed.LeaseUntil.Equal(originalLeaseUntil) {
		t.Fatalf("same-owner replay extended or refenced the lease: first=%+v replay=%+v",
			claim, replayed)
	}
	if _, err := st.AcquireTaskDefinitionEditReceipt(
		ctx, types.AcquireTaskDefinitionEditReceiptParams{
			ID: f.receipt.ID, TenantID: f.tenantID, UserID: f.userID,
			LeaseOwner: "definition-edit-receipt-worker-2", LeaseDuration: time.Minute,
		}); !errors.Is(err, types.ErrTaskDefinitionEditReceiptBusy) {
		t.Fatalf("second active owner must be busy: %v", err)
	}
	if err := st.MarkTaskDefinitionEditReceiptSent(
		ctx, claim.Lease(), "om_delivery_result"); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("send before payload/session checkpoints must conflict: %v", err)
	}

	payload := []byte("{\n  \"card\": \"definition edited\"\n}")
	digest := taskDefinitionEditReceiptTestDigest(payload)
	lostStore := storeWithCommitResponseLost(st)
	if err := lostStore.CheckpointTaskDefinitionEditReceiptPayload(
		ctx, claim.Lease(), payload, digest); !errors.Is(err, types.ErrDatabase) {
		t.Fatalf("payload commit response loss must surface database error: %v", err)
	}
	if err := st.CheckpointTaskDefinitionEditReceiptPayload(
		ctx, claim.Lease(), payload, digest); err != nil {
		t.Fatalf("payload response-lost exact replay failed: %v", err)
	}
	different := []byte(`{"card":"different"}`)
	if err := st.CheckpointTaskDefinitionEditReceiptPayload(
		ctx, claim.Lease(), different,
		taskDefinitionEditReceiptTestDigest(different),
	); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("different immutable payload must conflict: %v", err)
	}
	if err := st.MarkTaskDefinitionEditReceiptSent(
		ctx, claim.Lease(), "om_delivery_result"); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("send before session checkpoint must conflict: %v", err)
	}

	if err := st.RecordTaskDefinitionEditReceiptSessionMessages(
		ctx, claim.Lease(), json.RawMessage(`{"role":"user"}`),
	); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("non-array session checkpoint must validate: %v", err)
	}
	if err := st.RecordTaskDefinitionEditReceiptSessionMessages(
		ctx, claim.Lease(),
		json.RawMessage(`[{"role":"user","role":"assistant"}]`),
	); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("duplicate-key session checkpoint must validate: %v", err)
	}
	messages := json.RawMessage(
		`[{"role":"user","content":"[卡片回调] 已完成任务定义编辑"}]`)
	if err := lostStore.RecordTaskDefinitionEditReceiptSessionMessages(
		ctx, claim.Lease(), messages); !errors.Is(err, types.ErrDatabase) {
		t.Fatalf("session commit response loss must surface database error: %v", err)
	}
	if err := st.RecordTaskDefinitionEditReceiptSessionMessages(
		ctx, claim.Lease(), messages); err != nil {
		t.Fatalf("session response-lost exact replay failed: %v", err)
	}
	var recorded []map[string]any
	if err := st.pool.QueryRow(ctx,
		`SELECT messages FROM agent_sessions WHERE id = $1`, f.sessionID,
	).Scan(&recorded); err != nil {
		t.Fatal(err)
	}
	if len(recorded) != 1 {
		t.Fatalf("session messages appended more than once: %+v", recorded)
	}
	var definitionEventCount int
	if err := st.pool.QueryRow(ctx,
		`SELECT count(*)
		   FROM agent_events
		  WHERE tenant_id=$1 AND user_id=$2 AND session_id=$3
		    AND batch_idempotency_key LIKE 'side.%'`,
		f.tenantID, f.userID, f.sessionID,
	).Scan(&definitionEventCount); err != nil {
		t.Fatal(err)
	}
	if definitionEventCount != 3 {
		t.Fatalf("definition receipt snapshot event count=%d want=3",
			definitionEventCount)
	}

	if err := lostStore.MarkTaskDefinitionEditReceiptSent(
		ctx, claim.Lease(), "om_delivery_result"); !errors.Is(err, types.ErrDatabase) {
		t.Fatalf("sent commit response loss must surface database error: %v", err)
	}
	if err := st.MarkTaskDefinitionEditReceiptSent(
		ctx, claim.Lease(), "om_delivery_result"); err != nil {
		t.Fatalf("sent response-lost exact replay failed: %v", err)
	}
	// A terminal-operation response-lost retry may arrive after dispatch has
	// already advanced the receipt. It must adopt immutable identity without
	// trying to rewind the delivery state to pending.
	replayTaskDefinitionEditTerminalReceipt(t, f)
	if err := st.MarkTaskDefinitionEditReceiptSent(
		ctx, claim.Lease(), "om_different_result"); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("different sent replay must conflict: %v", err)
	}
	if _, err := st.AcquireTaskDefinitionEditReceipt(
		ctx, types.AcquireTaskDefinitionEditReceiptParams{
			ID: f.receipt.ID, TenantID: f.tenantID, UserID: f.userID,
			LeaseOwner: "definition-edit-receipt-worker-3", LeaseDuration: time.Minute,
		}); !errors.Is(err, types.ErrTaskDefinitionEditReceiptTerminal) {
		t.Fatalf("sent receipt must be terminal: %v", err)
	}
}

func TestTaskDefinitionEditReceipt_SendFailureClassesAndDatabaseRetryClock(
	t *testing.T,
) {
	st := tenantTestStore(t)
	f := newTaskDefinitionEditReceiptFixture(
		t, st, "feishu_card_patch:test", "om_"+uuid.NewString())
	ctx := t.Context()
	makeTaskDefinitionEditReceiptDue(t, f)

	first := acquireTaskDefinitionEditReceiptForTest(t, f, "failure-worker-1", time.Minute)
	if err := st.RecordTaskDefinitionEditReceiptSendFailure(ctx,
		types.RecordTaskDefinitionEditReceiptSendFailureParams{
			Lease: first.Lease(),
			Class: types.TaskDefinitionEditReceiptFailureTargetUnbound,
		}); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("target_unbound is reserved for suppressed insertion: %v", err)
	}
	if err := st.RecordTaskDefinitionEditReceiptSendFailure(ctx,
		types.RecordTaskDefinitionEditReceiptSendFailureParams{
			Lease: first.Lease(), Class: types.TaskDefinitionEditReceiptFailureAmbiguous,
			RetryAfter: time.Hour,
		}); err != nil {
		t.Fatal(err)
	}
	ambiguous := loadTaskDefinitionEditReceiptForTest(t, f)
	if ambiguous.Status != types.TaskDefinitionEditReceiptStatusPending ||
		ambiguous.FailureClass != types.TaskDefinitionEditReceiptFailureAmbiguous ||
		ambiguous.AmbiguousSince == nil || ambiguous.LeaseOwner != "" ||
		ambiguous.LeaseUntil != nil {
		t.Fatalf("ambiguous failure checkpoint mismatch: %+v", ambiguous)
	}
	due, err := st.ListDueTaskDefinitionEditReceipts(
		ctx, f.tenantID, time.Now().Add(2*time.Hour), 100)
	if err != nil {
		t.Fatal(err)
	}
	if containsTaskDefinitionEditReceipt(due, f.receipt.ID) {
		t.Fatalf("caller future time bypassed DB-clock retry boundary: %+v", due)
	}

	makeTaskDefinitionEditReceiptDue(t, f)
	second := acquireTaskDefinitionEditReceiptForTest(t, f, "failure-worker-2", time.Minute)
	if err := st.RecordTaskDefinitionEditReceiptSendFailure(ctx,
		types.RecordTaskDefinitionEditReceiptSendFailureParams{
			Lease: second.Lease(), Class: types.TaskDefinitionEditReceiptFailureRetryable,
			RetryAfter: time.Minute,
		}); err != nil {
		t.Fatal(err)
	}
	retryable := loadTaskDefinitionEditReceiptForTest(t, f)
	if retryable.FailureClass != types.TaskDefinitionEditReceiptFailureAmbiguous ||
		retryable.AmbiguousSince == nil ||
		!retryable.AmbiguousSince.Equal(*ambiguous.AmbiguousSince) {
		t.Fatalf("retryable failure must preserve old ambiguity evidence: %+v", retryable)
	}

	makeTaskDefinitionEditReceiptDue(t, f)
	third := acquireTaskDefinitionEditReceiptForTest(t, f, "failure-worker-3", time.Minute)
	permanent := types.RecordTaskDefinitionEditReceiptSendFailureParams{
		Lease: third.Lease(), Class: types.TaskDefinitionEditReceiptFailurePermanent,
	}
	if err := st.RecordTaskDefinitionEditReceiptSendFailure(
		ctx, permanent); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("ambiguous receipt accepted permanent downgrade: %v", err)
	}
	stillAmbiguous := loadTaskDefinitionEditReceiptForTest(t, f)
	if stillAmbiguous.Status != types.TaskDefinitionEditReceiptStatusPending ||
		stillAmbiguous.FailureClass != types.TaskDefinitionEditReceiptFailureAmbiguous ||
		stillAmbiguous.AmbiguousSince == nil {
		t.Fatalf("permanent downgrade changed ambiguous evidence: %+v", stillAmbiguous)
	}

	permanentFixture := newTaskDefinitionEditReceiptFixture(
		t, st, "feishu_card_patch:test", "om_"+uuid.NewString())
	makeTaskDefinitionEditReceiptDue(t, permanentFixture)
	permanentLease := acquireTaskDefinitionEditReceiptForTest(
		t, permanentFixture, "permanent-worker", time.Minute)
	permanent = types.RecordTaskDefinitionEditReceiptSendFailureParams{
		Lease: permanentLease.Lease(),
		Class: types.TaskDefinitionEditReceiptFailurePermanent,
	}
	if err := st.RecordTaskDefinitionEditReceiptSendFailure(ctx, permanent); err != nil {
		t.Fatal(err)
	}
	if err := st.RecordTaskDefinitionEditReceiptSendFailure(ctx, permanent); err != nil {
		t.Fatalf("permanent failure exact replay failed: %v", err)
	}
	blocked := loadTaskDefinitionEditReceiptForTest(t, permanentFixture)
	if blocked.Status != types.TaskDefinitionEditReceiptStatusBlocked ||
		blocked.FailureClass != types.TaskDefinitionEditReceiptFailurePermanent ||
		blocked.BlockedAt == nil || blocked.AmbiguousSince != nil ||
		blocked.LeaseOwner != "" || blocked.LeaseUntil != nil {
		t.Fatalf("permanent failure checkpoint mismatch: %+v", blocked)
	}
}

func TestTaskDefinitionEditReceipt_DBClockAfterLockWaitAndScopeFailClosed(
	t *testing.T,
) {
	st := tenantTestStore(t)
	f := newTaskDefinitionEditReceiptFixture(
		t, st, "feishu_card_patch:test", "om_"+uuid.NewString())
	ctx := t.Context()
	makeTaskDefinitionEditReceiptDue(t, f)
	claim := acquireTaskDefinitionEditReceiptForTest(
		t, f, "lock-wait-worker", 750*time.Millisecond)

	holder, err := st.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	holderOpen := true
	t.Cleanup(func() {
		if holderOpen {
			_ = holder.Rollback(context.WithoutCancel(t.Context()))
		}
	})
	var lockedID int64
	if err := holder.QueryRow(ctx, `
		SELECT id FROM task_definition_edit_receipts
		 WHERE id = $1 FOR UPDATE`, f.receipt.ID).Scan(&lockedID); err != nil {
		t.Fatal(err)
	}

	payload := []byte(`{"card":"waited past lease"}`)
	digest := taskDefinitionEditReceiptTestDigest(payload)
	done := make(chan error, 1)
	go func() {
		done <- st.CheckpointTaskDefinitionEditReceiptPayload(
			ctx, claim.Lease(), payload, digest)
	}()
	waitForTaskDefinitionEditReceiptLockWait(t, st)
	waitForTaskDefinitionEditReceiptLeaseExpiry(t, st, f.receipt.ID)
	if err := holder.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	holderOpen = false
	select {
	case err := <-done:
		if !errors.Is(err, types.ErrTaskDefinitionEditReceiptLeaseLost) {
			t.Fatalf("post-lock DB clock must reject the expired lease: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("receipt checkpoint remained blocked after lock release")
	}
	if _, err := st.AcquireTaskDefinitionEditReceipt(
		ctx, types.AcquireTaskDefinitionEditReceiptParams{
			ID: f.receipt.ID, TenantID: f.tenantID, UserID: f.userID,
			LeaseOwner: "premature-takeover", LeaseDuration: time.Minute,
		}); !errors.Is(err, types.ErrTaskDefinitionEditReceiptBusy) {
		t.Fatalf("expired lease must remain busy until fixed takeover grace: %v", err)
	}
	if _, err := st.pool.Exec(ctx, `
		UPDATE task_definition_edit_receipts
		   SET lease_until = clock_timestamp() - interval '2 seconds',
		       takeover_not_before = clock_timestamp() - interval '1 second'
		 WHERE id = $1`, f.receipt.ID); err != nil {
		t.Fatal(err)
	}
	takeover := acquireTaskDefinitionEditReceiptForTest(
		t, f, "post-grace-takeover", time.Minute)
	if takeover.Fence != claim.Fence+1 || takeover.Attempt != claim.Attempt+1 {
		t.Fatalf("post-grace takeover did not advance fence exactly once: first=%+v next=%+v",
			claim, takeover)
	}

	if _, err := st.LoadTaskDefinitionEditReceiptByOperation(
		ctx, f.operationID, f.tenantID+1_000_000, f.userID,
	); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("cross-tenant load must be not found: %v", err)
	}
	if _, err := st.LoadTaskDefinitionEditReceiptByOperation(
		ctx, f.operationID, f.tenantID, f.userID+1_000_000,
	); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("cross-user load must be not found: %v", err)
	}

	if _, err := st.beginTaskDefinitionEditReceiptTx(ctx, 0); err == nil {
		t.Fatal("receipt transaction accepted an empty tenant context")
	}
	emptyTx, err := st.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := emptyTx.Exec(ctx,
		`SELECT set_config('app.tenant_id', '', true)`); err != nil {
		rollbackTaskDefinitionEditTx(ctx, emptyTx)
		t.Fatal(err)
	}
	if _, err := emptyTx.Exec(ctx,
		`SET LOCAL ROLE vane_edit_receipt`); err != nil {
		rollbackTaskDefinitionEditTx(ctx, emptyTx)
		t.Fatal(err)
	}
	var visibleWithoutTenant int
	if err := emptyTx.QueryRow(ctx,
		`SELECT count(*) FROM task_definition_edit_receipts`,
	).Scan(&visibleWithoutTenant); err != nil {
		rollbackTaskDefinitionEditTx(ctx, emptyTx)
		t.Fatal(err)
	}
	if visibleWithoutTenant != 0 {
		rollbackTaskDefinitionEditTx(ctx, emptyTx)
		t.Fatalf("missing tenant context exposed %d receipt rows", visibleWithoutTenant)
	}
	if err := emptyTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	privilegeTx, err := st.beginTaskDefinitionEditReceiptTx(ctx, f.tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := privilegeTx.Exec(ctx, `
		UPDATE task_definition_edit_operations
		   SET updated_at = clock_timestamp()
		 WHERE id = $1`, f.operationID); err == nil {
		rollbackTaskDefinitionEditTx(ctx, privilegeTx)
		t.Fatal("receipt role unexpectedly mutated an edit operation")
	}
	rollbackTaskDefinitionEditTx(ctx, privilegeTx)
}

func newTaskDefinitionEditReceiptFixture(
	t *testing.T,
	st *Store,
	provider string,
	target string,
) *taskDefinitionEditReceiptFixture {
	t.Helper()
	base := newCompiledTaskFixture(t, st)
	ctx := t.Context()
	session, err := st.CreateAgentSession(ctx, base.userID)
	if err != nil {
		t.Fatal(err)
	}
	f := &taskDefinitionEditReceiptFixture{
		store: st, tenantID: base.tenantID, userID: base.userID,
		sessionID:   session.ID,
		operationID: "definition-edit-receipt-" + uuid.NewString(),
		taskID:      "definition-edit-receipt-task-" + uuid.NewString(),
	}
	baseDefinition := []byte(`{"schema":"approved-definition/v1","kind":"base"}`)
	targetDefinition := []byte(`{"schema":"approved-definition/v1","kind":"target"}`)
	proposal := []byte(`{"schema":"frozen-task-definition-edit-proposal/v1"}`)
	prepared := []byte(`{"schema":"prepared-task-definition-edit/v1"}`)
	baseSnapshot := []byte(`{"schema":"task-definition-edit-snapshot/v1"}`)
	if _, err := st.pool.Exec(ctx, `
		INSERT INTO task_definition_edit_operations (
			id, tenant_id, user_id, target_tenant_id, target_user_id,
			task_id, session_id, approval_ref, status, phase, expires_at,
			original_status,
			base_definition_version, base_definition_digest, base_definition,
			target_definition_version, target_definition_digest, target_definition,
			canonical_proposal, proposal_digest,
			prepared_edit, prepared_edit_digest,
			base_snapshot, base_snapshot_digest,
			receipt_provider, receipt_target, tombstoned_at
		) VALUES (
			$1, $2, $3, $2, $3, $4, $5, $6,
			'cancelled', 'proposal_sealed', clock_timestamp() + interval '1 day',
			'paused', 1, $7, $8, 2, $9, $10, $11, $12, $13, $14,
			$15, $16, $17, $18, clock_timestamp()
		)`,
		f.operationID, f.tenantID, f.userID, f.taskID, f.sessionID,
		"approval:"+f.operationID,
		taskDefinitionEditReceiptTestDigest(baseDefinition), baseDefinition,
		taskDefinitionEditReceiptTestDigest(targetDefinition), targetDefinition,
		proposal, taskDefinitionEditReceiptTestDigest(proposal),
		prepared, taskDefinitionEditReceiptTestDigest(prepared),
		baseSnapshot, taskDefinitionEditReceiptTestDigest(baseSnapshot),
		provider, target); err != nil {
		t.Fatalf("insert terminal edit operation: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM task_definition_edit_receipts WHERE operation_id = $1`,
			f.operationID)
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM task_definition_edit_operations WHERE id = $1`, f.operationID)
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM agent_events WHERE session_id = $1`, f.sessionID)
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM agent_sessions WHERE id = $1`, f.sessionID)
	})

	tx, err := st.beginTaskDefinitionEditTx(ctx, f.tenantID)
	if err != nil {
		t.Fatal(err)
	}
	defer rollbackTaskDefinitionEditTx(ctx, tx)
	if err := insertTaskDefinitionEditReceiptForTerminal(
		ctx, tx, f.operationID, f.tenantID, f.userID); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	f.receipt, err = st.LoadTaskDefinitionEditReceiptByOperation(
		ctx, f.operationID, f.tenantID, f.userID)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func replayTaskDefinitionEditTerminalReceipt(
	t *testing.T,
	f *taskDefinitionEditReceiptFixture,
) {
	t.Helper()
	ctx := t.Context()
	tx, err := f.store.beginTaskDefinitionEditTx(ctx, f.tenantID)
	if err != nil {
		t.Fatal(err)
	}
	defer rollbackTaskDefinitionEditTx(ctx, tx)
	if err := insertTaskDefinitionEditReceiptForTerminal(
		ctx, tx, f.operationID, f.tenantID, f.userID); err != nil {
		t.Fatalf("terminal receipt exact replay failed: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

func acquireTaskDefinitionEditReceiptForTest(
	t *testing.T,
	f *taskDefinitionEditReceiptFixture,
	owner string,
	duration time.Duration,
) *types.TaskDefinitionEditReceipt {
	t.Helper()
	receipt, err := f.store.AcquireTaskDefinitionEditReceipt(
		t.Context(), types.AcquireTaskDefinitionEditReceiptParams{
			ID: f.receipt.ID, TenantID: f.tenantID, UserID: f.userID,
			LeaseOwner: owner, LeaseDuration: duration,
		})
	if err != nil {
		t.Fatal(err)
	}
	return receipt
}

func raceTaskDefinitionEditReceiptAcquireForTest(
	t *testing.T,
	f *taskDefinitionEditReceiptFixture,
) *types.TaskDefinitionEditReceipt {
	t.Helper()
	type outcome struct {
		receipt *types.TaskDefinitionEditReceipt
		err     error
	}
	start := make(chan struct{})
	results := make(chan outcome, 2)
	var wait sync.WaitGroup
	for _, owner := range []string{"definition-edit-receipt-worker-a", "definition-edit-receipt-worker-b"} {
		wait.Go(func() {
			<-start
			receipt, err := f.store.AcquireTaskDefinitionEditReceipt(
				t.Context(), types.AcquireTaskDefinitionEditReceiptParams{
					ID: f.receipt.ID, TenantID: f.tenantID, UserID: f.userID,
					LeaseOwner: owner, LeaseDuration: time.Minute,
				})
			results <- outcome{receipt: receipt, err: err}
		})
	}
	close(start)
	wait.Wait()
	close(results)

	var winner *types.TaskDefinitionEditReceipt
	busy := 0
	for result := range results {
		switch {
		case result.err == nil:
			if winner != nil {
				t.Fatalf("two receipt acquisitions won: first=%+v second=%+v",
					winner, result.receipt)
			}
			winner = result.receipt
		case errors.Is(result.err, types.ErrTaskDefinitionEditReceiptBusy):
			busy++
		default:
			t.Fatalf("concurrent receipt acquisition failed unexpectedly: %v", result.err)
		}
	}
	if winner == nil || busy != 1 {
		t.Fatalf("concurrent receipt acquisition winner=%+v busy=%d", winner, busy)
	}
	return winner
}

func loadTaskDefinitionEditReceiptForTest(
	t *testing.T,
	f *taskDefinitionEditReceiptFixture,
) *types.TaskDefinitionEditReceipt {
	t.Helper()
	receipt, err := f.store.LoadTaskDefinitionEditReceiptByOperation(
		t.Context(), f.operationID, f.tenantID, f.userID)
	if err != nil {
		t.Fatal(err)
	}
	return receipt
}

func makeTaskDefinitionEditReceiptDue(
	t *testing.T,
	f *taskDefinitionEditReceiptFixture,
) {
	t.Helper()
	if _, err := f.store.pool.Exec(t.Context(), `
		UPDATE task_definition_edit_receipts
		   SET next_attempt_at = clock_timestamp() - interval '1 second'
		 WHERE id = $1`, f.receipt.ID); err != nil {
		t.Fatal(err)
	}
}

func waitForTaskDefinitionEditReceiptLockWait(t *testing.T, st *Store) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var waiting bool
		if err := st.pool.QueryRow(t.Context(), `
			SELECT EXISTS (
				SELECT 1
				  FROM pg_stat_activity
				 WHERE datname = current_database()
				   AND pid <> pg_backend_pid()
				   AND wait_event_type = 'Lock'
				   AND query LIKE '%task_definition_edit_receipts r%'
				   AND query LIKE '%FOR UPDATE OF r%'
			)`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("receipt checkpoint did not reach the row-lock wait")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func waitForTaskDefinitionEditReceiptLeaseExpiry(
	t *testing.T,
	st *Store,
	receiptID int64,
) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var expired bool
		if err := st.pool.QueryRow(t.Context(), `
			SELECT clock_timestamp() >= lease_until
			  FROM task_definition_edit_receipts
			 WHERE id = $1`, receiptID).Scan(&expired); err != nil {
			t.Fatal(err)
		}
		if expired {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("receipt lease did not expire at the database clock")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func assertTaskDefinitionEditReceiptCount(
	t *testing.T,
	st *Store,
	operationID string,
	want int,
) {
	t.Helper()
	var count int
	if err := st.pool.QueryRow(t.Context(), `
		SELECT count(*) FROM task_definition_edit_receipts
		 WHERE operation_id = $1`, operationID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != want {
		t.Fatalf("operation %s receipt count=%d want=%d", operationID, count, want)
	}
}

func containsTaskDefinitionEditReceipt(
	receipts []types.TaskDefinitionEditReceipt,
	id int64,
) bool {
	for _, receipt := range receipts {
		if receipt.ID == id {
			return true
		}
	}
	return false
}

func taskDefinitionEditReceiptContainsTenantID(values []int64, want int64) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func TestTaskDefinitionEditReceiptReferencesRejectControlsAndFormatRunes(t *testing.T) {
	for _, value := range []string{"worker\nother", "worker\u200eother"} {
		if validTaskDefinitionEditReceiptReference(value, 255) {
			t.Fatalf("receipt reference accepted control/format rune: %q", value)
		}
		if err := validateAcquireTaskDefinitionEditReceiptParams(
			types.AcquireTaskDefinitionEditReceiptParams{
				ID: 1, TenantID: 1, UserID: 1,
				LeaseOwner: value, LeaseDuration: time.Second,
			},
		); err == nil {
			t.Fatalf("receipt lease owner accepted control/format rune: %q", value)
		}
		if err := validateTaskDefinitionEditReceiptTarget("lark", value, false); err == nil {
			t.Fatalf("receipt target accepted control/format rune: %q", value)
		}
	}
}

func taskDefinitionEditReceiptExpectedProviderKey(operationID string) string {
	sum := sha256.Sum256([]byte(
		"vane/task-definition-edit-receipt/v1:" + operationID))
	var id uuid.UUID
	copy(id[:], sum[:16])
	return id.String()
}

func taskDefinitionEditReceiptTestDigest(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}
