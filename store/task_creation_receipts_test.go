package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/agentledger"
	"github.com/YouToco/vane/types"
)

func TestTaskCreationReceipt_TerminalAtomicityBindingAndStableProviderKey(t *testing.T) {
	st := tenantTestStore(t)
	f := newCompiledTaskFixture(t, st)
	ctx := t.Context()
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM task_creation_receipts WHERE tenant_id = $1`, f.tenantID)
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM pending_actions WHERE tenant_id = $1`, f.tenantID)
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM agent_events WHERE tenant_id = $1`, f.tenantID)
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM agent_sessions WHERE tenant_id = $1`, f.tenantID)
	})

	session, err := st.CreateAgentSession(ctx, f.userID)
	if err != nil {
		t.Fatal(err)
	}
	id := "00000000-0000-0000-0000-000000000001"
	p := taskCreationCreateParams(f, id)
	p.SessionID = &session.ID
	if _, err := st.CreateTaskCreationOperation(ctx, p); err != nil {
		t.Fatal(err)
	}
	cancelParams := taskCreationCancelParams(
		id, f.tenantID, f.userID, "om_original_card")
	if _, err := st.CancelTaskCreationOperation(ctx, cancelParams); err != nil {
		t.Fatal(err)
	}
	if _, err := st.CancelTaskCreationOperation(ctx, cancelParams); err != nil {
		t.Fatalf("terminal replay must adopt existing receipt: %v", err)
	}
	differentTarget := cancelParams
	differentTarget.ReceiptTarget = "om_different"
	if _, err := st.CancelTaskCreationOperation(ctx, differentTarget); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("different target must conflict: %v", err)
	}

	receipt, err := st.LoadTaskCreationReceiptByOperation(ctx, id, f.tenantID, f.userID)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.ProviderKey != "970fcdc2-4e2e-694c-7448-12cdb3dedb7a" ||
		receipt.Provider != "feishu_message_patch" || receipt.Target != "om_original_card" ||
		receipt.Status != types.TaskCreationReceiptStatusPending ||
		receipt.SessionID == nil || *receipt.SessionID != session.ID ||
		receipt.OperationStatus != types.PendingActionStatusCancelled ||
		receipt.OperationPhase != types.TaskCreationPhaseCancelled {
		t.Fatalf("receipt mismatch: %+v", receipt)
	}
	var count int
	if err := st.pool.QueryRow(ctx,
		`SELECT count(*) FROM task_creation_receipts WHERE operation_id = $1`, id,
	).Scan(&count); err != nil || count != 1 {
		t.Fatalf("terminal replay must retain exactly one receipt: count=%d err=%v", count, err)
	}

	secondID := "00000000-0000-0000-0000-000000000002"
	second := taskCreationCreateParams(f, secondID)
	if _, err := st.CreateTaskCreationOperation(ctx, second); err != nil {
		t.Fatal(err)
	}
	if _, err := st.CancelTaskCreationOperation(ctx,
		taskCreationCancelParams(secondID, f.tenantID, f.userID, "om_second")); err != nil {
		t.Fatal(err)
	}
	secondReceipt, err := st.LoadTaskCreationReceiptByOperation(
		ctx, secondID, f.tenantID, f.userID)
	if err != nil {
		t.Fatal(err)
	}
	if secondReceipt.ProviderKey == receipt.ProviderKey {
		t.Fatal("different operations must have different provider keys")
	}
}

func TestTaskCreationReceipt_AcquireBindsTargetAtomically(t *testing.T) {
	st := tenantTestStore(t)
	f := newCompiledTaskFixture(t, st)
	ctx := t.Context()
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM task_creation_receipts WHERE tenant_id = $1`, f.tenantID)
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM pending_actions WHERE tenant_id = $1`, f.tenantID)
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM agent_sessions WHERE tenant_id = $1`, f.tenantID)
	})

	p := taskCreationCreateParams(f, uuid.NewString())
	if _, err := st.CreateTaskCreationOperation(ctx, p); err != nil {
		t.Fatal(err)
	}
	acquire := types.AcquireTaskCreationOperationParams{
		ID: p.ID, TenantID: f.tenantID, UserID: f.userID,
		LeaseOwner: "atomic-bind", LeaseDuration: time.Minute,
		ReceiptProvider: "feishu_message_patch", ReceiptTarget: "om_atomic",
	}
	op, err := st.AcquireTaskCreationOperation(ctx, acquire)
	if err != nil {
		t.Fatal(err)
	}
	if op.ReceiptProvider != acquire.ReceiptProvider || op.ReceiptTarget != acquire.ReceiptTarget {
		t.Fatalf("acquire did not persist receipt target: %+v", op)
	}
	replayed, err := st.AcquireTaskCreationOperation(ctx, acquire)
	if err != nil || replayed.Fence != op.Fence {
		t.Fatalf("same owner/target replay must adopt: op=%+v err=%v", replayed, err)
	}
	different := acquire
	different.ReceiptTarget = "om_different"
	if _, err := st.AcquireTaskCreationOperation(ctx, different); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("same operation cannot change receipt target: %v", err)
	}
}

func TestTaskCreationReceipt_LegacyUnboundExecutingOperationConverges(t *testing.T) {
	st := tenantTestStore(t)
	f := newCompiledTaskFixture(t, st)
	ctx := t.Context()
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM task_creation_receipts WHERE tenant_id = $1`, f.tenantID)
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM pending_actions WHERE tenant_id = $1`, f.tenantID)
	})

	createLegacyExecuting := func(t *testing.T, stale bool) types.TaskCreationOperation {
		t.Helper()
		p := taskCreationCreateParams(f, uuid.NewString())
		if _, err := st.CreateTaskCreationOperation(ctx, p); err != nil {
			t.Fatal(err)
		}
		op, err := st.AcquireTaskCreationOperation(ctx,
			types.AcquireTaskCreationOperationParams{
				ID: p.ID, TenantID: f.tenantID, UserID: f.userID,
				LeaseOwner: "pre-a6", LeaseDuration: time.Minute,
				ReceiptProvider: "feishu_message_patch", ReceiptTarget: "om_pre_a6",
			})
		if err != nil {
			t.Fatal(err)
		}
		staleLease := ""
		if stale {
			staleLease = `,
			       lease_until = clock_timestamp() - interval '2 seconds',
			       takeover_not_before = clock_timestamp() - interval '1 second'`
		}
		if _, err := st.pool.Exec(ctx, `
			UPDATE pending_actions
			   SET receipt_provider = '', receipt_target = ''`+staleLease+`
			 WHERE id = $1`, p.ID); err != nil {
			t.Fatal(err)
		}
		return *op
	}

	t.Run("background recovery remains fail closed", func(t *testing.T) {
		legacy := createLegacyExecuting(t, true)
		recovered, err := st.AcquireTaskCreationOperation(ctx,
			types.AcquireTaskCreationOperationParams{
				ID: legacy.ID, TenantID: f.tenantID, UserID: f.userID,
				LeaseOwner: "a6-recovery", LeaseDuration: time.Minute,
			})
		if err != nil {
			t.Fatalf("unbound pre-A6 operation must remain recoverable: %v", err)
		}
		if recovered.ReceiptProvider != "" || recovered.ReceiptTarget != "" {
			t.Fatalf("background recovery invented a target: %+v", recovered)
		}
		if err := st.FailTaskCreationOperation(
			ctx, recovered.Lease(), "LEGACY_RECOVERY", "pre-A6 operation converged",
		); err != nil {
			t.Fatal(err)
		}
		receipt, err := st.LoadTaskCreationReceiptByOperation(
			ctx, legacy.ID, f.tenantID, f.userID)
		if err != nil {
			t.Fatal(err)
		}
		if receipt.Status != types.TaskCreationReceiptStatusBlocked ||
			receipt.FailureClass != types.TaskCreationReceiptFailureTargetUnbound ||
			receipt.Provider != "" || receipt.Target != "" {
			t.Fatalf("unbound legacy terminal must be audit-only: %+v", receipt)
		}
	})

	t.Run("a new click binds the original card during takeover", func(t *testing.T) {
		legacy := createLegacyExecuting(t, true)
		rebound, err := st.AcquireTaskCreationOperation(ctx,
			types.AcquireTaskCreationOperationParams{
				ID: legacy.ID, TenantID: f.tenantID, UserID: f.userID,
				LeaseOwner: "a6-click", LeaseDuration: time.Minute,
				ReceiptProvider: "feishu_message_patch", ReceiptTarget: "om_rebound",
			})
		if err != nil {
			t.Fatalf("user click should bind a legacy operation: %v", err)
		}
		if rebound.ReceiptProvider != "feishu_message_patch" ||
			rebound.ReceiptTarget != "om_rebound" {
			t.Fatalf("legacy takeover did not bind the user target: %+v", rebound)
		}
	})

	t.Run("a click binds while the legacy worker still owns its lease", func(t *testing.T) {
		legacy := createLegacyExecuting(t, false)
		clicked, err := st.AcquireTaskCreationOperation(ctx,
			types.AcquireTaskCreationOperationParams{
				ID: legacy.ID, TenantID: f.tenantID, UserID: f.userID,
				LeaseOwner: "a6-active-click", LeaseDuration: time.Minute,
				ReceiptProvider: "feishu_message_patch", ReceiptTarget: "om_active_rebound",
			})
		if !errors.Is(err, types.ErrTaskCreationBusy) {
			t.Fatalf("active legacy worker should remain fenced: op=%+v err=%v", clicked, err)
		}
		if clicked == nil || clicked.ReceiptProvider != "feishu_message_patch" ||
			clicked.ReceiptTarget != "om_active_rebound" {
			t.Fatalf("busy click did not durably bind its target: %+v", clicked)
		}
		loaded, err := st.LoadTaskCreationOperation(
			ctx, legacy.ID, f.tenantID, f.userID)
		if err != nil {
			t.Fatal(err)
		}
		if loaded.ReceiptProvider != clicked.ReceiptProvider ||
			loaded.ReceiptTarget != clicked.ReceiptTarget {
			t.Fatalf("busy receipt binding was rolled back: %+v", loaded)
		}
		if _, err := st.pool.Exec(ctx, `
			UPDATE pending_actions
			   SET lease_until = clock_timestamp() - interval '2 seconds',
			       takeover_not_before = clock_timestamp() - interval '1 second'
			 WHERE id = $1`, legacy.ID); err != nil {
			t.Fatal(err)
		}
		recovered, err := st.AcquireTaskCreationOperation(ctx,
			types.AcquireTaskCreationOperationParams{
				ID: legacy.ID, TenantID: f.tenantID, UserID: f.userID,
				LeaseOwner: "a6-after-click-recovery", LeaseDuration: time.Minute,
				ReceiptProvider: loaded.ReceiptProvider, ReceiptTarget: loaded.ReceiptTarget,
			})
		if err != nil {
			t.Fatal(err)
		}
		if err := st.FailTaskCreationOperation(
			ctx, recovered.Lease(), "LEGACY_RECOVERY", "pre-A6 operation converged",
		); err != nil {
			t.Fatal(err)
		}
		receipt, err := st.LoadTaskCreationReceiptByOperation(
			ctx, legacy.ID, f.tenantID, f.userID)
		if err != nil {
			t.Fatal(err)
		}
		if receipt.Status != types.TaskCreationReceiptStatusPending ||
			receipt.Provider != loaded.ReceiptProvider || receipt.Target != loaded.ReceiptTarget {
			t.Fatalf("bound legacy target did not reach the durable outbox: %+v", receipt)
		}
	})

	t.Run("a cancel click binds while the legacy worker still owns its lease", func(t *testing.T) {
		legacy := createLegacyExecuting(t, false)
		clicked, err := st.CancelTaskCreationOperation(ctx,
			types.CancelTaskCreationOperationParams{
				ID: legacy.ID, TenantID: f.tenantID, UserID: f.userID,
				ReceiptProvider: "feishu_message_patch", ReceiptTarget: "om_cancel_rebound",
			})
		if !errors.Is(err, types.ErrTaskCreationBusy) {
			t.Fatalf("active legacy worker should remain fenced: op=%+v err=%v", clicked, err)
		}
		if clicked == nil || clicked.ReceiptProvider != "feishu_message_patch" ||
			clicked.ReceiptTarget != "om_cancel_rebound" {
			t.Fatalf("busy cancel did not durably bind its target: %+v", clicked)
		}
		loaded, err := st.LoadTaskCreationOperation(
			ctx, legacy.ID, f.tenantID, f.userID)
		if err != nil {
			t.Fatal(err)
		}
		if loaded.ReceiptProvider != clicked.ReceiptProvider ||
			loaded.ReceiptTarget != clicked.ReceiptTarget {
			t.Fatalf("busy cancellation receipt binding was rolled back: %+v", loaded)
		}
	})
}

func TestTaskCreationReceipt_LeasePayloadSessionAndDeliveryLifecycle(t *testing.T) {
	st := tenantTestStore(t)
	f := newCompiledTaskFixture(t, st)
	ctx := t.Context()
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM task_creation_receipts WHERE tenant_id = $1`, f.tenantID)
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM pending_actions WHERE tenant_id = $1`, f.tenantID)
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM agent_events WHERE tenant_id = $1`, f.tenantID)
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM agent_sessions WHERE tenant_id = $1`, f.tenantID)
	})

	session, err := st.CreateAgentSession(ctx, f.userID)
	if err != nil {
		t.Fatal(err)
	}
	initializeEmptyAgentSessionLedgerAuthority(
		t, st, agentledger.Scope{
			TenantID: f.tenantID, UserID: f.userID, SessionID: session.ID,
		}, "creation-receipt-ledger-authority",
	)
	p := taskCreationCreateParams(f, uuid.NewString())
	p.SessionID = &session.ID
	if _, err := st.CreateTaskCreationOperation(ctx, p); err != nil {
		t.Fatal(err)
	}
	if _, err := st.CancelTaskCreationOperation(ctx,
		taskCreationCancelParams(p.ID, f.tenantID, f.userID, "om_lifecycle")); err != nil {
		t.Fatal(err)
	}
	receipt, err := st.LoadTaskCreationReceiptByOperation(ctx, p.ID, f.tenantID, f.userID)
	if err != nil {
		t.Fatal(err)
	}

	due, err := st.ListDueTaskCreationReceipts(ctx, f.tenantID, time.Now().Add(time.Hour), 100)
	if err != nil || containsTaskCreationReceipt(due, receipt.ID) {
		t.Fatalf("new receipt must respect callback-response delay: receipts=%+v err=%v", due, err)
	}
	if receipt.NextAttemptAt.Sub(receipt.CreatedAt) < 3*time.Second {
		t.Fatalf("receipt delay shorter than callback safety window: created=%v next=%v",
			receipt.CreatedAt, receipt.NextAttemptAt)
	}
	if _, err := st.pool.Exec(ctx,
		`UPDATE task_creation_receipts SET next_attempt_at = clock_timestamp() - interval '1 second'
		  WHERE id = $1`, receipt.ID); err != nil {
		t.Fatal(err)
	}
	claimed, err := st.AcquireTaskCreationReceipt(ctx, types.AcquireTaskCreationReceiptParams{
		ID: receipt.ID, TenantID: f.tenantID, UserID: f.userID,
		LeaseOwner: "receipt-worker-1", LeaseDuration: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if claimed.Fence != 1 || claimed.Attempt != 1 {
		t.Fatalf("first claim mismatch: %+v", claimed)
	}
	if _, err := st.AcquireTaskCreationReceipt(ctx, types.AcquireTaskCreationReceiptParams{
		ID: receipt.ID, TenantID: f.tenantID, UserID: f.userID,
		LeaseOwner: "receipt-worker-2", LeaseDuration: time.Minute,
	}); !errors.Is(err, types.ErrTaskCreationReceiptBusy) {
		t.Fatalf("concurrent owner must be busy: %v", err)
	}

	payload := []byte("{\n  \"card\": \"patched\"\n}")
	digest := sha256.Sum256(payload)
	digestHex := hex.EncodeToString(digest[:])
	if err := st.CheckpointTaskCreationReceiptPayload(
		ctx, claimed.Lease(), payload, digestHex); err != nil {
		t.Fatal(err)
	}
	if err := st.CheckpointTaskCreationReceiptPayload(
		ctx, claimed.Lease(), payload, digestHex); err != nil {
		t.Fatalf("payload exact replay: %v", err)
	}
	if err := st.CheckpointTaskCreationReceiptPayload(
		ctx, claimed.Lease(), []byte(`{"different":true}`),
		digestHex); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("payload/digest mismatch must validate: %v", err)
	}

	messages := json.RawMessage(`[{"role":"user","content":"[卡片回调] 已取消任务"}]`)
	lostStore := storeWithCommitResponseLost(st)
	if err := lostStore.RecordTaskCreationReceiptSessionMessages(
		ctx, claimed.Lease(), messages); !errors.Is(err, types.ErrDatabase) {
		t.Fatalf("session commit response loss must surface database error: %v", err)
	}
	if err := st.RecordTaskCreationReceiptSessionMessages(
		ctx, claimed.Lease(), messages); err != nil {
		t.Fatalf("session response-lost exact replay: %v", err)
	}
	var recordedMessages []map[string]any
	if err := st.pool.QueryRow(ctx,
		`SELECT messages FROM agent_sessions WHERE id = $1`, session.ID,
	).Scan(&recordedMessages); err != nil {
		t.Fatal(err)
	}
	if len(recordedMessages) != 1 {
		t.Fatalf("session messages must append exactly once: %+v", recordedMessages)
	}
	var creationEventCount int
	if err := st.pool.QueryRow(ctx,
		`SELECT count(*)
		   FROM agent_events
		  WHERE tenant_id=$1 AND user_id=$2 AND session_id=$3
		    AND batch_idempotency_key LIKE 'side.%'`,
		f.tenantID, f.userID, session.ID,
	).Scan(&creationEventCount); err != nil {
		t.Fatal(err)
	}
	if creationEventCount != 3 {
		t.Fatalf("creation receipt snapshot event count=%d want=3",
			creationEventCount)
	}
	if err := lostStore.MarkTaskCreationReceiptSent(
		ctx, claimed.Lease(), "om_lifecycle"); !errors.Is(err, types.ErrDatabase) {
		t.Fatalf("sent commit response loss must surface database error: %v", err)
	}
	if err := st.MarkTaskCreationReceiptSent(
		ctx, claimed.Lease(), "om_lifecycle"); err != nil {
		t.Fatalf("sent response-lost exact replay: %v", err)
	}
	if _, err := st.AcquireTaskCreationReceipt(ctx, types.AcquireTaskCreationReceiptParams{
		ID: receipt.ID, TenantID: f.tenantID, UserID: f.userID,
		LeaseOwner: "receipt-worker-3", LeaseDuration: time.Minute,
	}); !errors.Is(err, types.ErrTaskCreationReceiptTerminal) {
		t.Fatalf("sent receipt cannot be reacquired: %v", err)
	}
}

func TestTaskCreationReceiptSessionCheckpointUsesTenantScopedRuntimeRole(
	t *testing.T,
) {
	st := tenantTestStore(t)
	f := newCompiledTaskFixture(t, st)
	ctx := t.Context()
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM task_creation_receipts WHERE tenant_id=$1`,
			f.tenantID)
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM pending_actions WHERE tenant_id=$1`,
			f.tenantID)
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM agent_events WHERE tenant_id=$1`,
			f.tenantID)
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM agent_sessions WHERE tenant_id=$1`,
			f.tenantID)
	})

	session, err := st.CreateAgentSession(ctx, f.userID)
	if err != nil {
		t.Fatal(err)
	}
	params := taskCreationCreateParams(f, uuid.NewString())
	params.SessionID = &session.ID
	if _, err := st.CreateTaskCreationOperation(ctx, params); err != nil {
		t.Fatal(err)
	}
	if _, err := st.CancelTaskCreationOperation(
		ctx,
		taskCreationCancelParams(
			params.ID, f.tenantID, f.userID, "om_runtime_scope",
		),
	); err != nil {
		t.Fatal(err)
	}
	receipt, err := st.LoadTaskCreationReceiptByOperation(
		ctx, params.ID, f.tenantID, f.userID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(ctx, `
		UPDATE task_creation_receipts
		   SET next_attempt_at=clock_timestamp()-interval '1 second'
		 WHERE id=$1`, receipt.ID); err != nil {
		t.Fatal(err)
	}
	receipt, err = st.AcquireTaskCreationReceipt(
		ctx,
		types.AcquireTaskCreationReceiptParams{
			ID: receipt.ID, TenantID: f.tenantID, UserID: f.userID,
			LeaseOwner: "runtime-scope-worker", LeaseDuration: time.Minute,
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	scopedStore := *st
	var audited *taskCreationReceiptRuntimeScopeTx
	scopedStore.beginTx = func(
		ctx context.Context,
		options pgx.TxOptions,
	) (pgx.Tx, error) {
		tx, err := st.pool.BeginTx(ctx, options)
		if err != nil {
			return nil, err
		}
		audited = &taskCreationReceiptRuntimeScopeTx{
			Tx: tx,
		}
		return audited, nil
	}
	messages := json.RawMessage(
		`[{"role":"user","content":"runtime scoped receipt"}]`,
	)
	if err := scopedStore.RecordTaskCreationReceiptSessionMessages(
		ctx, receipt.Lease(), messages,
	); err != nil {
		t.Fatal(err)
	}
	if audited == nil || !audited.receiptLockObserved ||
		audited.role != "vane_app" ||
		audited.tenantSetting != fmt.Sprintf("%d", f.tenantID) ||
		audited.scopeErr != nil {
		t.Fatalf(
			"receipt root lock scope role=%q tenant=%q observed=%v err=%v",
			audited.role,
			audited.tenantSetting,
			audited.receiptLockObserved,
			audited.scopeErr,
		)
	}

	var (
		receiptRead, sessionMessageUpdate, eventPayloadInsert bool
		eventUpdate, eventDelete                              bool
	)
	if err := st.pool.QueryRow(ctx, `
		SELECT
		  has_table_privilege(
		    'vane_app','task_creation_receipts','SELECT'),
		  has_column_privilege(
		    'vane_app','agent_sessions','messages','UPDATE'),
		  has_column_privilege(
		    'vane_app','agent_events','payload','INSERT'),
		  has_table_privilege(
		    'vane_app','agent_events','UPDATE'),
		  has_table_privilege(
		    'vane_app','agent_events','DELETE')`,
	).Scan(
		&receiptRead,
		&sessionMessageUpdate,
		&eventPayloadInsert,
		&eventUpdate,
		&eventDelete,
	); err != nil {
		t.Fatal(err)
	}
	if !receiptRead || !sessionMessageUpdate || !eventPayloadInsert ||
		eventUpdate || eventDelete {
		t.Fatalf(
			"vane_app receipt capability drift receipt_read=%v "+
				"session_update=%v event_insert/update/delete=%v/%v/%v",
			receiptRead,
			sessionMessageUpdate,
			eventPayloadInsert,
			eventUpdate,
			eventDelete,
		)
	}

	wrongScope := receipt.Lease()
	wrongScope.TenantID += 999
	if err := st.RecordTaskCreationReceiptSessionMessages(
		ctx, wrongScope, messages,
	); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("cross-tenant receipt checkpoint error=%v, want not found", err)
	}
}

type taskCreationReceiptRuntimeScopeTx struct {
	pgx.Tx
	receiptLockObserved bool
	role                string
	tenantSetting       string
	scopeErr            error
}

func (tx *taskCreationReceiptRuntimeScopeTx) QueryRow(
	ctx context.Context,
	sql string,
	args ...any,
) pgx.Row {
	if !tx.receiptLockObserved &&
		strings.Contains(sql, "FROM task_creation_receipts r") &&
		strings.Contains(sql, "FOR UPDATE OF r") {
		tx.receiptLockObserved = true
		tx.scopeErr = tx.Tx.QueryRow(ctx, `
			SELECT current_user,
			       current_setting('app.tenant_id', true)`,
		).Scan(&tx.role, &tx.tenantSetting)
	}
	return tx.Tx.QueryRow(ctx, sql, args...)
}

func TestTaskCreationReceipt_SendFailureRetryTakeoverAndPermanentBlock(t *testing.T) {
	st := tenantTestStore(t)
	f := newCompiledTaskFixture(t, st)
	ctx := t.Context()
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM task_creation_receipts WHERE tenant_id = $1`, f.tenantID)
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM pending_actions WHERE tenant_id = $1`, f.tenantID)
	})

	p := taskCreationCreateParams(f, uuid.NewString())
	if _, err := st.CreateTaskCreationOperation(ctx, p); err != nil {
		t.Fatal(err)
	}
	if _, err := st.CancelTaskCreationOperation(ctx,
		taskCreationCancelParams(p.ID, f.tenantID, f.userID, "om_failure")); err != nil {
		t.Fatal(err)
	}
	receipt, err := st.LoadTaskCreationReceiptByOperation(ctx, p.ID, f.tenantID, f.userID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(ctx,
		`UPDATE task_creation_receipts SET next_attempt_at = clock_timestamp() - interval '1 second'
		  WHERE id = $1`, receipt.ID); err != nil {
		t.Fatal(err)
	}
	first, err := st.AcquireTaskCreationReceipt(ctx, types.AcquireTaskCreationReceiptParams{
		ID: receipt.ID, TenantID: f.tenantID, UserID: f.userID,
		LeaseOwner: "failure-worker-1", LeaseDuration: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.RecordTaskCreationReceiptSendFailure(ctx,
		types.RecordTaskCreationReceiptSendFailureParams{
			Lease: first.Lease(), Class: types.TaskCreationReceiptFailureRetryable,
			RetryAfter: time.Hour,
		}); err != nil {
		t.Fatal(err)
	}
	due, err := st.ListDueTaskCreationReceipts(ctx, f.tenantID, time.Now().Add(2*time.Hour), 100)
	if err != nil || containsTaskCreationReceipt(due, receipt.ID) {
		t.Fatalf("caller future time cannot bypass DB next_attempt: due=%+v err=%v", due, err)
	}
	if _, err := st.pool.Exec(ctx,
		`UPDATE task_creation_receipts SET next_attempt_at = clock_timestamp() - interval '1 second'
		  WHERE id = $1`, receipt.ID); err != nil {
		t.Fatal(err)
	}
	second, err := st.AcquireTaskCreationReceipt(ctx, types.AcquireTaskCreationReceiptParams{
		ID: receipt.ID, TenantID: f.tenantID, UserID: f.userID,
		LeaseOwner: "failure-worker-2", LeaseDuration: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if second.Fence != first.Fence+1 || second.Attempt != first.Attempt+1 {
		t.Fatalf("retry claim must advance fence/attempt: first=%+v second=%+v", first, second)
	}
	if err := st.RecordTaskCreationReceiptSendFailure(ctx,
		types.RecordTaskCreationReceiptSendFailureParams{
			Lease: second.Lease(), Class: types.TaskCreationReceiptFailurePermanent,
		}); err != nil {
		t.Fatal(err)
	}
	blocked, err := st.LoadTaskCreationReceiptByOperation(ctx, p.ID, f.tenantID, f.userID)
	if err != nil {
		t.Fatal(err)
	}
	if blocked.Status != types.TaskCreationReceiptStatusBlocked || blocked.BlockedAt == nil {
		t.Fatalf("permanent failure must block: %+v", blocked)
	}
}

func TestTaskCreationReceipt_ConcurrentClaimAndStaleTakeover(t *testing.T) {
	st := tenantTestStore(t)
	f := newCompiledTaskFixture(t, st)
	ctx := t.Context()
	st2, err := New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	registerStoreClose(t, st2)
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM task_creation_receipts WHERE tenant_id = $1`, f.tenantID)
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM pending_actions WHERE tenant_id = $1`, f.tenantID)
	})

	p := taskCreationCreateParams(f, uuid.NewString())
	if _, err := st.CreateTaskCreationOperation(ctx, p); err != nil {
		t.Fatal(err)
	}
	if _, err := st.CancelTaskCreationOperation(ctx,
		taskCreationCancelParams(p.ID, f.tenantID, f.userID, "om-race-receipt")); err != nil {
		t.Fatal(err)
	}
	receipt, err := st.LoadTaskCreationReceiptByOperation(ctx, p.ID, f.tenantID, f.userID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(ctx,
		`UPDATE task_creation_receipts SET next_attempt_at = clock_timestamp() - interval '1 second'
		  WHERE id = $1`, receipt.ID); err != nil {
		t.Fatal(err)
	}

	first := raceTaskCreationReceiptAcquire(t, ctx, st, st2, receipt, "first")
	if first.Fence != 1 || first.Attempt != 1 {
		t.Fatalf("first concurrent winner mismatch: %+v", first)
	}
	if _, err := st.pool.Exec(ctx, `
		UPDATE task_creation_receipts
		   SET lease_until = clock_timestamp() - interval '2 seconds',
		       takeover_not_before = clock_timestamp() - interval '1 second'
		 WHERE id = $1`, receipt.ID); err != nil {
		t.Fatal(err)
	}
	second := raceTaskCreationReceiptAcquire(t, ctx, st, st2, receipt, "takeover")
	if second.Fence != 2 || second.Attempt != 2 || second.LeaseOwner == first.LeaseOwner {
		t.Fatalf("stale takeover must have one new fenced winner: first=%+v second=%+v",
			first, second)
	}
}

func TestTaskCreationReceipt_MissingSessionDegradesWithoutBlockingDelivery(t *testing.T) {
	st := tenantTestStore(t)
	f := newCompiledTaskFixture(t, st)
	ctx := t.Context()
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM task_creation_receipts WHERE tenant_id = $1`, f.tenantID)
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM pending_actions WHERE tenant_id = $1`, f.tenantID)
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM agent_sessions WHERE tenant_id = $1`, f.tenantID)
	})

	session, err := st.CreateAgentSession(ctx, f.userID)
	if err != nil {
		t.Fatal(err)
	}
	p := taskCreationCreateParams(f, uuid.NewString())
	p.SessionID = &session.ID
	if _, err := st.CreateTaskCreationOperation(ctx, p); err != nil {
		t.Fatal(err)
	}
	if _, err := st.CancelTaskCreationOperation(ctx,
		taskCreationCancelParams(p.ID, f.tenantID, f.userID, "om-session-gone")); err != nil {
		t.Fatal(err)
	}
	// pending_actions is the long-lived operation audit and currently also owns
	// a session FK. Simulate its retention policy releasing that optional link;
	// the receipt FK itself must not keep the session alive.
	if _, err := st.pool.Exec(ctx,
		`UPDATE pending_actions SET session_id = NULL WHERE id = $1`, p.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(ctx,
		`DELETE FROM agent_sessions WHERE id = $1`, session.ID); err != nil {
		t.Fatalf("receipt session FK must use ON DELETE SET NULL: %v", err)
	}
	receipt, err := st.LoadTaskCreationReceiptByOperation(ctx, p.ID, f.tenantID, f.userID)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.SessionID != nil {
		t.Fatalf("deleted session link was retained: %+v", receipt)
	}
	if _, err := st.pool.Exec(ctx,
		`UPDATE task_creation_receipts SET next_attempt_at = clock_timestamp() - interval '1 second'
		  WHERE id = $1`, receipt.ID); err != nil {
		t.Fatal(err)
	}
	claimed, err := st.AcquireTaskCreationReceipt(ctx, types.AcquireTaskCreationReceiptParams{
		ID: receipt.ID, TenantID: f.tenantID, UserID: f.userID,
		LeaseOwner: "missing-session", LeaseDuration: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	messages := json.RawMessage(`[{"role":"user","content":"receipt state"}]`)
	if err := st.RecordTaskCreationReceiptSessionMessages(
		ctx, claimed.Lease(), messages); err != nil {
		t.Fatalf("missing session must degrade to durable marker: %v", err)
	}
	loaded, err := st.LoadTaskCreationReceiptByOperation(ctx, p.ID, f.tenantID, f.userID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.SessionRecordedAt == nil || loaded.SessionMessagesDigest == "" {
		t.Fatalf("missing session did not checkpoint messages: %+v", loaded)
	}
}

func TestTaskCreationReceipt_CrossScopeAndTerminationRollback(t *testing.T) {
	st := tenantTestStore(t)
	f := newCompiledTaskFixture(t, st)
	ctx := t.Context()
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM task_creation_receipts WHERE tenant_id = $1`, f.tenantID)
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM pending_actions WHERE tenant_id = $1`, f.tenantID)
	})

	p := taskCreationCreateParams(f, uuid.NewString())
	if _, err := st.CreateTaskCreationOperation(ctx, p); err != nil {
		t.Fatal(err)
	}
	if _, err := st.CancelTaskCreationOperation(ctx,
		taskCreationCancelParams(p.ID, f.tenantID, f.userID+1, "om_wrong")); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("cross-user cancel must be not found: %v", err)
	}

	rollbackParams := taskCreationCreateParams(f, uuid.NewString())
	if _, err := st.CreateTaskCreationOperation(ctx, rollbackParams); err != nil {
		t.Fatal(err)
	}
	faultStore := *st
	faultStore.beginTx = func(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error) {
		realTx, err := st.pool.BeginTx(ctx, opts)
		if err != nil {
			return nil, err
		}
		return &compiledTaskFaultTx{
			Tx: realTx, failContains: "INSERT INTO task_creation_receipts",
		}, nil
	}
	if _, err := faultStore.CancelTaskCreationOperation(ctx,
		taskCreationCancelParams(
			rollbackParams.ID, f.tenantID, f.userID, "om-insert-fault"),
	); !errors.Is(err, types.ErrDatabase) {
		t.Fatalf("receipt insert fault must abort terminal transaction: %v", err)
	}
	var pendingRows, rollbackReceiptRows int
	if err := st.pool.QueryRow(ctx, `
		SELECT
		  (SELECT count(*) FROM pending_actions
		    WHERE id = $1 AND status = 'pending' AND tombstoned_at IS NULL),
		  (SELECT count(*) FROM task_creation_receipts WHERE operation_id = $1)`,
		rollbackParams.ID,
	).Scan(&pendingRows, &rollbackReceiptRows); err != nil {
		t.Fatal(err)
	}
	if pendingRows != 1 || rollbackReceiptRows != 0 {
		t.Fatalf("receipt failure must roll back tombstone: pending=%d receipts=%d",
			pendingRows, rollbackReceiptRows)
	}

	fault := storeWithCommitResponseLost(st)
	cancelParams := taskCreationCancelParams(p.ID, f.tenantID, f.userID, "om_rollback")
	if _, err := fault.CancelTaskCreationOperation(ctx, cancelParams); !errors.Is(err, types.ErrDatabase) {
		t.Fatalf("lost commit response must surface database error: %v", err)
	}
	// The transaction did commit; retry must exact-adopt both tombstone and receipt.
	if _, err := st.CancelTaskCreationOperation(ctx, cancelParams); err != nil {
		t.Fatalf("response-lost retry must exact-adopt: %v", err)
	}
	var operationRows, receiptRows int
	if err := st.pool.QueryRow(ctx, `
		SELECT
		  (SELECT count(*) FROM pending_actions WHERE id = $1 AND tombstoned_at IS NOT NULL),
		  (SELECT count(*) FROM task_creation_receipts WHERE operation_id = $1)`, p.ID,
	).Scan(&operationRows, &receiptRows); err != nil {
		t.Fatal(err)
	}
	if operationRows != 1 || receiptRows != 1 {
		t.Fatalf("terminal/receipt must commit together: operation=%d receipt=%d",
			operationRows, receiptRows)
	}

	loaded, err := st.LoadTaskCreationReceiptByOperation(ctx, p.ID, f.tenantID, f.userID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.LoadTaskCreationReceiptByOperation(
		ctx, p.ID, f.tenantID+999, f.userID); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("cross-tenant load must be not found: %v", err)
	}
	if _, err := st.AcquireTaskCreationReceipt(ctx, types.AcquireTaskCreationReceiptParams{
		ID: loaded.ID, TenantID: f.tenantID, UserID: f.userID + 999,
		LeaseOwner: "wrong-scope", LeaseDuration: time.Minute,
	}); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("cross-user acquire must be not found: %v", err)
	}

	if os.Getenv("DATABASE_URL") == "" {
		t.Fatal("tenantTestStore should have skipped without DATABASE_URL")
	}
}

func containsTaskCreationReceipt(receipts []types.TaskCreationReceipt, id int64) bool {
	for _, receipt := range receipts {
		if receipt.ID == id {
			return true
		}
	}
	return false
}

func assertTaskCreationReceiptExactlyOne(t *testing.T, st *Store, operationID string) {
	t.Helper()
	var count int
	if err := st.pool.QueryRow(t.Context(),
		`SELECT count(*) FROM task_creation_receipts WHERE operation_id = $1`, operationID,
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("terminal operation %s must have exactly one durable receipt, got %d",
			operationID, count)
	}
}

func taskCreationCancelParams(
	id string,
	tenantID int64,
	userID int64,
	target string,
) types.CancelTaskCreationOperationParams {
	return types.CancelTaskCreationOperationParams{
		ID: id, TenantID: tenantID, UserID: userID,
		ReceiptProvider: "feishu_message_patch", ReceiptTarget: target,
	}
}

func raceTaskCreationReceiptAcquire(
	t *testing.T,
	ctx context.Context,
	st1 *Store,
	st2 *Store,
	receipt *types.TaskCreationReceipt,
	prefix string,
) *types.TaskCreationReceipt {
	t.Helper()
	type outcome struct {
		receipt *types.TaskCreationReceipt
		err     error
	}
	stores := []*Store{st1, st2}
	start := make(chan struct{})
	results := make(chan outcome, len(stores))
	var wg sync.WaitGroup
	for i, st := range stores {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			claimed, err := st.AcquireTaskCreationReceipt(ctx,
				types.AcquireTaskCreationReceiptParams{
					ID: receipt.ID, TenantID: receipt.TenantID, UserID: receipt.UserID,
					LeaseOwner:    prefix + "-" + string(rune('a'+i)),
					LeaseDuration: time.Minute,
				})
			results <- outcome{receipt: claimed, err: err}
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	var winner *types.TaskCreationReceipt
	busy := 0
	for result := range results {
		switch {
		case result.err == nil:
			if winner != nil {
				t.Fatalf("multiple concurrent receipt owners: first=%+v second=%+v",
					winner, result.receipt)
			}
			winner = result.receipt
		case errors.Is(result.err, types.ErrTaskCreationReceiptBusy):
			busy++
		default:
			t.Fatalf("unexpected concurrent receipt acquire error: %v", result.err)
		}
	}
	if winner == nil || busy != 1 {
		t.Fatalf("concurrent receipt acquire must have one winner/one busy: winner=%+v busy=%d",
			winner, busy)
	}
	return winner
}
