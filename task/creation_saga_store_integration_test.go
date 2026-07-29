package task

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
)

// completeResponseLossStore keeps every coordinator checkpoint in real
// PostgreSQL, but loses the response after Complete has committed. Cancelling
// the caller at the same boundary proves that convergence uses a detached
// readback instead of accidentally succeeding through the request context.
type completeResponseLossStore struct {
	*store.Store
	cancel        context.CancelFunc
	completeCalls int
}

type postgresReceiptSessionRecorder struct{ store *store.Store }

func (r postgresReceiptSessionRecorder) RecordCreationReceiptSession(
	ctx context.Context,
	receipt types.TaskCreationReceipt,
	messages json.RawMessage,
) error {
	return r.store.RecordTaskCreationReceiptSessionMessages(
		ctx, receipt.Lease(), messages,
	)
}

func (s *completeResponseLossStore) CompleteTaskCreationOperation(
	ctx context.Context,
	lease types.TaskCreationLease,
	taskID string,
	result json.RawMessage,
) error {
	s.completeCalls++
	if err := s.Store.CompleteTaskCreationOperation(ctx, lease, taskID, result); err != nil {
		return err
	}
	s.cancel()
	return errors.New("complete committed but response was lost")
}

// TestCreationCoordinator_PostgreSQLRoundTrip exercises the complete A5 saga
// through a real Store. In particular, task_creation_operations.args and result are
// JSONB: PostgreSQL rewrites their object bytes, so raw-byte identity checks
// would reject both the first proposal and the terminal replay.
func TestCreationCoordinator_PostgreSQLRoundTrip(t *testing.T) {
	st, tenantID, userID := newCreationCoordinatorPostgreSQLFixture(t)

	t.Run("complete confirm and terminal replay survive JSONB rewrite", func(t *testing.T) {
		schedules := &creationSagaFakeScheduler{}
		coordinator := NewCreationCoordinator(st, schedules, nil)
		actionID := "task-create-jsonb-" + uuid.NewString()
		rawArgs := mustCreateProposalArgs(t, "每天寻找全球 AI 热点", "每天 AI")
		expiresAt := time.Now().Add(time.Hour)

		proposal, err := coordinator.Prepare(t.Context(), CreationProposalInput{
			ActionID: actionID, UserID: userID, RawArgs: rawArgs,
			ExpiresAt: expiresAt,
		})
		if err != nil {
			t.Fatalf("Propose() through PostgreSQL: %v", err)
		}
		if proposal.ID != actionID || proposal.Summary == "" {
			t.Fatalf("proposal = %+v", proposal)
		}

		persisted, err := st.LoadTaskCreationOperation(
			t.Context(), actionID, tenantID, userID,
		)
		if err != nil {
			t.Fatalf("LoadTaskCreationOperation() after proposal: %v", err)
		}
		command, _, err := normalizeCreateScheduleCommand(persisted.Args)
		if err != nil {
			t.Fatalf("normalize persisted fixture: %v", err)
		}
		canonicalArgs, err := canonicalCreationProposalArgs(command)
		if err != nil {
			t.Fatalf("canonicalize persisted fixture: %v", err)
		}
		if bytes.Equal(persisted.Args, canonicalArgs) {
			t.Fatalf("fixture did not exercise JSONB byte rewriting: %s", persisted.Args)
		}
		if !creationProposalArgsEqual(persisted.Args, canonicalArgs) {
			t.Fatalf("persisted args changed meaning: %s", persisted.Args)
		}

		result, err := coordinator.Execute(t.Context(), userID, actionID, testCreationReceiptTarget)
		if err != nil {
			t.Fatalf("Execute() through PostgreSQL: %v", err)
		}
		if result.Status != types.TaskOperationStatusExecuted || result.TaskID == "" ||
			result.Recovering || result.Replayed {
			t.Fatalf("first confirmation result = %+v", result)
		}

		terminal, err := st.LoadTaskCreationOperation(
			t.Context(), actionID, tenantID, userID,
		)
		if err != nil {
			t.Fatalf("LoadTaskCreationOperation() after completion: %v", err)
		}
		canonicalResult, err := marshalCreationSuccess(result.TaskID)
		if err != nil {
			t.Fatalf("marshal result fixture: %v", err)
		}
		if bytes.Equal(terminal.Result, canonicalResult) {
			t.Fatalf("fixture did not exercise terminal JSONB byte rewriting: %s", terminal.Result)
		}

		schedulerEvents := len(schedules.events)
		replayed, err := coordinator.Execute(t.Context(), userID, actionID, testCreationReceiptTarget)
		if err != nil {
			t.Fatalf("Execute() terminal replay: %v", err)
		}
		if replayed.Status != types.TaskOperationStatusExecuted || !replayed.Replayed ||
			replayed.TaskID != result.TaskID || replayed.Message != result.Message {
			t.Fatalf("terminal replay result = %+v; first = %+v", replayed, result)
		}
		if len(schedules.events) != schedulerEvents {
			t.Fatalf("terminal replay touched scheduler: before=%d after=%d events=%v",
				schedulerEvents, len(schedules.events), schedules.events)
		}

		// The same terminal transaction produced one delayed outbox row. A real
		// Store + dispatcher freezes the session fact and commits the receipt
		// without any live request state.
		receipt, err := st.LoadTaskCreationReceiptByOperation(
			t.Context(), actionID, tenantID, userID,
		)
		if err != nil {
			t.Fatalf("load terminal receipt: %v", err)
		}
		receipt = waitForDueTaskCreationReceipt(t, st, *receipt)
		dispatcher, err := NewCreationReceiptDispatcher(CreationReceiptDispatcherDeps{
			Store:    st,
			Sessions: postgresReceiptSessionRecorder{store: st},
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := dispatcher.dispatchReceipt(t.Context(), *receipt); err != nil {
			t.Fatalf("dispatch terminal receipt through PostgreSQL: %v", err)
		}
		receipt, err = st.LoadTaskCreationReceiptByOperation(
			t.Context(), actionID, tenantID, userID,
		)
		if err != nil || receipt.Status != types.TaskCreationReceiptStatusSent ||
			receipt.SessionRecordedAt == nil || receipt.ProviderMessageID != testCreationReceiptTarget.Target {
			t.Fatalf("terminal receipt did not converge: receipt=%+v err=%v", receipt, err)
		}
	})

	t.Run("complete response loss converges with detached readback", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		responseLoss := &completeResponseLossStore{Store: st, cancel: cancel}
		coordinator := NewCreationCoordinator(
			responseLoss, &creationSagaFakeScheduler{}, nil,
		)
		actionID := "task-create-complete-loss-" + uuid.NewString()
		if _, err := coordinator.Prepare(ctx, CreationProposalInput{
			ActionID: actionID, UserID: userID,
			RawArgs:   mustCreateProposalArgs(t, "每天监控 AI 模型发布", "AI 模型发布"),
			ExpiresAt: time.Now().Add(time.Hour),
		}); err != nil {
			t.Fatalf("Propose(): %v", err)
		}

		result, err := coordinator.Execute(ctx, userID, actionID, testCreationReceiptTarget)
		if err != nil {
			t.Fatalf("Execute() should adopt committed terminal row: %v", err)
		}
		if !errors.Is(ctx.Err(), context.Canceled) {
			t.Fatalf("response-loss boundary did not cancel request context: %v", ctx.Err())
		}
		if responseLoss.completeCalls != 1 ||
			result.Status != types.TaskOperationStatusExecuted || result.TaskID == "" ||
			result.Recovering || result.Replayed {
			t.Fatalf("result=%+v complete_calls=%d", result, responseLoss.completeCalls)
		}

		replayed, err := coordinator.Execute(t.Context(), userID, actionID, testCreationReceiptTarget)
		if err != nil {
			t.Fatalf("Execute() after adopted response loss: %v", err)
		}
		if !replayed.Replayed || replayed.Status != types.TaskOperationStatusExecuted ||
			replayed.TaskID != result.TaskID || responseLoss.completeCalls != 1 {
			t.Fatalf("replayed=%+v complete_calls=%d", replayed, responseLoss.completeCalls)
		}
	})
}

func newCreationCoordinatorPostgreSQLFixture(
	t *testing.T,
) (*store.Store, int64, int64) {
	t.Helper()
	dbURL := os.Getenv("VANE_TEST_DATABASE_URL")
	if dbURL == "" {
		dbURL = os.Getenv("DATABASE_URL")
	}
	if dbURL == "" {
		t.Skip("未设置 VANE_TEST_DATABASE_URL 或 DATABASE_URL，跳过 Coordinator 真库测试")
	}
	if err := store.Migrate(t.Context(), dbURL); err != nil {
		t.Fatalf("store.Migrate(): %v", err)
	}
	st, err := store.New(t.Context(), dbURL)
	if err != nil {
		t.Fatalf("store.New(): %v", err)
	}
	t.Cleanup(st.Close)

	user, err := st.UpsertUserByOpenID(
		t.Context(), "ou_task_creation_coordinator_"+uuid.NewString(),
		"A5 Coordinator PostgreSQL test",
	)
	if err != nil {
		t.Fatalf("create fixture user: %v", err)
	}
	inviteCode := "task-creation-coordinator-" + uuid.NewString()
	if _, err := st.IssueInvite(t.Context(), inviteCode, nil, 1, nil); err != nil {
		t.Fatalf("create fixture invite: %v", err)
	}
	tenant, err := st.CreateTenantWithInvite(t.Context(), inviteCode, user.ID)
	if err != nil {
		t.Fatalf("create fixture tenant: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if _, err := st.PurgeTenant(ctx, tenant.ID, false); err != nil {
			t.Errorf("purge fixture tenant %d: %v", tenant.ID, err)
			return
		}
		conn, err := pgx.Connect(ctx, dbURL)
		if err != nil {
			t.Errorf("connect for fixture user cleanup: %v", err)
			return
		}
		defer func() {
			if err := conn.Close(ctx); err != nil {
				t.Errorf("close fixture cleanup connection: %v", err)
			}
		}()
		if _, err := conn.Exec(ctx, `DELETE FROM users WHERE id = $1`, user.ID); err != nil {
			t.Errorf("delete fixture user %d: %v", user.ID, err)
		}
	})
	return st, tenant.ID, user.ID
}

func waitForDueTaskCreationReceipt(
	t *testing.T,
	st *store.Store,
	receipt types.TaskCreationReceipt,
) *types.TaskCreationReceipt {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		// The production due query compares next_attempt_at with the database
		// clock. Poll that same boundary instead of estimating it from the Go
		// process clock, which may be slightly ahead of PostgreSQL on CI hosts.
		due, err := st.ListDueTaskCreationReceipts(
			ctx, receipt.TenantID, time.Now().Add(time.Hour), 100,
		)
		if err != nil {
			t.Fatalf("list due terminal receipts: %v", err)
		}
		for i := range due {
			if due[i].ID == receipt.ID {
				return &due[i]
			}
		}

		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatalf(
				"terminal receipt %d did not become due by database clock: %v",
				receipt.ID, ctx.Err(),
			)
		}
	}
}
