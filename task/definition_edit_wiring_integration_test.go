//go:build integration

package task

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"

	"github.com/YouToco/vane/scheduler"
	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
)

func TestTaskDefinitionEditCoordinatorIntegration_CancelBindsOriginalReceipt(
	t *testing.T,
) {
	dbURL := definitionEditIntegrationDatabaseURL(t)
	server, namespace, taskQueue := startDefinitionEditIntegrationServer(t)
	fixture := newDefinitionEditCoordinatorIntegrationFixture(
		t, dbURL, server.Client(), namespace, taskQueue,
		definitionEditCoordinatorKillNone, types.ScheduleStatusActive, false,
	)

	outcome, err := fixture.coordinator.Cancel(
		t.Context(), fixture.operation.Scope(), fixture.receipt,
	)
	if err != nil {
		t.Fatalf("Cancel(): %v", err)
	}
	if outcome.Status != types.TaskDefinitionEditOperationStatusCancelled ||
		!outcome.ReceiptBound || outcome.Recovering {
		t.Fatalf("cancel outcome = %+v", outcome)
	}
	operation, err := fixture.store.LoadTaskDefinitionEditOperation(
		t.Context(), fixture.operation.Scope(),
	)
	if err != nil {
		t.Fatalf("load cancelled operation: %v", err)
	}
	if operation.Status != types.TaskDefinitionEditOperationStatusCancelled ||
		operation.Phase != types.TaskDefinitionEditPhaseProposalSealed {
		t.Fatalf("cancelled operation = %+v", operation)
	}
	receipt, err := fixture.store.LoadTaskDefinitionEditReceiptByOperation(
		t.Context(), operation.ID, operation.TenantID, operation.UserID,
	)
	if err != nil {
		t.Fatalf("load cancellation receipt: %v", err)
	}
	if receipt.Status != types.TaskDefinitionEditReceiptStatusPending ||
		receipt.Provider != fixture.receipt.Provider ||
		receipt.Target != fixture.receipt.Target {
		t.Fatalf("cancellation receipt = %+v", receipt)
	}
}

func TestTaskDefinitionEditCoordinatorIntegration_LegacyReconcileCannotOverwriteEdit(
	t *testing.T,
) {
	dbURL := definitionEditIntegrationDatabaseURL(t)
	server, namespace, taskQueue := startDefinitionEditIntegrationServer(t)
	fixture := newDefinitionEditCoordinatorIntegrationFixture(
		t, dbURL, server.Client(), namespace, taskQueue,
		definitionEditCoordinatorKillNone, types.ScheduleStatusActive, false,
	)
	blockingStore := &definitionEditBlockingReconcileStore{
		Store: fixture.store, targetID: fixture.operation.TaskID,
		acquired: make(chan bool, 1),
		proceed:  make(chan struct{}),
	}
	reconciler := scheduler.New(
		server.Client(), taskQueue, blockingStore,
		scheduler.WithTaskScheduleNamespace(namespace),
		scheduler.WithCompiledRuntimeRollout(true, "", true),
	)
	reconcileDone := make(chan error, 1)
	go func() {
		reconcileDone <- reconciler.ReconcileActions(t.Context())
	}()
	select {
	case authorized := <-blockingStore.acquired:
		if authorized {
			t.Fatal("legacy reconcile authorized a task with a pending definition edit")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("legacy reconcile did not acquire the shared PostgreSQL gate")
	}

	type confirmResult struct {
		outcome TaskDefinitionEditOutcome
		err     error
	}
	confirmDone := make(chan confirmResult, 1)
	go func() {
		outcome, err := fixture.coordinator.Confirm(
			t.Context(), fixture.operation.Scope(), fixture.receipt,
		)
		confirmDone <- confirmResult{outcome: outcome, err: err}
	}()
	select {
	case result := <-confirmDone:
		t.Fatalf("definition edit crossed held reconcile gate: %+v", result)
	case <-time.After(200 * time.Millisecond):
	}
	close(blockingStore.proceed)

	if err := <-reconcileDone; err != nil {
		t.Fatalf("ReconcileActions(): %v", err)
	}
	result := <-confirmDone
	if result.err != nil {
		t.Fatalf("Confirm(): %v", result.err)
	}
	if result.outcome.Status !=
		types.TaskDefinitionEditOperationStatusCompleted {
		t.Fatalf("confirm outcome = %+v", result.outcome)
	}
	assertDefinitionEditCoordinatorIntegrationConverged(t, fixture)
}

func TestDefinitionEditReceiptDispatcherIntegration_PostgreSQLResponseLossRecovery(
	t *testing.T,
) {
	dbURL := definitionEditIntegrationDatabaseURL(t)
	server, namespace, taskQueue := startDefinitionEditIntegrationServer(t)
	fixture := newDefinitionEditCoordinatorIntegrationFixture(
		t, dbURL, server.Client(), namespace, taskQueue,
		definitionEditCoordinatorKillNone, types.ScheduleStatusActive, false,
	)
	outcome, err := fixture.coordinator.Confirm(
		t.Context(), fixture.operation.Scope(), fixture.receipt,
	)
	if err != nil ||
		outcome.Status != types.TaskDefinitionEditOperationStatusCompleted {
		t.Fatalf("Confirm() outcome=%+v err=%v", outcome, err)
	}

	sender := &definitionEditReceiptFakeSender{failAfterApply: 1}
	dispatcher, err := NewDefinitionEditReceiptDispatcher(
		DefinitionEditReceiptDispatcherDeps{
			Store: fixture.store, Sender: sender,
			Sessions: definitionEditPostgreSQLReceiptSessions{
				store: fixture.store,
			},
			BuildCard: func(markdown string) string {
				raw, _ := json.Marshal(map[string]any{
					"schema": "2.0",
					"config": map[string]any{"update_multi": true},
					"body":   map[string]any{"text": markdown},
				})
				return string(raw)
			},
		},
	)
	if err != nil {
		t.Fatalf("NewDefinitionEditReceiptDispatcher(): %v", err)
	}
	firstDeadline := time.Now().Add(5 * time.Second)
	var firstErr error
	for {
		err := dispatcher.DispatchOnce(t.Context())
		if err != nil {
			firstErr = err
		}
		calls, _ := sender.snapshot()
		if calls > 0 {
			break
		}
		if time.Now().After(firstDeadline) {
			t.Fatal("terminal receipt never reached the original-card sender")
		}
		time.Sleep(25 * time.Millisecond)
	}
	if firstErr == nil {
		t.Fatal("ambiguous original-card Patch response loss was not surfaced")
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		receipt, err := fixture.store.LoadTaskDefinitionEditReceiptByOperation(
			t.Context(), fixture.operation.ID,
			fixture.operation.TenantID, fixture.operation.UserID,
		)
		if err != nil {
			t.Fatalf("load recovering receipt: %v", err)
		}
		if receipt.Status == types.TaskDefinitionEditReceiptStatusSent {
			if receipt.SessionRecordedAt == nil || len(receipt.Payload) == 0 {
				t.Fatalf("sent receipt lacks immutable checkpoints: %+v", receipt)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("receipt did not recover before deadline: %+v", receipt)
		}
		time.Sleep(50 * time.Millisecond)
		if err := dispatcher.DispatchOnce(t.Context()); err != nil &&
			!errors.Is(err, types.ErrTaskDefinitionEditReceiptBusy) {
			t.Fatalf("receipt recovery pass: %v", err)
		}
	}
	calls, resources := sender.snapshot()
	if calls != 2 || len(resources) != 1 ||
		resources[fixture.receipt.Target] == "" {
		t.Fatalf("original-card replay calls=%d resources=%v", calls, resources)
	}
}

type definitionEditPostgreSQLReceiptSessions struct {
	store *store.Store
}

func (s definitionEditPostgreSQLReceiptSessions) RecordDefinitionEditReceiptSession(
	ctx context.Context,
	receipt types.TaskDefinitionEditReceipt,
	messages json.RawMessage,
) error {
	return s.store.RecordTaskDefinitionEditReceiptSessionMessages(
		ctx, receipt.Lease(), messages,
	)
}

type definitionEditBlockingReconcileStore struct {
	*store.Store
	targetID string
	acquired chan bool
	proceed  chan struct{}
	once     sync.Once
}

func (s *definitionEditBlockingReconcileStore) AcquireScheduleReconcile(
	ctx context.Context,
	id string,
) (*types.Schedule, func(context.Context) error, error) {
	if id != s.targetID {
		return nil, nil, nil
	}
	schedule, release, err := s.Store.AcquireScheduleReconcile(ctx, id)
	if err != nil {
		return schedule, release, err
	}
	s.once.Do(func() { s.acquired <- schedule != nil })
	select {
	case <-s.proceed:
		return schedule, release, nil
	case <-ctx.Done():
		if release != nil {
			err = errors.Join(err, release(context.Background()))
		}
		return nil, nil, errors.Join(ctx.Err(), err)
	}
}

func definitionEditIntegrationDatabaseURL(t *testing.T) string {
	t.Helper()
	dbURL := os.Getenv("VANE_TEST_DATABASE_URL")
	if dbURL == "" {
		dbURL = os.Getenv("DATABASE_URL")
	}
	if dbURL == "" {
		t.Skip("未设置 VANE_TEST_DATABASE_URL 或 DATABASE_URL")
	}
	return dbURL
}

func startDefinitionEditIntegrationServer(
	t *testing.T,
) (*testsuite.DevServer, string, string) {
	t.Helper()
	const (
		namespace = "c2b3-definition-edit-wiring-integration"
		taskQueue = "c2b3-definition-edit-wiring-integration"
	)
	startCtx, cancelStart := context.WithTimeout(t.Context(), 2*time.Minute)
	server, err := testsuite.StartDevServer(startCtx, testsuite.DevServerOptions{
		ClientOptions: &client.Options{Namespace: namespace},
		LogLevel:      "error",
	})
	cancelStart()
	if err != nil {
		t.Fatalf("start Temporal dev server: %v", err)
	}
	t.Cleanup(func() {
		if err := server.Stop(); err != nil {
			t.Errorf("stop Temporal dev server: %v", err)
		}
		server.Client().Close()
	})
	return server, namespace, taskQueue
}
