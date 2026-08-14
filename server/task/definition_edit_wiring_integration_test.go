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

func TestTaskDefinitionEditCoordinatorIntegration_LegacyReconcileCannotOverwriteEdit(
	t *testing.T,
) {
	dbURL := definitionEditIntegrationDatabaseURL(t)
	server, namespace, taskQueue := startDefinitionEditIntegrationServer(t)
	fixture := newDefinitionEditCoordinatorIntegrationFixture(
		t, dbURL, server.Client(), namespace, taskQueue,
		definitionEditCoordinatorKillNone, types.ScheduleStatusActive, false, false,
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
		outcome, err := fixture.coordinator.Execute(
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
		t.Fatalf("Execute(): %v", result.err)
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
		definitionEditCoordinatorKillNone, types.ScheduleStatusActive, false, false,
	)
	outcome, err := fixture.coordinator.Execute(
		t.Context(), fixture.operation.Scope(), fixture.receipt,
	)
	if err != nil ||
		outcome.Status != types.TaskDefinitionEditOperationStatusCompleted {
		t.Fatalf("Execute() outcome=%+v err=%v", outcome, err)
	}

	dispatcher, err := NewDefinitionEditReceiptDispatcher(
		DefinitionEditReceiptDispatcherDeps{
			Store: fixture.store,
			Sessions: definitionEditPostgreSQLReceiptSessions{
				store: fixture.store,
			},
		},
	)
	if err != nil {
		t.Fatalf("NewDefinitionEditReceiptDispatcher(): %v", err)
	}
	firstDeadline := time.Now().Add(5 * time.Second)
	for {
		if err := dispatcher.DispatchOnce(t.Context()); err != nil {
			t.Fatalf("DispatchOnce(): %v", err)
		}
		receipt, loadErr := fixture.store.LoadTaskDefinitionEditReceiptByOperation(
			t.Context(), fixture.operation.ID,
			fixture.operation.TenantID, fixture.operation.UserID,
		)
		if loadErr == nil && receipt.Status == types.TaskDefinitionEditReceiptStatusSent {
			break
		}
		if time.Now().After(firstDeadline) {
			t.Fatal("terminal receipt was not recorded in the Agent session")
		}
		time.Sleep(25 * time.Millisecond)
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
