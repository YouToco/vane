//go:build integration

package task

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"

	"github.com/YouToco/vane/server/runtimepolicy"
	"github.com/YouToco/vane/server/scheduler"
	"github.com/YouToco/vane/server/store"
	"github.com/YouToco/vane/server/taskstate"
	"github.com/YouToco/vane/server/types"
)

type researchV3EditCreateResponseLossStore struct {
	*store.Store
	createCalls int
}

func (s *researchV3EditCreateResponseLossStore) CreateResearchTaskDefinitionEditOperationV3(
	ctx context.Context, params types.CreateResearchTaskDefinitionEditOperationV3Params,
) (*types.TaskDefinitionEditOperation, error) {
	op, err := s.Store.CreateResearchTaskDefinitionEditOperationV3(ctx, params)
	if err != nil {
		return op, err
	}
	s.createCalls++
	return nil, errors.New("simulated native V3 edit create response loss")
}

func TestTaskDefinitionEditCoordinatorIntegration_PostgreSQLTemporalKillPoints(t *testing.T) {
	dbURL := os.Getenv("VANE_TEST_DATABASE_URL")
	if dbURL == "" {
		dbURL = os.Getenv("DATABASE_URL")
	}
	if dbURL == "" {
		t.Skip("未设置 VANE_TEST_DATABASE_URL 或 DATABASE_URL，跳过 C2b3 Coordinator 真库测试")
	}

	const (
		namespace = "c2b3-definition-edit-coordinator-integration"
		taskQueue = "c2b3-definition-edit-coordinator-integration"
	)
	startCtx, cancelStart := context.WithTimeout(t.Context(), 5*time.Minute)
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

	tests := []struct {
		name              string
		killPoint         definitionEditCoordinatorIntegrationKillPoint
		originalStatus    types.ScheduleStatus
		staleTakeover     bool
		retainedV1Create  bool
		activeCutover     bool
		wantRecovery      bool
		wantDurablePhase  types.TaskDefinitionEditPhase
		wantTemporalPhase scheduler.TaskDefinitionEditPhase
	}{
		{name: "uninterrupted active", killPoint: definitionEditCoordinatorKillNone, originalStatus: types.ScheduleStatusActive},
		{
			name:             "uninterrupted active retained v1 creation",
			killPoint:        definitionEditCoordinatorKillNone,
			originalStatus:   types.ScheduleStatusActive,
			retainedV1Create: true,
		},
		{name: "uninterrupted paused", killPoint: definitionEditCoordinatorKillNone, originalStatus: types.ScheduleStatusPaused},
		{name: "create response lost", killPoint: definitionEditCoordinatorKillCreate, originalStatus: types.ScheduleStatusActive},
		{
			name: "acquire response lost", killPoint: definitionEditCoordinatorKillAcquire,
			originalStatus: types.ScheduleStatusActive, wantRecovery: true,
			wantDurablePhase:  types.TaskDefinitionEditPhaseProposalSealed,
			wantTemporalPhase: scheduler.TaskDefinitionEditPhaseBaseOriginal,
		},
		{
			name: "renew response lost", killPoint: definitionEditCoordinatorKillRenew,
			originalStatus: types.ScheduleStatusActive, wantRecovery: true,
			wantDurablePhase:  types.TaskDefinitionEditPhaseProposalSealed,
			wantTemporalPhase: scheduler.TaskDefinitionEditPhaseBaseOriginal,
		},
		{
			name: "quiesce response lost", killPoint: definitionEditCoordinatorKillQuiesce,
			originalStatus: types.ScheduleStatusActive, wantRecovery: true,
			wantDurablePhase:  types.TaskDefinitionEditPhaseDBQuiesced,
			wantTemporalPhase: scheduler.TaskDefinitionEditPhaseBaseOriginal,
		},
		{
			name: "pause authorization response lost", killPoint: definitionEditCoordinatorKillAuthorizePause,
			originalStatus: types.ScheduleStatusActive, wantRecovery: true,
			wantDurablePhase:  types.TaskDefinitionEditPhaseDBQuiesced,
			wantTemporalPhase: scheduler.TaskDefinitionEditPhaseBaseOriginal,
		},
		{
			name: "remote pause landed before checkpoint", killPoint: definitionEditCoordinatorKillBeforeBaseCheckpoint,
			originalStatus: types.ScheduleStatusActive, wantRecovery: true,
			wantDurablePhase:  types.TaskDefinitionEditPhaseDBQuiesced,
			wantTemporalPhase: scheduler.TaskDefinitionEditPhaseBasePaused,
		},
		{
			name: "paused remote pause landed before checkpoint", killPoint: definitionEditCoordinatorKillBeforeBaseCheckpoint,
			originalStatus: types.ScheduleStatusPaused, wantRecovery: true,
			wantDurablePhase:  types.TaskDefinitionEditPhaseDBQuiesced,
			wantTemporalPhase: scheduler.TaskDefinitionEditPhaseBaseOriginal,
		},
		{
			name: "pause checkpoint response lost", killPoint: definitionEditCoordinatorKillBaseCheckpoint,
			originalStatus: types.ScheduleStatusActive, wantRecovery: true,
			wantDurablePhase:  types.TaskDefinitionEditPhaseTemporalBasePaused,
			wantTemporalPhase: scheduler.TaskDefinitionEditPhaseBasePaused,
		},
		{
			name: "definition commit response lost", killPoint: definitionEditCoordinatorKillDefinitionCommit,
			originalStatus: types.ScheduleStatusActive, activeCutover: true, wantRecovery: true,
			wantDurablePhase:  types.TaskDefinitionEditPhaseDefinitionCommitted,
			wantTemporalPhase: scheduler.TaskDefinitionEditPhaseBasePaused,
		},
		{
			name: "apply authorization response lost", killPoint: definitionEditCoordinatorKillAuthorizeApply,
			originalStatus: types.ScheduleStatusActive, activeCutover: true, wantRecovery: true,
			wantDurablePhase:  types.TaskDefinitionEditPhaseDefinitionCommitted,
			wantTemporalPhase: scheduler.TaskDefinitionEditPhaseBasePaused,
		},
		{
			name: "remote apply landed before checkpoint", killPoint: definitionEditCoordinatorKillBeforeApplyCheckpoint,
			originalStatus: types.ScheduleStatusActive, wantRecovery: true,
			wantDurablePhase:  types.TaskDefinitionEditPhaseDefinitionCommitted,
			wantTemporalPhase: scheduler.TaskDefinitionEditPhaseTargetPaused,
		},
		{
			name: "paused remote apply landed before checkpoint", killPoint: definitionEditCoordinatorKillBeforeApplyCheckpoint,
			originalStatus: types.ScheduleStatusPaused, wantRecovery: true,
			wantDurablePhase:  types.TaskDefinitionEditPhaseDefinitionCommitted,
			wantTemporalPhase: scheduler.TaskDefinitionEditPhaseTargetFinal,
		},
		{
			name: "apply checkpoint response lost", killPoint: definitionEditCoordinatorKillApplyCheckpoint,
			originalStatus: types.ScheduleStatusActive, wantRecovery: true,
			wantDurablePhase:  types.TaskDefinitionEditPhaseTemporalTargetApplied,
			wantTemporalPhase: scheduler.TaskDefinitionEditPhaseTargetPaused,
		},
		{
			name: "restore authorization response lost", killPoint: definitionEditCoordinatorKillAuthorizeRestore,
			originalStatus: types.ScheduleStatusActive, activeCutover: true, wantRecovery: true,
			wantDurablePhase:  types.TaskDefinitionEditPhaseTemporalTargetApplied,
			wantTemporalPhase: scheduler.TaskDefinitionEditPhaseTargetPaused,
		},
		{
			name: "remote restore landed before checkpoint", killPoint: definitionEditCoordinatorKillBeforeRestoreCheckpoint,
			originalStatus: types.ScheduleStatusActive, wantRecovery: true,
			wantDurablePhase:  types.TaskDefinitionEditPhaseTemporalTargetApplied,
			wantTemporalPhase: scheduler.TaskDefinitionEditPhaseTargetFinal,
		},
		{
			name: "paused remote restore landed before checkpoint", killPoint: definitionEditCoordinatorKillBeforeRestoreCheckpoint,
			originalStatus: types.ScheduleStatusPaused, wantRecovery: true,
			wantDurablePhase:  types.TaskDefinitionEditPhaseTemporalTargetApplied,
			wantTemporalPhase: scheduler.TaskDefinitionEditPhaseTargetFinal,
		},
		{
			name: "restore checkpoint response lost", killPoint: definitionEditCoordinatorKillRestoreCheckpoint,
			originalStatus: types.ScheduleStatusActive, wantRecovery: true,
			wantDurablePhase:  types.TaskDefinitionEditPhaseTemporalTargetRestored,
			wantTemporalPhase: scheduler.TaskDefinitionEditPhaseTargetFinal,
		},
		{
			name: "complete response lost", killPoint: definitionEditCoordinatorKillComplete,
			originalStatus: types.ScheduleStatusActive, activeCutover: true,
		},
		{
			name: "stale takeover response lost", killPoint: definitionEditCoordinatorKillTakeover,
			originalStatus: types.ScheduleStatusActive, staleTakeover: true,
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			fixture := newDefinitionEditCoordinatorIntegrationFixture(
				t, dbURL, server.Client(), namespace, taskQueue, testCase.killPoint,
				testCase.originalStatus, testCase.retainedV1Create,
				testCase.activeCutover,
			)
			if testCase.retainedV1Create &&
				(fixture.prepared.Creation.FingerprintVersion != "v1" ||
					fixture.prepared.WireVersion != "v2") {
				t.Fatalf(
					"retained creation compatibility = fingerprint %q wire %q",
					fixture.prepared.Creation.FingerprintVersion,
					fixture.prepared.WireVersion,
				)
			}
			if testCase.staleTakeover {
				runDefinitionEditCoordinatorIntegrationTakeoverLoss(t, fixture)
				assertDefinitionEditCoordinatorIntegrationConverged(t, fixture)
				return
			}

			confirmCtx, cancelConfirm := context.WithCancel(t.Context())
			defer cancelConfirm()
			fixture.killStore.setCancel(cancelConfirm)
			outcome, err := fixture.coordinator.Execute(
				confirmCtx, fixture.operation.Scope(), fixture.receipt,
			)
			if err != nil {
				t.Fatalf("Execute(): %v", err)
			}
			if !fixture.killStore.didTrip() &&
				testCase.killPoint != definitionEditCoordinatorKillNone {
				t.Fatalf("kill point %q did not execute", testCase.killPoint)
			}
			if testCase.wantRecovery {
				if !outcome.Recovering || outcome.Status != types.TaskDefinitionEditOperationStatusExecuting {
					t.Fatalf("kill-point outcome = %+v, want durable recovery", outcome)
				}
				assertDefinitionEditCoordinatorIntegrationInterrupted(
					t, fixture, testCase.wantDurablePhase, testCase.wantTemporalPhase,
				)
				forceDefinitionEditCoordinatorTakeover(t, dbURL, fixture.operation.ID)
				if err := fixture.coordinator.RecoverStaleOnce(t.Context()); err != nil {
					t.Fatalf("RecoverStaleOnce(): %v", err)
				}
			} else if outcome.Status != types.TaskDefinitionEditOperationStatusCompleted ||
				outcome.Recovering {
				t.Fatalf("terminal outcome = %+v", outcome)
			}

			assertDefinitionEditCoordinatorIntegrationConverged(t, fixture)
			if testCase.activeCutover {
				assertDefinitionEditCoordinatorIntegrationCutover(
					t, fixture, fixture.targetHead, 3,
				)
				beforeReplay := fixture.temporalCalls.snapshot()
				replay, replayErr := fixture.coordinator.Execute(
					t.Context(), fixture.operation.Scope(), fixture.receipt,
				)
				if replayErr != nil || !replay.Replayed ||
					replay.Status != types.TaskDefinitionEditOperationStatusCompleted {
					t.Fatalf("terminal exact replay=%+v err=%v", replay, replayErr)
				}
				afterReplay := fixture.temporalCalls.snapshot()
				if afterReplay != beforeReplay {
					t.Fatalf(
						"terminal exact replay made Temporal schedule RPCs: before=%+v after=%+v",
						beforeReplay, afterReplay,
					)
				}
				assertDefinitionEditCoordinatorIntegrationCutover(
					t, fixture, fixture.targetHead, 3,
				)
			}
		})
	}
	t.Run("native v3 whole-tool response loss replay", func(t *testing.T) {
		runNativeV3DefinitionEditWholeToolReplay(
			t, server.Client(), namespace, taskQueue)
	})
}

func runNativeV3DefinitionEditWholeToolReplay(
	t *testing.T, temporalClient client.Client, namespace, taskQueue string,
) {
	t.Helper()
	st, tenantID, userID := newCreationCoordinatorPostgreSQLFixture(t)
	schedules := scheduler.New(temporalClient, taskQueue, nil,
		scheduler.WithTaskScheduleNamespace(namespace))
	creation := NewCreationCoordinator(st, schedules, nil,
		WithResearchV3CreationPolicy(testResearchV3CreationPolicy()))
	creationSession, err := st.CreateAgentSession(t.Context(), userID)
	if err != nil {
		t.Fatalf("create V3 creation session: %v", err)
	}
	createInput := testResearchV3CreationInput()
	createInput.ActionID = "native-v3-edit-base-" + uuid.NewString()
	createInput.UserID = userID
	createInput.SessionID = &creationSession.ID
	createInput.SpecJSON = json.RawMessage(
		`{"tz":"Asia/Shanghai","every_seconds":3600}`)
	if _, err := creation.PrepareResearchV3(t.Context(), createInput); err != nil {
		t.Fatalf("prepare native V3 base: %v", err)
	}
	created, err := creation.ExecuteResearchV3(
		t.Context(), userID, createInput.ActionID, testCreationReceiptTarget)
	if err != nil || created.TaskID == "" ||
		created.Status != types.TaskOperationStatusExecuted {
		t.Fatalf("create native V3 base=%+v err=%v", created, err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := temporalClient.ScheduleClient().GetHandle(
			cleanupCtx, created.TaskID).Delete(cleanupCtx); err != nil {
			if _, notFound := errors.AsType[*serviceerror.NotFound](err); !notFound {
				t.Errorf("delete native V3 replay schedule: %v", err)
			}
		}
	})

	editSession, err := st.CreateAgentSession(t.Context(), userID)
	if err != nil {
		t.Fatalf("create V3 edit session: %v", err)
	}
	manual := "只查官方原文并与历史证据比较；无重大更新不推送。"
	input := ResearchTaskDefinitionEditChangesInputV3{
		ActionID: "native-v3-edit-loss-" + uuid.NewString(),
		TenantID: tenantID, UserID: userID, TaskID: created.TaskID,
		SessionID: editSession.ID,
		Changes:   ResearchV3DefinitionChanges{TaskManual: &manual},
		ExpiresAt: time.Now().Add(time.Hour),
	}
	responseLoss := &researchV3EditCreateResponseLossStore{Store: st}
	coordinator := NewResearchTaskDefinitionEditCoordinatorV3(
		responseLoss, schedules, nil)
	op, err := coordinator.PrepareChanges(t.Context(), input)
	if err != nil || op == nil || responseLoss.createCalls != 1 {
		t.Fatalf("adopt V3 edit create response loss op=%+v calls=%d err=%v",
			op, responseLoss.createCalls, err)
	}
	receipt := TaskDefinitionEditReceiptTarget{
		Provider: AgentAutoReceiptProvider, Target: input.ActionID,
	}
	outcome, err := coordinator.Execute(t.Context(), op.Scope(), receipt)
	if err != nil || outcome.Status != types.TaskDefinitionEditOperationStatusCompleted {
		t.Fatalf("complete native V3 edit outcome=%+v err=%v", outcome, err)
	}
	before, err := st.LoadResearchTaskDefinitionEditBasisV3(
		t.Context(), tenantID, userID, created.TaskID)
	if err != nil || before.DefinitionVersion != 2 {
		t.Fatalf("native V3 head before replay=%+v err=%v", before, err)
	}

	replayInput := input
	replayInput.ExpiresAt = input.ExpiresAt.Add(24 * time.Hour)
	replayCoordinator := NewResearchTaskDefinitionEditCoordinatorV3(st, schedules, nil)
	replayedOp, err := replayCoordinator.PrepareChanges(t.Context(), replayInput)
	if err != nil || replayedOp == nil || replayedOp.ID != op.ID ||
		!replayedOp.ExpiresAt.Equal(op.ExpiresAt) {
		t.Fatalf("whole-tool replay op=%+v err=%v want=%+v", replayedOp, err, op)
	}
	replayedOutcome, err := replayCoordinator.Execute(
		t.Context(), replayedOp.Scope(), receipt)
	if err != nil || !replayedOutcome.Replayed ||
		replayedOutcome.Status != types.TaskDefinitionEditOperationStatusCompleted {
		t.Fatalf("terminal whole-tool replay=%+v err=%v", replayedOutcome, err)
	}
	after, err := st.LoadResearchTaskDefinitionEditBasisV3(
		t.Context(), tenantID, userID, created.TaskID)
	if err != nil || after.DefinitionVersion != before.DefinitionVersion ||
		after.DefinitionDigest != before.DefinitionDigest {
		t.Fatalf("whole-tool replay changed head before=%+v after=%+v err=%v",
			before, after, err)
	}
}

func runDefinitionEditCoordinatorIntegrationTakeoverLoss(
	t *testing.T,
	fixture definitionEditCoordinatorIntegrationFixture,
) {
	t.Helper()
	seed, err := fixture.store.AcquireTaskDefinitionEditOperation(
		t.Context(), types.AcquireTaskDefinitionEditOperationParams{
			Scope: fixture.operation.Scope(), LeaseOwner: "integration-stale-owner",
			LeaseDuration:   taskDefinitionEditLeaseDuration,
			ReceiptProvider: fixture.receipt.Provider,
			ReceiptTarget:   fixture.receipt.Target,
		},
	)
	if err != nil {
		t.Fatalf("seed stale operation acquisition: %v", err)
	}
	if err := fixture.store.QuiesceTaskDefinitionEdit(t.Context(), seed.Lease()); err != nil {
		t.Fatalf("seed stale operation quiesce: %v", err)
	}
	forceDefinitionEditCoordinatorTakeover(t, fixture.dbURL, fixture.operation.ID)

	recoverCtx, cancelRecover := context.WithCancel(t.Context())
	fixture.killStore.setCancel(cancelRecover)
	err = fixture.coordinator.RecoverStaleOnce(recoverCtx)
	fixture.killStore.setCancel(nil)
	cancelRecover()
	if err == nil {
		t.Fatal("first RecoverStaleOnce() unexpectedly survived takeover response loss")
	}
	if !fixture.killStore.didTrip() {
		t.Fatalf("kill point %q did not execute", definitionEditCoordinatorKillTakeover)
	}
	assertDefinitionEditCoordinatorIntegrationInterrupted(
		t, fixture, types.TaskDefinitionEditPhaseDBQuiesced,
		scheduler.TaskDefinitionEditPhaseBaseOriginal,
	)
	assertDefinitionEditCoordinatorIntegrationTakeoverFence(t, fixture, 2, 2)

	forceDefinitionEditCoordinatorTakeover(t, fixture.dbURL, fixture.operation.ID)
	if err := fixture.coordinator.RecoverStaleOnce(t.Context()); err != nil {
		t.Fatalf("second RecoverStaleOnce(): %v", err)
	}
}

type definitionEditCoordinatorIntegrationFixture struct {
	dbURL              string
	store              *store.Store
	coordinator        *TaskDefinitionEditCoordinator
	killStore          *definitionEditCoordinatorIntegrationKillStore
	schedules          *scheduler.Scheduler
	operation          *types.TaskDefinitionEditOperation
	receipt            TaskDefinitionEditReceiptTarget
	target             taskstate.ApprovedDefinitionV1
	targetHead         scheduler.TaskDefinitionEditHead
	prepared           scheduler.PreparedTaskDefinitionEdit
	wantStatus         types.ScheduleStatus
	temporalCalls      *definitionEditTemporalRPCCounter
	baseCutoverEventID *int64
}

type definitionEditTemporalRPCSnapshot struct {
	Create   int
	Delete   int
	Backfill int
	Update   int
	Describe int
	Trigger  int
	Pause    int
	Unpause  int
}

type definitionEditTemporalRPCCounter struct {
	mu            sync.Mutex
	snapshotValue definitionEditTemporalRPCSnapshot
}

func (c *definitionEditTemporalRPCCounter) add(
	update func(*definitionEditTemporalRPCSnapshot),
) {
	c.mu.Lock()
	defer c.mu.Unlock()
	update(&c.snapshotValue)
}

func (c *definitionEditTemporalRPCCounter) snapshot() definitionEditTemporalRPCSnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.snapshotValue
}

type definitionEditCountingTemporalClient struct {
	client.Client
	counter *definitionEditTemporalRPCCounter
}

func newDefinitionEditCountingTemporalClient(
	base client.Client,
) *definitionEditCountingTemporalClient {
	return &definitionEditCountingTemporalClient{
		Client:  base,
		counter: &definitionEditTemporalRPCCounter{},
	}
}

func (c *definitionEditCountingTemporalClient) ScheduleClient() client.ScheduleClient {
	return &definitionEditCountingScheduleClient{
		ScheduleClient: c.Client.ScheduleClient(),
		counter:        c.counter,
	}
}

type definitionEditCountingScheduleClient struct {
	client.ScheduleClient
	counter *definitionEditTemporalRPCCounter
}

func (c *definitionEditCountingScheduleClient) Create(
	ctx context.Context,
	options client.ScheduleOptions,
) (client.ScheduleHandle, error) {
	c.counter.add(func(value *definitionEditTemporalRPCSnapshot) {
		value.Create++
	})
	handle, err := c.ScheduleClient.Create(ctx, options)
	if err != nil {
		return handle, err
	}
	return &definitionEditCountingScheduleHandle{
		ScheduleHandle: handle,
		counter:        c.counter,
	}, nil
}

func (c *definitionEditCountingScheduleClient) GetHandle(
	ctx context.Context,
	scheduleID string,
) client.ScheduleHandle {
	return &definitionEditCountingScheduleHandle{
		ScheduleHandle: c.ScheduleClient.GetHandle(ctx, scheduleID),
		counter:        c.counter,
	}
}

type definitionEditCountingScheduleHandle struct {
	client.ScheduleHandle
	counter *definitionEditTemporalRPCCounter
}

func (h *definitionEditCountingScheduleHandle) Delete(ctx context.Context) error {
	h.counter.add(func(value *definitionEditTemporalRPCSnapshot) {
		value.Delete++
	})
	return h.ScheduleHandle.Delete(ctx)
}

func (h *definitionEditCountingScheduleHandle) Backfill(
	ctx context.Context,
	options client.ScheduleBackfillOptions,
) error {
	h.counter.add(func(value *definitionEditTemporalRPCSnapshot) {
		value.Backfill++
	})
	return h.ScheduleHandle.Backfill(ctx, options)
}

func (h *definitionEditCountingScheduleHandle) Update(
	ctx context.Context,
	options client.ScheduleUpdateOptions,
) error {
	h.counter.add(func(value *definitionEditTemporalRPCSnapshot) {
		value.Update++
	})
	return h.ScheduleHandle.Update(ctx, options)
}

func (h *definitionEditCountingScheduleHandle) Describe(
	ctx context.Context,
) (*client.ScheduleDescription, error) {
	h.counter.add(func(value *definitionEditTemporalRPCSnapshot) {
		value.Describe++
	})
	return h.ScheduleHandle.Describe(ctx)
}

func (h *definitionEditCountingScheduleHandle) Trigger(
	ctx context.Context,
	options client.ScheduleTriggerOptions,
) error {
	h.counter.add(func(value *definitionEditTemporalRPCSnapshot) {
		value.Trigger++
	})
	return h.ScheduleHandle.Trigger(ctx, options)
}

func (h *definitionEditCountingScheduleHandle) Pause(
	ctx context.Context,
	options client.SchedulePauseOptions,
) error {
	h.counter.add(func(value *definitionEditTemporalRPCSnapshot) {
		value.Pause++
	})
	return h.ScheduleHandle.Pause(ctx, options)
}

func (h *definitionEditCountingScheduleHandle) Unpause(
	ctx context.Context,
	options client.ScheduleUnpauseOptions,
) error {
	h.counter.add(func(value *definitionEditTemporalRPCSnapshot) {
		value.Unpause++
	})
	return h.ScheduleHandle.Unpause(ctx, options)
}

func newDefinitionEditCoordinatorIntegrationFixture(
	t *testing.T,
	dbURL string,
	temporalClient client.Client,
	namespace string,
	taskQueue string,
	killPoint definitionEditCoordinatorIntegrationKillPoint,
	originalStatus types.ScheduleStatus,
	retainedV1Create bool,
	activeCutover bool,
) definitionEditCoordinatorIntegrationFixture {
	t.Helper()
	st, tenantID, userID := newCreationCoordinatorPostgreSQLFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()

	options := []scheduler.SchedulerOption{
		scheduler.WithTaskScheduleNamespace(namespace),
	}
	if !retainedV1Create {
		options = append(options, scheduler.WithCompiledRuntimeRollout(true, "", true))
	}
	countingTemporal := newDefinitionEditCountingTemporalClient(temporalClient)
	schedules := scheduler.New(countingTemporal, taskQueue, nil, options...)
	creation := NewCreationCoordinator(st, schedules, nil)
	creationSession, err := st.CreateAgentSession(ctx, userID)
	if err != nil {
		t.Fatalf("create creation session: %v", err)
	}
	creationID := "c2b3-create-" + uuid.NewString()
	creationTarget := AgentAutoReceiptTarget(creationID)
	if _, err := creation.Prepare(ctx, CreationProposalInput{
		ActionID: creationID, UserID: userID, SessionID: &creationSession.ID,
		RawArgs: mustCreateArgs(
			t, "持续监控 C2b3 coordinator 集成测试", "C2b3 coordinator base",
		),
		ExpiresAt: time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatalf("create base proposal: %v", err)
	}
	creationResult, err := creation.Execute(ctx, userID, creationID, creationTarget)
	if err != nil || creationResult.Status != types.TaskOperationStatusExecuted ||
		creationResult.Recovering || creationResult.TaskID == "" {
		t.Fatalf("create base task result=%+v err=%v", creationResult, err)
	}
	taskID := creationResult.TaskID
	handle := temporalClient.ScheduleClient().GetHandle(ctx, taskID)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		if err := handle.Delete(cleanupCtx); err != nil {
			if _, notFound := errors.AsType[*serviceerror.NotFound](err); !notFound {
				t.Errorf("delete integration schedule: %v", err)
			}
		}
	})

	creationOp, err := st.LoadTaskCreationOperation(ctx, creationID, tenantID, userID)
	if err != nil {
		t.Fatalf("load creation provenance: %v", err)
	}
	var preparedCreation scheduler.PreparedTaskSchedule
	if err := json.Unmarshal(creationOp.PreparedSchedule, &preparedCreation); err != nil {
		t.Fatalf("decode creation prepared schedule: %v", err)
	}
	if originalStatus == types.ScheduleStatusPaused {
		if err := handle.Pause(ctx, client.SchedulePauseOptions{
			Note: "operator-paused-before-definition-edit",
		}); err != nil {
			t.Fatalf("pause integration base schedule: %v", err)
		}
		setDefinitionEditCoordinatorScheduleStatus(
			t, dbURL, tenantID, userID, taskID, types.ScheduleStatusPaused,
		)
	}
	base, err := st.GetCurrentApprovedDefinition(ctx, tenantID, userID, taskID)
	if err != nil {
		t.Fatalf("load base Approved definition: %v", err)
	}
	var baseCutoverEventID *int64
	if activeCutover {
		eventID := activateDefinitionEditCoordinatorIntegrationCutover(
			t, st, tenantID, userID, taskID,
		)
		baseCutoverEventID = &eventID
	}
	target := base.Definition
	target.NLDescription = "C2b3 coordinator target " + uuid.NewString()
	target.SpecJSON = json.RawMessage(`{"every_seconds":7200,"tz":"UTC"}`)
	targetBytes, err := taskstate.EncodeApprovedDefinitionV1(target)
	if err != nil {
		t.Fatalf("encode target Approved definition: %v", err)
	}
	targetHead := scheduler.TaskDefinitionEditHead{
		Version: base.Version + 1,
		Digest:  sha256Hex(targetBytes),
	}
	editSession, err := st.CreateAgentSession(ctx, userID)
	if err != nil {
		t.Fatalf("create edit session: %v", err)
	}
	operationID := "c2b3-edit-" + uuid.NewString()
	receipt := TaskDefinitionEditReceiptTarget{
		Provider: AgentAutoReceiptProvider,
		Target:   operationID,
	}
	wrapped := &definitionEditCoordinatorIntegrationKillStore{
		Store: st, point: killPoint,
	}
	coordinator := NewTaskDefinitionEditCoordinator(wrapped, schedules, nil)
	sealCtx, cancelSeal := context.WithCancel(ctx)
	if killPoint == definitionEditCoordinatorKillCreate {
		wrapped.setCancel(cancelSeal)
	}
	op, err := coordinator.PrepareAndSealProposal(sealCtx, PrepareTaskDefinitionEditProposalInput{
		OperationID:   operationID,
		OperationRef:  "c2b3-approval-" + uuid.NewString(),
		ActorTenantID: tenantID, ActorUserID: userID,
		TargetTenantID: tenantID, TargetUserID: userID, TaskID: taskID,
		SessionID: editSession.ID, ExpiresAt: time.Now().Add(time.Hour),
		OriginalStatus: originalStatus,
		BaseHead: scheduler.TaskDefinitionEditHead{
			Version: base.Version, Digest: base.Digest,
		},
		TargetHead: targetHead, BaseDefinition: base.Definition,
		TargetDefinition: target, Creation: preparedCreation,
	})
	cancelSeal()
	wrapped.setCancel(nil)
	if err != nil {
		t.Fatalf("PrepareAndSealProposal(): %v", err)
	}
	prepared, err := scheduler.DecodePreparedTaskDefinitionEdit(op.PreparedEdit)
	if err != nil {
		t.Fatalf("decode sealed prepared edit: %v", err)
	}
	return definitionEditCoordinatorIntegrationFixture{
		dbURL: dbURL, store: st, coordinator: coordinator, killStore: wrapped,
		schedules: schedules,
		operation: op, receipt: receipt, target: target,
		targetHead: targetHead, prepared: prepared, wantStatus: originalStatus,
		temporalCalls:      countingTemporal.counter,
		baseCutoverEventID: baseCutoverEventID,
	}
}

func assertDefinitionEditCoordinatorIntegrationInterrupted(
	t *testing.T,
	fixture definitionEditCoordinatorIntegrationFixture,
	wantDurable types.TaskDefinitionEditPhase,
	wantTemporal scheduler.TaskDefinitionEditPhase,
) {
	t.Helper()
	op, err := fixture.store.LoadTaskDefinitionEditOperation(
		t.Context(), fixture.operation.Scope(),
	)
	if err != nil {
		t.Fatalf("load interrupted operation: %v", err)
	}
	if op.Status != types.TaskDefinitionEditOperationStatusExecuting ||
		op.Phase != wantDurable {
		t.Fatalf("interrupted durable state=(%s,%s), want=(executing,%s)",
			op.Status, op.Phase, wantDurable)
	}
	remote, err := fixture.schedules.DescribeTaskDefinitionEdit(
		t.Context(), fixture.prepared,
	)
	if err != nil {
		t.Fatalf("describe interrupted Temporal state: %v", err)
	}
	if remote.Phase != wantTemporal {
		t.Fatalf("interrupted Temporal phase=%s, want=%s", remote.Phase, wantTemporal)
	}
	if fixture.baseCutoverEventID != nil {
		switch wantDurable {
		case types.TaskDefinitionEditPhaseDefinitionCommitted,
			types.TaskDefinitionEditPhaseTemporalTargetApplied,
			types.TaskDefinitionEditPhaseTemporalTargetRestored:
			assertDefinitionEditCoordinatorIntegrationCutover(
				t, fixture, fixture.targetHead, 3,
			)
		default:
			baseHead := scheduler.TaskDefinitionEditHead{
				Version: fixture.operation.BaseDefinitionVersion,
				Digest:  fixture.operation.BaseDefinitionDigest,
			}
			assertDefinitionEditCoordinatorIntegrationCutover(
				t, fixture, baseHead, 1,
			)
		}
	}
}

func assertDefinitionEditCoordinatorIntegrationConverged(
	t *testing.T,
	fixture definitionEditCoordinatorIntegrationFixture,
) {
	t.Helper()
	op, err := fixture.store.LoadTaskDefinitionEditOperation(
		t.Context(), fixture.operation.Scope(),
	)
	if err != nil {
		t.Fatalf("load converged operation: %v", err)
	}
	if op.Status != types.TaskDefinitionEditOperationStatusCompleted ||
		op.Phase != types.TaskDefinitionEditPhaseTemporalTargetRestored ||
		len(op.PauseSnapshot) == 0 || len(op.ApplySnapshot) == 0 ||
		len(op.RestoreSnapshot) == 0 {
		t.Fatalf("converged operation = %+v", op)
	}
	head, err := fixture.store.GetCurrentApprovedDefinition(
		t.Context(), op.TargetTenantID, op.TargetUserID, op.TaskID,
	)
	if err != nil {
		t.Fatalf("load converged Approved head: %v", err)
	}
	if head.Version != fixture.targetHead.Version || head.Digest != fixture.targetHead.Digest ||
		head.Definition.NLDescription != fixture.target.NLDescription {
		t.Fatalf("Approved head = %+v, want version=%d digest=%s target=%q",
			head, fixture.targetHead.Version, fixture.targetHead.Digest,
			fixture.target.NLDescription,
		)
	}
	remote, err := fixture.schedules.DescribeTaskDefinitionEdit(
		t.Context(), fixture.prepared,
	)
	if err != nil {
		t.Fatalf("DescribeTaskDefinitionEdit(): %v", err)
	}
	if remote.Phase != scheduler.TaskDefinitionEditPhaseTargetFinal ||
		remote.RepresentationDigest != fixture.prepared.TargetFinal.Digest {
		t.Fatalf("remote final snapshot = %+v", remote)
	}
	receipt, err := fixture.store.LoadTaskDefinitionEditReceiptByOperation(
		t.Context(), op.ID, op.TenantID, op.UserID,
	)
	if err != nil || receipt.Status != types.TaskDefinitionEditReceiptStatusPending ||
		receipt.Provider != fixture.receipt.Provider || receipt.Target != fixture.receipt.Target {
		t.Fatalf("terminal receipt = %+v err=%v", receipt, err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, fixture.dbURL)
	if err != nil {
		t.Fatalf("connect for terminal audit: %v", err)
	}
	defer func() {
		if err := conn.Close(context.Background()); err != nil {
			t.Errorf("close terminal audit connection: %v", err)
		}
	}()
	var (
		markerOperation *string
		markerFence     *int64
		receiptCount    int
		scheduleStatus  string
		headVersion     *int64
		headDigest      *string
	)
	err = conn.QueryRow(ctx,
		`SELECT s.definition_edit_operation_id, s.definition_edit_fence,
		        s.status, s.approved_definition_version, s.approved_definition_digest,
		        (SELECT count(*) FROM task_definition_edit_receipts r
		          WHERE r.operation_id=$1)
		   FROM schedules s WHERE s.id=$2`,
		op.ID, op.TaskID,
	).Scan(
		&markerOperation, &markerFence, &scheduleStatus, &headVersion, &headDigest,
		&receiptCount,
	)
	if err != nil || markerOperation != nil || markerFence != nil || receiptCount != 1 ||
		scheduleStatus != string(fixture.wantStatus) || headVersion == nil ||
		*headVersion != fixture.targetHead.Version || headDigest == nil ||
		*headDigest != fixture.targetHead.Digest {
		t.Fatalf("terminal marker=(%v,%v) schedule=%s head=(%v,%v) receipt_count=%d err=%v",
			markerOperation, markerFence, scheduleStatus, headVersion, headDigest,
			receiptCount, err)
	}
}

func forceDefinitionEditCoordinatorTakeover(
	t *testing.T,
	dbURL string,
	operationID string,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		t.Fatalf("connect for kill-point takeover: %v", err)
	}
	defer func() {
		if err := conn.Close(context.Background()); err != nil {
			t.Errorf("close kill-point takeover connection: %v", err)
		}
	}()
	tag, err := conn.Exec(ctx,
		`UPDATE task_definition_edit_operations
		    SET lease_until=clock_timestamp()-interval '2 seconds',
		        takeover_not_before=clock_timestamp()-interval '1 second'
		  WHERE id=$1 AND status='executing'`,
		operationID,
	)
	if err != nil || tag.RowsAffected() != 1 {
		t.Fatalf("force stale operation rows=%d err=%v", tag.RowsAffected(), err)
	}
}

func assertDefinitionEditCoordinatorIntegrationTakeoverFence(
	t *testing.T,
	fixture definitionEditCoordinatorIntegrationFixture,
	wantFence int64,
	wantAttempt int32,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, fixture.dbURL)
	if err != nil {
		t.Fatalf("connect for takeover fence audit: %v", err)
	}
	defer func() {
		if err := conn.Close(context.Background()); err != nil {
			t.Errorf("close takeover fence audit connection: %v", err)
		}
	}()
	var (
		operationFence int64
		attempt        int32
		markerID       *string
		markerFence    *int64
	)
	err = conn.QueryRow(ctx,
		`SELECT o.fence, o.attempt,
		        s.definition_edit_operation_id, s.definition_edit_fence
		   FROM task_definition_edit_operations o
		   JOIN schedules s
		     ON s.tenant_id=o.target_tenant_id
		    AND s.user_id=o.target_user_id
		    AND s.id=o.task_id
		  WHERE o.id=$1 AND o.tenant_id=$2 AND o.user_id=$3`,
		fixture.operation.ID, fixture.operation.TenantID, fixture.operation.UserID,
	).Scan(&operationFence, &attempt, &markerID, &markerFence)
	if err != nil || operationFence != wantFence || attempt != wantAttempt ||
		markerID == nil || *markerID != fixture.operation.ID ||
		markerFence == nil || *markerFence != wantFence {
		t.Fatalf(
			"takeover operation=(fence=%d attempt=%d) marker=(%v,%v), want fence=%d attempt=%d err=%v",
			operationFence, attempt, markerID, markerFence, wantFence, wantAttempt, err,
		)
	}
}

func setDefinitionEditCoordinatorScheduleStatus(
	t *testing.T,
	dbURL string,
	tenantID int64,
	userID int64,
	taskID string,
	status types.ScheduleStatus,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		t.Fatalf("connect for schedule status fixture: %v", err)
	}
	defer func() {
		if err := conn.Close(context.Background()); err != nil {
			t.Errorf("close schedule status fixture connection: %v", err)
		}
	}()
	tag, err := conn.Exec(ctx,
		`UPDATE schedules SET status=$4
		  WHERE tenant_id=$1 AND user_id=$2 AND id=$3`,
		tenantID, userID, taskID, status,
	)
	if err != nil || tag.RowsAffected() != 1 {
		t.Fatalf("set schedule status rows=%d err=%v", tag.RowsAffected(), err)
	}
}

func activateDefinitionEditCoordinatorIntegrationCutover(
	t *testing.T,
	st *store.Store,
	tenantID int64,
	userID int64,
	taskID string,
) int64 {
	t.Helper()
	policy, err := runtimepolicy.BuildV1(runtimepolicy.BuildInputV1{
		AllowedCapabilities: []runtimepolicy.CapabilityV1{{
			Platform: "web", Capability: "search", Kind: "article",
			ImplementationVersion: "fetcher.exa/v1",
			CredentialRef: runtimepolicy.CredentialRefV1{
				ID: runtimepolicy.CredentialIDExaPrimaryV1, Generation: 1,
			},
		}},
		ScorePrompt: runtimepolicy.PromptStageV1{
			SystemPrompt: "score prompt", RendererVersion: "scorer.render/v1",
		},
		CardGenPrompt: runtimepolicy.PromptStageV1{
			SystemPrompt: "card prompt", RendererVersion: "cardgen.render/v1",
		},
		ProfileEvolvePrompt: runtimepolicy.PromptStageV1{
			SystemPrompt: "evolve prompt", RendererVersion: "evolver.render/v1",
		},
		TaskInstructionEnabled: true,
		ModelProvider:          "deepseek",
		ModelEndpoint: runtimepolicy.EndpointRefV1{
			ID: runtimepolicy.EndpointIDDeepSeekCompatiblePrimaryV1, Generation: 1,
		},
		ModelCredentialRef: runtimepolicy.CredentialRefV1{
			ID: runtimepolicy.CredentialIDLLMPrimaryV1, Generation: 1,
		},
		ModelCalls: []runtimepolicy.ModelCallV1{
			{
				Stage: runtimepolicy.ModelStageScore, Model: "model-1",
				MaxTokens: 16, DisableThinking: true,
			},
			{
				Stage: runtimepolicy.ModelStageCardGen, Model: "model-1",
				Temperature: 0.7, MaxTokens: 400, DisableThinking: true,
			},
			{
				Stage: runtimepolicy.ModelStageProfileEvolve, Model: "model-1",
				MaxTokens: 800, DisableThinking: true,
			},
		},
		QuotaBuckets: []runtimepolicy.QuotaBucketV1{{
			Name:      "llm_tokens",
			Financial: true, EnforcementVersion: "precharge-reconcile/v1",
		}},
	})
	if err != nil {
		t.Fatalf("build active-cutover runtime policy: %v", err)
	}
	identity := types.RunIdentity{
		TemporalWorkflowID: "wf-" + taskID,
		TemporalRunID:      "definition-edit-cutover-" + uuid.NewString(),
		RunKind:            types.RunSnapshotKindScheduled,
		TenantID:           tenantID,
		UserID:             userID,
		TaskID:             taskID,
	}
	ref, err := st.CreateOrGetCompiledRunSnapshotShadowV2(
		t.Context(), identity, policy,
	)
	if err != nil {
		t.Fatalf("create active-cutover retained-v2 snapshot: %v", err)
	}
	audit, err := st.AuditCompiledTaskRunSnapshotV2(
		t.Context(), identity, ref,
	)
	if err != nil || audit.Status != store.CompiledRunSnapshotV2AuditMatch ||
		audit.ShadowStatus != store.TaskRunSnapshotShadowMatch ||
		!audit.TypedEqual {
		t.Fatalf("active-cutover retained-v2 audit=%+v err=%v", audit, err)
	}
	activation, err := st.ControlTaskRunSnapshotCutover(
		t.Context(), tenantID, userID, taskID,
		store.TaskRunSnapshotCutoverActivate,
	)
	if err != nil {
		t.Fatalf("activate retained-v2 authority: %v", err)
	}
	return activation.EventID
}

func assertDefinitionEditCoordinatorIntegrationCutover(
	t *testing.T,
	fixture definitionEditCoordinatorIntegrationFixture,
	wantHead scheduler.TaskDefinitionEditHead,
	wantEventCount int,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, fixture.dbURL)
	if err != nil {
		t.Fatalf("connect for definition-edit cutover audit: %v", err)
	}
	defer func() {
		if err := conn.Close(context.Background()); err != nil {
			t.Errorf("close definition-edit cutover audit connection: %v", err)
		}
	}()
	var (
		pointerID         int64
		action            string
		definitionVersion int64
		definitionDigest  string
		eventCount        int
		editEventCount    int
	)
	err = conn.QueryRow(ctx, `
		SELECT s.run_snapshot_cutover_event_id, e.action,
		       e.approved_definition_version, e.approved_definition_digest,
		       (SELECT count(*)
		          FROM task_run_snapshot_v2_cutover_events scoped
		         WHERE scoped.tenant_id=s.tenant_id
		           AND scoped.user_id=s.user_id
		           AND scoped.task_id=s.id),
		       (SELECT count(*)
		          FROM task_run_snapshot_v2_cutover_events owned
		         WHERE owned.tenant_id=s.tenant_id
		           AND owned.user_id=s.user_id
		           AND owned.task_id=s.id
		           AND owned.definition_edit_operation_id=$4)
		  FROM schedules s
		  JOIN task_run_snapshot_v2_cutover_events e
		    ON e.id=s.run_snapshot_cutover_event_id
		   AND e.tenant_id=s.tenant_id
		   AND e.user_id=s.user_id
		   AND e.task_id=s.id
		 WHERE s.tenant_id=$1 AND s.user_id=$2 AND s.id=$3`,
		fixture.operation.TargetTenantID,
		fixture.operation.TargetUserID,
		fixture.operation.TaskID,
		fixture.operation.ID,
	).Scan(
		&pointerID, &action, &definitionVersion, &definitionDigest,
		&eventCount, &editEventCount,
	)
	if err != nil {
		t.Fatalf("load definition-edit cutover state: %v", err)
	}
	wantEditEvents := 0
	if wantEventCount == 3 {
		wantEditEvents = 2
	}
	if action != string(store.TaskRunSnapshotCutoverActivate) ||
		definitionVersion != wantHead.Version ||
		definitionDigest != wantHead.Digest ||
		eventCount != wantEventCount ||
		editEventCount != wantEditEvents {
		t.Fatalf(
			"cutover pointer=%d action=%q head=%d/%s events=%d edit_events=%d, want active %d/%s events=%d edit_events=%d",
			pointerID, action, definitionVersion, definitionDigest,
			eventCount, editEventCount, wantHead.Version, wantHead.Digest,
			wantEventCount, wantEditEvents,
		)
	}
	if wantEventCount == 1 && fixture.baseCutoverEventID != nil &&
		pointerID != *fixture.baseCutoverEventID {
		t.Fatalf(
			"pre-commit active cutover pointer=%d, want base event=%d",
			pointerID, *fixture.baseCutoverEventID,
		)
	}
}

type definitionEditCoordinatorIntegrationKillPoint string

const (
	definitionEditCoordinatorKillNone                    definitionEditCoordinatorIntegrationKillPoint = ""
	definitionEditCoordinatorKillCreate                  definitionEditCoordinatorIntegrationKillPoint = "create"
	definitionEditCoordinatorKillAcquire                 definitionEditCoordinatorIntegrationKillPoint = "acquire"
	definitionEditCoordinatorKillTakeover                definitionEditCoordinatorIntegrationKillPoint = "takeover"
	definitionEditCoordinatorKillRenew                   definitionEditCoordinatorIntegrationKillPoint = "renew"
	definitionEditCoordinatorKillQuiesce                 definitionEditCoordinatorIntegrationKillPoint = "quiesce"
	definitionEditCoordinatorKillAuthorizePause          definitionEditCoordinatorIntegrationKillPoint = "authorize_pause"
	definitionEditCoordinatorKillBeforeBaseCheckpoint    definitionEditCoordinatorIntegrationKillPoint = "before_base_checkpoint"
	definitionEditCoordinatorKillBaseCheckpoint          definitionEditCoordinatorIntegrationKillPoint = "base_checkpoint"
	definitionEditCoordinatorKillDefinitionCommit        definitionEditCoordinatorIntegrationKillPoint = "definition_commit"
	definitionEditCoordinatorKillAuthorizeApply          definitionEditCoordinatorIntegrationKillPoint = "authorize_apply"
	definitionEditCoordinatorKillBeforeApplyCheckpoint   definitionEditCoordinatorIntegrationKillPoint = "before_apply_checkpoint"
	definitionEditCoordinatorKillApplyCheckpoint         definitionEditCoordinatorIntegrationKillPoint = "apply_checkpoint"
	definitionEditCoordinatorKillAuthorizeRestore        definitionEditCoordinatorIntegrationKillPoint = "authorize_restore"
	definitionEditCoordinatorKillBeforeRestoreCheckpoint definitionEditCoordinatorIntegrationKillPoint = "before_restore_checkpoint"
	definitionEditCoordinatorKillRestoreCheckpoint       definitionEditCoordinatorIntegrationKillPoint = "restore_checkpoint"
	definitionEditCoordinatorKillComplete                definitionEditCoordinatorIntegrationKillPoint = "complete"
)

type definitionEditCoordinatorIntegrationKillStore struct {
	*store.Store

	mu      sync.Mutex
	point   definitionEditCoordinatorIntegrationKillPoint
	tripped bool
	cancel  context.CancelFunc
}

func (s *definitionEditCoordinatorIntegrationKillStore) setCancel(cancel context.CancelFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cancel = cancel
}

func (s *definitionEditCoordinatorIntegrationKillStore) didTrip() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tripped
}

func (s *definitionEditCoordinatorIntegrationKillStore) CreateTaskDefinitionEditOperation(
	ctx context.Context,
	params types.CreateTaskDefinitionEditOperationParams,
) (*types.TaskDefinitionEditOperation, error) {
	op, err := s.Store.CreateTaskDefinitionEditOperation(ctx, params)
	if err != nil {
		return op, err
	}
	if err := s.afterCommit(definitionEditCoordinatorKillCreate); err != nil {
		return nil, err
	}
	return op, nil
}

func (s *definitionEditCoordinatorIntegrationKillStore) AcquireTaskDefinitionEditOperation(
	ctx context.Context,
	params types.AcquireTaskDefinitionEditOperationParams,
) (*types.TaskDefinitionEditOperation, error) {
	op, err := s.Store.AcquireTaskDefinitionEditOperation(ctx, params)
	if err != nil {
		return op, err
	}
	point := definitionEditCoordinatorKillAcquire
	if op.Attempt > 1 {
		point = definitionEditCoordinatorKillTakeover
	}
	if err := s.afterCommit(point); err != nil {
		return nil, err
	}
	return op, nil
}

func (s *definitionEditCoordinatorIntegrationKillStore) RenewTaskDefinitionEditLease(
	ctx context.Context,
	lease types.TaskDefinitionEditLease,
	duration time.Duration,
) error {
	if err := s.Store.RenewTaskDefinitionEditLease(ctx, lease, duration); err != nil {
		return err
	}
	return s.afterCommit(definitionEditCoordinatorKillRenew)
}

func (s *definitionEditCoordinatorIntegrationKillStore) QuiesceTaskDefinitionEdit(
	ctx context.Context,
	lease types.TaskDefinitionEditLease,
) error {
	if err := s.Store.QuiesceTaskDefinitionEdit(ctx, lease); err != nil {
		return err
	}
	return s.afterCommit(definitionEditCoordinatorKillQuiesce)
}

func (s *definitionEditCoordinatorIntegrationKillStore) AuthorizeTaskDefinitionEditRemotePhase(
	ctx context.Context,
	lease types.TaskDefinitionEditLease,
	expected types.TaskDefinitionEditPhase,
) (*types.TaskDefinitionEditOperation, error) {
	op, err := s.Store.AuthorizeTaskDefinitionEditRemotePhase(ctx, lease, expected)
	if err != nil {
		return op, err
	}
	point := definitionEditCoordinatorIntegrationKillPoint("")
	switch expected {
	case types.TaskDefinitionEditPhaseDBQuiesced:
		point = definitionEditCoordinatorKillAuthorizePause
	case types.TaskDefinitionEditPhaseDefinitionCommitted:
		point = definitionEditCoordinatorKillAuthorizeApply
	case types.TaskDefinitionEditPhaseTemporalTargetApplied:
		point = definitionEditCoordinatorKillAuthorizeRestore
	}
	if err := s.afterCommit(point); err != nil {
		return nil, err
	}
	return op, nil
}

func (s *definitionEditCoordinatorIntegrationKillStore) CheckpointTaskDefinitionEditBasePaused(
	ctx context.Context,
	lease types.TaskDefinitionEditLease,
	snapshot []byte,
) error {
	if err := s.beforeCommit(definitionEditCoordinatorKillBeforeBaseCheckpoint); err != nil {
		return err
	}
	if err := s.Store.CheckpointTaskDefinitionEditBasePaused(ctx, lease, snapshot); err != nil {
		return err
	}
	return s.afterCommit(definitionEditCoordinatorKillBaseCheckpoint)
}

func (s *definitionEditCoordinatorIntegrationKillStore) beforeCommit(
	point definitionEditCoordinatorIntegrationKillPoint,
) error {
	s.mu.Lock()
	if s.point != point || s.tripped {
		s.mu.Unlock()
		return nil
	}
	s.tripped = true
	cancel := s.cancel
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return fmt.Errorf("integration kill point %s exited before the Store checkpoint", point)
}

func (s *definitionEditCoordinatorIntegrationKillStore) CommitTaskDefinitionEditDefinition(
	ctx context.Context,
	lease types.TaskDefinitionEditLease,
) error {
	if err := s.Store.CommitTaskDefinitionEditDefinition(ctx, lease); err != nil {
		return err
	}
	return s.afterCommit(definitionEditCoordinatorKillDefinitionCommit)
}

func (s *definitionEditCoordinatorIntegrationKillStore) CheckpointTaskDefinitionEditTargetApplied(
	ctx context.Context,
	lease types.TaskDefinitionEditLease,
	snapshot []byte,
) error {
	if err := s.beforeCommit(definitionEditCoordinatorKillBeforeApplyCheckpoint); err != nil {
		return err
	}
	if err := s.Store.CheckpointTaskDefinitionEditTargetApplied(ctx, lease, snapshot); err != nil {
		return err
	}
	return s.afterCommit(definitionEditCoordinatorKillApplyCheckpoint)
}

func (s *definitionEditCoordinatorIntegrationKillStore) CheckpointTaskDefinitionEditTargetRestored(
	ctx context.Context,
	lease types.TaskDefinitionEditLease,
	snapshot []byte,
) error {
	if err := s.beforeCommit(definitionEditCoordinatorKillBeforeRestoreCheckpoint); err != nil {
		return err
	}
	if err := s.Store.CheckpointTaskDefinitionEditTargetRestored(ctx, lease, snapshot); err != nil {
		return err
	}
	return s.afterCommit(definitionEditCoordinatorKillRestoreCheckpoint)
}

func (s *definitionEditCoordinatorIntegrationKillStore) CompleteTaskDefinitionEditOperation(
	ctx context.Context,
	lease types.TaskDefinitionEditLease,
	result json.RawMessage,
) error {
	if err := s.Store.CompleteTaskDefinitionEditOperation(ctx, lease, result); err != nil {
		return err
	}
	return s.afterCommit(definitionEditCoordinatorKillComplete)
}

func (s *definitionEditCoordinatorIntegrationKillStore) afterCommit(
	point definitionEditCoordinatorIntegrationKillPoint,
) error {
	s.mu.Lock()
	if s.point != point || s.tripped {
		s.mu.Unlock()
		return nil
	}
	s.tripped = true
	cancel := s.cancel
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return fmt.Errorf("integration kill point %s committed but response was lost", point)
}
