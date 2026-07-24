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

	"github.com/YouToco/vane/scheduler"
	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

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

	tests := []struct {
		name              string
		killPoint         definitionEditCoordinatorIntegrationKillPoint
		originalStatus    types.ScheduleStatus
		staleTakeover     bool
		retainedV1Create  bool
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
			originalStatus: types.ScheduleStatusActive, wantRecovery: true,
			wantDurablePhase:  types.TaskDefinitionEditPhaseDefinitionCommitted,
			wantTemporalPhase: scheduler.TaskDefinitionEditPhaseBasePaused,
		},
		{
			name: "apply authorization response lost", killPoint: definitionEditCoordinatorKillAuthorizeApply,
			originalStatus: types.ScheduleStatusActive, wantRecovery: true,
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
			originalStatus: types.ScheduleStatusActive, wantRecovery: true,
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
		{name: "complete response lost", killPoint: definitionEditCoordinatorKillComplete, originalStatus: types.ScheduleStatusActive},
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
			outcome, err := fixture.coordinator.Confirm(
				confirmCtx, fixture.operation.Scope(), fixture.receipt,
			)
			if err != nil {
				t.Fatalf("Confirm(): %v", err)
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
		})
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
	dbURL       string
	store       *store.Store
	coordinator *TaskDefinitionEditCoordinator
	killStore   *definitionEditCoordinatorIntegrationKillStore
	schedules   *scheduler.Scheduler
	operation   *types.TaskDefinitionEditOperation
	receipt     TaskDefinitionEditReceiptTarget
	target      taskstate.ApprovedDefinitionV1
	targetHead  scheduler.TaskDefinitionEditHead
	prepared    scheduler.PreparedTaskDefinitionEdit
	wantStatus  types.ScheduleStatus
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
	schedules := scheduler.New(temporalClient, taskQueue, nil, options...)
	creation := NewCreationCoordinator(st, schedules, nil)
	creationSession, err := st.CreateAgentSession(ctx, userID)
	if err != nil {
		t.Fatalf("create creation session: %v", err)
	}
	creationID := "c2b3-create-" + uuid.NewString()
	creationTarget := CreationReceiptTarget{
		Provider: FeishuCardPatchReceiptProviderForApp("c2b3-integration"),
		Target:   "om_creation_" + uuid.NewString(),
	}
	if _, err := creation.Propose(ctx, CreationProposalInput{
		ActionID: creationID, UserID: userID, SessionID: &creationSession.ID,
		RawArgs: mustCreateArgs(
			t, "持续监控 C2b3 coordinator 集成测试", "C2b3 coordinator base",
		),
		ExpiresAt: time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatalf("create base proposal: %v", err)
	}
	creationResult, err := creation.Confirm(ctx, userID, creationID, creationTarget)
	if err != nil || creationResult.Status != types.PendingActionStatusExecuted ||
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
	receipt := TaskDefinitionEditReceiptTarget{
		Provider: FeishuCardPatchReceiptProviderForApp("c2b3-integration"),
		Target:   "om_definition_edit_" + uuid.NewString(),
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
		OperationID:   "c2b3-edit-" + uuid.NewString(),
		ApprovalRef:   "c2b3-approval-" + uuid.NewString(),
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
