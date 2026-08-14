package task

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/YouToco/vane/scheduler"
	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

var testTaskDefinitionEditReceiptTarget = TaskDefinitionEditReceiptTarget{
	Provider: "feishu_card_patch:test-app",
	Target:   "om_definition_edit_test",
}

func TestTaskDefinitionEditCoordinator_AttemptLifecycle(t *testing.T) {
	tests := []struct {
		name           string
		originalStatus types.ScheduleStatus
		wantSnapshots  []scheduler.TaskDefinitionEditPhase
	}{
		{
			name:           "originally active",
			originalStatus: types.ScheduleStatusActive,
			wantSnapshots: []scheduler.TaskDefinitionEditPhase{
				scheduler.TaskDefinitionEditPhaseBasePaused,
				scheduler.TaskDefinitionEditPhaseTargetPaused,
				scheduler.TaskDefinitionEditPhaseTargetFinal,
			},
		},
		{
			name:           "originally paused",
			originalStatus: types.ScheduleStatusPaused,
			wantSnapshots: []scheduler.TaskDefinitionEditPhase{
				scheduler.TaskDefinitionEditPhaseBaseOriginal,
				scheduler.TaskDefinitionEditPhaseTargetFinal,
				scheduler.TaskDefinitionEditPhaseTargetFinal,
			},
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			frozen := definitionEditCoordinatorFrozenFixture(t, testCase.originalStatus)
			store := newDefinitionEditCoordinatorFakeStore(
				definitionEditCoordinatorOperation(frozen, true),
			)
			schedules := &definitionEditCoordinatorFakeScheduler{}
			coordinator := newTestTaskDefinitionEditCoordinator(store, schedules)
			lease := store.operation().Lease()
			wantPhases := []types.TaskDefinitionEditPhase{
				types.TaskDefinitionEditPhaseDBQuiesced,
				types.TaskDefinitionEditPhaseTemporalBasePaused,
				types.TaskDefinitionEditPhaseDefinitionCommitted,
				types.TaskDefinitionEditPhaseTemporalTargetApplied,
				types.TaskDefinitionEditPhaseTemporalTargetRestored,
				types.TaskDefinitionEditPhaseTemporalTargetRestored,
			}

			for attempt, wantPhase := range wantPhases {
				before := schedules.totalCalls()
				op, done, err := coordinator.runTaskDefinitionEditAttempt(t.Context(), lease)
				if err != nil {
					t.Fatalf("attempt %d from phase %s: %v", attempt, store.operation().Phase, err)
				}
				delta := schedules.totalCalls() - before
				if delta > 1 {
					t.Fatalf("attempt %d made %d raw Temporal calls; want at most one", attempt, delta)
				}
				if op == nil || op.Phase != wantPhase {
					t.Fatalf("attempt %d phase = %+v, want %s", attempt, op, wantPhase)
				}
				wantDone := attempt == len(wantPhases)-1
				if done != wantDone {
					t.Fatalf("attempt %d done = %v, want %v", attempt, done, wantDone)
				}
			}

			terminal := store.operation()
			if terminal.Status != types.TaskDefinitionEditOperationStatusCompleted ||
				terminal.Phase != types.TaskDefinitionEditPhaseTemporalTargetRestored ||
				len(terminal.Result) == 0 {
				t.Fatalf("terminal operation = %+v", terminal)
			}
			if got := schedules.snapshotPhases(); !slices.Equal(got, testCase.wantSnapshots) {
				t.Fatalf("raw snapshots = %v, want %v", got, testCase.wantSnapshots)
			}
			if got := store.eventsSnapshot(); !slices.Equal(got, []string{
				"renew", "quiesce",
				"renew", "authorize:db_quiesced", "checkpoint:base_paused",
				"renew", "commit_definition",
				"renew", "authorize:definition_committed", "checkpoint:target_applied",
				"renew", "authorize:temporal_target_applied", "checkpoint:target_restored",
				"renew", "complete",
			}) {
				t.Fatalf("durable event order = %v", got)
			}
		})
	}
}

func TestTaskDefinitionEditCoordinator_RemoteErrorMapping(t *testing.T) {
	tests := []struct {
		name       string
		remoteErr  error
		wantDone   bool
		wantStatus types.TaskDefinitionEditOperationStatus
		wantReason types.TaskDefinitionEditBlockReason
		wantErr    error
	}{
		{
			name: "not found is quarantined", remoteErr: scheduler.ErrTaskScheduleNotFound,
			wantDone: true, wantStatus: types.TaskDefinitionEditOperationStatusBlocked,
			wantReason: types.TaskDefinitionEditBlockTemporalNotFound,
		},
		{
			name: "unsafe state is quarantined", remoteErr: scheduler.ErrTaskScheduleUnsafeState,
			wantDone: true, wantStatus: types.TaskDefinitionEditOperationStatusBlocked,
			wantReason: types.TaskDefinitionEditBlockUnsafeRemoteState,
		},
		{
			name: "conflict is quarantined", remoteErr: scheduler.ErrTaskScheduleConflict,
			wantDone: true, wantStatus: types.TaskDefinitionEditOperationStatusBlocked,
			wantReason: types.TaskDefinitionEditBlockUnsafeRemoteState,
		},
		{
			name: "invalid checkpoint is quarantined", remoteErr: scheduler.ErrTaskScheduleInvalid,
			wantDone: true, wantStatus: types.TaskDefinitionEditOperationStatusBlocked,
			wantReason: types.TaskDefinitionEditBlockCheckpointInvalid,
		},
		{
			name: "blocked environment stays recoverable", remoteErr: scheduler.ErrTaskScheduleBlocked,
			wantStatus: types.TaskDefinitionEditOperationStatusExecuting,
			wantErr:    scheduler.ErrTaskScheduleBlocked,
		},
		{
			name: "transient stays recoverable", remoteErr: scheduler.ErrTaskScheduleTransient,
			wantStatus: types.TaskDefinitionEditOperationStatusExecuting,
			wantErr:    scheduler.ErrTaskScheduleTransient,
		},
		{
			name: "unknown outcome stays recoverable", remoteErr: scheduler.ErrTaskScheduleOutcomeUnknown,
			wantStatus: types.TaskDefinitionEditOperationStatusExecuting,
			wantErr:    scheduler.ErrTaskScheduleOutcomeUnknown,
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			frozen := definitionEditCoordinatorFrozenFixture(t, types.ScheduleStatusActive)
			op := definitionEditCoordinatorOperation(frozen, true)
			op.Phase = types.TaskDefinitionEditPhaseDBQuiesced
			store := newDefinitionEditCoordinatorFakeStore(op)
			schedules := &definitionEditCoordinatorFakeScheduler{pauseErr: testCase.remoteErr}
			coordinator := newTestTaskDefinitionEditCoordinator(store, schedules)

			got, done, err := coordinator.runTaskDefinitionEditAttempt(t.Context(), op.Lease())
			if done != testCase.wantDone {
				t.Fatalf("done = %v, want %v; op=%+v err=%v", done, testCase.wantDone, got, err)
			}
			if testCase.wantErr == nil {
				if err != nil {
					t.Fatalf("runTaskDefinitionEditAttempt() error = %v", err)
				}
			} else if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("runTaskDefinitionEditAttempt() error = %v, want %v", err, testCase.wantErr)
			}
			persisted := store.operation()
			if persisted.Status != testCase.wantStatus {
				t.Fatalf("status = %s, want %s", persisted.Status, testCase.wantStatus)
			}
			if testCase.wantReason != "" && persisted.ErrorCode != string(testCase.wantReason) {
				t.Fatalf("error code = %q, want %q", persisted.ErrorCode, testCase.wantReason)
			}
			if schedules.totalCalls() != 1 {
				t.Fatalf("raw calls = %d, want 1", schedules.totalCalls())
			}
		})
	}
}

func TestTaskDefinitionEditCoordinator_QuarantinesCorruptCheckpointBeforeRemoteCall(t *testing.T) {
	frozen := definitionEditCoordinatorFrozenFixture(t, types.ScheduleStatusActive)
	op := definitionEditCoordinatorOperation(frozen, true)
	op.Phase = types.TaskDefinitionEditPhaseDefinitionCommitted
	op.PauseSnapshot = []byte(`{"phase":"forged"}`)
	op.PauseSnapshotDigest = sha256Hex(op.PauseSnapshot)
	store := newDefinitionEditCoordinatorFakeStore(op)
	schedules := &definitionEditCoordinatorFakeScheduler{}
	coordinator := newTestTaskDefinitionEditCoordinator(store, schedules)

	got, done, err := coordinator.runTaskDefinitionEditAttempt(t.Context(), op.Lease())
	if err != nil || !done || got.Status != types.TaskDefinitionEditOperationStatusBlocked ||
		got.ErrorCode != string(types.TaskDefinitionEditBlockCheckpointInvalid) {
		t.Fatalf("corrupt checkpoint result = %+v done=%v err=%v", got, done, err)
	}
	if schedules.totalCalls() != 0 {
		t.Fatalf("corrupt checkpoint reached Temporal %d times", schedules.totalCalls())
	}
}

func TestTaskDefinitionEditCoordinator_AdoptsStoreResponseLoss(t *testing.T) {
	t.Run("proposal insert", func(t *testing.T) {
		frozen := definitionEditCoordinatorFrozenFixture(t, types.ScheduleStatusActive)
		store := newDefinitionEditCoordinatorFakeStore(nil)
		store.createErr = errors.New("insert committed but response was lost")
		store.createApplyBeforeError = true
		coordinator := newTestTaskDefinitionEditCoordinator(
			store, &definitionEditCoordinatorFakeScheduler{},
		)

		op, err := coordinator.sealTaskDefinitionEditProposal(t.Context(), frozen)
		if err != nil || op == nil || op.ID != frozen.Proposal.OperationID {
			t.Fatalf("SealProposal() = %+v, %v", op, err)
		}
		if store.createCalls != 2 {
			t.Fatalf("create calls = %d, want exact replay", store.createCalls)
		}
	})

	t.Run("acquire and terminal completion", func(t *testing.T) {
		frozen := definitionEditCoordinatorFrozenFixture(t, types.ScheduleStatusActive)
		store := newDefinitionEditCoordinatorFakeStore(
			definitionEditCoordinatorOperation(frozen, false),
		)
		store.acquireErr = errors.New("acquire committed but response was lost")
		store.acquireApplyBeforeError = true
		store.completeErr = errors.New("complete committed but response was lost")
		store.completeApplyBeforeError = true
		coordinator := newTestTaskDefinitionEditCoordinator(
			store, &definitionEditCoordinatorFakeScheduler{},
		)

		outcome, err := coordinator.Execute(
			t.Context(), store.operation().Scope(), testTaskDefinitionEditReceiptTarget,
		)
		if err != nil || outcome.Status != types.TaskDefinitionEditOperationStatusCompleted ||
			outcome.Recovering {
			t.Fatalf("Execute() outcome=%+v err=%v", outcome, err)
		}
		if store.acquireCalls != 2 || store.completeCalls != 1 {
			t.Fatalf("acquire=%d complete=%d, want replayed acquire and one terminal write",
				store.acquireCalls, store.completeCalls)
		}
	})
}

func TestTaskDefinitionEditCoordinator_AdoptsIntermediateStoreResponseLoss(t *testing.T) {
	tests := []struct {
		name               string
		configure          func(*definitionEditCoordinatorFakeStore)
		responseLossCallAt int
		consumed           func(*definitionEditCoordinatorFakeStore) bool
		wantReadback       types.TaskDefinitionEditPhase
		wantNextPhase      types.TaskDefinitionEditPhase
		wantNextDone       bool
	}{
		{
			name: "quiesce", configure: func(store *definitionEditCoordinatorFakeStore) {
				store.quiesceErr = errors.New("quiesce committed but response was lost")
			},
			responseLossCallAt: 0,
			consumed: func(store *definitionEditCoordinatorFakeStore) bool {
				return store.quiesceErr == nil
			},
			wantReadback:  types.TaskDefinitionEditPhaseDBQuiesced,
			wantNextPhase: types.TaskDefinitionEditPhaseTemporalBasePaused,
		},
		{
			name: "pause checkpoint", configure: func(store *definitionEditCoordinatorFakeStore) {
				store.baseCheckpointErr = errors.New("pause checkpoint committed but response was lost")
			},
			responseLossCallAt: 1,
			consumed: func(store *definitionEditCoordinatorFakeStore) bool {
				return store.baseCheckpointErr == nil
			},
			wantReadback:  types.TaskDefinitionEditPhaseTemporalBasePaused,
			wantNextPhase: types.TaskDefinitionEditPhaseDefinitionCommitted,
		},
		{
			name: "definition commit", configure: func(store *definitionEditCoordinatorFakeStore) {
				store.definitionCommitErr = errors.New("definition committed but response was lost")
			},
			responseLossCallAt: 2,
			consumed: func(store *definitionEditCoordinatorFakeStore) bool {
				return store.definitionCommitErr == nil
			},
			wantReadback:  types.TaskDefinitionEditPhaseDefinitionCommitted,
			wantNextPhase: types.TaskDefinitionEditPhaseTemporalTargetApplied,
		},
		{
			name: "apply checkpoint", configure: func(store *definitionEditCoordinatorFakeStore) {
				store.applyCheckpointErr = errors.New("apply checkpoint committed but response was lost")
			},
			responseLossCallAt: 3,
			consumed: func(store *definitionEditCoordinatorFakeStore) bool {
				return store.applyCheckpointErr == nil
			},
			wantReadback:  types.TaskDefinitionEditPhaseTemporalTargetApplied,
			wantNextPhase: types.TaskDefinitionEditPhaseTemporalTargetRestored,
		},
		{
			name: "restore checkpoint", configure: func(store *definitionEditCoordinatorFakeStore) {
				store.restoreCheckpointErr = errors.New("restore checkpoint committed but response was lost")
			},
			responseLossCallAt: 4,
			consumed: func(store *definitionEditCoordinatorFakeStore) bool {
				return store.restoreCheckpointErr == nil
			},
			wantReadback:  types.TaskDefinitionEditPhaseTemporalTargetRestored,
			wantNextPhase: types.TaskDefinitionEditPhaseTemporalTargetRestored,
			wantNextDone:  true,
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			frozen := definitionEditCoordinatorFrozenFixture(t, types.ScheduleStatusActive)
			store := newDefinitionEditCoordinatorFakeStore(
				definitionEditCoordinatorOperation(frozen, true),
			)
			testCase.configure(store)
			schedules := &definitionEditCoordinatorFakeScheduler{}
			coordinator := newTestTaskDefinitionEditCoordinator(store, schedules)
			lease := store.operation().Lease()

			var readback *types.TaskDefinitionEditOperation
			for attempt := 0; attempt <= testCase.responseLossCallAt; attempt++ {
				before := schedules.totalCalls()
				got, done, err := coordinator.runTaskDefinitionEditAttempt(t.Context(), lease)
				if delta := schedules.totalCalls() - before; delta > 1 {
					t.Fatalf("attempt %d made %d raw calls", attempt, delta)
				}
				if err != nil || done {
					t.Fatalf("attempt %d failed before readback: op=%+v done=%v err=%v",
						attempt, got, done, err)
				}
				readback = got
			}
			if readback == nil || !testCase.consumed(store) {
				t.Fatal("injected response loss was not adopted from the durable readback")
			}
			if readback.Phase != testCase.wantReadback || readback.Lease() != lease ||
				readback.Status != types.TaskDefinitionEditOperationStatusExecuting {
				t.Fatalf("response-loss readback = %+v, want phase %s under the same lease",
					readback, testCase.wantReadback)
			}
			before := schedules.totalCalls()
			next, done, err := coordinator.runTaskDefinitionEditAttempt(t.Context(), lease)
			if err != nil || done != testCase.wantNextDone || next.Phase != testCase.wantNextPhase {
				t.Fatalf("next attempt = %+v done=%v err=%v; want phase=%s done=%v",
					next, done, err, testCase.wantNextPhase, testCase.wantNextDone)
			}
			if delta := schedules.totalCalls() - before; delta > 1 {
				t.Fatalf("next attempt made %d raw calls", delta)
			}
		})
	}
}

func TestTaskDefinitionEditCoordinator_RejectsNonAdjacentStoreResponseLossReadback(t *testing.T) {
	frozen := definitionEditCoordinatorFrozenFixture(t, types.ScheduleStatusActive)
	prior := definitionEditCoordinatorOperation(frozen, true)
	prior.Phase = types.TaskDefinitionEditPhaseDefinitionCommitted
	cause := errors.New("definition commit response was lost")

	for _, loadedPhase := range []types.TaskDefinitionEditPhase{
		types.TaskDefinitionEditPhaseDBQuiesced,
		types.TaskDefinitionEditPhaseTemporalTargetRestored,
	} {
		t.Run(string(loadedPhase), func(t *testing.T) {
			loaded := cloneDefinitionEditCoordinatorOperation(prior)
			loaded.Phase = loadedPhase
			store := newDefinitionEditCoordinatorFakeStore(loaded)
			coordinator := newTestTaskDefinitionEditCoordinator(
				store, &definitionEditCoordinatorFakeScheduler{},
			)

			got, done, err := coordinator.reloadTaskDefinitionEditAfterStoreError(
				t.Context(), prior, cause,
			)
			if done || !errors.Is(err, cause) || got == nil || got.Phase != loadedPhase {
				t.Fatalf("non-adjacent readback = %+v done=%v err=%v", got, done, err)
			}
		})
	}
}

func TestTaskDefinitionEditCoordinator_BusyAcquireAdoptsConcurrentTerminal(t *testing.T) {
	tests := []struct {
		name       string
		status     types.TaskDefinitionEditOperationStatus
		wantPhase  types.TaskDefinitionEditPhase
		wantReason string
	}{
		{
			name: "completed", status: types.TaskDefinitionEditOperationStatusCompleted,
			wantPhase: types.TaskDefinitionEditPhaseTemporalTargetRestored,
		},
		{
			name: "blocked", status: types.TaskDefinitionEditOperationStatusBlocked,
			wantPhase:  types.TaskDefinitionEditPhaseDBQuiesced,
			wantReason: string(types.TaskDefinitionEditBlockUnsafeRemoteState),
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			frozen := definitionEditCoordinatorFrozenFixture(t, types.ScheduleStatusActive)
			store := newDefinitionEditCoordinatorFakeStore(
				definitionEditCoordinatorOperation(frozen, false),
			)
			store.acquireBusyTerminalStatus = testCase.status
			store.acquireBusyTerminalPhase = testCase.wantPhase
			store.acquireBusyTerminalReason = testCase.wantReason
			coordinator := newTestTaskDefinitionEditCoordinator(
				store, &definitionEditCoordinatorFakeScheduler{},
			)

			outcome, err := coordinator.Execute(
				t.Context(), store.operation().Scope(), testTaskDefinitionEditReceiptTarget,
			)
			if err != nil || outcome.Status != testCase.status ||
				outcome.Phase != testCase.wantPhase || !outcome.Replayed || outcome.Recovering {
				t.Fatalf("Execute() outcome=%+v err=%v", outcome, err)
			}
			if persisted := store.operation(); persisted.Status != testCase.status ||
				persisted.ErrorCode != testCase.wantReason {
				t.Fatalf("persisted terminal = %+v", persisted)
			}
		})
	}
}

func TestTaskDefinitionEditCoordinator_PrepareAndSealRejectsMalformedScopeBeforeTemporal(t *testing.T) {
	fixture := loadDefinitionEditProposalFixture(t)
	targetBytes, err := taskstate.EncodeApprovedDefinitionV1(fixture.target)
	if err != nil {
		t.Fatal(err)
	}
	input := PrepareTaskDefinitionEditProposalInput{
		OperationID:    fixture.prepared.OperationID,
		OperationRef:   "approval-definition-edit-malformed-scope",
		ActorTenantID:  fixture.prepared.Creation.TenantID,
		ActorUserID:    fixture.prepared.Creation.UserID,
		TargetTenantID: fixture.prepared.Creation.TenantID,
		// A cross-user target must fail before it can reveal whether Temporal has
		// the referenced schedule.
		TargetUserID:   fixture.prepared.Creation.UserID + 1,
		TaskID:         fixture.prepared.Creation.TaskID,
		SessionID:      91,
		ExpiresAt:      time.Now().Add(time.Hour),
		OriginalStatus: types.ScheduleStatusActive,
		BaseHead:       fixture.prepared.BaseHead,
		TargetHead: scheduler.TaskDefinitionEditHead{
			Version: fixture.prepared.BaseHead.Version + 1,
			Digest:  sha256Hex(targetBytes),
		},
		BaseDefinition: fixture.base, TargetDefinition: fixture.target,
		Creation: fixture.prepared.Creation,
	}
	store := newDefinitionEditCoordinatorFakeStore(nil)
	schedules := &definitionEditCoordinatorFakeScheduler{}
	coordinator := newTestTaskDefinitionEditCoordinator(store, schedules)

	if _, err := coordinator.PrepareAndSealProposal(t.Context(), input); !errors.Is(
		err, ErrDefinitionEditProposalInvalid,
	) {
		t.Fatalf("PrepareAndSealProposal() error = %v, want invalid proposal", err)
	}
	if schedules.prepareCallCount() != 0 || store.createCalls != 0 {
		t.Fatalf("malformed scope reached prepare/create: prepare=%d create=%d",
			schedules.prepareCallCount(), store.createCalls)
	}
}

func TestTaskDefinitionEditCoordinator_ValidateRuntimeEnvironmentScansNonterminalOperations(t *testing.T) {
	frozen := definitionEditCoordinatorFrozenFixture(t, types.ScheduleStatusActive)
	store := newDefinitionEditCoordinatorFakeStore(
		definitionEditCoordinatorOperation(frozen, false),
	)
	schedules := &definitionEditCoordinatorFakeScheduler{}
	coordinator := newTestTaskDefinitionEditCoordinator(store, schedules)

	if err := coordinator.ValidateRuntimeEnvironment(t.Context()); err != nil {
		t.Fatalf("ValidateRuntimeEnvironment: %v", err)
	}
	if schedules.environmentValidationCalls != 1 {
		t.Fatalf("environment validations = %d, want 1",
			schedules.environmentValidationCalls)
	}

	schedules.environmentValidationErr = errors.New("namespace identity differs")
	if err := coordinator.ValidateRuntimeEnvironment(t.Context()); err == nil ||
		!strings.Contains(err.Error(), "runtime environment differs") {
		t.Fatalf("namespace mismatch must fail startup preflight: %v", err)
	}
}

func TestTaskDefinitionEditCoordinator_ValidateRuntimeEnvironmentPaginatesExactFullPage(t *testing.T) {
	frozen := definitionEditCoordinatorFrozenFixture(t, types.ScheduleStatusActive)
	op := definitionEditCoordinatorOperation(frozen, false)
	base := newDefinitionEditCoordinatorFakeStore(op)
	store := &definitionEditPreflightPagedStore{
		TaskDefinitionEditStore: base,
		op:                      *op,
		fullPages:               1,
		shortTail:               1,
	}
	schedules := &definitionEditCoordinatorFakeScheduler{}

	if err := newTestTaskDefinitionEditCoordinator(
		store, schedules,
	).ValidateRuntimeEnvironment(t.Context()); err != nil {
		t.Fatalf("ValidateRuntimeEnvironment: %v", err)
	}
	if !slices.Equal(store.operationCursors, []string{"", op.ID}) {
		t.Fatalf("operation cursors = %v, want empty then %q",
			store.operationCursors, op.ID)
	}
	if schedules.environmentValidationCalls != taskDefinitionEditPreflightOpLimit+1 {
		t.Fatalf("environment validations = %d, want %d",
			schedules.environmentValidationCalls, taskDefinitionEditPreflightOpLimit+1)
	}
}

func TestValidateTaskDefinitionEditPreflightBudgetFailsClosed(t *testing.T) {
	if err := validateTaskDefinitionEditPreflightBudget(
		taskDefinitionEditPreflightMaxOps,
		taskDefinitionEditPreflightMaxOps,
	); err != nil {
		t.Fatalf("exact preflight cap rejected: %v", err)
	}
	err := validateTaskDefinitionEditPreflightBudget(
		taskDefinitionEditPreflightMaxOps+1,
		taskDefinitionEditPreflightMaxOps,
	)
	if err == nil || !strings.Contains(err.Error(), "exceeds 10000 operations") {
		t.Fatalf("oversize nonterminal backlog must fail closed: %v", err)
	}
}

func TestTaskDefinitionEditCoordinator_ValidateRuntimeEnvironmentRejectsDamagedCheckpointBeforeTemporal(t *testing.T) {
	frozen := definitionEditCoordinatorFrozenFixture(t, types.ScheduleStatusActive)
	op := definitionEditCoordinatorOperation(frozen, false)
	op.PreparedEdit = bytes.Clone(op.PreparedEdit)
	op.PreparedEdit[0] ^= 0xff
	store := newDefinitionEditCoordinatorFakeStore(op)
	schedules := &definitionEditCoordinatorFakeScheduler{}

	err := newTestTaskDefinitionEditCoordinator(
		store, schedules,
	).ValidateRuntimeEnvironment(t.Context())
	if err == nil || !strings.Contains(err.Error(), "before startup") {
		t.Fatalf("damaged durable checkpoint must fail startup: %v", err)
	}
	if schedules.environmentValidationCalls != 0 {
		t.Fatalf("damaged checkpoint reached Temporal validation %d times",
			schedules.environmentValidationCalls)
	}
}

func TestTaskDefinitionEditCoordinator_RecoveryPassIsTenantShardedAndBounded(t *testing.T) {
	tenantIDs := make([]int64, taskDefinitionEditRecoveryTenantLimit)
	for i := range tenantIDs {
		tenantIDs[i] = int64(i + 1)
	}
	store := newDefinitionEditCoordinatorFakeStore(nil)
	store.tenantIDs = tenantIDs
	store.fillRecovery = true
	coordinator := newTestTaskDefinitionEditCoordinator(
		store, &definitionEditCoordinatorFakeScheduler{},
	)

	if err := coordinator.RecoverStaleOnce(t.Context()); err != nil {
		t.Fatal(err)
	}
	queried, limits, cursors := store.recoverySnapshot()
	wantTenants := taskDefinitionEditRecoveryPassLimit / taskDefinitionEditRecoveryPerTenant
	if len(queried) != wantTenants {
		t.Fatalf("queried tenants = %v, want %d bounded shards", queried, wantTenants)
	}
	requested := 0
	for _, limit := range limits {
		if limit > taskDefinitionEditRecoveryPerTenant {
			t.Fatalf("per-tenant limit = %d, max %d", limit, taskDefinitionEditRecoveryPerTenant)
		}
		requested += limit
	}
	if requested != taskDefinitionEditRecoveryPassLimit {
		t.Fatalf("requested operations = %d, want global limit %d", requested, taskDefinitionEditRecoveryPassLimit)
	}
	if !slices.Equal(cursors, []int64{0}) {
		t.Fatalf("first-pass tenant cursors = %v", cursors)
	}

	store.resetRecoveryObservations()
	if err := coordinator.RecoverStaleOnce(t.Context()); err != nil {
		t.Fatal(err)
	}
	queried, _, _ = store.recoverySnapshot()
	if len(queried) == 0 || queried[0] != int64(wantTenants+1) {
		t.Fatalf("second pass did not rotate after tenant %d: %v", wantTenants, queried)
	}
}

func TestTaskDefinitionEditCoordinator_RecoveryContinuesAfterShardError(t *testing.T) {
	store := newDefinitionEditCoordinatorFakeStore(nil)
	store.tenantIDs = []int64{7, 8}
	store.shardErrs = map[int64]error{7: errors.New("tenant 7 unavailable")}
	coordinator := newTestTaskDefinitionEditCoordinator(
		store, &definitionEditCoordinatorFakeScheduler{},
	)

	err := coordinator.RecoverStaleOnce(t.Context())
	queried, _, _ := store.recoverySnapshot()
	if err == nil || !slices.Equal(queried, []int64{7, 8}) {
		t.Fatalf("later shard must still be scanned: queried=%v err=%v", queried, err)
	}
}

func TestTaskDefinitionEditCoordinator_RecoveryWrapDoesNotRepeatTenant(t *testing.T) {
	store := newDefinitionEditCoordinatorFakeStore(nil)
	store.tenantIDs = []int64{1, 2}
	coordinator := newTestTaskDefinitionEditCoordinator(
		store, &definitionEditCoordinatorFakeScheduler{},
	)
	coordinator.recoveryCursor = 1

	if err := coordinator.RecoverStaleOnce(t.Context()); err != nil {
		t.Fatal(err)
	}
	queried, _, cursors := store.recoverySnapshot()
	if !slices.Equal(queried, []int64{2, 1}) {
		t.Fatalf("wrapped tenant shards = %v, want each tenant once in rotation order", queried)
	}
	if !slices.Equal(cursors, []int64{1, 0}) {
		t.Fatalf("wrapped discovery cursors = %v, want after-cursor then zero", cursors)
	}
}

func newTestTaskDefinitionEditCoordinator(
	store TaskDefinitionEditStore,
	schedules TaskDefinitionEditScheduler,
) *TaskDefinitionEditCoordinator {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewTaskDefinitionEditCoordinator(store, schedules, logger)
}

type definitionEditCoordinatorFakeScheduler struct {
	mu sync.Mutex

	prepareCalls               int
	prepared                   scheduler.PreparedTaskDefinitionEdit
	baseSnapshot               scheduler.TaskDefinitionEditSnapshot
	prepareErr                 error
	pauseErr                   error
	applyErr                   error
	restoreErr                 error
	calls                      []scheduler.TaskDefinitionEditPhase
	environmentValidationCalls int
	environmentValidationErr   error
}

func (s *definitionEditCoordinatorFakeScheduler) PrepareTaskDefinitionEdit(
	_ context.Context,
	_ scheduler.TaskDefinitionEditRequest,
) (scheduler.PreparedTaskDefinitionEdit, scheduler.TaskDefinitionEditSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.prepareCalls++
	if s.prepareErr != nil {
		return scheduler.PreparedTaskDefinitionEdit{}, scheduler.TaskDefinitionEditSnapshot{}, s.prepareErr
	}
	if s.prepared.OperationID == "" {
		return scheduler.PreparedTaskDefinitionEdit{}, scheduler.TaskDefinitionEditSnapshot{},
			errors.New("unexpected PrepareTaskDefinitionEdit call")
	}
	return s.prepared, s.baseSnapshot, nil
}

func (s *definitionEditCoordinatorFakeScheduler) PauseTaskDefinitionEdit(
	_ context.Context,
	prepared scheduler.PreparedTaskDefinitionEdit,
) (scheduler.TaskDefinitionEditSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pauseErr != nil {
		s.calls = append(s.calls, scheduler.TaskDefinitionEditPhaseBasePaused)
		return scheduler.TaskDefinitionEditSnapshot{}, s.pauseErr
	}
	phase := scheduler.TaskDefinitionEditPhaseBasePaused
	representation := prepared.BasePaused
	if prepared.OriginalState == scheduler.TaskDefinitionEditOriginalStatePaused {
		phase = scheduler.TaskDefinitionEditPhaseBaseOriginal
		representation = prepared.BaseOriginal
	}
	s.calls = append(s.calls, phase)
	return definitionEditCoordinatorSnapshot(prepared, phase, representation, "Aw"), nil
}

func (s *definitionEditCoordinatorFakeScheduler) ApplyTaskDefinitionEdit(
	_ context.Context,
	prepared scheduler.PreparedTaskDefinitionEdit,
	source scheduler.TaskDefinitionEditSnapshot,
) (scheduler.TaskDefinitionEditSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.applyErr != nil {
		s.calls = append(s.calls, scheduler.TaskDefinitionEditPhaseTargetPaused)
		return scheduler.TaskDefinitionEditSnapshot{}, s.applyErr
	}
	wantSource := scheduler.TaskDefinitionEditPhaseBasePaused
	phase := scheduler.TaskDefinitionEditPhaseTargetPaused
	representation := prepared.TargetPaused
	if prepared.OriginalState == scheduler.TaskDefinitionEditOriginalStatePaused {
		wantSource = scheduler.TaskDefinitionEditPhaseBaseOriginal
		phase = scheduler.TaskDefinitionEditPhaseTargetFinal
		representation = prepared.TargetFinal
	}
	if source.Phase != wantSource {
		return scheduler.TaskDefinitionEditSnapshot{}, fmt.Errorf(
			"apply source phase = %s, want %s", source.Phase, wantSource,
		)
	}
	s.calls = append(s.calls, phase)
	return definitionEditCoordinatorSnapshot(prepared, phase, representation, "BA"), nil
}

func (s *definitionEditCoordinatorFakeScheduler) RestoreTaskDefinitionEdit(
	_ context.Context,
	prepared scheduler.PreparedTaskDefinitionEdit,
	source scheduler.TaskDefinitionEditSnapshot,
) (scheduler.TaskDefinitionEditSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.restoreErr != nil {
		s.calls = append(s.calls, scheduler.TaskDefinitionEditPhaseTargetFinal)
		return scheduler.TaskDefinitionEditSnapshot{}, s.restoreErr
	}
	wantSource := scheduler.TaskDefinitionEditPhaseTargetPaused
	if prepared.OriginalState == scheduler.TaskDefinitionEditOriginalStatePaused {
		wantSource = scheduler.TaskDefinitionEditPhaseTargetFinal
	}
	if source.Phase != wantSource {
		return scheduler.TaskDefinitionEditSnapshot{}, fmt.Errorf(
			"restore source phase = %s, want %s", source.Phase, wantSource,
		)
	}
	s.calls = append(s.calls, scheduler.TaskDefinitionEditPhaseTargetFinal)
	return definitionEditCoordinatorSnapshot(
		prepared, scheduler.TaskDefinitionEditPhaseTargetFinal, prepared.TargetFinal, "BQ",
	), nil
}

func (s *definitionEditCoordinatorFakeScheduler) ValidateTaskDefinitionEditEnvironment(
	_ context.Context,
	_ scheduler.PreparedTaskDefinitionEdit,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.environmentValidationCalls++
	return s.environmentValidationErr
}

func (s *definitionEditCoordinatorFakeScheduler) totalCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.calls)
}

func (s *definitionEditCoordinatorFakeScheduler) prepareCallCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.prepareCalls
}

func (s *definitionEditCoordinatorFakeScheduler) snapshotPhases() []scheduler.TaskDefinitionEditPhase {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.calls)
}

func definitionEditCoordinatorSnapshot(
	prepared scheduler.PreparedTaskDefinitionEdit,
	phase scheduler.TaskDefinitionEditPhase,
	representation scheduler.PreparedTaskDefinitionEditSchedule,
	revision string,
) scheduler.TaskDefinitionEditSnapshot {
	return scheduler.TaskDefinitionEditSnapshot{
		TaskID:               prepared.Creation.TaskID,
		RequestDigest:        prepared.RequestDigest,
		Phase:                phase,
		RepresentationDigest: representation.Digest,
		Revision:             revision,
	}
}

type definitionEditCoordinatorFakeStore struct {
	mu sync.Mutex

	op *types.TaskDefinitionEditOperation

	events []string

	createCalls               int
	createErr                 error
	createApplyBeforeError    bool
	acquireCalls              int
	acquireErr                error
	acquireApplyBeforeError   bool
	acquireBusyTerminalStatus types.TaskDefinitionEditOperationStatus
	acquireBusyTerminalPhase  types.TaskDefinitionEditPhase
	acquireBusyTerminalReason string
	allowTakeover             bool
	completeCalls             int
	completeErr               error
	completeApplyBeforeError  bool
	quiesceErr                error
	baseCheckpointErr         error
	definitionCommitErr       error
	applyCheckpointErr        error
	restoreCheckpointErr      error

	tenantIDs    []int64
	shardErrs    map[int64]error
	fillRecovery bool
	queried      []int64
	limits       []int
	cursors      []int64
}

type definitionEditPreflightPagedStore struct {
	TaskDefinitionEditStore

	op               types.TaskDefinitionEditOperation
	fullPages        int
	shortTail        int
	operationCursors []string
}

func (s *definitionEditPreflightPagedStore) ListRecoveryTenantCatalogPage(
	_ context.Context,
	afterTenantID int64,
	_ int,
) ([]int64, error) {
	if afterTenantID >= s.op.TenantID {
		return nil, nil
	}
	return []int64{s.op.TenantID}, nil
}

func (s *definitionEditPreflightPagedStore) ListNonterminalTaskDefinitionEditOperations(
	_ context.Context,
	_ int64,
	afterOperationID string,
	limit int,
) ([]types.TaskDefinitionEditOperation, error) {
	s.operationCursors = append(s.operationCursors, afterOperationID)
	if len(s.operationCursors) <= s.fullPages {
		operations := make([]types.TaskDefinitionEditOperation, limit)
		for i := range operations {
			operations[i] = *cloneDefinitionEditCoordinatorOperation(&s.op)
		}
		return operations, nil
	}
	if s.shortTail > 0 {
		tail := s.shortTail
		s.shortTail = 0
		operations := make([]types.TaskDefinitionEditOperation, tail)
		for i := range operations {
			operations[i] = *cloneDefinitionEditCoordinatorOperation(&s.op)
		}
		return operations, nil
	}
	return nil, nil
}

func newDefinitionEditCoordinatorFakeStore(
	op *types.TaskDefinitionEditOperation,
) *definitionEditCoordinatorFakeStore {
	return &definitionEditCoordinatorFakeStore{op: cloneDefinitionEditCoordinatorOperation(op)}
}

func (s *definitionEditCoordinatorFakeStore) CreateTaskDefinitionEditOperation(
	_ context.Context,
	params types.CreateTaskDefinitionEditOperationParams,
) (*types.TaskDefinitionEditOperation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.createCalls++
	injectedErr := s.createErr
	if injectedErr != nil && !s.createApplyBeforeError {
		s.createErr = nil
		return nil, injectedErr
	}
	frozen, err := DecodeFrozenTaskDefinitionEditProposal(
		params.CanonicalProposal, params.BaseDefinition, params.TargetDefinition,
		params.PreparedEdit, params.BaseSnapshot,
	)
	if err != nil {
		return nil, err
	}
	if s.op == nil {
		s.op = definitionEditCoordinatorOperation(frozen, false)
	} else if !definitionEditOperationMatchesFrozen(s.op, frozen) {
		return nil, types.ErrConflict
	}
	result := cloneDefinitionEditCoordinatorOperation(s.op)
	if injectedErr != nil {
		s.createErr = nil
		return nil, injectedErr
	}
	return result, nil
}

func (s *definitionEditCoordinatorFakeStore) ListRecoveryTenantCatalogPage(
	_ context.Context,
	afterTenantID int64,
	limit int,
) ([]int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cursors = append(s.cursors, afterTenantID)
	if len(s.tenantIDs) > 0 {
		result := make([]int64, 0, limit)
		for _, tenantID := range s.tenantIDs {
			if tenantID > afterTenantID {
				result = append(result, tenantID)
				if len(result) == limit {
					break
				}
			}
		}
		return result, nil
	}
	if s.op == nil || s.op.TenantID <= afterTenantID ||
		taskDefinitionEditOperationTerminal(s.op.Status) {
		return nil, nil
	}
	return []int64{s.op.TenantID}, nil
}

func (s *definitionEditCoordinatorFakeStore) ListNonterminalTaskDefinitionEditOperations(
	_ context.Context,
	tenantID int64,
	afterOperationID string,
	_ int,
) ([]types.TaskDefinitionEditOperation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.op == nil || s.op.TenantID != tenantID || s.op.ID <= afterOperationID ||
		taskDefinitionEditOperationTerminal(s.op.Status) {
		return nil, nil
	}
	return []types.TaskDefinitionEditOperation{
		*cloneDefinitionEditCoordinatorOperation(s.op),
	}, nil
}

func (s *definitionEditCoordinatorFakeStore) LoadTaskDefinitionEditOperation(
	_ context.Context,
	scope types.TaskDefinitionEditScope,
) (*types.TaskDefinitionEditOperation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.op == nil || s.op.Scope() != scope {
		return nil, types.ErrNotFound
	}
	return cloneDefinitionEditCoordinatorOperation(s.op), nil
}

func (s *definitionEditCoordinatorFakeStore) ExpireTaskDefinitionEditOperation(
	_ context.Context,
	params types.ExpireTaskDefinitionEditOperationParams,
) (*types.TaskDefinitionEditOperation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.op == nil || s.op.Scope() != params.Scope {
		return nil, types.ErrNotFound
	}
	if s.op.Status != types.TaskDefinitionEditOperationStatusPending {
		return cloneDefinitionEditCoordinatorOperation(s.op), types.ErrTaskDefinitionEditTerminal
	}
	s.op.Status = types.TaskDefinitionEditOperationStatusExpired
	s.op.ReceiptProvider = params.ReceiptProvider
	s.op.ReceiptTarget = params.ReceiptTarget
	return cloneDefinitionEditCoordinatorOperation(s.op), nil
}

func (s *definitionEditCoordinatorFakeStore) AcquireTaskDefinitionEditOperation(
	_ context.Context,
	params types.AcquireTaskDefinitionEditOperationParams,
) (*types.TaskDefinitionEditOperation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.acquireCalls++
	if s.op == nil || s.op.Scope() != params.Scope {
		return nil, types.ErrNotFound
	}
	if s.acquireBusyTerminalStatus != "" {
		s.op.Status = s.acquireBusyTerminalStatus
		s.op.Phase = s.acquireBusyTerminalPhase
		s.op.ErrorCode = s.acquireBusyTerminalReason
		s.op.ReceiptProvider = params.ReceiptProvider
		s.op.ReceiptTarget = params.ReceiptTarget
		if s.op.Status == types.TaskDefinitionEditOperationStatusCompleted {
			s.op.Result = json.RawMessage(`{"version":"test"}`)
		}
		s.acquireBusyTerminalStatus = ""
		return cloneDefinitionEditCoordinatorOperation(s.op), types.ErrTaskDefinitionEditBusy
	}
	injectedErr := s.acquireErr
	if injectedErr != nil && !s.acquireApplyBeforeError {
		s.acquireErr = nil
		return nil, injectedErr
	}
	switch s.op.Status {
	case types.TaskDefinitionEditOperationStatusPending:
		s.op.Status = types.TaskDefinitionEditOperationStatusExecuting
		s.op.LeaseOwner = params.LeaseOwner
		s.op.Fence++
		s.op.Attempt++
	case types.TaskDefinitionEditOperationStatusExecuting:
		if s.op.LeaseOwner == params.LeaseOwner && s.op.Fence > 0 {
			// Exact acquire replay after a lost response.
		} else if s.allowTakeover {
			s.allowTakeover = false
			s.op.LeaseOwner = params.LeaseOwner
			s.op.Fence++
			s.op.Attempt++
		} else {
			return cloneDefinitionEditCoordinatorOperation(s.op), types.ErrTaskDefinitionEditBusy
		}
	default:
		return cloneDefinitionEditCoordinatorOperation(s.op), types.ErrTaskDefinitionEditTerminal
	}
	s.op.ReceiptProvider = params.ReceiptProvider
	s.op.ReceiptTarget = params.ReceiptTarget
	leaseUntil := time.Now().Add(params.LeaseDuration)
	s.op.LeaseUntil = &leaseUntil
	result := cloneDefinitionEditCoordinatorOperation(s.op)
	if injectedErr != nil {
		s.acquireErr = nil
		return nil, injectedErr
	}
	return result, nil
}

func (s *definitionEditCoordinatorFakeStore) RenewTaskDefinitionEditLease(
	_ context.Context,
	lease types.TaskDefinitionEditLease,
	_ time.Duration,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkLease(lease); err != nil {
		return err
	}
	s.events = append(s.events, "renew")
	return nil
}

func (s *definitionEditCoordinatorFakeStore) ListStaleTaskDefinitionEditOperations(
	_ context.Context,
	tenantID int64,
	_ time.Time,
	limit int,
) ([]types.TaskDefinitionEditOperation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queried = append(s.queried, tenantID)
	s.limits = append(s.limits, limit)
	if err := s.shardErrs[tenantID]; err != nil {
		return nil, err
	}
	if !s.fillRecovery {
		return nil, nil
	}
	result := make([]types.TaskDefinitionEditOperation, limit)
	for i := range result {
		result[i] = types.TaskDefinitionEditOperation{
			ID:       fmt.Sprintf("missing-%d-%d", tenantID, i),
			TenantID: tenantID,
			UserID:   42,
			TaskID:   fmt.Sprintf("missing-task-%d-%d", tenantID, i),
		}
	}
	return result, nil
}

func (s *definitionEditCoordinatorFakeStore) QuiesceTaskDefinitionEdit(
	_ context.Context,
	lease types.TaskDefinitionEditLease,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkLease(lease); err != nil {
		return err
	}
	if s.op.Phase != types.TaskDefinitionEditPhaseProposalSealed {
		return types.ErrConflict
	}
	s.events = append(s.events, "quiesce")
	s.op.Phase = types.TaskDefinitionEditPhaseDBQuiesced
	if s.quiesceErr != nil {
		err := s.quiesceErr
		s.quiesceErr = nil
		return err
	}
	return nil
}

func (s *definitionEditCoordinatorFakeStore) AuthorizeTaskDefinitionEditRemotePhase(
	_ context.Context,
	lease types.TaskDefinitionEditLease,
	expected types.TaskDefinitionEditPhase,
) (*types.TaskDefinitionEditOperation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkLease(lease); err != nil {
		return nil, err
	}
	if s.op.Phase != expected {
		return nil, types.ErrConflict
	}
	s.events = append(s.events, "authorize:"+string(expected))
	return cloneDefinitionEditCoordinatorOperation(s.op), nil
}

func (s *definitionEditCoordinatorFakeStore) CheckpointTaskDefinitionEditBasePaused(
	_ context.Context,
	lease types.TaskDefinitionEditLease,
	raw []byte,
) error {
	if err := s.checkpoint(
		lease, types.TaskDefinitionEditPhaseDBQuiesced,
		types.TaskDefinitionEditPhaseTemporalBasePaused, "base_paused", raw,
		func(op *types.TaskDefinitionEditOperation, value []byte) {
			op.PauseSnapshot = value
			op.PauseSnapshotDigest = sha256Hex(value)
		},
	); err != nil {
		return err
	}
	return s.takeCheckpointError(&s.baseCheckpointErr)
}

func (s *definitionEditCoordinatorFakeStore) CommitTaskDefinitionEditDefinition(
	_ context.Context,
	lease types.TaskDefinitionEditLease,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkLease(lease); err != nil {
		return err
	}
	if s.op.Phase != types.TaskDefinitionEditPhaseTemporalBasePaused {
		return types.ErrConflict
	}
	s.events = append(s.events, "commit_definition")
	s.op.Phase = types.TaskDefinitionEditPhaseDefinitionCommitted
	if s.definitionCommitErr != nil {
		err := s.definitionCommitErr
		s.definitionCommitErr = nil
		return err
	}
	return nil
}

func (s *definitionEditCoordinatorFakeStore) CheckpointTaskDefinitionEditTargetApplied(
	_ context.Context,
	lease types.TaskDefinitionEditLease,
	raw []byte,
) error {
	if err := s.checkpoint(
		lease, types.TaskDefinitionEditPhaseDefinitionCommitted,
		types.TaskDefinitionEditPhaseTemporalTargetApplied, "target_applied", raw,
		func(op *types.TaskDefinitionEditOperation, value []byte) {
			op.ApplySnapshot = value
			op.ApplySnapshotDigest = sha256Hex(value)
		},
	); err != nil {
		return err
	}
	return s.takeCheckpointError(&s.applyCheckpointErr)
}

func (s *definitionEditCoordinatorFakeStore) CheckpointTaskDefinitionEditTargetRestored(
	_ context.Context,
	lease types.TaskDefinitionEditLease,
	raw []byte,
) error {
	if err := s.checkpoint(
		lease, types.TaskDefinitionEditPhaseTemporalTargetApplied,
		types.TaskDefinitionEditPhaseTemporalTargetRestored, "target_restored", raw,
		func(op *types.TaskDefinitionEditOperation, value []byte) {
			op.RestoreSnapshot = value
			op.RestoreSnapshotDigest = sha256Hex(value)
		},
	); err != nil {
		return err
	}
	return s.takeCheckpointError(&s.restoreCheckpointErr)
}

func (s *definitionEditCoordinatorFakeStore) BlockTaskDefinitionEditOperation(
	_ context.Context,
	lease types.TaskDefinitionEditLease,
	reason types.TaskDefinitionEditBlockReason,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkLease(lease); err != nil {
		return err
	}
	s.events = append(s.events, "block:"+string(reason))
	s.op.Status = types.TaskDefinitionEditOperationStatusBlocked
	s.op.ErrorCode = string(reason)
	s.op.LeaseUntil = nil
	return nil
}

func (s *definitionEditCoordinatorFakeStore) CompleteTaskDefinitionEditOperation(
	_ context.Context,
	lease types.TaskDefinitionEditLease,
	result json.RawMessage,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.completeCalls++
	if err := s.checkLease(lease); err != nil {
		return err
	}
	if s.op.Phase != types.TaskDefinitionEditPhaseTemporalTargetRestored {
		return types.ErrConflict
	}
	injectedErr := s.completeErr
	if injectedErr != nil && !s.completeApplyBeforeError {
		s.completeErr = nil
		return injectedErr
	}
	s.events = append(s.events, "complete")
	s.op.Status = types.TaskDefinitionEditOperationStatusCompleted
	s.op.Result = bytes.Clone(result)
	s.op.LeaseUntil = nil
	if injectedErr != nil {
		s.completeErr = nil
		return injectedErr
	}
	return nil
}

func (s *definitionEditCoordinatorFakeStore) checkpoint(
	lease types.TaskDefinitionEditLease,
	from types.TaskDefinitionEditPhase,
	to types.TaskDefinitionEditPhase,
	event string,
	raw []byte,
	apply func(*types.TaskDefinitionEditOperation, []byte),
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkLease(lease); err != nil {
		return err
	}
	if s.op.Phase != from {
		return types.ErrConflict
	}
	value := bytes.Clone(raw)
	apply(s.op, value)
	s.op.Phase = to
	s.events = append(s.events, "checkpoint:"+event)
	return nil
}

func (s *definitionEditCoordinatorFakeStore) takeCheckpointError(target *error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := *target
	*target = nil
	return err
}

func (s *definitionEditCoordinatorFakeStore) checkLease(
	lease types.TaskDefinitionEditLease,
) error {
	if s.op == nil || s.op.Lease() != lease ||
		s.op.Status != types.TaskDefinitionEditOperationStatusExecuting {
		return types.ErrTaskDefinitionEditLeaseLost
	}
	return nil
}

func (s *definitionEditCoordinatorFakeStore) operation() *types.TaskDefinitionEditOperation {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneDefinitionEditCoordinatorOperation(s.op)
}

func (s *definitionEditCoordinatorFakeStore) eventsSnapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.events)
}

func (s *definitionEditCoordinatorFakeStore) recoverySnapshot() ([]int64, []int, []int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.queried), slices.Clone(s.limits), slices.Clone(s.cursors)
}

func (s *definitionEditCoordinatorFakeStore) resetRecoveryObservations() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queried = nil
	s.limits = nil
	s.cursors = nil
}

func cloneDefinitionEditCoordinatorOperation(
	op *types.TaskDefinitionEditOperation,
) *types.TaskDefinitionEditOperation {
	if op == nil {
		return nil
	}
	clone := *op
	clone.BaseDefinition = bytes.Clone(op.BaseDefinition)
	clone.TargetDefinition = bytes.Clone(op.TargetDefinition)
	clone.CanonicalProposal = bytes.Clone(op.CanonicalProposal)
	clone.PreparedEdit = bytes.Clone(op.PreparedEdit)
	clone.BaseSnapshot = bytes.Clone(op.BaseSnapshot)
	clone.PauseSnapshot = bytes.Clone(op.PauseSnapshot)
	clone.ApplySnapshot = bytes.Clone(op.ApplySnapshot)
	clone.RestoreSnapshot = bytes.Clone(op.RestoreSnapshot)
	clone.Result = bytes.Clone(op.Result)
	if op.LeaseUntil != nil {
		value := *op.LeaseUntil
		clone.LeaseUntil = &value
	}
	return &clone
}

func definitionEditCoordinatorOperation(
	frozen FrozenTaskDefinitionEditProposal,
	executing bool,
) *types.TaskDefinitionEditOperation {
	proposal := frozen.Proposal
	originalStatus := types.ScheduleStatusActive
	if proposal.OriginalStatus == TaskDefinitionEditOriginalStatusV2Paused {
		originalStatus = types.ScheduleStatusPaused
	}
	op := &types.TaskDefinitionEditOperation{
		ID:             proposal.OperationID,
		TenantID:       proposal.Actor.TenantID,
		UserID:         proposal.Actor.UserID,
		TargetTenantID: proposal.Target.TenantID,
		TargetUserID:   proposal.Target.UserID,
		TaskID:         proposal.Target.TaskID,
		SessionID:      proposal.SessionID,
		OperationRef:   proposal.OperationRef,
		Status:         types.TaskDefinitionEditOperationStatusPending,
		Phase:          types.TaskDefinitionEditPhaseProposalSealed,
		ExpiresAt:      time.UnixMicro(proposal.ExpiresAtUnixMicros),
		OriginalStatus: originalStatus,

		BaseDefinitionVersion:   proposal.BaseHead.Version,
		BaseDefinitionDigest:    proposal.BaseHead.Digest,
		BaseDefinition:          bytes.Clone(frozen.BaseDefinitionBytes),
		TargetDefinitionVersion: proposal.TargetHead.Version,
		TargetDefinitionDigest:  proposal.TargetHead.Digest,
		TargetDefinition:        bytes.Clone(frozen.TargetDefinitionBytes),
		CanonicalProposal:       bytes.Clone(frozen.CanonicalProposal),
		ProposalDigest:          frozen.ProposalDigest,
		PreparedEdit:            bytes.Clone(frozen.PreparedEditBytes),
		PreparedEditDigest:      proposal.PreparedEditDigest,
		BaseSnapshot:            bytes.Clone(frozen.BaseSnapshotBytes),
		BaseSnapshotDigest:      proposal.BaseSnapshotDigest,
	}
	if executing {
		op.Status = types.TaskDefinitionEditOperationStatusExecuting
		op.LeaseOwner = "definition-edit-unit-worker"
		op.Fence = 1
		op.Attempt = 1
		op.ReceiptProvider = testTaskDefinitionEditReceiptTarget.Provider
		op.ReceiptTarget = testTaskDefinitionEditReceiptTarget.Target
		leaseUntil := time.Now().Add(time.Minute)
		op.LeaseUntil = &leaseUntil
	}
	return op
}

func definitionEditCoordinatorFrozenFixture(
	t *testing.T,
	originalStatus types.ScheduleStatus,
) FrozenTaskDefinitionEditProposal {
	t.Helper()
	fixture := loadDefinitionEditProposalFixture(t)
	if originalStatus == types.ScheduleStatusPaused {
		fixture = pausedDefinitionEditCoordinatorFixture(t, fixture)
	}
	input := validDefinitionEditProposalInput(fixture)
	input.OriginalStatus = originalStatus
	frozen, err := BuildFrozenTaskDefinitionEditProposal(input)
	if err != nil {
		t.Fatalf("BuildFrozenTaskDefinitionEditProposal(%s): %v", originalStatus, err)
	}
	return frozen
}

// pausedDefinitionEditCoordinatorFixture derives a second canonical wire from
// the checked-in active fixture. The digest helpers below deliberately mirror
// the public JSON protocol so this test cannot bypass production decoding.
func pausedDefinitionEditCoordinatorFixture(
	t *testing.T,
	fixture decodedDefinitionEditProposalFixture,
) decodedDefinitionEditProposalFixture {
	t.Helper()
	prepared := fixture.prepared
	prepared.OriginalState = scheduler.TaskDefinitionEditOriginalStatePaused
	prepared.BaseOriginal.State.Paused = true
	prepared.BaseOriginal.State.Note = "operator-paused-before-edit"
	prepared.BasePaused = prepared.BaseOriginal

	seed := definitionEditCoordinatorOperationSeed{
		WireVersion:            prepared.WireVersion,
		OperationID:            prepared.OperationID,
		CreationRequestDigest:  prepared.Creation.RequestDigest,
		TenantID:               prepared.Creation.TenantID,
		UserID:                 prepared.Creation.UserID,
		TaskID:                 prepared.Creation.TaskID,
		BaseHead:               prepared.BaseHead,
		TargetHead:             prepared.TargetHead,
		OriginalState:          prepared.OriginalState,
		BaseProjectionDigest:   prepared.BaseProjectionDigest,
		TargetProjectionDigest: prepared.TargetProjectionDigest,
		BaseTiming:             prepared.BaseOriginal.Timing,
		BaseAction:             prepared.BaseOriginal.Action,
		BasePolicy:             prepared.BaseOriginal.Policy,
		BaseReusePolicy:        prepared.BaseOriginal.WorkflowIDReusePolicy,
		BaseState:              prepared.BaseOriginal.State,
		TargetTiming:           prepared.TargetFinal.Timing,
		TargetAction:           prepared.TargetFinal.Action,
		TargetPolicy:           prepared.TargetFinal.Policy,
		TargetReusePolicy:      prepared.TargetFinal.WorkflowIDReusePolicy,
	}
	prepared.OperationDigest = definitionEditCoordinatorJSONDigest(t, seed)

	target := prepared.TargetFinal
	target.State = prepared.BaseOriginal.State
	target.Fingerprint.DefinitionVersion = prepared.TargetHead.Version
	target.Fingerprint.DefinitionDigest = prepared.TargetHead.Digest
	target.Fingerprint.EditOperationDigest = prepared.OperationDigest
	target.Fingerprint.EditPhase = "final_paused"
	target.Digest = definitionEditCoordinatorRepresentationDigest(t, target)
	prepared.TargetFinal = target
	prepared.TargetPaused = target
	prepared.BaseOriginal.Digest = definitionEditCoordinatorRepresentationDigest(
		t, prepared.BaseOriginal,
	)
	prepared.BasePaused = prepared.BaseOriginal
	prepared.RequestDigest = ""
	prepared.RequestDigest = definitionEditCoordinatorJSONDigest(t, prepared)

	preparedBytes, err := scheduler.EncodePreparedTaskDefinitionEdit(prepared)
	if err != nil {
		t.Fatalf("encode paused prepared edit: %v", err)
	}
	prepared, err = scheduler.DecodePreparedTaskDefinitionEdit(preparedBytes)
	if err != nil {
		t.Fatalf("decode paused prepared edit: %v", err)
	}
	baseSnapshot := definitionEditCoordinatorSnapshot(
		prepared, scheduler.TaskDefinitionEditPhaseBaseOriginal,
		prepared.BaseOriginal, prepared.BaseRevision,
	)
	fixture.prepared = prepared
	fixture.baseSnapshot = baseSnapshot
	return fixture
}

type definitionEditCoordinatorOperationSeed struct {
	WireVersion            string                                    `json:"wire_version"`
	OperationID            string                                    `json:"operation_id"`
	CreationRequestDigest  string                                    `json:"creation_request_digest"`
	TenantID               int64                                     `json:"tenant_id"`
	UserID                 int64                                     `json:"user_id"`
	TaskID                 string                                    `json:"task_id"`
	BaseHead               scheduler.TaskDefinitionEditHead          `json:"base_head"`
	TargetHead             scheduler.TaskDefinitionEditHead          `json:"target_head"`
	OriginalState          scheduler.TaskDefinitionEditOriginalState `json:"original_state"`
	BaseProjectionDigest   string                                    `json:"base_projection_digest"`
	TargetProjectionDigest string                                    `json:"target_projection_digest"`
	BaseTiming             scheduler.PreparedTaskScheduleTiming      `json:"base_timing"`
	BaseAction             scheduler.PreparedTaskScheduleAction      `json:"base_action"`
	BasePolicy             scheduler.PreparedTaskSchedulePolicy      `json:"base_policy"`
	BaseReusePolicy        int32                                     `json:"base_reuse_policy"`
	BaseState              scheduler.TaskDefinitionEditScheduleState `json:"base_state"`
	TargetTiming           scheduler.PreparedTaskScheduleTiming      `json:"target_timing"`
	TargetAction           scheduler.PreparedTaskScheduleAction      `json:"target_action"`
	TargetPolicy           scheduler.PreparedTaskSchedulePolicy      `json:"target_policy"`
	TargetReusePolicy      int32                                     `json:"target_reuse_policy"`
}

func definitionEditCoordinatorRepresentationDigest(
	t *testing.T,
	representation scheduler.PreparedTaskDefinitionEditSchedule,
) string {
	t.Helper()
	representation.Digest = ""
	return definitionEditCoordinatorJSONDigest(t, representation)
}

func definitionEditCoordinatorJSONDigest(t *testing.T, value any) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal digest fixture: %v", err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
