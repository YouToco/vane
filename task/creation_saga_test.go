package task

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/YouToco/vane/scheduler"
	"github.com/YouToco/vane/types"
)

var testCreationReceiptTarget = CreationReceiptTarget{
	Provider: FeishuCardPatchReceiptProviderForApp("cli_task_test"),
	Target:   "om_task_creation_test",
}

type creationSagaFakeStore struct {
	creationPrepareFakeStore
	tenant types.Tenant

	events                   []string
	createCalls              int
	allowTakeover            bool
	definition               types.PausedCompiledTaskDefinition
	definitionLimit          bool
	activationCommitErr      error
	beginCleanupErr          error
	finishCleanupErr         error
	completeErr              error
	acquireErr               error
	acquireApplyBeforeError  bool
	completeApplyBeforeError bool
}

func newCreationSagaFakeStore() *creationSagaFakeStore {
	return &creationSagaFakeStore{tenant: types.Tenant{
		ID: 7, Status: types.TenantStatusActive,
	}}
}

func (s *creationSagaFakeStore) ListMembershipsByUser(
	_ context.Context,
	userID int64,
) ([]types.Membership, error) {
	return []types.Membership{{TenantID: s.tenant.ID, UserID: userID}}, nil
}

func (s *creationSagaFakeStore) GetTenant(context.Context, int64) (*types.Tenant, error) {
	tenant := s.tenant
	return &tenant, nil
}

func (s *creationSagaFakeStore) LoadTaskCreationOperationByUser(
	_ context.Context,
	id string,
	userID int64,
) (*types.TaskCreationOperation, error) {
	if s.op.ID != id || s.op.UserID != userID ||
		s.op.ExecutionVersion != types.TaskCreationExecutionVersionV1 {
		return nil, types.ErrNotFound
	}
	clone := s.op
	return &clone, nil
}

func (s *creationSagaFakeStore) CreateTaskCreationOperation(
	_ context.Context,
	p types.CreateTaskCreationOperationParams,
) (*types.TaskCreationOperation, error) {
	s.createCalls++
	if s.op.ID != "" {
		return nil, types.ErrConflict
	}
	s.op = types.TaskCreationOperation{
		ID: p.ID, TenantID: p.TenantID, UserID: p.UserID, SessionID: p.SessionID,
		ToolName: "create_schedule", Args: bytes.Clone(p.Args), Summary: p.Summary,
		Status: types.PendingActionStatusPending, ExpiresAt: p.ExpiresAt,
		ExecutionVersion: types.TaskCreationExecutionVersionV1,
	}
	clone := s.op
	return &clone, nil
}

func (s *creationSagaFakeStore) CancelTaskCreationOperation(
	_ context.Context,
	p types.CancelTaskCreationOperationParams,
) (*types.TaskCreationOperation, error) {
	if s.op.ID != p.ID || s.op.TenantID != p.TenantID || s.op.UserID != p.UserID ||
		s.op.ExecutionVersion != types.TaskCreationExecutionVersionV1 {
		return nil, types.ErrNotFound
	}
	if s.op.ReceiptProvider != "" &&
		(s.op.ReceiptProvider != p.ReceiptProvider || s.op.ReceiptTarget != p.ReceiptTarget) {
		return nil, types.ErrConflict
	}
	switch s.op.Status {
	case types.PendingActionStatusCancelled:
		clone := s.op
		return &clone, nil
	case types.PendingActionStatusExecuting:
		if s.op.ReceiptProvider == "" && s.op.ReceiptTarget == "" {
			s.op.ReceiptProvider = p.ReceiptProvider
			s.op.ReceiptTarget = p.ReceiptTarget
		} else if s.op.ReceiptProvider != p.ReceiptProvider ||
			s.op.ReceiptTarget != p.ReceiptTarget {
			return nil, types.ErrConflict
		}
		clone := s.op
		return &clone, types.ErrTaskCreationBusy
	case types.PendingActionStatusPending:
		s.op.ReceiptProvider = p.ReceiptProvider
		s.op.ReceiptTarget = p.ReceiptTarget
		s.op.Status = types.PendingActionStatusCancelled
		s.op.Phase = types.TaskCreationPhaseCancelled
		now := time.Now()
		s.op.TombstonedAt = &now
		clone := s.op
		return &clone, nil
	default:
		return nil, types.ErrTaskCreationTerminal
	}
}

func (s *creationSagaFakeStore) AcquireTaskCreationOperation(
	_ context.Context,
	p types.AcquireTaskCreationOperationParams,
) (*types.TaskCreationOperation, error) {
	if s.op.ID != p.ID || s.op.TenantID != p.TenantID || s.op.UserID != p.UserID {
		return nil, types.ErrNotFound
	}
	injectedErr := s.acquireErr
	if injectedErr != nil && !s.acquireApplyBeforeError {
		s.acquireErr = nil
		return nil, injectedErr
	}
	switch s.op.Status {
	case types.PendingActionStatusPending:
		s.op.ReceiptProvider = p.ReceiptProvider
		s.op.ReceiptTarget = p.ReceiptTarget
		s.op.Status = types.PendingActionStatusExecuting
		s.op.Phase = types.TaskCreationPhaseClaimed
		s.op.LeaseOwner = p.LeaseOwner
		s.op.Fence++
		s.op.Attempt++
	case types.PendingActionStatusExecuting:
		if s.op.ReceiptProvider == "" && s.op.ReceiptTarget == "" &&
			p.ReceiptProvider != "" && p.ReceiptTarget != "" {
			s.op.ReceiptProvider = p.ReceiptProvider
			s.op.ReceiptTarget = p.ReceiptTarget
		} else if s.op.ReceiptProvider != p.ReceiptProvider || s.op.ReceiptTarget != p.ReceiptTarget {
			return nil, types.ErrConflict
		}
		if s.op.LeaseOwner == p.LeaseOwner && s.op.Fence > 0 {
			clone := s.op
			return &clone, nil
		}
		if !s.allowTakeover {
			clone := s.op
			return &clone, types.ErrTaskCreationBusy
		}
		s.allowTakeover = false
		s.op.LeaseOwner = p.LeaseOwner
		s.op.Fence++
		s.op.Attempt++
	default:
		return nil, types.ErrTaskCreationTerminal
	}
	leaseUntil := time.Now().Add(p.LeaseDuration)
	s.op.LeaseUntil = &leaseUntil
	clone := s.op
	if injectedErr != nil {
		s.acquireErr = nil
		return nil, injectedErr
	}
	return &clone, nil
}

func (s *creationSagaFakeStore) CheckpointTaskCreationEnsureReceipt(
	_ context.Context,
	lease types.TaskCreationLease,
	receipt []byte,
	taskID string,
) error {
	if err := s.checkLease(lease); err != nil {
		return err
	}
	s.events = append(s.events, "checkpoint_ensure")
	s.op.EnsureReceipt = bytes.Clone(receipt)
	s.op.TaskID = taskID
	s.op.Phase = types.TaskCreationPhaseScheduleEnsured
	return nil
}

func (s *creationSagaFakeStore) CommitPausedCompiledTaskDefinitionForCreation(
	_ context.Context,
	p types.CommitPausedCompiledTaskDefinitionForCreationParams,
) error {
	if err := s.checkLease(p.Lease); err != nil {
		return err
	}
	s.events = append(s.events, "commit_definition")
	if s.definitionLimit {
		return types.ErrTaskCreationLimit
	}
	if creationPhaseAtLeast(s.op.Phase, types.TaskCreationPhaseDefinitionCommitted) {
		return nil
	}
	if s.op.Phase != types.TaskCreationPhaseScheduleEnsured ||
		s.op.TaskID != p.Definition.TaskID ||
		!bytes.Equal(s.op.PreparedSchedule, p.PreparedSchedule) ||
		!bytes.Equal(s.op.EnsureReceipt, p.EnsureReceipt) {
		return types.ErrConflict
	}
	s.definition = p.Definition
	s.op.Phase = types.TaskCreationPhaseDefinitionCommitted
	return nil
}

func (s *creationSagaFakeStore) BeginTaskCreationActivation(
	_ context.Context,
	lease types.TaskCreationLease,
	_ string,
) (bool, error) {
	if err := s.checkLease(lease); err != nil {
		return false, err
	}
	s.events = append(s.events, "begin_activation")
	if creationPhaseAtLeast(s.op.Phase, types.TaskCreationPhaseActivationStarted) {
		return false, nil
	}
	s.op.Phase = types.TaskCreationPhaseActivationStarted
	return true, nil
}

func (s *creationSagaFakeStore) CommitTaskCreationActivation(
	_ context.Context,
	lease types.TaskCreationLease,
	_ string,
) error {
	if err := s.checkLease(lease); err != nil {
		return err
	}
	s.events = append(s.events, "commit_activation")
	if s.activationCommitErr != nil {
		return s.activationCommitErr
	}
	s.op.Phase = types.TaskCreationPhaseActivated
	return nil
}

func (s *creationSagaFakeStore) BeginTaskCreationCleanup(
	_ context.Context,
	lease types.TaskCreationLease,
	_ string,
	errorCode string,
	errorMessage string,
) (bool, error) {
	if err := s.checkLease(lease); err != nil {
		return false, err
	}
	s.events = append(s.events, "begin_cleanup")
	if s.beginCleanupErr != nil {
		return false, s.beginCleanupErr
	}
	s.op.ErrorCode, s.op.ErrorMessage = errorCode, errorMessage
	s.op.Phase = types.TaskCreationPhaseCleanupPending
	return true, nil
}

func (s *creationSagaFakeStore) FinishTaskCreationCleanup(
	_ context.Context,
	lease types.TaskCreationLease,
	_ string,
	status types.PendingActionStatus,
) error {
	if err := s.checkLease(lease); err != nil {
		return err
	}
	s.events = append(s.events, "finish_cleanup")
	if s.finishCleanupErr != nil {
		return s.finishCleanupErr
	}
	s.op.Status = status
	s.op.Phase = types.TaskCreationPhaseFailed
	now := time.Now()
	s.op.TombstonedAt = &now
	s.op.LeaseUntil = nil
	s.op.TakeoverNotBefore = nil
	return nil
}

func (s *creationSagaFakeStore) BlockTaskCreationOperationAfterSideEffect(
	_ context.Context,
	lease types.TaskCreationLease,
	_ string,
	errorCode string,
	errorMessage string,
) error {
	if err := s.checkLease(lease); err != nil {
		return err
	}
	s.events = append(s.events, "block_side_effect")
	s.op.ErrorCode, s.op.ErrorMessage = errorCode, errorMessage
	s.op.Status = types.PendingActionStatusBlocked
	s.op.Phase = types.TaskCreationPhaseBlocked
	now := time.Now()
	s.op.TombstonedAt = &now
	s.op.LeaseUntil = nil
	s.op.TakeoverNotBefore = nil
	return nil
}

func (s *creationSagaFakeStore) CompleteTaskCreationOperation(
	_ context.Context,
	lease types.TaskCreationLease,
	_ string,
	result json.RawMessage,
) error {
	if err := s.checkLease(lease); err != nil {
		return err
	}
	s.events = append(s.events, "complete")
	if s.completeErr != nil {
		if !s.completeApplyBeforeError {
			return s.completeErr
		}
	}
	s.op.Result = bytes.Clone(result)
	s.op.Status = types.PendingActionStatusExecuted
	s.op.Phase = types.TaskCreationPhaseCompleted
	now := time.Now()
	s.op.ExecutedAt = &now
	s.op.TombstonedAt = &now
	s.op.LeaseUntil = nil
	s.op.TakeoverNotBefore = nil
	if s.terminalWriteCancel != nil {
		s.terminalWriteCancel()
	}
	if s.completeErr != nil {
		return s.completeErr
	}
	return nil
}

func (s *creationSagaFakeStore) ListStaleTaskCreationTenantIDs(
	context.Context, time.Time, int64, int,
) ([]int64, error) {
	return nil, nil
}

func (s *creationSagaFakeStore) ListStaleTaskCreationOperations(
	context.Context, int64, time.Time, int,
) ([]types.TaskCreationOperation, error) {
	return nil, nil
}

type creationSagaFakeScheduler struct {
	events                   []string
	state                    scheduler.TaskScheduleState
	prepared                 scheduler.PreparedTaskSchedule
	prepareErr               error
	ensureErr                error
	activateErr              error
	activateApplyBeforeError bool
	activateCalls            int
	deleteErr                error
	deleteCalls              int
}

type recoveryListingStore struct {
	*creationSagaFakeStore
	tenantIDs []int64
	shardErrs map[int64]error
	fill      bool
	queried   []int64
	limits    []int
	cursors   []int64
}

func (s *recoveryListingStore) ListStaleTaskCreationTenantIDs(
	_ context.Context, _ time.Time, afterTenantID int64, limit int,
) ([]int64, error) {
	s.cursors = append(s.cursors, afterTenantID)
	result := make([]int64, 0, limit)
	for _, tenantID := range s.tenantIDs {
		if tenantID <= afterTenantID {
			continue
		}
		result = append(result, tenantID)
		if len(result) == limit {
			break
		}
	}
	return result, nil
}

func (s *recoveryListingStore) ListStaleTaskCreationOperations(
	_ context.Context,
	tenantID int64,
	_ time.Time,
	limit int,
) ([]types.TaskCreationOperation, error) {
	s.queried = append(s.queried, tenantID)
	s.limits = append(s.limits, limit)
	if err := s.shardErrs[tenantID]; err != nil {
		return nil, err
	}
	if !s.fill {
		return nil, nil
	}
	ops := make([]types.TaskCreationOperation, limit)
	for i := range ops {
		ops[i] = types.TaskCreationOperation{
			ID:       fmt.Sprintf("missing-%d-%d", tenantID, i),
			TenantID: tenantID, UserID: 11,
		}
	}
	return ops, nil
}

func (s *creationSagaFakeScheduler) PrepareTaskSchedule(
	_ context.Context,
	req scheduler.TaskScheduleRequest,
) (scheduler.PreparedTaskSchedule, error) {
	s.events = append(s.events, "prepare")
	if s.prepareErr != nil {
		return scheduler.PreparedTaskSchedule{}, s.prepareErr
	}
	s.prepared = validPreparedSchedule(req)
	return s.prepared, nil
}

func (s *creationSagaFakeScheduler) EnsurePausedTask(
	context.Context,
	scheduler.PreparedTaskSchedule,
) (scheduler.EnsurePausedTaskResult, error) {
	s.events = append(s.events, "ensure")
	if s.ensureErr != nil {
		return scheduler.EnsurePausedTaskResult{}, s.ensureErr
	}
	s.state = scheduler.TaskSchedulePausedProvisioningExact
	return scheduler.EnsurePausedTaskResult{
		Disposition: scheduler.TaskScheduleEnsured,
		Snapshot: scheduler.TaskScheduleSnapshot{
			TaskID: s.prepared.TaskID, RequestDigest: s.prepared.RequestDigest,
			PreparedDigest: s.prepared.PreparedDigest, Revision: "revision-1",
			State: scheduler.TaskSchedulePausedVirginExact,
		},
	}, nil
}

func (s *creationSagaFakeScheduler) DescribeTask(
	context.Context,
	scheduler.PreparedTaskSchedule,
) (scheduler.TaskScheduleSnapshot, error) {
	s.events = append(s.events, "describe")
	return scheduler.TaskScheduleSnapshot{
		TaskID: s.prepared.TaskID, RequestDigest: s.prepared.RequestDigest,
		PreparedDigest: s.prepared.PreparedDigest, Revision: "revision-2",
		State: s.state,
	}, nil
}

func (s *creationSagaFakeScheduler) ActivateTask(
	context.Context,
	scheduler.PreparedTaskSchedule,
	scheduler.TaskScheduleSnapshot,
) (scheduler.TaskScheduleSnapshot, error) {
	s.events = append(s.events, "activate")
	s.activateCalls++
	if s.activateErr != nil {
		if s.activateApplyBeforeError {
			s.state = scheduler.TaskScheduleActiveVirginExact
			s.activateApplyBeforeError = false
		}
		err := s.activateErr
		s.activateErr = nil
		return scheduler.TaskScheduleSnapshot{}, err
	}
	s.state = scheduler.TaskScheduleActiveVirginExact
	return s.DescribeTask(context.Background(), s.prepared)
}

func (s *creationSagaFakeScheduler) DeleteTask(
	context.Context,
	scheduler.PreparedTaskSchedule,
) error {
	s.events = append(s.events, "delete")
	s.deleteCalls++
	if s.deleteErr != nil {
		return s.deleteErr
	}
	s.state = scheduler.TaskScheduleStateUnknown
	return nil
}

func TestCreationCoordinator_ProposalCanonicalizesBeforePersistence(t *testing.T) {
	store := newCreationSagaFakeStore()
	schedules := &creationSagaFakeScheduler{}
	coordinator := NewCreationCoordinator(store, schedules, nil)

	proposal, err := coordinator.Propose(t.Context(), CreationProposalInput{
		ActionID: "action-1", UserID: 11, RawArgs: mustCreateArgs(t, "每天寻找全球 AI 热点", "每天 AI"),
		ExpiresAt: time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if proposal.ID != "action-1" || store.createCalls != 1 || len(schedules.events) != 0 {
		t.Fatalf("proposal=%+v create=%d scheduler_events=%v", proposal, store.createCalls, schedules.events)
	}
	for _, want := range []string{"目标：每天寻找全球 AI 热点", "筛选：标准", "[web/search]", "搜索“AI”"} {
		if !strings.Contains(proposal.Summary, want) {
			t.Fatalf("summary missing %q: %s", want, proposal.Summary)
		}
	}
	command, _, err := normalizeCreateScheduleCommand(store.op.Args)
	if err != nil || !bytes.Equal(command.ApprovedFetchPlan, json.RawMessage(
		`{"sources":[{"platform":"web","capability":"search","title":"A","url":"vane://web/search?q=AI\u0026category=news","config":{"category":"news","query":"AI"}}]}`,
	)) {
		t.Fatalf("durable canonical args invalid: command=%+v err=%v", command, err)
	}
}

func TestCreationCoordinator_RejectsSSRFBeforePendingAction(t *testing.T) {
	store := newCreationSagaFakeStore()
	coordinator := NewCreationCoordinator(store, &creationSagaFakeScheduler{}, nil)
	raw := mustCreateArgsWithPlan(t, "监控本地服务", "本地", json.RawMessage(
		`{"sources":[{"platform":"web","capability":"feed","url":"http://127.0.0.1/rss","config":{}}]}`,
	))
	_, err := coordinator.Propose(t.Context(), CreationProposalInput{
		ActionID: "action-ssrf", UserID: 11, RawArgs: raw,
		ExpiresAt: time.Now().Add(time.Hour),
	})
	if !errors.Is(err, types.ErrValidation) || store.createCalls != 0 {
		t.Fatalf("err=%v create_calls=%d", err, store.createCalls)
	}
}

func TestCreationCoordinator_ConfirmHappyPathAndTerminalReplay(t *testing.T) {
	store := newCreationSagaFakeStore()
	schedules := &creationSagaFakeScheduler{}
	coordinator := NewCreationCoordinator(store, schedules, nil)
	mustProposeCreation(t, coordinator, "action-happy")

	result, err := coordinator.Confirm(t.Context(), 11, "action-happy", testCreationReceiptTarget)
	if err != nil {
		t.Fatalf("Confirm: %v", err)
	}
	if result.Status != types.PendingActionStatusExecuted || result.Recovering || !result.ReceiptBound ||
		store.op.Phase != types.TaskCreationPhaseCompleted || schedules.activateCalls != 1 {
		t.Fatalf("result=%+v phase=%q activate=%d", result, store.op.Phase, schedules.activateCalls)
	}
	if !bytes.Equal(store.definition.FetchPlan, json.RawMessage(
		`{"sources":[{"platform":"web","capability":"search","title":"A","url":"vane://web/search?q=AI\u0026category=news","config":{"category":"news","query":"AI"}}]}`,
	)) {
		t.Fatalf("final plan=%s", store.definition.FetchPlan)
	}
	events := append([]string(nil), store.events...)
	replayed, err := coordinator.Confirm(t.Context(), 11, "action-happy", testCreationReceiptTarget)
	if err != nil || replayed.TaskID != result.TaskID || !bytes.Equal(mustMarshal(t, events), mustMarshal(t, store.events)) {
		t.Fatalf("terminal replay=%+v err=%v before=%v after=%v", replayed, err, events, store.events)
	}
	differentTarget := testCreationReceiptTarget
	differentTarget.Target = "om_forwarded_copy"
	if _, err := coordinator.Confirm(t.Context(), 11, "action-happy", differentTarget); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("terminal replay on a different provider resource must conflict: %v", err)
	}
}

func TestCreationCoordinator_BusyLegacyOperationBindsDurableReceipt(t *testing.T) {
	store := newCreationSagaFakeStore()
	schedules := &creationSagaFakeScheduler{}
	coordinator := NewCreationCoordinator(store, schedules, nil)
	mustProposeCreation(t, coordinator, "action-busy-legacy")

	store.op.Status = types.PendingActionStatusExecuting
	store.op.Phase = types.TaskCreationPhaseClaimed
	store.op.LeaseOwner = "pre-a6-worker"
	store.op.Fence = 1
	store.op.Attempt = 1
	leaseUntil := time.Now().Add(time.Minute)
	store.op.LeaseUntil = &leaseUntil

	result, err := coordinator.Confirm(
		t.Context(), 11, "action-busy-legacy", testCreationReceiptTarget,
	)
	if err != nil || !result.Recovering || !result.ReceiptBound ||
		result.Status != types.PendingActionStatusExecuting {
		t.Fatalf("busy result=%+v err=%v", result, err)
	}
	if store.op.ReceiptProvider != testCreationReceiptTarget.Provider ||
		store.op.ReceiptTarget != testCreationReceiptTarget.Target ||
		len(schedules.events) != 0 {
		t.Fatalf("op=%+v scheduler_events=%v", store.op, schedules.events)
	}
}

func TestCreationCoordinator_ActivationResponseLossAdoptsWithoutSecondWrite(t *testing.T) {
	store := newCreationSagaFakeStore()
	schedules := &creationSagaFakeScheduler{
		activateErr: scheduler.ErrTaskScheduleTransient, activateApplyBeforeError: true,
	}
	coordinator := NewCreationCoordinator(store, schedules, nil)
	mustProposeCreation(t, coordinator, "action-recover")

	first, err := coordinator.Confirm(t.Context(), 11, "action-recover", testCreationReceiptTarget)
	if err != nil || !first.Recovering || store.op.Phase != types.TaskCreationPhaseActivationStarted {
		t.Fatalf("first=%+v phase=%q err=%v", first, store.op.Phase, err)
	}
	store.allowTakeover = true
	second, err := coordinator.Confirm(t.Context(), 11, "action-recover", testCreationReceiptTarget)
	if err != nil || second.Status != types.PendingActionStatusExecuted || schedules.activateCalls != 1 {
		t.Fatalf("second=%+v activate_calls=%d err=%v", second, schedules.activateCalls, err)
	}
	if !containsString(schedules.events, "describe") {
		t.Fatalf("recovery did not Describe before adopt: %v", schedules.events)
	}
}

func TestCreationCoordinator_TaskLimitDeletesPausedTaskAndFails(t *testing.T) {
	store := newCreationSagaFakeStore()
	store.definitionLimit = true
	schedules := &creationSagaFakeScheduler{}
	coordinator := NewCreationCoordinator(store, schedules, nil)
	mustProposeCreation(t, coordinator, "action-limit")

	result, err := coordinator.Confirm(t.Context(), 11, "action-limit", testCreationReceiptTarget)
	if err != nil || result.Status != types.PendingActionStatusFailed ||
		schedules.deleteCalls != 1 || schedules.activateCalls != 0 {
		t.Fatalf("result=%+v delete=%d activate=%d err=%v",
			result, schedules.deleteCalls, schedules.activateCalls, err)
	}
}

func TestCreationCoordinator_DeterministicEnsureFailureDoesNotLoop(t *testing.T) {
	store := newCreationSagaFakeStore()
	schedules := &creationSagaFakeScheduler{ensureErr: scheduler.ErrTaskScheduleConflict}
	coordinator := NewCreationCoordinator(store, schedules, nil)
	mustProposeCreation(t, coordinator, "action-ensure-conflict")

	result, err := coordinator.Confirm(t.Context(), 11, "action-ensure-conflict", testCreationReceiptTarget)
	if err != nil || result.Status != types.PendingActionStatusFailed ||
		schedules.deleteCalls != 1 || containsString(schedules.events, "activate") {
		t.Fatalf("result=%+v events=%v delete=%d err=%v",
			result, schedules.events, schedules.deleteCalls, err)
	}
}

func TestCreationCoordinator_CleanupFinalizationConflictQuarantines(t *testing.T) {
	store := newCreationSagaFakeStore()
	store.definitionLimit = true
	store.finishCleanupErr = types.ErrConflict
	schedules := &creationSagaFakeScheduler{}
	coordinator := NewCreationCoordinator(store, schedules, nil)
	mustProposeCreation(t, coordinator, "action-cleanup-finalize-conflict")

	result, err := coordinator.Confirm(t.Context(), 11, store.op.ID, testCreationReceiptTarget)
	if err != nil || result.Status != types.PendingActionStatusBlocked ||
		store.op.Status != types.PendingActionStatusBlocked ||
		store.op.ErrorCode != "cleanup_finalization_invalid" || schedules.deleteCalls != 1 {
		t.Fatalf("result=%+v op=%+v delete=%d err=%v",
			result, store.op, schedules.deleteCalls, err)
	}
}

func TestCreationCoordinator_CleanupCheckpointConflictQuarantinesWithoutDelete(t *testing.T) {
	store := newCreationSagaFakeStore()
	store.definitionLimit = true
	store.beginCleanupErr = types.ErrConflict
	schedules := &creationSagaFakeScheduler{}
	coordinator := NewCreationCoordinator(store, schedules, nil)
	mustProposeCreation(t, coordinator, "action-cleanup-checkpoint-conflict")

	result, err := coordinator.Confirm(t.Context(), 11, store.op.ID, testCreationReceiptTarget)
	if err != nil || result.Status != types.PendingActionStatusBlocked ||
		store.op.Status != types.PendingActionStatusBlocked ||
		store.op.ErrorCode != "cleanup_checkpoint_invalid" || schedules.deleteCalls != 0 {
		t.Fatalf("result=%+v op=%+v delete=%d err=%v",
			result, store.op, schedules.deleteCalls, err)
	}
}

func TestCreationCoordinator_DeleteBlockedQuarantinesCleanup(t *testing.T) {
	store := newCreationSagaFakeStore()
	store.definitionLimit = true
	schedules := &creationSagaFakeScheduler{deleteErr: scheduler.ErrTaskScheduleBlocked}
	coordinator := NewCreationCoordinator(store, schedules, nil)
	mustProposeCreation(t, coordinator, "action-delete-blocked")

	result, err := coordinator.Confirm(t.Context(), 11, store.op.ID, testCreationReceiptTarget)
	if err != nil || result.Status != types.PendingActionStatusBlocked ||
		store.op.Status != types.PendingActionStatusBlocked || schedules.deleteCalls != 1 {
		t.Fatalf("result=%+v op=%+v delete=%d err=%v",
			result, store.op, schedules.deleteCalls, err)
	}
}

func TestCreationCoordinator_ActivationCommitValidationDeletesActiveTask(t *testing.T) {
	store := newCreationSagaFakeStore()
	store.activationCommitErr = types.ErrValidation
	schedules := &creationSagaFakeScheduler{}
	coordinator := NewCreationCoordinator(store, schedules, nil)
	mustProposeCreation(t, coordinator, "action-activation-commit")

	result, err := coordinator.Confirm(t.Context(), 11, "action-activation-commit", testCreationReceiptTarget)
	if err != nil || result.Status != types.PendingActionStatusFailed ||
		schedules.activateCalls != 1 || schedules.deleteCalls != 1 ||
		schedules.state != scheduler.TaskScheduleStateUnknown {
		t.Fatalf("result=%+v state=%q activate=%d delete=%d err=%v",
			result, schedules.state, schedules.activateCalls, schedules.deleteCalls, err)
	}
}

func TestCreationCoordinator_CompletionConflictQuarantinesActivatedTask(t *testing.T) {
	store := newCreationSagaFakeStore()
	store.completeErr = types.ErrConflict
	schedules := &creationSagaFakeScheduler{}
	coordinator := NewCreationCoordinator(store, schedules, nil)
	mustProposeCreation(t, coordinator, "action-complete-conflict")

	result, err := coordinator.Confirm(t.Context(), 11, "action-complete-conflict", testCreationReceiptTarget)
	if err != nil || result.Status != types.PendingActionStatusBlocked ||
		schedules.activateCalls != 1 || schedules.deleteCalls != 0 ||
		store.op.Phase != types.TaskCreationPhaseBlocked {
		t.Fatalf("result=%+v activate=%d delete=%d err=%v",
			result, schedules.activateCalls, schedules.deleteCalls, err)
	}
}

func TestCreationCoordinator_CancelV1IsScopedAndLinearized(t *testing.T) {
	store := newCreationSagaFakeStore()
	coordinator := NewCreationCoordinator(store, &creationSagaFakeScheduler{}, nil)
	mustProposeCreation(t, coordinator, "action-cancel")

	result, err := coordinator.Cancel(t.Context(), 11, "action-cancel", testCreationReceiptTarget)
	if err != nil || result.Status != types.PendingActionStatusCancelled ||
		store.op.Status != types.PendingActionStatusCancelled ||
		store.op.Phase != types.TaskCreationPhaseCancelled {
		t.Fatalf("cancel result=%+v op=%+v err=%v", result, store.op, err)
	}
	replayed, err := coordinator.Cancel(t.Context(), 11, "action-cancel", testCreationReceiptTarget)
	if err != nil || replayed.Status != types.PendingActionStatusCancelled || !replayed.Replayed {
		t.Fatalf("cancel replay=%+v err=%v", replayed, err)
	}
	if _, err := coordinator.Cancel(t.Context(), 12, "action-cancel", testCreationReceiptTarget); !errors.Is(err, ErrCreationOperationNotFound) {
		t.Fatalf("foreign user must get exact v1-not-found sentinel, got %v", err)
	}
}

func TestCreationCoordinator_CancelRacingAcquireDoesNotUndoCreation(t *testing.T) {
	store := newCreationSagaFakeStore()
	coordinator := NewCreationCoordinator(store, &creationSagaFakeScheduler{}, nil)
	mustProposeCreation(t, coordinator, "action-cancel-busy")
	if _, err := store.AcquireTaskCreationOperation(t.Context(), types.AcquireTaskCreationOperationParams{
		ID: store.op.ID, TenantID: store.op.TenantID, UserID: store.op.UserID,
		LeaseOwner: "worker", LeaseDuration: time.Minute,
		ReceiptProvider: testCreationReceiptTarget.Provider,
		ReceiptTarget:   testCreationReceiptTarget.Target,
	}); err != nil {
		t.Fatal(err)
	}
	result, err := coordinator.Cancel(t.Context(), 11, "action-cancel-busy", testCreationReceiptTarget)
	if err != nil || !result.Recovering || result.Status != types.PendingActionStatusExecuting ||
		store.op.Status != types.PendingActionStatusExecuting {
		t.Fatalf("busy cancel result=%+v op=%+v err=%v", result, store.op, err)
	}
}

func TestCreationCoordinator_CancelBusyLegacyOperationBindsDurableReceipt(t *testing.T) {
	store := newCreationSagaFakeStore()
	coordinator := NewCreationCoordinator(store, &creationSagaFakeScheduler{}, nil)
	mustProposeCreation(t, coordinator, "action-cancel-busy-legacy")
	store.op.Status = types.PendingActionStatusExecuting
	store.op.Phase = types.TaskCreationPhaseClaimed
	store.op.LeaseOwner = "pre-a6-worker"
	store.op.Fence = 1
	store.op.Attempt = 1
	leaseUntil := time.Now().Add(time.Minute)
	store.op.LeaseUntil = &leaseUntil

	result, err := coordinator.Cancel(
		t.Context(), 11, "action-cancel-busy-legacy", testCreationReceiptTarget,
	)
	if err != nil || !result.Recovering || !result.ReceiptBound ||
		result.Status != types.PendingActionStatusExecuting {
		t.Fatalf("busy cancel result=%+v err=%v", result, err)
	}
	if store.op.ReceiptProvider != testCreationReceiptTarget.Provider ||
		store.op.ReceiptTarget != testCreationReceiptTarget.Target {
		t.Fatalf("busy cancel did not retain receipt target: %+v", store.op)
	}
}

func TestCreationCoordinator_AdoptsPreparationTerminalAfterCancelledResponseLoss(t *testing.T) {
	tests := []struct {
		name        string
		scheduleErr error
		wantStatus  types.PendingActionStatus
		configure   func(*creationSagaFakeStore)
	}{
		{
			name:        "fail",
			scheduleErr: scheduler.ErrTaskScheduleInvalid,
			wantStatus:  types.PendingActionStatusFailed,
			configure: func(s *creationSagaFakeStore) {
				s.failErr = errors.New("commit applied but response lost")
				s.failApplyBeforeError = true
			},
		},
		{
			name:        "block",
			scheduleErr: scheduler.ErrTaskScheduleBlocked,
			wantStatus:  types.PendingActionStatusBlocked,
			configure: func(s *creationSagaFakeStore) {
				s.blockErr = errors.New("commit applied but response lost")
				s.blockApplyBeforeError = true
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newCreationSagaFakeStore()
			coordinator := NewCreationCoordinator(store,
				&creationSagaFakeScheduler{prepareErr: tt.scheduleErr}, nil)
			mustProposeCreation(t, coordinator, "action-terminal-loss-"+tt.name)
			ctx, cancel := context.WithCancel(t.Context())
			store.terminalWriteCancel = cancel
			store.honorLoadContext = true
			tt.configure(store)

			result, err := coordinator.Confirm(ctx, 11, store.op.ID, testCreationReceiptTarget)
			if err != nil || result.Status != tt.wantStatus || store.op.Status != tt.wantStatus {
				t.Fatalf("result=%+v op=%+v err=%v", result, store.op, err)
			}
		})
	}
}

func TestCreationCoordinator_AdoptsCompletedTerminalAfterCancelledResponseLoss(t *testing.T) {
	store := newCreationSagaFakeStore()
	coordinator := NewCreationCoordinator(store, &creationSagaFakeScheduler{}, nil)
	mustProposeCreation(t, coordinator, "action-complete-loss")
	ctx, cancel := context.WithCancel(t.Context())
	store.terminalWriteCancel = cancel
	store.honorLoadContext = true
	store.completeErr = errors.New("commit applied but response lost")
	store.completeApplyBeforeError = true

	result, err := coordinator.Confirm(ctx, 11, store.op.ID, testCreationReceiptTarget)
	if err != nil || result.Status != types.PendingActionStatusExecuted ||
		store.op.Status != types.PendingActionStatusExecuted {
		t.Fatalf("result=%+v op=%+v err=%v", result, store.op, err)
	}
}

func TestCreationCoordinator_RecoveryAdoptsAcquireResponseLossSamePass(t *testing.T) {
	store := newCreationSagaFakeStore()
	coordinator := NewCreationCoordinator(store, &creationSagaFakeScheduler{}, nil)
	mustProposeCreation(t, coordinator, "action-recovery-acquire-loss")
	stale, err := store.AcquireTaskCreationOperation(t.Context(), types.AcquireTaskCreationOperationParams{
		ID: store.op.ID, TenantID: store.op.TenantID, UserID: store.op.UserID,
		LeaseOwner: "dead-worker", LeaseDuration: time.Minute,
		ReceiptProvider: testCreationReceiptTarget.Provider,
		ReceiptTarget:   testCreationReceiptTarget.Target,
	})
	if err != nil {
		t.Fatal(err)
	}
	store.allowTakeover = true
	store.acquireErr = errors.New("takeover committed but response lost")
	store.acquireApplyBeforeError = true

	if err := coordinator.recoverOperation(t.Context(), *stale); err != nil {
		t.Fatalf("same recovery pass should adopt takeover: %v", err)
	}
	if store.op.Status != types.PendingActionStatusExecuted || store.op.Attempt != 2 || store.op.Fence != 2 {
		t.Fatalf("op=%+v; takeover must increment fence/attempt exactly once", store.op)
	}
}

func TestCreationCoordinator_RecoveryContinuesAfterShardListError(t *testing.T) {
	store := &recoveryListingStore{
		creationSagaFakeStore: newCreationSagaFakeStore(),
		tenantIDs:             []int64{7, 8},
		shardErrs:             map[int64]error{7: errors.New("tenant 7 unavailable")},
	}
	coordinator := NewCreationCoordinator(store, &creationSagaFakeScheduler{}, nil)
	err := coordinator.RecoverStaleOnce(t.Context())
	if err == nil || !slices.Equal(store.queried, []int64{7, 8}) {
		t.Fatalf("later shard must still be scanned: queried=%v err=%v", store.queried, err)
	}
}

func TestCreationCoordinator_RecoveryPassIsGloballyBoundedAndRotates(t *testing.T) {
	tenantIDs := make([]int64, creationRecoveryTenantLimit)
	for i := range tenantIDs {
		tenantIDs[i] = int64(i + 1)
	}
	store := &recoveryListingStore{
		creationSagaFakeStore: newCreationSagaFakeStore(),
		tenantIDs:             tenantIDs,
		fill:                  true,
	}
	coordinator := NewCreationCoordinator(store, &creationSagaFakeScheduler{}, nil)
	if err := coordinator.RecoverStaleOnce(t.Context()); err != nil {
		t.Fatal(err)
	}
	requested := 0
	for _, limit := range store.limits {
		requested += limit
	}
	if requested != creationRecoveryPassLimit || len(store.queried) !=
		creationRecoveryPassLimit/creationRecoveryPerTenant {
		t.Fatalf("pass not bounded: tenants=%v limits=%v total=%d",
			store.queried, store.limits, requested)
	}
	firstPassTenants := len(store.queried)
	store.queried = nil
	store.limits = nil
	if err := coordinator.RecoverStaleOnce(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(store.queried) == 0 || store.queried[0] != int64(firstPassTenants+1) {
		t.Fatalf("tenant cursor did not rotate: first pass=%d second=%v",
			firstPassTenants, store.queried)
	}
}

func TestCreationCoordinator_RecoveryKeysetCannotStarveTenantAfterFailedFirstPage(t *testing.T) {
	tenantIDs := make([]int64, creationRecoveryTenantLimit+1)
	shardErrs := make(map[int64]error, creationRecoveryTenantLimit)
	for i := range tenantIDs {
		tenantIDs[i] = int64(i + 1)
		if i < creationRecoveryTenantLimit {
			shardErrs[tenantIDs[i]] = errors.New("tenant shard unavailable")
		}
	}
	store := &recoveryListingStore{
		creationSagaFakeStore: newCreationSagaFakeStore(),
		tenantIDs:             tenantIDs,
		shardErrs:             shardErrs,
	}
	coordinator := NewCreationCoordinator(store, &creationSagaFakeScheduler{}, nil)
	if err := coordinator.RecoverStaleOnce(t.Context()); err == nil {
		t.Fatal("first failed page should report its shard errors")
	}
	if slices.Contains(store.queried, int64(creationRecoveryTenantLimit+1)) {
		t.Fatalf("tenant 101 must not appear in the first bounded page: %v", store.queried)
	}
	store.queried = nil
	store.limits = nil
	if err := coordinator.RecoverStaleOnce(t.Context()); err == nil {
		t.Fatal("wrapped pass still includes failing shards and should report them")
	}
	if len(store.queried) == 0 || store.queried[0] != creationRecoveryTenantLimit+1 {
		t.Fatalf("global keyset cursor starved tenant 101: cursors=%v queried=%v",
			store.cursors, store.queried)
	}
}

func TestCreationTerminalResultRejectsIncompleteTombstones(t *testing.T) {
	now := time.Now()
	valid := &types.TaskCreationOperation{
		ID: "op", TenantID: 7, UserID: 11,
		Status: types.PendingActionStatusCancelled,
		Phase:  types.TaskCreationPhaseCancelled, TombstonedAt: &now,
	}
	if result, done, err := creationTerminalResult(valid); err != nil || !done ||
		result.Status != types.PendingActionStatusCancelled {
		t.Fatalf("valid cancellation rejected: result=%+v done=%v err=%v", result, done, err)
	}
	corruptions := []struct {
		name string
		edit func(*types.TaskCreationOperation)
	}{
		{name: "wrong phase", edit: func(op *types.TaskCreationOperation) { op.Phase = types.TaskCreationPhaseFailed }},
		{name: "missing tombstone", edit: func(op *types.TaskCreationOperation) { op.TombstonedAt = nil }},
		{name: "live lease", edit: func(op *types.TaskCreationOperation) { op.LeaseUntil = &now }},
		{name: "unexpected fence", edit: func(op *types.TaskCreationOperation) { op.Fence = 1 }},
	}
	for _, tt := range corruptions {
		t.Run(tt.name, func(t *testing.T) {
			op := *valid
			tt.edit(&op)
			if _, done, err := creationTerminalResult(&op); !done ||
				!errors.Is(err, ErrCreationCheckpointInvalid) {
				t.Fatalf("corruption accepted: done=%v err=%v op=%+v", done, err, op)
			}
		})
	}
}

func TestCreationCoordinator_LateCheckpointCorruptionCleansOrQuarantines(t *testing.T) {
	t.Run("valid prepared binding is deleted", func(t *testing.T) {
		store := newCreationSagaFakeStore()
		schedules := &creationSagaFakeScheduler{}
		coordinator := NewCreationCoordinator(store, schedules, nil)
		mustProposeCreation(t, coordinator, "action-corrupt-compiled")
		op, err := store.AcquireTaskCreationOperation(t.Context(), types.AcquireTaskCreationOperationParams{
			ID: store.op.ID, TenantID: store.op.TenantID, UserID: store.op.UserID,
			LeaseOwner: "seed", LeaseDuration: time.Minute,
			ReceiptProvider: testCreationReceiptTarget.Provider,
			ReceiptTarget:   testCreationReceiptTarget.Target,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := coordinator.preparer.Prepare(t.Context(), CreationPrepareInput{
			TenantID: op.TenantID, UserID: op.UserID, OperationID: op.ID, Lease: op.Lease(),
		}); err != nil {
			t.Fatal(err)
		}
		store.op.CompiledDigest = strings.Repeat("0", 64)
		store.allowTakeover = true
		if err := coordinator.recoverOperation(t.Context(), store.op); err != nil {
			t.Fatal(err)
		}
		if store.op.Status != types.PendingActionStatusFailed || schedules.deleteCalls != 1 {
			t.Fatalf("status=%q delete=%d", store.op.Status, schedules.deleteCalls)
		}
	})

	t.Run("unprovable prepared binding is quarantined", func(t *testing.T) {
		store := newCreationSagaFakeStore()
		schedules := &creationSagaFakeScheduler{}
		coordinator := NewCreationCoordinator(store, schedules, nil)
		mustProposeCreation(t, coordinator, "action-corrupt-prepared")
		op, err := store.AcquireTaskCreationOperation(t.Context(), types.AcquireTaskCreationOperationParams{
			ID: store.op.ID, TenantID: store.op.TenantID, UserID: store.op.UserID,
			LeaseOwner: "seed", LeaseDuration: time.Minute,
			ReceiptProvider: testCreationReceiptTarget.Provider,
			ReceiptTarget:   testCreationReceiptTarget.Target,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := coordinator.preparer.Prepare(t.Context(), CreationPrepareInput{
			TenantID: op.TenantID, UserID: op.UserID, OperationID: op.ID, Lease: op.Lease(),
		}); err != nil {
			t.Fatal(err)
		}
		store.op.PreparedSchedule = []byte(`{"corrupt":true}`)
		store.allowTakeover = true
		if err := coordinator.recoverOperation(t.Context(), store.op); err != nil {
			t.Fatal(err)
		}
		if store.op.Status != types.PendingActionStatusBlocked || schedules.deleteCalls != 0 ||
			!containsString(store.events, "block_side_effect") {
			t.Fatalf("status=%q store_events=%v delete=%d",
				store.op.Status, store.events, schedules.deleteCalls)
		}
	})
}

func TestCreationCoordinator_ScopeRevokedBeforeEnsureConvergesThroughExactDelete(t *testing.T) {
	store := newCreationSagaFakeStore()
	schedules := &creationSagaFakeScheduler{}
	coordinator := NewCreationCoordinator(store, schedules, nil)
	mustProposeCreation(t, coordinator, "action-revoked")
	store.tenant.Status = types.TenantStatusSuspended

	// Recovery acquires by the operation's durable scope even though normal
	// intake would no longer resolve this tenant as active.
	store.op.Status = types.PendingActionStatusExecuting
	store.op.Phase = types.TaskCreationPhaseClaimed
	store.allowTakeover = true
	err := coordinator.recoverOperation(t.Context(), store.op)
	if err != nil {
		t.Fatalf("recoverOperation: %v", err)
	}
	if store.op.Status != types.PendingActionStatusFailed || schedules.deleteCalls != 1 ||
		containsString(schedules.events, "ensure") || containsString(schedules.events, "activate") {
		t.Fatalf("status=%q scheduler_events=%v delete_calls=%d",
			store.op.Status, schedules.events, schedules.deleteCalls)
	}
}

func mustProposeCreation(t *testing.T, coordinator *CreationCoordinator, id string) {
	t.Helper()
	if _, err := coordinator.Propose(t.Context(), CreationProposalInput{
		ActionID: id, UserID: 11, RawArgs: mustCreateArgs(t, "每天寻找全球 AI 热点", "每天 AI"),
		ExpiresAt: time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatalf("Propose: %v", err)
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
