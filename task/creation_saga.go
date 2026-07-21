package task

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/scheduler"
	"github.com/YouToco/vane/types"
	"github.com/YouToco/vane/workflow"
)

const (
	creationLeaseDuration      = time.Minute
	creationAttemptTimeout     = 25 * time.Second
	creationExternalRPCTimeout = 8 * time.Second
	creationConvergenceTimeout = 5 * time.Second
	// Compensation may outlive a cancelled card request so an already-issued
	// Temporal mutation can converge. Keep the whole detached cleanup bounded;
	// Manager shutdown reserves enough time for this budget before closing DB.
	creationCompensationTimeout  = 15 * time.Second
	creationRecoveryInterval     = 15 * time.Second
	creationRecoveryTenantLimit  = 100
	creationRecoveryPerTenant    = 4
	creationRecoveryPassLimit    = 64
	creationRecoveryPassTimeout  = 90 * time.Second
	creationRecoveryConcurrency  = 4
	creationEnsureReceiptVersion = "vane.task-creation-ensure/v1"
	creationResultVersion        = "vane.task-creation-result/v1"
	maxCreationSummaryBytes      = 4 << 10
)

// ErrCreationOperationNotFound is deliberately narrower than types.ErrNotFound.
// Agent may fall back to the historical v0 confirmation path only when this
// sentinel proves that the action ID is not a v1 creation operation.
var ErrCreationOperationNotFound = errors.New("task: creation operation not found")

// CreationProposalInput is the trusted Agent-to-control-plane handoff. RawArgs
// still come from the model, so the coordinator validates and canonicalizes
// them before writing any durable confirmation action.
type CreationProposalInput struct {
	ActionID  string
	UserID    int64
	SessionID *int64
	RawArgs   json.RawMessage
	ExpiresAt time.Time
}

type CreationProposal struct {
	ID      string
	Summary string
}

// CreationReceiptTarget is the immutable provider resource on which A6 will
// converge the terminal user receipt. Confirm/Cancel bind it in the same
// transaction that accepts the click; a different target on replay conflicts.
type CreationReceiptTarget struct {
	Provider string
	Target   string
}

// CreationResult is safe to render at the callback boundary. Recovering means
// the operation owns durable checkpoints but did not finish inside this HTTP
// callback; the background runner will continue it without a second task.
type CreationResult struct {
	OperationID string
	TaskID      string
	Message     string
	Status      types.PendingActionStatus
	Recovering  bool
	Replayed    bool
	// ReceiptBound is true only after the callback's provider resource was
	// durably accepted with this operation. The Feishu handler may replace the
	// buttons with a processing card only in this state; legacy A5 tombstones
	// migrated without a target keep their historical synchronous response.
	ReceiptBound bool
	SessionID    *int64
	Summary      string
	Arguments    json.RawMessage
}

// CreationSagaStore is the complete A5 persistence boundary. Keeping it narrow
// makes crash tests inject a failure after every database checkpoint without
// substituting a different saga implementation.
type CreationSagaStore interface {
	CreationPrepareStore

	ListMembershipsByUser(ctx context.Context, userID int64) ([]types.Membership, error)
	GetTenant(ctx context.Context, id int64) (*types.Tenant, error)
	LoadTaskCreationOperationByUser(ctx context.Context, id string, userID int64) (*types.TaskCreationOperation, error)
	CreateTaskCreationOperation(ctx context.Context, p types.CreateTaskCreationOperationParams) (*types.TaskCreationOperation, error)
	CancelTaskCreationOperation(ctx context.Context, p types.CancelTaskCreationOperationParams) (*types.TaskCreationOperation, error)
	AcquireTaskCreationOperation(ctx context.Context, p types.AcquireTaskCreationOperationParams) (*types.TaskCreationOperation, error)
	CheckpointTaskCreationEnsureReceipt(ctx context.Context, lease types.TaskCreationLease, receipt []byte, taskID string) error
	CommitPausedCompiledTaskDefinitionForCreation(ctx context.Context, p types.CommitPausedCompiledTaskDefinitionForCreationParams) error
	BeginTaskCreationActivation(ctx context.Context, lease types.TaskCreationLease, taskID string) (bool, error)
	CommitTaskCreationActivation(ctx context.Context, lease types.TaskCreationLease, taskID string) error
	BeginTaskCreationCleanup(ctx context.Context, lease types.TaskCreationLease, taskID, errorCode, errorMessage string) (bool, error)
	FinishTaskCreationCleanup(ctx context.Context, lease types.TaskCreationLease, taskID string, terminalStatus types.PendingActionStatus) error
	BlockTaskCreationOperationAfterSideEffect(ctx context.Context, lease types.TaskCreationLease, taskID, errorCode, errorMessage string) error
	CompleteTaskCreationOperation(ctx context.Context, lease types.TaskCreationLease, taskID string, result json.RawMessage) error
	ListStaleTaskCreationTenantIDs(ctx context.Context, before time.Time, afterTenantID int64, limit int) ([]int64, error)
	ListStaleTaskCreationOperations(ctx context.Context, tenantID int64, before time.Time, limit int) ([]types.TaskCreationOperation, error)
}

// CreationTaskScheduler is implemented by *scheduler.Scheduler. Workflow code
// never sees this interface: every Temporal call remains in the control plane.
type CreationTaskScheduler interface {
	PrepareTaskSchedule(ctx context.Context, req scheduler.TaskScheduleRequest) (scheduler.PreparedTaskSchedule, error)
	EnsurePausedTask(ctx context.Context, prepared scheduler.PreparedTaskSchedule) (scheduler.EnsurePausedTaskResult, error)
	DescribeTask(ctx context.Context, prepared scheduler.PreparedTaskSchedule) (scheduler.TaskScheduleSnapshot, error)
	ActivateTask(ctx context.Context, prepared scheduler.PreparedTaskSchedule, ensured scheduler.TaskScheduleSnapshot) (scheduler.TaskScheduleSnapshot, error)
	DeleteTask(ctx context.Context, prepared scheduler.PreparedTaskSchedule) error
}

type creationSchedulePreparer struct {
	scheduler CreationTaskScheduler
}

func (p creationSchedulePreparer) DeriveID(tenantID, userID int64, operationID string) (string, error) {
	return scheduler.TaskIDForOperation(tenantID, userID, operationID)
}

func (p creationSchedulePreparer) Prepare(
	ctx context.Context,
	req scheduler.TaskScheduleRequest,
) (scheduler.PreparedTaskSchedule, error) {
	return p.scheduler.PrepareTaskSchedule(ctx, req)
}

// CreationCoordinator is the only production owner of v1 create_schedule.
// Agent receives Propose/Confirm only; lease, fence and lifecycle methods are
// intentionally not exposed through its dependency graph.
type CreationCoordinator struct {
	store     CreationSagaStore
	scheduler CreationTaskScheduler
	preparer  *CreationPreparer
	logger    *slog.Logger

	// Recovery passes are serialized so two callers cannot race the same stale
	// scan. The cursor rotates tenant order between bounded passes, preventing a
	// noisy oldest shard from starving later tenants forever.
	recoveryMu     sync.Mutex
	recoveryCursor int64
}

func NewCreationCoordinator(
	store CreationSagaStore,
	schedules CreationTaskScheduler,
	logger *slog.Logger,
) *CreationCoordinator {
	if logger == nil {
		logger = slog.Default()
	}
	return &CreationCoordinator{
		store:     store,
		scheduler: schedules,
		preparer:  NewCreationPreparer(store, creationSchedulePreparer{scheduler: schedules}),
		logger:    logger,
	}
}

// Propose validates the complete approved definition before it creates a
// confirmation action. There is no LLM, network discovery, or Temporal call on
// this path.
func (c *CreationCoordinator) Propose(
	ctx context.Context,
	in CreationProposalInput,
) (CreationProposal, error) {
	if err := ctx.Err(); err != nil {
		return CreationProposal{}, err
	}
	if c == nil || c.store == nil || c.scheduler == nil || c.preparer == nil {
		return CreationProposal{}, errors.New("task: creation coordinator dependencies are incomplete")
	}
	if strings.TrimSpace(in.ActionID) == "" || in.ActionID != strings.TrimSpace(in.ActionID) ||
		in.UserID <= 0 || in.ExpiresAt.IsZero() {
		return CreationProposal{}, creationValidation("任务确认请求不完整", nil)
	}
	command, _, err := normalizeCreateScheduleCommand(in.RawArgs)
	if err != nil {
		return CreationProposal{}, creationValidation("任务方案未通过校验："+err.Error(), err)
	}
	canonicalArgs, err := canonicalCreationProposalArgs(command)
	if err != nil {
		return CreationProposal{}, creationValidation("任务方案无法规范化", err)
	}
	summary, err := summarizeCreationProposal(command)
	if err != nil {
		return CreationProposal{}, creationValidation("任务方案无法生成确认摘要", err)
	}
	tenantID, err := c.resolveActiveTenant(ctx, in.UserID)
	if err != nil {
		return CreationProposal{}, err
	}
	params := types.CreateTaskCreationOperationParams{
		ID: in.ActionID, TenantID: tenantID, UserID: in.UserID,
		SessionID: in.SessionID, Args: canonicalArgs, Summary: summary,
		ExpiresAt: in.ExpiresAt.UTC().Truncate(time.Microsecond),
	}
	op, createErr := c.store.CreateTaskCreationOperation(ctx, params)
	if createErr != nil {
		if deterministicCreationProposalFailure(createErr) {
			return CreationProposal{}, createErr
		}
		// A database commit response can be lost. Exact-ID readback adopts only
		// the same immutable proposal; a different row remains a conflict.
		readCtx, cancel := context.WithTimeout(
			context.WithoutCancel(ctx), creationConvergenceTimeout,
		)
		op, err = c.store.LoadTaskCreationOperation(readCtx, in.ActionID, tenantID, in.UserID)
		cancel()
		if err != nil || !proposalOperationMatches(op, params) {
			return CreationProposal{}, createErr
		}
	}
	if !proposalOperationMatches(op, params) {
		return CreationProposal{}, types.NewAppError(
			types.CodeConflict, "任务确认动作与已保存内容冲突", types.ErrConflict,
		)
	}
	return CreationProposal{ID: op.ID, Summary: op.Summary}, nil
}

func deterministicCreationProposalFailure(err error) bool {
	return errors.Is(err, types.ErrValidation) ||
		errors.Is(err, types.ErrConflict) ||
		errors.Is(err, types.ErrNotFound) ||
		errors.Is(err, types.ErrTaskCreationTerminal)
}

// Confirm resumes the same fenced operation on every click. Busy is a handled
// v1 result, never permission to fall through to legacy v0 execution.
func (c *CreationCoordinator) Confirm(
	ctx context.Context,
	userID int64,
	actionID string,
	receiptTarget CreationReceiptTarget,
) (CreationResult, error) {
	if err := ctx.Err(); err != nil {
		return CreationResult{}, err
	}
	if c == nil || c.store == nil || c.scheduler == nil || c.preparer == nil {
		return CreationResult{}, errors.New("task: creation coordinator dependencies are incomplete")
	}
	op, err := c.store.LoadTaskCreationOperationByUser(ctx, actionID, userID)
	if err != nil {
		if errors.Is(err, types.ErrNotFound) {
			return CreationResult{}, fmt.Errorf("%w: %s", ErrCreationOperationNotFound, actionID)
		}
		return CreationResult{}, err
	}
	if err := validateCreationReceiptTarget(op, receiptTarget); err != nil {
		return CreationResult{}, err
	}
	tenantID := op.TenantID
	if result, done, err := creationTerminalResult(op); done || err != nil {
		result.Replayed = done && err == nil
		return result, err
	}
	owner := "confirm-" + uuid.NewString()
	approved := op
	op, err = c.acquireCreationOperation(ctx, types.AcquireTaskCreationOperationParams{
		ID: actionID, TenantID: tenantID, UserID: userID,
		LeaseOwner: owner, LeaseDuration: creationLeaseDuration,
		ReceiptProvider: receiptTarget.Provider, ReceiptTarget: receiptTarget.Target,
	})
	if err != nil {
		switch {
		case errors.Is(err, types.ErrTaskCreationBusy):
			// Store may have atomically attached the first A6 provider target to
			// a pre-A6 in-flight operation before reporting its still-live owner.
			// Prefer that returned row so the callback knows the durable receipt
			// boundary is now armed; ordinary busy paths may return nil.
			auditOp := op
			if auditOp == nil {
				auditOp = approved
			}
			return attachCreationAudit(
				recoveringCreationResult(actionID, opTaskID(auditOp)), auditOp,
			), nil
		case errors.Is(err, types.ErrTaskCreationTerminal):
			loaded, loadErr := c.loadCreationOperationConvergent(
				ctx, actionID, tenantID, userID,
			)
			if loadErr != nil {
				return CreationResult{}, errors.Join(err, loadErr)
			}
			if result, done, resultErr := creationTerminalResult(loaded); done || resultErr != nil {
				result.Replayed = done && resultErr == nil
				return result, resultErr
			}
			return CreationResult{
				OperationID: actionID, Status: loaded.Status,
				Message: "这张任务确认已过期，请重新描述需求。",
			}, nil
		default:
			return CreationResult{}, err
		}
	}
	attemptCtx, cancel := context.WithTimeout(ctx, creationAttemptTimeout)
	defer cancel()
	result, runErr := c.runAcquired(attemptCtx, op)
	if runErr == nil {
		return attachCreationAudit(result, op), nil
	}
	// Once acquired, every mutation is checkpointed and the stale runner can
	// finish it. The callback reports recovery instead of inviting a duplicate.
	c.logger.WarnContext(ctx, "task creation will continue in recovery",
		"operation_id", actionID, "tenant_id", tenantID, "user_id", userID,
		"phase", op.Phase, "err", runErr)
	return attachCreationAudit(recoveringCreationResult(actionID, opTaskID(op)), op), nil
}

// Cancel linearizes a v1 cancellation with Acquire on the same durable row.
// Only an exact proof that the ID is not v1 permits Agent to fall back to the
// legacy pending-action path.
func (c *CreationCoordinator) Cancel(
	ctx context.Context,
	userID int64,
	actionID string,
	receiptTarget CreationReceiptTarget,
) (CreationResult, error) {
	if err := ctx.Err(); err != nil {
		return CreationResult{}, err
	}
	if c == nil || c.store == nil {
		return CreationResult{}, errors.New("task: creation coordinator dependencies are incomplete")
	}
	op, err := c.store.LoadTaskCreationOperationByUser(ctx, actionID, userID)
	if err != nil {
		if errors.Is(err, types.ErrNotFound) {
			return CreationResult{}, fmt.Errorf("%w: %s", ErrCreationOperationNotFound, actionID)
		}
		return CreationResult{}, err
	}
	if err := validateCreationReceiptTarget(op, receiptTarget); err != nil {
		return CreationResult{}, err
	}
	if result, done, terminalErr := creationTerminalResult(op); done || terminalErr != nil {
		result.Replayed = done && terminalErr == nil
		return result, terminalErr
	}

	cancelled, cancelErr := c.store.CancelTaskCreationOperation(ctx,
		types.CancelTaskCreationOperationParams{
			ID: actionID, TenantID: op.TenantID, UserID: userID,
			ReceiptProvider: receiptTarget.Provider, ReceiptTarget: receiptTarget.Target,
		})
	if cancelErr == nil {
		result, done, resultErr := creationTerminalResult(cancelled)
		if !done && resultErr == nil {
			return CreationResult{}, errors.New("task: cancellation did not produce a terminal operation")
		}
		return result, resultErr
	}
	if errors.Is(cancelErr, types.ErrTaskCreationBusy) {
		auditOp := cancelled
		if auditOp == nil {
			auditOp = op
		}
		result := attachCreationAudit(
			recoveringCreationResult(actionID, opTaskID(auditOp)), auditOp,
		)
		result.Message = "任务已经开始创建，无法再取消；系统会自动完成或安全回滚。"
		return result, nil
	}
	if errors.Is(cancelErr, types.ErrTaskCreationTerminal) {
		loaded, loadErr := c.loadCreationOperationConvergent(
			ctx, actionID, op.TenantID, userID,
		)
		if loadErr != nil {
			return CreationResult{}, errors.Join(cancelErr, loadErr)
		}
		result, done, resultErr := creationTerminalResult(loaded)
		if done || resultErr != nil {
			result.Replayed = done && resultErr == nil
			return result, resultErr
		}
		return CreationResult{}, cancelErr
	}
	if errors.Is(cancelErr, types.ErrValidation) ||
		errors.Is(cancelErr, types.ErrConflict) ||
		errors.Is(cancelErr, types.ErrNotFound) {
		return CreationResult{}, cancelErr
	}

	// The cancellation commit may have succeeded while its response was lost.
	// Exact readback adopts only the cancelled tombstone for this v1 operation.
	loaded, loadErr := c.loadCreationOperationConvergent(
		ctx, actionID, op.TenantID, userID,
	)
	if loadErr == nil && loaded.Status == types.PendingActionStatusCancelled {
		result, done, resultErr := creationTerminalResult(loaded)
		if done || resultErr != nil {
			return result, resultErr
		}
	}
	return CreationResult{}, errors.Join(cancelErr, loadErr)
}

func (c *CreationCoordinator) acquireCreationOperation(
	ctx context.Context,
	params types.AcquireTaskCreationOperationParams,
) (*types.TaskCreationOperation, error) {
	op, err := c.store.AcquireTaskCreationOperation(ctx, params)
	if err == nil || deterministicCreationAcquireFailure(err) {
		return op, err
	}
	// Acquire may have committed while its response was lost. Replay with the
	// exact same owner under a detached bounded context; Store returns the same
	// fence for that owner instead of incrementing a second attempt.
	retryCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx), creationConvergenceTimeout,
	)
	defer cancel()
	replayed, replayErr := c.store.AcquireTaskCreationOperation(retryCtx, params)
	if replayErr == nil {
		return replayed, nil
	}
	loaded, loadErr := c.store.LoadTaskCreationOperation(
		retryCtx, params.ID, params.TenantID, params.UserID,
	)
	if loadErr == nil && loaded.Status == types.PendingActionStatusExecuting &&
		loaded.LeaseOwner == params.LeaseOwner && loaded.Fence > 0 {
		return loaded, nil
	}
	return nil, errors.Join(err, replayErr, loadErr)
}

func deterministicCreationAcquireFailure(err error) bool {
	return errors.Is(err, types.ErrTaskCreationBusy) ||
		errors.Is(err, types.ErrTaskCreationTerminal) ||
		errors.Is(err, types.ErrNotFound) ||
		errors.Is(err, types.ErrValidation) ||
		errors.Is(err, types.ErrConflict)
}

func (c *CreationCoordinator) runAcquired(
	ctx context.Context,
	op *types.TaskCreationOperation,
) (CreationResult, error) {
	if op == nil {
		return CreationResult{}, errors.New("task: acquired creation operation is nil")
	}
	lease := op.Lease()
	preparedResult, err := c.preparer.Prepare(ctx, CreationPrepareInput{
		TenantID: op.TenantID, UserID: op.UserID, OperationID: op.ID, Lease: lease,
	})
	if err != nil {
		return c.handlePrepareFailure(ctx, op, err)
	}
	preparedBytes, err := json.Marshal(preparedResult.Schedule)
	if err != nil {
		return CreationResult{}, fmt.Errorf("marshal prepared task schedule: %w", err)
	}

	current, err := c.store.LoadTaskCreationOperation(ctx, op.ID, op.TenantID, op.UserID)
	if err != nil {
		return CreationResult{}, err
	}
	if current.Lease() != lease {
		return CreationResult{}, types.ErrTaskCreationLeaseLost
	}
	if current.Phase == types.TaskCreationPhaseCleanupPending {
		return c.resumeCleanup(ctx, current, preparedResult.Schedule)
	}
	scopeActive, err := c.creationScopeActive(ctx, current.TenantID, current.UserID)
	if err != nil {
		return CreationResult{}, fmt.Errorf("revalidate task creation scope: %w", err)
	}
	if !scopeActive {
		// Preparation has already durably frozen the exact deterministic task
		// identity, but no new remote call is made after scope revocation. An
		// exact Delete is still required: an earlier Ensure may have succeeded
		// while its response/checkpoint was lost. Delete/NotFound convergence
		// makes this safe even when no remote schedule ever existed.
		return c.cleanupKnownFailure(
			ctx, current, preparedResult.Schedule, "creation_scope_inactive",
			"创建期间工作空间或成员权限失效，本次创建已安全撤销",
			errors.New("task creation scope is no longer active"),
		)
	}

	receipt, receiptBytes, err := c.ensureReceipt(ctx, current, preparedResult.Schedule)
	if err != nil {
		return c.handleEnsureFailure(ctx, current, preparedResult.Schedule, err)
	}
	if err := c.store.CommitPausedCompiledTaskDefinitionForCreation(ctx,
		types.CommitPausedCompiledTaskDefinitionForCreationParams{
			Lease: lease, Definition: preparedResult.Definition,
			CompiledDigest:   preparedResult.DefinitionDigest,
			PreparedSchedule: preparedBytes, EnsureReceipt: receiptBytes,
		}); err != nil {
		return c.handlePostEnsureFailure(
			ctx, current, preparedResult.Schedule, err, "definition_commit_failed",
		)
	}

	started, err := c.store.BeginTaskCreationActivation(
		ctx, lease, preparedResult.Schedule.TaskID,
	)
	if err != nil {
		return c.handlePostEnsureFailure(
			ctx, current, preparedResult.Schedule, err, "activation_checkpoint_failed",
		)
	}
	if err := c.activateTask(ctx, preparedResult.Schedule, receipt.Snapshot, started); err != nil {
		return c.handleActivationFailure(ctx, current, preparedResult.Schedule, err)
	}
	if err := c.store.CommitTaskCreationActivation(
		ctx, lease, preparedResult.Schedule.TaskID,
	); err != nil {
		return c.handlePostEnsureFailure(
			ctx, current, preparedResult.Schedule, err, "activation_commit_failed",
		)
	}

	resultBytes, err := marshalCreationSuccess(preparedResult.Schedule.TaskID)
	if err != nil {
		return CreationResult{}, err
	}
	if err := c.store.CompleteTaskCreationOperation(
		ctx, lease, preparedResult.Schedule.TaskID, resultBytes,
	); err != nil {
		if adopted, done, adoptErr := c.adoptTerminalAfterWriteError(ctx, op, err); done {
			return adopted, adoptErr
		}
		if deterministicCreationSideEffectFailure(err) {
			// The schedule and its DB mirror are already active. Reusing the
			// pre-activation cleanup protocol would fail its paused-generation
			// proof (or worse, delete a task that may already be running). Quarantine
			// the exact active aggregate instead; matureSchedulePredicate and the
			// workflow authorization gate stop it from spending or becoming visible.
			return c.blockUnsafeSideEffect(
				ctx, current, preparedResult.Schedule.TaskID,
				"completion_finalization_invalid",
				"任务已激活但最终记录异常，系统已停止运行并保留现场", err,
			)
		}
		return CreationResult{}, fmt.Errorf("complete task creation operation: %w", err)
	}
	return CreationResult{
		OperationID: op.ID, TaskID: preparedResult.Schedule.TaskID,
		Message: "任务已创建并开始监控。", Status: types.PendingActionStatusExecuted,
	}, nil
}

type taskCreationEnsureReceipt struct {
	Version  string                         `json:"version"`
	Snapshot scheduler.TaskScheduleSnapshot `json:"snapshot"`
}

func (c *CreationCoordinator) ensureReceipt(
	ctx context.Context,
	op *types.TaskCreationOperation,
	prepared scheduler.PreparedTaskSchedule,
) (taskCreationEnsureReceipt, []byte, error) {
	if len(op.EnsureReceipt) != 0 {
		receipt, err := decodeCreationEnsureReceipt(op.EnsureReceipt, prepared)
		return receipt, bytes.Clone(op.EnsureReceipt), err
	}
	if creationPhaseAtLeast(op.Phase, types.TaskCreationPhaseScheduleEnsured) {
		return taskCreationEnsureReceipt{}, nil,
			fmt.Errorf("%w: ensured phase has no receipt", ErrCreationCheckpointInvalid)
	}
	rpcCtx, cancel := context.WithTimeout(ctx, creationExternalRPCTimeout)
	ensured, err := c.scheduler.EnsurePausedTask(rpcCtx, prepared)
	cancel()
	if err != nil {
		return taskCreationEnsureReceipt{}, nil, fmt.Errorf("ensure paused task: %w", err)
	}
	receipt := taskCreationEnsureReceipt{
		Version: creationEnsureReceiptVersion, Snapshot: ensured.Snapshot,
	}
	receiptBytes, err := json.Marshal(receipt)
	if err != nil {
		return taskCreationEnsureReceipt{}, nil, fmt.Errorf("marshal ensure receipt: %w", err)
	}
	if _, err := decodeCreationEnsureReceipt(receiptBytes, prepared); err != nil {
		return taskCreationEnsureReceipt{}, nil, err
	}
	if err := c.store.CheckpointTaskCreationEnsureReceipt(
		ctx, op.Lease(), receiptBytes, prepared.TaskID,
	); err != nil {
		return taskCreationEnsureReceipt{}, nil,
			fmt.Errorf("checkpoint task creation ensure receipt: %w", err)
	}
	return receipt, receiptBytes, nil
}

func decodeCreationEnsureReceipt(
	raw []byte,
	prepared scheduler.PreparedTaskSchedule,
) (taskCreationEnsureReceipt, error) {
	var receipt *taskCreationEnsureReceipt
	if err := decodeStrictJSON(raw, &receipt); err != nil || receipt == nil {
		return taskCreationEnsureReceipt{}, fmt.Errorf("%w: ensure receipt is invalid", ErrCreationCheckpointInvalid)
	}
	canonical, err := json.Marshal(receipt)
	if err != nil || !bytes.Equal(canonical, raw) {
		return taskCreationEnsureReceipt{}, fmt.Errorf("%w: ensure receipt is not canonical", ErrCreationCheckpointInvalid)
	}
	snapshot := receipt.Snapshot
	if receipt.Version != creationEnsureReceiptVersion || snapshot.TaskID != prepared.TaskID ||
		snapshot.RequestDigest != prepared.RequestDigest ||
		snapshot.PreparedDigest != prepared.PreparedDigest || snapshot.Revision == "" ||
		snapshot.State != scheduler.TaskSchedulePausedVirginExact || snapshot.NumActions != 0 {
		return taskCreationEnsureReceipt{}, fmt.Errorf("%w: ensure receipt binding differs", ErrCreationCheckpointInvalid)
	}
	return *receipt, nil
}

func (c *CreationCoordinator) activateTask(
	ctx context.Context,
	prepared scheduler.PreparedTaskSchedule,
	receipt scheduler.TaskScheduleSnapshot,
	started bool,
) error {
	if !started {
		rpcCtx, cancel := context.WithTimeout(ctx, creationExternalRPCTimeout)
		observed, err := c.scheduler.DescribeTask(rpcCtx, prepared)
		cancel()
		if err != nil {
			return fmt.Errorf("describe task before activation recovery: %w", err)
		}
		switch observed.State {
		case scheduler.TaskScheduleActiveVirginExact,
			scheduler.TaskScheduleActiveUsedExact:
			return nil
		case scheduler.TaskSchedulePausedProvisioningExact:
			// The prior authorized call did not activate, so replay the same
			// revision-bound transition below.
		default:
			return fmt.Errorf("%w: activation recovery observed %s",
				scheduler.ErrTaskScheduleUnsafeState, observed.State)
		}
	}
	rpcCtx, cancel := context.WithTimeout(ctx, creationExternalRPCTimeout)
	_, err := c.scheduler.ActivateTask(rpcCtx, prepared, receipt)
	cancel()
	if err != nil {
		return fmt.Errorf("activate task: %w", err)
	}
	return nil
}

func (c *CreationCoordinator) handlePrepareFailure(
	ctx context.Context,
	op *types.TaskCreationOperation,
	cause error,
) (CreationResult, error) {
	loaded, loadErr := c.loadCreationOperationConvergent(
		ctx, op.ID, op.TenantID, op.UserID,
	)
	if loadErr != nil {
		return CreationResult{}, errors.Join(cause,
			fmt.Errorf("reload failed task creation preparation: %w", loadErr))
	}
	if result, done, resultErr := creationTerminalResult(loaded); done || resultErr != nil {
		return result, resultErr
	}
	if !errors.Is(cause, ErrCreationCheckpointInvalid) ||
		!creationExternalEffectPossible(loaded) {
		return CreationResult{}, cause
	}
	prepared, decodeErr := decodePreparedScheduleForCleanup(loaded)
	if decodeErr == nil {
		return c.cleanupKnownFailure(
			ctx, loaded, prepared, "checkpoint_invalid_after_prepare",
			"任务创建检查点损坏，本次创建已撤销并保留审计记录", cause,
		)
	}
	// The exact remote binding can no longer be proven, so deletion would risk
	// touching another schedule. Quarantine retains every byte and a capacity
	// reservation for manual reconciliation instead of retrying forever.
	return c.blockUnsafeSideEffect(
		ctx, loaded, strings.TrimSpace(loaded.TaskID),
		"unknown_schedule_after_checkpoint_corruption",
		"任务创建检查点损坏且无法安全识别远端调度，已隔离并保留现场",
		errors.Join(cause, decodeErr),
	)
}

func (c *CreationCoordinator) handleEnsureFailure(
	ctx context.Context,
	op *types.TaskCreationOperation,
	prepared scheduler.PreparedTaskSchedule,
	cause error,
) (CreationResult, error) {
	if deterministicCreationSideEffectFailure(cause) {
		return c.cleanupKnownFailure(
			ctx, op, prepared, "schedule_ensure_failed",
			"任务调度未能安全建立，本次创建已撤销", cause,
		)
	}
	return CreationResult{}, cause
}

func deterministicCreationSideEffectFailure(err error) bool {
	return errors.Is(err, ErrCreationCheckpointInvalid) ||
		errors.Is(err, types.ErrConflict) ||
		errors.Is(err, types.ErrValidation) ||
		errors.Is(err, types.ErrTaskCreationLimit) ||
		errors.Is(err, scheduler.ErrTaskScheduleInvalid) ||
		errors.Is(err, scheduler.ErrTaskScheduleNotFound) ||
		errors.Is(err, scheduler.ErrTaskScheduleConflict) ||
		errors.Is(err, scheduler.ErrTaskScheduleBlocked) ||
		errors.Is(err, scheduler.ErrTaskScheduleUnsafeState)
}

func creationExternalEffectPossible(op *types.TaskCreationOperation) bool {
	return op != nil && (creationPhaseAtLeast(op.Phase, types.TaskCreationPhaseSchedulePrepared) ||
		len(op.PreparedSchedule) != 0 || len(op.EnsureReceipt) != 0 || op.TaskID != "")
}

func decodePreparedScheduleForCleanup(
	op *types.TaskCreationOperation,
) (scheduler.PreparedTaskSchedule, error) {
	if op == nil || len(op.PreparedSchedule) == 0 {
		return scheduler.PreparedTaskSchedule{}, errors.New("prepared schedule checkpoint is missing")
	}
	var prepared *scheduler.PreparedTaskSchedule
	if err := decodeStrictJSON(op.PreparedSchedule, &prepared); err != nil || prepared == nil {
		return scheduler.PreparedTaskSchedule{}, errors.New("prepared schedule checkpoint is invalid")
	}
	canonical, err := json.Marshal(prepared)
	if err != nil || !bytes.Equal(canonical, op.PreparedSchedule) {
		return scheduler.PreparedTaskSchedule{}, errors.New("prepared schedule checkpoint is not canonical")
	}
	if prepared.TaskID == "" || prepared.TenantID != op.TenantID ||
		prepared.UserID != op.UserID || prepared.OperationID != op.ID ||
		prepared.Action.Params.UserID != op.UserID ||
		prepared.Action.Params.RunKind != workflow.PushRunKindScheduled ||
		prepared.Action.Params.ScheduleID != prepared.TaskID {
		return scheduler.PreparedTaskSchedule{}, errors.New("prepared schedule scope binding differs")
	}
	return *prepared, nil
}

func (c *CreationCoordinator) handleActivationFailure(
	ctx context.Context,
	op *types.TaskCreationOperation,
	prepared scheduler.PreparedTaskSchedule,
	cause error,
) (CreationResult, error) {
	switch {
	case deterministicCreationSideEffectFailure(cause):
		return c.cleanupKnownFailure(
			ctx, op, prepared, "activation_failed",
			"任务调度未能安全激活，本次创建已撤销", cause,
		)
	default:
		return CreationResult{}, cause
	}
}

func (c *CreationCoordinator) handlePostEnsureFailure(
	ctx context.Context,
	op *types.TaskCreationOperation,
	prepared scheduler.PreparedTaskSchedule,
	cause error,
	code string,
) (CreationResult, error) {
	if errors.Is(cause, types.ErrTaskCreationLimit) {
		return c.cleanupKnownFailure(
			ctx, op, prepared, "task_limit_reached",
			"任务数量已达上限，本次创建已安全撤销", cause,
		)
	}
	if deterministicCreationSideEffectFailure(cause) {
		return c.cleanupKnownFailure(
			ctx, op, prepared, code,
			"任务创建状态无法安全提交，本次创建已撤销", cause,
		)
	}
	return CreationResult{}, cause
}

func (c *CreationCoordinator) cleanupKnownFailure(
	ctx context.Context,
	op *types.TaskCreationOperation,
	prepared scheduler.PreparedTaskSchedule,
	code string,
	message string,
	cause error,
) (CreationResult, error) {
	_, err := c.store.BeginTaskCreationCleanup(
		ctx, op.Lease(), prepared.TaskID, code, message,
	)
	if err != nil {
		loaded, loadErr := c.loadCreationOperationConvergent(
			ctx, op.ID, op.TenantID, op.UserID,
		)
		if loadErr == nil {
			if result, done, terminalErr := creationTerminalResult(loaded); done || terminalErr != nil {
				return result, terminalErr
			}
			if loaded.Lease() == op.Lease() &&
				loaded.Phase == types.TaskCreationPhaseCleanupPending &&
				loaded.TaskID == prepared.TaskID && loaded.ErrorCode == code &&
				loaded.ErrorMessage == message {
				return c.finishCleanup(ctx, loaded, prepared, cause)
			}
		}
		if deterministicCreationSideEffectFailure(err) && loadErr == nil {
			quarantineCtx, cancel := context.WithTimeout(
				context.WithoutCancel(ctx), creationConvergenceTimeout,
			)
			defer cancel()
			return c.blockUnsafeSideEffect(
				quarantineCtx, loaded, prepared.TaskID, "cleanup_checkpoint_invalid",
				"任务清理边界无法安全确认，系统已隔离并保留现场",
				errors.Join(cause, err),
			)
		}
		return CreationResult{}, errors.Join(cause, fmt.Errorf("begin task creation cleanup: %w", err))
	}
	checkpointed := *op
	checkpointed.ErrorCode = code
	checkpointed.ErrorMessage = message
	checkpointed.Phase = types.TaskCreationPhaseCleanupPending
	return c.finishCleanup(ctx, &checkpointed, prepared, cause)
}

func (c *CreationCoordinator) resumeCleanup(
	ctx context.Context,
	op *types.TaskCreationOperation,
	prepared scheduler.PreparedTaskSchedule,
) (CreationResult, error) {
	if op.ErrorCode == "" || op.ErrorMessage == "" {
		return c.blockUnsafeSideEffect(
			ctx, op, prepared.TaskID, "unsafe_cleanup_checkpoint",
			"任务清理检查点不完整，已停止自动处理并保留审计证据",
			ErrCreationCheckpointInvalid,
		)
	}
	return c.finishCleanup(ctx, op, prepared, errors.New(op.ErrorMessage))
}

func (c *CreationCoordinator) finishCleanup(
	ctx context.Context,
	op *types.TaskCreationOperation,
	prepared scheduler.PreparedTaskSchedule,
	cause error,
) (CreationResult, error) {
	compCtx, cancelComp := context.WithTimeout(
		context.WithoutCancel(ctx), creationCompensationTimeout,
	)
	defer cancelComp()
	rpcCtx, cancel := context.WithTimeout(compCtx, creationExternalRPCTimeout)
	err := c.scheduler.DeleteTask(rpcCtx, prepared)
	cancel()
	if err != nil {
		if errors.Is(err, scheduler.ErrTaskScheduleConflict) ||
			errors.Is(err, scheduler.ErrTaskScheduleUnsafeState) ||
			errors.Is(err, scheduler.ErrTaskScheduleInvalid) ||
			errors.Is(err, scheduler.ErrTaskScheduleBlocked) {
			return c.blockUnsafeSideEffect(
				compCtx, op, prepared.TaskID, "unsafe_cleanup_target",
				"无法证明待清理调度属于本次创建，已保留现场并停止自动处理",
				errors.Join(cause, err),
			)
		}
		return CreationResult{}, errors.Join(cause, fmt.Errorf("delete task during cleanup: %w", err))
	}
	commitCtx, cancelCommit := context.WithTimeout(compCtx, creationExternalRPCTimeout)
	defer cancelCommit()
	if err := c.store.FinishTaskCreationCleanup(
		commitCtx, op.Lease(), prepared.TaskID, types.PendingActionStatusFailed,
	); err != nil {
		if adopted, done, adoptErr := c.adoptTerminalAfterWriteError(
			commitCtx, op, err,
		); done {
			return adopted, adoptErr
		}
		if deterministicCreationSideEffectFailure(err) {
			return c.blockUnsafeSideEffect(
				compCtx, op, prepared.TaskID, "cleanup_finalization_invalid",
				"远端调度已清理，但本地聚合无法安全收口；系统已隔离并保留审计记录",
				errors.Join(cause, err),
			)
		}
		return CreationResult{}, errors.Join(cause, fmt.Errorf("finish task creation cleanup: %w", err))
	}
	return CreationResult{
		OperationID: op.ID, TaskID: prepared.TaskID,
		Message: op.ErrorMessage, Status: types.PendingActionStatusFailed,
	}, nil
}

func (c *CreationCoordinator) blockUnsafeSideEffect(
	ctx context.Context,
	op *types.TaskCreationOperation,
	taskID string,
	code string,
	message string,
	cause error,
) (CreationResult, error) {
	if err := c.store.BlockTaskCreationOperationAfterSideEffect(
		ctx, op.Lease(), taskID, code, message,
	); err != nil {
		if adopted, done, adoptErr := c.adoptTerminalAfterWriteError(ctx, op, err); done {
			return adopted, adoptErr
		}
		return CreationResult{}, errors.Join(cause,
			fmt.Errorf("block unsafe task creation operation: %w", err))
	}
	c.logger.ErrorContext(ctx, "task creation blocked with retained side effects",
		"operation_id", op.ID, "tenant_id", op.TenantID, "user_id", op.UserID,
		"task_id", taskID, "error_code", code, "cause", cause)
	return CreationResult{
		OperationID: op.ID, TaskID: taskID, Message: message,
		Status: types.PendingActionStatusBlocked,
	}, nil
}

// RunRecovery owns the complete startup/periodic stale-operation loop. It is
// safe to launch before HTTP intake; it never blocks readiness on one failed
// operation and exits only after all in-flight recoveries observe cancellation.
func (c *CreationCoordinator) RunRecovery(ctx context.Context) {
	if c == nil || c.store == nil || c.scheduler == nil || c.preparer == nil {
		return
	}
	c.recoverAndLog(ctx)
	ticker := time.NewTicker(creationRecoveryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.recoverAndLog(ctx)
		}
	}
}

func (c *CreationCoordinator) recoverAndLog(ctx context.Context) {
	if err := c.RecoverStaleOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
		c.logger.ErrorContext(ctx, "task creation recovery pass failed", "err", err)
	}
}

// RecoverStaleOnce is exported for startup probes and crash-boundary tests.
func (c *CreationCoordinator) RecoverStaleOnce(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.recoveryMu.Lock()
	defer c.recoveryMu.Unlock()
	passCtx, cancelPass := context.WithTimeout(ctx, creationRecoveryPassTimeout)
	defer cancelPass()
	boundary := time.Now()
	tenantIDs, err := c.store.ListStaleTaskCreationTenantIDs(
		passCtx, boundary, c.recoveryCursor, creationRecoveryTenantLimit,
	)
	if err != nil {
		return fmt.Errorf("list stale task creation tenant shards: %w", err)
	}
	// Tenant-ID keyset pagination makes the cursor global rather than an index
	// inside one perpetually failing top-N page. When the tail is shorter than a
	// page, wrap once to the beginning; failures in tenants 1..100 can therefore
	// never hide tenant 101 forever.
	if c.recoveryCursor > 0 && len(tenantIDs) < creationRecoveryTenantLimit {
		wrapped, wrapErr := c.store.ListStaleTaskCreationTenantIDs(
			passCtx, boundary, 0, creationRecoveryTenantLimit-len(tenantIDs),
		)
		if wrapErr != nil {
			return fmt.Errorf("wrap stale task creation tenant shards: %w", wrapErr)
		}
		tenantIDs = append(tenantIDs, wrapped...)
	}
	operations := make([]types.TaskCreationOperation, 0, creationRecoveryPassLimit)
	var scanErrors []error
	visited := 0
	for visited < len(tenantIDs) && len(operations) < creationRecoveryPassLimit {
		tenantID := tenantIDs[visited]
		visited++
		c.recoveryCursor = tenantID
		remaining := creationRecoveryPassLimit - len(operations)
		limit := min(creationRecoveryPerTenant, remaining)
		shard, err := c.store.ListStaleTaskCreationOperations(
			passCtx, tenantID, time.Now(), limit,
		)
		if err != nil {
			scanErrors = append(scanErrors,
				fmt.Errorf("list stale task creation operations for tenant %d: %w", tenantID, err))
			continue
		}
		if len(shard) > remaining {
			shard = shard[:remaining]
		}
		operations = append(operations, shard...)
	}
	semaphore := make(chan struct{}, creationRecoveryConcurrency)
	var wg sync.WaitGroup
	var errorsMu sync.Mutex
	recoveryErrors := scanErrors
	for i := range operations {
		op := operations[i]
		select {
		case semaphore <- struct{}{}:
		case <-passCtx.Done():
			wg.Wait()
			return errors.Join(passCtx.Err(), errors.Join(recoveryErrors...))
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-semaphore }()
			if err := c.recoverOperation(passCtx, op); err != nil {
				errorsMu.Lock()
				recoveryErrors = append(recoveryErrors, err)
				errorsMu.Unlock()
			}
		}()
	}
	wg.Wait()
	return errors.Join(recoveryErrors...)
}

func (c *CreationCoordinator) recoverOperation(
	ctx context.Context,
	stale types.TaskCreationOperation,
) error {
	owner := "recovery-" + uuid.NewString()
	op, err := c.acquireCreationOperation(ctx, types.AcquireTaskCreationOperationParams{
		ID: stale.ID, TenantID: stale.TenantID, UserID: stale.UserID,
		LeaseOwner: owner, LeaseDuration: creationLeaseDuration,
		ReceiptProvider: stale.ReceiptProvider, ReceiptTarget: stale.ReceiptTarget,
	})
	if err != nil {
		if errors.Is(err, types.ErrTaskCreationBusy) ||
			errors.Is(err, types.ErrTaskCreationTerminal) || errors.Is(err, types.ErrNotFound) {
			return nil
		}
		return fmt.Errorf("acquire stale operation %s: %w", stale.ID, err)
	}
	attemptCtx, cancel := context.WithTimeout(ctx, creationAttemptTimeout)
	defer cancel()
	result, err := c.runAcquired(attemptCtx, op)
	if err != nil {
		return fmt.Errorf("recover operation %s at phase %s: %w", stale.ID, op.Phase, err)
	}
	c.logger.InfoContext(ctx, "task creation recovery converged",
		"operation_id", stale.ID, "tenant_id", stale.TenantID,
		"user_id", stale.UserID, "status", result.Status, "task_id", result.TaskID)
	return nil
}

func validateCreationReceiptTarget(
	op *types.TaskCreationOperation,
	target CreationReceiptTarget,
) error {
	if strings.TrimSpace(target.Provider) == "" ||
		target.Provider != strings.TrimSpace(target.Provider) ||
		strings.TrimSpace(target.Target) == "" ||
		target.Target != strings.TrimSpace(target.Target) {
		return creationValidation("任务回执目标缺失，请重新点击原确认卡。", nil)
	}
	if !validFeishuCardPatchReceiptProvider(target.Provider) {
		return creationValidation("任务回执通道不受支持，请重新点击原确认卡。", nil)
	}
	if op != nil && (op.ReceiptProvider != "" || op.ReceiptTarget != "") &&
		(op.ReceiptProvider != target.Provider || op.ReceiptTarget != target.Target) {
		return types.NewAppError(types.CodeConflict,
			"任务确认卡与已接受的回执目标不一致。", nil)
	}
	return nil
}

func (c *CreationCoordinator) resolveActiveTenant(ctx context.Context, userID int64) (int64, error) {
	if userID <= 0 {
		return 0, creationValidation("用户身份无效", nil)
	}
	memberships, err := c.store.ListMembershipsByUser(ctx, userID)
	if err != nil {
		return 0, fmt.Errorf("list task creation memberships: %w", err)
	}
	active := make([]int64, 0, 1)
	for _, membership := range memberships {
		if membership.UserID != userID || membership.TenantID <= 0 {
			return 0, types.NewAppError(
				types.CodeInternal, "任务创建成员关系损坏", types.ErrInternal,
			)
		}
		tenant, err := c.store.GetTenant(ctx, membership.TenantID)
		if err != nil {
			return 0, fmt.Errorf("load task creation tenant %d: %w", membership.TenantID, err)
		}
		if tenant.Status == types.TenantStatusActive && tenant.DeletedAt == nil {
			active = append(active, tenant.ID)
		}
	}
	if len(active) != 1 {
		return 0, creationValidation("当前账号没有唯一可用的工作空间，暂不能创建任务", nil)
	}
	return active[0], nil
}

// creationScopeActive revalidates the exact operation scope immediately before
// the first potentially state-changing Temporal call. Recovery intentionally
// scans suspended/deleting tenants too; returning false routes those operations
// through exact cleanup instead of leaking a paused or ambiguously active task.
func (c *CreationCoordinator) creationScopeActive(
	ctx context.Context,
	tenantID int64,
	userID int64,
) (bool, error) {
	if tenantID <= 0 || userID <= 0 {
		return false, types.NewAppError(
			types.CodeInternal, "任务创建作用域损坏", types.ErrInternal,
		)
	}
	memberships, err := c.store.ListMembershipsByUser(ctx, userID)
	if err != nil {
		return false, err
	}
	found := false
	for _, membership := range memberships {
		if membership.UserID != userID || membership.TenantID <= 0 {
			return false, types.NewAppError(
				types.CodeInternal, "任务创建成员关系损坏", types.ErrInternal,
			)
		}
		if membership.TenantID == tenantID {
			if found {
				return false, types.NewAppError(
					types.CodeInternal, "任务创建成员关系重复", types.ErrInternal,
				)
			}
			found = true
		}
	}
	if !found {
		return false, nil
	}
	tenant, err := c.store.GetTenant(ctx, tenantID)
	if err != nil {
		if errors.Is(err, types.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	if tenant.ID != tenantID {
		return false, types.NewAppError(
			types.CodeInternal, "任务创建租户边界损坏", types.ErrInternal,
		)
	}
	return tenant.Status == types.TenantStatusActive && tenant.DeletedAt == nil, nil
}

func canonicalCreationProposalArgs(
	command normalizedCreateScheduleCommand,
) (json.RawMessage, error) {
	args := struct {
		Spec              scheduler.ScheduleSpec `json:"spec"`
		Intent            string                 `json:"intent"`
		NLDescription     string                 `json:"nl_description"`
		Strictness        types.PushStrictness   `json:"strictness"`
		ApprovedFetchPlan json.RawMessage        `json:"approved_fetch_plan"`
	}{
		Spec: command.Spec, Intent: command.Intent,
		NLDescription: command.NLDescription, Strictness: command.Strictness,
		ApprovedFetchPlan: bytes.Clone(command.ApprovedFetchPlan),
	}
	canonical, err := json.Marshal(args)
	if err != nil {
		return nil, fmt.Errorf("marshal canonical proposal args: %w", err)
	}
	verification, _, err := normalizeCreateScheduleCommand(canonical)
	if err != nil {
		return nil, fmt.Errorf("verify canonical proposal args: %w", err)
	}
	if verification.Version != command.Version || verification.Spec != command.Spec ||
		verification.Intent != command.Intent ||
		verification.NLDescription != command.NLDescription ||
		verification.Strictness != command.Strictness ||
		!bytes.Equal(verification.ApprovedFetchPlan, command.ApprovedFetchPlan) {
		return nil, errors.New("canonical proposal args changed the approved command")
	}
	return canonical, nil
}

func summarizeCreationProposal(command normalizedCreateScheduleCommand) (string, error) {
	var plan compiledFetchPlan
	if err := decodeStrictJSON(command.ApprovedFetchPlan, &plan); err != nil || len(plan.Sources) == 0 {
		return "", errors.New("approved plan cannot be summarized")
	}
	var builder strings.Builder
	builder.WriteString("任务：")
	builder.WriteString(command.NLDescription)
	builder.WriteString("\n目标：")
	builder.WriteString(command.Intent)
	builder.WriteString("\n时间：")
	builder.WriteString(summarizeCreationTiming(command.Spec))
	strictness := command.Strictness
	if strictness == "" {
		strictness = types.DefaultStrictness
	}
	builder.WriteString("\n筛选：")
	builder.WriteString(summarizeCreationStrictness(strictness))
	builder.WriteString(fmt.Sprintf("\n信息范围（%d）：", len(plan.Sources)))
	for _, source := range plan.Sources {
		builder.WriteString("\n- ")
		builder.WriteString(summarizeApprovedSource(source))
	}
	summary := builder.String()
	if len(summary) > maxCreationSummaryBytes {
		return "", fmt.Errorf("confirmation summary exceeds %d bytes", maxCreationSummaryBytes)
	}
	return summary, nil
}

func summarizeCreationTiming(spec scheduler.ScheduleSpec) string {
	if spec.Cron != "" {
		return fmt.Sprintf("按 %s（%s）", spec.Cron, spec.TZ)
	}
	if spec.AnchorAt != "" {
		return fmt.Sprintf("从 %s 起每 %d 秒（%s）", spec.AnchorAt, spec.EverySeconds, spec.TZ)
	}
	return fmt.Sprintf("每 %d 秒（%s，固定时间边界）", spec.EverySeconds, spec.TZ)
}

func summarizeCreationStrictness(strictness types.PushStrictness) string {
	switch strictness {
	case types.StrictnessStrict:
		return "严格，只推高相关内容"
	case types.StrictnessNormal:
		return "标准，过滤弱相关内容"
	default:
		return "宽松，只过滤无关内容"
	}
}

func summarizeApprovedSource(source compiledFetchSource) string {
	label := strings.TrimSpace(source.Title)
	if label == "" {
		label = source.Platform + "/" + source.Capability
	}
	base := fmt.Sprintf("%s [%s/%s] %s",
		label, source.Platform, source.Capability, source.URL)
	switch {
	case source.Platform == string(types.PlatformWeb) && source.Capability == string(types.CapSearch):
		var config struct {
			Query          string   `json:"query"`
			Category       string   `json:"category"`
			IncludeDomains []string `json:"include_domains"`
		}
		if decodeStrictJSON(source.Config, &config) == nil {
			detail := "搜索“" + config.Query + "”"
			if config.Category != "" {
				detail += "，分类 " + config.Category
			}
			if len(config.IncludeDomains) != 0 {
				detail += "，仅限 " + strings.Join(config.IncludeDomains, "、")
			}
			return base + "；" + detail
		}
	case source.Platform == string(types.PlatformWeb) && source.Capability == string(types.CapContents):
		var config struct {
			URL string `json:"url"`
		}
		if decodeStrictJSON(source.Config, &config) == nil {
			return base + "；页面 " + config.URL
		}
	case source.Platform == string(types.PlatformWeb) && source.Capability == string(types.CapFeed):
		return base
	}
	return base + "；参数 " + string(source.Config)
}

func proposalOperationMatches(
	op *types.TaskCreationOperation,
	p types.CreateTaskCreationOperationParams,
) bool {
	if op == nil || op.ID != p.ID || op.TenantID != p.TenantID || op.UserID != p.UserID ||
		op.ToolName != "create_schedule" ||
		op.ExecutionVersion != types.TaskCreationExecutionVersionV1 ||
		op.Status != types.PendingActionStatusPending || op.Phase != "" ||
		op.Summary != p.Summary || !creationProposalArgsEqual(op.Args, p.Args) ||
		!op.ExpiresAt.Equal(p.ExpiresAt) {
		return false
	}
	if (op.SessionID == nil) != (p.SessionID == nil) {
		return false
	}
	return op.SessionID == nil || *op.SessionID == *p.SessionID
}

// PostgreSQL JSONB does not preserve object key order or whitespace. Proposal
// identity must therefore compare the normalized command, never raw bytes read
// back from pending_actions.args.
func creationProposalArgsEqual(left, right json.RawMessage) bool {
	leftCommand, _, err := normalizeCreateScheduleCommand(left)
	if err != nil {
		return false
	}
	rightCommand, _, err := normalizeCreateScheduleCommand(right)
	if err != nil {
		return false
	}
	leftCanonical, err := canonicalCreationProposalArgs(leftCommand)
	if err != nil {
		return false
	}
	rightCanonical, err := canonicalCreationProposalArgs(rightCommand)
	return err == nil && bytes.Equal(leftCanonical, rightCanonical)
}

type creationSuccessCheckpoint struct {
	Version string `json:"version"`
	TaskID  string `json:"task_id"`
	Message string `json:"message"`
}

func marshalCreationSuccess(taskID string) (json.RawMessage, error) {
	return json.Marshal(creationSuccessCheckpoint{
		Version: creationResultVersion, TaskID: taskID,
		Message: "任务已创建并开始监控。",
	})
}

func creationTerminalResult(
	op *types.TaskCreationOperation,
) (CreationResult, bool, error) {
	if op == nil {
		return CreationResult{}, false, nil
	}
	result := attachCreationAudit(CreationResult{
		OperationID: op.ID, TaskID: op.TaskID, Status: op.Status,
	}, op)
	expectedPhase := map[types.PendingActionStatus]types.TaskCreationPhase{
		types.PendingActionStatusExecuted:  types.TaskCreationPhaseCompleted,
		types.PendingActionStatusCancelled: types.TaskCreationPhaseCancelled,
		types.PendingActionStatusExpired:   types.TaskCreationPhaseExpired,
		types.PendingActionStatusBlocked:   types.TaskCreationPhaseBlocked,
		types.PendingActionStatusFailed:    types.TaskCreationPhaseFailed,
	}[op.Status]
	if expectedPhase != "" && (op.Phase != expectedPhase || op.TombstonedAt == nil ||
		op.LeaseUntil != nil || op.TakeoverNotBefore != nil) {
		return CreationResult{}, true,
			fmt.Errorf("%w: terminal operation tombstone is incomplete", ErrCreationCheckpointInvalid)
	}
	switch op.Status {
	case types.PendingActionStatusExecuted:
		if strings.TrimSpace(op.TaskID) == "" || op.ExecutedAt == nil ||
			op.ErrorCode != "" || op.ErrorMessage != "" {
			return CreationResult{}, true,
				fmt.Errorf("%w: completed operation metadata is invalid", ErrCreationCheckpointInvalid)
		}
		var checkpoint *creationSuccessCheckpoint
		if err := decodeStrictJSON(op.Result, &checkpoint); err != nil || checkpoint == nil ||
			checkpoint.Version != creationResultVersion || checkpoint.TaskID != op.TaskID ||
			strings.TrimSpace(checkpoint.Message) == "" {
			return CreationResult{}, true,
				fmt.Errorf("%w: completed result checkpoint is invalid", ErrCreationCheckpointInvalid)
		}
		// result is stored as JSONB, which intentionally rewrites key order and
		// whitespace. Strict schema + bound fields are the invariant; byte-level
		// canonicality cannot survive a database round trip.
		result.Message = checkpoint.Message
		return result, true, nil
	case types.PendingActionStatusFailed, types.PendingActionStatusBlocked:
		if strings.TrimSpace(op.ErrorCode) == "" || strings.TrimSpace(op.ErrorMessage) == "" ||
			op.ExecutedAt != nil {
			return CreationResult{}, true,
				fmt.Errorf("%w: failed operation metadata is invalid", ErrCreationCheckpointInvalid)
		}
		result.Message = strings.TrimSpace(op.ErrorMessage)
		if result.Message == "" {
			result.Message = "任务创建已安全停止，请重新发起。"
		}
		return result, true, nil
	case types.PendingActionStatusCancelled:
		if op.LeaseOwner != "" || op.Fence != 0 || op.Attempt != 0 ||
			op.TaskID != "" || len(op.Result) != 0 || op.ExecutedAt != nil {
			return CreationResult{}, true,
				fmt.Errorf("%w: cancelled operation metadata is invalid", ErrCreationCheckpointInvalid)
		}
		result.Message = "已取消本次任务创建。"
		return result, true, nil
	case types.PendingActionStatusExpired:
		if op.LeaseOwner != "" || op.Fence != 0 || op.Attempt != 0 ||
			op.TaskID != "" || len(op.Result) != 0 || op.ExecutedAt != nil {
			return CreationResult{}, true,
				fmt.Errorf("%w: expired operation metadata is invalid", ErrCreationCheckpointInvalid)
		}
		result.Message = "这张任务确认已过期，请重新描述需求。"
		return result, true, nil
	default:
		return CreationResult{}, false, nil
	}
}

func recoveringCreationResult(operationID, taskID string) CreationResult {
	return CreationResult{
		OperationID: operationID, TaskID: taskID,
		Message: "任务正在创建，系统会自动继续处理，无需重复确认。",
		Status:  types.PendingActionStatusExecuting, Recovering: true,
	}
}

func attachCreationAudit(
	result CreationResult,
	op *types.TaskCreationOperation,
) CreationResult {
	if op == nil {
		return result
	}
	result.OperationID = op.ID
	if result.TaskID == "" {
		result.TaskID = op.TaskID
	}
	result.Summary = op.Summary
	result.Arguments = bytes.Clone(op.Args)
	result.ReceiptBound = op.ReceiptProvider != "" && op.ReceiptTarget != ""
	if op.SessionID != nil {
		sessionID := *op.SessionID
		result.SessionID = &sessionID
	}
	return result
}

func (c *CreationCoordinator) loadCreationOperationConvergent(
	ctx context.Context,
	id string,
	tenantID int64,
	userID int64,
) (*types.TaskCreationOperation, error) {
	readCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx), creationConvergenceTimeout,
	)
	defer cancel()
	return c.store.LoadTaskCreationOperation(readCtx, id, tenantID, userID)
}

func (c *CreationCoordinator) adoptTerminalAfterWriteError(
	ctx context.Context,
	op *types.TaskCreationOperation,
	cause error,
) (CreationResult, bool, error) {
	loaded, loadErr := c.loadCreationOperationConvergent(
		ctx, op.ID, op.TenantID, op.UserID,
	)
	if loadErr != nil {
		return CreationResult{}, false, errors.Join(cause, loadErr)
	}
	result, done, resultErr := creationTerminalResult(loaded)
	if done || resultErr != nil {
		return result, true, resultErr
	}
	return CreationResult{}, false, cause
}

func opTaskID(op *types.TaskCreationOperation) string {
	if op == nil {
		return ""
	}
	return op.TaskID
}

func creationValidation(message string, cause error) error {
	return types.NewAppError(types.CodeValidation, message, cause)
}
