package task

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/scheduler"
	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

// CreationV3ServerPolicy contains only policy selected by the trusted server
// deployment. It is not part of the Agent/model-facing create request.
type CreationV3ServerPolicy struct {
	PlannerBudget types.PlannerBudget
}

// CreationCoordinatorOption configures trusted control-plane behavior without
// changing retained V1 creation semantics.
type CreationCoordinatorOption func(*CreationCoordinator)

// WithResearchV3CreationPolicy enables native V3 proposal persistence. An
// invalid policy leaves the path disabled so startup wiring fails closed.
func WithResearchV3CreationPolicy(policy CreationV3ServerPolicy) CreationCoordinatorOption {
	return func(coordinator *CreationCoordinator) {
		if coordinator == nil ||
			policy.PlannerBudget.ValidateForMode(types.ExecutionModeDiscoverAtRun) != nil {
			return
		}
		copy := policy
		coordinator.v3Policy = &copy
	}
}

// ResearchV3CreationProposalInput is the complete model-facing durable intent.
// Budget and delivery policy are intentionally absent and injected by the
// coordinator. It contains no source entity or frozen execution plan.
type ResearchV3CreationProposalInput struct {
	ActionID     string
	UserID       int64
	SessionID    *int64
	TaskName     string
	TaskManual   string
	SpecJSON     json.RawMessage
	Notification taskstate.NotificationPolicyV3
	Output       taskstate.OutputPreferenceV3
	ExpiresAt    time.Time
}

type researchV3CreationOperationStore interface {
	CreateResearchTaskCreationOperationV3(
		context.Context,
		types.CreateResearchTaskCreationOperationV3Params,
	) (*types.TaskCreationOperation, error)
	LoadResearchTaskCreationOperationV3(
		context.Context, string, int64, int64,
	) (*types.TaskCreationOperation, error)
}

type researchV3CreationSagaStore interface {
	researchV3CreationOperationStore
	LoadResearchTaskCreationOperationV3(context.Context, string, int64, int64) (*types.TaskCreationOperation, error)
	LoadResearchTaskCreationOperationV3ByUser(context.Context, string, int64) (*types.TaskCreationOperation, error)
	AcquireResearchTaskCreationOperationV3(context.Context, types.AcquireTaskCreationOperationParams) (*types.TaskCreationOperation, error)
	CheckpointResearchTaskCreationPreparedV3(context.Context, types.TaskCreationLease, []byte, string, []byte, string) error
	CheckpointResearchTaskCreationEnsureReceiptV3(context.Context, types.TaskCreationLease, []byte, string) error
	CommitPausedResearchTaskDefinitionV3ForCreation(context.Context, types.CommitPausedResearchTaskDefinitionV3ForCreationParams) error
	BeginResearchTaskCreationActivationV3(context.Context, types.TaskCreationLease, types.ResearchTaskCreationActivationBindingV3) (bool, error)
	CommitResearchTaskCreationActivationV3(context.Context, types.TaskCreationLease, types.ResearchTaskCreationActivationBindingV3) error
	CompleteResearchTaskCreationOperationV3(context.Context, types.TaskCreationLease, string, json.RawMessage) error
	CleanupResearchTaskCreationOperationV3(context.Context, types.TaskCreationLease, string, string, string) error
	BlockResearchTaskCreationOperationV3(context.Context, types.TaskCreationLease, string, string, string) error
	ListStaleResearchTaskCreationOperationsV3(context.Context, int64, time.Time, int) ([]types.TaskCreationOperation, error)
}

type researchV3CreationScheduler interface {
	PrepareResearchTaskScheduleV3(context.Context, scheduler.TaskScheduleRequest) (scheduler.PreparedResearchTaskScheduleV3, error)
	RecoverPreparedResearchTaskScheduleV3(context.Context, scheduler.PreparedResearchTaskScheduleV3) (scheduler.PreparedResearchTaskScheduleV3, error)
	EnsurePausedResearchTaskV3(context.Context, scheduler.PreparedResearchTaskScheduleV3) (scheduler.EnsurePausedTaskResult, error)
	DescribeResearchTaskV3(context.Context, scheduler.PreparedResearchTaskScheduleV3) (scheduler.TaskScheduleSnapshot, error)
	ActivateResearchTaskV3(context.Context, scheduler.PreparedResearchTaskScheduleV3, scheduler.TaskScheduleSnapshot) (scheduler.TaskScheduleSnapshot, error)
	DeleteResearchTaskV3(context.Context, scheduler.PreparedResearchTaskScheduleV3) error
}

// PrepareResearchV3 validates and freezes a native V3 task proposal in the
// existing CreationCoordinator operation journal using execution version 2.
func (c *CreationCoordinator) PrepareResearchV3(
	ctx context.Context,
	in ResearchV3CreationProposalInput,
) (CreationProposal, error) {
	if err := ctx.Err(); err != nil {
		return CreationProposal{}, err
	}
	if c == nil {
		return CreationProposal{}, errors.New("task: native V3 creation is not configured")
	}
	store, ok := c.store.(researchV3CreationOperationStore)
	if !ok || store == nil || c.v3Policy == nil {
		return CreationProposal{}, errors.New("task: native V3 creation is not configured")
	}
	if !validResearchV3CreationOperationID(in.ActionID) ||
		in.UserID <= 0 || in.ExpiresAt.IsZero() {
		return CreationProposal{}, creationValidation("V3 任务创建请求不完整", nil)
	}
	tenantID, err := c.resolveActiveOwnerTenantV3(ctx, in.UserID)
	if err != nil {
		return CreationProposal{}, err
	}
	taskID, err := nativeResearchTaskIDV3(tenantID, in.UserID, in.ActionID)
	if err != nil {
		return CreationProposal{}, creationValidation("V3 任务标识无法生成", err)
	}
	definition, err := taskstate.BuildApprovedDefinitionV3(taskstate.ApprovedDefinitionInputV3{
		TenantID: tenantID, UserID: in.UserID, TaskID: taskID,
		TaskName: in.TaskName, TaskManual: in.TaskManual, SpecJSON: in.SpecJSON,
		ExecutionMode: types.ExecutionModeDiscoverAtRun,
		Notification:  in.Notification, Output: in.Output,
		PlannerBudget:      c.v3Policy.PlannerBudget,
		DeliveryPolicy:     taskstate.DeliveryPolicyOwnerFeishu,
		TenantBudgetPolicy: taskstate.BudgetPolicyInheritTenantQuota,
	})
	if err != nil {
		return CreationProposal{}, creationValidation("V3 任务方案未通过校验", err)
	}
	payload, err := taskstate.EncodeApprovedDefinitionV3(definition)
	if err != nil {
		return CreationProposal{}, creationValidation("V3 任务方案无法规范化", err)
	}
	params := types.CreateResearchTaskCreationOperationV3Params{
		ID: in.ActionID, TenantID: tenantID, UserID: in.UserID,
		SessionID: in.SessionID, Args: payload, Summary: definition.TaskName,
		ExpiresAt: in.ExpiresAt.UTC().Truncate(time.Microsecond),
	}
	op, createErr := store.CreateResearchTaskCreationOperationV3(ctx, params)
	if createErr != nil {
		if deterministicCreationProposalFailure(createErr) {
			return CreationProposal{}, createErr
		}
		readCtx, cancel := context.WithTimeout(
			context.WithoutCancel(ctx), creationConvergenceTimeout)
		op, err = store.LoadResearchTaskCreationOperationV3(
			readCtx, params.ID, params.TenantID, params.UserID)
		cancel()
		if err != nil || !researchV3CreationOperationMatches(op, params) {
			return CreationProposal{}, createErr
		}
	}
	if !researchV3CreationOperationMatches(op, params) {
		return CreationProposal{}, types.NewAppError(
			types.CodeConflict, "V3 任务创建操作与已保存内容冲突", types.ErrConflict)
	}
	return CreationProposal{ID: op.ID, Summary: op.Summary}, nil
}

func researchV3CreationOperationMatches(
	op *types.TaskCreationOperation,
	p types.CreateResearchTaskCreationOperationV3Params,
) bool {
	if op == nil || op.ID != p.ID || op.TenantID != p.TenantID || op.UserID != p.UserID ||
		op.ToolName != "manage_tasks" ||
		op.ExecutionVersion != types.TaskCreationExecutionVersionV2 ||
		op.Summary != p.Summary || !researchV3CreationDefinitionsEqual(op.Args, p.Args) {
		return false
	}
	if (op.SessionID == nil) != (p.SessionID == nil) {
		return false
	}
	return op.SessionID == nil || *op.SessionID == *p.SessionID
}

// ExecuteResearchV3 is the external native-V3 lifecycle entry. Every retry
// resumes the same fenced operation; it never creates a replacement task.
func (c *CreationCoordinator) ExecuteResearchV3(
	ctx context.Context, userID int64, actionID string,
	receiptTarget CreationReceiptTarget,
) (CreationResult, error) {
	if err := ctx.Err(); err != nil {
		return CreationResult{}, err
	}
	store, schedulerV3, err := c.researchV3CreationDependencies()
	if err != nil {
		return CreationResult{}, err
	}
	op, err := store.LoadResearchTaskCreationOperationV3ByUser(ctx, actionID, userID)
	if err != nil {
		if errors.Is(err, types.ErrNotFound) {
			return CreationResult{}, errors.Join(ErrCreationOperationNotFound, err)
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
	owner := "task-create-v3-execute-" + uuid.NewString()
	op, err = c.acquireResearchV3CreationOperation(ctx, store,
		types.AcquireTaskCreationOperationParams{
			ID: actionID, TenantID: op.TenantID, UserID: userID,
			LeaseOwner: owner, LeaseDuration: creationLeaseDuration,
			ReceiptProvider: receiptTarget.Provider, ReceiptTarget: receiptTarget.Target,
		})
	if err != nil {
		if errors.Is(err, types.ErrTaskCreationBusy) {
			return attachCreationAudit(recoveringCreationResult(actionID, opTaskID(op)), op), nil
		}
		if errors.Is(err, types.ErrTaskCreationTerminal) {
			loaded, loadErr := store.LoadResearchTaskCreationOperationV3(
				ctx, actionID, op.TenantID, userID)
			if loadErr != nil {
				return CreationResult{}, errors.Join(err, loadErr)
			}
			result, _, terminalErr := creationTerminalResult(loaded)
			result.Replayed = terminalErr == nil
			return result, terminalErr
		}
		return CreationResult{}, err
	}
	attemptCtx, cancel := context.WithTimeout(ctx, creationAttemptTimeout)
	defer cancel()
	result, runErr := c.runResearchV3Acquired(attemptCtx, store, schedulerV3, op)
	if runErr == nil {
		return attachCreationAudit(result, op), nil
	}
	c.logger.WarnContext(ctx, "native V3 task creation will continue in recovery",
		"operation_id", actionID, "tenant_id", op.TenantID,
		"user_id", userID, "phase", op.Phase, "err", runErr)
	return attachCreationAudit(recoveringCreationResult(actionID, opTaskID(op)), op), nil
}

func (c *CreationCoordinator) researchV3CreationDependencies() (researchV3CreationSagaStore, researchV3CreationScheduler, error) {
	if c == nil || c.store == nil || c.scheduler == nil || c.v3Policy == nil {
		return nil, nil, errors.New("task: native V3 creation is not configured")
	}
	store, storeOK := c.store.(researchV3CreationSagaStore)
	schedulerV3, schedulerOK := c.scheduler.(researchV3CreationScheduler)
	if !storeOK || !schedulerOK {
		return nil, nil, errors.New("task: native V3 creation execution is not configured")
	}
	return store, schedulerV3, nil
}

func (c *CreationCoordinator) acquireResearchV3CreationOperation(
	ctx context.Context, store researchV3CreationSagaStore,
	params types.AcquireTaskCreationOperationParams,
) (*types.TaskCreationOperation, error) {
	op, err := store.AcquireResearchTaskCreationOperationV3(ctx, params)
	if err == nil || deterministicCreationAcquireFailure(err) {
		return op, err
	}
	retryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), creationConvergenceTimeout)
	defer cancel()
	replayed, replayErr := store.AcquireResearchTaskCreationOperationV3(retryCtx, params)
	if replayErr == nil {
		return replayed, nil
	}
	loaded, loadErr := store.LoadResearchTaskCreationOperationV3(
		retryCtx, params.ID, params.TenantID, params.UserID)
	if loadErr == nil && loaded.Status == types.TaskOperationStatusExecuting &&
		loaded.LeaseOwner == params.LeaseOwner && loaded.Fence > 0 {
		return loaded, nil
	}
	return nil, errors.Join(err, replayErr, loadErr)
}

func (c *CreationCoordinator) runResearchV3Acquired(
	ctx context.Context, store researchV3CreationSagaStore,
	schedulerV3 researchV3CreationScheduler, op *types.TaskCreationOperation,
) (CreationResult, error) {
	if op == nil || op.ExecutionVersion != types.TaskCreationExecutionVersionV2 ||
		op.ToolName != "manage_tasks" {
		return CreationResult{}, errors.New("task: acquired native V3 operation is invalid")
	}
	definition, err := taskstate.DecodeApprovedDefinitionV3(op.Args)
	if err != nil || definition.TenantID != op.TenantID || definition.UserID != op.UserID {
		return CreationResult{}, fmt.Errorf("%w: native V3 definition is invalid", ErrCreationCheckpointInvalid)
	}
	definitionPayload, err := taskstate.EncodeApprovedDefinitionV3(definition)
	if err != nil {
		return CreationResult{}, err
	}
	definitionDigest, err := taskstate.DigestApprovedDefinitionV3(definition)
	if err != nil {
		return CreationResult{}, err
	}
	var spec scheduler.ScheduleSpec
	if err := json.Unmarshal(definition.SpecJSON, &spec); err != nil ||
		scheduler.ValidateTaskScheduleSpec(spec) != nil {
		return CreationResult{}, creationValidation("V3 任务调度定义无效", err)
	}
	lease := op.Lease()
	var artifacts scheduler.PreparedResearchTaskScheduleV3
	var preparedBytes []byte
	if len(op.PreparedSchedule) == 0 {
		artifacts, err = schedulerV3.PrepareResearchTaskScheduleV3(ctx,
			scheduler.TaskScheduleRequest{
				TenantID: op.TenantID, UserID: op.UserID, OperationID: op.ID,
				Spec: spec, NLDescription: definition.TaskName,
				PreparedDigest: definitionDigest,
			})
		if err != nil {
			return CreationResult{}, err
		}
		preparedBytes, err = json.Marshal(artifacts)
		if err != nil {
			return CreationResult{}, err
		}
		if err := store.CheckpointResearchTaskCreationPreparedV3(
			ctx, lease, definitionPayload, definitionDigest, preparedBytes,
			artifacts.Schedule.TaskID); err != nil {
			return CreationResult{}, err
		}
	} else {
		prepared, decodeErr := decodePreparedResearchTaskScheduleV3(op)
		if decodeErr != nil {
			return c.blockResearchV3Creation(ctx, store, op,
				"prepared_checkpoint_invalid",
				"V3 任务调度检查点损坏，系统已隔离并保留现场", decodeErr)
		}
		artifacts, err = schedulerV3.RecoverPreparedResearchTaskScheduleV3(ctx, prepared)
		if err != nil {
			return c.blockResearchV3Creation(ctx, store, op,
				"prepared_artifact_invalid",
				"V3 任务调度证据无法恢复，系统已隔离并保留现场", err)
		}
		preparedBytes = bytes.Clone(op.PreparedSchedule)
	}

	current, err := store.LoadResearchTaskCreationOperationV3(ctx, op.ID, op.TenantID, op.UserID)
	if err != nil || current.Lease() != lease {
		return CreationResult{}, errors.Join(err, types.ErrTaskCreationLeaseLost)
	}
	scopeActive, err := c.creationOwnerScopeActiveV3(ctx, op.TenantID, op.UserID)
	if err != nil {
		return CreationResult{}, err
	}
	if !scopeActive {
		return c.cleanupResearchV3Creation(ctx, store, schedulerV3, current, artifacts,
			"creation_scope_inactive", "创建期间工作空间或成员权限失效，本次创建已安全撤销",
			errors.New("native V3 creation scope is inactive"))
	}

	receipt, receiptBytes, err := c.ensureResearchV3Receipt(
		ctx, store, schedulerV3, current, artifacts)
	if err != nil {
		if deterministicCreationSideEffectFailure(err) {
			return c.cleanupResearchV3Creation(ctx, store, schedulerV3, current, artifacts,
				"schedule_ensure_failed", "V3 任务调度未能安全建立，本次创建已撤销", err)
		}
		return CreationResult{}, err
	}
	if err := store.CommitPausedResearchTaskDefinitionV3ForCreation(ctx,
		types.CommitPausedResearchTaskDefinitionV3ForCreationParams{
			Lease: lease, TaskID: artifacts.Schedule.TaskID,
			DefinitionPayload: definitionPayload, DefinitionDigest: definitionDigest,
			PreparedSchedule: preparedBytes,
			EnsureReceipt:    receiptBytes, TargetAction: artifacts.TargetAction,
			TargetActionDigest:        artifacts.TargetActionDigest,
			ActionAuthorizationDigest: artifacts.ActionAuthorizationDigest,
		}); err != nil {
		if errors.Is(err, types.ErrTaskCreationLimit) {
			return c.cleanupResearchV3Creation(ctx, store, schedulerV3, current, artifacts,
				"task_limit_reached", "任务数量已达上限，本次创建已安全撤销", err)
		}
		if errors.Is(err, types.ErrTaskCreationOwnerScopeInactive) {
			return c.cleanupResearchV3Creation(ctx, store, schedulerV3, current, artifacts,
				"creation_scope_inactive", "创建期间所有者权限失效，本次创建已安全撤销", err)
		}
		if deterministicCreationSideEffectFailure(err) {
			return c.cleanupResearchV3Creation(ctx, store, schedulerV3, current, artifacts,
				"definition_commit_failed", "V3 任务定义无法安全提交，本次创建已撤销", err)
		}
		return CreationResult{}, err
	}
	activationBinding := types.ResearchTaskCreationActivationBindingV3{
		TaskID: artifacts.Schedule.TaskID, DefinitionDigest: definitionDigest,
		TargetActionDigest:        artifacts.TargetActionDigest,
		ActionAuthorizationDigest: artifacts.ActionAuthorizationDigest,
	}
	started, err := store.BeginResearchTaskCreationActivationV3(
		ctx, lease, activationBinding)
	if err != nil {
		if errors.Is(err, types.ErrTaskCreationOwnerScopeInactive) {
			return c.cleanupResearchV3Creation(ctx, store, schedulerV3, current, artifacts,
				"creation_scope_inactive", "激活前所有者权限失效，本次创建已安全撤销", err)
		}
		return CreationResult{}, err
	}
	if err := c.activateResearchTaskV3(
		ctx, schedulerV3, artifacts, receipt.Snapshot, started); err != nil {
		if deterministicCreationSideEffectFailure(err) {
			return c.reconcileResearchV3ActivationFailure(ctx, store, schedulerV3,
				current, artifacts, err)
		}
		return CreationResult{}, err
	}
	if err := store.CommitResearchTaskCreationActivationV3(
		ctx, lease, activationBinding); err != nil {
		if errors.Is(err, types.ErrTaskCreationOwnerScopeInactive) {
			return c.cleanupResearchV3Creation(ctx, store, schedulerV3, current, artifacts,
				"creation_scope_inactive", "激活期间所有者权限失效，本次创建已安全撤销", err)
		}
		return CreationResult{}, err
	}
	resultBytes, err := marshalCreationSuccess(artifacts.Schedule.TaskID)
	if err != nil {
		return CreationResult{}, err
	}
	if err := store.CompleteResearchTaskCreationOperationV3(
		ctx, lease, artifacts.Schedule.TaskID, resultBytes); err != nil {
		readCtx, cancel := context.WithTimeout(
			context.WithoutCancel(ctx), creationConvergenceTimeout)
		loaded, loadErr := store.LoadResearchTaskCreationOperationV3(
			readCtx, op.ID, op.TenantID, op.UserID)
		cancel()
		if loadErr == nil {
			if result, done, terminalErr := creationTerminalResult(loaded); done || terminalErr != nil {
				return result, terminalErr
			}
		}
		return c.blockResearchV3Creation(ctx, store, current,
			"completion_finalization_failed",
			"V3 任务已激活但最终记录异常，系统已停止运行并保留现场",
			errors.Join(err, loadErr))
	}
	return CreationResult{
		OperationID: op.ID, TaskID: artifacts.Schedule.TaskID,
		Message: "任务已创建并开始监控。", Status: types.TaskOperationStatusExecuted,
	}, nil
}

func (c *CreationCoordinator) resolveActiveOwnerTenantV3(
	ctx context.Context, userID int64,
) (int64, error) {
	if userID <= 0 {
		return 0, creationValidation("用户身份无效", nil)
	}
	memberships, err := c.store.ListMembershipsByUser(ctx, userID)
	if err != nil {
		return 0, fmt.Errorf("list native V3 owner memberships: %w", err)
	}
	active := make([]int64, 0, 1)
	for _, membership := range memberships {
		if membership.UserID != userID || membership.TenantID <= 0 {
			return 0, types.NewAppError(
				types.CodeInternal, "V3 任务创建成员关系损坏", types.ErrInternal)
		}
		if membership.Role != types.MembershipRoleOwner {
			continue
		}
		tenant, err := c.store.GetTenant(ctx, membership.TenantID)
		if err != nil {
			return 0, fmt.Errorf("load native V3 owner tenant %d: %w",
				membership.TenantID, err)
		}
		if tenant.Status == types.TenantStatusActive && tenant.DeletedAt == nil {
			active = append(active, tenant.ID)
		}
	}
	if len(active) != 1 {
		return 0, creationValidation(
			"当前账号不是唯一可用工作空间的所有者，暂不能创建任务", nil)
	}
	return active[0], nil
}

func (c *CreationCoordinator) creationOwnerScopeActiveV3(
	ctx context.Context, tenantID, userID int64,
) (bool, error) {
	if tenantID <= 0 || userID <= 0 {
		return false, types.NewAppError(
			types.CodeInternal, "V3 任务创建作用域损坏", types.ErrInternal)
	}
	memberships, err := c.store.ListMembershipsByUser(ctx, userID)
	if err != nil {
		return false, err
	}
	found := false
	for _, membership := range memberships {
		if membership.UserID != userID || membership.TenantID <= 0 {
			return false, types.NewAppError(
				types.CodeInternal, "V3 任务创建成员关系损坏", types.ErrInternal)
		}
		if membership.TenantID == tenantID &&
			membership.Role == types.MembershipRoleOwner {
			if found {
				return false, types.NewAppError(
					types.CodeInternal, "V3 任务创建所有者关系重复", types.ErrInternal)
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
			types.CodeInternal, "V3 任务创建租户边界损坏", types.ErrInternal)
	}
	return tenant.Status == types.TenantStatusActive && tenant.DeletedAt == nil, nil
}

func (c *CreationCoordinator) ensureResearchV3Receipt(
	ctx context.Context, store researchV3CreationSagaStore,
	schedulerV3 researchV3CreationScheduler, op *types.TaskCreationOperation,
	prepared scheduler.PreparedResearchTaskScheduleV3,
) (taskCreationEnsureReceipt, []byte, error) {
	if len(op.EnsureReceipt) != 0 {
		receipt, err := decodeCreationEnsureReceipt(op.EnsureReceipt, prepared.Schedule)
		return receipt, bytes.Clone(op.EnsureReceipt), err
	}
	rpcCtx, cancel := context.WithTimeout(ctx, creationExternalRPCTimeout)
	ensured, err := schedulerV3.EnsurePausedResearchTaskV3(rpcCtx, prepared)
	cancel()
	if err != nil {
		return taskCreationEnsureReceipt{}, nil, err
	}
	receipt := taskCreationEnsureReceipt{
		Version: creationEnsureReceiptVersion, Snapshot: ensured.Snapshot,
	}
	receiptBytes, err := json.Marshal(receipt)
	if err != nil {
		return taskCreationEnsureReceipt{}, nil, err
	}
	if _, err := decodeCreationEnsureReceipt(receiptBytes, prepared.Schedule); err != nil {
		return taskCreationEnsureReceipt{}, nil, err
	}
	if err := store.CheckpointResearchTaskCreationEnsureReceiptV3(
		ctx, op.Lease(), receiptBytes, prepared.Schedule.TaskID); err != nil {
		return taskCreationEnsureReceipt{}, nil, err
	}
	return receipt, receiptBytes, nil
}

func decodePreparedResearchTaskScheduleV3(
	op *types.TaskCreationOperation,
) (scheduler.PreparedResearchTaskScheduleV3, error) {
	if op == nil || len(op.PreparedSchedule) == 0 {
		return scheduler.PreparedResearchTaskScheduleV3{},
			errors.New("native V3 prepared schedule checkpoint is missing")
	}
	var prepared *scheduler.PreparedResearchTaskScheduleV3
	if err := decodeStrictJSON(op.PreparedSchedule, &prepared); err != nil || prepared == nil {
		return scheduler.PreparedResearchTaskScheduleV3{},
			errors.New("native V3 prepared schedule checkpoint is invalid")
	}
	canonical, err := json.Marshal(prepared)
	if err != nil || !bytes.Equal(canonical, op.PreparedSchedule) {
		return scheduler.PreparedResearchTaskScheduleV3{},
			errors.New("native V3 prepared schedule checkpoint is not canonical")
	}
	schedule := prepared.Schedule
	if schedule.TaskID == "" || schedule.TaskID != op.TaskID ||
		schedule.TenantID != op.TenantID || schedule.UserID != op.UserID ||
		schedule.OperationID != op.ID || prepared.Input.TenantID != op.TenantID ||
		prepared.Input.UserID != op.UserID || prepared.Input.TaskID != schedule.TaskID ||
		len(prepared.TargetAction) == 0 || prepared.TargetActionDigest == "" ||
		prepared.ActionAuthorizationDigest == "" {
		return scheduler.PreparedResearchTaskScheduleV3{},
			errors.New("native V3 prepared schedule scope binding differs")
	}
	return *prepared, nil
}

func (c *CreationCoordinator) activateResearchTaskV3(
	ctx context.Context,
	schedulerV3 researchV3CreationScheduler,
	prepared scheduler.PreparedResearchTaskScheduleV3,
	receipt scheduler.TaskScheduleSnapshot,
	started bool,
) error {
	if !started {
		rpcCtx, cancel := context.WithTimeout(ctx, creationExternalRPCTimeout)
		observed, err := schedulerV3.DescribeResearchTaskV3(rpcCtx, prepared)
		cancel()
		if err != nil {
			return fmt.Errorf("describe native V3 task before activation recovery: %w", err)
		}
		switch observed.State {
		case scheduler.TaskScheduleActiveVirginExact,
			scheduler.TaskScheduleActiveUsedExact:
			return nil
		case scheduler.TaskSchedulePausedProvisioningExact:
		default:
			return fmt.Errorf("%w: native V3 activation recovery observed %s",
				scheduler.ErrTaskScheduleUnsafeState, observed.State)
		}
	}
	rpcCtx, cancel := context.WithTimeout(ctx, creationExternalRPCTimeout)
	_, err := schedulerV3.ActivateResearchTaskV3(rpcCtx, prepared, receipt)
	cancel()
	if err != nil {
		return fmt.Errorf("activate native V3 task: %w", err)
	}
	return nil
}

func (c *CreationCoordinator) cleanupResearchV3Creation(
	ctx context.Context, store researchV3CreationSagaStore,
	schedulerV3 researchV3CreationScheduler, op *types.TaskCreationOperation,
	prepared scheduler.PreparedResearchTaskScheduleV3,
	code, message string, cause error,
) (CreationResult, error) {
	rpcCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx), creationCompensationTimeout)
	err := schedulerV3.DeleteResearchTaskV3(rpcCtx, prepared)
	cancel()
	if err != nil {
		if deterministicCreationSideEffectFailure(err) {
			return c.blockResearchV3Creation(ctx, store, op,
				"cleanup_remote_unsafe", "V3 任务远端状态无法安全清理，系统已隔离并保留现场",
				errors.Join(cause, err))
		}
		return CreationResult{}, errors.Join(cause, err)
	}
	cleanupCtx, cleanupCancel := context.WithTimeout(
		context.WithoutCancel(ctx), creationConvergenceTimeout)
	defer cleanupCancel()
	if err := store.CleanupResearchTaskCreationOperationV3(
		cleanupCtx, op.Lease(), prepared.Schedule.TaskID, code, message); err != nil {
		return CreationResult{}, errors.Join(cause, err)
	}
	return attachCreationAudit(CreationResult{
		OperationID: op.ID, TaskID: prepared.Schedule.TaskID, Message: message,
		Status: types.TaskOperationStatusFailed,
	}, op), nil
}

func (c *CreationCoordinator) reconcileResearchV3ActivationFailure(
	ctx context.Context, store researchV3CreationSagaStore,
	schedulerV3 researchV3CreationScheduler, op *types.TaskCreationOperation,
	prepared scheduler.PreparedResearchTaskScheduleV3,
	cause error,
) (CreationResult, error) {
	rpcCtx, cancel := context.WithTimeout(ctx, creationExternalRPCTimeout)
	observed, err := schedulerV3.DescribeResearchTaskV3(rpcCtx, prepared)
	cancel()
	if err != nil {
		return c.blockResearchV3Creation(ctx, store, op,
			"activation_state_unsafe", "V3 任务激活状态无法确认，系统已隔离并保留现场",
			errors.Join(cause, err))
	}
	if observed.State == scheduler.TaskScheduleActiveUsedExact {
		return c.blockResearchV3Creation(ctx, store, op,
			"activation_already_ran", "V3 任务激活后已经运行，系统已隔离并保留现场", cause)
	}
	return c.cleanupResearchV3Creation(ctx, store, schedulerV3, op, prepared,
		"activation_failed", "V3 任务调度未能安全激活，本次创建已撤销", cause)
}

func (c *CreationCoordinator) blockResearchV3Creation(
	ctx context.Context, store researchV3CreationSagaStore,
	op *types.TaskCreationOperation, code, message string, cause error,
) (CreationResult, error) {
	if op == nil || op.TaskID == "" {
		return CreationResult{}, cause
	}
	blockCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx), creationConvergenceTimeout)
	defer cancel()
	if err := store.BlockResearchTaskCreationOperationV3(
		blockCtx, op.Lease(), op.TaskID, code, message); err != nil {
		return CreationResult{}, errors.Join(cause, err)
	}
	return attachCreationAudit(CreationResult{
		OperationID: op.ID, TaskID: op.TaskID, Message: message,
		Status: types.TaskOperationStatusBlocked,
	}, op), nil
}

func researchV3CreationDefinitionsEqual(left, right json.RawMessage) bool {
	leftDefinition, err := taskstate.DecodeApprovedDefinitionV3(left)
	if err != nil {
		return false
	}
	rightDefinition, err := taskstate.DecodeApprovedDefinitionV3(right)
	if err != nil {
		return false
	}
	leftDigest, err := taskstate.DigestApprovedDefinitionV3(leftDefinition)
	if err != nil {
		return false
	}
	rightDigest, err := taskstate.DigestApprovedDefinitionV3(rightDefinition)
	return err == nil && leftDigest == rightDigest
}

func validResearchV3CreationOperationID(value string) bool {
	if len(value) == 0 || len(value) > 255 || strings.TrimSpace(value) != value {
		return false
	}
	for _, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') || strings.ContainsRune("-_.:", char) {
			continue
		}
		return false
	}
	return true
}
