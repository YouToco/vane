package task

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/scheduler"
	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

type ResearchTaskDefinitionEditProposalInputV3 struct {
	ActionID string
	ResearchV3DefinitionEditInput
	SessionID int64
	ExpiresAt time.Time
}

type ResearchTaskDefinitionEditStoreV3 interface {
	LoadResearchTaskDefinitionEditBasisV3(context.Context, int64, int64, string) (*types.ResearchTaskDefinitionEditBasisV3, error)
	CreateResearchTaskDefinitionEditOperationV3(context.Context, types.CreateResearchTaskDefinitionEditOperationV3Params) (*types.TaskDefinitionEditOperation, error)
	LoadResearchTaskDefinitionEditOperationV3(context.Context, types.TaskDefinitionEditScope) (*types.TaskDefinitionEditOperation, error)
	AcquireResearchTaskDefinitionEditOperationV3(context.Context, types.AcquireTaskDefinitionEditOperationParams) (*types.TaskDefinitionEditOperation, error)
	QuiesceResearchTaskDefinitionEditV3(context.Context, types.TaskDefinitionEditLease) error
	AuthorizeResearchTaskDefinitionEditRemotePhaseV3(context.Context, types.TaskDefinitionEditLease, types.TaskDefinitionEditPhase) (*types.TaskDefinitionEditOperation, error)
	CheckpointResearchTaskDefinitionEditBasePausedV3(context.Context, types.TaskDefinitionEditLease, []byte) error
	CommitResearchTaskDefinitionEditDefinitionV3(context.Context, types.TaskDefinitionEditLease) error
	CheckpointResearchTaskDefinitionEditTargetAppliedV3(context.Context, types.TaskDefinitionEditLease, []byte) error
	CheckpointResearchTaskDefinitionEditTargetRestoredV3(context.Context, types.TaskDefinitionEditLease, []byte) error
	CompleteResearchTaskDefinitionEditOperationV3(context.Context, types.TaskDefinitionEditLease, json.RawMessage) error
	BlockResearchTaskDefinitionEditOperationV3(context.Context, types.TaskDefinitionEditLease, types.TaskDefinitionEditBlockReason) error
	ClaimStaleResearchTaskDefinitionEditOperationV3(context.Context, time.Time, string, time.Duration) (*types.TaskDefinitionEditOperation, error)
}

type ResearchTaskDefinitionEditSchedulerV3 interface {
	PrepareResearchDefinitionEditV3(context.Context, string, scheduler.PreparedResearchTaskScheduleV3, int64, string, int64, taskstate.ApprovedDefinitionV3) (scheduler.PreparedResearchTaskDefinitionEditV3, scheduler.ResearchTaskDefinitionEditSnapshotV3, error)
	PauseResearchDefinitionEditV3(context.Context, scheduler.PreparedResearchTaskDefinitionEditV3) (scheduler.ResearchTaskDefinitionEditSnapshotV3, error)
	ApplyResearchDefinitionEditV3(context.Context, scheduler.PreparedResearchTaskDefinitionEditV3) (scheduler.ResearchTaskDefinitionEditSnapshotV3, error)
	RestoreResearchDefinitionEditV3(context.Context, scheduler.PreparedResearchTaskDefinitionEditV3) (scheduler.ResearchTaskDefinitionEditSnapshotV3, error)
}

// ResearchTaskDefinitionEditCoordinatorV3 is a decoder-isolated native V3
// saga. It accepts a complete owner-visible target and never produces a card,
// Source entity, fetch target, or long-lived Tool call.
type ResearchTaskDefinitionEditCoordinatorV3 struct {
	store     ResearchTaskDefinitionEditStoreV3
	scheduler ResearchTaskDefinitionEditSchedulerV3
	logger    *slog.Logger
}

func NewResearchTaskDefinitionEditCoordinatorV3(
	store ResearchTaskDefinitionEditStoreV3,
	schedules ResearchTaskDefinitionEditSchedulerV3,
	logger *slog.Logger,
) *ResearchTaskDefinitionEditCoordinatorV3 {
	if logger == nil {
		logger = slog.Default()
	}
	return &ResearchTaskDefinitionEditCoordinatorV3{
		store: store, scheduler: schedules, logger: logger,
	}
}

func (c *ResearchTaskDefinitionEditCoordinatorV3) Prepare(
	ctx context.Context, in ResearchTaskDefinitionEditProposalInputV3,
) (*types.TaskDefinitionEditOperation, error) {
	if c == nil || c.store == nil || c.scheduler == nil {
		return nil, errors.New("task: native V3 edit is not configured")
	}
	if !validResearchV3CreationOperationID(in.ActionID) || in.TenantID <= 0 ||
		in.UserID <= 0 || in.TaskID == "" || in.SessionID <= 0 || in.ExpiresAt.IsZero() {
		return nil, creationValidation("V3 任务编辑请求不完整", nil)
	}
	basis, err := c.store.LoadResearchTaskDefinitionEditBasisV3(
		ctx, in.TenantID, in.UserID, in.TaskID)
	if err != nil {
		return nil, err
	}
	base, err := taskstate.DecodeApprovedDefinitionV3(basis.DefinitionPayload)
	if err != nil {
		return nil, fmt.Errorf("task: native V3 base definition: %w", err)
	}
	target, err := BuildResearchV3DefinitionEditTarget(base, in.ResearchV3DefinitionEditInput)
	if err != nil {
		return nil, err
	}
	basePayload, err := taskstate.EncodeApprovedDefinitionV3(base)
	if err != nil {
		return nil, err
	}
	targetPayload, err := taskstate.EncodeApprovedDefinitionV3(target)
	if err != nil {
		return nil, err
	}
	if bytes.Equal(basePayload, targetPayload) {
		return nil, creationValidation("V3 任务编辑没有产生任何变化", nil)
	}
	basePrepared, err := scheduler.DecodePreparedResearchTaskScheduleV3(
		basis.PreparedSchedule)
	if err != nil {
		return nil, fmt.Errorf("task: native V3 prepared Schedule: %w", err)
	}
	prepared, baseSnapshot, err := c.scheduler.PrepareResearchDefinitionEditV3(
		ctx, in.ActionID, basePrepared, basis.DefinitionVersion,
		basis.DefinitionDigest, basis.DefinitionVersion+1, target)
	if err != nil {
		return nil, err
	}
	preparedBytes, err := scheduler.EncodePreparedResearchTaskDefinitionEditV3(prepared)
	if err != nil {
		return nil, err
	}
	snapshotBytes, err := scheduler.EncodeResearchTaskDefinitionEditSnapshotV3(
		prepared, baseSnapshot)
	if err != nil {
		return nil, err
	}
	params := types.CreateResearchTaskDefinitionEditOperationV3Params{
		ID: in.ActionID, TenantID: in.TenantID, UserID: in.UserID,
		TaskID: in.TaskID, SessionID: in.SessionID,
		ExpiresAt:   in.ExpiresAt.UTC().Truncate(time.Microsecond),
		BaseVersion: basis.DefinitionVersion, BaseDefinition: basePayload,
		TargetVersion: basis.DefinitionVersion + 1, TargetDefinition: targetPayload,
		PreparedEdit: preparedBytes, BaseSnapshot: snapshotBytes,
	}
	op, createErr := c.store.CreateResearchTaskDefinitionEditOperationV3(ctx, params)
	if createErr == nil {
		return op, nil
	}
	readCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx), taskDefinitionEditConvergenceTimeout)
	defer cancel()
	loaded, loadErr := c.store.LoadResearchTaskDefinitionEditOperationV3(
		readCtx, researchTaskDefinitionEditScopeV3(params.ID, params.TenantID,
			params.UserID, params.TaskID))
	if loadErr == nil && researchTaskDefinitionEditProposalMatchesV3(loaded, params) {
		return loaded, nil
	}
	return nil, errors.Join(createErr, loadErr)
}

func researchTaskDefinitionEditProposalMatchesV3(
	op *types.TaskDefinitionEditOperation,
	p types.CreateResearchTaskDefinitionEditOperationV3Params,
) bool {
	return op != nil && op.Protocol == types.TaskDefinitionEditProtocolResearchV3 &&
		op.ID == p.ID && op.TenantID == p.TenantID && op.UserID == p.UserID &&
		op.TaskID == p.TaskID && op.SessionID == p.SessionID &&
		(op.Status == types.TaskDefinitionEditOperationStatusPending ||
			op.Status == types.TaskDefinitionEditOperationStatusExecuting ||
			op.Status == types.TaskDefinitionEditOperationStatusCompleted) &&
		op.BaseDefinitionVersion == p.BaseVersion &&
		op.TargetDefinitionVersion == p.TargetVersion &&
		bytes.Equal(op.BaseDefinition, p.BaseDefinition) &&
		bytes.Equal(op.TargetDefinition, p.TargetDefinition) &&
		bytes.Equal(op.PreparedEdit, p.PreparedEdit) &&
		bytes.Equal(op.BaseSnapshot, p.BaseSnapshot) &&
		op.ExpiresAt.Equal(p.ExpiresAt)
}

func (c *ResearchTaskDefinitionEditCoordinatorV3) Execute(
	ctx context.Context, scope types.TaskDefinitionEditScope,
	receipt TaskDefinitionEditReceiptTarget,
) (TaskDefinitionEditOutcome, error) {
	if c == nil || c.store == nil || c.scheduler == nil {
		return TaskDefinitionEditOutcome{}, errors.New("task: native V3 edit is not configured")
	}
	op, err := c.store.LoadResearchTaskDefinitionEditOperationV3(ctx, scope)
	if err != nil {
		return TaskDefinitionEditOutcome{}, err
	}
	if taskDefinitionEditOperationTerminal(op.Status) {
		outcome := taskDefinitionEditOutcome(op)
		outcome.Replayed = true
		return outcome, nil
	}
	params := types.AcquireTaskDefinitionEditOperationParams{
		Scope: scope, LeaseOwner: "definition-edit-v3-" + uuid.NewString(),
		LeaseDuration:   taskDefinitionEditLeaseDuration,
		ReceiptProvider: receipt.Provider, ReceiptTarget: receipt.Target,
	}
	op, err = c.acquire(ctx, params)
	if err != nil {
		if errors.Is(err, types.ErrTaskDefinitionEditBusy) {
			if current, loadErr := c.load(ctx, scope); loadErr == nil {
				op = current
			}
			outcome := taskDefinitionEditOutcome(op)
			outcome.Recovering = true
			return outcome, nil
		}
		return TaskDefinitionEditOutcome{}, err
	}
	attemptCtx, cancel := context.WithTimeout(ctx, taskDefinitionEditAttemptTimeout)
	defer cancel()
	current, runErr := c.run(attemptCtx, op)
	if runErr == nil {
		return taskDefinitionEditOutcome(current), nil
	}
	loaded, loadErr := c.load(context.WithoutCancel(ctx), scope)
	if loadErr == nil {
		current = loaded
		if taskDefinitionEditOperationTerminal(current.Status) {
			return taskDefinitionEditOutcome(current), nil
		}
	}
	c.logger.WarnContext(ctx, "native V3 definition edit will continue in recovery",
		"operation_id", op.ID, "tenant_id", op.TenantID, "user_id", op.UserID,
		"task_id", op.TaskID, "phase", op.Phase, "err", runErr)
	outcome := taskDefinitionEditOutcome(current)
	outcome.Recovering = true
	return outcome, nil
}

func (c *ResearchTaskDefinitionEditCoordinatorV3) acquire(
	ctx context.Context, params types.AcquireTaskDefinitionEditOperationParams,
) (*types.TaskDefinitionEditOperation, error) {
	op, err := c.store.AcquireResearchTaskDefinitionEditOperationV3(ctx, params)
	if err == nil || deterministicTaskDefinitionEditAcquireFailure(err) {
		return op, err
	}
	retryCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx), taskDefinitionEditConvergenceTimeout)
	defer cancel()
	replayed, replayErr := c.store.AcquireResearchTaskDefinitionEditOperationV3(
		retryCtx, params)
	if replayErr == nil || deterministicTaskDefinitionEditAcquireFailure(replayErr) {
		return replayed, replayErr
	}
	loaded, loadErr := c.store.LoadResearchTaskDefinitionEditOperationV3(
		retryCtx, params.Scope)
	if loadErr == nil && loaded.Status == types.TaskDefinitionEditOperationStatusExecuting &&
		loaded.LeaseOwner == params.LeaseOwner && loaded.Fence > 0 &&
		loaded.ReceiptProvider == params.ReceiptProvider &&
		loaded.ReceiptTarget == params.ReceiptTarget {
		return loaded, nil
	}
	return nil, errors.Join(err, replayErr, loadErr)
}

func (c *ResearchTaskDefinitionEditCoordinatorV3) run(
	ctx context.Context, acquired *types.TaskDefinitionEditOperation,
) (*types.TaskDefinitionEditOperation, error) {
	if acquired == nil || acquired.Protocol != types.TaskDefinitionEditProtocolResearchV3 {
		return nil, errors.New("task: acquired native V3 edit is invalid")
	}
	lease := acquired.Lease()
	current := acquired
	for range taskDefinitionEditMaxLocalAttempts {
		var done bool
		var err error
		current, done, err = c.advance(ctx, lease)
		if err != nil || done {
			return current, err
		}
	}
	return current, errors.New("task: native V3 edit exceeded transition bound")
}

func (c *ResearchTaskDefinitionEditCoordinatorV3) advance(
	ctx context.Context, lease types.TaskDefinitionEditLease,
) (*types.TaskDefinitionEditOperation, bool, error) {
	op, err := c.load(ctx, researchTaskDefinitionEditScopeV3(
		lease.ID, lease.TenantID, lease.UserID, lease.TaskID))
	if err != nil {
		return nil, false, err
	}
	if taskDefinitionEditOperationTerminal(op.Status) {
		return op, true, nil
	}
	if op.Lease() != lease || op.Protocol != types.TaskDefinitionEditProtocolResearchV3 {
		return op, false, types.ErrTaskDefinitionEditLeaseLost
	}
	prepared, err := scheduler.DecodePreparedResearchTaskDefinitionEditV3(op.PreparedEdit)
	if err != nil {
		return c.quarantine(ctx, op, types.TaskDefinitionEditBlockCheckpointInvalid, err)
	}
	switch op.Phase {
	case types.TaskDefinitionEditPhaseProposalSealed:
		err = c.store.QuiesceResearchTaskDefinitionEditV3(ctx, lease)
	case types.TaskDefinitionEditPhaseDBQuiesced:
		err = c.runRemote(ctx, op, prepared, "pause")
	case types.TaskDefinitionEditPhaseTemporalBasePaused:
		err = c.store.CommitResearchTaskDefinitionEditDefinitionV3(ctx, lease)
	case types.TaskDefinitionEditPhaseDefinitionCommitted:
		err = c.runRemote(ctx, op, prepared, "apply")
	case types.TaskDefinitionEditPhaseTemporalTargetApplied:
		err = c.runRemote(ctx, op, prepared, "restore")
	case types.TaskDefinitionEditPhaseTemporalTargetRestored:
		result, resultErr := marshalTaskDefinitionEditSuccess(op)
		if resultErr != nil {
			return c.quarantine(ctx, op,
				types.TaskDefinitionEditBlockCheckpointInvalid, resultErr)
		}
		err = c.store.CompleteResearchTaskDefinitionEditOperationV3(ctx, lease, result)
	default:
		return c.quarantine(ctx, op,
			types.TaskDefinitionEditBlockCheckpointInvalid,
			errors.New("unsupported native V3 edit phase"))
	}
	if err != nil {
		return c.adoptAfterError(ctx, op, err)
	}
	loaded, err := c.load(ctx, op.Scope())
	return loaded, loaded != nil && taskDefinitionEditOperationTerminal(loaded.Status), err
}

func (c *ResearchTaskDefinitionEditCoordinatorV3) runRemote(
	ctx context.Context, op *types.TaskDefinitionEditOperation,
	prepared scheduler.PreparedResearchTaskDefinitionEditV3, phase string,
) error {
	expected := types.TaskDefinitionEditPhaseDBQuiesced
	if phase == "apply" {
		expected = types.TaskDefinitionEditPhaseDefinitionCommitted
	} else if phase == "restore" {
		expected = types.TaskDefinitionEditPhaseTemporalTargetApplied
	}
	if _, err := c.store.AuthorizeResearchTaskDefinitionEditRemotePhaseV3(
		ctx, op.Lease(), expected); err != nil {
		return err
	}
	rpcCtx, cancel := context.WithTimeout(ctx, taskDefinitionEditExternalRPCTimeout)
	defer cancel()
	var snapshot scheduler.ResearchTaskDefinitionEditSnapshotV3
	var err error
	switch phase {
	case "pause":
		snapshot, err = c.scheduler.PauseResearchDefinitionEditV3(rpcCtx, prepared)
	case "apply":
		snapshot, err = c.scheduler.ApplyResearchDefinitionEditV3(rpcCtx, prepared)
	case "restore":
		snapshot, err = c.scheduler.RestoreResearchDefinitionEditV3(rpcCtx, prepared)
	default:
		return errors.New("task: unsupported native V3 remote phase")
	}
	if err != nil {
		if reason, deterministic := researchTaskDefinitionEditRemoteBlockReasonV3(err); deterministic {
			return c.store.BlockResearchTaskDefinitionEditOperationV3(ctx, op.Lease(), reason)
		}
		return err
	}
	encoded, err := scheduler.EncodeResearchTaskDefinitionEditSnapshotV3(prepared, snapshot)
	if err != nil {
		return err
	}
	switch phase {
	case "pause":
		return c.store.CheckpointResearchTaskDefinitionEditBasePausedV3(ctx, op.Lease(), encoded)
	case "apply":
		return c.store.CheckpointResearchTaskDefinitionEditTargetAppliedV3(ctx, op.Lease(), encoded)
	default:
		return c.store.CheckpointResearchTaskDefinitionEditTargetRestoredV3(ctx, op.Lease(), encoded)
	}
}

func researchTaskDefinitionEditRemoteBlockReasonV3(err error) (types.TaskDefinitionEditBlockReason, bool) {
	switch {
	case errors.Is(err, scheduler.ErrTaskScheduleNotFound):
		return types.TaskDefinitionEditBlockTemporalNotFound, true
	case errors.Is(err, scheduler.ErrTaskScheduleInvalid):
		return types.TaskDefinitionEditBlockCheckpointInvalid, true
	case errors.Is(err, scheduler.ErrTaskScheduleConflict),
		errors.Is(err, scheduler.ErrTaskScheduleUnsafeState):
		return types.TaskDefinitionEditBlockUnsafeRemoteState, true
	default:
		return "", false
	}
}

func (c *ResearchTaskDefinitionEditCoordinatorV3) quarantine(
	ctx context.Context, op *types.TaskDefinitionEditOperation,
	reason types.TaskDefinitionEditBlockReason, cause error,
) (*types.TaskDefinitionEditOperation, bool, error) {
	if err := c.store.BlockResearchTaskDefinitionEditOperationV3(
		ctx, op.Lease(), reason); err != nil {
		return op, false, errors.Join(cause, err)
	}
	loaded, err := c.load(ctx, op.Scope())
	return loaded, err == nil, err
}

func (c *ResearchTaskDefinitionEditCoordinatorV3) adoptAfterError(
	ctx context.Context, prior *types.TaskDefinitionEditOperation, cause error,
) (*types.TaskDefinitionEditOperation, bool, error) {
	loaded, err := c.load(context.WithoutCancel(ctx), prior.Scope())
	if err == nil && taskDefinitionEditOperationTerminal(loaded.Status) {
		return loaded, true, nil
	}
	if err == nil && loaded.Lease() == prior.Lease() &&
		taskDefinitionEditPhaseDirectlyFollows(prior.Phase, loaded.Phase) {
		return loaded, false, nil
	}
	return loaded, false, errors.Join(cause, err)
}

func (c *ResearchTaskDefinitionEditCoordinatorV3) load(
	ctx context.Context, scope types.TaskDefinitionEditScope,
) (*types.TaskDefinitionEditOperation, error) {
	readCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx), taskDefinitionEditConvergenceTimeout)
	defer cancel()
	return c.store.LoadResearchTaskDefinitionEditOperationV3(readCtx, scope)
}

func researchTaskDefinitionEditScopeV3(
	id string, tenantID, userID int64, taskID string,
) types.TaskDefinitionEditScope {
	return types.TaskDefinitionEditScope{
		ID: id, TenantID: tenantID, UserID: userID,
		TargetTenantID: tenantID, TargetUserID: userID, TaskID: taskID,
	}
}

// RecoverStaleOnceV3 adopts only expired protocol-3 leases. Retained V1/V2
// rows cannot enter this decoder or scheduler surface.
func (c *ResearchTaskDefinitionEditCoordinatorV3) RecoverStaleOnceV3(
	ctx context.Context,
) error {
	if c == nil || c.store == nil || c.scheduler == nil {
		return errors.New("task: native V3 edit recovery is not configured")
	}
	before := time.Now().UTC()
	for processed := 0; processed < taskDefinitionEditRecoveryPassLimit; processed++ {
		leaseOwner := "definition-edit-v3-recovery-" + uuid.NewString()
		acquired, err := c.store.ClaimStaleResearchTaskDefinitionEditOperationV3(
			ctx, before, leaseOwner, taskDefinitionEditLeaseDuration)
		if err != nil {
			return err
		}
		if acquired == nil {
			return nil
		}
		attemptCtx, cancel := context.WithTimeout(ctx, taskDefinitionEditAttemptTimeout)
		_, runErr := c.run(attemptCtx, acquired)
		cancel()
		if runErr != nil {
			c.logger.WarnContext(ctx, "native V3 edit recovery remains pending",
				"operation_id", acquired.ID, "phase", acquired.Phase, "err", runErr)
		}
	}
	return nil
}

func (c *ResearchTaskDefinitionEditCoordinatorV3) RunRecoveryV3(ctx context.Context) {
	if err := c.RecoverStaleOnceV3(ctx); err != nil {
		c.logger.ErrorContext(ctx, "native V3 edit recovery pass failed", "err", err)
	}
	ticker := time.NewTicker(taskDefinitionEditRecoveryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.RecoverStaleOnceV3(ctx); err != nil {
				c.logger.ErrorContext(ctx, "native V3 edit recovery pass failed", "err", err)
			}
		}
	}
}
