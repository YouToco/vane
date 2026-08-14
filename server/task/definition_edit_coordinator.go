package task

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/scheduler"
	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

const (
	taskDefinitionEditLeaseDuration        = time.Minute
	taskDefinitionEditAttemptTimeout       = 30 * time.Second
	taskDefinitionEditExternalRPCTimeout   = 8 * time.Second
	taskDefinitionEditConvergenceTimeout   = 5 * time.Second
	taskDefinitionEditRecoveryInterval     = 15 * time.Second
	taskDefinitionEditRecoveryPassTimeout  = 90 * time.Second
	taskDefinitionEditRecoveryTenantLimit  = 100
	taskDefinitionEditRecoveryPerTenant    = 4
	taskDefinitionEditRecoveryPassLimit    = 64
	taskDefinitionEditRecoveryConcurrency  = 4
	taskDefinitionEditResultVersion        = "vane.task-definition-edit-result/v1"
	taskDefinitionEditMaxLocalAttempts     = 16
	taskDefinitionEditPreflightTenantLimit = 100
	taskDefinitionEditPreflightOpLimit     = 100
	taskDefinitionEditPreflightMaxOps      = 10_000
)

// TaskDefinitionEditStore is the complete C2b3 operation boundary. Receipt
// delivery intentionally is not part of this interface; the Agent session
// receipt is handled above the saga boundary.
type TaskDefinitionEditStore interface {
	CreateTaskDefinitionEditOperation(
		context.Context,
		types.CreateTaskDefinitionEditOperationParams,
	) (*types.TaskDefinitionEditOperation, error)
	LoadTaskDefinitionEditOperation(
		context.Context,
		types.TaskDefinitionEditScope,
	) (*types.TaskDefinitionEditOperation, error)
	ExpireTaskDefinitionEditOperation(
		context.Context,
		types.ExpireTaskDefinitionEditOperationParams,
	) (*types.TaskDefinitionEditOperation, error)
	AcquireTaskDefinitionEditOperation(
		context.Context,
		types.AcquireTaskDefinitionEditOperationParams,
	) (*types.TaskDefinitionEditOperation, error)
	RenewTaskDefinitionEditLease(
		context.Context,
		types.TaskDefinitionEditLease,
		time.Duration,
	) error
	ListRecoveryTenantCatalogPage(
		context.Context,
		int64,
		int,
	) ([]int64, error)
	ListStaleTaskDefinitionEditOperations(
		context.Context,
		int64,
		time.Time,
		int,
	) ([]types.TaskDefinitionEditOperation, error)
	ListNonterminalTaskDefinitionEditOperations(
		context.Context,
		int64,
		string,
		int,
	) ([]types.TaskDefinitionEditOperation, error)
	QuiesceTaskDefinitionEdit(
		context.Context,
		types.TaskDefinitionEditLease,
	) error
	AuthorizeTaskDefinitionEditRemotePhase(
		context.Context,
		types.TaskDefinitionEditLease,
		types.TaskDefinitionEditPhase,
	) (*types.TaskDefinitionEditOperation, error)
	CheckpointTaskDefinitionEditBasePaused(
		context.Context,
		types.TaskDefinitionEditLease,
		[]byte,
	) error
	CommitTaskDefinitionEditDefinition(
		context.Context,
		types.TaskDefinitionEditLease,
	) error
	CheckpointTaskDefinitionEditTargetApplied(
		context.Context,
		types.TaskDefinitionEditLease,
		[]byte,
	) error
	CheckpointTaskDefinitionEditTargetRestored(
		context.Context,
		types.TaskDefinitionEditLease,
		[]byte,
	) error
	BlockTaskDefinitionEditOperation(
		context.Context,
		types.TaskDefinitionEditLease,
		types.TaskDefinitionEditBlockReason,
	) error
	CompleteTaskDefinitionEditOperation(
		context.Context,
		types.TaskDefinitionEditLease,
		json.RawMessage,
	) error
}

// TaskDefinitionEditScheduler is the only production-facing reference to the
// C2b3-1 raw Temporal surface. Prepare is read-only and runs before a proposal
// is sealed. Each post-acquisition coordinator attempt calls at most one of
// the three mutating/observing phase methods.
type TaskDefinitionEditScheduler interface {
	PrepareTaskDefinitionEdit(
		context.Context,
		scheduler.TaskDefinitionEditRequest,
	) (
		scheduler.PreparedTaskDefinitionEdit,
		scheduler.TaskDefinitionEditSnapshot,
		error,
	)
	PauseTaskDefinitionEdit(
		context.Context,
		scheduler.PreparedTaskDefinitionEdit,
	) (scheduler.TaskDefinitionEditSnapshot, error)
	ApplyTaskDefinitionEdit(
		context.Context,
		scheduler.PreparedTaskDefinitionEdit,
		scheduler.TaskDefinitionEditSnapshot,
	) (scheduler.TaskDefinitionEditSnapshot, error)
	RestoreTaskDefinitionEdit(
		context.Context,
		scheduler.PreparedTaskDefinitionEdit,
		scheduler.TaskDefinitionEditSnapshot,
	) (scheduler.TaskDefinitionEditSnapshot, error)
	ValidateTaskDefinitionEditEnvironment(
		context.Context,
		scheduler.PreparedTaskDefinitionEdit,
	) error
}

// PrepareTaskDefinitionEditProposalInput contains the authenticated identities
// and exact database values needed to freeze an edit proposal. Prepared
// Temporal bytes are deliberately absent: only this coordinator may obtain
// them from the raw read-only prepare API.
type PrepareTaskDefinitionEditProposalInput struct {
	OperationID      string
	OperationRef     string
	ActorTenantID    int64
	ActorUserID      int64
	TargetTenantID   int64
	TargetUserID     int64
	TaskID           string
	SessionID        int64
	ExpiresAt        time.Time
	OriginalStatus   types.ScheduleStatus
	BaseHead         scheduler.TaskDefinitionEditHead
	TargetHead       scheduler.TaskDefinitionEditHead
	BaseDefinition   taskstate.ApprovedDefinitionV1
	TargetDefinition taskstate.ApprovedDefinitionV1
	Creation         scheduler.PreparedTaskSchedule
}

// TaskDefinitionEditReceiptTarget is the server-owned session resource accepted
// by the Store in the same transaction as operation acquisition.
type TaskDefinitionEditReceiptTarget struct {
	Provider string
	Target   string
}

// TaskDefinitionEditOutcome is an internal, provider-neutral view of durable
// progress. Recovering means the operation owns checkpoints and a later pass
// will continue it; it never grants permission to start a second edit.
type TaskDefinitionEditOutcome struct {
	OperationID string
	TaskID      string
	SessionID   int64
	Status      types.TaskDefinitionEditOperationStatus
	Phase       types.TaskDefinitionEditPhase
	Recovering  bool
	Replayed    bool
	// ReceiptBound proves that the session receipt identity was accepted
	// durably with the operation.
	ReceiptBound bool
}

// TaskDefinitionEditCoordinator is the sole C2b3 cross-system writer. Process
// startup constructs it for the environment Gate and recovery. The default-off
// DefinitionEditController is its sole authenticated Agent ingress; HTTP and
// every retired definition writer remain unable to reach these methods.
type TaskDefinitionEditCoordinator struct {
	store     TaskDefinitionEditStore
	scheduler TaskDefinitionEditScheduler
	logger    *slog.Logger

	recoveryMu     sync.Mutex
	recoveryCursor int64
}

func NewTaskDefinitionEditCoordinator(
	store TaskDefinitionEditStore,
	schedules TaskDefinitionEditScheduler,
	logger *slog.Logger,
) *TaskDefinitionEditCoordinator {
	if logger == nil {
		logger = slog.Default()
	}
	return &TaskDefinitionEditCoordinator{
		store: store, scheduler: schedules, logger: logger,
	}
}

// ValidateRuntimeEnvironment scans every durable nonterminal operation before
// ingress and proves its sealed namespace name/ID and retained converter are
// still served by this process. Queries are tenant-sharded and page-bounded;
// an unexpectedly large backlog fails closed instead of silently truncating.
func (c *TaskDefinitionEditCoordinator) ValidateRuntimeEnvironment(
	ctx context.Context,
) error {
	if err := c.validateDependencies(true); err != nil {
		return err
	}

	var (
		afterTenantID int64
		scanned       int
	)
	for {
		tenantIDs, err := c.store.ListRecoveryTenantCatalogPage(
			ctx, afterTenantID, taskDefinitionEditPreflightTenantLimit,
		)
		if err != nil {
			return fmt.Errorf("task: list definition edit preflight tenants: %w", err)
		}
		if len(tenantIDs) == 0 {
			return nil
		}
		for _, tenantID := range tenantIDs {
			afterOperationID := ""
			for {
				operations, err := c.store.ListNonterminalTaskDefinitionEditOperations(
					ctx, tenantID, afterOperationID, taskDefinitionEditPreflightOpLimit,
				)
				if err != nil {
					return fmt.Errorf(
						"task: list definition edit preflight operations for tenant %d: %w",
						tenantID, err,
					)
				}
				if len(operations) == 0 {
					break
				}
				for i := range operations {
					scanned++
					if err := validateTaskDefinitionEditPreflightBudget(
						scanned, taskDefinitionEditPreflightMaxOps,
					); err != nil {
						return err
					}
					frozen, err := validateTaskDefinitionEditOperationCheckpoints(&operations[i])
					if err != nil {
						return fmt.Errorf(
							"task: validate definition edit operation %s before startup: %w",
							operations[i].ID, err,
						)
					}
					if err := c.scheduler.ValidateTaskDefinitionEditEnvironment(
						ctx, frozen.PreparedEdit,
					); err != nil {
						return fmt.Errorf(
							"task: definition edit operation %s runtime environment differs: %w",
							operations[i].ID, err,
						)
					}
				}
				afterOperationID = operations[len(operations)-1].ID
				if len(operations) < taskDefinitionEditPreflightOpLimit {
					break
				}
			}
		}
		afterTenantID = tenantIDs[len(tenantIDs)-1]
		if len(tenantIDs) < taskDefinitionEditPreflightTenantLimit {
			return nil
		}
	}
}

func validateTaskDefinitionEditPreflightBudget(scanned, maximum int) error {
	if maximum <= 0 || scanned < 0 || scanned > maximum {
		return fmt.Errorf(
			"task: definition edit environment preflight exceeds %d operations",
			maximum,
		)
	}
	return nil
}

// PrepareAndSealProposal performs the only legal pre-execution Temporal
// read, binds it to the exact Approved definitions, and persists the five
// canonical checkpoints. Only DefinitionEditController may call it from a
// default-off authenticated Agent proposal.
func (c *TaskDefinitionEditCoordinator) PrepareAndSealProposal(
	ctx context.Context,
	in PrepareTaskDefinitionEditProposalInput,
) (*types.TaskDefinitionEditOperation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := c.validateDependencies(true); err != nil {
		return nil, err
	}
	frozen, err := c.prepareTaskDefinitionEditProposal(ctx, in)
	if err != nil {
		return nil, err
	}
	return c.sealTaskDefinitionEditProposal(ctx, frozen)
}

// prepareTaskDefinitionEditProposal is kept as one small wrapper so executable
// guards can prove that raw Prepare and the frozen proposal builder each have
// exactly one production caller.
func (c *TaskDefinitionEditCoordinator) prepareTaskDefinitionEditProposal(
	ctx context.Context,
	in PrepareTaskDefinitionEditProposalInput,
) (FrozenTaskDefinitionEditProposal, error) {
	if err := validateTaskDefinitionEditPrepareInput(in); err != nil {
		return FrozenTaskDefinitionEditProposal{}, err
	}
	base, err := definitionEditSchedulerProjection(in.BaseDefinition)
	if err != nil {
		return FrozenTaskDefinitionEditProposal{}, invalidDefinitionEditProposal(
			"prepare base projection", err,
		)
	}
	target, err := definitionEditSchedulerProjection(in.TargetDefinition)
	if err != nil {
		return FrozenTaskDefinitionEditProposal{}, invalidDefinitionEditProposal(
			"prepare target projection", err,
		)
	}
	_, originalState, err := definitionEditOriginalStatusForWrite(in.OriginalStatus)
	if err != nil {
		return FrozenTaskDefinitionEditProposal{}, invalidDefinitionEditProposal(
			"prepare original status", err,
		)
	}
	prepared, baseSnapshot, err := c.scheduler.PrepareTaskDefinitionEdit(
		ctx,
		scheduler.TaskDefinitionEditRequest{
			OperationID: in.OperationID, Creation: in.Creation,
			BaseHead: in.BaseHead, TargetHead: in.TargetHead,
			OriginalState: originalState, Base: base, Target: target,
		},
	)
	if err != nil {
		return FrozenTaskDefinitionEditProposal{}, err
	}
	return BuildFrozenTaskDefinitionEditProposal(
		BuildTaskDefinitionEditProposalInput{
			OperationID: in.OperationID, OperationRef: in.OperationRef,
			ActorTenantID: in.ActorTenantID, ActorUserID: in.ActorUserID,
			TargetTenantID: in.TargetTenantID, TargetUserID: in.TargetUserID,
			TaskID: in.TaskID, SessionID: in.SessionID, ExpiresAt: in.ExpiresAt,
			OriginalStatus: in.OriginalStatus, BaseHead: in.BaseHead,
			TargetHead: in.TargetHead, BaseDefinition: in.BaseDefinition,
			TargetDefinition: in.TargetDefinition, PreparedEdit: prepared,
			BaseSnapshot: baseSnapshot,
		},
	)
}

// validateTaskDefinitionEditPrepareInput runs before the read-only Temporal
// Describe. It prevents an eventual authenticated caller from using a malformed
// proposal as a cross-scope schedule oracle and leaves the complete byte-level
// binding to BuildFrozenTaskDefinitionEditProposal after Prepare returns.
func validateTaskDefinitionEditPrepareInput(
	in PrepareTaskDefinitionEditProposalInput,
) error {
	if !validTaskDefinitionEditIdentifier(
		in.OperationID, maxTaskDefinitionEditOperationIDBytes,
	) || !validTaskDefinitionEditIdentifier(
		in.OperationRef, maxTaskDefinitionEditReferenceBytes,
	) || !validTaskDefinitionEditIdentifier(
		in.TaskID, maxTaskDefinitionEditTaskIDBytes,
	) || in.ActorTenantID <= 0 || in.ActorUserID <= 0 ||
		in.TargetTenantID <= 0 || in.TargetUserID <= 0 || in.SessionID <= 0 ||
		in.ExpiresAt.UnixMicro() <= 0 {
		return invalidDefinitionEditProposal(
			"validate pre-prepare identity", errors.New("identity or expiry is invalid"),
		)
	}
	if in.ActorTenantID != in.TargetTenantID || in.ActorUserID != in.TargetUserID ||
		in.Creation.TenantID != in.TargetTenantID ||
		in.Creation.UserID != in.TargetUserID || in.Creation.TaskID != in.TaskID ||
		in.BaseDefinition.TenantID != in.TargetTenantID ||
		in.BaseDefinition.UserID != in.TargetUserID ||
		in.BaseDefinition.TaskID != in.TaskID ||
		in.TargetDefinition.TenantID != in.TargetTenantID ||
		in.TargetDefinition.UserID != in.TargetUserID ||
		in.TargetDefinition.TaskID != in.TaskID {
		return invalidDefinitionEditProposal(
			"validate pre-prepare scope", errors.New("actor, creation, or definition scope differs"),
		)
	}
	if in.BaseHead.Version <= 0 || in.BaseHead.Version == math.MaxInt64 ||
		in.TargetHead.Version != in.BaseHead.Version+1 ||
		!validLowerSHA256(in.BaseHead.Digest) ||
		!validLowerSHA256(in.TargetHead.Digest) {
		return invalidDefinitionEditProposal(
			"validate pre-prepare heads", errors.New("definition heads are invalid"),
		)
	}
	baseBytes, err := taskstate.EncodeApprovedDefinitionV1(in.BaseDefinition)
	if err != nil {
		return invalidDefinitionEditProposal("encode pre-prepare base definition", err)
	}
	targetBytes, err := taskstate.EncodeApprovedDefinitionV1(in.TargetDefinition)
	if err != nil {
		return invalidDefinitionEditProposal("encode pre-prepare target definition", err)
	}
	if !definitionEditDigestEqual(in.BaseHead.Digest, sha256Hex(baseBytes)) ||
		!definitionEditDigestEqual(in.TargetHead.Digest, sha256Hex(targetBytes)) {
		return invalidDefinitionEditProposal(
			"validate pre-prepare definition digests",
			errors.New("definition bytes differ from the requested heads"),
		)
	}
	if in.BaseDefinition.ExecutionMode != types.ExecutionModeCompiled {
		return invalidDefinitionEditProposal(
			"validate pre-prepare base definition",
			errors.New("base definition is not compiled"),
		)
	}
	if err := validateDefinitionEditTargetPolicy(in.TargetDefinition, true); err != nil {
		return err
	}
	return nil
}

// sealTaskDefinitionEditProposal persists only exact bytes returned by the
// immediately preceding frozen proposal builder. Keeping this method private
// prevents a later ingress from bypassing the current-writer Prepare/Build
// gates. Re-decoding also rejects typed fields wrapped around different bytes.
func (c *TaskDefinitionEditCoordinator) sealTaskDefinitionEditProposal(
	ctx context.Context,
	frozen FrozenTaskDefinitionEditProposal,
) (*types.TaskDefinitionEditOperation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := c.validateDependencies(false); err != nil {
		return nil, err
	}
	decoded, err := DecodeFrozenTaskDefinitionEditProposal(
		frozen.CanonicalProposal,
		frozen.BaseDefinitionBytes,
		frozen.TargetDefinitionBytes,
		frozen.PreparedEditBytes,
		frozen.BaseSnapshotBytes,
	)
	if err != nil {
		return nil, err
	}
	params := definitionEditCreateParams(decoded)
	op, createErr := c.store.CreateTaskDefinitionEditOperation(ctx, params)
	if createErr == nil || deterministicTaskDefinitionEditStoreFailure(createErr) {
		return op, createErr
	}

	// The INSERT may have committed while its response was lost. Replaying the
	// exact five checkpoints is safe, then a scoped readback adopts only the same
	// immutable proposal.
	retryCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx), taskDefinitionEditConvergenceTimeout,
	)
	defer cancel()
	replayed, replayErr := c.store.CreateTaskDefinitionEditOperation(retryCtx, params)
	if replayErr == nil {
		return replayed, nil
	}
	loaded, loadErr := c.store.LoadTaskDefinitionEditOperation(
		retryCtx, definitionEditProposalScope(decoded),
	)
	if loadErr == nil && definitionEditOperationMatchesFrozen(loaded, decoded) {
		return loaded, nil
	}
	return nil, errors.Join(createErr, replayErr, loadErr)
}

// Execute acquires the exact frozen operation, then runs bounded local
// attempts. Each runTaskDefinitionEditAttempt advances one durable transition
// and invokes at most one raw Temporal phase. Once acquired, a transient error
// is reported as durable recovery rather than permission to submit a duplicate.
func (c *TaskDefinitionEditCoordinator) Execute(
	ctx context.Context,
	scope types.TaskDefinitionEditScope,
	receipt TaskDefinitionEditReceiptTarget,
) (TaskDefinitionEditOutcome, error) {
	if err := ctx.Err(); err != nil {
		return TaskDefinitionEditOutcome{}, err
	}
	if err := c.validateDependencies(true); err != nil {
		return TaskDefinitionEditOutcome{}, err
	}
	loaded, err := c.store.LoadTaskDefinitionEditOperation(ctx, scope)
	if err != nil {
		return TaskDefinitionEditOutcome{}, err
	}
	if taskDefinitionEditOperationTerminal(loaded.Status) {
		if loaded.ReceiptProvider != receipt.Provider || loaded.ReceiptTarget != receipt.Target {
			return TaskDefinitionEditOutcome{}, types.NewAppError(
				types.CodeConflict,
				"task definition edit: terminal receipt target differs",
				types.ErrConflict,
			)
		}
		outcome := taskDefinitionEditOutcome(loaded)
		outcome.Replayed = true
		return outcome, nil
	}

	owner := "definition-edit-execute-" + uuid.NewString()
	op, err := c.acquireTaskDefinitionEdit(ctx,
		types.AcquireTaskDefinitionEditOperationParams{
			Scope: scope, LeaseOwner: owner,
			LeaseDuration:   taskDefinitionEditLeaseDuration,
			ReceiptProvider: receipt.Provider,
			ReceiptTarget:   receipt.Target,
		})
	if err != nil {
		switch {
		case errors.Is(err, types.ErrTaskDefinitionEditBusy):
			current, loadErr := c.loadTaskDefinitionEditConvergent(ctx, scope)
			if loadErr != nil {
				return TaskDefinitionEditOutcome{}, errors.Join(err, loadErr)
			}
			if current.ReceiptProvider != "" &&
				(current.ReceiptProvider != receipt.Provider ||
					current.ReceiptTarget != receipt.Target) {
				return TaskDefinitionEditOutcome{}, types.NewAppError(
					types.CodeConflict,
					"task definition edit: active receipt target differs",
					types.ErrConflict,
				)
			}
			outcome := taskDefinitionEditOutcome(current)
			if taskDefinitionEditOperationTerminal(current.Status) {
				outcome.Replayed = true
				return outcome, nil
			}
			if current.Status != types.TaskDefinitionEditOperationStatusExecuting {
				return TaskDefinitionEditOutcome{}, errors.Join(
					err,
					types.NewAppError(
						types.CodeConflict,
						"task definition edit: busy operation is not executing",
						types.ErrConflict,
					),
				)
			}
			outcome.Recovering = true
			return outcome, nil
		case errors.Is(err, types.ErrTaskDefinitionEditTerminal):
			terminal, loadErr := c.loadTaskDefinitionEditConvergent(ctx, scope)
			if loadErr != nil {
				return TaskDefinitionEditOutcome{}, errors.Join(err, loadErr)
			}
			if terminal.ReceiptProvider != receipt.Provider ||
				terminal.ReceiptTarget != receipt.Target {
				return TaskDefinitionEditOutcome{}, types.NewAppError(
					types.CodeConflict,
					"task definition edit: terminal receipt target differs",
					types.ErrConflict,
				)
			}
			outcome := taskDefinitionEditOutcome(terminal)
			outcome.Replayed = true
			return outcome, nil
		default:
			return TaskDefinitionEditOutcome{}, err
		}
	}

	attemptCtx, cancel := context.WithTimeout(ctx, taskDefinitionEditAttemptTimeout)
	defer cancel()
	terminal, runErr := c.runTaskDefinitionEditAcquired(attemptCtx, op)
	if runErr == nil {
		return taskDefinitionEditOutcome(terminal), nil
	}
	latest, loadErr := c.loadTaskDefinitionEditConvergent(ctx, scope)
	if loadErr == nil {
		op = latest
		if taskDefinitionEditOperationTerminal(op.Status) {
			return taskDefinitionEditOutcome(op), nil
		}
	}
	c.logger.WarnContext(ctx, "task definition edit will continue in recovery",
		"operation_id", op.ID,
		"tenant_id", op.TenantID,
		"user_id", op.UserID,
		"task_id", op.TaskID,
		"phase", op.Phase,
		"err", runErr,
	)
	outcome := taskDefinitionEditOutcome(op)
	outcome.Recovering = true
	return outcome, nil
}

// Expire tombstones only a proposal proven expired by the database clock.
func (c *TaskDefinitionEditCoordinator) Expire(
	ctx context.Context,
	scope types.TaskDefinitionEditScope,
	receipt TaskDefinitionEditReceiptTarget,
) (TaskDefinitionEditOutcome, error) {
	if err := c.validateDependencies(false); err != nil {
		return TaskDefinitionEditOutcome{}, err
	}
	op, err := c.store.ExpireTaskDefinitionEditOperation(ctx,
		types.ExpireTaskDefinitionEditOperationParams{
			Scope: scope, ReceiptProvider: receipt.Provider,
			ReceiptTarget: receipt.Target,
		})
	return taskDefinitionEditOutcome(op), err
}

func (c *TaskDefinitionEditCoordinator) validateDependencies(
	requireScheduler bool,
) error {
	if c == nil || c.store == nil || (requireScheduler && c.scheduler == nil) {
		return errors.New("task: definition edit coordinator dependencies are incomplete")
	}
	return nil
}

func (c *TaskDefinitionEditCoordinator) acquireTaskDefinitionEdit(
	ctx context.Context,
	params types.AcquireTaskDefinitionEditOperationParams,
) (*types.TaskDefinitionEditOperation, error) {
	op, err := c.store.AcquireTaskDefinitionEditOperation(ctx, params)
	if err == nil || deterministicTaskDefinitionEditAcquireFailure(err) {
		return op, err
	}
	retryCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx), taskDefinitionEditConvergenceTimeout,
	)
	defer cancel()
	replayed, replayErr := c.store.AcquireTaskDefinitionEditOperation(retryCtx, params)
	if replayErr == nil || deterministicTaskDefinitionEditAcquireFailure(replayErr) {
		return replayed, replayErr
	}
	loaded, loadErr := c.store.LoadTaskDefinitionEditOperation(retryCtx, params.Scope)
	if loadErr == nil && loaded.Status == types.TaskDefinitionEditOperationStatusExecuting &&
		loaded.LeaseOwner == params.LeaseOwner && loaded.Fence > 0 &&
		loaded.ReceiptProvider == params.ReceiptProvider &&
		loaded.ReceiptTarget == params.ReceiptTarget {
		return loaded, nil
	}
	return nil, errors.Join(err, replayErr, loadErr)
}

func deterministicTaskDefinitionEditStoreFailure(err error) bool {
	return errors.Is(err, types.ErrValidation) ||
		errors.Is(err, types.ErrConflict) ||
		errors.Is(err, types.ErrNotFound) ||
		errors.Is(err, types.ErrTaskDefinitionEditTerminal)
}

func deterministicTaskDefinitionEditAcquireFailure(err error) bool {
	return deterministicTaskDefinitionEditStoreFailure(err) ||
		errors.Is(err, types.ErrTaskDefinitionEditBusy)
}

func (c *TaskDefinitionEditCoordinator) runTaskDefinitionEditAcquired(
	ctx context.Context,
	op *types.TaskDefinitionEditOperation,
) (*types.TaskDefinitionEditOperation, error) {
	if op == nil {
		return nil, errors.New("task: acquired definition edit operation is nil")
	}
	lease := op.Lease()
	current := op
	for range taskDefinitionEditMaxLocalAttempts {
		var done bool
		var err error
		current, done, err = c.runTaskDefinitionEditAttempt(ctx, lease)
		if err != nil || done {
			return current, err
		}
	}
	return current, errors.New("task: definition edit exceeded the bounded transition count")
}

// runTaskDefinitionEditAttempt advances exactly one durable transition. Only
// the three remote cases below call Temporal, and each branch contains exactly
// one raw phase call followed by one exact Store checkpoint.
func (c *TaskDefinitionEditCoordinator) runTaskDefinitionEditAttempt(
	ctx context.Context,
	lease types.TaskDefinitionEditLease,
) (*types.TaskDefinitionEditOperation, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if err := c.store.RenewTaskDefinitionEditLease(
		ctx, lease, taskDefinitionEditLeaseDuration,
	); err != nil {
		if errors.Is(err, types.ErrTaskDefinitionEditTerminal) {
			terminal, loadErr := c.loadTaskDefinitionEditConvergent(
				ctx, definitionEditLeaseScope(lease),
			)
			return terminal, loadErr == nil, loadErr
		}
		return nil, false, err
	}
	op, err := c.store.LoadTaskDefinitionEditOperation(
		ctx, definitionEditLeaseScope(lease),
	)
	if err != nil {
		return nil, false, err
	}
	if taskDefinitionEditOperationTerminal(op.Status) {
		return op, true, nil
	}
	if op.Status != types.TaskDefinitionEditOperationStatusExecuting ||
		op.Lease() != lease {
		return op, false, types.ErrTaskDefinitionEditLeaseLost
	}
	if _, err := validateTaskDefinitionEditOperationCheckpoints(op); err != nil {
		return c.blockTaskDefinitionEditCheckpoint(ctx, op, err)
	}

	switch op.Phase {
	case types.TaskDefinitionEditPhaseProposalSealed:
		if err := c.store.QuiesceTaskDefinitionEdit(ctx, lease); err != nil {
			return c.reloadTaskDefinitionEditAfterStoreError(ctx, op, err)
		}
		return c.reloadTaskDefinitionEditProgress(ctx, lease)

	case types.TaskDefinitionEditPhaseDBQuiesced:
		return c.runTaskDefinitionEditPauseAttempt(ctx, lease)

	case types.TaskDefinitionEditPhaseTemporalBasePaused:
		if err := c.store.CommitTaskDefinitionEditDefinition(ctx, lease); err != nil {
			return c.reloadTaskDefinitionEditAfterStoreError(ctx, op, err)
		}
		return c.reloadTaskDefinitionEditProgress(ctx, lease)

	case types.TaskDefinitionEditPhaseDefinitionCommitted:
		return c.runTaskDefinitionEditApplyAttempt(ctx, lease)

	case types.TaskDefinitionEditPhaseTemporalTargetApplied:
		return c.runTaskDefinitionEditRestoreAttempt(ctx, lease)

	case types.TaskDefinitionEditPhaseTemporalTargetRestored:
		result, resultErr := marshalTaskDefinitionEditSuccess(op)
		if resultErr != nil {
			return c.blockTaskDefinitionEditCheckpoint(ctx, op, resultErr)
		}
		if err := c.store.CompleteTaskDefinitionEditOperation(ctx, lease, result); err != nil {
			return c.reloadTaskDefinitionEditAfterStoreError(ctx, op, err)
		}
		terminal, err := c.loadTaskDefinitionEditConvergent(
			ctx, definitionEditLeaseScope(lease),
		)
		return terminal, err == nil, err

	default:
		return c.blockTaskDefinitionEditCheckpoint(
			ctx, op, errors.New("durable definition edit phase is unsupported"),
		)
	}
}

func (c *TaskDefinitionEditCoordinator) runTaskDefinitionEditPauseAttempt(
	ctx context.Context,
	lease types.TaskDefinitionEditLease,
) (*types.TaskDefinitionEditOperation, bool, error) {
	op, frozen, done, err := c.authorizeTaskDefinitionEditRemote(
		ctx, lease, types.TaskDefinitionEditPhaseDBQuiesced,
	)
	if err != nil || done {
		return op, done, err
	}
	rpcCtx, cancel := context.WithTimeout(ctx, taskDefinitionEditExternalRPCTimeout)
	snapshot, rpcErr := c.scheduler.PauseTaskDefinitionEdit(rpcCtx, frozen.PreparedEdit)
	cancel()
	if rpcErr != nil {
		return c.handleTaskDefinitionEditRemoteError(ctx, op, rpcErr)
	}
	encoded, err := scheduler.EncodeTaskDefinitionEditPhaseSnapshot(
		frozen.PreparedEdit, snapshot,
	)
	if err != nil {
		return c.blockTaskDefinitionEditCheckpoint(ctx, op, err)
	}
	if err := c.store.CheckpointTaskDefinitionEditBasePaused(ctx, lease, encoded); err != nil {
		return c.reloadTaskDefinitionEditAfterStoreError(ctx, op, err)
	}
	return c.reloadTaskDefinitionEditProgress(ctx, lease)
}

func (c *TaskDefinitionEditCoordinator) runTaskDefinitionEditApplyAttempt(
	ctx context.Context,
	lease types.TaskDefinitionEditLease,
) (*types.TaskDefinitionEditOperation, bool, error) {
	op, frozen, done, err := c.authorizeTaskDefinitionEditRemote(
		ctx, lease, types.TaskDefinitionEditPhaseDefinitionCommitted,
	)
	if err != nil || done {
		return op, done, err
	}
	source, err := scheduler.DecodeTaskDefinitionEditPhaseSnapshot(
		frozen.PreparedEdit, op.PauseSnapshot,
	)
	if err != nil {
		return c.blockTaskDefinitionEditCheckpoint(ctx, op, err)
	}
	rpcCtx, cancel := context.WithTimeout(ctx, taskDefinitionEditExternalRPCTimeout)
	snapshot, rpcErr := c.scheduler.ApplyTaskDefinitionEdit(
		rpcCtx, frozen.PreparedEdit, source,
	)
	cancel()
	if rpcErr != nil {
		return c.handleTaskDefinitionEditRemoteError(ctx, op, rpcErr)
	}
	encoded, err := scheduler.EncodeTaskDefinitionEditPhaseSnapshot(
		frozen.PreparedEdit, snapshot,
	)
	if err != nil {
		return c.blockTaskDefinitionEditCheckpoint(ctx, op, err)
	}
	if err := c.store.CheckpointTaskDefinitionEditTargetApplied(ctx, lease, encoded); err != nil {
		return c.reloadTaskDefinitionEditAfterStoreError(ctx, op, err)
	}
	return c.reloadTaskDefinitionEditProgress(ctx, lease)
}

func (c *TaskDefinitionEditCoordinator) runTaskDefinitionEditRestoreAttempt(
	ctx context.Context,
	lease types.TaskDefinitionEditLease,
) (*types.TaskDefinitionEditOperation, bool, error) {
	op, frozen, done, err := c.authorizeTaskDefinitionEditRemote(
		ctx, lease, types.TaskDefinitionEditPhaseTemporalTargetApplied,
	)
	if err != nil || done {
		return op, done, err
	}
	source, err := scheduler.DecodeTaskDefinitionEditPhaseSnapshot(
		frozen.PreparedEdit, op.ApplySnapshot,
	)
	if err != nil {
		return c.blockTaskDefinitionEditCheckpoint(ctx, op, err)
	}
	rpcCtx, cancel := context.WithTimeout(ctx, taskDefinitionEditExternalRPCTimeout)
	snapshot, rpcErr := c.scheduler.RestoreTaskDefinitionEdit(
		rpcCtx, frozen.PreparedEdit, source,
	)
	cancel()
	if rpcErr != nil {
		return c.handleTaskDefinitionEditRemoteError(ctx, op, rpcErr)
	}
	encoded, err := scheduler.EncodeTaskDefinitionEditPhaseSnapshot(
		frozen.PreparedEdit, snapshot,
	)
	if err != nil {
		return c.blockTaskDefinitionEditCheckpoint(ctx, op, err)
	}
	if err := c.store.CheckpointTaskDefinitionEditTargetRestored(ctx, lease, encoded); err != nil {
		return c.reloadTaskDefinitionEditAfterStoreError(ctx, op, err)
	}
	return c.reloadTaskDefinitionEditProgress(ctx, lease)
}

func (c *TaskDefinitionEditCoordinator) authorizeTaskDefinitionEditRemote(
	ctx context.Context,
	lease types.TaskDefinitionEditLease,
	expected types.TaskDefinitionEditPhase,
) (
	*types.TaskDefinitionEditOperation,
	FrozenTaskDefinitionEditProposal,
	bool,
	error,
) {
	op, err := c.store.AuthorizeTaskDefinitionEditRemotePhase(ctx, lease, expected)
	if err != nil {
		if errors.Is(err, types.ErrTaskDefinitionEditTerminal) {
			terminal, loadErr := c.loadTaskDefinitionEditConvergent(
				ctx, definitionEditLeaseScope(lease),
			)
			return terminal, FrozenTaskDefinitionEditProposal{}, loadErr == nil, loadErr
		}
		return op, FrozenTaskDefinitionEditProposal{}, false, err
	}
	frozen, err := validateTaskDefinitionEditOperationCheckpoints(op)
	if err != nil {
		blocked, done, blockErr := c.blockTaskDefinitionEditCheckpoint(ctx, op, err)
		return blocked, FrozenTaskDefinitionEditProposal{}, done, blockErr
	}
	return op, frozen, false, nil
}

func (c *TaskDefinitionEditCoordinator) handleTaskDefinitionEditRemoteError(
	ctx context.Context,
	op *types.TaskDefinitionEditOperation,
	cause error,
) (*types.TaskDefinitionEditOperation, bool, error) {
	var reason types.TaskDefinitionEditBlockReason
	switch {
	case errors.Is(cause, scheduler.ErrTaskScheduleNotFound):
		reason = types.TaskDefinitionEditBlockTemporalNotFound
	case errors.Is(cause, scheduler.ErrTaskScheduleInvalid):
		reason = types.TaskDefinitionEditBlockCheckpointInvalid
	case errors.Is(cause, scheduler.ErrTaskScheduleConflict),
		errors.Is(cause, scheduler.ErrTaskScheduleUnsafeState):
		reason = types.TaskDefinitionEditBlockUnsafeRemoteState
	case errors.Is(cause, scheduler.ErrTaskScheduleBlocked):
		// Environment, namespace availability, permission, and converter
		// failures are operator-repairable. Preserve the executing operation and
		// its exact source checkpoint so recovery can retry after repair.
		return op, false, cause
	case errors.Is(cause, scheduler.ErrTaskScheduleOutcomeUnknown),
		errors.Is(cause, scheduler.ErrTaskScheduleTransient),
		errors.Is(cause, context.Canceled),
		errors.Is(cause, context.DeadlineExceeded):
		return op, false, cause
	default:
		return op, false, cause
	}
	if err := c.store.BlockTaskDefinitionEditOperation(ctx, op.Lease(), reason); err != nil {
		return c.reloadTaskDefinitionEditAfterStoreError(ctx, op, errors.Join(cause, err))
	}
	terminal, err := c.loadTaskDefinitionEditConvergent(ctx, op.Scope())
	if err == nil {
		c.logger.ErrorContext(ctx, "task definition edit entered quarantine",
			"operation_id", op.ID,
			"tenant_id", op.TenantID,
			"user_id", op.UserID,
			"task_id", op.TaskID,
			"phase", op.Phase,
			"reason", reason,
			"cause", cause,
		)
	}
	return terminal, err == nil, err
}

func (c *TaskDefinitionEditCoordinator) blockTaskDefinitionEditCheckpoint(
	ctx context.Context,
	op *types.TaskDefinitionEditOperation,
	cause error,
) (*types.TaskDefinitionEditOperation, bool, error) {
	if op == nil {
		return nil, false, cause
	}
	err := c.store.BlockTaskDefinitionEditOperation(
		ctx, op.Lease(), types.TaskDefinitionEditBlockCheckpointInvalid,
	)
	if err != nil {
		return c.reloadTaskDefinitionEditAfterStoreError(ctx, op, errors.Join(cause, err))
	}
	terminal, loadErr := c.loadTaskDefinitionEditConvergent(ctx, op.Scope())
	if loadErr == nil {
		c.logger.ErrorContext(ctx, "task definition edit checkpoint was quarantined",
			"operation_id", op.ID,
			"tenant_id", op.TenantID,
			"user_id", op.UserID,
			"task_id", op.TaskID,
			"phase", op.Phase,
			"cause", cause,
		)
	}
	return terminal, loadErr == nil, loadErr
}

func (c *TaskDefinitionEditCoordinator) reloadTaskDefinitionEditProgress(
	ctx context.Context,
	lease types.TaskDefinitionEditLease,
) (*types.TaskDefinitionEditOperation, bool, error) {
	op, err := c.store.LoadTaskDefinitionEditOperation(
		ctx, definitionEditLeaseScope(lease),
	)
	return op, op != nil && taskDefinitionEditOperationTerminal(op.Status), err
}

func (c *TaskDefinitionEditCoordinator) reloadTaskDefinitionEditAfterStoreError(
	ctx context.Context,
	prior *types.TaskDefinitionEditOperation,
	cause error,
) (*types.TaskDefinitionEditOperation, bool, error) {
	if prior == nil {
		return nil, false, cause
	}
	loaded, err := c.loadTaskDefinitionEditConvergent(ctx, prior.Scope())
	if err == nil && taskDefinitionEditOperationTerminal(loaded.Status) {
		return loaded, true, nil
	}
	if err == nil && loaded.Status == types.TaskDefinitionEditOperationStatusExecuting &&
		loaded.Lease() == prior.Lease() &&
		taskDefinitionEditPhaseDirectlyFollows(prior.Phase, loaded.Phase) {
		// A short Store transaction may have committed while its response was
		// lost. The exact adjacent durable phase under the same fence is the only
		// admissible proof; a backward or skipped phase is corruption, not progress.
		return loaded, false, nil
	}
	return loaded, false, errors.Join(cause, err)
}

func taskDefinitionEditPhaseDirectlyFollows(
	prior types.TaskDefinitionEditPhase,
	loaded types.TaskDefinitionEditPhase,
) bool {
	switch prior {
	case types.TaskDefinitionEditPhaseProposalSealed:
		return loaded == types.TaskDefinitionEditPhaseDBQuiesced
	case types.TaskDefinitionEditPhaseDBQuiesced:
		return loaded == types.TaskDefinitionEditPhaseTemporalBasePaused
	case types.TaskDefinitionEditPhaseTemporalBasePaused:
		return loaded == types.TaskDefinitionEditPhaseDefinitionCommitted
	case types.TaskDefinitionEditPhaseDefinitionCommitted:
		return loaded == types.TaskDefinitionEditPhaseTemporalTargetApplied
	case types.TaskDefinitionEditPhaseTemporalTargetApplied:
		return loaded == types.TaskDefinitionEditPhaseTemporalTargetRestored
	default:
		return false
	}
}

func (c *TaskDefinitionEditCoordinator) loadTaskDefinitionEditConvergent(
	ctx context.Context,
	scope types.TaskDefinitionEditScope,
) (*types.TaskDefinitionEditOperation, error) {
	readCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx), taskDefinitionEditConvergenceTimeout,
	)
	defer cancel()
	return c.store.LoadTaskDefinitionEditOperation(readCtx, scope)
}

// RunRecovery provides the C2b3-2c startup and periodic dark recovery loop.
// The composition root may call only this method; authenticated proposal,
// execution, cancellation, and receipt delivery remain separate work.
func (c *TaskDefinitionEditCoordinator) RunRecovery(ctx context.Context) {
	if c.validateDependencies(true) != nil {
		return
	}
	c.recoverTaskDefinitionEditsAndLog(ctx)
	ticker := time.NewTicker(taskDefinitionEditRecoveryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.recoverTaskDefinitionEditsAndLog(ctx)
		}
	}
}

func (c *TaskDefinitionEditCoordinator) recoverTaskDefinitionEditsAndLog(
	ctx context.Context,
) {
	if err := c.RecoverStaleOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
		c.logger.ErrorContext(ctx, "task definition edit recovery pass failed", "err", err)
	}
}

// RecoverStaleOnce performs one serialized, tenant-sharded and globally bounded
// pass. Store queries use PostgreSQL clock predicates; the process timestamp is
// only an upper scan boundary and can never authorize an early takeover.
func (c *TaskDefinitionEditCoordinator) RecoverStaleOnce(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := c.validateDependencies(true); err != nil {
		return err
	}
	c.recoveryMu.Lock()
	defer c.recoveryMu.Unlock()

	passCtx, cancel := context.WithTimeout(ctx, taskDefinitionEditRecoveryPassTimeout)
	defer cancel()
	tenantIDs, err := c.store.ListRecoveryTenantCatalogPage(
		passCtx, c.recoveryCursor, taskDefinitionEditRecoveryTenantLimit,
	)
	if err != nil {
		return fmt.Errorf("list stale task definition edit tenant shards: %w", err)
	}
	if c.recoveryCursor > 0 && len(tenantIDs) < taskDefinitionEditRecoveryTenantLimit {
		wrapped, wrapErr := c.store.ListRecoveryTenantCatalogPage(
			passCtx, 0,
			taskDefinitionEditRecoveryTenantLimit-len(tenantIDs),
		)
		if wrapErr != nil {
			return fmt.Errorf("wrap stale task definition edit tenant shards: %w", wrapErr)
		}
		// The wrap query has no upper-bound parameter. If fewer than the limit
		// tenants exist after the cursor it may include those same tenant IDs
		// again after returning the lower IDs. De-duplicate the bounded page so
		// one noisy shard cannot consume two slots in the same recovery pass.
		seen := make(map[int64]struct{}, len(tenantIDs)+len(wrapped))
		for _, tenantID := range tenantIDs {
			seen[tenantID] = struct{}{}
		}
		for _, tenantID := range wrapped {
			if _, duplicate := seen[tenantID]; duplicate {
				continue
			}
			seen[tenantID] = struct{}{}
			tenantIDs = append(tenantIDs, tenantID)
		}
	}

	operations := make([]types.TaskDefinitionEditOperation, 0,
		taskDefinitionEditRecoveryPassLimit)
	var recoveryErrors []error
	for _, tenantID := range tenantIDs {
		if len(operations) >= taskDefinitionEditRecoveryPassLimit {
			break
		}
		c.recoveryCursor = tenantID
		remaining := taskDefinitionEditRecoveryPassLimit - len(operations)
		limit := min(taskDefinitionEditRecoveryPerTenant, remaining)
		shard, shardErr := c.store.ListStaleTaskDefinitionEditOperations(
			passCtx, tenantID, time.Now(), limit,
		)
		if shardErr != nil {
			recoveryErrors = append(recoveryErrors, fmt.Errorf(
				"list stale task definition edits for tenant %d: %w",
				tenantID, shardErr,
			))
			continue
		}
		if len(shard) > remaining {
			shard = shard[:remaining]
		}
		operations = append(operations, shard...)
	}

	semaphore := make(chan struct{}, taskDefinitionEditRecoveryConcurrency)
	var wg sync.WaitGroup
	var errorsMu sync.Mutex
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
			if recoverErr := c.recoverTaskDefinitionEditOperation(passCtx, op); recoverErr != nil {
				errorsMu.Lock()
				recoveryErrors = append(recoveryErrors, recoverErr)
				errorsMu.Unlock()
			}
		}()
	}
	wg.Wait()
	return errors.Join(recoveryErrors...)
}

func (c *TaskDefinitionEditCoordinator) recoverTaskDefinitionEditOperation(
	ctx context.Context,
	stale types.TaskDefinitionEditOperation,
) error {
	owner := "definition-edit-recovery-" + uuid.NewString()
	op, err := c.acquireTaskDefinitionEdit(ctx,
		types.AcquireTaskDefinitionEditOperationParams{
			Scope: stale.Scope(), LeaseOwner: owner,
			LeaseDuration:   taskDefinitionEditLeaseDuration,
			ReceiptProvider: stale.ReceiptProvider,
			ReceiptTarget:   stale.ReceiptTarget,
		})
	if err != nil {
		if errors.Is(err, types.ErrTaskDefinitionEditBusy) ||
			errors.Is(err, types.ErrTaskDefinitionEditTerminal) ||
			errors.Is(err, types.ErrNotFound) {
			return nil
		}
		return fmt.Errorf("acquire stale task definition edit %s: %w", stale.ID, err)
	}
	attemptCtx, cancel := context.WithTimeout(ctx, taskDefinitionEditAttemptTimeout)
	defer cancel()
	terminal, err := c.runTaskDefinitionEditAcquired(attemptCtx, op)
	if err != nil {
		return fmt.Errorf(
			"recover task definition edit %s at phase %s: %w",
			stale.ID, op.Phase, err,
		)
	}
	c.logger.InfoContext(ctx, "task definition edit recovery converged",
		"operation_id", stale.ID,
		"tenant_id", stale.TenantID,
		"user_id", stale.UserID,
		"task_id", stale.TaskID,
		"status", terminal.Status,
		"phase", terminal.Phase,
	)
	return nil
}

type taskDefinitionEditSuccessV1 struct {
	Version           string `json:"version"`
	TaskID            string `json:"task_id"`
	DefinitionVersion int64  `json:"definition_version"`
	DefinitionDigest  string `json:"definition_digest"`
}

func marshalTaskDefinitionEditSuccess(
	op *types.TaskDefinitionEditOperation,
) (json.RawMessage, error) {
	if op == nil || op.TaskID == "" || op.TargetDefinitionVersion <= 0 ||
		op.TargetDefinitionDigest == "" {
		return nil, errors.New("task: definition edit completion identity is invalid")
	}
	result, err := json.Marshal(taskDefinitionEditSuccessV1{
		Version:           taskDefinitionEditResultVersion,
		TaskID:            op.TaskID,
		DefinitionVersion: op.TargetDefinitionVersion,
		DefinitionDigest:  op.TargetDefinitionDigest,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal task definition edit result: %w", err)
	}
	return result, nil
}

func decodeTaskDefinitionEditOperation(
	op *types.TaskDefinitionEditOperation,
) (FrozenTaskDefinitionEditProposal, error) {
	if op == nil {
		return FrozenTaskDefinitionEditProposal{}, errors.New(
			"task: definition edit operation is nil",
		)
	}
	frozen, err := DecodeFrozenTaskDefinitionEditProposal(
		op.CanonicalProposal,
		op.BaseDefinition,
		op.TargetDefinition,
		op.PreparedEdit,
		op.BaseSnapshot,
	)
	if err != nil {
		return FrozenTaskDefinitionEditProposal{}, err
	}
	if !definitionEditOperationMatchesFrozen(op, frozen) {
		return FrozenTaskDefinitionEditProposal{}, fmt.Errorf(
			"%w: durable operation columns differ from frozen checkpoints",
			ErrDefinitionEditProposalInvalid,
		)
	}
	return frozen, nil
}

// validateTaskDefinitionEditOperationCheckpoints distrusts every row loaded in
// a later transaction. The durable phase determines the exact immutable
// snapshot prefix that must exist; no missing or future checkpoint may be
// reconstructed from current configuration or a fresh Temporal Describe.
func validateTaskDefinitionEditOperationCheckpoints(
	op *types.TaskDefinitionEditOperation,
) (FrozenTaskDefinitionEditProposal, error) {
	frozen, err := decodeTaskDefinitionEditOperation(op)
	if err != nil {
		return FrozenTaskDefinitionEditProposal{}, err
	}

	var requirePause, requireApply, requireRestore bool
	switch op.Phase {
	case types.TaskDefinitionEditPhaseProposalSealed,
		types.TaskDefinitionEditPhaseDBQuiesced:
	case types.TaskDefinitionEditPhaseTemporalBasePaused,
		types.TaskDefinitionEditPhaseDefinitionCommitted:
		requirePause = true
	case types.TaskDefinitionEditPhaseTemporalTargetApplied:
		requirePause, requireApply = true, true
	case types.TaskDefinitionEditPhaseTemporalTargetRestored:
		requirePause, requireApply, requireRestore = true, true, true
	default:
		return FrozenTaskDefinitionEditProposal{}, invalidDefinitionEditProposal(
			"validate durable phase", errors.New("unsupported durable progress phase"),
		)
	}

	pausePhase := scheduler.TaskDefinitionEditPhaseBasePaused
	applyPhase := scheduler.TaskDefinitionEditPhaseTargetPaused
	if frozen.PreparedEdit.OriginalState == scheduler.TaskDefinitionEditOriginalStatePaused {
		pausePhase = scheduler.TaskDefinitionEditPhaseBaseOriginal
		applyPhase = scheduler.TaskDefinitionEditPhaseTargetFinal
	}
	for _, checkpoint := range []struct {
		name     string
		raw      []byte
		digest   string
		required bool
		phase    scheduler.TaskDefinitionEditPhase
	}{
		{name: "pause", raw: op.PauseSnapshot, digest: op.PauseSnapshotDigest,
			required: requirePause, phase: pausePhase},
		{name: "apply", raw: op.ApplySnapshot, digest: op.ApplySnapshotDigest,
			required: requireApply, phase: applyPhase},
		{name: "restore", raw: op.RestoreSnapshot, digest: op.RestoreSnapshotDigest,
			required: requireRestore, phase: scheduler.TaskDefinitionEditPhaseTargetFinal},
	} {
		if err := validateTaskDefinitionEditPhaseCheckpoint(
			frozen.PreparedEdit, checkpoint.name, checkpoint.raw,
			checkpoint.digest, checkpoint.required, checkpoint.phase,
		); err != nil {
			return FrozenTaskDefinitionEditProposal{}, err
		}
	}
	return frozen, nil
}

func validateTaskDefinitionEditPhaseCheckpoint(
	prepared scheduler.PreparedTaskDefinitionEdit,
	name string,
	raw []byte,
	digest string,
	required bool,
	expected scheduler.TaskDefinitionEditPhase,
) error {
	if !required {
		if len(raw) != 0 || digest != "" {
			return invalidDefinitionEditProposal(
				"validate "+name+" checkpoint prefix",
				errors.New("future phase checkpoint is already populated"),
			)
		}
		return nil
	}
	if len(raw) == 0 || !validLowerSHA256(digest) ||
		!definitionEditDigestEqual(digest, sha256Hex(raw)) {
		return invalidDefinitionEditProposal(
			"validate "+name+" checkpoint digest",
			errors.New("checkpoint bytes or digest are invalid"),
		)
	}
	snapshot, err := scheduler.DecodeTaskDefinitionEditPhaseSnapshot(prepared, raw)
	if err != nil {
		return invalidDefinitionEditProposal("decode "+name+" checkpoint", err)
	}
	if snapshot.Phase != expected {
		return invalidDefinitionEditProposal(
			"validate "+name+" checkpoint phase",
			fmt.Errorf("phase %q differs from expected %q", snapshot.Phase, expected),
		)
	}
	return nil
}

func definitionEditCreateParams(
	frozen FrozenTaskDefinitionEditProposal,
) types.CreateTaskDefinitionEditOperationParams {
	return types.CreateTaskDefinitionEditOperationParams{
		CanonicalProposal: bytes.Clone(frozen.CanonicalProposal),
		BaseDefinition:    bytes.Clone(frozen.BaseDefinitionBytes),
		TargetDefinition:  bytes.Clone(frozen.TargetDefinitionBytes),
		PreparedEdit:      bytes.Clone(frozen.PreparedEditBytes),
		BaseSnapshot:      bytes.Clone(frozen.BaseSnapshotBytes),
	}
}

func definitionEditProposalScope(
	frozen FrozenTaskDefinitionEditProposal,
) types.TaskDefinitionEditScope {
	proposal := frozen.Proposal
	return types.TaskDefinitionEditScope{
		ID:             proposal.OperationID,
		TenantID:       proposal.Actor.TenantID,
		UserID:         proposal.Actor.UserID,
		TargetTenantID: proposal.Target.TenantID,
		TargetUserID:   proposal.Target.UserID,
		TaskID:         proposal.Target.TaskID,
	}
}

func definitionEditLeaseScope(
	lease types.TaskDefinitionEditLease,
) types.TaskDefinitionEditScope {
	return types.TaskDefinitionEditScope{
		ID:             lease.ID,
		TenantID:       lease.TenantID,
		UserID:         lease.UserID,
		TargetTenantID: lease.TargetTenantID,
		TargetUserID:   lease.TargetUserID,
		TaskID:         lease.TaskID,
	}
}

func definitionEditOperationMatchesFrozen(
	op *types.TaskDefinitionEditOperation,
	frozen FrozenTaskDefinitionEditProposal,
) bool {
	if op == nil {
		return false
	}
	proposal := frozen.Proposal
	originalStatus := types.ScheduleStatusActive
	if proposal.OriginalStatus == TaskDefinitionEditOriginalStatusV2Paused {
		originalStatus = types.ScheduleStatusPaused
	}
	return op.Scope() == definitionEditProposalScope(frozen) &&
		op.SessionID == proposal.SessionID &&
		op.OperationRef == proposal.OperationRef &&
		op.ExpiresAt.Equal(time.UnixMicro(proposal.ExpiresAtUnixMicros)) &&
		op.OriginalStatus == originalStatus &&
		op.BaseDefinitionVersion == proposal.BaseHead.Version &&
		op.BaseDefinitionDigest == proposal.BaseHead.Digest &&
		op.TargetDefinitionVersion == proposal.TargetHead.Version &&
		op.TargetDefinitionDigest == proposal.TargetHead.Digest &&
		op.ProposalDigest == frozen.ProposalDigest &&
		op.PreparedEditDigest == proposal.PreparedEditDigest &&
		op.BaseSnapshotDigest == proposal.BaseSnapshotDigest &&
		bytes.Equal(op.CanonicalProposal, frozen.CanonicalProposal) &&
		bytes.Equal(op.BaseDefinition, frozen.BaseDefinitionBytes) &&
		bytes.Equal(op.TargetDefinition, frozen.TargetDefinitionBytes) &&
		bytes.Equal(op.PreparedEdit, frozen.PreparedEditBytes) &&
		bytes.Equal(op.BaseSnapshot, frozen.BaseSnapshotBytes)
}

func taskDefinitionEditOperationTerminal(
	status types.TaskDefinitionEditOperationStatus,
) bool {
	switch status {
	case types.TaskDefinitionEditOperationStatusCompleted,
		types.TaskDefinitionEditOperationStatusCancelled,
		types.TaskDefinitionEditOperationStatusExpired,
		types.TaskDefinitionEditOperationStatusBlocked,
		types.TaskDefinitionEditOperationStatusSuperseded:
		return true
	default:
		return false
	}
}

func taskDefinitionEditOutcome(
	op *types.TaskDefinitionEditOperation,
) TaskDefinitionEditOutcome {
	if op == nil {
		return TaskDefinitionEditOutcome{}
	}
	return TaskDefinitionEditOutcome{
		OperationID:  op.ID,
		TaskID:       op.TaskID,
		SessionID:    op.SessionID,
		Status:       op.Status,
		Phase:        op.Phase,
		Recovering:   op.Status == types.TaskDefinitionEditOperationStatusExecuting,
		ReceiptBound: op.ReceiptProvider != "" && op.ReceiptTarget != "",
	}
}
