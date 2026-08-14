package runoutcome

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"

	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
)

const (
	RecoveryInterval     = 30 * time.Second
	RecoveryPageSize     = 100
	RecoveryConcurrency  = 4
	RecoveryQueryTimeout = 5 * time.Second
	RecoveryPassTimeout  = 30 * time.Second
)

type RecoveryStore interface {
	ListStaleRunOutcomeCandidatesV1(
		context.Context, *store.RunOutcomeRecoveryCursorV1, int,
	) ([]store.RunOutcomeRecoveryCandidateV1, error)
	FinalizeRecoveredRunOutcomeClaimV1(
		context.Context, types.RunIdentity, types.RunOutcomeClaimV1,
	) (types.RunOutcomeV1, error)
}

type Execution struct {
	Status enumspb.WorkflowExecutionStatus
	Err    error
}

type Inspector interface {
	Inspect(context.Context, string, string) (Execution, error)
}

type TemporalInspector struct {
	Client client.Client
}

func (i TemporalInspector) Inspect(
	ctx context.Context, workflowID, runID string,
) (Execution, error) {
	if i.Client == nil {
		return Execution{}, errors.New("run outcome Temporal client is nil")
	}
	description, err := i.Client.DescribeWorkflowExecution(
		ctx, workflowID, runID)
	if err != nil {
		return Execution{}, err
	}
	if description == nil || description.WorkflowExecutionInfo == nil {
		return Execution{}, errors.New("run outcome Temporal description is empty")
	}
	status := description.WorkflowExecutionInfo.Status
	execution := Execution{Status: status}
	if status == enumspb.WORKFLOW_EXECUTION_STATUS_FAILED {
		failure := i.Client.GetWorkflow(ctx, workflowID, runID).
			Get(ctx, nil)
		exactFailure, ok := exactWorkflowFailure(failure)
		if !ok {
			// Describe proved the execution is terminal, but a second RPC is
			// still required to obtain its controlled ApplicationError. A
			// timeout/transport error here is not evidence of the workflow's
			// failure semantics and must never be finalized irreversibly.
			return Execution{}, failure
		}
		execution.Err = exactFailure
	}
	return execution, nil
}

func exactWorkflowFailure(err error) (error, bool) {
	var workflowFailure *temporal.WorkflowExecutionError
	return err, err != nil && errors.As(err, &workflowFailure)
}

type Runner struct {
	store     RecoveryStore
	inspector Inspector
	logger    *slog.Logger
	pass      chan struct{}
}

func NewRunner(
	st RecoveryStore, inspector Inspector, logger *slog.Logger,
) (*Runner, error) {
	if st == nil || inspector == nil {
		return nil, errors.New("run outcome recovery dependencies are incomplete")
	}
	if logger == nil {
		logger = slog.Default()
	}
	r := &Runner{
		store: st, inspector: inspector, logger: logger,
		pass: make(chan struct{}, 1),
	}
	r.pass <- struct{}{}
	return r, nil
}

func (r *Runner) RunStartup(ctx context.Context) error {
	return r.runPass(ctx)
}

// Run stops admitting new work as soon as ctx is canceled and returns only
// after every candidate admitted by the current pass has drained.
func (r *Runner) Run(ctx context.Context) {
	timer := time.NewTimer(RecoveryInterval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if err := r.runPass(ctx); err != nil &&
				!errors.Is(err, context.Canceled) {
				r.logger.WarnContext(ctx,
					"run outcome recovery pass failed",
					"error_code", types.CodeOf(err))
			}
			timer.Reset(RecoveryInterval)
		}
	}
}

func (r *Runner) runPass(parent context.Context) error {
	select {
	case <-parent.Done():
		return parent.Err()
	case <-r.pass:
		defer func() { r.pass <- struct{}{} }()
	}
	ctx, cancel := context.WithTimeout(parent, RecoveryPassTimeout)
	defer cancel()
	sem := make(chan struct{}, RecoveryConcurrency)
	var wg sync.WaitGroup
	var errMu sync.Mutex
	var passErrs []error
	recordErr := func(err error) {
		if err == nil {
			return
		}
		errMu.Lock()
		passErrs = append(passErrs, err)
		errMu.Unlock()
	}
	var cursor *store.RunOutcomeRecoveryCursorV1
	for {
		candidates, err := r.store.ListStaleRunOutcomeCandidatesV1(
			ctx, cursor, RecoveryPageSize)
		if err != nil {
			recordErr(err)
			break
		}
		if len(candidates) == 0 {
			break
		}
		for _, candidate := range candidates {
			select {
			case <-ctx.Done():
				recordErr(ctx.Err())
				goto drain
			case sem <- struct{}{}:
			}
			wg.Add(1)
			go func(candidate store.RunOutcomeRecoveryCandidateV1) {
				defer wg.Done()
				defer func() { <-sem }()
				recordErr(r.recoverOne(ctx, candidate))
			}(candidate)
		}
		last := candidates[len(candidates)-1]
		cursor = &store.RunOutcomeRecoveryCursorV1{
			CreatedAt: last.CreatedAt, ID: last.Marker.ID,
		}
		if len(candidates) < RecoveryPageSize {
			break
		}
	}

drain:
	wg.Wait()
	return errors.Join(passErrs...)
}

func (r *Runner) recoverOne(
	passCtx context.Context,
	candidate store.RunOutcomeRecoveryCandidateV1,
) error {
	queryCtx, cancel := context.WithTimeout(
		passCtx, RecoveryQueryTimeout)
	execution, err := r.inspector.Inspect(
		queryCtx,
		candidate.Identity.TemporalWorkflowID,
		candidate.Identity.TemporalRunID)
	cancel()
	if err != nil {
		return err
	}
	claim, terminal := recoveryClaim(candidate.Marker, execution)
	if !terminal {
		return nil
	}
	_, err = r.store.FinalizeRecoveredRunOutcomeClaimV1(
		passCtx, candidate.Identity, claim)
	if errors.Is(err, types.ErrConflict) {
		// A normal workflow finalizer may win after the stale keyset read.
		// Conflict proves the marker is already immutable; recovery has
		// converged even when its inferred claim differs.
		return nil
	}
	return err
}

func recoveryClaim(
	marker types.RunOutcomeMarkerV1, execution Execution,
) (types.RunOutcomeClaimV1, bool) {
	claim := types.RunOutcomeClaimV1{
		RunOutcomeMarkerV1: marker,
		SourceCoverage:     types.RunCompletenessPartial,
		Processing:         types.RunCompletenessPartial,
	}
	switch execution.Status {
	case enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED:
		return types.RunOutcomeClaimV1{}, false
	case enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED:
		claim.Result = types.RunResultFailed
		claim.FailureCode = "outcome_missing_terminal_receipt"
		claim.FailureMessage =
			"workflow completed without a terminal outcome receipt"
	case enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED:
		claim.Result = types.RunResultInterrupted
		claim.FailureCode = "workflow_canceled"
		claim.FailureMessage = "workflow was canceled"
	case enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED:
		claim.Result = types.RunResultInterrupted
		claim.FailureCode = "workflow_terminated"
		claim.FailureMessage = "workflow was terminated"
	case enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
		claim.Result = types.RunResultInterrupted
		claim.FailureCode = "workflow_timed_out"
		claim.FailureMessage = "workflow timed out"
	case enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW:
		claim.Result = types.RunResultInterrupted
		claim.FailureCode = "workflow_continued_as_new"
		claim.FailureMessage = "workflow continued as new"
	case enumspb.WORKFLOW_EXECUTION_STATUS_FAILED:
		claim.Result = types.RunResultFailed
		claim.FailureCode = "workflow_failed"
		claim.FailureMessage =
			"workflow failed before a reliable terminal result"
		var application *temporal.ApplicationError
		if errors.As(execution.Err, &application) &&
			knownFailureCode(application.Type()) {
			claim.FailureCode = application.Type()
			claim.FailureMessage = boundMessage(application.Message())
		}
	default:
		return types.RunOutcomeClaimV1{}, false
	}
	return claim, true
}

func knownFailureCode(code string) bool {
	switch types.ErrCode(code) {
	case types.CodeNotFound, types.CodeConflict, types.CodeValidation,
		types.CodeDatabase, types.CodeInternal, types.CodeLLMRateLimit,
		types.CodeLLMBadRequest, types.CodeLLMUnavailable,
		types.CodeQuotaExceeded, types.CodeFetchTimeout,
		types.CodeFetchRateLimit, types.CodePushFailed,
		types.CodeDBDeadlock, types.CodeDBConnLost, types.CodeDBConstraint:
		return true
	default:
		return false
	}
}

func boundMessage(message string) string {
	const maxBytes = 4096
	if len(message) <= maxBytes {
		return message
	}
	cut := maxBytes
	for cut > 0 && message[cut]&0xc0 == 0x80 {
		cut--
	}
	return message[:cut]
}
