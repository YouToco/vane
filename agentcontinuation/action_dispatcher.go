package agentcontinuation

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/store"
)

const (
	actionPollInterval   = 2 * time.Second
	actionLeaseDuration  = time.Minute
	actionTenantPageSize = 100
	actionPageSize       = 64
	actionConcurrency    = 4
	actionReleaseTimeout = 5 * time.Second
)

// actionDispatcherStore is the exact-action durable execution boundary. Its Store
// implementation independently verifies execution_version=2, the generation-1
// durable authority event, frozen enable_source bytes, and confirmed status at
// acquisition and again in the effect transaction.
type actionDispatcherStore interface {
	ListDueAgentActionContinuationTenantIDs(
		context.Context, time.Time, int64, int,
	) ([]int64, error)
	ListDueAgentActionContinuations(
		context.Context, int64, time.Time, int,
	) ([]store.AgentActionContinuation, error)
	AcquireAgentActionContinuation(
		context.Context, string, int64, int64, string, time.Duration,
	) (*store.AgentActionContinuation, error)
	ProjectAgentActionContinuation(
		context.Context, store.AgentActionContinuationLease,
	) error
	ReleaseAgentActionContinuation(
		context.Context, store.AgentActionContinuationLease, time.Duration,
	) error
}

// ActionDispatcher converges explicitly activated, confirmed v2 actions. It
// owns one stable process identity, bounded admission, and an explicit
// Stop/Wait lifecycle so Store cannot close while an admitted effect is live.
type ActionDispatcher struct {
	store  actionDispatcherStore
	logger *slog.Logger
	owner  string

	dispatchMu sync.Mutex
	cursor     int64

	lifecycleMu sync.Mutex
	cancel      context.CancelFunc
	done        chan struct{}
	started     bool
}

func NewActionDispatcher(
	st actionDispatcherStore,
	logger *slog.Logger,
) (*ActionDispatcher, error) {
	if st == nil {
		return nil, errors.New(
			"agentcontinuation: action dispatcher Store is required")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &ActionDispatcher{
		store:  st,
		logger: logger,
		owner:  "agent-action-dispatcher-" + uuid.NewString(),
	}, nil
}

// Start begins with an immediate bounded pass, then polls every two seconds.
// It may be called exactly once; Stop is the only admission-closing operation.
func (d *ActionDispatcher) Start(parent context.Context) error {
	if d == nil || d.store == nil {
		return errors.New(
			"agentcontinuation: action dispatcher Store is required")
	}
	if parent == nil {
		return errors.New(
			"agentcontinuation: action dispatcher context is required")
	}
	d.lifecycleMu.Lock()
	defer d.lifecycleMu.Unlock()
	if d.started {
		return errors.New(
			"agentcontinuation: action dispatcher already started")
	}
	ctx, cancel := context.WithCancel(parent)
	d.cancel = cancel
	d.done = make(chan struct{})
	d.started = true
	go d.run(ctx, d.done)
	return nil
}

// Stop closes new admission. Already admitted projections are allowed to
// finish; Wait proves their completion.
func (d *ActionDispatcher) Stop() {
	if d == nil {
		return
	}
	d.lifecycleMu.Lock()
	cancel := d.cancel
	d.lifecycleMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// Wait blocks until the dispatch loop and every admitted projection have
// exited. The caller controls only the wait budget, not dispatcher lifetime.
func (d *ActionDispatcher) Wait(ctx context.Context) error {
	if d == nil {
		return errors.New(
			"agentcontinuation: action dispatcher is required")
	}
	if ctx == nil {
		return errors.New(
			"agentcontinuation: action dispatcher wait context is required")
	}
	d.lifecycleMu.Lock()
	done := d.done
	started := d.started
	d.lifecycleMu.Unlock()
	if !started || done == nil {
		return errors.New(
			"agentcontinuation: action dispatcher is not started")
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (d *ActionDispatcher) run(ctx context.Context, done chan struct{}) {
	defer close(done)
	d.dispatchAndLog(ctx)
	ticker := time.NewTicker(actionPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.dispatchAndLog(ctx)
		}
	}
}

func (d *ActionDispatcher) dispatchAndLog(ctx context.Context) {
	if err := d.DispatchOnce(ctx); err != nil &&
		!errors.Is(err, context.Canceled) {
		d.logger.ErrorContext(
			ctx, "agent action continuation dispatch pass failed", "err", err)
	}
}

// DispatchOnce drains one bounded keyset page. The Store is the enforcement
// point for exact execution version, authority generation, route, frozen
// adapter, and status; this layer never calls Tool.Execute or discovers a
// provider, Temporal run, or current Agent session.
func (d *ActionDispatcher) DispatchOnce(ctx context.Context) error {
	if d == nil || d.store == nil {
		return errors.New(
			"agentcontinuation: action dispatcher Store is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	d.dispatchMu.Lock()
	defer d.dispatchMu.Unlock()

	boundary := time.Now().Add(24 * time.Hour)
	tenantIDs, err := d.store.ListDueAgentActionContinuationTenantIDs(
		ctx, boundary, d.cursor, actionTenantPageSize)
	if err != nil {
		return fmt.Errorf("list action continuation tenant shards: %w", err)
	}
	if len(tenantIDs) == 0 && d.cursor > 0 {
		tenantIDs, err = d.store.ListDueAgentActionContinuationTenantIDs(
			ctx, boundary, 0, actionTenantPageSize)
		if err != nil {
			return fmt.Errorf(
				"wrap action continuation tenant shards: %w", err)
		}
	}
	if len(tenantIDs) > 0 {
		d.cursor = tenantIDs[len(tenantIDs)-1]
	}

	semaphore := make(chan struct{}, actionConcurrency)
	var wait sync.WaitGroup
	var errorsMu sync.Mutex
	var dispatchErrors []error
	appendError := func(err error) {
		errorsMu.Lock()
		dispatchErrors = append(dispatchErrors, err)
		errorsMu.Unlock()
	}
	for _, tenantID := range tenantIDs {
		actions, listErr := d.store.ListDueAgentActionContinuations(
			ctx, tenantID, boundary, actionPageSize)
		if listErr != nil {
			appendError(fmt.Errorf(
				"list tenant %d action continuations: %w",
				tenantID, listErr))
			continue
		}
		for i := range actions {
			action := actions[i]
			select {
			case semaphore <- struct{}{}:
			case <-ctx.Done():
				wait.Wait()
				appendError(ctx.Err())
				return errors.Join(dispatchErrors...)
			}
			wait.Add(1)
			go func() {
				defer wait.Done()
				defer func() { <-semaphore }()
				if err := d.dispatchAction(ctx, action); err != nil &&
					!errors.Is(err, store.ErrAgentActionBusy) &&
					!errors.Is(err, store.ErrAgentActionTerminal) {
					appendError(fmt.Errorf(
						"action continuation %s: %w",
						action.ActionID, err))
				}
			}()
		}
	}
	wait.Wait()
	return errors.Join(dispatchErrors...)
}

func (d *ActionDispatcher) dispatchAction(
	ctx context.Context,
	listed store.AgentActionContinuation,
) error {
	action, err := d.store.AcquireAgentActionContinuation(
		ctx, listed.ActionID, listed.TenantID, listed.UserID,
		d.owner, actionLeaseDuration)
	if err != nil {
		return err
	}
	if action == nil {
		return errors.New(
			"agentcontinuation: action acquisition returned no action")
	}
	lease, err := action.Lease()
	if err != nil {
		return err
	}
	if err := d.store.ProjectAgentActionContinuation(ctx, lease); err != nil {
		if errors.Is(err, store.ErrAgentActionBusy) ||
			errors.Is(err, store.ErrAgentActionTerminal) {
			return err
		}
		releaseCtx, cancel := context.WithTimeout(
			context.WithoutCancel(ctx), actionReleaseTimeout)
		defer cancel()
		releaseErr := d.store.ReleaseAgentActionContinuation(
			releaseCtx, lease, actionRetryBackoff(int64(action.AttemptCount)))
		if releaseErr != nil {
			return errors.Join(
				err,
				fmt.Errorf(
					"checkpoint action continuation retry: %w",
					releaseErr),
			)
		}
		return err
	}
	return nil
}

func actionRetryBackoff(attempt int64) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if attempt > 9 {
		attempt = 9
	}
	delay := 5 * time.Second * time.Duration(1<<(attempt-1))
	if delay > 15*time.Minute {
		return 15 * time.Minute
	}
	return delay
}
