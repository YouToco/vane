// Package agentcontinuation projects durable business facts into their frozen
// exact Agent sessions. It has no provider dependency and cannot rediscover a
// session at dispatch time.
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
	pollInterval   = 2 * time.Second
	leaseDuration  = time.Minute
	tenantPageSize = 100
	factPageSize   = 64
	concurrency    = 4
)

type Store interface {
	ListRecoveryTenantCatalogPage(
		context.Context, int64, int,
	) ([]int64, error)
	ListDueAgentSessionFacts(
		context.Context, int64, time.Time, int,
	) ([]store.AgentSessionFact, error)
	AcquireAgentSessionFact(
		context.Context, store.AcquireAgentSessionFactParams,
	) (*store.AgentSessionFact, error)
	ProjectAgentSessionFact(
		context.Context, store.AgentSessionFactLease,
	) error
	ReleaseAgentSessionFact(
		context.Context, store.AgentSessionFactLease, time.Duration,
	) error
}

type Dispatcher struct {
	store  Store
	logger *slog.Logger

	dispatchMu sync.Mutex
	cursor     int64
}

func New(st Store, logger *slog.Logger) (*Dispatcher, error) {
	if st == nil {
		return nil, errors.New(
			"agentcontinuation: dispatcher Store is required")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Dispatcher{store: st, logger: logger}, nil
}

// Run performs a startup scan before polling. Cancellation stops admission;
// DispatchOnce waits for every admitted projection before returning.
func (d *Dispatcher) Run(ctx context.Context) {
	if d == nil {
		return
	}
	d.dispatchAndLog(ctx)
	ticker := time.NewTicker(pollInterval)
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

func (d *Dispatcher) dispatchAndLog(ctx context.Context) {
	if err := d.DispatchOnce(ctx); err != nil &&
		!errors.Is(err, context.Canceled) {
		d.logger.ErrorContext(
			ctx, "agent continuation dispatch pass failed", "err", err)
	}
}

func (d *Dispatcher) DispatchOnce(ctx context.Context) error {
	if d == nil || d.store == nil {
		return errors.New(
			"agentcontinuation: dispatcher Store is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	d.dispatchMu.Lock()
	defer d.dispatchMu.Unlock()

	boundary := time.Now().Add(24 * time.Hour)
	tenantIDs, err := d.store.ListRecoveryTenantCatalogPage(
		ctx, d.cursor, tenantPageSize)
	if err != nil {
		return fmt.Errorf("list continuation tenant shards: %w", err)
	}
	if len(tenantIDs) == 0 && d.cursor > 0 {
		tenantIDs, err = d.store.ListRecoveryTenantCatalogPage(
			ctx, 0, tenantPageSize)
		if err != nil {
			return fmt.Errorf("wrap continuation tenant shards: %w", err)
		}
	}
	if len(tenantIDs) > 0 {
		d.cursor = tenantIDs[len(tenantIDs)-1]
	}

	semaphore := make(chan struct{}, concurrency)
	var wait sync.WaitGroup
	var errorsMu sync.Mutex
	var dispatchErrors []error
	appendError := func(err error) {
		errorsMu.Lock()
		dispatchErrors = append(dispatchErrors, err)
		errorsMu.Unlock()
	}
	for _, tenantID := range tenantIDs {
		facts, listErr := d.store.ListDueAgentSessionFacts(
			ctx, tenantID, boundary, factPageSize)
		if listErr != nil {
			appendError(fmt.Errorf(
				"list tenant %d continuation facts: %w",
				tenantID, listErr))
			continue
		}
		for i := range facts {
			fact := facts[i]
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
				if err := d.dispatchFact(ctx, fact); err != nil &&
					!errors.Is(err, store.ErrAgentSessionFactBusy) &&
					!errors.Is(err, store.ErrAgentSessionFactTerminal) {
					appendError(fmt.Errorf(
						"continuation fact %d: %w", fact.ID, err))
				}
			}()
		}
	}
	wait.Wait()
	return errors.Join(dispatchErrors...)
}

func (d *Dispatcher) dispatchFact(
	ctx context.Context,
	listed store.AgentSessionFact,
) error {
	owner := "agent-continuation-" + uuid.NewString()
	fact, err := d.store.AcquireAgentSessionFact(
		ctx, store.AcquireAgentSessionFactParams{
			ID: listed.ID, TenantID: listed.TenantID,
			UserID: listed.UserID, LeaseOwner: owner,
			LeaseDuration: leaseDuration,
		})
	if err != nil {
		return err
	}
	lease, err := fact.Lease()
	if err != nil {
		// Corrupt durable input is handled by ProjectAgentSessionFact after the
		// row is locked and can be checkpointed blocked. A structurally
		// impossible lease cannot safely identify that checkpoint.
		return err
	}
	if err := d.store.ProjectAgentSessionFact(ctx, lease); err != nil {
		if errors.Is(err, store.ErrAgentSessionFactBusy) ||
			errors.Is(err, store.ErrAgentSessionFactTerminal) {
			return err
		}
		checkpointCtx, cancel := context.WithTimeout(
			context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		releaseErr := d.store.ReleaseAgentSessionFact(
			checkpointCtx, lease, retryBackoff(int64(fact.AttemptCount)))
		if releaseErr != nil {
			return errors.Join(
				err, fmt.Errorf("checkpoint continuation retry: %w", releaseErr))
		}
		return err
	}
	return nil
}

func retryBackoff(attempt int64) time.Duration {
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
