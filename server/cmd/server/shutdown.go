package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/YouToco/vane/server/internal/releaseinfo"
	"go.temporal.io/sdk/worker"
)

const (
	// The first HTTP Shutdown closes listener admission immediately and waits a
	// short period. Long-lived handlers are drained by their owning subsystem;
	// a second call then proves no unowned handler remains.
	httpInitialShutdownTimeout = 5 * time.Second
	httpDrainProofTimeout      = 5 * time.Second
	// assistant.chat has a 120s total budget, then may finish a 10s LLM ledger
	// tail. Keep another 20s for SDK event processing and the durable terminal
	// task-store update instead of balancing exactly on the two known budgets.
	a2aShutdownTimeout = 150 * time.Second
	// Deep-dive owns a 150s generation budget, followed by a bounded 10s LLM
	// accounting tail and delivery bookkeeping.
	feedbackShutdownTimeout = 170 * time.Second
	// Startup orphan reconciliation is best-effort and must not prevent the
	// health endpoint from coming up indefinitely when PostgreSQL stalls.
	a2aStartupCleanupTimeout = 5 * time.Second
	// Pipeline Activities have a 120s StartToCloseTimeout. Temporal's zero-value
	// WorkerStopTimeout is 0s, so it must be set explicitly with ledger tailroom.
	temporalWorkerStopTimeout = 150 * time.Second
)

func temporalWorkerOptions() worker.Options {
	return worker.Options{
		WorkerStopTimeout: temporalWorkerStopTimeout,
		// BuildID is recorded in WorkflowTask history even though routing remains
		// unversioned. The Agent-first retention audit binds its clock witness to
		// this immutable VCS revision; an unstamped development binary can still
		// run normal workflows but can never satisfy the production audit.
		BuildID: temporalWorkerBuildID(),
	}
}

func temporalWorkerBuildID() string {
	return temporalWorkerBuildIDForRevision(releaseinfo.Revision())
}

func temporalWorkerBuildIDForRevision(revision string, ok bool) string {
	if !ok {
		return "vane/development"
	}
	return "vane/" + revision
}

func beginHTTPShutdown(srv *http.Server, timeout time.Duration) <-chan error {
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		done <- srv.Shutdown(ctx)
	}()
	return done
}

// completeHTTPShutdown accepts an initial timeout when subsystem drains have
// since made progress, but requires a fresh, bounded proof that no handler is
// still running. The initial attempt is still included when the proof fails so
// operators can distinguish a continuously stuck handler from a late race.
func completeHTTPShutdown(srv *http.Server, initial <-chan error, proofTimeout time.Duration) error {
	initialErr := <-initial
	if initialErr == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), proofTimeout)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		return errors.Join(
			fmt.Errorf("initial HTTP shutdown: %w", initialErr),
			fmt.Errorf("final HTTP drain proof: %w", err),
		)
	}
	return nil
}

// releaseAfterSafeDrain makes dependency release conditional on affirmative
// proof that every ingress/background owner stopped. Returning from main will
// still terminate a broken process, but it will not create an avoidable window
// where live goroutines race explicitly closed clients or connection pools.
func releaseAfterSafeDrain(drainErr error, release func()) error {
	if drainErr != nil {
		return drainErr
	}
	release()
	return nil
}
