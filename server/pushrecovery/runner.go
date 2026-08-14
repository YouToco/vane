package pushrecovery

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/YouToco/vane/server/pusheffect"
	"github.com/YouToco/vane/server/types"
)

const (
	defaultRecoveryInterval    = time.Minute
	defaultRecoveryPageSize    = 100
	defaultRecoveryConcurrency = 4
	defaultAttemptTimeout      = 90 * time.Second
	defaultPassTimeout         = 2 * time.Minute
	defaultMaxEffectsPerPass   = 1000
	maxRecoveryPageSize        = 1000
	maxRecoveryConcurrency     = 32
	maxRecoveryInterval        = 24 * time.Hour
	maxAttemptTimeout          = 5 * time.Minute
	maxPassTimeout             = 10 * time.Minute
	maxEffectsPerPass          = 10000
)

// DiscoveryStore exposes only task-bound, page-bounded recovery discovery.
// Every returned scope is still reloaded and re-authorized by Coordinator.
type DiscoveryStore interface {
	ReadPushEffectRecoveryCutoff(context.Context) (time.Time, error)
	ListRecoveryTenantCatalogPage(
		context.Context,
		int64,
		int,
	) ([]int64, error)
	ListRecoverablePushEffects(
		context.Context,
		string,
		int64,
		time.Time,
		string,
		int,
	) ([]pusheffect.Effect, error)
}

type Attempter interface {
	Attempt(context.Context, pusheffect.Scope) (Outcome, error)
}

type RunnerConfig struct {
	ExactTaskID    string
	Interval       time.Duration
	PageSize       int
	MaxConcurrent  int
	AttemptTimeout time.Duration
	// PassTimeout bounds discovery and new admission. Work admitted before
	// that deadline retains its full AttemptTimeout so a provider result can
	// reach a durable checkpoint; total pass time is therefore bounded by
	// PassTimeout + AttemptTimeout.
	PassTimeout time.Duration
	MaxEffects  int
}

type RunnerDeps struct {
	Store       DiscoveryStore
	Coordinator Attempter
	Config      RunnerConfig
	Logger      *slog.Logger
}

type Runner struct {
	store       DiscoveryStore
	coordinator Attempter
	config      RunnerConfig
	logger      *slog.Logger
	stats       runnerStats
	passToken   chan struct{}
	cursorMu    sync.Mutex
	cursor      recoveryCursor
}

type recoveryCursor struct {
	TenantID int64
	EffectID string
}

type runnerStats struct {
	passes        atomic.Uint64
	limitedPasses atomic.Uint64
	passErrors    atomic.Uint64
	attempts      atomic.Uint64
	attemptErrors atomic.Uint64
	ignored       atomic.Uint64
	sent          atomic.Uint64
	definiteFail  atomic.Uint64
	ambiguous     atomic.Uint64
	blocked       atomic.Uint64
	deferred      atomic.Uint64
	unauthorized  atomic.Uint64
}

// RunnerSnapshot is a low-cardinality process metric snapshot. It deliberately
// contains no tenant, user, task, recipient, card, message, or provider UUID.
type RunnerSnapshot struct {
	Passes        uint64
	LimitedPasses uint64
	PassErrors    uint64
	Attempts      uint64
	AttemptErrors uint64
	Ignored       uint64
	Sent          uint64
	DefiniteFail  uint64
	Ambiguous     uint64
	Blocked       uint64
	Deferred      uint64
	Unauthorized  uint64
}

func NewRunner(deps RunnerDeps) (*Runner, error) {
	if deps.Store == nil || deps.Coordinator == nil ||
		!validExactTaskID(deps.Config.ExactTaskID) {
		return nil, ErrDependencies
	}
	if deps.Config.Interval <= 0 {
		deps.Config.Interval = defaultRecoveryInterval
	} else if deps.Config.Interval > maxRecoveryInterval {
		return nil, ErrDependencies
	}
	if deps.Config.PageSize <= 0 {
		deps.Config.PageSize = defaultRecoveryPageSize
	} else if deps.Config.PageSize > maxRecoveryPageSize {
		return nil, ErrDependencies
	}
	if deps.Config.MaxConcurrent <= 0 {
		deps.Config.MaxConcurrent = defaultRecoveryConcurrency
	} else if deps.Config.MaxConcurrent > maxRecoveryConcurrency {
		return nil, ErrDependencies
	}
	if deps.Config.AttemptTimeout <= 0 {
		deps.Config.AttemptTimeout = defaultAttemptTimeout
	} else if deps.Config.AttemptTimeout > maxAttemptTimeout {
		return nil, ErrDependencies
	}
	if deps.Config.PassTimeout <= 0 {
		deps.Config.PassTimeout = defaultPassTimeout
	} else if deps.Config.PassTimeout > maxPassTimeout {
		return nil, ErrDependencies
	}
	if deps.Config.MaxEffects <= 0 {
		deps.Config.MaxEffects = defaultMaxEffectsPerPass
	} else if deps.Config.MaxEffects > maxEffectsPerPass {
		return nil, ErrDependencies
	}
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	runner := &Runner{
		store: deps.Store, coordinator: deps.Coordinator,
		config: deps.Config, logger: deps.Logger,
		passToken: make(chan struct{}, 1),
	}
	runner.passToken <- struct{}{}
	return runner, nil
}

// RunStartup completes one full bounded discovery pass. The server calls this
// synchronously after outbound-only provider preparation and before any
// Temporal, Feishu, A2A, or HTTP ingress is admitted.
func (r *Runner) RunStartup(ctx context.Context) error {
	return r.runPass(ctx, "startup")
}

// Run performs periodic recovery until ctx is canceled. The caller owns the
// goroutine and must wait for this method to return before releasing Store or
// provider dependencies.
func (r *Runner) Run(ctx context.Context) {
	if r == nil {
		return
	}
	timer := time.NewTimer(r.config.Interval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if err := r.runPass(ctx, "periodic"); err != nil &&
				!errors.Is(err, context.Canceled) {
				r.logger.WarnContext(
					ctx,
					"push effect recovery pass failed",
					"trigger", "periodic",
					"error_code", types.CodeOf(err),
				)
			}
			timer.Reset(r.config.Interval)
		}
	}
}

func (r *Runner) Snapshot() RunnerSnapshot {
	if r == nil {
		return RunnerSnapshot{}
	}
	return RunnerSnapshot{
		Passes:        r.stats.passes.Load(),
		LimitedPasses: r.stats.limitedPasses.Load(),
		PassErrors:    r.stats.passErrors.Load(),
		Attempts:      r.stats.attempts.Load(),
		AttemptErrors: r.stats.attemptErrors.Load(),
		Ignored:       r.stats.ignored.Load(),
		Sent:          r.stats.sent.Load(),
		DefiniteFail:  r.stats.definiteFail.Load(),
		Ambiguous:     r.stats.ambiguous.Load(),
		Blocked:       r.stats.blocked.Load(),
		Deferred:      r.stats.deferred.Load(),
		Unauthorized:  r.stats.unauthorized.Load(),
	}
}

func (r *Runner) runPass(ctx context.Context, trigger string) error {
	if r == nil || r.store == nil || r.coordinator == nil {
		return ErrDependencies
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-r.passToken:
		defer func() { r.passToken <- struct{}{} }()
	case <-ctx.Done():
		return ctx.Err()
	}
	passCtx, cancelPass := context.WithTimeout(ctx, r.config.PassTimeout)
	defer cancelPass()
	r.stats.passes.Add(1)
	before, err := r.store.ReadPushEffectRecoveryCutoff(passCtx)
	if err != nil {
		r.stats.passErrors.Add(1)
		return err
	}
	sem := make(chan struct{}, r.config.MaxConcurrent)
	var (
		wg       sync.WaitGroup
		errMu    sync.Mutex
		passErrs []error
	)
	recordErr := func(err error) {
		if err == nil {
			return
		}
		errMu.Lock()
		passErrs = append(passErrs, err)
		errMu.Unlock()
	}
	scheduled := 0
	schedule := func(scope pusheffect.Scope) error {
		if scheduled >= r.config.MaxEffects {
			return nil
		}
		if err := passCtx.Err(); err != nil {
			return err
		}
		select {
		case sem <- struct{}{}:
		case <-passCtx.Done():
			return passCtx.Err()
		}
		if err := passCtx.Err(); err != nil {
			<-sem
			return err
		}
		scheduled++
		wg.Go(func() {
			defer func() { <-sem }()
			attemptCtx, cancel := context.WithTimeout(
				ctx, r.config.AttemptTimeout)
			outcome, err := r.coordinator.Attempt(attemptCtx, scope)
			cancel()
			r.stats.attempts.Add(1)
			validOutcome := false
			if outcome != "" {
				validOutcome = r.recordOutcome(outcome)
				if !validOutcome {
					recordErr(errors.New(
						"push effect recovery outcome is invalid"))
				}
			} else if err == nil {
				recordErr(errors.New(
					"push effect recovery outcome is empty"))
			}
			if err != nil {
				r.stats.attemptErrors.Add(1)
				r.logger.WarnContext(
					passCtx,
					"push effect recovery attempt failed",
					"trigger", trigger,
					"error_code", types.CodeOf(err),
				)
				// A typed outcome proves the provider boundary has already
				// converged to a durable checkpoint. Count/log the provider
				// error, but fail the pass only when no valid checkpoint
				// outcome exists.
				if !validOutcome {
					recordErr(err)
				}
			} else if !validOutcome {
				r.stats.attemptErrors.Add(1)
			}
		})
		return nil
	}

	r.cursorMu.Lock()
	cursor := r.cursor
	r.cursorMu.Unlock()
	nextCursor := cursor
	limited := false
	setCursor := func(value recoveryCursor) {
		nextCursor = value
	}
	scanTenant := func(
		tenantID int64,
		afterEffectID string,
	) (bool, error) {
		for {
			effects, err := r.store.ListRecoverablePushEffects(
				passCtx, r.config.ExactTaskID, tenantID, before,
				afterEffectID, r.config.PageSize,
			)
			if err != nil {
				return false, err
			}
			for i := range effects {
				if scheduled >= r.config.MaxEffects {
					limited = true
					setCursor(recoveryCursor{
						TenantID: tenantID, EffectID: afterEffectID})
					return false, nil
				}
				if err := schedule(effects[i].Scope()); err != nil {
					setCursor(recoveryCursor{
						TenantID: tenantID, EffectID: afterEffectID})
					return false, err
				}
				afterEffectID = effects[i].ID
				setCursor(recoveryCursor{
					TenantID: tenantID, EffectID: afterEffectID})
			}
			if len(effects) < r.config.PageSize {
				return true, nil
			}
		}
	}

	var afterTenantID int64
	if cursor.TenantID > 0 {
		finished, err := scanTenant(cursor.TenantID, cursor.EffectID)
		if err != nil {
			recordErr(err)
			goto waitAttempts
		}
		if !finished {
			goto waitAttempts
		}
		afterTenantID = cursor.TenantID
	}
scanTenants:
	for {
		tenantIDs, err := r.store.ListRecoveryTenantCatalogPage(
			passCtx, afterTenantID, r.config.PageSize,
		)
		if err != nil {
			recordErr(err)
			break
		}
		for _, tenantID := range tenantIDs {
			setCursor(recoveryCursor{TenantID: tenantID})
			finished, err := scanTenant(tenantID, "")
			if err != nil {
				recordErr(err)
				break scanTenants
			}
			if !finished {
				break scanTenants
			}
			afterTenantID = tenantID
		}
		if len(tenantIDs) < r.config.PageSize {
			setCursor(recoveryCursor{})
			break
		}
		afterTenantID = tenantIDs[len(tenantIDs)-1]
	}
waitAttempts:
	wg.Wait()
	r.cursorMu.Lock()
	r.cursor = nextCursor
	r.cursorMu.Unlock()
	if limited {
		r.stats.limitedPasses.Add(1)
	}
	if passCtx.Err() != nil {
		recordErr(passCtx.Err())
	}
	errMu.Lock()
	passErr := errors.Join(passErrs...)
	errMu.Unlock()
	if passErr != nil {
		r.stats.passErrors.Add(1)
	}
	snapshot := r.Snapshot()
	r.logger.InfoContext(
		ctx,
		"push effect recovery pass completed",
		"trigger", trigger,
		"limited", limited,
		"attempts_total", snapshot.Attempts,
		"attempt_errors_total", snapshot.AttemptErrors,
		"pass_errors_total", snapshot.PassErrors,
		"sent_total", snapshot.Sent,
		"definite_failed_total", snapshot.DefiniteFail,
		"ambiguous_total", snapshot.Ambiguous,
		"blocked_total", snapshot.Blocked,
		"deferred_total", snapshot.Deferred,
		"not_authorized_total", snapshot.Unauthorized,
	)
	return passErr
}

func (r *Runner) recordOutcome(outcome Outcome) bool {
	switch outcome {
	case OutcomeIgnored:
		r.stats.ignored.Add(1)
	case OutcomeSent:
		r.stats.sent.Add(1)
	case OutcomeDefiniteFail:
		r.stats.definiteFail.Add(1)
	case OutcomeAmbiguous:
		r.stats.ambiguous.Add(1)
	case OutcomeBlocked:
		r.stats.blocked.Add(1)
	case OutcomeDeferred:
		r.stats.deferred.Add(1)
	case OutcomeNotAuthorized:
		r.stats.unauthorized.Add(1)
	default:
		return false
	}
	return true
}
