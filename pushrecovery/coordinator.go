// Package pushrecovery contains the dark, provider-effect recovery
// coordinator. It deliberately has no production composition-root call point.
package pushrecovery

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/pusheffect"
)

const (
	defaultRecoveryInterval    = 15 * time.Second
	defaultPassTimeout         = 90 * time.Second
	defaultAttemptTimeout      = 30 * time.Second
	defaultLeaseDuration       = time.Minute
	defaultRetryAfter          = 30 * time.Second
	defaultTenantLimit         = 100
	defaultPerTenantLimit      = 4
	defaultPassLimit           = 64
	defaultConcurrency         = 4
	defaultMaxAttempts         = 8
	defaultHistoryBoundarySkew = time.Minute
)

var ErrDependencies = errors.New(
	"push recovery: dependencies are incomplete")

// Store is intentionally ahead of *store.Store. The authorized claim and
// predicate-constrained block methods require the later restricted-role
// migration and have no concrete production implementation in this train.
// Keeping them in this dark interface makes provider Create unreachable until
// that authority lands; fakes may implement them to verify the state machine.
type Store interface {
	ListRecoverablePushEffectTenantIDs(
		context.Context,
		time.Time,
		int64,
		int,
	) ([]int64, error)
	ListRecoverablePushEffects(
		context.Context,
		int64,
		time.Time,
		int,
	) ([]pusheffect.Effect, error)
	TakeOverStalePushEffect(
		context.Context,
		pusheffect.Scope,
	) (*pusheffect.Effect, error)

	// AuthorizePushEffectRunSideEffect is only a fail-closed preflight. The
	// following claim methods must repeat the exact authorization atomically
	// with their fenced state transition.
	AuthorizePushEffectRunSideEffect(
		context.Context,
		pusheffect.Scope,
	) (bool, error)
	ClaimAuthorizedPushEffect(
		context.Context,
		pusheffect.ClaimParams,
	) (*pusheffect.Effect, error)
	ClaimAuthorizedPushEffectReconciliation(
		context.Context,
		pusheffect.ClaimParams,
	) (*pusheffect.Effect, error)

	RecordPushEffectDefiniteFailure(
		context.Context,
		pusheffect.FailureParams,
	) error
	RecordPushEffectAmbiguous(
		context.Context,
		pusheffect.FailureParams,
	) error
	RecordPushEffectSentWithDeliveries(
		context.Context,
		pusheffect.SentReceipt,
	) error
	DeferPushEffectReconciliation(
		context.Context,
		pusheffect.Resolution,
		time.Duration,
	) error
	DeferPushEffectReconciliationUntilExpiry(
		context.Context,
		pusheffect.Resolution,
	) error

	// These are not aliases for the existing generic operator transition.
	// Their future SQL must enforce the expiry/conflict predicate and expected
	// fence inside the restricted transaction.
	BlockExpiredPushEffect(
		context.Context,
		pusheffect.Resolution,
	) error
	BlockConflictingPushEffectHistory(
		context.Context,
		pusheffect.Resolution,
	) error
}

type Provider interface {
	PushWithUUID(
		context.Context,
		string,
		string,
		string,
		string,
	) (pusheffect.ProviderObservation, error)
	ResolvePushEffectMessage(
		context.Context,
		pusheffect.HistoryQuery,
	) (pusheffect.HistoryObservation, error)
}

type Config struct {
	RecoveryInterval time.Duration
	PassTimeout      time.Duration
	AttemptTimeout   time.Duration
	LeaseDuration    time.Duration
	RetryAfter       time.Duration
	TenantLimit      int
	PerTenantLimit   int
	PassLimit        int
	Concurrency      int
	MaxAttempts      int
	HistorySkew      time.Duration
}

func (c Config) withDefaults() Config {
	if c.RecoveryInterval <= 0 {
		c.RecoveryInterval = defaultRecoveryInterval
	}
	if c.PassTimeout <= 0 {
		c.PassTimeout = defaultPassTimeout
	}
	if c.AttemptTimeout <= 0 {
		c.AttemptTimeout = defaultAttemptTimeout
	}
	if c.LeaseDuration <= 0 {
		c.LeaseDuration = defaultLeaseDuration
	}
	if c.RetryAfter <= 0 {
		c.RetryAfter = defaultRetryAfter
	}
	if c.TenantLimit <= 0 {
		c.TenantLimit = defaultTenantLimit
	}
	if c.PerTenantLimit <= 0 {
		c.PerTenantLimit = defaultPerTenantLimit
	}
	if c.PassLimit <= 0 {
		c.PassLimit = defaultPassLimit
	}
	if c.Concurrency <= 0 {
		c.Concurrency = defaultConcurrency
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = defaultMaxAttempts
	}
	if c.HistorySkew <= 0 {
		c.HistorySkew = defaultHistoryBoundarySkew
	}
	return c
}

type Deps struct {
	Store    Store
	Provider Provider
	Logger   *slog.Logger
	Config   Config
}

type Coordinator struct {
	store    Store
	provider Provider
	logger   *slog.Logger
	config   Config
	now      func() time.Time

	passMu sync.Mutex
	cursor int64
}

func New(d Deps) (*Coordinator, error) {
	if d.Store == nil || d.Provider == nil {
		return nil, ErrDependencies
	}
	if d.Logger == nil {
		d.Logger = slog.Default()
	}
	return &Coordinator{
		store: d.Store, provider: d.Provider, logger: d.Logger,
		config: d.Config.withDefaults(), now: time.Now,
	}, nil
}

type Outcome string

const (
	OutcomeIgnored       Outcome = "ignored"
	OutcomeSent          Outcome = "sent"
	OutcomeDefiniteFail  Outcome = "definite_failed"
	OutcomeAmbiguous     Outcome = "ambiguous"
	OutcomeBlocked       Outcome = "blocked"
	OutcomeDeferred      Outcome = "deferred"
	OutcomeNotAuthorized Outcome = "not_authorized"
)

// Run performs one startup pass and then serialized periodic passes. This
// method remains dark until the restricted transition interface is backed by
// the later migration and explicitly wired by the composition root.
func (c *Coordinator) Run(ctx context.Context) {
	if c == nil {
		return
	}
	c.recoverAndLog(ctx)
	ticker := time.NewTicker(c.config.RecoveryInterval)
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

func (c *Coordinator) recoverAndLog(ctx context.Context) {
	if err := c.RecoverOnce(ctx); err != nil &&
		!errors.Is(err, context.Canceled) {
		c.logger.ErrorContext(ctx, "push effect recovery pass failed",
			"err", err)
	}
}

// RecoverOnce scans one tenant-keyset page, caps per-tenant and global work,
// and waits for every bounded attempt before returning.
func (c *Coordinator) RecoverOnce(ctx context.Context) error {
	if c == nil || c.store == nil || c.provider == nil {
		return ErrDependencies
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c.passMu.Lock()
	defer c.passMu.Unlock()

	passCtx, cancel := context.WithTimeout(ctx, c.config.PassTimeout)
	defer cancel()
	// Store predicates clamp this deliberately future process boundary to the
	// PostgreSQL clock. Process-clock skew therefore cannot authorize early
	// lease takeover or retry.
	boundary := c.now().Add(24 * time.Hour)
	tenants, err := c.store.ListRecoverablePushEffectTenantIDs(
		passCtx,
		boundary,
		c.cursor,
		c.config.TenantLimit,
	)
	if err != nil {
		return fmt.Errorf("list push effect tenant shards: %w", err)
	}
	if c.cursor > 0 && len(tenants) < c.config.TenantLimit {
		wrapped, wrapErr := c.store.ListRecoverablePushEffectTenantIDs(
			passCtx,
			boundary,
			0,
			c.config.TenantLimit-len(tenants),
		)
		if wrapErr != nil {
			return fmt.Errorf("wrap push effect tenant shards: %w", wrapErr)
		}
		seen := make(map[int64]struct{}, len(tenants)+len(wrapped))
		for _, tenantID := range tenants {
			seen[tenantID] = struct{}{}
		}
		for _, tenantID := range wrapped {
			if _, duplicate := seen[tenantID]; duplicate {
				continue
			}
			seen[tenantID] = struct{}{}
			tenants = append(tenants, tenantID)
		}
	}

	effects := make([]pusheffect.Effect, 0, c.config.PassLimit)
	var recoveryErrors []error
	for _, tenantID := range tenants {
		if len(effects) >= c.config.PassLimit {
			break
		}
		c.cursor = tenantID
		remaining := c.config.PassLimit - len(effects)
		limit := min(c.config.PerTenantLimit, remaining)
		shard, shardErr := c.store.ListRecoverablePushEffects(
			passCtx,
			tenantID,
			boundary,
			limit,
		)
		if shardErr != nil {
			recoveryErrors = append(recoveryErrors, fmt.Errorf(
				"list tenant %d push effects: %w",
				tenantID,
				shardErr,
			))
			continue
		}
		if len(shard) > remaining {
			shard = shard[:remaining]
		}
		effects = append(effects, shard...)
	}

	semaphore := make(chan struct{}, c.config.Concurrency)
	var wait sync.WaitGroup
	var errorsMu sync.Mutex
	for i := range effects {
		effect := effects[i]
		select {
		case semaphore <- struct{}{}:
		case <-passCtx.Done():
			wait.Wait()
			return errors.Join(
				passCtx.Err(),
				errors.Join(recoveryErrors...),
			)
		}
		wait.Add(1)
		go func() {
			defer wait.Done()
			defer func() { <-semaphore }()
			attemptCtx, cancelAttempt := context.WithTimeout(
				passCtx,
				c.config.AttemptTimeout,
			)
			defer cancelAttempt()
			if _, recoverErr := c.recoverEffect(
				attemptCtx,
				effect,
			); recoverErr != nil {
				errorsMu.Lock()
				recoveryErrors = append(recoveryErrors, recoverErr)
				errorsMu.Unlock()
			}
		}()
	}
	wait.Wait()
	return errors.Join(recoveryErrors...)
}

// recoverEffect converges one listed checkpoint. Listing grants no authority:
// every provider Create still requires the future atomic authorized claim.
func (c *Coordinator) recoverEffect(
	ctx context.Context,
	listed pusheffect.Effect,
) (Outcome, error) {
	if c == nil || c.store == nil || c.provider == nil {
		return "", ErrDependencies
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}

	effect := &listed
	switch effect.Status {
	case pusheffect.StatusSent, pusheffect.StatusBlocked:
		return OutcomeIgnored, nil
	case pusheffect.StatusSending:
		taken, err := c.store.TakeOverStalePushEffect(
			ctx,
			effect.Scope(),
		)
		if err != nil {
			return "", fmt.Errorf(
				"take over stale push effect %s: %w",
				effect.ID,
				err,
			)
		}
		effect = taken
		if effect.Status != pusheffect.StatusAmbiguous {
			return "", fmt.Errorf(
				"takeover returned push effect state %q",
				effect.Status,
			)
		}
	case pusheffect.StatusPrepared, pusheffect.StatusDefiniteFailed:
		return c.recoverSafeSend(ctx, effect, false)
	case pusheffect.StatusAmbiguous:
		// Continue below.
	default:
		return "", fmt.Errorf(
			"push effect %s has unsupported state %q",
			effect.ID,
			effect.Status,
		)
	}
	return c.recoverAmbiguous(ctx, effect)
}

func (c *Coordinator) recoverAmbiguous(
	ctx context.Context,
	effect *pusheffect.Effect,
) (Outcome, error) {
	observation, err := c.provider.ResolvePushEffectMessage(
		ctx,
		pusheffect.HistoryQuery{
			EffectID:       effect.ID,
			ProviderChatID: effect.ProviderChatID,
			AppIdentity:    effect.AppIdentity,
			CardDigest:     effect.CardDigest,
			StartTime: effect.CreatedAt.
				Add(-c.config.HistorySkew),
			EndTime: effect.IdempotencyExpiresAt.
				Add(c.config.HistorySkew),
		},
	)
	if err != nil {
		deferErr := c.deferAmbiguous(
			ctx,
			effect,
			"provider_history_unavailable",
			false,
		)
		return OutcomeAmbiguous, errors.Join(fmt.Errorf(
			"resolve ambiguous push effect %s: %w",
			effect.ID,
			err,
		), deferErr)
	}
	switch {
	case observation.MatchCount == 1 && observation.MessageID != "":
		err := c.store.RecordPushEffectSentWithDeliveries(
			ctx,
			pusheffect.SentReceipt{
				Scope:             effect.Scope(),
				ExpectedFence:     effect.Fence,
				ProviderMessageID: observation.MessageID,
			},
		)
		if err != nil {
			return "", fmt.Errorf(
				"record positive history for push effect %s: %w",
				effect.ID,
				err,
			)
		}
		return OutcomeSent, nil
	case observation.MatchCount > 1:
		err := c.store.BlockConflictingPushEffectHistory(
			ctx,
			pusheffect.Resolution{
				Scope:         effect.Scope(),
				ExpectedFence: effect.Fence,
				Class:         "provider_history_conflict",
			},
		)
		if err != nil {
			return "", fmt.Errorf(
				"block conflicting history for push effect %s: %w",
				effect.ID,
				err,
			)
		}
		return OutcomeBlocked, nil
	case observation.MatchCount != 0 || observation.MessageID != "":
		return "", fmt.Errorf(
			"push effect %s history observation is invalid",
			effect.ID,
		)
	}

	if !c.now().Before(effect.IdempotencyExpiresAt) {
		err := c.store.BlockExpiredPushEffect(
			ctx,
			pusheffect.Resolution{
				Scope:         effect.Scope(),
				ExpectedFence: effect.Fence,
				Class:         "provider_window_expired",
			},
		)
		if err != nil {
			return "", fmt.Errorf(
				"block expired push effect %s: %w",
				effect.ID,
				err,
			)
		}
		return OutcomeBlocked, nil
	}
	if effect.Attempt >= c.config.MaxAttempts {
		// Positive history remains eligible on later passes. The attempt cap
		// suppresses Create only; it must not erase a remotely sent message.
		if err := c.deferAmbiguous(
			ctx,
			effect,
			"attempt_budget_exhausted",
			true,
		); err != nil {
			return "", err
		}
		return OutcomeDeferred, nil
	}
	return c.recoverSafeSend(ctx, effect, true)
}

func (c *Coordinator) recoverSafeSend(
	ctx context.Context,
	effect *pusheffect.Effect,
	reconciliation bool,
) (Outcome, error) {
	if effect.Attempt >= c.config.MaxAttempts {
		if reconciliation {
			if err := c.deferAmbiguous(
				ctx,
				effect,
				"attempt_budget_exhausted",
				true,
			); err != nil {
				return "", err
			}
		}
		return OutcomeDeferred, nil
	}
	authorized, err := c.store.AuthorizePushEffectRunSideEffect(
		ctx,
		effect.Scope(),
	)
	if err != nil {
		var deferErr error
		if reconciliation {
			deferErr = c.deferAmbiguous(
				ctx,
				effect,
				"run_authorization_unavailable",
				false,
			)
		}
		return "", fmt.Errorf(
			"preflight push effect %s run authority: %w",
			effect.ID,
			errors.Join(err, deferErr),
		)
	}
	if !authorized {
		if reconciliation {
			if err := c.deferAmbiguous(
				ctx,
				effect,
				"run_not_authorized",
				false,
			); err != nil {
				return "", err
			}
		}
		return OutcomeNotAuthorized, nil
	}

	claim := pusheffect.ClaimParams{
		Scope:         effect.Scope(),
		LeaseOwner:    "push-recovery/" + uuid.NewString(),
		LeaseDuration: c.config.LeaseDuration,
	}
	var claimed *pusheffect.Effect
	if reconciliation {
		claimed, err = c.store.ClaimAuthorizedPushEffectReconciliation(
			ctx,
			claim,
		)
	} else {
		claimed, err = c.store.ClaimAuthorizedPushEffect(ctx, claim)
	}
	if err != nil {
		return "", fmt.Errorf(
			"claim authorized push effect %s: %w",
			effect.ID,
			err,
		)
	}
	if claimed == nil ||
		claimed.Scope() != effect.Scope() ||
		claimed.Status != pusheffect.StatusSending ||
		claimed.LeaseOwner == "" ||
		claimed.Fence <= effect.Fence {
		return "", fmt.Errorf(
			"authorized push effect %s claim is invalid",
			effect.ID,
		)
	}
	return c.sendClaimed(ctx, claimed, reconciliation)
}

func (c *Coordinator) sendClaimed(
	ctx context.Context,
	effect *pusheffect.Effect,
	wasAmbiguous bool,
) (Outcome, error) {
	observation, sendErr := c.provider.PushWithUUID(
		ctx,
		effect.AppIdentity,
		effect.Target,
		string(effect.Card),
		effect.ProviderUUID,
	)
	lease := pusheffect.Lease{
		Scope:      effect.Scope(),
		LeaseOwner: effect.LeaseOwner,
		Fence:      effect.Fence,
	}
	if sendErr == nil && validSentObservation(effect, observation) {
		err := c.store.RecordPushEffectSentWithDeliveries(
			ctx,
			pusheffect.SentReceipt{
				Scope:             effect.Scope(),
				ExpectedFence:     effect.Fence,
				LeaseOwner:        effect.LeaseOwner,
				ProviderMessageID: observation.MessageID,
			},
		)
		if err != nil {
			return "", errors.Join(sendErr, fmt.Errorf(
				"record push effect %s sent receipt: %w",
				effect.ID,
				err,
			))
		}
		return OutcomeSent, sendErr
	}

	class := "provider_response_unknown"
	definite := observation.Disposition ==
		pusheffect.AttemptDefiniteNotSent && !wasAmbiguous
	switch {
	case observation.Disposition == pusheffect.AttemptSent:
		class = "provider_receipt_invalid"
	case definite:
		class = "provider_definite_rejection"
	case wasAmbiguous:
		class = "reconciliation_without_positive_receipt"
	}
	failure := pusheffect.FailureParams{
		Lease: lease,
		Class: class,
	}
	var persistErr error
	var outcome Outcome
	if definite {
		failure.RetryAfter = c.config.RetryAfter
		persistErr = c.store.RecordPushEffectDefiniteFailure(
			ctx,
			failure,
		)
		outcome = OutcomeDefiniteFail
	} else {
		persistErr = c.store.RecordPushEffectAmbiguous(
			ctx,
			failure,
		)
		outcome = OutcomeAmbiguous
	}
	if persistErr != nil {
		return "", errors.Join(sendErr, fmt.Errorf(
			"record push effect %s provider outcome: %w",
			effect.ID,
			persistErr,
		))
	}
	if outcome == OutcomeAmbiguous {
		deferErr := c.deferAmbiguous(
			ctx,
			effect,
			class,
			false,
		)
		if deferErr != nil {
			return "", errors.Join(sendErr, deferErr)
		}
	}
	return outcome, sendErr
}

func (c *Coordinator) deferAmbiguous(
	ctx context.Context,
	effect *pusheffect.Effect,
	class string,
	untilExpiry bool,
) error {
	resolution := pusheffect.Resolution{
		Scope:         effect.Scope(),
		ExpectedFence: effect.Fence,
		Class:         class,
	}
	var err error
	if untilExpiry {
		err = c.store.DeferPushEffectReconciliationUntilExpiry(
			ctx,
			resolution,
		)
	} else {
		err = c.store.DeferPushEffectReconciliation(
			ctx,
			resolution,
			c.config.RetryAfter,
		)
	}
	if err != nil {
		return fmt.Errorf(
			"defer ambiguous push effect %s: %w",
			effect.ID,
			err,
		)
	}
	return nil
}

func validSentObservation(
	effect *pusheffect.Effect,
	observation pusheffect.ProviderObservation,
) bool {
	return effect != nil &&
		observation.Disposition == pusheffect.AttemptSent &&
		observation.AppIdentity == effect.AppIdentity &&
		observation.MessageID != "" &&
		(observation.ChatID == "" ||
			observation.ChatID == effect.ProviderChatID)
}
