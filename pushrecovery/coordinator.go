// Package pushrecovery contains one dark, exact-task provider-effect recovery
// attempt. It deliberately has no scan loop, lifecycle, or production wiring.
package pushrecovery

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/YouToco/vane/pusheffect"
)

const (
	defaultLeaseDuration     = time.Minute
	defaultRetryAfter        = 30 * time.Second
	defaultHistorySkew       = time.Minute
	defaultProviderTimeout   = 30 * time.Second
	defaultHistoryTimeout    = 30 * time.Second
	defaultCheckpointTimeout = 5 * time.Second
	maxExternalTimeout       = 2 * time.Minute
	maxCheckpointTimeout     = 30 * time.Second
	maxRetryAfter            = 30 * 24 * time.Hour
	maxHistorySkew           = 30 * time.Minute
)

var ErrDependencies = errors.New(
	"push recovery: dependencies or exact task are invalid")

type Store interface {
	LoadPushEffect(
		context.Context,
		pusheffect.Scope,
	) (*pusheffect.Effect, error)
	TakeOverStalePushEffect(
		context.Context,
		pusheffect.Scope,
	) (*pusheffect.Effect, error)
	ClaimAuthorizedPushEffect(
		context.Context,
		pusheffect.AuthorizedClaimParams,
	) (*pusheffect.Effect, pusheffect.AuthorizedClaimDecision, error)
	ClaimAuthorizedPushEffectReconciliation(
		context.Context,
		pusheffect.AuthorizedClaimParams,
	) (*pusheffect.Effect, pusheffect.AuthorizedClaimDecision, error)
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
	DeferOrBlockPushEffectReconciliation(
		context.Context,
		pusheffect.ReconciliationSchedule,
	) (pusheffect.ReconciliationDecision, error)
	BlockConflictingPushEffectHistory(
		context.Context,
		pusheffect.HistoryResolution,
	) error
	BlockExhaustedPushEffectAttempts(
		context.Context,
		pusheffect.ExhaustedResolution,
	) error
}

type Sender interface {
	PushWithUUID(
		context.Context,
		string,
		string,
		string,
		string,
	) (pusheffect.ProviderObservation, error)
}

type HistoryResolver interface {
	ResolvePushEffectMessage(
		context.Context,
		pusheffect.HistoryQuery,
	) (pusheffect.HistoryObservation, error)
}

type Config struct {
	ExactTaskID       string
	LeaseDuration     time.Duration
	RetryAfter        time.Duration
	HistorySkew       time.Duration
	ProviderTimeout   time.Duration
	HistoryTimeout    time.Duration
	CheckpointTimeout time.Duration
}

type Deps struct {
	Store           Store
	Sender          Sender
	HistoryResolver HistoryResolver
	Config          Config
}

type Coordinator struct {
	store           Store
	sender          Sender
	historyResolver HistoryResolver
	config          Config
}

func New(deps Deps) (*Coordinator, error) {
	if deps.Store == nil || deps.Sender == nil || deps.HistoryResolver == nil ||
		!validExactTaskID(deps.Config.ExactTaskID) {
		return nil, ErrDependencies
	}
	if deps.Config.LeaseDuration <= 0 {
		deps.Config.LeaseDuration = defaultLeaseDuration
	} else if deps.Config.LeaseDuration.Microseconds() <= 0 ||
		deps.Config.LeaseDuration > pusheffect.MaxLeaseDuration {
		return nil, ErrDependencies
	}
	if deps.Config.RetryAfter <= 0 {
		deps.Config.RetryAfter = defaultRetryAfter
	} else if deps.Config.RetryAfter.Microseconds() <= 0 {
		return nil, ErrDependencies
	} else if deps.Config.RetryAfter > maxRetryAfter {
		deps.Config.RetryAfter = maxRetryAfter
	}
	if deps.Config.HistorySkew <= 0 {
		deps.Config.HistorySkew = defaultHistorySkew
	} else if deps.Config.HistorySkew.Microseconds() <= 0 ||
		deps.Config.HistorySkew > maxHistorySkew {
		return nil, ErrDependencies
	}
	if deps.Config.ProviderTimeout <= 0 {
		deps.Config.ProviderTimeout = defaultProviderTimeout
	} else if deps.Config.ProviderTimeout.Microseconds() <= 0 {
		return nil, ErrDependencies
	} else if deps.Config.ProviderTimeout > maxExternalTimeout {
		deps.Config.ProviderTimeout = maxExternalTimeout
	}
	if deps.Config.HistoryTimeout <= 0 {
		deps.Config.HistoryTimeout = defaultHistoryTimeout
	} else if deps.Config.HistoryTimeout.Microseconds() <= 0 {
		return nil, ErrDependencies
	} else if deps.Config.HistoryTimeout > maxExternalTimeout {
		deps.Config.HistoryTimeout = maxExternalTimeout
	}
	if deps.Config.CheckpointTimeout <= 0 {
		deps.Config.CheckpointTimeout = defaultCheckpointTimeout
	} else if deps.Config.CheckpointTimeout.Microseconds() <= 0 {
		return nil, ErrDependencies
	} else if deps.Config.CheckpointTimeout > maxCheckpointTimeout {
		deps.Config.CheckpointTimeout = maxCheckpointTimeout
	}
	if deps.Config.ProviderTimeout+deps.Config.CheckpointTimeout >=
		deps.Config.LeaseDuration {
		return nil, ErrDependencies
	}
	return &Coordinator{
		store: deps.Store, sender: deps.Sender,
		historyResolver: deps.HistoryResolver,
		config:          deps.Config,
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

// Attempt converges one explicitly scoped effect by at most one provider
// operation. Loading or history resolution grants no send authority; every
// Create crosses ClaimAuthorizedPushEffect immediately beforehand.
func (c *Coordinator) Attempt(
	ctx context.Context,
	scope pusheffect.Scope,
) (Outcome, error) {
	if c == nil || c.store == nil || c.sender == nil ||
		c.historyResolver == nil {
		return "", ErrDependencies
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	effect, err := c.store.LoadPushEffect(ctx, scope)
	if err != nil {
		return "", fmt.Errorf("load push effect recovery target: %w", err)
	}
	if effect == nil || effect.Scope() != scope ||
		effect.TaskID != c.config.ExactTaskID {
		return "", fmt.Errorf(
			"push effect is outside the enabled recovery task")
	}
	switch effect.Status {
	case pusheffect.StatusSent, pusheffect.StatusBlocked:
		return OutcomeIgnored, nil
	case pusheffect.StatusSending:
		effect, err = c.store.TakeOverStalePushEffect(ctx, scope)
		if err != nil {
			return "", fmt.Errorf("take over stale push effect: %w", err)
		}
		if effect == nil || effect.Status != pusheffect.StatusAmbiguous {
			return "", errors.New(
				"push effect takeover returned an invalid checkpoint")
		}
	case pusheffect.StatusPrepared, pusheffect.StatusDefiniteFailed:
		if effect.RecoveryBudgetExhausted() {
			return c.blockExhausted(ctx, effect)
		}
		return c.claimAndSend(ctx, effect, false)
	case pusheffect.StatusAmbiguous:
		// Continue below.
	default:
		return "", fmt.Errorf("push effect recovery state is unsupported")
	}
	return c.resolveAmbiguous(ctx, effect)
}

func (c *Coordinator) resolveAmbiguous(
	ctx context.Context,
	effect *pusheffect.Effect,
) (Outcome, error) {
	providerCtx, cancelProvider := context.WithTimeout(
		ctx, c.config.HistoryTimeout)
	observation, err := c.historyResolver.ResolvePushEffectMessage(
		providerCtx,
		pusheffect.HistoryQuery{
			EffectID:       effect.ID,
			ProviderChatID: effect.ProviderChatID,
			AppIdentity:    effect.AppIdentity,
			CardDigest:     effect.CardDigest,
			StartTime:      effect.CreatedAt.Add(-c.config.HistorySkew),
			EndTime: effect.IdempotencyExpiresAt.
				Add(c.config.HistorySkew),
		},
	)
	cancelProvider()
	if err != nil {
		_, deferErr := c.deferAmbiguous(ctx, effect, false)
		return OutcomeAmbiguous, errors.Join(
			fmt.Errorf("resolve push effect history: %w", err),
			deferErr,
		)
	}
	switch {
	case observation.MatchCount == 1 && observation.MessageID != "":
		checkpointCtx, cancelCheckpoint := c.checkpointContext(ctx)
		defer cancelCheckpoint()
		if err := c.store.RecordPushEffectSentWithDeliveries(
			checkpointCtx,
			pusheffect.SentReceipt{
				Scope:                effect.Scope(),
				ExpectedFence:        effect.Fence,
				ProviderMessageID:    observation.MessageID,
				ObservationEventKeys: effect.ObservationEventKeys,
			},
		); err != nil {
			return "", fmt.Errorf(
				"record positive push effect history: %w", err)
		}
		return OutcomeSent, nil
	case observation.MatchCount > 1:
		checkpointCtx, cancelCheckpoint := c.checkpointContext(ctx)
		defer cancelCheckpoint()
		if err := c.store.BlockConflictingPushEffectHistory(
			checkpointCtx,
			pusheffect.HistoryResolution{
				Scope:         effect.Scope(),
				ExpectedFence: effect.Fence,
			},
		); err != nil {
			return "", fmt.Errorf(
				"block conflicting push effect history: %w", err)
		}
		return OutcomeBlocked, nil
	case observation.MatchCount != 0 || observation.MessageID != "":
		return "", errors.New("push effect history observation is invalid")
	}
	if effect.RecoveryBudgetExhausted() {
		return c.deferAmbiguous(ctx, effect, true)
	}
	return c.claimAndSend(ctx, effect, true)
}

func (c *Coordinator) claimAndSend(
	ctx context.Context,
	effect *pusheffect.Effect,
	reconciliation bool,
) (Outcome, error) {
	if effect.RecoveryBudgetExhausted() {
		if reconciliation {
			return c.deferAmbiguous(ctx, effect, true)
		}
		return c.blockExhausted(ctx, effect)
	}
	params := pusheffect.AuthorizedClaimParams{
		ClaimParams: pusheffect.ClaimParams{
			Scope:         effect.Scope(),
			LeaseOwner:    "push-recovery/" + uuid.NewString(),
			LeaseDuration: c.config.LeaseDuration,
		},
		ExpectedTaskID:   c.config.ExactTaskID,
		DenialRetryAfter: c.config.RetryAfter,
	}
	var (
		claimed  *pusheffect.Effect
		decision pusheffect.AuthorizedClaimDecision
		err      error
	)
	if reconciliation {
		claimed, decision, err =
			c.store.ClaimAuthorizedPushEffectReconciliation(ctx, params)
	} else {
		claimed, decision, err =
			c.store.ClaimAuthorizedPushEffect(ctx, params)
	}
	if err != nil {
		var deferErr error
		if reconciliation {
			_, deferErr = c.deferAmbiguous(ctx, effect, false)
		}
		return "", fmt.Errorf(
			"claim authorized push effect: %w",
			errors.Join(err, deferErr),
		)
	}
	switch decision {
	case pusheffect.AuthorizedClaimNotDue:
		return OutcomeDeferred, nil
	case pusheffect.AuthorizedClaimDenied:
		if reconciliation {
			if _, err := c.deferAmbiguous(ctx, effect, false); err != nil {
				return "", err
			}
		}
		return OutcomeNotAuthorized, nil
	case pusheffect.AuthorizedClaimed:
		// Continue.
	default:
		return "", errors.New("authorized push effect decision is invalid")
	}
	if claimed == nil || claimed.Scope() != effect.Scope() ||
		claimed.TaskID != c.config.ExactTaskID ||
		claimed.Status != pusheffect.StatusSending ||
		claimed.LeaseOwner == "" || claimed.Fence <= effect.Fence {
		return "", errors.New("authorized push effect claim is invalid")
	}
	return c.sendClaimed(ctx, claimed, reconciliation)
}

func (c *Coordinator) blockExhausted(
	ctx context.Context,
	effect *pusheffect.Effect,
) (Outcome, error) {
	checkpointCtx, cancelCheckpoint := c.checkpointContext(ctx)
	defer cancelCheckpoint()
	if err := c.store.BlockExhaustedPushEffectAttempts(
		checkpointCtx,
		pusheffect.ExhaustedResolution{
			Scope: effect.Scope(), ExpectedFence: effect.Fence,
			ExpectedTaskID: c.config.ExactTaskID,
		},
	); err != nil {
		return "", fmt.Errorf("block exhausted push effect: %w", err)
	}
	return OutcomeBlocked, nil
}

func (c *Coordinator) sendClaimed(
	ctx context.Context,
	effect *pusheffect.Effect,
	wasAmbiguous bool,
) (Outcome, error) {
	providerCtx, cancelProvider := context.WithTimeout(
		ctx, c.config.ProviderTimeout)
	observation, sendErr := c.sender.PushWithUUID(
		providerCtx,
		effect.AppIdentity,
		effect.Target,
		string(effect.Card),
		effect.ProviderUUID,
	)
	cancelProvider()
	lease := pusheffect.Lease{
		Scope: effect.Scope(), LeaseOwner: effect.LeaseOwner,
		Fence: effect.Fence,
	}
	if sendErr == nil && validSentObservation(effect, observation) {
		checkpointCtx, cancelCheckpoint := c.checkpointContext(ctx)
		defer cancelCheckpoint()
		if err := c.store.RecordPushEffectSentWithDeliveries(
			checkpointCtx,
			pusheffect.SentReceipt{
				Scope:                effect.Scope(),
				ExpectedFence:        effect.Fence,
				LeaseOwner:           effect.LeaseOwner,
				ProviderMessageID:    observation.MessageID,
				ObservationEventKeys: effect.ObservationEventKeys,
			},
		); err != nil {
			return "", fmt.Errorf("record push effect sent receipt: %w", err)
		}
		return OutcomeSent, nil
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
	if definite {
		failure.RetryAfter = c.config.RetryAfter
		checkpointCtx, cancelCheckpoint := c.checkpointContext(ctx)
		defer cancelCheckpoint()
		if err := c.store.RecordPushEffectDefiniteFailure(
			checkpointCtx, failure); err != nil {
			return "", errors.Join(sendErr, fmt.Errorf(
				"record definite push effect failure: %w", err))
		}
		return OutcomeDefiniteFail, sendErr
	}
	checkpointCtx, cancelCheckpoint := c.checkpointContext(ctx)
	if err := c.store.RecordPushEffectAmbiguous(
		checkpointCtx, failure); err != nil {
		cancelCheckpoint()
		return "", errors.Join(sendErr, fmt.Errorf(
			"record ambiguous push effect result: %w", err))
	}
	cancelCheckpoint()
	updated := *effect
	updated.Status = pusheffect.StatusAmbiguous
	updated.LeaseOwner = ""
	updated.LeaseUntil = nil
	if decision, err := c.deferAmbiguous(ctx, &updated, false); err != nil {
		return "", errors.Join(sendErr, err)
	} else if decision == OutcomeBlocked {
		return OutcomeBlocked, sendErr
	}
	return OutcomeAmbiguous, sendErr
}

func (c *Coordinator) deferAmbiguous(
	ctx context.Context,
	effect *pusheffect.Effect,
	untilExpiry bool,
) (Outcome, error) {
	checkpointCtx, cancelCheckpoint := c.checkpointContext(ctx)
	defer cancelCheckpoint()
	decision, err := c.store.DeferOrBlockPushEffectReconciliation(
		checkpointCtx,
		pusheffect.ReconciliationSchedule{
			Scope:         effect.Scope(),
			ExpectedFence: effect.Fence,
			RetryAfter:    c.config.RetryAfter,
			UntilExpiry:   untilExpiry,
		},
	)
	if err != nil {
		return "", fmt.Errorf("defer ambiguous push effect: %w", err)
	}
	switch decision {
	case pusheffect.ReconciliationDeferred:
		return OutcomeDeferred, nil
	case pusheffect.ReconciliationBlocked:
		return OutcomeBlocked, nil
	default:
		return "", errors.New(
			"push effect reconciliation returned an invalid decision")
	}
}

func (c *Coordinator) checkpointContext(
	parent context.Context,
) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, c.config.CheckpointTimeout)
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

func validExactTaskID(value string) bool {
	if value == "" || len(value) > 512 || !utf8.ValidString(value) ||
		strings.TrimSpace(value) != value {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}
