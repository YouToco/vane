package scheduler

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/proto"

	"github.com/YouToco/vane/types"
	"github.com/YouToco/vane/workflow"
)

type researchV3CutoverJournal interface {
	GetSchedule(context.Context, string, int64) (*types.Schedule, error)
	LoadCurrentResearchApprovedDefinitionV3Head(
		context.Context, int64, int64, string,
	) (types.ResearchV3DefinitionHead, error)
	BeginResearchV3Cutover(
		context.Context, types.BeginResearchV3CutoverParams,
	) (types.ResearchV3CutoverOperation, error)
	LoadResearchV3Cutover(
		context.Context, int64, int64, string, string,
	) (types.ResearchV3CutoverOperation, bool, error)
	RecheckResearchV3CutoverDefinition(
		context.Context, types.ResearchV3CutoverOperation,
	) error
	BeginResearchV3RollbackPause(
		context.Context, types.ResearchV3CutoverOperation, []byte, string,
	) (types.ResearchV3CutoverOperation, error)
	AdvanceResearchV3Cutover(
		context.Context, types.ResearchV3CutoverOperation,
		types.ResearchV3CutoverPhase, types.ResearchV3CutoverPhase,
	) (types.ResearchV3CutoverOperation, error)
	RevokeResearchV3DeliveryAuthority(
		context.Context, types.ResearchV3CutoverOperation,
	) error
}

type researchV3ScheduleRemote interface {
	Describe(context.Context, string) (*workflowservice.DescribeScheduleResponse, error)
	CompareAndSwap(context.Context, string, *schedulepb.Schedule, []byte, string) error
}

type researchV3ParamsEncoder func(any) (*commonpb.Payload, error)

// researchV3CutoverCoordinator is intentionally not reachable from config,
// server startup, HTTP, or the Agent tool surface.  It is the dark, exact-task
// core that will be wired only after the delivery Gate is independently green.
type researchV3CutoverCoordinator struct {
	exactTaskID string
	journal     researchV3CutoverJournal
	remote      researchV3ScheduleRemote
	encode      researchV3ParamsEncoder
}

type researchV3CutoverRequest struct {
	TaskID         string
	UserID         int64
	IdempotencyKey string
}

const researchV3CutoverConflictRollbackTimeout = 30 * time.Second

func newResearchV3CutoverCoordinator(
	exactTaskID string, journal researchV3CutoverJournal,
	remote researchV3ScheduleRemote, encode researchV3ParamsEncoder,
) (*researchV3CutoverCoordinator, error) {
	if exactTaskID == "" || strings.TrimSpace(exactTaskID) != exactTaskID ||
		len(exactTaskID) > 255 || journal == nil || remote == nil || encode == nil {
		return nil, types.NewAppError(types.CodeValidation,
			"research V3 cutover coordinator is invalid", types.ErrValidation)
	}
	return &researchV3CutoverCoordinator{
		exactTaskID: exactTaskID, journal: journal, remote: remote, encode: encode,
	}, nil
}

// newDarkResearchV3CutoverCore is deliberately unused by production wiring.
// Keeping the adapter here makes the future Gate a small composition change,
// while the hard-disabled authority option remains the current authority.
func (s *Scheduler) newDarkResearchV3CutoverCore(
	exactTaskID string,
) (*researchV3CutoverCoordinator, error) {
	journal, ok := s.st.(researchV3CutoverJournal)
	if !ok || s == nil || s.c == nil || s.taskScheduleEnv.namespace == "" {
		return nil, types.NewAppError(types.CodeInternal,
			"research V3 cutover dependencies are unavailable", nil)
	}
	dc := s.taskScheduleDecoder(exactTaskID)
	return newResearchV3CutoverCoordinator(exactTaskID, journal,
		&schedulerResearchV3ScheduleRemote{scheduler: s},
		func(params any) (*commonpb.Payload, error) {
			return dc.ToPayload(params)
		})
}

func (c *researchV3CutoverCoordinator) Cutover(
	ctx context.Context, req researchV3CutoverRequest,
) (types.ResearchV3CutoverOperation, error) {
	if err := c.validateRequest(req); err != nil {
		return types.ResearchV3CutoverOperation{}, err
	}
	mirror, err := c.journal.GetSchedule(ctx, req.TaskID, req.UserID)
	if err != nil {
		return types.ResearchV3CutoverOperation{}, err
	}
	if mirror == nil || mirror.ID != req.TaskID || mirror.UserID != req.UserID ||
		mirror.TenantID <= 0 || mirror.Status != types.ScheduleStatusActive {
		return types.ResearchV3CutoverOperation{}, types.NewAppError(
			types.CodeNotFound, "research V3 cutover task is not active", types.ErrNotFound)
	}
	if existing, found, loadErr := c.journal.LoadResearchV3Cutover(
		ctx, mirror.TenantID, req.UserID, req.TaskID, req.IdempotencyKey,
	); loadErr != nil {
		return types.ResearchV3CutoverOperation{}, loadErr
	} else if found {
		return c.resumeCutover(ctx, existing)
	}
	head, err := c.journal.LoadCurrentResearchApprovedDefinitionV3Head(
		ctx, mirror.TenantID, req.UserID, req.TaskID)
	if err != nil {
		return types.ResearchV3CutoverOperation{}, err
	}
	desc, err := c.remote.Describe(ctx, req.TaskID)
	if err != nil {
		return types.ResearchV3CutoverOperation{}, cutoverRemoteError("describe", err)
	}
	frozen, err := cloneDescribedScheduleV3(desc)
	if err != nil {
		return types.ResearchV3CutoverOperation{}, err
	}
	authorityToken, authorityDigest, err := newResearchV3ActionAuthorization()
	if err != nil {
		return types.ResearchV3CutoverOperation{}, err
	}
	targetAction, err := c.buildTargetAction(mirror, frozen.GetAction(), authorityToken)
	if err != nil {
		return types.ResearchV3CutoverOperation{}, err
	}
	frozenBytes, frozenDigest, err := marshalScheduleArtifactV3(frozen)
	if err != nil {
		return types.ResearchV3CutoverOperation{}, err
	}
	targetBytes, targetDigest, err := marshalScheduleArtifactV3(targetAction)
	if err != nil {
		return types.ResearchV3CutoverOperation{}, err
	}
	conflictToken := append([]byte(nil), desc.GetConflictToken()...)
	conflictDigestBytes := sha256.Sum256(conflictToken)
	op, err := c.journal.BeginResearchV3Cutover(ctx,
		types.BeginResearchV3CutoverParams{
			TenantID: mirror.TenantID, UserID: req.UserID, TaskID: req.TaskID,
			IdempotencyKey: req.IdempotencyKey, Definition: head,
			FrozenSchedule: frozenBytes, FrozenScheduleDigest: frozenDigest,
			FrozenConflictToken: conflictToken,
			ConflictTokenDigest: hex.EncodeToString(conflictDigestBytes[:]),
			TargetAction:        targetBytes, TargetActionDigest: targetDigest,
			ActionAuthorizationDigest: authorityDigest,
			OriginalPaused:            frozen.GetState().GetPaused(),
		})
	if err != nil {
		return types.ResearchV3CutoverOperation{}, err
	}
	return c.resumeCutover(ctx, op)
}

func (c *researchV3CutoverCoordinator) Resume(
	ctx context.Context, tenantID int64, req researchV3CutoverRequest,
) (types.ResearchV3CutoverOperation, error) {
	if err := c.validateRequest(req); err != nil || tenantID <= 0 {
		if err != nil {
			return types.ResearchV3CutoverOperation{}, err
		}
		return types.ResearchV3CutoverOperation{}, types.NewAppError(
			types.CodeValidation, "research V3 cutover tenant is invalid", types.ErrValidation)
	}
	op, found, err := c.journal.LoadResearchV3Cutover(
		ctx, tenantID, req.UserID, req.TaskID, req.IdempotencyKey)
	if err != nil {
		return types.ResearchV3CutoverOperation{}, err
	}
	if !found {
		return types.ResearchV3CutoverOperation{}, types.NewAppError(
			types.CodeNotFound, "research V3 cutover journal is unavailable", types.ErrNotFound)
	}
	return c.resumeCutover(ctx, op)
}

func (c *researchV3CutoverCoordinator) resumeCutover(
	ctx context.Context, op types.ResearchV3CutoverOperation,
) (types.ResearchV3CutoverOperation, error) {
	frozen, target, err := decodeCutoverArtifactsV3(op)
	if err != nil {
		return types.ResearchV3CutoverOperation{}, err
	}
	for attempt := 0; attempt < 12; attempt++ {
		desc, describeErr := c.remote.Describe(ctx, op.TaskID)
		if describeErr != nil {
			return types.ResearchV3CutoverOperation{}, cutoverRemoteError("recover describe", describeErr)
		}
		observed, err := verifyCutoverScheduleV3(desc, frozen, target)
		if err != nil {
			return types.ResearchV3CutoverOperation{}, err
		}
		switch op.Phase {
		case types.ResearchV3CutoverPrepared:
			if observed.target {
				return types.ResearchV3CutoverOperation{}, c.rollbackCutoverConflict(ctx, op,
					types.NewAppError(types.CodeConflict,
						"research V3 target Action appeared before pause ownership", types.ErrConflict))
			}
			if !op.OriginalPaused && observed.paused {
				return types.ResearchV3CutoverOperation{}, c.abortUnownedSchedulePause(ctx, op)
			}
			if !bytes.Equal(desc.GetConflictToken(), op.FrozenConflictToken) {
				return types.ResearchV3CutoverOperation{}, c.abortUnownedSchedulePause(ctx, op)
			}
			op, err = c.advance(ctx, op, types.ResearchV3CutoverPrepared,
				types.ResearchV3CutoverPauseRequested)
		case types.ResearchV3CutoverPauseRequested:
			if observed.target {
				return types.ResearchV3CutoverOperation{}, c.rollbackCutoverConflict(ctx, op,
					types.NewAppError(types.CodeConflict,
						"research V3 target Action appeared before pause checkpoint", types.ErrConflict))
			}
			if op.OriginalPaused {
				if !observed.paused {
					return types.ResearchV3CutoverOperation{}, c.abortUnownedSchedulePause(ctx, op)
				}
				op, err = c.advance(ctx, op, types.ResearchV3CutoverPauseRequested,
					types.ResearchV3CutoverPaused)
			} else if !observed.paused {
				if !bytes.Equal(desc.GetConflictToken(), op.FrozenConflictToken) {
					return types.ResearchV3CutoverOperation{}, c.abortUnownedSchedulePause(ctx, op)
				}
				err = c.casState(ctx, op, desc, frozen, false, true, "pause")
				if err == nil {
					op, err = c.advance(ctx, op, types.ResearchV3CutoverPauseRequested,
						types.ResearchV3CutoverPaused)
				}
			} else {
				// Re-submit the exact initial-token/request-id mutation. Temporal's
				// request receipt proves this pause was ours; a stale-token failure
				// is treated as an independent operator pause and is never undone.
				paused := proto.Clone(frozen).(*schedulepb.Schedule)
				if paused.State == nil {
					paused.State = &schedulepb.ScheduleState{}
				}
				paused.State.Paused = true
				err = c.remote.CompareAndSwap(ctx, op.TaskID, paused,
					op.FrozenConflictToken, researchV3CutoverRequestID(op, "pause"))
				if err != nil {
					return types.ResearchV3CutoverOperation{}, c.abortUnownedSchedulePause(ctx, op)
				}
				op, err = c.advance(ctx, op, types.ResearchV3CutoverPauseRequested,
					types.ResearchV3CutoverPaused)
			}
		case types.ResearchV3CutoverPaused:
			if observed.target {
				current := op
				var advanced types.ResearchV3CutoverOperation
				advanced, err = c.advance(ctx, current, types.ResearchV3CutoverPaused, types.ResearchV3CutoverActionSwapped)
				if err != nil && types.CodeOf(err) == types.CodeConflict {
					return types.ResearchV3CutoverOperation{}, c.rollbackCutoverConflict(ctx, current, err)
				}
				if err == nil {
					op = advanced
				} else {
					op = current
				}
			} else if !observed.paused {
				err = types.NewAppError(types.CodeConflict,
					"research V3 cutover lost its paused fence", types.ErrConflict)
			} else if err = c.journal.RecheckResearchV3CutoverDefinition(ctx, op); err == nil {
				err = c.casAction(ctx, op, desc, frozen, target, "swap-action")
			} else if types.CodeOf(err) == types.CodeConflict {
				return types.ResearchV3CutoverOperation{}, c.rollbackCutoverConflict(ctx, op, err)
			}
		case types.ResearchV3CutoverActionSwapped:
			if !observed.target {
				err = types.NewAppError(types.CodeConflict,
					"research V3 cutover target Action disappeared", types.ErrConflict)
			} else if observed.paused == op.OriginalPaused {
				current := op
				var advanced types.ResearchV3CutoverOperation
				advanced, err = c.advance(ctx, current, types.ResearchV3CutoverActionSwapped, types.ResearchV3CutoverActive)
				if err != nil && types.CodeOf(err) == types.CodeConflict {
					return types.ResearchV3CutoverOperation{}, c.rollbackCutoverConflict(ctx, current, err)
				}
				if err == nil {
					op = advanced
				} else {
					op = current
				}
			} else {
				err = c.casState(ctx, op, desc, targetScheduleV3(frozen, target), true,
					op.OriginalPaused, "restore-state")
			}
		case types.ResearchV3CutoverActive:
			if !observed.target || observed.paused != op.OriginalPaused {
				return types.ResearchV3CutoverOperation{}, types.NewAppError(
					types.CodeConflict, "research V3 cutover active checkpoint drifted", types.ErrConflict)
			}
			return op, nil
		case types.ResearchV3CutoverAborted:
			return types.ResearchV3CutoverOperation{}, types.NewAppError(
				types.CodeConflict, "research V3 cutover was aborted without Schedule ownership", types.ErrConflict)
		case types.ResearchV3CutoverRollbackPaused, types.ResearchV3CutoverRolledBack:
			return types.ResearchV3CutoverOperation{}, types.NewAppError(
				types.CodeConflict, "research V3 cutover is rolling back", types.ErrConflict)
		default:
			return types.ResearchV3CutoverOperation{}, types.NewAppError(
				types.CodeValidation, "research V3 cutover phase is invalid", types.ErrValidation)
		}
		if err != nil {
			// An UpdateSchedule response can be lost after Temporal applied it.
			// Only a fresh Describe may decide the outcome, so loop once more.
			if ctx.Err() != nil {
				return types.ResearchV3CutoverOperation{}, cutoverRemoteError("cutover canceled", ctx.Err())
			}
			if latest, found, loadErr := c.journal.LoadResearchV3Cutover(
				ctx, op.TenantID, op.UserID, op.TaskID, op.IdempotencyKey,
			); loadErr == nil && found {
				op = latest
			}
			continue
		}
	}
	return types.ResearchV3CutoverOperation{}, types.NewAppError(
		types.CodeInternal, "research V3 cutover recovery budget exhausted", nil)
}

func (c *researchV3CutoverCoordinator) abortUnownedSchedulePause(
	ctx context.Context, op types.ResearchV3CutoverOperation,
) error {
	cause := types.NewAppError(types.CodeConflict,
		"research V3 cutover cannot prove Schedule pause ownership", types.ErrConflict)
	if err := c.journal.RevokeResearchV3DeliveryAuthority(ctx, op); err != nil {
		return errors.Join(cause, err)
	}
	if _, err := c.advance(ctx, op, op.Phase, types.ResearchV3CutoverAborted); err != nil {
		return errors.Join(cause, err)
	}
	return cause
}

// rollbackCutoverConflict closes the unavoidable Postgres-to-Temporal gap.
// A definition edit can commit after the pre-CAS DB check but before the
// Schedule CAS.  The post-CAS checkpoint rechecks the head; on drift, this
// helper revokes the still-staged authority and restores the frozen Action
// before returning the original conflict.  A disconnected bounded context
// keeps the compensation alive if the caller canceled after Temporal applied.
func (c *researchV3CutoverCoordinator) rollbackCutoverConflict(
	ctx context.Context, op types.ResearchV3CutoverOperation, cause error,
) error {
	recoveryCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx), researchV3CutoverConflictRollbackTimeout)
	defer cancel()
	_, rollbackErr := c.Rollback(recoveryCtx, op.TenantID, researchV3CutoverRequest{
		TaskID: op.TaskID, UserID: op.UserID, IdempotencyKey: op.IdempotencyKey,
	})
	if rollbackErr != nil {
		return errors.Join(cause, types.NewAppError(types.CodeInternal,
			"research V3 cutover conflict rollback failed", rollbackErr))
	}
	return cause
}

func (c *researchV3CutoverCoordinator) Rollback(
	ctx context.Context, tenantID int64, req researchV3CutoverRequest,
) (types.ResearchV3CutoverOperation, error) {
	if err := c.validateRequest(req); err != nil || tenantID <= 0 {
		if err != nil {
			return types.ResearchV3CutoverOperation{}, err
		}
		return types.ResearchV3CutoverOperation{}, types.NewAppError(
			types.CodeValidation, "research V3 rollback tenant is invalid", types.ErrValidation)
	}
	op, found, err := c.journal.LoadResearchV3Cutover(
		ctx, tenantID, req.UserID, req.TaskID, req.IdempotencyKey)
	if err != nil || !found {
		if err == nil {
			err = types.NewAppError(types.CodeNotFound,
				"research V3 cutover journal is unavailable", types.ErrNotFound)
		}
		return types.ResearchV3CutoverOperation{}, err
	}
	if err := c.journal.RevokeResearchV3DeliveryAuthority(ctx, op); err != nil {
		return types.ResearchV3CutoverOperation{}, err
	}
	frozen, target, err := decodeCutoverArtifactsV3(op)
	if err != nil {
		return types.ResearchV3CutoverOperation{}, err
	}
	for attempt := 0; attempt < 12; attempt++ {
		desc, describeErr := c.remote.Describe(ctx, op.TaskID)
		if describeErr != nil {
			return types.ResearchV3CutoverOperation{}, cutoverRemoteError("rollback describe", describeErr)
		}
		observed, err := verifyCutoverScheduleV3(desc, frozen, target)
		if err != nil {
			return types.ResearchV3CutoverOperation{}, err
		}
		if op.Phase == types.ResearchV3CutoverAborted {
			if observed.target {
				return types.ResearchV3CutoverOperation{}, types.NewAppError(
					types.CodeConflict, "aborted research V3 cutover has target Action", types.ErrConflict)
			}
			return op, nil
		}
		if op.Phase == types.ResearchV3CutoverManualIntervention {
			return types.ResearchV3CutoverOperation{}, types.NewAppError(
				types.CodeConflict, "research V3 rollback requires manual intervention", types.ErrConflict)
		}
		if op.Phase == types.ResearchV3CutoverPrepared && !observed.target {
			if observed.paused != op.OriginalPaused {
				op, err = c.advance(ctx, op, types.ResearchV3CutoverPrepared,
					types.ResearchV3CutoverAborted)
				if err != nil {
					return types.ResearchV3CutoverOperation{}, err
				}
				return op, nil
			}
			op, err = c.advance(ctx, op, types.ResearchV3CutoverPrepared,
				types.ResearchV3CutoverRollbackPaused)
			if err != nil {
				continue
			}
			continue
		}
		if op.Phase == types.ResearchV3CutoverPauseRequested {
			if observed.target {
				return types.ResearchV3CutoverOperation{}, types.NewAppError(
					types.CodeConflict, "rollback found target Action before pause ownership", types.ErrConflict)
			}
			if !op.OriginalPaused && observed.paused {
				paused := proto.Clone(frozen).(*schedulepb.Schedule)
				if paused.State == nil {
					paused.State = &schedulepb.ScheduleState{}
				}
				paused.State.Paused = true
				if proofErr := c.remote.CompareAndSwap(ctx, op.TaskID, paused,
					op.FrozenConflictToken, researchV3CutoverRequestID(op, "pause")); proofErr != nil {
					op, err = c.advance(ctx, op, types.ResearchV3CutoverPauseRequested,
						types.ResearchV3CutoverAborted)
					if err != nil {
						return types.ResearchV3CutoverOperation{}, err
					}
					return op, nil
				}
			}
			op, err = c.advance(ctx, op, types.ResearchV3CutoverPauseRequested,
				types.ResearchV3CutoverRollbackPaused)
			if err != nil {
				continue
			}
			continue
		}
		if op.Phase == types.ResearchV3CutoverRolledBack {
			if observed.target || observed.paused != op.OriginalPaused {
				return types.ResearchV3CutoverOperation{}, types.NewAppError(
					types.CodeConflict, "research V3 rollback checkpoint drifted", types.ErrConflict)
			}
			return op, nil
		}
		if op.Phase != types.ResearchV3CutoverRollbackPaused {
			switch op.Phase {
			case types.ResearchV3CutoverActive:
				if observed.paused && !op.OriginalPaused {
					return types.ResearchV3CutoverOperation{}, c.markManualIntervention(
						ctx, op, "research V3 rollback cannot prove emergency pause ownership")
				}
				if observed.paused {
					op, err = c.advance(ctx, op, op.Phase, types.ResearchV3CutoverRollbackPaused)
				} else {
					token := append([]byte(nil), desc.GetConflictToken()...)
					digest := sha256.Sum256(token)
					op, err = c.journal.BeginResearchV3RollbackPause(
						ctx, op, token, hex.EncodeToString(digest[:]))
				}
			case types.ResearchV3CutoverRollbackPauseRequested:
				if len(op.RollbackConflictToken) == 0 {
					return types.ResearchV3CutoverOperation{}, c.markManualIntervention(
						ctx, op, "research V3 rollback pause proof is unavailable")
				}
				pausedSchedule := chooseObservedScheduleV3(frozen, target, observed.target)
				pausedSchedule = proto.Clone(pausedSchedule).(*schedulepb.Schedule)
				if pausedSchedule.State == nil {
					pausedSchedule.State = &schedulepb.ScheduleState{}
				}
				pausedSchedule.State.Paused = true
				if !observed.paused && !bytes.Equal(desc.GetConflictToken(), op.RollbackConflictToken) {
					return types.ResearchV3CutoverOperation{}, c.markManualIntervention(
						ctx, op, "research V3 rollback pause conflict token drifted")
				}
				if proofErr := c.remote.CompareAndSwap(ctx, op.TaskID, pausedSchedule,
					op.RollbackConflictToken, researchV3CutoverRequestID(op, "rollback-pause")); proofErr != nil {
					if !observed.paused && ctx.Err() == nil {
						// The provider may have committed the exact request and lost
						// the response. A fresh Describe followed by the same durable
						// request id is the only ownership proof.
						continue
					}
					return types.ResearchV3CutoverOperation{}, c.markManualIntervention(
						ctx, op, "research V3 rollback cannot prove pause ownership")
				}
				op, err = c.advance(ctx, op, op.Phase, types.ResearchV3CutoverRollbackPaused)
			case types.ResearchV3CutoverActionSwapped:
				if !observed.paused {
					token := append([]byte(nil), desc.GetConflictToken()...)
					digest := sha256.Sum256(token)
					op, err = c.journal.BeginResearchV3RollbackPause(
						ctx, op, token, hex.EncodeToString(digest[:]))
				} else {
					op, err = c.advance(ctx, op, op.Phase, types.ResearchV3CutoverRollbackPaused)
				}
			case types.ResearchV3CutoverPaused:
				if !observed.paused {
					return types.ResearchV3CutoverOperation{}, c.markManualIntervention(
						ctx, op, "research V3 rollback lost the cutover pause fence")
				}
				op, err = c.advance(ctx, op, op.Phase, types.ResearchV3CutoverRollbackPaused)
			default:
				return types.ResearchV3CutoverOperation{}, types.NewAppError(
					types.CodeConflict, "research V3 rollback checkpoint is not recoverable", types.ErrConflict)
			}
			if err != nil {
				if latest, found, loadErr := c.journal.LoadResearchV3Cutover(
					ctx, op.TenantID, op.UserID, op.TaskID, op.IdempotencyKey,
				); loadErr == nil && found {
					op = latest
				}
				continue
			}
		}
		if observed.target {
			err = c.casAction(ctx, op, desc, targetScheduleV3(frozen, target), frozen.GetAction(), "rollback-action")
			if err != nil && ctx.Err() != nil {
				return types.ResearchV3CutoverOperation{}, err
			}
			continue
		}
		if observed.paused != op.OriginalPaused {
			err = c.casState(ctx, op, desc, frozen, true, op.OriginalPaused, "rollback-state")
			if err != nil && ctx.Err() != nil {
				return types.ResearchV3CutoverOperation{}, err
			}
			continue
		}
		op, err = c.advance(ctx, op, types.ResearchV3CutoverRollbackPaused, types.ResearchV3CutoverRolledBack)
		if err == nil {
			return op, nil
		}
		if latest, found, loadErr := c.journal.LoadResearchV3Cutover(
			ctx, op.TenantID, op.UserID, op.TaskID, op.IdempotencyKey,
		); loadErr == nil && found {
			op = latest
		}
	}
	return types.ResearchV3CutoverOperation{}, types.NewAppError(
		types.CodeInternal, "research V3 rollback recovery budget exhausted", nil)
}

func (c *researchV3CutoverCoordinator) markManualIntervention(
	ctx context.Context, op types.ResearchV3CutoverOperation, message string,
) error {
	cause := types.NewAppError(types.CodeConflict, message, types.ErrConflict)
	if _, err := c.advance(ctx, op, op.Phase, types.ResearchV3CutoverManualIntervention); err != nil {
		return errors.Join(cause, err)
	}
	return cause
}

func (c *researchV3CutoverCoordinator) validateRequest(req researchV3CutoverRequest) error {
	if req.TaskID != c.exactTaskID || req.UserID <= 0 || req.IdempotencyKey == "" ||
		strings.TrimSpace(req.IdempotencyKey) != req.IdempotencyKey || len(req.IdempotencyKey) > 512 {
		return types.NewAppError(types.CodeNotFound,
			"research V3 exact cutover task is unavailable", types.ErrNotFound)
	}
	return nil
}

func (c *researchV3CutoverCoordinator) buildTargetAction(
	mirror *types.Schedule, old *schedulepb.ScheduleAction, authorityToken string,
) (*schedulepb.ScheduleAction, error) {
	if old == nil || old.GetStartWorkflow() == nil {
		return nil, types.NewAppError(types.CodeConflict,
			"research V3 cutover requires a workflow Action", types.ErrConflict)
	}
	payload, err := c.encode(workflow.ResearchScheduledInputV3{
		TenantID: mirror.TenantID, UserID: mirror.UserID, TaskID: mirror.ID,
		ActionAuthorizationToken: authorityToken,
	})
	if err != nil || payload == nil {
		return nil, types.NewAppError(types.CodeInternal,
			"encode research V3 Schedule Action", err)
	}
	target := proto.Clone(old).(*schedulepb.ScheduleAction)
	target.GetStartWorkflow().WorkflowType = &commonpb.WorkflowType{
		Name: workflow.ResearchScheduledWorkflowV3Name,
	}
	target.GetStartWorkflow().Input = &commonpb.Payloads{Payloads: []*commonpb.Payload{payload}}
	return target, nil
}

func newResearchV3ActionAuthorization() (string, string, error) {
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return "", "", types.NewAppError(types.CodeInternal,
			"mint research V3 Action authorization", err)
	}
	token := hex.EncodeToString(secret)
	digest := sha256.Sum256([]byte(token))
	return token, hex.EncodeToString(digest[:]), nil
}

type observedCutoverScheduleV3 struct {
	target bool
	paused bool
}

func verifyCutoverScheduleV3(
	desc *workflowservice.DescribeScheduleResponse,
	frozen *schedulepb.Schedule, target *schedulepb.ScheduleAction,
) (observedCutoverScheduleV3, error) {
	if desc == nil || desc.GetSchedule() == nil || len(desc.GetConflictToken()) == 0 {
		return observedCutoverScheduleV3{}, types.NewAppError(types.CodeConflict,
			"research V3 cutover Describe is incomplete", types.ErrConflict)
	}
	current := desc.GetSchedule()
	if !proto.Equal(current.GetSpec(), frozen.GetSpec()) ||
		!proto.Equal(current.GetPolicies(), frozen.GetPolicies()) {
		return observedCutoverScheduleV3{}, types.NewAppError(types.CodeConflict,
			"research V3 cutover Schedule spec or policy changed", types.ErrConflict)
	}
	isOld := proto.Equal(current.GetAction(), frozen.GetAction())
	isTarget := proto.Equal(current.GetAction(), target)
	if !isOld && !isTarget {
		return observedCutoverScheduleV3{}, types.NewAppError(types.CodeConflict,
			"research V3 cutover Schedule Action changed independently", types.ErrConflict)
	}
	if !stateEqualsExceptPausedV3(current.GetState(), frozen.GetState()) {
		return observedCutoverScheduleV3{}, types.NewAppError(types.CodeConflict,
			"research V3 cutover Schedule state changed independently", types.ErrConflict)
	}
	return observedCutoverScheduleV3{target: isTarget, paused: current.GetState().GetPaused()}, nil
}

func stateEqualsExceptPausedV3(left, right *schedulepb.ScheduleState) bool {
	l := proto.Clone(left)
	r := proto.Clone(right)
	if l == nil {
		l = &schedulepb.ScheduleState{}
	}
	if r == nil {
		r = &schedulepb.ScheduleState{}
	}
	ls := l.(*schedulepb.ScheduleState)
	rs := r.(*schedulepb.ScheduleState)
	ls.Paused, rs.Paused = false, false
	return proto.Equal(ls, rs)
}

func (c *researchV3CutoverCoordinator) casState(
	ctx context.Context, op types.ResearchV3CutoverOperation,
	desc *workflowservice.DescribeScheduleResponse, base *schedulepb.Schedule,
	expectedPaused, targetPaused bool, step string,
) error {
	if desc.GetSchedule().GetState().GetPaused() != expectedPaused {
		return types.NewAppError(types.CodeConflict,
			"research V3 cutover Schedule state raced", types.ErrConflict)
	}
	next := proto.Clone(base).(*schedulepb.Schedule)
	if next.State == nil {
		next.State = &schedulepb.ScheduleState{}
	}
	next.State.Paused = targetPaused
	return c.remote.CompareAndSwap(ctx, op.TaskID, next,
		desc.GetConflictToken(), researchV3CutoverRequestID(op, step))
}

func (c *researchV3CutoverCoordinator) casAction(
	ctx context.Context, op types.ResearchV3CutoverOperation,
	desc *workflowservice.DescribeScheduleResponse, base *schedulepb.Schedule,
	target *schedulepb.ScheduleAction, step string,
) error {
	if !desc.GetSchedule().GetState().GetPaused() {
		return types.NewAppError(types.CodeConflict,
			"research V3 cutover Action CAS requires paused Schedule", types.ErrConflict)
	}
	next := proto.Clone(base).(*schedulepb.Schedule)
	next.Action = proto.Clone(target).(*schedulepb.ScheduleAction)
	if next.State == nil {
		next.State = &schedulepb.ScheduleState{}
	}
	next.State.Paused = true
	return c.remote.CompareAndSwap(ctx, op.TaskID, next,
		desc.GetConflictToken(), researchV3CutoverRequestID(op, step))
}

func (c *researchV3CutoverCoordinator) advance(
	ctx context.Context, op types.ResearchV3CutoverOperation,
	expected, next types.ResearchV3CutoverPhase,
) (types.ResearchV3CutoverOperation, error) {
	advanced, err := c.journal.AdvanceResearchV3Cutover(ctx, op, expected, next)
	if err != nil {
		return op, err
	}
	return advanced, nil
}

func cloneDescribedScheduleV3(
	desc *workflowservice.DescribeScheduleResponse,
) (*schedulepb.Schedule, error) {
	if desc == nil || desc.GetSchedule() == nil || desc.GetSchedule().GetAction() == nil ||
		len(desc.GetConflictToken()) == 0 {
		return nil, types.NewAppError(types.CodeConflict,
			"research V3 cutover Describe is incomplete", types.ErrConflict)
	}
	return proto.Clone(desc.GetSchedule()).(*schedulepb.Schedule), nil
}

func targetScheduleV3(
	frozen *schedulepb.Schedule, target *schedulepb.ScheduleAction,
) *schedulepb.Schedule {
	result := proto.Clone(frozen).(*schedulepb.Schedule)
	result.Action = proto.Clone(target).(*schedulepb.ScheduleAction)
	return result
}

func chooseObservedScheduleV3(
	frozen *schedulepb.Schedule, target *schedulepb.ScheduleAction, targetObserved bool,
) *schedulepb.Schedule {
	if targetObserved {
		return targetScheduleV3(frozen, target)
	}
	return frozen
}

func marshalScheduleArtifactV3(message proto.Message) ([]byte, string, error) {
	payload, err := (proto.MarshalOptions{Deterministic: true}).Marshal(message)
	if err != nil || len(payload) == 0 {
		return nil, "", types.NewAppError(types.CodeInternal,
			"encode research V3 cutover artifact", err)
	}
	digest := sha256.Sum256(payload)
	return payload, hex.EncodeToString(digest[:]), nil
}

func decodeCutoverArtifactsV3(
	op types.ResearchV3CutoverOperation,
) (*schedulepb.Schedule, *schedulepb.ScheduleAction, error) {
	check := func(payload []byte, expected string) bool {
		digest := sha256.Sum256(payload)
		return hex.EncodeToString(digest[:]) == expected
	}
	if !check(op.FrozenSchedule, op.FrozenScheduleDigest) ||
		!check(op.FrozenConflictToken, op.ConflictTokenDigest) ||
		!check(op.TargetAction, op.TargetActionDigest) {
		return nil, nil, types.NewAppError(types.CodeValidation,
			"research V3 cutover journal digest mismatch", types.ErrValidation)
	}
	frozen := new(schedulepb.Schedule)
	target := new(schedulepb.ScheduleAction)
	if err := proto.Unmarshal(op.FrozenSchedule, frozen); err != nil {
		return nil, nil, types.NewAppError(types.CodeValidation,
			"decode research V3 frozen Schedule", err)
	}
	if err := proto.Unmarshal(op.TargetAction, target); err != nil {
		return nil, nil, types.NewAppError(types.CodeValidation,
			"decode research V3 target Action", err)
	}
	return frozen, target, nil
}

func researchV3CutoverRequestID(op types.ResearchV3CutoverOperation, step string) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf(
		"vane/research-v3-cutover/%d/%d/%s", op.ID, op.Generation, step)))
	return hex.EncodeToString(digest[:])
}

func cutoverRemoteError(operation string, err error) error {
	if err == nil {
		err = errors.New("remote operation failed")
	}
	return types.NewAppError(types.CodeInternal,
		"research V3 cutover "+operation+" failed", err)
}

type schedulerResearchV3ScheduleRemote struct {
	scheduler *Scheduler
}

func (r *schedulerResearchV3ScheduleRemote) Describe(
	ctx context.Context, taskID string,
) (*workflowservice.DescribeScheduleResponse, error) {
	return r.scheduler.describeResearchV3CutoverSchedule(ctx, taskID)
}

func (s *Scheduler) describeResearchV3CutoverSchedule(
	ctx context.Context, taskID string,
) (*workflowservice.DescribeScheduleResponse, error) {
	return s.c.WorkflowService().DescribeSchedule(
		ctx, &workflowservice.DescribeScheduleRequest{
			Namespace: s.taskScheduleEnv.namespace, ScheduleId: taskID,
		})
}

func (r *schedulerResearchV3ScheduleRemote) CompareAndSwap(
	ctx context.Context, taskID string, schedule *schedulepb.Schedule,
	conflictToken []byte, requestID string,
) error {
	if schedule == nil || len(conflictToken) == 0 {
		return types.ErrValidation
	}
	return r.scheduler.compareAndSwapResearchV3CutoverSchedule(
		ctx, taskID, schedule, conflictToken, requestID)
}

func (s *Scheduler) compareAndSwapResearchV3CutoverSchedule(
	ctx context.Context, taskID string, schedule *schedulepb.Schedule,
	conflictToken []byte, requestID string,
) error {
	_, err := s.c.WorkflowService().UpdateSchedule(
		ctx, &workflowservice.UpdateScheduleRequest{
			Namespace: s.taskScheduleEnv.namespace, ScheduleId: taskID,
			Schedule: schedule, ConflictToken: append([]byte(nil), conflictToken...),
			RequestId: requestID, Identity: "vane/research-v3-exact-cutover/v1",
		})
	return err
}
