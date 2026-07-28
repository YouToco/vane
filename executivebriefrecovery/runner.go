package executivebriefrecovery

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/YouToco/vane/executivebrief"
	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
)

const (
	RecoveryInterval    = 30 * time.Second
	RecoveryPageSize    = 100
	RecoveryConcurrency = 4
	RecoveryPassTimeout = 30 * time.Second
)

type RecoveryStore interface {
	ListExecutiveSynthesisRecoveryCandidatesV1(
		context.Context, *store.ExecutiveSynthesisRecoveryCursorV1, int,
	) ([]store.ExecutiveSynthesisRecoveryCandidateV1, error)
	LoadPreparedBriefDraftV1(
		context.Context, types.RunIdentity, types.RunSnapshotRef,
		types.RunOutcomeMarkerV1,
	) (types.BriefDraftV1, bool, error)
	LoadExecutiveSynthesisReceiptV1(
		context.Context, types.RunIdentity, types.RunSnapshotRef,
		types.RunOutcomeMarkerV1,
	) (store.ExecutiveSynthesisReceiptV1, error)
	RecoverExecutiveSynthesisFallbackV1(
		context.Context, types.RunIdentity, types.RunSnapshotRef,
		types.RunOutcomeMarkerV1, types.ExecutiveBriefContentV1,
	) (store.ExecutiveSynthesisReceiptV1, error)
	FreezeExecutiveBriefArtifactV1(
		context.Context, types.RunIdentity, types.RunSnapshotRef,
		types.ExecutiveBriefArtifactDraftV1,
	) (types.ExecutiveBriefArtifactV1, error)
}

type Runner struct {
	store  RecoveryStore
	logger *slog.Logger
	pass   chan struct{}
}

func NewRunner(
	st RecoveryStore, logger *slog.Logger,
) (*Runner, error) {
	if st == nil {
		return nil, errors.New(
			"executive Brief recovery store is missing")
	}
	if logger == nil {
		logger = slog.Default()
	}
	runner := &Runner{
		store: st, logger: logger, pass: make(chan struct{}, 1),
	}
	runner.pass <- struct{}{}
	return runner, nil
}

func (r *Runner) RunStartup(ctx context.Context) error {
	return r.runPass(ctx)
}

// Run stops admitting new work on cancellation and drains the current pass.
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
					"executive Brief recovery pass failed",
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
	var (
		wg       sync.WaitGroup
		errMu    sync.Mutex
		passErrs []error
		cursor   *store.ExecutiveSynthesisRecoveryCursorV1
	)
	recordErr := func(err error) {
		if err == nil {
			return
		}
		errMu.Lock()
		passErrs = append(passErrs, err)
		errMu.Unlock()
	}
	for {
		candidates, err :=
			r.store.ListExecutiveSynthesisRecoveryCandidatesV1(
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
			go func(candidate store.ExecutiveSynthesisRecoveryCandidateV1) {
				defer wg.Done()
				defer func() { <-sem }()
				recordErr(r.recoverOne(ctx, candidate))
			}(candidate)
		}
		last := candidates[len(candidates)-1]
		cursor = &store.ExecutiveSynthesisRecoveryCursorV1{
			CandidateAt: last.CandidateAt,
			OutcomeID:   last.Marker.ID,
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
	ctx context.Context,
	candidate store.ExecutiveSynthesisRecoveryCandidateV1,
) error {
	draft, found, err := r.store.LoadPreparedBriefDraftV1(
		ctx, candidate.Identity, candidate.Ref, candidate.Marker)
	if err != nil {
		return err
	}
	if !found {
		return types.NewAppError(types.CodeConflict,
			"executive Brief recovery draft is unavailable", nil)
	}
	receipt, err := r.store.LoadExecutiveSynthesisReceiptV1(
		ctx, candidate.Identity, candidate.Ref, candidate.Marker)
	if err != nil {
		return err
	}
	if candidate.Kind == "fallback" {
		content, fallbackErr := executivebrief.DeterministicFallbackV1(
			executivebrief.ProfileContextV1{}, draft)
		if fallbackErr != nil {
			return fallbackErr
		}
		receipt, err = r.store.RecoverExecutiveSynthesisFallbackV1(
			ctx, candidate.Identity, candidate.Ref,
			candidate.Marker, content)
		if err != nil {
			if errors.Is(err, types.ErrConflict) {
				return nil
			}
			return err
		}
	}
	if receipt.Status != store.ExecutiveSynthesisFinalized &&
		receipt.Status != store.ExecutiveSynthesisFallback {
		return nil
	}
	artifactDraft, err := artifactDraftFromReceipt(
		receipt, draft)
	if err != nil {
		return err
	}
	_, err = r.store.FreezeExecutiveBriefArtifactV1(
		ctx, candidate.Identity, candidate.Ref, artifactDraft)
	if errors.Is(err, types.ErrNotFound) ||
		errors.Is(err, types.ErrConflict) {
		// The normal outcome finalizer may not have promoted the canonical
		// Brief yet, or another writer may have frozen the artifact.
		return nil
	}
	return err
}

func artifactDraftFromReceipt(
	receipt store.ExecutiveSynthesisReceiptV1,
	draft types.BriefDraftV1,
) (types.ExecutiveBriefArtifactDraftV1, error) {
	if receipt.Content == nil || receipt.FinalizedAt == nil ||
		receipt.PushBatchID != draft.PushBatchID {
		return types.ExecutiveBriefArtifactDraftV1{},
			types.NewAppError(types.CodeConflict,
				"executive Brief recovery receipt is incomplete", nil)
	}
	artifact := types.ExecutiveBriefArtifactDraftV1{
		SchemaVersion:  types.ExecutiveBriefSchemaVersionV1,
		RunOutcomeID:   receipt.Marker.ID,
		RunSnapshotID:  receipt.Marker.RunSnapshotID,
		PushBatchID:    receipt.PushBatchID,
		TenantID:       receipt.Marker.TenantID,
		UserID:         receipt.Marker.UserID,
		TaskID:         receipt.Marker.TaskID,
		ProfileEpoch:   receipt.ProfileEpoch,
		ProfileVersion: receipt.ProfileVersion,
		ProfileDigest:  receipt.ProfileDigest,
		InputDigest:    receipt.InputDigest,
		GenerationMode: receipt.GenerationMode,
		Processing:     receipt.Processing,
		GeneratedAt:    *receipt.FinalizedAt,
		Content:        *receipt.Content,
	}
	if err := artifact.Validate(); err != nil {
		return types.ExecutiveBriefArtifactDraftV1{},
			types.NewAppError(types.CodeConflict,
				"executive Brief recovery artifact is invalid", err)
	}
	return artifact, nil
}
