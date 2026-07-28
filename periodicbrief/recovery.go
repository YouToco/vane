package periodicbrief

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"

	"github.com/YouToco/vane/executivebrief"
	"github.com/YouToco/vane/pusheffect"
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
	DeliveryStore
	ListPeriodicSynthesisRecoveryCandidatesV1(
		context.Context, *store.PeriodicSynthesisRecoveryCursorV1, int,
	) ([]store.PeriodicSynthesisRecoveryCandidateV1, error)
	LoadPeriodicBriefIntentInputsForRecoveryV1(
		context.Context, int64, int64, int64,
	) (store.PeriodicBriefIntentInputsV1, error)
	GetProfileForTenant(
		context.Context, int64, int64,
	) (*types.Profile, error)
	LoadPeriodicSynthesisPolicyV1(
		context.Context, int64, int64, string, int64,
	) (store.PeriodicSynthesisPolicyV1, error)
	ClaimPeriodicSynthesisSpendV1(
		context.Context, int64, int64, int64, string,
		int64, int64, string, string,
	) (store.PeriodicSynthesisReceiptV1, bool, error)
	RecoverPeriodicBriefReportV1(
		context.Context, int64, int64, int64, string,
		types.PeriodicBriefReportDraftV1,
	) (types.PeriodicBriefReportV1, error)
	ListPeriodicDeliveryRecoveryCandidatesV1(
		context.Context, *store.PeriodicDeliveryRecoveryCursorV1, int,
	) ([]store.PeriodicDeliveryRecoveryCandidateV1, error)
	ListPeriodicMissingDeliveryReportsV1(
		context.Context, int64, int,
	) ([]types.PeriodicBriefReportV1, error)
}

type WorkflowExecutionDescriber interface {
	DescribeWorkflowExecution(
		context.Context, string, string,
	) (*workflowservice.DescribeWorkflowExecutionResponse, error)
}

type RecoveryRunner struct {
	store          RecoveryStore
	temporal       WorkflowExecutionDescriber
	sender         DeliveryRecoverySender
	origin         string
	deliveryTaskID string
	logger         *slog.Logger
	pass           chan struct{}
}

type DeliveryRecoverySender interface {
	DeliverySender
	ResolvePeriodicReportMessage(
		context.Context, pusheffect.HistoryQuery,
	) (pusheffect.HistoryObservation, error)
}

func NewRecoveryRunner(
	st RecoveryStore,
	temporal WorkflowExecutionDescriber,
	sender DeliveryRecoverySender,
	dashboardOrigin string,
	deliveryTaskID string,
	logger *slog.Logger,
) (*RecoveryRunner, error) {
	if st == nil || temporal == nil || sender == nil ||
		dashboardOrigin == "" {
		return nil, errors.New("periodic Brief recovery store is missing")
	}
	if logger == nil {
		logger = slog.Default()
	}
	runner := &RecoveryRunner{
		store: st, temporal: temporal, sender: sender,
		origin:         dashboardOrigin,
		deliveryTaskID: strings.TrimSpace(deliveryTaskID),
		logger:         logger,
		pass:           make(chan struct{}, 1)}
	runner.pass <- struct{}{}
	return runner, nil
}

func (r *RecoveryRunner) RunStartup(ctx context.Context) error {
	return r.runPass(ctx)
}

func (r *RecoveryRunner) Run(ctx context.Context) {
	ticker := time.NewTicker(RecoveryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.runPass(ctx); err != nil &&
				!errors.Is(err, context.Canceled) {
				r.logger.WarnContext(ctx,
					"periodic Brief recovery pass failed",
					"error_code", types.CodeOf(err))
			}
		}
	}
}

func (r *RecoveryRunner) runPass(parent context.Context) error {
	select {
	case <-parent.Done():
		return parent.Err()
	case <-r.pass:
		defer func() { r.pass <- struct{}{} }()
	}
	ctx, cancel := context.WithTimeout(parent, RecoveryPassTimeout)
	defer cancel()
	var cursor *store.PeriodicSynthesisRecoveryCursorV1
	var allErrs []error
	for {
		candidates, err :=
			r.store.ListPeriodicSynthesisRecoveryCandidatesV1(
				ctx, cursor, RecoveryPageSize)
		if err != nil {
			allErrs = append(allErrs, err)
			break
		}
		if len(candidates) == 0 {
			break
		}
		sem := make(chan struct{}, RecoveryConcurrency)
		var wg sync.WaitGroup
		var errMu sync.Mutex
		for _, candidate := range candidates {
			select {
			case <-ctx.Done():
				allErrs = append(allErrs, ctx.Err())
				goto drain
			case sem <- struct{}{}:
			}
			wg.Add(1)
			go func(candidate store.PeriodicSynthesisRecoveryCandidateV1) {
				defer wg.Done()
				defer func() { <-sem }()
				if recoverErr := r.recoverOne(ctx, candidate); recoverErr != nil {
					errMu.Lock()
					allErrs = append(allErrs, recoverErr)
					errMu.Unlock()
				}
			}(candidate)
		}
	drain:
		wg.Wait()
		last := candidates[len(candidates)-1]
		cursor = &store.PeriodicSynthesisRecoveryCursorV1{
			CandidateAt: last.CandidateAt, IntentID: last.IntentID}
		if len(candidates) < RecoveryPageSize || ctx.Err() != nil {
			break
		}
	}
	if err := r.runMissingDeliveryPass(ctx); err != nil {
		allErrs = append(allErrs, err)
	}
	if err := r.runDeliveryPass(ctx); err != nil {
		allErrs = append(allErrs, err)
	}
	return errors.Join(allErrs...)
}

func (r *RecoveryRunner) runMissingDeliveryPass(ctx context.Context) error {
	var afterReportID int64
	var errs []error
	for {
		reports, err := r.store.ListPeriodicMissingDeliveryReportsV1(
			ctx, afterReportID, RecoveryPageSize)
		if err != nil {
			return err
		}
		if len(reports) == 0 {
			break
		}
		sem := make(chan struct{}, RecoveryConcurrency)
		var wg sync.WaitGroup
		var errMu sync.Mutex
		for _, report := range reports {
			select {
			case <-ctx.Done():
				errs = append(errs, ctx.Err())
				goto drain
			case sem <- struct{}{}:
			}
			wg.Add(1)
			go func(report types.PeriodicBriefReportV1) {
				defer wg.Done()
				defer func() { <-sem }()
				if report.TaskID != r.deliveryTaskID {
					return
				}
				if recoverErr := deliverPeriodicBriefV1(
					ctx, report, r.store, r.sender, r.origin,
				); recoverErr != nil {
					errMu.Lock()
					errs = append(errs, recoverErr)
					errMu.Unlock()
				}
			}(report)
		}
	drain:
		wg.Wait()
		afterReportID = reports[len(reports)-1].ID
		if len(reports) < RecoveryPageSize || ctx.Err() != nil {
			break
		}
	}
	return errors.Join(errs...)
}

func (r *RecoveryRunner) runDeliveryPass(ctx context.Context) error {
	var cursor *store.PeriodicDeliveryRecoveryCursorV1
	var errs []error
	for {
		candidates, err :=
			r.store.ListPeriodicDeliveryRecoveryCandidatesV1(
				ctx, cursor, RecoveryPageSize)
		if err != nil {
			return err
		}
		if len(candidates) == 0 {
			break
		}
		sem := make(chan struct{}, RecoveryConcurrency)
		var wg sync.WaitGroup
		var errMu sync.Mutex
		for _, candidate := range candidates {
			select {
			case <-ctx.Done():
				errs = append(errs, ctx.Err())
				goto drain
			case sem <- struct{}{}:
			}
			wg.Add(1)
			go func(candidate store.PeriodicDeliveryRecoveryCandidateV1) {
				defer wg.Done()
				defer func() { <-sem }()
				if recoverErr := r.recoverDeliveryOne(
					ctx, candidate,
				); recoverErr != nil {
					errMu.Lock()
					errs = append(errs, recoverErr)
					errMu.Unlock()
				}
			}(candidate)
		}
	drain:
		wg.Wait()
		last := candidates[len(candidates)-1]
		cursor = &store.PeriodicDeliveryRecoveryCursorV1{
			UpdatedAt: last.UpdatedAt, ReportID: last.ReportID}
		if len(candidates) < RecoveryPageSize || ctx.Err() != nil {
			break
		}
	}
	return errors.Join(errs...)
}

func (r *RecoveryRunner) recoverDeliveryOne(
	ctx context.Context,
	candidate store.PeriodicDeliveryRecoveryCandidateV1,
) error {
	delivery := candidate.PeriodicReportDeliveryV1
	if delivery.Status == store.PeriodicReportDeliveryPrepared {
		claimed, authority, err :=
			r.store.ClaimPeriodicReportDeliveryV1(
				ctx, delivery.TenantID, delivery.UserID,
				delivery.ReportID)
		if err != nil || !authority {
			return err
		}
		observation, sendErr := r.sender.SendCardWithUUIDResult(
			ctx, claimed.AppIdentity, claimed.TargetOpenID,
			string(claimed.CardPayload), claimed.ProviderUUID)
		switch observation.Disposition {
		case pusheffect.AttemptSent:
			return r.store.FinalizePeriodicReportDeliveryV1(
				ctx, claimed.TenantID, claimed.UserID,
				claimed.ReportID, store.PeriodicReportDeliverySent,
				observation.MessageID)
		case pusheffect.AttemptDefiniteNotSent:
			finalizeErr := r.store.FinalizePeriodicReportDeliveryV1(
				ctx, claimed.TenantID, claimed.UserID,
				claimed.ReportID, store.PeriodicReportDeliveryPrepared,
				"")
			return errors.Join(sendErr, finalizeErr)
		default:
			// An unknown boundary crossing remains sending. Neither an adapter
			// error nor absence from provider history proves no send occurred.
			if sendErr != nil {
				return types.NewAppError(
					types.CodeOf(sendErr),
					"周期报告推送结果未完全确认", nil)
			}
			return types.NewAppError(
				types.CodePushFailed,
				"周期报告推送结果未完全确认", nil)
		}
	}
	if delivery.Status != store.PeriodicReportDeliverySending ||
		delivery.AttemptStartedAt == nil {
		return nil
	}
	if delivery.ProviderChatID != "" {
		historyCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		observation, err := r.sender.ResolvePeriodicReportMessage(
			historyCtx, pusheffect.HistoryQuery{
				EffectID:       delivery.ProviderUUID,
				ProviderChatID: delivery.ProviderChatID,
				AppIdentity:    delivery.AppIdentity,
				CardDigest:     delivery.CardDigest,
				StartTime:      delivery.AttemptStartedAt.Add(-time.Minute),
				EndTime:        delivery.AttemptStartedAt.Add(time.Hour),
			})
		cancel()
		if err == nil && observation.MatchCount == 1 {
			return r.store.FinalizePeriodicReportDeliveryV1(
				ctx, delivery.TenantID, delivery.UserID,
				delivery.ReportID, store.PeriodicReportDeliverySent,
				observation.MessageID)
		}
		if err != nil {
			return nil
		}
		if observation.MatchCount == 0 && observation.MessageID == "" {
			// The protocol deliberately treats empty history as no positive
			// evidence, not as proof that the original send failed.
			return nil
		}
		if observation.MatchCount != 1 || observation.MessageID == "" {
			return r.store.FinalizePeriodicReportDeliveryV1(
				ctx, delivery.TenantID, delivery.UserID,
				delivery.ReportID, store.PeriodicReportDeliveryAmbiguous,
				"")
		}
	}
	return nil
}

func (r *RecoveryRunner) recoverOne(
	ctx context.Context,
	candidate store.PeriodicSynthesisRecoveryCandidateV1,
) error {
	if candidate.Kind == "prepared" {
		describeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		description, err := r.temporal.DescribeWorkflowExecution(
			describeCtx, candidate.WorkflowID, candidate.TemporalRunID)
		cancel()
		if err != nil {
			// A missing execution means the coordinator may safely retry the
			// exact Workflow ID; it is not proof of a terminal run.
			return nil
		}
		info := description.GetWorkflowExecutionInfo()
		if info == nil || info.GetExecution() == nil {
			return types.NewAppError(types.CodeConflict,
				"周期报告 Temporal 执行信息不完整", nil)
		}
		if candidate.TemporalRunID != "" &&
			info.GetExecution().GetRunId() != candidate.TemporalRunID {
			return types.NewAppError(types.CodeConflict,
				"周期报告 Temporal Run 已不同", nil)
		}
		if info.GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
			return nil
		}
	}
	loaded, err := r.store.LoadPeriodicBriefIntentInputsForRecoveryV1(
		ctx, candidate.TenantID, candidate.UserID, candidate.IntentID)
	if err != nil {
		return err
	}
	profile := executivebrief.ProfileContextV1{
		Epoch: candidate.ProfileEpoch, Version: candidate.ProfileVersion}
	if candidate.Kind == "prepared" {
		storedProfile, profileErr := r.store.GetProfileForTenant(
			ctx, candidate.TenantID, candidate.UserID)
		if profileErr == nil {
			profile = executivebrief.ProfileContextV1{
				Epoch:      storedProfile.ProfileEpoch,
				Version:    storedProfile.ProfileVersion,
				Industry:   storedProfile.Industry,
				Occupation: storedProfile.Occupation,
				Tags:       append([]string(nil), storedProfile.Tags...),
				Summary:    storedProfile.Summary,
			}
		} else if types.CodeOf(profileErr) != types.CodeNotFound {
			return profileErr
		}
		candidate.ProfileDigest, err =
			executivebrief.ProfileDigestV1(profile)
		if err != nil {
			return err
		}
	}
	_, selected, partial, err := executivebrief.BuildPeriodicPromptV1(
		loaded.Intent.TaskID, profile,
		loaded.Intent.PeriodStart, loaded.Intent.PeriodEnd, loaded.Briefs)
	if err != nil {
		return err
	}
	if candidate.Kind == "prepared" {
		var policy store.PeriodicSynthesisPolicyV1
		if len(selected) > 0 {
			policy, err = r.store.LoadPeriodicSynthesisPolicyV1(
				ctx, candidate.TenantID, candidate.UserID,
				loaded.Intent.TaskID, selected[0].RunSnapshotID)
			if types.CodeOf(err) == types.CodeNotFound {
				err = nil
			}
			if err != nil {
				return err
			}
		}
		candidate.InputDigest = loaded.Intent.InputDigest
		candidate.RequestDigest, err = synthesisRequestDigestV1(
			candidate.InputDigest, candidate.ProfileDigest, policy)
		if err != nil {
			return err
		}
		_, claimed, claimErr :=
			r.store.ClaimPeriodicSynthesisSpendV1(
				ctx, candidate.TenantID, candidate.UserID,
				candidate.IntentID, candidate.RequestDigest,
				profile.Epoch, profile.Version,
				candidate.ProfileDigest, candidate.InputDigest)
		if claimErr != nil {
			if errors.Is(claimErr, types.ErrConflict) {
				return nil
			}
			return claimErr
		}
		if !claimed {
			return nil
		}
	}
	var content types.ExecutiveBriefContentV1
	if len(selected) == 0 {
		content = quietContentV1()
	} else {
		content, err =
			executivebrief.DeterministicPeriodicFallbackV1(selected)
		if err != nil {
			return err
		}
	}
	inputs := make([]types.PeriodicBriefInputV1, len(selected))
	for index, brief := range selected {
		inputs[index] = types.PeriodicBriefInputV1{
			BriefID: brief.ID, Digest: brief.Digest}
	}
	processing := types.RunCompletenessPartial
	if len(selected) == 0 && !partial &&
		loaded.Intent.Processing == types.RunCompletenessComplete {
		// No provider call happened for a quiet period, but a stale spending
		// marker still means synthesis authority was interrupted.
		processing = types.RunCompletenessPartial
	}
	draft, err := (types.PeriodicBriefReportDraftV1{
		SchemaVersion: types.PeriodicBriefSchemaVersionV1,
		TenantID:      candidate.TenantID, UserID: candidate.UserID,
		TaskID:         loaded.Intent.TaskID,
		Cadence:        string(loaded.Intent.Cadence),
		Timezone:       loaded.Intent.Timezone,
		PeriodStart:    loaded.Intent.PeriodStart,
		PeriodEnd:      loaded.Intent.PeriodEnd,
		GeneratedAt:    time.Now().Round(0).UTC().Truncate(time.Microsecond),
		ProfileEpoch:   candidate.ProfileEpoch,
		ProfileVersion: candidate.ProfileVersion,
		ProfileDigest:  candidate.ProfileDigest,
		InputDigest:    candidate.InputDigest,
		Inputs:         inputs,
		RunOutcomeIDs: append(
			[]int64(nil), loaded.Intent.RunOutcomeIDs...),
		OutcomeDigest:  loaded.Intent.OutcomeDigest,
		GenerationMode: types.ExecutiveGenerationFallback,
		SourceCoverage: loaded.Intent.SourceCoverage,
		Processing:     processing,
		Content:        content,
	}).Canonical()
	if err != nil {
		return err
	}
	_, err = r.store.RecoverPeriodicBriefReportV1(
		ctx, candidate.TenantID, candidate.UserID,
		candidate.IntentID, candidate.RequestDigest, draft)
	if errors.Is(err, types.ErrConflict) {
		return nil
	}
	return err
}
