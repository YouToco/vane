package periodicbrief

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/YouToco/vane/server/executivebrief"
	"github.com/YouToco/vane/server/llm"
	"github.com/YouToco/vane/server/runtimepolicy"
	"github.com/YouToco/vane/server/store"
	"github.com/YouToco/vane/server/types"
)

const WorkflowNameV1 = "PeriodicBriefWorkflowV1"

type WorkflowInputV1 struct {
	IntentID int64 `json:"intent_id"`
	TenantID int64 `json:"tenant_id"`
	UserID   int64 `json:"user_id"`
}

type SynthesizeInputV1 struct {
	WorkflowInputV1
	TraceID string `json:"trace_id"`
}

type Store interface {
	LoadPeriodicBriefIntentInputsV1(
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
	FinalizePeriodicBriefReportV1(
		context.Context, int64, int64, int64, string,
		types.PeriodicBriefReportDraftV1, bool,
	) (types.PeriodicBriefReportV1, error)
	AuthorizeAndConsumePeriodicSynthesisQuotaV1(
		context.Context, int64, int64, int64, float64,
	) error
}

type ModelResolver interface {
	ResolveRuntimeModelPolicyV1(
		runtimepolicy.ModelPolicyV1,
	) (*llm.Client, error)
}

type Activities struct {
	store             Store
	modelResolver     ModelResolver
	recorder          *llm.Recorder
	deliveryStore     DeliveryStore
	sender            DeliverySender
	dashboardOrigin   string
	deliveryTaskID    string
	channelMu         sync.RWMutex
	channelDispatcher ChannelDispatcher
}

func NewActivities(
	st Store,
	modelResolver ModelResolver,
	recorder *llm.Recorder,
	sender DeliverySender,
	dashboardOrigin string,
	deliveryTaskID string,
) (*Activities, error) {
	deliveryStore, ok := st.(DeliveryStore)
	if st == nil || modelResolver == nil || recorder == nil ||
		!ok || sender == nil || dashboardOrigin == "" {
		return nil, errors.New("periodic Brief dependencies are incomplete")
	}
	return &Activities{
		store: st, modelResolver: modelResolver, recorder: recorder,
		deliveryStore: deliveryStore, sender: sender,
		dashboardOrigin: dashboardOrigin,
		deliveryTaskID:  strings.TrimSpace(deliveryTaskID)}, nil
}

// SetChannelDispatcher is called during process composition before the
// Temporal worker starts. Workflows depend only on the provider-neutral
// dispatcher and cannot obtain a Telegram client or credential.
func (a *Activities) SetChannelDispatcher(dispatcher ChannelDispatcher) {
	a.channelMu.Lock()
	defer a.channelMu.Unlock()
	a.channelDispatcher = dispatcher
}

func (a *Activities) getChannelDispatcher() ChannelDispatcher {
	a.channelMu.RLock()
	defer a.channelMu.RUnlock()
	return a.channelDispatcher
}

func WorkflowV1(
	ctx workflow.Context,
	input WorkflowInputV1,
) (types.PeriodicBriefReportV1, error) {
	if input.IntentID <= 0 || input.TenantID <= 0 || input.UserID <= 0 {
		return types.PeriodicBriefReportV1{},
			temporal.NewNonRetryableApplicationError(
				"周期报告输入无效", "validation", nil)
	}
	info := workflow.GetInfo(ctx)
	options := workflow.ActivityOptions{
		StartToCloseTimeout: 2 * time.Minute,
		HeartbeatTimeout:    20 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 1,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, options)
	var report types.PeriodicBriefReportV1
	err := workflow.ExecuteActivity(
		ctx, "SynthesizePeriodicBriefV1",
		SynthesizeInputV1{
			WorkflowInputV1: input,
			TraceID:         info.WorkflowExecution.RunID,
		},
	).Get(ctx, &report)
	if err != nil {
		return report, err
	}
	// Delivery is a separately durable effect. A channel failure never removes
	// the already-frozen Web report; recovery continues from its receipt.
	_ = workflow.ExecuteActivity(
		ctx, "DeliverPeriodicBriefV1",
		DeliverInputV1{Report: report},
	).Get(ctx, nil)
	return report, nil
}

func (a *Activities) SynthesizePeriodicBriefV1(
	ctx context.Context,
	input SynthesizeInputV1,
) (types.PeriodicBriefReportV1, error) {
	if input.IntentID <= 0 || input.TenantID <= 0 ||
		input.UserID <= 0 || input.TraceID == "" {
		return types.PeriodicBriefReportV1{},
			types.NewAppError(types.CodeValidation,
				"周期报告综合输入无效", nil)
	}
	loaded, err := a.store.LoadPeriodicBriefIntentInputsV1(
		ctx, input.TenantID, input.UserID, input.IntentID)
	if err != nil {
		return types.PeriodicBriefReportV1{}, err
	}
	profile := executivebrief.ProfileContextV1{}
	storedProfile, profileErr := a.store.GetProfileForTenant(
		ctx, input.TenantID, input.UserID)
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
		return types.PeriodicBriefReportV1{}, profileErr
	}
	profileDigest, err := executivebrief.ProfileDigestV1(profile)
	if err != nil {
		return types.PeriodicBriefReportV1{}, err
	}
	prompt, selected, selectionPartial, promptErr :=
		executivebrief.BuildPeriodicPromptV1(
			loaded.Intent.TaskID, profile,
			loaded.Intent.PeriodStart, loaded.Intent.PeriodEnd,
			loaded.Briefs)
	var policy store.PeriodicSynthesisPolicyV1
	policyUnavailable := false
	if len(selected) > 0 && promptErr == nil {
		policy, err = a.store.LoadPeriodicSynthesisPolicyV1(
			ctx, input.TenantID, input.UserID, loaded.Intent.TaskID,
			selected[0].RunSnapshotID)
		if types.CodeOf(err) == types.CodeNotFound {
			policyUnavailable = true
			err = nil
		}
		if err != nil {
			return types.PeriodicBriefReportV1{}, err
		}
	}
	requestDigest, err := synthesisRequestDigestV1(
		loaded.Intent,
		loaded.Intent.InputDigest, profileDigest, policy)
	if err != nil {
		return types.PeriodicBriefReportV1{}, err
	}
	receipt, claimed, err := a.store.ClaimPeriodicSynthesisSpendV1(
		ctx, input.TenantID, input.UserID, input.IntentID,
		requestDigest, profile.Epoch, profile.Version,
		profileDigest, loaded.Intent.InputDigest)
	if err != nil {
		return types.PeriodicBriefReportV1{}, err
	}
	if !claimed {
		if receipt.Status == store.ExecutiveSynthesisSpending {
			return types.PeriodicBriefReportV1{},
				types.NewAppError(types.CodeConflict,
					"周期综合已领取，等待恢复", nil)
		}
		return types.PeriodicBriefReportV1{},
			types.NewAppError(types.CodeConflict,
				"周期综合已结束", nil)
	}
	fallback := promptErr != nil || len(selected) == 0 || policyUnavailable
	var content types.ExecutiveBriefContentV1
	if len(selected) == 0 {
		content = quietContentV1()
	} else if promptErr != nil {
		content, err =
			executivebrief.DeterministicPeriodicFallbackV1(selected)
		if err != nil {
			return types.PeriodicBriefReportV1{}, err
		}
	} else {
		modelClient, resolveErr :=
			a.modelResolver.ResolveRuntimeModelPolicyV1(policy.ModelPolicy)
		if resolveErr != nil {
			fallback = true
			content, err =
				executivebrief.DeterministicPeriodicFallbackV1(selected)
			if err != nil {
				return types.PeriodicBriefReportV1{}, err
			}
		}
		if fallback {
			goto synthesisComplete
		}
		temperature := float32(policy.ModelCall.Temperature)
		maxTokens := policy.ModelCall.MaxTokens
		tenantID, userID := input.TenantID, input.UserID
		quotaRule := policy.QuotaRule
		response, callErr := llm.Do(
			ctx, modelClient, a.recorder,
			llm.CallMeta{
				TraceID: input.TraceID, TenantID: &tenantID,
				UserID:    &userID,
				SpanName:  runtimepolicy.ModelStagePeriodicSynthesis,
				QuotaRule: &quotaRule,
				BeforeSpend: func(
					effectCtx context.Context,
					amount float64,
				) error {
					return a.store.
						AuthorizeAndConsumePeriodicSynthesisQuotaV1(
							effectCtx, input.TenantID, input.UserID,
							input.IntentID, amount)
				},
			},
			llm.Request{
				System: policy.SystemPrompt,
				User:   prompt, Model: policy.ModelCall.Model,
				Temperature: &temperature, MaxTokens: &maxTokens,
				DisableThinking: policy.ModelCall.DisableThinking,
			},
		)
		if callErr == nil {
			content, err = executivebrief.ParsePeriodicContentV1(
				[]byte(response.Content), selected)
		}
		if callErr != nil || err != nil {
			fallback = true
			content, err =
				executivebrief.DeterministicPeriodicFallbackV1(selected)
			if err != nil {
				return types.PeriodicBriefReportV1{}, err
			}
		}
	}
synthesisComplete:
	processing := loaded.Intent.Processing
	if fallback || selectionPartial {
		processing = types.RunCompletenessPartial
	}
	if len(selected) == 0 && !selectionPartial &&
		loaded.Intent.Processing == types.RunCompletenessComplete {
		processing = types.RunCompletenessComplete
	}
	inputs := make([]types.PeriodicBriefInputV1, len(selected))
	for index, brief := range selected {
		inputs[index] = types.PeriodicBriefInputV1{
			BriefID: brief.ID, Digest: brief.Digest}
	}
	draft := types.PeriodicBriefReportDraftV1{
		SchemaVersion: types.PeriodicBriefSchemaVersionV1,
		TenantID:      input.TenantID, UserID: input.UserID,
		TaskID:         loaded.Intent.TaskID,
		Cadence:        string(loaded.Intent.Cadence),
		Timezone:       loaded.Intent.Timezone,
		PeriodStart:    loaded.Intent.PeriodStart,
		PeriodEnd:      loaded.Intent.PeriodEnd,
		GeneratedAt:    time.Now().Round(0).UTC().Truncate(time.Microsecond),
		ProfileEpoch:   profile.Epoch,
		ProfileVersion: profile.Version,
		ProfileDigest:  profileDigest,
		InputDigest:    loaded.Intent.InputDigest,
		Inputs:         inputs,
		RunOutcomeIDs: append(
			[]int64(nil), loaded.Intent.RunOutcomeIDs...),
		OutcomeDigest:  loaded.Intent.OutcomeDigest,
		GenerationMode: types.ExecutiveGenerationModel,
		SourceCoverage: loaded.Intent.SourceCoverage,
		Processing:     processing,
		Content:        content,
	}
	if fallback {
		draft.GenerationMode = types.ExecutiveGenerationFallback
	}
	canonicalDraft, err := draft.Canonical()
	if err != nil {
		return types.PeriodicBriefReportV1{}, err
	}
	draft = canonicalDraft
	return a.store.FinalizePeriodicBriefReportV1(
		ctx, input.TenantID, input.UserID, input.IntentID,
		requestDigest, draft, fallback)
}

func synthesisRequestDigestV1(
	intent store.PeriodicBriefIntentV1,
	inputDigest, profileDigest string,
	policy store.PeriodicSynthesisPolicyV1,
) (string, error) {
	taskID := strings.TrimSpace(intent.TaskID)
	timezone := strings.TrimSpace(intent.Timezone)
	cadence := strings.TrimSpace(string(intent.Cadence))
	periodStart := intent.PeriodStart.Round(0).UTC().
		Truncate(time.Microsecond)
	periodEnd := intent.PeriodEnd.Round(0).UTC().
		Truncate(time.Microsecond)
	if taskID == "" || timezone == "" || cadence == "" ||
		!periodStart.Before(periodEnd) {
		return "", errors.New("periodic synthesis request scope is invalid")
	}
	renderer, model, policyDigest := "periodic-brief.quiet/v1", "none", ""
	if policy.PolicyDigest != "" {
		renderer, model, policyDigest =
			policy.Renderer, policy.ModelCall.Model, policy.PolicyDigest
	}
	payload, err := json.Marshal(struct {
		SchemaVersion string `json:"schema_version"`
		TaskID        string `json:"task_id"`
		Cadence       string `json:"cadence"`
		Timezone      string `json:"timezone"`
		PeriodStart   string `json:"period_start"`
		PeriodEnd     string `json:"period_end"`
		InputDigest   string `json:"input_digest"`
		ProfileDigest string `json:"profile_digest"`
		Renderer      string `json:"renderer"`
		Model         string `json:"model"`
		PolicyDigest  string `json:"policy_digest,omitempty"`
	}{
		SchemaVersion: types.PeriodicBriefSchemaVersionV1,
		TaskID:        taskID,
		Cadence:       cadence,
		Timezone:      timezone,
		PeriodStart:   periodStart.Format(time.RFC3339Nano),
		PeriodEnd:     periodEnd.Format(time.RFC3339Nano),
		InputDigest:   inputDigest, ProfileDigest: profileDigest,
		Renderer: renderer, Model: model, PolicyDigest: policyDigest,
	})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return fmt.Sprintf("%x", sum[:]), nil
}

func quietContentV1() types.ExecutiveBriefContentV1 {
	return types.ExecutiveBriefContentV1{
		Headline:         "本期没有需要行动的新信号",
		ExecutiveSummary: "本周期已完成检查，没有形成达到证据门槛的新情报。",
		DecisionState:    types.ExecutiveDecisionNoAction,
		WhyForYou:        "当前无需额外操作；后续出现有依据的变化时会继续更新。",
		Signals:          []types.ExecutiveSignalV1{},
		NextSteps:        []types.ExecutiveNextStepV1{},
	}
}
