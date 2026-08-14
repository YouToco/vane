package workflow

import (
	"context"

	"github.com/YouToco/vane/executivebrief"
	"github.com/YouToco/vane/llm"
	"github.com/YouToco/vane/runtimepolicy"
	storepkg "github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
)

// SynthesizeExecutiveBriefV1 performs at most one paid issue-level model call.
// The durable spend claim is committed before the provider request. Any known
// model/output failure converges to a deterministic fallback and never blocks
// the already-prepared canonical Brief.
func (a *Activities) SynthesizeExecutiveBriefV1(
	ctx context.Context,
	in ExecutiveBriefSynthesizeIn,
) (ExecutiveBriefSynthesizeResult, error) {
	if a.executiveBriefStore == nil || a.canonicalBriefStore == nil ||
		a.compiledStore == nil || a.llmRecorder == nil ||
		in.TraceID == "" || in.Draft.Validate() != nil {
		return ExecutiveBriefSynthesizeResult{}, nonRetryable(
			types.NewAppError(types.CodeValidation,
				"executive Brief synthesis input is invalid", nil))
	}
	expected, err := activityRunIdentityV1(ctx, in.UserID, &in.Run)
	if err != nil {
		return ExecutiveBriefSynthesizeResult{}, nonRetryable(err)
	}
	if err := validateExecutiveSynthesisEnvelopeV1(
		expected, in.Run.Snapshot, in.Marker, in.Draft); err != nil {
		return ExecutiveBriefSynthesizeResult{}, nonRetryable(err)
	}
	ctx = llm.WithRunSnapshotAttribution(
		ctx, in.Run.Snapshot.SnapshotID)
	snapshot, authority, err :=
		a.compiledStore.LoadAuthoritativeCompiledTaskRunSnapshot(
			ctx, expected, in.Run.Snapshot)
	if err != nil {
		return ExecutiveBriefSynthesizeResult{}, retryableOrNot(err)
	}
	a.logCompiledSnapshotAuthority(
		ctx, expected, in.Run.Snapshot, authority,
		"executive_brief_synthesis")
	if snapshot.Policy.PromptPolicy.IssueSynthesis == nil {
		return ExecutiveBriefSynthesizeResult{}, nonRetryable(
			types.NewAppError(types.CodeValidation,
				"executive Brief prompt policy is missing", nil))
	}
	modelCall, ok := snapshot.Policy.ModelPolicy.Call(
		runtimepolicy.ModelStageIssueSynthesis)
	if !ok {
		return ExecutiveBriefSynthesizeResult{}, nonRetryable(
			types.NewAppError(types.CodeValidation,
				"executive Brief model policy is missing", nil))
	}
	quotaRule, ok := snapshot.Policy.QuotaPolicy.Bucket("llm_tokens")
	if !ok {
		return ExecutiveBriefSynthesizeResult{}, nonRetryable(
			types.NewAppError(types.CodeValidation,
				"executive Brief quota policy is missing", nil))
	}
	profile := executivebrief.ProfileContextV1{}
	storedProfile, profileErr := a.executiveBriefStore.GetProfileForTenant(
		ctx, expected.TenantID, expected.UserID)
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
		return ExecutiveBriefSynthesizeResult{},
			retryableOrNot(profileErr)
	}
	profileDigest, err := executivebrief.ProfileDigestV1(profile)
	if err != nil {
		return ExecutiveBriefSynthesizeResult{}, nonRetryable(
			types.NewAppError(types.CodeValidation,
				"executive Brief profile is invalid", err))
	}
	inputDigest, err := storepkg.ExecutiveSynthesisInputDigestV1(in.Draft)
	if err != nil {
		return ExecutiveBriefSynthesizeResult{}, nonRetryable(
			types.NewAppError(types.CodeValidation,
				"executive Brief input digest failed", err))
	}
	requestDigest, err := storepkg.ExecutiveSynthesisRequestDigestV1(
		profileDigest, inputDigest,
		snapshot.Policy.PromptPolicy.IssueSynthesis.RendererVersion)
	if err != nil {
		return ExecutiveBriefSynthesizeResult{}, nonRetryable(
			types.NewAppError(types.CodeValidation,
				"executive Brief request digest failed", err))
	}
	receipt, err := a.executiveBriefStore.PrepareExecutiveSynthesisV1(
		ctx, expected, in.Run.Snapshot,
		storepkg.ExecutiveSynthesisPrepareV1{
			Marker:         in.Marker,
			PushBatchID:    in.Draft.PushBatchID,
			ProfileEpoch:   profile.Epoch,
			ProfileVersion: profile.Version,
			ProfileDigest:  profileDigest,
			InputDigest:    inputDigest,
			RequestDigest:  requestDigest,
		})
	if err != nil {
		return ExecutiveBriefSynthesizeResult{}, retryableOrNot(err)
	}
	switch receipt.Status {
	case storepkg.ExecutiveSynthesisFinalized,
		storepkg.ExecutiveSynthesisFallback:
		return executiveSynthesisResultV1(receipt, in.Draft)
	case storepkg.ExecutiveSynthesisPrepared:
		// Continue below.
	case storepkg.ExecutiveSynthesisSpending:
		return a.finalizeExecutiveSynthesisFallbackV1(
			ctx, expected, in, profile)
	case storepkg.ExecutiveSynthesisAmbiguous:
		return ExecutiveBriefSynthesizeResult{},
			types.NewAppError(types.CodeConflict,
				"executive Brief synthesis awaits recovery", nil)
	default:
		return ExecutiveBriefSynthesizeResult{}, nonRetryable(
			types.NewAppError(types.CodeConflict,
				"executive Brief synthesis state is invalid", nil))
	}
	receipt, claimed, err :=
		a.executiveBriefStore.ClaimExecutiveSynthesisSpendV1(
			ctx, expected, in.Run.Snapshot, in.Marker)
	if err != nil {
		return ExecutiveBriefSynthesizeResult{}, retryableOrNot(err)
	}
	if !claimed {
		switch receipt.Status {
		case storepkg.ExecutiveSynthesisFinalized,
			storepkg.ExecutiveSynthesisFallback:
			return executiveSynthesisResultV1(receipt, in.Draft)
		case storepkg.ExecutiveSynthesisSpending:
			return a.finalizeExecutiveSynthesisFallbackV1(
				ctx, expected, in, profile)
		default:
			return ExecutiveBriefSynthesizeResult{},
				types.NewAppError(types.CodeConflict,
					"executive Brief spend authority is unavailable", nil)
		}
	}
	prompt, err := executivebrief.BuildIssuePromptV1(
		expected.TaskID, profile, in.Draft)
	if err != nil {
		return a.finalizeExecutiveSynthesisFallbackV1(
			ctx, expected, in, profile)
	}
	modelClient, err := a.resolveCompiledModelPolicyV1(
		snapshot.Policy.ModelPolicy)
	if err != nil {
		return a.finalizeExecutiveSynthesisFallbackV1(
			ctx, expected, in, profile)
	}
	temperature := float32(modelCall.Temperature)
	maxTokens := modelCall.MaxTokens
	tenantID, userID, batchID :=
		expected.TenantID, expected.UserID, in.Draft.PushBatchID
	response, callErr := llm.Do(
		ctx, modelClient, a.llmRecorder,
		llm.CallMeta{
			TraceID: in.TraceID, TenantID: &tenantID,
			SpanName: runtimepolicy.ModelStageIssueSynthesis,
			UserID:   &userID, RefType: types.RefTypePushBatch,
			RefID: &batchID, QuotaRule: &quotaRule,
			BeforeSpend: func(
				effectCtx context.Context, amount float64,
			) error {
				return a.consumeCompiledLLMQuotaV1(
					effectCtx, in.UserID, &in.Run,
					snapshot.Policy.QuotaPolicy, amount)
			},
		},
		llm.Request{
			System: snapshot.Policy.PromptPolicy.
				IssueSynthesis.SystemPrompt,
			User: prompt, Model: modelCall.Model,
			Temperature: &temperature, MaxTokens: &maxTokens,
			DisableThinking: modelCall.DisableThinking,
		},
	)
	if callErr != nil {
		return a.finalizeExecutiveSynthesisFallbackV1(
			ctx, expected, in, profile)
	}
	content, err := executivebrief.ParseIssueContentV1(
		[]byte(response.Content), in.Draft)
	if err != nil {
		return a.finalizeExecutiveSynthesisFallbackV1(
			ctx, expected, in, profile)
	}
	receipt, err = a.executiveBriefStore.FinalizeExecutiveSynthesisV1(
		ctx, expected, in.Run.Snapshot, in.Marker, content)
	if err != nil {
		// Keep the durable row in spending. A retry is prohibited after the
		// provider call; recovery will converge it to fallback.
		return ExecutiveBriefSynthesizeResult{}, retryableOrNot(err)
	}
	return executiveSynthesisResultV1(receipt, in.Draft)
}

func validateExecutiveSynthesisEnvelopeV1(
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	marker types.RunOutcomeMarkerV1,
	draft types.BriefDraftV1,
) error {
	if marker.Validate() != nil ||
		marker.TenantID != expected.TenantID ||
		marker.UserID != expected.UserID ||
		marker.TaskID != expected.TaskID ||
		marker.RunSnapshotID != ref.SnapshotID ||
		draft.RunOutcomeID != marker.ID ||
		draft.RunSnapshotID != ref.SnapshotID ||
		draft.TenantID != expected.TenantID ||
		draft.UserID != expected.UserID ||
		draft.TaskID != expected.TaskID ||
		len(draft.Insights) == 0 {
		return types.NewAppError(types.CodeValidation,
			"executive Brief exact-run envelope differs", nil)
	}
	return nil
}

func (a *Activities) finalizeExecutiveSynthesisFallbackV1(
	ctx context.Context,
	expected types.RunIdentity,
	in ExecutiveBriefSynthesizeIn,
	profile executivebrief.ProfileContextV1,
) (ExecutiveBriefSynthesizeResult, error) {
	content, err := executivebrief.DeterministicFallbackV1(
		profile, in.Draft)
	if err != nil {
		return ExecutiveBriefSynthesizeResult{}, nonRetryable(
			types.NewAppError(types.CodeValidation,
				"executive Brief fallback failed", err))
	}
	receipt, err :=
		a.executiveBriefStore.FinalizeExecutiveSynthesisFallbackV1(
			ctx, expected, in.Run.Snapshot, in.Marker, content)
	if err != nil {
		return ExecutiveBriefSynthesizeResult{}, retryableOrNot(err)
	}
	return executiveSynthesisResultV1(receipt, in.Draft)
}

func executiveSynthesisResultV1(
	receipt storepkg.ExecutiveSynthesisReceiptV1,
	draft types.BriefDraftV1,
) (ExecutiveBriefSynthesizeResult, error) {
	if receipt.Content == nil || receipt.FinalizedAt == nil ||
		(receipt.Status != storepkg.ExecutiveSynthesisFinalized &&
			receipt.Status != storepkg.ExecutiveSynthesisFallback) {
		return ExecutiveBriefSynthesizeResult{}, types.NewAppError(
			types.CodeConflict,
			"executive Brief terminal receipt is incomplete", nil)
	}
	artifactDraft := types.ExecutiveBriefArtifactDraftV1{
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
	if artifactDraft.Validate() != nil ||
		artifactDraft.PushBatchID != draft.PushBatchID {
		return ExecutiveBriefSynthesizeResult{}, types.NewAppError(
			types.CodeConflict,
			"executive Brief terminal receipt differs from draft", nil)
	}
	return ExecutiveBriefSynthesizeResult{
		ArtifactDraft: artifactDraft,
		Fallback:      receipt.Status == storepkg.ExecutiveSynthesisFallback,
	}, nil
}

func (a *Activities) FreezeExecutiveBriefV1(
	ctx context.Context,
	in ExecutiveBriefFreezeIn,
) (types.ExecutiveBriefArtifactV1, error) {
	if a.executiveBriefStore == nil {
		return types.ExecutiveBriefArtifactV1{}, nonRetryable(
			types.NewAppError(types.CodeInternal,
				"executive Brief store is not configured", nil))
	}
	expected, err := activityRunIdentityV1(ctx, in.UserID, &in.Run)
	if err != nil {
		return types.ExecutiveBriefArtifactV1{}, nonRetryable(err)
	}
	artifact, err := a.executiveBriefStore.FreezeExecutiveBriefArtifactV1(
		ctx, expected, in.Run.Snapshot, in.Draft)
	if err != nil {
		return types.ExecutiveBriefArtifactV1{}, retryableOrNot(err)
	}
	return artifact, nil
}
