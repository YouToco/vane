package workflow

import (
	"context"
	"strings"

	"go.temporal.io/sdk/activity"

	"github.com/YouToco/vane/fetcher"
	"github.com/YouToco/vane/runcontext"
	"github.com/YouToco/vane/types"
)

// ExecuteToolInvocationV2 is the dark Source-free data-plane Activity. It is
// recovery-first and returns only a receipt. Provider errors after execution
// begins are non-retryable because Exa/TikHub do not provide a reliable
// idempotency key; a Workflow must decide how to surface/reconcile uncertainty.
func (a *Activities) ExecuteToolInvocationV2(
	ctx context.Context,
	input ExecuteToolInvocationV2Input,
) (ToolInvocationReceiptV1, error) {
	if a.compiledToolStoreV2 == nil ||
		input.TenantID <= 0 || input.UserID <= 0 ||
		strings.TrimSpace(input.TaskID) == "" ||
		len(input.InvocationDigest) != 64 ||
		!activity.IsActivity(ctx) {
		return ToolInvocationReceiptV1{}, nonRetryable(
			types.NewAppError(types.CodeValidation,
				"compiled Tool invocation input is invalid", nil))
	}
	info := activity.GetInfo(ctx)
	expected := types.RunIdentity{
		TemporalWorkflowID: info.WorkflowExecution.ID,
		TemporalRunID:      info.WorkflowExecution.RunID,
		RunKind:            types.RunSnapshotKindScheduled,
		TenantID:           input.TenantID,
		UserID:             input.UserID,
		TaskID:             input.TaskID,
	}
	if input.Snapshot.ValidateFor(expected) != nil {
		return ToolInvocationReceiptV1{}, nonRetryable(
			types.NewAppError(types.CodeValidation,
				"compiled Tool invocation snapshot is invalid", nil))
	}
	recovered, found, err :=
		a.compiledToolStoreV2.LoadContentObservationForTaskRunV2(
			ctx, expected, input.Snapshot, input.InvocationDigest)
	if err != nil {
		return ToolInvocationReceiptV1{}, retryableOrNot(err)
	}
	if found {
		return toolInvocationReceiptV1(input, recovered)
	}

	snapshot, err := a.compiledToolStoreV2.
		LoadCompiledTaskRunSnapshotV2(ctx, expected, input.Snapshot)
	if err != nil {
		return ToolInvocationReceiptV1{}, retryableOrNot(err)
	}
	if err := snapshot.ValidateFor(expected); err != nil {
		return ToolInvocationReceiptV1{}, nonRetryable(err)
	}
	binding, found := frozenToolBindingV2(
		snapshot, input.InvocationDigest)
	if !found {
		return ToolInvocationReceiptV1{}, nonRetryable(
			types.NewAppError(types.CodeValidation,
				"Tool invocation is outside the frozen run", nil))
	}
	target := types.FetchTarget{
		Platform:   types.Platform(binding.Request.Platform),
		Capability: types.Capability(binding.Request.Capability),
		URL:        binding.Request.URL, Title: binding.Request.Title,
		Config: append([]byte(nil), binding.Request.Config...),
		Status: types.FetchTargetStatusActive,
	}
	frozenFetcher, ok := a.fetcher.(compiledFetcherV1)
	if !ok {
		return ToolInvocationReceiptV1{}, nonRetryable(
			types.NewAppError(types.CodeInternal,
				"compiled Tool fetcher v1 is unsupported", nil))
	}
	if err := frozenFetcher.ValidateRuntimeFetchRouteV1(
		binding.Capability, target); err != nil {
		return ToolInvocationReceiptV1{}, nonRetryable(err)
	}
	authorized, err := a.compiledToolStoreV2.
		AuthorizeTaskRunSideEffectV2(ctx, expected, input.Snapshot)
	if err != nil {
		return ToolInvocationReceiptV1{}, retryableOrNot(err)
	}
	if !authorized {
		return ToolInvocationReceiptV1{}, nonRetryable(
			types.NewAppError(types.CodeNotFound,
				"compiled Tool run is no longer authorized", nil))
	}
	ctx = fetcher.WithBindingRunAttribution(
		ctx, expected.TemporalWorkflowID,
		expected.TenantID, expected.UserID)
	items, fetchErr := frozenFetcher.FetchWithPolicyV1(
		ctx, target, binding.Capability,
		func(effectCtx context.Context) error {
			live, authErr := a.compiledToolStoreV2.
				AuthorizeTaskRunSideEffectV2(
					effectCtx, expected, input.Snapshot)
			if authErr != nil {
				return authErr
			}
			if !live {
				return types.NewAppError(types.CodeNotFound,
					"compiled Tool run is no longer authorized", nil)
			}
			return nil
		})
	if fetchErr != nil {
		return ToolInvocationReceiptV1{}, nonRetryable(fetchErr)
	}
	if items == nil {
		items = []types.ContentItem{}
	}
	for i := range items {
		if items[i].SourceID != 0 {
			return ToolInvocationReceiptV1{}, nonRetryable(
				types.NewAppError(types.CodeValidation,
					"compiled Tool result leaked a Source identity", nil))
		}
	}
	persisted, err :=
		a.compiledToolStoreV2.CommitContentObservationForTaskRunV2(
			ctx, expected, input.Snapshot,
			input.InvocationDigest, items)
	if err != nil {
		// A provider response now exists. Do not let Temporal automatically
		// repeat the paid call when the evidence commit is uncertain.
		return ToolInvocationReceiptV1{}, nonRetryable(err)
	}
	return toolInvocationReceiptV1(input, persisted)
}

// CollectToolRunContentV2 returns only current-run immutable observations that
// have not already been delivered to this user. It never consults a Source
// candidate reader and performs no external effect.
func (a *Activities) CollectToolRunContentV2(
	ctx context.Context,
	input CollectToolRunContentV2Input,
) ([]runcontext.ToolCandidateV1, error) {
	if a.compiledToolStoreV2 == nil ||
		input.TenantID <= 0 || input.UserID <= 0 ||
		strings.TrimSpace(input.TaskID) == "" ||
		!activity.IsActivity(ctx) {
		return nil, nonRetryable(types.NewAppError(
			types.CodeValidation,
			"compiled Tool content input is invalid", nil))
	}
	info := activity.GetInfo(ctx)
	expected := types.RunIdentity{
		TemporalWorkflowID: info.WorkflowExecution.ID,
		TemporalRunID:      info.WorkflowExecution.RunID,
		RunKind:            types.RunSnapshotKindScheduled,
		TenantID:           input.TenantID,
		UserID:             input.UserID,
		TaskID:             input.TaskID,
	}
	if input.Snapshot.ValidateFor(expected) != nil {
		return nil, nonRetryable(types.NewAppError(
			types.CodeValidation,
			"compiled Tool content snapshot is invalid", nil))
	}
	authorized, err := a.compiledToolStoreV2.
		AuthorizeTaskRunSideEffectV2(ctx, expected, input.Snapshot)
	if err != nil {
		return nil, retryableOrNot(err)
	}
	if !authorized {
		return []runcontext.ToolCandidateV1{}, nil
	}
	items, err := a.compiledToolStoreV2.
		ListContentCandidatesForTaskRunV2(
			ctx, expected, input.Snapshot, maxScoreCandidates)
	if err != nil {
		return nil, retryableOrNot(err)
	}
	return items, nil
}

func frozenToolBindingV2(
	snapshot runcontext.CompiledSnapshotV2,
	invocationDigest string,
) (runcontext.ToolBindingV1, bool) {
	for _, binding := range snapshot.ToolBindings {
		if binding.InvocationDigest == invocationDigest {
			return binding, true
		}
	}
	return runcontext.ToolBindingV1{}, false
}

func toolInvocationReceiptV1(
	input ExecuteToolInvocationV2Input,
	items []types.ContentItem,
) (ToolInvocationReceiptV1, error) {
	_, _, digest, err := runcontext.BuildToolObservationSetV1(
		input.Snapshot.SnapshotID, input.InvocationDigest, items)
	if err != nil {
		return ToolInvocationReceiptV1{}, nonRetryable(
			types.NewAppError(types.CodeValidation,
				"compiled Tool observation receipt is invalid", nil))
	}
	return ToolInvocationReceiptV1{
		InvocationDigest:  input.InvocationDigest,
		ObservationDigest: digest,
		ContentCount:      len(items),
	}, nil
}
