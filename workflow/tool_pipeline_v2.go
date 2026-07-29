package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/sdk/activity"

	cardgenpkg "github.com/YouToco/vane/cardgen"
	"github.com/YouToco/vane/dedup"
	"github.com/YouToco/vane/runcontext"
	"github.com/YouToco/vane/runtimepolicy"
	scorerpkg "github.com/YouToco/vane/scorer"
	"github.com/YouToco/vane/selector"
	storepkg "github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
)

// CompiledToolRunInputV2 is intentionally disjoint from CompiledRunInputV1.
// A Source-free reference must never be widened or cast into the retained V1
// Source runtime.
type CompiledToolRunInputV2 struct {
	TenantID int64                  `json:"tenant_id"`
	TaskID   string                 `json:"task_id"`
	Snapshot types.RunSnapshotRefV2 `json:"snapshot"`
}

type DedupToolCandidatesV2Input struct {
	UserID     int64                        `json:"user_id"`
	TraceID    string                       `json:"trace_id"`
	Run        CompiledToolRunInputV2       `json:"run"`
	Candidates []runcontext.ToolCandidateV1 `json:"candidates"`
}

type ScoreToolCandidatesV2Input struct {
	UserID     int64                        `json:"user_id"`
	TraceID    string                       `json:"trace_id"`
	Run        CompiledToolRunInputV2       `json:"run"`
	Candidates []runcontext.ToolCandidateV1 `json:"candidates"`
}

type SelectToolCandidatesV2Input struct {
	UserID     int64                              `json:"user_id"`
	TraceID    string                             `json:"trace_id"`
	Run        CompiledToolRunInputV2             `json:"run"`
	Candidates []runcontext.ToolScoredCandidateV1 `json:"candidates"`
}

type CardGenToolCandidatesV2Input struct {
	UserID     int64                              `json:"user_id"`
	TraceID    string                             `json:"trace_id"`
	Run        CompiledToolRunInputV2             `json:"run"`
	Candidates []runcontext.ToolScoredCandidateV1 `json:"candidates"`
}

// ToolGeneratedCardV1 keeps the observation invocation attached until the
// delivery row is durably created. GeneratedCard remains unchanged for V1
// replay compatibility.
type ToolGeneratedCardV1 struct {
	InvocationDigest string        `json:"invocation_digest"`
	Card             GeneratedCard `json:"card"`
}

func activityToolRunIdentityV2(
	ctx context.Context,
	userID int64,
	run CompiledToolRunInputV2,
) (types.RunIdentity, error) {
	if userID <= 0 || run.TenantID <= 0 ||
		strings.TrimSpace(run.TaskID) == "" ||
		!activity.IsActivity(ctx) {
		return types.RunIdentity{}, types.NewAppError(
			types.CodeValidation, "compiled Tool activity input is invalid", nil)
	}
	info := activity.GetInfo(ctx)
	expected := types.RunIdentity{
		TemporalWorkflowID: info.WorkflowExecution.ID,
		TemporalRunID:      info.WorkflowExecution.RunID,
		RunKind:            types.RunSnapshotKindScheduled,
		TenantID:           run.TenantID,
		UserID:             userID,
		TaskID:             run.TaskID,
	}
	if err := run.Snapshot.ValidateFor(expected); err != nil {
		return types.RunIdentity{}, err
	}
	return expected, nil
}

func (a *Activities) loadAuthoritativeToolRunV2(
	ctx context.Context,
	userID int64,
	run CompiledToolRunInputV2,
) (runcontext.CompiledSnapshotV2, types.RunIdentity, error) {
	if a.compiledToolStoreV2 == nil {
		return runcontext.CompiledSnapshotV2{}, types.RunIdentity{},
			types.NewAppError(types.CodeInternal,
				"compiled Tool runtime store is not configured", nil)
	}
	expected, err := activityToolRunIdentityV2(ctx, userID, run)
	if err != nil {
		return runcontext.CompiledSnapshotV2{}, types.RunIdentity{}, err
	}
	snapshot, err := a.compiledToolStoreV2.LoadCompiledTaskRunSnapshotV2(
		ctx, expected, run.Snapshot)
	if err != nil {
		return runcontext.CompiledSnapshotV2{}, types.RunIdentity{}, err
	}
	if err := snapshot.ValidateFor(expected); err != nil {
		return runcontext.CompiledSnapshotV2{}, types.RunIdentity{}, err
	}
	return snapshot, expected, nil
}

func (a *Activities) authorizeToolEffectV2(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRefV2,
) error {
	authorized, err := a.compiledToolStoreV2.
		AuthorizeTaskRunSideEffectV2(ctx, expected, ref)
	if err != nil {
		return err
	}
	if !authorized {
		return types.NewAppError(types.CodeNotFound,
			"compiled Tool run is no longer authorized", nil)
	}
	return nil
}

func (a *Activities) consumeToolLLMQuotaV2(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRefV2,
	policy runtimepolicy.QuotaPolicyV1,
	amount float64,
) error {
	rule, ok := policy.Bucket("llm_tokens")
	if !ok {
		return types.NewAppError(types.CodeValidation,
			"compiled Tool llm quota policy is missing", nil)
	}
	return a.compiledToolStoreV2.AuthorizeAndConsumeTaskRunLLMQuotaV2(
		ctx, expected, ref, rule, amount)
}

func frozenToolInvocationDigestsV2(
	snapshot runcontext.CompiledSnapshotV2,
) map[string]struct{} {
	out := make(map[string]struct{}, len(snapshot.Definition.ToolCalls))
	for _, call := range snapshot.Definition.ToolCalls {
		out[call.Digest] = struct{}{}
	}
	return out
}

func validateToolCandidatesV2(
	snapshot runcontext.CompiledSnapshotV2,
	candidates []runcontext.ToolCandidateV1,
) error {
	frozen := frozenToolInvocationDigestsV2(snapshot)
	seenIDs := make(map[int64]struct{}, len(candidates))
	for _, candidate := range candidates {
		if _, ok := frozen[candidate.InvocationDigest]; !ok ||
			candidate.Item.ID <= 0 || candidate.Item.SourceID != 0 {
			return types.NewAppError(types.CodeValidation,
				"compiled Tool candidate is outside the frozen run", nil)
		}
		if _, duplicate := seenIDs[candidate.Item.ID]; duplicate {
			return types.NewAppError(types.CodeValidation,
				"compiled Tool candidate content is duplicated", nil)
		}
		seenIDs[candidate.Item.ID] = struct{}{}
	}
	return nil
}

func validateToolScoredCandidatesV2(
	snapshot runcontext.CompiledSnapshotV2,
	candidates []runcontext.ToolScoredCandidateV1,
) error {
	unscored := make([]runcontext.ToolCandidateV1, len(candidates))
	for i, candidate := range candidates {
		unscored[i] = runcontext.ToolCandidateV1{
			InvocationDigest: candidate.InvocationDigest,
			Item:             candidate.Scored.Item,
		}
	}
	return validateToolCandidatesV2(snapshot, unscored)
}

func (a *Activities) validateCanonicalToolCandidatesV2(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRefV2,
	candidates []runcontext.ToolCandidateV1,
) error {
	byInvocation := make(map[string][]types.ContentItem)
	for _, candidate := range candidates {
		if _, loaded := byInvocation[candidate.InvocationDigest]; !loaded {
			items, found, err := a.compiledToolStoreV2.
				LoadContentObservationForTaskRunV2(
					ctx, expected, ref, candidate.InvocationDigest)
			if err != nil {
				return err
			}
			if !found {
				return types.NewAppError(types.CodeValidation,
					"compiled Tool observation is not authoritative", nil)
			}
			byInvocation[candidate.InvocationDigest] = items
		}
		var stored *types.ContentItem
		for i := range byInvocation[candidate.InvocationDigest] {
			item := &byInvocation[candidate.InvocationDigest][i]
			if item.ID == candidate.Item.ID {
				stored = item
				break
			}
		}
		if stored == nil {
			return types.NewAppError(types.CodeValidation,
				"compiled Tool candidate provenance is not authoritative", nil)
		}
		// Dedup is allowed to compute Simhash in memory. Every other content
		// field consumed by the model must match the canonical Store row.
		storedItem := *stored
		storedItem.Simhash = nil
		candidate.Item.Simhash = nil
		if !reflect.DeepEqual(storedItem, candidate.Item) {
			return types.NewAppError(types.CodeValidation,
				"compiled Tool candidate payload is not authoritative", nil)
		}
	}
	return nil
}

// DedupToolCandidatesV2 performs the retained pure simhash algorithm while
// reading history only through the exact Source-free run reference.
func (a *Activities) DedupToolCandidatesV2(
	ctx context.Context,
	in DedupToolCandidatesV2Input,
) ([]runcontext.ToolCandidateV1, error) {
	snapshot, expected, err := a.loadAuthoritativeToolRunV2(
		ctx, in.UserID, in.Run)
	if err != nil {
		return nil, retryableOrNot(err)
	}
	if err := validateToolCandidatesV2(snapshot, in.Candidates); err != nil {
		return nil, nonRetryable(err)
	}
	if err := a.validateCanonicalToolCandidatesV2(
		ctx, expected, in.Run.Snapshot, in.Candidates); err != nil {
		return nil, retryableOrNot(err)
	}
	batchIDs := make([]int64, 0, len(in.Candidates))
	for _, candidate := range in.Candidates {
		batchIDs = append(batchIDs, candidate.Item.ID)
	}
	history, err := a.compiledToolStoreV2.ListRecentSimhashesForTaskRunV2(
		ctx, expected, in.Run.Snapshot,
		time.Now().Add(-simhashWindow), batchIDs)
	if err != nil {
		return nil, retryableOrNot(err)
	}
	batchSeen := make([]int64, 0, len(in.Candidates))
	kept := make([]runcontext.ToolCandidateV1, 0, len(in.Candidates))
	for _, candidate := range in.Candidates {
		sh := dedup.Simhash(candidate.Item.Title + " " + candidate.Item.Content)
		candidate.Item.Simhash = &sh
		if candidate.Item.Kind == types.KindPageContent {
			kept = append(kept, candidate)
			continue
		}
		comparison := append(append([]int64{}, history...), batchSeen...)
		if dedup.IsNearDup(sh, comparison, simhashThreshold) {
			continue
		}
		kept = append(kept, candidate)
		batchSeen = append(batchSeen, sh)
	}
	return kept, nil
}

// ScoreToolCandidatesV2 uses only the snapshot's model/prompt/quota policies
// and its frozen task manual. Every model call is guarded by live auth and an
// atomic V2 quota reservation.
func (a *Activities) ScoreToolCandidatesV2(
	ctx context.Context,
	in ScoreToolCandidatesV2Input,
) ([]runcontext.ToolScoredCandidateV1, error) {
	snapshot, expected, err := a.loadAuthoritativeToolRunV2(
		ctx, in.UserID, in.Run)
	if err != nil {
		return nil, retryableOrNot(err)
	}
	if err := validateToolCandidatesV2(snapshot, in.Candidates); err != nil {
		return nil, nonRetryable(err)
	}
	if err := a.validateCanonicalToolCandidatesV2(
		ctx, expected, in.Run.Snapshot, in.Candidates); err != nil {
		return nil, retryableOrNot(err)
	}
	if a.compiledToolModelResolverV2 == nil {
		return nil, nonRetryable(types.NewAppError(types.CodeInternal,
			"compiled Tool model resolver is not configured", nil))
	}
	modelClient, err := a.compiledToolModelResolverV2.
		ResolveRuntimeModelPolicyV1(snapshot.Policy.ModelPolicy)
	if err != nil {
		return nil, nonRetryable(types.NewAppError(types.CodeValidation,
			"compiled Tool score model route is unavailable", err))
	}
	consumer, ok := a.scorer.(compiledScorerV1)
	if !ok {
		return nil, nonRetryable(types.NewAppError(types.CodeInternal,
			"compiled Tool scorer is unsupported", nil))
	}
	policy, err := scorerpkg.PrepareCompiledPolicyV1(
		snapshot.Policy.PromptPolicy, snapshot.Policy.ModelPolicy,
		snapshot.Policy.QuotaPolicy, modelClient)
	if err != nil {
		return nil, nonRetryable(types.NewAppError(types.CodeValidation,
			"compiled Tool score policy is invalid", err))
	}
	taskManual := ""
	if snapshot.Policy.PromptPolicy.TaskInstructionEnabled {
		taskManual = snapshot.Definition.TaskManual
	}
	var quotaHit atomic.Bool
	var authorizationOnce sync.Once
	var authorizationErr error
	scored := mapConcurrent(ctx, in.Candidates, parBatchFanout,
		func(ctx context.Context, candidate runcontext.ToolCandidateV1) (
			runcontext.ToolScoredCandidateV1, error,
		) {
			if err := a.authorizeToolEffectV2(
				ctx, expected, in.Run.Snapshot); err != nil {
				authorizationOnce.Do(func() { authorizationErr = err })
				return runcontext.ToolScoredCandidateV1{}, err
			}
			beforeSpend := func(effectCtx context.Context, amount float64) error {
				err := a.consumeToolLLMQuotaV2(
					effectCtx, expected, in.Run.Snapshot,
					snapshot.Policy.QuotaPolicy, amount)
				if err != nil && !errors.Is(err, storepkg.ErrQuotaExceeded) {
					authorizationOnce.Do(func() { authorizationErr = err })
				}
				return err
			}
			score, err := consumer.ScoreWithPolicyV1(
				ctx, snapshot.Definition.TenantID, in.UserID, candidate.Item,
				in.TraceID, taskManual, policy, beforeSpend)
			if err != nil {
				return runcontext.ToolScoredCandidateV1{}, err
			}
			score = max(0, min(100,
				score+candidate.Item.ObservationScorePenalty))
			return runcontext.ToolScoredCandidateV1{
				InvocationDigest: candidate.InvocationDigest,
				Scored: types.ScoredItem{
					Item:  candidate.Item,
					Score: score,
				},
			}, nil
		},
		func(candidate runcontext.ToolCandidateV1, err error) {
			if isQuotaErr(err) {
				quotaHit.Store(true)
			}
			logPipelineItemFailure(ctx,
				"Tool V2 score failed; item skipped",
				candidate.Item.ID, in.TraceID, err)
		})
	if len(scored) == 0 && len(in.Candidates) > 0 {
		if authorizationErr != nil {
			return nil, retryableOrNot(authorizationErr)
		}
		if quotaHit.Load() {
			return nil, nonRetryable(types.NewAppError(
				types.CodeQuotaExceeded,
				"compiled Tool LLM quota exhausted during score", nil))
		}
		return nil, types.NewAppError(types.CodeLLMUnavailable,
			"compiled Tool score batch failed", nil)
	}
	return scored, nil
}

// SelectToolCandidatesV2 uses only frozen task scope and strictness. Provenance
// is restored by content ID after the pure deterministic ranking function.
func (a *Activities) SelectToolCandidatesV2(
	ctx context.Context,
	in SelectToolCandidatesV2Input,
) ([]runcontext.ToolScoredCandidateV1, error) {
	snapshot, expected, err := a.loadAuthoritativeToolRunV2(
		ctx, in.UserID, in.Run)
	if err != nil {
		return nil, retryableOrNot(err)
	}
	if err := validateToolScoredCandidatesV2(
		snapshot, in.Candidates); err != nil {
		return nil, nonRetryable(err)
	}
	canonicalCandidates := make([]runcontext.ToolCandidateV1, len(in.Candidates))
	for i, candidate := range in.Candidates {
		canonicalCandidates[i] = runcontext.ToolCandidateV1{
			InvocationDigest: candidate.InvocationDigest,
			Item:             candidate.Scored.Item,
		}
	}
	if err := a.validateCanonicalToolCandidatesV2(
		ctx, expected, in.Run.Snapshot, canonicalCandidates); err != nil {
		return nil, retryableOrNot(err)
	}
	var scope PushScope
	if err := json.Unmarshal(snapshot.Definition.ScopeJSON, &scope); err != nil ||
		scope.TopN < 0 || len(scope.SourceIDs) != 0 {
		return nil, nonRetryable(types.NewAppError(types.CodeValidation,
			"compiled Tool task scope is invalid", err))
	}
	topN := scope.TopN
	if topN <= 0 {
		topN = defaultTopN
	}
	threshold := snapshot.Definition.Strictness.MinKeepScore()
	scored := make([]types.ScoredItem, 0, len(in.Candidates))
	provenance := make(map[int64]string, len(in.Candidates))
	for _, candidate := range in.Candidates {
		if candidate.Scored.Score < float64(threshold) {
			continue
		}
		scored = append(scored, candidate.Scored)
		provenance[candidate.Scored.Item.ID] = candidate.InvocationDigest
	}
	ranked := selector.RankTopN(scored, topN, time.Now())
	out := make([]runcontext.ToolScoredCandidateV1, 0, len(ranked))
	for _, item := range ranked {
		out = append(out, runcontext.ToolScoredCandidateV1{
			InvocationDigest: provenance[item.Item.ID],
			Scored:           item,
		})
	}
	return out, nil
}

// CardGenToolCandidatesV2 deliberately uses the stable V1 markdown generator
// rather than the Source-dependent structured-event generator.
func (a *Activities) CardGenToolCandidatesV2(
	ctx context.Context,
	in CardGenToolCandidatesV2Input,
) ([]ToolGeneratedCardV1, error) {
	snapshot, expected, err := a.loadAuthoritativeToolRunV2(
		ctx, in.UserID, in.Run)
	if err != nil {
		return nil, retryableOrNot(err)
	}
	if err := validateToolScoredCandidatesV2(
		snapshot, in.Candidates); err != nil {
		return nil, nonRetryable(err)
	}
	canonicalCandidates := make([]runcontext.ToolCandidateV1, len(in.Candidates))
	for i, candidate := range in.Candidates {
		canonicalCandidates[i] = runcontext.ToolCandidateV1{
			InvocationDigest: candidate.InvocationDigest,
			Item:             candidate.Scored.Item,
		}
	}
	if err := a.validateCanonicalToolCandidatesV2(
		ctx, expected, in.Run.Snapshot, canonicalCandidates); err != nil {
		return nil, retryableOrNot(err)
	}
	if a.compiledToolModelResolverV2 == nil {
		return nil, nonRetryable(types.NewAppError(types.CodeInternal,
			"compiled Tool model resolver is not configured", nil))
	}
	modelClient, err := a.compiledToolModelResolverV2.
		ResolveRuntimeModelPolicyV1(snapshot.Policy.ModelPolicy)
	if err != nil {
		return nil, nonRetryable(types.NewAppError(types.CodeValidation,
			"compiled Tool card model route is unavailable", err))
	}
	consumer, ok := a.cardgen.(compiledCardGeneratorV1)
	if !ok {
		return nil, nonRetryable(types.NewAppError(types.CodeInternal,
			"compiled Tool card generator is unsupported", nil))
	}
	policy, err := cardgenpkg.PrepareCompiledPolicyV1(
		snapshot.Policy.PromptPolicy, snapshot.Policy.ModelPolicy,
		snapshot.Policy.QuotaPolicy, modelClient)
	if err != nil {
		return nil, nonRetryable(types.NewAppError(types.CodeValidation,
			"compiled Tool card policy is invalid", err))
	}
	taskManual := ""
	if snapshot.Policy.PromptPolicy.TaskInstructionEnabled {
		taskManual = snapshot.Definition.TaskManual
	}
	var quotaHit atomic.Bool
	var authorizationOnce sync.Once
	var authorizationErr error
	cards := mapConcurrent(ctx, in.Candidates, parBatchFanout,
		func(ctx context.Context, candidate runcontext.ToolScoredCandidateV1) (
			ToolGeneratedCardV1, error,
		) {
			if err := a.authorizeToolEffectV2(
				ctx, expected, in.Run.Snapshot); err != nil {
				authorizationOnce.Do(func() { authorizationErr = err })
				return ToolGeneratedCardV1{}, err
			}
			beforeSpend := func(effectCtx context.Context, amount float64) error {
				err := a.consumeToolLLMQuotaV2(
					effectCtx, expected, in.Run.Snapshot,
					snapshot.Policy.QuotaPolicy, amount)
				if err != nil && !errors.Is(err, storepkg.ErrQuotaExceeded) {
					authorizationOnce.Do(func() { authorizationErr = err })
				}
				return err
			}
			body, err := consumer.GenerateWithPolicyV1(
				ctx, snapshot.Definition.TenantID, in.UserID,
				candidate.Scored, in.TraceID, taskManual, policy, beforeSpend)
			if err != nil {
				return ToolGeneratedCardV1{}, err
			}
			return ToolGeneratedCardV1{
				InvocationDigest: candidate.InvocationDigest,
				Card: GeneratedCard{
					Scored: candidate.Scored,
					BodyMD: body,
				},
			}, nil
		},
		func(candidate runcontext.ToolScoredCandidateV1, err error) {
			if isQuotaErr(err) {
				quotaHit.Store(true)
			}
			logPipelineItemFailure(ctx,
				"Tool V2 card generation failed; item skipped",
				candidate.Scored.Item.ID, in.TraceID, err)
		})
	if len(cards) == 0 && len(in.Candidates) > 0 {
		if authorizationErr != nil {
			return nil, retryableOrNot(authorizationErr)
		}
		if quotaHit.Load() {
			return nil, nonRetryable(types.NewAppError(
				types.CodeQuotaExceeded,
				"compiled Tool LLM quota exhausted during card generation", nil))
		}
		return nil, types.NewAppError(types.CodeLLMUnavailable,
			"compiled Tool card generation batch failed", nil)
	}
	return cards, nil
}
