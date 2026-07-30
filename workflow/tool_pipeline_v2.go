package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/sdk/activity"

	cardgenpkg "github.com/YouToco/vane/cardgen"
	"github.com/YouToco/vane/dedup"
	"github.com/YouToco/vane/eventqualifier"
	"github.com/YouToco/vane/observation"
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

type QualifyToolCandidatesV2Input struct {
	UserID     int64                        `json:"user_id"`
	TraceID    string                       `json:"trace_id"`
	Run        CompiledToolRunInputV2       `json:"run"`
	Candidates []runcontext.ToolCandidateV1 `json:"candidates"`
}

type QualifyToolCandidatesV2Result struct {
	Candidates       []runcontext.ToolCandidateV1  `json:"candidates"`
	Evidence         []ToolQualificationEvidenceV1 `json:"evidence,omitempty"`
	EvidenceRequired bool                          `json:"evidence_required,omitempty"`
	Outcome          string                        `json:"outcome"`
}

type ToolQualificationEvidenceV1 struct {
	PrimaryContentID int64                        `json:"primary_content_id"`
	Candidates       []runcontext.ToolCandidateV1 `json:"candidates"`
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
	UserID           int64                              `json:"user_id"`
	TraceID          string                             `json:"trace_id"`
	Run              CompiledToolRunInputV2             `json:"run"`
	Candidates       []runcontext.ToolScoredCandidateV1 `json:"candidates"`
	Evidence         []ToolQualificationEvidenceV1      `json:"evidence,omitempty"`
	EvidenceRequired bool                               `json:"evidence_required,omitempty"`
}

// ToolGeneratedCardV1 keeps the observation invocation attached until the
// delivery row is durably created. GeneratedCard remains unchanged for V1
// replay compatibility.
type ToolGeneratedCardV1 struct {
	InvocationDigest string               `json:"invocation_digest"`
	Card             GeneratedCard        `json:"card"`
	Evidence         []ToolCardEvidenceV1 `json:"evidence,omitempty"`
}

type ToolCardEvidenceV1 struct {
	Candidate runcontext.ToolCandidateV1       `json:"candidate"`
	Source    cardgenpkg.EventEvidenceSourceV1 `json:"source"`
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

// QualifyToolCandidatesV2 makes the sealed task manual an authority gate over
// current Tool evidence. It never invents a locator or consults model memory:
// the qualifier can cite only canonical candidates fetched in this exact run.
//
// This paid Activity has MaximumAttempts=1, like every other Tool V2 model
// stage, so an Activity completion-ack loss cannot repeat the model call.
func (a *Activities) QualifyToolCandidatesV2(
	ctx context.Context,
	in QualifyToolCandidatesV2Input,
) (QualifyToolCandidatesV2Result, error) {
	snapshot, expected, err := a.loadAuthoritativeToolRunV2(
		ctx, in.UserID, in.Run)
	if err != nil {
		return QualifyToolCandidatesV2Result{}, retryableOrNot(err)
	}
	if err := validateToolCandidatesV2(snapshot, in.Candidates); err != nil {
		return QualifyToolCandidatesV2Result{}, nonRetryable(err)
	}
	if err := a.validateCanonicalToolCandidatesV2(
		ctx, expected, in.Run.Snapshot, in.Candidates); err != nil {
		return QualifyToolCandidatesV2Result{}, retryableOrNot(err)
	}
	policy, window, err := compiledObservationPolicyAndWindow(
		snapshot.Definition.ScopeJSON,
		snapshot.Definition.SpecJSON,
		expected,
	)
	if err != nil {
		return QualifyToolCandidatesV2Result{}, nonRetryable(err)
	}
	manualGate := taskManualRequiresObservationGate(
		snapshot.Definition.TaskManual)
	if policy == nil {
		if manualGate {
			return QualifyToolCandidatesV2Result{Outcome: "uncertain"}, nil
		}
		return QualifyToolCandidatesV2Result{
			Candidates: in.Candidates, Outcome: "not_configured",
		}, nil
	}
	switch snapshot.ObservationRollout {
	case observation.RolloutOff:
		if manualGate {
			return QualifyToolCandidatesV2Result{Outcome: "uncertain"}, nil
		}
		return QualifyToolCandidatesV2Result{
			Candidates: in.Candidates, Outcome: "rollout_off",
		}, nil
	case observation.RolloutShadow:
		if manualGate {
			return QualifyToolCandidatesV2Result{Outcome: "uncertain"}, nil
		}
		// Tool V2 has no operator-funded shadow-spend store contract. Preserve
		// the existing shadow behavior without charging tenant quota; authority
		// is the only mode allowed to suppress a delivery.
		return QualifyToolCandidatesV2Result{
			Candidates: in.Candidates, Outcome: "shadow_not_enforced",
		}, nil
	case observation.RolloutAuthority:
	default:
		return QualifyToolCandidatesV2Result{}, nonRetryable(
			types.NewAppError(types.CodeValidation,
				"compiled Tool observation rollout is invalid", nil))
	}

	provenance := make(map[int64]string, len(in.Candidates))
	items := make([]types.ContentItem, len(in.Candidates))
	for i, candidate := range in.Candidates {
		provenance[candidate.Item.ID] = candidate.InvocationDigest
		items[i] = candidate.Item
	}
	if policy.Mode == observation.ModeContent {
		if manualGate {
			return QualifyToolCandidatesV2Result{Outcome: "uncertain"}, nil
		}
		qualified := qualifyContentWindow(*policy, window, items)
		return toolQualificationResultV2(
			qualified, provenance, outcomeForQualifiedItems(qualified), nil), nil
	}
	if a.eventQualifier == nil || a.compiledToolModelResolverV2 == nil {
		return QualifyToolCandidatesV2Result{}, nonRetryable(
			types.NewAppError(types.CodeInternal,
				"compiled Tool event qualifier is not configured", nil))
	}
	candidates := admissibleEventEvidenceCandidates(*policy, window, items)
	if len(candidates) == 0 {
		return QualifyToolCandidatesV2Result{Outcome: "no_match"}, nil
	}
	modelClient, err := a.compiledToolModelResolverV2.
		ResolveRuntimeModelPolicyV1(snapshot.Policy.ModelPolicy)
	if err != nil {
		return QualifyToolCandidatesV2Result{}, nonRetryable(
			types.NewAppError(types.CodeValidation,
				"compiled Tool qualifier model route is unavailable", err))
	}
	modelCall, ok := snapshot.Policy.ModelPolicy.Call(
		runtimepolicy.ModelStageCardGen)
	if !ok {
		return QualifyToolCandidatesV2Result{}, nonRetryable(
			types.NewAppError(types.CodeValidation,
				"compiled Tool qualifier model call is missing", nil))
	}
	quotaRule, ok := snapshot.Policy.QuotaPolicy.Bucket("llm_tokens")
	if !ok {
		return QualifyToolCandidatesV2Result{}, nonRetryable(
			types.NewAppError(types.CodeValidation,
				"compiled Tool qualifier quota rule is missing", nil))
	}
	beforeSpend := func(effectCtx context.Context, amount float64) error {
		return a.consumeToolLLMQuotaV2(
			effectCtx, expected, in.Run.Snapshot,
			snapshot.Policy.QuotaPolicy, amount)
	}
	result, _, err := a.eventQualifier.Qualify(
		ctx,
		eventqualifier.Request{
			TenantID:    expected.TenantID,
			UserID:      expected.UserID,
			TraceID:     in.TraceID,
			Policy:      *policy,
			Window:      window,
			TaskManual:  snapshot.Definition.TaskManual,
			Candidates:  candidates,
			Client:      modelClient,
			ModelCall:   modelCall,
			QuotaRule:   &quotaRule,
			BeforeSpend: beforeSpend,
		},
	)
	if err != nil {
		if isQuotaErr(err) {
			return QualifyToolCandidatesV2Result{}, nonRetryable(
				types.NewAppError(types.CodeQuotaExceeded,
					"compiled Tool LLM quota exhausted during qualification", err))
		}
		switch types.CodeOf(err) {
		case types.CodeLLMUnavailable, types.CodeLLMBadRequest:
			return QualifyToolCandidatesV2Result{Outcome: "uncertain"}, nil
		default:
			return QualifyToolCandidatesV2Result{}, retryableOrNot(err)
		}
	}
	policyDigest, err := observation.PolicyDigest(*policy)
	if err != nil {
		return QualifyToolCandidatesV2Result{}, nonRetryable(
			types.NewAppError(types.CodeValidation,
				"compiled Tool observation policy digest failed", err))
	}
	qualified, outcome, err := a.validateQualifiedEvents(
		*policy, policyDigest, window, candidates,
		snapshot.Definition.TaskManual, result)
	if err != nil {
		// Model output is untrusted. Reject malformed or unsupported evidence
		// as uncertain instead of letting it reach score/card/push.
		if types.CodeOf(err) == types.CodeValidation {
			return QualifyToolCandidatesV2Result{Outcome: "uncertain"}, nil
		}
		return QualifyToolCandidatesV2Result{}, err
	}
	evidence := toolEventQualificationEvidenceV2(result, in.Candidates)
	qualifiedResult := toolQualificationResultV2(
		qualified, provenance, outcome, evidence)
	qualifiedResult.EvidenceRequired = manualGate
	return qualifiedResult, nil
}

func outcomeForQualifiedItems(items []types.ContentItem) string {
	if len(items) == 0 {
		return "no_match"
	}
	return "match"
}

func toolQualificationResultV2(
	qualified []types.ContentItem,
	provenance map[int64]string,
	outcome string,
	evidence []ToolQualificationEvidenceV1,
) QualifyToolCandidatesV2Result {
	out := make([]runcontext.ToolCandidateV1, 0, len(qualified))
	for _, item := range qualified {
		invocationDigest, ok := provenance[item.ID]
		if !ok {
			continue
		}
		// Qualification is a filter in Tool V2. Downstream stages reload and
		// compare canonical Store content, so do not transport derived event
		// fields across the Activity boundary.
		item.ObservationEventKey = ""
		item.ObservationPolicyDigest = ""
		item.ObservationEventJSON = nil
		item.ObservationScorePenalty = 0
		out = append(out, runcontext.ToolCandidateV1{
			InvocationDigest: invocationDigest,
			Item:             item,
		})
	}
	return QualifyToolCandidatesV2Result{
		Candidates: out,
		Evidence:   evidence,
		Outcome:    outcome,
	}
}

func toolEventQualificationEvidenceV2(
	result eventqualifier.Result,
	candidates []runcontext.ToolCandidateV1,
) []ToolQualificationEvidenceV1 {
	if result.Outcome != "match" {
		return nil
	}
	byID := make(map[int64]runcontext.ToolCandidateV1, len(candidates))
	for _, candidate := range candidates {
		byID[candidate.Item.ID] = candidate
	}
	out := make([]ToolQualificationEvidenceV1, 0, len(result.Events))
	seenPrimary := make(map[int64]struct{}, len(result.Events))
	for _, event := range result.Events {
		if len(event.EvidenceContentIDs) == 0 {
			continue
		}
		primaryContentID := event.EvidenceContentIDs[0]
		if _, duplicate := seenPrimary[primaryContentID]; duplicate {
			continue
		}
		group := ToolQualificationEvidenceV1{
			PrimaryContentID: primaryContentID,
			Candidates: make(
				[]runcontext.ToolCandidateV1, 0,
				len(event.EvidenceContentIDs)),
		}
		for _, contentID := range event.EvidenceContentIDs {
			candidate, ok := byID[contentID]
			if !ok {
				group.Candidates = nil
				break
			}
			group.Candidates = append(group.Candidates, candidate)
		}
		if len(group.Candidates) > 0 {
			seenPrimary[primaryContentID] = struct{}{}
			out = append(out, group)
		}
	}
	return out
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
	evidenceByPrimary := make(
		map[int64][]cardgenpkg.EventEvidenceSourceV1, len(in.Evidence))
	evidenceCandidatesByPrimary := make(
		map[int64][]runcontext.ToolCandidateV1, len(in.Evidence))
	for _, group := range in.Evidence {
		if group.PrimaryContentID <= 0 || len(group.Candidates) == 0 ||
			group.Candidates[0].Item.ID != group.PrimaryContentID {
			return nil, nonRetryable(types.NewAppError(
				types.CodeValidation,
				"compiled Tool card evidence is invalid", nil))
		}
		if err := validateToolCandidatesV2(
			snapshot, group.Candidates); err != nil {
			return nil, nonRetryable(err)
		}
		if err := a.validateCanonicalToolCandidatesV2(
			ctx, expected, in.Run.Snapshot, group.Candidates); err != nil {
			return nil, retryableOrNot(err)
		}
		sources, err := toolCardEvidenceSourcesV2(
			snapshot, group.Candidates)
		if err != nil {
			return nil, nonRetryable(err)
		}
		if _, duplicate := evidenceByPrimary[group.PrimaryContentID]; duplicate {
			return nil, nonRetryable(types.NewAppError(
				types.CodeValidation,
				"compiled Tool card evidence is duplicated", nil))
		}
		evidenceByPrimary[group.PrimaryContentID] = sources
		evidenceCandidatesByPrimary[group.PrimaryContentID] =
			group.Candidates
	}
	if in.EvidenceRequired && len(evidenceByPrimary) == 0 {
		return nil, nonRetryable(types.NewAppError(
			types.CodeValidation,
			"compiled Tool card evidence is required", nil))
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
			var body string
			evidence := evidenceByPrimary[candidate.Scored.Item.ID]
			if in.EvidenceRequired && len(evidence) == 0 {
				return ToolGeneratedCardV1{}, types.NewAppError(
					types.CodeValidation,
					"compiled Tool card candidate lacks required evidence",
					nil)
			}
			if len(evidence) > 0 {
				evidenceConsumer, ok :=
					a.cardgen.(compiledEvidenceCardGeneratorV1)
				if !ok {
					return ToolGeneratedCardV1{}, types.NewAppError(
						types.CodeInternal,
						"compiled Tool evidence card generator is unsupported",
						nil)
				}
				body, err = evidenceConsumer.GenerateWithEvidencePolicyV1(
					ctx, snapshot.Definition.TenantID, in.UserID,
					candidate.Scored, evidence, in.TraceID,
					taskManual, policy, beforeSpend)
			} else {
				body, err = consumer.GenerateWithPolicyV1(
					ctx, snapshot.Definition.TenantID, in.UserID,
					candidate.Scored, in.TraceID,
					taskManual, policy, beforeSpend)
			}
			if err != nil {
				return ToolGeneratedCardV1{}, err
			}
			cardEvidence := make(
				[]ToolCardEvidenceV1, len(evidence))
			evidenceCandidates :=
				evidenceCandidatesByPrimary[candidate.Scored.Item.ID]
			if len(evidenceCandidates) != len(evidence) {
				return ToolGeneratedCardV1{}, types.NewAppError(
					types.CodeValidation,
					"compiled Tool card evidence mapping is invalid", nil)
			}
			for i := range evidence {
				cardEvidence[i] = ToolCardEvidenceV1{
					Candidate: evidenceCandidates[i],
					Source:    evidence[i],
				}
			}
			return ToolGeneratedCardV1{
				InvocationDigest: candidate.InvocationDigest,
				Card: GeneratedCard{
					Scored: candidate.Scored,
					BodyMD: body,
				},
				Evidence: cardEvidence,
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

func toolCardEvidenceSourcesV2(
	snapshot runcontext.CompiledSnapshotV2,
	candidates []runcontext.ToolCandidateV1,
) ([]cardgenpkg.EventEvidenceSourceV1, error) {
	if len(candidates) == 0 || len(candidates) > 8 {
		return nil, types.NewAppError(types.CodeValidation,
			"compiled Tool card evidence is outside bounds", nil)
	}
	bindings := make(
		map[string]runcontext.ToolBindingV1, len(snapshot.ToolBindings))
	for _, binding := range snapshot.ToolBindings {
		bindings[binding.InvocationDigest] = binding
	}
	out := make([]cardgenpkg.EventEvidenceSourceV1, 0, len(candidates))
	for index, candidate := range candidates {
		binding, ok := bindings[candidate.InvocationDigest]
		discoveredAt := candidate.Item.CreatedAt
		if discoveredAt.IsZero() {
			discoveredAt = candidate.Item.FetchedAt
		}
		if !ok || discoveredAt.IsZero() {
			return nil, types.NewAppError(types.CodeValidation,
				"compiled Tool card evidence metadata is invalid", nil)
		}
		publishedAt := candidate.Item.PublishedAt
		if publishedAt != nil {
			published := publishedAt.Round(0).UTC().Truncate(time.Microsecond)
			publishedAt = &published
		}
		source := cardgenpkg.EventEvidenceSourceV1{
			ContentItemID: candidate.Item.ID,
			Metadata: types.StructuredEvidenceSourceV1{
				Ref:         "source-" + strconv.Itoa(index+1),
				Title:       strings.TrimSpace(candidate.Item.Title),
				SourceTitle: strings.TrimSpace(binding.Contract.ToolName),
				Platform:    strings.TrimSpace(binding.Contract.Platform),
				SourceURL:   strings.TrimSpace(candidate.Item.URL),
				PublishedAt: publishedAt,
				DiscoveredAt: discoveredAt.Round(0).UTC().
					Truncate(time.Microsecond),
			},
			EvidenceText: cardgenpkg.StructuredEvidenceTextV1(
				candidate.Item),
		}
		if source.Validate(index) != nil {
			return nil, types.NewAppError(types.CodeValidation,
				"compiled Tool card evidence source is invalid", nil)
		}
		out = append(out, source)
	}
	return out, nil
}
