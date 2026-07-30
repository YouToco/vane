// Package taskhealth converts operational task facts into a bounded,
// user-facing Web projection. It never returns raw provider, driver, SQL, or
// Temporal errors.
package taskhealth

import (
	"math"
	"strings"
	"time"

	"github.com/YouToco/vane/types"
)

const SchemaVersionV1 = "vane.task-health/v1"

type StateV1 string

const (
	StateHealthyV1   StateV1 = "healthy"
	StateAttentionV1 StateV1 = "attention"
	StateWaitingV1   StateV1 = "waiting"
	StateNeverRunV1  StateV1 = "never_run"
)

type IssueV1 string

const (
	IssueNoneV1                   IssueV1 = ""
	IssueCoverageIncompleteV1     IssueV1 = "coverage_incomplete"
	IssueAcquisitionUnavailableV1 IssueV1 = "acquisition_unavailable"
	IssueQuotaPausedV1            IssueV1 = "quota_paused"
	IssueModelUnavailableV1       IssueV1 = "model_temporarily_unavailable"
	IssueDeliveryFailedV1         IssueV1 = "delivery_failed"
	IssueCheckInterruptedV1       IssueV1 = "check_interrupted"
	IssueCheckFailedV1            IssueV1 = "check_failed"
)

type RecommendedActionV1 string

const (
	ActionNoneV1           RecommendedActionV1 = ""
	ActionWaitForRetryV1   RecommendedActionV1 = "wait_for_retry"
	ActionReviewTaskV1     RecommendedActionV1 = "review_task"
	ActionReviewUsageV1    RecommendedActionV1 = "review_usage"
	ActionReviewDeliveryV1 RecommendedActionV1 = "review_delivery"
	ActionRunAgainV1       RecommendedActionV1 = "run_again"
	ActionContactSupportV1 RecommendedActionV1 = "contact_support"
)

type CostCoverageV1 string

const (
	CostCoverageNoneV1               CostCoverageV1 = "none"
	CostCoverageLLMOnlyV1            CostCoverageV1 = "llm_only"
	CostCoverageLLMPartialV1         CostCoverageV1 = "llm_partial"
	CostCoverageToolsOnlyV1          CostCoverageV1 = "tools_only"
	CostCoverageToolsPartialV1       CostCoverageV1 = "tools_partial"
	CostCoverageLLMAndToolsV1        CostCoverageV1 = "llm_and_tools"
	CostCoverageLLMAndToolsPartialV1 CostCoverageV1 = "llm_and_tools_partial"
)

type AcquisitionFailureV1 string

const (
	AcquisitionFailureNoneV1           AcquisitionFailureV1 = ""
	AcquisitionFailureTimeoutV1        AcquisitionFailureV1 = "timeout"
	AcquisitionFailureProviderV1       AcquisitionFailureV1 = "provider_error"
	AcquisitionFailureInvalidRequestV1 AcquisitionFailureV1 = "invalid_request"
	AcquisitionFailureUsageLimitV1     AcquisitionFailureV1 = "usage_limit"
	AcquisitionFailureInternalV1       AcquisitionFailureV1 = "internal"
)

type BudgetStateV1 string

const (
	BudgetNotConfiguredV1 BudgetStateV1 = "not_configured"
	BudgetOKV1            BudgetStateV1 = "ok"
	BudgetWarningV1       BudgetStateV1 = "warning"
	BudgetExhaustedV1     BudgetStateV1 = "exhausted"
	BudgetIncompleteV1    BudgetStateV1 = "incomplete"
)

type LatestCheckV1 struct {
	Result         types.RunResultV1
	SourceCoverage types.RunCompletenessV1
	Processing     types.RunCompletenessV1
	FailureCode    string
	FinalizedAt    time.Time
}

type AcquisitionSummaryV1 struct {
	Total         int                  `json:"total"`
	Failing       int                  `json:"failing"`
	MaxFailCount  int                  `json:"max_fail_count"`
	FailureReason AcquisitionFailureV1 `json:"failure_reason,omitempty"`
}

// UsageV1 represents attribution facts. Every component is nullable so
// "unknown" cannot collapse into a real zero.
type UsageV1 struct {
	LLMCostUSD        *float64
	LLMCalls          *int64
	LLMPricedCalls    *int64
	LLMEstimatedCalls *int64
	ToolCostUSD       *float64
	ToolCalls         *int64
	// ToolPricedCalls counts Tool rows with an explicit provider cost receipt.
	// It may be lower than ToolCalls; the known subtotal remains useful but is
	// then a lower bound rather than complete attribution.
	ToolPricedCalls       *int64
	ToolEstimatedCalls    *int64
	KnownCosts            []CurrencyCostV1
	PromptTokens          *int64
	PromptCacheHitTokens  *int64
	PromptCacheMissTokens *int64
	CompletionTokens      *int64
	ReasoningTokens       *int64
	WindowStart           time.Time
	WindowEnd             time.Time
	BudgetUSD             *float64
}

type AccessV1 struct {
	Role                  types.MembershipRole
	TaskAccessVerified    bool
	DefinitionEditEnabled bool
}

type PermissionsV1 struct {
	Role         types.MembershipRole `json:"role"`
	CanRun       bool                 `json:"can_run"`
	CanPause     bool                 `json:"can_pause"`
	CanEdit      bool                 `json:"can_edit"`
	CanDelete    bool                 `json:"can_delete"`
	CanViewUsage bool                 `json:"can_view_usage"`
}

type CurrencyCostV1 struct {
	Currency string  `json:"currency"`
	Amount   float64 `json:"amount"`
}

type UsageProjectionV1 struct {
	KnownCostUSD          float64          `json:"known_cost_usd"`
	KnownCosts            []CurrencyCostV1 `json:"known_costs"`
	Coverage              CostCoverageV1   `json:"coverage"`
	LLMCalls              *int64           `json:"llm_calls,omitempty"`
	LLMPricedCalls        *int64           `json:"llm_priced_calls,omitempty"`
	LLMEstimatedCalls     *int64           `json:"llm_estimated_calls,omitempty"`
	ToolCalls             *int64           `json:"tool_calls,omitempty"`
	ToolPricedCalls       *int64           `json:"tool_priced_calls,omitempty"`
	ToolEstimatedCalls    *int64           `json:"tool_estimated_calls,omitempty"`
	PromptTokens          *int64           `json:"prompt_tokens,omitempty"`
	PromptCacheHitTokens  *int64           `json:"prompt_cache_hit_tokens,omitempty"`
	PromptCacheMissTokens *int64           `json:"prompt_cache_miss_tokens,omitempty"`
	CompletionTokens      *int64           `json:"completion_tokens,omitempty"`
	ReasoningTokens       *int64           `json:"reasoning_tokens,omitempty"`
	WindowStart           *time.Time       `json:"window_start,omitempty"`
	WindowEnd             *time.Time       `json:"window_end,omitempty"`
	BudgetUSD             *float64         `json:"budget_usd,omitempty"`
	BudgetState           BudgetStateV1    `json:"budget_state"`
}

type ProjectionV1 struct {
	SchemaVersion     string               `json:"schema_version"`
	State             StateV1              `json:"state"`
	Issue             IssueV1              `json:"issue,omitempty"`
	RecommendedAction RecommendedActionV1  `json:"recommended_action,omitempty"`
	LastCheckedAt     *time.Time           `json:"last_checked_at,omitempty"`
	Acquisition       AcquisitionSummaryV1 `json:"acquisition"`
	Usage             *UsageProjectionV1   `json:"usage,omitempty"`
	Permissions       PermissionsV1        `json:"permissions"`
}

func ProjectV1(
	latest *LatestCheckV1,
	acquisition AcquisitionSummaryV1,
	usage UsageV1,
	access AccessV1,
) ProjectionV1 {
	permissions := projectPermissionsV1(access)
	normalizedAcquisition, acquisitionValid :=
		normalizeAcquisitionV1(acquisition)
	projection := ProjectionV1{
		SchemaVersion: SchemaVersionV1,
		Acquisition:   normalizedAcquisition,
		Permissions:   permissions,
	}
	if permissions.CanViewUsage {
		projected := projectUsageV1(usage)
		projection.Usage = &projected
	}
	if !acquisitionValid {
		return failedProjectionV1(projection, IssueCheckFailedV1,
			ActionContactSupportV1)
	}
	if latest == nil || latest.FinalizedAt.IsZero() {
		projection.State = StateNeverRunV1
		return projection
	}
	checkedAt := latest.FinalizedAt.Round(0).UTC()
	projection.LastCheckedAt = &checkedAt
	if !validLatestCheckV1(*latest) {
		return failedProjectionV1(projection, IssueCheckFailedV1,
			ActionContactSupportV1)
	}

	switch latest.Result {
	case types.RunResultContent, types.RunResultQuiet:
		switch {
		case normalizedAcquisition.Failing > 0:
			projection.State = StateAttentionV1
			projection.Issue = IssueAcquisitionUnavailableV1
			projection.RecommendedAction =
				acquisitionFailureActionV1(
					normalizedAcquisition.FailureReason)
		case latest.SourceCoverage == types.RunCompletenessPartial ||
			latest.Processing == types.RunCompletenessPartial:
			projection.State = StateAttentionV1
			projection.Issue = IssueCoverageIncompleteV1
			projection.RecommendedAction = ActionWaitForRetryV1
		default:
			projection.State = StateHealthyV1
		}
	case types.RunResultInterrupted:
		projection.State = StateWaitingV1
		projection.Issue = IssueCheckInterruptedV1
		projection.RecommendedAction = ActionRunAgainV1
	case types.RunResultFailed:
		projection.State = StateAttentionV1
		projection.Issue, projection.RecommendedAction =
			failurePresentationV1(latest.FailureCode)
	}
	return projection
}

func failedProjectionV1(
	projection ProjectionV1,
	issue IssueV1,
	action RecommendedActionV1,
) ProjectionV1 {
	projection.State = StateAttentionV1
	projection.Issue = issue
	projection.RecommendedAction = action
	return projection
}

func validLatestCheckV1(latest LatestCheckV1) bool {
	switch latest.Result {
	case types.RunResultContent, types.RunResultQuiet:
		if latest.FailureCode != "" {
			return false
		}
	case types.RunResultFailed, types.RunResultInterrupted:
		if strings.TrimSpace(latest.FailureCode) == "" {
			return false
		}
		if latest.Processing != types.RunCompletenessPartial {
			return false
		}
	default:
		return false
	}
	return (latest.SourceCoverage == types.RunCompletenessComplete ||
		latest.SourceCoverage == types.RunCompletenessPartial) &&
		(latest.Processing == types.RunCompletenessComplete ||
			latest.Processing == types.RunCompletenessPartial)
}

func failurePresentationV1(code string) (IssueV1, RecommendedActionV1) {
	switch types.ErrCode(strings.TrimSpace(code)) {
	case types.CodeQuotaExceeded:
		return IssueQuotaPausedV1, ActionReviewUsageV1
	case types.CodeFetchTimeout, types.CodeFetchRateLimit:
		return IssueAcquisitionUnavailableV1, ActionReviewTaskV1
	case types.CodeLLMRateLimit, types.CodeLLMUnavailable:
		return IssueModelUnavailableV1, ActionWaitForRetryV1
	case types.CodePushFailed:
		return IssueDeliveryFailedV1, ActionReviewDeliveryV1
	default:
		switch strings.TrimSpace(code) {
		case "workflow_canceled", "workflow_terminated", "workflow_timeout":
			return IssueCheckInterruptedV1, ActionRunAgainV1
		default:
			return IssueCheckFailedV1, ActionContactSupportV1
		}
	}
}

func normalizeAcquisitionV1(
	in AcquisitionSummaryV1,
) (AcquisitionSummaryV1, bool) {
	valid := in.Total >= 0 && in.Failing >= 0 &&
		in.Failing <= in.Total && in.MaxFailCount >= 0 &&
		((in.Failing == 0 && in.MaxFailCount == 0 &&
			in.FailureReason == AcquisitionFailureNoneV1) ||
			(in.Failing > 0 &&
				validAcquisitionFailureV1(in.FailureReason)))
	if !valid {
		return AcquisitionSummaryV1{}, false
	}
	return in, true
}

func validAcquisitionFailureV1(reason AcquisitionFailureV1) bool {
	switch reason {
	case AcquisitionFailureTimeoutV1,
		AcquisitionFailureProviderV1,
		AcquisitionFailureInvalidRequestV1,
		AcquisitionFailureUsageLimitV1,
		AcquisitionFailureInternalV1:
		return true
	default:
		return false
	}
}

func acquisitionFailureActionV1(
	reason AcquisitionFailureV1,
) RecommendedActionV1 {
	switch reason {
	case AcquisitionFailureTimeoutV1, AcquisitionFailureProviderV1:
		return ActionWaitForRetryV1
	case AcquisitionFailureInvalidRequestV1:
		return ActionReviewTaskV1
	case AcquisitionFailureUsageLimitV1:
		return ActionReviewUsageV1
	default:
		return ActionContactSupportV1
	}
}

func projectUsageV1(in UsageV1) UsageProjectionV1 {
	out := UsageProjectionV1{
		Coverage:    CostCoverageNoneV1,
		BudgetState: BudgetNotConfiguredV1,
		KnownCosts:  make([]CurrencyCostV1, 0),
	}
	llmPriced := in.LLMPricedCalls
	if llmPriced == nil && in.LLMCostUSD != nil && in.LLMCalls != nil {
		// Compatibility for internal callers created before the pricing ledger:
		// an explicit amount and call count meant every LLM row was priced.
		v := *in.LLMCalls
		llmPriced = &v
	}
	llmEstimated := in.LLMEstimatedCalls
	if llmEstimated == nil {
		v := int64(0)
		llmEstimated = &v
	}
	toolEstimated := in.ToolEstimatedCalls
	if toolEstimated == nil {
		v := int64(0)
		toolEstimated = &v
	}
	llmKnown := in.LLMCostUSD != nil && in.LLMCalls != nil &&
		llmPriced != nil &&
		validMoneyV1(*in.LLMCostUSD) && *in.LLMCalls >= 0 &&
		*llmPriced >= 0 && *llmPriced <= *in.LLMCalls &&
		*llmEstimated >= 0 && *llmEstimated <= *llmPriced &&
		(*llmPriced > 0 || *in.LLMCalls == 0)
	llmComplete := llmKnown && *llmPriced == *in.LLMCalls &&
		*llmEstimated == 0
	toolsKnown := in.ToolCostUSD != nil && in.ToolCalls != nil &&
		in.ToolPricedCalls != nil &&
		validMoneyV1(*in.ToolCostUSD) && *in.ToolCalls >= 0 &&
		*in.ToolPricedCalls >= 0 &&
		*in.ToolPricedCalls <= *in.ToolCalls &&
		*toolEstimated >= 0 &&
		*toolEstimated <= *in.ToolPricedCalls &&
		(*in.ToolPricedCalls > 0 || *in.ToolCalls == 0)
	toolsComplete := toolsKnown &&
		*in.ToolPricedCalls == *in.ToolCalls &&
		*toolEstimated == 0
	switch {
	case llmKnown && toolsKnown:
		sum := *in.LLMCostUSD + *in.ToolCostUSD
		if validMoneyV1(sum) {
			out.KnownCostUSD = sum
			if llmComplete && toolsComplete {
				out.Coverage = CostCoverageLLMAndToolsV1
			} else {
				out.Coverage = CostCoverageLLMAndToolsPartialV1
			}
			llmCalls, pricedLLMCalls, toolCalls, pricedToolCalls :=
				*in.LLMCalls, *llmPriced, *in.ToolCalls, *in.ToolPricedCalls
			out.LLMCalls, out.ToolCalls = &llmCalls, &toolCalls
			out.LLMPricedCalls = &pricedLLMCalls
			out.ToolPricedCalls = &pricedToolCalls
			llmEstimated, toolEstimated :=
				*llmEstimated, *toolEstimated
			out.LLMEstimatedCalls = &llmEstimated
			out.ToolEstimatedCalls = &toolEstimated
		}
	case llmKnown:
		out.KnownCostUSD = *in.LLMCostUSD
		if llmComplete {
			out.Coverage = CostCoverageLLMOnlyV1
		} else {
			out.Coverage = CostCoverageLLMPartialV1
		}
		llmCalls, pricedLLMCalls := *in.LLMCalls, *llmPriced
		out.LLMCalls = &llmCalls
		out.LLMPricedCalls = &pricedLLMCalls
		estimated := *llmEstimated
		out.LLMEstimatedCalls = &estimated
	case toolsKnown:
		out.KnownCostUSD = *in.ToolCostUSD
		if toolsComplete {
			out.Coverage = CostCoverageToolsOnlyV1
		} else {
			out.Coverage = CostCoverageToolsPartialV1
		}
		toolCalls, pricedToolCalls :=
			*in.ToolCalls, *in.ToolPricedCalls
		out.ToolCalls = &toolCalls
		out.ToolPricedCalls = &pricedToolCalls
		estimated := *toolEstimated
		out.ToolEstimatedCalls = &estimated
	}
	if in.LLMCalls != nil && *in.LLMCalls >= 0 && out.LLMCalls == nil {
		v := *in.LLMCalls
		out.LLMCalls = &v
	}
	if in.LLMPricedCalls != nil && *in.LLMPricedCalls >= 0 &&
		in.LLMCalls != nil && *in.LLMPricedCalls <= *in.LLMCalls &&
		out.LLMPricedCalls == nil {
		v := *in.LLMPricedCalls
		out.LLMPricedCalls = &v
	}
	if in.LLMEstimatedCalls != nil && *in.LLMEstimatedCalls >= 0 &&
		in.LLMPricedCalls != nil && *in.LLMEstimatedCalls <= *in.LLMPricedCalls &&
		out.LLMEstimatedCalls == nil {
		v := *in.LLMEstimatedCalls
		out.LLMEstimatedCalls = &v
	}
	if in.ToolEstimatedCalls != nil && *in.ToolEstimatedCalls >= 0 &&
		in.ToolPricedCalls != nil && *in.ToolEstimatedCalls <= *in.ToolPricedCalls &&
		out.ToolEstimatedCalls == nil {
		v := *in.ToolEstimatedCalls
		out.ToolEstimatedCalls = &v
	}
	copyToken := func(v *int64) *int64 {
		if v == nil || *v < 0 {
			return nil
		}
		n := *v
		return &n
	}
	out.PromptTokens = copyToken(in.PromptTokens)
	out.PromptCacheHitTokens = copyToken(in.PromptCacheHitTokens)
	out.PromptCacheMissTokens = copyToken(in.PromptCacheMissTokens)
	out.CompletionTokens = copyToken(in.CompletionTokens)
	out.ReasoningTokens = copyToken(in.ReasoningTokens)
	for _, cost := range in.KnownCosts {
		if (cost.Currency == "USD" || cost.Currency == "CNY") &&
			validMoneyV1(cost.Amount) {
			out.KnownCosts = append(out.KnownCosts, cost)
		}
	}
	usdBudgetComparable := true
	for _, cost := range out.KnownCosts {
		if cost.Currency != "USD" && cost.Amount > 0 {
			usdBudgetComparable = false
			break
		}
	}
	if !in.WindowStart.IsZero() && in.WindowEnd.After(in.WindowStart) {
		start := in.WindowStart.Round(0).UTC()
		end := in.WindowEnd.Round(0).UTC()
		out.WindowStart, out.WindowEnd = &start, &end
	}
	if in.BudgetUSD != nil && validMoneyV1(*in.BudgetUSD) && *in.BudgetUSD > 0 {
		budget := *in.BudgetUSD
		out.BudgetUSD = &budget
		ratio := out.KnownCostUSD / budget
		switch {
		case ratio >= 1:
			out.BudgetState = BudgetExhaustedV1
		case ratio >= 0.8:
			out.BudgetState = BudgetWarningV1
		case out.Coverage == CostCoverageLLMAndToolsV1 && usdBudgetComparable:
			out.BudgetState = BudgetOKV1
		default:
			out.BudgetState = BudgetIncompleteV1
		}
	}
	return out
}

func projectPermissionsV1(in AccessV1) PermissionsV1 {
	out := PermissionsV1{Role: in.Role}
	switch in.Role {
	case types.MembershipRoleOwner,
		types.MembershipRoleAdmin,
		types.MembershipRoleMember:
	default:
		out.Role = ""
	}
	if !in.TaskAccessVerified {
		return out
	}
	// Current task actions authorize the authenticated user against the exact
	// task owner, not the tenant membership label. ListTaskBriefsV1 has already
	// locked and verified that same tenant/user/task tuple before this
	// projection is built, so these booleans mirror the real action endpoints.
	out.CanRun = true
	out.CanPause = true
	out.CanEdit = in.DefinitionEditEnabled
	out.CanDelete = true
	out.CanViewUsage = true
	return out
}

func validMoneyV1(value float64) bool {
	return value >= 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}
