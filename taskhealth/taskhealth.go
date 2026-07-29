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
	IssueNoneV1               IssueV1 = ""
	IssueCoverageIncompleteV1 IssueV1 = "coverage_incomplete"
	IssueSourcesUnavailableV1 IssueV1 = "sources_unavailable"
	IssueQuotaPausedV1        IssueV1 = "quota_paused"
	IssueModelUnavailableV1   IssueV1 = "model_temporarily_unavailable"
	IssueDeliveryFailedV1     IssueV1 = "delivery_failed"
	IssueCheckInterruptedV1   IssueV1 = "check_interrupted"
	IssueCheckFailedV1        IssueV1 = "check_failed"
)

type RecommendedActionV1 string

const (
	ActionNoneV1           RecommendedActionV1 = ""
	ActionWaitForRetryV1   RecommendedActionV1 = "wait_for_retry"
	ActionReviewSourcesV1  RecommendedActionV1 = "review_sources"
	ActionReviewUsageV1    RecommendedActionV1 = "review_usage"
	ActionReviewDeliveryV1 RecommendedActionV1 = "review_delivery"
	ActionRunAgainV1       RecommendedActionV1 = "run_again"
	ActionContactSupportV1 RecommendedActionV1 = "contact_support"
)

type CostCoverageV1 string

const (
	CostCoverageNoneV1        CostCoverageV1 = "none"
	CostCoverageLLMOnlyV1     CostCoverageV1 = "llm_only"
	CostCoverageToolsOnlyV1   CostCoverageV1 = "tools_only"
	CostCoverageLLMAndToolsV1 CostCoverageV1 = "llm_and_tools"
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

type SourceSummaryV1 struct {
	Total        int `json:"total"`
	Failing      int `json:"failing"`
	MaxFailCount int `json:"max_fail_count"`
}

// UsageV1 represents attribution facts. Every component is nullable so
// "unknown" cannot collapse into a real zero.
type UsageV1 struct {
	LLMCostUSD  *float64
	LLMCalls    *int64
	ToolCostUSD *float64
	ToolCalls   *int64
	WindowStart time.Time
	WindowEnd   time.Time
	BudgetUSD   *float64
}

type AccessV1 struct {
	Role                  types.MembershipRole
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

type UsageProjectionV1 struct {
	KnownCostUSD float64        `json:"known_cost_usd"`
	Coverage     CostCoverageV1 `json:"coverage"`
	LLMCalls     *int64         `json:"llm_calls,omitempty"`
	ToolCalls    *int64         `json:"tool_calls,omitempty"`
	WindowStart  *time.Time     `json:"window_start,omitempty"`
	WindowEnd    *time.Time     `json:"window_end,omitempty"`
	BudgetUSD    *float64       `json:"budget_usd,omitempty"`
	BudgetState  BudgetStateV1  `json:"budget_state"`
}

type ProjectionV1 struct {
	SchemaVersion     string              `json:"schema_version"`
	State             StateV1             `json:"state"`
	Issue             IssueV1             `json:"issue,omitempty"`
	RecommendedAction RecommendedActionV1 `json:"recommended_action,omitempty"`
	LastCheckedAt     *time.Time          `json:"last_checked_at,omitempty"`
	Sources           SourceSummaryV1     `json:"sources"`
	Usage             *UsageProjectionV1  `json:"usage,omitempty"`
	Permissions       PermissionsV1       `json:"permissions"`
}

func ProjectV1(
	latest *LatestCheckV1,
	sources SourceSummaryV1,
	usage UsageV1,
	access AccessV1,
) ProjectionV1 {
	permissions := projectPermissionsV1(access)
	normalizedSources, sourcesValid := normalizeSourcesV1(sources)
	projection := ProjectionV1{
		SchemaVersion: SchemaVersionV1,
		Sources:       normalizedSources,
		Permissions:   permissions,
	}
	if permissions.CanViewUsage {
		projected := projectUsageV1(usage)
		projection.Usage = &projected
	}
	if !sourcesValid {
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
		case normalizedSources.Failing > 0:
			projection.State = StateAttentionV1
			projection.Issue = IssueSourcesUnavailableV1
			projection.RecommendedAction = ActionReviewSourcesV1
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
		return IssueSourcesUnavailableV1, ActionReviewSourcesV1
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

func normalizeSourcesV1(in SourceSummaryV1) (SourceSummaryV1, bool) {
	valid := in.Total >= 0 && in.Failing >= 0 &&
		in.Failing <= in.Total && in.MaxFailCount >= 0 &&
		((in.Failing == 0 && in.MaxFailCount == 0) ||
			(in.Failing > 0 && in.MaxFailCount > 0))
	if !valid {
		return SourceSummaryV1{}, false
	}
	return in, true
}

func projectUsageV1(in UsageV1) UsageProjectionV1 {
	out := UsageProjectionV1{
		Coverage:    CostCoverageNoneV1,
		BudgetState: BudgetNotConfiguredV1,
	}
	llmKnown := in.LLMCostUSD != nil && in.LLMCalls != nil &&
		validMoneyV1(*in.LLMCostUSD) && *in.LLMCalls >= 0
	toolsKnown := in.ToolCostUSD != nil && in.ToolCalls != nil &&
		validMoneyV1(*in.ToolCostUSD) && *in.ToolCalls >= 0
	switch {
	case llmKnown && toolsKnown:
		sum := *in.LLMCostUSD + *in.ToolCostUSD
		if validMoneyV1(sum) {
			out.KnownCostUSD = sum
			out.Coverage = CostCoverageLLMAndToolsV1
			llmCalls, toolCalls := *in.LLMCalls, *in.ToolCalls
			out.LLMCalls, out.ToolCalls = &llmCalls, &toolCalls
		}
	case llmKnown:
		out.KnownCostUSD = *in.LLMCostUSD
		out.Coverage = CostCoverageLLMOnlyV1
		llmCalls := *in.LLMCalls
		out.LLMCalls = &llmCalls
	case toolsKnown:
		out.KnownCostUSD = *in.ToolCostUSD
		out.Coverage = CostCoverageToolsOnlyV1
		toolCalls := *in.ToolCalls
		out.ToolCalls = &toolCalls
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
		case out.Coverage == CostCoverageLLMAndToolsV1:
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
	case types.MembershipRoleOwner:
		out.CanRun = true
		out.CanPause = true
		out.CanEdit = in.DefinitionEditEnabled
		out.CanDelete = true
		out.CanViewUsage = true
	case types.MembershipRoleAdmin:
		out.CanRun = true
		out.CanPause = true
		out.CanEdit = in.DefinitionEditEnabled
		out.CanViewUsage = true
	case types.MembershipRoleMember:
		// Members remain read-only until team task permissions have an
		// explicit authority model.
	default:
		out.Role = ""
	}
	return out
}

func validMoneyV1(value float64) bool {
	return value >= 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}
