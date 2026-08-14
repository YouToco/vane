package taskhealth

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/YouToco/vane/server/types"
)

func TestProjectV1HealthyAndPartialResults(t *testing.T) {
	now := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
	healthy := ProjectV1(&LatestCheckV1{
		Result:         types.RunResultContent,
		SourceCoverage: types.RunCompletenessComplete,
		Processing:     types.RunCompletenessComplete,
		FinalizedAt:    now,
	}, AcquisitionSummaryV1{Total: 2}, UsageV1{}, AccessV1{})
	if healthy.State != StateHealthyV1 || healthy.Issue != IssueNoneV1 {
		t.Fatalf("healthy projection = %#v", healthy)
	}

	partial := ProjectV1(&LatestCheckV1{
		Result:         types.RunResultQuiet,
		SourceCoverage: types.RunCompletenessPartial,
		Processing:     types.RunCompletenessComplete,
		FinalizedAt:    now,
	}, AcquisitionSummaryV1{Total: 2}, UsageV1{}, AccessV1{})
	if partial.State != StateAttentionV1 ||
		partial.Issue != IssueCoverageIncompleteV1 ||
		partial.RecommendedAction != ActionWaitForRetryV1 {
		t.Fatalf("partial projection = %#v", partial)
	}
}

func TestProjectV1AcquisitionFailureTakesPrecedence(t *testing.T) {
	got := ProjectV1(&LatestCheckV1{
		Result:         types.RunResultContent,
		SourceCoverage: types.RunCompletenessPartial,
		Processing:     types.RunCompletenessPartial,
		FinalizedAt:    time.Now(),
	}, AcquisitionSummaryV1{
		Total: 2, Failing: 1, MaxFailCount: 4,
		FailureReason: AcquisitionFailureInvalidRequestV1,
	},
		UsageV1{}, AccessV1{})
	if got.Issue != IssueAcquisitionUnavailableV1 ||
		got.RecommendedAction != ActionReviewTaskV1 {
		t.Fatalf("acquisition failure projection = %#v", got)
	}
}

func TestProjectV1AcquisitionFailureActionsAreActionable(t *testing.T) {
	for _, test := range []struct {
		reason AcquisitionFailureV1
		action RecommendedActionV1
	}{
		{AcquisitionFailureTimeoutV1, ActionWaitForRetryV1},
		{AcquisitionFailureProviderV1, ActionWaitForRetryV1},
		{AcquisitionFailureInvalidRequestV1, ActionReviewTaskV1},
		{AcquisitionFailureUsageLimitV1, ActionReviewUsageV1},
		{AcquisitionFailureInternalV1, ActionContactSupportV1},
	} {
		got := ProjectV1(&LatestCheckV1{
			Result:         types.RunResultContent,
			SourceCoverage: types.RunCompletenessPartial,
			Processing:     types.RunCompletenessComplete,
			FinalizedAt:    time.Now(),
		}, AcquisitionSummaryV1{
			Total: 1, Failing: 1, MaxFailCount: 1,
			FailureReason: test.reason,
		}, UsageV1{}, AccessV1{})
		if got.RecommendedAction != test.action {
			t.Fatalf("reason=%q action=%q want=%q",
				test.reason, got.RecommendedAction, test.action)
		}
	}
}

func TestProjectV1SanitizesFailureCodes(t *testing.T) {
	now := time.Now()
	tests := []struct {
		code   string
		issue  IssueV1
		action RecommendedActionV1
	}{
		{string(types.CodeQuotaExceeded), IssueQuotaPausedV1, ActionReviewUsageV1},
		{string(types.CodeLLMRateLimit), IssueModelUnavailableV1, ActionWaitForRetryV1},
		{string(types.CodeFetchTimeout), IssueAcquisitionUnavailableV1, ActionReviewTaskV1},
		{string(types.CodePushFailed), IssueDeliveryFailedV1, ActionReviewDeliveryV1},
		{"workflow_terminated", IssueCheckInterruptedV1, ActionRunAgainV1},
		{"postgres password=secret", IssueCheckFailedV1, ActionContactSupportV1},
	}
	for _, test := range tests {
		t.Run(test.code, func(t *testing.T) {
			got := ProjectV1(&LatestCheckV1{
				Result:         types.RunResultFailed,
				SourceCoverage: types.RunCompletenessPartial,
				Processing:     types.RunCompletenessPartial,
				FailureCode:    test.code,
				FinalizedAt:    now,
			}, AcquisitionSummaryV1{}, UsageV1{}, AccessV1{})
			if got.Issue != test.issue || got.RecommendedAction != test.action {
				t.Fatalf("projection = %#v", got)
			}
		})
	}
}

func TestProjectUsageV1NeverClaimsUnknownToolCostIsZero(t *testing.T) {
	llmCost := 1.25
	llmCalls := int64(3)
	llmOnly := projectUsageV1(UsageV1{
		LLMCostUSD: &llmCost,
		LLMCalls:   &llmCalls,
	})
	if llmOnly.Coverage != CostCoverageLLMOnlyV1 ||
		llmOnly.KnownCostUSD != 1.25 || llmOnly.ToolCalls != nil {
		t.Fatalf("LLM-only usage = %#v", llmOnly)
	}
	toolCost := 0.75
	toolCalls := int64(2)
	pricedToolCalls := int64(2)
	complete := projectUsageV1(UsageV1{
		LLMCostUSD:      &llmCost,
		LLMCalls:        &llmCalls,
		ToolCostUSD:     &toolCost,
		ToolCalls:       &toolCalls,
		ToolPricedCalls: &pricedToolCalls,
	})
	if complete.Coverage != CostCoverageLLMAndToolsV1 ||
		complete.KnownCostUSD != 2 ||
		complete.ToolCalls == nil || *complete.ToolCalls != 2 {
		t.Fatalf("complete usage = %#v", complete)
	}
	onePriced := int64(1)
	partial := projectUsageV1(UsageV1{
		LLMCostUSD:      &llmCost,
		LLMCalls:        &llmCalls,
		ToolCostUSD:     &toolCost,
		ToolCalls:       &toolCalls,
		ToolPricedCalls: &onePriced,
	})
	if partial.Coverage != CostCoverageLLMAndToolsPartialV1 ||
		partial.KnownCostUSD != 2 ||
		partial.ToolPricedCalls == nil ||
		*partial.ToolPricedCalls != 1 {
		t.Fatalf("partially priced tool usage = %#v", partial)
	}
	estimatedLLM := int64(1)
	llmPriced := llmCalls
	estimated := projectUsageV1(UsageV1{
		LLMCostUSD:        &llmCost,
		LLMCalls:          &llmCalls,
		LLMPricedCalls:    &llmPriced,
		LLMEstimatedCalls: &estimatedLLM,
	})
	if estimated.Coverage != CostCoverageLLMOnlyV1 ||
		estimated.LLMPricedCalls == nil ||
		*estimated.LLMPricedCalls != llmCalls-estimatedLLM ||
		estimated.LLMEstimatedCalls == nil ||
		*estimated.LLMEstimatedCalls != 1 {
		t.Fatalf("estimated LLM price was presented as complete: %#v", estimated)
	}
	estimatedTool := int64(1)
	estimatedTools := projectUsageV1(UsageV1{
		ToolCostUSD:        &toolCost,
		ToolCalls:          &toolCalls,
		ToolPricedCalls:    &pricedToolCalls,
		ToolEstimatedCalls: &estimatedTool,
	})
	if estimatedTools.Coverage != CostCoverageToolsPartialV1 ||
		estimatedTools.ToolPricedCalls == nil ||
		*estimatedTools.ToolPricedCalls != pricedToolCalls-estimatedTool {
		t.Fatalf("wildcard tool price was presented as complete: %#v", estimatedTools)
	}
	zeroEstimated := int64(0)
	exactToolsWithEstimatedLLM := projectUsageV1(UsageV1{
		LLMCostUSD:         &llmCost,
		LLMCalls:           &llmCalls,
		LLMPricedCalls:     &llmPriced,
		LLMEstimatedCalls:  &estimatedLLM,
		ToolCostUSD:        &toolCost,
		ToolCalls:          &toolCalls,
		ToolPricedCalls:    &pricedToolCalls,
		ToolEstimatedCalls: &zeroEstimated,
	})
	if exactToolsWithEstimatedLLM.Coverage != CostCoverageLLMAndToolsV1 ||
		exactToolsWithEstimatedLLM.BudgetState != BudgetNotConfiguredV1 {
		t.Fatalf("estimated LLM broke legacy v1 tool coverage: %#v",
			exactToolsWithEstimatedLLM)
	}
}

func TestProjectUsageV1BudgetStatesAndInvalidMoney(t *testing.T) {
	budget := 10.0
	zero := 0.0
	zeroCalls := int64(0)
	for _, test := range []struct {
		cost float64
		want BudgetStateV1
	}{
		{7.99, BudgetOKV1},
		{8, BudgetWarningV1},
		{10, BudgetExhaustedV1},
	} {
		cost := test.cost
		calls := int64(1)
		got := projectUsageV1(UsageV1{
			LLMCostUSD:      &cost,
			LLMCalls:        &calls,
			ToolCostUSD:     &zero,
			ToolCalls:       &zeroCalls,
			ToolPricedCalls: &zeroCalls,
			BudgetUSD:       &budget,
		})
		if got.BudgetState != test.want {
			t.Fatalf("cost %.2f budget state = %q, want %q", test.cost, got.BudgetState, test.want)
		}
	}
	nan := math.NaN()
	oneCall := int64(1)
	invalid := projectUsageV1(UsageV1{
		LLMCostUSD: &nan,
		LLMCalls:   &oneCall,
	})
	if invalid.Coverage != CostCoverageNoneV1 || invalid.KnownCostUSD != 0 {
		t.Fatalf("invalid money was presented as known: %#v", invalid)
	}
	partialCost := 1.0
	partial := projectUsageV1(UsageV1{
		LLMCostUSD: &partialCost,
		LLMCalls:   &oneCall,
		BudgetUSD:  &budget,
	})
	if partial.BudgetState != BudgetIncompleteV1 {
		t.Fatalf("partial low usage claimed budget safety: %#v", partial)
	}
	priced, estimated := int64(1), int64(0)
	cny := CurrencyCostV1{Currency: "CNY", Amount: 8}
	mixedCurrency := projectUsageV1(UsageV1{
		LLMCostUSD:         &partialCost,
		LLMCalls:           &oneCall,
		LLMPricedCalls:     &priced,
		LLMEstimatedCalls:  &estimated,
		ToolCostUSD:        &zero,
		ToolCalls:          &zeroCalls,
		ToolPricedCalls:    &zeroCalls,
		ToolEstimatedCalls: &zeroCalls,
		KnownCosts:         []CurrencyCostV1{cny},
		BudgetUSD:          &budget,
	})
	if mixedCurrency.BudgetState != BudgetIncompleteV1 {
		t.Fatalf("CNY amount cannot be compared to a USD budget: %#v", mixedCurrency)
	}
	estimatedHighCost := 20.0
	oneEstimated := int64(1)
	estimatedHigh := projectUsageV1(UsageV1{
		LLMCostUSD:         &estimatedHighCost,
		LLMCalls:           &oneCall,
		LLMPricedCalls:     &priced,
		LLMEstimatedCalls:  &oneEstimated,
		ToolCostUSD:        &zero,
		ToolCalls:          &zeroCalls,
		ToolPricedCalls:    &zeroCalls,
		ToolEstimatedCalls: &zeroCalls,
		BudgetUSD:          &budget,
	})
	if estimatedHigh.BudgetState != BudgetIncompleteV1 {
		t.Fatalf("estimated amount must not claim budget exhaustion: %#v", estimatedHigh)
	}
}

func TestProjectV1MalformedFactsFailClosed(t *testing.T) {
	got := ProjectV1(&LatestCheckV1{
		Result:      types.RunResultContent,
		FinalizedAt: time.Now(),
	}, AcquisitionSummaryV1{Total: 0, Failing: 2}, UsageV1{}, AccessV1{})
	if got.State != StateAttentionV1 || got.Issue != IssueCheckFailedV1 {
		t.Fatalf("malformed facts projected healthy: %#v", got)
	}
}

func TestProjectV1AcquisitionFailureDoesNotInventAStreak(t *testing.T) {
	got := ProjectV1(&LatestCheckV1{
		Result:         types.RunResultContent,
		SourceCoverage: types.RunCompletenessPartial,
		Processing:     types.RunCompletenessComplete,
		FinalizedAt:    time.Now(),
	}, AcquisitionSummaryV1{
		Total: 2, Failing: 2,
		FailureReason: AcquisitionFailureTimeoutV1,
	}, UsageV1{}, AccessV1{})
	if got.Acquisition.MaxFailCount != 0 ||
		got.RecommendedAction != ActionWaitForRetryV1 {
		t.Fatalf("source-free call failures invented streak=%#v", got)
	}
}

func TestProjectV1OmitsUsageWithoutPermission(t *testing.T) {
	cost := 6.0
	calls := int64(2)
	budget := 10.0
	got := ProjectV1(nil, AcquisitionSummaryV1{}, UsageV1{
		LLMCostUSD: &cost,
		LLMCalls:   &calls,
		BudgetUSD:  &budget,
	}, AccessV1{Role: types.MembershipRoleMember})
	if got.Permissions.CanViewUsage || got.Usage != nil {
		t.Fatalf("read-only member received usage: %#v", got)
	}
}

func TestProjectionV1JSONOmitsInvalidWindowAndUsesSnakeCase(t *testing.T) {
	got := ProjectV1(nil, AcquisitionSummaryV1{}, UsageV1{},
		AccessV1{
			Role:               types.MembershipRoleOwner,
			TaskAccessVerified: true,
		})
	raw, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	for _, forbidden := range []string{"0001-", `"Total"`, `"Failing"`, `"MaxFailCount"`} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("projection leaked invalid JSON field %q: %s", forbidden, text)
		}
	}
	if !strings.Contains(text, `"acquisition":{"total":0,"failing":0,"max_fail_count":0}`) {
		t.Fatalf("acquisition JSON shape mismatch: %s", text)
	}
}

func TestProjectUsageV1RejectsOverflowingTotal(t *testing.T) {
	max := math.MaxFloat64
	calls := int64(1)
	got := projectUsageV1(UsageV1{
		LLMCostUSD:      &max,
		LLMCalls:        &calls,
		ToolCostUSD:     &max,
		ToolCalls:       &calls,
		ToolPricedCalls: &calls,
	})
	if got.Coverage != CostCoverageNoneV1 || got.KnownCostUSD != 0 {
		t.Fatalf("overflowing total was presented: %#v", got)
	}
}

func TestProjectPermissionsV1FailsClosed(t *testing.T) {
	owner := projectPermissionsV1(AccessV1{
		Role:                  types.MembershipRoleOwner,
		TaskAccessVerified:    true,
		DefinitionEditEnabled: true,
	})
	if !owner.CanRun || !owner.CanPause || !owner.CanEdit ||
		!owner.CanDelete || !owner.CanViewUsage {
		t.Fatalf("owner permissions = %#v", owner)
	}
	admin := projectPermissionsV1(AccessV1{
		Role:                  types.MembershipRoleAdmin,
		TaskAccessVerified:    true,
		DefinitionEditEnabled: true,
	})
	if !admin.CanRun || !admin.CanPause || !admin.CanEdit ||
		!admin.CanDelete || !admin.CanViewUsage {
		t.Fatalf("admin permissions = %#v", admin)
	}
	member := projectPermissionsV1(AccessV1{
		Role:                  types.MembershipRoleMember,
		TaskAccessVerified:    true,
		DefinitionEditEnabled: true,
	})
	if !member.CanRun || !member.CanPause || !member.CanEdit ||
		!member.CanDelete || !member.CanViewUsage {
		t.Fatalf("task owner permissions drifted by membership label: %#v", member)
	}
	unknown := projectPermissionsV1(AccessV1{
		Role:                  types.MembershipRole("super-owner"),
		TaskAccessVerified:    true,
		DefinitionEditEnabled: true,
	})
	if unknown.Role != "" || !unknown.CanEdit || !unknown.CanDelete {
		t.Fatalf("verified task access drifted with unknown role: %#v", unknown)
	}
	unverified := projectPermissionsV1(AccessV1{
		Role:                  types.MembershipRoleOwner,
		DefinitionEditEnabled: true,
	})
	if unverified.CanRun || unverified.CanPause || unverified.CanEdit ||
		unverified.CanDelete || unverified.CanViewUsage {
		t.Fatalf("unverified task received authority: %#v", unverified)
	}
}
