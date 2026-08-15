package api

import (
	"log/slog"
	"net/http"
	"strconv"

	"github.com/YouToco/vane/server/auth"
	"github.com/YouToco/vane/server/store"
	"github.com/YouToco/vane/server/taskhealth"
	"github.com/YouToco/vane/server/types"
)

// handleListTaskBriefs returns immutable whole Briefs for one exact task.
// LatestCheck is independent of Items[0]: a quiet or failed check may be newer
// than the most recent non-empty Brief.
func (s *server) handleListTaskBriefs(w http.ResponseWriter, r *http.Request) {
	taskID := r.PathValue("id")
	if taskID == "" {
		writeError(w, http.StatusBadRequest, "缺少 schedule id")
		return
	}
	pageSize := 0
	if raw := r.URL.Query().Get("page_size"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > 20 {
			writeError(w, http.StatusBadRequest,
				"page_size 必须是 1 到 20 之间的整数")
			return
		}
		pageSize = parsed
	}
	principal, err := auth.PrincipalFromContext(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}
	task, err := s.deps.Store.GetScheduleForMember(
		r.Context(), int64(principal.TenantID), principal.UserID, taskID)
	if err != nil {
		writeAppError(w, err)
		return
	}
	page, err := s.deps.Store.ListTaskBriefsV1(
		r.Context(),
		int64(principal.TenantID),
		task.UserID,
		taskID,
		store.TaskBriefQuery{
			PageSize:  pageSize,
			PageToken: r.URL.Query().Get("page_token"),
		},
	)
	if err != nil {
		writeAppError(w, err)
		return
	}
	memberships, membershipErr := s.deps.Auth.ListMembershipsByUser(
		r.Context(), principal.UserID,
	)
	role := membershipRoleForTenantV1(
		memberships,
		int64(principal.TenantID),
		principal.UserID,
	)
	if membershipErr != nil {
		slog.WarnContext(
			r.Context(),
			"api: task health membership projection unavailable",
			"task_id", taskID,
			"error_code", types.CodeOf(membershipErr),
		)
		role = ""
	}
	cost, costErr := s.deps.Store.GetScheduleRunCost(
		r.Context(), task.UserID, taskID,
	)
	if costErr != nil {
		slog.WarnContext(
			r.Context(),
			"api: task health usage projection unavailable",
			"task_id", taskID,
			"error_code", types.CodeOf(costErr),
		)
		cost = nil
	}
	response := publicTaskBriefPageV1(
		page, s.executiveBriefProjectionEnabled(taskID))
	health := projectTaskHealthV1(
		page.LatestCheck,
		cost,
		role,
		true,
	)
	response.Health = &health
	writeJSON(w, http.StatusOK, response)
}

func membershipRoleForTenantV1(
	memberships []types.Membership,
	tenantID int64,
	userID int64,
) types.MembershipRole {
	for _, membership := range memberships {
		if membership.TenantID == tenantID &&
			membership.UserID == userID {
			return membership.Role
		}
	}
	return ""
}

func projectTaskHealthV1(
	latest *store.TaskLatestCheckV1,
	cost *store.ScheduleRunCost,
	role types.MembershipRole,
	definitionEditEnabled bool,
) taskhealth.ProjectionV1 {
	var latestCheck *taskhealth.LatestCheckV1
	if latest != nil {
		latestCheck = &taskhealth.LatestCheckV1{
			Result:         latest.Result,
			SourceCoverage: latest.SourceCoverage,
			Processing:     latest.Processing,
			FailureCode:    latest.FailureCode,
			FinalizedAt:    latest.FinalizedAt,
		}
	}
	usage := taskhealth.UsageV1{}
	acquisition := taskhealth.AcquisitionSummaryV1{}
	if cost != nil {
		usage.LLMCostUSD = &cost.LLMCostUSD
		usage.LLMCalls = &cost.LLMCalls
		usage.LLMPricedCalls = &cost.LLMPricedCalls
		usage.LLMEstimatedCalls = &cost.LLMEstimatedCalls
		usage.PromptTokens = &cost.PromptTokens
		usage.PromptCacheHitTokens = &cost.PromptCacheHitTokens
		usage.PromptCacheMissTokens = &cost.PromptCacheMissTokens
		usage.CompletionTokens = &cost.CompletionTokens
		usage.ReasoningTokens = &cost.ReasoningTokens
		usage.KnownCosts = make([]taskhealth.CurrencyCostV1, 0, len(cost.KnownCosts))
		for _, known := range cost.KnownCosts {
			usage.KnownCosts = append(usage.KnownCosts, taskhealth.CurrencyCostV1{
				Currency: known.Currency,
				Amount:   known.Amount,
			})
		}
		if cost.ToolCalls > 0 {
			usage.ToolCostUSD = &cost.ToolCostUSD
			usage.ToolCalls = &cost.ToolCalls
			usage.ToolPricedCalls = &cost.ToolPricedCalls
			usage.ToolEstimatedCalls = &cost.ToolEstimatedCalls
		}
		acquisition.Total = cost.LatestAcquisitionCalls
		acquisition.Failing = cost.LatestAcquisitionFailures
		if acquisition.Failing > 0 {
			acquisition.FailureReason =
				projectAcquisitionFailureV1(
					cost.LatestAcquisitionErrorType)
		}
	}
	return taskhealth.ProjectV1(
		latestCheck,
		acquisition,
		usage,
		taskhealth.AccessV1{
			Role:                  role,
			TaskAccessVerified:    true,
			DefinitionEditEnabled: definitionEditEnabled,
		},
	)
}

func projectAcquisitionFailureV1(
	errorType string,
) taskhealth.AcquisitionFailureV1 {
	switch errorType {
	case types.ToolErrTimeout:
		return taskhealth.AcquisitionFailureTimeoutV1
	case types.ToolErrHTTP:
		return taskhealth.AcquisitionFailureProviderV1
	case types.ToolErrInvalidArgs:
		return taskhealth.AcquisitionFailureInvalidRequestV1
	case types.ToolErrBudgetExceeded:
		return taskhealth.AcquisitionFailureUsageLimitV1
	default:
		return taskhealth.AcquisitionFailureInternalV1
	}
}
