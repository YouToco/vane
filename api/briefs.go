package api

import (
	"log/slog"
	"net/http"
	"strconv"

	"github.com/YouToco/vane/auth"
	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/taskhealth"
	"github.com/YouToco/vane/types"
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
	page, err := s.deps.Store.ListTaskBriefsV1(
		r.Context(),
		int64(principal.TenantID),
		principal.UserID,
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
		r.Context(), principal.UserID, taskID,
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
		s.deps.DefinitionEditEnabled,
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
	if cost != nil {
		usage.LLMCostUSD = &cost.LLMCostUSD
		usage.LLMCalls = &cost.LLMCalls
	}
	return taskhealth.ProjectV1(
		latestCheck,
		taskhealth.AcquisitionSummaryV1{},
		usage,
		taskhealth.AccessV1{
			Role:                  role,
			TaskAccessVerified:    true,
			DefinitionEditEnabled: definitionEditEnabled,
		},
	)
}
