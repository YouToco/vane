package api

import (
	"net/http"
	"strconv"

	"github.com/YouToco/vane/auth"
	"github.com/YouToco/vane/store"
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
	writeJSON(w, http.StatusOK, page)
}
