package api

import (
	"net/http"
	"strconv"

	"github.com/YouToco/vane/server/auth"
)

func setAdminTraceNoStore(w http.ResponseWriter) {
	w.Header().Set("Cache-Control", "no-store")
}

func parsePositivePathInt64(w http.ResponseWriter, r *http.Request, name string) (int64, bool) {
	raw := r.PathValue(name)
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value <= 0 {
		writeError(w, http.StatusBadRequest, name+" 必须是正整数")
		return 0, false
	}
	return value, true
}

func (s *server) handleListAdminTraceUsers(w http.ResponseWriter, r *http.Request) {
	if !s.requirePlatformOwner(w, r) {
		return
	}
	setAdminTraceNoStore(w)
	items, err := s.deps.Store.ListAdminTraceUsers(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *server) handleListAdminTraceTasks(w http.ResponseWriter, r *http.Request) {
	if !s.requirePlatformOwner(w, r) {
		return
	}
	setAdminTraceNoStore(w)
	tenantID, ok := parsePositivePathInt64(w, r, "tenant_id")
	if !ok {
		return
	}
	userID, ok := parsePositivePathInt64(w, r, "user_id")
	if !ok {
		return
	}
	items, err := s.deps.Store.ListAdminTraceTasks(r.Context(), tenantID, userID)
	if err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *server) handleListAdminTraceRuns(w http.ResponseWriter, r *http.Request) {
	if !s.requirePlatformOwner(w, r) {
		return
	}
	setAdminTraceNoStore(w)
	tenantID, ok := parsePositivePathInt64(w, r, "tenant_id")
	if !ok {
		return
	}
	userID, ok := parsePositivePathInt64(w, r, "user_id")
	if !ok {
		return
	}
	taskID := r.PathValue("task_id")
	if taskID == "" {
		writeError(w, http.StatusBadRequest, "task_id 不能为空")
		return
	}
	items, err := s.deps.Store.ListAdminTraceRuns(
		r.Context(), tenantID, userID, taskID, 50,
	)
	if err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *server) handleGetAdminExecutionTrace(w http.ResponseWriter, r *http.Request) {
	if !s.requirePlatformOwner(w, r) {
		return
	}
	setAdminTraceNoStore(w)
	tenantID, ok := parsePositivePathInt64(w, r, "tenant_id")
	if !ok {
		return
	}
	userID, ok := parsePositivePathInt64(w, r, "user_id")
	if !ok {
		return
	}
	snapshotID, ok := parsePositivePathInt64(w, r, "snapshot_id")
	if !ok {
		return
	}
	taskID := r.PathValue("task_id")
	if taskID == "" {
		writeError(w, http.StatusBadRequest, "task_id 不能为空")
		return
	}
	actor, err := auth.PrincipalFromContext(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}
	trace, err := s.deps.Store.GetAdminExecutionTrace(
		r.Context(),
		int64(actor.TenantID), actor.UserID,
		tenantID, userID, taskID, snapshotID,
	)
	if err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, trace)
}
