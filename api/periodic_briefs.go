package api

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"

	"github.com/YouToco/vane/auth"
	"github.com/YouToco/vane/store"
)

const reportSettingsBodyLimit = 4 << 10

func (s *server) handleListPeriodicBriefReports(
	w http.ResponseWriter,
	r *http.Request,
) {
	taskID := r.PathValue("id")
	if taskID == "" {
		writeError(w, http.StatusBadRequest, "缺少 schedule id")
		return
	}
	query := store.PeriodicBriefReportQueryV1{
		Cursor: r.URL.Query().Get("cursor"),
		Cadence: store.BriefReportCadenceV1(
			r.URL.Query().Get("cadence")),
	}
	if raw := r.URL.Query().Get("page_size"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 1 || value > 20 {
			writeError(w, http.StatusBadRequest,
				"page_size 必须是 1 到 20 之间的整数")
			return
		}
		query.PageSize = value
	}
	for key := range r.URL.Query() {
		if key != "cursor" && key != "cadence" && key != "page_size" {
			writeError(w, http.StatusBadRequest, "包含未知查询参数")
			return
		}
	}
	principal, err := auth.PrincipalFromContext(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}
	page, err := s.deps.Store.ListPeriodicBriefReportsV1(
		r.Context(), int64(principal.TenantID), principal.UserID,
		taskID, query)
	if err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func (s *server) handleGetBriefReportSettings(
	w http.ResponseWriter,
	r *http.Request,
) {
	principal, err := auth.PrincipalFromContext(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}
	settings, err := s.deps.Store.GetBriefReportSettingsV1(
		r.Context(), int64(principal.TenantID), principal.UserID,
		r.PathValue("id"))
	if err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, settings)
}

func (s *server) handlePatchBriefReportSettings(
	w http.ResponseWriter,
	r *http.Request,
) {
	var patch store.BriefReportSettingsPatchV1
	body := http.MaxBytesReader(w, r.Body, reportSettingsBodyLimit)
	decoder := json.NewDecoder(body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&patch); err != nil {
		writeError(w, http.StatusBadRequest, "周期报告设置 JSON 无效")
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		writeError(w, http.StatusBadRequest, "周期报告设置 JSON 无效")
		return
	}
	principal, err := auth.PrincipalFromContext(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}
	settings, err := s.deps.Store.PatchBriefReportSettingsV1(
		r.Context(), int64(principal.TenantID), principal.UserID,
		r.PathValue("id"), patch)
	if err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, settings)
}
