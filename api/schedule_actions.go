package api

import (
	"net/http"
	"strings"
)

func (s *server) handleRunScheduleNow(
	w http.ResponseWriter,
	r *http.Request,
) {
	s.handleScheduleCommand(w, r, "run")
}

func (s *server) handlePauseSchedule(
	w http.ResponseWriter,
	r *http.Request,
) {
	s.handleScheduleCommand(w, r, "pause")
}

func (s *server) handleResumeSchedule(
	w http.ResponseWriter,
	r *http.Request,
) {
	s.handleScheduleCommand(w, r, "resume")
}

func (s *server) handleScheduleCommand(
	w http.ResponseWriter,
	r *http.Request,
	command string,
) {
	id := strings.TrimSpace(r.PathValue("id"))
	if id == "" || len(id) > 255 {
		writeError(w, http.StatusBadRequest, "任务标识无效")
		return
	}
	userID, err := s.ownerUserID(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}
	controller, ok := s.deps.Scheduler.(scheduleActionController)
	if !ok {
		writeError(w, http.StatusServiceUnavailable, "任务操作控制面尚未就绪")
		return
	}
	switch command {
	case "run":
		err = controller.TriggerScheduleNow(
			r.Context(), id, userID,
		)
	case "pause":
		err = controller.PausePush(
			r.Context(), id, userID,
		)
	case "resume":
		err = controller.ResumePush(
			r.Context(), id, userID,
		)
	default:
		writeError(w, http.StatusNotFound, "未知任务操作")
		return
	}
	if err != nil {
		writeAppError(w, err)
		return
	}
	status := http.StatusOK
	if command == "run" {
		status = http.StatusAccepted
	}
	writeJSON(w, status, map[string]bool{"ok": true})
}
