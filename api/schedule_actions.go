package api

import (
	"net/http"
	"strings"
)

func scheduleCommandIdempotencyKey(r *http.Request) (string, bool) {
	key := r.Header.Get("Idempotency-Key")
	if key == "" || len(key) > 128 || strings.TrimSpace(key) != key {
		return "", false
	}
	for i, ch := range []byte(key) {
		if (i == 0 && !isScheduleCommandKeyAlphaNumeric(ch)) ||
			(i > 0 && !isScheduleCommandKeyAlphaNumeric(ch) &&
				ch != '.' && ch != '_' && ch != ':' && ch != '-') {
			return "", false
		}
	}
	return key, true
}

func isScheduleCommandKeyAlphaNumeric(ch byte) bool {
	return (ch >= 'A' && ch <= 'Z') ||
		(ch >= 'a' && ch <= 'z') ||
		(ch >= '0' && ch <= '9')
}

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
	idempotencyKey, ok := scheduleCommandIdempotencyKey(r)
	if !ok {
		writeError(
			w, http.StatusBadRequest,
			"缺少或无效的 Idempotency-Key",
		)
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
		err = controller.TriggerScheduleNowIdempotent(
			r.Context(), id, userID, idempotencyKey,
		)
	case "pause":
		err = controller.PausePushIdempotent(
			r.Context(), id, userID, idempotencyKey,
		)
	case "resume":
		err = controller.ResumePushIdempotent(
			r.Context(), id, userID, idempotencyKey,
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
