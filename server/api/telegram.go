package api

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/YouToco/vane/server/auth"
	"github.com/YouToco/vane/server/types"
)

func (s *server) handleTelegramStatus(w http.ResponseWriter, r *http.Request) {
	if s.deps.Telegram == nil {
		writeJSON(w, http.StatusOK, map[string]any{
			"enabled": false, "ready": false, "bound": false,
		})
		return
	}
	principal, err := auth.PrincipalFromContext(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}
	status := s.deps.Telegram.PrincipalStatus(
		r.Context(), int64(principal.TenantID), principal.UserID)
	blocked, err := s.deps.Telegram.BlockedReplies(
		r.Context(), int64(principal.TenantID), principal.UserID)
	if err != nil {
		writeAppError(w, err)
		return
	}
	response := map[string]any{
		"enabled":              status.Enabled,
		"ready":                status.Ready,
		"bot_id":               status.BotID,
		"bot_username":         status.BotUsername,
		"webhook_url":          status.WebhookURL,
		"pending_update_count": status.PendingUpdateCount,
		"last_error_code":      status.LastErrorCode,
		"blocked_reply_count":  blocked.Count,
		"oldest_blocked_at":    blocked.OldestAt,
		"bound":                false,
	}
	if status.Ready {
		identity, err := s.deps.Telegram.Binding(
			r.Context(), int64(principal.TenantID), principal.UserID)
		if err == nil {
			response["bound"] = true
			response["bound_at"] = identity.BoundAt
		} else if !errors.Is(err, types.ErrNotFound) {
			writeAppError(w, err)
			return
		}
		routes, err := s.deps.Telegram.Routes(
			r.Context(), int64(principal.TenantID), principal.UserID)
		if err != nil {
			writeAppError(w, err)
			return
		}
		response["routes"] = routes
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *server) handleTelegramRouteLink(w http.ResponseWriter, r *http.Request) {
	if !s.checkOrigin(w, r) {
		return
	}
	principal, err := auth.PrincipalFromContext(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}
	if s.deps.Telegram == nil || !s.deps.Telegram.PrincipalStatus(
		r.Context(), int64(principal.TenantID), principal.UserID).Ready {
		writeError(w, http.StatusConflict, "Telegram Bot 尚未就绪")
		return
	}
	link, err := s.deps.Telegram.IssueRouteLink(
		r.Context(), int64(principal.TenantID), principal.UserID)
	if err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, link)
}

func (s *server) handleTelegramRouteUnlink(w http.ResponseWriter, r *http.Request) {
	if !s.checkOrigin(w, r) {
		return
	}
	if s.deps.Telegram == nil {
		writeError(w, http.StatusConflict, "Telegram Bot 尚未启用")
		return
	}
	principal, err := auth.PrincipalFromContext(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}
	routeID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || routeID <= 0 {
		writeError(w, http.StatusBadRequest, "Telegram 路由 ID 无效")
		return
	}
	if err := s.deps.Telegram.UnlinkRoute(r.Context(),
		int64(principal.TenantID), principal.UserID, routeID); err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *server) handleTelegramLink(w http.ResponseWriter, r *http.Request) {
	if !s.checkOrigin(w, r) {
		return
	}
	principal, err := auth.PrincipalFromContext(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}
	if s.deps.Telegram == nil || !s.deps.Telegram.PrincipalStatus(
		r.Context(), int64(principal.TenantID), principal.UserID).Ready {
		writeError(w, http.StatusConflict, "Telegram Bot 尚未就绪")
		return
	}
	link, err := s.deps.Telegram.IssueLink(
		r.Context(), int64(principal.TenantID), principal.UserID)
	if err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, link)
}

func (s *server) handleTelegramUnlink(w http.ResponseWriter, r *http.Request) {
	if !s.checkOrigin(w, r) {
		return
	}
	if s.deps.Telegram == nil {
		writeError(w, http.StatusConflict, "Telegram Bot 尚未启用")
		return
	}
	principal, err := auth.PrincipalFromContext(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}
	if err := s.deps.Telegram.Unlink(
		r.Context(), int64(principal.TenantID), principal.UserID); err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *server) handleTelegramTest(w http.ResponseWriter, r *http.Request) {
	if !s.checkOrigin(w, r) {
		return
	}
	if s.deps.Telegram == nil {
		writeError(w, http.StatusConflict, "Telegram Bot 尚未启用")
		return
	}
	principal, err := auth.PrincipalFromContext(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}
	if err := s.deps.Telegram.SendTest(
		r.Context(), int64(principal.TenantID), principal.UserID); err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}
