package api

import (
	"crypto/sha256"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/YouToco/vane/server/auth"
	"github.com/YouToco/vane/server/types"
)

const installationTokenMaxLen = 256

func (s *server) handleInstallationSetupStatus(w http.ResponseWriter, r *http.Request) {
	setup := s.installationSetupStore()
	if setup == nil {
		writeError(w, http.StatusServiceUnavailable, "初始化服务暂不可用")
		return
	}
	required, err := setup.InstallationSetupRequired(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}
	state := "active"
	if required {
		state = "setup_required"
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"state": state, "setup_required": required,
	})
}

func (s *server) handleInstallationSetupClaim(w http.ResponseWriter, r *http.Request) {
	if !s.checkOrigin(w, r) {
		return
	}
	if !s.limiter.allowAndRecord(ipLimitKey(r), time.Now()) {
		writeError(w, http.StatusTooManyRequests, "操作过于频繁，请稍后再试")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, authBodyLimit)
	var req struct {
		Token    string `json:"token"`
		Email    string `json:"email"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "请求体不是合法 JSON")
		return
	}
	req.Token = strings.TrimSpace(req.Token)
	req.Email = strings.TrimSpace(req.Email)
	if len(req.Token) < 32 || len(req.Token) > installationTokenMaxLen ||
		strings.ContainsAny(req.Token, " \t\r\n") {
		writeError(w, http.StatusBadRequest, "初始化令牌无效或已失效")
		return
	}
	if req.Email == "" || !strings.Contains(req.Email, "@") {
		writeError(w, http.StatusBadRequest, "请填写合法邮箱")
		return
	}
	if err := auth.ValidatePassword(req.Password); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	setup := s.installationSetupStore()
	if setup == nil {
		writeError(w, http.StatusServiceUnavailable, "初始化服务暂不可用")
		return
	}
	digest := sha256.Sum256([]byte(req.Token))
	usable, err := setup.InstallationBootstrapTokenUsable(r.Context(), digest[:])
	if err != nil {
		writeAppError(w, err)
		return
	}
	if !usable {
		writeError(w, http.StatusBadRequest, "初始化令牌无效或已失效")
		return
	}
	passwordHash, err := auth.HashPasswordCtx(r.Context(), req.Password)
	if err != nil {
		slog.Error("setup: 密码哈希失败", "err", err)
		writeError(w, http.StatusInternalServerError, "服务器内部错误，请稍后重试")
		return
	}
	sessionToken, sessionHash, err := auth.NewSessionToken()
	if err != nil {
		slog.Error("setup: 生成首个会话 token 失败", "err", err)
		writeError(w, http.StatusInternalServerError, "服务器内部错误，请稍后重试")
		return
	}
	sessionExpiresAt := time.Now().Add(sessionTTL)
	_, err = setup.ClaimInstallationBootstrap(
		r.Context(), digest[:], req.Email, passwordHash,
		sessionHash, sessionExpiresAt)
	if err != nil {
		writeAppError(w, err)
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name: sessionCookieName, Value: sessionToken, Path: "/",
		Expires: sessionExpiresAt, HttpOnly: true, Secure: true,
		SameSite: http.SameSiteLaxMode,
	})
	writeJSON(w, http.StatusOK, map[string]any{
		"ok": true, "tenant_id": int64(types.SingleTenantID), "restart_required": true,
	})
	if s.deps.SetupClaimed != nil {
		s.deps.SetupClaimed()
	}
}
