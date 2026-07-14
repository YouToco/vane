// 登录 / 登出 / 会话自检（契约 §5）。
package api

import (
	"crypto/subtle"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"
)

// handleLogin 校验密码并签发会话 cookie。
// POST /api/auth/login {"password":"xxx"} → 200 {"ok":true} + Set-Cookie / 401
func (s *server) handleLogin(w http.ResponseWriter, r *http.Request) {
	// 这是唯一未鉴权入口：限流挡在线爆破，MaxBytesReader 挡超大 body 内存 DoS。
	ip := clientIP(r)
	if !s.limiter.allow(ip, time.Now()) {
		writeError(w, http.StatusTooManyRequests, "登录尝试过于频繁，请稍后再试")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 4<<10) // 密码请求体 4KB 足够

	var req struct {
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "请求体不是合法 JSON")
		return
	}

	if s.deps.Password == "" {
		// 密码未配置时不给任何人开门（包括空密码尝试）：提示运维配置而不是放行。
		slog.Warn("dashboard 密码未配置，登录被拒绝（请设置 VANE_DASHBOARD_PASSWORD）")
		writeError(w, http.StatusUnauthorized, "服务端未配置 Dashboard 密码")
		return
	}

	// 常数时间对比防时序侧信道（契约 §5 明确要求 subtle.ConstantTimeCompare）。
	if subtle.ConstantTimeCompare([]byte(req.Password), []byte(s.deps.Password)) != 1 {
		s.limiter.recordFailure(ip, time.Now())
		// 失败固定小延迟：进一步压低爆破速率，对正常用户无感。
		time.Sleep(200 * time.Millisecond)
		writeError(w, http.StatusUnauthorized, "密码错误")
		return
	}

	token, exp := s.sessions.issue(time.Now())
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    token,
		Path:     "/",
		Expires:  exp,
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteLaxMode,
	})
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// handleLogout 清除会话 cookie。token 无状态、服务端无会话表，
// 清 cookie 即完成"登出"；token 本身到期前理论上仍有效，MVP 接受。
func (s *server) handleLogout(w http.ResponseWriter, _ *http.Request) {
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    "",
		Path:     "/",
		MaxAge:   -1, // 立即删除
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteLaxMode,
	})
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// handleMe 供前端探测登录态：能走到这里说明中间件已放行。
func (s *server) handleMe(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}
