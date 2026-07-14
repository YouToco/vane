// Package api 提供 Dashboard 的 HTTP API：登录会话 + 飞书接入管理（契约 §5）。
// 所有响应为 JSON；错误统一 {"error":"人话"}，状态码语义化。
package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/YouToco/vane/feishu"
	"github.com/YouToco/vane/store"
)

// Manager 抽象 feishu.Manager 中 API 层用到的能力。
// 接口定义在消费方（本包）便于单测替身；方法签名与 feishu.Manager 一致（契约 §4）。
type Manager interface {
	Status() feishu.Status
	Verify(ctx context.Context, appID, appSecret string) feishu.VerifyResult
	Reconfigure(ctx context.Context) error
	SendTestCard(ctx context.Context) error
}

// Deps 是 Mount 所需的全部依赖，由 main.go 注入。
type Deps struct {
	Store   *store.Store
	Manager Manager
	// Password 是 Dashboard 登录密码（VANE_DASHBOARD_PASSWORD）；
	// 为空时登录一律 401——见 handleLogin。
	Password string
}

type server struct {
	deps     Deps
	sessions *sessions
	limiter  *loginLimiter
}

// Mount 把 /api/* 路由挂到 mux。除 /api/auth/login 外全部要求会话 cookie；
// /healthz /readyz 不在 /api 前缀下，不受本中间件影响。
func Mount(mux *http.ServeMux, deps Deps) {
	s := &server{deps: deps, sessions: newSessions(deps.Password), limiter: newLoginLimiter()}

	inner := http.NewServeMux()
	inner.HandleFunc("POST /api/auth/login", s.handleLogin)
	inner.HandleFunc("POST /api/auth/logout", s.handleLogout)
	inner.HandleFunc("GET /api/auth/me", s.handleMe)
	inner.HandleFunc("GET /api/feishu/status", s.handleFeishuStatus)
	inner.HandleFunc("POST /api/feishu/verify", s.handleFeishuVerify)
	inner.HandleFunc("POST /api/feishu/config", s.handleFeishuConfig)
	inner.HandleFunc("POST /api/feishu/test", s.handleFeishuTest)

	mux.Handle("/api/", s.requireSession(inner))
}

// requireSession 是 /api/* 的会话中间件。login 必须豁免，否则永远无法登录。
func (s *server) requireSession(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/auth/login" {
			next.ServeHTTP(w, r)
			return
		}
		c, err := r.Cookie(sessionCookieName)
		if err != nil || !s.sessions.verify(c.Value, time.Now()) {
			writeError(w, http.StatusUnauthorized, "未登录或会话已过期")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		// 响应写一半失败无从补救，只留日志供排查。
		slog.Error("api: 写响应失败", "err", err)
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
