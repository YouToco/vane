// Package api 提供 Dashboard 的 HTTP API：登录会话 + 飞书接入管理（契约 §5）。
// 所有响应为 JSON；错误统一 {"error":"人话"}，状态码语义化。
package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/YouToco/vane/feishu"
	"github.com/YouToco/vane/scheduler"
	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
	"github.com/YouToco/vane/workflow"
)

// Manager 抽象 feishu.Manager 中 API 层用到的能力。
// 接口定义在消费方（本包）便于单测替身；方法签名与 feishu.Manager 一致（契约 §4）。
type Manager interface {
	Status() feishu.Status
	Verify(ctx context.Context, appID, appSecret string) feishu.VerifyResult
	Reconfigure(ctx context.Context) error
	SendTestCard(ctx context.Context) error
}

// Scheduler 抽象 scheduler.Scheduler 中 API 层用到的能力（契约 B7/B8）。
// 同样定义在消费方便于单测；方法签名与 scheduler.Scheduler 严格对齐。
// 入参用 scheduler.ScheduleSpec / workflow.PushScope 具体类型：这两个是全链路
// 共享的中立结构，API 只做"HTTP DTO → 中立结构"的翻译，不自造平行类型。
type Scheduler interface {
	CreatePush(ctx context.Context, userID int64, spec scheduler.ScheduleSpec, scope workflow.PushScope, nlDesc string) (schedID string, err error)
	PushNow(ctx context.Context, userID int64, scope workflow.PushScope) (runID string, err error)
	UpdatePushSpec(ctx context.Context, schedID string, spec scheduler.ScheduleSpec) error
	DeletePush(ctx context.Context, schedID string) error
	TriggerNow(ctx context.Context, schedID string) error
}

// Deps 是 Mount 所需的全部依赖，由 main.go 注入。
type Deps struct {
	Store     *store.Store
	Manager   Manager
	Scheduler Scheduler
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

	// M3 推送管道端点（契约 B8）：全部走会话中间件，是"人与未来 AI 同一出口"的确定性 API。
	inner.HandleFunc("GET /api/schedules", s.handleListSchedules)
	inner.HandleFunc("POST /api/schedules", s.handleCreateSchedule)
	inner.HandleFunc("DELETE /api/schedules/{id}", s.handleDeleteSchedule)
	inner.HandleFunc("POST /api/push/now", s.handlePushNow)
	inner.HandleFunc("GET /api/subscriptions", s.handleListSubscriptions)
	inner.HandleFunc("POST /api/subscriptions", s.handleAddSubscription)
	inner.HandleFunc("DELETE /api/subscriptions/{source_id}", s.handleRemoveSubscription)

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

// writeAppError 把链路里的 AppError 按错误码映射成语义化 HTTP 状态并回其 Message。
// AppError.Message 均按"人话"书写，可直接面向 Dashboard 用户；非 AppError（未预期的
// 底层错误）不泄露细节，只回 500 + 通用文案，真实原因走日志。
func writeAppError(w http.ResponseWriter, err error) {
	var ae *types.AppError
	if !errors.As(err, &ae) {
		slog.Error("api: 未分类错误", "err", err)
		writeError(w, http.StatusInternalServerError, "服务器内部错误，请稍后重试")
		return
	}
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, types.ErrValidation):
		status = http.StatusBadRequest
	case errors.Is(err, types.ErrNotFound):
		status = http.StatusNotFound
	case errors.Is(err, types.ErrConflict):
		status = http.StatusConflict
	case errors.Is(err, types.ErrPush), errors.Is(err, types.ErrLLM), errors.Is(err, types.ErrFetch):
		status = http.StatusBadGateway // 外部依赖失败：对客户端是上游故障
	}
	if status >= http.StatusInternalServerError {
		// 5xx 落日志便于排查；4xx 是调用方问题，不刷日志。
		slog.Error("api: 请求处理失败", "code", ae.Code, "err", err)
	}
	writeError(w, status, ae.Message)
}
