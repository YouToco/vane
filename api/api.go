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

	"github.com/YouToco/vane/auth"
	"github.com/YouToco/vane/feishu"
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

// Scheduler abstracts the remaining API-safe scheduler capabilities. Task
// definition writes are intentionally absent from this consumer interface.
type Scheduler interface {
	PushNow(ctx context.Context, userID int64, scope workflow.PushScope) (runID string, err error)
	DeletePush(ctx context.Context, schedID string, userID int64) error
}

// AuthStore 是认证路径所需的窄接口（生产实现 *store.Store）。
//
// 单独收窄而不直接用 Deps.Store（具体类型 *store.Store）：认证中间件在每个请求上
// 查会话，若依赖具体类型，**api 包的任何鉴权测试都必须连真数据库**——
// 而鉴权正是最该用大量廉价用例覆盖的地方（枚举、爆破、越权、会话固定…）。
// 与 Manager / Scheduler 的窄接口同一惯例。
type AuthStore interface {
	RegisterWithInvite(ctx context.Context, email, passwordHash, code string) (*types.User, *types.Tenant, error)
	GetUserByEmail(ctx context.Context, email string) (*types.User, error)
	GetUserEmailByID(ctx context.Context, userID int64) (string, error)
	UpdatePasswordHash(ctx context.Context, userID int64, passwordHash string) error
	ListMembershipsByUser(ctx context.Context, userID int64) ([]types.Membership, error)
	InviteUsable(ctx context.Context, code string) (bool, error)
	CreateSession(ctx context.Context, tokenHash []byte, userID, tenantID int64, expiresAt time.Time) error
	LookupSession(ctx context.Context, tokenHash []byte) (*types.Session, error)
	DeleteSession(ctx context.Context, tokenHash []byte) error
}

// Deps 是 Mount 所需的全部依赖，由 main.go 注入。
type Deps struct {
	Store *store.Store
	// Auth 是认证路径的窄接口；生产与 Store 同为 *store.Store。
	Auth      AuthStore
	Manager   Manager
	Scheduler Scheduler
	// Principal 是全系统唯一的 principal 来源（企业级契约 §1.1，不变量 I-A1）。
	// 生产由 main.go 注入 auth.NewOwnerResolver；单测可注入假实现。
	Principal auth.PrincipalResolver
	// Origin 是唯一放行 CORS 的前端源（VANE_DASHBOARD_ORIGIN，默认生产 Dashboard 域）。
	// 前端迁 OSS+CDN 后与 API 跨源（vane.* → api.*），凭证请求要求逐字匹配的
	// Allow-Origin + Allow-Credentials，不允许通配符。为空 = 不放行任何跨源。
	Origin string
}

type server struct {
	deps    Deps
	limiter *authLimiter
}

// Mount 把 /api/* 路由挂到 mux。除 /api/auth/login 外全部要求会话 cookie；
// /healthz /readyz 不在 /api 前缀下，不受本中间件影响。
func Mount(mux *http.ServeMux, deps Deps) {
	s := &server{deps: deps, limiter: newAuthLimiter()}

	inner := http.NewServeMux()
	inner.HandleFunc("POST /api/auth/register", s.handleRegister)
	inner.HandleFunc("POST /api/auth/login", s.handleLogin)
	inner.HandleFunc("POST /api/auth/logout", s.handleLogout)
	inner.HandleFunc("GET /api/auth/me", s.handleMe)
	inner.HandleFunc("GET /api/feishu/status", s.handleFeishuStatus)
	inner.HandleFunc("POST /api/feishu/verify", s.handleFeishuVerify)
	inner.HandleFunc("POST /api/feishu/config", s.handleFeishuConfig)
	inner.HandleFunc("POST /api/feishu/test", s.handleFeishuTest)

	// M3 推送管道端点（契约 B8）：全部走会话中间件，是"人与未来 AI 同一出口"的确定性 API。
	inner.HandleFunc("GET /api/schedules", s.handleListSchedules)
	inner.HandleFunc("DELETE /api/schedules/{id}", s.handleDeleteSchedule)
	inner.HandleFunc("POST /api/push/now", s.handlePushNow)
	inner.HandleFunc("GET /api/subscriptions", s.handleListSubscriptions)
	inner.HandleFunc("POST /api/subscriptions", s.handleAddSubscription)
	inner.HandleFunc("DELETE /api/subscriptions/{source_id}", s.handleRemoveSubscription)
	inner.HandleFunc("POST /api/sources/{source_id}/enable", s.handleEnableSource)

	// M5 Gate 探针端点（契约 §16）：只读体检，与 cmd/gate 共用 probe 包同一份判定。
	inner.HandleFunc("GET /api/admin/observability", s.handleObservability)

	// M7 推送历史端点（功能 6.4）：只读，回溯每条推送的打分、状态与反馈。
	inner.HandleFunc("GET /api/deliveries", s.handleListDeliveries)

	// M7 运行统计端点（功能 6.5）：只读，成本/token/延迟/缓存按 span 聚合。
	inner.HandleFunc("GET /api/admin/runstats", s.handleRunstats)

	// M7 画像查看端点（功能 6.3）：只读，展示结构化标签 + 摘要画像（含负偏好尾句）；编辑写回留二期。
	inner.HandleFunc("GET /api/profile", s.handleProfile)

	// 邀请码管理端点（D4 准入闸门的管理面）：替代 SSH 跑 useradmin invite。
	// 全部锁 requirePlatformOwner（handler 内第一行，非 owner 404）。
	inner.HandleFunc("GET /api/admin/invites", s.handleListInvites)
	inner.HandleFunc("POST /api/admin/invites", s.handleCreateInvite)
	inner.HandleFunc("DELETE /api/admin/invites/{code}", s.handleDeleteInvite)

	mux.Handle("/api/", s.cors(s.requireSession(inner)))
}

// cors 处理 Dashboard 前端的跨源请求（前端在 vane.*、API 在 api.*，同站不同源）。
//
// 套在 requireSession 外层是必须的：预检 OPTIONS 是浏览器自动发起的，不带 cookie，
// 落进会话中间件会 401，浏览器随即判定跨源失败——真请求根本发不出来。
//
// 只放行 deps.Origin 一个源：带凭证（cookie）的 CORS 规范禁止 Allow-Origin 通配符，
// 且回显任意 Origin 等于把带 cookie 的 API 开放给全网页面。非放行源不加任何 CORS 头，
// 浏览器侧按同源策略拒绝（curl / A2A 等非浏览器客户端不受影响，语义不变）。
//
// 会话 cookie 是 SameSite=Lax（auth.go）：vane.* 与 api.* 同注册域即同站，
// Lax 不拦同站请求，故跨源 fetch(credentials:"include") 能带上 cookie，无需放宽 cookie 属性。
func (s *server) cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if s.deps.Origin != "" && origin == s.deps.Origin {
			h := w.Header()
			h.Set("Access-Control-Allow-Origin", origin)
			h.Set("Access-Control-Allow-Credentials", "true")
			// 缓存（CDN/浏览器）必须按 Origin 区分响应，否则放行头可能被错误复用。
			h.Add("Vary", "Origin")
			if r.Method == http.MethodOptions {
				// 这里漏一个方法，跨源前端就调不通对应端点——预检不放行，浏览器
				// 连请求都不发（fetch 拿到的是网络错误，不是状态码）。新增写端点时
				// 必须同步这一行；已退役的方法不得继续被浏览器预检广告。
				h.Set("Access-Control-Allow-Methods", "GET, POST, DELETE")
				h.Set("Access-Control-Allow-Headers", "Content-Type")
				h.Set("Access-Control-Max-Age", "600")
				w.WriteHeader(http.StatusNoContent)
				return
			}
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
