// Package api 提供 Dashboard 的 HTTP API：登录会话 + 飞书接入管理（契约 §5）。
// 所有响应为 JSON；错误统一 {"error":"人话"}，状态码语义化。
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	neturl "net/url"
	"strings"
	"sync"
	"time"

	"github.com/YouToco/vane/agent"
	"github.com/YouToco/vane/auth"
	"github.com/YouToco/vane/feedback"
	"github.com/YouToco/vane/feishu"
	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/task"
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

type scheduleActionController interface {
	TriggerScheduleNowIdempotent(
		ctx context.Context,
		schedID string,
		userID int64,
		idempotencyKey string,
	) error
	PausePushIdempotent(
		ctx context.Context,
		schedID string,
		userID int64,
		idempotencyKey string,
	) error
	ResumePushIdempotent(
		ctx context.Context,
		schedID string,
		userID int64,
		idempotencyKey string,
	) error
}

type scheduleDeleteController interface {
	DeletePushIdempotent(
		ctx context.Context,
		schedID string,
		userID int64,
		idempotencyKey string,
	) error
}

type scheduleNextRunReader interface {
	NextRun(ctx context.Context, schedID string, userID int64) (*time.Time, error)
}

// TaskAgent is the existing confirmed-write control plane exposed to the Web
// transport. The API never receives raw Store/coordinator phase methods.
type TaskAgent interface {
	HandleMessage(
		ctx context.Context,
		userID int64,
		text string,
	) (agent.Outcome, error)
	HandleTaskCreationMessage(
		ctx context.Context,
		userID int64,
		actionID string,
		text string,
	) (agent.Outcome, error)
	HandleTaskDefinitionEditMessage(
		ctx context.Context,
		userID int64,
		actionID string,
		taskID string,
		text string,
	) (agent.Outcome, error)
	ExecuteActionWithReceipt(
		ctx context.Context,
		userID int64,
		actionID string,
		receipt task.CreationReceiptTarget,
	) (agent.CardActionOutcome, error)
	CancelActionWithReceipt(
		ctx context.Context,
		userID int64,
		actionID string,
		receipt task.CreationReceiptTarget,
	) (agent.CardActionOutcome, error)
}

// TaskActionStore is the owner-scoped durable identity read boundary used by
// the Web proposal/replay protocol. It prevents a transport retry from
// re-running the model after the operation commit response was lost.
type TaskActionStore interface {
	GetSchedule(
		ctx context.Context,
		id string,
		userID int64,
	) (*types.Schedule, error)
	LoadTaskCreationOperationByUser(
		ctx context.Context,
		id string,
		userID int64,
	) (*types.TaskCreationOperation, error)
	LoadTaskDefinitionEditOperationByActor(
		ctx context.Context,
		actionID string,
		userID int64,
	) (*types.TaskDefinitionEditOperation, error)
}

// BriefFeedback is the existing explicit-user deep-dive control plane. P2-D
// reuses it after proving the clicked Insight is an immutable evidence
// reference of the exact Brief/report next step.
type BriefFeedback interface {
	HandleClick(
		ctx context.Context,
		userID int64,
		click feedback.Click,
	) (feedback.ClickResult, error)
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
	TaskAgent TaskAgent
	// BriefFeedback is separate from grounded read-only Agent follow-up:
	// deep-dive is an explicit fixed action and keeps the existing durable
	// feedback/idempotency/delivery behavior.
	BriefFeedback BriefFeedback
	// TaskActions is the narrow durable replay/identity reader. Production
	// injects the same Store; tests can prove transport invariants without PG.
	TaskActions TaskActionStore
	// Principal 是全系统唯一的 principal 来源（企业级契约 §1.1，不变量 I-A1）。
	// 生产由 main.go 注入 auth.NewOwnerResolver；单测可注入假实现。
	Principal auth.PrincipalResolver
	// DefinitionEditEnabled is the live process capability. The Web must not
	// advertise or enter the definition-edit proposal path while the Agent
	// feature flag is disabled, even though historical cards remain routable.
	DefinitionEditEnabled bool
	// P2-D Web exposure is independent from dark synthesis and Feishu render.
	// Routes remain mounted, but every non-Web-canary task looks absent.
	ExecutiveBriefWebCanaryScheduleID string
	// Origin 是唯一放行 CORS 的前端源（VANE_DASHBOARD_ORIGIN，默认生产 Dashboard 域）。
	// 前端迁 OSS+CDN 后与 API 跨源（vane.* → api.*），凭证请求要求逐字匹配的
	// Allow-Origin + Allow-Credentials，不允许通配符。为空 = 不放行任何跨源。
	Origin string
}

type server struct {
	deps              Deps
	limiter           *authLimiter
	taskActionLimiter *authLimiter
	taskActionMu      sync.Mutex
	taskActionActive  map[int64]struct{}
}

func (s *server) executiveBriefTaskEnabled(taskID string) bool {
	return taskID != "" &&
		taskID == s.deps.ExecutiveBriefWebCanaryScheduleID
}

// Mount 把 /api/* 路由挂到 mux。除 /api/auth/login 外全部要求会话 cookie；
// /healthz /readyz 不在 /api 前缀下，不受本中间件影响。
func Mount(mux *http.ServeMux, deps Deps) {
	taskActionLimiter := newAuthLimiter()
	taskActionLimiter.max = 6
	s := &server{
		deps: deps, limiter: newAuthLimiter(),
		taskActionLimiter: taskActionLimiter,
		taskActionActive:  make(map[int64]struct{}),
	}

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

	// M7 任务数据面端点（功能 6.6/6.7）：只读，任务详情/运行历史/任务推送/列表概览。
	// "summary" 是字面段，ServeMux 精确度规则保证它优先于 {id} 通配匹配。
	inner.HandleFunc("GET /api/schedules/summary", s.handleListScheduleSummaries)
	inner.HandleFunc("GET /api/schedules/{id}", s.handleGetScheduleDetail)
	inner.HandleFunc("GET /api/schedules/{id}/batches", s.handleListScheduleBatches)
	inner.HandleFunc("GET /api/schedules/{id}/briefs", s.handleListTaskBriefs)
	inner.HandleFunc("GET /api/schedules/{id}/briefs/{target_id}", s.handleIssueBriefGrounding)
	inner.HandleFunc("POST /api/schedules/{id}/briefs/{target_id}/ask", s.handleIssueBriefFollowup)
	inner.HandleFunc("POST /api/schedules/{id}/briefs/{target_id}/deep-dive", s.handleIssueBriefDeepDive)
	inner.HandleFunc("GET /api/schedules/{id}/reports", s.handleListPeriodicBriefReports)
	inner.HandleFunc("GET /api/schedules/{id}/reports/{target_id}", s.handlePeriodicBriefGrounding)
	inner.HandleFunc("POST /api/schedules/{id}/reports/{target_id}/ask", s.handlePeriodicBriefFollowup)
	inner.HandleFunc("POST /api/schedules/{id}/reports/{target_id}/deep-dive", s.handlePeriodicBriefDeepDive)
	inner.HandleFunc("GET /api/schedules/{id}/report-settings", s.handleGetBriefReportSettings)
	inner.HandleFunc("PATCH /api/schedules/{id}/report-settings", s.handlePatchBriefReportSettings)
	inner.HandleFunc("GET /api/schedules/{id}/deliveries", s.handleListScheduleDeliveries)
	inner.HandleFunc("POST /api/schedules/{id}/run", s.handleRunScheduleNow)
	inner.HandleFunc("POST /api/schedules/{id}/pause", s.handlePauseSchedule)
	inner.HandleFunc("POST /api/schedules/{id}/resume", s.handleResumeSchedule)
	inner.HandleFunc("POST /api/push/now", s.handlePushNow)
	inner.HandleFunc("POST /api/task-actions/propose", s.handleProposeTaskAction)
	inner.HandleFunc("GET /api/task-actions/{id}", s.handleGetTaskAction)
	inner.HandleFunc("POST /api/task-actions/{id}/confirm", s.handleConfirmTaskAction)
	inner.HandleFunc("POST /api/task-actions/{id}/cancel", s.handleCancelTaskAction)
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

	// M7 画像 authority：profile 是只读投影；来源级 claim 操作使用
	// version CAS + 幂等回执 + append-only 补偿事件。
	inner.HandleFunc("GET /api/profile", s.handleProfile)
	inner.HandleFunc("PATCH /api/profile", s.handlePatchProfile)
	inner.HandleFunc("GET /api/profile/edits", s.handleListProfileEdits)
	inner.HandleFunc("POST /api/profile/edits/{id}/undo", s.handleUndoProfileEdit)
	inner.HandleFunc("GET /api/profile/claims", s.handleListProfileClaims)
	inner.HandleFunc("POST /api/profile/claims/actions", s.handleProfileClaimAction)
	inner.HandleFunc("POST /api/profile/epochs/actions", s.handleProfileEpochAction)

	// 邀请码管理端点（D4 准入闸门的管理面）：替代 SSH 跑 useradmin invite。
	// 全部锁 requirePlatformOwner（handler 内第一行，非 owner 404）。
	inner.HandleFunc("GET /api/admin/invites", s.handleListInvites)
	inner.HandleFunc("POST /api/admin/invites", s.handleCreateInvite)
	inner.HandleFunc("DELETE /api/admin/invites/{code}", s.handleDeleteInvite)

	apiHandler := s.cors(
		groundedBriefFollowupDeadlineV1(s.requireSession(inner)),
	)
	mux.Handle("/api/", apiHandler)
}

// groundedBriefFollowupDeadlineV1 is deliberately outside session auth (and
// inside the non-blocking CORS boundary), but only acts on the two exact
// grounded ask route shapes. Slow session lookup must not consume the server's
// inherited 30s WriteTimeout before the handler gets a chance to widen it. The
// shorter execution context leaves 23s nominal write headroom; even when the
// last detached accounting tail consumes its full 10s after context expiry,
// at least 13s remains for the bounded JSON response.
func groundedBriefFollowupDeadlineV1(next http.Handler) http.Handler {
	return groundedBriefFollowupDeadlineWithBudgetV1(
		next,
		groundedBriefFollowupExecutionBudget,
		groundedBriefFollowupResponseHeadroom,
	)
}

func groundedBriefFollowupDeadlineWithBudgetV1(
	next http.Handler,
	executionBudget time.Duration,
	responseHeadroom time.Duration,
) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !isGroundedBriefFollowupRequestV1(r) {
			next.ServeHTTP(w, r)
			return
		}
		now := time.Now()
		writeDeadline := now.Add(executionBudget + responseHeadroom)
		if err := http.NewResponseController(w).SetWriteDeadline(
			writeDeadline,
		); err != nil {
			slog.Error(
				"api: 简报追问无法在鉴权前设置有界写超时",
				"err", err,
			)
			writeError(
				w, http.StatusServiceUnavailable,
				"简报追问暂时不可用，请稍后重试",
			)
			return
		}
		executionDeadline := now.Add(executionBudget)
		ctx, cancel := context.WithDeadline(r.Context(), executionDeadline)
		defer cancel()
		ctx = context.WithValue(
			ctx,
			briefFollowupWriteDeadlineContextKeyV1{},
			writeDeadline,
		)
		buffered := newGroundedBriefResponseBufferV1(w)
		next.ServeHTTP(buffered, r.WithContext(ctx))
		switch ctx.Err() {
		case context.Canceled:
			// The peer is gone. Discard any response auth/handler raced to
			// produce instead of writing a misleading error to a dead socket.
			return
		case context.DeadlineExceeded:
			// requireSession intentionally maps every lookup failure to 401.
			// At this exact route boundary we still know the true cause and can
			// replace that buffered response before any bytes reach the peer.
			buffered.resetBody()
			writeAppError(buffered, types.NewAppError(
				types.CodeLLMUnavailable,
				"简报追问处理超时，请稍后重试",
				context.DeadlineExceeded,
			))
		}
		if err := buffered.flush(); err != nil {
			slog.Error("api: 简报追问写入最终响应失败", "err", err)
		}
	})
}

type groundedBriefResponseBufferV1 struct {
	dst    http.ResponseWriter
	header http.Header
	body   bytes.Buffer
	status int
}

func newGroundedBriefResponseBufferV1(
	dst http.ResponseWriter,
) *groundedBriefResponseBufferV1 {
	return &groundedBriefResponseBufferV1{
		dst: dst, header: make(http.Header),
	}
}

func (w *groundedBriefResponseBufferV1) Header() http.Header {
	return w.header
}

func (w *groundedBriefResponseBufferV1) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
}

func (w *groundedBriefResponseBufferV1) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.body.Write(p)
}

func (w *groundedBriefResponseBufferV1) Unwrap() http.ResponseWriter {
	return w.dst
}

func (w *groundedBriefResponseBufferV1) resetBody() {
	w.body.Reset()
	w.status = 0
	w.header.Del("Content-Length")
}

func (w *groundedBriefResponseBufferV1) flush() error {
	for key, values := range w.header {
		w.dst.Header()[key] = append([]string(nil), values...)
	}
	status := w.status
	if status == 0 {
		status = http.StatusOK
	}
	w.dst.WriteHeader(status)
	_, err := w.dst.Write(w.body.Bytes())
	return err
}

func isGroundedBriefFollowupRequestV1(r *http.Request) bool {
	if r.Method != http.MethodPost {
		return false
	}
	parts := strings.Split(
		strings.TrimPrefix(r.URL.EscapedPath(), "/"), "/")
	if len(parts) != 6 ||
		!briefFollowupPathSegmentEqualsV1(parts[0], "api") ||
		!briefFollowupPathSegmentEqualsV1(parts[1], "schedules") ||
		!briefFollowupPathSegmentNonEmptyV1(parts[2]) ||
		!briefFollowupPathSegmentNonEmptyV1(parts[4]) ||
		!briefFollowupPathSegmentEqualsV1(parts[5], "ask") {
		return false
	}
	return briefFollowupPathSegmentEqualsV1(parts[3], "briefs") ||
		briefFollowupPathSegmentEqualsV1(parts[3], "reports")
}

func briefFollowupPathSegmentEqualsV1(
	escaped string,
	want string,
) bool {
	value, err := neturl.PathUnescape(escaped)
	return err == nil && value == want
}

func briefFollowupPathSegmentNonEmptyV1(escaped string) bool {
	value, err := neturl.PathUnescape(escaped)
	return err == nil && value != ""
}

// cors 处理 Dashboard 前端的跨源请求（前端在 vane.*、API 在 api.*，同站不同源）。
//
// 套在 requireSession 外层是必须的：预检 OPTIONS 是浏览器自动发起的，不带 cookie，
// 落进会话中间件会 401，浏览器随即判定跨源失败——真请求根本发不出来。
//
// 只放行 deps.Origin 一个源：带凭证（cookie）的 CORS 规范禁止 Allow-Origin 通配符，
// 且回显任意 Origin 等于把带 cookie 的 API 开放给全网页面。带非空错误 Origin 的
// unsafe 请求在会话中间件之前直接 403；只省略 CORS 响应头并不能阻止浏览器完成
// simple POST 的副作用，尤其 vane.* / api.* 同站时 SameSite=Lax 仍会携带 cookie。
// 无 Origin 的 curl / 服务端调用保持兼容。
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
				h.Set("Access-Control-Allow-Methods", "GET, POST, PATCH, DELETE")
				h.Set(
					"Access-Control-Allow-Headers",
					"Content-Type, Idempotency-Key",
				)
				h.Set("Access-Control-Max-Age", "600")
				w.WriteHeader(http.StatusNoContent)
				return
			}
		}
		if r.Method != http.MethodOptions && isUnsafeHTTPMethod(r.Method) &&
			!s.checkOrigin(w, r) {
			return
		}
		next.ServeHTTP(w, r)
	})
}

func isUnsafeHTTPMethod(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodTrace:
		return false
	default:
		return true
	}
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
