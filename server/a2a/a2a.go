// Package a2a 把 vane 暴露为 Agent2Agent server。每个请求由 hash-only bearer
// 解析为完整 workspace Principal，任务、历史与内容投影都不得脱离该 scope。
//
// 形态仿 api/：Deps 注入 + 窄接口定义在消费方 + Mount 挂根 mux。SDK（a2a-go/v2）类型
// 只出现在本包，store 只见 types.A2ATask（隔离原则，同 agent.Store 窄接口先例）。
// executor 是确定性执行：直接查 store 不经 LLM——零注入面、零 token 成本（契约 §0）。
package a2a

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/a2aproject/a2a-go/v2/a2asrv/limiter"

	"github.com/YouToco/vane/server/agent"
	"github.com/YouToco/vane/server/auth"
	"github.com/YouToco/vane/server/llm"
	"github.com/YouToco/vane/server/types"
)

// 配额默认值（契约 §5.7，Boss 拍板"契约给默认值再核"）。
const (
	// maxConcurrentExecutions 是 SDK 并发执行上限（立即拒绝、不排队）。
	// **已知取舍（对抗审查 A-3）**：assistant.chat 上线后本限额被两类任务共享——
	// 毫秒级的 content.query 与至多 120s 的 chat。对端并发扇出 ≥8 个慢 chat 会占满
	// 全部槽位，期间新来的 content.query 也被立即拒绝。身份已按工作区
	// 隔离，但进程级并发槽仍是共享资源；正式多租户放量前要增加 tenant 公平配额。
	maxConcurrentExecutions = 8
	// dbQueryTimeout 是 executor 单次 DB 查询超时：pgxpool.Close 阻塞等借出连接
	//（main.go 关停警告），查询不能无界占用。
	dbQueryTimeout = 5 * time.Second
	// chatBudget 是 assistant.chat 单任务总预算（契约 §12：对齐 agent chatCallTimeout
	// 的 120s——多轮 FC 共享这一个预算，不是每轮 120s）。超预算任务由 SDK 后台完成
	// 或留 WORKING，客户端 GetTask 兜底。
	chatBudget = 120 * time.Second
	// a2aWriteBudget 是 /a2a 路由的写超时（> chatBudget 留响应余量）。
	a2aWriteBudget = 150 * time.Second
	// maxA2ARequestBytes caps the entire JSON-RPC envelope. A public multi-user
	// ingress must not let an authenticated peer turn an unbounded chat text or
	// JSON tail into process memory/model-input amplification.
	maxA2ARequestBytes int64 = 256 << 10
)

// TaskStorage 是任务持久化窄接口，*store.Store 满足（契约 §4.1 前四方法）。
type TaskStorage interface {
	CreateA2APrincipalTask(ctx context.Context, scope types.A2AExecutionScope, t *types.A2ATask) error
	GetA2APrincipalTask(ctx context.Context, scope types.A2AExecutionScope, id string) (*types.A2ATask, error)
	UpdateA2APrincipalTask(ctx context.Context, scope types.A2AExecutionScope, id string, expectedVersion int64, status string, task json.RawMessage) error
	ListA2APrincipalTasks(ctx context.Context, scope types.A2AExecutionScope, q types.A2ATaskQuery) ([]types.A2ATask, int64, string, error)
}

// ContentStore 是 content.query 数据面窄接口，*store.Store 满足（契约 §4.2）。
// 只有检索一个方法：画像/个性化分/推送历史在编译期就不可达（契约 §8.3）。
type ContentStore interface {
	SearchContentItemsForA2A(ctx context.Context, scope types.A2AExecutionScope, keyword string, since time.Time, limit int) ([]types.ContentItem, error)
}

// ChatRunner 是 assistant.chat 的执行窄接口（契约 §12 P2；生产 = *agent.Loop 的
// A2A 轨实例，M4 契约 §7.1 RunOnce）。历史与并发语义由本包管理（按 contextId 重建），
// RunOnce 不碰会话存储、不与 owner 飞书轨互相排队。
type ChatRunner interface {
	RunOnce(ctx context.Context, principal auth.Principal, history []llm.ChatMessage, text string) (agent.Outcome, []llm.ChatMessage, error)
}

// Deps 由 cmd/server/main.go 装配（契约 §7）。
type Deps struct {
	Storage       TaskStorage         // 生产 = *store.Store
	Content       ContentStore        // 生产 = *store.Store
	Chat          ChatRunner          // 生产 = A2A 轨 agent.Loop；nil = assistant.chat 未启用（REJECTED）
	Authenticator AccessAuthenticator // 生产 = *store.Store；每请求解析 live Principal
	BaseURL       string              // cfg.A2A.BaseURL，进 AgentCard supportedInterfaces 的 url
	Version       string              // 服务版本串，进 AgentCard.version
}

// Mount 把 A2A 端点挂到根 mux。cfg.A2A.Enabled=false 时 main.go 不调用本函数（零暴露面）。
// Authenticator 未装配时照常挂载但所有受保护请求恒 401；不存在全局
// owner 或静态 token 回退。
func Mount(mux *http.ServeMux, deps Deps) (*Runtime, error) {
	if deps.Authenticator == nil {
		slog.Warn("a2a: scoped authenticator 未装配，/a2a 将拒绝所有请求")
	}
	runtime := newRuntime()
	executor := &lifecycleExecutor{inner: newExecutor(deps), runtime: runtime}
	rh := a2asrv.NewHandler(executor,
		a2asrv.WithTaskStore(newTaskStore(deps.Storage)),
		// 显式传入 capabilities：SDK 对 streaming 方法按它拒绝（ErrUnsupportedOperation），
		// 与 buildCard 共用同一包级变量，杜绝"卡片说不支持、handler 却放行"的分裂。
		a2asrv.WithCapabilityChecks(&capabilities),
		a2asrv.WithConcurrencyConfig(limiter.ConcurrencyConfig{MaxExecutions: maxConcurrentExecutions}),
		a2asrv.WithLogger(slog.Default()))
	rh = &lifecycleRequestHandler{RequestHandler: rh, runtime: runtime}
	jsonRPCHandler := a2asrv.NewJSONRPCHandler(rh)
	boundedJSONRPCHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ContentLength > maxA2ARequestBytes {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxA2ARequestBytes)
		jsonRPCHandler.ServeHTTP(w, r)
	})
	a2aHandler := requireScopedBearer(deps.Authenticator, boundedJSONRPCHandler)
	mux.Handle("POST /a2a", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 逐路由放宽写超时（契约 §12 的既定杠杆，不动 server 级 WriteTimeout=30s）：
		// assistant.chat 的同步执行预算 120s（chatBudget）超过全局写超时，不放宽的话
		// 响应会在 30s 被掐断——任务仍会在 SDK 后台完成（GetTask 可取），但同步返回
		// 路径没了。SetWriteDeadline 失败只记日志：老式 ResponseWriter 不支持时
		// 行为退化回"超时靠 GetTask 兜底"，不值得整个请求失败。
		if err := http.NewResponseController(w).SetWriteDeadline(time.Now().Add(a2aWriteBudget)); err != nil {
			slog.Warn("a2a: 放宽写超时失败，长执行将依赖 GetTask 兜底", "err", err)
		}
		a2aHandler.ServeHTTP(w, r)
	}))
	// card 端点公开无认证（设计选择非规范强制，契约 §5.2）：卡片内无内部 URL/密钥/owner
	// 信息。路径用 SDK 常量不硬编码；handler 自带 GET/OPTIONS 方法过滤。
	mux.Handle(a2asrv.WellKnownAgentCardPath, a2asrv.NewStaticAgentCardHandler(buildCard(deps)))
	return runtime, nil
}
