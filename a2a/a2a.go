// Package a2a 把 vane 暴露为 Agent2Agent server（a2a-contract，第一期单 skill content.query）。
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

	"github.com/YouToco/vane/types"
)

// 配额默认值（契约 §5.7，Boss 拍板"契约给默认值再核"）。
const (
	// maxConcurrentExecutions 是 SDK 并发执行上限：确定性 DB 查询无 LLM 争抢，单机小 VPS。
	maxConcurrentExecutions = 4
	// dbQueryTimeout 是 executor 单次 DB 查询超时：pgxpool.Close 阻塞等借出连接
	//（main.go 关停警告），查询不能无界占用。
	dbQueryTimeout = 5 * time.Second
)

// TaskStorage 是任务持久化窄接口，*store.Store 满足（契约 §4.1 前四方法）。
type TaskStorage interface {
	CreateA2ATask(ctx context.Context, t *types.A2ATask) error
	GetA2ATask(ctx context.Context, id string) (*types.A2ATask, error)
	UpdateA2ATask(ctx context.Context, id string, expectedVersion int64, status string, task json.RawMessage) error
	ListA2ATasks(ctx context.Context, q types.A2ATaskQuery) ([]types.A2ATask, int64, string, error)
}

// ContentStore 是 content.query 数据面窄接口，*store.Store 满足（契约 §4.2）。
// 只有检索一个方法：画像/个性化分/推送历史在编译期就不可达（契约 §8.3）。
type ContentStore interface {
	SearchContentItems(ctx context.Context, keyword string, since time.Time, limit int) ([]types.ContentItem, error)
}

// Deps 由 cmd/server/main.go 装配（契约 §7）。
type Deps struct {
	Storage TaskStorage  // 生产 = *store.Store
	Content ContentStore // 生产 = *store.Store
	Token   string       // cfg.A2A.Token；空值 = 挂载但 auth 恒 401（§6）
	BaseURL string       // cfg.A2A.BaseURL，进 AgentCard supportedInterfaces 的 url
	Version string       // 服务版本串，进 AgentCard.version
}

// Mount 把 A2A 端点挂到根 mux。cfg.A2A.Enabled=false 时 main.go 不调用本函数（零暴露面）。
// Token 为空 → slog.Warn 一次并照常挂载（auth 恒 401），与 config 对 dashboard.password
// 的"只告警不拒启动"语义一致。
func Mount(mux *http.ServeMux, deps Deps) error {
	if deps.Token == "" {
		slog.Warn("a2a: token 未配置（VANE_A2A_TOKEN），/a2a 将拒绝所有请求（恒 401）")
	}
	rh := a2asrv.NewHandler(newExecutor(deps),
		a2asrv.WithTaskStore(newTaskStore(deps.Storage)),
		// 显式传入 capabilities：SDK 对 streaming 方法按它拒绝（ErrUnsupportedOperation），
		// 与 buildCard 共用同一包级变量，杜绝"卡片说不支持、handler 却放行"的分裂。
		a2asrv.WithCapabilityChecks(&capabilities),
		a2asrv.WithConcurrencyConfig(limiter.ConcurrencyConfig{MaxExecutions: maxConcurrentExecutions}),
		a2asrv.WithLogger(slog.Default()))
	mux.Handle("POST /a2a", requireBearer(deps.Token, a2asrv.NewJSONRPCHandler(rh)))
	// card 端点公开无认证（设计选择非规范强制，契约 §5.2）：卡片内无内部 URL/密钥/owner
	// 信息。路径用 SDK 常量不硬编码；handler 自带 GET/OPTIONS 方法过滤。
	mux.Handle(a2asrv.WellKnownAgentCardPath, a2asrv.NewStaticAgentCardHandler(buildCard(deps)))
	return nil
}
