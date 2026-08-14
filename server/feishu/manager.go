// Package feishu 实现见微 Vane 的飞书通道：WS 长连接生命周期管理、
// 消息处理链（用户消息 → LLM → 卡片回复）、凭证校验与交互卡片构建。
// 飞书凭证由用户在 Dashboard 向导填入并存 settings 表，因此本包不读
// config 里的 FeishuConfig，而是每次连接前从 store 重读。
package feishu

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
	larkws "github.com/larksuite/oapi-sdk-go/v3/ws"

	"github.com/YouToco/vane/agent"
	"github.com/YouToco/vane/feedback"
	"github.com/YouToco/vane/llm"
	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
)

// AgentRunner is the Feishu natural-language message boundary.
// handler 单测可注入假实现；直接引用 agent.Outcome 不构成循环
// （agent 不 import feishu，依赖单向）。
type AgentRunner interface {
	HandleMessage(ctx context.Context, userID int64, text string) (agent.Outcome, error)
	// HandleExternalContextMessage 用于已经拼入推送正文/引用消息的输入。它与
	// 普通用户文本是不同的信任类型，agent 从首轮就关闭画像与工具面。
	HandleExternalContextMessage(ctx context.Context, userID int64, text string) (agent.Outcome, error)
}

// FeedbackRunner 是 feishu 对 feedback 服务的窄依赖面（M5 契约 §10.4）：
// 推送卡按钮点击 + 追问识别。同 AgentRunner 的形态——feedback 不 import feishu
// （构卡与发送都靠注入），依赖单向、无环。
type FeedbackRunner interface {
	HandleClick(ctx context.Context, userID int64, click feedback.Click) (feedback.ClickResult, error)
	HandleReasonSubmit(ctx context.Context, userID int64, submit feedback.ReasonSubmit) (feedback.ClickResult, error)
	// WrapQuestion 尝试把"回复推送卡"的消息识别为追问并包装上下文；
	// matched=false 时调用方按普通消息原样处理。
	WrapQuestion(
		ctx context.Context,
		userID int64,
		appIdentity string,
		inboundMsgID string,
		parentMsgID string,
		rootMsgID string,
		text string,
	) (wrapped string, matched bool, err error)
}

// settings 表的两个已知 key（契约 §1）。导出供 api 层写入时复用，
// 避免跨包用魔法字符串各写各的 "feishu"（改一处漏一处会导致保存后读不到）。
const (
	SettingKeyFeishu = "feishu"
	SettingKeyOwner  = "feishu_owner"

	settingKeyFeishu = SettingKeyFeishu
	settingKeyOwner  = SettingKeyOwner
)

// feishuSetting 对应 settings.feishu 的 value 结构。
type feishuSetting struct {
	AppID     string `json:"app_id"`
	AppSecret string `json:"app_secret"`
	Enabled   bool   `json:"enabled"`
}

// ownerSetting 对应 settings.feishu_owner 的 value 结构。
// owner = 第一个给机器人发消息的飞书用户，是测试卡片与后续推送的收件人。
type ownerSetting struct {
	OpenID      string `json:"open_id"`
	Name        string `json:"name"`
	AppIdentity string `json:"app_identity,omitempty"`
	ChatID      string `json:"chat_id,omitempty"`
	CapturedAt  string `json:"captured_at"`
}

// Status 是飞书通道状态快照，直接序列化为 GET /api/feishu/status 的响应体。
type Status struct {
	Configured  bool       `json:"configured"`
	Connected   bool       `json:"connected"`
	BotName     string     `json:"bot_name"`
	OwnerOpenID string     `json:"owner_open_id"`
	OwnerName   string     `json:"owner_name"`
	LastError   string     `json:"last_error"`
	ConnectedAt *time.Time `json:"connected_at,omitempty"`
}

// VerifyResult 是凭证校验结果，直接序列化为 POST /api/feishu/verify 的响应体。
type VerifyResult struct {
	CredentialsOK bool   `json:"credentials_ok"`
	BotOK         bool   `json:"bot_ok"`
	BotName       string `json:"bot_name"`
	Detail        string `json:"detail"` // 失败/未就绪原因，面向向导用户的人话
}

// Manager 管理飞书 WS 长连接的完整生命周期。
// 状态字段全部由 mu 保护；owner 信息在内存缓存一份，
// 因为 Status() 没有 ctx 参数、不能查库。
type Manager struct {
	st  *store.Store
	cli *llm.Client
	rec *llm.Recorder

	// asyncMu + asyncWG 是所有飞书回调异步工作的生命周期闸门。Add/Go 与
	// Shutdown 关闭准入必须在同一把锁下裁决；关闸后才 Wait，避免 Wait 与首次
	// Add 并发导致 Shutdown 提前返回。已接纳工作各自仍有硬超时；Shutdown 先停止
	// 新准入并等待，只有调用方给的停机宽限耗尽才取消 asyncCtx 强制收敛。
	asyncMu        sync.Mutex
	asyncAccepting bool
	asyncClosed    atomic.Bool
	asyncCtx       context.Context
	asyncCancel    context.CancelFunc
	asyncWG        sync.WaitGroup

	// agent 是消息链的 agent loop 入口（SetAgent 注入）；nil 时消息链
	// 回退 chat_reply 直连 LLM——保证 agent 配置不全时通道仍可用。
	// 由 mu 保护：注入发生在 main 装配期，但 handler goroutine 并发读。
	agent AgentRunner

	// feedback 是推送卡反馈与追问的入口（SetFeedback 注入）；nil 时反馈按钮
	// 点击静默忽略、追问降级为普通消息——同 agent 的可选注入语义。
	feedback FeedbackRunner

	// captureMu 串行化 owner 捕获的 check-then-act（见 handler.captureOwnerIfFirst）。
	captureMu sync.Mutex

	// reloadMu 串行化整个 reload：断旧连接与建新连接之间会释放 mu 做网络 I/O
	//（GetSetting / verifyCredentials 各是一次 HTTPS 往返），若不整体串行，
	// 并发的 Start/Reconfigure（如 Dashboard「保存并连接」双击）会互相穿插——
	// 后一次在前一次装好 wsCancel 之前就走过了"断旧连接"段，导致前一次的
	// 活 WS 连接被覆盖且永不 cancel（泄漏一条活连接，且其错误因代数不符被静默吞掉）。
	// mu 仍只做短临界区状态锁；reloadMu 只在 reload 全程持有。
	reloadMu sync.Mutex

	mu       sync.Mutex
	baseCtx  context.Context    // Start 传入的进程级 ctx，重连时从它派生 wsCtx
	wsCancel context.CancelFunc // 取消当前 WS 连接；nil 表示当前无连接
	// gen 是连接代数：每次 reload 递增。旧连接的 WS goroutine 回写错误
	// 状态前先比对代数，避免"旧连接的尸体"覆盖新连接的健康状态。
	gen       int64
	apiClient *lark.Client // 发消息用的 API 客户端，与 WS 客户端分离
	apiAppID  string       // 与 apiClient 同代；A6 用其防止跨 App 修改旧卡
	// startupOutbound exists only between the synchronous outbound-only Gate
	// and Start. It prevents Start from clearing that freshly verified client
	// while Temporal ingress is already live, and is consumed exactly once.
	startupOutbound *feishuSetting
	configured      bool
	connected       bool
	botName         string
	ownerOpenID     string
	ownerName       string
	ownerAppID      string
	ownerChatID     string
	lastError       string
	connectedAt     *time.Time
}

// NewManager 构造 Manager。llm 客户端与记账器由 main 注入，
// 消息处理链（chat_reply）复用它们。
func NewManager(st *store.Store, cli *llm.Client, rec *llm.Recorder) *Manager {
	asyncCtx, asyncCancel := context.WithCancel(context.Background())
	return &Manager{
		st:             st,
		cli:            cli,
		rec:            rec,
		asyncAccepting: true,
		asyncCtx:       asyncCtx,
		asyncCancel:    asyncCancel,
	}
}

// startAsync 接纳一项有界异步工作，并让 Shutdown 能等待它完成。
// detachParent=true 只移除连接/回调 ctx 的取消信号（保留 trace 等 values）：
// 已经开始的确认与反馈不能因 Dashboard 重连被截断；Manager 自己的停机取消
// 仍通过 asyncCtx 生效。false 用于普通消息/菜单，它们应随 WS 换代停止。
func (m *Manager) startAsync(
	parent context.Context,
	timeout time.Duration,
	detachParent bool,
	name string,
	fn func(context.Context),
) bool {
	if parent == nil {
		parent = context.TODO()
	}

	m.asyncMu.Lock()
	defer m.asyncMu.Unlock()
	if !m.asyncAccepting {
		return false
	}
	lifecycle := m.asyncCtx
	m.asyncWG.Go(func() {
		base := parent
		if detachParent {
			base = context.WithoutCancel(parent)
		}
		ctx, cancel := context.WithTimeout(base, timeout)
		stopLifecycleCancel := context.AfterFunc(lifecycle, cancel)
		defer func() {
			stopLifecycleCancel()
			cancel()
			if recovered := recover(); recovered != nil {
				slog.Error("feishu: 异步工作 panic", "work", name, "recover", recovered)
			}
		}()
		fn(ctx)
	})
	return true
}

// beginCallback 把 SDK 同步执行的卡片回调本身也纳入 WaitGroup。回调在启动
// worker 前会查库；若只跟踪 worker，Shutdown 可能在这段查库尚未返回时误以为
// 已排空并关闭 DB。Add 与准入裁决同样在 asyncMu 下完成，返回的 finish 必须 defer。
func (m *Manager) beginCallback() (finish func(), ok bool) {
	m.asyncMu.Lock()
	defer m.asyncMu.Unlock()
	if !m.asyncAccepting {
		return nil, false
	}
	m.asyncWG.Add(1)
	return m.asyncWG.Done, true
}

// Shutdown 停止接纳新回调、断开当前 WS，并等待所有已接纳的 Manager 工作结束。
// API 发送客户端刻意保留到进程退出：Manager 回调结束后，feedback deep-dive 与
// Temporal Push Activity 仍可能在各自的 drain 阶段发送最后一条消息。lark.Client
// 没有需要显式关闭的资源，提前置 nil 只会把一次可完成的送达变成确定性失败。
// A6 的跨进程耐久送达由独立 receipt dispatcher 承担；它在 Manager ingress
// 排空后仍复用这里保留的同代 API 客户端，直到 dispatcher 自己完成停机。
func (m *Manager) Shutdown(ctx context.Context) error {
	m.asyncClosed.Store(true)
	m.asyncMu.Lock()
	m.asyncAccepting = false
	m.asyncMu.Unlock()
	m.mu.Lock()
	m.startupOutbound = nil
	m.mu.Unlock()

	// 先取消连接，让消息/菜单工作收到取消；确认/反馈使用 detachParent，仍可在
	// 调用方给出的停机宽限内完成并发送结果。
	m.cancelWebSocket()

	done := make(chan struct{})
	go func() {
		m.asyncWG.Wait()
		close(done)
	}()

	select {
	case <-done:
		m.asyncCancel()
		// 启动 reload 可能与第一次 cancel 并发并在稍后装好新连接；等它进入
		// asyncWG 终态后再取消一次，关闭这个窗口。
		m.cancelWebSocket()
		return nil
	case <-ctx.Done():
		// 宽限耗尽后才强制取消脱离 WS 的确认/反馈工作。若下游无视 ctx，
		// Shutdown 仍按调用方 deadline 有界返回；Wait goroutine 会在其终止后退出。
		m.asyncCancel()
		m.cancelWebSocket()
		return ctx.Err()
	}
}

// cancelWebSocket 只取消入站 WS；发送端由下游 drain 共用，不能在这里释放。
func (m *Manager) cancelWebSocket() {
	m.mu.Lock()
	if m.wsCancel != nil {
		m.wsCancel()
		m.wsCancel = nil
	}
	m.connected = false
	m.connectedAt = nil
	m.mu.Unlock()
}

// SetAgent 注入 agent loop（main 装配期、Start 之前调用）。
// 选 Set 注入而非 NewManager 增参：agent 的构造依赖 scheduler，
// 而 Manager 必须先于 scheduler 构造（pusher 依赖它做推送出口），
// 增参会把装配顺序拧成环。
func (m *Manager) SetAgent(a AgentRunner) {
	m.mu.Lock()
	m.agent = a
	m.mu.Unlock()
}

// agentRunner 返回已注入的 agent；nil 表示未注入（消息链回退 chat_reply）。
func (m *Manager) agentRunner() AgentRunner {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.agent
}

// SetFeedback 注入反馈服务（main 装配期、Start 之前调用）。与 SetAgent 同构：
// feedback 的构造依赖 agentLoop（Notifier），故只能在 agent 之后注入。
func (m *Manager) SetFeedback(f FeedbackRunner) {
	m.mu.Lock()
	m.feedback = f
	m.mu.Unlock()
}

// feedbackRunner 返回已注入的反馈服务；nil 表示未注入。
func (m *Manager) feedbackRunner() FeedbackRunner {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.feedback
}

// ReplyMarkdown 以回复形式在指定消息下发一张 markdown 卡（深度解读结果送达用，
// M5 契约 §10.4）。与 handler.reply 的差异只在可从任意 goroutine 调用：
// 深度解读的异步生成结束时早已脱离原回调链。
func (m *Manager) ReplyMarkdown(ctx context.Context, parentMessageID, markdown string) error {
	api := m.api()
	if api == nil {
		return types.NewAppError(types.CodeInternal, "飞书未连接，无法发送消息", nil)
	}
	resp, err := api.Im.Message.Reply(ctx, larkim.NewReplyMessageReqBuilder().
		MessageId(parentMessageID).
		Body(larkim.NewReplyMessageReqBodyBuilder().
			MsgType(larkim.MsgTypeInteractive).
			Content(BuildReplyCard(markdown)).
			Build()).
		Build())
	if err != nil {
		return types.NewAppError(types.CodeInternal, "回复飞书消息失败", err)
	}
	if !resp.Success() {
		return types.NewAppError(types.CodeInternal,
			fmt.Sprintf("回复飞书消息失败（code=%d）: %s", resp.Code, resp.Msg), nil)
	}
	return nil
}

// Start 读取 settings 并在有配置且 enabled 时建立 WS 连接；无配置静默待命。
// 非阻塞：连接失败不影响 main 启动，一切结果通过 Status() 暴露给 Dashboard。
func (m *Manager) Start(ctx context.Context) {
	m.mu.Lock()
	m.baseCtx = ctx
	m.mu.Unlock()

	// Prepared startup bypasses startAsync's goroutine, so take the same
	// admission lock explicitly. Shutdown cannot close admission between this
	// decision and WS installation, and a post-Shutdown Start cannot resurrect
	// the connection.
	m.asyncMu.Lock()
	if !m.asyncAccepting {
		m.asyncMu.Unlock()
		slog.Warn("feishu: Manager 正在关闭，跳过启动重载")
		return
	}
	preparedStarted := m.startPreparedWebSocket(ctx)
	m.asyncMu.Unlock()
	if preparedStarted {
		return
	}
	if !m.startAsync(ctx, 30*time.Second, false, "startup_reload", func(ctx context.Context) {
		if err := m.reload(ctx); err != nil {
			slog.Error("feishu: 启动时连接失败（可在 Dashboard 重新配置）", "err", err)
		}
	}) {
		slog.Warn("feishu: Manager 正在关闭，跳过启动重载")
	}
}

// PrepareOutbound synchronously prepares only the provider API client used by
// durable send/reconciliation. It does not create a WebSocket client, admit a
// callback, or start a goroutine. The server uses this as a startup barrier
// before the first push-effect recovery pass; ordinary default-off startup
// continues to use Start directly.
func (m *Manager) PrepareOutbound(ctx context.Context) error {
	if m == nil || m.st == nil {
		return types.NewAppError(
			types.CodeInternal, "飞书发送端未就绪", nil)
	}
	finish, accepted := m.beginCallback()
	if !accepted {
		return types.NewAppError(
			types.CodeConflict, "服务正在重启，请稍后重试", types.ErrConflict)
	}
	defer finish()
	m.reloadMu.Lock()
	defer m.reloadMu.Unlock()
	m.mu.Lock()
	started := m.baseCtx != nil || m.wsCancel != nil || m.connected
	m.mu.Unlock()
	if started {
		return types.NewAppError(
			types.CodeConflict,
			"飞书发送端仅允许在服务入口启动前预热",
			types.ErrConflict,
		)
	}

	// This is an outbound permission boundary just like Reconfigure: never
	// retain an earlier client while current durable settings are evaluated.
	m.mu.Lock()
	m.apiClient = nil
	m.apiAppID = ""
	m.startupOutbound = nil
	m.configured = false
	m.connected = false
	m.connectedAt = nil
	m.lastError = ""
	m.mu.Unlock()

	raw, err := m.st.GetSetting(ctx, settingKeyFeishu)
	if errors.Is(err, types.ErrNotFound) {
		return types.NewAppError(
			types.CodeConflict, "飞书发送端尚未配置", nil)
	}
	if err != nil {
		return types.NewAppError(
			types.CodeInternal, "读取飞书发送端配置失败", nil)
	}
	var cfg feishuSetting
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return types.NewAppError(
			types.CodeInternal, "飞书发送端配置无效", nil)
	}
	configured := cfg.AppID != "" && cfg.AppSecret != ""
	m.mu.Lock()
	m.configured = configured
	m.mu.Unlock()
	if !configured || !cfg.Enabled {
		return types.NewAppError(
			types.CodeConflict, "飞书发送端未启用", nil)
	}

	vr := verifyCredentials(ctx, cfg.AppID, cfg.AppSecret)
	if !vr.CredentialsOK {
		m.setError(vr.Detail)
		return types.NewAppError(
			types.CodeValidation, "飞书发送端凭证校验失败", nil)
	}
	m.loadOwner(ctx)
	if m.asyncClosed.Load() {
		return types.NewAppError(
			types.CodeConflict, "服务正在重启，请稍后重试", types.ErrConflict)
	}
	m.mu.Lock()
	m.apiClient = lark.NewClient(cfg.AppID, cfg.AppSecret)
	m.apiAppID = cfg.AppID
	m.botName = vr.BotName
	prepared := cfg
	m.startupOutbound = &prepared
	m.mu.Unlock()
	if m.asyncClosed.Load() {
		m.mu.Lock()
		m.apiClient = nil
		m.apiAppID = ""
		m.startupOutbound = nil
		m.mu.Unlock()
		return types.NewAppError(
			types.CodeConflict, "服务正在重启，请稍后重试", types.ErrConflict)
	}
	if err := m.backfillOwnerChatID(ctx); err != nil &&
		!errors.Is(err, types.ErrNotFound) {
		// Chat backfill is supplementary for old installations. Exact recovery
		// uses the frozen chat checkpoint and does not depend on this cache.
		slog.WarnContext(ctx, "feishu: outbound-only owner chat backfill skipped",
			"error_code", types.CodeOf(err))
	}
	return nil
}

// startPreparedWebSocket consumes the credential generation synchronously
// prepared by PrepareOutbound. It never clears the ready API client, so the
// worker admitted immediately before Manager.Start cannot observe a false
// outbound-not-ready gap.
func (m *Manager) startPreparedWebSocket(ctx context.Context) bool {
	m.reloadMu.Lock()
	defer m.reloadMu.Unlock()
	m.mu.Lock()
	prepared := m.startupOutbound
	m.startupOutbound = nil
	if prepared == nil || m.apiClient == nil ||
		m.apiAppID != prepared.AppID {
		m.mu.Unlock()
		return false
	}
	if m.wsCancel != nil {
		m.wsCancel()
		m.wsCancel = nil
	}
	m.gen++
	gen := m.gen
	wsCtx, cancel := context.WithCancel(ctx)
	m.wsCancel = cancel
	m.mu.Unlock()

	wsCli := m.newWebSocketClient(wsCtx, *prepared)
	now := time.Now()
	m.mu.Lock()
	m.connected = true
	m.connectedAt = &now
	m.mu.Unlock()
	go func() {
		err := wsCli.Start(wsCtx)
		if err == nil {
			return
		}
		m.mu.Lock()
		defer m.mu.Unlock()
		if m.gen != gen {
			return
		}
		m.recordWSFailureLocked(slog.Default(), err)
	}()
	return true
}

func (m *Manager) newWebSocketClient(
	ctx context.Context,
	cfg feishuSetting,
) *larkws.Client {
	h := newHandlerForApp(m, ctx, cfg.AppID)
	return larkws.NewClient(cfg.AppID, cfg.AppSecret,
		larkws.WithEventHandler(h.eventDispatcher()),
		// The SDK may include the complete WebSocket URL not only in INFO
		// connection logs, but also in malformed-URL errors. Discard all
		// SDK-supplied values before they can reach journald.
		larkws.WithLogger(newFeishuSDKLogger()),
		larkws.WithLogLevel(larkcore.LogLevelError),
	)
}

// Reconfigure 在 API 层保存新配置后调用：断开旧连接 → 重读 settings → 重连。
// 与 Start 不同，这里同步返回错误，让 POST /api/feishu/config 能把失败告诉用户。
func (m *Manager) Reconfigure(ctx context.Context) error {
	finish, accepted := m.beginCallback()
	if !accepted {
		return types.NewAppError(types.CodeConflict, "服务正在重启，请稍后重试", types.ErrConflict)
	}
	defer finish()
	return m.reload(ctx)
}

// Status 返回当前状态快照（纯内存读取，供前端轮询）。
func (m *Manager) Status() Status {
	m.mu.Lock()
	defer m.mu.Unlock()
	return Status{
		Configured:  m.configured,
		Connected:   m.connected,
		BotName:     m.botName,
		OwnerOpenID: m.ownerOpenID,
		OwnerName:   m.ownerName,
		LastError:   m.lastError,
		ConnectedAt: m.connectedAt,
	}
}

// Verify 校验一对凭证但不保存（POST /api/feishu/verify 用）。
func (m *Manager) Verify(ctx context.Context, appID, appSecret string) VerifyResult {
	return verifyCredentials(ctx, appID, appSecret)
}

// SendTestCard 给已捕获的 owner 发一张测试卡片（向导第 5 步）。
// 无 owner 时返回 CodeNotFound 的 AppError，API 层据此回 409。
func (m *Manager) SendTestCard(ctx context.Context) error {
	m.mu.Lock()
	client := m.apiClient
	openID := m.ownerOpenID
	m.mu.Unlock()

	if openID == "" {
		// 缓存可能落后于库（如 owner 是上次进程运行时捕获的），兜底查一次。
		m.loadOwner(ctx)
		m.mu.Lock()
		openID = m.ownerOpenID
		m.mu.Unlock()
	}
	if openID == "" {
		return types.NewAppError(types.CodeNotFound, "还没有捕获到 owner，请先给机器人发一条消息", nil)
	}
	if client == nil {
		return types.NewAppError(types.CodeConflict, "飞书通道未连接，请先完成配置并连接", nil)
	}

	card := BuildReplyCard("**测试卡片**\n\n如果你看到这张卡片，说明「配置 → 长连接 → 事件订阅 → 消息收发」整条链路已经打通。")
	resp, err := client.Im.Message.Create(ctx, larkim.NewCreateMessageReqBuilder().
		ReceiveIdType(larkim.CreateMessageV1ReceiveIDTypeOpenId).
		Body(larkim.NewCreateMessageReqBodyBuilder().
			ReceiveId(openID).
			MsgType(larkim.MsgTypeInteractive).
			Content(card).
			Build()).
		Build())
	if err != nil {
		return types.NewAppError(types.CodePushFailed, "发送测试卡片失败", err)
	}
	if !resp.Success() {
		return types.NewAppError(types.CodePushFailed,
			fmt.Sprintf("发送测试卡片被飞书拒绝（code %d：%s）", resp.Code, resp.Msg), nil)
	}
	return nil
}

// reload 是 Start 与 Reconfigure 的共同实现：
// 断旧连接 → 重读 settings → 校验凭证 → 建新连接。
func (m *Manager) reload(ctx context.Context) error {
	// 全程持有 reloadMu：保证"断旧连接 → 建新连接"对并发 reload 原子。
	m.reloadMu.Lock()
	defer m.reloadMu.Unlock()

	// 先断旧连接并递增代数（代数用途见字段注释）。
	m.mu.Lock()
	if m.wsCancel != nil {
		m.wsCancel()
		m.wsCancel = nil
	}
	m.gen++
	gen := m.gen
	m.connected = false
	m.connectedAt = nil
	m.lastError = ""
	// Reconfiguration is an immediate outbound permission boundary. Keeping the
	// old client while a disabled/invalid/new setting is evaluated would let
	// background senders continue using credentials the user just revoked.
	m.apiClient = nil
	m.apiAppID = ""
	m.startupOutbound = nil
	base := m.baseCtx
	m.mu.Unlock()
	if base == nil {
		// 防御：正常接线 Start 先于 Reconfigure，此分支不该走到。
		base = context.Background()
	}

	// 重读配置。无配置是首次部署 / 向导未完成的常态，静默待命而非报错。
	raw, err := m.st.GetSetting(ctx, settingKeyFeishu)
	if errors.Is(err, types.ErrNotFound) {
		m.mu.Lock()
		m.configured = false
		m.apiClient = nil
		m.mu.Unlock()
		return nil
	}
	if err != nil {
		m.setError("读取飞书配置失败：" + err.Error())
		return err
	}
	var cfg feishuSetting
	if err := json.Unmarshal(raw, &cfg); err != nil {
		m.setError("飞书配置格式异常：" + err.Error())
		return types.NewAppError(types.CodeInternal, "飞书配置 JSON 解析失败", err)
	}

	configured := cfg.AppID != "" && cfg.AppSecret != ""
	m.mu.Lock()
	m.configured = configured
	m.mu.Unlock()
	if !configured || !cfg.Enabled {
		return nil // 有记录但未启用/凭证不全：同样待命
	}

	// 预热 owner 缓存（可能是上次运行捕获的），失败不影响连接。
	m.loadOwner(ctx)

	// 连接前先校验凭证：既能立刻拿到 bot 名字供 Status 展示，也避免
	// 用一对坏凭证去 SDK 里绕一圈才发现失败。
	vr := verifyCredentials(ctx, cfg.AppID, cfg.AppSecret)
	if !vr.CredentialsOK {
		m.setError(vr.Detail)
		return types.NewAppError(types.CodeValidation, vr.Detail, nil)
	}
	// BotOK=false（如"未发布版本"）不阻塞连接：向导要求先建立长连接，
	// 用户回控制台保存"长连接"订阅方式并发布版本后自然就绪。

	wsCtx, cancel := context.WithCancel(base)
	wsCli := m.newWebSocketClient(wsCtx, cfg)

	now := time.Now()
	m.mu.Lock()
	m.wsCancel = cancel
	m.apiClient = lark.NewClient(cfg.AppID, cfg.AppSecret)
	m.apiAppID = cfg.AppID
	m.botName = vr.BotName
	// 乐观置位：凭证刚通过校验，连接大概率成功；若 Start 立刻失败，
	// 下方 goroutine 会（在同一代数下）把状态改回 false 并记录原因。
	m.connected = true
	m.connectedAt = &now
	m.mu.Unlock()

	if err := m.backfillOwnerChatID(ctx); err != nil {
		if errors.Is(err, types.ErrNotFound) {
			slog.Info("feishu: owner chat 暂无历史回执可回填")
		} else {
			slog.Warn("feishu: owner chat 回填未完成",
				"error_code", types.CodeOf(err))
		}
	}

	go func() {
		// larkws 的 Start 成功时永久阻塞（SDK 内部尾部 select{}）：
		// wsCtx 取消只能终止连接循环，goroutine 本身会永远停在 select{}
		// 上——因此每次 Reconfigure 都会泄漏一个 parked goroutine。
		// MVP 已知并接受：重配置是极低频操作，泄漏量恒定为个位数
		//（M2 事实基准明确此为 SDK 行为）。这条 SDK parked goroutine 刻意不
		// 加入 asyncWG：它无法返回，Shutdown 只能 cancel 连接，等待它会让
		// 每次停机永久卡死。其余回调与启动工作全部走 startAsync。
		err := wsCli.Start(wsCtx)
		if err == nil {
			return
		}
		m.mu.Lock()
		defer m.mu.Unlock()
		if m.gen != gen {
			return // 已被更新的连接取代，不覆盖新状态
		}
		m.recordWSFailureLocked(slog.Default(), err)
	}()
	return nil
}

// recordWSFailureLocked records a WS failure without retaining any provider
// value. The caller must hold m.mu.
func (m *Manager) recordWSFailureLocked(logger *slog.Logger, err error) {
	m.connected = false
	m.connectedAt = nil
	m.lastError = "WS 连接失败（详情已脱敏）"
	if logger != nil {
		logger.Error("feishu: WS 连接退出（详情已脱敏）",
			"error_type", fmt.Sprintf("%T", err))
	}
}

// api 返回当前发消息用的 API 客户端（handler 回复消息时取用）。
func (m *Manager) api() *lark.Client {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.apiClient
}

func (m *Manager) currentAPI() (*lark.Client, string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.apiClient == nil || m.apiAppID == "" {
		return nil, m.apiAppID, false
	}
	return m.apiClient, m.apiAppID, true
}

// apiForExpectedApp atomically binds one durable provider attempt to the
// exact configured App generation. The returned identity belongs to the
// returned client even if Reconfigure replaces the Manager fields while the
// request is in flight.
func (m *Manager) apiForExpectedApp(expectedAppID string) (*lark.Client, string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	actualAppID := m.apiAppID
	if expectedAppID == "" ||
		m.apiClient == nil ||
		actualAppID == "" ||
		actualAppID != expectedAppID {
		return nil, actualAppID, false
	}
	return m.apiClient, actualAppID, true
}

func (m *Manager) appIsCurrent(appID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return appID != "" && m.apiClient != nil && m.apiAppID == appID
}

// setOwner 更新 owner 内存缓存（handler 捕获 owner、loadOwner 预热时调用）。
func (m *Manager) setOwner(openID, name string) {
	m.setOwnerWithChat(openID, name, "", "")
}

func (m *Manager) setOwnerWithChat(openID, name, appID, chatID string) {
	m.mu.Lock()
	if m.ownerOpenID != "" && m.ownerOpenID != openID {
		m.ownerAppID = ""
		m.ownerChatID = ""
	}
	m.ownerOpenID = openID
	m.ownerName = name
	if appID != "" {
		if m.ownerAppID != appID {
			m.ownerChatID = ""
		}
		m.ownerAppID = appID
	}
	if appID != "" && chatID != "" {
		m.ownerChatID = chatID
	}
	m.mu.Unlock()
}

// ownerCaptured 报告是否已有 owner（handler 用于跳过重复捕获）。
func (m *Manager) ownerCaptured() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ownerOpenID != ""
}

// ownerID 返回当前 owner 的 open_id（授权白名单用）；未捕获时为空串。
func (m *Manager) ownerID() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ownerOpenID
}

func (m *Manager) ownerIdentity() (openID, appID, chatID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ownerOpenID, m.ownerAppID, m.ownerChatID
}

// setError 记录最近一次错误供 Status 展示。
func (m *Manager) setError(msg string) {
	m.mu.Lock()
	m.lastError = msg
	m.mu.Unlock()
}

// loadOwner 从 settings 表读 feishu_owner 并刷新缓存；不存在时静默返回。
func (m *Manager) loadOwner(ctx context.Context) {
	raw, err := m.st.GetSetting(ctx, settingKeyOwner)
	if err != nil {
		if !errors.Is(err, types.ErrNotFound) {
			slog.Error("feishu: 读取 owner 设置失败", "err", err)
		}
		return
	}
	var own ownerSetting
	if err := json.Unmarshal(raw, &own); err != nil {
		slog.Error("feishu: owner 设置格式异常", "err", err)
		return
	}
	m.setOwnerWithChat(own.OpenID, own.Name, own.AppIdentity, own.ChatID)
}
