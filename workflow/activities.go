package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/sdk/activity"

	"github.com/YouToco/vane/dedup"
	"github.com/YouToco/vane/feedback"
	"github.com/YouToco/vane/fetcher"
	"github.com/YouToco/vane/selector"
	"github.com/YouToco/vane/types"
)

// simhashWindow 是近似去重的回看窗口；simhashThreshold 是 64-bit simhash
// 判为"近重复"的汉明距离上限（规格 B4 建议 3）。
const (
	simhashWindow    = 72 * time.Hour
	simhashThreshold = 3
)

// maxScoreCandidates 是一次 pipeline 最多送去打分的候选条数上限（成本护栏 #6）。
// Fetch 返回未投递候选时用它截断——防止某源积压大量历史内容时，一次触发就把
// 几百条都送进 LLM 打分，费用/时延失控。50 条足够 TopN 择优，又把上限压在可控范围。
const maxScoreCandidates = 50

// maxPerSourceCandidates 是单个信源最多进入候选窗口的条数（审查 #候选公平性）。
// 全局窗口按 fetched_at DESC 截断时，高产源（tikhub 单页 20 条/轮 + exa 10 条/轮）
// 会把先抓的低产 RSS 源永远挤出窗口；按源限额保证每源都有配额进入打分。
const maxPerSourceCandidates = 20

// alertFetchFailThreshold 是"连续失败几次触发一次主动告警"的阈值（功能 5.2）。
// sources.fail_count 每个包含该源的调度轮 +1、任一次抓成清零，故它就是"连续失败
// streak"。只在 fail_count 恰好等于本阈值那一轮发一次告警（见 markFetchResult 返回值）：
// 跨阈后每轮恒真，若"≥阈值就发"会每轮刷屏；"恰好等于"让每个失败 streak 只告警一次，
// 源恢复（fail_count 归零）后再坏可再次告警。取 3：滤掉单次瞬时抖动，又不至于坏太久
// 才通知。5.2 定为仅告警 MVP——不顺带做 status=disabled 自动停用（那要配套重启用入口）。
// 单用户 MVP 用常量；将来要按源/按用户可调再提到 config。
//
// 已知局限（单用户 MVP 容忍）：fail_count 是读-改-写（markFetchResult 读 src.FailCount
// 再 SET +1，非 DB 侧原子自增），故若两条 pipeline run 并发重叠（如"现在推"按钮 run 与
// 定时轮同时——定时用 SKIP overlap、agent 用确定性 ID 都互斥，唯 adhoc 按钮无并发护栏），
// 可能各自读到同值、各算到阈值、各发一张卡 → 同一 streak 多一张告警（不损数据、不漏报）。
// 多用户/高频触发前根治：把跨阈判定下沉为 DB 侧原子 `UPDATE ... RETURNING fail_count`，
// 或给告警加幂等键。
const alertFetchFailThreshold = 3

// aggMaxItemsPerCard / aggMaxCardBytes 聚合卡的两道体积护栏（附录 A）：
// 条数封顶防一屏塞爆；字节上限对齐飞书卡片 ~30KB 限制留余量，超限对半拆卡，
// 绝不静默截断条目。
const (
	aggMaxItemsPerCard = 8
	aggMaxCardBytes    = 28 << 10
)

// disableFetchFailThreshold 是"连续失败达此值自动停用"的阈值（功能 5.2，Boss 拍板
// 「告警后再宽限」）。刻意远高于告警阈值：先在 3 次发预警卡给 owner 一个人工介入窗口，
// 短暂宕机的站点在 3→10 次之间恢复即清零、不会被误停；持续失败到 10 次才判定失效自动停用，
// 并发一张"已暂停 + 如何重新启用"的卡。停用后 ListDueSourcesByUser 不再返回它（抓取停止），
// 直到用户经 enable_source 工具或信源页按钮重新启用。
const disableFetchFailThreshold = 10

// ============================================================
// 消费方接口：本包只依赖这些窄接口，具体实现（fetcher/scorer/cardgen/
// pusher/store/feishu）由 cmd/server 装配时注入。方法签名对齐各业务包
// 的导出方法（规格 B4/B5/B3），是并行开发的对接契约。
// ============================================================

// Fetcher 抓取单个信源的内容。生产实现是 fetcher.Multi——按 (Platform, Capability)
// 分发到 RSS/Exa/TikHub 具体抓取器；Activity 侧不关心信源类型差异。
type Fetcher interface {
	Fetch(ctx context.Context, src types.Source) ([]types.ContentItem, error)
}

// Scorer 给单条内容打分（scorer.Score，0-100）。traceID 贯穿 llm 记账。
type Scorer interface {
	Score(ctx context.Context, userID int64, item types.ContentItem, traceID string) (float64, error)
}

// CardGenerator 为单条打分内容生成解读正文 markdown（cardgen.Generate 的
// bodyMD，含阅读原文行）。最终卡片 JSON 由 Push 内经注入的 buildCard 构造——
// 按钮 value 携带 delivery_id，只能在拿到 id 后生成（契约 §8.2）。
type CardGenerator interface {
	Generate(ctx context.Context, userID int64, item types.ScoredItem, traceID string) (string, error)
}

// ProfileEvolver 画像演化器（生产实现 evolver.Evolver）：推送前批量消费该用户
// 的新反馈、演化画像 summary/tags（Boss 拍板①：演化随推送批量，非反馈即时）。
type ProfileEvolver interface {
	Evolve(ctx context.Context, userID int64, traceID string) error
}

// Pusher 主动推送一张卡片给指定 open_id（pusher.Push），返回飞书消息 ID。
type Pusher interface {
	Push(ctx context.Context, ownerOpenID, cardJSON string) (string, error)
}

// FeishuManager 暴露 owner（推送收件人）。SendCard 由 Pusher 内部承担，
// 本接口只取 OwnerOpenID——Push Activity 需要知道"推给谁"。
type FeishuManager interface {
	OwnerOpenID() string
}

// Store 是 Activity 需要的数据访问子集（规格 B3 的相关方法）。
// 收窄成接口而非直接依赖 *store.Store：便于 Activity 单测注入替身。
type Store interface {
	// ListDueSourcesByUser 只返回 next_fetch_at <= now() 的到期源（审查 #重复计费）：
	// markFetchResult 成功后推进 next_fetch_at，Activity 超时重试时已抓成功的
	// Exa/TikHub 付费调用自然被跳过，不重复计费。
	ListDueSourcesByUser(ctx context.Context, userID int64) ([]types.Source, error)
	// UpsertContentItem 按 canonical_key 落内容（全局一份）再登记本次出现的源
	// （content_sources）。取代 InsertContentItemIfNew：改名是因为语义变了——
	// isNew 现在表示**内容本身首次入库**，而非"这个源首次见到"。本包不依赖 isNew
	// （见 Fetch 里的调用点），故无分支受影响。
	UpsertContentItem(ctx context.Context, item *types.ContentItem) (id int64, isNew bool, err error)
	// ListRecentSimhashesByUser 按 user 维度（跨该用户全部订阅源）查 72h simhash 历史
	// （审查 #跨源去重）：Exa 搜索天然与 RSS 源内容重叠，per-source 历史拦不住
	// "昨天 RSS 推过、今天 Exa 又抓到"的跨源重复推送。
	ListRecentSimhashesByUser(ctx context.Context, userID int64, since time.Time, excludeIDs []int64) ([]int64, error)
	// UpdateSourceFetchState / ListUnpushedByUser 在 store 里 M3 就已实现，但此前没进本接口，
	// 于是 UpdateSourceFetchState 从没被 Activity 调用过（抓取状态死代码 #7）；Fetch 重构后启用。
	UpdateSourceFetchState(ctx context.Context, id int64, lastFetched, nextFetch time.Time, failCount int) error
	// DisableSourceIfActive 连续失败达停用阈值时把源置 disabled（仅当前 active 才翻转，
	// 幂等）；disabled=true 表示本次真的翻转，用于只发一次停用告警（功能 5.2）。
	DisableSourceIfActive(ctx context.Context, id int64) (disabled bool, err error)
	ListUnpushedByUser(ctx context.Context, userID int64, limit, perSourceCap int) ([]types.ContentItem, error)
	// 任务手册 P1b b3 —— 按本任务的软范围源隔离抓取与候选（决策 #3「情报任务自包含」）。
	// ScheduleHasSources 是分流开关：有链接才走隔离路径，否则退回用户级（决策 #4 老任务不变）。
	ScheduleHasSources(ctx context.Context, scheduleID string) (bool, error)
	ListDueSourcesBySchedule(ctx context.Context, scheduleID string) ([]types.Source, error)
	ListUnpushedBySchedule(ctx context.Context, scheduleID string, limit, perSourceCap int) ([]types.ContentItem, error)
	// 幂等推送地基（FIX-A 新增实现）：重试时复用同一 batch、跳过已发条目，杜绝重复发卡（#1 CRITICAL）。
	// 取代原 CreatePushBatch / InsertDelivery（store 仍保留其实现，只是 Activity 不再用）。
	CreatePushBatchIdempotent(ctx context.Context, userID int64, idempKey, scheduleID string) (int64, error)
	// RecordEmptyPushBatch 记录"跑完了但没东西可推"的空批次（009）。与上面那条幂等
	// 地基共用 idempotency_key，但**写入路径完全独立**——空批次与真实推送在同一次
	// 运行里互斥，刻意不去动 CreatePushBatchIdempotent 的签名（改它就得改这个窄接口
	// 的全部替身，且那是 #1 CRITICAL 的地基）。
	//
	// skipped=true 表示 store 侧防覆写护栏拦下了本次写入（该 traceID 已有真实批次），
	// 此时 id=0、err=nil——**不是错误，但必须出声**（见 RecordEmptyBatch 的处理）。
	RecordEmptyPushBatch(ctx context.Context, userID int64, idempKey, scheduleID string,
		gate types.BatchExitGate, counts types.PipelineCounts) (id int64, skipped bool, err error)
	InsertDeliveryIdempotent(ctx context.Context, d *types.Delivery) (id int64, existed bool, sentAlready bool, err error)
	UpdatePushBatchStatus(ctx context.Context, batchID int64, status types.BatchStatus) error
	// MarkDeliverySent 发送成功后回填 message_id 与最终卡 JSON（契约 §3.3：
	// 最终卡在拿到 delivery id 后才构造，只能在此落库而非 Insert 时）。
	MarkDeliverySent(ctx context.Context, id int64, feishuMessageID string, cardJSON json.RawMessage, sentAt time.Time) error
	// GetSource 按 id 查信源（卡片改版：subtitle 需要 source.Title 和 Platform）。
	GetSource(ctx context.Context, id int64) (*types.Source, error)
	// ScheduleSourceForContent 取内容在本任务源集里的命中源（content_sources ∩ schedule_sources）。
	// 隔离任务构卡时用它标源，而非全局首发源 content_items.source_id（#8 卡片源归属）。
	ScheduleSourceForContent(ctx context.Context, contentItemID int64, scheduleID string) (int64, bool, error)
	// TenantLiveForUser 报告该用户所属租户是否仍在服务中（D9 软删除的执行面）。
	TenantLiveForUser(ctx context.Context, userID int64) (bool, error)
}

// Activities 持有 6 步 pipeline 的全部依赖，方法即 Temporal Activity。
// 字段未导出，通过 NewActivities 注入——给 cmd/server 一个稳定构造入口，
// 避免各 Activity 直接对 *store.Store 等具体类型硬编码。
type Activities struct {
	fetcher Fetcher
	scorer  Scorer
	cardgen CardGenerator
	pusher  Pusher
	store   Store
	feishu  FeishuManager
	evolver ProfileEvolver // nil = EvolveProfile 退化 no-op（灰度装配开关）
	// buildAggCard 构造聚合推送卡（生产装配传 feishu.BuildAggregateCard）；
	// aggHeader 由任务名派生 header（生产装配传 feishu.AggHeaderForTask）。
	// 函数注入而非 import feishu：feishu→agent→workflow 依赖链已存在，
	// 本包直接依赖 feishu 即成 import 环（契约 §8.2 CRITICAL）。
	// （原单条 buildCard 注入已随聚合卡改版删除：Push 不再逐条构卡，
	// 单条构卡函数只活在 feedback 重建路径——历史卡的原地更新仍用它。）
	buildAggCard func(in feedback.AggregateCardInput) string
	aggHeader    func(task string, n int) (title, template string)
	// buildNotice 构造无按钮的普通通知卡（生产装配传 feishu.BuildReplyCard），
	// 用于抓取失败主动告警（功能 5.2）。同理走注入避开 import 环。刻意与 buildCard
	// 分开：告警卡不带任何反馈按钮/回调，绝不复用 M5 的 delivery 卡结构（5.2 不碰
	// 卡片反馈路径）。nil = 抓取失败告警退化为静默 no-op（灰度/测试装配）。
	buildNotice func(markdown string) string
}

// NewActivities 装配 Activities。前六参顺序与规格 B6"持有 fetcher/scorer/
// cardgen/pusher/store/feishuMgr"一致；ev 可为 nil（演化灰度关闭）；
// buildNotice 可为 nil（抓取失败告警退化为 no-op）。
func NewActivities(f Fetcher, sc Scorer, cg CardGenerator, p Pusher, st Store, fs FeishuManager,
	ev ProfileEvolver, buildNotice func(markdown string) string,
	buildAggCard func(in feedback.AggregateCardInput) string,
	aggHeader func(task string, n int) (title, template string)) *Activities {
	return &Activities{fetcher: f, scorer: sc, cardgen: cg, pusher: p, store: st, feishu: fs,
		evolver: ev, buildNotice: buildNotice,
		buildAggCard: buildAggCard, aggHeader: aggHeader}
}

// ============================================================
// Activity 入参结构：每步用具体 struct 承载（含 UserID/TraceID/上一步结果），
// 避免裸切片跨步传递时语义歧义（规格 B6）。出参直接复用 types.go 的跨包类型。
// ============================================================

// EvolveIn 是 EvolveProfile Activity 的入参。
type EvolveIn struct {
	UserID  int64  `json:"user_id"`
	TraceID string `json:"trace_id"`
}

// DedupIn 是 Dedup Activity 的入参。
type DedupIn struct {
	UserID  int64               `json:"user_id"`
	TraceID string              `json:"trace_id"`
	Items   []types.ContentItem `json:"items"`
}

// ScoreIn 是 Score Activity 的入参。
type ScoreIn struct {
	UserID  int64               `json:"user_id"`
	TraceID string              `json:"trace_id"`
	Items   []types.ContentItem `json:"items"`
}

// SelectIn 是 Select Activity 的入参。
type SelectIn struct {
	UserID  int64              `json:"user_id"`
	TraceID string             `json:"trace_id"`
	TopN    int                `json:"top_n"`
	Scored  []types.ScoredItem `json:"scored"`
}

// CardGenIn 是 CardGen Activity 的入参。
type CardGenIn struct {
	UserID  int64              `json:"user_id"`
	TraceID string             `json:"trace_id"`
	Items   []types.ScoredItem `json:"items"`
}

// PushIn 是 Push Activity 的入参。
type PushIn struct {
	UserID     int64           `json:"user_id"`
	ScheduleID string          `json:"schedule_id,omitempty"` // 触发本批的任务 id（P1b），空=即时/老任务
	TraceID    string          `json:"trace_id"`
	Cards      []GeneratedCard `json:"cards"`
	// TaskTitle 任务名（调度的 nl_description），聚合卡 header 用。
	// 空串合法（存量调度/即时推送无任务名），构卡落兜底标题。
	TaskTitle string `json:"task_title,omitempty"`
}

// RecordEmptyIn 是 RecordEmptyBatch Activity 的入参（009 / 契约 §16「空批次缺口」）。
type RecordEmptyIn struct {
	UserID     int64  `json:"user_id"`
	ScheduleID string `json:"schedule_id,omitempty"` // 触发本批的任务 id（P1b），空=即时/老任务
	TraceID    string `json:"trace_id"`              // = 幂等键，与 Push 建批次用的是同一个
	// Gate 从哪个闸门退出。恒非空——五个调用点各自传死值（workflow.go）。
	Gate types.BatchExitGate `json:"gate"`
	// Counts 截至退出时刻的漏斗快照；未跑到的阶段字段为 nil（见 types.PipelineCounts）。
	Counts types.PipelineCounts `json:"counts"`
}

// ============================================================
// 6 个 Activity。约定：单条失败（单源抓取失败 / 单条打分失败）不阻断整批，
// 只 warn 并跳过；只有"整批全军覆没"才返回错误触发 Temporal 重试。
// ============================================================

// isQuotaErr 判定一个错误是否为额度用尽。
// 用 AppError 的 Code 而非字符串匹配：文案会改，错误码不会。
func isQuotaErr(err error) bool {
	var ae *types.AppError
	return errors.As(err, &ae) && ae.Code == types.CodeQuotaExceeded
}

// refuseIfTenantGone 是 D9 软删除的**执行面**：注销若不落到这一层，
// 「已注销」就只是数据库里的一个标记——定时调度照跑、Exa/TikHub/LLM 照花钱、
// 推送照发到对方手机上。落库只是记账，这里才是真的停下来。
//
// 装在**每个开始花钱的活动**开头（EvolveProfile 花 LLM、Fetch 花 Exa/TikHub），
// 而不是只装在 workflow 入口：新增 workflow 步骤会改变 Temporal 的执行历史，
// 在途 workflow 重放时解不出新命令（契约 §8.2）；装进既有活动则零重放风险。
//
// 查询失败时**放行**而不是拦截：这是一道旁路闸门，数据库抖一下就让全部用户停推
// 是更坏的失败方向。真正的兜底在登录层（LookupSession 拒绝已注销租户的会话）。
func (a *Activities) refuseIfTenantGone(ctx context.Context, userID int64, stage string) bool {
	live, err := a.store.TenantLiveForUser(ctx, userID)
	if err != nil {
		slog.Warn("租户服务状态查询失败，本次放行（旁路闸门不阻塞主流程）",
			"user_id", userID, "stage", stage, "err", err)
		return false
	}
	if !live {
		slog.Info("租户已注销，跳过本步骤——不为已注销租户花钱",
			"user_id", userID, "stage", stage)
		return true
	}
	return false
}

// EvolveProfile 画像演化前置步（契约 §8.1）。evolver 未注入时 no-op：统编阶段
// 可先不传 evolver 灰度上线，pipeline 行为与 M4 完全一致。错误原样上抛交给
// RetryPolicy；重试耗尽后由 workflow 侧吞掉——演化失败永不阻断推送（红线）。
func (a *Activities) EvolveProfile(ctx context.Context, in EvolveIn) error {
	if a.evolver == nil {
		return nil
	}
	if a.refuseIfTenantGone(ctx, in.UserID, "evolve_profile") {
		return nil // 正常终态，不报错——报错会触发重试，而重试同样会被拒。
	}
	return a.evolver.Evolve(ctx, in.UserID, in.TraceID)
}

// Fetch 现查用户的 active 订阅源，逐源抓取入库并推进各源抓取状态，最后返回
// 该用户"未投递候选"（带条数上限）。TraceID 由 workflow 生成后经后续 Activity
// 入参下传，Fetch 本身无 LLM 调用故不需要。
//
// 相比初版有三处关键修复：
//   - #3 重试丢内容：原来只返回本次 isNew=true 的条目，一旦 Fetch 因重试/中断
//     没走完 pipeline，已入库但未投递的内容就永久丢失（下次 isNew=false 不再返回）。
//     改为返回 ListUnpushedByUser（DB 事实来源），重试可续、不丢内容。
//   - #7 抓取状态死代码：每源抓取后调 markFetchResult 真正推进 fail_count / 时间戳。
//   - #6 成本护栏：返回时用 maxScoreCandidates 截断候选，避免积压内容一次性打爆 LLM。
func (a *Activities) Fetch(ctx context.Context, p PushParams) ([]types.ContentItem, error) {
	// D9：已注销租户不抓取——Exa/TikHub 按次计费，这里是花钱的起点。
	// 返回空候选而非报错：空候选走 workflow 既有的空批次早退路径（正常终态、不推送），
	// 报错则会重试三次、每次都被同样拒绝，白白制造噪音。
	if a.refuseIfTenantGone(ctx, p.UserID, "fetch") {
		return nil, nil
	}
	// 任务手册 P1b b3 的分流开关：本次触发绑定的定时任务若已编译出源（schedule_sources 非空），
	// 则「只按本任务的源抓/挑」（决策 #3 自包含）；否则退回用户级（决策 #4 老任务不变，push_now
	// 的 ScheduleID 为空也走这里）。生产里没建过带源手册的任务时 planScoped 恒 false，b3 休眠。
	//
	// 用"有无 schedule_sources 链接"当开关（而非"有无 playbook"）是刻意的：所有 create_schedule
	// 任务都带 P0 空 playbook，若以 playbook 分流，存量任务会全部转隔离、其空计划→抓 0 源→生产
	// 推送全断。故只有真编译出源的任务才隔离。副作用（已知、可接受）：一个原本有源的 plan 任务
	// 若手册被改成零源（schedule_sources 被清空），会退回用户级抓全部订阅，而非"什么都不抓"——
	// 这是安全默认（不因清空手册而静默停推），代价是"清空手册的取材范围"语义偏softly。
	//
	// b3 只隔离**抓取 + 候选选材**两层；**去重（Dedup）仍是用户级**（ListRecentSimhashesByUser
	// 按用户全部订阅源的 72h simhash 历史）——plan 内容会与用户订阅内容跨任务近似去重。这是本轮
	// 刻意留的 scope 边界（完整"自包含"需按 schedule 隔离 simhash 历史，留后续）；打分/出卡的
	// 按任务注入是 P1c。
	planScoped := false
	if p.ScheduleID != "" {
		has, herr := a.store.ScheduleHasSources(ctx, p.ScheduleID)
		if herr != nil {
			return nil, herr
		}
		planScoped = has
	}

	// 只抓到期源（next_fetch_at <= now()）：重试不重复计费，详见接口注释。
	// 未到期源被跳过不影响推送——其已入库内容仍由下方候选查询捞出。
	var sources []types.Source
	var err error
	if planScoped {
		sources, err = a.store.ListDueSourcesBySchedule(ctx, p.ScheduleID) // 只抓本任务的源
	} else {
		sources, err = a.store.ListDueSourcesByUser(ctx, p.UserID)
	}
	if err != nil {
		return nil, err
	}
	if len(p.Scope.SourceIDs) > 0 {
		sources = filterSources(sources, p.Scope.SourceIDs)
	}

	// 绑定引擎记账的 trace 锚点（endpoint-binding-contract.md §5）：用 workflow
	// execution ID 而非管线 traceID——后者在 PushParams 里没有，为它改活动入参
	// 会碰在途 run 的确定性；执行 ID 同样稳定可查（wf-push-… 关联到调度与批次）。
	// 在真实 activity 外（单测直调）GetInfo 会 panic，故判一下。
	if activity.IsActivity(ctx) {
		ctx = fetcher.WithBindingTrace(ctx, activity.GetInfo(ctx).WorkflowExecution.ID)
	}

	// 逐源"抓取→立刻入库"的顺序是**有成本含义**的，别改成"先抓完所有源再统一入库"：
	// TikHub 详情补全的付费闸门查的是库里已补全的 canonical_key（fetcher.SeenChecker）。
	// 同一 run 内源 A 补全的笔记当场入库，源 B 抓到同一篇时闸门才命中、跳过付费；
	// 若把入库推迟到循环之后，源 B 查库时 A 的内容还没落地，同一篇笔记会被重复付费。
	var alertable []fetchFailure // 本轮"恰好"连续失败达告警阈值的源，循环后批量告警一次（功能 5.2）。
	var disabled []fetchFailure  // 本轮达停用阈值、刚被自动停用的源，循环后单发一张停用卡（功能 5.2）。
	for _, src := range sources {
		items, ferr := a.fetcher.Fetch(ctx, src)
		if ferr != nil {
			// 单源失败不拖垮整批：某个源挂了不该让当次推送整体失败；
			// 同时自增 fail_count 并推进 next_fetch_at，避免调度紧循环重试。
			crossed, justDisabled := a.markFetchResult(ctx, src, false)
			slog.Warn("fetch: 单源抓取失败，跳过", "source_id", src.ID, "platform", src.Platform, "capability", src.Capability, "url", src.URL, "err", ferr)
			// src.FailCount 此刻仍是旧值，新计数=src.FailCount+1。
			f := fetchFailure{src: src, failCount: src.FailCount + 1, reason: fetchFailureReason(ferr)}
			if crossed {
				alertable = append(alertable, f) // 恰好达告警阈值：预警卡。
			}
			if justDisabled {
				disabled = append(disabled, f) // 刚被自动停用：停用卡。
			}
			continue
		}
		for i := range items {
			// isNew 刻意丢弃：007 起它的语义是"内容本身首次入库"（canonical_key
			// 全局唯一）而非"这个源首次见到"，跨源命中同一篇时为 false。本活动
			// 不据此分支——候选一律由下方 ListUnpushedByUser 从 DB 事实重新捞
			// （修 #3 时就已改成这样），所以语义变化对这里无影响：跨源重复的
			// 第二份不新建内容行，但 UpsertContentItem 会登记 content_sources，
			// 用户仍能经自己订阅的源关联到那唯一一份。
			if _, _, ierr := a.store.UpsertContentItem(ctx, &items[i]); ierr != nil {
				// 单条入库失败只 warn：已入库的其它条目仍会被后面的 ListUnpushedByUser 捞出。
				slog.Warn("fetch: 内容入库失败，跳过", "source_id", src.ID, "err", ierr)
			}
		}
		a.markFetchResult(ctx, src, true) // 成功：清零 fail_count、推进 last/next_fetch_at。
	}

	// 本轮有源恰好连续失败达阈值 → 给 owner 发一张汇总告警卡（功能 5.2）。
	// 放在返回候选之前、与推送早退无关：即便本轮全源挂掉（下方候选为空、workflow
	// 走空批次早退），告警也已在此发出。best-effort，失败只 warn 不拖垮推送。
	a.alertFetchFailures(ctx, alertable)
	// 本轮有源达停用阈值被自动停用 → 单发一张"已暂停 + 如何重新启用"卡（功能 5.2）。
	a.alertSourcesDisabled(ctx, disabled)

	// 返回"未投递候选"而非"本次新入库"，让 Fetch 重试幂等可续（修 #3）；
	// 全局上限 + 每源配额双重截断，控制单批打分规模且防高产源饿死低产源（修 #6）。
	// planScoped：只在本任务的源见过、且本任务未投过的内容里挑（决策 #3 互不干扰，P1b b3）。
	if planScoped {
		return a.store.ListUnpushedBySchedule(ctx, p.ScheduleID, maxScoreCandidates, maxPerSourceCandidates)
	}
	return a.store.ListUnpushedByUser(ctx, p.UserID, maxScoreCandidates, maxPerSourceCandidates)
}

// markFetchResult 抓取一个源后推进其抓取状态，消除 UpdateSourceFetchState 从不被调用的死代码（#7）。
//   - ok=true：清零 fail_count，last_fetched_at=now，next_fetch_at=now+interval（正常节奏）。
//   - ok=false：fail_count 自增，保留上次 last_fetched_at（本次没抓成不算"抓过"），
//     next_fetch_at 仍推进一个 interval——否则调度下一 tick 会立刻重试，形成紧循环。
//
// 返回 crossedAlertThreshold=本轮 fail_count 恰好跨过告警阈值（功能 5.2）：仅失败分支
// 且 failCount==alertFetchFailThreshold 且状态成功落库时为 true——调用方据此发一次告警。
// gate 在落库成功之上：若 UpdateSourceFetchState 失败，fail_count 没推进，下轮会再次算到
// 阈值，此时告警会错乱重复，故落库失败一律不告警（保持"恰好一次"不变量）。
//
// 两级阈值（Boss 拍板「告警后再宽限」）：
//   - failCount==alertFetchFailThreshold(3) → crossedAlertThreshold：发一次预警卡（人工窗口）。
//   - failCount>=disableFetchFailThreshold(10) 且当前仍 active → justDisabled：自动停用 + 发停用卡。
//
// 两个返回值互斥地驱动两种告警卡（3 与 10 不重叠）。停用经 DisableSourceIfActive 幂等完成，
// justDisabled 仅在"这一刻从 active 翻成 disabled"时为 true，据此只发一次停用告警。
func (a *Activities) markFetchResult(ctx context.Context, src types.Source, ok bool) (crossedAlertThreshold, justDisabled bool) {
	now := time.Now()
	nextFetch := now.Add(time.Duration(src.FetchIntervalSeconds) * time.Second)

	var lastFetched time.Time
	failCount := 0
	if ok {
		lastFetched = now
	} else {
		failCount = src.FailCount + 1
		if src.LastFetchedAt != nil {
			lastFetched = *src.LastFetchedAt // 保留上次成功抓取时间（从未抓过则为零值）。
		}
		crossedAlertThreshold = failCount == alertFetchFailThreshold
	}
	if err := a.store.UpdateSourceFetchState(ctx, src.ID, lastFetched, nextFetch, failCount); err != nil {
		slog.Warn("fetch: 更新抓取状态失败", "source_id", src.ID, "err", err)
		return false, false // 状态未落库：不告警、不停用，避免下轮重复算到阈值再报。
	}
	// 达停用阈值：自动停用（幂等，只翻转仍 active 的源）。停用失败不影响抓取，下轮会再试。
	if !ok && failCount >= disableFetchFailThreshold {
		disabled, derr := a.store.DisableSourceIfActive(ctx, src.ID)
		if derr != nil {
			slog.Warn("fetch: 自动停用失败（不影响抓取）", "source_id", src.ID, "err", derr)
		} else {
			justDisabled = disabled
		}
	}
	return crossedAlertThreshold, justDisabled
}

// fetchFailure 承载一个"连续失败恰好达告警阈值"的信源及其对 owner 可见的失败原因（功能 5.2）。
type fetchFailure struct {
	src       types.Source
	failCount int    // 本轮跨阈后的连续失败次数（= src.FailCount+1，等于阈值）。
	reason    string // 面向 owner 的可读原因（已按红线3 从 AppError.Message 提取）。
}

// fetchFailureReason 从抓取错误里提取面向 owner 的可读原因（红线3：不外泄裸错误链）。
// 取 AppError.Message（各 fetcher 在自己的转换点写的人读描述），无 AppError 时给中性兜底。
func fetchFailureReason(err error) string {
	var ae *types.AppError
	if errors.As(err, &ae) && ae.Message != "" {
		return ae.Message
	}
	return "抓取失败（未知原因）"
}

// alertFetchFailures 给 owner 发一张汇总告警卡，列出本轮连续失败达阈值的信源（功能 5.2）。
// best-effort：无失败源 / 未注入构卡器 / 未捕获 owner / 发送失败，一律静默或只 warn，
// 绝不让告警把抓取或推送管道拖挂（与 EvolveProfile warn-only、"不制造假失败告警"同一红线）。
// 走 pusher.Push（= feishu SendCard 主动新消息）+ 注入的 buildNotice（= BuildReplyCard，
// 无按钮卡），与 M5 卡片反馈路径完全隔离。
func (a *Activities) alertFetchFailures(ctx context.Context, failures []fetchFailure) {
	if len(failures) == 0 {
		return
	}
	if a.buildNotice == nil {
		return // 未注入告警卡构造器（灰度/测试）：静默 no-op。
	}
	owner := a.feishu.OwnerOpenID()
	if owner == "" {
		return // 尚未捕获 owner：无收件人，静默跳过（同 Push 对无 owner 的处理）。
	}
	card := a.buildNotice(renderFetchFailureAlert(failures))
	if _, err := a.pusher.Push(ctx, owner, card); err != nil {
		slog.Warn("fetch: 抓取失败告警发送失败（不影响推送）", "count", len(failures), "err", err)
	}
}

// renderFetchFailureAlert 把失败源列表渲染成告警卡的 markdown 正文（功能 5.2）。
func renderFetchFailureAlert(failures []fetchFailure) string {
	var b strings.Builder
	fmt.Fprintf(&b, "**⚠️ 信源抓取失败告警**\n\n以下信源已连续失败达 %d 次，可能已失效，建议检查：\n", alertFetchFailThreshold)
	for _, f := range failures {
		title := f.src.Title
		if title == "" {
			title = f.src.URL // 没标题的源（如裸 RSS）退回用 URL 指代。
		}
		fmt.Fprintf(&b, "\n**%s**（%s / %s）· 连续失败 %d 次\n原因：%s\n",
			title, f.src.Platform, f.src.Capability, f.failCount, f.reason)
		if f.src.URL != "" {
			fmt.Fprintf(&b, "链接：%s\n", f.src.URL)
		}
	}
	b.WriteString("\n修复来源后会在下次抓取自动恢复；持续失败不影响其它信源正常推送。")
	return b.String()
}

// alertSourcesDisabled 给 owner 发一张"已自动暂停"卡，列出本轮达停用阈值被停用的信源，
// 并告知如何重新启用（功能 5.2）。与 alertFetchFailures 同为 best-effort：无失败源 /
// 未注入构卡器 / 未捕获 owner / 发送失败一律静默或只 warn，绝不拖挂抓取或推送管道。
func (a *Activities) alertSourcesDisabled(ctx context.Context, disabled []fetchFailure) {
	if len(disabled) == 0 {
		return
	}
	if a.buildNotice == nil {
		return
	}
	owner := a.feishu.OwnerOpenID()
	if owner == "" {
		return
	}
	card := a.buildNotice(renderSourcesDisabledAlert(disabled))
	if _, err := a.pusher.Push(ctx, owner, card); err != nil {
		slog.Warn("fetch: 信源停用告警发送失败（不影响推送）", "count", len(disabled), "err", err)
	}
}

// renderSourcesDisabledAlert 渲染"已暂停"卡正文：与预警卡措辞不同——预警是"建议检查"，
// 本卡是"已停止抓取，需你重新启用"，并给出两条恢复路径（信源页按钮 / 对 AI 说重新启用）。
func renderSourcesDisabledAlert(disabled []fetchFailure) string {
	var b strings.Builder
	fmt.Fprintf(&b, "**⛔ 信源已自动暂停**\n\n以下信源连续失败达 %d 次，已判定失效并停止抓取：\n", disableFetchFailThreshold)
	for _, f := range disabled {
		title := f.src.Title
		if title == "" {
			title = f.src.URL
		}
		fmt.Fprintf(&b, "\n**%s**（%s / %s）· 连续失败 %d 次\n原因：%s\n",
			title, f.src.Platform, f.src.Capability, f.failCount, f.reason)
		if f.src.URL != "" {
			fmt.Fprintf(&b, "链接：%s\n", f.src.URL)
		}
	}
	b.WriteString("\n修复来源后可重新启用：在「信源管理」页点该源的『重新启用』，或直接对我说「重新启用信源 <id>」。启用后失败计数清零、立即恢复抓取。")
	return b.String()
}

// Dedup 做近似去重：精确去重（canonical_key）已在 Fetch 的 UpsertContentItem
// 完成，本步用 simhash + 72h 窗口过滤"改标题/转载"式近重复。跨批用 store 里的
// 历史 simhash（user 维度、跨全部订阅源——审查 #跨源去重：Exa 搜索天然与 RSS 源
// 内容重叠，per-source 历史拦不住"昨天 RSS 推过、今天 Exa 又抓到"的跨源同稿），
// 批内用累积集合——两者合并比对，避免同批内互为近重复的漏网。
//
// 关键修复（自撞）：Fetch 已在抓取时把 simhash 写入 content_items，本批内容自身也在
// content_items 里。若查历史不排除本批 ID，每条内容都会查到自己刚入库的 simhash 而被
// 判近重复，导致整批全删、pipeline "去重后无内容" 早退、永远推不出卡片。故先收集本批
// 全部 ID，传给 ListRecentSimhashesByUser 排除——"历史"只含本批之外的内容。
func (a *Activities) Dedup(ctx context.Context, in DedupIn) ([]types.ContentItem, error) {
	since := time.Now().Add(-simhashWindow)

	// 本批内容自身的 ID 集合，查历史时排除（避免每条与自己刚入库的 simhash 相撞）。
	batchIDs := make([]int64, 0, len(in.Items))
	for _, item := range in.Items {
		if item.ID != 0 {
			batchIDs = append(batchIDs, item.ID)
		}
	}

	// 用户级历史一次取齐（跨该用户全部订阅源的 72h simhash），替代原 per-source
	// 逐源查询——既堵住跨源跨批重复推送，也把 N 源 N 次查询合并为 1 次。
	hist, err := a.store.ListRecentSimhashesByUser(ctx, in.UserID, since, batchIDs)
	if err != nil {
		return nil, err
	}

	// 批内已保留项的 simhash 用单一全局切片（跨源合并比对）：多源转载同一篇稿时，
	// 后到的转载能与批内已保留的同稿命中近重复而被拦下。
	var batchSeen []int64

	kept := make([]types.ContentItem, 0, len(in.Items))
	for _, item := range in.Items {
		sh := dedup.Simhash(item.Title + " " + item.Content)

		// KindPageContent（web/contents 页面监控）豁免近似去重：同一 URL 的相邻版本
		// 正文几乎相同（定价页只几个价格数字变），simhash 距离必 ≤ simhashThreshold，
		// 无条件近似去重会把"变化"当重复吞掉——这正是 page_watch 当年的事故（契约 §1.1）。
		// 精确去重由 canonical_key（contents://url#textHash）的 UNIQUE 承担：内容没变
		// 撞键、在 UpsertContentItem 就被去重，根本进不到本批；能到这里的都是真的新版本。
		// 仍回填 simhash（Push 建 Delivery 要用）；但**不进 batchSeen**——一个页面的
		// 版本不该成为别人的近重复判据。
		if item.Kind == types.KindPageContent {
			s := sh
			item.Simhash = &s
			kept = append(kept, item)
			continue
		}

		// 候选集 = 用户级历史 simhash ∪ 全局批内已保留 simhash。
		candidates := append(append([]int64{}, hist...), batchSeen...)
		if dedup.IsNearDup(sh, candidates, simhashThreshold) {
			continue
		}

		s := sh
		item.Simhash = &s // 回填 simhash，Push 建 Delivery 时随内容一并可用
		kept = append(kept, item)
		batchSeen = append(batchSeen, sh)
	}
	return kept, nil
}

// parBatchFanout 是逐条 LLM 活动（Score / CardGen）**同时在飞的条数**上限。
//
// 取值必须 ≤ `llm.max_concurrent`（默认 5），原因不是节流，而是**排队公平性**：
// llm.Client 的信号量是全进程单例，Complete（打分/出卡）与 Chat（飞书对话）共用同一个
// `c.sem`（llm/client.go:138 与 llm/chat.go:148）。Go 的 channel sendq 是 FIFO——
// 扇出超过信号量容量时，多出来的 goroutine 不是"闲着等"，而是**占住队头**：
// 此后到达的交互请求要排在它们全部之后。而超出部分对吞吐**一点贡献都没有**
// （吞吐本就被信号量卡在 5），纯粹是拿交互延迟换零收益。原值 16 正是这个错误。
//
// 也就是说这里刻意与 llm.max_concurrent 取同值（2026-07-18 起两者同为 32，
// 依据是真实 API 受控实验：45 条批次并发 5 → 5.7 秒、并发 32 → 1.25 秒，零 429；
// v4-flash 官方并发限额 2500，上限侧余量极大）。不做成配置项读取是因为 NewActivities
// 已有 10 个参数、32 处调用点，为此加参数不划算；代价是调 llm.max_concurrent 时
// 必须记得把这里一并改——两处注释互相指认，改一处漏另一处会在 review 时露出来。
//
// **它不限制 goroutine 数量**。每条输入无条件起一个 goroutine，数量等于批次条数；
// 真正的上限是上游的 maxScoreCandidates（50）。早先注释声称本常量"防止凭空拉起几千个
// goroutine"是错的——审查指出后更正。
const parBatchFanout = 32

// mapConcurrent 并发映射 in，返回成功项，**保持输入顺序**；单项失败调 onErr 后跳过。
//
// 顺序保持不是可有可无的装饰：结果写进按下标预留的槽位、最后按序压紧，于是同一批输入
// 无论 goroutine 怎么交错，产出都与串行版逐字节相同——这让并发化成为**可证明的等价改写**，
// 而不是"大概也一样"。（RankTopN 的同分裁决本身落在 Item.ID 上、与顺序无关，
// 所以即便乱序也选得出同一批；但那是下游的性质，不该被上游拿来当免责理由。）
//
// 各 goroutine 只写自己下标的槽位，互不重叠；wg.Wait() 建立 happens-before 边，
// 读取时无需再加锁。onErr 用锁串起来——slog 本身并发安全，但 onErr 是调用方传进来的，
// 不能替它假设。
func mapConcurrent[T, R any](
	ctx context.Context,
	in []T,
	fanout int,
	fn func(context.Context, T) (R, error),
	onErr func(T, error),
) []R {
	// 非 nil 空切片：与串行版的 make([]R, 0, n) 对齐。Temporal 会把结果序列化成
	// JSON 交给下一个活动，nil 编成 null、空切片编成 []——这个差别会穿过进程边界。
	out := make([]R, 0, len(in))
	if len(in) == 0 {
		return out
	}
	if fanout < 1 {
		fanout = 1
	}

	type slot struct {
		val R
		ok  bool
	}
	slots := make([]slot, len(in))
	if fanout > len(in) {
		fanout = len(in)
	}

	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()

	var panicOnce sync.Once
	var panicVal any
	var panicStack []byte
	var errMu sync.Mutex

	// worker pool 而非"每条一个 goroutine + 信号量"。后者写起来更短，但有两处真问题：
	//   1. goroutine 数等于批次条数，扇出常量根本不限制它（真正的上限在上游
	//      maxScoreCandidates=50）——早先注释把这份功劳记错了地方，审查指出后更正。
	//   2. **panic 后取消不可靠**。一次性建好的 goroutine 抢令牌的顺序由调度器决定、
	//      与输入顺序无关；panic 的那条若恰好被排到最后才跑，前面全部早已发完请求。
	//      实测 45 条批次第 3 条 panic：12 次运行里 7 次省到 10 次调用，但 2 次是满打满算
	//      45 次、另有 40/35/30 各一次——省不省全看调度器心情。
	// worker pool 按输入顺序派发，panic 后派发立即停止，省下的量不再依赖运气。
	idx := make(chan int)
	var wg sync.WaitGroup
	for w := 0; w < fanout; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// panic 必须捕获后在 Wait 之后原样重抛，**不能就地让它飞**：goroutine 里
			// 未捕获的 panic 会直接终止整个进程，而串行版里 fn 的 panic 沿 activity
			// 自己的栈上抛、由 Temporal SDK 接住转成可重试的 activity 错误。
			// 同一个空指针，串行下只是这批推送重试，不捕获就会把 vane 打挂由 systemd 重拉。
			// cancelRun 让派发停下——只捕获不取消的话，其余条目仍会全部发出真实计费请求
			// （审查实测：45 条批次第 3 条 panic，串行 3 次调用、只捕获重抛的并发版 45 次，
			// 再乘 Temporal 的 3 次重试 = 135 次计费调用与 135 行 llm_calls）。
			defer func() {
				if r := recover(); r != nil {
					stack := debug.Stack()
					panicOnce.Do(func() { panicVal, panicStack = r, stack })
					cancelRun()
				}
			}()
			for i := range idx {
				v, err := fn(runCtx, in[i])
				if err != nil {
					errMu.Lock()
					onErr(in[i], err)
					errMu.Unlock()
					continue
				}
				slots[i] = slot{val: v, ok: true}
			}
		}()
	}

	// 按输入顺序派发；一旦取消就停止派发，**剩余条目从不被触及**——
	// 与串行版"第 k 条 panic 后 k+1..n 根本不会被碰"对齐。
feed:
	for i := range in {
		// 先查一次再进 select。只靠 select 也**碰巧**能工作——刚 spawn 的 worker 往往
		// 还没跑到 range idx，发送 case 未就绪，于是 Done 分支必胜（本机 15/15 如此）。
		// 但那是调度时序的运气，不是保证：worker 起得快一点，select 两个 case 同时就绪时
		// 是随机挑的，取消后就会漏派发若干条，每条都是一次真实计费请求。
		// 一次比较换掉这份运气依赖。
		if runCtx.Err() != nil {
			break feed
		}
		select {
		case idx <- i:
		case <-runCtx.Done():
			break feed
		}
	}
	close(idx)
	wg.Wait()

	// 重抛带上原始栈：直接 panic(panicVal) 会把现场换成这一行，
	// 排查时只看得见"某个 goroutine 挂了"，看不见挂在哪。
	if panicVal != nil {
		panic(fmt.Sprintf("%v\n\n原始 goroutine 栈:\n%s", panicVal, panicStack))
	}

	for _, s := range slots {
		if s.ok {
			out = append(out, s.val)
		}
	}
	return out
}

// Score 逐条打分（并发扇出，见 mapConcurrent）。同一 TraceID 串起整批的 llm_calls
// 便于事后追踪。单条失败跳过；整批全失败（大概率 LLM 不可用）返回错误触发重试。
//
// 改并发的理由是实测：生产批次 33–45 条、单条平均 709ms，串行即 32 秒纯排队等网络，
// 而 llm.Client 早已配好 5 路并发闸门却只被喂进 1 个。顺带也把 activity 的
// StartToCloseTimeout=120s 从"正在变薄的余量"拉回安全区（45 条 × 最坏 1372ms 已达 62 秒）。
func (a *Activities) Score(ctx context.Context, in ScoreIn) ([]types.ScoredItem, error) {
	// 并发扇出里各 goroutine 都可能撞到配额，用原子标记而非普通 bool。
	var quotaHit atomic.Bool
	scored := mapConcurrent(ctx, in.Items, parBatchFanout,
		func(ctx context.Context, item types.ContentItem) (types.ScoredItem, error) {
			s, err := a.scorer.Score(ctx, in.UserID, item, in.TraceID)
			if err != nil {
				return types.ScoredItem{}, err
			}
			return types.ScoredItem{Item: item, Score: s}, nil
		},
		func(item types.ContentItem, err error) {
			if isQuotaErr(err) {
				quotaHit.Store(true)
			}
			slog.Warn("score: 单条打分失败，跳过", "content_item_id", item.ID, "trace_id", in.TraceID, "err", err)
		})
	if len(scored) == 0 && len(in.Items) > 0 {
		// 额度用尽与"LLM 挂了"必须分开报。混在一起的代价很具体：走 LLMUnavailable
		// 会被 Temporal 重试三次（而额度按秒补充，秒级重试必然三次都失败），
		// 且最终用户收到的是「打分后没有达标的」——那是假话，会让人以为是内容质量
		// 问题、跑去改画像或换信源，而真相是额度用完了。
		if quotaHit.Load() {
			// 必须经 nonRetryable 包装：Activity 直接返回裸 *AppError 时，
			// Temporal 取 Go 类型名 "AppError" 作 Type，于是 ① NonRetryableErrorTypes
			// 匹配不到（被白重试三次）② 编排层 isQuotaFailure 也认不出来（当成一般
			// 故障让 workflow 失败，用户收不到任何提示）。包装后 Type 恰为错误码。
			return nil, nonRetryable(types.NewAppError(types.CodeQuotaExceeded,
				"本租户 LLM 额度已用尽，本轮跳过打分", nil))
		}
		return nil, types.NewAppError(types.CodeLLMUnavailable, "整批打分全部失败", nil)
	}
	return scored, nil
}

// Select 取 TopN，复用 selector.RankTopN（显式同分裁决 + 新鲜度衰减排序，
// 契约 §6）。activity 内取 time.Now() 合法：确定性约束只限 workflow 函数体。
func (a *Activities) Select(ctx context.Context, in SelectIn) ([]types.ScoredItem, error) {
	n := in.TopN
	if n <= 0 {
		n = defaultTopN
	}
	return selector.RankTopN(in.Scored, n, time.Now()), nil
}

// CardGen 逐条生成解读正文（并发扇出，见 mapConcurrent）。
// 单条失败跳过；整批全失败返回错误触发重试。
//
// 顺序保持在这里比 Score 更要紧：cards 的顺序**直接决定聚合卡里条目的排列**
// （Push 按本切片顺序拼卡），而 Score 的产出还要过一遍 RankTopN 重排。
// 换句话说，这里若按完成先后收集，同一批内容每次推送的卡面顺序都会不一样。
func (a *Activities) CardGen(ctx context.Context, in CardGenIn) ([]GeneratedCard, error) {
	var quotaHit atomic.Bool
	cards := mapConcurrent(ctx, in.Items, parBatchFanout,
		func(ctx context.Context, si types.ScoredItem) (GeneratedCard, error) {
			body, err := a.cardgen.Generate(ctx, in.UserID, si, in.TraceID)
			if err != nil {
				return GeneratedCard{}, err
			}
			return GeneratedCard{Scored: si, BodyMD: body}, nil
		},
		func(si types.ScoredItem, err error) {
			if isQuotaErr(err) {
				quotaHit.Store(true)
			}
			slog.Warn("cardgen: 单条生成失败，跳过", "content_item_id", si.Item.ID, "trace_id", in.TraceID, "err", err)
		})
	if len(cards) == 0 && len(in.Items) > 0 {
		if quotaHit.Load() {
			return nil, nonRetryable(types.NewAppError(types.CodeQuotaExceeded,
				"本租户 LLM 额度已用尽，本轮跳过出卡", nil))
		}
		return nil, types.NewAppError(types.CodeLLMUnavailable, "整批卡片生成全部失败", nil)
	}
	return cards, nil
}

// RecordEmptyBatch 把一次"无内容可推"的正常终态落进 push_batches（009 /
// 契约 §16 修订记录「空批次缺口」）。
//
// 本活动**没有**任何业务副作用：不发卡、不改内容、不推进任何游标，纯记账。
// 这是它能被 workflow 侧安全吞错的前提——参见 workflow.go 的 recordEmpty，
// 失败只 Warn 不阻断（红线：无内容可推必须仍是正常终态，workflow.go:19）。
//
// 错误交给 retryableOrNot 分流：CodeDatabase 可重试（连接抖动重试就好），
// CodeValidation 不可重试（闸门/幂等键传空是代码 bug，重试只是重复失败，
// 让它立刻失败并在 Warn 里露出来）。
func (a *Activities) RecordEmptyBatch(ctx context.Context, in RecordEmptyIn) error {
	// 返回的 batch_id 刻意丢弃：空批次底下没有 deliveries，没有任何后续写入需要它。
	_, skipped, err := a.store.RecordEmptyPushBatch(ctx, in.UserID, in.TraceID, in.ScheduleID, in.Gate, in.Counts)
	if err != nil {
		return retryableOrNot(err)
	}
	if skipped {
		// 护栏拦下：该 traceID 已有真实批次（done/failed/pending），本次不覆写。
		// 拦对了，所以不是错误——但必须出声。正常路径上这一条永远不会打印
		// （空批次与真实推送在一次运行里互斥），它一旦出现就说明发生了别的事：
		// 有人 reset 了这个 workflow，或者出现了第三个写 push_batches 的路径。
		// 静默跳过会让"记录没写成"变成又一次静默失败——那正是本 PR 要消灭的东西，
		// 在本 PR 自己身上重演一遍就太讽刺了。
		slog.Info("空批次记账被护栏拦下：该 trace 已有真实批次，不覆写",
			"user_id", in.UserID, "trace_id", in.TraceID, "gate", in.Gate)
	}
	return nil
}

// Push 建批次 → 逐条建 Delivery → 主动推送 → 标记已发 → 收尾批次状态。
// 收件人是飞书 owner（M3 单用户）；无 owner 直接失败。单卡推送失败跳过，
// 只要有一张成功就算 done，全失败则 failed 并返回错误。
func (a *Activities) Push(ctx context.Context, in PushIn) error {
	owner := a.feishu.OwnerOpenID()
	if owner == "" {
		// 无 owner 是"还没人给机器人发过消息"，属确定性前置条件缺失，重试只是重复失败——
		// 包成不可重试的 ApplicationError（Type=NOT_FOUND），让 NonRetryableErrorTypes 立即终止（修 #2）。
		return nonRetryable(types.NewAppError(types.CodeNotFound, "尚未捕获飞书 owner，无法推送", nil))
	}

	// 用确定性 traceID 作幂等键：Temporal 重试 Push 时复用同一 batch，不再每次新建批次（修 #1 CRITICAL 地基）。
	batchID, err := a.store.CreatePushBatchIdempotent(ctx, in.UserID, in.TraceID, in.ScheduleID)
	if err != nil {
		return err
	}

	// 聚合卡改版（card-redesign-spec.md 附录 A，2026-07-18）：一批一张聚合卡，
	// 不再一条内容一张卡。deliveries 仍 per-content（数据模型不变），
	// 同批各 delivery 共享同一 feishu_message_id——重建路径靠它找兄弟条目。
	//
	// 幂等语义相应调整：先为全部候选建 delivery（幂等），已 sent 的条目不再进新卡
	// （它们已在上次重试成功发出的那张卡里）；只有存在未发条目时才推一张新聚合卡。
	anySent := false
	type pendingItem struct {
		delID int64
		input feedback.CardInput
	}
	var pending []pendingItem
	for _, card := range in.Cards {
		d := &types.Delivery{
			BatchID: batchID,
			UserID:  in.UserID,
			Score:   card.Scored.Score,
			// 解读正文入 body_md；card_json 此时留空（store 归一为 '{}'）——
			// 最终卡按钮 value 携带 delivery_id，只能在拿到 id 后构造，
			// 由 MarkDeliverySent 回填（契约 §8.2）。
			BodyMD: card.BodyMD,
			Status: types.DeliveryStatusPending,
		}
		if card.Scored.Item.ID != 0 {
			cid := card.Scored.Item.ID
			d.ContentItemID = &cid
		}
		delID, existed, sentAlready, ierr := a.store.InsertDeliveryIdempotent(ctx, d)
		if ierr != nil {
			slog.Warn("push: 建投递记录失败，跳过", "trace_id", in.TraceID, "err", ierr)
			continue
		}
		if existed && sentAlready {
			// 重试时该 (batch, content) 已发过：不进新卡，绝不重复推——幂等核心（#1 CRITICAL）。
			anySent = true
			continue
		}

		ci := feedback.CardInput{
			BodyMD:      card.BodyMD,
			DeliveryID:  delID,
			State:       feedback.CardState{},
			Title:       card.Scored.Item.Title,
			Score:       int(card.Scored.Score),
			URL:         card.Scored.Item.URL,
			PublishedAt: card.Scored.Item.PublishedAt,
		}
		// 源归属（#8）：默认用全局首发源 content_items.source_id；隔离任务（ScheduleID 非空）
		// 改用「本任务通过哪个源看到它」——content_sources ∩ schedule_sources 的命中源，否则会给
		// 隔离任务的卡打上一个该任务根本不含的源名（首发源可能是用户订阅的另一个源）。无交集/查询
		// 失败静默回退首发源（不因源归属这一显示细节而中断推送）。
		displaySourceID := card.Scored.Item.SourceID
		if in.ScheduleID != "" && card.Scored.Item.ID != 0 {
			if tsid, ok, terr := a.store.ScheduleSourceForContent(ctx, card.Scored.Item.ID, in.ScheduleID); terr == nil && ok {
				displaySourceID = tsid
			}
		}
		if displaySourceID != 0 {
			if src, serr := a.store.GetSource(ctx, displaySourceID); serr == nil {
				ci.SourceTitle = src.Title
				ci.Platform = src.Platform
			}
		}
		pending = append(pending, pendingItem{delID: delID, input: ci})
	}

	// 分块发送：每卡条数封顶 + 构卡后字节硬校验（附录 A 吸收自被否方案的两点之一）。
	// 超限拆卡而非静默截断——静默丢条目会让"已打分未送达"的内容永远消失。
	//
	// 拆分用显式 size 内环收敛（初版把对半结果写进 end 再 continue，外层循环顶部
	// 重算 end 会丢弃拆分——超大块死循环；size 单调递减到 1 保证必然终止）。
	failedItems := 0
	buildChunk := func(chunk []pendingItem) string {
		items := make([]feedback.CardInput, len(chunk))
		for i, p := range chunk {
			items[i] = p.input
		}
		var title, tmpl string
		if a.aggHeader != nil {
			title, tmpl = a.aggHeader(in.TaskTitle, len(items))
		}
		return a.buildAggCard(feedback.AggregateCardInput{
			HeaderTitle: title, HeaderTemplate: tmpl, Items: items,
		})
	}
	for start := 0; start < len(pending); {
		size := min(aggMaxItemsPerCard, len(pending)-start)
		cardJSON := buildChunk(pending[start : start+size])
		for len(cardJSON) > aggMaxCardBytes && size > 1 {
			size = max(size/2, 1)
			cardJSON = buildChunk(pending[start : start+size])
		}
		chunk := pending[start : start+size]
		if len(cardJSON) > aggMaxCardBytes {
			slog.Warn("push: 单条内容构卡即超字节上限，硬发（可能被飞书拒）",
				"delivery_id", chunk[0].delID, "bytes", len(cardJSON))
		}

		msgID, perr := a.pusher.Push(ctx, owner, cardJSON)
		if perr != nil {
			slog.Warn("push: 聚合卡推送失败，跳过该块", "trace_id", in.TraceID,
				"items", len(chunk), "err", perr)
			failedItems += len(chunk)
			start += size
			continue
		}
		for _, p := range chunk {
			if merr := a.store.MarkDeliverySent(ctx, p.delID, msgID, json.RawMessage(cardJSON), time.Now()); merr != nil {
				// 已发出但回执标记失败：记录不阻断，靠对账补偿（避免重复推送）。
				slog.Warn("push: 标记已发失败（消息已送达）", "delivery_id", p.delID,
					"feishu_message_id", msgID, "err", merr)
			}
		}
		anySent = true
		start += size
	}

	if !anySent {
		if err := a.store.UpdatePushBatchStatus(ctx, batchID, types.BatchStatusFailed); err != nil {
			return err
		}
		return types.NewAppError(types.CodePushFailed, "本批次全部推送失败", nil)
	}
	// 部分块失败（对抗审查 HIGH）：**不结算 done、返回可重试错误**。批次终态留待重试
	// 收敛——sentAlready 幂等保证重试不重发成功块，只补失败块；若记 done 并吞掉错误，
	// 失败块的条目会永久搁浅 pending 且（ListUnpushedByUser 按 deliveries 任意状态排除）
	// 永不再成为候选，正是上方注释声称要消灭的"已打分未送达永远消失"。
	// 重试耗尽时批次停在 pending——作为可见异常留给探针，而非谎报 done。
	if failedItems > 0 {
		return types.NewAppError(types.CodePushFailed,
			fmt.Sprintf("部分推送失败（%d 条未送达），等待重试补发", failedItems), nil)
	}
	if err := a.store.UpdatePushBatchStatus(ctx, batchID, types.BatchStatusDone); err != nil {
		return err
	}
	return nil
}

// filterSources 只保留 id ∈ want 的信源（PushScope.SourceIDs 非空时用）。
func filterSources(sources []types.Source, want []int64) []types.Source {
	set := make(map[int64]struct{}, len(want))
	for _, id := range want {
		set[id] = struct{}{}
	}
	out := sources[:0:0]
	for _, s := range sources {
		if _, ok := set[s.ID]; ok {
			out = append(out, s)
		}
	}
	return out
}

// NotifyEmptyIn 是 NotifyEmptyResult Activity 的入参。
type NotifyEmptyIn struct {
	UserID  int64                `json:"user_id"`
	TraceID string               `json:"trace_id"`
	Gate    types.BatchExitGate  `json:"gate"`
	Counts  types.PipelineCounts `json:"counts"`
}

// NotifyEmptyResult 给用户发一张"本次没有新内容"的通知卡。
//
// 只服务**用户主动触发**的推送（"现在推"按钮 / agent push_now）——用户明确要了
// 一次推送，管道跑完却一声不吭，空结果和故障在用户侧不可区分（2026-07-18 Boss
// 点了立即推送等不到任何回音，来查"服务器是不是坏了"——服务其实完全正常）。
// 定时任务的空批次保持静默：每天早上收一条"今天没新闻"是噪音不是信息。
// 是否用户触发由 workflow 侧按 workflow ID 前缀判定（见 workflow.go），本活动不判。
//
// best-effort：通知失败只记日志不返回错误——空批次是正常终态（红线），
// 不能让一张通知卡的失败把它变成 workflow 失败。
func (a *Activities) NotifyEmptyResult(ctx context.Context, in NotifyEmptyIn) error {
	if a.buildNotice == nil {
		return nil // 灰度/测试装配未注入通知构卡：静默跳过。
	}
	owner := a.feishu.OwnerOpenID()
	if owner == "" {
		return nil
	}
	md := emptyResultMarkdown(in.Gate, in.Counts)
	if _, err := a.pusher.Push(ctx, owner, a.buildNotice(md)); err != nil {
		slog.Warn("push: 空结果通知发送失败（不阻断）", "trace_id", in.TraceID, "err", err)
	}
	return nil
}

// emptyResultMarkdown 按退出闸门生成人话说明——"为什么这次没有卡片"。
// 文案与 exit_gate 语义一一对应（enums.go BatchExitGate）。
func emptyResultMarkdown(gate types.BatchExitGate, c types.PipelineCounts) string {
	nz := func(p *int) int {
		if p == nil {
			return 0
		}
		return *p
	}
	switch gate {
	case types.BatchExitGateFetch:
		return "📭 本次推送没有新内容：所有信源都没有产出新条目。有新内容时定时任务会照常送达。"
	case types.BatchExitGateDedup:
		return fmt.Sprintf("📭 本次推送没有新内容：抓到 %d 条，但都已推送过（去重后 0 条）。有新内容时定时任务会照常送达。", nz(c.Fetched))
	case types.BatchExitGateScore:
		return fmt.Sprintf("📭 本次推送没有新内容：%d 条候选打分后没有达标的。", nz(c.Deduped))
	case types.BatchExitGateSelect:
		return fmt.Sprintf("📭 本次推送没有新内容：%d 条打分内容择优后没有入选的。", nz(c.Scored))
	case types.BatchExitGateQuota:
		// 刻意不说"没有新内容"——内容很可能是有的，只是没额度去处理它。
		// 说成"没内容"会让人去改画像、换信源，白折腾一圈还找不到原因。
		return "⏳ 本次推送暂停：本租户的 AI 额度已用尽。额度会随时间自动恢复，" +
			"恢复后定时任务会照常送达；如需更高额度请联系管理员。"
	case types.BatchExitGateCardGen:
		return "📭 本次推送没有新内容：卡片生成后无可推条目。"
	default:
		return "📭 本次推送没有新内容。"
	}
}
