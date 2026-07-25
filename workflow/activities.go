package workflow

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/sdk/activity"

	cardgenpkg "github.com/YouToco/vane/cardgen"
	"github.com/YouToco/vane/dedup"
	"github.com/YouToco/vane/eventqualifier"
	evolverpkg "github.com/YouToco/vane/evolver"
	"github.com/YouToco/vane/feedback"
	"github.com/YouToco/vane/fetcher"
	"github.com/YouToco/vane/llm"
	"github.com/YouToco/vane/observation"
	"github.com/YouToco/vane/promptguard"
	"github.com/YouToco/vane/pusheffect"
	"github.com/YouToco/vane/runcontext"
	"github.com/YouToco/vane/runtimepolicy"
	scorerpkg "github.com/YouToco/vane/scorer"
	"github.com/YouToco/vane/selector"
	storepkg "github.com/YouToco/vane/store"
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

const (
	pushEffectLeaseDuration   = time.Minute
	pushEffectRetryAfter      = 30 * time.Second
	pushEffectProvider        = "feishu-im-message-create"
	pushEffectStepID          = "push-card"
	pushEffectMarkerWidthSeed = "00000000-0000-5000-8000-000000000000"
)

var pushEffectUUIDNamespace = uuid.MustParse(
	"9a5ffb09-a6ca-51c1-b7c8-9f6e804f69ad",
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

type compiledFetcherV1 interface {
	ValidateRuntimeFetchRouteV1(
		runtimepolicy.CapabilityV1,
		types.Source,
	) error
	FetchWithPolicyV1(
		context.Context,
		types.Source,
		runtimepolicy.CapabilityV1,
		func(context.Context) error,
	) ([]types.ContentItem, error)
}

// Scorer 给单条内容打分（scorer.Score，0-100）。traceID 贯穿 llm 记账。
type Scorer interface {
	Score(ctx context.Context, userID int64, item types.ContentItem, traceID, taskInstruction string) (float64, error)
}

type compiledScorerV1 interface {
	ScoreWithPolicyV1(context.Context, int64, int64, types.ContentItem, string, string, scorerpkg.PolicyV1, func(context.Context, float64) error) (float64, error)
}

// CardGenerator 为单条打分内容生成解读正文 markdown（cardgen.Generate 的
// bodyMD，含阅读原文行）。最终卡片 JSON 由 Push 内经注入的 buildCard 构造——
// 按钮 value 携带 delivery_id，只能在拿到 id 后生成（契约 §8.2）。
type CardGenerator interface {
	Generate(ctx context.Context, userID int64, item types.ScoredItem, traceID, taskInstruction string) (string, error)
}

type compiledCardGeneratorV1 interface {
	GenerateWithPolicyV1(context.Context, int64, int64, types.ScoredItem, string, string, cardgenpkg.PolicyV1, func(context.Context, float64) error) (string, error)
}

// ProfileEvolver 画像演化器（生产实现 evolver.Evolver）：推送前批量消费该用户
// 的新反馈、演化画像 summary/tags（Boss 拍板①：演化随推送批量，非反馈即时）。
type ProfileEvolver interface {
	Evolve(ctx context.Context, userID int64, traceID string) error
}

type compiledProfileEvolverV1 interface {
	EvolveWithPolicyV1(context.Context, int64, int64, string, evolverpkg.PolicyV1, func(context.Context, float64) error, evolverpkg.CompiledProfileWritesV1) error
}

// Pusher 主动推送一张卡片给指定 open_id（pusher.Push），返回飞书消息 ID。
type Pusher interface {
	Push(ctx context.Context, ownerOpenID, cardJSON string) (string, error)
	PushWithUUID(
		context.Context,
		string,
		string,
		string,
	) (pusheffect.ProviderObservation, error)
}

// FeishuManager 暴露 owner（推送收件人）。SendCard 由 Pusher 内部承担，
// 本接口只取 OwnerOpenID——Push Activity 需要知道"推给谁"。
type FeishuManager interface {
	OwnerOpenID() string
	OwnerChatID() string
	AppIdentity() string
}

// Store 是 Activity 需要的数据访问子集（规格 B3 的相关方法）。
// 收窄成接口而非直接依赖 *store.Store：便于 Activity 单测注入替身。
type Store interface {
	// AuthorizeScheduledRun is the fail-closed activation mirror gate. A newly
	// unpaused Temporal schedule must not spend money or push until the exact
	// DB task is active, mature, and still belongs to an active tenant/member.
	AuthorizeScheduledRun(ctx context.Context, scheduleID string, userID int64) (bool, error)
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
	// GetScheduleStrictness 读任务的推送门槛档位（migration 025，Select 过滤用）。
	// 空串 = 未设置/行不存在 → 调用方按 types.DefaultStrictness 兜底。
	GetScheduleStrictness(ctx context.Context, scheduleID string) (types.PushStrictness, error)
	// GetSchedulePlaybook supplies the user-approved task instruction consumed
	// by Score and CardGen. Both Activities read it once before their fan-out.
	GetSchedulePlaybook(ctx context.Context, userID int64, scheduleID string) (*types.SchedulePlaybook, error)
}

// CompiledRunStore is deliberately separate from the legacy Store interface:
// old Activity tests and ad-hoc runs need no snapshot privileges, while C1b
// consumers receive only the immutable/ref/live-authorization operations they
// require. The concrete production implementation is *store.Store.
type CompiledRunStore interface {
	LoadCompiledRunSnapshotRefV1(context.Context, types.RunIdentity) (types.RunSnapshotRef, bool, error)
	CreateOrGetCompiledRunSnapshotV1(context.Context, types.RunIdentity, runtimepolicy.BundleV1) (types.RunSnapshotRef, error)
	LoadAuthoritativeCompiledTaskRunSnapshot(context.Context, types.RunIdentity, types.RunSnapshotRef) (runcontext.CompiledSnapshotV1, storepkg.CompiledRunSnapshotAuthority, error)
	AuthorizeTaskRunSideEffect(context.Context, types.RunIdentity, types.RunSnapshotRef) (bool, error)
	AuthorizeAndConsumeTaskRunLLMQuotaV1(context.Context, types.RunIdentity, types.RunSnapshotRef, runtimepolicy.QuotaBucketV1, float64) error
	UpsertContentItemForTaskRunV1(context.Context, types.RunIdentity, types.RunSnapshotRef, int64, *types.ContentItem) (int64, bool, error)
	UpdateSourceFetchStateForTaskRunV1(context.Context, types.RunIdentity, types.RunSnapshotRef, int64, time.Time, time.Time, int) (bool, error)
	DisableSourceIfActiveForTaskRunV1(context.Context, types.RunIdentity, types.RunSnapshotRef, int64) (bool, error)
	ListRecentSimhashesForTaskRunV1(context.Context, types.RunIdentity, types.RunSnapshotRef, time.Time, []int64) ([]int64, error)
	EvolveProfileForTaskRunV1(context.Context, types.RunIdentity, types.RunSnapshotRef, string, []string, int64, time.Time, int64) error
	AdvanceProfileCursorForTaskRunV1(context.Context, types.RunIdentity, types.RunSnapshotRef, int64, time.Time, int64) error
	CreatePushBatchForTaskRunV1(context.Context, types.RunIdentity, types.RunSnapshotRef, string) (int64, error)
	CreateOrRecoverPushBatchForTaskRunV1(context.Context, types.RunIdentity, types.RunSnapshotRef, string) (int64, bool, error)
	RecordEmptyPushBatchForTaskRunV1(context.Context, types.RunIdentity, types.RunSnapshotRef, string, types.BatchExitGate, types.PipelineCounts) (int64, bool, error)
	InsertDeliveryForTaskRunV1(context.Context, types.RunIdentity, types.RunSnapshotRef, string, *types.Delivery) (int64, bool, bool, error)
	UpdatePushBatchStatusForTaskRunV1(context.Context, types.RunIdentity, types.RunSnapshotRef, string, int64, types.BatchStatus) error
	MarkPushBatchDoneReceiptV1(context.Context, types.RunIdentity, types.RunSnapshotRef, string, int64) error
	MarkDeliverySentForTaskRunV1(context.Context, types.RunIdentity, types.RunSnapshotRef, string, int64, int64, string, json.RawMessage, time.Time) error
	ListDueSourcesByIDs(context.Context, []int64) ([]types.Source, error)
	ListUnpushedForTaskRunV1(context.Context, types.RunIdentity, types.RunSnapshotRef, []int64, int, int) ([]types.ContentItem, error)
	SourceForContentFromIDs(context.Context, int64, []int64) (int64, bool, error)
}

type CompiledRunSnapshotShadowV2Store interface {
	CreateOrGetCompiledRunSnapshotShadowV2(
		context.Context, types.RunIdentity, runtimepolicy.BundleV1,
	) (types.RunSnapshotRef, error)
}

type CompiledRunSnapshotV2AuditReader interface {
	AuditCompiledTaskRunSnapshotV2(
		context.Context, types.RunIdentity, types.RunSnapshotRef,
	) (storepkg.CompiledRunSnapshotV2AuditResult, error)
}

type PushEffectStore interface {
	CreatePushEffect(context.Context, pusheffect.Prepared) (*pusheffect.Effect, error)
	ClaimPushEffect(context.Context, pusheffect.ClaimParams) (*pusheffect.Effect, error)
	ClaimPushEffectReconciliation(
		context.Context,
		pusheffect.ClaimParams,
	) (*pusheffect.Effect, error)
	RecordPushEffectDefiniteFailure(
		context.Context,
		pusheffect.FailureParams,
	) error
	RecordPushEffectAmbiguous(
		context.Context,
		pusheffect.FailureParams,
	) error
	RecordPushEffectSentWithDeliveries(
		context.Context,
		pusheffect.SentReceipt,
	) error
}

// CompiledPolicyBuilderV1 freezes the process's current non-secret runtime
// policy. The boolean is the per-task result of the existing playbook rollout
// decision, not a mutable pointer to configuration.
type CompiledPolicyBuilderV1 func(
	context.Context,
	int64,
	bool,
) (runtimepolicy.BundleV1, error)

type CompiledModelResolverV1 interface {
	ResolveRuntimeModelPolicyV1(runtimepolicy.ModelPolicyV1) (*llm.Client, error)
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
	// playbookPromptsEnabled is the P1c rollout switch. A non-empty canary ID
	// narrows injection to one real schedule; empty means all schedules once
	// the canary has passed. Both are immutable after worker construction.
	playbookPromptsEnabled           bool
	playbookPromptCanaryScheduleID   string
	compiledStore                    CompiledRunStore
	buildCompiledPolicyV1            CompiledPolicyBuilderV1
	compiledModelResolverV1          CompiledModelResolverV1
	compiledShadowStoreV2            CompiledRunSnapshotShadowV2Store
	snapshotV2ShadowCanaryTaskID     string
	compiledSnapshotV2AuditReader    CompiledRunSnapshotV2AuditReader
	snapshotV2ReadAuditCanaryTaskID  string
	snapshotV2ReadAuditTimeout       time.Duration
	observationStore                 ObservationRuntimeStore
	eventQualifier                   *eventqualifier.Qualifier
	observationShadowCanaryTaskID    string
	observationAuthorityCanaryTaskID string
	pushEffectStore                  PushEffectStore
	pushEffectCanaryTaskID           string
}

// ActivitiesOption configures rollout-only Activity behavior without adding
// another positional constructor parameter to every test and composition root.
type ActivitiesOption func(*Activities)

type ObservationRuntimeStore interface {
	PrepareObservationQualificationStep(
		context.Context, types.RunIdentity, types.RunSnapshotRef, string, string,
	) (string, json.RawMessage, error)
	MarkObservationQualificationSending(
		context.Context, types.RunIdentity, types.RunSnapshotRef, string, string,
	) error
	CompleteObservationQualificationStep(
		context.Context, types.RunIdentity, types.RunSnapshotRef, string, string,
		json.RawMessage,
	) error
	MarkObservationQualificationUncertain(
		context.Context, types.RunIdentity, types.RunSnapshotRef, string, string,
	) error
	ReserveObservedEventV1(
		context.Context, types.RunIdentity, types.RunSnapshotRef,
		observation.QualifiedEvent,
	) (bool, error)
	BindObservedEventDeliveryV1(
		context.Context, types.RunIdentity, types.RunSnapshotRef,
		string, string, int64,
	) error
	MarkObservedEventDeliveredV1(
		context.Context, types.RunIdentity, types.RunSnapshotRef, int64,
	) error
}

// WithObservationRuntime enables the bounded qualifier for exactly one shadow
// task and, optionally, one exact authority task. Empty IDs are a complete
// rollback switch. Authority is still selected in Workflow history through a
// versioned Activity call; mutable config never changes an in-flight command
// sequence.
func WithObservationRuntime(
	st ObservationRuntimeStore,
	qualifier *eventqualifier.Qualifier,
	shadowTaskID, authorityTaskID string,
) ActivitiesOption {
	return func(a *Activities) {
		a.observationStore = st
		a.eventQualifier = qualifier
		a.observationShadowCanaryTaskID = strings.TrimSpace(shadowTaskID)
		a.observationAuthorityCanaryTaskID = strings.TrimSpace(authorityTaskID)
	}
}

// WithPlaybookPromptPolicy controls P1c prompt injection. enabled=false is an
// exact rollback switch. When canaryScheduleID is non-empty, only that one task
// may load a playbook; all other tasks stay on the byte-identical legacy path.
// The composition root validates a separate explicit allow-all key before it
// may call this with enabled=true and an empty canary ID.
func WithPlaybookPromptPolicy(enabled bool, canaryScheduleID string) ActivitiesOption {
	return func(a *Activities) {
		a.playbookPromptsEnabled = enabled
		a.playbookPromptCanaryScheduleID = strings.TrimSpace(canaryScheduleID)
	}
}

// WithCompiledRuntimeV1 installs C1b's typed snapshot boundary. Both
// dependencies are required together; PrepareRun fails closed if composition
// is incomplete, while replay/legacy paths remain byte-compatible.
func WithCompiledRuntimeV1(
	st CompiledRunStore,
	builder CompiledPolicyBuilderV1,
	modelResolver CompiledModelResolverV1,
) ActivitiesOption {
	return func(a *Activities) {
		a.compiledStore = st
		a.buildCompiledPolicyV1 = builder
		a.compiledModelResolverV1 = modelResolver
	}
}

// WithSnapshotV2ShadowCanary enables C2c-2 for exactly one task. It changes
// only the run-start persistence call; PrepareRun still returns and consumes
// the sealed v1 reference.
func WithSnapshotV2ShadowCanary(
	st CompiledRunSnapshotShadowV2Store,
	taskID string,
) ActivitiesOption {
	return func(a *Activities) {
		a.compiledShadowStoreV2 = st
		a.snapshotV2ShadowCanaryTaskID = taskID
	}
}

// WithSnapshotV2ReadAuditCanary enables C2c-3a observation for exactly one
// task. The materialized v2 view is compared and logged, then discarded; every
// runtime consumer continues using the independently loaded pinned v1 view.
func WithSnapshotV2ReadAuditCanary(
	reader CompiledRunSnapshotV2AuditReader,
	taskID string,
) ActivitiesOption {
	return func(a *Activities) {
		a.compiledSnapshotV2AuditReader = reader
		a.snapshotV2ReadAuditCanaryTaskID = taskID
	}
}

// WithPushEffectCanary enables durable provider effects for exactly one
// compiled task. Empty task IDs keep all call points dark.
func WithPushEffectCanary(
	st PushEffectStore,
	taskID string,
) ActivitiesOption {
	return func(a *Activities) {
		a.pushEffectStore = st
		a.pushEffectCanaryTaskID = strings.TrimSpace(taskID)
	}
}

// NewActivities 装配 Activities。前六参顺序与规格 B6"持有 fetcher/scorer/
// cardgen/pusher/store/feishuMgr"一致；ev 可为 nil（演化灰度关闭）；
// buildNotice 可为 nil（抓取失败告警退化为 no-op）。
func NewActivities(f Fetcher, sc Scorer, cg CardGenerator, p Pusher, st Store, fs FeishuManager,
	ev ProfileEvolver, buildNotice func(markdown string) string,
	buildAggCard func(in feedback.AggregateCardInput) string,
	aggHeader func(task string, n int) (title, template string), opts ...ActivitiesOption) *Activities {
	a := &Activities{fetcher: f, scorer: sc, cardgen: cg, pusher: p, store: st, feishu: fs,
		evolver: ev, buildNotice: buildNotice,
		buildAggCard: buildAggCard, aggHeader: aggHeader,
		snapshotV2ReadAuditTimeout: 2 * time.Second}
	for _, opt := range opts {
		if opt != nil {
			opt(a)
		}
	}
	return a
}

// ============================================================
// Activity 入参结构：每步用具体 struct 承载（含 UserID/TraceID/上一步结果），
// 避免裸切片跨步传递时语义歧义（规格 B6）。出参直接复用 types.go 的跨包类型。
// ============================================================

// EvolveIn 是 EvolveProfile Activity 的入参。
type EvolveIn struct {
	UserID  int64               `json:"user_id"`
	TraceID string              `json:"trace_id"`
	Run     *CompiledRunInputV1 `json:"run,omitempty"`
}

// DedupIn 是 Dedup Activity 的入参。
type DedupIn struct {
	UserID  int64               `json:"user_id"`
	TraceID string              `json:"trace_id"`
	Items   []types.ContentItem `json:"items"`
	Run     *CompiledRunInputV1 `json:"run,omitempty"`
}

type QualifyEventsIn struct {
	UserID     int64               `json:"user_id"`
	TraceID    string              `json:"trace_id"`
	ScheduleID string              `json:"schedule_id"`
	Items      []types.ContentItem `json:"items"`
	Run        *CompiledRunInputV1 `json:"run,omitempty"`
}

type QualifyEventsResult struct {
	Items   []types.ContentItem `json:"items"`
	Outcome string              `json:"outcome"`
}

// ScoreIn 是 Score Activity 的入参。
type ScoreIn struct {
	UserID     int64               `json:"user_id"`
	TraceID    string              `json:"trace_id"`
	Items      []types.ContentItem `json:"items"`
	ScheduleID string              `json:"schedule_id,omitempty"`
	Run        *CompiledRunInputV1 `json:"run,omitempty"`
}

// SelectIn 是 Select Activity 的入参。
type SelectIn struct {
	UserID  int64              `json:"user_id"`
	TraceID string             `json:"trace_id"`
	TopN    int                `json:"top_n"`
	Scored  []types.ScoredItem `json:"scored"`
	// ScheduleID 触发本次推送的任务 id（空=即时/老任务触发）：Select 据此查任务的
	// 门槛档位（schedules.push_strictness）；空或未设置一律走 types.DefaultStrictness
	// 全局兜底——0-20"不该推"档在任何路径都不推（2026-07-19 五张 0 分卡的修复）。
	ScheduleID string              `json:"schedule_id,omitempty"`
	Run        *CompiledRunInputV1 `json:"run,omitempty"`
}

// Select 的返回**保持** []types.ScoredItem 不升级成结构体，是重放兼容性逼出来的
// （replay_test.TestPushPipelineWorkflow_ReplayBaselineHappyPath 钉死）：发布时停在
// CardGen/Push 上的 in-flight workflow 重放旧历史，Select 结果 payload 是数组，
// 新代码若按结构体解码直接炸历史。门槛过滤致空时通知卡要的上下文走别的通道：
// MaxScore 由 workflow 从 scored 纯计算（确定性），档位由 NotifyEmptyResult
// 自己查库（activity 内 I/O 合法）。

// CardGenIn 是 CardGen Activity 的入参。
type CardGenIn struct {
	UserID     int64               `json:"user_id"`
	TraceID    string              `json:"trace_id"`
	Items      []types.ScoredItem  `json:"items"`
	ScheduleID string              `json:"schedule_id,omitempty"`
	Run        *CompiledRunInputV1 `json:"run,omitempty"`
}

// PushIn 是 Push Activity 的入参。
type PushIn struct {
	UserID     int64           `json:"user_id"`
	ScheduleID string          `json:"schedule_id,omitempty"` // 触发本批的任务 id（P1b），空=即时/老任务
	TraceID    string          `json:"trace_id"`
	Cards      []GeneratedCard `json:"cards"`
	// TaskTitle 任务名（调度的 nl_description），聚合卡 header 用。
	// 空串合法（存量调度/即时推送无任务名），构卡落兜底标题。
	TaskTitle string              `json:"task_title,omitempty"`
	Run       *CompiledRunInputV1 `json:"run,omitempty"`
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
	Run    *CompiledRunInputV1  `json:"run,omitempty"`
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
	snapshot, compiled, err := a.loadAuthoritativeCompiledRun(ctx, in.UserID, in.Run)
	if err != nil {
		return retryableOrNot(err)
	}
	if a.evolver == nil {
		if compiled {
			return nonRetryable(types.NewAppError(types.CodeInternal,
				"compiled profile evolver is not configured", nil))
		}
		return nil
	}
	if compiled {
		modelClient, err := a.resolveCompiledModelPolicyV1(snapshot.Policy.ModelPolicy)
		if err != nil {
			return retryableOrNot(err)
		}
		consumer, ok := a.evolver.(compiledProfileEvolverV1)
		if !ok {
			return nonRetryable(types.NewAppError(types.CodeInternal,
				"compiled profile evolver v1 is unsupported", nil))
		}
		policy, err := evolverpkg.PrepareCompiledPolicyV1(
			snapshot.Policy.PromptPolicy, snapshot.Policy.ModelPolicy,
			snapshot.Policy.QuotaPolicy, modelClient)
		if err != nil {
			return nonRetryable(types.NewAppError(types.CodeValidation,
				"compiled profile policy is invalid", err))
		}
		if err := a.authorizeCompiledEffectV1(ctx, in.UserID, in.Run); err != nil {
			return retryableOrNot(err)
		}
		return retryableOrNot(consumer.EvolveWithPolicyV1(
			ctx, snapshot.Definition.TenantID, in.UserID, in.TraceID, policy,
			func(effectCtx context.Context, amount float64) error {
				return a.consumeCompiledLLMQuotaV1(
					effectCtx, in.UserID, in.Run, snapshot.Policy.QuotaPolicy, amount)
			},
			evolverpkg.CompiledProfileWritesV1{
				Evolve: func(effectCtx context.Context, summary string, tags []string,
					newCursor int64, expectedAt time.Time, expectedCursor int64) error {
					expected, err := activityRunIdentityV1(effectCtx, in.UserID, in.Run)
					if err != nil {
						return err
					}
					return a.compiledStore.EvolveProfileForTaskRunV1(
						effectCtx, expected, in.Run.Snapshot, summary, tags,
						newCursor, expectedAt, expectedCursor)
				},
				AdvanceCursor: func(effectCtx context.Context, newCursor int64,
					expectedAt time.Time, expectedCursor int64) error {
					expected, err := activityRunIdentityV1(effectCtx, in.UserID, in.Run)
					if err != nil {
						return err
					}
					return a.compiledStore.AdvanceProfileCursorForTaskRunV1(
						effectCtx, expected, in.Run.Snapshot, newCursor,
						expectedAt, expectedCursor)
				},
			},
		))
	}
	if a.refuseIfTenantGone(ctx, in.UserID, "evolve_profile") {
		return nil // 正常终态，不报错——报错会触发重试，而重试同样会被拒。
	}
	// retryableOrNot：Evolve 内部会经 llm.Do 撞上配额，而裸 *AppError 跨 activity
	// 边界后 Temporal 取的 Type 是 Go 类型名 "AppError"，NonRetryableErrorTypes
	// 匹配不到 —— 于是额度用尽会被白重试三次（额度按秒补，三次必然都失败），
	// 徒增噪音还把明确原因埋进一串重试错误里。
	return retryableOrNot(a.evolver.Evolve(ctx, in.UserID, in.TraceID))
}

// AuthorizeRun is the first Activity in every pipeline. Scheduled runs fail
// closed until Postgres confirms the exact task is active. Ad-hoc runs must be
// explicitly labelled: an empty ScheduleID is also the frozen shape of a
// legacy schedule before reconcile and must never silently bypass this gate.
func (a *Activities) AuthorizeRun(ctx context.Context, p PushParams) (bool, error) {
	if p.UserID <= 0 {
		return false, nil
	}
	switch p.RunKind {
	case PushRunKindAdHoc:
		return p.ScheduleID == "", nil
	case PushRunKindScheduled:
		if p.ScheduleID == "" {
			return false, nil
		}
		return a.store.AuthorizeScheduledRun(ctx, p.ScheduleID, p.UserID)
	default:
		return false, nil
	}
}

// PrepareRun is C1b's only run-start snapshot producer. Temporal identity is
// taken from ActivityInfo, while tenant/user/task are copied from the trusted
// Schedule Action. An exact committed ref is recovered before rebuilding any
// current policy, so response-lost retries remain first-writer-wins across task
// deletion or deployment changes.
func (a *Activities) PrepareRun(ctx context.Context, p PushParams) (PrepareRunResult, error) {
	_, compiledFetcherConfigured := a.fetcher.(compiledFetcherV1)
	if a.compiledStore == nil || a.buildCompiledPolicyV1 == nil || a.compiledModelResolverV1 == nil ||
		!compiledFetcherConfigured ||
		p.Snapshot != nil || p.RunKind != PushRunKindScheduled ||
		p.ExecutionMode != types.ExecutionModeCompiled ||
		p.RuntimeVersion != CompiledRuntimeSnapshotV1 ||
		p.TenantID <= 0 || p.UserID <= 0 || strings.TrimSpace(p.ScheduleID) == "" ||
		!activity.IsActivity(ctx) {
		return PrepareRunResult{}, nonRetryable(types.NewAppError(types.CodeValidation,
			"compiled scheduled run input is invalid", nil))
	}
	info := activity.GetInfo(ctx)
	expected := types.RunIdentity{
		TemporalWorkflowID: info.WorkflowExecution.ID,
		TemporalRunID:      info.WorkflowExecution.RunID,
		RunKind:            types.RunSnapshotKindScheduled,
		TenantID:           p.TenantID,
		UserID:             p.UserID,
		TaskID:             p.ScheduleID,
	}
	if err := expected.Validate(); err != nil {
		return PrepareRunResult{}, nonRetryable(err)
	}

	ref, found, err := a.compiledStore.LoadCompiledRunSnapshotRefV1(ctx, expected)
	if err != nil {
		return PrepareRunResult{}, retryableOrNot(err)
	}
	if !found {
		policy, buildErr := a.buildCompiledPolicyV1(
			ctx, p.TenantID, a.taskInstructionEnabled(p.ScheduleID))
		if buildErr != nil {
			var appErr *types.AppError
			if errors.As(buildErr, &appErr) {
				return PrepareRunResult{}, retryableOrNot(buildErr)
			}
			return PrepareRunResult{}, nonRetryable(types.NewAppError(types.CodeInternal,
				"compiled runtime policy is invalid", buildErr))
		}
		if _, err := a.resolveCompiledModelPolicyV1(policy.ModelPolicy); err != nil {
			return PrepareRunResult{}, nonRetryable(err)
		}
		if a.compiledShadowStoreV2 != nil &&
			p.ScheduleID == a.snapshotV2ShadowCanaryTaskID {
			ref, err = a.compiledShadowStoreV2.CreateOrGetCompiledRunSnapshotShadowV2(
				ctx, expected, policy)
		} else {
			ref, err = a.compiledStore.CreateOrGetCompiledRunSnapshotV1(
				ctx, expected, policy)
		}
		if err != nil {
			// A task may be paused/deleted after Temporal has started the run but
			// before its first immutable snapshot is committed. That is a normal
			// authorization denial, not a broken workflow: expose neither the
			// candidate policy nor a reusable reference and do not retry it.
			if errors.Is(err, types.ErrNotFound) {
				return PrepareRunResult{}, nil
			}
			return PrepareRunResult{}, retryableOrNot(err)
		}
	}
	snapshot, authority, err := a.compiledStore.LoadAuthoritativeCompiledTaskRunSnapshot(
		ctx, expected, ref)
	if err != nil {
		return PrepareRunResult{}, retryableOrNot(err)
	}
	if err := a.validateCompiledSnapshotRoutesV1(snapshot); err != nil {
		return PrepareRunResult{}, nonRetryable(err)
	}
	authorized, err := a.compiledStore.AuthorizeTaskRunSideEffect(ctx, expected, ref)
	if err != nil {
		return PrepareRunResult{}, retryableOrNot(err)
	}
	if !authorized {
		return PrepareRunResult{}, nil
	}
	a.logCompiledSnapshotAuthority(ctx, expected, ref, authority, "prepare_run")
	a.auditCompiledSnapshotV2(
		ctx, expected, ref, authority, "prepare_run")
	result := PrepareRunResult{Authorized: true, Snapshot: ref}
	if err := result.ValidateFor(expected); err != nil {
		return PrepareRunResult{}, nonRetryable(err)
	}
	return result, nil
}

func (a *Activities) taskInstructionEnabled(scheduleID string) bool {
	if !a.playbookPromptsEnabled || scheduleID == "" {
		return false
	}
	canaryID := a.playbookPromptCanaryScheduleID
	return canaryID == "" || canaryID == scheduleID
}

func activityRunIdentityV1(ctx context.Context, userID int64, run *CompiledRunInputV1) (types.RunIdentity, error) {
	if run == nil || run.TenantID <= 0 || userID <= 0 ||
		strings.TrimSpace(run.TaskID) == "" || !activity.IsActivity(ctx) {
		return types.RunIdentity{}, types.NewAppError(types.CodeValidation,
			"compiled activity run input is invalid", nil)
	}
	info := activity.GetInfo(ctx)
	expected := types.RunIdentity{
		TemporalWorkflowID: info.WorkflowExecution.ID,
		TemporalRunID:      info.WorkflowExecution.RunID,
		RunKind:            types.RunSnapshotKindScheduled,
		TenantID:           run.TenantID,
		UserID:             userID,
		TaskID:             run.TaskID,
	}
	if err := expected.Validate(); err != nil {
		return types.RunIdentity{}, err
	}
	return expected, nil
}

func (a *Activities) loadAuthoritativeCompiledRun(
	ctx context.Context,
	userID int64,
	run *CompiledRunInputV1,
) (runcontext.CompiledSnapshotV1, bool, error) {
	if run == nil {
		return runcontext.CompiledSnapshotV1{}, false, nil
	}
	if a.compiledStore == nil {
		return runcontext.CompiledSnapshotV1{}, true,
			types.NewAppError(types.CodeInternal, "compiled runtime store is not configured", nil)
	}
	expected, err := activityRunIdentityV1(ctx, userID, run)
	if err != nil {
		return runcontext.CompiledSnapshotV1{}, true, err
	}
	snapshot, authority, err := a.compiledStore.LoadAuthoritativeCompiledTaskRunSnapshot(
		ctx, expected, run.Snapshot)
	if err != nil {
		return runcontext.CompiledSnapshotV1{}, true, err
	}
	a.logCompiledSnapshotAuthority(
		ctx, expected, run.Snapshot, authority, "activity_consumer")
	a.auditCompiledSnapshotV2(
		ctx, expected, run.Snapshot, authority, "activity_consumer")
	return snapshot, true, nil
}

// auditCompiledSnapshotV2 is C2c-3a's only Activity-side v2 read router. It is
// deliberately observation-only: missing/non-match/corrupt shadows remain
// visible in structured logs but never select authority. The immutable parent
// marker was already interpreted by loadAuthoritativeCompiledRun; this helper
// only preserves the independent C2c-3a canary signal.
func (a *Activities) auditCompiledSnapshotV2(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	authority storepkg.CompiledRunSnapshotAuthority,
	stage string,
) {
	if a.compiledSnapshotV2AuditReader == nil ||
		expected.TaskID != a.snapshotV2ReadAuditCanaryTaskID {
		return
	}
	timeout := a.snapshotV2ReadAuditTimeout
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	auditCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	result, err := a.compiledSnapshotV2AuditReader.
		AuditCompiledTaskRunSnapshotV2(auditCtx, expected, ref)
	info := activity.GetInfo(ctx)
	attrs := []any{
		"stage", stage,
		"activity_type", info.ActivityType.Name,
		"activity_attempt", info.Attempt,
		"task_id", expected.TaskID,
		"temporal_workflow_id", expected.TemporalWorkflowID,
		"temporal_run_id", expected.TemporalRunID,
		"snapshot_id", ref.SnapshotID,
		"v1_payload_digest", ref.PayloadDigest,
		"authoritative", authority,
		"shadow", "v2",
	}
	if err != nil {
		attrs = append(attrs,
			"outcome", "error",
			"error_code", types.CodeOf(err),
			"error_type", fmt.Sprintf("%T", err))
		slog.ErrorContext(ctx, "compiled snapshot v2 read audit", attrs...)
		return
	}
	attrs = append(attrs,
		"outcome", result.Status,
		"shadow_status", result.ShadowStatus,
		"shadow_payload_digest", result.ShadowPayloadDigest,
		"typed_equal", result.TypedEqual)
	slog.InfoContext(ctx, "compiled snapshot v2 read audit", attrs...)
}

func (a *Activities) logCompiledSnapshotAuthority(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	authority storepkg.CompiledRunSnapshotAuthority,
	stage string,
) {
	if authority != storepkg.CompiledRunSnapshotAuthorityV2 {
		return
	}
	info := activity.GetInfo(ctx)
	slog.InfoContext(ctx, "compiled snapshot authority selected",
		"stage", stage,
		"activity_type", info.ActivityType.Name,
		"activity_attempt", info.Attempt,
		"task_id", expected.TaskID,
		"temporal_workflow_id", expected.TemporalWorkflowID,
		"temporal_run_id", expected.TemporalRunID,
		"snapshot_id", ref.SnapshotID,
		"authority", authority)
}

func (a *Activities) resolveCompiledModelPolicyV1(
	policy runtimepolicy.ModelPolicyV1,
) (*llm.Client, error) {
	if a.compiledModelResolverV1 == nil {
		return nil, types.NewAppError(types.CodeInternal,
			"compiled model resolver v1 is not configured", nil)
	}
	client, err := a.compiledModelResolverV1.ResolveRuntimeModelPolicyV1(policy)
	if err != nil {
		return nil, types.NewAppError(types.CodeValidation,
			"compiled model route is unavailable", err)
	}
	return client, nil
}

func (a *Activities) validateCompiledSnapshotRoutesV1(snapshot runcontext.CompiledSnapshotV1) error {
	if snapshot.Mode != types.ExecutionModeCompiled {
		return types.NewAppError(types.CodeValidation,
			"compiled run snapshot has an invalid execution mode", nil)
	}
	frozenFetcher, ok := a.fetcher.(compiledFetcherV1)
	if !ok {
		return types.NewAppError(types.CodeInternal,
			"compiled fetch route resolver is not configured", nil)
	}
	for _, source := range snapshot.Definition.Sources {
		capability, ok := compiledCapabilityV1(
			snapshot.Policy.CapabilityCatalog, source.Platform, source.Capability)
		if !ok {
			return types.NewAppError(types.CodeValidation,
				"compiled source capability is not allowed", nil)
		}
		if err := frozenFetcher.ValidateRuntimeFetchRouteV1(capability, types.Source{
			Platform: source.Platform, Capability: source.Capability,
		}); err != nil {
			return types.NewAppError(types.CodeValidation,
				"compiled source route is unavailable", err)
		}
	}
	if _, err := a.resolveCompiledModelPolicyV1(snapshot.Policy.ModelPolicy); err != nil {
		return err
	}
	return nil
}

func (a *Activities) consumeCompiledLLMQuotaV1(
	ctx context.Context,
	userID int64,
	run *CompiledRunInputV1,
	policy runtimepolicy.QuotaPolicyV1,
	amount float64,
) error {
	if run == nil || a.compiledStore == nil {
		return types.NewAppError(types.CodeInternal,
			"compiled runtime store is not configured", nil)
	}
	rule, ok := policy.Bucket("llm_tokens")
	if !ok {
		return types.NewAppError(types.CodeValidation,
			"compiled llm quota policy is missing", nil)
	}
	expected, err := activityRunIdentityV1(ctx, userID, run)
	if err != nil {
		return err
	}
	return a.compiledStore.AuthorizeAndConsumeTaskRunLLMQuotaV1(
		ctx, expected, run.Snapshot, rule, amount)
}

func compiledCapabilityV1(
	catalog runtimepolicy.CapabilityCatalogV1,
	platform types.Platform,
	capability types.Capability,
) (runtimepolicy.CapabilityV1, bool) {
	for _, allowed := range catalog.Allowed {
		if allowed.Platform == string(platform) && allowed.Capability == string(capability) {
			return allowed, true
		}
	}
	return runtimepolicy.CapabilityV1{}, false
}

// authorizeCompiledEffectV1 is called immediately before a compiled Activity
// starts a paid/external/write effect. Unlike the legacy D9 side gate it is
// fail-closed on database errors and binds the ref to the current Activity
// attempt's WorkflowID/RunID every time.
func (a *Activities) authorizeCompiledEffectV1(
	ctx context.Context,
	userID int64,
	run *CompiledRunInputV1,
) error {
	if run == nil {
		return nil
	}
	if a.compiledStore == nil {
		return types.NewAppError(types.CodeInternal, "compiled runtime store is not configured", nil)
	}
	expected, err := activityRunIdentityV1(ctx, userID, run)
	if err != nil {
		return err
	}
	authorized, err := a.compiledStore.AuthorizeTaskRunSideEffect(ctx, expected, run.Snapshot)
	if err != nil {
		return err
	}
	if !authorized {
		return types.NewAppError(types.CodeNotFound,
			"compiled task run is no longer authorized", nil)
	}
	return nil
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
	var compiledInput *CompiledRunInputV1
	if p.Snapshot != nil {
		compiledInput = &CompiledRunInputV1{
			TenantID: p.TenantID,
			TaskID:   p.ScheduleID,
			Snapshot: *p.Snapshot,
		}
	}
	snapshot, compiled, err := a.loadAuthoritativeCompiledRun(ctx, p.UserID, compiledInput)
	if err != nil {
		return nil, retryableOrNot(err)
	}
	var compiledIdentity types.RunIdentity
	if compiled {
		compiledIdentity, err = activityRunIdentityV1(ctx, p.UserID, compiledInput)
		if err != nil {
			return nil, nonRetryable(err)
		}
		if err := a.validateCompiledSnapshotRoutesV1(snapshot); err != nil {
			return nil, nonRetryable(err)
		}
	}
	var frozenFetcher compiledFetcherV1
	if compiled {
		var ok bool
		frozenFetcher, ok = a.fetcher.(compiledFetcherV1)
		if !ok {
			return nil, nonRetryable(types.NewAppError(types.CodeInternal,
				"compiled fetcher v1 is unsupported", nil))
		}
	}
	// D9：已注销租户不抓取——Exa/TikHub 按次计费，这里是花钱的起点。
	// 返回空候选而非报错：空候选走 workflow 既有的空批次早退路径（正常终态、不推送），
	// 报错则会重试三次、每次都被同样拒绝，白白制造噪音。
	if !compiled && a.refuseIfTenantGone(ctx, p.UserID, "fetch") {
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
	if !compiled && p.ScheduleID != "" {
		has, herr := a.store.ScheduleHasSources(ctx, p.ScheduleID)
		if herr != nil {
			return nil, herr
		}
		planScoped = has
	}

	// 只抓到期源（next_fetch_at <= now()）：重试不重复计费，详见接口注释。
	// 未到期源被跳过不影响推送——其已入库内容仍由下方候选查询捞出。
	var sources []types.Source
	var frozenSourceIDs []int64
	var frozenSources map[int64]runcontext.SourceV1
	if compiled {
		frozenSourceIDs = make([]int64, len(snapshot.Definition.Sources))
		frozenSources = make(map[int64]runcontext.SourceV1, len(snapshot.Definition.Sources))
		for i, source := range snapshot.Definition.Sources {
			frozenSourceIDs[i] = source.SourceID
			frozenSources[source.SourceID] = source
		}
		if len(frozenSourceIDs) == 0 {
			return nil, nil
		}
		liveSources, loadErr := a.compiledStore.ListDueSourcesByIDs(ctx, frozenSourceIDs)
		if loadErr != nil {
			return nil, loadErr
		}
		sources = make([]types.Source, 0, len(liveSources))
		for _, live := range liveSources {
			frozen, ok := frozenSources[live.ID]
			if !ok {
				return nil, nonRetryable(types.NewAppError(types.CodeValidation,
					"compiled source health escaped frozen scope", nil))
			}
			live.Platform = frozen.Platform
			live.Capability = frozen.Capability
			live.Title = frozen.Title
			live.URL = frozen.URL
			live.Config = append(json.RawMessage(nil), frozen.Config...)
			sources = append(sources, live)
		}
	} else if planScoped {
		sources, err = a.store.ListDueSourcesBySchedule(ctx, p.ScheduleID) // 只抓本任务的源
	} else {
		sources, err = a.store.ListDueSourcesByUser(ctx, p.UserID)
	}
	if err != nil {
		return nil, err
	}
	if compiled {
		var frozenScope PushScope
		if err := json.Unmarshal(snapshot.Definition.ScopeJSON, &frozenScope); err != nil {
			return nil, nonRetryable(types.NewAppError(types.CodeValidation,
				"compiled task scope is invalid", err))
		}
		if len(frozenScope.SourceIDs) > 0 {
			sources = filterSources(sources, frozenScope.SourceIDs)
		}
	} else if len(p.Scope.SourceIDs) > 0 {
		sources = filterSources(sources, p.Scope.SourceIDs)
	}

	// 绑定引擎记账的 trace 锚点（endpoint-binding-contract.md §5）：用 workflow
	// execution ID 而非管线 traceID——后者在 PushParams 里没有，为它改活动入参
	// 会碰在途 run 的确定性；执行 ID 同样稳定可查（wf-push-… 关联到调度与批次）。
	// 在真实 activity 外（单测直调）GetInfo 会 panic，故判一下。
	if activity.IsActivity(ctx) {
		workflowID := activity.GetInfo(ctx).WorkflowExecution.ID
		if compiled {
			ctx = fetcher.WithBindingRunAttribution(
				ctx, workflowID, snapshot.Definition.TenantID, snapshot.Definition.UserID)
		} else {
			ctx = fetcher.WithBindingTrace(ctx, workflowID)
		}
	}

	// 逐源"抓取→立刻入库"的顺序是**有成本含义**的，别改成"先抓完所有源再统一入库"：
	// TikHub 详情补全的付费闸门查的是库里已补全的 canonical_key（fetcher.SeenChecker）。
	// 同一 run 内源 A 补全的笔记当场入库，源 B 抓到同一篇时闸门才命中、跳过付费；
	// 若把入库推迟到循环之后，源 B 查库时 A 的内容还没落地，同一篇笔记会被重复付费。
	var alertable []fetchFailure // 本轮"恰好"连续失败达告警阈值的源，循环后批量告警一次（功能 5.2）。
	var disabled []fetchFailure  // 本轮达停用阈值、刚被自动停用的源，循环后单发一张停用卡（功能 5.2）。
	for _, src := range sources {
		// Fetcher 应尊重 ctx，但单个 provider 可能只在自己的网络超时后才返回。
		// 一旦 Activity 已取消，绝不能继续启动后续按次计费的源调用。
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if compiled {
			if err := a.authorizeCompiledEffectV1(ctx, p.UserID, compiledInput); err != nil {
				return nil, retryableOrNot(err)
			}
		}
		var items []types.ContentItem
		var ferr error
		if compiled {
			capability, ok := compiledCapabilityV1(
				snapshot.Policy.CapabilityCatalog, src.Platform, src.Capability)
			if !ok {
				return nil, nonRetryable(types.NewAppError(types.CodeValidation,
					"compiled source capability is not allowed", nil))
			}
			var effectAuthorizationErr error
			var effectAuthorizationOnce sync.Once
			items, ferr = frozenFetcher.FetchWithPolicyV1(
				ctx, src, capability, func(effectCtx context.Context) error {
					authErr := a.authorizeCompiledEffectV1(
						effectCtx, p.UserID, compiledInput)
					if authErr != nil {
						effectAuthorizationOnce.Do(func() {
							effectAuthorizationErr = authErr
						})
					}
					return authErr
				})
			if effectAuthorizationErr != nil {
				return nil, retryableOrNot(effectAuthorizationErr)
			}
		} else {
			items, ferr = a.fetcher.Fetch(ctx, src)
		}
		if ferr != nil {
			// 单源失败不拖垮整批：某个源挂了不该让当次推送整体失败；
			// 同时自增 fail_count 并推进 next_fetch_at，避免调度紧循环重试。
			var crossed, justDisabled bool
			if compiled {
				crossed, justDisabled, err = a.markCompiledFetchResultV1(
					ctx, compiledIdentity, compiledInput.Snapshot, src, false)
				if err != nil {
					return nil, retryableOrNot(err)
				}
			} else {
				crossed, justDisabled = a.markFetchResult(ctx, src, false)
			}
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
			if compiled {
				if _, _, ierr := a.compiledStore.UpsertContentItemForTaskRunV1(
					ctx, compiledIdentity, compiledInput.Snapshot, src.ID, &items[i],
				); ierr != nil {
					return nil, retryableOrNot(ierr)
				}
				continue
			}
			if _, _, ierr := a.store.UpsertContentItem(ctx, &items[i]); ierr != nil {
				// 单条入库失败只 warn：已入库的其它条目仍会被后面的 ListUnpushedByUser 捞出。
				slog.Warn("fetch: 内容入库失败，跳过", "source_id", src.ID, "err", ierr)
			}
		}
		if compiled {
			if _, _, err := a.markCompiledFetchResultV1(
				ctx, compiledIdentity, compiledInput.Snapshot, src, true,
			); err != nil {
				return nil, retryableOrNot(err)
			}
		} else {
			a.markFetchResult(ctx, src, true) // 成功：清零 fail_count、推进 last/next_fetch_at。
		}
	}

	// 本轮有源恰好连续失败达阈值 → 给 owner 发一张汇总告警卡（功能 5.2）。
	// 放在返回候选之前、与推送早退无关：即便本轮全源挂掉（下方候选为空、workflow
	// 走空批次早退），告警也已在此发出。best-effort，失败只 warn 不拖垮推送。
	if len(alertable) > 0 {
		var beforeSend func(context.Context) error
		if compiled {
			beforeSend = func(effectCtx context.Context) error {
				return a.authorizeCompiledEffectV1(effectCtx, p.UserID, compiledInput)
			}
		}
		if err := a.alertFetchFailures(ctx, alertable, beforeSend); err != nil {
			return nil, retryableOrNot(err)
		}
	}
	// 本轮有源达停用阈值被自动停用 → 单发一张"已暂停 + 如何重新启用"卡（功能 5.2）。
	if len(disabled) > 0 {
		var beforeSend func(context.Context) error
		if compiled {
			beforeSend = func(effectCtx context.Context) error {
				return a.authorizeCompiledEffectV1(effectCtx, p.UserID, compiledInput)
			}
		}
		if err := a.alertSourcesDisabled(ctx, disabled, beforeSend); err != nil {
			return nil, retryableOrNot(err)
		}
	}

	// 返回"未投递候选"而非"本次新入库"，让 Fetch 重试幂等可续（修 #3）；
	// 全局上限 + 每源配额双重截断，控制单批打分规模且防高产源饿死低产源（修 #6）。
	// planScoped：只在本任务的源见过、且用户未读过的内容里挑——取材按任务隔离（决策 #3，
	// P1b b3），去重按用户级（决策 A：用户已读的内容任何路径都不再重推）。
	if compiled {
		return a.compiledStore.ListUnpushedForTaskRunV1(
			ctx, compiledIdentity, compiledInput.Snapshot, frozenSourceIDs,
			maxScoreCandidates, maxPerSourceCandidates)
	}
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
	lastFetched, nextFetch, failCount, crossedAlertThreshold := fetchResultState(src, ok)
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

// markCompiledFetchResultV1 keeps the legacy alert/disable thresholds while
// routing every global source-health mutation through the exact task-run
// transaction. A changed source identity/config is a safe no-op: the old run
// may finish with its frozen execution plan, but must not advance or disable
// the newly configured shared source.
func (a *Activities) markCompiledFetchResultV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	src types.Source,
	ok bool,
) (crossedAlertThreshold, justDisabled bool, err error) {
	lastFetched, nextFetch, failCount, crossed := fetchResultState(src, ok)
	updated, err := a.compiledStore.UpdateSourceFetchStateForTaskRunV1(
		ctx, expected, ref, src.ID, lastFetched, nextFetch, failCount,
	)
	if err != nil || !updated {
		return false, false, err
	}
	if !ok && failCount >= disableFetchFailThreshold {
		disabled, err := a.compiledStore.DisableSourceIfActiveForTaskRunV1(
			ctx, expected, ref, src.ID,
		)
		if err != nil {
			return false, false, err
		}
		justDisabled = disabled
	}
	return crossed, justDisabled, nil
}

func fetchResultState(
	src types.Source,
	ok bool,
) (lastFetched, nextFetch time.Time, failCount int, crossedAlertThreshold bool) {
	now := time.Now()
	nextFetch = now.Add(time.Duration(src.FetchIntervalSeconds) * time.Second)
	if ok {
		return now, nextFetch, 0, false
	}
	failCount = src.FailCount + 1
	if src.LastFetchedAt != nil {
		lastFetched = *src.LastFetchedAt
	}
	return lastFetched, nextFetch, failCount, failCount == alertFetchFailThreshold
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
func (a *Activities) alertFetchFailures(
	ctx context.Context,
	failures []fetchFailure,
	beforeSend func(context.Context) error,
) error {
	if len(failures) == 0 {
		return nil
	}
	if a.buildNotice == nil {
		return nil // 未注入告警卡构造器（灰度/测试）：静默 no-op。
	}
	owner := a.feishu.OwnerOpenID()
	if owner == "" {
		return nil // 尚未捕获 owner：无收件人，静默跳过（同 Push 对无 owner 的处理）。
	}
	card := a.buildNotice(renderFetchFailureAlert(failures))
	// Compiled runs revalidate after all local card/recipient preparation and
	// immediately before the external send. Authorization failures are not a
	// best-effort notification failure: swallowing one would let a revoked task
	// keep writing to the user's channel.
	if beforeSend != nil {
		if err := beforeSend(ctx); err != nil {
			return err
		}
	}
	if _, err := a.pusher.Push(ctx, owner, card); err != nil {
		slog.Warn("fetch: 抓取失败告警发送失败（不影响推送）", "count", len(failures), "err", err)
	}
	return nil
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
func (a *Activities) alertSourcesDisabled(
	ctx context.Context,
	disabled []fetchFailure,
	beforeSend func(context.Context) error,
) error {
	if len(disabled) == 0 {
		return nil
	}
	if a.buildNotice == nil {
		return nil
	}
	owner := a.feishu.OwnerOpenID()
	if owner == "" {
		return nil
	}
	card := a.buildNotice(renderSourcesDisabledAlert(disabled))
	if beforeSend != nil {
		if err := beforeSend(ctx); err != nil {
			return err
		}
	}
	if _, err := a.pusher.Push(ctx, owner, card); err != nil {
		slog.Warn("fetch: 信源停用告警发送失败（不影响推送）", "count", len(disabled), "err", err)
	}
	return nil
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
	_, compiled, err := a.loadAuthoritativeCompiledRun(ctx, in.UserID, in.Run)
	if err != nil {
		return nil, retryableOrNot(err)
	}
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
	var hist []int64
	if compiled {
		expected, identityErr := activityRunIdentityV1(ctx, in.UserID, in.Run)
		if identityErr != nil {
			return nil, nonRetryable(identityErr)
		}
		hist, err = a.compiledStore.ListRecentSimhashesForTaskRunV1(
			ctx, expected, in.Run.Snapshot, since, batchIDs,
		)
	} else {
		hist, err = a.store.ListRecentSimhashesByUser(ctx, in.UserID, since, batchIDs)
	}
	if err != nil {
		return nil, retryableOrNot(err)
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

// loadTaskInstruction reads the approved playbook once for one Activity batch.
// Missing/empty/error all fail open to an empty instruction so pre-playbook
// tasks retain the exact legacy LLM request. Logs expose only state and size;
// the content reaches only the downstream LLM request and its existing audit
// ledger, never Temporal history or application logs.
func (a *Activities) loadTaskInstruction(
	ctx context.Context,
	userID int64,
	scheduleID, traceID, stage string,
) string {
	if scheduleID == "" || a.store == nil {
		return ""
	}
	if !a.playbookPromptsEnabled {
		slog.DebugContext(ctx, "task playbook: prompt injection disabled; using legacy prompt",
			"stage", stage,
			"user_id", userID,
			"schedule_id", scheduleID,
			"trace_id", traceID,
			"status", "disabled")
		return ""
	}
	if canaryID := a.playbookPromptCanaryScheduleID; canaryID != "" && canaryID != scheduleID {
		slog.DebugContext(ctx, "task playbook: schedule outside canary; using legacy prompt",
			"stage", stage,
			"user_id", userID,
			"schedule_id", scheduleID,
			"trace_id", traceID,
			"status", "not_canary")
		return ""
	}
	pb, err := a.store.GetSchedulePlaybook(ctx, userID, scheduleID)
	if err != nil {
		if errors.Is(err, types.ErrNotFound) {
			slog.DebugContext(ctx, "task playbook: no instruction loaded; using legacy prompt",
				"stage", stage,
				"user_id", userID,
				"schedule_id", scheduleID,
				"trace_id", traceID,
				"status", "not_found")
		} else {
			slog.WarnContext(ctx, "task playbook: instruction read failed; using legacy prompt",
				"stage", stage,
				"user_id", userID,
				"schedule_id", scheduleID,
				"trace_id", traceID,
				"status", "error",
				"err", err)
		}
		return ""
	}
	if pb == nil || strings.TrimSpace(pb.Content) == "" {
		slog.DebugContext(ctx, "task playbook: no instruction loaded; using legacy prompt",
			"stage", stage,
			"user_id", userID,
			"schedule_id", scheduleID,
			"trace_id", traceID,
			"status", "empty")
		return ""
	}
	slog.InfoContext(ctx, "task playbook: instruction loaded",
		"stage", stage,
		"user_id", userID,
		"schedule_id", scheduleID,
		"trace_id", traceID,
		"status", "loaded",
		"stored_runes", len([]rune(pb.Content)),
		"injection_cap_runes", promptguard.TaskInstructionMaxRunes)
	return pb.Content
}

// QualifyEvents applies the immutable observation policy after content dedup
// and before relevance scoring. Shadow computes and audits a decision but
// returns the original candidates; exact authority returns only admitted
// candidates.
func (a *Activities) QualifyEvents(
	ctx context.Context,
	in QualifyEventsIn,
) (QualifyEventsResult, error) {
	snapshot, compiled, err := a.loadAuthoritativeCompiledRun(ctx, in.UserID, in.Run)
	if err != nil {
		return QualifyEventsResult{}, retryableOrNot(err)
	}
	if !compiled {
		return QualifyEventsResult{Items: in.Items, Outcome: "not_configured"}, nil
	}
	var scope struct {
		Observation *observation.PolicyV1 `json:"observation,omitempty"`
	}
	if err := json.Unmarshal(snapshot.Definition.ScopeJSON, &scope); err != nil {
		return QualifyEventsResult{}, nonRetryable(types.NewAppError(
			types.CodeValidation, "compiled observation scope is invalid", err))
	}
	if scope.Observation == nil {
		return QualifyEventsResult{Items: in.Items, Outcome: "not_configured"}, nil
	}
	shadow := in.ScheduleID == a.observationShadowCanaryTaskID
	authority := in.ScheduleID == a.observationAuthorityCanaryTaskID
	if !shadow && !authority {
		return QualifyEventsResult{Items: in.Items, Outcome: "rollout_off"}, nil
	}
	if a.observationStore == nil {
		return QualifyEventsResult{}, nonRetryable(types.NewAppError(
			types.CodeInternal, "observation runtime store is not configured", nil))
	}
	expected, err := activityRunIdentityV1(ctx, in.UserID, in.Run)
	if err != nil {
		return QualifyEventsResult{}, retryableOrNot(err)
	}
	nominal, err := observation.NominalTrigger(
		expected.TaskID, expected.TemporalWorkflowID)
	if err != nil {
		return QualifyEventsResult{}, nonRetryable(types.NewAppError(
			types.CodeValidation, "scheduled observation has no nominal trigger", err))
	}
	var spec struct {
		Cron         string `json:"cron"`
		EverySeconds int    `json:"every_seconds"`
		AnchorAt     string `json:"anchor_at"`
		TZ           string `json:"tz"`
	}
	if err := json.Unmarshal(snapshot.Definition.SpecJSON, &spec); err != nil {
		return QualifyEventsResult{}, nonRetryable(types.NewAppError(
			types.CodeValidation, "compiled observation schedule is invalid", err))
	}
	window, err := observation.WindowForNominal(*scope.Observation, observation.Schedule{
		Cron: spec.Cron, EverySeconds: spec.EverySeconds,
		AnchorAt: spec.AnchorAt, TimeZone: spec.TZ,
	}, nominal)
	if err != nil {
		return QualifyEventsResult{}, nonRetryable(types.NewAppError(
			types.CodeValidation, "compiled observation window is invalid", err))
	}

	var qualified []types.ContentItem
	outcome := "no_match"
	if scope.Observation.Mode == observation.ModeContent {
		qualified = qualifyContentWindow(*scope.Observation, window, in.Items)
		if len(qualified) > 0 {
			outcome = "match"
		}
	} else {
		qualified, outcome, err = a.qualifyEventCandidates(
			ctx, expected, snapshot, in, *scope.Observation, window)
		if err != nil {
			return QualifyEventsResult{}, err
		}
	}
	rollout := "shadow"
	if authority {
		rollout = "authority"
	}
	slog.InfoContext(ctx, "observation qualification",
		"task_id", expected.TaskID,
		"snapshot_id", in.Run.Snapshot.SnapshotID,
		"mode", scope.Observation.Mode,
		"rollout", rollout,
		"outcome", outcome,
		"candidate_count", len(in.Items),
		"qualified_count", len(qualified),
		"window_start", window.Start,
		"window_end", window.End)
	if shadow && !authority {
		return QualifyEventsResult{Items: in.Items, Outcome: "shadow_" + outcome}, nil
	}
	return QualifyEventsResult{Items: qualified, Outcome: outcome}, nil
}

func qualifyContentWindow(
	policy observation.PolicyV1,
	window observation.Window,
	items []types.ContentItem,
) []types.ContentItem {
	start := window.Start
	if policy.LatePolicy == observation.LateBounded {
		start = start.Add(-time.Duration(policy.AllowedLatenessSecs) * time.Second)
	}
	admission := observation.Window{Start: start, End: window.End}
	out := make([]types.ContentItem, 0, len(items))
	for _, item := range items {
		if item.PublishedAt == nil {
			switch policy.UnknownTime {
			case observation.UnknownTimeDeprioritize:
				item.ObservationScorePenalty = -20
				out = append(out, item)
			case observation.UnknownTimeAllow:
				out = append(out, item)
			}
			continue
		}
		if admission.Contains(item.PublishedAt.UTC()) {
			out = append(out, item)
		}
	}
	return out
}

func (a *Activities) qualifyEventCandidates(
	ctx context.Context,
	expected types.RunIdentity,
	snapshot runcontext.CompiledSnapshotV1,
	in QualifyEventsIn,
	policy observation.PolicyV1,
	window observation.Window,
) ([]types.ContentItem, string, error) {
	if a.eventQualifier == nil || a.compiledModelResolverV1 == nil {
		return nil, "", nonRetryable(types.NewAppError(
			types.CodeInternal, "event qualifier is not configured", nil))
	}
	policyDigest, err := observation.PolicyDigest(policy)
	if err != nil {
		return nil, "", nonRetryable(types.NewAppError(
			types.CodeValidation, "observation policy digest failed", err))
	}
	requestDigest, err := observationQualificationRequestDigest(
		policyDigest, window, in.Items)
	if err != nil {
		return nil, "", nonRetryable(types.NewAppError(
			types.CodeValidation, "observation request digest failed", err))
	}
	const stepID = "qualify-events-v1"
	status, cached, err := a.observationStore.PrepareObservationQualificationStep(
		ctx, expected, in.Run.Snapshot, stepID, requestDigest)
	if err != nil {
		return nil, "", retryableOrNot(err)
	}
	var result eventqualifier.Result
	switch status {
	case storepkg.ObservationStepCompleted:
		result, err = eventqualifier.Decode(cached)
		if err != nil {
			return nil, "", nonRetryable(types.NewAppError(
				types.CodeValidation, "stored event qualification is invalid", err))
		}
	case storepkg.ObservationStepUncertain:
		return nil, "uncertain", nil
	case storepkg.ObservationStepPrepared:
		modelClient, resolveErr := a.resolveCompiledModelPolicyV1(
			snapshot.Policy.ModelPolicy)
		if resolveErr != nil {
			return nil, "", retryableOrNot(resolveErr)
		}
		modelCall, ok := snapshot.Policy.ModelPolicy.Call(runtimepolicy.ModelStageCardGen)
		if !ok {
			return nil, "", nonRetryable(types.NewAppError(
				types.CodeValidation, "compiled qualifier model call is missing", nil))
		}
		quotaRule, ok := snapshot.Policy.QuotaPolicy.Bucket("llm_tokens")
		if !ok {
			return nil, "", nonRetryable(types.NewAppError(
				types.CodeValidation, "compiled qualifier quota rule is missing", nil))
		}
		beforeSpend := func(effectCtx context.Context, amount float64) error {
			if err := a.consumeCompiledLLMQuotaV1(
				effectCtx, in.UserID, in.Run,
				snapshot.Policy.QuotaPolicy, amount,
			); err != nil {
				return err
			}
			return a.observationStore.MarkObservationQualificationSending(
				effectCtx, expected, in.Run.Snapshot, stepID, requestDigest)
		}
		var canonical []byte
		result, canonical, err = a.eventQualifier.Qualify(ctx, eventqualifier.Request{
			TenantID: expected.TenantID, UserID: expected.UserID,
			TraceID: in.TraceID, Policy: policy, Window: window,
			Candidates: in.Items, Client: modelClient, ModelCall: modelCall,
			QuotaRule: &quotaRule, BeforeSpend: beforeSpend,
		})
		if err != nil {
			receiptCtx, cancel := detachedObservationReceiptContext(ctx)
			defer cancel()
			_ = a.observationStore.MarkObservationQualificationUncertain(
				receiptCtx, expected, in.Run.Snapshot,
				stepID, requestDigest)
			return nil, "uncertain", nil
		}
		receiptCtx, cancel := detachedObservationReceiptContext(ctx)
		defer cancel()
		if err := a.observationStore.CompleteObservationQualificationStep(
			receiptCtx, expected, in.Run.Snapshot,
			stepID, requestDigest, canonical,
		); err != nil {
			return nil, "", retryableOrNot(err)
		}
	default:
		return nil, "", nonRetryable(types.NewAppError(
			types.CodeConflict, "observation qualification state is invalid", nil))
	}
	qualified, outcome, err := a.validateQualifiedEvents(
		policy, policyDigest, window, in.Items, result)
	if err != nil && types.CodeOf(err) == types.CodeValidation {
		slog.WarnContext(ctx, "observation qualifier output rejected",
			"task_id", expected.TaskID,
			"snapshot_id", in.Run.Snapshot.SnapshotID,
			"err", err)
		return nil, "uncertain", nil
	}
	return qualified, outcome, err
}

func detachedObservationReceiptContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
}

func observationQualificationRequestDigest(
	policyDigest string,
	window observation.Window,
	items []types.ContentItem,
) (string, error) {
	type candidate struct {
		ID          int64      `json:"id"`
		URL         string     `json:"url"`
		ContentHash string     `json:"content_hash"`
		PublishedAt *time.Time `json:"published_at,omitempty"`
	}
	payload := struct {
		PolicyDigest string             `json:"policy_digest"`
		Window       observation.Window `json:"window"`
		Candidates   []candidate        `json:"candidates"`
	}{PolicyDigest: policyDigest, Window: window}
	for _, item := range items {
		payload.Candidates = append(payload.Candidates, candidate{
			ID: item.ID, URL: item.URL, ContentHash: item.ContentHash,
			PublishedAt: item.PublishedAt,
		})
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func (a *Activities) validateQualifiedEvents(
	policy observation.PolicyV1,
	policyDigest string,
	window observation.Window,
	items []types.ContentItem,
	result eventqualifier.Result,
) ([]types.ContentItem, string, error) {
	if result.Outcome == "no_match" || result.Outcome == "uncertain" {
		if len(result.Events) != 0 {
			return nil, "", nonRetryable(types.NewAppError(
				types.CodeValidation, "non-match qualifier returned events", nil))
		}
		return nil, result.Outcome, nil
	}
	if result.Outcome != "match" || len(result.Events) == 0 ||
		policy.Event == nil {
		return nil, "", nonRetryable(types.NewAppError(
			types.CodeValidation, "event qualifier outcome is invalid", nil))
	}
	byID := make(map[int64]types.ContentItem, len(items))
	for _, item := range items {
		byID[item.ID] = item
	}
	admissionStart := window.Start
	if policy.LatePolicy == observation.LateBounded {
		admissionStart = admissionStart.Add(
			-time.Duration(policy.AllowedLatenessSecs) * time.Second)
	}
	admission := observation.Window{Start: admissionStart, End: window.End}
	out := make([]types.ContentItem, 0, len(result.Events))
	seenKeys := make(map[string]struct{}, len(result.Events))
	for _, event := range result.Events {
		if event.EventType != policy.Event.EventKind ||
			event.Subject != policy.Event.Subject ||
			strings.TrimSpace(event.ReleaseIdentifier) == "" ||
			!qualificationAllowed(
				policy.Event.Qualification,
				observation.Qualification(event.Qualification)) ||
			len(event.EvidenceContentIDs) == 0 {
			return nil, "", nonRetryable(types.NewAppError(
				types.CodeValidation, "qualified event differs from approved definition", nil))
		}
		occurredAt, err := time.Parse(time.RFC3339, event.OccurredAt)
		if err != nil || !admission.Contains(occurredAt.UTC()) {
			return nil, "", nonRetryable(types.NewAppError(
				types.CodeValidation, "qualified event time is outside the approved window", err))
		}
		var primary types.ContentItem
		for index, contentID := range event.EvidenceContentIDs {
			candidate, ok := byID[contentID]
			if !ok || candidate.PublishedAt == nil ||
				!candidate.PublishedAt.UTC().Truncate(time.Second).
					Equal(occurredAt.UTC().Truncate(time.Second)) {
				return nil, "", nonRetryable(types.NewAppError(
					types.CodeValidation, "qualified event cited unverifiable evidence", nil))
			}
			if policy.Evidence.Requirement == observation.EvidenceOfficialRequired &&
				!observation.OfficialURLAllowed(
					candidate.URL, policy.Evidence.OfficialDomains) {
				return nil, "", nonRetryable(types.NewAppError(
					types.CodeValidation, "qualified event lacks approved official evidence", nil))
			}
			if index == 0 {
				primary = candidate
			}
		}
		releaseIdentity := canonicalReleaseIdentity(event.ReleaseIdentifier)
		if releaseIdentity == "" {
			return nil, "", nonRetryable(types.NewAppError(
				types.CodeValidation, "qualified event release identity is invalid", nil))
		}
		eventKey := qualifiedEventKey(
			policyDigest, event.EventType, releaseIdentity)
		if _, duplicate := seenKeys[eventKey]; duplicate {
			continue
		}
		seenKeys[eventKey] = struct{}{}
		evidence, _ := json.Marshal(event)
		primary.ObservationEventKey = eventKey
		primary.ObservationPolicyDigest = policyDigest
		primary.ObservationEventJSON = evidence
		out = append(out, primary)
	}
	if len(out) == 0 {
		return nil, "no_match", nil
	}
	return out, "match", nil
}

func qualificationAllowed(
	approved, actual observation.Qualification,
) bool {
	if approved == observation.QualificationEither {
		return actual == observation.QualificationAnnouncement ||
			actual == observation.QualificationGeneralAvailability
	}
	return approved == actual
}

func qualifiedEventKey(policyDigest, eventType, releaseIdentifier string) string {
	sum := sha256.Sum256([]byte(
		policyDigest + "\x00" + eventType + "\x00" + releaseIdentifier))
	return hex.EncodeToString(sum[:])
}

func canonicalReleaseIdentity(value string) string {
	stop := map[string]struct{}{
		"announce": {}, "announced": {}, "announcement": {},
		"introduce": {}, "introduced": {}, "introducing": {},
		"launch": {}, "launched": {}, "release": {}, "released": {},
		"model": {}, "official": {}, "general": {}, "availability": {},
	}
	tokens := strings.FieldsFunc(strings.ToLower(strings.TrimSpace(value)), func(r rune) bool {
		return (r < 'a' || r > 'z') && (r < '0' || r > '9')
	})
	var identity strings.Builder
	for _, token := range tokens {
		if _, ignored := stop[token]; ignored {
			continue
		}
		identity.WriteString(token)
	}
	if identity.Len() == 0 || identity.Len() > 128 {
		return ""
	}
	return identity.String()
}

func observedEventForPush(item types.ContentItem) (observation.QualifiedEvent, error) {
	if item.ObservationEventKey == "" ||
		item.ObservationPolicyDigest == "" ||
		len(item.ObservationEventJSON) == 0 {
		return observation.QualifiedEvent{}, types.NewAppError(
			types.CodeValidation, "qualified event push metadata is incomplete", nil)
	}
	var event eventqualifier.Event
	if err := json.Unmarshal(item.ObservationEventJSON, &event); err != nil {
		return observation.QualifiedEvent{}, types.NewAppError(
			types.CodeValidation, "qualified event push evidence is invalid", err)
	}
	occurredAt, err := time.Parse(time.RFC3339, event.OccurredAt)
	if err != nil || event.EventType == "" || event.Subject == "" {
		return observation.QualifiedEvent{}, types.NewAppError(
			types.CodeValidation, "qualified event push identity is invalid", err)
	}
	return observation.QualifiedEvent{
		PolicyDigest: item.ObservationPolicyDigest,
		EventKey:     item.ObservationEventKey,
		EventType:    event.EventType,
		Subject:      event.Subject,
		OccurredAt:   occurredAt.UTC(),
		EvidenceJSON: append(json.RawMessage(nil), item.ObservationEventJSON...),
	}, nil
}

// Score 逐条打分（并发扇出，见 mapConcurrent）。同一 TraceID 串起整批的 llm_calls
// 便于事后追踪。单条失败跳过；整批全失败（大概率 LLM 不可用）返回错误触发重试。
//
// 改并发的理由是实测：生产批次 33–45 条、单条平均 709ms，串行即 32 秒纯排队等网络，
// 而 llm.Client 早已配好 5 路并发闸门却只被喂进 1 个。顺带也把 activity 的
// StartToCloseTimeout=120s 从"正在变薄的余量"拉回安全区（45 条 × 最坏 1372ms 已达 62 秒）。
func (a *Activities) Score(ctx context.Context, in ScoreIn) ([]types.ScoredItem, error) {
	snapshot, compiled, err := a.loadAuthoritativeCompiledRun(ctx, in.UserID, in.Run)
	if err != nil {
		return nil, retryableOrNot(err)
	}
	// D9 闸门（bug 狩猎 2026-07-19 HIGH：此前只有 EvolveProfile/Fetch 有闸，
	// Fetch 之后软删的租户仍会在这里烧一整批 LLM 打分钱）。返回空切片走
	// score 闸门的空批正常终态，与 EvolveProfile 的处理同一条理由：报错会重试，
	// 而重试同样会被拒。
	if !compiled && a.refuseIfTenantGone(ctx, in.UserID, "score") {
		return nil, nil
	}
	taskInstruction := ""
	var compiledConsumer compiledScorerV1
	var compiledPolicy scorerpkg.PolicyV1
	if compiled {
		modelClient, err := a.resolveCompiledModelPolicyV1(snapshot.Policy.ModelPolicy)
		if err != nil {
			return nil, retryableOrNot(err)
		}
		var ok bool
		compiledConsumer, ok = a.scorer.(compiledScorerV1)
		if !ok {
			return nil, nonRetryable(types.NewAppError(types.CodeInternal,
				"compiled scorer v1 is unsupported", nil))
		}
		compiledPolicy, err = scorerpkg.PrepareCompiledPolicyV1(
			snapshot.Policy.PromptPolicy, snapshot.Policy.ModelPolicy,
			snapshot.Policy.QuotaPolicy, modelClient)
		if err != nil {
			return nil, nonRetryable(types.NewAppError(types.CodeValidation,
				"compiled score policy is invalid", err))
		}
		if snapshot.Policy.PromptPolicy.TaskInstructionEnabled {
			taskInstruction = snapshot.Definition.PlaybookContent
		}
	} else {
		taskInstruction = a.loadTaskInstruction(ctx, in.UserID, in.ScheduleID, in.TraceID, "score")
	}
	// 并发扇出里各 goroutine 都可能撞到配额，用原子标记而非普通 bool。
	var quotaHit atomic.Bool
	var authorizationOnce sync.Once
	var authorizationErr error
	scored := mapConcurrent(ctx, in.Items, parBatchFanout,
		func(ctx context.Context, item types.ContentItem) (types.ScoredItem, error) {
			authorize := func(effectCtx context.Context) error {
				err := a.authorizeCompiledEffectV1(effectCtx, in.UserID, in.Run)
				if err != nil {
					authorizationOnce.Do(func() { authorizationErr = err })
				}
				return err
			}
			if compiled {
				if err := authorize(ctx); err != nil {
					return types.ScoredItem{}, err
				}
			}
			beforeSpend := func(effectCtx context.Context, amount float64) error {
				err := a.consumeCompiledLLMQuotaV1(
					effectCtx, in.UserID, in.Run, snapshot.Policy.QuotaPolicy, amount)
				if err != nil && !errors.Is(err, storepkg.ErrQuotaExceeded) {
					authorizationOnce.Do(func() { authorizationErr = err })
				}
				return err
			}
			var s float64
			var err error
			if compiled {
				s, err = compiledConsumer.ScoreWithPolicyV1(
					ctx, snapshot.Definition.TenantID, in.UserID, item,
					in.TraceID, taskInstruction, compiledPolicy, beforeSpend)
			} else {
				s, err = a.scorer.Score(ctx, in.UserID, item, in.TraceID, taskInstruction)
			}
			if err != nil {
				return types.ScoredItem{}, err
			}
			s = max(0, min(100, s+item.ObservationScorePenalty))
			return types.ScoredItem{Item: item, Score: s}, nil
		},
		func(item types.ContentItem, err error) {
			if isQuotaErr(err) {
				quotaHit.Store(true)
			}
			logPipelineItemFailure(ctx, "score: 单条打分失败，跳过", item.ID, in.TraceID, err)
		})
	if len(scored) == 0 && len(in.Items) > 0 {
		if authorizationErr != nil {
			return nil, retryableOrNot(authorizationErr)
		}
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
	snapshot, compiled, err := a.loadAuthoritativeCompiledRun(ctx, in.UserID, in.Run)
	if err != nil {
		return nil, retryableOrNot(err)
	}
	if compiled {
		var frozenScope PushScope
		if err := json.Unmarshal(snapshot.Definition.ScopeJSON, &frozenScope); err != nil ||
			frozenScope.TopN < 0 {
			return nil, nonRetryable(types.NewAppError(types.CodeValidation,
				"compiled task scope is invalid", err))
		}
		n = frozenScope.TopN
		if n <= 0 {
			n = defaultTopN
		}
	}

	// 任务门槛档位：有 ScheduleID 才查库；查库失败**降级兜底而非中断推送**——
	// 与画像读取失败降级空画像同一条先例（profilehint）：门槛是过滤器不是闸门，
	// DB 抖一下就把整批推送打死，比偶尔按兜底档放行几条弱相关内容伤害大得多。
	// 空串（未设置/行不存在/即时触发无任务）由 MinKeepScore 按 DefaultStrictness 兜底。
	strictness := types.PushStrictness("")
	if compiled {
		strictness = snapshot.Definition.Strictness
	} else if in.ScheduleID != "" {
		v, err := a.store.GetScheduleStrictness(ctx, in.ScheduleID)
		if err != nil {
			slog.Warn("select: 查询任务门槛档位失败，按全局兜底档过滤",
				"schedule_id", in.ScheduleID, "trace_id", in.TraceID, "err", err)
		} else {
			strictness = v
		}
	}
	threshold := strictness.MinKeepScore()

	// 门槛过滤在 RankTopN 之前（契约 §6 修订）：低于最低保留分的条目不参与择优——
	// 纯 TopN 会在"这批全不相关"时硬凑满员（2026-07-19 deliveries 155-159 五张
	// 0 分卡实锤）。
	kept := make([]types.ScoredItem, 0, len(in.Scored))
	for _, si := range in.Scored {
		if si.Score >= float64(threshold) {
			kept = append(kept, si)
		}
	}
	if dropped := len(in.Scored) - len(kept); dropped > 0 {
		slog.Info("select: 门槛过滤",
			"trace_id", in.TraceID, "schedule_id", in.ScheduleID, "strictness", strictness,
			"threshold", threshold, "in", len(in.Scored), "kept", len(kept))
	}
	return selector.RankTopN(kept, n, time.Now()), nil
}

// CardGen 逐条生成解读正文（并发扇出，见 mapConcurrent）。
// 单条失败跳过；整批全失败返回错误触发重试。
//
// 顺序保持在这里比 Score 更要紧：cards 的顺序**直接决定聚合卡里条目的排列**
// （Push 按本切片顺序拼卡），而 Score 的产出还要过一遍 RankTopN 重排。
// 换句话说，这里若按完成先后收集，同一批内容每次推送的卡面顺序都会不一样。
func (a *Activities) CardGen(ctx context.Context, in CardGenIn) ([]GeneratedCard, error) {
	snapshot, compiled, err := a.loadAuthoritativeCompiledRun(ctx, in.UserID, in.Run)
	if err != nil {
		return nil, retryableOrNot(err)
	}
	// D9 闸门（同 Score，bug 狩猎 2026-07-19 HIGH）：不为已注销租户生成解读正文。
	if !compiled && a.refuseIfTenantGone(ctx, in.UserID, "cardgen") {
		return nil, nil
	}
	taskInstruction := ""
	var compiledConsumer compiledCardGeneratorV1
	var compiledPolicy cardgenpkg.PolicyV1
	if compiled {
		modelClient, err := a.resolveCompiledModelPolicyV1(snapshot.Policy.ModelPolicy)
		if err != nil {
			return nil, retryableOrNot(err)
		}
		var ok bool
		compiledConsumer, ok = a.cardgen.(compiledCardGeneratorV1)
		if !ok {
			return nil, nonRetryable(types.NewAppError(types.CodeInternal,
				"compiled card generator v1 is unsupported", nil))
		}
		compiledPolicy, err = cardgenpkg.PrepareCompiledPolicyV1(
			snapshot.Policy.PromptPolicy, snapshot.Policy.ModelPolicy,
			snapshot.Policy.QuotaPolicy, modelClient)
		if err != nil {
			return nil, nonRetryable(types.NewAppError(types.CodeValidation,
				"compiled card policy is invalid", err))
		}
		if snapshot.Policy.PromptPolicy.TaskInstructionEnabled {
			taskInstruction = snapshot.Definition.PlaybookContent
		}
	} else {
		taskInstruction = a.loadTaskInstruction(ctx, in.UserID, in.ScheduleID, in.TraceID, "cardgen")
	}
	var quotaHit atomic.Bool
	var authorizationOnce sync.Once
	var authorizationErr error
	cards := mapConcurrent(ctx, in.Items, parBatchFanout,
		func(ctx context.Context, si types.ScoredItem) (GeneratedCard, error) {
			authorize := func(effectCtx context.Context) error {
				err := a.authorizeCompiledEffectV1(effectCtx, in.UserID, in.Run)
				if err != nil {
					authorizationOnce.Do(func() { authorizationErr = err })
				}
				return err
			}
			if compiled {
				if err := authorize(ctx); err != nil {
					return GeneratedCard{}, err
				}
			}
			beforeSpend := func(effectCtx context.Context, amount float64) error {
				err := a.consumeCompiledLLMQuotaV1(
					effectCtx, in.UserID, in.Run, snapshot.Policy.QuotaPolicy, amount)
				if err != nil && !errors.Is(err, storepkg.ErrQuotaExceeded) {
					authorizationOnce.Do(func() { authorizationErr = err })
				}
				return err
			}
			var body string
			var err error
			if compiled {
				body, err = compiledConsumer.GenerateWithPolicyV1(
					ctx, snapshot.Definition.TenantID, in.UserID, si,
					in.TraceID, taskInstruction, compiledPolicy, beforeSpend)
			} else {
				body, err = a.cardgen.Generate(ctx, in.UserID, si, in.TraceID, taskInstruction)
			}
			if err != nil {
				return GeneratedCard{}, err
			}
			return GeneratedCard{Scored: si, BodyMD: body}, nil
		},
		func(si types.ScoredItem, err error) {
			if isQuotaErr(err) {
				quotaHit.Store(true)
			}
			logPipelineItemFailure(ctx, "cardgen: 单条生成失败，跳过", si.Item.ID, in.TraceID, err)
		})
	if len(cards) == 0 && len(in.Items) > 0 {
		if authorizationErr != nil {
			return nil, retryableOrNot(authorizationErr)
		}
		if quotaHit.Load() {
			return nil, nonRetryable(types.NewAppError(types.CodeQuotaExceeded,
				"本租户 LLM 额度已用尽，本轮跳过出卡", nil))
		}
		return nil, types.NewAppError(types.CodeLLMUnavailable, "整批卡片生成全部失败", nil)
	}
	return cards, nil
}

// logPipelineItemFailure keeps arbitrary downstream error strings out of app
// logs. LLM/provider errors can echo the complete prompt; structured code,
// concrete Go type and retryability are enough to triage before consulting the
// llm_calls audit ledger by trace ID.
func logPipelineItemFailure(ctx context.Context, message string, contentItemID int64, traceID string, err error) {
	slog.WarnContext(ctx, message,
		"content_item_id", contentItemID,
		"trace_id", traceID,
		"error_code", types.CodeOf(err),
		"error_type", fmt.Sprintf("%T", err),
		"retryable", types.IsRetryable(err))
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
	if _, compiled, err := a.loadAuthoritativeCompiledRun(ctx, in.UserID, in.Run); compiled {
		if err != nil {
			return retryableOrNot(err)
		}
		if in.ScheduleID != in.Run.TaskID {
			return nonRetryable(types.NewAppError(types.CodeValidation,
				"compiled empty batch task identity does not match", nil))
		}
		expected, identityErr := activityRunIdentityV1(ctx, in.UserID, in.Run)
		if identityErr != nil {
			return retryableOrNot(identityErr)
		}
		_, skipped, writeErr := a.compiledStore.RecordEmptyPushBatchForTaskRunV1(
			ctx, expected, in.Run.Snapshot, in.TraceID, in.Gate, in.Counts)
		if writeErr != nil {
			return retryableOrNot(writeErr)
		}
		if skipped {
			slog.Info("空批次记账被护栏拦下：该 trace 已有真实批次，不覆写",
				"user_id", in.UserID, "trace_id", in.TraceID, "gate", in.Gate)
		}
		return nil
	}
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

type pushPendingItem struct {
	delID    int64
	input    feedback.CardInput
	eventKey string
}

type plannedPushChunk struct {
	items    []pushPendingItem
	cardJSON string
}

func planPushChunks(
	pending []pushPendingItem,
	marker string,
	build func([]pushPendingItem, string) string,
) []plannedPushChunk {
	chunks := make([]plannedPushChunk, 0)
	for start := 0; start < len(pending); {
		size := min(aggMaxItemsPerCard, len(pending)-start)
		cardJSON := build(pending[start:start+size], marker)
		for len(cardJSON) > aggMaxCardBytes && size > 1 {
			size = max(size/2, 1)
			cardJSON = build(pending[start:start+size], marker)
		}
		chunks = append(chunks, plannedPushChunk{
			items: pending[start : start+size], cardJSON: cardJSON,
		})
		start += size
	}
	return chunks
}

// Push 建批次 → 逐条建 Delivery → 主动推送 → 标记已发 → 收尾批次状态。
// 收件人是飞书 owner（M3 单用户）；无 owner 直接失败。单卡推送失败跳过，
// 只要有一张成功就算 done，全失败则 failed 并返回错误。
func (a *Activities) Push(ctx context.Context, in PushIn) error {
	snapshot, compiled, err := a.loadAuthoritativeCompiledRun(ctx, in.UserID, in.Run)
	if err != nil {
		return retryableOrNot(err)
	}
	taskTitle := in.TaskTitle
	var compiledIdentity types.RunIdentity
	var frozenSourceIDs []int64
	var frozenSources map[int64]runcontext.SourceV1
	if compiled {
		if in.ScheduleID != in.Run.TaskID {
			return nonRetryable(types.NewAppError(types.CodeValidation,
				"compiled push task identity does not match", nil))
		}
		compiledIdentity, err = activityRunIdentityV1(ctx, in.UserID, in.Run)
		if err != nil {
			return retryableOrNot(err)
		}
		taskTitle = snapshot.Definition.NLDescription
		frozenSourceIDs = make([]int64, len(snapshot.Definition.Sources))
		frozenSources = make(map[int64]runcontext.SourceV1, len(snapshot.Definition.Sources))
		for i, source := range snapshot.Definition.Sources {
			frozenSourceIDs[i] = source.SourceID
			frozenSources[source.SourceID] = source
		}
		if len(frozenSourceIDs) == 0 {
			return nonRetryable(types.NewAppError(types.CodeValidation,
				"compiled push has no frozen sources", nil))
		}
	}
	// D9 闸门（同 Score/CardGen，bug 狩猎 2026-07-19 HIGH）：卡片不发给已注销
	// 租户的用户——这是整条管道最后也最用户可见的一道；返回 nil 是正常终态。
	if !compiled && a.refuseIfTenantGone(ctx, in.UserID, "push") {
		return nil
	}
	owner := a.feishu.OwnerOpenID()
	if !compiled && owner == "" {
		// 无 owner 是"还没人给机器人发过消息"，属确定性前置条件缺失，重试只是重复失败——
		// 包成不可重试的 ApplicationError（Type=NOT_FOUND），让 NonRetryableErrorTypes 立即终止（修 #2）。
		return nonRetryable(types.NewAppError(types.CodeNotFound, "尚未捕获飞书 owner，无法推送", nil))
	}

	// 用确定性 traceID 作幂等键：Temporal 重试 Push 时复用同一 batch，不再每次新建批次（修 #1 CRITICAL 地基）。
	var batchID int64
	var recoveryOnly bool
	if compiled {
		batchID, recoveryOnly, err = a.compiledStore.CreateOrRecoverPushBatchForTaskRunV1(
			ctx, compiledIdentity, in.Run.Snapshot, in.TraceID)
	} else {
		batchID, err = a.store.CreatePushBatchIdempotent(
			ctx, in.UserID, in.TraceID, in.ScheduleID)
	}
	if err != nil {
		return err
	}
	if recoveryOnly {
		// A previous attempt durably recorded every delivery as sent but exited
		// before the terminal batch receipt. Live authority may now be revoked;
		// the store only returns this state from exact immutable sent evidence.
		// Finish that receipt immediately and never rebuild or resend the card.
		return retryableOrNot(a.compiledStore.MarkPushBatchDoneReceiptV1(
			ctx, compiledIdentity, in.Run.Snapshot, in.TraceID, batchID))
	}
	if owner == "" {
		// Compiled receipt-only recovery above must remain possible after the
		// owner identity disappears: the external card is already sent and this
		// branch only applies to a new send. Legacy keeps its original pre-batch
		// owner check above.
		return nonRetryable(types.NewAppError(types.CodeNotFound,
			"尚未捕获飞书 owner，无法推送", nil))
	}

	// 聚合卡改版（card-redesign-spec.md 附录 A，2026-07-18）：一批一张聚合卡，
	// 不再一条内容一张卡。deliveries 仍 per-content（数据模型不变），
	// 同批各 delivery 共享同一 feishu_message_id——重建路径靠它找兄弟条目。
	//
	// 幂等语义相应调整：先为全部候选建 delivery（幂等），已 sent 的条目不再进新卡
	// （它们已在上次重试成功发出的那张卡里）；只有存在未发条目时才推一张新聚合卡。
	anySent := false
	sentThisAttempt := false
	var pending []pushPendingItem
	skippedObservedEvents := 0
	for _, card := range in.Cards {
		eventKey := card.Scored.Item.ObservationEventKey
		if compiled && eventKey != "" {
			if a.observationStore == nil {
				return nonRetryable(types.NewAppError(types.CodeInternal,
					"observation delivery store is not configured", nil))
			}
			event, eventErr := observedEventForPush(card.Scored.Item)
			if eventErr != nil {
				return nonRetryable(eventErr)
			}
			accepted, reserveErr := a.observationStore.ReserveObservedEventV1(
				ctx, compiledIdentity, in.Run.Snapshot, event)
			if reserveErr != nil {
				return retryableOrNot(reserveErr)
			}
			if !accepted {
				// Another successful or still-live run already owns this exact
				// task event. It must not create even a pending delivery.
				skippedObservedEvents++
				continue
			}
		}
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
		var delID int64
		var existed, sentAlready bool
		var ierr error
		if compiled {
			delID, existed, sentAlready, ierr = a.compiledStore.InsertDeliveryForTaskRunV1(
				ctx, compiledIdentity, in.Run.Snapshot, in.TraceID, d)
		} else {
			delID, existed, sentAlready, ierr = a.store.InsertDeliveryIdempotent(ctx, d)
		}
		if ierr != nil {
			if compiled && eventKey != "" {
				// The event reservation is durable. Retrying the activity lets
				// the same run reuse it; silently skipping would strand it.
				return retryableOrNot(ierr)
			}
			slog.Warn("push: 建投递记录失败，跳过", "trace_id", in.TraceID, "err", ierr)
			continue
		}
		if compiled && card.Scored.Item.ObservationEventKey != "" {
			if a.observationStore == nil {
				return nonRetryable(types.NewAppError(types.CodeInternal,
					"observation delivery store is not configured", nil))
			}
			if err := a.observationStore.BindObservedEventDeliveryV1(
				ctx, compiledIdentity, in.Run.Snapshot,
				card.Scored.Item.ObservationPolicyDigest,
				card.Scored.Item.ObservationEventKey, delID,
			); err != nil {
				return retryableOrNot(err)
			}
		}
		if existed && sentAlready {
			// 重试时该 (batch, content) 已发过：不进新卡，绝不重复推——幂等核心（#1 CRITICAL）。
			if compiled && eventKey != "" {
				if err := a.observationStore.MarkObservedEventDeliveredV1(
					ctx, compiledIdentity, in.Run.Snapshot, delID,
				); err != nil {
					return retryableOrNot(err)
				}
			}
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
		// 源归属（#8）：legacy 默认用全局首发源，隔离任务再查当前任务命中源。
		// compiled 运行只在冻结 source 集合里归属，并直接使用快照里的标题/平台；
		// 任务在运行中改源或源元数据漂移都不能改变这一批卡片。
		displaySourceID := card.Scored.Item.SourceID
		if compiled && card.Scored.Item.ID != 0 {
			if sourceID, ok, sourceErr := a.compiledStore.SourceForContentFromIDs(
				ctx, card.Scored.Item.ID, frozenSourceIDs,
			); sourceErr == nil && ok {
				displaySourceID = sourceID
			} else if _, frozen := frozenSources[displaySourceID]; !frozen {
				displaySourceID = 0
			}
		} else if in.ScheduleID != "" && card.Scored.Item.ID != 0 {
			if tsid, ok, terr := a.store.ScheduleSourceForContent(ctx, card.Scored.Item.ID, in.ScheduleID); terr == nil && ok {
				displaySourceID = tsid
			}
		}
		if displaySourceID != 0 {
			if compiled {
				if source, ok := frozenSources[displaySourceID]; ok {
					ci.SourceTitle = source.Title
					ci.Platform = source.Platform
				}
			} else if src, serr := a.store.GetSource(ctx, displaySourceID); serr == nil {
				ci.SourceTitle = src.Title
				ci.Platform = src.Platform
			}
		}
		pending = append(pending, pushPendingItem{
			delID: delID, input: ci,
			eventKey: eventKey,
		})
	}
	if len(pending) == 0 && skippedObservedEvents > 0 && !anySent {
		// A concurrent/prior run already owns every qualified event. This is a
		// successful no-op observation cycle, not a push failure.
		if err := a.compiledStore.UpdatePushBatchStatusForTaskRunV1(
			ctx, compiledIdentity, in.Run.Snapshot, in.TraceID,
			batchID, types.BatchStatusDone,
		); err != nil {
			return retryableOrNot(err)
		}
		return nil
	}

	// 分块发送：每卡条数封顶 + 构卡后字节硬校验（附录 A 吸收自被否方案的两点之一）。
	// 超限拆卡而非静默截断——静默丢条目会让"已打分未送达"的内容永远消失。
	//
	// 拆分用显式 size 内环收敛（初版把对半结果写进 end 再 continue，外层循环顶部
	// 重算 end 会丢弃拆分——超大块死循环；size 单调递减到 1 保证必然终止）。
	failedItems := 0
	// anyRetryableFail：本轮是否有**可重试**的块失败。全部失败都是确定性错误
	//（如 200673 卡片非法）时整活动包成不可重试——SendCard 层的 Retryable=false
	// 若只在这里被聚合成新 AppError 就会丢失（bug 狩猎批审查发现），Temporal
	// 会白重试三次必然同败的卡。
	anyRetryableFail := false
	buildChunk := func(chunk []pushPendingItem, effectID string) string {
		items := make([]feedback.CardInput, len(chunk))
		for i, p := range chunk {
			items[i] = p.input
		}
		var title, tmpl string
		if a.aggHeader != nil {
			title, tmpl = a.aggHeader(taskTitle, len(items))
		}
		return a.buildAggCard(feedback.AggregateCardInput{
			HeaderTitle: title, HeaderTemplate: tmpl,
			EffectID: effectID, Items: items,
		})
	}
	effectEnabled := compiled &&
		a.pushEffectStore != nil &&
		a.pushEffectCanaryTaskID != "" &&
		a.pushEffectCanaryTaskID == compiledIdentity.TaskID
	planningMarker := ""
	if effectEnabled {
		planningMarker = pushEffectMarkerWidthSeed
	}
	plannedChunks := planPushChunks(pending, planningMarker, buildChunk)
	for chunkIndex, planned := range plannedChunks {
		effectID := ""
		if effectEnabled {
			effectID = pushEffectID(compiledIdentity, chunkIndex)
		}
		cardJSON := planned.cardJSON
		if effectEnabled {
			cardJSON = buildChunk(planned.items, effectID)
		}
		chunk := planned.items
		if len(cardJSON) > aggMaxCardBytes {
			slog.Warn("push: 单条内容构卡即超字节上限，硬发（可能被飞书拒）",
				"delivery_id", chunk[0].delID, "bytes", len(cardJSON))
		}

		if compiled {
			if err := a.authorizeCompiledEffectV1(ctx, in.UserID, in.Run); err != nil {
				return retryableOrNot(err)
			}
		}
		var msgID string
		var perr error
		if effectEnabled {
			msgID, perr = a.sendDurablePushChunk(
				ctx,
				compiledIdentity,
				in.Run.Snapshot,
				batchID,
				chunkIndex,
				len(plannedChunks),
				chunk,
				cardJSON,
				effectID,
				owner,
			)
		} else {
			msgID, perr = a.pusher.Push(ctx, owner, cardJSON)
		}
		if perr != nil {
			slog.Warn("push: 聚合卡推送失败，跳过该块", "trace_id", in.TraceID,
				"items", len(chunk), "err", perr)
			failedItems += len(chunk)
			if types.IsRetryable(perr) {
				anyRetryableFail = true
			}
			continue
		}
		if effectEnabled {
			anySent = true
			sentThisAttempt = true
			continue
		}
		// MarkDeliverySent and the final batch status are the durable receipt of
		// the just-authorized external send. Do not insert another revocation
		// check between delivery and receipt: stopping there would make a sent
		// card look pending and a retry could duplicate it. C3 will strengthen
		// this remaining send/receipt window with a durable effect checkpoint.
		var compiledReceiptErr error
		for _, p := range chunk {
			var merr error
			if compiled {
				merr = a.compiledStore.MarkDeliverySentForTaskRunV1(
					ctx, compiledIdentity, in.Run.Snapshot, in.TraceID,
					batchID, p.delID, msgID, json.RawMessage(cardJSON), time.Now())
			} else {
				merr = a.store.MarkDeliverySent(
					ctx, p.delID, msgID, json.RawMessage(cardJSON), time.Now())
			}
			if merr != nil {
				slog.Warn("push: 标记已发失败（消息已送达）", "delivery_id", p.delID,
					"feishu_message_id", msgID, "err", merr)
				if compiled && compiledReceiptErr == nil {
					compiledReceiptErr = merr
				}
			} else if compiled && p.eventKey != "" {
				merr = a.observationStore.MarkObservedEventDeliveredV1(
					ctx, compiledIdentity, in.Run.Snapshot, p.delID)
				if merr != nil && compiledReceiptErr == nil {
					compiledReceiptErr = merr
				}
			}
		}
		anySent = true
		sentThisAttempt = true
		if compiledReceiptErr != nil {
			// The external send succeeded, so claiming batch=done while any
			// delivery receipt is missing would make the loss permanent. Keep the
			// batch pending and retry. The remaining send/receipt crash window is
			// handled by C3's durable effect checkpoint.
			return retryableOrNot(compiledReceiptErr)
		}
	}

	if !anySent {
		if compiled {
			err = a.compiledStore.UpdatePushBatchStatusForTaskRunV1(
				ctx, compiledIdentity, in.Run.Snapshot, in.TraceID,
				batchID, types.BatchStatusFailed)
		} else {
			err = a.store.UpdatePushBatchStatus(ctx, batchID, types.BatchStatusFailed)
		}
		if err != nil {
			return retryableOrNot(err)
		}
		ae := types.NewAppError(types.CodePushFailed, "本批次全部推送失败", nil)
		if !anyRetryableFail {
			// 全部失败且全为确定性拒收：重试必然逐字复演，包成不可重试让 Temporal
			// 立即终止（批次已标 failed，探针/看板可见），而不是烧满重试预算。
			return nonRetryable(ae)
		}
		return ae
	}
	// 部分块失败（对抗审查 HIGH）：**不结算 done、返回可重试错误**。批次终态留待重试
	// 收敛——sentAlready 幂等保证重试不重发成功块，只补失败块；若记 done 并吞掉错误，
	// 本轮就会把失败条目伪装成成功。重试耗尽时批次停在 pending，作为可见异常留给
	// 探针；事件模式下，超过接管窗的 qualified+pending 绑定会在下一运行重新进入
	// compiled 候选并转移账本所有权，避免永久吞事件。
	if failedItems > 0 {
		ae := types.NewAppError(types.CodePushFailed,
			fmt.Sprintf("部分推送失败（%d 条未送达），等待重试补发", failedItems), nil)
		if !anyRetryableFail {
			// 失败块全为确定性拒收：重试补发不可能成功，立即终止。
			// 成功块已送达并标记，失败块留 pending 由探针暴露（与重试耗尽同终态）。
			return nonRetryable(ae)
		}
		return ae
	}
	if compiled {
		if sentThisAttempt {
			err = a.compiledStore.MarkPushBatchDoneReceiptV1(
				ctx, compiledIdentity, in.Run.Snapshot, in.TraceID, batchID)
		} else {
			err = a.compiledStore.UpdatePushBatchStatusForTaskRunV1(
				ctx, compiledIdentity, in.Run.Snapshot, in.TraceID,
				batchID, types.BatchStatusDone)
		}
	} else {
		err = a.store.UpdatePushBatchStatus(ctx, batchID, types.BatchStatusDone)
	}
	if err != nil {
		return retryableOrNot(err)
	}
	return nil
}

func pushEffectID(identity types.RunIdentity, chunkIndex int) string {
	name := fmt.Sprintf(
		"%d|%d|%s|%s|%s|%s|%d",
		identity.TenantID,
		identity.UserID,
		identity.TaskID,
		identity.TemporalRunID,
		pushEffectStepID,
		identity.TemporalWorkflowID,
		chunkIndex,
	)
	return uuid.NewSHA1(pushEffectUUIDNamespace, []byte(name)).String()
}

func (a *Activities) sendDurablePushChunk(
	ctx context.Context,
	identity types.RunIdentity,
	ref types.RunSnapshotRef,
	batchID int64,
	chunkIndex int,
	chunkCount int,
	chunk []pushPendingItem,
	cardJSON string,
	effectID string,
	ownerOpenID string,
) (string, error) {
	if a.pushEffectStore == nil || !activity.IsActivity(ctx) ||
		batchID <= 0 || chunkIndex < 0 || chunkCount <= 0 ||
		chunkIndex >= chunkCount || len(chunk) == 0 ||
		effectID == "" || ownerOpenID == "" {
		return "", types.NewAppError(
			types.CodeValidation,
			"durable push effect input is invalid",
			nil,
		)
	}
	ownerChatID := a.feishu.OwnerChatID()
	appIdentity := a.feishu.AppIdentity()
	if ownerChatID == "" || appIdentity == "" {
		return "", types.NewAppError(
			types.CodeConflict,
			"durable push effect provider identity is unavailable",
			nil,
		)
	}
	info := activity.GetInfo(ctx)
	scheduledAt := info.ScheduledTime.UTC().Truncate(time.Microsecond)
	if scheduledAt.IsZero() {
		return "", types.NewAppError(
			types.CodeValidation,
			"durable push effect scheduled time is unavailable",
			nil,
		)
	}
	deliveryIDs := make([]int64, len(chunk))
	for i := range chunk {
		deliveryIDs[i] = chunk[i].delID
	}
	prepared := pusheffect.Prepared{
		ID: effectID, TenantID: identity.TenantID, UserID: identity.UserID,
		TaskID: identity.TaskID, RunSnapshotID: ref.SnapshotID,
		RunID: identity.TemporalRunID, StepID: pushEffectStepID,
		ChunkIndex: chunkIndex, ChunkCount: chunkCount,
		BatchID: batchID, DeliveryIDs: deliveryIDs,
		Provider: pushEffectProvider, AppIdentity: appIdentity,
		ProviderChatID: ownerChatID, Target: ownerOpenID,
		Card: []byte(cardJSON), ProviderUUID: effectID,
		IdempotencyExpiresAt: scheduledAt.Add(time.Hour),
	}
	effect, err := a.pushEffectStore.CreatePushEffect(ctx, prepared)
	if err != nil {
		return "", err
	}

	leaseMaterial := fmt.Sprintf(
		"%s/%s/%s/%d",
		identity.TemporalWorkflowID,
		identity.TemporalRunID,
		info.ActivityID,
		info.Attempt,
	)
	leaseOwner := "push/" + uuid.NewSHA1(
		pushEffectUUIDNamespace, []byte(leaseMaterial),
	).String()
	claim := pusheffect.ClaimParams{
		Scope: effect.Scope(), LeaseOwner: leaseOwner,
		LeaseDuration: pushEffectLeaseDuration,
	}
	wasAmbiguous := effect.Status == pusheffect.StatusAmbiguous
	switch effect.Status {
	case pusheffect.StatusPrepared, pusheffect.StatusDefiniteFailed:
		effect, err = a.pushEffectStore.ClaimPushEffect(ctx, claim)
	case pusheffect.StatusAmbiguous:
		effect, err = a.pushEffectStore.ClaimPushEffectReconciliation(ctx, claim)
	case pusheffect.StatusSent:
		if err := a.pushEffectStore.RecordPushEffectSentWithDeliveries(
			ctx,
			pusheffect.SentReceipt{
				Scope: effect.Scope(), ExpectedFence: effect.Fence,
				ProviderMessageID: effect.ProviderMessageID,
			},
		); err != nil {
			return "", err
		}
		return effect.ProviderMessageID, nil
	default:
		err = types.NewAppError(
			types.CodeConflict,
			"durable push effect is already in flight or blocked",
			nil,
		)
	}
	if err != nil {
		return "", err
	}
	lease := pusheffect.Lease{
		Scope: effect.Scope(), LeaseOwner: effect.LeaseOwner,
		Fence: effect.Fence,
	}
	observation, sendErr := a.pusher.PushWithUUID(
		ctx, ownerOpenID, cardJSON, effect.ProviderUUID)
	if observation.Disposition == pusheffect.AttemptSent &&
		sendErr == nil &&
		observation.MessageID != "" &&
		(observation.ChatID == "" || observation.ChatID == ownerChatID) {
		if err := a.pushEffectStore.RecordPushEffectSentWithDeliveries(
			ctx,
			pusheffect.SentReceipt{
				Scope: effect.Scope(), ExpectedFence: effect.Fence,
				LeaseOwner:        effect.LeaseOwner,
				ProviderMessageID: observation.MessageID,
			},
		); err != nil {
			return "", err
		}
		return observation.MessageID, nil
	}

	failureClass := "provider_response_unknown"
	definite := observation.Disposition == pusheffect.AttemptDefiniteNotSent &&
		!wasAmbiguous
	if observation.Disposition == pusheffect.AttemptSent {
		failureClass = "provider_receipt_invalid"
	} else if definite {
		failureClass = "provider_definite_rejection"
	} else if wasAmbiguous {
		failureClass = "reconciliation_without_positive_receipt"
	}
	failure := pusheffect.FailureParams{
		Lease: lease, Class: failureClass,
		RetryAfter: pushEffectRetryAfter,
	}
	if definite {
		err = a.pushEffectStore.RecordPushEffectDefiniteFailure(ctx, failure)
	} else {
		err = a.pushEffectStore.RecordPushEffectAmbiguous(ctx, failure)
	}
	if err != nil {
		return "", err
	}
	if sendErr != nil {
		return "", sendErr
	}
	return "", types.NewAppError(
		types.CodePushFailed,
		"durable push provider attempt did not produce a valid sent receipt",
		nil,
	)
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
	Run     *CompiledRunInputV1  `json:"run,omitempty"`
	// 门槛上下文（仅 Gate=select 时有值）：空批轻量卡要能回答"抓了多少、最高几分、
	// 门槛多高、怎么调"（Boss 拍板 2026-07-19——31 小时静默停摆史之后，"没内容"
	// 必须与"系统死了"可区分且可解释）。MaxScore 由 workflow 从 scored 纯计算；
	// 档位由本 Activity 按 ScheduleID 自行查库（Select 的返回类型被重放兼容性钉死
	// 为裸切片，带不回来，见 Select 注释）。
	ScheduleID string  `json:"schedule_id,omitempty"`
	MaxScore   float64 `json:"max_score,omitempty"`
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
	snapshot, compiled, err := a.loadAuthoritativeCompiledRun(ctx, in.UserID, in.Run)
	if err != nil {
		return retryableOrNot(err)
	}
	if a.buildNotice == nil {
		return nil // 灰度/测试装配未注入通知构卡：静默跳过。
	}
	owner := a.feishu.OwnerOpenID()
	if owner == "" {
		return nil
	}
	// 门槛过滤致空时补查任务档位（渲染"标准/严格门槛（≥N 分）"）：Select 的返回
	// 类型被重放兼容性钉死为裸切片带不回档位（见 Select 注释）。查库失败降级
	// 空档位 → 文案落"推送底线"通用话术，通知本身照发——同 Select 的降级取舍。
	strictness := types.PushStrictness("")
	if compiled {
		strictness = snapshot.Definition.Strictness
	} else if in.Gate == types.BatchExitGateSelect && in.ScheduleID != "" {
		v, err := a.store.GetScheduleStrictness(ctx, in.ScheduleID)
		if err != nil {
			slog.Warn("push: 空批通知查询门槛档位失败，按通用话术渲染",
				"schedule_id", in.ScheduleID, "trace_id", in.TraceID, "err", err)
		} else {
			strictness = v
		}
	}
	md := emptyResultMarkdown(in, strictness)
	card := a.buildNotice(md)
	if compiled {
		if err := a.authorizeCompiledEffectV1(ctx, in.UserID, in.Run); err != nil {
			return retryableOrNot(err)
		}
	}
	if _, err := a.pusher.Push(ctx, owner, card); err != nil {
		slog.Warn("push: 空结果通知发送失败（不阻断）", "trace_id", in.TraceID, "err", err)
	}
	return nil
}

// emptyResultMarkdown 按退出闸门生成人话说明——"为什么这次没有卡片"。
// 文案与 exit_gate 语义一一对应（enums.go BatchExitGate）。
// strictness 仅 select 闸门消费（空串 = 未设置/查询降级 → 通用话术）。
func emptyResultMarkdown(in NotifyEmptyIn, strictness types.PushStrictness) string {
	c := in.Counts
	nz := func(p *int) int {
		if p == nil {
			return 0
		}
		return *p
	}
	switch in.Gate {
	case types.BatchExitGateFetch:
		return "📭 本次推送没有新内容：所有信源都没有产出新条目。有新内容时定时任务会照常送达。"
	case types.BatchExitGateDedup:
		return fmt.Sprintf("📭 本次推送没有新内容：抓到 %d 条，但都已推送过（去重后 0 条）。有新内容时定时任务会照常送达。", nz(c.Fetched))
	case types.BatchExitGateScore:
		return fmt.Sprintf("📭 本次推送没有新内容：%d 条候选打分后没有达标的。", nz(c.Deduped))
	case types.BatchExitGateSelect:
		// 门槛过滤致空（契约 §6 修订）：这张轻量卡是门槛机制的用户反馈面——
		// 说清"内容有、但都不够相关"，并给出调松的出口。最高分帮用户判断
		// "是真没好内容，还是我门槛设太狠"。
		return fmt.Sprintf("📭 本次推送没有新内容：%d 条内容打分后最高 %.0f 分，均未达到%s——不够相关的内容不值得打扰你。想放宽就在对话里说「这个任务松一点」。",
			nz(c.Scored), in.MaxScore, strictnessPhrase(strictness))
	case types.BatchExitGateQuota:
		// 刻意不说"没有新内容"——内容很可能是有的，只是没额度去处理它。
		// 说成"没内容"会让人去改画像、换信源，白折腾一圈还找不到原因。
		//
		// 三件事必须都说到，缺一件用户就只能干等：**发生了什么**（额度用尽，
		// 不是没内容）、**会不会自己好**（会，按时间恢复）、**要不要做什么**
		// （持续出现才需要找管理员）。只说前两件会让反复撞额度的人一直等下去。
		msg := "⏳ 本次推送暂停：AI 额度已用尽，本轮内容没有处理。\n\n" +
			"额度按时间自动恢复，通常下一轮定时推送即可恢复正常，无需操作。\n" +
			"若连续多轮都收到本提示，说明用量持续超出配额，请联系管理员调整额度。"
		if n := nz(c.Fetched); n > 0 {
			// 报出抓到多少条，是为了让"内容是有的、只是没能处理"这句话可被验证，
			// 而不是一句需要用户信任的断言。
			msg += fmt.Sprintf("\n\n（本轮已抓取 %d 条内容，额度恢复后会重新参与筛选）", n)
		}
		return msg
	case types.BatchExitGateCardGen:
		return "📭 本次推送没有新内容：卡片生成后无可推条目。"
	default:
		return "📭 本次推送没有新内容。"
	}
}

// strictnessPhrase 把档位渲染成空批文案里的人话片段（下限由 MinKeepScore 派生，
// 不另传参——两处各算一遍迟早漂）。
func strictnessPhrase(s types.PushStrictness) string {
	switch s {
	case types.StrictnessNormal:
		return fmt.Sprintf("本任务的「标准」门槛（≥%d 分）", s.MinKeepScore())
	case types.StrictnessStrict:
		return fmt.Sprintf("本任务的「严格」门槛（≥%d 分）", s.MinKeepScore())
	default:
		return "推送底线（不推与你画像无关的内容）"
	}
}
