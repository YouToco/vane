package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/YouToco/vane/dedup"
	"github.com/YouToco/vane/feedback"
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
	// 幂等推送地基（FIX-A 新增实现）：重试时复用同一 batch、跳过已发条目，杜绝重复发卡（#1 CRITICAL）。
	// 取代原 CreatePushBatch / InsertDelivery（store 仍保留其实现，只是 Activity 不再用）。
	CreatePushBatchIdempotent(ctx context.Context, userID int64, idempKey string) (int64, error)
	// RecordEmptyPushBatch 记录"跑完了但没东西可推"的空批次（009）。与上面那条幂等
	// 地基共用 idempotency_key，但**写入路径完全独立**——空批次与真实推送在同一次
	// 运行里互斥，刻意不去动 CreatePushBatchIdempotent 的签名（改它就得改这个窄接口
	// 的全部替身，且那是 #1 CRITICAL 的地基）。
	//
	// skipped=true 表示 store 侧防覆写护栏拦下了本次写入（该 traceID 已有真实批次），
	// 此时 id=0、err=nil——**不是错误，但必须出声**（见 RecordEmptyBatch 的处理）。
	RecordEmptyPushBatch(ctx context.Context, userID int64, idempKey string,
		gate types.BatchExitGate, counts types.PipelineCounts) (id int64, skipped bool, err error)
	InsertDeliveryIdempotent(ctx context.Context, d *types.Delivery) (id int64, existed bool, sentAlready bool, err error)
	UpdatePushBatchStatus(ctx context.Context, batchID int64, status types.BatchStatus) error
	// MarkDeliverySent 发送成功后回填 message_id 与最终卡 JSON（契约 §3.3：
	// 最终卡在拿到 delivery id 后才构造，只能在此落库而非 Insert 时）。
	MarkDeliverySent(ctx context.Context, id int64, feishuMessageID string, cardJSON json.RawMessage, sentAt time.Time) error
	// GetSource 按 id 查信源（卡片改版：subtitle 需要 source.Title 和 Platform）。
	GetSource(ctx context.Context, id int64) (*types.Source, error)
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
	// buildCard 构造带反馈按钮的最终卡（生产装配传 feishu.BuildDeliveryCard）。
	// 函数注入而非 import feishu：feishu→agent→workflow 依赖链已存在，
	// 本包直接依赖 feishu 即成 import 环（契约 §8.2 CRITICAL）。
	buildCard func(input feedback.CardInput) string
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
	ev ProfileEvolver, buildCard func(input feedback.CardInput) string,
	buildNotice func(markdown string) string) *Activities {
	return &Activities{fetcher: f, scorer: sc, cardgen: cg, pusher: p, store: st, feishu: fs,
		evolver: ev, buildCard: buildCard, buildNotice: buildNotice}
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
	UserID  int64           `json:"user_id"`
	TraceID string          `json:"trace_id"`
	Cards   []GeneratedCard `json:"cards"`
}

// RecordEmptyIn 是 RecordEmptyBatch Activity 的入参（009 / 契约 §16「空批次缺口」）。
type RecordEmptyIn struct {
	UserID  int64  `json:"user_id"`
	TraceID string `json:"trace_id"` // = 幂等键，与 Push 建批次用的是同一个
	// Gate 从哪个闸门退出。恒非空——五个调用点各自传死值（workflow.go）。
	Gate types.BatchExitGate `json:"gate"`
	// Counts 截至退出时刻的漏斗快照；未跑到的阶段字段为 nil（见 types.PipelineCounts）。
	Counts types.PipelineCounts `json:"counts"`
}

// ============================================================
// 6 个 Activity。约定：单条失败（单源抓取失败 / 单条打分失败）不阻断整批，
// 只 warn 并跳过；只有"整批全军覆没"才返回错误触发 Temporal 重试。
// ============================================================

// EvolveProfile 画像演化前置步（契约 §8.1）。evolver 未注入时 no-op：统编阶段
// 可先不传 evolver 灰度上线，pipeline 行为与 M4 完全一致。错误原样上抛交给
// RetryPolicy；重试耗尽后由 workflow 侧吞掉——演化失败永不阻断推送（红线）。
func (a *Activities) EvolveProfile(ctx context.Context, in EvolveIn) error {
	if a.evolver == nil {
		return nil
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
	// 只抓到期源（next_fetch_at <= now()）：重试不重复计费，详见接口注释。
	// 未到期源被跳过不影响推送——其已入库内容仍由下方 ListUnpushedByUser 捞出。
	sources, err := a.store.ListDueSourcesByUser(ctx, p.UserID)
	if err != nil {
		return nil, err
	}
	if len(p.Scope.SourceIDs) > 0 {
		sources = filterSources(sources, p.Scope.SourceIDs)
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

// Score 逐条打分。同一 TraceID 串起整批的 llm_calls 便于事后追踪。
// 单条失败跳过；整批全失败（大概率 LLM 不可用）返回错误触发重试。
func (a *Activities) Score(ctx context.Context, in ScoreIn) ([]types.ScoredItem, error) {
	scored := make([]types.ScoredItem, 0, len(in.Items))
	for _, item := range in.Items {
		s, err := a.scorer.Score(ctx, in.UserID, item, in.TraceID)
		if err != nil {
			slog.Warn("score: 单条打分失败，跳过", "content_item_id", item.ID, "trace_id", in.TraceID, "err", err)
			continue
		}
		scored = append(scored, types.ScoredItem{Item: item, Score: s})
	}
	if len(scored) == 0 && len(in.Items) > 0 {
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

// CardGen 逐条生成解读正文。单条失败跳过；整批全失败返回错误触发重试。
func (a *Activities) CardGen(ctx context.Context, in CardGenIn) ([]GeneratedCard, error) {
	cards := make([]GeneratedCard, 0, len(in.Items))
	for _, si := range in.Items {
		body, err := a.cardgen.Generate(ctx, in.UserID, si, in.TraceID)
		if err != nil {
			slog.Warn("cardgen: 单条生成失败，跳过", "content_item_id", si.Item.ID, "trace_id", in.TraceID, "err", err)
			continue
		}
		cards = append(cards, GeneratedCard{Scored: si, BodyMD: body})
	}
	if len(cards) == 0 && len(in.Items) > 0 {
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
	_, skipped, err := a.store.RecordEmptyPushBatch(ctx, in.UserID, in.TraceID, in.Gate, in.Counts)
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
	batchID, err := a.store.CreatePushBatchIdempotent(ctx, in.UserID, in.TraceID)
	if err != nil {
		return err
	}

	anySent := false
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
			// 重试时该 (batch, content) 已发过：直接跳过，绝不重复推卡——幂等核心，修 #1 CRITICAL 重复发卡。
			anySent = true
			continue
		}

		// 构卡元数据：标题/分数/URL 从 pipeline 上下文取，源信息 best-effort 查库。
		ci := feedback.CardInput{
			BodyMD:      card.BodyMD,
			DeliveryID:  delID,
			State:       feedback.CardState{},
			Title:       card.Scored.Item.Title,
			Score:       int(card.Scored.Score),
			URL:         card.Scored.Item.URL,
			PublishedAt: card.Scored.Item.PublishedAt,
		}
		if card.Scored.Item.SourceID != 0 {
			if src, serr := a.store.GetSource(ctx, card.Scored.Item.SourceID); serr == nil {
				ci.SourceTitle = src.Title
				ci.Platform = src.Platform
			}
		}
		cardJSON := a.buildCard(ci)
		msgID, perr := a.pusher.Push(ctx, owner, cardJSON)
		if perr != nil {
			slog.Warn("push: 单卡推送失败，跳过", "delivery_id", delID, "trace_id", in.TraceID, "err", perr)
			continue
		}
		if merr := a.store.MarkDeliverySent(ctx, delID, msgID, json.RawMessage(cardJSON), time.Now()); merr != nil {
			// 已发出但回执标记失败：记录不阻断，靠对账补偿（避免重复推送）。
			slog.Warn("push: 标记已发失败（消息已送达）", "delivery_id", delID, "feishu_message_id", msgID, "err", merr)
		}
		anySent = true
	}

	status := types.BatchStatusDone
	if !anySent {
		status = types.BatchStatusFailed
	}
	if err := a.store.UpdatePushBatchStatus(ctx, batchID, status); err != nil {
		return err
	}
	if !anySent {
		return types.NewAppError(types.CodePushFailed, "本批次全部推送失败", nil)
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
