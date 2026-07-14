package workflow

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/YouToco/vane/dedup"
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

// ============================================================
// 消费方接口：本包只依赖这些窄接口，具体实现（fetcher/scorer/cardgen/
// pusher/store/feishu）由 cmd/server 装配时注入。方法签名对齐各业务包
// 的导出方法（规格 B4/B5/B3），是并行开发的对接契约。
// ============================================================

// Fetcher 抓取单个信源的内容（fetcher.FetchRSS）。
type Fetcher interface {
	FetchRSS(ctx context.Context, src types.Source) ([]types.ContentItem, error)
}

// Scorer 给单条内容打分（scorer.Score，0-100）。traceID 贯穿 llm 记账。
type Scorer interface {
	Score(ctx context.Context, userID int64, item types.ContentItem, traceID string) (float64, error)
}

// CardGenerator 为单条打分内容生成解读卡片（cardgen.Generate），返回卡片 JSON。
type CardGenerator interface {
	Generate(ctx context.Context, userID int64, item types.ScoredItem, traceID string) (string, error)
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
	ListActiveSourcesByUser(ctx context.Context, userID int64) ([]types.Source, error)
	InsertContentItemIfNew(ctx context.Context, item *types.ContentItem) (id int64, isNew bool, err error)
	ListRecentSimhashes(ctx context.Context, sourceID int64, since time.Time, excludeIDs []int64) ([]int64, error)
	// UpdateSourceFetchState / ListUnpushedByUser 在 store 里 M3 就已实现，但此前没进本接口，
	// 于是 UpdateSourceFetchState 从没被 Activity 调用过（抓取状态死代码 #7）；Fetch 重构后启用。
	UpdateSourceFetchState(ctx context.Context, id int64, lastFetched, nextFetch time.Time, failCount int) error
	ListUnpushedByUser(ctx context.Context, userID int64, limit int) ([]types.ContentItem, error)
	// 幂等推送地基（FIX-A 新增实现）：重试时复用同一 batch、跳过已发条目，杜绝重复发卡（#1 CRITICAL）。
	// 取代原 CreatePushBatch / InsertDelivery（store 仍保留其实现，只是 Activity 不再用）。
	CreatePushBatchIdempotent(ctx context.Context, userID int64, idempKey string) (int64, error)
	InsertDeliveryIdempotent(ctx context.Context, d *types.Delivery) (id int64, existed bool, sentAlready bool, err error)
	UpdatePushBatchStatus(ctx context.Context, batchID int64, status types.BatchStatus) error
	MarkDeliverySent(ctx context.Context, id int64, feishuMessageID string, sentAt time.Time) error
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
}

// NewActivities 装配 Activities。参数顺序与规格 B6"持有 fetcher/scorer/
// cardgen/pusher/store/feishuMgr"一致。
func NewActivities(f Fetcher, sc Scorer, cg CardGenerator, p Pusher, st Store, fs FeishuManager) *Activities {
	return &Activities{fetcher: f, scorer: sc, cardgen: cg, pusher: p, store: st, feishu: fs}
}

// ============================================================
// Activity 入参结构：每步用具体 struct 承载（含 UserID/TraceID/上一步结果），
// 避免裸切片跨步传递时语义歧义（规格 B6）。出参直接复用 types.go 的跨包类型。
// ============================================================

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

// ============================================================
// 6 个 Activity。约定：单条失败（单源抓取失败 / 单条打分失败）不阻断整批，
// 只 warn 并跳过；只有"整批全军覆没"才返回错误触发 Temporal 重试。
// ============================================================

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
	sources, err := a.store.ListActiveSourcesByUser(ctx, p.UserID)
	if err != nil {
		return nil, err
	}
	if len(p.Scope.SourceIDs) > 0 {
		sources = filterSources(sources, p.Scope.SourceIDs)
	}

	for _, src := range sources {
		items, ferr := a.fetcher.FetchRSS(ctx, src)
		if ferr != nil {
			// 单源失败不拖垮整批：某个 RSS 源挂了不该让当次推送整体失败；
			// 同时自增 fail_count 并推进 next_fetch_at，避免调度紧循环重试。
			a.markFetchResult(ctx, src, false)
			slog.Warn("fetch: 单源抓取失败，跳过", "source_id", src.ID, "url", src.URL, "err", ferr)
			continue
		}
		for i := range items {
			if _, _, ierr := a.store.InsertContentItemIfNew(ctx, &items[i]); ierr != nil {
				// 单条入库失败只 warn：已入库的其它条目仍会被后面的 ListUnpushedByUser 捞出。
				slog.Warn("fetch: 内容入库失败，跳过", "source_id", src.ID, "err", ierr)
			}
		}
		a.markFetchResult(ctx, src, true) // 成功：清零 fail_count、推进 last/next_fetch_at。
	}

	// 返回"未投递候选"而非"本次新入库"，让 Fetch 重试幂等可续（修 #3）；
	// 带上限截断，控制单批打分规模（修 #6）。
	return a.store.ListUnpushedByUser(ctx, p.UserID, maxScoreCandidates)
}

// markFetchResult 抓取一个源后推进其抓取状态，消除 UpdateSourceFetchState 从不被调用的死代码（#7）。
//   - ok=true：清零 fail_count，last_fetched_at=now，next_fetch_at=now+interval（正常节奏）。
//   - ok=false：fail_count 自增，保留上次 last_fetched_at（本次没抓成不算"抓过"），
//     next_fetch_at 仍推进一个 interval——否则调度下一 tick 会立刻重试，形成紧循环。
//
// M3 先不做"fail_count 达阈值自动 disabled"：UpdateSourceFetchState 签名不含 status，
// 留待后续扩参或另走一条更新路径（见 TODO），当前只保证抓取状态被真实推进。
func (a *Activities) markFetchResult(ctx context.Context, src types.Source, ok bool) {
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
		// TODO(M3+): failCount 达阈值时置 status=disabled（需扩 UpdateSourceFetchState 或另一路径）。
	}
	if err := a.store.UpdateSourceFetchState(ctx, src.ID, lastFetched, nextFetch, failCount); err != nil {
		slog.Warn("fetch: 更新抓取状态失败", "source_id", src.ID, "err", err)
	}
}

// Dedup 做近似去重：精确去重（content_hash）已在 Fetch 的 InsertContentItemIfNew
// 完成，本步用 simhash + 72h 窗口过滤"改标题/转载"式近重复。跨批用 store 里的
// 历史 simhash，批内用累积集合——两者合并比对，避免同批内互为近重复的漏网。
//
// 关键修复（自撞）：Fetch 已在抓取时把 simhash 写入 content_items，本批内容自身也在
// content_items 里。若查历史不排除本批 ID，每条内容都会查到自己刚入库的 simhash 而被
// 判近重复，导致整批全删、pipeline "去重后无内容" 早退、永远推不出卡片。故先收集本批
// 全部 ID，传给 ListRecentSimhashes 排除——"历史"只含本批之外的内容。
func (a *Activities) Dedup(ctx context.Context, in DedupIn) ([]types.ContentItem, error) {
	since := time.Now().Add(-simhashWindow)

	// 本批内容自身的 ID 集合，查历史时排除（避免每条与自己刚入库的 simhash 相撞）。
	batchIDs := make([]int64, 0, len(in.Items))
	for _, item := range in.Items {
		if item.ID != 0 {
			batchIDs = append(batchIDs, item.ID)
		}
	}

	// 按源缓存历史 simhash，避免逐条查库（同源多条只查一次；历史本就是 per-source 的）。
	histCache := make(map[int64][]int64)
	// 批内已保留项的 simhash 改用单一全局切片（不再按 source 分桶）：修 #simhash 批内只同源。
	// 多源转载同一篇稿时，原来分桶后各源互相看不见、双双放行造成重复；合并成全局候选后，
	// 后到的转载能与批内已保留的同稿命中近重复而被拦下。
	var batchSeen []int64

	kept := make([]types.ContentItem, 0, len(in.Items))
	for _, item := range in.Items {
		hist, ok := histCache[item.SourceID]
		if !ok {
			h, err := a.store.ListRecentSimhashes(ctx, item.SourceID, since, batchIDs)
			if err != nil {
				return nil, err
			}
			hist = h
			histCache[item.SourceID] = h
		}

		sh := dedup.Simhash(item.Title + " " + item.Content)
		// 候选集 = 该源历史 simhash ∪ 全局批内已保留 simhash（跨源合并比对）。
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

// Select 取 TopN，直接复用 selector.SelectTopN——ScoredItem 统一为 types.ScoredItem
// 后，selector 与 workflow 都只 import types、彼此不依赖，无环，故不必再内联排序。
func (a *Activities) Select(ctx context.Context, in SelectIn) ([]types.ScoredItem, error) {
	n := in.TopN
	if n <= 0 {
		n = defaultTopN
	}
	return selector.SelectTopN(in.Scored, n), nil
}

// CardGen 逐条生成解读卡片。单条失败跳过；整批全失败返回错误触发重试。
func (a *Activities) CardGen(ctx context.Context, in CardGenIn) ([]GeneratedCard, error) {
	cards := make([]GeneratedCard, 0, len(in.Items))
	for _, si := range in.Items {
		cj, err := a.cardgen.Generate(ctx, in.UserID, si, in.TraceID)
		if err != nil {
			slog.Warn("cardgen: 单条生成失败，跳过", "content_item_id", si.Item.ID, "trace_id", in.TraceID, "err", err)
			continue
		}
		cards = append(cards, GeneratedCard{Scored: si, CardJSON: cj})
	}
	if len(cards) == 0 && len(in.Items) > 0 {
		return nil, types.NewAppError(types.CodeLLMUnavailable, "整批卡片生成全部失败", nil)
	}
	return cards, nil
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
			BatchID:  batchID,
			UserID:   in.UserID,
			Score:    card.Scored.Score,
			CardJSON: json.RawMessage(card.CardJSON),
			Status:   types.DeliveryStatusPending,
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

		msgID, perr := a.pusher.Push(ctx, owner, card.CardJSON)
		if perr != nil {
			slog.Warn("push: 单卡推送失败，跳过", "delivery_id", delID, "trace_id", in.TraceID, "err", perr)
			continue
		}
		if merr := a.store.MarkDeliverySent(ctx, delID, msgID, time.Now()); merr != nil {
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
