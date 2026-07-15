// TikHub 小红书信源：把一个搜索关键词当作一个信源，周期性调 TikHub 的
// search_notes 接口抓取最新笔记，映射为 types.ContentItem。与 Exa 同理，
// 目标是固定可信主机 api.tikhub.io（URL 非用户可控），不需要 RSS 的 SSRF 拦截。
//
// 契约（2026-07-14 / 2026-07-15 用真实 key 实测确认）：
//   - 搜索 GET {base}/api/v1/xiaohongshu/app_v2/search_notes?keyword=&page=&sort_type=
//   - 详情 GET {base}/api/v1/xiaohongshu/web_v3/fetch_note_detail?note_id=&xsec_token=
//   - 鉴权头 Authorization: Bearer <key>
//   - 响应 code=200 且 data.success=true 时，笔记在 data.data.items[].note，
//     字段含 id/title/desc/timestamp(秒)/xsec_token/user.nickname。
//
// 为什么搜索之后还要逐条调详情（M5 缺陷 1）：search_notes 的 desc 被上游硬截断到
// 60 rune，生产库 129 条小红书内容无一例外全部 ≤60 —— 卡片生成拿到的"正文"其实是
// 半句话（实测某条 59 字断在句中），模型在证据不足时会顺着标题编造摘要还打高分。
// 详情接口返回完整 desc（实测同一条 59→689 rune，8 条样本平均 5.8x），是把"证据不足"
// 从根上消灭的唯一手段。代价是按次计费，故有 SeenChecker 这道只为新笔记付费的闸门。
package fetcher

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/YouToco/vane/config"
	"github.com/YouToco/vane/types"
)

const (
	tikhubDefaultBaseURL = "https://api.tikhub.io"
	tikhubSearchPath     = "/api/v1/xiaohongshu/app_v2/search_notes"
	// tikhubNoteDetailPath 是笔记详情接口。只用 web_v3 这一个：app_v2 的
	// get_video_note_detail 对图文笔记会**静默返回别人的笔记**（HTTP 200 + code=200
	// + success=true，照常计费，实测确认）——拿错笔记比报错危险得多，因为它一路
	// 无声地流进 content_items 和推送卡片。即便如此仍要校验返回的 note_id（见 detailDesc）。
	tikhubNoteDetailPath = "/api/v1/xiaohongshu/web_v3/fetch_note_detail"
	// tikhubDefaultSort 默认按发布时间降序：推送场景要"最新动态"，
	// 相关性排序（general）会反复返回同批高赞旧帖，全靠去重挡、白耗配额。
	tikhubDefaultSort = "time_descending"
	// tikhubMaxDescBytes 截断笔记正文的字节上限，防超长内容打爆后续打分 token
	// （成本护栏）。截断按 rune 边界回退（truncateUTF8），绝不产生非法 UTF-8。
	tikhubMaxDescBytes = 4000
	// tikhubDetailMinRunes 是触发详情补全的 desc 长度阈值，单位是 **rune 不是字节**
	// （60 rune 的中文 desc 占 180 字节，用 len() 判会漏掉每一条）。上游正是截到
	// 60 rune，所以"恰好 60 rune"就是被截断的信号。实测 <60 的 desc 都是完整正文
	// （一条 30 rune 的补全后 51 rune，差异仅 [话题]# 标记展开，没丢正文），
	// 跳过它们零内容损失、省一次计费。
	tikhubDetailMinRunes = 60
	// tikhubDetailInterval 是两次详情调用之间的最小间隔。上游报文写 1 req/s、
	// 接口元数据写 10 req/s，两者矛盾且实测超速直接 429 —— 按保守的 1 req/s 串行，
	// 多留 10% 余量。这是补全串行化的唯一原因。
	tikhubDetailInterval = 1100 * time.Millisecond
	// tikhubEnrichBudget 是单次 Fetch 花在详情补全上的时间上限。
	//
	// 存在的理由是 Fetch 活动的 120s StartToCloseTimeout **由全部到期源共享**：
	// 补全串行且单次最坏等满 client.Timeout（20s），20 条就能吃掉 400s+，撞破活动
	// 预算 → DeadlineExceeded 重试 → 已付费的补全全部作废重付。40s 的取舍：
	// 正常网络（~1s/次）够补 ~30 条，远超单页 20 条；病态网络下也只占活动预算的 1/3，
	// 给其余源留出余量。超预算未补的笔记下轮自然重试（seen 按正文长度判定）。
	tikhubEnrichBudget = 40 * time.Second
)

// SeenChecker 报告哪些 canonical_key **已入库且正文已补全**（生产实现是 *store.Store）。
//
// 存在的理由：详情接口 $0.01/次，已经拿到全文的笔记不该每轮重复付费。
// 抓取器本身没有 DB 访问（去重发生在 workflow 活动层），因此只注入这一个窄方法，
// 而不是把整个 store 拖进 fetcher。
//
// 按 **canonical_key 而非 (source_id, external_id)** 查是 M5 多用户重构的关键：
// 内容身份全局化后，"哪些笔记已补全"是全局事实，不该按源各问一遍——否则用户 A 的
// 「AI编程」补全过的笔记，用户 B 的「AI工具」搜到同一条时会被当成新笔记再付一次钱
// （跨源重复，多用户才暴露）。去掉 sourceID 入参正是为了让这种误用无法表达。
//
// 判据是**正文长度**而非"行是否存在"：补全会失败（429/抖动/风控），失败的笔记
// 仍以 ≤60 rune 的搜索摘要落库；若按"存在"跳过，一次瞬时 429 就让它终身 60 字
// 且再无自愈路径（详见 store.EnrichedCanonicalKeys 的注释）。
type SeenChecker interface {
	EnrichedCanonicalKeys(ctx context.Context, keys []string, minRunes int) (map[string]struct{}, error)
}

// TikHubFetcher 调用 TikHub 小红书搜索。
type TikHubFetcher struct {
	apiKey   string
	baseURL  string // 可覆盖以便单测指向 httptest.Server
	client   *http.Client
	maxBytes int64

	// seen 用于判断笔记正文是否已补全，只为还没拿到全文的笔记调用计费的详情接口。
	// 为 nil 时**跳过整个补全**：无从判断新旧，就只能要么全量补全（每轮为同一批
	// 老笔记重复烧钱）要么不补，宁可不补——退回 60 字 desc 只是不改善，不是倒退。
	seen SeenChecker
	// detailInterval 抽成字段仅为可测：生产恒为 tikhubDetailInterval，
	// 测试调小以便断言"串行且有间隔"而不必真等 1.1s。
	detailInterval time.Duration
	// enrichBudget 单次 Fetch 里花在详情补全上的时间上限（见 enrichDescs）。
	enrichBudget time.Duration

	// rateMu/lastDetailAt 是**跨 Fetch 调用**的限速状态：Multi 只持有一个
	// TikHubFetcher，而 Fetch 活动在同一活动里串行遍历全部到期源——限速若只在
	// 单次调用内计数，源 A 的末次详情与源 B 的首次详情只隔几百毫秒，稳吃 429，
	// 该笔记就此被钉在 60 字。提升为实例状态后跨源、跨 goroutine 都受同一把闸门约束。
	rateMu       sync.Mutex
	lastDetailAt time.Time
}

// NewTikHub 按抓取配置构造 TikHubFetcher。超时/响应上限兜底与 RSS 一致（20s / 5MB）。
// apiKey 为空不在此报错——留到 Fetch 时返回明确的 CodeValidation。
// seen 可为 nil（详情补全会被跳过，见 TikHubFetcher.seen）。
func NewTikHub(cfg config.FetchConfig, seen SeenChecker) *TikHubFetcher {
	timeout := time.Duration(cfg.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 20 * time.Second
	}
	maxMB := cfg.MaxResponseMB
	if maxMB <= 0 {
		maxMB = 5
	}
	return &TikHubFetcher{
		apiKey:  cfg.TikhubAPIKey,
		baseURL: tikhubDefaultBaseURL,
		// 禁跟随重定向：与 Exa 一致，防 Bearer key 被同域/子域 30x 外带（见 noRedirect）。
		client:         &http.Client{Timeout: timeout, CheckRedirect: noRedirect},
		maxBytes:       int64(maxMB) * 1024 * 1024,
		seen:           seen,
		detailInterval: tikhubDetailInterval,
		enrichBudget:   tikhubEnrichBudget,
	}
}

// tikhubSourceConfig 是 tikhub_xhs 信源的 config JSONB 结构。keyword 必填。
type tikhubSourceConfig struct {
	Keyword  string `json:"keyword"`             // 搜索关键词，必填
	SortType string `json:"sort_type,omitempty"` // 排序：time_descending（默认）/ general / popularity_descending
	NoteType string `json:"note_type,omitempty"` // 笔记类型过滤，API 默认"不限"
}

// tikhubEnvelope 是 TikHub 的统一响应外壳（只取需要的字段）。
type tikhubEnvelope struct {
	Code int              `json:"code"`
	Data tikhubSearchData `json:"data"`
}

type tikhubSearchData struct {
	Success bool            `json:"success"`
	Msg     json.RawMessage `json:"msg"` // 类型不稳定（null/string/对象），原样保留只用于错误信息
	Data    tikhubItemsWrap `json:"data"`
}

type tikhubItemsWrap struct {
	Items []tikhubSearchItem `json:"items"`
}

// tikhubSearchItem 是搜索结果流的一项；model_type=note 才是笔记，
// 其余（广告位/用户卡片/专题）跳过。
type tikhubSearchItem struct {
	ModelType string      `json:"model_type"`
	Note      *tikhubNote `json:"note"`
}

type tikhubNote struct {
	ID        string     `json:"id"`
	Title     string     `json:"title"`
	Desc      string     `json:"desc"`
	Timestamp int64      `json:"timestamp"` // 发布时间，Unix 秒；0 表示未提供
	XsecToken string     `json:"xsec_token"`
	User      tikhubUser `json:"user"`
}

type tikhubUser struct {
	Nickname string `json:"nickname"`
}

// tikhubDetailEnvelope 是 fetch_note_detail 的响应外壳。外层 code/success/msg 与
// 搜索一致，但 data.data.items[] 的形状不同（note_card 而非 note），故单独建模。
type tikhubDetailEnvelope struct {
	Code int              `json:"code"`
	Data tikhubDetailData `json:"data"`
}

type tikhubDetailData struct {
	Success bool             `json:"success"`
	Msg     json.RawMessage  `json:"msg"` // 类型不稳定，原样保留只用于错误信息
	Data    tikhubDetailWrap `json:"data"`
}

type tikhubDetailWrap struct {
	Items []tikhubDetailItem `json:"items"`
}

type tikhubDetailItem struct {
	NoteCard tikhubNoteCard `json:"note_card"`
}

// tikhubNoteCard 只取详情里真正要用的两个字段。
// 刻意不解析 note_card.time：它是**毫秒**，而搜索的 timestamp 是**秒**，混用会把
// 发布时间打到公元 58000 年。发布时间一律以搜索结果的 timestamp 为准。
type tikhubNoteCard struct {
	NoteID string `json:"note_id"`
	Desc   string `json:"desc"`
}

// Fetch 按信源 config 里的 keyword 搜索小红书笔记，返回映射后的内容条目。
// 失败语义与 Exa 一致：缺 key / 缺 keyword / 非法 config → CodeValidation（不可重试）；
// 超时 → CodeFetchTimeout；429 → CodeFetchRateLimit；非 2xx 按 5xx/4xx 定可否重试。
func (t *TikHubFetcher) Fetch(ctx context.Context, src types.Source) ([]types.ContentItem, error) {
	if t.apiKey == "" {
		return nil, types.NewAppError(types.CodeValidation,
			"TikHub 信源需要配置 VANE_FETCH_TIKHUB_API_KEY，当前为空", nil)
	}

	var sc tikhubSourceConfig
	if len(src.Config) > 0 {
		if err := json.Unmarshal(src.Config, &sc); err != nil {
			return nil, types.NewAppError(types.CodeValidation,
				fmt.Sprintf("解析 TikHub 信源 config 失败（source_id=%d）", src.ID), err)
		}
	}
	if sc.Keyword == "" {
		return nil, types.NewAppError(types.CodeValidation,
			fmt.Sprintf("TikHub 信源缺少 keyword（source_id=%d）", src.ID), nil)
	}
	sort := sc.SortType
	if sort == "" {
		sort = tikhubDefaultSort
	}

	q := url.Values{}
	q.Set("keyword", sc.Keyword)
	q.Set("page", "1") // MVP 单页 20 条；周期抓取下靠 canonical_key 全局唯一增量去重
	q.Set("sort_type", sort)
	if sc.NoteType != "" {
		q.Set("note_type", sc.NoteType)
	}
	reqURL := t.baseURL + tikhubSearchPath + "?" + q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, types.NewAppError(types.CodeValidation, "构造 TikHub 请求失败", err)
	}
	req.Header.Set("Authorization", "Bearer "+t.apiKey)
	req.Header.Set("Accept", "application/json")

	resp, err := t.client.Do(req)
	if err != nil {
		return nil, classifyDoError(reqURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTooManyRequests {
		return nil, types.NewAppError(types.CodeFetchRateLimit, "TikHub 搜索被限流(429)", nil)
	}
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return nil, types.NewAppError(types.CodeValidation,
			fmt.Sprintf("TikHub 鉴权失败（HTTP %d），请检查 API key 与 scopes", resp.StatusCode), nil)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		ae := types.NewAppError(types.CodeFetchTimeout,
			fmt.Sprintf("TikHub 搜索返回非 2xx 状态 %d", resp.StatusCode), nil)
		ae.Retryable = resp.StatusCode >= 500 // 5xx 瞬态可重试，4xx 确定性不可重试。
		return nil, ae
	}

	data, err := io.ReadAll(io.LimitReader(resp.Body, t.maxBytes+1))
	if err != nil {
		return nil, classifyDoError(reqURL, err)
	}
	if int64(len(data)) > t.maxBytes {
		return nil, types.NewAppError(types.CodeValidation,
			fmt.Sprintf("TikHub 响应体超过 %d 字节上限", t.maxBytes), nil)
	}

	var env tikhubEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		ae := types.NewAppError(types.CodeFetchTimeout, "解析 TikHub 响应失败", err)
		ae.Retryable = false
		return nil, ae
	}
	// 业务层错误：外壳 code 非 200 或 data.success=false（如关键词违规、上游风控）。
	// 保守按确定性处理不重试——TikHub 的瞬态故障通常直接表现为 HTTP 5xx。
	if env.Code != http.StatusOK || !env.Data.Success {
		ae := types.NewAppError(types.CodeFetchTimeout,
			fmt.Sprintf("TikHub 搜索业务失败（code=%d, success=%v, msg=%s）",
				env.Code, env.Data.Success, string(env.Data.Msg)), nil)
		ae.Retryable = false
		return nil, ae
	}

	// 二阶段：先就地把被截断的 desc 换成详情全文，再映射。顺序不能反 ——
	// finalize 在 mapTikhubNotes 里算 content_hash/simhash，必须基于全文，
	// 否则指纹建立在 60 字残句上，跨批去重会把"同一条笔记的不同截断"当成新内容。
	// 补全全程降级：任何失败都只是保留 60 字 desc，绝不让 Fetch 失败（见 enrichDescs）。
	t.enrichDescs(ctx, src, env.Data.Data.Items)

	return mapTikhubNotes(src, env.Data.Data.Items), nil
}

// enrichDescs 就地把被截断的笔记 desc 替换为详情接口返回的完整正文。
//
// 降级铁律：这里**没有任何路径返回错误**。补全是纯增益——失败时保留搜索给的 60 字
// desc 继续入库，与补全上线前的行为完全一致，不倒退。这与 Fetch 其余部分"全有或全无"
// 的语义相反，所以刻意做成无返回值：让"这个函数不该让抓取失败"在签名上就成立。
//
// 只对同时满足三个条件的笔记付费：正文尚未补全（长度判定 + **全局** canonical_key
// 判定，见 SeenChecker）、desc ≥60 rune（被截断的信号）、有 xsec_token（详情接口
// 必填，空的话稳吃 400 还照样计费）。
//
// "全局"是 M5 的实质改动：闸门查的是 note_id 这个全局身份，不带源号——多用户下
// 同一篇笔记会同时命中好几个关键词源（A 订「AI编程」、B 订「AI工具」），按源查的话
// 每个源都认为它是新笔记，$0.01 付 N 次；按全局身份查，第一个源补全后其余源全部命中跳过。
//
// **耗时预算**是硬约束而非注释警告：Fetch 活动在**同一个** 120s 的 StartToCloseTimeout
// 里串行遍历用户的全部到期源，而单条详情最坏要等满 client.Timeout（默认 20s）——
// 光一个源的 20 条就能吃掉 20×20s，远超整个活动的预算，届时活动 DeadlineExceeded
// 重试、已付费的补全全部丢弃再重付。故本函数只在 enrichBudget 内尽力补，超预算就
// 收手：**没补到的笔记以 60 字落库，但因为 seen 按正文长度判定，下一轮会自然重试**——
// 这正是长度判据（而非"行是否存在"）换来的安全网。
func (t *TikHubFetcher) enrichDescs(ctx context.Context, src types.Source, items []tikhubSearchItem) {
	if t.seen == nil {
		return // 无从判断新旧，跳过补全（见 TikHubFetcher.seen 的注释）。
	}

	// 候选：被截断且可调详情的笔记。指向 items 里的 Note 指针以便就地改写。
	var cands []*tikhubNote
	for _, it := range items {
		if it.ModelType != "note" || it.Note == nil || it.Note.ID == "" {
			continue
		}
		// RuneCountInString 而非 len：60 rune 的中文 desc 是 180 字节。
		if utf8.RuneCountInString(it.Note.Desc) < tikhubDetailMinRunes {
			continue // 未被截断，已是完整正文。
		}
		if it.Note.XsecToken == "" {
			continue // 详情接口必填 xsec_token，缺了必然 400。
		}
		cands = append(cands, it.Note)
	}
	if len(cands) == 0 {
		return // 无候选：不查库、不发请求。
	}

	// 用 canonical_key（xhs 即 note_id）查闸门，**不带 source_id**。这正是跨源重复
	// 不再重复付费的地方：用户 A 订「AI编程」时已补全过的笔记，用户 B 订「AI工具」
	// 搜到同一条时（两个源、同一 note_id）直接命中、跳过详情调用——旧的按
	// (source_id, external_id) 查做不到这点，B 的源号对不上 A 的行，于是同一篇笔记
	// 被付两次钱、还在库里存了两份。
	// 键一律经 xhsKey 构造，与 finalize 落库用的键同源，杜绝归一化漂移导致的全 miss。
	keys := make([]string, 0, len(cands))
	for _, n := range cands {
		keys = append(keys, xhsKey(n.ID))
	}
	seen, err := t.seen.EnrichedCanonicalKeys(ctx, keys, tikhubDetailMinRunes)
	if err != nil {
		// 查不出新旧就不补：宁可少补一轮，也不为一库老笔记重复付费。
		slog.Warn("tikhub: 查询已补全 canonical_key 失败，跳过本轮详情补全",
			"source_id", src.ID, "candidates", len(cands), "err", err)
		return
	}

	deadline := time.Now().Add(t.enrichBudget)
	sent := 0
	for _, n := range cands {
		if _, ok := seen[xhsKey(n.ID)]; ok {
			continue // 正文已补全过（可能是别的源、别的用户补的），不重复付费。
		}
		// 预算用尽就收手：剩余笔记保留 60 字 desc 入库，下轮重试（见函数注释）。
		if time.Now().After(deadline) {
			slog.Warn("tikhub: 详情补全预算用尽，剩余笔记保留搜索摘要待下轮补全",
				"source_id", src.ID, "enriched", sent, "budget", t.enrichBudget)
			return
		}
		// 限速：上游 1 req/s，闸门是**实例级**的（跨源、跨调用共享），
		// 否则多源串行时源 B 的首条详情会紧贴源 A 的末条发出而吃 429。
		if !t.waitDetailSlot(ctx) {
			slog.Warn("tikhub: 上下文取消，剩余笔记保留搜索摘要",
				"source_id", src.ID, "enriched", sent)
			return
		}
		sent++

		desc, derr := t.detailDesc(ctx, n.ID, n.XsecToken)
		if derr != nil {
			// 429/422/400/success=false/网络错/note_id 不匹配都走这里：保留搜索 desc。
			slog.Warn("tikhub: 笔记详情补全失败，保留搜索摘要",
				"source_id", src.ID, "note_id", n.ID, "err", derr)
			continue
		}
		if desc == "" {
			continue // 详情正文为空（纯图笔记）：搜索 desc 至少还有点内容，别覆盖成空。
		}
		n.Desc = desc
	}
}

// waitDetailSlot 拿到下一个详情调用的限速槽位：距上次调用不足 detailInterval 就等。
// 返回 false 表示 ctx 已取消（调用方应停止补全）。
//
// 闸门状态挂在实例上而非调用栈上：Multi 只持有一个 TikHubFetcher，Fetch 活动串行
// 遍历多个源——若每次 Fetch 各自从零计数，源 B 的首条详情会紧贴源 A 的末条发出
// （中间只隔几百毫秒的入库），稳吃 429，那条笔记就被钉在 60 字。
// 持锁跨越 sleep 是刻意的：上游限的是"每秒 1 次请求"，并发放行会直接破坏该约束；
// 补全本就是串行低频路径，锁竞争不是问题。
func (t *TikHubFetcher) waitDetailSlot(ctx context.Context) bool {
	t.rateMu.Lock()
	defer t.rateMu.Unlock()

	if wait := t.detailInterval - time.Since(t.lastDetailAt); wait > 0 && !t.lastDetailAt.IsZero() {
		select {
		case <-ctx.Done():
			return false
		case <-time.After(wait):
		}
	}
	t.lastDetailAt = time.Now()
	return true
}

// detailDesc 取单条笔记的完整正文。任何异常都返回 error 交给调用方降级，不重试
// （补全是尽力而为，重试只会加剧限流并翻倍成本）。
func (t *TikHubFetcher) detailDesc(ctx context.Context, noteID, xsecToken string) (string, error) {
	// xsec_token 含 + / = 等字符，必须走 url.Values 编码；裸拼会让 + 在服务端
	// 被解成空格，token 校验失败（表现为 200 + success=false，白花钱找不到原因）。
	q := url.Values{}
	q.Set("note_id", noteID)
	q.Set("xsec_token", xsecToken)
	reqURL := t.baseURL + tikhubNoteDetailPath + "?" + q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return "", fmt.Errorf("构造详情请求: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+t.apiKey)
	req.Header.Set("Accept", "application/json")

	resp, err := t.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("请求详情: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// 实测形态：缺 token→422、空 token→400、超速→429。都只降级不重试。
		return "", fmt.Errorf("详情返回非 2xx 状态 %d", resp.StatusCode)
	}

	data, err := io.ReadAll(io.LimitReader(resp.Body, t.maxBytes+1))
	if err != nil {
		return "", fmt.Errorf("读取详情响应: %w", err)
	}
	if int64(len(data)) > t.maxBytes {
		return "", fmt.Errorf("详情响应体超过 %d 字节上限", t.maxBytes)
	}

	var env tikhubDetailEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		return "", fmt.Errorf("解析详情响应: %w", err)
	}
	// token 失效/笔记已删的实测形态是 HTTP 200 + success=false + msg="当前笔记暂时无法浏览"。
	if env.Code != http.StatusOK || !env.Data.Success {
		return "", fmt.Errorf("详情业务失败（code=%d, success=%v, msg=%s）",
			env.Code, env.Data.Success, string(env.Data.Msg))
	}
	if len(env.Data.Data.Items) == 0 {
		return "", fmt.Errorf("详情响应无 items")
	}

	card := env.Data.Data.Items[0].NoteCard
	// 防静默污染：确认拿回来的确实是我们请求的那条笔记。上游有过"200 + 正常外壳 +
	// 别人的笔记"的先例，不校验的话别人的正文会被安在这条 external_id 上入库。
	if card.NoteID != noteID {
		return "", fmt.Errorf("详情返回的 note_id=%q 与请求的 %q 不符，疑似上游串号", card.NoteID, noteID)
	}
	return card.Desc, nil
}

// mapTikhubNotes 把笔记映射为 ContentItem，指纹与身份由 finalize 统一补齐。
// URL 拼 xsec_token（2024 起小红书 web 端直链必带，否则 404），来源标记 pc_search。
//
// 注意这个 URL **不能当身份用**：xsec_token 是每次搜索新发的临时票据，同一笔记
// 两次搜到的 url 不同（实测）。小红书的身份是 note_id，即下面填进 ExternalID 的
// n.ID，由 CanonicalKey 取用——与 rss/exa 认 url 的规则恰好相反。
func mapTikhubNotes(src types.Source, items []tikhubSearchItem) []types.ContentItem {
	now := time.Now().UTC()
	out := make([]types.ContentItem, 0, len(items))
	for _, it := range items {
		if it.ModelType != "note" || it.Note == nil || it.Note.ID == "" {
			continue // 广告位/用户卡片/异常项跳过。
		}
		n := it.Note

		noteURL := "https://www.xiaohongshu.com/explore/" + url.PathEscape(n.ID)
		if n.XsecToken != "" {
			noteURL += "?xsec_token=" + url.QueryEscape(n.XsecToken) + "&xsec_source=pc_search"
		}

		content := truncateUTF8(n.Desc, tikhubMaxDescBytes)

		item := types.ContentItem{
			SourceID:    src.ID,
			ExternalID:  n.ID,
			URL:         noteURL,
			Title:       n.Title,
			Content:     content,
			Author:      n.User.Nickname,
			PublishedAt: parseUnixSeconds(n.Timestamp),
			FetchedAt:   now,
		}
		// 上面的 n.ID == "" 已挡掉无身份的笔记，这里是同一判定的第二道
		// （身份规则只写在 finalize 一处，此处不重复表达）。
		if !finalize(src, &item) {
			continue
		}
		out = append(out, item)
	}
	return out
}

// parseUnixSeconds 把 Unix 秒转为 *time.Time；0 或负值视为未提供（列可空）。
func parseUnixSeconds(sec int64) *time.Time {
	if sec <= 0 {
		return nil
	}
	t := time.Unix(sec, 0).UTC()
	return &t
}
