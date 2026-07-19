// Package fetcher 负责从 RSS 信源抓取内容并映射为 types.ContentItem。
//
// 安全边界（Step 4 设计，M3 落地）：
//   - 超时：http.Client.Timeout（cfg.TimeoutSeconds），防慢速源拖死 Activity；
//   - 响应大小上限：io.LimitReader（cfg.MaxResponseMB），防超大响应打爆内存；
//   - 私网 IP 拦截：抓取前 LookupIP + 连接时 Dialer.Control 双重校验，
//     防 SSRF / DNS rebinding（源 URL 由用户提交，不可信）。
//
// 之所以自己 GET 再交给 gofeed 的 Parser.Parse(io.Reader)，而不是直接用
// ParseURL/ParseURLWithContext：只有拿到 resp.Body 才能套 io.LimitReader
// 强制大小上限，这是契约 B4 明确要求的安全约束（gofeed 内部抓取无法拦）。
package fetcher

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/mmcdole/gofeed"

	"github.com/YouToco/vane/config"
	"github.com/YouToco/vane/dedup"
	"github.com/YouToco/vane/types"
)

var htmlTagRe = regexp.MustCompile(`<[a-zA-Z/!]`)

// errBlockedDial 是连接阶段命中私网地址时 Dialer.Control 返回的哨兵。
// 之所以单独定义：Control 触发时机在 http.Client 内部，只能通过 error 冒泡，
// FetchRSS 再据此判定为 CodeValidation（与抓取前 LookupIP 预检的语义一致）。
var errBlockedDial = errors.New("fetcher: 目标解析到私网/环回地址，已拒绝连接")

// rssDefaultLookbackDays 是 RSS 源默认只收最近 N 天发布的条目。
// 注意 lookback_days 在两个能力下**默认语义刻意不同**（m6 契约 §5.3）：RSS 的 pubDate
// 是结构化必填字段，默认过滤（7 天）是对的；而 web/search 的 Exa publishedDate 是从 HTML
// 猜的，默认必须关闭（否则删光无日期的官方页，§0.3 实测 0/15）。故此默认只属 RSS。
// 之所以要有默认而非默认全量，见 applyLookback 的注释。
const rssDefaultLookbackDays = 7

// Fetcher 是 RSS 抓取器。零依赖外部状态，可被多个 goroutine 并发复用。
type Fetcher struct {
	client   *http.Client
	parser   *gofeed.Parser
	maxBytes int64

	// lookupIP / isBlocked 抽成字段是为了可测试：httptest.Server 监听 127.0.0.1
	// 本会被私网规则拒绝，测试通过覆盖 isBlocked 放行环回来验证正常解析路径，
	// 同时另一条用例保持默认规则验证私网拦截。生产路径永远用默认实现。
	lookupIP  func(host string) ([]net.IP, error)
	isBlocked func(ip net.IP) bool

	// now 同样为可测试而抽出：lookback 过滤的截止点由它算出，测试注入固定时刻
	// 才能让带固定 pubDate 的 fixture 不随真实时间流逝而漂出窗口（否则用例会在
	// 未来某天无故变红）。生产路径用 time.Now。
	now func() time.Time

	// 正文补全依赖（nil = 不补全，行为与改造前一致）。
	// 只在 NewMulti 装配时注入：RSS 抓取器的单测全是纯 httptest，不该为了这条能力
	// 被迫拖进 Exa 客户端与数据库替身。
	enricher pageTextFetcher
	seen     SeenChecker
}

// itemContent 取条目正文：优先 Content，空则回退 Description（与 mapItems 一致）。
// 抽成函数是为了让补全判定与真正落库的那份正文用同一口径——两处不一致会让
// 「判定说需要补、映射时其实有正文」这类错配无声发生。
func itemContent(it *gofeed.Item) string {
	if it.Content != "" {
		return it.Content
	}
	return it.Description
}

// New 按抓取配置构造 Fetcher。TimeoutSeconds / MaxResponseMB 为非正数时回退到
// 与 config 默认值一致的兜底（20s / 5MB），避免误配成 0 导致立即超时或零上限。
func New(cfg config.FetchConfig) *Fetcher {
	timeout := time.Duration(cfg.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 20 * time.Second
	}
	maxMB := cfg.MaxResponseMB
	if maxMB <= 0 {
		maxMB = 5
	}

	f := &Fetcher{
		parser:    gofeed.NewParser(),
		maxBytes:  int64(maxMB) * 1024 * 1024,
		lookupIP:  net.LookupIP,
		isBlocked: defaultBlockedIP,
		now:       time.Now,
	}

	// Dialer.Control 在 DNS 解析之后、真正建连之前执行，拿到的是最终 IP，
	// 因此能挡住"域名首次解析公网、二次解析私网"的 DNS rebinding —— 这是
	// 抓取前一次性 LookupIP 预检挡不住的，两者互补。闭包捕获 f 以便测试覆盖。
	dialer := &net.Dialer{
		Timeout: 10 * time.Second,
		Control: func(_, address string, _ syscall.RawConn) error {
			host, _, err := net.SplitHostPort(address)
			if err != nil {
				host = address
			}
			if ip := net.ParseIP(host); ip != nil && f.isBlocked(ip) {
				return errBlockedDial
			}
			return nil
		},
	}
	f.client = &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			DialContext:         dialer.DialContext,
			TLSHandshakeTimeout: 10 * time.Second,
			// 禁用连接复用意义不大，但限制 idle 连接数避免抓取大量源时句柄膨胀。
			MaxIdleConnsPerHost: 2,
		},
	}
	return f
}

// FetchRSS 抓取单个 RSS 源并返回映射后的内容条目。
//
// 抓下来的条目会先经 applyLookback 按 config.lookback_days 滤掉过老的，再做映射。
//
// 失败语义：私网/非法 URL、非法 config → CodeValidation（不可重试）；超时 →
// CodeFetchTimeout；HTTP 429 → CodeFetchRateLimit；其余（非 2xx、超限、解析失败）
// 归入 fetch 类但按确定性/瞬态区分 Retryable。解析失败只返回 error，绝不 panic。
func (f *Fetcher) FetchRSS(ctx context.Context, src types.Source) ([]types.ContentItem, error) {
	u, err := url.Parse(src.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, types.NewAppError(types.CodeValidation,
			fmt.Sprintf("非法 RSS URL: %q", src.URL), err)
	}

	// 抓取前 IP 预检：把明显的私网/环回目标挡在发请求之前，
	// 并以 CodeValidation 明确告知调用方（这是配置/输入问题，非瞬态故障）。
	host := u.Hostname()
	if ips, lerr := f.lookupIP(host); lerr == nil {
		for _, ip := range ips {
			if f.isBlocked(ip) {
				return nil, types.NewAppError(types.CodeValidation,
					fmt.Sprintf("RSS 源 %q 解析到私网/环回地址 %s，已拒绝", host, ip), nil)
			}
		}
	}
	// LookupIP 失败不在此拦截：交给后续请求，由 Dialer.Control 兜底并归类为 fetch 错误。

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, src.URL, nil)
	if err != nil {
		return nil, types.NewAppError(types.CodeValidation, "构造抓取请求失败", err)
	}
	req.Header.Set("User-Agent", "Vane/0.3 (+https://vane.zhuoqidev.com)")
	req.Header.Set("Accept", "application/rss+xml, application/atom+xml, application/xml, text/xml;q=0.9, */*;q=0.5")

	resp, err := f.client.Do(req)
	if err != nil {
		return nil, classifyDoError(src.URL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTooManyRequests {
		return nil, types.NewAppError(types.CodeFetchRateLimit,
			fmt.Sprintf("抓取 %s 被限流(429)", src.URL), nil)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		ae := types.NewAppError(types.CodeFetchTimeout,
			fmt.Sprintf("抓取 %s 返回非 2xx 状态 %d", src.URL, resp.StatusCode), nil)
		// 5xx 视为瞬态可重试，4xx 视为确定性不可重试（覆盖默认的 true）。
		ae.Retryable = resp.StatusCode >= 500
		return nil, ae
	}

	// 读到 maxBytes+1 字节以便判断是否超限：LimitReader 会静默截断，
	// 只有多读 1 字节才能区分"恰好等于上限"与"超过上限"。
	data, err := io.ReadAll(io.LimitReader(resp.Body, f.maxBytes+1))
	if err != nil {
		return nil, classifyDoError(src.URL, err)
	}
	if int64(len(data)) > f.maxBytes {
		return nil, types.NewAppError(types.CodeValidation,
			fmt.Sprintf("抓取 %s 响应体超过 %d 字节上限", src.URL, f.maxBytes), nil)
	}

	feed, err := f.parser.Parse(bytes.NewReader(data))
	if err != nil {
		// 解析失败是确定性错误（同一份内容重试仍会失败），标记不可重试。
		ae := types.NewAppError(types.CodeFetchTimeout,
			fmt.Sprintf("解析 %s 的 RSS 失败", src.URL), err)
		ae.Retryable = false
		return nil, ae
	}
	if feed == nil {
		return nil, nil
	}

	items, err := f.applyLookback(src, feed.Items)
	if err != nil {
		return nil, err
	}
	items = f.applyCategories(src, items)

	// 正文补全放在**正常过滤之后、映射之前**（见 enrich.go）：
	//   - 之后：不为一条马上要被 lookback/categories 滤掉的条目付费。
	//   - 之前：补回来的正文要参与 finalize 的指纹与 §12.3 护栏判定；放到映射之后，
	//     链接型条目会先被护栏丢掉，补全永远等不到执行。
	skippedSeen := f.enrichItems(ctx, src, items)

	// 全灭判定必须在此处、比较**映射函数的入参与产出**——不能拿 feed.Items 当分母。
	// applyLookback / applyCategories 是用户声明的正常过滤（B 类），它们在这一行之前
	// 就把条目摘掉了；若拿过滤前的条数当分母，一次寻常的「博客 8 天没更新、RSS 里全是
	// 7 天窗口外的旧闻」就会被判成抓取失败，每轮误告警、10 轮后还会把健康的源自动停用。
	mapped, tally := mapItems(src, items)

	// 分母要扣掉「因为库里已有其正文而刻意跳过补全」的条目（2026-07-19 生产实测抓到的
	// 误报）：成本闸门与全灭防线会互相踩——闸门正确地不重复付费，但被跳过的条目本轮
	// 仍是原样（含裸 HTML），照旧被 §12.3 丢弃，于是"全灭"成立、信源被判成故障。
	//
	// 而那些内容**我们本来就有**，什么都没丢。生产上 Gemini 官方博客正因此走到
	// fail_count=2：再一轮告警、八轮后一个完全健康的源会被自动停用。
	//
	// 扣掉之后语义才对：分母是「这一轮真正指望它产出的条目数」。全都是已有内容时
	// 分母为 0，回到「合法空轮」，与 feed 本来就没新东西同等对待——事实上也确实如此。
	denom := len(items) - skippedSeen
	if denom > 0 && len(mapped) == 0 {
		return nil, allDroppedErr(src, denom, tally)
	}
	return mapped, nil
}

// rssSourceConfig 是 RSS 信源的 config JSONB 结构。
type rssSourceConfig struct {
	// LookbackDays 只收最近 N 天发布的条目：0（含字段缺省）用默认 rssDefaultLookbackDays（7 天）；
	// <0 表示不限（全量）。**与 web/search 默认语义相反**——web/search 的 lookback 默认关闭、
	// 仅作逃生阀（Exa 的 publishedDate 从 HTML 猜、官方页普遍猜不出，m6 契约 §5.3/§0.3）。
	// 本键对 web/feed 默认开启、对 web/search 默认关闭，不是"跨源统一"。
	LookbackDays int      `json:"lookback_days,omitempty"`
	Categories   []string `json:"categories,omitempty"`
}

// applyLookback 按 src.Config 的 lookback_days 丢弃过老的条目。
//
// 为什么 RSS 必须做这层过滤（2026-07-16 生产实证）：feed 常把全部历史塞在一份文档里
// （openai.com/news/rss.xml 实测 1038 条，最早回溯到 2023 年）。首抓会把它们一次性入库
// 且 fetched_at 全部相同，而候选窗口 ListUnpushedByUser 按 (fetched_at DESC, id DESC)
// 取——feed 按时间倒序解析、越老的条目 id 越大，于是候选窗口恰好被最老的条目占满。
// 下游两道闸门都拦不住：scorer 的 prompt 不含发布时间（模型看不出是旧闻），selector
// 的新鲜度衰减封顶 12 分（85 分的 2023 年旧闻扣完仍有 73 分照样出线）。净效果是首批
// 推送全是陈年旧闻，并污染 M5 的反馈与画像数据。
//
// 无 PublishedParsed 的条目一律保留：feed 不给日期时无从判断新旧，丢弃会让这类源静默
// 颗粒无收——那比放进几条旧闻更难发现。代价是无日期的 feed 仍可能带进历史条目（已知
// 取舍，实测 openai / blog.google 均提供 pubDate）。
func (f *Fetcher) applyLookback(src types.Source, items []*gofeed.Item) ([]*gofeed.Item, error) {
	var sc rssSourceConfig
	if len(src.Config) > 0 {
		if err := json.Unmarshal(src.Config, &sc); err != nil {
			return nil, types.NewAppError(types.CodeValidation,
				fmt.Sprintf("解析 RSS 信源 config 失败（source_id=%d）", src.ID), err)
		}
	}

	lookback := sc.LookbackDays
	if lookback == 0 {
		lookback = rssDefaultLookbackDays
	}
	if lookback < 0 {
		return items, nil
	}

	cutoff := f.now().UTC().Add(-time.Duration(lookback) * 24 * time.Hour)
	out := make([]*gofeed.Item, 0, len(items))
	dropped := 0
	for _, it := range items {
		if it == nil {
			continue
		}
		if it.PublishedParsed != nil && it.PublishedParsed.Before(cutoff) {
			dropped++
			continue
		}
		out = append(out, it)
	}
	if dropped > 0 {
		slog.Debug("RSS lookback 过滤掉过老条目",
			"source_id", src.ID, "lookback_days", lookback,
			"dropped", dropped, "kept", len(out))
	}
	return out, nil
}

func (f *Fetcher) applyCategories(src types.Source, items []*gofeed.Item) []*gofeed.Item {
	var sc rssSourceConfig
	if len(src.Config) > 0 {
		_ = json.Unmarshal(src.Config, &sc)
	}
	if len(sc.Categories) == 0 {
		return items
	}

	want := make(map[string]struct{}, len(sc.Categories))
	for _, c := range sc.Categories {
		want[strings.ToLower(strings.TrimSpace(c))] = struct{}{}
	}

	out := make([]*gofeed.Item, 0, len(items))
	for _, it := range items {
		if it == nil {
			continue
		}
		if matchesCategory(it.Categories, want) {
			out = append(out, it)
		}
	}
	if dropped := len(items) - len(out); dropped > 0 {
		slog.Debug("RSS categories 过滤掉不匹配条目",
			"source_id", src.ID, "categories", sc.Categories,
			"dropped", dropped, "kept", len(out))
	}
	return out
}

func matchesCategory(itemCats []string, want map[string]struct{}) bool {
	for _, c := range itemCats {
		if _, ok := want[strings.ToLower(strings.TrimSpace(c))]; ok {
			return true
		}
	}
	return false
}

// mapItems 把 gofeed 条目映射为 ContentItem，并在入库前算好 content_hash（精确指纹）
// 与 simhash（近似指纹）——原设计把这步留给 dedup 环节，但 dedup 只在内存回填、
// 从不写回 content_items，导致两列恒为空、跨批去重全失效（审查 CRITICAL 数据链）。
// 在此落库前算好，才能让 ListRecentSimhashes 拿到历史、UNIQUE 精确去重生效。
// 第二个返回值是本轮各原因的丢弃计数，供调用方判定「全灭」（见 drop.go）。
func mapItems(src types.Source, items []*gofeed.Item) ([]types.ContentItem, dropTally) {
	now := time.Now().UTC()
	var tally dropTally
	out := make([]types.ContentItem, 0, len(items))
	for _, it := range items {
		if it == nil {
			continue
		}
		// external_id 用 GUID 兜底 Link：部分源不给 guid，link 作为源内唯一键。
		// 注意 external_id 自 007 起**不再是身份**（BBC 给同一篇文章发多个 guid，
		// 生产 13 组冗余全是 url 相同、guid 不同），只作为源侧原始 ID 留档；
		// 身份是 finalize 算出的 canonical_key（rss 认 url，见 CanonicalKey）。
		externalID := it.GUID
		if externalID == "" {
			externalID = it.Link
		}
		// content 优先取全文 Content，缺失则退回摘要 Description。
		content := itemContent(it)

		item := types.ContentItem{
			SourceID:    src.ID,
			ExternalID:  externalID,
			URL:         it.Link,
			Title:       it.Title,
			Content:     content,
			Author:      authorName(it),
			PublishedAt: it.PublishedParsed,
			FetchedAt:   now,
			Kind:        types.KindArticle, // rss 产出的是"一篇内容"（M6 契约 §7.2(b)：构造处赋值，finalize 只校验）
		}
		// 无 link 的条目在此被丢弃：rss 的身份就是 url，没有 url 就没有身份。
		if r := finalize(src, &item); r != dropNone {
			tally.add(r)
			continue
		}
		out = append(out, item)
	}
	return out, tally
}

// truncateUTF8 把 s 截到至多 max 字节，并回退到最近的 rune 边界，保证结果是
// 合法 UTF-8。裸 s[:max] 会把多字节字符（中文 3 字节、emoji 4 字节）从中间切裂，
// 产生非法字节序列——Postgres 以 22021 invalid byte sequence 拒绝 INSERT，该条
// 内容每轮抓取都在同一位置截坏、永久无法入库（审查 CRITICAL）。
func truncateUTF8(s string, max int) string {
	if len(s) <= max {
		return s
	}
	cut := max
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
}

// noRedirect 禁止 http.Client 跟随重定向，30x 原样返回给调用方按非 2xx 处理。
// 原因：Go 跨域重定向只剥离 Authorization/Cookie 等四个标准头，自定义头
// （如 Exa 的 x-api-key）会被原样带到任意 Location 目标——上游 CDN 配置错误或
// 域名被劫持时一次 30x 即可外带凭证。API 调用没有跟随重定向的正当需求。
func noRedirect(_ *http.Request, _ []*http.Request) error {
	return http.ErrUseLastResponse
}

// finalize 为一条抓取到的内容补齐落库前的确定性字段：精确指纹 content_hash、
// 近似指纹 simhash、全局身份 canonical_key、以及 external_id 兜底。全部抓取器
// （RSS/Exa/TikHub）共用此逻辑，保证计算方式一致——尤其 simhash 必须在抓取时就
// 写入 content_items，Dedup 的排除自撞（查历史 simhash 时排除本批 ID）才成立。
// 调用方只需填好业务字段。
//
// 需要 src 是因为 canonical_key 按 Platform 分派（web 认 url、xhs 认
// note_id，见 CanonicalKey）——身份不是内容自己能决定的，得知道它从哪来。
//
// 返回 false 表示**这条必须丢弃**，三个 map 函数一律 continue（契约 §5）。做成
// 返回值而非让调用方自己查 CanonicalKey，是为了让"忘了判空"这件事写不出来。
//
// 顺序有依赖，不能重排：content_hash 先算（external_id 兜底要用它）；canonical_key
// 必须在 external_id 兜底**之前**算，否则 xhs 缺 note_id 时会拿到 content_hash 兜底
// 出来的假身份——正文一改就变，同一笔记长出 N 份，而判空分支永远不触发。
// 返回 dropNone 表示该条可以入库；否则返回丢弃原因，供调用方累计成 dropTally
// 并在「全灭」时组装用户可读的诊断（见 drop.go）。原先返回 bool，只够决定去留、
// 不够回答「为什么一条都没剩下」——而那正是 HN 静默零产出事故里唯一缺的信息。
func finalize(src types.Source, item *types.ContentItem) dropReason {
	if item.FetchedAt.IsZero() {
		item.FetchedAt = time.Now().UTC()
	}
	// 精确指纹：让 content_hash 列真正可用（同文去重依赖它）。
	item.ContentHash = dedup.ContentHash(*item)
	// 近似指纹：72h 跨批近似去重依赖它，抓取时即落库。
	sh := dedup.Simhash(item.Title + " " + item.Content)
	item.Simhash = &sh

	// 全局身份：落库前就必须定好。M5 起内容按 canonical_key 全局唯一。
	// 若调用方已预填 CanonicalKey，不覆盖。
	if item.CanonicalKey == "" {
		item.CanonicalKey = CanonicalKey(src, *item)
	}
	if item.CanonicalKey == "" {
		slog.Warn("fetcher: 内容缺少身份字段，跳过该条",
			"source_id", src.ID, "platform", src.Platform, "url", item.URL, "title", item.Title)
		return dropNoIdentity
	}

	// Kind 必须非空（M6 契约 §7.2(b)）——做成"写不出来"而非注释提醒。零值 "" 会被
	// UpsertContentItem 显式 INSERT、覆盖 DB 列的 DEFAULT 'article'，下游 Dedup 拿到
	// 空 Kind 按 article 处理 → 页面变化被 simhash 静默吞掉，且无任何错误信号
	// （2026-07-16 生产实证：008 上线后全部新内容 kind 落成空串，012 回填）。
	// 这里只校验不兜底：Kind 是"这条内容是什么"的事实，只有构造 item 的抓取器知道；
	// 在此补默认值会把"抓取器忘了赋值"这个 bug 永久藏起来。
	if item.Kind == "" {
		slog.Warn("fetcher: 内容缺少 kind，跳过该条",
			"source_id", src.ID, "platform", src.Platform, "url", item.URL, "title", item.Title)
		return dropNoKind
	}

	if htmlTagRe.MatchString(item.Content) {
		slog.Warn("fetcher: 正文含裸 HTML，抽取未在指纹之前完成（契约 §12.3），跳过该条",
			"source_id", src.ID, "url", item.URL, "title", item.Title)
		return dropBareHTML
	}

	// 无自然外部 ID 时用 content_hash 派生稳定键，避免空串在源内唯一约束上
	// 静默冲突吞条目（001 的设计约束）。注意这是**留档字段**不是身份：007 起
	// external_id 只作为"该源给这条内容的 id"存进 content_sources 供溯源。
	if item.ExternalID == "" {
		item.ExternalID = item.ContentHash
	}
	return dropNone
}

// authorName 从 gofeed 条目提取作者名。Author 已被 gofeed 标记 deprecated，
// 优先用 Authors[0]，兜底回退 Author，两者皆空返回空串。
func authorName(it *gofeed.Item) string {
	if len(it.Authors) > 0 && it.Authors[0] != nil && it.Authors[0].Name != "" {
		return it.Authors[0].Name
	}
	if it.Author != nil {
		return it.Author.Name
	}
	return ""
}

// classifyDoError 把 client.Do / 读取 body 阶段的错误归类为 AppError。
// 超时（含 context 截止、net 超时）→ CodeFetchTimeout（可重试）；
// 私网连接拦截 → CodeValidation；其余 → CodeFetchTimeout 但不可重试。
func classifyDoError(rawURL string, err error) *types.AppError {
	if errors.Is(err, errBlockedDial) {
		return types.NewAppError(types.CodeValidation,
			fmt.Sprintf("抓取 %s 时命中私网地址拦截", rawURL), err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return types.NewAppError(types.CodeFetchTimeout,
			fmt.Sprintf("抓取 %s 超时", rawURL), err)
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return types.NewAppError(types.CodeFetchTimeout,
			fmt.Sprintf("抓取 %s 超时", rawURL), err)
	}
	ae := types.NewAppError(types.CodeFetchTimeout,
		fmt.Sprintf("抓取 %s 失败", rawURL), err)
	ae.Retryable = false // 非超时的连接/读取失败按确定性处理，避免无谓重试。
	return ae
}

// blockedCIDRs 是 Go 的 IsPrivate 未覆盖、但抓取时同样应拦截的特殊用途段：
// 运营商级 NAT / Tailscale 常用的 100.64.0.0/10、IANA 特殊用途/文档段等。
// 若 VPS 处于 CGNAT/Tailscale 网络，缺这些段会让用户提交解析到 100.64.x.x 的
// 源探测内网主机（审查 SSRF 缺口）。包级预编译，判定时 Contains。
var blockedCIDRs = func() []*net.IPNet {
	nets := []string{
		"100.64.0.0/10",   // CGNAT / Tailscale
		"192.0.0.0/24",    // IETF 协议分配
		"198.18.0.0/15",   // 基准测试
		"192.0.2.0/24",    // 文档 TEST-NET-1
		"198.51.100.0/24", // 文档 TEST-NET-2
		"203.0.113.0/24",  // 文档 TEST-NET-3
	}
	out := make([]*net.IPNet, 0, len(nets))
	for _, c := range nets {
		if _, n, err := net.ParseCIDR(c); err == nil {
			out = append(out, n)
		}
	}
	return out
}()

// defaultBlockedIP 判定一个 IP 是否属于禁止抓取的内网/特殊地址段。
// 覆盖：私网（RFC1918 / ULA）、环回、链路本地、未指定地址，
// 外加 blockedCIDRs 里的 CGNAT/文档等特殊用途段。
func defaultBlockedIP(ip net.IP) bool {
	if ip.IsPrivate() ||
		ip.IsLoopback() ||
		ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() ||
		ip.IsUnspecified() {
		return true
	}
	for _, n := range blockedCIDRs {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}
