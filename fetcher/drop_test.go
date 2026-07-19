package fetcher

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/YouToco/vane/types"
)

// feedWithItems 拼一份 RSS，itemBody 是每条 <item> 的内部 XML。
func feedWithItems(itemBodies ...string) string {
	var b strings.Builder
	b.WriteString(`<?xml version="1.0"?><rss version="2.0"><channel><title>测试源</title>`)
	for _, it := range itemBodies {
		b.WriteString("<item>" + it + "</item>")
	}
	b.WriteString(`</channel></rss>`)
	return b.String()
}

// bareHTMLItem 复刻 Hacker News RSS 的真实形态：description 是被 CDATA 包住的裸锚点。
// 这正是 2026-07-18 生产事故的形态——30 条全长这样，全部命中 §12.3 护栏。
func bareHTMLItem(n int) string {
	return fmt.Sprintf(
		`<title>条目 %d</title><link>https://example.com/p/%d</link>`+
			`<description><![CDATA[<a href="https://news.ycombinator.com/item?id=%d">Comments</a>]]></description>`,
		n, n, n)
}

func serveFeed(t *testing.T, body string) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/rss+xml")
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func feedSource(url string) types.Source {
	return types.Source{ID: 42, Platform: types.PlatformWeb, Capability: types.CapFeed, URL: url}
}

// TestFetchRSS_AllDroppedIsAnError 是本次修复的核心守卫。
//
// 2026-07-18 生产实证：加了 Hacker News RSS 后抓取路径全程「成功」——last_fetched_at
// 正常刷新、fail_count=0、status=active——而该源入库内容数恒为 0。真相只在 journalctl 的
// 30 行 WARN 里，用户侧看到的是一个健康的 active 信源却永远收不到东西。
//
// 报错（而非只记日志）是刻意的：错误会经 markFetchResult(false) → fail_count++ →
// 连续 3 次 → 飞书告警卡，卡里「原因：」字段直接渲染这里的 AppError.Message。
func TestFetchRSS_AllDroppedIsAnError(t *testing.T) {
	bodies := make([]string, 30)
	for i := range bodies {
		bodies[i] = bareHTMLItem(i + 1)
	}
	url := serveFeed(t, feedWithItems(bodies...))

	f := newTestFetcher()
	items, err := f.FetchRSS(context.Background(), feedSource(url))

	if err == nil {
		t.Fatalf("30 条全部被裸 HTML 护栏丢弃却没报错——这正是被修的静默零产出："+
			"抓取「成功」、入库 0 条、用户侧无任何信号（实得 %d 条）", len(items))
	}
	var ae *types.AppError
	if !errors.As(err, &ae) {
		t.Fatalf("应返回 AppError（其 Message 会原样进飞书告警卡），实得 %T", err)
	}
	// 文案必须足以让用户判断"这个源坏在哪"，否则告警卡等于没说。
	for _, want := range []string{"30", string(dropBareHTML), "source_id=42"} {
		if !strings.Contains(ae.Message, want) {
			t.Errorf("告警文案缺少 %q，用户看不出问题所在：%s", want, ae.Message)
		}
	}
	// 红线：绝不把 feed 原文拼进错误——它会原样出现在飞书卡片里。
	if strings.Contains(ae.Message, "<a href") || strings.Contains(ae.Message, "CDATA") {
		t.Errorf("错误文案把源站原文带进了用户可见的告警卡：%s", ae.Message)
	}
}

// TestFetchRSS_LookbackFilteredAllIsNotAnError 是**防误报**守卫，比上一条更容易被写错。
//
// 生产有 5 个 feed 源走默认 7 天 lookback。任何一个博客只要 8 天不更新，它的 RSS 照样
// 返回满满 30 条旧条目、被 applyLookback 全部正常滤掉、入库 0 条。
//
// 若「全灭」的分母取 feed.Items（过滤前），这种寻常情况会被判成抓取失败：每轮误告警，
// 连续 10 轮后还会把一个**完全健康**的源自动停用。所以判定必须发生在 lookback /
// categories 之后，比较的是映射函数的入参与产出。
func TestFetchRSS_LookbackFilteredAllIsNotAnError(t *testing.T) {
	old := time.Now().UTC().Add(-30 * 24 * time.Hour).Format(time.RFC1123Z)
	bodies := make([]string, 12)
	for i := range bodies {
		bodies[i] = fmt.Sprintf(
			`<title>旧闻 %d</title><link>https://example.com/old/%d</link>`+
				`<description>正文</description><pubDate>%s</pubDate>`, i, i, old)
	}
	url := serveFeed(t, feedWithItems(bodies...))

	f := newTestFetcher()
	items, err := f.FetchRSS(context.Background(), feedSource(url))

	if err != nil {
		t.Fatalf("全部条目被 lookback 正常滤掉不是故障（博客几天没更新就会这样），"+
			"报错会导致每轮误告警、10 轮后自动停用健康信源：%v", err)
	}
	if len(items) != 0 {
		t.Errorf("30 天前的条目应被 7 天 lookback 滤掉，实得 %d 条", len(items))
	}
}

// TestFetchRSS_EmptyFeedIsNotAnError：源本轮真的没给条目 —— 合法空轮。
// 「无内容可推必须仍是正常终态」是红线，分母为 0 时绝不能报错。
func TestFetchRSS_EmptyFeedIsNotAnError(t *testing.T) {
	url := serveFeed(t, feedWithItems())

	f := newTestFetcher()
	items, err := f.FetchRSS(context.Background(), feedSource(url))
	if err != nil {
		t.Fatalf("空 feed 是合法空轮，不得报错：%v", err)
	}
	if len(items) != 0 {
		t.Errorf("空 feed 应产出 0 条，实得 %d", len(items))
	}
}

// TestFetchRSS_PartialDropIsNotAnError：只要还剩下一条，就不是"源坏了"。
// 防线针对的是**全灭**；部分丢弃是常态（个别条目没有 link 之类），报错会让噪音淹没信号。
func TestFetchRSS_PartialDropIsNotAnError(t *testing.T) {
	url := serveFeed(t, feedWithItems(
		bareHTMLItem(1),
		`<title>好条目</title><link>https://example.com/ok</link><description>干净正文</description>`,
	))

	f := newTestFetcher()
	items, err := f.FetchRSS(context.Background(), feedSource(url))
	if err != nil {
		t.Fatalf("尚有条目存活时不得报错：%v", err)
	}
	if len(items) != 1 {
		t.Errorf("应留下 1 条干净条目，实得 %d", len(items))
	}
}

// TestDropTallySummaryIsStable：诊断文案的顺序必须稳定。
// map 遍历顺序随机会让同一个故障每次长得不一样，看起来像好几个不同的故障。
func TestDropTallySummaryIsStable(t *testing.T) {
	build := func() string {
		var tl dropTally
		for i := 0; i < 27; i++ {
			tl.add(dropBareHTML)
		}
		for i := 0; i < 3; i++ {
			tl.add(dropNoIdentity)
		}
		tl.add(dropNoKind)
		return tl.summary()
	}
	want := build()
	for i := 0; i < 20; i++ {
		if got := build(); got != want {
			t.Fatalf("同一组丢弃计数产出了不同文案：%q vs %q", got, want)
		}
	}
	// 按条数降序：最主要的原因排在最前，用户一眼看到重点。
	if !strings.HasPrefix(want, string(dropBareHTML)) {
		t.Errorf("条数最多的原因应排最前，实得 %q", want)
	}
}

// TestDropTally_NoneIsNotCounted：dropNone 不是丢弃，混进计数会让"全灭"判定失真。
func TestDropTally_NoneIsNotCounted(t *testing.T) {
	var tl dropTally
	tl.add(dropNone)
	tl.add(dropNone)
	if tl.total != 0 {
		t.Errorf("dropNone 不该被计数，实得 total=%d", tl.total)
	}
	if s := tl.summary(); s != "" {
		t.Errorf("无丢弃时摘要应为空，实得 %q", s)
	}
}

// ---- Exa 搜索路径的全灭防线 ----
//
// 这一组是**对抗审查抓出来的漏洞**：原提交声称把防线推广到「RSS 与 Exa 两条路径」，
// 并列举了 6 条反向验证，但**没有一条落在 Exa search 上**——把 exa.go 的守卫整段删掉，
// 测试照样全绿。声称做过而实际没做，比没做更糟：它让后来的人以为那里有守卫。
//
// 这条路径在生产完全可达：净化（sanitizeContentsText）只存在于 exa_contents.go，
// exa.go 的 search 分支对 r.Text 只做截断不做净化，Exa 一旦返回带 HTML 的正文，
// 整批命中 htmlTagRe → 全灭。**HN 事故在 Exa 侧的同构形态。**

func serveExa(t *testing.T, body string) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

// TestExaFetch_AllDroppedIsAnError：Exa 返回了结果却一条都没能入库 → 必须报错。
func TestExaFetch_AllDroppedIsAnError(t *testing.T) {
	body := `{"results":[
		{"id":"a","url":"https://x.example/1","title":"一","text":"<div>正文</div>"},
		{"id":"b","url":"https://x.example/2","title":"二","text":"<p>正文</p>"}
	]}`
	items, err := newTestExa(serveExa(t, body)).Fetch(context.Background(), exaSrc(9, `{"query":"q"}`))
	if err == nil {
		t.Fatalf("2 条结果全被裸 HTML 护栏丢弃却没报错——Exa 侧的静默零产出（实得 %d 条）", len(items))
	}
	var ae *types.AppError
	if !errors.As(err, &ae) {
		t.Fatalf("应返回 AppError，实得 %T", err)
	}
	if !strings.Contains(ae.Message, string(dropBareHTML)) {
		t.Errorf("告警文案应点明丢弃原因，实得：%s", ae.Message)
	}
}

// TestExaFetch_EmptyResultsIsNotAnError 是 Exa 侧的**防误报**守卫。
//
// 缺它的代价具体而严重：把守卫的 `len(er.Results) > 0` 前置去掉后测试依然全绿，
// 而那个版本会让一个窄查询的搜索源（Exa 正常返回 `{"results":[]}`）每轮都报错
// → fail_count++ → 第 3 轮告警「条目全部无法入库（0 条：，source_id=N）」
// → 第 10 轮把一个**完全健康**的源自动停用。
func TestExaFetch_EmptyResultsIsNotAnError(t *testing.T) {
	items, err := newTestExa(serveExa(t, `{"results":[]}`)).Fetch(context.Background(), exaSrc(9, `{"query":"窄查询"}`))
	if err != nil {
		t.Fatalf("Exa 返回空结果集是合法空轮（窄查询、服务端时间过滤后没命中都会这样），"+
			"报错会导致每轮误告警、10 轮后自动停用健康的搜索源：%v", err)
	}
	if len(items) != 0 {
		t.Errorf("空结果集应产出 0 条，实得 %d", len(items))
	}
}

// TestExaFetch_PartialDropIsNotAnError：只要还剩一条就不是"源坏了"。
func TestExaFetch_PartialDropIsNotAnError(t *testing.T) {
	body := `{"results":[
		{"id":"a","url":"https://x.example/1","title":"坏的","text":"<div>x</div>"},
		{"id":"b","url":"https://x.example/2","title":"好的","text":"干净正文"}
	]}`
	items, err := newTestExa(serveExa(t, body)).Fetch(context.Background(), exaSrc(9, `{"query":"q"}`))
	if err != nil {
		t.Fatalf("尚有条目存活时不得报错：%v", err)
	}
	if len(items) != 1 {
		t.Errorf("应留下 1 条干净结果，实得 %d", len(items))
	}
}

// TestDropEmptyResult_两条路径语义相反且都被钉住
//
// dropEmptyResult 在两条路径上**含义是相反的**，这是刻意的、但此前无人守：
//   - exa_contents（页面监控）：Exa 还没抓到页面正文 → 合法空轮，放行
//   - exa search：一条既无 url 又无标题的结果 → 是残缺数据，计入全灭分子
//
// 差异合理（"页面还没爬到" vs "搜索结果本身是残的"），但**两侧都无用例锁住**时，
// 任何一侧的改动都不会变红。这里把两侧同时钉死。
func TestDropEmptyResult_两条路径语义相反且都被钉住(t *testing.T) {
	// search 侧：全是残缺结果 → 计入分子 → 报错，且**文案里要点明原因**。
	// 只断言"报错了"是不够的：残缺结果即使不进 tally 也照样 continue、mapped 仍为 0、
	// 照样报错——只是文案变成「条目全部无法入库（1 条：，source_id=9）」，原因栏是空的。
	// 那样的告警卡等于没说，用户看不出源坏在哪。所以必须断言原因真的被记进去了。
	body := `{"results":[{"id":"a","url":"","title":"","text":"有正文但无身份"}]}`
	_, err := newTestExa(serveExa(t, body)).Fetch(context.Background(), exaSrc(9, `{"query":"q"}`))
	if err == nil {
		t.Fatal("search 侧：结果既无 url 又无标题是残缺数据，全是这种时应报错")
	}
	var ae *types.AppError
	if !errors.As(err, &ae) {
		t.Fatalf("应返回 AppError，实得 %T", err)
	}
	if !strings.Contains(ae.Message, string(dropEmptyResult)) {
		t.Errorf("告警文案必须点明是 %q，否则原因栏为空、用户看不出源坏在哪：%s", dropEmptyResult, ae.Message)
	}
	// contents 侧由 TestExaContents_映射拒收会带出原因 守住（dropEmptyResult 放行为空轮）。
}
