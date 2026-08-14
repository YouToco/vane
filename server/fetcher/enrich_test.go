package fetcher

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/YouToco/vane/types"
)

// fakeEnricher 记录被请求过哪些 URL——**调用次数是成本，必须逐次可断言**。
type fakeEnricher struct {
	mu    sync.Mutex
	calls []string
	text  string
	err   error
}

func (f *fakeEnricher) pageResultsWithEffectGate(
	ctx context.Context,
	pageURL string,
	_ int,
	_ *types.FetchTarget,
	beforeEffect func(context.Context) error,
) ([]exaContentsResult, bool, error) {
	if err := checkEffectGate(ctx, beforeEffect); err != nil {
		return nil, false, err
	}
	f.mu.Lock()
	f.calls = append(f.calls, pageURL)
	f.mu.Unlock()
	if f.err != nil {
		return nil, false, f.err
	}
	return []exaContentsResult{{Text: f.text}}, false, nil
}

func (f *fakeEnricher) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

// fakeSeen 是 SeenChecker 替身：enriched 里的键视为"已入库且正文已补全"。
// enrichSeen 与 binding_test.go 的 fakeSeen 分开：那个恒返回全集、不支持报错与计次，
// 而本组用例要断言的恰恰是「查了几次」「错误时会不会误付费」。
type enrichSeen struct {
	enriched map[string]struct{}
	err      error
	queried  int
}

func (f *enrichSeen) EnrichedCanonicalKeys(_ context.Context, keys []string, _ int) (map[string]struct{}, error) {
	f.queried++
	if f.err != nil {
		return nil, f.err
	}
	out := map[string]struct{}{}
	for _, k := range keys {
		if _, ok := f.enriched[k]; ok {
			out[k] = struct{}{}
		}
	}
	return out, nil
}

// hnLikeFeed 复刻 Hacker News 的真实形态：只有标题、链接和一个指向评论区的裸锚点，
// **正文一个字都没有**。这正是 2026-07-18 生产事故的形态。
func hnLikeFeed(n int) string {
	bodies := make([]string, n)
	for i := range bodies {
		bodies[i] = fmt.Sprintf(
			`<title>条目 %d</title><link>https://example.com/a/%d</link>`+
				`<description><![CDATA[<a href="https://news.ycombinator.com/item?id=%d">Comments</a>]]></description>`,
			i, i, i)
	}
	return feedWithItems(bodies...)
}

func enrichFetcher(t *testing.T, en pageTextFetcher, seen SeenChecker) *Fetcher {
	t.Helper()
	f := newTestFetcher()
	f.enricher = en
	f.seen = seen
	return f
}

// longBody 刻意远超 enrichMinRunes，避免门槛微调时用例连带失效——
// 夹具的长度不该成为被测行为的一部分。
const longBody = "这是一段足够长的正文，用来稳稳越过补全门槛。" +
	"链接型聚合器只给标题和地址，真正的内容要跟着链接去原文里取回来，" +
	"取回来之后才谈得上打分与出卡，否则整批会被裸 HTML 护栏丢弃。" +
	"这段文字反复叙述同一件事只为凑够长度，内容本身没有意义。" +
	"再写一句以确保余量充足，让门槛在合理范围内变动时本用例都不受影响。"

// TestEnrich_LinkOnlyFeedBecomesUsable 是这条能力的核心用例。
//
// 修复前：HN 形态的 30 条全部被 §12.3 护栏丢弃，用户一条都收不到。
// 修复后：跟着 link 取回原文，条目变成可打分、可出卡的内容。
func TestEnrich_LinkOnlyFeedBecomesUsable(t *testing.T) {
	en := &fakeEnricher{text: longBody}
	f := enrichFetcher(t, en, &enrichSeen{})
	url := serveFeed(t, hnLikeFeed(3))

	items, err := f.FetchRSS(context.Background(), feedSource(url))
	if err != nil {
		t.Fatalf("补全后不应再全灭：%v", err)
	}
	if len(items) != 3 {
		t.Fatalf("3 条链接型条目补全后都应可用，实得 %d 条", len(items))
	}
	if en.count() != 3 {
		t.Errorf("应为 3 条各取一次原文，实得 %d 次", en.count())
	}
	for i, it := range items {
		if !strings.Contains(it.Content, "链接型聚合器") {
			t.Errorf("第 %d 条正文没有换成取回的原文：%q", i, it.Content)
		}
		if strings.Contains(it.Content, "<a href") {
			t.Errorf("第 %d 条仍残留 feed 里的裸锚点，会被 §12.3 护栏丢掉：%q", i, it.Content)
		}
	}
}

func TestEnrich_EffectGateFailureIsNotSwallowed(t *testing.T) {
	en := &fakeEnricher{text: longBody}
	f := enrichFetcher(t, en, &enrichSeen{})
	url := serveFeed(t, hnLikeFeed(1))

	var calls atomic.Int32
	errRevoked := errors.New("compiled task revoked")
	beforeEffect := func(context.Context) error {
		if calls.Add(1) == 1 {
			return nil // RSS feed GET remains authorized.
		}
		return errRevoked // Revoked before the Exa enrichment call.
	}

	_, err := f.fetchRSSWithEffectGate(
		t.Context(), feedSource(url), enrichMaxPerRound, beforeEffect)
	if !errors.Is(err, errRevoked) {
		t.Fatalf("Fetch error = %v, want revocation", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("effect gate calls = %d, want feed + enrichment", got)
	}
	if got := en.count(); got != 0 {
		t.Fatalf("enrichment upstream calls after revocation = %d, want 0", got)
	}
}

// TestEnrich_SeenGateSkipsPaidCalls 钉住第一道成本闸门。
//
// HN 每半小时抓一轮，但每轮真正新上榜的只有几条。若不查 SeenChecker，
// 每轮都会为同样的 30 篇文章重新付费——一天 48 轮就是 1440 次付费调用。
// 闸门按 canonical_key 全局查，跨源命中同一篇时也只有第一个源付费。
func TestEnrich_SeenGateSkipsPaidCalls(t *testing.T) {
	url := serveFeed(t, hnLikeFeed(3))
	src := feedSource(url)

	// 前两条已在库里且正文已补全 → 只应为第三条付费。
	seen := &enrichSeen{enriched: map[string]struct{}{
		CanonicalKey(src, types.ContentItem{URL: "https://example.com/a/0"}): {},
		CanonicalKey(src, types.ContentItem{URL: "https://example.com/a/1"}): {},
	}}
	en := &fakeEnricher{text: longBody}

	if _, err := enrichFetcher(t, en, seen).FetchRSS(context.Background(), src); err != nil {
		t.Fatalf("FetchRSS 失败：%v", err)
	}
	if en.count() != 1 {
		t.Errorf("已补全的 2 条不得重新付费，应只调用 1 次，实得 %d 次（调用了 %v）", en.count(), en.calls)
	}
	if seen.queried != 1 {
		t.Errorf("闸门应一次性批量查询，实得 %d 次往返", seen.queried)
	}
}

// TestEnrich_SeenGateFailureSkipsRatherThanPays：闸门查不动时**跳过补全**，不是"全部补全"。
// 数据库抖一下就打出一批付费调用是最坏的失败方向——内容不丢（下轮再补），钱花出去要不回来。
func TestEnrich_SeenGateFailureSkipsRatherThanPays(t *testing.T) {
	en := &fakeEnricher{text: longBody}
	seen := &enrichSeen{err: errors.New("数据库抖动（测试构造）")}
	url := serveFeed(t, hnLikeFeed(5))

	if _, err := enrichFetcher(t, en, seen).FetchRSS(context.Background(), feedSource(url)); err == nil {
		t.Log("闸门失效时不补全，条目仍会被护栏丢弃 —— 这是预期的降级")
	}
	if en.count() != 0 {
		t.Errorf("闸门查询失败时不得付费补全，实得 %d 次调用", en.count())
	}
}

// TestEnrich_PerRoundCap 钉住第二道闸门：首轮或大量翻榜时也不会一次打出几十个付费调用。
func TestEnrich_PerRoundCap(t *testing.T) {
	en := &fakeEnricher{text: longBody}
	url := serveFeed(t, hnLikeFeed(enrichMaxPerRound*3))

	if _, err := enrichFetcher(t, en, &enrichSeen{}).FetchRSS(context.Background(), feedSource(url)); err != nil {
		t.Fatalf("FetchRSS 失败：%v", err)
	}
	if en.count() != enrichMaxPerRound {
		t.Errorf("单轮补全应封顶在 %d 次，实得 %d 次", enrichMaxPerRound, en.count())
	}
}

// TestEnrich_DoesNotTouchItemsWithBody：已有正文的条目不补——那是白花钱。
// 你订的 OpenAI News / Google Blog 都在 RSS 里塞了正文，它们一次都不该被补。
func TestEnrich_DoesNotTouchItemsWithBody(t *testing.T) {
	en := &fakeEnricher{text: longBody}
	url := serveFeed(t, feedWithItems(fmt.Sprintf(
		`<title>有正文</title><link>https://example.com/full</link><description>%s</description>`, longBody)))

	items, err := enrichFetcher(t, en, &enrichSeen{}).FetchRSS(context.Background(), feedSource(url))
	if err != nil {
		t.Fatalf("FetchRSS 失败：%v", err)
	}
	if en.count() != 0 {
		t.Errorf("feed 自带正文的条目不该补全（白花钱），实得 %d 次调用", en.count())
	}
	if len(items) != 1 {
		t.Fatalf("应产出 1 条，实得 %d", len(items))
	}
}

// TestEnrich_FailureDegradesGracefully：单条补全失败只保留原样，不拖垮整批、不改变既有语义。
// 补全是「只增不减」的能力——补不到的条目回到改造前的命运（被护栏丢弃），
// 绝不能因为补全失败就让整个信源被判成故障。
func TestEnrich_FailureDegradesGracefully(t *testing.T) {
	en := &fakeEnricher{err: errors.New("上游 5xx（测试构造）")}
	url := serveFeed(t, feedWithItems(
		fmt.Sprintf(`<title>链接型</title><link>https://example.com/a</link>`+
			`<description><![CDATA[<a href="x">Comments</a>]]></description>`),
		fmt.Sprintf(`<title>有正文</title><link>https://example.com/b</link><description>%s</description>`, longBody),
	))

	items, err := enrichFetcher(t, en, &enrichSeen{}).FetchRSS(context.Background(), feedSource(url))
	if err != nil {
		t.Fatalf("一条补全失败不该让整批失败：%v", err)
	}
	if len(items) != 1 {
		t.Errorf("补全失败的条目回到原命运（被护栏丢弃），自带正文的仍应产出，实得 %d 条", len(items))
	}
}

// TestEnrich_NilEnricherIsNoOp：未装配补全能力时行为与改造前逐字一致。
// 这是灰度/测试路径的保证——RSS 抓取器的单测全是纯 httptest，不该被迫拖进 Exa 客户端。
func TestEnrich_NilEnricherIsNoOp(t *testing.T) {
	f := newTestFetcher() // enricher 与 seen 都是 nil
	url := serveFeed(t, hnLikeFeed(3))

	_, err := f.FetchRSS(context.Background(), feedSource(url))
	if err == nil {
		t.Error("未装配补全时，链接型条目仍应全灭报错（与改造前一致）")
	}
}

// TestNeedsEnrichment 锁住判定口径本身。
func TestNeedsEnrichment(t *testing.T) {
	cases := []struct {
		name    string
		link    string
		content string
		want    bool
	}{
		{"链接型：正文只有裸锚点", "https://x.example/a", `<a href="y">Comments</a>`, true},
		{"正文为空", "https://x.example/a", "", true},
		{"正文过短", "https://x.example/a", "短", true},
		{"正文含裸 HTML（本来就会被护栏丢）", "https://x.example/a", longBody + "<div>x</div>", true},
		{"正文完整干净", "https://x.example/a", longBody, false},
		{"没有链接：补也无从谈起", "", "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := needsEnrichment(c.link, c.content); got != c.want {
				t.Errorf("needsEnrichment(%q, %.20q) = %v，期望 %v", c.link, c.content, got, c.want)
			}
		})
	}
}

// TestEnrich_SanitizesFetchedText：补回来的正文必须过净化，否则前功尽弃。
//
// Exa 抽出的正文里 `<` 是内容（代码片段、比较符如 "延迟 <10ms"、"<div> 标签的用法"），
// 不是未抽取的 HTML。不净化的话 §12.3 护栏会把**刚花钱补回来的正文**又丢掉——
// 付了费、条目照样收不到，是最坏的组合。
//
// 这条是探针逼出来的：原用例的夹具正文里一个 `<` 都没有，净化对它是空操作，
// 把 sanitizeContentsText 整个删掉测试照样绿。夹具不含被测特征时，用例只是看着在测。
func TestEnrich_SanitizesFetchedText(t *testing.T) {
	withAngle := longBody + " 示例代码 <div>x</div>，延迟 <10ms。"
	en := &fakeEnricher{text: withAngle}
	url := serveFeed(t, hnLikeFeed(1))

	items, err := enrichFetcher(t, en, &enrichSeen{}).FetchRSS(context.Background(), feedSource(url))
	if err != nil {
		t.Fatalf("补回来的正文含 < 时不应被护栏丢弃（那等于白花钱）：%v", err)
	}
	if len(items) != 1 {
		t.Fatalf("应产出 1 条，实得 %d —— 刚补回来的正文被 §12.3 护栏丢了", len(items))
	}
	if strings.Contains(items[0].Content, "<div") {
		t.Errorf("`<` 紧跟字母应被净化成 `< 字母`，实得：%q", items[0].Content)
	}
	if !strings.Contains(items[0].Content, "10ms") {
		t.Errorf("净化不该破坏内容本身（<10ms 里的数字），实得：%q", items[0].Content)
	}
}

// TestEnrich_AlreadySeenItemsDoNotTriggerAllDropped 复刻 2026-07-19 生产抓到的误报。
//
// 现场：Gemini 官方博客（source 8）的 description 是 251 字符的 `<img src="...">`——
// 够长（不触发"过短"）、含裸 HTML（触发补全）。但它的 canonical_key 早已在库里且正文
// 达标，SeenChecker 正确地跳过、不重复付费。于是本轮条目仍是原样的裸 HTML、被 §12.3
// 丢弃、"全灭"成立 → fail_count++。生产实测已经走到 fail_count=2：再一轮发告警卡，
// 八轮后把一个**完全健康**的源自动停用。
//
// 两个机制各自都对，踩在一起就错了：成本闸门不该重复付费，全灭防线不该把
// 「我们本来就有的内容」算进损失。修法是把跳过的条目从分母里扣掉——
// 分母的正确语义是「这一轮真正指望它产出的条目数」。
func TestEnrich_AlreadySeenItemsDoNotTriggerAllDropped(t *testing.T) {
	// 单条条目：够长、含裸 HTML（与 Gemini 现场同形），且已在库里补全过。
	body := `<title>已有的文章</title><link>https://example.com/known</link>` +
		`<description>&lt;img src="https://cdn.example/pic.png"&gt;` + longBody + `</description>`
	url := serveFeed(t, feedWithItems(body))
	src := feedSource(url)

	seen := &enrichSeen{enriched: map[string]struct{}{
		CanonicalKey(src, types.ContentItem{URL: "https://example.com/known"}): {},
	}}
	en := &fakeEnricher{text: longBody}

	items, err := enrichFetcher(t, en, seen).FetchRSS(context.Background(), src)

	if err != nil {
		t.Fatalf("全部条目都是「库里已有」时不得报全灭——那会让一个健康信源走向自动停用：%v", err)
	}
	if len(items) != 0 {
		t.Errorf("该条本轮仍被护栏丢弃（内容已在库里，不重取），应产出 0 条，实得 %d", len(items))
	}
	if en.count() != 0 {
		t.Errorf("库里已有正文的条目不得重复付费，实得 %d 次调用", en.count())
	}
}

// TestEnrich_MixedSeenAndBrokenStillAlerts：扣分母不能把真故障也一起扣没了。
// 一半已有、另一半是真的补不回来 —— 后者仍应触发全灭，否则这道防线就被掏空了。
func TestEnrich_MixedSeenAndBrokenStillAlerts(t *testing.T) {
	url := serveFeed(t, feedWithItems(
		`<title>已有</title><link>https://example.com/known</link>`+
			`<description>&lt;img src="x"&gt;`+longBody+`</description>`,
		`<title>坏的</title><link>https://example.com/broken</link>`+
			`<description>&lt;img src="y"&gt;`+longBody+`</description>`,
	))
	src := feedSource(url)

	seen := &enrichSeen{enriched: map[string]struct{}{
		CanonicalKey(src, types.ContentItem{URL: "https://example.com/known"}): {},
	}}
	// 补全对第二条也失败（目标站反爬之类）→ 它仍是裸 HTML → 被丢弃。
	en := &fakeEnricher{err: errors.New("目标站拒绝（测试构造）")}

	_, err := enrichFetcher(t, en, seen).FetchRSS(context.Background(), src)
	if err == nil {
		t.Error("仍有一条真的补不回来且被丢弃，全灭防线应触发——扣分母不该把真故障一起扣没")
	}
}
