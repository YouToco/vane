package fetcher

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/YouToco/vane/config"
	"github.com/YouToco/vane/types"
)

const sampleRSS = `<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Vane Test Feed</title>
    <link>https://example.com</link>
    <description>fixture</description>
    <item>
      <title>First Post</title>
      <link>https://example.com/posts/1</link>
      <guid>guid-0001</guid>
      <description>hello world</description>
      <author>alice@example.com (Alice)</author>
      <pubDate>Tue, 14 Jul 2026 08:00:00 +0000</pubDate>
    </item>
    <item>
      <title>Second Post</title>
      <link>https://example.com/posts/2</link>
      <description>no guid here, falls back to link</description>
    </item>
  </channel>
</rss>`

// testNow 是所有 fetcher 用例的固定"现在"，取在 sampleRSS 的 pubDate（2026-07-14）
// 之后一天：既让 fixture 落在默认 lookback 窗口内，又不依赖真实时钟。
var testNow = time.Date(2026, 7, 15, 0, 0, 0, 0, time.UTC)

// newTestFetcher 构造放行环回地址的 Fetcher，以便对 httptest.Server（127.0.0.1）
// 走通正常抓取路径。生产默认规则的私网拦截由 TestFetchRSS_BlocksLoopback 覆盖。
//
// now 钉死在 testNow：否则 lookback 默认窗口会随真实时间推移把带固定 pubDate 的
// fixture 筛掉，让既有用例在未来某天毫无征兆地变红。
func newTestFetcher() *Fetcher {
	f := New(config.FetchConfig{TimeoutSeconds: 10, MaxResponseMB: 1})
	f.isBlocked = func(net.IP) bool { return false }
	f.now = func() time.Time { return testNow }
	return f
}

func TestFetchRSS_ParsesFeed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/rss+xml")
		_, _ = w.Write([]byte(sampleRSS))
	}))
	defer srv.Close()

	f := newTestFetcher()
	// Platform/Capability 是必填的，不是摆设：007 起 canonical_key 按 Platform 分派
	// （见 CanonicalKey），缺 Platform 的源算不出身份、条目会被 finalize 全部丢弃。
	// 生产上 sources.platform 为 NOT NULL 且 Multi.Fetch 会先拒掉未知组合，
	// 故这里补全才是真实形态。
	items, err := f.FetchRSS(context.Background(), types.Source{ID: 7, Platform: types.PlatformWeb, Capability: types.CapFeed, URL: srv.URL})
	if err != nil {
		t.Fatalf("FetchRSS 意外失败: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("期望 2 条，实际 %d", len(items))
	}

	got := items[0]
	if got.SourceID != 7 {
		t.Errorf("SourceID: 期望 7，实际 %d", got.SourceID)
	}
	if got.ExternalID != "guid-0001" {
		t.Errorf("ExternalID: 期望 guid-0001，实际 %q", got.ExternalID)
	}
	if got.Title != "First Post" {
		t.Errorf("Title: 期望 First Post，实际 %q", got.Title)
	}
	if got.URL != "https://example.com/posts/1" {
		t.Errorf("URL: 期望 .../posts/1，实际 %q", got.URL)
	}
	if got.Content != "hello world" {
		t.Errorf("Content 应回退到 description，实际 %q", got.Content)
	}
	if got.Author != "Alice" {
		t.Errorf("Author: 期望 Alice，实际 %q", got.Author)
	}
	if got.PublishedAt == nil {
		t.Error("PublishedAt 应被解析出来")
	}
	if got.FetchedAt.IsZero() {
		t.Error("FetchedAt 应被设置")
	}

	// 第二条无 guid，external_id 应兜底为 link。
	if items[1].ExternalID != "https://example.com/posts/2" {
		t.Errorf("无 guid 时 ExternalID 应兜底 link，实际 %q", items[1].ExternalID)
	}
	if items[1].Author != "" {
		t.Errorf("无 author 应为空串，实际 %q", items[1].Author)
	}
}

func TestFetchRSS_BlocksLoopback(t *testing.T) {
	// 用默认（生产）拦截规则：指向 127.0.0.1 的源必须被拒。
	f := New(config.FetchConfig{TimeoutSeconds: 10, MaxResponseMB: 1})
	_, err := f.FetchRSS(context.Background(), types.Source{ID: 1, URL: "http://127.0.0.1:9/feed.xml"})
	if err == nil {
		t.Fatal("私网地址应被拒绝，却返回 nil error")
	}
	if !errors.Is(err, types.ErrValidation) {
		t.Errorf("期望 ErrValidation 类错误，实际 %v", err)
	}
	if types.CodeOf(err) != types.CodeValidation {
		t.Errorf("期望 CodeValidation，实际 %s", types.CodeOf(err))
	}
}

func TestFetchRSS_BlocksZonedIPv6LiteralBeforeRequest(t *testing.T) {
	f := newTestFetcher()
	var lookups int
	f.lookupIP = func(string) ([]net.IP, error) {
		lookups++
		return nil, errors.New("lookup must not run")
	}
	for _, rawURL := range []string{
		"http://[::1%25lo0]/feed.xml",
		"http://[fe80::1%25en0]:8080/feed.xml",
	} {
		t.Run(rawURL, func(t *testing.T) {
			_, err := f.FetchRSS(t.Context(), types.Source{ID: 1, URL: rawURL})
			if !errors.Is(err, types.ErrValidation) ||
				types.CodeOf(err) != types.CodeValidation {
				t.Fatalf("zoned IPv6 literal must fail before request: err=%v", err)
			}
		})
	}
	if lookups != 0 {
		t.Fatalf("zoned literal must be rejected before DNS or request: lookups=%d", lookups)
	}
}

func TestFetchRSS_RedirectCannotBypassDialerWithZonedIPv6(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://[fe80::1%25en0]/feed.xml", http.StatusFound)
	}))
	defer srv.Close()

	// Allow the initial httptest loopback target. The redirect does not re-enter
	// fetchBody's preflight, so only Dialer.Control can stop the zoned target.
	f := newTestFetcher()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	_, err := f.FetchRSS(ctx, types.Source{ID: 1, URL: srv.URL})
	if !errors.Is(err, errBlockedDial) ||
		!errors.Is(err, types.ErrValidation) ||
		types.CodeOf(err) != types.CodeValidation {
		t.Fatalf("redirected zoned IPv6 must be blocked by Dialer.Control: err=%v", err)
	}
}

func TestFetchRSS_RejectsBadScheme(t *testing.T) {
	f := newTestFetcher()
	_, err := f.FetchRSS(context.Background(), types.Source{ID: 1, URL: "ftp://example.com/feed"})
	if !errors.Is(err, types.ErrValidation) {
		t.Errorf("非 http(s) scheme 应判 ErrValidation，实际 %v", err)
	}
}

func TestFetchRSS_RejectsOversize(t *testing.T) {
	// maxBytes = 1MB；返回 2MB 响应应触发超限。
	big := make([]byte, 2*1024*1024)
	for i := range big {
		big[i] = 'x'
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(big)
	}))
	defer srv.Close()

	f := newTestFetcher()
	_, err := f.FetchRSS(context.Background(), types.Source{ID: 1, URL: srv.URL})
	if err == nil {
		t.Fatal("超大响应应被拒绝")
	}
	if types.CodeOf(err) != types.CodeValidation {
		t.Errorf("超限期望 CodeValidation，实际 %s", types.CodeOf(err))
	}
}

func TestFetchRSS_RateLimited(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	f := newTestFetcher()
	_, err := f.FetchRSS(context.Background(), types.Source{ID: 1, URL: srv.URL})
	if types.CodeOf(err) != types.CodeFetchRateLimit {
		t.Errorf("429 期望 CodeFetchRateLimit，实际 %s", types.CodeOf(err))
	}
	if !types.IsRetryable(err) {
		t.Error("限流应可重试")
	}
}

func TestFetchRSS_Non2xxRetryability(t *testing.T) {
	cases := []struct {
		status    int
		retryable bool
	}{
		{http.StatusInternalServerError, true}, // 5xx 瞬态可重试
		{http.StatusNotFound, false},           // 4xx 确定性不可重试
	}
	for _, tc := range cases {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(tc.status)
		}))
		f := newTestFetcher()
		_, err := f.FetchRSS(context.Background(), types.Source{ID: 1, URL: srv.URL})
		if err == nil {
			t.Errorf("status %d 应返回错误", tc.status)
		}
		if got := types.IsRetryable(err); got != tc.retryable {
			t.Errorf("status %d: Retryable 期望 %v，实际 %v", tc.status, tc.retryable, got)
		}
		srv.Close()
	}
}

func TestFetchRSS_ParseError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("this is not a feed at all <<<"))
	}))
	defer srv.Close()

	f := newTestFetcher()
	_, err := f.FetchRSS(context.Background(), types.Source{ID: 1, URL: srv.URL})
	if err == nil {
		t.Fatal("非法内容应解析失败")
	}
	if !errors.Is(err, types.ErrFetch) {
		t.Errorf("解析失败应归入 fetch 类，实际 %v", err)
	}
	if types.IsRetryable(err) {
		t.Error("解析失败是确定性错误，不应可重试")
	}
}

func TestDefaultBlockedIP(t *testing.T) {
	blocked := []string{
		"127.0.0.1", "10.0.0.1", "192.168.1.1", "172.16.0.1", "169.254.1.1", "::1", "fc00::1", "0.0.0.0",
		"100.64.0.1",  // CGNAT / Tailscale——审查补的核心 SSRF 缺口
		"100.127.0.1", // 100.64.0.0/10 上界侧
		"198.18.0.5",  // 基准测试段
		"203.0.113.5", // TEST-NET-3 文档段（非真实公网，拦截合理）
	}
	for _, s := range blocked {
		if !defaultBlockedIP(net.ParseIP(s)) {
			t.Errorf("%s 应被拦截", s)
		}
	}
	allowed := []string{"8.8.8.8", "1.1.1.1"} // 真实公网 DNS，不应被拦
	for _, s := range allowed {
		if defaultBlockedIP(net.ParseIP(s)) {
			t.Errorf("%s 不应被拦截", s)
		}
	}
}

// lookbackRSS 造一份"新旧混排"的 feed，模拟 openai.com/news/rss.xml 的真实形态：
// 按时间倒序、新条目在前、历史一路排到 2023 年。
const lookbackRSS = `<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Lookback Feed</title>
    <link>https://example.com</link>
    <description>fixture</description>
    <item>
      <title>Fresh Post</title>
      <link>https://example.com/posts/fresh</link>
      <pubDate>Tue, 14 Jul 2026 08:00:00 +0000</pubDate>
    </item>
    <item>
      <title>Undated Post</title>
      <link>https://example.com/posts/undated</link>
    </item>
    <item>
      <title>Stale Post</title>
      <link>https://example.com/posts/stale</link>
      <pubDate>Mon, 03 Jul 2026 08:00:00 +0000</pubDate>
    </item>
    <item>
      <title>Ancient Post</title>
      <link>https://example.com/posts/ancient</link>
      <pubDate>Mon, 06 Mar 2023 08:00:00 +0000</pubDate>
    </item>
  </channel>
</rss>`

func lookbackServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/rss+xml")
		_, _ = w.Write([]byte(lookbackRSS))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func titlesOf(items []types.ContentItem) []string {
	out := make([]string, 0, len(items))
	for _, it := range items {
		out = append(out, it.Title)
	}
	return out
}

// TestFetchRSS_LookbackFiltersStaleItems 锁住本次修复的核心行为：默认窗口只放行
// 近 7 天的条目，2023 年的历史条目不得入库——它们正是会占满候选窗口的那批。
func TestFetchRSS_LookbackFiltersStaleItems(t *testing.T) {
	srv := lookbackServer(t)
	f := newTestFetcher()

	items, err := f.FetchRSS(context.Background(),
		types.Source{ID: 7, Platform: types.PlatformWeb, Capability: types.CapFeed, URL: srv.URL})
	if err != nil {
		t.Fatalf("FetchRSS 意外失败: %v", err)
	}

	got := titlesOf(items)
	// Fresh 在窗口内；Undated 无日期按约定保留；Stale(11 天前) 与 Ancient(2023) 应被滤掉。
	want := []string{"Fresh Post", "Undated Post"}
	if len(got) != len(want) {
		t.Fatalf("期望 %v，实际 %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("期望 %v，实际 %v", want, got)
		}
	}
}

// TestFetchRSS_LookbackNegativeMeansUnlimited 覆盖 <0 = 全量的逃生舱：
// 语义必须与 exaSourceConfig.LookbackDays 一致。
func TestFetchRSS_LookbackNegativeMeansUnlimited(t *testing.T) {
	srv := lookbackServer(t)
	f := newTestFetcher()

	items, err := f.FetchRSS(context.Background(), types.Source{
		ID: 7, Platform: types.PlatformWeb, Capability: types.CapFeed, URL: srv.URL,
		Config: json.RawMessage(`{"lookback_days":-1}`),
	})
	if err != nil {
		t.Fatalf("FetchRSS 意外失败: %v", err)
	}
	if len(items) != 4 {
		t.Fatalf("lookback_days=-1 应放行全部 4 条，实际 %d：%v", len(items), titlesOf(items))
	}
}

// TestFetchRSS_LookbackCustomWindow 验证自定义窗口真的按天算：30 天能捞回 11 天前的
// Stale，但仍挡住 2023 年的 Ancient。这条用例能杀死"把 lookback 当小时/分钟"的突变。
func TestFetchRSS_LookbackCustomWindow(t *testing.T) {
	srv := lookbackServer(t)
	f := newTestFetcher()

	items, err := f.FetchRSS(context.Background(), types.Source{
		ID: 7, Platform: types.PlatformWeb, Capability: types.CapFeed, URL: srv.URL,
		Config: json.RawMessage(`{"lookback_days":30}`),
	})
	if err != nil {
		t.Fatalf("FetchRSS 意外失败: %v", err)
	}
	got := titlesOf(items)
	want := []string{"Fresh Post", "Undated Post", "Stale Post"}
	if len(got) != len(want) {
		t.Fatalf("期望 %v，实际 %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("期望 %v，实际 %v", want, got)
		}
	}
}

// TestFetchRSS_LookbackEmptyConfigUsesDefault 锁住现存生产源的形态：config 为 {}
// （BBC 等 M3 期建的源就是这样）必须走默认 7 天，而不是被当成 0 天全滤光。
func TestFetchRSS_LookbackEmptyConfigUsesDefault(t *testing.T) {
	srv := lookbackServer(t)
	f := newTestFetcher()

	items, err := f.FetchRSS(context.Background(), types.Source{
		ID: 7, Platform: types.PlatformWeb, Capability: types.CapFeed, URL: srv.URL,
		Config: json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatalf("FetchRSS 意外失败: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("空 config 应回落默认 7 天窗口（2 条），实际 %d：%v", len(items), titlesOf(items))
	}
}

// TestFetchRSS_InvalidConfigRejected 非法 config 必须是不可重试的 CodeValidation：
// 重试同一份坏配置永远不会好，只会白烧 Activity 预算。
func TestFetchRSS_InvalidConfigRejected(t *testing.T) {
	srv := lookbackServer(t)
	f := newTestFetcher()

	_, err := f.FetchRSS(context.Background(), types.Source{
		ID: 7, Platform: types.PlatformWeb, Capability: types.CapFeed, URL: srv.URL,
		Config: json.RawMessage(`{"lookback_days":"seven"}`),
	})
	if err == nil {
		t.Fatal("非法 config 应当报错")
	}
	if !errors.Is(err, types.ErrValidation) {
		t.Errorf("非法 config 应归入 validation 类，实际 %v", err)
	}
	if types.IsRetryable(err) {
		t.Error("非法 config 是确定性错误，不应可重试")
	}
}

func categoriesServer(t *testing.T) *httptest.Server {
	t.Helper()
	rss := `<?xml version="1.0"?>
<rss version="2.0">
<channel><title>Test</title>
<item><title>Product Launch</title><link>https://example.com/product</link>
  <category>Product</category><pubDate>` + time.Now().UTC().Format(time.RFC1123Z) + `</pubDate></item>
<item><title>Research Paper</title><link>https://example.com/research</link>
  <category>Research</category><pubDate>` + time.Now().UTC().Format(time.RFC1123Z) + `</pubDate></item>
<item><title>Company Update</title><link>https://example.com/company</link>
  <category>Company</category><pubDate>` + time.Now().UTC().Format(time.RFC1123Z) + `</pubDate></item>
<item><title>Multi Cat</title><link>https://example.com/multi</link>
  <category>Research</category><category>Product</category><pubDate>` + time.Now().UTC().Format(time.RFC1123Z) + `</pubDate></item>
<item><title>No Category</title><link>https://example.com/nocat</link>
  <pubDate>` + time.Now().UTC().Format(time.RFC1123Z) + `</pubDate></item>
</channel></rss>`
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.Write([]byte(rss))
	}))
}

func TestFetchRSS_CategoriesFilter(t *testing.T) {
	srv := categoriesServer(t)
	defer srv.Close()
	f := newTestFetcher()

	items, err := f.FetchRSS(context.Background(), types.Source{
		ID: 7, Platform: types.PlatformWeb, Capability: types.CapFeed, URL: srv.URL,
		Config: json.RawMessage(`{"categories":["Product"]}`),
	})
	if err != nil {
		t.Fatalf("FetchRSS 出错: %v", err)
	}
	got := titlesOf(items)
	if len(got) != 2 {
		t.Fatalf("categories=[Product] 应匹配 2 条（Product Launch + Multi Cat），实际 %d: %v", len(got), got)
	}
}

func TestFetchRSS_CategoriesCaseInsensitive(t *testing.T) {
	srv := categoriesServer(t)
	defer srv.Close()
	f := newTestFetcher()

	items, err := f.FetchRSS(context.Background(), types.Source{
		ID: 7, Platform: types.PlatformWeb, Capability: types.CapFeed, URL: srv.URL,
		Config: json.RawMessage(`{"categories":["research"]}`),
	})
	if err != nil {
		t.Fatalf("FetchRSS 出错: %v", err)
	}
	got := titlesOf(items)
	if len(got) != 2 {
		t.Fatalf("categories=[research] 大小写不敏感应匹配 2 条，实际 %d: %v", len(got), got)
	}
}

func TestFetchRSS_CategoriesEmpty_NoFilter(t *testing.T) {
	srv := categoriesServer(t)
	defer srv.Close()
	f := newTestFetcher()

	items, err := f.FetchRSS(context.Background(), types.Source{
		ID: 7, Platform: types.PlatformWeb, Capability: types.CapFeed, URL: srv.URL,
		Config: json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatalf("FetchRSS 出错: %v", err)
	}
	if len(items) != 5 {
		t.Fatalf("无 categories 应返回全部 5 条，实际 %d", len(items))
	}
}

// TestFetchRSS_FuturePubDateClampedToNow：未来 pubDate（时区错配/站点乱填）必须被
// 钳到抓取时刻（bug 狩猎 2026-07-19 MEDIUM）——selector 只把负龄钳为 0，零衰减
// 本身就是持续的排名优势（同分下最多 +12 分），钳到 now 保留条目、只消除优势。
func TestFetchRSS_FuturePubDateClampedToNow(t *testing.T) {
	feed := `<?xml version="1.0"?><rss version="2.0"><channel><title>f</title>
<item><title>Future Post</title><link>https://x.example/future</link><description>far future body long enough to keep</description><pubDate>Mon, 01 Jan 2091 00:00:00 +0000</pubDate></item>
<item><title>Normal Post</title><link>https://x.example/normal</link><description>normal body long enough to keep here</description><pubDate>Tue, 14 Jul 2026 12:00:00 +0000</pubDate></item>
</channel></rss>`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/rss+xml")
		_, _ = w.Write([]byte(feed))
	}))
	defer srv.Close()

	f := newTestFetcher()
	items, err := f.FetchRSS(context.Background(),
		types.Source{ID: 7, Platform: types.PlatformWeb, Capability: types.CapFeed, URL: srv.URL})
	if err != nil {
		t.Fatalf("FetchRSS 意外失败: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("未来条目应保留（钳制不是丢弃），期望 2 条实得 %d：%v", len(items), titlesOf(items))
	}
	var future *types.ContentItem
	for i := range items {
		if items[i].Title == "Future Post" {
			future = &items[i]
		}
	}
	if future == nil {
		t.Fatal("未来条目不见了")
	}
	if future.PublishedAt == nil {
		t.Fatal("钳制应保留 PublishedAt（= 抓取时刻），实为 nil")
	}
	if !future.PublishedAt.Equal(testNow) {
		t.Errorf("未来 pubDate 应钳到 f.now()（%v），实为 %v", testNow, future.PublishedAt)
	}
	// 钳制不许反向殃及正常条目。
	for i := range items {
		if items[i].Title == "Normal Post" && !items[i].PublishedAt.Equal(time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)) {
			t.Errorf("正常条目的 PublishedAt 不得被改动，实为 %v", items[i].PublishedAt)
		}
	}
}
