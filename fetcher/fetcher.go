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
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"syscall"
	"time"

	"github.com/mmcdole/gofeed"

	"github.com/YouToco/vane/config"
	"github.com/YouToco/vane/dedup"
	"github.com/YouToco/vane/types"
)

// errBlockedDial 是连接阶段命中私网地址时 Dialer.Control 返回的哨兵。
// 之所以单独定义：Control 触发时机在 http.Client 内部，只能通过 error 冒泡，
// FetchRSS 再据此判定为 CodeValidation（与抓取前 LookupIP 预检的语义一致）。
var errBlockedDial = errors.New("fetcher: 目标解析到私网/环回地址，已拒绝连接")

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
// 失败语义：私网/非法 URL → CodeValidation（不可重试）；超时 → CodeFetchTimeout；
// HTTP 429 → CodeFetchRateLimit；其余（非 2xx、超限、解析失败）归入 fetch 类
// 但按确定性/瞬态区分 Retryable。解析失败只返回 error，绝不 panic。
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

	return mapItems(src, feed.Items), nil
}

// mapItems 把 gofeed 条目映射为 ContentItem，并在入库前算好 content_hash（精确指纹）
// 与 simhash（近似指纹）——原设计把这步留给 dedup 环节，但 dedup 只在内存回填、
// 从不写回 content_items，导致两列恒为空、跨批去重全失效（审查 CRITICAL 数据链）。
// 在此落库前算好，才能让 ListRecentSimhashes 拿到历史、UNIQUE 精确去重生效。
func mapItems(src types.Source, items []*gofeed.Item) []types.ContentItem {
	now := time.Now().UTC()
	out := make([]types.ContentItem, 0, len(items))
	for _, it := range items {
		if it == nil {
			continue
		}
		// external_id 用 GUID 兜底 Link：部分源不给 guid，link 作为源内唯一键。
		externalID := it.GUID
		if externalID == "" {
			externalID = it.Link
		}
		// content 优先取全文 Content，缺失则退回摘要 Description。
		content := it.Content
		if content == "" {
			content = it.Description
		}

		item := types.ContentItem{
			SourceID:    src.ID,
			ExternalID:  externalID,
			URL:         it.Link,
			Title:       it.Title,
			Content:     content,
			Author:      authorName(it),
			PublishedAt: it.PublishedParsed,
			FetchedAt:   now,
		}
		// 精确指纹落库（原为空串死代码）：让 content_hash 列真正可用。
		item.ContentHash = dedup.ContentHash(item)
		// 近似指纹落库（原为 nil）：72h 跨批近似去重依赖它。
		sh := dedup.Simhash(item.Title + " " + item.Content)
		item.Simhash = &sh
		// 源既无 guid 又无 link 时用 content_hash 派生稳定键，
		// 避免空串在 UNIQUE(source_id, external_id) 上静默冲突吞条目（001 的设计约束）。
		if item.ExternalID == "" {
			item.ExternalID = item.ContentHash
		}
		out = append(out, item)
	}
	return out
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
