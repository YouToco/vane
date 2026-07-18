// web/contents 信源：监控指定 URL 的内容变化（如产品定价页），改由 Exa /contents
// fetch API 抓取——不在 Go 侧自建抓取 + 基线 diff + LLM 门（page_watch 的复杂度已被
// refactor/drop-page-watch 否决，见 CHANGELOG）。
//
// 变化检测极简，不需要任何基线表：canonical_key = "contents://<url>#<textHash>"。
// 内容没变 → 同 hash → 同 canonical_key → content_items 的 UNIQUE 天然去重、不产出；
// 内容变了 → 新 hash → 新 canonical_key → 产出一条新 content_item（走正常打分/推送）。
// 首轮抓取产出一次（用户看到当前快照），此后仅变化才产出。
//
// 为什么用 Exa /contents 而不自建抓取：
//   - Exa 托管抽取是**确定性**的（同 URL 两次 maxAgeHours:0 活抓 text 逐字节相同，
//     2026-07-17 实测），无 Radix UI 随机组件 ID 之类的渲染噪音——hash 稳定，不会每轮误报；
//   - 免维护 SSRF 防护栈 + http.Client（请求源是 Exa 基础设施，不是 Vane 服务器）；
//   - Exa 把 <table> 转 markdown 表格行，价格页的"一行一模型"粒度天然保留，diff 可读。
//
// 与 web/search（exa.go）的区别：search 是关键词语义搜索（内联 contents.text 取搜索
// 结果正文）；本能力是对**固定 URL** 拿正文做变化监控。两者都打 Exa 但端点、语义不同。
package fetcher

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/YouToco/vane/config"
	"github.com/YouToco/vane/types"
)

const (
	exaContentsURL = "https://api.exa.ai/contents"
	// exaContentsMaxTextBytes 截断单页正文（成本护栏，同 exa.go 的 exaMaxTextBytes）。
	// 定价页压平文本实测 ~26KB，4000 字节覆盖核心价格区；截断按 rune 边界回退。
	exaContentsMaxTextBytes = 4000
	// exaContentsHashLen 是 canonical_key 里 textHash 的十六进制长度（sha256 前 16 hex
	// = 8 字节 = 64 bit）。碰撞概率对单源的变化序列可忽略，且键更短。
	exaContentsHashLen = 16
)

// ExaContentsFetcher 调用 Exa /contents 抓取指定 URL 的内容。零外部状态，可并发复用。
type ExaContentsFetcher struct {
	apiKey     string
	contentURL string // 可覆盖以便单测指向 httptest.Server
	client     *http.Client
	maxBytes   int64
}

// NewExaContents 按抓取配置构造。超时/响应上限兜底与 exa.go 一致（20s / 5MB）。
// apiKey 为空不在此报错——留到 Fetch 返回可诊断的 CodeValidation。
func NewExaContents(cfg config.FetchConfig) *ExaContentsFetcher {
	timeout := time.Duration(cfg.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 20 * time.Second
	}
	maxMB := cfg.MaxResponseMB
	if maxMB <= 0 {
		maxMB = 5
	}
	return &ExaContentsFetcher{
		apiKey:     cfg.ExaAPIKey,
		contentURL: exaContentsURL,
		// 禁跟随重定向：防 x-api-key 被 30x 外带（同 exa.go 的 noRedirect）。
		client:   &http.Client{Timeout: timeout, CheckRedirect: noRedirect},
		maxBytes: int64(maxMB) * 1024 * 1024,
	}
}

// exaContentsSourceConfig 是 web/contents 信源的 config JSONB 结构。url 必填。
type exaContentsSourceConfig struct {
	URL   string `json:"url"`             // 要监控的页面地址，必填
	Title string `json:"title,omitempty"` // 可选：覆盖 Exa 返回的页面标题（展示名）
}

// exaContentsRequest 是 POST /contents 请求体。maxAgeHours:0 强制活抓（不吃缓存）——
// 漏了它 Exa 默认优先返回缓存，页面变化监控会静默失效（2026-07-17 实测的头号坑）。
type exaContentsRequest struct {
	URLs        []string `json:"urls"`
	Text        bool     `json:"text"`
	MaxAgeHours int      `json:"maxAgeHours"`
}

// exaContentsResponse 只取需要的字段。statuses[] 必须检查——Exa /contents 在抓取失败时
// 返回 HTTP 200 + results 缺失/为空 + statuses[].status="error"（同 exa.go 审计 D6 的坑，
// 但 /contents 把状态显式放进 statuses，本 fetcher 据此把"抓取失败"和"内容为空"分开）。
type exaContentsResponse struct {
	Results  []exaContentsResult `json:"results"`
	Statuses []exaContentsStatus `json:"statuses"`
}

type exaContentsResult struct {
	ID    string `json:"id"`
	URL   string `json:"url"`
	Title string `json:"title"`
	Text  string `json:"text"`
}

type exaContentsStatus struct {
	Status string `json:"status"` // "success" | "error"
	Source string `json:"source"` // "cached" | "crawled"
}

// Fetch 抓一个 web/contents 源。失败语义对齐 exa.go：缺 key / 缺 url / 非法 config /
// 401/403 → CodeValidation（不可重试）；超时 → CodeFetchTimeout；429 → CodeFetchRateLimit；
// statuses[].status=="error" → CodeFetchTimeout（可重试，抓取瞬态）。
func (e *ExaContentsFetcher) Fetch(ctx context.Context, src types.Source) ([]types.ContentItem, error) {
	if e.apiKey == "" {
		return nil, types.NewAppError(types.CodeValidation,
			"web/contents 信源需要配置 VANE_FETCH_EXA_API_KEY，当前为空", nil)
	}

	var sc exaContentsSourceConfig
	if len(src.Config) > 0 {
		if err := json.Unmarshal(src.Config, &sc); err != nil {
			return nil, types.NewAppError(types.CodeValidation,
				fmt.Sprintf("解析 web/contents 信源 config 失败（source_id=%d）", src.ID), err)
		}
	}
	pageURL := strings.TrimSpace(sc.URL)
	if pageURL == "" {
		return nil, types.NewAppError(types.CodeValidation,
			fmt.Sprintf("web/contents 信源缺少 url（source_id=%d）", src.ID), nil)
	}

	reqBody := exaContentsRequest{URLs: []string{pageURL}, Text: true, MaxAgeHours: 0}
	payload, err := json.Marshal(reqBody)
	if err != nil {
		return nil, types.NewAppError(types.CodeValidation, "构造 Exa /contents 请求体失败", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.contentURL, bytes.NewReader(payload))
	if err != nil {
		return nil, types.NewAppError(types.CodeValidation, "构造 Exa /contents 请求失败", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-api-key", e.apiKey)
	req.Header.Set("Accept", "application/json")

	resp, err := e.client.Do(req)
	if err != nil {
		return nil, classifyDoError(e.contentURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTooManyRequests {
		return nil, types.NewAppError(types.CodeFetchRateLimit, "Exa /contents 被限流(429)", nil)
	}
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return nil, types.NewAppError(types.CodeValidation,
			fmt.Sprintf("Exa 鉴权失败（HTTP %d），请检查 VANE_FETCH_EXA_API_KEY", resp.StatusCode), nil)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		ae := types.NewAppError(types.CodeFetchTimeout,
			fmt.Sprintf("Exa /contents 返回非 2xx 状态 %d", resp.StatusCode), nil)
		ae.Retryable = resp.StatusCode >= 500
		return nil, ae
	}

	data, err := io.ReadAll(io.LimitReader(resp.Body, e.maxBytes+1))
	if err != nil {
		return nil, classifyDoError(e.contentURL, err)
	}
	if int64(len(data)) > e.maxBytes {
		return nil, types.NewAppError(types.CodeValidation,
			fmt.Sprintf("Exa /contents 响应体超过 %d 字节上限", e.maxBytes), nil)
	}

	var cr exaContentsResponse
	if err := json.Unmarshal(data, &cr); err != nil {
		ae := types.NewAppError(types.CodeFetchTimeout, "解析 Exa /contents 响应失败", err)
		ae.Retryable = false
		return nil, ae
	}

	// statuses[] 检查（审计 D6）：抓取失败是 HTTP 200 + status="error"，绝不能当成
	// "内容为空"静默返回 0 条——那会让监控在页面挂掉时误判"没变化"。
	for _, s := range cr.Statuses {
		if s.Status == "error" {
			ae := types.NewAppError(types.CodeFetchTimeout,
				fmt.Sprintf("Exa /contents 抓取失败（url=%s，status=error）", pageURL), nil)
			ae.Retryable = true // 抓取瞬态（目标站 5xx/challenge），可重试。
			return nil, ae
		}
		// 显式传了 maxAgeHours:0 却返回缓存（对抗审查发现 4）：Exa 无视了强制活抓，
		// 拿到的可能是旧正文——其 hash 与上次相同则被当"没变化"，会漏报一次真实变化。
		// 不阻塞（缓存也是内容，下轮活抓能补），但记 WARN 让这种漏报可见。
		if s.Source == "cached" {
			slog.Warn("fetcher: web/contents Exa 无视 maxAgeHours:0 返回缓存，可能漏报一次页面变化",
				"source_id", src.ID, "url", pageURL)
		}
	}

	item, ok := mapExaContents(src, pageURL, sc.Title, cr.Results)
	if !ok {
		// results 为空但 statuses 无 error：Exa 返回了成功却没正文，按空结果处理
		//（不报错，下一轮再抓）。
		return nil, nil
	}
	return []types.ContentItem{item}, nil
}

// mapExaContents 把 /contents 结果映射为一条 ContentItem，并自填 canonical_key
// （含 textHash，承载变化检测）。ok=false 表示无可用正文。
func mapExaContents(src types.Source, pageURL, titleOverride string, results []exaContentsResult) (types.ContentItem, bool) {
	var r exaContentsResult
	for i := range results {
		if strings.TrimSpace(results[i].Text) != "" {
			r = results[i]
			break
		}
	}
	if strings.TrimSpace(r.Text) == "" {
		return types.ContentItem{}, false
	}

	title := strings.TrimSpace(titleOverride)
	if title == "" {
		title = strings.TrimSpace(r.Title)
	}
	if title == "" {
		title = "页面内容: " + pageURL
	}

	// 监控文本 = 截断 + 净化，一份到底：canonical_key 的 hash、存储的 Content、finalize
	// 的 simhash/content_hash 全用它，杜绝"身份对全文、存储对截断"的不一致（对抗审查
	// 发现 2）。两个后果被这一步同时消除：
	//   ① 截断区（前 N 字节，监控意图所在）之外的尾部噪音（"最后更新于…"、"N 人在看"、
	//      A/B 文案）不再翻转身份——否则页脚每轮变都产出新 content_item，且 Dedup 对
	//      KindPageContent 豁免后这些噪音会被直接推给用户。
	//   ② `<` 紧跟字母（正文比较符 "x<y"、代码片段、Exa 对复杂 <table> 的降级输出）本会
	//      命中 finalize 的 htmlTagRe（`<[a-zA-Z/!]`）被静默拒收 → 监控永久失效无信号
	//      （发现 3）。sanitizeContentsText 把这种 `<X` 改成 `< X`，破坏标签形态、保留可读。
	text := sanitizeContentsText(truncateUTF8(r.Text, exaContentsMaxTextBytes))

	// canonical_key 自填（finalize 不覆盖已填值）：contents://<url>#<textHash>。
	// 用 config 里的 pageURL（稳定）而非 Exa 回显的 r.URL（可能带归一化差异），
	// 保证同一源的键前缀逐轮稳定，只有 textHash 随监控文本变。
	sum := sha256.Sum256([]byte(text))
	textHash := hex.EncodeToString(sum[:])[:exaContentsHashLen]
	canonicalKey := "contents://" + pageURL + "#" + textHash

	item := types.ContentItem{
		SourceID:     src.ID,
		ExternalID:   r.ID, // Exa 结果 id；为空时 finalize 用 content_hash 兜底。
		URL:          pageURL,
		Title:        title,
		Content:      text,
		PublishedAt:  nil, // 页面内容无发布时间，展示回退 fetched_at。
		FetchedAt:    time.Now().UTC(),
		CanonicalKey: canonicalKey,          // 自填，承载"监控文本变→新身份"。
		Kind:         types.KindPageContent, // 页面内容——Dedup 据此豁免近似去重（否则变化被 simhash 吞）。
	}
	if !finalize(src, &item) {
		return types.ContentItem{}, false
	}
	return item, true
}

// contentsHTMLLikeRe 匹配"< 紧跟字母/斜杠/感叹号"——与 fetcher.htmlTagRe 同形。
var contentsHTMLLikeRe = regexp.MustCompile(`<([a-zA-Z/!])`)

// sanitizeContentsText 把会被 finalize.htmlTagRe 误判为裸 HTML 的 `<X` 改成 `< X`：
// web/contents 抓的是 Exa 已抽取的页面正文，其中的 `<` 是内容（比较符/代码）而非"未抽取
// 的 HTML"，htmlTagRe 对它是误伤。加空格破坏标签形态、保留可读（价格数字不受影响）。
func sanitizeContentsText(s string) string {
	return contentsHTMLLikeRe.ReplaceAllString(s, "< $1")
}
