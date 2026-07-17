// Exa 语义搜索信源：把一条 query 当作一个信源，周期性抓取 Exa 返回的最新结果，
// 映射为 types.ContentItem。与 RSS 不同，Exa 目标是固定可信主机 api.exa.ai，
// 故不需要 RSS 那套私网/SSRF 拦截（URL 非用户可控）。
//
// 契约：请求 POST https://api.exa.ai/search，鉴权头 x-api-key；请求体带
// query/type/category/numResults/includeDomains 与 contents.text=true；
// 响应 results[] 每项含 id/title/url/publishedDate/author/text。
package fetcher

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/YouToco/vane/config"
	"github.com/YouToco/vane/types"
)

const (
	exaSearchURL = "https://api.exa.ai/search"
	// exaDefaultNumResults 是单次搜索默认取回条数（成本护栏：Exa 按搜索+取正文计费）。
	exaDefaultNumResults = 10
	// exaMaxNumResults 是 Exa /search 的硬上限（numResults ≤ 100）。
	exaMaxNumResults = 100
	// exaMaxTextBytes 截断单条正文的字节上限，防超长文档打爆后续打分 token
	// （成本护栏）。截断按 rune 边界回退（truncateUTF8），绝不产生非法 UTF-8。
	exaMaxTextBytes = 4000
)

// ExaFetcher 调用 Exa /search。零外部状态，多 goroutine 可并发复用。
type ExaFetcher struct {
	apiKey    string
	searchURL string // 可覆盖以便单测指向 httptest.Server
	client    *http.Client
	maxBytes  int64
}

// NewExa 按抓取配置构造 ExaFetcher。TimeoutSeconds / MaxResponseMB 非正数时
// 回退到与 RSS 一致的兜底（20s / 5MB）。apiKey 为空不在此报错——留到 Fetch
// 时返回明确的 CodeValidation，便于"未配置 Exa 却订了 Exa 源"给出可诊断信息。
func NewExa(cfg config.FetchConfig) *ExaFetcher {
	timeout := time.Duration(cfg.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 20 * time.Second
	}
	maxMB := cfg.MaxResponseMB
	if maxMB <= 0 {
		maxMB = 5
	}
	return &ExaFetcher{
		apiKey:    cfg.ExaAPIKey,
		searchURL: exaSearchURL,
		// 禁跟随重定向：防 x-api-key 被 30x 外带到第三方域（见 noRedirect）。
		client:   &http.Client{Timeout: timeout, CheckRedirect: noRedirect},
		maxBytes: int64(maxMB) * 1024 * 1024,
	}
}

// exaSourceConfig 是 Exa 信源的 config JSONB 结构。query 必填，其余可选。
type exaSourceConfig struct {
	Query          string   `json:"query"`                      // 搜索词，必填
	Category       string   `json:"category,omitempty"`         // 结果类别，如 "news"（留空则不限）
	Type           string   `json:"type,omitempty"`             // 搜索模式，默认 "auto"
	NumResults     int      `json:"num_results,omitempty"`      // 取回条数，默认 10、上限 100
	LookbackDays   int      `json:"lookback_days,omitempty"`    // >0 时只取最近 N 天；0 或 <0 不过滤
	IncludeDomains []string `json:"include_domains,omitempty"`  // 限定结果域名白名单
}

// exaRequest 是 POST /search 请求体（字段名对齐 Exa API）。
type exaRequest struct {
	Query              string         `json:"query"`
	Type               string         `json:"type,omitempty"`
	Category           string         `json:"category,omitempty"`
	NumResults         int            `json:"numResults,omitempty"`
	StartPublishedDate string         `json:"startPublishedDate,omitempty"`
	IncludeDomains     []string       `json:"includeDomains,omitempty"`
	Contents           exaContentsReq `json:"contents"`
}

type exaContentsReq struct {
	Text bool `json:"text"`
}

// exaResponse 是 /search 响应体，只取需要的字段。
type exaResponse struct {
	Results []exaResult `json:"results"`
}

type exaResult struct {
	ID            string `json:"id"`
	Title         string `json:"title"`
	URL           string `json:"url"`
	PublishedDate string `json:"publishedDate"`
	Author        string `json:"author"`
	Text          string `json:"text"`
}

// Fetch 按信源 config 里的 query 调 Exa /search，返回映射后的内容条目。
// 失败语义：缺 key / 缺 query / 非法 config / 401/403 鉴权失败 → CodeValidation
// （不可重试）；超时 → CodeFetchTimeout；429 → CodeFetchRateLimit；
// 其余非 2xx 按 5xx/4xx 定可否重试。
func (e *ExaFetcher) Fetch(ctx context.Context, src types.Source) ([]types.ContentItem, error) {
	if e.apiKey == "" {
		return nil, types.NewAppError(types.CodeValidation,
			"Exa 信源需要配置 VANE_FETCH_EXA_API_KEY，当前为空", nil)
	}

	var sc exaSourceConfig
	if len(src.Config) > 0 {
		if err := json.Unmarshal(src.Config, &sc); err != nil {
			return nil, types.NewAppError(types.CodeValidation,
				fmt.Sprintf("解析 Exa 信源 config 失败（source_id=%d）", src.ID), err)
		}
	}
	if sc.Query == "" {
		return nil, types.NewAppError(types.CodeValidation,
			fmt.Sprintf("Exa 信源缺少 query（source_id=%d）", src.ID), nil)
	}

	num := sc.NumResults
	if num <= 0 {
		num = exaDefaultNumResults
	}
	if num > exaMaxNumResults {
		num = exaMaxNumResults
	}
	typ := sc.Type
	if typ == "" {
		typ = "auto"
	}

	reqBody := exaRequest{
		Query:          sc.Query,
		Type:           typ,
		Category:       sc.Category,
		NumResults:     num,
		IncludeDomains: sc.IncludeDomains,
		Contents:       exaContentsReq{Text: true},
	}
	if sc.LookbackDays > 0 {
		start := time.Now().UTC().Add(-time.Duration(sc.LookbackDays) * 24 * time.Hour)
		reqBody.StartPublishedDate = start.Format(time.RFC3339)
	}

	payload, err := json.Marshal(reqBody)
	if err != nil {
		return nil, types.NewAppError(types.CodeValidation, "构造 Exa 请求体失败", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.searchURL, bytes.NewReader(payload))
	if err != nil {
		return nil, types.NewAppError(types.CodeValidation, "构造 Exa 请求失败", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-api-key", e.apiKey)
	req.Header.Set("Accept", "application/json")

	resp, err := e.client.Do(req)
	if err != nil {
		return nil, classifyDoError(e.searchURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTooManyRequests {
		return nil, types.NewAppError(types.CodeFetchRateLimit, "Exa 搜索被限流(429)", nil)
	}
	// 401/403 归 CodeValidation（与 TikHub 对齐）：key 配错是本方配置问题，
	// 需要可诊断的错误信息而非笼统的"非 2xx"。
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return nil, types.NewAppError(types.CodeValidation,
			fmt.Sprintf("Exa 鉴权失败（HTTP %d），请检查 VANE_FETCH_EXA_API_KEY", resp.StatusCode), nil)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		ae := types.NewAppError(types.CodeFetchTimeout,
			fmt.Sprintf("Exa 搜索返回非 2xx 状态 %d", resp.StatusCode), nil)
		ae.Retryable = resp.StatusCode >= 500 // 5xx 瞬态可重试，4xx 确定性不可重试。
		return nil, ae
	}

	data, err := io.ReadAll(io.LimitReader(resp.Body, e.maxBytes+1))
	if err != nil {
		return nil, classifyDoError(e.searchURL, err)
	}
	if int64(len(data)) > e.maxBytes {
		return nil, types.NewAppError(types.CodeValidation,
			fmt.Sprintf("Exa 响应体超过 %d 字节上限", e.maxBytes), nil)
	}

	var er exaResponse
	if err := json.Unmarshal(data, &er); err != nil {
		ae := types.NewAppError(types.CodeFetchTimeout, "解析 Exa 响应失败", err)
		ae.Retryable = false // 解析失败是确定性错误，重试无益。
		return nil, ae
	}

	return mapExaResults(src, er.Results), nil
}

// mapExaResults 把 Exa 结果映射为 ContentItem，正文过长时截断，指纹由 finalize 统一补齐。
func mapExaResults(src types.Source, results []exaResult) []types.ContentItem {
	now := time.Now().UTC()
	out := make([]types.ContentItem, 0, len(results))
	for _, r := range results {
		if r.URL == "" && r.Title == "" {
			continue // 无 URL 又无标题的空结果跳过。
		}
		content := truncateUTF8(r.Text, exaMaxTextBytes)
		item := types.ContentItem{
			SourceID:    src.ID,
			ExternalID:  r.ID, // Exa 结果 id；为空则 finalize 用 content_hash 兜底。
			URL:         r.URL,
			Title:       r.Title,
			Content:     content,
			Author:      r.Author,
			PublishedAt: parseExaDate(r.PublishedDate),
			FetchedAt:   now,
			Kind:        types.KindArticle, // 搜索结果是"一篇内容"（M6 契约 §7.2(b)：构造处赋值，finalize 只校验）
		}
		// finalize 据 src.Platform 定身份：exa 与 rss 同属 web 平台、url 派——这正是
		// "Exa 搜到用户 RSS 源里的同一篇文章"能被识别成一份的原因。
		// 上面只挡了"url 与 title 双空"，有 title 无 url 的结果在此被丢弃（无身份）。
		if !finalize(src, &item) {
			continue
		}
		out = append(out, item)
	}
	return out
}

// parseExaDate 解析 Exa 的 ISO 8601 发布时间；解析失败或为空返回 nil（列可空）。
func parseExaDate(s string) *time.Time {
	if s == "" {
		return nil
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return &t
	}
	// Exa 有时带毫秒（RFC3339Nano 可覆盖），再退一步用日期。
	if t, err := time.Parse("2006-01-02", s); err == nil {
		return &t
	}
	return nil
}
