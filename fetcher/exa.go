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
	rec       BindingCallRecorder // nil 合法（不记账）；复用 binding 同一接口，避免开新抽象
}

// NewExa 按抓取配置构造 ExaFetcher。TimeoutSeconds / MaxResponseMB 非正数时
// 回退到与 RSS 一致的兜底（20s / 5MB）。apiKey 为空不在此报错——留到 Fetch
// 时返回明确的 CodeValidation，便于"未配置 Exa 却订了 Exa 源"给出可诊断信息。
// rec 为 nil 时不记账（与 BindingFetcher 同纪律）。
func NewExa(cfg config.FetchConfig, rec BindingCallRecorder) *ExaFetcher {
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
		rec:      rec,
	}
}

// exaSourceConfig 是 Exa 信源的 config JSONB 结构。query 必填，其余可选。
type exaSourceConfig struct {
	Query          string   `json:"query"`                     // 搜索词，必填
	Category       string   `json:"category,omitempty"`        // 结果类别，如 "news"（留空则不限）
	Type           string   `json:"type,omitempty"`            // 搜索模式，默认 "auto"
	NumResults     int      `json:"num_results,omitempty"`     // 取回条数，默认 10、上限 100
	LookbackDays   int      `json:"lookback_days,omitempty"`   // >0 时只取最近 N 天；0 或 <0 不过滤
	IncludeDomains []string `json:"include_domains,omitempty"` // 限定结果域名白名单
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

// exaCostDollars 是 Exa 响应里的 costDollars 结构（只取 total）。
type exaCostDollars struct {
	Total float64 `json:"total"`
}

// exaResponse 是 /search 响应体，只取需要的字段。
type exaResponse struct {
	Results     []exaResult    `json:"results"`
	CostDollars exaCostDollars `json:"costDollars"`
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

	start := time.Now()
	resp, err := e.client.Do(req)
	if err != nil {
		return nil, classifyDoError(e.searchURL, err)
	}
	defer resp.Body.Close()
	elapsed := time.Since(start)

	// 错误路径也记账（bug 狩猎 2026-07-19 MEDIUM，两路独立发现）：此前 recordCall
	// 只在 JSON 解析成功后调用，429 限流一整天 tool_calls 里零行 Exa 记录——故障在
	// 账本上隐形，只能翻应用日志。与 TikHub（binding.go 成败都记）对齐：拿到 HTTP
	// 响应即记，cost 只在成功路径填。fail 闭包统一"记账后返回该错误"。
	fail := func(status, bodySize int, ae error) error {
		e.recordCall(ctx, src, status, elapsed, bodySize, 0, ae)
		return ae
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		return nil, fail(resp.StatusCode, 0,
			types.NewAppError(types.CodeFetchRateLimit, "Exa 搜索被限流(429)", nil))
	}
	// 401/403 归 CodeValidation（与 TikHub 对齐）：key 配错是本方配置问题，
	// 需要可诊断的错误信息而非笼统的"非 2xx"。
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return nil, fail(resp.StatusCode, 0, types.NewAppError(types.CodeValidation,
			fmt.Sprintf("Exa 鉴权失败（HTTP %d），请检查 VANE_FETCH_EXA_API_KEY", resp.StatusCode), nil))
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		ae := types.NewAppError(types.CodeFetchTimeout,
			fmt.Sprintf("Exa 搜索返回非 2xx 状态 %d", resp.StatusCode), nil)
		ae.Retryable = resp.StatusCode >= 500 // 5xx 瞬态可重试，4xx 确定性不可重试。
		return nil, fail(resp.StatusCode, 0, ae)
	}

	data, err := io.ReadAll(io.LimitReader(resp.Body, e.maxBytes+1))
	if err != nil {
		return nil, fail(resp.StatusCode, 0, classifyDoError(e.searchURL, err))
	}
	if int64(len(data)) > e.maxBytes {
		return nil, fail(resp.StatusCode, len(data), types.NewAppError(types.CodeValidation,
			fmt.Sprintf("Exa 响应体超过 %d 字节上限", e.maxBytes), nil))
	}

	var er exaResponse
	if err := json.Unmarshal(data, &er); err != nil {
		ae := types.NewAppError(types.CodeFetchTimeout, "解析 Exa 响应失败", err)
		ae.Retryable = false // 解析失败是确定性错误，重试无益。
		return nil, fail(resp.StatusCode, len(data), ae)
	}

	e.recordCall(ctx, src, resp.StatusCode, elapsed, len(data), er.CostDollars.Total, nil)

	// 全灭防线：Exa 的 lookback 是服务端过滤（请求里的 startPublishedDate），
	// 客户端**没有**正常过滤，所以「收到结果却一条都没能入库」直接就是不兼容/漂移的确证。
	mapped, tally := mapExaResults(src, er.Results)
	if len(er.Results) > 0 && len(mapped) == 0 {
		return nil, allDroppedErr(src, len(er.Results), tally)
	}
	return mapped, nil
}

// mapExaResults 把 Exa 结果映射为 ContentItem，正文过长时截断，指纹由 finalize 统一补齐。
// 第二个返回值是本轮各原因的丢弃计数，供调用方判定「全灭」（见 drop.go）。
func mapExaResults(src types.Source, results []exaResult) ([]types.ContentItem, dropTally) {
	now := time.Now().UTC()
	var tally dropTally
	out := make([]types.ContentItem, 0, len(results))
	for _, r := range results {
		if r.URL == "" && r.Title == "" {
			tally.add(dropEmptyResult) // 无 URL 又无标题的空结果跳过。
			continue
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
		if r := finalize(src, &item); r != dropNone {
			tally.add(r)
			continue
		}
		out = append(out, item)
	}
	return out, tally
}

// recordCall 写一行 tool_calls（与 binding.record 同纪律：旁路，失败不放大）。
func (e *ExaFetcher) recordCall(ctx context.Context, src types.Source, status int, elapsed time.Duration, bodySize int, costTotal float64, callErr error) {
	if e.rec == nil {
		return
	}
	ctx = context.WithoutCancel(ctx)
	trace, _ := ctx.Value(bindingTraceKey).(string)
	srcID := src.ID
	rec := &types.ToolCall{
		TraceID:      trace,
		ToolName:     "exa:search",
		ToolKind:     types.ToolCallKindExaFetch,
		EndpointPath: "/search",
		DurationMs:   int(elapsed.Milliseconds()),
		ResultSize:   bodySize,
		HTTPStatus:   &status,
		SourceID:     &srcID,
	}
	if costTotal > 0 {
		rec.CostUSD = &costTotal
	}
	if callErr != nil {
		rec.ErrorType = types.ToolErrInternal
		rec.Error = truncateUTF8(callErr.Error(), 500)
	} else if status < 200 || status >= 300 {
		rec.ErrorType = types.ToolErrHTTP
	}
	e.rec.RecordBindingCall(ctx, rec)
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
