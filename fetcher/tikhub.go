// TikHub 小红书信源：把一个搜索关键词当作一个信源，周期性调 TikHub 的
// search_notes 接口抓取最新笔记，映射为 types.ContentItem。与 Exa 同理，
// 目标是固定可信主机 api.tikhub.io（URL 非用户可控），不需要 RSS 的 SSRF 拦截。
//
// 契约（2026-07-14 用真实 key 实测确认）：
//   - GET {base}/api/v1/xiaohongshu/app_v2/search_notes?keyword=&page=&sort_type=
//   - 鉴权头 Authorization: Bearer <key>
//   - 响应 code=200 且 data.success=true 时，笔记在 data.data.items[].note，
//     字段含 id/title/desc/timestamp(秒)/xsec_token/user.nickname。
package fetcher

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/YouToco/vane/config"
	"github.com/YouToco/vane/types"
)

const (
	tikhubDefaultBaseURL = "https://api.tikhub.io"
	tikhubSearchPath     = "/api/v1/xiaohongshu/app_v2/search_notes"
	// tikhubDefaultSort 默认按发布时间降序：推送场景要"最新动态"，
	// 相关性排序（general）会反复返回同批高赞旧帖，全靠去重挡、白耗配额。
	tikhubDefaultSort = "time_descending"
	// tikhubMaxDescBytes 截断笔记正文的字节上限，防超长内容打爆后续打分 token
	// （成本护栏）。截断按 rune 边界回退（truncateUTF8），绝不产生非法 UTF-8。
	tikhubMaxDescBytes = 4000
)

// TikHubFetcher 调用 TikHub 小红书搜索。零外部状态，多 goroutine 可并发复用。
type TikHubFetcher struct {
	apiKey   string
	baseURL  string // 可覆盖以便单测指向 httptest.Server
	client   *http.Client
	maxBytes int64
}

// NewTikHub 按抓取配置构造 TikHubFetcher。超时/响应上限兜底与 RSS 一致（20s / 5MB）。
// apiKey 为空不在此报错——留到 Fetch 时返回明确的 CodeValidation。
func NewTikHub(cfg config.FetchConfig) *TikHubFetcher {
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
		client:   &http.Client{Timeout: timeout, CheckRedirect: noRedirect},
		maxBytes: int64(maxMB) * 1024 * 1024,
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
	q.Set("page", "1") // MVP 单页 20 条；周期抓取下靠 UNIQUE(source_id, external_id) 增量去重
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

	return mapTikhubNotes(src, env.Data.Data.Items), nil
}

// mapTikhubNotes 把笔记映射为 ContentItem，指纹由 finalize 统一补齐。
// URL 拼 xsec_token（2024 起小红书 web 端直链必带，否则 404），来源标记 pc_search。
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
		finalize(&item)
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
