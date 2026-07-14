// 信源订阅端点（契约 B8）：加订阅 / 列订阅 / 删订阅。
// M3 固化：填 URL 加 RSS 订阅；M3+ 扩展 exa（搜索词即信源）与 tikhub_xhs
// （小红书关键词即信源）。自然语言加源是 M4。
package api

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/YouToco/vane/types"
)

// maxSourceParamRunes 是 query/keyword/title 的长度上限（字符数）。
// 无上限时超长输入会生成超长 sources.url/title（仅受 8KB 请求体约束）。
const maxSourceParamRunes = 256

// addSubscriptionReq 是 POST /api/subscriptions 的请求体。
// type 决定必填字段：rss（默认）→ url；exa → query；tikhub_xhs → keyword。
type addSubscriptionReq struct {
	Type    string `json:"type"`    // rss（默认）/ exa / tikhub_xhs
	URL     string `json:"url"`     // rss 必填
	Query   string `json:"query"`   // exa 必填：搜索词
	Keyword string `json:"keyword"` // tikhub_xhs 必填：小红书搜索关键词
	Title   string `json:"title"`   // 可选：展示名，缺省按类型生成
	// Category 是 exa 可选的结果类别（如 "news"）；其余类型忽略。
	Category string `json:"category"`
}

// handleAddSubscription 幂等 upsert 信源（按 URL/合成键）→ 建立当前 owner 的订阅关系。
// POST /api/subscriptions {type,url|query|keyword,...} → 201 {source_id}
func (s *server) handleAddSubscription(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 8<<10)
	var req addSubscriptionReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "请求体不是合法 JSON")
		return
	}

	src, errMsg := buildSource(req)
	if errMsg != "" {
		writeError(w, http.StatusBadRequest, errMsg)
		return
	}

	userID, err := s.ownerUserID(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}

	sourceID, err := s.deps.Store.UpsertSource(r.Context(), src)
	if err != nil {
		writeAppError(w, err)
		return
	}
	if err := s.deps.Store.AddSubscription(r.Context(), userID, sourceID); err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]int64{"source_id": sourceID})
}

// buildSource 按请求类型构造待 upsert 的信源；校验失败返回给用户的错误文案。
//
// 关键设计：UpsertSource 以 sources.url 为幂等键，而 exa/tikhub 源没有天然 URL，
// 这里用确定性合成键（exa://search?q=... / tikhub://xhs/search?keyword=...）占位——
// 同一搜索词重复添加会命中同一信源，不产生重复行；真实请求参数放 config JSONB，
// 由对应 fetcher 解析。
func buildSource(req addSubscriptionReq) (*types.Source, string) {
	// 归一化：去首尾空白再校验。否则全空白 query 穿透校验建出永久失败的源，
	// "AI" 与 "AI " 生成两个幂等键、产生重复信源双倍烧配额。
	req.Query = strings.TrimSpace(req.Query)
	req.Keyword = strings.TrimSpace(req.Keyword)
	req.Title = strings.TrimSpace(req.Title)
	for name, v := range map[string]string{"query": req.Query, "keyword": req.Keyword, "title": req.Title} {
		if utf8.RuneCountInString(v) > maxSourceParamRunes {
			return nil, name + " 过长（上限 256 字符）"
		}
	}

	switch types.SourceType(req.Type) {
	case types.SourceTypeRSS, "": // 缺省向后兼容为 rss
		// 只做结构校验（scheme 合法）；SSRF/私网拦截在抓取时由 fetcher 统一兜底，
		// 不在此重复一套 DNS 解析——加订阅与抓取是两个时点，抓取侧才是权威防线。
		u, err := url.Parse(req.URL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return nil, "url 必须是合法的 http/https 地址"
		}
		return &types.Source{
			Type:   types.SourceTypeRSS,
			URL:    req.URL,
			Title:  req.Title,
			Status: types.SourceStatusActive,
		}, ""

	case types.SourceTypeExa:
		if req.Query == "" {
			return nil, "exa 信源必须提供 query（搜索词）"
		}
		cfgMap := map[string]string{"query": req.Query}
		if req.Category != "" {
			cfgMap["category"] = req.Category
		}
		cfg, err := json.Marshal(cfgMap)
		if err != nil {
			return nil, "构造信源配置失败"
		}
		title := req.Title
		if title == "" {
			title = "Exa: " + req.Query
		}
		// category 参与幂等键：它改变抓取语义（news 与不限类别是两个信源），
		// 不入键会让同 query 不同 category 撞同一行、config 被静默覆盖。
		// 空 category 不追加，兼容已建的无 category 源行。
		syntheticURL := "exa://search?q=" + url.QueryEscape(req.Query)
		if req.Category != "" {
			syntheticURL += "&category=" + url.QueryEscape(req.Category)
		}
		return &types.Source{
			Type:   types.SourceTypeExa,
			URL:    syntheticURL,
			Title:  title,
			Config: cfg,
			Status: types.SourceStatusActive,
		}, ""

	case types.SourceTypeTikHubXHS:
		if req.Keyword == "" {
			return nil, "tikhub_xhs 信源必须提供 keyword（小红书搜索关键词）"
		}
		cfg, err := json.Marshal(map[string]string{"keyword": req.Keyword})
		if err != nil {
			return nil, "构造信源配置失败"
		}
		title := req.Title
		if title == "" {
			title = "小红书: " + req.Keyword
		}
		return &types.Source{
			Type:   types.SourceTypeTikHubXHS,
			URL:    "tikhub://xhs/search?keyword=" + url.QueryEscape(req.Keyword),
			Title:  title,
			Config: cfg,
			Status: types.SourceStatusActive,
		}, ""

	default:
		return nil, "type 仅支持 rss / exa / tikhub_xhs"
	}
}

// handleListSubscriptions 返回当前 owner 的全部订阅源，含 disabled/paused。
// 用 ListSubscribedSourcesByUser（不过滤 source.status）而非 ListActiveSourcesByUser：
// 否则被自动 disabled 或暂停的源在列表里直接消失，前端状态灯永远点不亮、用户
// 也无从知道某源为何不再推送。抓取扇出才用 active-only 的 ListActiveSourcesByUser。
// GET /api/subscriptions → 200 [Source...]
func (s *server) handleListSubscriptions(w http.ResponseWriter, r *http.Request) {
	userID, err := s.ownerUserID(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}
	sources, err := s.deps.Store.ListSubscribedSourcesByUser(r.Context(), userID)
	if err != nil {
		writeAppError(w, err)
		return
	}
	if sources == nil {
		sources = []types.Source{}
	}
	writeJSON(w, http.StatusOK, sources)
}

// handleRemoveSubscription 取消当前 owner 对某源的订阅（保留信源本身与内容）。
// DELETE /api/subscriptions/{source_id} → 200 {ok}
func (s *server) handleRemoveSubscription(w http.ResponseWriter, r *http.Request) {
	sourceID, err := strconv.ParseInt(r.PathValue("source_id"), 10, 64)
	if err != nil || sourceID <= 0 {
		writeError(w, http.StatusBadRequest, "source_id 必须是正整数")
		return
	}
	userID, err := s.ownerUserID(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}
	if err := s.deps.Store.RemoveSubscription(r.Context(), userID, sourceID); err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}
