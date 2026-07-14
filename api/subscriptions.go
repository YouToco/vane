// 信源订阅端点（契约 B8）：加订阅 / 列订阅 / 删订阅。
// M3 固化：填 URL 加 RSS 订阅；M3+ 扩展 exa（搜索词即信源）与 tikhub_xhs
// （小红书关键词即信源）。自然语言加源是 M4。
// M4 起校验/构造逻辑收敛到 sourcespec 包（与 agent add_source 工具共用），
// 本文件只做 HTTP 薄适配：decode → sourcespec.Spec → Build。
package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/YouToco/vane/sourcespec"
	"github.com/YouToco/vane/types"
)

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

	src, errMsg := sourcespec.Build(sourcespec.Spec{
		Type:     req.Type,
		URL:      req.URL,
		Query:    req.Query,
		Keyword:  req.Keyword,
		Title:    req.Title,
		Category: req.Category,
	})
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
