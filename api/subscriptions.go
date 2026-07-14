// 信源订阅端点（契约 B8）：加订阅 / 列订阅 / 删订阅。
// M3 固化：填 URL 加 RSS 订阅（自然语言加源是 M4）。
package api

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"

	"github.com/YouToco/vane/types"
)

// addSubscriptionReq 是 POST /api/subscriptions 的请求体。
type addSubscriptionReq struct {
	URL string `json:"url"`
}

// handleAddSubscription 幂等 upsert 信源（按 URL）→ 建立当前 owner 的订阅关系。
// POST /api/subscriptions {url} → 201 {source_id}
func (s *server) handleAddSubscription(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 8<<10)
	var req addSubscriptionReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "请求体不是合法 JSON")
		return
	}
	// 只做结构校验（scheme 合法）；SSRF/私网拦截在抓取时由 fetcher 统一兜底，
	// 不在此重复一套 DNS 解析——加订阅与抓取是两个时点，抓取侧才是权威防线。
	u, err := url.Parse(req.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		writeError(w, http.StatusBadRequest, "url 必须是合法的 http/https 地址")
		return
	}
	userID, err := s.ownerUserID(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}

	src := &types.Source{
		Type:   types.SourceTypeRSS,
		URL:    req.URL,
		Status: types.SourceStatusActive,
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
