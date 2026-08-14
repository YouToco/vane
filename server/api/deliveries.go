// 推送历史只读端点（M7 功能 6.4）：Dashboard 回溯每条推送的打分、状态与反馈。
// 只读（不写表、不调模型），GET 语义完整；挂 /api/ 前缀自动继承会话中间件
// （单用户阶段 Dashboard 密码即 owner 凭证，理由见 observability.go 头注释）。
package api

import (
	"net/http"
	"net/url"
	"strconv"

	"github.com/YouToco/vane/store"
)

// deliveriesResp 是 GET /api/deliveries 的响应体。
type deliveriesResp struct {
	Items         []store.DeliveryHistoryItem `json:"items"`
	Total         int64                       `json:"total"`
	NextPageToken string                      `json:"next_page_token,omitempty"`
}

// parseHistoryQuery 解析 page_size / page_token。
// 空值按缺省处理（与 parseWindowHours 同理：前端清空输入框拼出的就是空串）；
// page_size 非数字或越界回人话 400，钳制交给 store 层统一做。
func parseHistoryQuery(q url.Values) (store.DeliveryHistoryQuery, string) {
	out := store.DeliveryHistoryQuery{PageToken: q.Get("page_token")}
	raw := q.Get("page_size")
	if raw == "" {
		return out, ""
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 || n > 100 {
		return store.DeliveryHistoryQuery{}, "page_size 必须是 1 到 100 之间的整数"
	}
	out.PageSize = n
	return out, ""
}

// handleListDeliveries 返回当前 owner 的推送历史（倒序、键集分页、含反馈）。
// GET /api/deliveries?page_size=20&page_token=... → 200 deliveriesResp
func (s *server) handleListDeliveries(w http.ResponseWriter, r *http.Request) {
	q, errMsg := parseHistoryQuery(r.URL.Query())
	if errMsg != "" {
		writeError(w, http.StatusBadRequest, errMsg)
		return
	}

	userID, err := s.ownerUserID(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}

	items, total, next, err := s.deps.Store.ListDeliveryHistory(r.Context(), userID, q)
	if err != nil {
		writeAppError(w, err)
		return
	}
	if items == nil {
		items = []store.DeliveryHistoryItem{} // 首页为空时序列化出 [] 而非 null
	}
	writeJSON(w, http.StatusOK, deliveriesResp{Items: items, Total: total, NextPageToken: next})
}
