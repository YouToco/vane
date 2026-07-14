// "现在推"端点（契约 B8）：立即触发一次推送管道（不建定时调度）。
package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/YouToco/vane/workflow"
)

// pushNowReq 是 POST /api/push/now 的请求体；scope 可选（缺省=该用户全部 active 订阅）。
type pushNowReq struct {
	Scope workflow.PushScope `json:"scope"`
}

// handlePushNow 立即执行一次推送 workflow（ExecuteWorkflow，非定时）。
// POST /api/push/now {scope?} → 202 {run_id}
//
// 回 202 而非 200：workflow 是异步执行，此刻只是"已受理并派发"，卡片稍后才到达飞书。
func (s *server) handlePushNow(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
	var req pushNowReq
	// scope 可选：空 body 也合法（等价于默认全量）。仅在有内容却解析失败时报 400。
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "请求体不是合法 JSON")
		return
	}
	userID, err := s.ownerUserID(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}
	runID, err := s.deps.Scheduler.PushNow(r.Context(), userID, req.Scope)
	if err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"run_id": runID})
}
