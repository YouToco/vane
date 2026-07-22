// 定时任务端点（契约 B8）：列出 / 删除推送调度。定义更新自 C2b 起只能
// 经过完整提案确认控制面；历史 HTTP PATCH 不再注册。
package api

import (
	"net/http"

	"github.com/YouToco/vane/types"
)

// handleListSchedules 读 Postgres 镜像返回当前 owner 的调度列表。
// GET /api/schedules → 200 [Schedule...]
func (s *server) handleListSchedules(w http.ResponseWriter, r *http.Request) {
	userID, err := s.ownerUserID(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}
	list, err := s.deps.Store.ListSchedulesByUser(r.Context(), userID)
	if err != nil {
		writeAppError(w, err)
		return
	}
	// 空列表回 [] 而非 null：前端可直接 map，无需判空。
	if list == nil {
		list = []types.Schedule{}
	}
	writeJSON(w, http.StatusOK, list)
}

// handleDeleteSchedule 删除一个调度（Temporal + 镜像）。
// DELETE /api/schedules/{id} → 200 {ok}
//
// 归属校验由 Scheduler.DeletePush 内的 GetSchedule(id, userID) 承担：
// 「不存在」与「不属于你」归一为 NotFound，不给调用方枚举他人调度 id 的机会。
//
// 此处原注释写着「单 owner：所有调度同属一人，故不再逐条校验归属」——那是 M3 的实情，
// 契约 §2.8 曾据此把本处列为已知越权洞。多租户改造时校验已经补上，但注释留在了原地，
// **代码在校验、注释说不校验**。留着它的风险很具体：后来的人会据此把 userID 参数
// 当作冗余优化掉，而那一刀下去就是真的越权洞。
func (s *server) handleDeleteSchedule(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "缺少 schedule id")
		return
	}
	userID, err := s.ownerUserID(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}
	if err := s.deps.Scheduler.DeletePush(r.Context(), id, userID); err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}
