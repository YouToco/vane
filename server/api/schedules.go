// 定时任务端点（契约 B8）：列出 / 删除推送调度。定义更新自 C2b 起只能
// 经过完整提案确认控制面；历史 HTTP PATCH 不再注册。
package api

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/YouToco/vane/server/store"
	"github.com/YouToco/vane/server/types"
)

type scheduleListItem struct {
	types.Schedule
	DeliveryChannel store.DeliveryChannelPreference `json:"delivery_channel"`
}

// handleListSchedules 读 Postgres 镜像返回当前 owner 的调度列表。
// GET /api/schedules → 200 [Schedule...]
func (s *server) handleListSchedules(w http.ResponseWriter, r *http.Request) {
	principal, err := s.deps.Principal.FromContext(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}
	list, err := s.deps.Store.ListSchedulesForMember(
		r.Context(), int64(principal.TenantID), principal.UserID)
	if err != nil {
		writeAppError(w, err)
		return
	}
	// 空列表回 [] 而非 null：前端可直接 map，无需判空。
	result := make([]scheduleListItem, 0, len(list))
	for _, schedule := range list {
		preference, err := s.deps.Store.ResolveDeliveryChannelPreference(
			r.Context(), int64(principal.TenantID), principal.UserID, schedule.ID)
		if err != nil {
			writeAppError(w, err)
			return
		}
		result = append(result, scheduleListItem{
			Schedule: schedule, DeliveryChannel: preference,
		})
	}
	writeJSON(w, http.StatusOK, result)
}

// handleDeleteSchedule 删除一个调度（Temporal + 镜像）。
// DELETE /api/schedules/{id} → 200 {ok}
//
// 归属校验由 Scheduler.DeletePushIdempotent 内的 GetSchedule(id, userID) 承担：
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
	idempotencyKey, ok := scheduleCommandIdempotencyKey(r)
	if !ok {
		writeError(
			w, http.StatusBadRequest,
			"缺少或无效的 Idempotency-Key",
		)
		return
	}
	principal, err := s.deps.Principal.FromContext(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}
	task, err := s.teamTaskAccess().AuthorizeScheduleMutation(
		r.Context(), int64(principal.TenantID), principal.UserID,
		id, store.TaskMutationDelete)
	if err != nil {
		writeAppError(w, err)
		return
	}
	controller, ok := s.deps.Scheduler.(scheduleDeleteController)
	if !ok {
		writeError(w, http.StatusServiceUnavailable, "任务操作控制面尚未就绪")
		return
	}
	if err := controller.DeletePushIdempotent(
		r.Context(), id, task.UserID, idempotencyKey,
	); err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

const transferScheduleAssigneeBodyLimit = 4 << 10

func (s *server) handleTransferScheduleAssignee(w http.ResponseWriter, r *http.Request) {
	var body struct {
		UserID int64 `json:"user_id"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(
		w, r.Body, transferScheduleAssigneeBodyLimit))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "负责人参数无效")
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF || body.UserID <= 0 {
		writeError(w, http.StatusBadRequest, "负责人参数无效")
		return
	}
	principal, err := s.deps.Principal.FromContext(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}
	task, err := s.teamTaskAccess().TransferScheduleAssignee(
		r.Context(), int64(principal.TenantID), principal.UserID,
		r.PathValue("id"), body.UserID)
	if err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, task)
}
