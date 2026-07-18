// 定时任务端点（契约 B8）：列出 / 新建 / 删除推送调度。
// 铁律：cron 不接受任意透传——前端时间选择器把频率档位编译成结构化 spec
// （{cron} 或 {every_seconds}），后端在此再做结构与频率下限校验，再交 scheduler 落 Temporal。
package api

import (
	"encoding/json"
	"net/http"

	"github.com/YouToco/vane/scheduler"
	"github.com/YouToco/vane/types"
	"github.com/YouToco/vane/workflow"
)

// scheduleSpecDTO 是前端编译出的结构化调度 spec。二选一：cron 或 every_seconds。
// 不直接反序列化进 scheduler.ScheduleSpec：DTO 是 HTTP 契约、可独立演进，
// 且在翻译点集中做校验，避免把未校验的外部结构直接送进调度层。
type scheduleSpecDTO struct {
	Cron         string `json:"cron"`
	EverySeconds int    `json:"every_seconds"`
	// AnchorAt 只对 every_seconds 有效：把固定间隔的相位对齐到该绝对时刻，
	// 触发点变成 anchor、anchor+every…。空则相位为 0（Unix epoch 对齐）。
	AnchorAt string `json:"anchor_at"`
	TZ       string `json:"tz"`
}

// createScheduleReq 是 POST /api/schedules 的请求体。
type createScheduleReq struct {
	Spec          scheduleSpecDTO    `json:"spec"`
	Scope         workflow.PushScope `json:"scope"`
	NLDescription string             `json:"nl_description"`
}

// minEverySeconds 是 every_seconds 型调度的频率硬地板（1h），与 scheduler 侧一致。
// 这里前置拦截只为尽早给出清晰 400；scheduler 仍是权威校验方（cron 频率也在那侧算）。
const minEverySeconds = 3600

// toScheduleSpec 校验 DTO 并翻译成调度层中立结构。
// 校验：cron 与 every_seconds 恰好提供其一；every_seconds 不低于 1h 地板。
func (d scheduleSpecDTO) toScheduleSpec() (scheduler.ScheduleSpec, error) {
	hasCron := d.Cron != ""
	hasEvery := d.EverySeconds > 0
	if hasCron == hasEvery {
		return scheduler.ScheduleSpec{}, types.NewAppError(types.CodeValidation,
			"spec 必须且只能提供 cron 或 every_seconds 之一", nil)
	}
	if hasEvery && d.EverySeconds < minEverySeconds {
		return scheduler.ScheduleSpec{}, types.NewAppError(types.CodeValidation,
			"推送间隔不得小于 1 小时", nil)
	}
	return scheduler.ScheduleSpec{
		Cron:         d.Cron,
		EverySeconds: d.EverySeconds,
		AnchorAt:     d.AnchorAt,
		TZ:           d.TZ,
	}, nil
}

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

// handleCreateSchedule 校验结构化 spec → scheduler.CreatePush（建 Temporal Schedule + 镜像入库）。
// POST /api/schedules {spec, scope, nl_description} → 201 {schedule_id}
func (s *server) handleCreateSchedule(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
	var req createScheduleReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "请求体不是合法 JSON")
		return
	}
	spec, err := req.Spec.toScheduleSpec()
	if err != nil {
		writeAppError(w, err)
		return
	}
	userID, err := s.ownerUserID(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}
	schedID, err := s.deps.Scheduler.CreatePush(r.Context(), userID, spec, req.Scope, req.NLDescription)
	if err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"schedule_id": schedID})
}

// updateScheduleReq 是 PATCH /api/schedules/{id} 的请求体。
//
// 只收 spec 与 nl_description：scope（推哪些源）不在本端点的职责内——改频率与改范围
// 是两件事，混在一个 PATCH 里会让"只想改时间"的请求不小心把 scope 覆盖成零值。
// nl_description 用指针：省略=不改，显式 "" =清空（与 store 层 nil 语义一致）。
type updateScheduleReq struct {
	Spec          scheduleSpecDTO `json:"spec"`
	NLDescription *string         `json:"nl_description"`
}

// handleUpdateSchedule 原地改一个调度的触发频率（Temporal Update + 镜像同步）。
// PATCH /api/schedules/{id} {spec, nl_description?} → 200 {ok}
//
// 为什么是 PATCH 而不是 PUT：请求体只承载调度的一部分字段（频率、描述），
// scope/status 等不在其中，PUT 的"整体替换"语义会误导调用方。
//
// 与 DELETE 一致，M3 单 owner 不逐条校验归属（Dashboard 有密码门）。
func (s *server) handleUpdateSchedule(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "缺少 schedule id")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
	var req updateScheduleReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "请求体不是合法 JSON")
		return
	}
	spec, err := req.Spec.toScheduleSpec()
	if err != nil {
		writeAppError(w, err)
		return
	}
	if err := s.deps.Scheduler.UpdatePush(r.Context(), id, spec, req.NLDescription); err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// handleDeleteSchedule 删除一个调度（Temporal + 镜像）。
// DELETE /api/schedules/{id} → 200 {ok}
//
// M3 单 owner：所有调度同属一人，且 Dashboard 有密码门，故不再逐条校验归属。
func (s *server) handleDeleteSchedule(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "缺少 schedule id")
		return
	}
	if err := s.deps.Scheduler.DeletePush(r.Context(), id); err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}
