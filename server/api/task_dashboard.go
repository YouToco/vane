// 任务数据面只读端点（M7 功能 6.6/6.7）：任务详情页与任务列表页的任务级聚合读面。
// 本文件仍只负责 Postgres 聚合读取；6.8 的直接编辑与运行控制分别位于
// task_actions.go / schedule_actions.go。挂 /api/ 前缀后统一继承会话中间件。
package api

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/YouToco/vane/server/store"
	"github.com/YouToco/vane/server/types"
)

// schedulePlaybookDTO 是详情页手册块：只回正文与更新时间。fetch_plan 是编译产物
// （内部结构随编译器演进），不进用户面。
type schedulePlaybookDTO struct {
	Content   string    `json:"content"`
	UpdatedAt time.Time `json:"updated_at"`
}

type scheduleDetailScheduleDTO struct {
	types.Schedule
	NextRun      *time.Time           `json:"next_run,omitempty"`
	NextRunState scheduleNextRunState `json:"next_run_state"`
}

type scheduleNextRunState string

const (
	scheduleNextRunScheduled   scheduleNextRunState = "scheduled"
	scheduleNextRunPaused      scheduleNextRunState = "paused"
	scheduleNextRunNone        scheduleNextRunState = "none"
	scheduleNextRunUnavailable scheduleNextRunState = "unavailable"
)

type scheduleCapabilitiesDTO struct {
	DefinitionEdit bool `json:"definition_edit"`
}

// scheduleDetailResp 是 GET /api/schedules/{id} 的响应体。
type scheduleDetailResp struct {
	Schedule        scheduleDetailScheduleDTO       `json:"schedule"`
	Summary         store.ScheduleRunSummary        `json:"summary"`
	DeliveryChannel store.DeliveryChannelPreference `json:"delivery_channel"`
	Playbook        *schedulePlaybookDTO            `json:"playbook,omitempty"` // 无手册的老任务缺省
	Cost            store.ScheduleRunCost           `json:"cost"`               // 口径见 store/schedule_dashboard.go
	Capabilities    scheduleCapabilitiesDTO         `json:"capabilities"`
}

// handleGetScheduleDetail 返回单任务详情：本体 + 运行概览 + 手册 + 成本。
// GET /api/schedules/{id} → 200 scheduleDetailResp；不存在/非本人 → 404（GetSchedule 口径）。
func (s *server) handleGetScheduleDetail(w http.ResponseWriter, r *http.Request) {
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

	// 归属与存在性由 GetSchedule 一次把关（「不存在」与「不属于你」统一 404），
	// 后续查询全部带 userID 谓词，纵深防御但不再产生 404 分叉。
	sched, err := s.deps.Store.GetSchedule(r.Context(), id, userID)
	if err != nil {
		writeAppError(w, err)
		return
	}

	summary, err := s.deps.Store.GetScheduleRunSummary(r.Context(), userID, id)
	if err != nil {
		writeAppError(w, err)
		return
	}
	cost, err := s.deps.Store.GetScheduleRunCost(r.Context(), userID, id)
	if err != nil {
		writeAppError(w, err)
		return
	}
	deliveryChannel, err := s.deps.Store.ResolveDeliveryChannelPreference(
		r.Context(), sched.TenantID, userID, id)
	if err != nil {
		writeAppError(w, err)
		return
	}
	reader, _ := s.deps.Scheduler.(scheduleNextRunReader)
	nextRun, nextRunState, nextRunErr := projectScheduleNextRun(
		r.Context(), sched, reader,
	)
	if nextRunErr != nil {
		// Next run is an optional live Temporal projection. The durable
		// Postgres detail remains useful during a transient control-plane
		// outage, so degrade only this field and keep the page readable.
		slog.WarnContext(
			r.Context(),
			"api: read schedule next run",
			"schedule_id", id,
			"user_id", userID,
			"err", nextRunErr,
		)
	}

	// 手册是可选块：老任务/空手册任务没有行，NotFound 不是错误，整块缺省。
	var playbook *schedulePlaybookDTO
	pb, err := s.deps.Store.GetSchedulePlaybook(r.Context(), userID, id)
	switch {
	case err == nil:
		playbook = &schedulePlaybookDTO{Content: pb.Content, UpdatedAt: pb.UpdatedAt}
	case errors.Is(err, types.ErrNotFound):
		// 无手册，playbook 保持 nil。
	default:
		writeAppError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, scheduleDetailResp{
		Schedule: scheduleDetailScheduleDTO{
			Schedule:     *sched,
			NextRun:      nextRun,
			NextRunState: nextRunState,
		},
		Summary: *summary, DeliveryChannel: deliveryChannel,
		Playbook: playbook, Cost: *cost,
		Capabilities: scheduleCapabilitiesDTO{
			DefinitionEdit: true,
		},
	})
}

func projectScheduleNextRun(
	ctx context.Context,
	schedule *types.Schedule,
	reader scheduleNextRunReader,
) (*time.Time, scheduleNextRunState, error) {
	if schedule == nil {
		return nil, scheduleNextRunUnavailable, errors.New(
			"schedule next-run projection has no schedule",
		)
	}
	if schedule.Status == types.ScheduleStatusPaused {
		return nil, scheduleNextRunPaused, nil
	}
	if schedule.Status != types.ScheduleStatusActive {
		return nil, scheduleNextRunUnavailable, errors.New(
			"schedule next-run projection has invalid schedule status",
		)
	}
	if reader == nil {
		return nil, scheduleNextRunUnavailable, nil
	}
	nextRun, err := reader.NextRun(ctx, schedule.ID, schedule.UserID)
	if err != nil {
		return nil, scheduleNextRunUnavailable, err
	}
	if nextRun == nil {
		return nil, scheduleNextRunNone, nil
	}
	return nextRun, scheduleNextRunScheduled, nil
}

// scheduleBatchesResp 是 GET /api/schedules/{id}/batches 的响应体。
type scheduleBatchesResp struct {
	Items         []store.ScheduleBatchItem `json:"items"`
	Total         int64                     `json:"total"`
	NextPageToken string                    `json:"next_page_token,omitempty"`
}

// handleListScheduleBatches 返回单任务运行历史（倒序、键集分页）。
// GET /api/schedules/{id}/batches?page_size=20&page_token=... → 200 scheduleBatchesResp
func (s *server) handleListScheduleBatches(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "缺少 schedule id")
		return
	}
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
	// 404 语义与详情页一致：先证明任务可见，再翻它的历史。
	if _, err := s.deps.Store.GetSchedule(r.Context(), id, userID); err != nil {
		writeAppError(w, err)
		return
	}

	items, total, next, err := s.deps.Store.ListScheduleBatches(r.Context(), userID, id,
		store.BatchHistoryQuery{PageSize: q.PageSize, PageToken: q.PageToken})
	if err != nil {
		writeAppError(w, err)
		return
	}
	if items == nil {
		items = []store.ScheduleBatchItem{}
	}
	writeJSON(w, http.StatusOK, scheduleBatchesResp{Items: items, Total: total, NextPageToken: next})
}

// scheduleSummariesResp 是 GET /api/schedules/summary 的响应体。
type scheduleSummariesResp struct {
	Items []store.ScheduleRunSummary `json:"items"`
}

// handleListScheduleSummaries 返回当前用户全部任务的运行概览（列表页 6.7 一次请求喂饱：
// 上次运行/近 7 天批次、空批与推送数）。与 GET /api/schedules 同序，前端按 schedule_id 装配。
// GET /api/schedules/summary → 200 scheduleSummariesResp
func (s *server) handleListScheduleSummaries(w http.ResponseWriter, r *http.Request) {
	userID, err := s.ownerUserID(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}
	items, err := s.deps.Store.ListScheduleRunSummaries(r.Context(), userID)
	if err != nil {
		writeAppError(w, err)
		return
	}
	if items == nil {
		items = []store.ScheduleRunSummary{}
	}
	writeJSON(w, http.StatusOK, scheduleSummariesResp{Items: items})
}
