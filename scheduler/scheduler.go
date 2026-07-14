// Package scheduler 是唯一直接持有 Temporal SDK client 的调度封装：
// 把 Vane 侧中立的 ScheduleSpec 翻译成 client.ScheduleSpec，负责定时任务的
// 增删改与即时触发，并把创建结果镜像进 Postgres schedules 表（供 API 读取）。
//
// 分层意图：workflow 包只管"怎么执行一次推送"，scheduler 包管"何时、以何种
// 频率触发"，二者经 PushParams 解耦。API 层只调本包，不直接碰 SDK client。
package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	enums "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"

	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
	"github.com/YouToco/vane/workflow"
)

const (
	// minIntervalSeconds 是触发频率的 1 小时硬地板（规格 B7）：防止用户/前端
	// 配出每分钟触发这类会打爆 LLM 预算与飞书限流的调度。
	minIntervalSeconds = 3600
	// maxActiveSchedules 是单 owner 活跃调度上限（规格 B7）。
	maxActiveSchedules = 20
	// defaultTZ 是 spec 未指定时区时的默认值。
	defaultTZ = "Asia/Shanghai"
	// scheduleStatusActive 是 schedules 表 status 列的活跃取值。
	scheduleStatusActive = "active"
)

// ScheduleSpec 是 Vane 侧中立的调度频率描述：cron 与 every_seconds 二选一。
// 前端时间选择器编译出结构化 spec 后经 API 传入，内部翻译成 client.ScheduleSpec。
type ScheduleSpec struct {
	Cron         string `json:"cron,omitempty"`          // 5 段 cron，如 "0 8 * * *"
	EverySeconds int    `json:"every_seconds,omitempty"` // 固定间隔秒数，如 86400
	TZ           string `json:"tz,omitempty"`            // 时区名，空则用 defaultTZ
}

// Scheduler 持有 Temporal client、任务队列名与 store（镜像用）。
type Scheduler struct {
	c  client.Client
	tq string
	st *store.Store
}

// New 构造 Scheduler。client 由 cmd/server 用 client.Dial 建好后注入。
func New(c client.Client, taskQueue string, st *store.Store) *Scheduler {
	return &Scheduler{c: c, tq: taskQueue, st: st}
}

// CreatePush 创建一个定时推送调度：校验 spec → 校验活跃上限 → Temporal Create →
// 镜像入库。顺序铁律：先 Temporal Create 成功再 InsertSchedule 镜像；镜像失败则
// 补偿删除刚建的 Temporal schedule 并返回错误，使二者原子化——避免孤儿调度绕过
// 活跃上限计数（上限校验读镜像表）且对 API 不可见。补偿删除也失败才留孤儿并 slog.Error。
func (s *Scheduler) CreatePush(ctx context.Context, userID int64, spec ScheduleSpec, scope workflow.PushScope, nlDesc string) (string, error) {
	if err := validateSpec(spec); err != nil {
		return "", err
	}

	// 活跃上限在 Create 前校验：读镜像表统计当前活跃数。
	existing, err := s.st.ListSchedulesByUser(ctx, userID)
	if err != nil {
		return "", err
	}
	active := 0
	for _, sc := range existing {
		if sc.Status == scheduleStatusActive {
			active++
		}
	}
	if err := checkActiveLimit(active); err != nil {
		return "", err
	}

	sdkSpec, err := translateSpec(spec)
	if err != nil {
		return "", err
	}

	schedID := fmt.Sprintf("push-%d-%s", userID, uuid.NewString())
	params := workflow.PushParams{UserID: userID, Scope: scope}

	_, err = s.c.ScheduleClient().Create(ctx, client.ScheduleOptions{
		ID:   schedID,
		Spec: sdkSpec,
		Action: &client.ScheduleWorkflowAction{
			ID:        "wf-" + schedID,
			Workflow:  workflow.PushPipelineWorkflow,
			Args:      []any{params},
			TaskQueue: s.tq,
		},
		// SKIP：上一次触发还没跑完时跳过本次，避免推送堆叠。
		Overlap: enums.SCHEDULE_OVERLAP_POLICY_SKIP,
	})
	if err != nil {
		return "", types.NewAppError(types.CodeInternal, "创建 Temporal 定时任务失败", err)
	}

	specJSON, _ := json.Marshal(spec)
	scopeJSON, _ := json.Marshal(scope)
	mirror := &types.Schedule{
		ID:            schedID,
		UserID:        userID,
		NLDescription: nlDesc,
		SpecJSON:      json.RawMessage(specJSON),
		ScopeJSON:     json.RawMessage(scopeJSON),
		Status:        scheduleStatusActive,
	}
	if err := s.st.InsertSchedule(ctx, mirror); err != nil {
		// 镜像入库失败 → 补偿删除刚建的 Temporal schedule，使"Create+镜像"原子化。
		// 为何必须补偿：活跃上限校验读的是镜像表（ListSchedulesByUser），若留下一个
		// Temporal 有、镜像无的孤儿调度，它既绕过 ≤20 上限计数（下次校验看不到它），
		// 又对 API 完全不可见（无法在前端删除/管理），却仍在真实触发推送。
		if derr := s.c.ScheduleClient().GetHandle(ctx, schedID).Delete(ctx); derr != nil {
			// 补偿删除也失败：此时确实产生了孤儿调度，slog.Error 记 schedule_id 供人工对账。
			slog.Error("scheduler: 镜像入库失败后补偿删除 Temporal schedule 也失败（产生孤儿调度，需人工对账）",
				"schedule_id", schedID, "insert_err", err, "delete_err", derr)
		} else {
			slog.Error("scheduler: schedules 镜像入库失败，已补偿删除 Temporal schedule",
				"schedule_id", schedID, "err", err)
		}
		return "", types.NewAppError(types.CodeDatabase, "创建定时任务镜像失败，已回滚 Temporal 调度", err)
	}
	return schedID, nil
}

// PushNow 立即触发一次推送（不建调度），供"现在推"按钮用。
// 返回 workflow ID（含 uuid，唯一），调用方可据此在 Temporal 查该次执行。
func (s *Scheduler) PushNow(ctx context.Context, userID int64, scope workflow.PushScope) (string, error) {
	params := workflow.PushParams{UserID: userID, Scope: scope}
	run, err := s.c.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			ID:        fmt.Sprintf("push-adhoc-%d-%s", userID, uuid.NewString()),
			TaskQueue: s.tq,
		},
		workflow.PushPipelineWorkflow, params)
	if err != nil {
		return "", types.NewAppError(types.CodeInternal, "触发即时推送失败", err)
	}
	return run.GetID(), nil
}

// TriggerPushNow 是 agent push_now 工具的窄入口（M4 契约 §8 PushTrigger 接口）：
// 行为等价于 API POST /api/push/now 空 body（零值 scope = 该用户全部 active 订阅）。
// 单独包一层而非让 agent 直接调 PushNow：PushTrigger 只暴露 userID，
// 不把 workflow.PushScope 这类管道内部结构泄进 agent 工具面（agent 也被禁止 import api）。
//
// 与 API 路径的关键差异（审查 #push_now 扇出）：workflow ID 用**确定性** ID 而非 uuid。
// push_now 是免确认工具，模型一轮可产出任意多个调用——每个都是一整条烧 LLM 的推送管道。
// 确定性 ID 依赖 Temporal 的 WorkflowExecutionAlreadyStarted 把并发钉死为 1：
// 同一用户同时只有一条 agent 触发的管道在跑，跑完 ID 可复用（默认重用策略）。
// API 的 PushNow 保持 uuid：按钮触发天然低频，且每次独立 run_id 便于排查。
func (s *Scheduler) TriggerPushNow(ctx context.Context, userID int64) (string, error) {
	params := workflow.PushParams{UserID: userID, Scope: workflow.PushScope{}}
	run, err := s.c.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			ID:        fmt.Sprintf("push-agent-%d", userID),
			TaskQueue: s.tq,
		},
		workflow.PushPipelineWorkflow, params)
	if err != nil {
		var already *serviceerror.WorkflowExecutionAlreadyStarted
		if errors.As(err, &already) {
			// 确定性拒绝：文案回给模型自纠（不再重复触发），不可重试。
			ae := types.NewAppError(types.CodeValidation,
				"已有一次推送正在进行，请等它完成后再触发", err)
			ae.Retryable = false
			return "", ae
		}
		return "", types.NewAppError(types.CodeInternal, "触发即时推送失败", err)
	}
	return run.GetID(), nil
}

// UpdatePushSpec 更新已有调度的触发频率。逐字段改写 Spec（而非整体替换）：
// 无论 SDK 里 Schedule.Spec 是值还是指针，字段赋值都自动解引用；同时把 cron
// 与 interval 两组字段都写入（互斥、另一组置空），实现 cron⇄interval 切换。
func (s *Scheduler) UpdatePushSpec(ctx context.Context, schedID string, spec ScheduleSpec) error {
	if err := validateSpec(spec); err != nil {
		return err
	}
	sdkSpec, err := translateSpec(spec)
	if err != nil {
		return err
	}

	h := s.c.ScheduleClient().GetHandle(ctx, schedID)
	err = h.Update(ctx, client.ScheduleUpdateOptions{
		DoUpdate: func(in client.ScheduleUpdateInput) (*client.ScheduleUpdate, error) {
			sched := in.Description.Schedule
			sched.Spec.CronExpressions = sdkSpec.CronExpressions
			sched.Spec.Intervals = sdkSpec.Intervals
			sched.Spec.TimeZoneName = sdkSpec.TimeZoneName
			return &client.ScheduleUpdate{Schedule: &sched}, nil
		},
	})
	if err != nil {
		return types.NewAppError(types.CodeInternal, "更新定时任务失败", err)
	}
	return nil
}

// DeletePush 删除调度：先 Temporal Delete，再删镜像（镜像删除失败只 slog）。
func (s *Scheduler) DeletePush(ctx context.Context, schedID string) error {
	h := s.c.ScheduleClient().GetHandle(ctx, schedID)
	if err := h.Delete(ctx); err != nil {
		return types.NewAppError(types.CodeInternal, "删除定时任务失败", err)
	}
	if err := s.st.DeleteSchedule(ctx, schedID); err != nil {
		slog.Error("scheduler: schedules 镜像删除失败（Temporal 已删除）", "schedule_id", schedID, "err", err)
	}
	return nil
}

// TriggerNow 让一个已存在的定时任务立即跑一次（不影响其后续排期）。
func (s *Scheduler) TriggerNow(ctx context.Context, schedID string) error {
	h := s.c.ScheduleClient().GetHandle(ctx, schedID)
	if err := h.Trigger(ctx, client.ScheduleTriggerOptions{}); err != nil {
		return types.NewAppError(types.CodeInternal, "触发定时任务失败", err)
	}
	return nil
}

// ============================================================
// 纯逻辑（无 I/O，单测覆盖）：spec 校验、活跃上限、Vane spec→SDK spec 翻译。
// ============================================================

// validateSpec 校验中立 spec：cron 与 every_seconds 恰好提供一个，且频率不低于
// 1 小时硬地板。
func validateSpec(spec ScheduleSpec) error {
	hasCron := strings.TrimSpace(spec.Cron) != ""
	hasEvery := spec.EverySeconds > 0
	if hasCron == hasEvery {
		return types.NewAppError(types.CodeValidation, "spec 必须且只能提供 cron 或 every_seconds 之一", nil)
	}
	if hasEvery {
		if spec.EverySeconds < minIntervalSeconds {
			return types.NewAppError(types.CodeValidation,
				fmt.Sprintf("触发间隔 %ds 小于 1 小时硬地板（%ds）", spec.EverySeconds, minIntervalSeconds), nil)
		}
		return nil
	}
	return validateCronMinInterval(spec.Cron)
}

// validateCronMinInterval 强制 cron 触发频率不高于每小时一次。
// 判据：分钟字段必须是单一整数（0-59）——此时每小时至多触发一次，间隔 ≥3600s。
// 含 '*' / '/'（步长）/ ',' （列表）/ '-'（区间）的分钟字段都可能 sub-hourly，拒绝。
// 前端固化组件只产出整点档位，正常不会触发此拒绝；这是防御性白名单。
func validateCronMinInterval(cron string) error {
	fields := strings.Fields(cron)
	if len(fields) != 5 {
		return types.NewAppError(types.CodeValidation, "cron 必须是 5 段（分 时 日 月 周）", nil)
	}
	minute := fields[0]
	if strings.ContainsAny(minute, "*/,-") {
		return types.NewAppError(types.CodeValidation,
			"cron 分钟字段过细，触发频率超过 1 小时硬地板", nil)
	}
	n, err := strconv.Atoi(minute)
	if err != nil || n < 0 || n > 59 {
		return types.NewAppError(types.CodeValidation, "cron 分钟字段非法（应为 0-59 的整数）", nil)
	}
	return nil
}

// checkActiveLimit 校验单 owner 活跃调度数未达上限。
func checkActiveLimit(active int) error {
	if active >= maxActiveSchedules {
		return types.NewAppError(types.CodeValidation,
			fmt.Sprintf("活跃定时任务已达上限（%d 个）", maxActiveSchedules), nil)
	}
	return nil
}

// translateSpec 把中立 spec 翻译成 client.ScheduleSpec。默认时区 Asia/Shanghai。
// 假定入参已过 validateSpec；仍再校验一次以保证独立调用安全。
func translateSpec(spec ScheduleSpec) (client.ScheduleSpec, error) {
	if err := validateSpec(spec); err != nil {
		return client.ScheduleSpec{}, err
	}
	tz := spec.TZ
	if tz == "" {
		tz = defaultTZ
	}
	if spec.EverySeconds > 0 {
		return client.ScheduleSpec{
			Intervals:    []client.ScheduleIntervalSpec{{Every: time.Duration(spec.EverySeconds) * time.Second}},
			TimeZoneName: tz,
		}, nil
	}
	return client.ScheduleSpec{
		CronExpressions: []string{spec.Cron},
		TimeZoneName:    tz,
	}, nil
}
