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
//
// 两条路径的语义**不等价**，选哪条会影响触发时刻，不是同一件事的两种写法：
//   - Cron 是**日历语义**：按 tz 的墙上时间匹配（"每天 8:30"、"每周一 9:00"），
//     夏令时切换按该时区规则走。精度到分钟——validateCronMinInterval 只收 5 段
//     （分 时 日 月 周），带秒的 6 段 cron 会被拒；且分钟字段必须是固定整数，
//     这正是 1 小时频率地板的实现方式（分钟固定 ⇒ 每小时至多触发一次）。
//   - EverySeconds 是**固定间隔语义**：Temporal 的 ScheduleIntervalSpec 匹配
//     `Epoch + n*Every + Offset`（proto 里叫 phase），基准是 **UTC Unix epoch**。
//     不给 AnchorAt 时 Offset=0，故 every_seconds=21600（6h）落在 00/06/12/18 点整（UTC），
//     **不是**"从创建那一刻起每 6 小时"——这个 epoch 对齐曾是本类型最容易误解的地方。
//     给了 AnchorAt 就能把相位挪到任意时刻（见下）。
//
// AnchorAt 解决的正是"我要从某个具体时刻起、每 N 秒一次"：cron 表达不了 every 不整除
// 24 小时的周期（每 7 小时、每 3 天），而不带相位的 interval 又只能落在 epoch 对齐点上。
// 例：每 3 天的晚上 8 点 → EverySeconds=259200 + AnchorAt="2026-07-19T20:00:00+08:00"。
//
// 精度取舍（保留记录，2026-07-18 复核）：cron 只到分钟（分钟字段必须固定整数——这正是
// 1 小时地板的实现方式），every_seconds 则可到秒。**这不是因为秒级危险**：7 段 cron
// `15 30 8 * * * *`（每天 8:30:15）间隔 24h 完全合规，它被拒纯粹是 5 段校验的副作用。
// 不补 7 段 cron 的真实理由只有"推送场景无用例"，不是技术限制（Temporal 支持 5/6/7 段，
// 注意第 6 段是 Year 不是 Second，秒要写 7 段）。
type ScheduleSpec struct {
	Cron         string `json:"cron,omitempty"`          // 5 段 cron，如 "0 8 * * *"
	EverySeconds int    `json:"every_seconds,omitempty"` // 固定间隔秒数，如 86400
	// AnchorAt 是 RFC3339 绝对时刻，只对 EverySeconds 有效：把 interval 的相位对齐到
	// 该时刻，触发点变成 anchor、anchor+every、anchor+2*every…。留空则相位为 0
	//（epoch 对齐）。存的是用户给的原始时刻而非算出的相位——列表页要显示"从 X 起每 N"，
	// 存相位就只剩一个没人看得懂的秒数。
	AnchorAt string `json:"anchor_at,omitempty"`
	TZ       string `json:"tz,omitempty"` // 时区名，空则用 defaultTZ
}

// scheduleStore 是本包用到的 store 子集（镜像读写）。
//
// 收窄成接口而非直接持 *store.Store 的理由：Create/Update/Delete 三条路径的
// **补偿分支只在镜像写失败时才走到**，而那是最需要测、又最不可能在真库上自然发生的
// 分支——拿具体类型就只能把它们留成永不执行的注释（本包 CreatePush/DeletePush 至今
// 零测试正是这个原因）。有了接口，替身可以精确模拟"Temporal 成功但镜像失败"，
// 把回滚是否真的发生钉死在单测里。*store.Store 隐式满足本接口，装配处零改动。
type scheduleStore interface {
	ListSchedulesByUser(ctx context.Context, userID int64) ([]types.Schedule, error)
	InsertSchedule(ctx context.Context, sc *types.Schedule) error
	UpdateScheduleSpec(ctx context.Context, id string, spec json.RawMessage, nlDesc *string) error
	DeleteSchedule(ctx context.Context, id string, userID int64) error
	GetSchedule(ctx context.Context, id string, userID int64) (*types.Schedule, error)
}

// Scheduler 持有 Temporal client、任务队列名与 store（镜像用）。
type Scheduler struct {
	c  client.Client
	tq string
	st scheduleStore
}

// New 构造 Scheduler。client 由 cmd/server 用 client.Dial 建好后注入；
// st 传 *store.Store（隐式满足 scheduleStore）。
func New(c client.Client, taskQueue string, st scheduleStore) *Scheduler {
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
	params := workflow.PushParams{UserID: userID, Scope: scope, NLDesc: strings.TrimSpace(nlDesc)}

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
//
// SDK 坑：同 ID workflow 正在运行时 ExecuteWorkflow 默认**不返回错误**，而是静默
// attach 到现有 execution 并正常返回其 run 句柄——AlreadyStarted 只有显式置
// WorkflowExecutionErrorWhenAlreadyStarted:true 才会抛出（sdk v1.46.0 internal/client.go），
// 否则下方拒绝分支永远走不到，重复触发会返回与成功相同的文案。
func (s *Scheduler) TriggerPushNow(ctx context.Context, userID int64) (string, error) {
	params := workflow.PushParams{UserID: userID, Scope: workflow.PushScope{}}
	run, err := s.c.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			ID:        fmt.Sprintf("push-agent-%d", userID),
			TaskQueue: s.tq,
			// 见函数注释：不置 true 则同 ID 在跑时 SDK 静默 attach，并发护栏失效。
			// 不影响"跑完 ID 复用"：默认 WorkflowIDReusePolicy=AllowDuplicate 允许
			// 对已完成的同 ID re-run，本开关只在策略禁止 re-run 时才报错。
			WorkflowExecutionErrorWhenAlreadyStarted: true,
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

// UpdatePush 原地改一个已存在调度的触发频率（可选连带 nl_description）。
//
// 为什么要有它、而不是让调用方 Delete+Create（本方法存在的全部理由）：删重建会
// **换掉 schedule_id**（用户记的 id、前端列表里的行、外部引用全部失效）、在两次调用
// 之间留下一段没有调度的窗口，且 Create 失败时旧调度已经删了——非原子，改个时间
// 却可能把定时推送弄丢。原地 Update 由 Temporal 服务端做单次原子替换。
//
// **只替换 Spec，其余原样交回**：DoUpdate 回调拿到的 in.Description.Schedule 是当前
// 完整调度（Action=跑哪个 workflow/哪个 TaskQueue、Policy=Overlap SKIP 推送堆叠护栏、
// State）。这里值拷贝它、只覆盖 Spec 字段再返回——**绝不能自己 new 一个 Schedule**，
// 那会静默丢掉 Action 与 Overlap 策略，表现是调度还在、却再也不推送或开始堆叠。
//
// 原子性（CreatePush 那条铁律的 update 版）：先 Temporal Update 成功、再更新 Postgres
// 镜像；镜像失败则把 Temporal 补偿回旧 Spec，使二者不漂移，并把镜像错误上抛（调用方
// 会看到失败，不会以为改成了）。补偿本身也失败时只能留漂移（Temporal 新、镜像旧）
// 并 slog.Error——此时列表页显示的频率是假的，必须有日志可查。
//
// 并发注意：Temporal SDK 明确警告并行 Update 同一 schedule 有竞态。单 owner MVP 下
// 改调度是低频人工操作，不额外加锁；多用户/自动化改调度前需要补乐观锁（ConflictToken）。
func (s *Scheduler) UpdatePush(ctx context.Context, schedID string, userID int64, spec ScheduleSpec, nlDesc *string) error {
	if err := validateSpec(spec); err != nil {
		return err
	}
	// 归属校验先行，理由同 DeletePush（Temporal Update 同样先于镜像发生）。
	if _, err := s.st.GetSchedule(ctx, schedID, userID); err != nil {
		return err
	}
	sdkSpec, err := translateSpec(spec)
	if err != nil {
		return err
	}

	h := s.c.ScheduleClient().GetHandle(ctx, schedID)
	// 捕获旧 Spec 供镜像失败时补偿回滚（回调内是唯一能拿到服务端当前 Spec 的地方）。
	var oldSpec *client.ScheduleSpec
	err = h.Update(ctx, client.ScheduleUpdateOptions{
		DoUpdate: func(in client.ScheduleUpdateInput) (*client.ScheduleUpdate, error) {
			sch := in.Description.Schedule // 值拷贝：Action/Policy/State 随之带走
			oldSpec = sch.Spec
			sch.Spec = &sdkSpec
			return &client.ScheduleUpdate{Schedule: &sch}, nil
		},
	})
	if err != nil {
		var nf *serviceerror.NotFound
		if errors.As(err, &nf) {
			return types.NewAppError(types.CodeNotFound,
				fmt.Sprintf("定时任务 %s 不存在", schedID), err)
		}
		return types.NewAppError(types.CodeInternal, "更新定时任务失败", err)
	}

	specJSON, err := json.Marshal(spec)
	if err != nil {
		return types.NewAppError(types.CodeInternal, "序列化调度 spec 失败", err)
	}
	if mirrorErr := s.st.UpdateScheduleSpec(ctx, schedID, specJSON, nlDesc); mirrorErr != nil {
		if oldSpec != nil {
			rbErr := h.Update(ctx, client.ScheduleUpdateOptions{
				DoUpdate: func(in client.ScheduleUpdateInput) (*client.ScheduleUpdate, error) {
					sch := in.Description.Schedule
					sch.Spec = oldSpec
					return &client.ScheduleUpdate{Schedule: &sch}, nil
				},
			})
			if rbErr != nil {
				slog.Error("scheduler: 镜像更新失败且 Temporal 回滚也失败，调度已漂移（Temporal 新/镜像旧）",
					"schedule_id", schedID, "mirror_err", mirrorErr, "rollback_err", rbErr)
			}
		}
		return mirrorErr
	}
	return nil
}

// DeletePush 删除调度：**先校验归属**，再 Temporal Delete，最后删镜像
// （镜像删除失败只 slog）。
//
// 归属校验必须在动 Temporal 之前——Temporal 的删除不可逆，
// 「先删后校验」等于校验失败时对方的调度已经没了，校验形同虚设。
func (s *Scheduler) DeletePush(ctx context.Context, schedID string, userID int64) error {
	// GetSchedule 带 user_id 谓词：不存在与不属于你归一为 NotFound，
	// 不给调用方枚举他人调度 id 的机会。
	if _, err := s.st.GetSchedule(ctx, schedID, userID); err != nil {
		return err
	}
	h := s.c.ScheduleClient().GetHandle(ctx, schedID)
	if err := h.Delete(ctx); err != nil {
		return types.NewAppError(types.CodeInternal, "删除定时任务失败", err)
	}
	if err := s.st.DeleteSchedule(ctx, schedID, userID); err != nil {
		slog.Error("scheduler: schedules 镜像删除失败（Temporal 已删除）", "schedule_id", schedID, "err", err)
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
		if _, err := parseAnchor(spec.AnchorAt); err != nil {
			return err
		}
		return nil
	}
	// anchor_at 只对 interval 有意义：cron 本身就指定了墙上时刻，再给个相位锚点是
	// 两套互相矛盾的时间表达。静默忽略会让用户以为锚点生效了，故显式拒绝。
	if strings.TrimSpace(spec.AnchorAt) != "" {
		return types.NewAppError(types.CodeValidation,
			"anchor_at 只能与 every_seconds 搭配（cron 已经指定了触发时刻，无需锚点）", nil)
	}
	return validateCronMinInterval(spec.Cron)
}

// validateCronMinInterval 强制 cron 触发频率不高于每小时一次。
// 判据：分钟字段必须是单一整数（0-59）——此时每小时至多触发一次，间隔 ≥3600s。
// 含 '*' / '/'（步长）/ ',' （列表）/ '-'（区间）的分钟字段都可能 sub-hourly，拒绝。
// 前端固化组件只产出整点档位，正常不会触发此拒绝；这是防御性白名单。
// parseAnchor 解析 AnchorAt。空串返回零值 time（表示不设锚点，相位为 0）。
// 必须带时区信息（RFC3339 要求），否则"晚上 8 点"是哪个时区的 8 点无从谈起。
func parseAnchor(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, types.NewAppError(types.CodeValidation,
			fmt.Sprintf("anchor_at 必须是 RFC3339 时刻（如 2026-07-19T20:00:00+08:00），实得 %q", s), err)
	}
	return t, nil
}

// intervalOffset 由锚点算出 Temporal 的相位（proto 里的 phase）。
//
// Temporal 匹配 `epoch + n*interval + offset`，基准是 **UTC Unix epoch**（proto 文档的
// 例子全是 Z 时刻）。锚点是绝对时刻，其 Unix 秒对 interval 取模就是相位——**这个算法
// 与时区无关**：AnchorAt 带的时区只影响它解析成哪个绝对时刻，之后一切都在 Unix 秒上做。
//
// 取模用 `((x % n) + n) % n` 而非裸 `%`：Go 的 % 对负数返回负值（1969 年之前的锚点会
// 产生负 Unix 秒），而 Temporal 要求 phase 非负（proto: "Both interval and phase must
// be non-negative"），裸取模会让服务端拒绝整个调度。
func intervalOffset(anchor time.Time, everySeconds int) time.Duration {
	if anchor.IsZero() || everySeconds <= 0 {
		return 0
	}
	n := int64(everySeconds)
	off := ((anchor.Unix() % n) + n) % n
	return time.Duration(off) * time.Second
}

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
		anchor, err := parseAnchor(spec.AnchorAt)
		if err != nil {
			return client.ScheduleSpec{}, err
		}
		return client.ScheduleSpec{
			Intervals: []client.ScheduleIntervalSpec{{
				Every:  time.Duration(spec.EverySeconds) * time.Second,
				Offset: intervalOffset(anchor, spec.EverySeconds),
			}},
			TimeZoneName: tz,
		}, nil
	}
	return client.ScheduleSpec{
		CronExpressions: []string{spec.Cron},
		TimeZoneName:    tz,
	}, nil
}
