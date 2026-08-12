// Package scheduler 是唯一直接持有 Temporal SDK client 的调度封装：
// 把 Vane 侧中立的 ScheduleSpec 翻译成 client.ScheduleSpec，负责定时任务的
// 增删改与即时触发，并把创建结果镜像进 Postgres schedules 表（供 API 读取）。
//
// 分层意图：workflow 包只管"怎么执行一次推送"，scheduler 包管"何时、以何种
// 频率触发"，二者经 PushParams 解耦。API 层只调本包，不直接碰 SDK client。
package scheduler

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enums "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"

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
	// scheduleReconcileAttemptTimeout bounds the one startup-only transaction
	// which intentionally keeps a PostgreSQL advisory lock across Temporal I/O.
	// The lock closes the legacy List→quiesce race; the timeout prevents one
	// unhealthy remote schedule from pinning that lock indefinitely.
	scheduleReconcileAttemptTimeout = 15 * time.Second
	scheduleReconcileReleaseTimeout = 2 * time.Second
	// scheduleReconcilePassTimeout bounds the synchronous startup barrier as a
	// whole. Per-schedule timeouts alone scale as active-count × 15 seconds and
	// can otherwise keep every ingress dark indefinitely.
	scheduleReconcilePassTimeout = 90 * time.Second
	// ScheduleCommandRecoveryPassTimeout bounds both the synchronous startup
	// barrier and each periodic recovery pass. Per-command attempt bounds alone
	// would still scale as pending-count × 15 seconds and could keep ingress
	// dark, or prevent the periodic loop from returning to its ticker.
	ScheduleCommandRecoveryPassTimeout = 90 * time.Second
	// defaultScheduleCommandAttemptTimeout is one shared online/recovery budget
	// for the complete durable intent + one remote convergence attempt.
	defaultScheduleCommandAttemptTimeout = 15 * time.Second
	// scheduleCommandReleaseReserve leaves room inside the total attempt budget
	// for Store's bounded rollback, which is what actually releases row and
	// advisory locks when the remote call consumes its whole work deadline.
	scheduleCommandReleaseReserve = 2 * time.Second
	// scheduleCommandFactReadbackTimeout bounds detached Temporal fact reads
	// and terminal checkpoint writes after the request context is cancelled.
	// These paths intentionally survive client disconnects, but must not retain
	// the PostgreSQL transaction/advisory lock indefinitely.
	scheduleCommandFactReadbackTimeout = 5 * time.Second
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
	ResolveActiveTenantForUser(ctx context.Context, userID int64) (int64, error)
	ListRecoveryTenantCatalogPage(ctx context.Context, afterTenantID int64, limit int) ([]int64, error)
	ListSchedulesByUser(ctx context.Context, userID int64) ([]types.Schedule, error)
	ListActiveSchedules(ctx context.Context, tenantID int64) ([]types.Schedule, error)
	InsertSchedule(ctx context.Context, sc *types.Schedule) error
	UpdateScheduleSpec(ctx context.Context, id string, spec json.RawMessage, nlDesc *string) error
	DeleteSchedule(ctx context.Context, id string, userID int64) error
	GetSchedule(ctx context.Context, id string, userID int64) (*types.Schedule, error)
	AcquireScheduleReconcile(
		ctx context.Context,
		tenantID int64,
		id string,
	) (*types.Schedule, func(context.Context) error, error)
}

type scheduleStatusStore interface {
	BeginScheduleStatusChange(
		ctx context.Context,
		id string,
		userID int64,
		from types.ScheduleStatus,
		to types.ScheduleStatus,
	) (
		commit func(context.Context) error,
		rollback func(context.Context) error,
		err error,
	)
}

type scheduleCommandStore interface {
	CreateOrLoadScheduleCommand(
		ctx context.Context,
		tenantID, userID int64,
		taskID, key string,
		kind types.ScheduleCommandKind,
		payloadDigest, remoteRequestID string,
	) (*types.ScheduleCommand, error)
	LoadScheduleCommand(
		ctx context.Context,
		tenantID, userID int64,
		key string,
	) (*types.ScheduleCommand, error)
	BeginScheduleCommandAttempt(
		ctx context.Context,
		tenantID, userID int64,
		key string,
	) (
		command *types.ScheduleCommand,
		schedule *types.Schedule,
		complete func(context.Context) error,
		block func(context.Context, string, string) error,
		rollback func(context.Context) error,
		err error,
	)
	ListPendingScheduleCommands(
		ctx context.Context,
		tenantID int64,
		afterID string,
	) ([]types.ScheduleCommand, error)
}

type scheduleCommandRecoveryCursorStore interface {
	LoadScheduleCommandRecoveryCursor(context.Context) (int64, string, error)
	SaveScheduleCommandRecoveryCursor(context.Context, int64, string) error
}

type toolRuntimeCapabilityStore interface {
	HasCurrentToolApprovedDefinition(
		context.Context, int64, int64, string,
	) (bool, error)
}

type researchV3ActionAuthorityStore interface {
	VerifyEnabledResearchV3ActionAuthorization(
		context.Context, int64, int64, string, string,
	) error
}

// Scheduler 持有 Temporal client、任务队列名与 store（镜像用）。
type Scheduler struct {
	c                       client.Client
	tq                      string
	st                      scheduleStore
	taskScheduleGates       taskScheduleGateSet
	taskScheduleEnv         taskScheduleEnvironment
	compiledRuntime         compiledRuntimeRollout
	toolRuntime             toolRuntimeRollout
	runOutcome              runOutcomeRollout
	canonicalBrief          canonicalBriefRollout
	structuredInsight       structuredInsightRollout
	structuredEventEvidence rolloutScopeV1
	executiveBrief          rolloutScopeV1
	researchV3              researchV3Rollout
	commandAttempt          time.Duration
	commandRecoveryMu       sync.Mutex
	commandRecoveryCursor   scheduleCommandRecoveryCursor
}

// New 构造 Scheduler。client 由 cmd/server 用 client.Dial 建好后注入；
// st 传 *store.Store（隐式满足 scheduleStore）。
func New(c client.Client, taskQueue string, st scheduleStore, opts ...SchedulerOption) *Scheduler {
	s := &Scheduler{
		c: c, tq: taskQueue, st: st,
		commandAttempt: defaultScheduleCommandAttemptTimeout,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(s)
		}
	}
	return s
}

func withScheduleCommandAttemptTimeout(timeout time.Duration) SchedulerOption {
	return func(s *Scheduler) {
		s.commandAttempt = timeout
	}
}

func (s *Scheduler) scheduleCommandAttemptTimeout() time.Duration {
	if s.commandAttempt <= 0 {
		return defaultScheduleCommandAttemptTimeout
	}
	return s.commandAttempt
}

func requireToolRuntimeDefinition(
	ctx context.Context,
	st scheduleStore,
	tenantID, userID int64,
	taskID string,
) error {
	capabilities, ok := st.(toolRuntimeCapabilityStore)
	if !ok {
		return types.NewAppError(
			types.CodeConflict,
			"Tool runtime definition preflight is unavailable", nil)
	}
	available, err := capabilities.HasCurrentToolApprovedDefinition(
		ctx, tenantID, userID, taskID)
	if err != nil {
		return err
	}
	if !available {
		return types.NewAppError(
			types.CodeConflict,
			"task has no current Tool-approved definition", nil)
	}
	return nil
}

func (s *Scheduler) newScheduleCommandWorkContext(
	parent context.Context,
) (context.Context, context.CancelFunc) {
	total := s.scheduleCommandAttemptTimeout()
	reserve := scheduleCommandReleaseReserve
	if total <= reserve {
		reserve = total / 4
	}
	return context.WithTimeout(parent, total-reserve)
}

// CreatePush 创建一个定时推送调度：校验 spec → 校验活跃上限 → Temporal Create →
// 镜像入库。顺序铁律：先 Temporal Create 成功再 InsertSchedule 镜像；镜像失败则
// 补偿删除刚建的 Temporal schedule 并返回错误，使二者原子化——避免孤儿调度绕过
// 活跃上限计数（上限校验读镜像表）且对 API 不可见。补偿删除也失败才留孤儿并 slog.Error。
func (s *Scheduler) CreatePush(ctx context.Context, userID int64, spec ScheduleSpec, scope workflow.PushScope, nlDesc string) (string, error) {
	if err := validateSpec(spec); err != nil {
		return "", err
	}
	tenantID, err := s.st.ResolveActiveTenantForUser(ctx, userID)
	if err != nil {
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
	// makePushParams 统一构造 Action 入参：ScheduleID=schedID 让定时触发带上归属任务 id，
	// 供 Fetch/候选按本任务的源隔离（P1b b3）；NLDesc 是聚合卡 header 的任务名（#75）。
	// 与 ReconcileActions 共用同一构造器，杜绝"新建"与"补齐"两条路径的 params 漂移。
	// The legacy HTTP entry does not carry a tenant on the wire, so resolve its
	// one active membership before creating Temporal state. A tenant-less fresh
	// scheduled run is fail-closed and must never be created for later repair.
	params := makePushParams(tenantID, userID, schedID, scope, nlDesc)
	params = s.actionParamsFor(params)

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
		TenantID:      tenantID,
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

// makePushParams 构造 PushPipelineWorkflow 的入参（= Schedule.Action.Args[0]）。
// CreatePush 建调度、ReconcileActions 补齐存量调度都经此构造，保证两条路径逐字一致。
func makePushParams(tenantID, userID int64, schedID string, scope workflow.PushScope, nlDesc string) workflow.PushParams {
	return workflow.PushParams{
		TenantID:      tenantID,
		UserID:        userID,
		RunKind:       workflow.PushRunKindScheduled,
		ExecutionMode: types.ExecutionModeCompiled,
		ScheduleID:    schedID,
		Scope:         scope,
		NLDesc:        strings.TrimSpace(nlDesc),
	}
}

// ReconcileActions 把存量 active 调度的 Temporal Action 入参补齐到当前代码构造的 params
// （含 TenantID / RunKind / ExecutionMode / RuntimeVersion / ScheduleID / NLDesc）。
// **由 cmd/server 在启动时调用一次**。
//
// 为什么需要（P1b 上线才暴露的断裂）：Temporal 在**建调度那一刻**就把 workflow 启动入参
// 冻结进 schedule spec，b1 只在 CreatePush 时把 ScheduleID 写进 Action.Args。b1 之前建的
// 存量调度（如 07-14 建的早报任务）Action.Args 里没有 schedule_id，每次定时触发 workflow
// 都拿到空 ScheduleID → b3 的 planScoped 恒 false → 隔离永不激活。于是决策 #4 的"老任务
// 补手册→自包含"迁移成了死胡同：手册编译写了 task_fetch_targets，调度触发却照旧抓全部订阅。
// 本方法在启动时给存量调度补上 schedule_id，让已编译手册的任务真正走隔离；**b3 仍以
// ScheduleHasSources 门禁把关**——无手册任务的 schedule_id 到了 workflow 也因 task_fetch_targets
// 为空而回落用户级，决策 #4「无手册老任务抓全部订阅」不破。
//
// 同时以数据库镜像的 TenantID 升级旧 tenant=0 Action，并补齐显式 scheduled RunKind、
// Compiled 语义及当前 runtime rollout 版本；未知或不完整的新信封会 fail-closed。
//
// 顺带自愈任务名漂移（#3）：判据比对整套会漂移的字段，不只 schedule_id。
// UpdatePush 改任务名时只换 Spec、不碰 Action，镜像 nl_description 新而 Action.NLDesc 旧 → 聚合卡
// header 永久显示旧任务名；本方法在下次启动时据镜像把 Action 刷回新名（UpdatePush 侧另有即时回写）。
//
// fail-closed：单条失败会继续检查其余调度以收集诊断，但最终返回聚合错误，调用方不得
// 开放 worker/Agent/HTTP/飞书 ingress；全局预算耗尽时立即停止，不再按 active 数线性等待。
// 幂等：先 Describe 看 Action.Args 的可漂移字段是否已与期望 params 一致，一致则跳过、
// 不写 Temporal（新建调度、以及上次已 reconcile 过且未改名的调度，重启时都命中跳过）。
func (s *Scheduler) ReconcileActions(ctx context.Context) error {
	return s.reconcileActions(ctx, scheduleReconcilePassTimeout)
}

func (s *Scheduler) reconcileActions(ctx context.Context, budget time.Duration) error {
	if budget <= 0 {
		return fmt.Errorf("scheduler: reconcile 全局启动预算无效: %s", budget)
	}
	passCtx, cancelPass := context.WithTimeout(ctx, budget)
	defer cancelPass()

	const tenantPageSize = 100
	var updated, skipped, failed, total, processed int
	var reconcileErrors []error
	var afterTenantID int64
	for {
		tenantIDs, err := s.st.ListRecoveryTenantCatalogPage(
			passCtx, afterTenantID, tenantPageSize)
		if err != nil {
			return errors.Join(append(reconcileErrors, err)...)
		}
		for _, tenantID := range tenantIDs {
			schedules, listErr := s.st.ListActiveSchedules(passCtx, tenantID)
			if listErr != nil {
				reconcileErrors = append(reconcileErrors, fmt.Errorf(
					"list active schedules for tenant %d: %w", tenantID, listErr))
				continue
			}
			total += len(schedules)
			for _, sc := range schedules {
				if err := passCtx.Err(); err != nil {
					reconcileErrors = append(reconcileErrors, fmt.Errorf(
						"scheduler: reconcile 全局启动预算耗尽（processed=%d）: %w",
						processed, err))
					goto done
				}
				processed++
				didUpdate, rerr := s.reconcileOne(passCtx, sc)
				switch {
				case rerr != nil:
					failed++
					reconcileErrors = append(reconcileErrors, fmt.Errorf(
						"reconcile schedule %s: %w", sc.ID, rerr))
					slog.Error("scheduler: reconcile 调度 Action 失败（fail-closed）",
						"schedule_id", sc.ID, "err", rerr)
				case didUpdate:
					updated++
					slog.Info("scheduler: 已修正存量调度 Action 入参（下次触发生效）",
						"schedule_id", sc.ID)
				default:
					skipped++
				}
			}
		}
		if len(tenantIDs) < tenantPageSize {
			break
		}
		afterTenantID = tenantIDs[len(tenantIDs)-1]
	}
done:
	slog.Info("scheduler: 存量调度 Action reconcile 完成",
		"total", total, "updated", updated, "skipped", skipped, "failed", failed)
	return errors.Join(reconcileErrors...)
}

// reconcileOne 把单个调度的 Action.Args 自愈到期望 params。
// 返回 didUpdate=true 表示确实写了 Temporal。
func (s *Scheduler) reconcileOne(ctx context.Context, listed types.Schedule) (updated bool, err error) {
	attemptCtx, cancelAttempt := context.WithTimeout(ctx, scheduleReconcileAttemptTimeout)
	defer cancelAttempt()

	releaseProcessGate, err := s.acquireTaskScheduleGate(
		attemptCtx, "reconcile_schedule_action", listed.ID,
	)
	if err != nil {
		return false, err
	}
	defer releaseProcessGate()

	// ListActiveSchedules is only a discovery snapshot. Authorization is
	// deliberately repeated under a cross-process PostgreSQL advisory lock;
	// QuiesceTaskDefinitionEdit takes the same lock before installing its
	// marker. A nil schedule means the task is no longer active or an edit now
	// owns it, both normal skip outcomes.
	sc, releaseDatabaseGate, err := s.st.AcquireScheduleReconcile(
		attemptCtx, listed.TenantID, listed.ID)
	if err != nil {
		return false, err
	}
	if releaseDatabaseGate != nil {
		defer func() {
			releaseCtx, cancelRelease := context.WithTimeout(
				context.WithoutCancel(ctx), scheduleReconcileReleaseTimeout,
			)
			defer cancelRelease()
			if releaseErr := releaseDatabaseGate(releaseCtx); releaseErr != nil {
				err = errors.Join(err, fmt.Errorf(
					"释放调度 %s 的 reconcile 数据库 gate: %w",
					listed.ID, releaseErr,
				))
			}
		}()
	}
	if sc == nil {
		return false, nil
	}

	var scope workflow.PushScope
	if len(sc.ScopeJSON) > 0 {
		if err := json.Unmarshal(sc.ScopeJSON, &scope); err != nil {
			return false, fmt.Errorf("解析 scope_json（id=%s）: %w", sc.ID, err)
		}
	}
	want := makePushParams(sc.TenantID, sc.UserID, sc.ID, scope, sc.NLDescription)
	// The database mirror is the current control-plane routing truth. Reconcile
	// repairs frozen Action fields; it must never silently downgrade a dynamic
	// task to compiled merely because makePushParams is also used by legacy
	// compiled creation paths.
	want.ExecutionMode = sc.ExecutionMode
	want = s.actionParamsFor(want)

	h := s.c.ScheduleClient().GetHandle(attemptCtx, sc.ID)
	desc, err := h.Describe(attemptCtx)
	if err != nil {
		return false, err
	}
	formalV3, isFormalV3, err := decodeResearchScheduledActionV3(
		desc.Schedule.Action)
	if err != nil {
		return false, err
	}
	if isFormalV3 {
		if formalV3.TenantID != sc.TenantID ||
			formalV3.UserID != sc.UserID || formalV3.TaskID != sc.ID {
			return false, types.NewAppError(types.CodeConflict,
				"research V3 Schedule Action identity does not match the task scope", types.ErrConflict)
		}
		verifier, ok := s.st.(researchV3ActionAuthorityStore)
		if !ok {
			return false, types.NewAppError(types.CodeInternal,
				"research V3 Action authority verifier is unavailable", types.ErrInternal)
		}
		if err := verifier.VerifyEnabledResearchV3ActionAuthorization(
			ctx, sc.TenantID, sc.UserID, sc.ID,
			formalV3.ActionAuthorizationToken,
		); err != nil {
			return false, err
		}
		// Only the cutover/rollback saga may replace this envelope. In
		// particular, startup reconciliation must preserve its capability token.
		return false, nil
	}
	current, found, err := decodeScheduleActionPushParams(
		desc.Schedule.Action)
	if err != nil {
		return false, err
	}
	if found &&
		workflow.IsCompiledToolRuntimeV2(current.RuntimeVersion) &&
		!workflow.IsCompiledToolRuntimeV2(want.RuntimeVersion) {
		return false, fmt.Errorf(
			"Tool runtime task %s must be paused before canary removal",
			sc.ID)
	}
	if workflow.IsCompiledToolRuntimeV2(want.RuntimeVersion) {
		if err := requireToolRuntimeDefinition(
			attemptCtx, s.st,
			sc.TenantID, sc.UserID, sc.ID); err != nil {
			return false, err
		}
	}
	matches, err := actionMatchesParams(desc.Schedule.Action, want)
	if err != nil {
		return false, err
	}
	if matches {
		return false, nil // Action 的可漂移入参已与期望一致，无需重写
	}

	// 只替换 Action.Args，其余（Workflow 类型名、ID、TaskQueue、超时、Spec、Overlap、State）
	// 一律原样带走——照抄 UpdatePush 的值拷贝纪律：自己 new 一个 Action 会静默丢掉这些字段。
	err = h.Update(attemptCtx, client.ScheduleUpdateOptions{
		DoUpdate: func(in client.ScheduleUpdateInput) (*client.ScheduleUpdate, error) {
			sch := in.Description.Schedule
			wf, ok := sch.Action.(*client.ScheduleWorkflowAction)
			if !ok {
				return nil, fmt.Errorf("调度 %s 的 Action 非 workflow 类型，无法 reconcile", sc.ID)
			}
			na := *wf                     // 值拷贝 Action 结构体
			na.Args = []interface{}{want} // 只换入参
			sch.Action = &na
			return &client.ScheduleUpdate{Schedule: &sch}, nil
		},
	})
	if err != nil {
		return false, err
	}
	return true, nil
}

// actionMatchesParams 判断一个 Schedule Action 的入参是否已与期望 params 一致。
// Describe/Update 返回的 Action.Args 是 *commonpb.Payload（未解码原始态，见 SDK 注释），
// 故解出首个入参为 PushParams 再逐字段比对。非 workflow-action / 空入参 / 首参非 Payload
// 一律视为"不一致"（返回 false）——让 reconcile 走重建分支自愈，而非报错卡住。
//
// 只比 TenantID / RunKind / ScheduleID / NLDesc，不比整份 params：TenantID 从
// 数据库镜像恢复任务归属；RunKind 补齐旧 Action 的未知零值；ScheduleID 激活
// 任务级隔离；NLDesc 自愈改名后的聚合卡标题漂移。Scope 目前建后不可改，
// 纳入比较只会因 nil/空切片等价问题诱发无谓重写；将来 Scope 可改时再纳入。
// Snapshot 不属于漂移自愈字段：A5 构造器确保持久 Action 始终为 nil，每轮引用只能
// 由 PrepareRun 在 workflow 内存中产生；异常非 nil 入参会在 PrepareRun 中 fail-closed。
func actionMatchesParams(action client.ScheduleAction, want workflow.PushParams) (bool, error) {
	got, found, err := decodeScheduleActionPushParams(action)
	if err != nil || !found {
		return false, err
	}
	return got.TenantID == want.TenantID && got.RunKind == want.RunKind &&
		got.ExecutionMode == want.ExecutionMode &&
		got.RuntimeVersion == want.RuntimeVersion &&
		got.ScheduleID == want.ScheduleID &&
		got.NLDesc == want.NLDesc, nil
}

func decodeScheduleActionPushParams(
	action client.ScheduleAction,
) (workflow.PushParams, bool, error) {
	wf, ok := action.(*client.ScheduleWorkflowAction)
	if !ok || len(wf.Args) == 0 {
		return workflow.PushParams{}, false, nil
	}
	switch first := wf.Args[0].(type) {
	case workflow.PushParams:
		return first, true, nil
	case *commonpb.Payload:
		var got workflow.PushParams
		if err := converter.GetDefaultDataConverter().FromPayload(
			first, &got); err != nil {
			return workflow.PushParams{}, false,
				fmt.Errorf("解码调度 Action 入参: %w", err)
		}
		return got, true, nil
	default:
		return workflow.PushParams{}, false, nil
	}
}

func (s *Scheduler) decodeRawScheduleActionPushParams(
	response *workflowservice.DescribeScheduleResponse,
	taskID string,
) (workflow.PushParams, bool, error) {
	var params workflow.PushParams
	found, err := s.decodeRawScheduleActionInput(
		response, taskID, &params,
	)
	return params, found, err
}

func (s *Scheduler) decodeRawScheduleActionResearchV3Input(
	response *workflowservice.DescribeScheduleResponse,
	taskID string,
) (workflow.ResearchScheduledInputV3, bool, error) {
	var input workflow.ResearchScheduledInputV3
	found, err := s.decodeRawScheduleActionInput(
		response, taskID, &input,
	)
	return input, found, err
}

// decodeRawScheduleActionInput decodes the one immutable argument from a
// persisted Schedule Action with the converter identity sealed in its memo.
// It deliberately accepts an output pointer instead of selecting a workflow
// protocol: durable manual runs must preserve both the frozen PushParams wire
// and the independent ResearchScheduledInputV3 cutover wire.
func (s *Scheduler) decodeRawScheduleActionInput(
	response *workflowservice.DescribeScheduleResponse,
	taskID string,
	output any,
) (bool, error) {
	start := response.GetSchedule().GetAction().GetStartWorkflow()
	if start == nil || start.GetInput() == nil ||
		len(start.GetInput().GetPayloads()) != 1 {
		return false, nil
	}
	s.taskScheduleEnv.mu.Lock()
	namespace := s.taskScheduleEnv.namespace
	candidates := []converter.DataConverter{
		converter.GetDefaultDataConverter(),
		s.taskScheduleEnv.dc,
	}
	for _, dc := range s.taskScheduleEnv.decoders {
		candidates = append(candidates, dc)
	}
	s.taskScheduleEnv.mu.Unlock()
	decode := func(dc converter.DataConverter) error {
		if dc == nil {
			return errors.New("task schedule decoder is unavailable")
		}
		prepared := PreparedTaskSchedule{
			Namespace: namespace,
			Action: PreparedTaskScheduleAction{
				ActionID: start.GetWorkflowId(),
			},
		}
		actionDC, err := taskScheduleActionDataConverter(prepared, dc)
		if err != nil {
			return err
		}
		if err := actionDC.FromPayloads(
			start.GetInput(), output); err != nil {
			return err
		}
		return nil
	}

	memoPayload := start.GetMemo().GetFields()[taskScheduleMemoKey]
	if memoPayload == nil {
		err := decode(converter.GetDefaultDataConverter())
		if err != nil {
			return false,
				fmt.Errorf("解码旧调度 Action 入参: %w", err)
		}
		return true, nil
	}

	for _, candidate := range candidates {
		if candidate == nil {
			continue
		}
		prepared := PreparedTaskSchedule{
			Namespace: namespace,
			Action: PreparedTaskScheduleAction{
				ActionID: start.GetWorkflowId(),
			},
		}
		actionDC, err := taskScheduleActionDataConverter(
			prepared, candidate)
		if err != nil {
			continue
		}
		var fingerprint taskScheduleFingerprint
		if err := actionDC.FromPayload(
			memoPayload, &fingerprint); err != nil ||
			fingerprint.TaskID != taskID ||
			fingerprint.ConverterID == "" {
			continue
		}
		err = decode(s.taskScheduleDecoder(fingerprint.ConverterID))
		if err != nil {
			return false,
				fmt.Errorf("解码版本化调度 Action 入参: %w", err)
		}
		return true, nil
	}
	return false,
		errors.New("恢复任务无法解析版本化 Temporal Action")
}

func (s *Scheduler) authorizeToolRuntimeResume(
	ctx context.Context,
	sc *types.Schedule,
) error {
	if sc == nil {
		return types.NewAppError(
			types.CodeConflict,
			"恢复任务缺少调度定义", types.ErrConflict)
	}
	description, err := s.c.ScheduleClient().GetHandle(
		ctx, sc.ID).Describe(ctx)
	if err != nil {
		return err
	}
	if description == nil {
		return types.NewAppError(
			types.CodeConflict,
			"恢复任务无法验证 Temporal Action",
			types.ErrConflict)
	}
	params, found, err := decodeScheduleActionPushParams(
		description.Schedule.Action)
	if err != nil {
		return err
	}
	return s.validateToolRuntimeResume(
		ctx, sc, found, params)
}

func (s *Scheduler) validateToolRuntimeResume(
	ctx context.Context,
	sc *types.Schedule,
	actionFound bool,
	params workflow.PushParams,
) error {
	capabilities, ok := s.st.(toolRuntimeCapabilityStore)
	if !ok {
		return types.NewAppError(
			types.CodeConflict,
			"恢复任务无法验证 Tool runtime 定义",
			types.ErrConflict)
	}
	isToolTask, err := capabilities.HasCurrentToolApprovedDefinition(
		ctx, sc.TenantID, sc.UserID, sc.ID)
	if err != nil {
		return err
	}
	actionIsTool := actionFound &&
		workflow.IsCompiledToolRuntimeV2(params.RuntimeVersion)
	if !actionFound || actionIsTool != isToolTask {
		return types.NewAppError(
			types.CodeConflict,
			"恢复任务的 Action 与 Tool runtime 定义不一致",
			types.ErrConflict)
	}
	if !actionIsTool {
		return nil
	}
	if params.TenantID != sc.TenantID ||
		params.UserID != sc.UserID ||
		params.ScheduleID != sc.ID ||
		params.RunKind != workflow.PushRunKindScheduled ||
		params.ExecutionMode != types.ExecutionModeCompiled ||
		params.Snapshot != nil {
		return types.NewAppError(
			types.CodeConflict,
			"恢复任务的 Tool runtime Action 身份与任务不一致",
			types.ErrConflict)
	}
	if !workflow.IsCompiledToolRuntimeV2(
		s.runtimeVersionFor(sc.ID, sc.ExecutionMode),
	) {
		return types.NewAppError(
			types.CodeConflict,
			"Tool runtime canary 已关闭，任务保持暂停",
			types.ErrConflict)
	}
	return nil
}

func (s *Scheduler) authorizeToolRuntimeResumeRemote(
	ctx context.Context,
	sc *types.Schedule,
) error {
	namespace, _, _ := s.taskScheduleEnvironment()
	response, err := s.describeTaskSchedule(
		ctx,
		taskScheduleExpected{
			taskID: sc.ID,
			prepared: PreparedTaskSchedule{
				Namespace: namespace,
			},
		},
	)
	if err != nil {
		return err
	}
	params, found, err := s.decodeRawScheduleActionPushParams(
		response, sc.ID)
	if err != nil {
		return types.NewAppError(
			types.CodeConflict,
			"恢复任务无法验证版本化 Temporal Action",
			errors.Join(types.ErrConflict, err),
		)
	}
	return s.validateToolRuntimeResume(
		ctx, sc, found, params)
}

// TriggerScheduleNow retains the pre-6.8 internal signature. HTTP callers use
// TriggerScheduleNowIdempotent so a retry carries the same client key.
func (s *Scheduler) TriggerScheduleNow(
	ctx context.Context,
	schedID string,
	userID int64,
) error {
	if _, ok := s.st.(scheduleCommandStore); !ok {
		return s.triggerScheduleNowLegacy(ctx, schedID, userID)
	}
	return s.TriggerScheduleNowIdempotent(
		ctx, schedID, userID, legacyScheduleCommandKey(),
	)
}

// TriggerScheduleNowIdempotent starts the exact stored Action for one owned
// active or paused task without changing its recurring schedule state.
// A durable command ID becomes the one-off workflow ID, so response loss and
// recovery cannot start a second execution or mutate the recurring Schedule.
func (s *Scheduler) TriggerScheduleNowIdempotent(
	ctx context.Context,
	schedID string,
	userID int64,
	idempotencyKey string,
) error {
	return s.executeNewScheduleCommand(
		ctx, schedID, userID, idempotencyKey, types.ScheduleCommandRun,
	)
}

// PausePush retains the pre-6.8 internal signature.
func (s *Scheduler) PausePush(
	ctx context.Context,
	schedID string,
	userID int64,
) error {
	if _, ok := s.st.(scheduleCommandStore); !ok {
		return s.changePushPausedLegacy(ctx, schedID, userID, true)
	}
	return s.PausePushIdempotent(
		ctx, schedID, userID, legacyScheduleCommandKey(),
	)
}

func (s *Scheduler) PausePushIdempotent(
	ctx context.Context,
	schedID string,
	userID int64,
	idempotencyKey string,
) error {
	return s.executeNewScheduleCommand(
		ctx, schedID, userID, idempotencyKey, types.ScheduleCommandPause,
	)
}

// ResumePush retains the pre-6.8 internal signature.
func (s *Scheduler) ResumePush(
	ctx context.Context,
	schedID string,
	userID int64,
) error {
	if _, ok := s.st.(scheduleCommandStore); !ok {
		return s.changePushPausedLegacy(ctx, schedID, userID, false)
	}
	return s.ResumePushIdempotent(
		ctx, schedID, userID, legacyScheduleCommandKey(),
	)
}

func (s *Scheduler) ResumePushIdempotent(
	ctx context.Context,
	schedID string,
	userID int64,
	idempotencyKey string,
) error {
	return s.executeNewScheduleCommand(
		ctx, schedID, userID, idempotencyKey, types.ScheduleCommandResume,
	)
}

func legacyScheduleCommandKey() string {
	return "legacy-" + uuid.NewString()
}

func (s *Scheduler) triggerScheduleNowLegacy(
	ctx context.Context,
	schedID string,
	userID int64,
) error {
	sc, err := s.st.GetSchedule(ctx, schedID, userID)
	if err != nil {
		return err
	}
	if sc.Status != types.ScheduleStatusActive &&
		sc.Status != types.ScheduleStatusPaused {
		return types.NewAppError(
			types.CodeConflict,
			"任务当前状态不支持立即运行，请刷新后重试。",
			types.ErrConflict,
		)
	}
	if err := s.c.ScheduleClient().GetHandle(
		ctx, schedID,
	).Trigger(ctx, client.ScheduleTriggerOptions{}); err != nil {
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			return scheduleCommandNotFound(schedID, err)
		}
		return types.NewAppError(
			types.CodeInternal, "触发任务立即运行失败", err,
		)
	}
	return nil
}

func (s *Scheduler) changePushPausedLegacy(
	ctx context.Context,
	schedID string,
	userID int64,
	paused bool,
) error {
	sc, err := s.st.GetSchedule(ctx, schedID, userID)
	if err != nil {
		return err
	}
	from, to := types.ScheduleStatusActive, types.ScheduleStatusPaused
	if !paused {
		from, to = to, from
	}
	if sc.Status == to {
		return nil
	}
	if sc.Status != from {
		return types.NewAppError(
			types.CodeConflict,
			"任务当前状态不支持这项操作，请刷新后重试。",
			types.ErrConflict,
		)
	}
	if !paused {
		if err := s.authorizeToolRuntimeResume(ctx, sc); err != nil {
			return err
		}
	}
	statusStore, ok := s.st.(scheduleStatusStore)
	if !ok {
		return types.NewAppError(
			types.CodeInternal, "任务暂停/恢复控制面未配置", nil,
		)
	}
	commit, rollback, err := statusStore.BeginScheduleStatusChange(
		ctx, schedID, userID, from, to,
	)
	if err != nil {
		return err
	}
	defer func() {
		if rollbackErr := rollback(ctx); rollbackErr != nil {
			slog.Error(
				"scheduler: release legacy task status transaction",
				"schedule_id", schedID, "err", rollbackErr,
			)
		}
	}()
	handle := s.c.ScheduleClient().GetHandle(ctx, schedID)
	if err := mutateSchedulePaused(ctx, handle, paused); err != nil {
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			return scheduleCommandNotFound(schedID, err)
		}
		return types.NewAppError(
			types.CodeInternal, "更新任务暂停状态失败", err,
		)
	}
	return commit(ctx)
}

func scheduleCommandDigests(
	tenantID, userID int64,
	taskID, key string,
	kind types.ScheduleCommandKind,
) (string, string) {
	payload := sha256.Sum256([]byte(
		"schedule-command/v1\n" + string(kind) + "\n" + taskID,
	))
	payloadDigest := fmt.Sprintf("%x", payload[:])
	request := sha256.Sum256([]byte(fmt.Sprintf(
		"schedule-command-temporal/v1\n%d\n%d\n%s\n%s",
		tenantID, userID, key, payloadDigest,
	)))
	return payloadDigest, fmt.Sprintf("%x", request[:])
}

func (s *Scheduler) executeNewScheduleCommand(
	ctx context.Context,
	schedID string,
	userID int64,
	idempotencyKey string,
	kind types.ScheduleCommandKind,
) error {
	if ctx == nil {
		return types.NewAppError(
			types.CodeValidation, "任务命令上下文无效", types.ErrValidation,
		)
	}
	attemptCtx, cancelAttempt := s.newScheduleCommandWorkContext(ctx)
	defer cancelAttempt()
	commandStore, ok := s.st.(scheduleCommandStore)
	if !ok {
		return types.NewAppError(
			types.CodeInternal, "任务命令控制面未配置", nil,
		)
	}
	tenantID, err := s.st.ResolveActiveTenantForUser(attemptCtx, userID)
	if err != nil {
		return err
	}
	payloadDigest, requestID := scheduleCommandDigests(
		tenantID, userID, schedID, idempotencyKey, kind,
	)
	command, err := commandStore.CreateOrLoadScheduleCommand(
		attemptCtx, tenantID, userID, schedID, idempotencyKey, kind,
		payloadDigest, requestID,
	)
	if err != nil {
		return err
	}
	switch command.Status {
	case types.ScheduleCommandCompleted:
		return nil
	case types.ScheduleCommandBlocked:
		return scheduleCommandBlockedError(command)
	case types.ScheduleCommandPending:
		return s.runScheduleCommandAttempt(attemptCtx, command)
	default:
		return types.NewAppError(
			types.CodeInternal, "任务命令耐久状态损坏", nil,
		)
	}
}

func (s *Scheduler) runScheduleCommandAttempt(
	ctx context.Context,
	expected *types.ScheduleCommand,
) error {
	if expected == nil {
		return types.NewAppError(
			types.CodeValidation, "任务命令不得为空", types.ErrValidation,
		)
	}
	commandStore, ok := s.st.(scheduleCommandStore)
	if !ok {
		return types.NewAppError(
			types.CodeInternal, "任务命令控制面未配置", nil,
		)
	}
	releaseMemory, err := s.acquireTaskScheduleGate(
		ctx, "schedule_command", expected.TaskID,
	)
	if err != nil {
		return err
	}
	defer releaseMemory()
	command, schedule, complete, block, rollback, err :=
		commandStore.BeginScheduleCommandAttempt(
			ctx, expected.TenantID, expected.UserID, expected.IdempotencyKey,
		)
	if err != nil {
		return err
	}
	defer func() {
		if rollback != nil {
			if rollbackErr := rollback(ctx); rollbackErr != nil {
				slog.Error(
					"scheduler: release schedule command transaction",
					"schedule_id", expected.TaskID,
					"err", rollbackErr,
				)
			}
		}
	}()
	if command.Status == types.ScheduleCommandCompleted {
		return nil
	}
	if command.Status == types.ScheduleCommandBlocked {
		return scheduleCommandBlockedError(command)
	}
	if command.ID != expected.ID ||
		command.PayloadDigest != expected.PayloadDigest ||
		command.RemoteRequestID != expected.RemoteRequestID ||
		command.TaskID != expected.TaskID || command.Kind != expected.Kind {
		return types.NewAppError(
			types.CodeConflict,
			"任务命令恢复身份不一致",
			types.ErrConflict,
		)
	}
	if command.Kind == types.ScheduleCommandResume {
		if schedule == nil {
			return types.NewAppError(
				types.CodeConflict,
				"恢复任务缺少已锁定的调度定义",
				types.ErrConflict)
		}
		if err := s.authorizeToolRuntimeResumeRemote(
			ctx, schedule); err != nil {
			if isTaskScheduleNotFound(err) {
				finishCtx, cancelFinish := scheduleCommandDetachedContext(
					ctx, scheduleCommandFactReadbackTimeout)
				blockErr := block(
					finishCtx,
					"temporal_schedule_not_found",
					"Temporal 中不存在对应任务调度",
				)
				cancelFinish()
				return errors.Join(
					scheduleCommandNotFound(command.TaskID, err),
					blockErr,
				)
			}
			if !errors.Is(err, types.ErrConflict) {
				return err
			}
			finishCtx, cancelFinish := scheduleCommandDetachedContext(
				ctx, scheduleCommandFactReadbackTimeout)
			blockErr := block(
				finishCtx,
				"tool_runtime_canary_disabled",
				"Tool runtime canary 已关闭，任务保持暂停",
			)
			cancelFinish()
			return errors.Join(err, blockErr)
		}
	}

	remoteErr := s.applyScheduleCommandRemote(ctx, command)
	if remoteErr != nil {
		if isTaskScheduleNotFound(remoteErr) {
			finishCtx, cancelFinish := scheduleCommandDetachedContext(
				ctx,
				scheduleCommandFactReadbackTimeout,
			)
			blockErr := block(
				finishCtx,
				"temporal_schedule_not_found",
				"Temporal 中不存在对应任务调度",
			)
			cancelFinish()
			if blockErr != nil {
				return errors.Join(
					scheduleCommandNotFound(command.TaskID, remoteErr),
					blockErr,
				)
			}
			return scheduleCommandNotFound(command.TaskID, remoteErr)
		}
		ae := types.NewAppError(
			types.CodeInternal,
			"任务操作结果暂未确认，系统将自动恢复，请稍后重试。",
			remoteErr,
		)
		ae.Retryable = true
		return ae
	}
	if err := complete(ctx); err == nil {
		return nil
	} else {
		commitErr := err
		readCtx, cancel := scheduleCommandDetachedContext(
			ctx, 3*time.Second,
		)
		current, readErr := commandStore.LoadScheduleCommand(
			readCtx, command.TenantID, command.UserID,
			command.IdempotencyKey,
		)
		cancel()
		if readErr == nil && current.ID == command.ID &&
			current.Status == types.ScheduleCommandCompleted {
			return nil
		}
		return errors.Join(commitErr, readErr)
	}
}

func scheduleCommandNotFound(taskID string, cause error) error {
	return types.NewAppError(
		types.CodeNotFound,
		fmt.Sprintf("定时任务 %s 不存在", taskID),
		cause,
	)
}

func scheduleCommandBlockedError(command *types.ScheduleCommand) error {
	if command != nil &&
		command.ErrorCode == "temporal_schedule_not_found" {
		return scheduleCommandNotFound(command.TaskID, types.ErrNotFound)
	}
	return types.NewAppError(
		types.CodeConflict,
		"任务操作已被安全阻断，请刷新后重试。",
		types.ErrConflict,
	)
}

func (s *Scheduler) applyScheduleCommandRemote(
	ctx context.Context,
	command *types.ScheduleCommand,
) error {
	namespace := s.taskScheduleEnv.namespace
	switch command.Kind {
	case types.ScheduleCommandRun:
		description, err := s.describeTaskSchedule(
			ctx,
			taskScheduleExpected{
				taskID: command.TaskID,
				prepared: PreparedTaskSchedule{
					Namespace: namespace,
				},
			},
		)
		if err != nil {
			return err
		}
		start := description.GetSchedule().GetAction().GetStartWorkflow()
		if start != nil && start.GetWorkflowType().GetName() ==
			workflow.ResearchScheduledWorkflowV3Name {
			input, found, err := s.decodeRawScheduleActionResearchV3Input(
				description, command.TaskID,
			)
			if err != nil {
				return err
			}
			if !found ||
				input.TenantID != command.TenantID ||
				input.UserID != command.UserID || input.TaskID != command.TaskID ||
				validateResearchScheduledInputV3(input) != nil ||
				strings.TrimSpace(start.GetTaskQueue().GetName()) == "" {
				return types.NewAppError(
					types.CodeConflict,
					"立即运行的 Research V3 执行定义与耐久命令不一致",
					types.ErrConflict,
				)
			}
			verifier, ok := s.st.(researchV3ActionAuthorityStore)
			if !ok {
				return types.NewAppError(
					types.CodeInternal,
					"Research V3 立即运行授权校验器未配置",
					types.ErrInternal,
				)
			}
			if err := verifier.VerifyEnabledResearchV3ActionAuthorization(
				ctx, command.TenantID, command.UserID, command.TaskID,
				input.ActionAuthorizationToken,
			); err != nil {
				return err
			}
			_, err = s.c.ExecuteWorkflow(
				ctx,
				client.StartWorkflowOptions{
					ID: manualTaskWorkflowID(
						command.ID, command.CreatedAt,
					),
					TaskQueue: start.GetTaskQueue().GetName(),
					WorkflowIDReusePolicy: enums.
						WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
					WorkflowExecutionErrorWhenAlreadyStarted: true,
				},
				workflow.ResearchScheduledWorkflowV3,
				input,
			)
			var alreadyStarted *serviceerror.WorkflowExecutionAlreadyStarted
			if errors.As(err, &alreadyStarted) {
				return nil
			}
			return err
		}
		params, found, err := s.decodeRawScheduleActionPushParams(
			description, command.TaskID,
		)
		if err != nil {
			return err
		}
		if !found ||
			params.UserID != command.UserID ||
			params.ScheduleID != command.TaskID ||
			params.RunKind != workflow.PushRunKindScheduled ||
			params.Snapshot != nil ||
			(params.TenantID != 0 && params.TenantID != command.TenantID) {
			return types.NewAppError(
				types.CodeConflict,
				"立即运行的任务执行定义与耐久命令不一致",
				types.ErrConflict,
			)
		}
		_, err = s.c.ExecuteWorkflow(
			ctx,
			client.StartWorkflowOptions{
				ID: manualTaskWorkflowID(
					command.ID, command.CreatedAt,
				),
				TaskQueue: s.tq,
				WorkflowIDReusePolicy: enums.
					WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
				// A response-lost retry must surface AlreadyStarted so this
				// command can adopt the exact existing execution instead of
				// silently attaching to some unrelated caller state.
				WorkflowExecutionErrorWhenAlreadyStarted: true,
			},
			workflow.PushPipelineWorkflow,
			params,
		)
		var alreadyStarted *serviceerror.WorkflowExecutionAlreadyStarted
		if errors.As(err, &alreadyStarted) {
			return nil
		}
		return err
	case types.ScheduleCommandPause, types.ScheduleCommandResume:
		wantPaused := command.Kind == types.ScheduleCommandPause
		applied, err := s.schedulePausedFact(ctx, namespace, command.TaskID)
		if err == nil && applied == wantPaused {
			return nil
		}
		if err != nil {
			return err
		}
		patch := &schedulepb.SchedulePatch{}
		note := "vane-web:" + string(command.Kind) + ":" +
			shortScheduleCommandID(command.ID)
		if wantPaused {
			patch.Pause = note
		} else {
			patch.Unpause = note
		}
		_, patchErr := s.c.WorkflowService().PatchSchedule(
			ctx, &workflowservice.PatchScheduleRequest{
				Namespace: namespace, ScheduleId: command.TaskID,
				Identity:  taskScheduleIdentity(taskScheduleFingerprintVersion),
				RequestId: command.RemoteRequestID, Patch: patch,
			},
		)
		if patchErr == nil {
			return nil
		}
		readCtx, cancelRead := scheduleCommandDetachedContext(
			ctx,
			scheduleCommandFactReadbackTimeout,
		)
		applied, describeErr := s.schedulePausedFact(
			readCtx, namespace, command.TaskID,
		)
		cancelRead()
		if describeErr == nil && applied == wantPaused {
			return nil
		}
		return errors.Join(patchErr, describeErr)
	case types.ScheduleCommandDelete:
		_, deleteErr := s.c.WorkflowService().DeleteSchedule(
			ctx, &workflowservice.DeleteScheduleRequest{
				Namespace: namespace, ScheduleId: command.TaskID,
				Identity: taskScheduleIdentity(taskScheduleFingerprintVersion),
			},
		)
		if deleteErr == nil || isTaskScheduleNotFound(deleteErr) {
			return nil
		}
		readCtx, cancelRead := scheduleCommandDetachedContext(
			ctx,
			scheduleCommandFactReadbackTimeout,
		)
		_, describeErr := s.c.WorkflowService().DescribeSchedule(
			readCtx,
			&workflowservice.DescribeScheduleRequest{
				Namespace: namespace, ScheduleId: command.TaskID,
			},
		)
		cancelRead()
		if isTaskScheduleNotFound(describeErr) {
			return nil
		}
		return errors.Join(deleteErr, describeErr)
	default:
		return types.NewAppError(
			types.CodeValidation, "未知任务命令", types.ErrValidation,
		)
	}
}

// manualTaskWorkflowID binds one explicit run command to exactly one Temporal
// execution. It deliberately does not reuse or patch the recurring Schedule:
// manual execution and recurring cadence are independent product operations.
func manualTaskWorkflowID(commandID string, createdAt time.Time) string {
	const timestampLayout = "2006-01-02T15:04:05Z"
	return types.ManualTaskWorkflowPrefix + commandID + "-" +
		createdAt.UTC().Truncate(time.Second).Format(timestampLayout)
}

func scheduleCommandDetachedContext(
	parent context.Context,
	maximum time.Duration,
) (context.Context, context.CancelFunc) {
	detached := context.WithoutCancel(parent)
	deadline := time.Now().Add(maximum)
	if parentDeadline, ok := parent.Deadline(); ok &&
		parentDeadline.Before(deadline) {
		deadline = parentDeadline
	}
	return context.WithDeadline(detached, deadline)
}

func shortScheduleCommandID(id string) string {
	if len(id) <= 8 {
		return id
	}
	return id[:8]
}

func (s *Scheduler) schedulePausedFact(
	ctx context.Context,
	namespace, taskID string,
) (bool, error) {
	response, err := s.c.WorkflowService().DescribeSchedule(
		ctx, &workflowservice.DescribeScheduleRequest{
			Namespace: namespace, ScheduleId: taskID,
		},
	)
	if err != nil {
		return false, err
	}
	if response.GetSchedule() == nil ||
		response.GetSchedule().GetState() == nil {
		return false, errors.New("Temporal schedule state is missing")
	}
	return response.GetSchedule().GetState().GetPaused(), nil
}

func mutateSchedulePaused(
	ctx context.Context,
	handle client.ScheduleHandle,
	paused bool,
) error {
	if paused {
		return handle.Pause(ctx, client.SchedulePauseOptions{
			Note: "Paused from Vane Web",
		})
	}
	return handle.Unpause(ctx, client.ScheduleUnpauseOptions{
		Note: "Resumed from Vane Web",
	})
}

// NextRun returns Temporal's next planned action for one owned active task.
func (s *Scheduler) NextRun(
	ctx context.Context,
	schedID string,
	userID int64,
) (*time.Time, error) {
	sc, err := s.st.GetSchedule(ctx, schedID, userID)
	if err != nil {
		return nil, err
	}
	if sc.Status != types.ScheduleStatusActive {
		return nil, nil
	}
	description, err := s.c.ScheduleClient().GetHandle(
		ctx, schedID,
	).Describe(ctx)
	if err != nil {
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			return nil, types.NewAppError(
				types.CodeNotFound,
				fmt.Sprintf("定时任务 %s 不存在", schedID),
				err,
			)
		}
		return nil, types.NewAppError(
			types.CodeInternal, "读取任务下次运行时间失败", err,
		)
	}
	if len(description.Info.NextActionTimes) == 0 {
		return nil, nil
	}
	next := description.Info.NextActionTimes[0]
	return &next, nil
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
// 例外（#3 任务名漂移）：连带改名（nlDesc != nil）时，**同一次原子 Update** 里把
// Action.Args 的任务名一并回写——NLDesc 冻结在建调度时的 Action 入参里，只换 Spec
// 会让聚合卡 header 永久显示旧任务名。入参整体按 makePushParams 重建（与 CreatePush/
// ReconcileActions 同一构造器，scope 取镜像当前值），顺带给 b1 之前缺 schedule_id 的
// 老调度在改名那一刻即时补齐，不必等下次重启 reconcile（reconcile 是改名之外的兜底）。
// 正式 Research V3 Action 是 capability envelope，不能用 PushParams 重建；其改名请求
// 必须 fail-closed 并交给 V3 definition editor，频率-only 更新仍原样保留 envelope。
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
	// 镜像行还要在改名时提供当前 scope（重建 Action 入参用）。
	sc, err := s.st.GetSchedule(ctx, schedID, userID)
	if err != nil {
		return err
	}
	sdkSpec, err := translateSpec(spec)
	if err != nil {
		return err
	}

	// 连带改名时构造回写用的新 Action 入参（见函数注释「#3 任务名漂移」）。
	var wantArgs []interface{}
	if nlDesc != nil {
		var scope workflow.PushScope
		if len(sc.ScopeJSON) > 0 {
			if err := json.Unmarshal(sc.ScopeJSON, &scope); err != nil {
				return types.NewAppError(types.CodeInternal, "解析调度 scope 失败", err)
			}
		}
		params := makePushParams(sc.TenantID, userID, schedID, scope, *nlDesc)
		if sc.ExecutionMode != "" {
			params.ExecutionMode = sc.ExecutionMode
		}
		params = s.actionParamsFor(params)
		if workflow.IsCompiledToolRuntimeV2(params.RuntimeVersion) {
			if err := requireToolRuntimeDefinition(
				ctx, s.st,
				sc.TenantID, userID, schedID); err != nil {
				return err
			}
		}
		wantArgs = []interface{}{params}
	}

	h := s.c.ScheduleClient().GetHandle(ctx, schedID)
	// 捕获旧 Spec（改名时连旧 Action 入参）供镜像失败时补偿回滚
	//（回调内是唯一能拿到服务端当前值的地方）。
	var oldSpec *client.ScheduleSpec
	var oldArgs []interface{}
	err = h.Update(ctx, client.ScheduleUpdateOptions{
		DoUpdate: func(in client.ScheduleUpdateInput) (*client.ScheduleUpdate, error) {
			sch := in.Description.Schedule // 值拷贝：Action/Policy/State 随之带走
			oldSpec = sch.Spec
			sch.Spec = &sdkSpec
			if wantArgs != nil {
				formalV3, isFormalV3, decodeV3Err := decodeResearchScheduledActionV3(sch.Action)
				if decodeV3Err != nil {
					return nil, decodeV3Err
				}
				if isFormalV3 {
					if formalV3.TenantID != sc.TenantID || formalV3.UserID != userID ||
						formalV3.TaskID != schedID {
						return nil, types.NewAppError(types.CodeConflict,
							"research V3 Schedule Action identity does not match the task scope", types.ErrConflict)
					}
					return nil, types.NewAppError(types.CodeConflict,
						"research V3 task name must be changed through the V3 definition editor", types.ErrConflict)
				}
				wf, ok := sch.Action.(*client.ScheduleWorkflowAction)
				if !ok {
					return nil, fmt.Errorf("调度 %s 的 Action 非 workflow 类型，无法回写任务名", schedID)
				}
				current, found, decodeErr :=
					decodeScheduleActionPushParams(sch.Action)
				if decodeErr != nil {
					return nil, decodeErr
				}
				desired := wantArgs[0].(workflow.PushParams)
				if found &&
					workflow.IsCompiledToolRuntimeV2(
						current.RuntimeVersion) &&
					!workflow.IsCompiledToolRuntimeV2(
						desired.RuntimeVersion) {
					return nil, fmt.Errorf(
						"Tool runtime task %s must be paused before canary removal",
						schedID)
				}
				oldArgs = wf.Args
				na := *wf // 值拷贝 Action 结构体，只换入参，其余字段（ID/TaskQueue）原样
				na.Args = wantArgs
				sch.Action = &na
			}
			return &client.ScheduleUpdate{Schedule: &sch}, nil
		},
	})
	if err != nil {
		var appErr *types.AppError
		if errors.As(err, &appErr) {
			return appErr
		}
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
					if wantArgs != nil {
						// 改名被回滚：Action 入参一并恢复旧任务名，不留
						// 「镜像旧名 / Action 新名」的反向漂移。
						// 守卫用 wantArgs（写入侧标记）而非 oldArgs：旧入参为 nil
						// 时也必须把新入参清回去，否则漂移照旧发生。
						if wf, ok := sch.Action.(*client.ScheduleWorkflowAction); ok {
							na := *wf
							na.Args = oldArgs
							sch.Action = &na
						}
					}
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

// DeletePush retains the pre-6.8 internal signature. Production Store values
// still use the durable command path; narrow unit fakes retain the historical
// behavior so old interface consumers do not need to know about HTTP keys.
func (s *Scheduler) DeletePush(ctx context.Context, schedID string, userID int64) error {
	if _, ok := s.st.(scheduleCommandStore); ok {
		return s.DeletePushIdempotent(
			ctx, schedID, userID, legacyScheduleCommandKey(),
		)
	}
	return s.deletePushLegacy(ctx, schedID, userID)
}

func (s *Scheduler) DeletePushIdempotent(
	ctx context.Context,
	schedID string,
	userID int64,
	idempotencyKey string,
) error {
	return s.executeNewScheduleCommand(
		ctx, schedID, userID, idempotencyKey, types.ScheduleCommandDelete,
	)
}

func (s *Scheduler) deletePushLegacy(
	ctx context.Context,
	schedID string,
	userID int64,
) error {
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
		return err
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
	return taskScheduleIntervalOffsetV1(anchor, everySeconds)
}

// taskScheduleIntervalOffsetV1 is retained by durable task-schedule/v1 and
// definition-edit/v1 compilers. A future timing wire must add a new helper
// instead of changing this modulo contract.
func taskScheduleIntervalOffsetV1(anchor time.Time, everySeconds int) time.Duration {
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
