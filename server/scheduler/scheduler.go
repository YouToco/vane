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
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enums "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"

	"github.com/YouToco/vane/server/types"
	"github.com/YouToco/vane/server/workflow"
)

const (
	// minIntervalSeconds 是触发频率的 1 小时硬地板（规格 B7）：防止用户/前端
	// 配出每分钟触发这类会打爆 LLM 预算与飞书限流的调度。
	minIntervalSeconds = 3600
	// defaultTZ 是 spec 未指定时区时的默认值。
	defaultTZ = "Asia/Shanghai"
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

// scheduleStore 是当前调度命令、启动对账与只读查询使用的最小 Store 边界。
type scheduleStore interface {
	ResolveActiveTenantForUser(ctx context.Context, userID int64) (int64, error)
	ListRecoveryTenantCatalogPage(ctx context.Context, afterTenantID int64, limit int) ([]int64, error)
	ListActiveSchedules(ctx context.Context, tenantID int64) ([]types.Schedule, error)
	GetSchedule(ctx context.Context, id string, userID int64) (*types.Schedule, error)
	AcquireScheduleReconcile(
		ctx context.Context,
		tenantID int64,
		id string,
	) (*types.Schedule, func(context.Context) error, error)
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

type researchV3ActionEvidenceStore interface {
	VerifyEnabledResearchV3ActionEvidence(
		context.Context, int64, int64, string, string, string,
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

// makePushParams constructs the frozen V1/V2 wire retained only by historical
// creation recovery and offline replay compatibility.
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

// ReconcileActions is the startup invariant audit for active schedules. Every
// live Action must be an authorized ResearchScheduledWorkflowV3 envelope.
// Retired Actions are never repaired or rewritten: one such task blocks all
// worker, Agent, HTTP and Feishu ingress until the migration is completed.
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

// reconcileOne verifies one live Action under the exact-task database gate.
// The bool remains in the frozen caller contract but is always false now that
// startup reconciliation is read-only.
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
	if !isFormalV3 {
		return false, types.NewAppError(types.CodeConflict,
			"active task still uses the retired pre-V3 Schedule Action", types.ErrConflict)
	}
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
	// Only the cutover/rollback saga may replace this envelope. Startup
	// reconciliation is now an invariant audit and never rewrites Actions.
	return false, nil
}

// decodeScheduleActionPushParams is retained for immutable legacy decoding in
// replay tests and migration audits; no production path starts this protocol.
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
// protocol: current durable commands decode only ResearchScheduledInputV3.
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

func (s *Scheduler) authorizeResearchV3ResumeRemote(
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
	_, _, err = s.validateAuthorizedResearchV3Schedule(
		ctx, response, sc.TenantID, sc.UserID, sc.ID)
	return err
}

func (s *Scheduler) validateAuthorizedResearchV3Schedule(
	ctx context.Context,
	description *workflowservice.DescribeScheduleResponse,
	tenantID, userID int64,
	taskID string,
) (workflow.ResearchScheduledInputV3, string, error) {
	start := description.GetSchedule().GetAction().GetStartWorkflow()
	if start == nil || start.GetWorkflowType().GetName() !=
		workflow.ResearchScheduledWorkflowV3Name {
		return workflow.ResearchScheduledInputV3{}, "", types.NewAppError(
			types.CodeConflict,
			"task still uses the retired pre-V3 Schedule Action",
			types.ErrConflict,
		)
	}
	input, found, err := s.decodeRawScheduleActionResearchV3Input(
		description, taskID)
	queue := strings.TrimSpace(start.GetTaskQueue().GetName())
	if err != nil || !found || input.TenantID != tenantID ||
		input.UserID != userID || input.TaskID != taskID ||
		validateResearchScheduledInputV3(input) != nil || queue == "" {
		return workflow.ResearchScheduledInputV3{}, "", types.NewAppError(
			types.CodeConflict,
			"Research V3 Schedule Action does not match the durable command",
			errors.Join(types.ErrConflict, err),
		)
	}
	actionBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(
		description.GetSchedule().GetAction())
	if err != nil || len(actionBytes) == 0 {
		return workflow.ResearchScheduledInputV3{}, "", types.NewAppError(
			types.CodeConflict,
			"Research V3 Schedule Action evidence cannot be verified",
			errors.Join(types.ErrConflict, err),
		)
	}
	actionDigest := sha256.Sum256(actionBytes)
	verifier, ok := s.st.(researchV3ActionEvidenceStore)
	if !ok {
		return workflow.ResearchScheduledInputV3{}, "", types.NewAppError(
			types.CodeInternal,
			"Research V3 Action evidence verifier is unavailable",
			types.ErrInternal,
		)
	}
	if err := verifier.VerifyEnabledResearchV3ActionEvidence(
		ctx, tenantID, userID, taskID,
		input.ActionAuthorizationToken,
		fmt.Sprintf("%x", actionDigest[:]),
	); err != nil {
		return workflow.ResearchScheduledInputV3{}, "", err
	}
	return input, queue, nil
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
		if err := s.authorizeResearchV3ResumeRemote(
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
				"research_v3_action_invalid",
				"Research V3 调度授权无效，任务保持暂停",
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
		if command.Kind == types.ScheduleCommandRun &&
			errors.Is(remoteErr, types.ErrConflict) {
			finishCtx, cancelFinish := scheduleCommandDetachedContext(
				ctx, scheduleCommandFactReadbackTimeout)
			blockErr := block(
				finishCtx,
				"research_v3_action_invalid",
				"Research V3 调度授权或执行定义无效",
			)
			cancelFinish()
			return errors.Join(remoteErr, blockErr)
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
		input, taskQueue, err := s.validateAuthorizedResearchV3Schedule(
			ctx, description, command.TenantID, command.UserID, command.TaskID)
		if err != nil {
			return err
		}
		_, err = s.c.ExecuteWorkflow(
			ctx,
			client.StartWorkflowOptions{
				ID: manualTaskWorkflowID(
					command.ID, command.CreatedAt,
				),
				TaskQueue: taskQueue,
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
