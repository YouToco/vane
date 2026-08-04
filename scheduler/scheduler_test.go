package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"testing"
	"time"

	enums "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"

	"github.com/YouToco/vane/types"
	"github.com/YouToco/vane/workflow"
)

// TestValidateSpec 覆盖中立 spec 校验：cron/every 互斥、1 小时硬地板。
func TestValidateSpec(t *testing.T) {
	cases := []struct {
		name    string
		spec    ScheduleSpec
		wantErr bool
	}{
		{"每天8点cron合法", ScheduleSpec{Cron: "0 8 * * *"}, false},
		{"每小时整点cron合法", ScheduleSpec{Cron: "0 * * * *"}, false},
		{"每天一次interval合法", ScheduleSpec{EverySeconds: 86400}, false},
		{"恰好1小时interval合法", ScheduleSpec{EverySeconds: 3600}, false},
		{"两者都给非法", ScheduleSpec{Cron: "0 8 * * *", EverySeconds: 3600}, true},
		{"两者都空非法", ScheduleSpec{}, true},
		{"interval低于硬地板非法", ScheduleSpec{EverySeconds: 1800}, true},
		{"每分钟cron非法", ScheduleSpec{Cron: "* * * * *"}, true},
		{"每30分cron非法", ScheduleSpec{Cron: "*/30 * * * *"}, true},
		{"分钟列表cron非法", ScheduleSpec{Cron: "0,30 * * * *"}, true},
		{"cron段数不足非法", ScheduleSpec{Cron: "0 8 * *"}, true},
		{"分钟越界非法", ScheduleSpec{Cron: "60 8 * * *"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateSpec(tc.spec)
			if tc.wantErr != (err != nil) {
				t.Fatalf("validateSpec(%+v) err=%v，期望 wantErr=%v", tc.spec, err, tc.wantErr)
			}
			// 校验失败必须是 ErrValidation，让 API 层能回 400。
			if tc.wantErr && !errors.Is(err, types.ErrValidation) {
				t.Errorf("校验失败错误应为 ErrValidation，实际 %v", err)
			}
		})
	}
}

// TestTranslateSpec_Cron 校验 cron→CronExpressions 翻译与默认时区。
func TestTranslateSpec_Cron(t *testing.T) {
	got, err := translateSpec(ScheduleSpec{Cron: "0 8 * * *"})
	if err != nil {
		t.Fatalf("translateSpec 出错: %v", err)
	}
	if len(got.CronExpressions) != 1 || got.CronExpressions[0] != "0 8 * * *" {
		t.Errorf("CronExpressions=%v，期望 [0 8 * * *]", got.CronExpressions)
	}
	if len(got.Intervals) != 0 {
		t.Errorf("cron spec 不应产出 Intervals，实际 %v", got.Intervals)
	}
	if got.TimeZoneName != "Asia/Shanghai" {
		t.Errorf("默认时区应为 Asia/Shanghai，实际 %s", got.TimeZoneName)
	}
}

// TestTranslateSpec_Interval 校验 every_seconds→Intervals 翻译与自定义时区。
func TestTranslateSpec_Interval(t *testing.T) {
	got, err := translateSpec(ScheduleSpec{EverySeconds: 7200, TZ: "UTC"})
	if err != nil {
		t.Fatalf("translateSpec 出错: %v", err)
	}
	if len(got.Intervals) != 1 || got.Intervals[0].Every != 2*time.Hour {
		t.Errorf("Intervals=%v，期望 Every=2h", got.Intervals)
	}
	if len(got.CronExpressions) != 0 {
		t.Errorf("interval spec 不应产出 CronExpressions，实际 %v", got.CronExpressions)
	}
	if got.TimeZoneName != "UTC" {
		t.Errorf("自定义时区应为 UTC，实际 %s", got.TimeZoneName)
	}
}

// TestTranslateSpec_RejectsInvalid 确认非法 spec 在翻译阶段也被拦截（独立调用安全）。
func TestTranslateSpec_RejectsInvalid(t *testing.T) {
	if _, err := translateSpec(ScheduleSpec{EverySeconds: 60}); err == nil {
		t.Error("低于硬地板的 interval 应在 translateSpec 被拒")
	}
}

// ============================================================
type fakeTemporalClient struct {
	client.Client
	gotOptions client.StartWorkflowOptions
	gotArgs    []interface{}
	retRun     client.WorkflowRun
	retErr     error
	sched      *fakeScheduleClient
}

func (f *fakeTemporalClient) ScheduleClient() client.ScheduleClient { return f.sched }

func (f *fakeTemporalClient) ExecuteWorkflow(
	_ context.Context,
	options client.StartWorkflowOptions,
	_ interface{},
	args ...interface{},
) (client.WorkflowRun, error) {
	f.gotOptions = options
	f.gotArgs = append([]interface{}(nil), args...)
	if f.retErr != nil {
		return nil, f.retErr
	}
	return f.retRun, nil
}

type fakeScheduleClient struct {
	client.ScheduleClient
	handle  *fakeScheduleHandle
	handles map[string]*fakeScheduleHandle
	gotID   string
}

func (f *fakeScheduleClient) GetHandle(_ context.Context, id string) client.ScheduleHandle {
	f.gotID = id
	if f.handles != nil {
		if h, ok := f.handles[id]; ok {
			return h
		}
	}
	return f.handle
}

type fakeScheduleHandle struct {
	client.ScheduleHandle
	current       client.Schedule
	updateErr     error
	describeErr   error
	blockDescribe bool
	history       []client.Schedule
	info          client.ScheduleInfo
	triggerErr    error
	pauseErr      error
	unpauseErr    error
	triggerCalls  int
	pauseCalls    int
	unpauseCalls  int
}

func (h *fakeScheduleHandle) Describe(ctx context.Context) (*client.ScheduleDescription, error) {
	if h.blockDescribe {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if h.describeErr != nil {
		return nil, h.describeErr
	}
	return &client.ScheduleDescription{Schedule: h.current, Info: h.info}, nil
}

func (h *fakeScheduleHandle) Update(
	_ context.Context,
	o client.ScheduleUpdateOptions,
) error {
	if h.updateErr != nil {
		return h.updateErr
	}
	upd, err := o.DoUpdate(client.ScheduleUpdateInput{
		Description: client.ScheduleDescription{Schedule: h.current},
	})
	if err != nil {
		return err
	}
	h.current = *upd.Schedule
	h.history = append(h.history, h.current)
	return nil
}

func (h *fakeScheduleHandle) Trigger(
	_ context.Context,
	_ client.ScheduleTriggerOptions,
) error {
	h.triggerCalls++
	return h.triggerErr
}

func (h *fakeScheduleHandle) Pause(
	_ context.Context,
	_ client.SchedulePauseOptions,
) error {
	h.pauseCalls++
	return h.pauseErr
}

func (h *fakeScheduleHandle) Unpause(
	_ context.Context,
	_ client.ScheduleUnpauseOptions,
) error {
	h.unpauseCalls++
	return h.unpauseErr
}

type lifecycleScheduleStore struct {
	scheduleStore
	status         types.ScheduleStatus
	toolDefinition bool
}

func (s *lifecycleScheduleStore) GetSchedule(
	_ context.Context,
	id string,
	userID int64,
) (*types.Schedule, error) {
	return &types.Schedule{
		ID: id, TenantID: 7, UserID: userID, Status: s.status,
		ExecutionMode: types.ExecutionModeCompiled,
	}, nil
}

func (s *lifecycleScheduleStore) HasCurrentToolApprovedDefinition(
	context.Context,
	int64, int64,
	string,
) (bool, error) {
	return s.toolDefinition, nil
}

func TestNextRunReadsTemporalAfterOwnershipCheck(t *testing.T) {
	want := time.Date(2026, 7, 26, 9, 30, 0, 0, time.UTC)
	handle := &fakeScheduleHandle{
		info: client.ScheduleInfo{NextActionTimes: []time.Time{want}},
	}
	store := &lifecycleScheduleStore{status: types.ScheduleStatusActive}
	s := New(
		&fakeTemporalClient{
			sched: &fakeScheduleClient{handle: handle},
		},
		"tq",
		store,
	)
	got, err := s.NextRun(t.Context(), "task-web-2", 8)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || !got.Equal(want) {
		t.Fatalf("next=%v, want %v", got, want)
	}
	store.status = types.ScheduleStatusPaused
	got, err = s.NextRun(t.Context(), "task-web-2", 8)
	if err != nil || got != nil {
		t.Fatalf("paused next=%v err=%v", got, err)
	}
}

// fakeScheduleStore records current startup-reconcile and V3 authority calls.
type fakeScheduleStore struct {
	scheduleStore
	active                        []types.Schedule // ReconcileActions 用例：ListActiveSchedules 返回值
	activeErr                     error
	reconcileCurrent              map[string]*types.Schedule
	reconcileAcquireCalls         []string
	reconcileReleaseCalls         int
	reconcileAcquireErr           error
	reconcileReleaseErr           error
	toolDefinition                bool
	toolDefinitionErr             error
	researchV3AuthorizationToken  string
	researchV3AuthorizationByTask map[string]string
	researchV3AuthorityEnabled    bool
	researchV3AuthorityCalls      int
}

// ListActiveSchedules 供 ReconcileActions 用例注入存量调度集合。
func (f *fakeScheduleStore) ListRecoveryTenantCatalogPage(
	_ context.Context, after int64, limit int,
) ([]int64, error) {
	seen := make(map[int64]struct{})
	for _, schedule := range f.active {
		tenantID := schedule.TenantID
		if tenantID <= 0 {
			tenantID = 1
		}
		if tenantID > after {
			seen[tenantID] = struct{}{}
		}
	}
	ids := make([]int64, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	if len(ids) > limit {
		ids = ids[:limit]
	}
	return ids, nil
}

func (f *fakeScheduleStore) ListActiveSchedules(
	_ context.Context, tenantID int64,
) ([]types.Schedule, error) {
	active := make([]types.Schedule, 0, len(f.active))
	for _, schedule := range f.active {
		if schedule.TenantID == tenantID ||
			(schedule.TenantID <= 0 && tenantID == 1) {
			active = append(active, schedule)
		}
	}
	for i := range active {
		if active[i].ExecutionMode == "" {
			active[i].ExecutionMode = types.ExecutionModeCompiled
		}
	}
	return active, f.activeErr
}

func (f *fakeScheduleStore) AcquireScheduleReconcile(
	_ context.Context,
	_ int64,
	id string,
) (*types.Schedule, func(context.Context) error, error) {
	f.reconcileAcquireCalls = append(f.reconcileAcquireCalls, id)
	if f.reconcileAcquireErr != nil {
		return nil, nil, f.reconcileAcquireErr
	}
	release := func(context.Context) error {
		f.reconcileReleaseCalls++
		return f.reconcileReleaseErr
	}
	if f.reconcileCurrent != nil {
		current := f.reconcileCurrent[id]
		if current == nil {
			return nil, release, nil
		}
		copied := *current
		if copied.ExecutionMode == "" {
			copied.ExecutionMode = types.ExecutionModeCompiled
		}
		return &copied, release, nil
	}
	for i := range f.active {
		if f.active[i].ID != id {
			continue
		}
		copied := f.active[i]
		if copied.ExecutionMode == "" {
			copied.ExecutionMode = types.ExecutionModeCompiled
		}
		return &copied, release, nil
	}
	return nil, release, nil
}

func (f *fakeScheduleStore) HasCurrentToolApprovedDefinition(
	_ context.Context,
	_, _ int64,
	_ string,
) (bool, error) {
	return f.toolDefinition, f.toolDefinitionErr
}

func (f *fakeScheduleStore) VerifyEnabledResearchV3ActionAuthorization(
	_ context.Context, _, _ int64, taskID string, token string,
) error {
	f.researchV3AuthorityCalls++
	if expected, ok := f.researchV3AuthorizationByTask[taskID]; ok && expected == token {
		return nil
	}
	if !f.researchV3AuthorityEnabled || f.researchV3AuthorizationToken == "" ||
		token != f.researchV3AuthorizationToken {
		return types.NewAppError(types.CodeConflict,
			"research V3 Action authority mismatch", types.ErrConflict)
	}
	return nil
}

// GetSchedule 一律放行；归属校验由 Store 的真实 PostgreSQL 用例覆盖。
func (f *fakeScheduleStore) GetSchedule(_ context.Context, id string, userID int64) (*types.Schedule, error) {
	return &types.Schedule{
		ID: id, TenantID: 7, UserID: userID,
		ExecutionMode: types.ExecutionModeCompiled,
	}, nil
}

// AnchorAt → interval 相位（Temporal 的 phase）。这组用例守的是
// "推送落在用户指定的时刻"这个承诺——算错了不会报错，只会在错误的
// 时间推送，是最难在生产上察觉的那类 bug。
// ============================================================

// TestIntervalOffset_锚点决定相位 用真实时刻验算：触发序列必须精确穿过锚点。
func TestIntervalOffset_锚点决定相位(t *testing.T) {
	every := 6 * 3600 // 6 小时
	// 北京时间 2026-07-19 15:00 = UTC 07:00。
	anchor, err := time.Parse(time.RFC3339, "2026-07-19T15:00:00+08:00")
	if err != nil {
		t.Fatalf("fixture 时刻不合法: %v", err)
	}
	off := intervalOffset(anchor, every)

	// 关键不变量：epoch + n*every + offset 必须能精确命中锚点本身。
	// 即 (anchorUnix - offset) 能被 every 整除。
	if (anchor.Unix()-int64(off.Seconds()))%int64(every) != 0 {
		t.Errorf("相位算错：锚点 %v 不在 epoch+n*%ds+%v 的序列上", anchor, every, off)
	}
	// 6h 周期、UTC 07:00 锚点 → 相位应为 1h（epoch 对齐点是 00/06/12/18 UTC）。
	if off != time.Hour {
		t.Errorf("相位应为 1h，实得 %v", off)
	}
}

// TestIntervalOffset_跨周期与非整除 每 3 天这类 cron 表达不了的周期，正是 anchor 的主用例。
func TestIntervalOffset_跨周期与非整除(t *testing.T) {
	for _, tc := range []struct {
		name   string
		every  int
		anchor string
	}{
		{"每3天晚8点", 3 * 86400, "2026-07-19T20:00:00+08:00"},
		{"每7小时", 7 * 3600, "2026-07-19T09:30:00+08:00"},
		{"每36小时", 36 * 3600, "2026-07-19T06:00:00+08:00"},
		{"UTC锚点", 86400, "2026-07-19T00:00:00Z"},
		{"负Unix秒(1969年)", 86400, "1969-07-19T20:00:00+08:00"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, err := time.Parse(time.RFC3339, tc.anchor)
			if err != nil {
				t.Fatalf("fixture 不合法: %v", err)
			}
			off := intervalOffset(a, tc.every)
			// Temporal 要求 phase 非负（proto: interval and phase must be non-negative）；
			// Go 的 % 对负数返回负值，1969 年锚点会踩到。
			if off < 0 {
				t.Errorf("相位不得为负（Temporal 会拒绝整个调度），实得 %v", off)
			}
			if off >= time.Duration(tc.every)*time.Second {
				t.Errorf("相位应落在 [0, every) 内，实得 %v", off)
			}
			// 序列必须穿过锚点。
			if (a.Unix()-int64(off.Seconds()))%int64(tc.every) != 0 {
				t.Errorf("相位算错：锚点不在触发序列上（every=%d off=%v）", tc.every, off)
			}
		})
	}
}

// TestIntervalOffset_无锚点为零 不给锚点时保持原语义（epoch 对齐），不得意外引入相位。
func TestIntervalOffset_无锚点为零(t *testing.T) {
	if got := intervalOffset(time.Time{}, 3600); got != 0 {
		t.Errorf("无锚点相位应为 0，实得 %v", got)
	}
}

// TestTranslateSpec_锚点进 Temporal spec 端到端：spec 里的 anchor_at 必须真的变成 Offset。
func TestTranslateSpec_锚点进TemporalSpec(t *testing.T) {
	sdk, err := translateSpec(ScheduleSpec{
		EverySeconds: 6 * 3600,
		AnchorAt:     "2026-07-19T15:00:00+08:00",
	})
	if err != nil {
		t.Fatalf("translateSpec 失败: %v", err)
	}
	if len(sdk.Intervals) != 1 {
		t.Fatalf("应产出 1 个 interval，实得 %d", len(sdk.Intervals))
	}
	if sdk.Intervals[0].Every != 6*time.Hour {
		t.Errorf("Every 应为 6h，实得 %v", sdk.Intervals[0].Every)
	}
	if sdk.Intervals[0].Offset != time.Hour {
		t.Errorf("Offset 应为 1h（锚点 UTC 07:00 对 6h 取模），实得 %v", sdk.Intervals[0].Offset)
	}
}

// TestValidateSpec_锚点约束 anchor 只配 interval；格式必须 RFC3339（带时区）。
func TestValidateSpec_锚点约束(t *testing.T) {
	// 合法：interval + 锚点。
	if err := validateSpec(ScheduleSpec{EverySeconds: 7200, AnchorAt: "2026-07-19T20:00:00+08:00"}); err != nil {
		t.Errorf("interval + 合法锚点不应被拒: %v", err)
	}
	// 非法：cron + 锚点（两套互相矛盾的时间表达，静默忽略会让用户以为锚点生效了）。
	err := validateSpec(ScheduleSpec{Cron: "0 8 * * *", AnchorAt: "2026-07-19T20:00:00+08:00"})
	if err == nil {
		t.Error("cron 搭配 anchor_at 应被拒（否则用户以为锚点生效）")
	} else if !errors.Is(err, types.ErrValidation) {
		t.Errorf("应是 CodeValidation，实得 %v", err)
	}
	// 非法：不带时区的时刻（"晚上 8 点"是哪个时区的 8 点无从谈起）。
	for _, bad := range []string{"2026-07-19 20:00:00", "2026-07-19T20:00:00", "明天晚上八点", "1784000000"} {
		if err := validateSpec(ScheduleSpec{EverySeconds: 7200, AnchorAt: bad}); err == nil {
			t.Errorf("非 RFC3339 锚点 %q 应被拒", bad)
		}
	}
}

// ownershipStore 让 GetSchedule 按 (id, userID) 判定归属，并记录 Temporal 是否被动过。
type ownershipStore struct {
	scheduleStore
	ownerUserID int64
	getCalls    int
	deleteCalls int
}

func (o *ownershipStore) GetSchedule(_ context.Context, id string, userID int64) (*types.Schedule, error) {
	o.getCalls++
	if userID != o.ownerUserID {
		// 与生产同语义：不属于你 = 查不到（不给枚举他人 id 的机会）。
		return nil, types.NewAppError(types.CodeNotFound, "调度不存在", nil)
	}
	return &types.Schedule{ID: id, UserID: userID}, nil
}

func (o *ownershipStore) DeleteSchedule(_ context.Context, _ string, _ int64) error {
	o.deleteCalls++
	return nil
}

func payloadArg(t *testing.T, p workflow.PushParams) interface{} {
	t.Helper()
	pl, err := converter.GetDefaultDataConverter().ToPayload(p)
	if err != nil {
		t.Fatalf("编码 PushParams 失败: %v", err)
	}
	return pl
}

// reconcileSchedule 造一个"服务端已有"的完整调度：Action.Args 由调用方指定，
// 其余字段（Workflow 类型名 / TaskQueue / Spec / Overlap / State）是本组用例断言的保留对象。
func reconcileSchedule(actionID string, args []interface{}) client.Schedule {
	return client.Schedule{
		Action: &client.ScheduleWorkflowAction{
			ID:        actionID,
			Workflow:  "PushPipelineWorkflow",
			TaskQueue: "vane-tq",
			Args:      args,
		},
		Spec:   &client.ScheduleSpec{CronExpressions: []string{"30 8 * * *"}, TimeZoneName: "Asia/Shanghai"},
		Policy: &client.SchedulePolicies{Overlap: enums.SCHEDULE_OVERLAP_POLICY_SKIP},
		State:  &client.ScheduleState{Note: "原始状态"},
	}
}

func TestReconcileActions_写前重读发现编辑标记则跳过(t *testing.T) {
	const taskID = "push-1-editing"
	h := &fakeScheduleHandle{current: reconcileSchedule(
		"wf-"+taskID, []interface{}{payloadArg(t, workflow.PushParams{UserID: 1})},
	)}
	fc := &fakeTemporalClient{sched: &fakeScheduleClient{
		handles: map[string]*fakeScheduleHandle{taskID: h},
	}}
	stale := types.Schedule{
		ID: taskID, TenantID: 7, UserID: 1, NLDescription: "旧快照",
		ScopeJSON: json.RawMessage(`{}`), Status: types.ScheduleStatusActive,
	}
	st := &fakeScheduleStore{
		active: []types.Schedule{stale},
		reconcileCurrent: map[string]*types.Schedule{
			taskID: nil, // Store 在 advisory gate 内发现 paused/marker，拒绝授权。
		},
	}

	if err := New(fc, "tq", st).ReconcileActions(t.Context()); err != nil {
		t.Fatalf("ReconcileActions 失败: %v", err)
	}
	if len(h.history) != 0 {
		t.Fatalf("编辑已 quiesce 后不得再写 Temporal，实得 %d 次", len(h.history))
	}
	if st.reconcileReleaseCalls != 1 {
		t.Fatalf("跳过路径也必须释放 DB gate，实得 %d", st.reconcileReleaseCalls)
	}
}

func TestReconcileOne_远端失败仍释放数据库Gate并保留双错误(t *testing.T) {
	const taskID = "push-1-release-error"
	temporalErr := errors.New("temporal unavailable")
	releaseErr := errors.New("rollback unavailable")
	h := &fakeScheduleHandle{
		current:     reconcileSchedule("wf-"+taskID, nil),
		describeErr: temporalErr,
	}
	fc := &fakeTemporalClient{sched: &fakeScheduleClient{
		handles: map[string]*fakeScheduleHandle{taskID: h},
	}}
	current := types.Schedule{
		ID: taskID, TenantID: 7, UserID: 1, NLDescription: "release",
		ScopeJSON: json.RawMessage(`{}`), Status: types.ScheduleStatusActive,
	}
	st := &fakeScheduleStore{
		active:              []types.Schedule{current},
		reconcileReleaseErr: releaseErr,
	}

	updated, err := New(fc, "tq", st).reconcileOne(t.Context(), current)
	if updated || err == nil {
		t.Fatalf("reconcileOne = %v, %v; want false and joined error", updated, err)
	}
	if !errors.Is(err, temporalErr) || !errors.Is(err, releaseErr) {
		t.Fatalf("reconcileOne must preserve remote and release errors: %v", err)
	}
	if st.reconcileReleaseCalls != 1 {
		t.Fatalf("database gate release calls = %d, want 1", st.reconcileReleaseCalls)
	}
}

func TestReconcileActions_RejectsRetiredActionWithoutMutation(t *testing.T) {
	const taskID = "task-v1-only-tool-canary"
	initial := makePushParams(7, 1, taskID, workflow.PushScope{}, "legacy")
	initial.RuntimeVersion = workflow.CompiledRuntimeSnapshotV1
	h := &fakeScheduleHandle{current: reconcileSchedule(
		"wf-"+taskID, []interface{}{payloadArg(t, initial)},
	)}
	fc := &fakeTemporalClient{sched: &fakeScheduleClient{
		handles: map[string]*fakeScheduleHandle{taskID: h},
	}}
	st := &fakeScheduleStore{active: []types.Schedule{{
		ID: taskID, TenantID: 7, UserID: 1, NLDescription: "legacy",
		ScopeJSON:     json.RawMessage(`{}`),
		Status:        types.ScheduleStatusActive,
		ExecutionMode: types.ExecutionModeCompiled,
	}}}
	s := New(fc, "tq", st)
	if err := s.ReconcileActions(t.Context()); err == nil {
		t.Fatal("retired active Action did not block startup")
	}
	if len(h.history) != 0 {
		t.Fatalf("failed Tool preflight mutated Action %d times", len(h.history))
	}
}

// TestReconcileActions_单条失败继续检查但最终拒绝启动：尽量收集完整诊断，
// 但任何漏修都不得在 worker ingress 开放后静默遗留。
func TestReconcileActions_单条失败聚合后FailClosed(t *testing.T) {
	temporalErr := errors.New("temporal 抖动")
	bad := &fakeScheduleHandle{describeErr: temporalErr}
	goodFrozen := []interface{}{payloadArg(t, workflow.PushParams{UserID: 2})}
	good := &fakeScheduleHandle{current: reconcileSchedule("wf-push-2-ok", goodFrozen)}
	fc := &fakeTemporalClient{sched: &fakeScheduleClient{handles: map[string]*fakeScheduleHandle{
		"push-1-bad": bad,
		"push-2-ok":  good,
	}}}
	st := &fakeScheduleStore{active: []types.Schedule{
		{ID: "push-1-bad", UserID: 1, ScopeJSON: json.RawMessage(`{}`), Status: types.ScheduleStatusActive},
		{ID: "push-2-ok", UserID: 2, ScopeJSON: json.RawMessage(`{}`), Status: types.ScheduleStatusActive},
	}}
	s := New(fc, "tq", st)

	err := s.ReconcileActions(context.Background())
	if !errors.Is(err, temporalErr) {
		t.Fatalf("单条失败必须聚合并拒绝启动，实得 %v", err)
	}
	if !errors.Is(err, types.ErrConflict) {
		t.Fatalf("retired Action conflict was not preserved: %v", err)
	}
	if len(good.history) != 0 {
		t.Errorf("retired Action must never be rewritten, got %d updates", len(good.history))
	}
}

func TestReconcileActions_全局启动预算耗尽立即FailClosed(t *testing.T) {
	const (
		blockedID = "push-1-blocked"
		unseenID  = "push-2-unseen"
	)
	blocked := &fakeScheduleHandle{blockDescribe: true}
	unseen := &fakeScheduleHandle{current: reconcileSchedule("wf-"+unseenID, nil)}
	fc := &fakeTemporalClient{sched: &fakeScheduleClient{handles: map[string]*fakeScheduleHandle{
		blockedID: blocked,
		unseenID:  unseen,
	}}}
	st := &fakeScheduleStore{active: []types.Schedule{
		{ID: blockedID, UserID: 1, ScopeJSON: json.RawMessage(`{}`), Status: types.ScheduleStatusActive},
		{ID: unseenID, UserID: 2, ScopeJSON: json.RawMessage(`{}`), Status: types.ScheduleStatusActive},
	}}

	started := time.Now()
	err := New(fc, "tq", st).reconcileActions(t.Context(), 25*time.Millisecond)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("global startup budget must fail closed: %v", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("global startup budget took %s, want bounded completion", elapsed)
	}
	if len(st.reconcileAcquireCalls) != 1 ||
		st.reconcileAcquireCalls[0] != blockedID {
		t.Fatalf("post-budget schedules must remain untouched: %v",
			st.reconcileAcquireCalls)
	}
	if len(unseen.history) != 0 {
		t.Fatalf("post-budget schedule was updated %d times", len(unseen.history))
	}
}
