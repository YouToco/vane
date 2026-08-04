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

// TestReconcileActions_补齐缺失的scheduleID 验证旧 Action 只更新入参。
func TestReconcileActions_补齐缺失的scheduleID(t *testing.T) {
	frozen := []interface{}{payloadArg(t, workflow.PushParams{UserID: 1})} // 无 ScheduleID
	h := &fakeScheduleHandle{current: reconcileSchedule("wf-push-1-old", frozen)}
	fc := &fakeTemporalClient{sched: &fakeScheduleClient{
		handles: map[string]*fakeScheduleHandle{"push-1-old": h},
	}}
	st := &fakeScheduleStore{active: []types.Schedule{{
		ID: "push-1-old", UserID: 1, NLDescription: "每天早上 8:30 推送今日精选",
		ScopeJSON: json.RawMessage(`{}`), Status: types.ScheduleStatusActive,
	}}}
	s := New(fc, "tq", st)

	if err := s.ReconcileActions(context.Background()); err != nil {
		t.Fatalf("ReconcileActions 失败: %v", err)
	}
	if len(h.history) != 1 {
		t.Fatalf("缺 id 的调度应被 Update 一次，实得 %d", len(h.history))
	}
	if len(st.reconcileAcquireCalls) != 1 || st.reconcileAcquireCalls[0] != "push-1-old" ||
		st.reconcileReleaseCalls != 1 {
		t.Fatalf("reconcile 授权 acquire/release = %v/%d，期望 [push-1-old]/1",
			st.reconcileAcquireCalls, st.reconcileReleaseCalls)
	}
	act, ok := h.current.Action.(*client.ScheduleWorkflowAction)
	if !ok {
		t.Fatalf("Action 类型错: %T", h.current.Action)
	}
	got, ok := act.Args[0].(workflow.PushParams)
	if !ok {
		t.Fatalf("Args[0] 应为 PushParams，实得 %T", act.Args[0])
	}
	if got.ScheduleID != "push-1-old" {
		t.Errorf("应补上 schedule_id=push-1-old，实得 %q", got.ScheduleID)
	}
	if got.RunKind != workflow.PushRunKindScheduled {
		t.Errorf("应补上 run_kind=scheduled，实得 %q", got.RunKind)
	}
	if got.UserID != 1 {
		t.Errorf("UserID 应保留 1，实得 %d", got.UserID)
	}
	if got.NLDesc != "每天早上 8:30 推送今日精选" {
		t.Errorf("NLDesc 应从镜像补上任务名，实得 %q", got.NLDesc)
	}
	// 只换 Args：其余字段原样保留（丢了会让调度再也不推送 / 推送堆叠）。
	if act.ID != "wf-push-1-old" || act.TaskQueue != "vane-tq" || act.Workflow != "PushPipelineWorkflow" {
		t.Errorf("Action 其余字段必须保留，实得 %+v", act)
	}
	if len(h.current.Spec.CronExpressions) != 1 || h.current.Spec.CronExpressions[0] != "30 8 * * *" {
		t.Errorf("Spec 不该被动，实得 %+v", h.current.Spec)
	}
	if h.current.Policy == nil || h.current.Policy.Overlap != enums.SCHEDULE_OVERLAP_POLICY_SKIP {
		t.Errorf("Overlap=SKIP 不该被动")
	}
	if h.current.State == nil || h.current.State.Note != "原始状态" {
		t.Errorf("State 不该被动")
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

// 已有 schedule_id/NLDesc 但缺 run_kind 也是旧冻结入参。若只比前两者会误判
// 为已同步，AuthorizeRun 随后按 unknown fail-closed，使任务永久不执行。
func TestReconcileActions_补齐显式ScheduledRunKind(t *testing.T) {
	legacy := workflow.PushParams{
		UserID: 1, ScheduleID: "push-1-kind", NLDesc: "任务名", Scope: workflow.PushScope{},
	}
	h := &fakeScheduleHandle{current: reconcileSchedule("wf-push-1-kind", []interface{}{payloadArg(t, legacy)})}
	fc := &fakeTemporalClient{sched: &fakeScheduleClient{
		handles: map[string]*fakeScheduleHandle{"push-1-kind": h},
	}}
	st := &fakeScheduleStore{active: []types.Schedule{{
		ID: "push-1-kind", UserID: 1, NLDescription: "任务名",
		ScopeJSON: json.RawMessage(`{}`), Status: types.ScheduleStatusActive,
	}}}

	if err := New(fc, "tq", st).ReconcileActions(t.Context()); err != nil {
		t.Fatalf("ReconcileActions 失败: %v", err)
	}
	if len(h.history) != 1 {
		t.Fatalf("缺 run_kind 的调度应被 Update 一次，实得 %d", len(h.history))
	}
	got := h.current.Action.(*client.ScheduleWorkflowAction).Args[0].(workflow.PushParams)
	if got.RunKind != workflow.PushRunKindScheduled || got.ScheduleID != legacy.ScheduleID || got.NLDesc != legacy.NLDesc {
		t.Fatalf("reconciled params = %+v", got)
	}
}

// 旧 Action 没有 tenant_id，但数据库镜像是调度归属的真相源。reconcile
// 必须以镜像 TenantID 修正冻结入参，不能继续留在 tenant=0 的 legacy 路径。
func TestReconcileActions_按数据库租户修正旧Action(t *testing.T) {
	const tenantID int64 = 7
	legacy := makePushParams(0, 1, "push-1-tenant", workflow.PushScope{}, "任务名")
	h := &fakeScheduleHandle{current: reconcileSchedule(
		"wf-push-1-tenant", []interface{}{payloadArg(t, legacy)},
	)}
	fc := &fakeTemporalClient{sched: &fakeScheduleClient{
		handles: map[string]*fakeScheduleHandle{"push-1-tenant": h},
	}}
	st := &fakeScheduleStore{active: []types.Schedule{{
		ID: "push-1-tenant", TenantID: tenantID, UserID: 1, NLDescription: "任务名",
		ScopeJSON: json.RawMessage(`{}`), Status: types.ScheduleStatusActive,
	}}}

	if err := New(fc, "tq", st).ReconcileActions(t.Context()); err != nil {
		t.Fatalf("ReconcileActions 失败: %v", err)
	}
	if len(h.history) != 1 {
		t.Fatalf("tenant_id 漂移的调度应被 Update 一次，实得 %d", len(h.history))
	}
	got := h.current.Action.(*client.ScheduleWorkflowAction).Args[0].(workflow.PushParams)
	if got.TenantID != tenantID {
		t.Fatalf("reconciled tenant_id = %d，期望数据库镜像值 %d", got.TenantID, tenantID)
	}
	if got.Snapshot != nil {
		t.Fatalf("reconcile 不应把 run snapshot 写入持久 Action，实得 %+v", got.Snapshot)
	}
}

func TestReconcileActions_保留数据库ExecutionMode(t *testing.T) {
	const taskID = "push-1-dynamic"
	frozen := makePushParams(7, 1, taskID, workflow.PushScope{}, "动态研究")
	h := &fakeScheduleHandle{current: reconcileSchedule(
		"wf-"+taskID, []interface{}{payloadArg(t, frozen)},
	)}
	fc := &fakeTemporalClient{sched: &fakeScheduleClient{
		handles: map[string]*fakeScheduleHandle{taskID: h},
	}}
	st := &fakeScheduleStore{active: []types.Schedule{{
		ID: taskID, TenantID: 7, UserID: 1, NLDescription: "动态研究",
		ScopeJSON: json.RawMessage(`{}`), Status: types.ScheduleStatusActive,
		ExecutionMode: types.ExecutionModeDiscoverAtRun,
	}}}

	if err := New(fc, "tq", st).ReconcileActions(t.Context()); err != nil {
		t.Fatalf("ReconcileActions failed: %v", err)
	}
	if len(h.history) != 1 {
		t.Fatalf("mode drift should update once, got %d", len(h.history))
	}
	got := h.current.Action.(*client.ScheduleWorkflowAction).Args[0].(workflow.PushParams)
	if got.ExecutionMode != types.ExecutionModeDiscoverAtRun {
		t.Fatalf("reconciled execution_mode=%q, want %q",
			got.ExecutionMode, types.ExecutionModeDiscoverAtRun)
	}
}

// TestReconcileActions_已带id则跳过 守幂等：Action.Args 已与期望 params 一致
// （tenant_id + run_kind + schedule_id + 任务名）的调度——新建的、或上次已
// reconcile 过且未改名的——
// 重启时不得再写 Temporal。走真 payload 解码路径。
func TestReconcileActions_已带id则跳过(t *testing.T) {
	good := []interface{}{payloadArg(t, makePushParams(7, 1, "push-1-new", workflow.PushScope{}, "任务名"))}
	h := &fakeScheduleHandle{current: reconcileSchedule("wf-push-1-new", good)}
	fc := &fakeTemporalClient{sched: &fakeScheduleClient{
		handles: map[string]*fakeScheduleHandle{"push-1-new": h},
	}}
	st := &fakeScheduleStore{active: []types.Schedule{{
		ID: "push-1-new", TenantID: 7, UserID: 1, NLDescription: "任务名",
		ScopeJSON: json.RawMessage(`{}`), Status: types.ScheduleStatusActive,
	}}}
	s := New(fc, "tq", st)

	if err := s.ReconcileActions(context.Background()); err != nil {
		t.Fatalf("ReconcileActions 失败: %v", err)
	}
	if len(h.history) != 0 {
		t.Errorf("入参已一致的调度不应再 Update，实得 %d 次", len(h.history))
	}
}

// TestReconcileActions_任务名漂移回写 守 #3：镜像 nl_description 新、Action.NLDesc 旧
// （改名发生在可靠的定义编辑协议上线之前、或历史编辑失败过的调度），
// reconcile 必须把 Action 刷回新名——否则聚合卡 header 永久显示旧任务名。
func TestReconcileActions_任务名漂移回写(t *testing.T) {
	stale := []interface{}{payloadArg(t, makePushParams(0, 1, "push-1-ren", workflow.PushScope{}, "旧任务名"))}
	h := &fakeScheduleHandle{current: reconcileSchedule("wf-push-1-ren", stale)}
	fc := &fakeTemporalClient{sched: &fakeScheduleClient{
		handles: map[string]*fakeScheduleHandle{"push-1-ren": h},
	}}
	st := &fakeScheduleStore{active: []types.Schedule{{
		ID: "push-1-ren", UserID: 1, NLDescription: "新任务名",
		ScopeJSON: json.RawMessage(`{}`), Status: types.ScheduleStatusActive,
	}}}
	s := New(fc, "tq", st)

	if err := s.ReconcileActions(context.Background()); err != nil {
		t.Fatalf("ReconcileActions 失败: %v", err)
	}
	if len(h.history) != 1 {
		t.Fatalf("任务名漂移的调度应被 Update 一次，实得 %d", len(h.history))
	}
	act := h.current.Action.(*client.ScheduleWorkflowAction)
	got := act.Args[0].(workflow.PushParams)
	if got.NLDesc != "新任务名" {
		t.Errorf("NLDesc 应回写为新任务名，实得 %q", got.NLDesc)
	}
	if got.ScheduleID != "push-1-ren" || got.UserID != 1 {
		t.Errorf("schedule_id/user_id 应保持，实得 %+v", got)
	}
	// 只换 Args：Spec/Overlap/State 原样保留。
	if len(h.current.Spec.CronExpressions) != 1 || h.current.Spec.CronExpressions[0] != "30 8 * * *" {
		t.Errorf("Spec 不该被动，实得 %+v", h.current.Spec)
	}
	if h.current.Policy == nil || h.current.Policy.Overlap != enums.SCHEDULE_OVERLAP_POLICY_SKIP {
		t.Error("Overlap=SKIP 不该被动")
	}
}

func TestReconcileActions_UpgradesAndPreservesRunOutcomeCanary(t *testing.T) {
	const taskID = "task-run-outcome-reconcile"
	snapshot := makePushParams(7, 1, taskID, workflow.PushScope{}, "canary")
	snapshot.RuntimeVersion = workflow.CompiledRuntimeSnapshotV1
	h := &fakeScheduleHandle{current: reconcileSchedule(
		"wf-"+taskID, []interface{}{payloadArg(t, snapshot)},
	)}
	fc := &fakeTemporalClient{sched: &fakeScheduleClient{
		handles: map[string]*fakeScheduleHandle{taskID: h},
	}}
	st := &fakeScheduleStore{active: []types.Schedule{{
		ID: taskID, TenantID: 7, UserID: 1, NLDescription: "canary",
		ScopeJSON: json.RawMessage(`{}`), Status: types.ScheduleStatusActive,
		ExecutionMode: types.ExecutionModeCompiled,
	}}}
	s := New(
		fc, "tq", st,
		WithCompiledRuntimeRollout(true, taskID, false),
		WithRunOutcomeRollout(true, taskID, false),
	)
	if err := s.ReconcileActions(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(h.history) != 1 {
		t.Fatalf("snapshot action updates = %d, want 1", len(h.history))
	}
	got := h.current.Action.(*client.ScheduleWorkflowAction).
		Args[0].(workflow.PushParams)
	if got.RuntimeVersion != workflow.CompiledRuntimeRunOutcomeV1 {
		t.Fatalf("reconciled runtime = %q", got.RuntimeVersion)
	}
	h.current.Action.(*client.ScheduleWorkflowAction).Args[0] =
		payloadArg(t, got)
	if err := s.ReconcileActions(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(h.history) != 1 {
		t.Fatalf("combined action was rewritten %d times", len(h.history))
	}
}

func TestReconcileActions_SelectsAndRollsBackToolRuntimeCanary(t *testing.T) {
	const taskID = "task-tool-runtime-reconcile"
	initial := makePushParams(7, 1, taskID, workflow.PushScope{}, "canary")
	initial.RuntimeVersion = workflow.CompiledRuntimeSnapshotV1
	h := &fakeScheduleHandle{current: reconcileSchedule(
		"wf-"+taskID, []interface{}{payloadArg(t, initial)},
	)}
	fc := &fakeTemporalClient{sched: &fakeScheduleClient{
		handles: map[string]*fakeScheduleHandle{taskID: h},
	}}
	st := &fakeScheduleStore{active: []types.Schedule{{
		ID: taskID, TenantID: 7, UserID: 1, NLDescription: "canary",
		ScopeJSON:     json.RawMessage(`{}`),
		Status:        types.ScheduleStatusActive,
		ExecutionMode: types.ExecutionModeCompiled,
	}}, toolDefinition: true}
	s := New(
		fc, "tq", st,
		WithCompiledRuntimeRollout(true, taskID, false),
		WithCompiledToolRuntimeCanary(taskID),
	)
	if err := s.ReconcileActions(t.Context()); err != nil {
		t.Fatal(err)
	}
	got := h.current.Action.(*client.ScheduleWorkflowAction).
		Args[0].(workflow.PushParams)
	if got.RuntimeVersion != workflow.CompiledRuntimeToolSnapshotV2 {
		t.Fatalf("reconciled Tool runtime=%q", got.RuntimeVersion)
	}

	WithCompiledToolRuntimeCanary("")(s)
	h.current.Action.(*client.ScheduleWorkflowAction).Args[0] =
		payloadArg(t, got)
	if err := s.ReconcileActions(t.Context()); err == nil {
		t.Fatal("active Tool task was relabeled as V1 during rollback")
	}
	got, found, decodeErr := decodeScheduleActionPushParams(
		h.current.Action)
	if decodeErr != nil || !found {
		t.Fatalf("decode blocked Tool Action: found=%v err=%v",
			found, decodeErr)
	}
	if got.RuntimeVersion != workflow.CompiledRuntimeToolSnapshotV2 {
		t.Fatalf("blocked rollback mutated Tool runtime=%q", got.RuntimeVersion)
	}
	st.active = nil // owner paused the task before clearing the canary ID.
	if err := s.ReconcileActions(t.Context()); err != nil {
		t.Fatalf("paused Tool rollback should be inert: %v", err)
	}
}

func TestReconcileActions_RejectsV1OnlyTaskFromToolCanary(t *testing.T) {
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
	s := New(
		fc, "tq", st,
		WithCompiledRuntimeRollout(true, taskID, false),
		WithCompiledToolRuntimeCanary(taskID),
	)
	if err := s.ReconcileActions(t.Context()); err == nil {
		t.Fatal("V1-only task entered Tool runtime without definition preflight")
	}
	if len(h.history) != 0 {
		t.Fatalf("failed Tool preflight mutated Action %d times", len(h.history))
	}
}

func TestToolRuntimeResumeRequiresCanaryToBeReenabled(t *testing.T) {
	const taskID = "task-tool-runtime-resume"
	params := makePushParams(
		7, 1, taskID, workflow.PushScope{}, "paused Tool task")
	params.RuntimeVersion = workflow.CompiledRuntimeToolSnapshotV2
	h := &fakeScheduleHandle{current: reconcileSchedule(
		"wf-"+taskID, []interface{}{payloadArg(t, params)},
	)}
	fc := &fakeTemporalClient{sched: &fakeScheduleClient{
		handles: map[string]*fakeScheduleHandle{taskID: h},
	}}
	st := &fakeScheduleStore{toolDefinition: true}
	s := New(
		fc, "tq", st,
		WithCompiledRuntimeRollout(true, taskID, false),
	)
	sc := &types.Schedule{
		ID: taskID, TenantID: 7, UserID: 1,
		Status:        types.ScheduleStatusPaused,
		ExecutionMode: types.ExecutionModeCompiled,
	}
	if err := s.authorizeToolRuntimeResume(
		t.Context(), sc); err == nil {
		t.Fatal("paused Tool task resumed while canary was disabled")
	}
	WithCompiledToolRuntimeCanary(taskID)(s)
	if err := s.authorizeToolRuntimeResume(
		t.Context(), sc); err != nil {
		t.Fatalf("reenabled Tool canary could not resume: %v", err)
	}
}

func TestToolRuntimeResumeBindsFrozenActionIdentity(t *testing.T) {
	const taskID = "task-tool-runtime-resume-identity"
	baseline := makePushParams(
		7, 1, taskID, workflow.PushScope{}, "paused Tool task")
	baseline.RuntimeVersion = workflow.CompiledRuntimeToolSnapshotV2
	sc := &types.Schedule{
		ID: taskID, TenantID: 7, UserID: 1,
		Status:        types.ScheduleStatusPaused,
		ExecutionMode: types.ExecutionModeCompiled,
	}
	cases := map[string]func(*workflow.PushParams){
		"tenant":         func(p *workflow.PushParams) { p.TenantID++ },
		"user":           func(p *workflow.PushParams) { p.UserID++ },
		"schedule":       func(p *workflow.PushParams) { p.ScheduleID = "other-task" },
		"run kind":       func(p *workflow.PushParams) { p.RunKind = workflow.PushRunKindAdHoc },
		"execution mode": func(p *workflow.PushParams) { p.ExecutionMode = types.ExecutionModeDiscoverAtRun },
		"snapshot": func(p *workflow.PushParams) {
			p.Snapshot = &workflow.RunSnapshotRef{}
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			params := baseline
			mutate(&params)
			h := &fakeScheduleHandle{current: reconcileSchedule(
				"wf-"+taskID, []interface{}{payloadArg(t, params)},
			)}
			s := New(
				&fakeTemporalClient{sched: &fakeScheduleClient{
					handles: map[string]*fakeScheduleHandle{taskID: h},
				}},
				"tq",
				&fakeScheduleStore{toolDefinition: true},
				WithCompiledRuntimeRollout(true, taskID, false),
				WithCompiledToolRuntimeCanary(taskID),
			)
			if err := s.authorizeToolRuntimeResume(
				t.Context(), sc); !errors.Is(err, types.ErrConflict) {
				t.Fatalf("mismatched %s action error=%v", name, err)
			}
		})
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
	if len(good.history) != 1 {
		t.Errorf("失败调度不该阻断后续，good 应被 Update 一次，实得 %d", len(good.history))
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
