package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enums "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
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

// TestCheckActiveLimit 覆盖单 owner 活跃调度上限。
func TestCheckActiveLimit(t *testing.T) {
	if err := checkActiveLimit(maxActiveSchedules - 1); err != nil {
		t.Errorf("未达上限应放行，实际 %v", err)
	}
	if err := checkActiveLimit(0); err != nil {
		t.Errorf("零活跃应放行，实际 %v", err)
	}
	err := checkActiveLimit(maxActiveSchedules)
	if err == nil {
		t.Fatal("达到上限应拒绝")
	}
	if !errors.Is(err, types.ErrValidation) {
		t.Errorf("上限错误应为 ErrValidation，实际 %v", err)
	}
	if err := checkActiveLimit(maxActiveSchedules + 5); err == nil {
		t.Error("超过上限应拒绝")
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
// CreatePush：行为刻画（A0）。
//
// 这组测试只钉住当前入口的可观察行为，不为后续 task.Service / paused saga
// 预写新语义。故障替身共享一条调用轨迹，使每个失败注入能精确证明停在哪一阶段。
// ============================================================

type createPushTrace struct {
	calls []string
}

func (tr *createPushTrace) add(call string) {
	tr.calls = append(tr.calls, call)
}

type createPushStore struct {
	scheduleStore
	trace       *createPushTrace
	schedules   []types.Schedule
	listErr     error
	insertErr   error
	listCalls   int
	listUserID  int64
	insertCalls int
	inserted    *types.Schedule
	tenantID    int64
	tenantErr   error
}

func (st *createPushStore) ResolveActiveTenantForUser(_ context.Context, _ int64) (int64, error) {
	st.trace.add("store.tenant")
	if st.tenantErr != nil {
		return 0, st.tenantErr
	}
	if st.tenantID == 0 {
		return int64(types.SingleTenantID), nil
	}
	return st.tenantID, nil
}

func (st *createPushStore) ListSchedulesByUser(_ context.Context, userID int64) ([]types.Schedule, error) {
	st.trace.add("store.list")
	st.listCalls++
	st.listUserID = userID
	return st.schedules, st.listErr
}

func (st *createPushStore) InsertSchedule(_ context.Context, sc *types.Schedule) error {
	st.trace.add("store.insert")
	st.insertCalls++
	if sc != nil {
		copied := *sc
		copied.SpecJSON = append(json.RawMessage(nil), sc.SpecJSON...)
		copied.ScopeJSON = append(json.RawMessage(nil), sc.ScopeJSON...)
		st.inserted = &copied
	}
	return st.insertErr
}

type createPushTemporalClient struct {
	client.Client
	schedules *createPushScheduleClient
}

func (fc *createPushTemporalClient) ScheduleClient() client.ScheduleClient {
	return fc.schedules
}

type createPushScheduleClient struct {
	client.ScheduleClient
	trace       *createPushTrace
	handle      *createPushScheduleHandle
	createErr   error
	createCalls int
	createOpts  client.ScheduleOptions
	handleCalls int
	handleID    string
}

func (fc *createPushScheduleClient) Create(_ context.Context, opts client.ScheduleOptions) (client.ScheduleHandle, error) {
	fc.trace.add("temporal.create")
	fc.createCalls++
	fc.createOpts = opts
	if fc.createErr != nil {
		return nil, fc.createErr
	}
	return fc.handle, nil
}

func (fc *createPushScheduleClient) GetHandle(_ context.Context, id string) client.ScheduleHandle {
	fc.trace.add("temporal.get_handle")
	fc.handleCalls++
	fc.handleID = id
	fc.handle.id = id
	return fc.handle
}

type createPushScheduleHandle struct {
	client.ScheduleHandle
	trace       *createPushTrace
	id          string
	deleteErr   error
	deleteCalls int
}

func (h *createPushScheduleHandle) GetID() string {
	return h.id
}

func (h *createPushScheduleHandle) Delete(_ context.Context) error {
	h.trace.add("temporal.delete")
	h.deleteCalls++
	return h.deleteErr
}

func activeScheduleFixtures(n int) []types.Schedule {
	out := make([]types.Schedule, n)
	for i := range out {
		out[i].Status = types.ScheduleStatusActive
	}
	return out
}

// TestCreatePush_CurrentBehavior 锁定当前 Scheduler.CreatePush 的阶段顺序、
// 错误映射与补偿边界。后续 A1 迁移到 task.Service 时应复用同一组 golden 行为；
// 在此之前不得把 create 改成 paused，也不得改变用户可见错误文案。
func TestCreatePush_CurrentBehavior(t *testing.T) {
	listCause := errors.New("list failed")
	listErr := types.NewAppError(types.CodeDatabase, "读取调度镜像失败", listCause)
	createErr := errors.New("temporal unavailable")
	insertErr := errors.New("insert failed")
	deleteErr := errors.New("delete failed")

	validSpec := ScheduleSpec{Cron: "15 8 * * *"}
	scope := workflow.PushScope{SourceIDs: []int64{11, 22}, TopN: 3}

	tests := []struct {
		name          string
		spec          ScheduleSpec
		schedules     []types.Schedule
		listErr       error
		createErr     error
		insertErr     error
		deleteErr     error
		wantCalls     []string
		wantCode      types.ErrCode
		wantMessage   string
		wantCause     error
		wantExactErr  error
		wantDeleteErr bool
		wantSuccess   bool
		wantSDKSpec   client.ScheduleSpec
	}{
		{
			name:        "validation failure stops before I/O",
			spec:        ScheduleSpec{},
			wantCalls:   nil,
			wantCode:    types.CodeValidation,
			wantMessage: "spec 必须且只能提供 cron 或 every_seconds 之一",
		},
		{
			name:         "list failure is returned without touching Temporal",
			spec:         validSpec,
			listErr:      listErr,
			wantCalls:    []string{"store.tenant", "store.list"},
			wantCode:     types.CodeDatabase,
			wantMessage:  "读取调度镜像失败",
			wantCause:    listCause,
			wantExactErr: listErr,
		},
		{
			name:        "active limit stops after list",
			spec:        validSpec,
			schedules:   activeScheduleFixtures(maxActiveSchedules),
			wantCalls:   []string{"store.tenant", "store.list"},
			wantCode:    types.CodeValidation,
			wantMessage: "活跃定时任务已达上限（20 个）",
		},
		{
			name: "paused schedule does not consume active limit",
			spec: validSpec,
			schedules: append(
				activeScheduleFixtures(maxActiveSchedules-1),
				types.Schedule{Status: types.ScheduleStatusPaused},
			),
			wantCalls:   []string{"store.tenant", "store.list", "temporal.create", "store.insert"},
			wantSuccess: true,
			wantSDKSpec: client.ScheduleSpec{
				CronExpressions: []string{"15 8 * * *"},
				TimeZoneName:    "Asia/Shanghai",
			},
		},
		{
			name:        "Temporal create failure stops before mirror insert",
			spec:        validSpec,
			createErr:   createErr,
			wantCalls:   []string{"store.tenant", "store.list", "temporal.create"},
			wantCode:    types.CodeInternal,
			wantMessage: "创建 Temporal 定时任务失败",
			wantCause:   createErr,
		},
		{
			name:        "mirror insert failure deletes the created Temporal schedule",
			spec:        validSpec,
			insertErr:   insertErr,
			wantCalls:   []string{"store.tenant", "store.list", "temporal.create", "store.insert", "temporal.get_handle", "temporal.delete"},
			wantCode:    types.CodeDatabase,
			wantMessage: "创建定时任务镜像失败，已回滚 Temporal 调度",
			wantCause:   insertErr,
		},
		{
			name:          "compensation delete failure still returns the mirror error",
			spec:          validSpec,
			insertErr:     insertErr,
			deleteErr:     deleteErr,
			wantCalls:     []string{"store.tenant", "store.list", "temporal.create", "store.insert", "temporal.get_handle", "temporal.delete"},
			wantCode:      types.CodeDatabase,
			wantMessage:   "创建定时任务镜像失败，已回滚 Temporal 调度",
			wantCause:     insertErr,
			wantDeleteErr: true,
		},
		{
			name: "success creates active Temporal and mirror with the same ID",
			spec: validSpec,
			schedules: []types.Schedule{
				{Status: types.ScheduleStatusPaused},
			},
			wantCalls:   []string{"store.tenant", "store.list", "temporal.create", "store.insert"},
			wantSuccess: true,
			wantSDKSpec: client.ScheduleSpec{
				CronExpressions: []string{"15 8 * * *"},
				TimeZoneName:    "Asia/Shanghai",
			},
		},
		{
			name: "anchored interval success preserves every and phase",
			spec: ScheduleSpec{
				EverySeconds: 6 * 3600,
				AnchorAt:     "2026-07-19T15:00:00+08:00",
			},
			wantCalls:   []string{"store.tenant", "store.list", "temporal.create", "store.insert"},
			wantSuccess: true,
			wantSDKSpec: client.ScheduleSpec{
				Intervals: []client.ScheduleIntervalSpec{{
					Every:  6 * time.Hour,
					Offset: time.Hour,
				}},
				TimeZoneName: "Asia/Shanghai",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			trace := &createPushTrace{}
			handle := &createPushScheduleHandle{trace: trace, deleteErr: tt.deleteErr}
			scheduleClient := &createPushScheduleClient{
				trace:     trace,
				handle:    handle,
				createErr: tt.createErr,
			}
			store := &createPushStore{
				trace:     trace,
				schedules: tt.schedules,
				listErr:   tt.listErr,
				insertErr: tt.insertErr,
			}
			s := New(&createPushTemporalClient{schedules: scheduleClient}, "vane-create-tq", store)

			gotID, err := s.CreatePush(t.Context(), 42, tt.spec, scope, "每日 AI 情报")

			if !reflect.DeepEqual(trace.calls, tt.wantCalls) {
				t.Errorf("阶段顺序 = %v，期望 %v", trace.calls, tt.wantCalls)
			}
			if store.listCalls > 0 && store.listUserID != 42 {
				t.Errorf("ListSchedulesByUser userID = %d，期望 42", store.listUserID)
			}
			if tt.wantSuccess {
				if err != nil {
					t.Fatalf("CreatePush() 意外失败: %v", err)
				}
				assertCreatePushSuccess(t, gotID, tt.spec, tt.wantSDKSpec, scope, scheduleClient, store, handle)
				return
			}

			if err == nil {
				t.Fatal("CreatePush() 应失败")
			}
			if gotID != "" {
				t.Errorf("失败时返回 ID = %q，期望空串", gotID)
			}
			if got := types.CodeOf(err); got != tt.wantCode {
				t.Errorf("错误码 = %s，期望 %s（err=%v）", got, tt.wantCode, err)
			}
			var appErr *types.AppError
			if !errors.As(err, &appErr) {
				t.Fatalf("错误类型 = %T，期望 *types.AppError", err)
			}
			if appErr.Message != tt.wantMessage {
				t.Errorf("错误文案 = %q，期望 %q", appErr.Message, tt.wantMessage)
			}
			if tt.wantCause != nil && !errors.Is(err, tt.wantCause) {
				t.Errorf("错误链未保留阶段根因 %v：%v", tt.wantCause, err)
			}
			if tt.wantExactErr != nil && err != tt.wantExactErr {
				t.Errorf("list 失败应原样返回，实得 %T %v", err, err)
			}
			if tt.wantDeleteErr && errors.Is(err, deleteErr) {
				t.Errorf("当前行为只记录补偿删除错误，不应以它覆盖镜像错误：%v", err)
			}
			if tt.insertErr != nil {
				if scheduleClient.handleID != scheduleClient.createOpts.ID {
					t.Errorf("补偿删除 ID = %q，期望刚创建的 %q", scheduleClient.handleID, scheduleClient.createOpts.ID)
				}
				if handle.deleteCalls != 1 {
					t.Errorf("镜像失败应补偿删除一次，实得 %d", handle.deleteCalls)
				}
			}
		})
	}
}

func assertCreatePushSuccess(
	t *testing.T,
	gotID string,
	wantSpec ScheduleSpec,
	wantSDKSpec client.ScheduleSpec,
	wantScope workflow.PushScope,
	scheduleClient *createPushScheduleClient,
	store *createPushStore,
	handle *createPushScheduleHandle,
) {
	t.Helper()

	const prefix = "push-42-"
	if !strings.HasPrefix(gotID, prefix) || len(strings.TrimPrefix(gotID, prefix)) != 36 {
		t.Errorf("schedule ID = %q，期望 %s<uuid>", gotID, prefix)
	}
	if scheduleClient.createCalls != 1 {
		t.Errorf("Temporal Create 调用 %d 次，期望 1", scheduleClient.createCalls)
	}
	opts := scheduleClient.createOpts
	if opts.ID != gotID {
		t.Errorf("Temporal ID = %q，返回 ID = %q", opts.ID, gotID)
	}
	if opts.Paused {
		t.Error("当前 CreatePush 应创建 active Temporal schedule（Paused=false）")
	}
	if opts.Overlap != enums.SCHEDULE_OVERLAP_POLICY_SKIP {
		t.Errorf("Overlap = %s，期望 SKIP", opts.Overlap)
	}
	if !reflect.DeepEqual(opts.Spec, wantSDKSpec) {
		t.Errorf("Temporal spec = %+v，期望 %+v", opts.Spec, wantSDKSpec)
	}

	action, ok := opts.Action.(*client.ScheduleWorkflowAction)
	if !ok {
		t.Fatalf("Action 类型 = %T，期望 *client.ScheduleWorkflowAction", opts.Action)
	}
	if action.ID != "wf-"+gotID {
		t.Errorf("workflow ID = %q，期望 wf-%s", action.ID, gotID)
	}
	if action.TaskQueue != "vane-create-tq" {
		t.Errorf("TaskQueue = %q，期望 vane-create-tq", action.TaskQueue)
	}
	gotWorkflow := reflect.ValueOf(action.Workflow)
	wantWorkflow := reflect.ValueOf(workflow.PushPipelineWorkflow)
	if gotWorkflow.Kind() != reflect.Func || gotWorkflow.Pointer() != wantWorkflow.Pointer() {
		t.Errorf("Workflow = %T，期望 workflow.PushPipelineWorkflow", action.Workflow)
	}
	if len(action.Args) != 1 {
		t.Fatalf("Action Args 数量 = %d，期望 1", len(action.Args))
	}
	params, ok := action.Args[0].(workflow.PushParams)
	if !ok {
		t.Fatalf("Action Args[0] 类型 = %T，期望 workflow.PushParams", action.Args[0])
	}
	if params.TenantID != 1 {
		t.Errorf("CreatePush tenant_id = %d，期望精确活跃租户 1", params.TenantID)
	}
	if params.Snapshot != nil {
		t.Errorf("CreatePush 不应把单次 run snapshot 写入持久 Action，实得 %+v", params.Snapshot)
	}
	wantParams := workflow.PushParams{
		TenantID:      1,
		UserID:        42,
		RunKind:       workflow.PushRunKindScheduled,
		ExecutionMode: types.ExecutionModeCompiled,
		ScheduleID:    gotID,
		Scope:         wantScope,
		NLDesc:        "每日 AI 情报",
	}
	if !reflect.DeepEqual(params, wantParams) {
		t.Errorf("Action params = %+v，期望 %+v", params, wantParams)
	}

	if store.listCalls != 1 || store.insertCalls != 1 {
		t.Errorf("镜像调用次数 list=%d insert=%d，期望各 1", store.listCalls, store.insertCalls)
	}
	if store.inserted == nil {
		t.Fatal("成功路径应写入 schedule 镜像")
	}
	mirror := store.inserted
	if mirror.ID != gotID || mirror.TenantID != 1 || mirror.UserID != 42 || mirror.NLDescription != "每日 AI 情报" {
		t.Errorf("镜像身份字段 = %+v，期望与 Temporal/调用入参一致", mirror)
	}
	if mirror.Status != types.ScheduleStatusActive {
		t.Errorf("镜像 status = %q，期望 active", mirror.Status)
	}
	var gotSpec ScheduleSpec
	if err := json.Unmarshal(mirror.SpecJSON, &gotSpec); err != nil {
		t.Fatalf("解析镜像 spec_json: %v", err)
	}
	if !reflect.DeepEqual(gotSpec, wantSpec) {
		t.Errorf("镜像 spec = %+v，期望 %+v", gotSpec, wantSpec)
	}
	var gotScope workflow.PushScope
	if err := json.Unmarshal(mirror.ScopeJSON, &gotScope); err != nil {
		t.Fatalf("解析镜像 scope_json: %v", err)
	}
	if !reflect.DeepEqual(gotScope, wantScope) {
		t.Errorf("镜像 scope = %+v，期望 %+v", gotScope, wantScope)
	}
	if scheduleClient.handleCalls != 0 || handle.deleteCalls != 0 {
		t.Errorf("成功路径不应补偿删除：GetHandle=%d Delete=%d", scheduleClient.handleCalls, handle.deleteCalls)
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
	status types.ScheduleStatus

	beginCalls    int
	commitCalls   int
	rollbackCalls int
	commitErr     error
}

func (s *lifecycleScheduleStore) GetSchedule(
	_ context.Context,
	id string,
	userID int64,
) (*types.Schedule, error) {
	return &types.Schedule{
		ID: id, UserID: userID, Status: s.status,
	}, nil
}

func (s *lifecycleScheduleStore) BeginScheduleStatusChange(
	_ context.Context,
	_ string,
	_ int64,
	from types.ScheduleStatus,
	to types.ScheduleStatus,
) (
	func(context.Context) error,
	func(context.Context) error,
	error,
) {
	s.beginCalls++
	if s.status != from {
		return nil, nil, types.ErrConflict
	}
	committed := false
	commit := func(context.Context) error {
		s.commitCalls++
		if s.commitErr != nil {
			return s.commitErr
		}
		s.status = to
		committed = true
		return nil
	}
	rollback := func(context.Context) error {
		s.rollbackCalls++
		if committed {
			return nil
		}
		return nil
	}
	return commit, rollback, nil
}

func TestTaskLifecycleActionsPreserveSelectedScheduleIdentity(t *testing.T) {
	handle := &fakeScheduleHandle{}
	store := &lifecycleScheduleStore{status: types.ScheduleStatusActive}
	temporal := &fakeTemporalClient{
		sched: &fakeScheduleClient{handle: handle},
	}
	s := New(temporal, "tq", store)

	if err := s.TriggerScheduleNow(
		t.Context(), "task-web-1", 7,
	); err != nil {
		t.Fatal(err)
	}
	if handle.triggerCalls != 1 ||
		temporal.sched.gotID != "task-web-1" {
		t.Fatalf(
			"trigger calls=%d schedule=%q",
			handle.triggerCalls, temporal.sched.gotID,
		)
	}

	if err := s.PausePush(
		t.Context(), "task-web-1", 7,
	); err != nil {
		t.Fatal(err)
	}
	if store.status != types.ScheduleStatusPaused ||
		handle.pauseCalls != 1 || store.commitCalls != 1 {
		t.Fatalf(
			"status=%s pause=%d commits=%d",
			store.status, handle.pauseCalls, store.commitCalls,
		)
	}
	if err := s.TriggerScheduleNow(
		t.Context(), "task-web-1", 7,
	); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("paused trigger err=%v, want conflict", err)
	}
	if err := s.ResumePush(
		t.Context(), "task-web-1", 7,
	); err != nil {
		t.Fatal(err)
	}
	if store.status != types.ScheduleStatusActive ||
		handle.unpauseCalls != 1 || store.commitCalls != 2 {
		t.Fatalf(
			"status=%s unpause=%d commits=%d",
			store.status, handle.unpauseCalls, store.commitCalls,
		)
	}
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

// fakeScheduleStore 按预设让镜像写成功/失败，并记录收到的参数。
type fakeScheduleStore struct {
	scheduleStore
	updateErr             error
	gotID                 string
	gotSpec               json.RawMessage
	gotNLDesc             *string
	updateCall            int
	active                []types.Schedule // ReconcileActions 用例：ListActiveSchedules 返回值
	activeErr             error
	reconcileCurrent      map[string]*types.Schedule
	reconcileAcquireCalls []string
	reconcileReleaseCalls int
	reconcileAcquireErr   error
	reconcileReleaseErr   error
}

// ListActiveSchedules 供 ReconcileActions 用例注入存量调度集合。
func (f *fakeScheduleStore) ListActiveSchedules(_ context.Context) ([]types.Schedule, error) {
	active := append([]types.Schedule(nil), f.active...)
	for i := range active {
		if active[i].ExecutionMode == "" {
			active[i].ExecutionMode = types.ExecutionModeCompiled
		}
	}
	return active, f.activeErr
}

func (f *fakeScheduleStore) AcquireScheduleReconcile(
	_ context.Context,
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

// GetSchedule 一律放行：本组用例聚焦「更新 Spec 时不弄丢 Action/Policy」，
// 归属校验由 TestOwnershipCheckedBeforeTemporal 专门覆盖，不在此重复。
func (f *fakeScheduleStore) GetSchedule(_ context.Context, id string, userID int64) (*types.Schedule, error) {
	return &types.Schedule{ID: id, UserID: userID}, nil
}

func (f *fakeScheduleStore) UpdateScheduleSpec(_ context.Context, id string, spec json.RawMessage, nlDesc *string) error {
	f.updateCall++
	f.gotID, f.gotSpec, f.gotNLDesc = id, spec, nlDesc
	return f.updateErr
}

// baseSchedule 造一个"服务端已有"的调度：cron 频率 + 完整 Action/Policy/State。
// Action/Policy 是本组用例的核心断言对象——更新频率绝不能把它们弄丢。
func baseSchedule() client.Schedule {
	return client.Schedule{
		Action: &client.ScheduleWorkflowAction{
			ID:        "wf-push-1-abc",
			TaskQueue: "vane-tq",
			Args:      []any{"preserved"},
		},
		Spec: &client.ScheduleSpec{
			CronExpressions: []string{"0 8 * * *"},
			TimeZoneName:    "Asia/Shanghai",
		},
		Policy: &client.SchedulePolicies{Overlap: enums.SCHEDULE_OVERLAP_POLICY_SKIP},
		State:  &client.ScheduleState{Note: "原始状态"},
	}
}

func newUpdateFixture(mirrorErr error) (*Scheduler, *fakeScheduleHandle, *fakeScheduleStore) {
	h := &fakeScheduleHandle{current: baseSchedule()}
	fc := &fakeTemporalClient{sched: &fakeScheduleClient{handle: h}}
	st := &fakeScheduleStore{updateErr: mirrorErr}
	return New(fc, "tq", st), h, st
}

// TestUpdatePush_只换Spec保留Action与Overlap 是本能力的头号不变量：
// DoUpdate 回调必须在**当前 Schedule 之上**改 Spec，而不是新建一个 Schedule——
// 后者会静默丢掉 Action（跑哪个 workflow / 哪个 TaskQueue）与 Overlap=SKIP
// （推送堆叠护栏），表现是调度还在、却再也不推送或开始堆叠。
func TestUpdatePush_只换Spec保留Action与Overlap(t *testing.T) {
	s, h, st := newUpdateFixture(nil)

	err := s.UpdatePush(context.Background(), "push-1-abc", 1, ScheduleSpec{Cron: "30 9 * * *"}, nil)
	if err != nil {
		t.Fatalf("UpdatePush 失败: %v", err)
	}

	// Spec 换成了新 cron。
	if got := h.current.Spec.CronExpressions; len(got) != 1 || got[0] != "30 9 * * *" {
		t.Errorf("Spec 应更新为新 cron，实得 %v", got)
	}
	// Action 必须原样保留。
	act, ok := h.current.Action.(*client.ScheduleWorkflowAction)
	if !ok || act.ID != "wf-push-1-abc" || act.TaskQueue != "vane-tq" {
		t.Errorf("Action 必须原样保留（丢了就再也不推送），实得 %+v", h.current.Action)
	}
	// Overlap 策略必须原样保留。
	if h.current.Policy == nil || h.current.Policy.Overlap != enums.SCHEDULE_OVERLAP_POLICY_SKIP {
		t.Errorf("Overlap=SKIP 必须保留（丢了会推送堆叠），实得 %+v", h.current.Policy)
	}
	// State 也不该被动。
	if h.current.State == nil || h.current.State.Note != "原始状态" {
		t.Errorf("State 应原样保留，实得 %+v", h.current.State)
	}
	// 镜像同步跟上，且 spec_json 是新频率。
	if st.updateCall != 1 || st.gotID != "push-1-abc" {
		t.Errorf("应同步一次镜像，实得 call=%d id=%s", st.updateCall, st.gotID)
	}
	if !strings.Contains(string(st.gotSpec), "30 9 * * *") {
		t.Errorf("镜像 spec_json 应含新 cron，实得 %s", st.gotSpec)
	}
}

// TestUpdatePush_cron与interval互切 验证换频率类型时旧的那一组被清空——
// 否则 Temporal 会同时看到 cron 与 interval，触发时刻变成两者并集。
func TestUpdatePush_cron与interval互切(t *testing.T) {
	s, h, _ := newUpdateFixture(nil)

	// cron → interval
	if err := s.UpdatePush(context.Background(), "id", 1, ScheduleSpec{EverySeconds: 21600}, nil); err != nil {
		t.Fatalf("改成 interval 失败: %v", err)
	}
	if len(h.current.Spec.CronExpressions) != 0 {
		t.Errorf("改成 interval 后 CronExpressions 必须清空，实得 %v", h.current.Spec.CronExpressions)
	}
	if len(h.current.Spec.Intervals) != 1 || h.current.Spec.Intervals[0].Every != 6*time.Hour {
		t.Errorf("Intervals 应为 6h，实得 %+v", h.current.Spec.Intervals)
	}

	// interval → cron
	if err := s.UpdatePush(context.Background(), "id", 1, ScheduleSpec{Cron: "0 7 * * *"}, nil); err != nil {
		t.Fatalf("改回 cron 失败: %v", err)
	}
	if len(h.current.Spec.Intervals) != 0 {
		t.Errorf("改回 cron 后 Intervals 必须清空，实得 %+v", h.current.Spec.Intervals)
	}
}

// TestUpdatePush_镜像失败回滚Temporal 钉死原子性：Temporal 改成功但镜像写失败时，
// 必须把 Temporal 回滚到旧 Spec 并上抛错误——否则会留下"真按新时间推、列表显示旧时间"
// 的静默漂移，用户以为没改成、实际已经改了。
func TestUpdatePush_镜像失败回滚Temporal(t *testing.T) {
	mirrorErr := types.NewAppError(types.CodeDatabase, "模拟镜像写失败", nil)
	s, h, _ := newUpdateFixture(mirrorErr)

	err := s.UpdatePush(context.Background(), "id", 1, ScheduleSpec{Cron: "30 9 * * *"}, nil)
	if err == nil {
		t.Fatal("镜像失败必须上抛错误，不能让调用方以为改成了")
	}
	// 回滚后 Temporal 应回到旧 cron。
	if got := h.current.Spec.CronExpressions; len(got) != 1 || got[0] != "0 8 * * *" {
		t.Errorf("镜像失败后应回滚到旧 Spec，实得 %v", got)
	}
	// 两次 Update：一次改、一次回滚。
	if len(h.history) != 2 {
		t.Errorf("应发生 2 次 Update（改 + 回滚），实得 %d", len(h.history))
	}
	// 回滚也不能把 Action 弄丢。
	if act, ok := h.current.Action.(*client.ScheduleWorkflowAction); !ok || act.ID != "wf-push-1-abc" {
		t.Errorf("回滚同样必须保留 Action，实得 %+v", h.current.Action)
	}
}

// TestUpdatePush_改名即时回写Action 钉住 #3 修复：连带改名（nlDesc != nil）时，
// 同一次原子 Update 必须连 Action.Args 的任务名一起换——NLDesc 冻结在建调度时的
// 入参里，只换 Spec 会让聚合卡 header 永久显示旧任务名。
func TestUpdatePush_改名即时回写Action(t *testing.T) {
	h := &fakeScheduleHandle{current: reconcileSchedule("wf-push-1-abc",
		[]interface{}{payloadArg(t, makePushParams(0, 1, "push-1-abc", workflow.PushScope{}, "旧任务名"))})}
	fc := &fakeTemporalClient{sched: &fakeScheduleClient{handle: h}}
	st := &fakeScheduleStore{}
	s := New(fc, "tq", st)

	newName := "新任务名"
	err := s.UpdatePush(context.Background(), "push-1-abc", 1, ScheduleSpec{Cron: "30 9 * * *"}, &newName)
	if err != nil {
		t.Fatalf("UpdatePush 失败: %v", err)
	}

	// Spec 与任务名在同一次 Update 里换掉。
	if got := h.current.Spec.CronExpressions; len(got) != 1 || got[0] != "30 9 * * *" {
		t.Errorf("Spec 应更新为新 cron，实得 %v", got)
	}
	act := h.current.Action.(*client.ScheduleWorkflowAction)
	got, ok := act.Args[0].(workflow.PushParams)
	if !ok {
		t.Fatalf("Args[0] 应为 PushParams，实得 %T", act.Args[0])
	}
	if got.NLDesc != "新任务名" {
		t.Errorf("Action 入参任务名应即时回写，实得 %q", got.NLDesc)
	}
	if got.ScheduleID != "push-1-abc" || got.UserID != 1 {
		t.Errorf("schedule_id/user_id 应正确，实得 %+v", got)
	}
	// Action 其余字段与 Policy 原样保留。
	if act.ID != "wf-push-1-abc" || act.TaskQueue != "vane-tq" {
		t.Errorf("Action 其余字段必须保留，实得 %+v", act)
	}
	if h.current.Policy == nil || h.current.Policy.Overlap != enums.SCHEDULE_OVERLAP_POLICY_SKIP {
		t.Error("Overlap=SKIP 必须保留")
	}
	// 镜像同步收到新名。
	if st.gotNLDesc == nil || *st.gotNLDesc != "新任务名" {
		t.Errorf("镜像应收到新任务名，实得 %v", st.gotNLDesc)
	}
}

// TestUpdatePush_改名镜像失败回滚Action与Spec 钉住：改名+改频的复合 Update 在镜像
// 失败时，Spec 与 Action 入参要一起回到旧值——只回 Spec 会留下「镜像旧名 / Action
// 新名」的反向漂移（与原来「镜像新名 / Action 旧名」同样坏）。
func TestUpdatePush_改名镜像失败回滚Action与Spec(t *testing.T) {
	oldArgs := []interface{}{payloadArg(t, makePushParams(0, 1, "push-1-abc", workflow.PushScope{}, "旧任务名"))}
	h := &fakeScheduleHandle{current: reconcileSchedule("wf-push-1-abc", oldArgs)}
	fc := &fakeTemporalClient{sched: &fakeScheduleClient{handle: h}}
	st := &fakeScheduleStore{updateErr: types.NewAppError(types.CodeDatabase, "模拟镜像写失败", nil)}
	s := New(fc, "tq", st)

	newName := "新任务名"
	err := s.UpdatePush(context.Background(), "push-1-abc", 1, ScheduleSpec{Cron: "30 9 * * *"}, &newName)
	if err == nil {
		t.Fatal("镜像失败必须上抛错误，不能让调用方以为改成了")
	}
	// 两次 Update：一次改、一次回滚。
	if len(h.history) != 2 {
		t.Fatalf("应发生 2 次 Update（改 + 回滚），实得 %d", len(h.history))
	}
	// Spec 回到旧 cron。
	if got := h.current.Spec.CronExpressions; len(got) != 1 || got[0] != "30 8 * * *" {
		t.Errorf("Spec 应回滚到旧 cron，实得 %v", got)
	}
	// Action 入参回到旧任务名（回滚恢复的是原始 payload，需解码验证）。
	act := h.current.Action.(*client.ScheduleWorkflowAction)
	pl, ok := act.Args[0].(*commonpb.Payload)
	if !ok {
		t.Fatalf("回滚后 Args[0] 应为原始 payload，实得 %T", act.Args[0])
	}
	var got workflow.PushParams
	if err := converter.GetDefaultDataConverter().FromPayload(pl, &got); err != nil {
		t.Fatalf("解码回滚后的入参失败: %v", err)
	}
	if got.NLDesc != "旧任务名" {
		t.Errorf("回滚后任务名应恢复旧值，实得 %q", got.NLDesc)
	}
}

// TestUpdatePush_改名回滚旧入参为空也恢复 钉住回滚守卫用 wantArgs（写入侧标记）而非
// oldArgs：旧 Action 入参为 nil（坏调度）时，回滚同样必须把已写入的新入参清回去——
// 用 oldArgs != nil 当守卫会在此漏恢复，留下「镜像旧名 / Action 新名」漂移。
func TestUpdatePush_改名回滚旧入参为空也恢复(t *testing.T) {
	h := &fakeScheduleHandle{current: reconcileSchedule("wf-push-1-nil", nil)}
	fc := &fakeTemporalClient{sched: &fakeScheduleClient{handle: h}}
	st := &fakeScheduleStore{updateErr: types.NewAppError(types.CodeDatabase, "模拟镜像写失败", nil)}
	s := New(fc, "tq", st)

	newName := "新任务名"
	err := s.UpdatePush(context.Background(), "push-1-nil", 1, ScheduleSpec{Cron: "30 9 * * *"}, &newName)
	if err == nil {
		t.Fatal("镜像失败必须上抛错误")
	}
	act := h.current.Action.(*client.ScheduleWorkflowAction)
	if len(act.Args) != 0 {
		t.Errorf("回滚后入参应恢复为空（旧值），实得 %d 个", len(act.Args))
	}
}

// TestUpdatePush_校验失败不碰Temporal 确保非法 spec 在进 Temporal 之前就被拒。
func TestUpdatePush_校验失败不碰Temporal(t *testing.T) {
	for name, spec := range map[string]ScheduleSpec{
		"两者都给":     {Cron: "0 8 * * *", EverySeconds: 7200},
		"都不给":      {},
		"间隔低于地板":   {EverySeconds: 1800},
		"cron分钟过细": {Cron: "*/30 * * * *"},
		"cron六段带秒": {Cron: "0 30 8 * * *"},
	} {
		t.Run(name, func(t *testing.T) {
			s, h, st := newUpdateFixture(nil)
			err := s.UpdatePush(context.Background(), "id", 1, spec, nil)
			if err == nil {
				t.Fatal("非法 spec 应被拒")
			}
			if !errors.Is(err, types.ErrValidation) {
				t.Errorf("应是 CodeValidation，实得 %v", err)
			}
			if len(h.history) != 0 || st.updateCall != 0 {
				t.Error("校验失败绝不能碰 Temporal 或镜像")
			}
		})
	}
}

// TestUpdatePush_NotFound 目标调度不存在时应翻成 CodeNotFound（而非笼统 Internal），
// 让 API 回 404、agent 把「任务不存在」作为可自纠文案回给模型。
func TestUpdatePush_NotFound(t *testing.T) {
	s, _, st := newUpdateFixture(nil)
	s.c.(*fakeTemporalClient).sched.handle.updateErr = serviceerror.NewNotFound("schedule not found")

	err := s.UpdatePush(context.Background(), "missing", 1, ScheduleSpec{Cron: "0 8 * * *"}, nil)
	if err == nil {
		t.Fatal("不存在的调度应报错")
	}
	if !errors.Is(err, types.ErrNotFound) {
		t.Errorf("应是 CodeNotFound，实得 %v", err)
	}
	if st.updateCall != 0 {
		t.Error("Temporal 都没改成，不该去写镜像")
	}
}

// TestUpdatePush_nlDesc透传 nil=不改描述、非 nil=改，指针语义必须原样传到 store。
func TestUpdatePush_nlDesc透传(t *testing.T) {
	s, _, st := newUpdateFixture(nil)
	if err := s.UpdatePush(context.Background(), "id", 1, ScheduleSpec{Cron: "0 8 * * *"}, nil); err != nil {
		t.Fatalf("失败: %v", err)
	}
	if st.gotNLDesc != nil {
		t.Error("nil 应原样透传（表示不改描述）")
	}

	desc := "每天早上 8 点半"
	s2, _, st2 := newUpdateFixture(nil)
	if err := s2.UpdatePush(context.Background(), "id", 1, ScheduleSpec{Cron: "30 8 * * *"}, &desc); err != nil {
		t.Fatalf("失败: %v", err)
	}
	if st2.gotNLDesc == nil || *st2.gotNLDesc != desc {
		t.Errorf("描述应透传，实得 %v", st2.gotNLDesc)
	}
}

// ============================================================
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

// TestOwnershipCheckedBeforeTemporal 钉住本次修复最关键的一条：
// **归属校验必须发生在动 Temporal 之前**。
//
// Temporal 的删除不可逆——若实现写成「先 h.Delete(ctx) 再校验」，那么校验失败时
// 受害者的调度已经被销毁，校验形同虚设。本用例用一个必然 panic 的 Temporal client
// 反证：越权请求若真到达 Temporal，就会 panic；能干净地拿到 NotFound，
// 恰恰证明它在此之前就被拦下了。
func TestOwnershipCheckedBeforeTemporal(t *testing.T) {
	st := &ownershipStore{ownerUserID: 1}
	// c 为 nil：任何触碰 Temporal 的操作都会 panic。
	s := &Scheduler{st: st}

	err := s.DeletePush(context.Background(), "push-1-victim", 999) // 攻击者 999
	if err == nil {
		t.Fatal("越权删除应被拒")
	}
	var ae *types.AppError
	if !errors.As(err, &ae) || ae.Code != types.CodeNotFound {
		t.Errorf("应回 NotFound（不泄漏他人调度存在性），实得 %v", err)
	}
	if st.getCalls != 1 {
		t.Errorf("应先查归属一次，实得 %d", st.getCalls)
	}
	if st.deleteCalls != 0 {
		t.Error("越权请求不得删除镜像")
	}
}

// ============================================================
// ReconcileActions：给 b1 之前建的存量调度补齐 Action.Args 里的 schedule_id。
// 复用 UpdatePush 那套 ScheduleClient/Handle 替身，额外让 Handle 支持 Describe。
// ============================================================

// payloadArg 把一个 PushParams 编码成 Describe 返回的原始态（*commonpb.Payload），
// 塞进 Action.Args——模拟 Temporal 服务端持有的冻结入参。
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

// TestReconcileActions_补齐缺失的scheduleID 是本能力的头号不变量：
// 存量调度 Action.Args 冻结着 b1 之前的旧入参（无 schedule_id），reconcile 后必须带上
// schedule_id 与 NLDesc，且**只换 Args**——Workflow/TaskQueue/Spec/Overlap/State 全保留。
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
// （改名发生在 UpdatePush 即时回写能力上线之前、或即时回写失败过的调度），
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

func TestCreatePush_ComposesRunOutcomeAllowAll(t *testing.T) {
	trace := &createPushTrace{}
	handle := &createPushScheduleHandle{trace: trace}
	scheduleClient := &createPushScheduleClient{
		trace: trace, handle: handle,
	}
	st := &createPushStore{trace: trace}
	s := New(
		&createPushTemporalClient{schedules: scheduleClient},
		"vane-create-tq", st,
		WithCompiledRuntimeRollout(true, "", true),
		WithRunOutcomeRollout(true, "", true),
	)
	if _, err := s.CreatePush(
		t.Context(), 42, ScheduleSpec{Cron: "15 8 * * *"},
		workflow.PushScope{}, "canary",
	); err != nil {
		t.Fatal(err)
	}
	action := scheduleClient.createOpts.Action.(*client.ScheduleWorkflowAction)
	params := action.Args[0].(workflow.PushParams)
	if params.RuntimeVersion != workflow.CompiledRuntimeRunOutcomeV1 {
		t.Fatalf("create runtime = %q", params.RuntimeVersion)
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

func TestUpdatePush_RenamePreservesRunOutcomeCanary(t *testing.T) {
	const taskID = "task-run-outcome-rename"
	h := &fakeScheduleHandle{current: reconcileSchedule(
		"wf-"+taskID,
		[]interface{}{payloadArg(t, makePushParams(
			1, 1, taskID, workflow.PushScope{}, "old",
		))},
	)}
	fc := &fakeTemporalClient{sched: &fakeScheduleClient{handle: h}}
	st := &fakeScheduleStore{}
	s := New(
		fc, "tq", st,
		WithCompiledRuntimeRollout(true, taskID, false),
		WithRunOutcomeRollout(true, taskID, false),
	)
	name := "new"
	if err := s.UpdatePush(
		t.Context(), taskID, 1, ScheduleSpec{Cron: "30 9 * * *"}, &name,
	); err != nil {
		t.Fatal(err)
	}
	got := h.current.Action.(*client.ScheduleWorkflowAction).
		Args[0].(workflow.PushParams)
	if got.RuntimeVersion != workflow.CompiledRuntimeRunOutcomeV1 {
		t.Fatalf("renamed runtime = %q", got.RuntimeVersion)
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
