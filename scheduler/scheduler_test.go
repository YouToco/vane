package scheduler

import (
	"context"
	"encoding/json"
	"errors"
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
	if got.TimeZoneName != defaultTZ {
		t.Errorf("默认时区应为 %s，实际 %s", defaultTZ, got.TimeZoneName)
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
// TriggerPushNow：嵌入 client.Client 的窄 fake 只拦截 ExecuteWorkflow
// （其余方法继承 nil 接口，误调用即 panic），无需真 Temporal 连接。
// ============================================================

// fakeTemporalClient 记录传入的 StartWorkflowOptions 并按预设返回。
type fakeTemporalClient struct {
	client.Client
	gotOptions client.StartWorkflowOptions
	retRun     client.WorkflowRun
	retErr     error
	// sched 供 UpdatePush 一组用例注入 ScheduleClient 替身；
	// 既有 TriggerPushNow 用例不设它（那条路径不碰 ScheduleClient）。
	sched *fakeScheduleClient
}

func (f *fakeTemporalClient) ScheduleClient() client.ScheduleClient { return f.sched }

func (f *fakeTemporalClient) ExecuteWorkflow(_ context.Context, options client.StartWorkflowOptions, _ interface{}, _ ...interface{}) (client.WorkflowRun, error) {
	f.gotOptions = options
	if f.retErr != nil {
		return nil, f.retErr
	}
	return f.retRun, nil
}

// fakeWorkflowRun 只实现 GetID。
type fakeWorkflowRun struct {
	client.WorkflowRun
	id string
}

func (r *fakeWorkflowRun) GetID() string { return r.id }

// TestTriggerPushNow_Success 校验确定性 workflow ID 与 AlreadyStarted 报错开关：
// 开关不置 true 时 SDK 对同 ID 在跑的 workflow 静默 attach 并正常返回，
// 并发护栏（下面的 AlreadyStarted 用例）形同虚设。
func TestTriggerPushNow_Success(t *testing.T) {
	fc := &fakeTemporalClient{retRun: &fakeWorkflowRun{id: "push-agent-42"}}
	s := New(fc, "tq", nil)
	got, err := s.TriggerPushNow(context.Background(), 42)
	if err != nil {
		t.Fatalf("TriggerPushNow 出错: %v", err)
	}
	if got != "push-agent-42" {
		t.Errorf("返回 workflow ID=%q，期望 push-agent-42", got)
	}
	if fc.gotOptions.ID != "push-agent-42" {
		t.Errorf("workflow ID=%q，期望确定性 ID push-agent-42", fc.gotOptions.ID)
	}
	if !fc.gotOptions.WorkflowExecutionErrorWhenAlreadyStarted {
		t.Error("WorkflowExecutionErrorWhenAlreadyStarted 必须为 true，否则同 ID 在跑时 SDK 静默 attach")
	}
}

// TestTriggerPushNow_AlreadyStarted 校验并发护栏：AlreadyStarted 翻译成不可重试的
// ErrValidation，文案供模型自纠（agent pushNowTool 按 errors.Is(ErrValidation) 分流）。
func TestTriggerPushNow_AlreadyStarted(t *testing.T) {
	fc := &fakeTemporalClient{
		retErr: serviceerror.NewWorkflowExecutionAlreadyStarted("already started", "req-1", "run-1"),
	}
	s := New(fc, "tq", nil)
	if _, err := s.TriggerPushNow(context.Background(), 42); err == nil {
		t.Fatal("同 ID 在跑应返回错误")
	} else {
		if !errors.Is(err, types.ErrValidation) {
			t.Errorf("应为 ErrValidation，实际 %v", err)
		}
		var ae *types.AppError
		if !errors.As(err, &ae) {
			t.Fatalf("应为 *types.AppError，实际 %T", err)
		}
		if ae.Code != types.CodeValidation {
			t.Errorf("Code=%s，期望 %s", ae.Code, types.CodeValidation)
		}
		if ae.Retryable {
			t.Error("并发护栏拒绝不应可重试")
		}
		if !strings.Contains(ae.Message, "已有一次推送正在进行") {
			t.Errorf("文案应含\"已有一次推送正在进行\"，实际 %q", ae.Message)
		}
	}
}

// TestTriggerPushNow_OtherError 校验非 AlreadyStarted 错误仍按基础设施错误上抛。
func TestTriggerPushNow_OtherError(t *testing.T) {
	fc := &fakeTemporalClient{retErr: errors.New("connection refused")}
	s := New(fc, "tq", nil)
	_, err := s.TriggerPushNow(context.Background(), 42)
	if err == nil {
		t.Fatal("基础设施错误应上抛")
	}
	if errors.Is(err, types.ErrValidation) {
		t.Errorf("非 AlreadyStarted 错误不应翻译成 ErrValidation: %v", err)
	}
	if types.CodeOf(err) != types.CodeInternal {
		t.Errorf("Code=%s，期望 %s", types.CodeOf(err), types.CodeInternal)
	}
}

// ============================================================
// UpdatePush：ScheduleClient / ScheduleHandle 双层 fake（二者都是接口）。
// 这组替身让本包第一次能测到「Temporal 成功但镜像失败」的补偿分支——
// 那是最需要验证、又最不可能在真库上自然发生的路径。
// ============================================================

// fakeScheduleClient 只拦截 GetHandle。默认返回同一个 handle（UpdatePush 单调度用例）；
// 设了 handles 映射则按 id 返回对应 handle（ReconcileActions 多调度用例）。
type fakeScheduleClient struct {
	client.ScheduleClient
	handle  *fakeScheduleHandle
	handles map[string]*fakeScheduleHandle // 非 nil 时按 id 查；缺失回退 handle
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

// fakeScheduleHandle 模拟服务端持有的当前 Schedule：Update 时真的调用 DoUpdate 回调，
// 把返回的 Schedule 落成新的当前值——这样才能断言"只有 Spec 变了、Action/Policy 没丢"。
type fakeScheduleHandle struct {
	client.ScheduleHandle
	current     client.Schedule
	updateErr   error             // 非 nil = 模拟 Temporal Update 失败
	describeErr error             // 非 nil = 模拟 Temporal Describe 失败（reconcile 用例）
	history     []client.Schedule // 每次成功 Update 后的快照（用于验回滚发生过）
}

// Describe 返回当前持有的 Schedule 快照，供 ReconcileActions 判断是否已带 schedule_id。
func (h *fakeScheduleHandle) Describe(_ context.Context) (*client.ScheduleDescription, error) {
	if h.describeErr != nil {
		return nil, h.describeErr
	}
	return &client.ScheduleDescription{Schedule: h.current}, nil
}

func (h *fakeScheduleHandle) Update(_ context.Context, o client.ScheduleUpdateOptions) error {
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

// fakeScheduleStore 按预设让镜像写成功/失败，并记录收到的参数。
type fakeScheduleStore struct {
	scheduleStore
	updateErr  error
	gotID      string
	gotSpec    json.RawMessage
	gotNLDesc  *string
	updateCall int
	active     []types.Schedule // ReconcileActions 用例：ListActiveSchedules 返回值
	activeErr  error
}

// ListActiveSchedules 供 ReconcileActions 用例注入存量调度集合。
func (f *fakeScheduleStore) ListActiveSchedules(_ context.Context) ([]types.Schedule, error) {
	return f.active, f.activeErr
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
		[]interface{}{payloadArg(t, makePushParams(1, "push-1-abc", workflow.PushScope{}, "旧任务名"))})}
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
	oldArgs := []interface{}{payloadArg(t, makePushParams(1, "push-1-abc", workflow.PushScope{}, "旧任务名"))}
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

// TestReconcileActions_已带id则跳过 守幂等：Action.Args 已与期望 params 一致
// （schedule_id + 任务名）的调度——新建的、或上次已 reconcile 过且未改名的——
// 重启时不得再写 Temporal。走真 payload 解码路径。
func TestReconcileActions_已带id则跳过(t *testing.T) {
	good := []interface{}{payloadArg(t, makePushParams(1, "push-1-new", workflow.PushScope{}, "任务名"))}
	h := &fakeScheduleHandle{current: reconcileSchedule("wf-push-1-new", good)}
	fc := &fakeTemporalClient{sched: &fakeScheduleClient{
		handles: map[string]*fakeScheduleHandle{"push-1-new": h},
	}}
	st := &fakeScheduleStore{active: []types.Schedule{{
		ID: "push-1-new", UserID: 1, NLDescription: "任务名",
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
	stale := []interface{}{payloadArg(t, makePushParams(1, "push-1-ren", workflow.PushScope{}, "旧任务名"))}
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

// TestReconcileActions_单条失败不中断 守 best-effort：一个调度 Describe 失败只跳过它，
// 不阻断其它调度，整体仍返回 nil（不因个别 reconcile 失败而拒绝启动）。
func TestReconcileActions_单条失败不中断(t *testing.T) {
	bad := &fakeScheduleHandle{describeErr: errors.New("temporal 抖动")}
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

	if err := s.ReconcileActions(context.Background()); err != nil {
		t.Fatalf("best-effort 应返回 nil，实得 %v", err)
	}
	if len(good.history) != 1 {
		t.Errorf("失败调度不该阻断后续，good 应被 Update 一次，实得 %d", len(good.history))
	}
}
