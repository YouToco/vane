package scheduler

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"

	"github.com/YouToco/vane/types"
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
}

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
