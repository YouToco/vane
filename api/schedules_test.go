package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/YouToco/vane/scheduler"
	"github.com/YouToco/vane/types"
	"github.com/YouToco/vane/workflow"
)

// fakeScheduler 实现 api.Scheduler 接口（Deps.Scheduler 是接口，可替身——
// 与 Deps.Store 那条"具体类型不 mock"的纪律不同）。
// handleUpdateSchedule 全程不碰 Store，故 Store=nil 也能跑完整路径。
type fakeScheduler struct {
	gotID     string
	gotSpec   scheduler.ScheduleSpec
	gotNLDesc *string
	retErr    error
	calls     int
}

func (f *fakeScheduler) CreatePush(context.Context, int64, scheduler.ScheduleSpec, workflow.PushScope, string) (string, error) {
	return "", nil
}
func (f *fakeScheduler) PushNow(context.Context, int64, workflow.PushScope) (string, error) {
	return "", nil
}
func (f *fakeScheduler) DeletePush(context.Context, string) error { return nil }
func (f *fakeScheduler) UpdatePush(_ context.Context, id string, spec scheduler.ScheduleSpec, nlDesc *string) error {
	f.calls++
	f.gotID, f.gotSpec, f.gotNLDesc = id, spec, nlDesc
	return f.retErr
}

const schedTestPassword = "sched-test-password"

func newScheduleMux(sched Scheduler) *http.ServeMux {
	mux := http.NewServeMux()
	Mount(mux, Deps{Password: schedTestPassword, Scheduler: sched})
	return mux
}

func schedCookie(t *testing.T) *http.Cookie {
	t.Helper()
	token, exp := newSessions(schedTestPassword).issue(time.Now())
	return &http.Cookie{Name: sessionCookieName, Value: token, Expires: exp}
}

func patchSchedule(t *testing.T, mux *http.ServeMux, id, body string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(http.MethodPatch, "/api/schedules/"+id, strings.NewReader(body))
	r.AddCookie(schedCookie(t))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)
	return w
}

// TestUpdateSchedule_成功透传 验证 PATCH 路由已挂载、DTO 正确翻译成 ScheduleSpec、
// 路径参数取到 id、nl_description 的指针语义原样传下去。
func TestUpdateSchedule_成功透传(t *testing.T) {
	f := &fakeScheduler{}
	w := patchSchedule(t, newScheduleMux(f), "push-1-abc",
		`{"spec":{"cron":"30 9 * * *","tz":"Asia/Shanghai"},"nl_description":"改成九点半"}`)

	if w.Code != http.StatusOK {
		t.Fatalf("状态码 = %d, 期望 200（body=%s）", w.Code, w.Body.String())
	}
	if f.calls != 1 || f.gotID != "push-1-abc" {
		t.Errorf("应调用一次 UpdatePush 且 id 取自路径，实得 calls=%d id=%q", f.calls, f.gotID)
	}
	if f.gotSpec.Cron != "30 9 * * *" || f.gotSpec.TZ != "Asia/Shanghai" {
		t.Errorf("spec 翻译不符: %+v", f.gotSpec)
	}
	if f.gotNLDesc == nil || *f.gotNLDesc != "改成九点半" {
		t.Errorf("nl_description 应透传，实得 %v", f.gotNLDesc)
	}
}

// TestUpdateSchedule_省略描述传nil 指针语义：省略字段=不改描述，必须是 nil 而非空串，
// 否则会把用户原来的描述静默清空。
func TestUpdateSchedule_省略描述传nil(t *testing.T) {
	f := &fakeScheduler{}
	w := patchSchedule(t, newScheduleMux(f), "s1", `{"spec":{"every_seconds":7200}}`)

	if w.Code != http.StatusOK {
		t.Fatalf("状态码 = %d, 期望 200", w.Code)
	}
	if f.gotNLDesc != nil {
		t.Errorf("省略 nl_description 必须传 nil（否则清空原描述），实得 %q", *f.gotNLDesc)
	}
	if f.gotSpec.EverySeconds != 7200 {
		t.Errorf("every_seconds 应透传，实得 %d", f.gotSpec.EverySeconds)
	}
}

// TestUpdateSchedule_非法spec回400 校验在进 scheduler 之前拦住。
func TestUpdateSchedule_非法spec回400(t *testing.T) {
	for name, body := range map[string]string{
		"两者都给":   `{"spec":{"cron":"0 8 * * *","every_seconds":7200}}`,
		"都不给":    `{"spec":{}}`,
		"低于1h地板": `{"spec":{"every_seconds":60}}`,
		"body非法": `{`,
	} {
		t.Run(name, func(t *testing.T) {
			f := &fakeScheduler{}
			w := patchSchedule(t, newScheduleMux(f), "s1", body)
			if w.Code != http.StatusBadRequest {
				t.Errorf("状态码 = %d, 期望 400（body=%s）", w.Code, w.Body.String())
			}
			if f.calls != 0 {
				t.Error("校验失败不该调到 scheduler")
			}
		})
	}
}

// TestUpdateSchedule_NotFound回404 scheduler 的 CodeNotFound 应映射成 404，
// 而不是 500——"改一个不存在的任务"是客户端问题。
func TestUpdateSchedule_NotFound回404(t *testing.T) {
	f := &fakeScheduler{retErr: types.NewAppError(types.CodeNotFound, "定时任务不存在", nil)}
	w := patchSchedule(t, newScheduleMux(f), "missing", `{"spec":{"cron":"0 8 * * *"}}`)

	if w.Code != http.StatusNotFound {
		t.Errorf("状态码 = %d, 期望 404（body=%s）", w.Code, w.Body.String())
	}
}

// TestUpdateSchedule_未登录401 PATCH 与其他写端点一样必须过会话中间件。
func TestUpdateSchedule_未登录401(t *testing.T) {
	r := httptest.NewRequest(http.MethodPatch, "/api/schedules/s1",
		strings.NewReader(`{"spec":{"cron":"0 8 * * *"}}`))
	w := httptest.NewRecorder()
	newScheduleMux(&fakeScheduler{}).ServeHTTP(w, r)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("未登录状态码 = %d, 期望 401", w.Code)
	}
}
