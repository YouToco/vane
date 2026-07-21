package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/YouToco/vane/scheduler"
	"github.com/YouToco/vane/types"
	"github.com/YouToco/vane/workflow"
)

type fakeScheduler struct {
	gotID     string
	gotSpec   scheduler.ScheduleSpec
	gotNLDesc *string
	retErr    error
	calls     int
}

func (f *fakeScheduler) PushNow(context.Context, int64, workflow.PushScope) (string, error) {
	return "", nil
}

func (f *fakeScheduler) DeletePush(context.Context, string, int64) error { return nil }

func (f *fakeScheduler) UpdatePush(
	_ context.Context,
	id string,
	_ int64,
	spec scheduler.ScheduleSpec,
	nlDesc *string,
) error {
	f.calls++
	f.gotID, f.gotSpec, f.gotNLDesc = id, spec, nlDesc
	return f.retErr
}

// schedSession 存放当前用例的会话 cookie，由 newScheduleMux 设置。
// 包级变量在此可接受：api 包的用例不并行（无 t.Parallel）。
var schedSession *http.Cookie

func newScheduleMux(t *testing.T, sched Scheduler) *http.ServeMux {
	t.Helper()
	mux := http.NewServeMux()
	deps, cookie := authedDeps(t, Deps{Scheduler: sched})
	schedSession = cookie
	Mount(mux, deps)
	return mux
}

func schedCookie(t *testing.T) *http.Cookie {
	t.Helper()
	if schedSession == nil {
		t.Fatal("请先用 newScheduleMux 建 mux")
	}
	return schedSession
}

func patchSchedule(t *testing.T, mux *http.ServeMux, id, body string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(http.MethodPatch, "/api/schedules/"+id, strings.NewReader(body))
	r.AddCookie(schedCookie(t))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)
	return w
}

// A5 删除了没有完整任务定义、确认和恢复语义的旧 HTTP 创建入口。
// Web 后续创建必须接统一 task control plane，不能悄悄复活这个中间 API。
func TestCreateScheduleEndpointRemoved(t *testing.T) {
	mux := newScheduleMux(t, &fakeScheduler{})
	r := httptest.NewRequest(http.MethodPost, "/api/schedules",
		strings.NewReader(`{"spec":{"cron":"0 8 * * *"}}`))
	r.AddCookie(schedCookie(t))
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, r)

	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST /api/schedules status=%d body=%s, want 405", w.Code, w.Body.String())
	}
}

func TestUpdateScheduleSuccess(t *testing.T) {
	f := &fakeScheduler{}
	w := patchSchedule(t, newScheduleMux(t, f), "push-1-abc",
		`{"spec":{"cron":"30 9 * * *","tz":"Asia/Shanghai"},"nl_description":"改成九点半"}`)

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s, want 200", w.Code, w.Body.String())
	}
	if f.calls != 1 || f.gotID != "push-1-abc" {
		t.Fatalf("UpdatePush calls=%d id=%q", f.calls, f.gotID)
	}
	if f.gotSpec.Cron != "30 9 * * *" || f.gotSpec.TZ != "Asia/Shanghai" {
		t.Fatalf("spec=%+v", f.gotSpec)
	}
	if f.gotNLDesc == nil || *f.gotNLDesc != "改成九点半" {
		t.Fatalf("nl_description=%v", f.gotNLDesc)
	}
}

func TestUpdateScheduleOmittedDescriptionIsNil(t *testing.T) {
	f := &fakeScheduler{}
	w := patchSchedule(t, newScheduleMux(t, f), "s1", `{"spec":{"every_seconds":7200}}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s, want 200", w.Code, w.Body.String())
	}
	if f.gotNLDesc != nil {
		t.Fatalf("omitted nl_description=%q, want nil", *f.gotNLDesc)
	}
	if f.gotSpec.EverySeconds != 7200 {
		t.Fatalf("every_seconds=%d", f.gotSpec.EverySeconds)
	}
}

func TestUpdateScheduleAnchorPassesThrough(t *testing.T) {
	const anchor = "2026-07-19T20:00:00+08:00"
	f := &fakeScheduler{}
	w := patchSchedule(t, newScheduleMux(t, f), "s1",
		`{"spec":{"every_seconds":259200,"anchor_at":"`+anchor+`"}}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s, want 200", w.Code, w.Body.String())
	}
	if f.gotSpec.AnchorAt != anchor {
		t.Fatalf("anchor_at=%q", f.gotSpec.AnchorAt)
	}
}

func TestUpdateScheduleRejectsInvalidSpecBeforeScheduler(t *testing.T) {
	for name, body := range map[string]string{
		"both":     `{"spec":{"cron":"0 8 * * *","every_seconds":7200}}`,
		"neither":  `{"spec":{}}`,
		"too fast": `{"spec":{"every_seconds":60}}`,
		"bad json": `{`,
	} {
		t.Run(name, func(t *testing.T) {
			f := &fakeScheduler{}
			w := patchSchedule(t, newScheduleMux(t, f), "s1", body)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s, want 400", w.Code, w.Body.String())
			}
			if f.calls != 0 {
				t.Fatal("invalid request reached scheduler")
			}
		})
	}
}

func TestUpdateScheduleNotFound(t *testing.T) {
	f := &fakeScheduler{retErr: types.NewAppError(types.CodeNotFound, "定时任务不存在", nil)}
	w := patchSchedule(t, newScheduleMux(t, f), "missing", `{"spec":{"cron":"0 8 * * *"}}`)
	if w.Code != http.StatusNotFound {
		t.Fatalf("status=%d body=%s, want 404", w.Code, w.Body.String())
	}
}

func TestUpdateScheduleRequiresSession(t *testing.T) {
	r := httptest.NewRequest(http.MethodPatch, "/api/schedules/s1",
		strings.NewReader(`{"spec":{"cron":"0 8 * * *"}}`))
	w := httptest.NewRecorder()
	newScheduleMux(t, &fakeScheduler{}).ServeHTTP(w, r)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d, want 401", w.Code)
	}
}
