package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/YouToco/vane/server/scheduler"
)

type fakeScheduler struct {
	gotID     string
	gotSpec   scheduler.ScheduleSpec
	gotNLDesc *string
	retErr    error
	calls     int
}

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

func newScheduleMux(t *testing.T, sched any) *http.ServeMux {
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

func TestUpdateScheduleEndpointRemoved(t *testing.T) {
	f := &fakeScheduler{}
	w := patchSchedule(t, newScheduleMux(t, f), "push-1-abc",
		`{"spec":{"cron":"30 9 * * *","tz":"Asia/Shanghai"}}`)

	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("PATCH /api/schedules/{id} status=%d body=%s, want 405",
			w.Code, w.Body.String())
	}
	if f.calls != 0 {
		t.Fatalf("retired PATCH reached scheduler %d time(s)", f.calls)
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
