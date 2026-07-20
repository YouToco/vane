package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/YouToco/vane/scheduler"
	"github.com/YouToco/vane/types"
	"github.com/YouToco/vane/workflow"
)

// fakeScheduler 实现 api.Scheduler 接口（Deps.Scheduler 是接口，可替身——
// 与 Deps.Store 那条"具体类型不 mock"的纪律不同）。
// handleUpdateSchedule 全程不碰 Store，故 Store=nil 也能跑完整路径。
type fakeScheduler struct {
	createGotUserID int64
	createGotSpec   scheduler.ScheduleSpec
	createGotScope  workflow.PushScope
	createGotNLDesc string
	createRetID     string
	createRetErr    error
	createCalls     int

	gotID     string
	gotSpec   scheduler.ScheduleSpec
	gotNLDesc *string
	retErr    error
	calls     int
}

func (f *fakeScheduler) CreatePush(_ context.Context, userID int64, spec scheduler.ScheduleSpec, scope workflow.PushScope, nlDesc string) (string, error) {
	f.createCalls++
	f.createGotUserID = userID
	f.createGotSpec = spec
	f.createGotScope = scope
	f.createGotNLDesc = nlDesc
	return f.createRetID, f.createRetErr
}
func (f *fakeScheduler) PushNow(context.Context, int64, workflow.PushScope) (string, error) {
	return "", nil
}
func (f *fakeScheduler) DeletePush(context.Context, string, int64) error { return nil }
func (f *fakeScheduler) UpdatePush(_ context.Context, id string, _ int64, spec scheduler.ScheduleSpec, nlDesc *string) error {
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

func postSchedule(t *testing.T, mux *http.ServeMux, cookie *http.Cookie, body string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, "/api/schedules", strings.NewReader(body))
	if cookie != nil {
		r.AddCookie(cookie)
	}
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)
	return w
}

// TestCreateSchedule_成功透传 刻画旧 HTTP create 入口的现状：
// principal、HTTP DTO 与自然语言描述原样交给 Scheduler，成功响应固定为 201 JSON。
func TestCreateSchedule_成功透传(t *testing.T) {
	f := &fakeScheduler{createRetID: "push-4242-characterized"}
	mux, cookie := authzMux(t, 4242, 99, f)
	body := `{"spec":{"cron":"15 8 * * 1-5","tz":"Asia/Shanghai"},"scope":{"source_ids":[7,11],"top_n":3},"nl_description":"工作日早上筛选 AI 动态"}`

	w := postSchedule(t, mux, cookie, body)

	if w.Code != http.StatusCreated {
		t.Fatalf("状态码 = %d, 期望 201（body=%s）", w.Code, w.Body.String())
	}
	if got, want := w.Header().Get("Content-Type"), "application/json"; got != want {
		t.Errorf("Content-Type = %q, 期望 %q", got, want)
	}
	if got, want := w.Body.String(), "{\"schedule_id\":\"push-4242-characterized\"}\n"; got != want {
		t.Errorf("响应体 = %q, 期望 %q", got, want)
	}
	if f.createCalls != 1 {
		t.Fatalf("CreatePush 调用次数 = %d, 期望 1", f.createCalls)
	}
	if f.createGotUserID != 4242 {
		t.Errorf("userID = %d, 期望 principal 的 4242", f.createGotUserID)
	}
	wantSpec := scheduler.ScheduleSpec{Cron: "15 8 * * 1-5", TZ: "Asia/Shanghai"}
	if f.createGotSpec != wantSpec {
		t.Errorf("spec = %+v, 期望 %+v", f.createGotSpec, wantSpec)
	}
	wantScope := workflow.PushScope{SourceIDs: []int64{7, 11}, TopN: 3}
	if !reflect.DeepEqual(f.createGotScope, wantScope) {
		t.Errorf("scope = %+v, 期望 %+v", f.createGotScope, wantScope)
	}
	if f.createGotNLDesc != "工作日早上筛选 AI 动态" {
		t.Errorf("nl_description = %q, 期望原样透传", f.createGotNLDesc)
	}
}

// TestCreateSchedule_未登录不调用Scheduler 钉住鉴权位于创建副作用之前。
func TestCreateSchedule_未登录不调用Scheduler(t *testing.T) {
	f := &fakeScheduler{createRetID: "should-not-exist"}
	w := postSchedule(t, newScheduleMux(t, f), nil,
		`{"spec":{"cron":"0 8 * * *"},"nl_description":"不应创建"}`)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("状态码 = %d, 期望 401（body=%s）", w.Code, w.Body.String())
	}
	if f.createCalls != 0 {
		t.Errorf("未登录仍调用 CreatePush %d 次", f.createCalls)
	}
}

// TestCreateSchedule_请求体校验发生在Scheduler之前 刻画 JSON 上限与 DTO 前置校验。
func TestCreateSchedule_请求体校验发生在Scheduler之前(t *testing.T) {
	oversized := `{"spec":{"cron":"0 8 * * *"},"nl_description":"` +
		strings.Repeat("x", 16<<10) + `"}`
	tests := []struct {
		name     string
		body     string
		wantBody string
	}{
		{
			name:     "malformed JSON",
			body:     `{`,
			wantBody: "{\"error\":\"请求体不是合法 JSON\"}\n",
		},
		{
			name:     "超过 16KiB",
			body:     oversized,
			wantBody: "{\"error\":\"请求体不是合法 JSON\"}\n",
		},
		{
			name:     "cron 与 every 都缺失",
			body:     `{"spec":{}}`,
			wantBody: "{\"error\":\"spec 必须且只能提供 cron 或 every_seconds 之一\"}\n",
		},
		{
			name:     "cron 与 every 同时提供",
			body:     `{"spec":{"cron":"0 8 * * *","every_seconds":3600}}`,
			wantBody: "{\"error\":\"spec 必须且只能提供 cron 或 every_seconds 之一\"}\n",
		},
		{
			name:     "every 为负数",
			body:     `{"spec":{"every_seconds":-1}}`,
			wantBody: "{\"error\":\"spec 必须且只能提供 cron 或 every_seconds 之一\"}\n",
		},
		{
			name:     "every 低于一小时",
			body:     `{"spec":{"every_seconds":3599}}`,
			wantBody: "{\"error\":\"推送间隔不得小于 1 小时\"}\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &fakeScheduler{}
			deps, cookie := authedDeps(t, Deps{Scheduler: f})
			mux := http.NewServeMux()
			Mount(mux, deps)

			w := postSchedule(t, mux, cookie, tt.body)

			if w.Code != http.StatusBadRequest {
				t.Errorf("状态码 = %d, 期望 400（body=%s）", w.Code, w.Body.String())
			}
			if got := w.Body.String(); got != tt.wantBody {
				t.Errorf("响应体 = %q, 期望 %q", got, tt.wantBody)
			}
			if f.createCalls != 0 {
				t.Errorf("前置校验失败仍调用 CreatePush %d 次", f.createCalls)
			}
		})
	}
}

// TestCreateSchedule_Scheduler错误映射 刻画任务创建下游失败到旧 HTTP 契约的映射。
func TestCreateSchedule_Scheduler错误映射(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantStatus int
		wantBody   string
	}{
		{
			name:       "Validation",
			err:        types.NewAppError(types.CodeValidation, "cron 表达式无效", nil),
			wantStatus: http.StatusBadRequest,
			wantBody:   "{\"error\":\"cron 表达式无效\"}\n",
		},
		{
			name:       "Database",
			err:        types.NewAppError(types.CodeDatabase, "创建定时任务失败", nil),
			wantStatus: http.StatusInternalServerError,
			wantBody:   "{\"error\":\"创建定时任务失败\"}\n",
		},
		{
			name:       "Internal",
			err:        types.NewAppError(types.CodeInternal, "创建定时任务内部错误", nil),
			wantStatus: http.StatusInternalServerError,
			wantBody:   "{\"error\":\"创建定时任务内部错误\"}\n",
		},
		{
			name:       "裸 error",
			err:        errors.New("temporal unavailable"),
			wantStatus: http.StatusInternalServerError,
			wantBody:   "{\"error\":\"服务器内部错误，请稍后重试\"}\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &fakeScheduler{createRetErr: tt.err}
			deps, cookie := authedDeps(t, Deps{Scheduler: f})
			mux := http.NewServeMux()
			Mount(mux, deps)

			w := postSchedule(t, mux, cookie,
				`{"spec":{"cron":"0 8 * * *"},"scope":{"top_n":5},"nl_description":"错误映射"}`)

			if w.Code != tt.wantStatus {
				t.Errorf("状态码 = %d, 期望 %d（body=%s）", w.Code, tt.wantStatus, w.Body.String())
			}
			if got := w.Body.String(); got != tt.wantBody {
				t.Errorf("响应体 = %q, 期望 %q", got, tt.wantBody)
			}
			if f.createCalls != 1 {
				t.Errorf("CreatePush 调用次数 = %d, 期望 1", f.createCalls)
			}
		})
	}
}

// TestUpdateSchedule_成功透传 验证 PATCH 路由已挂载、DTO 正确翻译成 ScheduleSpec、
// 路径参数取到 id、nl_description 的指针语义原样传下去。
func TestUpdateSchedule_成功透传(t *testing.T) {
	f := &fakeScheduler{}
	w := patchSchedule(t, newScheduleMux(t, f), "push-1-abc",
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
	w := patchSchedule(t, newScheduleMux(t, f), "s1", `{"spec":{"every_seconds":7200}}`)

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

// TestUpdateSchedule_锚点透传 守 DTO 接线：json tag 拼错或 toScheduleSpec 漏一行，
// 都会让 anchor_at 静默丢失（后端仍回 200，调度却按 epoch 触发）——对抗审查实测
// 这两个变异体在补本断言前都是存活的。
func TestUpdateSchedule_锚点透传(t *testing.T) {
	const anchor = "2026-07-19T20:00:00+08:00"
	f := &fakeScheduler{}
	w := patchSchedule(t, newScheduleMux(t, f), "s1",
		`{"spec":{"every_seconds":259200,"anchor_at":"`+anchor+`"}}`)

	if w.Code != http.StatusOK {
		t.Fatalf("状态码 = %d, 期望 200（body=%s）", w.Code, w.Body.String())
	}
	if f.gotSpec.AnchorAt != anchor {
		t.Errorf("anchor_at 应透传到 scheduler（丢了则相位静默失效），实得 %q", f.gotSpec.AnchorAt)
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
			w := patchSchedule(t, newScheduleMux(t, f), "s1", body)
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
	w := patchSchedule(t, newScheduleMux(t, f), "missing", `{"spec":{"cron":"0 8 * * *"}}`)

	if w.Code != http.StatusNotFound {
		t.Errorf("状态码 = %d, 期望 404（body=%s）", w.Code, w.Body.String())
	}
}

// TestUpdateSchedule_未登录401 PATCH 与其他写端点一样必须过会话中间件。
func TestUpdateSchedule_未登录401(t *testing.T) {
	r := httptest.NewRequest(http.MethodPatch, "/api/schedules/s1",
		strings.NewReader(`{"spec":{"cron":"0 8 * * *"}}`))
	w := httptest.NewRecorder()
	newScheduleMux(t, &fakeScheduler{}).ServeHTTP(w, r)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("未登录状态码 = %d, 期望 401", w.Code)
	}
}
