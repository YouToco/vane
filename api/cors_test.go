package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// corsMux 挂一个带 CORS 的最小 /api 面（不触库的 401 路径足以判定头行为——
// 本包测试纪律：Deps.Store 是具体类型不 mock，触库行为靠真库或生产验收）。
func corsMux(origin string) *http.ServeMux {
	mux := http.NewServeMux()
	Mount(mux, Deps{Origin: origin})
	return mux
}

const testOrigin = "https://vane.example.com"

// TestCORS_预检不进会话中间件 验证放行源的 OPTIONS 预检：无 cookie 也 204 + 全套头。
// 预检是浏览器自动请求、不带凭证；落进 requireSession 会 401，跨源 fetch 根本发不出来。
func TestCORS_预检不进会话中间件(t *testing.T) {
	req := httptest.NewRequest(http.MethodOptions, "/api/subscriptions", nil)
	req.Header.Set("Origin", testOrigin)
	req.Header.Set("Access-Control-Request-Method", "POST")
	rec := httptest.NewRecorder()
	corsMux(testOrigin).ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("预检状态码 = %d, 期望 204", rec.Code)
	}
	h := rec.Header()
	if got := h.Get("Access-Control-Allow-Origin"); got != testOrigin {
		t.Errorf("Allow-Origin = %q, 期望 %q", got, testOrigin)
	}
	if h.Get("Access-Control-Allow-Credentials") != "true" {
		t.Error("凭证跨源必须应答 Allow-Credentials: true")
	}
	if h.Get("Access-Control-Allow-Methods") == "" || h.Get("Access-Control-Allow-Headers") == "" {
		t.Error("预检应答缺 Allow-Methods / Allow-Headers")
	}
}

// TestCORS_真请求带头且会话仍生效 验证放行源的真请求：CORS 头在，401 语义不变
// ——浏览器要能读到这个 401（没有 Allow-Origin 时 fetch 拿到的是网络错误而非状态码）。
func TestCORS_真请求带头且会话仍生效(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/subscriptions", nil)
	req.Header.Set("Origin", testOrigin)
	rec := httptest.NewRecorder()
	corsMux(testOrigin).ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("无会话状态码 = %d, 期望 401（CORS 不得绕过会话中间件）", rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != testOrigin {
		t.Errorf("Allow-Origin = %q, 期望 %q", got, testOrigin)
	}
	if vary := rec.Header().Values("Vary"); len(vary) == 0 {
		t.Error("缺 Vary: Origin——缓存会把按源区分的响应错误复用")
	}
}

// TestCORS_非放行源零头 验证其他源拿不到任何 CORS 头：回显任意 Origin
// 等于把带 cookie 的 API 开放给全网页面。
func TestCORS_非放行源零头(t *testing.T) {
	for _, evil := range []string{"https://evil.example.com", "http://vane.example.com", ""} {
		req := httptest.NewRequest(http.MethodGet, "/api/subscriptions", nil)
		if evil != "" {
			req.Header.Set("Origin", evil)
		}
		rec := httptest.NewRecorder()
		corsMux(testOrigin).ServeHTTP(rec, req)

		if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
			t.Errorf("Origin=%q 不应有 Allow-Origin，实得 %q", evil, got)
		}
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("Origin=%q 状态码 = %d, 期望 401（语义不变）", evil, rec.Code)
		}
	}
}

// TestCORS_未配置源全关 验证 Origin 配置为空时任何源都拿不到 CORS 头（同源部署场景）。
func TestCORS_未配置源全关(t *testing.T) {
	req := httptest.NewRequest(http.MethodOptions, "/api/subscriptions", nil)
	req.Header.Set("Origin", testOrigin)
	rec := httptest.NewRecorder()
	corsMux("").ServeHTTP(rec, req)

	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("未配置 Origin 不应放行，实得 Allow-Origin=%q", got)
	}
	// 未放行的 OPTIONS 落到会话中间件 → 401；浏览器据此判跨源失败，符合预期。
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("状态码 = %d, 期望 401", rec.Code)
	}
}

// TestCORS_预检不再广告PATCH guards the retired definition-write route at
// the browser boundary as well as at ServeMux.
func TestCORS_预检不再广告PATCH(t *testing.T) {
	req := httptest.NewRequest(http.MethodOptions, "/api/schedules/s1", nil)
	req.Header.Set("Origin", testOrigin)
	req.Header.Set("Access-Control-Request-Method", "PATCH")
	rec := httptest.NewRecorder()
	corsMux(testOrigin).ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("预检状态码 = %d, 期望 204", rec.Code)
	}
	allow := rec.Header().Get("Access-Control-Allow-Methods")
	for _, m := range []string{"GET", "POST", "DELETE"} {
		if !strings.Contains(allow, m) {
			t.Errorf("Allow-Methods 缺 %s（实得 %q）——跨源该方法的端点会被浏览器挡死", m, allow)
		}
	}
	if strings.Contains(allow, "PATCH") {
		t.Errorf("Allow-Methods must not advertise retired PATCH route: %q", allow)
	}
}

func TestCORS_AuthenticatedUnsafeMethodsRejectSameSiteForeignOrigin(
	t *testing.T,
) {
	const evilSameSiteOrigin = "https://evil.zhuoqidev.com"
	tests := []struct {
		name string
		path string
	}{
		{name: "confirm", path: "/api/task-actions/action-1/confirm"},
		{name: "cancel", path: "/api/task-actions/action-1/cancel"},
		{name: "run", path: "/api/schedules/task-1/run"},
		{name: "pause", path: "/api/schedules/task-1/pause"},
		{name: "resume", path: "/api/schedules/task-1/resume"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			actionAgent := &fakeTaskActionAgent{}
			scheduleController := &fakeScheduleActionController{}
			deps, cookie := authedDeps(t, Deps{
				Origin:      testOrigin,
				TaskAgent:   actionAgent,
				TaskActions: newFakeTaskActionStore(),
				Scheduler:   scheduleController,
			})
			mux := http.NewServeMux()
			Mount(mux, deps)
			req := httptest.NewRequest(http.MethodPost, tc.path, nil)
			req.AddCookie(cookie)
			req.Header.Set("Origin", evilSameSiteOrigin)
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)

			if rec.Code != http.StatusForbidden {
				t.Fatalf(
					"status=%d body=%s, want 403",
					rec.Code, rec.Body.String(),
				)
			}
			if actionAgent.executeCalls != 0 ||
				actionAgent.cancelCalls != 0 ||
				scheduleController.command != "" {
				t.Fatalf(
					"foreign Origin reached a side effect: agent=%+v schedule=%+v",
					actionAgent, scheduleController,
				)
			}
		})
	}
}
