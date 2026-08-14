// 为什么没有 happy path 测试：handleObservability 拿到合法参数后就把
// s.deps.Store（具体类型 *store.Store，非接口）交给 probe.Run，走到这一步必须有
// 真 Postgres。为了可 mock 去把 Deps.Store 改成接口，是让生产代码为测试让路——
// 且 probe 的判定逻辑已由 probe 包自己的替身 Store 覆盖，api 层这里只剩
// "参数校验 + 装配"两件事，装配的正确性由编译期保证（probe.Run 签名不匹配就编不过）。
//
// 故本文件覆盖的是全部能在不起 DB 的前提下判定的路径：参数校验（handler 在碰
// Store 之前返回）、路由已挂载、会话中间件生效。
package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/YouToco/vane/probe"
)

const obsPath = "/api/admin/observability"

// TestParseWindowHours 钉死窗口参数的边界：越界一律拒绝，缺省回 24h 红线窗口。
func TestParseWindowHours(t *testing.T) {
	cases := []struct {
		name   string
		raw    string // "" 表示不带该参数
		want   time.Duration
		wantOK bool
	}{
		{"缺省即契约 24h 红线窗口", "", probe.DefaultWindow, true},
		{"下界 1", "1", time.Hour, true},
		{"常规 24", "24", 24 * time.Hour, true},
		{"上界 720", "720", 720 * time.Hour, true},
		{"下界外 0", "0", 0, false},
		{"负数", "-1", 0, false},
		{"上界外 721", "721", 0, false},
		{"非数字", "abc", 0, false},
		{"小数", "24.5", 0, false},
		{"带空格", " 24", 0, false},
		// 溢出 int64：Atoi 回 ErrRange，必须当非法而不是被截断成某个合法值。
		{"天文数字", "99999999999999999999", 0, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			q := url.Values{}
			if c.raw != "" {
				q.Set("window_hours", c.raw)
			}
			got, errMsg := parseWindowHours(q)
			if c.wantOK {
				if errMsg != "" {
					t.Fatalf("window_hours=%q 应通过，却报 %q", c.raw, errMsg)
				}
				if got != c.want {
					t.Errorf("窗口 = %v, 期望 %v", got, c.want)
				}
				return
			}
			if errMsg == "" {
				t.Fatalf("window_hours=%q 应被拒绝，却通过并回窗口 %v", c.raw, got)
			}
			if got != 0 {
				t.Errorf("拒绝时窗口应为零值，实际 %v", got)
			}
		})
	}
}

// 空值单独测：url.Values.Set("") 与"不带参数"在 Get() 下同为 ""，
// 上表用 raw=="" 表达"不带"，故 ?window_hours= 这一形状在这里单独构造。
func TestParseWindowHoursEmptyValue(t *testing.T) {
	got, errMsg := parseWindowHours(url.Values{"window_hours": []string{""}})
	if errMsg != "" {
		t.Fatalf("?window_hours= 应按缺省处理，却报 %q", errMsg)
	}
	if got != probe.DefaultWindow {
		t.Errorf("窗口 = %v, 期望缺省 %v", got, probe.DefaultWindow)
	}
}

// newObsMux 起一个挂好路由的 mux 与一张有效会话 cookie。
// Deps.Store 刻意留 nil：本文件只走碰不到 Store 的路径，若哪天有测试意外走到
// probe.Run，nil 解引用会当场 panic——比静默测了个假东西好。
func newObsMux(t *testing.T) (*http.ServeMux, *http.Cookie) {
	t.Helper()
	const pw = "obs-test-password"
	mux := http.NewServeMux()
	deps, cookie := authedDeps(t, Deps{})
	Mount(mux, deps)
	return mux, cookie
}

// TestObservabilityBadWindowHours 端到端验证越界参数回 400 + 人话错误体，
// 顺带证明路由确实挂上了（没挂的话 ServeMux 会回 404）。
func TestObservabilityBadWindowHours(t *testing.T) {
	mux, cookie := newObsMux(t)
	for _, bad := range []string{"0", "-1", "721", "abc", "24.5"} {
		t.Run(bad, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, obsPath+"?window_hours="+url.QueryEscape(bad), nil)
			r.AddCookie(cookie)
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, r)

			if w.Code != http.StatusBadRequest {
				t.Fatalf("window_hours=%q 状态码 = %d, 期望 400", bad, w.Code)
			}
			var body map[string]string
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
				t.Fatalf("响应体不是合法 JSON: %v", err)
			}
			if body["error"] == "" {
				t.Errorf("400 响应应含人话 error 字段，实际 %s", w.Body.String())
			}
		})
	}
}

// TestObservabilityRequiresSession 验证端点受会话中间件保护——
// 本端点是"零新增鉴权面"的前提就是它老老实实待在 /api/ 前缀下（见 observability.go 文件头）。
func TestObservabilityRequiresSession(t *testing.T) {
	mux, _ := newObsMux(t)

	// 无 cookie。
	r := httptest.NewRequest(http.MethodGet, obsPath, nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("未登录状态码 = %d, 期望 401", w.Code)
	}

	// 伪造 cookie 同样挡住（签名过不了）。
	r = httptest.NewRequest(http.MethodGet, obsPath, nil)
	r.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "forged.token"})
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, r)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("伪造 token 状态码 = %d, 期望 401", w.Code)
	}
}
