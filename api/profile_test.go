package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

const profilePath = "/api/profile"

// newProfileMux 起一个挂好路由的 mux 与一张有效会话 cookie。
// Deps.Store 刻意留 nil（同 newObsMux）：本文件只走碰不到 Store 的路径——
// handleProfile 在 requireSession 放行后才碰 Store，故只测未授权被挡住的分支，
// 带真 owner+画像的 200/404 路径需真 Postgres，按本包既定纪律不 mock（见
// observability_test.go 头注释：Deps.Store 是具体类型，不为测试改成接口）。
func newProfileMux(t *testing.T) *http.ServeMux {
	t.Helper()
	mux := http.NewServeMux()
	Mount(mux, Deps{Password: "profile-test-password"})
	return mux
}

// TestProfileRequiresSession 验证 GET /api/profile 受会话中间件保护，
// 并顺带证明路由确实挂上了（没挂 ServeMux 会回 404 而非 401）。
// 画像页是"零新增鉴权面"，前提就是它老实待在 /api/ 前缀下继承会话中间件。
func TestProfileRequiresSession(t *testing.T) {
	mux := newProfileMux(t)

	// 无 cookie：requireSession 在碰 Store 前就回 401。
	r := httptest.NewRequest(http.MethodGet, profilePath, nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("未登录状态码 = %d, 期望 401", w.Code)
	}

	// 伪造 cookie 同样挡住（签名过不了）。
	r = httptest.NewRequest(http.MethodGet, profilePath, nil)
	r.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "forged.token"})
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, r)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("伪造 token 状态码 = %d, 期望 401", w.Code)
	}
}

// TestProfileRouteMounted 用一张有效 cookie 证明路由已挂载：请求能穿过会话中间件
// 到达 handler（不再是 401）。Store 为 nil，handler 走到 ownerUserID 会 panic，
// 故用 recover 捕获——能 panic 恰恰说明已过中间件、进了 handleProfile；
// 若路由没挂，ServeMux 会回 404 且不 panic，测试据此失败。
func TestProfileRouteMounted(t *testing.T) {
	mux := newProfileMux(t)
	token, exp := newSessions("profile-test-password").issue(time.Now())
	cookie := &http.Cookie{Name: sessionCookieName, Value: token, Expires: exp}

	defer func() {
		if recover() == nil {
			t.Fatal("期望 handler 因 Store=nil 而 panic（证明已过中间件进入 handleProfile），却未 panic——路由可能未挂载")
		}
	}()

	r := httptest.NewRequest(http.MethodGet, profilePath, nil)
	r.AddCookie(cookie)
	mux.ServeHTTP(httptest.NewRecorder(), r)
}
