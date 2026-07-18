package api

import (
	"errors"
	"testing"
)

// 复现「issueSession 失败」场景：注册事务已提交，CreateSession 失败。
func TestSkeptic1_RegisterSessionFailure(t *testing.T) {
	mux, fake := newAuthTestServer(t)
	fake.createErr = errors.New("connection pool exhausted")

	rec := postJSON(t, mux, "/api/auth/register", map[string]string{
		"email":       "victim@example.com",
		"password":    "correct-password-123",
		"invite_code": "GOOD-CODE",
	}, nil)

	t.Logf("状态码 = %d", rec.Code)
	t.Logf("响应体 = %q", rec.Body.String())
	t.Logf("Set-Cookie = %v", rec.Result().Cookies())

	// 关键问题：受害者能否自助恢复？用刚才设的密码直接登录。
	fake.createErr = nil // 第 2 条发现的 DoS 窗口过去后
	rec2 := postJSON(t, mux, "/api/auth/login", map[string]string{
		"email":    "victim@example.com",
		"password": "correct-password-123",
	}, nil)
	t.Logf("后续登录状态码 = %d，体 = %q", rec2.Code, rec2.Body.String())
	if rec2.Code != 200 {
		t.Fatalf("登录失败——用户确实被锁死")
	}
	c := sessionCookieFrom(t, rec2)
	t.Logf("登录拿到会话 cookie（前 8 位）= %.8s...", c.Value)

	// 再次注册确实撞「已注册」
	rec3 := postJSON(t, mux, "/api/auth/register", map[string]string{
		"email":       "victim@example.com",
		"password":    "correct-password-123",
		"invite_code": "ANOTHER-CODE",
	}, nil)
	t.Logf("重试注册 = %d %q", rec3.Code, rec3.Body.String())
}
