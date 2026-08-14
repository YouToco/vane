package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestMe_ReturnsEmail：/api/auth/me 除登录态外要回显邮箱——
// 前端用户块（头像首字母/邮箱展示）依赖它；无邮箱的存量飞书用户降级为空串。
func TestMe_ReturnsEmail(t *testing.T) {
	mux, fake := newAuthTestServer(t)
	fake.addUser(t, "who@example.com", "who-password-123", 7)

	login := postJSON(t, mux, "/api/auth/login",
		map[string]string{"email": "who@example.com", "password": "who-password-123"}, nil)
	if login.Code != http.StatusOK {
		t.Fatalf("登录应成功，实得 %d: %s", login.Code, login.Body.String())
	}
	cookie := sessionCookieFrom(t, login)

	req := httptest.NewRequest(http.MethodGet, "/api/auth/me", nil)
	req.AddCookie(cookie)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("me 应 200，实得 %d: %s", rec.Code, rec.Body.String())
	}
	var body struct {
		OK       bool   `json:"ok"`
		UserID   int64  `json:"user_id"`
		TenantID int64  `json:"tenant_id"`
		Email    string `json:"email"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("解析 me 响应失败: %v", err)
	}
	if !body.OK || body.TenantID != 7 {
		t.Errorf("登录态字段异常: %+v", body)
	}
	if body.Email != "who@example.com" {
		t.Errorf("me 应回显邮箱 who@example.com，实得 %q", body.Email)
	}
}
