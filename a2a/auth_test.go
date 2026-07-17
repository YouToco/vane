package a2a

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestRequireBearer 拒绝矩阵表驱动（契约 §9.1，仿 api/session_test.go）。
func TestRequireBearer(t *testing.T) {
	okHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	cases := []struct {
		name       string
		token      string // 服务端配置
		authHeader string
		wantStatus int
	}{
		{"无Authorization头", "secret", "", http.StatusUnauthorized},
		{"错token", "secret", "Bearer wrong", http.StatusUnauthorized},
		{"非Bearer形态", "secret", "Basic secret", http.StatusUnauthorized},
		{"空配置token恒401", "", "Bearer ", http.StatusUnauthorized},
		{"空配置token空串也401", "", "Bearer", http.StatusUnauthorized},
		{"正确token放行", "secret", "Bearer secret", http.StatusOK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := requireBearer(tc.token, okHandler)
			req := httptest.NewRequest(http.MethodPost, "/a2a", nil)
			if tc.authHeader != "" {
				req.Header.Set("Authorization", tc.authHeader)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != tc.wantStatus {
				t.Fatalf("status=%d，期望 %d", rec.Code, tc.wantStatus)
			}
			if tc.wantStatus == http.StatusUnauthorized && rec.Header().Get("WWW-Authenticate") != "Bearer" {
				t.Error("401 应带 WWW-Authenticate: Bearer")
			}
		})
	}
}

// TestAuthFailLimiter 同 IP 1 分钟 10 次失败即拒（契约 §5.7）；不同 IP 互不影响；
// 成功请求不计数。
func TestAuthFailLimiter(t *testing.T) {
	h := requireBearer("secret", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	do := func(ip, auth string) int {
		req := httptest.NewRequest(http.MethodPost, "/a2a", nil)
		req.RemoteAddr = ip + ":12345"
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec.Code
	}
	for i := 0; i < 10; i++ {
		if got := do("10.0.0.1", "Bearer wrong"); got != http.StatusUnauthorized {
			t.Fatalf("第 %d 次失败应 401，实际 %d", i+1, got)
		}
	}
	if got := do("10.0.0.1", "Bearer secret"); got != http.StatusTooManyRequests {
		t.Fatalf("超阈值后应 429（连正确 token 也拒），实际 %d", got)
	}
	if got := do("10.0.0.2", "Bearer secret"); got != http.StatusOK {
		t.Fatalf("其他 IP 不应受影响，实际 %d", got)
	}
}

// TestClientIP X-Forwarded-For 首段优先，缺失回退 RemoteAddr。
func TestClientIP(t *testing.T) {
	cases := []struct {
		xff, remote, want string
	}{
		{"1.2.3.4, 5.6.7.8", "9.9.9.9:1", "1.2.3.4"},
		{" 1.2.3.4 ", "9.9.9.9:1", "1.2.3.4"},
		{"", "9.9.9.9:1234", "9.9.9.9"},
	}
	for i, tc := range cases {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.RemoteAddr = tc.remote
			if tc.xff != "" {
				req.Header.Set("X-Forwarded-For", tc.xff)
			}
			if got := clientIP(req); got != tc.want {
				t.Fatalf("clientIP=%q，期望 %q", got, tc.want)
			}
		})
	}
}
