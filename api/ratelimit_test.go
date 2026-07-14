package api

import (
	"net/http/httptest"
	"testing"
	"time"
)

func TestLoginLimiter(t *testing.T) {
	l := newLoginLimiter()
	now := time.Now()
	ip := "1.2.3.4"

	// 前 maxFails 次尝试都应放行，逐次记失败。
	for i := 0; i < l.maxFails; i++ {
		if !l.allow(ip, now) {
			t.Fatalf("第 %d 次尝试应放行", i+1)
		}
		l.recordFailure(ip, now)
	}
	// 达到阈值后拒绝。
	if l.allow(ip, now) {
		t.Fatal("达到失败上限后应拒绝")
	}
	// 另一个 IP 不受影响（per-IP 隔离）。
	if !l.allow("5.6.7.8", now) {
		t.Fatal("不同 IP 不应被牵连")
	}
	// 窗口滑过后恢复。
	if !l.allow(ip, now.Add(2*time.Minute)) {
		t.Fatal("窗口过期后应恢复放行")
	}
}

func TestClientIP(t *testing.T) {
	cases := []struct {
		name string
		xff  string
		addr string
		want string
	}{
		{"XFF 单段", "9.9.9.9", "10.0.0.1:1234", "9.9.9.9"},
		{"XFF 多段取首", "9.9.9.9, 10.0.0.1", "10.0.0.1:1234", "9.9.9.9"},
		{"无 XFF 用 RemoteAddr", "", "10.0.0.1:1234", "10.0.0.1"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := httptest.NewRequest("POST", "/api/auth/login", nil)
			r.RemoteAddr = c.addr
			if c.xff != "" {
				r.Header.Set("X-Forwarded-For", c.xff)
			}
			if got := clientIP(r); got != c.want {
				t.Errorf("clientIP = %q, 期望 %q", got, c.want)
			}
		})
	}
}
