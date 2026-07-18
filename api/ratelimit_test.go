package api

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestLoginLimiter(t *testing.T) {
	l := newAuthLimiter()
	now := time.Now()
	ip := "1.2.3.4"

	// 前 maxFails 次尝试都应放行，逐次记失败。
	for i := 0; i < l.max; i++ {
		if !l.allowAndRecord(ip, now) {
			t.Fatalf("第 %d 次尝试应放行", i+1)
		}
		l.allowAndRecord(ip, now)
	}
	// 达到阈值后拒绝。
	if l.allowAndRecord(ip, now) {
		t.Fatal("达到失败上限后应拒绝")
	}
	// 另一个 IP 不受影响（per-IP 隔离）。
	if !l.allowAndRecord("5.6.7.8", now) {
		t.Fatal("不同 IP 不应被牵连")
	}
	// 窗口滑过后恢复。
	if !l.allowAndRecord(ip, now.Add(2*time.Minute)) {
		t.Fatal("窗口过期后应恢复放行")
	}
}

// TestClientIP 单跳可信反代取 IP（CWE-348 回归）：loopback 直连才采信 XFF，且取
// 最右段（Caddy 追加的真实 peer）；非 loopback 直连一律无视 XFF、用 RemoteAddr。
func TestClientIP(t *testing.T) {
	cases := []struct {
		name string
		xff  string
		addr string
		want string
	}{
		// 经 Caddy（loopback 直连）：客户端伪造的最左段被忽略，取 Caddy 追加的最右段。
		{"经 Caddy 多段取最右", "1.2.3.4, 203.0.113.7", "127.0.0.1:5000", "203.0.113.7"},
		{"经 Caddy 单段", " 203.0.113.7 ", "127.0.0.1:5000", "203.0.113.7"},
		{"经 Caddy 无 XFF 回退 loopback", "", "127.0.0.1:5000", "127.0.0.1"},
		{"IPv6 loopback 同理", "8.8.8.8", "[::1]:5000", "8.8.8.8"},
		// 关键安全用例：非 loopback 直连（如 8080 意外暴露公网）伪造 XFF 一律无视。
		{"直连伪造 XFF 被无视", "1.2.3.4", "9.9.9.9:1234", "9.9.9.9"},
		{"直连无 XFF 用 RemoteAddr", "", "9.9.9.9:1234", "9.9.9.9"},
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

// TestLoginLimiterNotBypassedBySpoofedXFF 端到端（CWE-348 回归）：经 Caddy（loopback
// 直连）时，攻击者每请求伪造不同的 XFF 最左段试图让每次都成"新 IP"绕过登录限流——
// 但 Caddy 追加的真实 peer 恒定在最右，限流按最右段计数，连续失败仍被封成 429。
func TestLoginLimiterNotBypassedBySpoofedXFF(t *testing.T) {
	s := &server{
		deps:    Deps{Auth: newFakeAuthStore()},
		limiter: newAuthLimiter(),
	}
	do := func(spoofLeft string) int {
		body := strings.NewReader(`{"password":"wrong"}`)
		req := httptest.NewRequest("POST", "/api/auth/login", body)
		req.RemoteAddr = "127.0.0.1:5000" // 经 Caddy
		// 攻击者每次换一个伪造最左段，Caddy 追加的真实 peer 恒定在最右。
		req.Header.Set("X-Forwarded-For", spoofLeft+", 203.0.113.7")
		rec := httptest.NewRecorder()
		s.handleLogin(rec, req)
		return rec.Code
	}
	// 前 maxFails 次失败尝试（各带不同伪造最左段）都应是普通的 401。
	for i := 0; i < s.limiter.max; i++ {
		if got := do(fmt.Sprintf("10.0.0.%d", i)); got != http.StatusUnauthorized {
			t.Fatalf("第 %d 次失败应 401，实际 %d", i+1, got)
		}
	}
	// 再换一个从未出现过的伪造最左段：若限流键取了最左段，这将是"新 IP"而被放行；
	// 取最右段（真实 peer）才会因额度已满而 429。
	if got := do("10.0.0.250"); got != http.StatusTooManyRequests {
		t.Fatalf("伪造 XFF 最左段不应绕过登录限流，实际 %d", got)
	}
}
