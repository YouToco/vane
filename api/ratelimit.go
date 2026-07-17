// 登录限流：公网单密码 Dashboard 的 /api/auth/login 是唯一未鉴权入口，
// 需要防在线暴力破解。这里用极简的 per-IP 滑动窗口计数（无外部依赖）。
package api

import (
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// loginLimiter 按客户端 IP 统计最近窗口内的失败次数，超阈值即拒绝。
// 只在登录失败时计数，成功不计——正常用户输错几次仍可继续，攻击者的
// 高频尝试会被挡下。窗口内存态，进程重启即清零（对 MVP 足够）。
type loginLimiter struct {
	mu       sync.Mutex
	fails    map[string][]time.Time
	window   time.Duration
	maxFails int
}

func newLoginLimiter() *loginLimiter {
	return &loginLimiter{
		fails:    make(map[string][]time.Time),
		window:   time.Minute,
		maxFails: 10, // 每 IP 每分钟最多 10 次失败尝试
	}
}

// allow 报告该 IP 当前是否还允许尝试登录。
func (l *loginLimiter) allow(ip string, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.prune(ip, now)
	return len(l.fails[ip]) < l.maxFails
}

// recordFailure 记一次失败；顺便清理过期条目防止 map 无界增长。
func (l *loginLimiter) recordFailure(ip string, now time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.prune(ip, now)
	l.fails[ip] = append(l.fails[ip], now)
}

// prune 丢弃窗口外的失败记录；清空后删除键，避免 map 随 IP 无限膨胀。
func (l *loginLimiter) prune(ip string, now time.Time) {
	cutoff := now.Add(-l.window)
	kept := l.fails[ip][:0]
	for _, t := range l.fails[ip] {
		if t.After(cutoff) {
			kept = append(kept, t)
		}
	}
	if len(kept) == 0 {
		delete(l.fails, ip)
		return
	}
	l.fails[ip] = kept
}

// clientIP 取用于限流的客户端 IP（CWE-348：绝不能信任 X-Forwarded-For 最左段——
// 那是攻击者可完全伪造的值，取它等于把登录爆破限流的键交给攻击者，每请求换一个"新 IP"
// 即可绕过，或伪造受害者 IP 打满其失败额度做定向 429 锁定）。
// 部署拓扑是**单跳可信反代**（Caddy network_mode:host 反代 127.0.0.1:8080）：
//   - 直连来自本机 loopback（即经 Caddy）→ 采信 XFF **最右段**：Caddy 把真实 peer IP
//     追加到客户端已带的 XFF 之后，最右一跳才是 Caddy 亲自记录的真实来源，客户端伪造的
//     值只能落在左侧、被忽略；
//   - 直连非 loopback（如 8080 意外暴露公网被直击）→ 一律用 RemoteAddr、无视 XFF。
//
// 与 a2a/auth.go 的 clientIP 同源同逻辑（两处 auth 零交集、各自持一份）：A2A PR-3 审查
// 在 a2a 侧修复时标注了本处存量同缺陷待跟进，此次一并修平。
func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			parts := strings.Split(xff, ",")
			if last := strings.TrimSpace(parts[len(parts)-1]); last != "" {
				return last
			}
		}
	}
	return host
}
