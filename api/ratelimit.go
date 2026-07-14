// 登录限流：公网单密码 Dashboard 的 /api/auth/login 是唯一未鉴权入口，
// 需要防在线暴力破解。这里用极简的 per-IP 滑动窗口计数（无外部依赖）。
package api

import (
	"net"
	"net/http"
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

// clientIP 取请求的客户端 IP。Caddy 反代会带 X-Forwarded-For，
// 取其首段（最初的客户端）；缺失则回退 RemoteAddr。
func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if i := indexComma(xff); i >= 0 {
			return trimSpace(xff[:i])
		}
		return trimSpace(xff)
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func indexComma(s string) int {
	for i := 0; i < len(s); i++ {
		if s[i] == ',' {
			return i
		}
	}
	return -1
}

func trimSpace(s string) string {
	start, end := 0, len(s)
	for start < end && (s[start] == ' ' || s[start] == '\t') {
		start++
	}
	for end > start && (s[end-1] == ' ' || s[end-1] == '\t') {
		end--
	}
	return s[start:end]
}
