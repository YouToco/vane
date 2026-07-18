// Bearer 认证（契约 §5.3）：每请求认证、无会话概念，与 api/ 的 cookie 体系零交集。
package a2a

import (
	"crypto/subtle"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// authFailLimiter 按客户端 IP 统计认证失败（仿 api/ratelimit.go 的 loginLimiter，
// 该实现未导出故本包自持一份；阈值对齐其量级，契约 §5.7）。
// 只在失败时计数，成功不计；窗口内存态，进程重启即清零。
type authFailLimiter struct {
	mu       sync.Mutex
	fails    map[string][]time.Time
	window   time.Duration
	maxFails int
}

func newAuthFailLimiter() *authFailLimiter {
	return &authFailLimiter{
		fails:    make(map[string][]time.Time),
		window:   time.Minute,
		maxFails: 10, // 同 IP 每分钟最多 10 次失败（§5.7）
	}
}

func (l *authFailLimiter) allow(ip string, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.prune(ip, now)
	return len(l.fails[ip]) < l.maxFails
}

// maxTrackedIPs 是失败记录 map 的硬上限（审查提示：prune 只在同 IP 复访时触发，
// 一次性失败的 IP 条目会永久滞留——IPv6 轮换可让内存无界增长）。超限先全局清窗口外
// 条目，仍超则随机逐出到上限内：宁可放过攻击者一次尝试，不让内存被打爆。
const maxTrackedIPs = 4096

func (l *authFailLimiter) recordFailure(ip string, now time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.prune(ip, now)
	l.fails[ip] = append(l.fails[ip], now)
	if len(l.fails) <= maxTrackedIPs {
		return
	}
	for tracked := range l.fails {
		l.prune(tracked, now)
	}
	for tracked := range l.fails {
		if len(l.fails) <= maxTrackedIPs {
			break
		}
		if tracked != ip {
			delete(l.fails, tracked)
		}
	}
}

func (l *authFailLimiter) prune(ip string, now time.Time) {
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

// clientIP 取用于限流的客户端 IP（审查 CONFIRMED HIGH：不能信任 XFF 最左段）。
// 部署拓扑是**单跳可信反代**（Caddy network_mode:host 反代 localhost:8080）：
//   - 直连来自本机 loopback（即经 Caddy）→ 采信 XFF **最右段**：Caddy 把真实 peer IP
//     追加到客户端已带的 XFF 之后，最右一跳是 Caddy 亲自记录的真实来源，客户端伪造的值
//     只能落在左侧、被忽略；
//   - 直连非 loopback（如 8080 意外暴露公网被直击）→ 一律用 RemoteAddr、无视 XFF：
//     否则攻击者自带任意 XFF 即可让每个请求都成"新 IP"，把唯一的 Bearer 暴力限流冲垮，
//     或伪造受害 peer 的 IP 打满其失败额度做定向 429 锁定。
//
// （api/ratelimit.go 的 clientIP 有同源缺陷——存量 dashboard 限流，已另立 task 跟进。）
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

// requireBearer 校验 Authorization: Bearer <token>。比对用 subtle.ConstantTimeCompare
// （防时序侧信道）；失败 401 + WWW-Authenticate: Bearer。token 空串 → 恒 401（不认可
// 任何请求，对齐 api/session.go 的 disabled 语义）。card 端点不经过本中间件（Mount 分路由）。
func requireBearer(token string, next http.Handler) http.Handler {
	lim := newAuthFailLimiter()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip := clientIP(r)
		now := time.Now()
		if !lim.allow(ip, now) {
			http.Error(w, "too many failed attempts", http.StatusTooManyRequests)
			return
		}
		got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		// token 空串恒拒：空 token 与"客户端也送空串"匹配等于无鉴权。
		if !ok || token == "" ||
			subtle.ConstantTimeCompare([]byte(got), []byte(token)) != 1 {
			lim.recordFailure(ip, now)
			w.Header().Set("WWW-Authenticate", "Bearer")
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}
