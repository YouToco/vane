// 会话签发与校验：无状态 HMAC token，不落库。
// 格式（契约 §5）：base64(exp_unix) + "." + hex(HMAC-SHA256(key, exp_unix))，
// key = SHA256("vane-session:" + Dashboard 密码)，有效期 30 天。
package api

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"strconv"
	"strings"
	"time"
)

const (
	sessionCookieName = "vane_session"
	sessionTTL        = 30 * 24 * time.Hour
)

// sessions 持有从密码派生的 HMAC key。
// key 与密码绑定的好处：改密码即让所有旧会话自动失效，无需服务端存储会话表。
type sessions struct {
	key []byte
	// disabled：密码为空时从不签发也从不认可任何会话——
	// 空密码派生的 key 人人可算，认了等于无鉴权。
	disabled bool
}

func newSessions(password string) *sessions {
	sum := sha256.Sum256([]byte("vane-session:" + password))
	return &sessions{key: sum[:], disabled: password == ""}
}

// sign 对过期时间的十进制字符串做 HMAC-SHA256，输出 hex。
func (s *sessions) sign(expStr string) string {
	mac := hmac.New(sha256.New, s.key)
	mac.Write([]byte(expStr))
	return hex.EncodeToString(mac.Sum(nil))
}

// issue 以 now 为基准签发 token，返回 token 与过期时刻（供 Set-Cookie 的 Expires）。
// base64 选 RawURL 变体：产物只含 cookie 安全字符，避免标准变体的 '=' 填充
// 在个别代理/客户端上的兼容性问题；仅服务端自产自销，变体选择不影响契约语义。
func (s *sessions) issue(now time.Time) (string, time.Time) {
	exp := now.Add(sessionTTL)
	expStr := strconv.FormatInt(exp.Unix(), 10)
	return base64.RawURLEncoding.EncodeToString([]byte(expStr)) + "." + s.sign(expStr), exp
}

// verify 校验 token：格式、签名、有效期三关全过才放行。
// 签名对比必须用 hmac.Equal（常数时间），防时序侧信道。
func (s *sessions) verify(token string, now time.Time) bool {
	if s.disabled {
		return false
	}
	encExp, sig, ok := strings.Cut(token, ".")
	if !ok {
		return false
	}
	expBytes, err := base64.RawURLEncoding.DecodeString(encExp)
	if err != nil {
		return false
	}
	expStr := string(expBytes)
	exp, err := strconv.ParseInt(expStr, 10, 64)
	if err != nil {
		return false
	}
	if !hmac.Equal([]byte(s.sign(expStr)), []byte(sig)) {
		return false
	}
	// 到期时刻本身（now == exp）也算过期：宁可早一秒重登，不多留一秒窗口。
	return now.Unix() < exp
}
