package api

import (
	"encoding/base64"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestSessionRoundTrip 验证签发的 token 能通过校验，且过期时刻为 now + 30 天。
func TestSessionRoundTrip(t *testing.T) {
	s := newSessions("s3cret")
	now := time.Unix(1_800_000_000, 0)

	token, exp := s.issue(now)
	if !s.verify(token, now) {
		t.Fatal("刚签发的 token 校验应通过")
	}
	if got := exp.Sub(now); got != sessionTTL {
		t.Errorf("过期时长 = %v, 期望 %v", got, sessionTTL)
	}
	// 有效期内的任意时刻都应通过。
	if !s.verify(token, now.Add(29*24*time.Hour)) {
		t.Error("有效期内（第 29 天）校验应通过")
	}
}

// TestSessionExpired 验证到期与过期的 token 被拒绝。
func TestSessionExpired(t *testing.T) {
	s := newSessions("s3cret")
	now := time.Unix(1_800_000_000, 0)
	token, _ := s.issue(now)

	// 恰在到期时刻也拒绝（now == exp 不放行）。
	if s.verify(token, now.Add(sessionTTL)) {
		t.Error("到期时刻的 token 应被拒绝")
	}
	if s.verify(token, now.Add(sessionTTL+time.Second)) {
		t.Error("过期 token 应被拒绝")
	}
}

// TestSessionTampered 验证各类篡改都被拒绝。
func TestSessionTampered(t *testing.T) {
	s := newSessions("s3cret")
	now := time.Unix(1_800_000_000, 0)
	token, _ := s.issue(now)

	encExp, sig, ok := strings.Cut(token, ".")
	if !ok {
		t.Fatal("token 格式应为 exp.sig")
	}

	// 篡改过期时间（试图延长有效期）但沿用原签名 → 签名不匹配。
	forgedExp := base64.RawURLEncoding.EncodeToString(
		[]byte(strconv.FormatInt(now.Add(365*24*time.Hour).Unix(), 10)))
	if s.verify(forgedExp+"."+sig, now) {
		t.Error("篡改过期时间的 token 应被拒绝")
	}

	// 篡改签名一个字符 → 拒绝。
	flipped := "0"
	if sig[0] == '0' {
		flipped = "1"
	}
	if s.verify(encExp+"."+flipped+sig[1:], now) {
		t.Error("篡改签名的 token 应被拒绝")
	}

	// 格式非法：无分隔符 / exp 不是 base64 / exp 解出来不是数字。
	for _, bad := range []string{
		"no-dot-here",
		"!!!." + sig,
		base64.RawURLEncoding.EncodeToString([]byte("abc")) + "." + sig,
		"",
	} {
		if s.verify(bad, now) {
			t.Errorf("非法格式 token %q 应被拒绝", bad)
		}
	}

	// 用另一个密码签发的 token → key 不同，拒绝。
	other, _ := newSessions("another-password").issue(now)
	if s.verify(other, now) {
		t.Error("其他密码签发的 token 应被拒绝")
	}
}

// TestSessionEmptyPassword 验证密码为空时任何 token 都不被认可——
// 空密码派生的 key 人人可算，必须整体禁用。
func TestSessionEmptyPassword(t *testing.T) {
	s := newSessions("")
	now := time.Unix(1_800_000_000, 0)

	// 即使是用空密码 key "正确"构造出来的 token 也要拒绝。
	token, _ := s.issue(now)
	if s.verify(token, now) {
		t.Error("空密码下自签 token 应被拒绝")
	}
	if s.verify("", now) {
		t.Error("空密码下空 token 应被拒绝")
	}
}
