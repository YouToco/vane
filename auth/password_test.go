package auth

import (
	"errors"
	"strings"
	"testing"
)

func TestHashPassword_RoundTrip(t *testing.T) {
	const pw = "correct horse battery staple"
	h, err := HashPassword(pw)
	if err != nil {
		t.Fatalf("哈希失败: %v", err)
	}
	if err := VerifyPassword(h, pw); err != nil {
		t.Errorf("正确密码应校验通过: %v", err)
	}
	if err := VerifyPassword(h, pw+"x"); !errors.Is(err, ErrPasswordMismatch) {
		t.Errorf("错误密码应返回 ErrPasswordMismatch，实得 %v", err)
	}
}

// TestHashPassword_SaltIsRandom：同一密码两次哈希必须不同——盐固定的话，
// 相同密码会产生相同哈希，一次撞库即可批量识别「哪些用户用了同一个弱密码」。
func TestHashPassword_SaltIsRandom(t *testing.T) {
	const pw = "same-password-twice"
	a, err := HashPassword(pw)
	if err != nil {
		t.Fatal(err)
	}
	b, err := HashPassword(pw)
	if err != nil {
		t.Fatal(err)
	}
	if a == b {
		t.Error("同一密码两次哈希不应相同（盐未随机化）")
	}
	// 但两者都必须能校验通过。
	if err := VerifyPassword(a, pw); err != nil {
		t.Error(err)
	}
	if err := VerifyPassword(b, pw); err != nil {
		t.Error(err)
	}
}

// TestHashPassword_EncodedFormat：自描述格式是「参数可演进」的前提——
// 参数写死在校验代码里的话，调高 memory 当天全体用户都会登录失败。
func TestHashPassword_EncodedFormat(t *testing.T) {
	h, err := HashPassword("format-check-password")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(h, "$argon2id$v=19$") {
		t.Errorf("哈希串前缀不符: %q", h)
	}
	if n := len(strings.Split(h, "$")); n != 6 {
		t.Errorf("哈希串应有 6 段，实得 %d: %q", n, h)
	}
}

func TestValidatePassword(t *testing.T) {
	cases := []struct {
		name string
		pw   string
		ok   bool
	}{
		{"太短", "1234567", false},
		{"恰好下限", "12345678", true},
		{"正常", "a-reasonable-password", true},
		{"恰好上限", strings.Repeat("a", MaxPasswordLen), true},
		{"超上限", strings.Repeat("a", MaxPasswordLen+1), false},
		// 中文密码按 rune 计数：8 个汉字是 24 字节，按字节判会误判为合格/不合格。
		{"中文八字", "密码密码密码密码", true},
		{"中文七字", "密码密码密码密", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := ValidatePassword(c.pw)
			if c.ok && err != nil {
				t.Errorf("应通过，实得 %v", err)
			}
			if !c.ok && err == nil {
				t.Error("应被拒绝，实际通过")
			}
		})
	}
}

// TestVerifyPassword_OversizedInputRejectedCheaply 锁住 DoS 护栏：
// 超长输入必须在进 argon2 **之前**被判不匹配。登录端点无需鉴权，
// 没有这道闸就等于给出「发 10MB 密码打满 CPU」的免费攻击面。
func TestVerifyPassword_OversizedInputRejectedCheaply(t *testing.T) {
	h, err := HashPassword("normal-password")
	if err != nil {
		t.Fatal(err)
	}
	huge := strings.Repeat("x", MaxPasswordLen+1000)
	if err := VerifyPassword(h, huge); !errors.Is(err, ErrPasswordMismatch) {
		t.Errorf("超长输入应判不匹配，实得 %v", err)
	}
}

// TestVerifyPassword_CorruptHash：哈希串损坏必须报「格式错」而非「密码错」——
// 混为一谈会把数据损坏伪装成用户输错密码，掩盖真问题。
func TestVerifyPassword_CorruptHash(t *testing.T) {
	cases := []struct{ name, hash string }{
		{"空串", ""},
		{"段数不足", "$argon2id$v=19$m=19456,t=2,p=1$salt"},
		{"算法不符", "$bcrypt$v=19$m=19456,t=2,p=1$c2FsdA$aGFzaA"},
		{"版本不符", "$argon2id$v=16$m=19456,t=2,p=1$c2FsdA$aGFzaA"},
		{"参数段非法", "$argon2id$v=19$bogus$c2FsdA$aGFzaA"},
		{"盐段非 base64", "$argon2id$v=19$m=19456,t=2,p=1$!!!$aGFzaA"},
		{"摘要为空", "$argon2id$v=19$m=19456,t=2,p=1$c2FsdA$"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := VerifyPassword(c.hash, "whatever")
			if err == nil {
				t.Fatal("损坏的哈希不应校验通过")
			}
			if errors.Is(err, ErrPasswordMismatch) {
				t.Error("损坏的哈希应报格式错，不应伪装成密码不匹配")
			}
		})
	}
}

func TestNeedsRehash(t *testing.T) {
	h, err := HashPassword("rehash-check-password")
	if err != nil {
		t.Fatal(err)
	}
	if NeedsRehash(h) {
		t.Error("当前参数生成的哈希不应需要重算")
	}
	// 旧参数（memory 更低）应被判定需要升级。
	old := "$argon2id$v=19$m=4096,t=1,p=1$c2FsdHNhbHRzYWx0c2E$aGFzaGhhc2hoYXNoaGFzaGhhc2hoYXNoaGE"
	if !NeedsRehash(old) {
		t.Error("低参数的旧哈希应需要重算")
	}
	if !NeedsRehash("garbage") {
		t.Error("解不开的哈希应需要重算")
	}
}

// TestNewSessionToken 锁住会话 token 的两条性质：足够随机、且**明文不等于存储值**。
func TestNewSessionToken(t *testing.T) {
	seen := make(map[string]bool)
	for i := 0; i < 200; i++ {
		tok, hash, err := NewSessionToken()
		if err != nil {
			t.Fatal(err)
		}
		if seen[tok] {
			t.Fatal("生成了重复的 token")
		}
		seen[tok] = true
		if len(tok) < 40 {
			t.Errorf("token 太短: %d", len(tok))
		}
		// 存储值必须是哈希，不是明文——库泄漏时明文 token 可直接重放。
		if string(hash) == tok {
			t.Fatal("存储哈希不得等于明文 token")
		}
		if len(hash) != 32 {
			t.Errorf("sha256 应为 32 字节，实得 %d", len(hash))
		}
	}
}

func TestHashSessionToken_Deterministic(t *testing.T) {
	tok, want, err := NewSessionToken()
	if err != nil {
		t.Fatal(err)
	}
	got := HashSessionToken(tok)
	if string(got) != string(want) {
		t.Error("同一 token 两次哈希应一致（否则按哈希查表永远查不到）")
	}
}
