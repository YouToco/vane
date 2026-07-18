package auth

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"golang.org/x/crypto/argon2"
)

// 密码哈希参数（argon2id，OWASP 2024 交互式登录推荐档）。
//
// 选 argon2id 而非 bcrypt：bcrypt 有 72 字节静默截断（长密码后段被忽略），
// 且不抗 GPU/ASIC 并行。argon2id 的内存硬特性让离线爆破的成本随 memory 线性上升。
//
// 参数取舍：19 MiB × 2 轮是 OWASP 的下限档，单次哈希约 30-50ms。本机是单台 VPS
// （还跑着 Postgres/Temporal/Caddy），配合登录限流，内存与延迟都可接受；
// 调高 memory 会让并发登录的内存占用线性放大，不划算。
const (
	argonMemory  = 19 * 1024 // KiB
	argonTime    = 2
	argonThreads = 1
	argonKeyLen  = 32
	argonSaltLen = 16
)

// 密码长度边界。
//
// 下限 8：低于此的密码在离线爆破面前形同虚设，再好的哈希也救不回来。
// 上限 1024：**必须有上限**——argon2 对超长输入照算不误，没有上限等于给出
// 一个「发 10MB 密码打满 CPU」的免鉴权 DoS 面（登录端点本就无需鉴权）。
// 1024 远超任何真实密码或密码管理器生成的口令。
const (
	MinPasswordLen = 8
	MaxPasswordLen = 1024
)

// ErrPasswordMismatch 是密码不匹配。调用方**不得**把它与「用户不存在」区分后
// 回给客户端——那等于提供账号枚举接口。
var ErrPasswordMismatch = errors.New("auth: 密码不匹配")

// HashPassword 用 argon2id 生成密码哈希，返回自描述的编码串：
//
//	$argon2id$v=19$m=19456,t=2,p=1$<base64 salt>$<base64 hash>
//
// 自描述格式的意义是**参数可演进**：将来调高 memory/time 时，老哈希仍能按自身
// 参数校验通过，用户下次登录时再按新参数重算（见 NeedsRehash）。若把参数写死在
// 校验代码里，调参当天全体用户都会登录失败。
func HashPassword(password string) (string, error) {
	if err := ValidatePassword(password); err != nil {
		return "", err
	}
	salt := make([]byte, argonSaltLen)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("auth: 生成盐失败: %w", err)
	}
	sum := argon2.IDKey([]byte(password), salt, argonTime, argonMemory, argonThreads, argonKeyLen)
	return fmt.Sprintf("$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s",
		argon2.Version, argonMemory, argonTime, argonThreads,
		base64.RawStdEncoding.EncodeToString(salt),
		base64.RawStdEncoding.EncodeToString(sum)), nil
}

// VerifyPassword 校验密码。匹配返回 nil，不匹配返回 ErrPasswordMismatch，
// 哈希串本身损坏返回其他错误（那是数据问题，不是密码错）。
//
// 比较用 subtle.ConstantTimeCompare 防时序侧信道——虽然 argon2 的计算耗时
// 远大于比较耗时、实际可利用性极低，但这是零成本的正确做法。
func VerifyPassword(encoded, password string) error {
	params, salt, want, err := decodeHash(encoded)
	if err != nil {
		return err
	}
	// 不在此处调 ValidatePassword：长度不合规的密码应当是「不匹配」而不是「参数错误」，
	// 否则错误类型本身会泄漏「你输入的长度不合法」这类信息。但仍要挡住超长输入的
	// DoS 面——超过上限直接判不匹配，不进 argon2。
	if utf8.RuneCountInString(password) > MaxPasswordLen {
		return ErrPasswordMismatch
	}
	got := argon2.IDKey([]byte(password), salt, params.time, params.memory, params.threads, uint32(len(want)))
	if subtle.ConstantTimeCompare(got, want) != 1 {
		return ErrPasswordMismatch
	}
	return nil
}

// NeedsRehash 报告该哈希是否用的是过时参数（当前参数被调高后为真）。
// 调用方应在登录成功后异步重算并更新，实现无感的参数升级。
func NeedsRehash(encoded string) bool {
	params, _, _, err := decodeHash(encoded)
	if err != nil {
		return true // 解不开的哈希应当被替换
	}
	return params.memory != argonMemory || params.time != argonTime || params.threads != argonThreads
}

// ValidatePassword 校验密码是否满足长度要求。**按 rune 计数而非字节**：
// 中文密码每字 3 字节，按字节判会把 3 个汉字算作 9 位而误判为合格。
func ValidatePassword(password string) error {
	n := utf8.RuneCountInString(password)
	if n < MinPasswordLen {
		return fmt.Errorf("auth: 密码至少 %d 位", MinPasswordLen)
	}
	if n > MaxPasswordLen {
		return fmt.Errorf("auth: 密码最多 %d 位", MaxPasswordLen)
	}
	return nil
}

type argonParams struct {
	memory  uint32
	time    uint32
	threads uint8
}

// decodeHash 解析自描述哈希串。任何格式偏差都报错而不是猜测——
// 猜测的下场是把损坏的哈希当成「密码不对」，掩盖真正的数据问题。
func decodeHash(encoded string) (argonParams, []byte, []byte, error) {
	var p argonParams
	parts := strings.Split(encoded, "$")
	// 形如 ["", "argon2id", "v=19", "m=..,t=..,p=..", salt, hash]
	if len(parts) != 6 || parts[0] != "" {
		return p, nil, nil, errors.New("auth: 密码哈希格式非法")
	}
	if parts[1] != "argon2id" {
		return p, nil, nil, fmt.Errorf("auth: 不支持的哈希算法 %q", parts[1])
	}
	var version int
	if _, err := fmt.Sscanf(parts[2], "v=%d", &version); err != nil {
		return p, nil, nil, errors.New("auth: 密码哈希版本段非法")
	}
	if version != argon2.Version {
		return p, nil, nil, fmt.Errorf("auth: 不支持的 argon2 版本 %d", version)
	}
	if _, err := fmt.Sscanf(parts[3], "m=%d,t=%d,p=%d", &p.memory, &p.time, &p.threads); err != nil {
		return p, nil, nil, errors.New("auth: 密码哈希参数段非法")
	}
	salt, err := base64.RawStdEncoding.DecodeString(parts[4])
	if err != nil {
		return p, nil, nil, errors.New("auth: 密码哈希盐段非法")
	}
	sum, err := base64.RawStdEncoding.DecodeString(parts[5])
	if err != nil {
		return p, nil, nil, errors.New("auth: 密码哈希摘要段非法")
	}
	if len(sum) == 0 {
		return p, nil, nil, errors.New("auth: 密码哈希摘要为空")
	}
	return p, salt, sum, nil
}
