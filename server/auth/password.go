package auth

import (
	"context"
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

// ---- 并发闸门 ----

// hashSlots 限制**同时**进行的 argon2 计算数，把内存占用封顶。
//
// 为什么必须有这道闸（对抗式安全审查的 CRITICAL 发现，审查员实测复现）：
// argon2id 每次计算独占 argonMemory（19 MiB）。未鉴权端点若允许无限并发，
// 200 个并发请求 ≈ 3.8 GiB 常驻——而本项目同机还跑着 Postgres/Temporal/Caddy，
// OOM killer 一介入就是全站停摆。**限流器挡不住这个**：限流是按时间窗口计次，
// 而内存峰值取决于**瞬时并发**，一秒内涌入的 200 个请求在任何窗口计数器眼里
// 都还没超额。
//
// 8 × 19 MiB ≈ 152 MiB 封顶，对单台 VPS 是可接受的常驻上限；超出的请求排队等待，
// 而不是各自申请内存。排队比拒绝更合适：正常登录只需几十毫秒，排队几乎无感，
// 而拒绝会把「服务器忙」变成用户可见的登录失败。
var hashSlots = make(chan struct{}, 8)

// acquireHashSlot 取一个计算槽位，ctx 取消时放弃等待。
func acquireHashSlot(ctx context.Context) error {
	select {
	case hashSlots <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func releaseHashSlot() { <-hashSlots }

// HashPasswordCtx 是 HashPassword 的带闸门版本，供 HTTP 路径使用。
// 无闸门的 HashPassword 保留给 CLI 等单次调用场景。
func HashPasswordCtx(ctx context.Context, password string) (string, error) {
	if err := acquireHashSlot(ctx); err != nil {
		return "", err
	}
	defer releaseHashSlot()
	return HashPassword(password)
}

// VerifyPasswordCtx 是 VerifyPassword 的带闸门版本，供 HTTP 路径使用。
func VerifyPasswordCtx(ctx context.Context, encoded, password string) error {
	if err := acquireHashSlot(ctx); err != nil {
		return err
	}
	defer releaseHashSlot()
	return VerifyPassword(encoded, password)
}

// dummyHash 是一个真实的 argon2id 哈希（口令为随机串，无人知晓），
// 专供 DummyVerify 用。包级初始化一次，避免每次请求重算。
var dummyHash = func() string {
	h, err := HashPassword("dummy-password-for-timing-alignment-only")
	if err != nil {
		// HashPassword 只在密码长度不合规或 rand 失败时报错，二者在此都不可能；
		// 真出错就让进程起不来，而不是留一个会泄漏时序的空串。
		panic("auth: 初始化 dummy 哈希失败: " + err.Error())
	}
	return h
}()

// DummyVerify 对固定假哈希跑一次 argon2，用于**时序对齐**。
//
// 存在的理由（安全审查 HIGH）：登录时若邮箱不存在就直接返回，会比「邮箱存在但
// 密码错」少跑一次 argon2（约 30ms）。这个耗时差可被测量，构成账号枚举的时序旁路
// ——统一文案挡住了内容泄漏，却挡不住时间泄漏。让不存在的路径也跑一次等价计算，
// 两条路径的耗时才真正不可区分。
//
// 返回值刻意丢弃：调用方无论如何都要走失败路径。
func DummyVerify(ctx context.Context, password string) {
	_ = VerifyPasswordCtx(ctx, dummyHash, password)
}
