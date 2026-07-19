// 邀请码生成器：cmd/useradmin（CLI 签发）与 api（管理端点签发）的**同一份**实现。
//
// 为什么住在 auth 包：邀请码是 D4 准入闸门的载体（不变量 I-A2「无有效邀请码
// 不得创建租户」），与密码、会话 token 同属「凭证类随机量」——本包的
// TestInviteCode_NoWeakRandomImport 守卫顺带把三者的随机源钉在 crypto/rand 上。
// 抽到共享包而不是两处复制：复制的第二份迟早被人"顺手优化"成 math/rand。
package auth

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"strings"
)

// inviteAlphabet 是邀请码字母表：去掉了 0/O、1/I/L 这些抄错就白折腾的形近字符。
// 邀请码要靠人念给人、贴进聊天窗口，形近字符的代价是「码是对的但对方输错了」——
// 那会表现成「邀请码无效」，谁都查不出问题在哪。
const inviteAlphabet = "23456789ABCDEFGHJKMNPQRSTUVWXYZ"

// inviteCodeLen 取 16：字母表 31 个字符，16 位约 79 bit 熵。
//
// 为什么必须是密码学随机而不是时间戳/自增/uuid-v1：邀请码是**不变量 I-A2 的唯一载体**
// ——「无有效邀请码不得创建租户」，而它守的是 D3（第三方 API 成本平台全垫付）的财务敞口。
// 码可猜 = 把按次计费的 TikHub/LLM 调用对公网开放。这不是"安全洁癖"，
// 是这条闸门存在的全部理由。
const inviteCodeLen = 16

// DefaultInviteExpireDays 是签发邀请码的默认有效期（天）。
//
// schema 允许 expires_at 为 NULL（永不过期），但**默认不该是它**：一个永不过期的码
// 一旦流出（截图、转发、离职员工的聊天记录）就是一张永久有效的白条，而 D4 的准入闸门
// 存在的意义正是「财务敞口由发出的邀请数封顶」。默认给 7 天，要永久必须显式选择。
const DefaultInviteExpireDays = 7

// NewInviteCode 生成密码学随机的邀请码。
//
// 用 crypto/rand + rand.Int 做无偏取样，而不是 `b[i] % len(alphabet)`：
// 后者在字母表长度不整除 256 时会让靠前的字符略微高频（模偏置）。
// 31 不整除 256，偏置真实存在——虽然对 79 bit 的熵来说影响微乎其微，
// 但"随机数取模"是个会被后人照抄的写法，不该在守财务闸门的地方留这个样板。
func NewInviteCode() (string, error) {
	var b strings.Builder
	b.Grow(inviteCodeLen)
	max := big.NewInt(int64(len(inviteAlphabet)))
	for range inviteCodeLen {
		n, err := rand.Int(rand.Reader, max)
		if err != nil {
			return "", fmt.Errorf("生成随机邀请码: %w", err)
		}
		b.WriteByte(inviteAlphabet[n.Int64()])
	}
	return b.String(), nil
}
