package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"math/big"
	"os"
	"strings"
	"time"

	"github.com/YouToco/vane/config"
	"github.com/YouToco/vane/store"
)

// 邀请码字母表：去掉了 0/O、1/I/L 这些抄错就白折腾的形近字符。
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

// inviteDefaultExpireDays 是默认有效期。
//
// schema 允许 expires_at 为 NULL（永不过期），但**默认不该是它**：一个永不过期的码
// 一旦流出（截图、转发、离职员工的聊天记录）就是一张永久有效的白条，而 D4 的准入闸门
// 存在的意义正是「财务敞口由发出的邀请数封顶」。默认给 7 天，要永久必须显式写 0。
const inviteDefaultExpireDays = 7

// newInviteCode 生成密码学随机的邀请码。
//
// 用 crypto/rand + rand.Int 做无偏取样，而不是 `b[i] % len(alphabet)`：
// 后者在字母表长度不整除 256 时会让靠前的字符略微高频（模偏置）。
// 31 不整除 256，偏置真实存在——虽然对 79 bit 的熵来说影响微乎其微，
// 但"随机数取模"是个会被后人照抄的写法，不该在守财务闸门的地方留这个样板。
func newInviteCode() (string, error) {
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

// runInvite 签发一个邀请码并打印。
//
// 为什么是 CLI 而不是 HTTP 端点：与 set-password 同理——它能凭空创造出「可以注册并
// 消耗平台预算的资格」，是纯管理员操作，只应由能登上 VPS 的人在本机执行。
// 做成端点就得再造一套鉴权，而那套鉴权的任何缺陷都直接等于财务敞口。
func runInvite(args []string) int {
	fs := flag.NewFlagSet("invite", flag.ExitOnError)
	uses := fs.Int("uses", 1, "该码可用次数")
	expireDays := fs.Int("expires-days", inviteDefaultExpireDays, "有效期天数；0 表示永不过期（不推荐）")
	custom := fs.String("code", "", "自定义邀请码；留空则生成密码学随机码（推荐）")
	_ = fs.Parse(args)

	if *uses < 1 {
		fmt.Fprintln(os.Stderr, "useradmin: -uses 至少为 1")
		return 2
	}

	code := strings.TrimSpace(*custom)
	if code == "" {
		var err error
		if code, err = newInviteCode(); err != nil {
			fmt.Fprintf(os.Stderr, "useradmin: %v\n", err)
			return 2
		}
	} else {
		// 自定义码是给「我就想要一个好记的码」留的口子，但必须出声警告：
		// 好记 = 好猜，而这条闸门守的是财务敞口。不阻塞（运维知道自己在做什么），
		// 只保证他不是无意间做的。
		fmt.Fprintln(os.Stderr, "警告: 使用自定义邀请码。可猜的码等于把按次计费的 API 敞口对公网开放（不变量 I-A2）。")
	}

	var expiresAt *time.Time
	if *expireDays > 0 {
		t := time.Now().Add(time.Duration(*expireDays) * 24 * time.Hour)
		expiresAt = &t
	} else {
		fmt.Fprintln(os.Stderr, "警告: 该码永不过期。流出后即为永久有效的注册白条，建议改用有限期。")
	}

	cfg, err := config.Load("")
	if err != nil {
		fmt.Fprintf(os.Stderr, "useradmin: 加载配置失败: %v\n", err)
		return 2
	}
	ctx := context.Background()
	st, err := store.New(ctx, cfg.DB.URL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "useradmin: 连接数据库失败: %v\n", err)
		return 2
	}
	defer st.Close()

	// issuedBy 传 nil = 平台自签（schema 注释里的既定语义）。
	inv, err := st.IssueInvite(ctx, code, nil, *uses, expiresAt)
	if err != nil {
		fmt.Fprintf(os.Stderr, "useradmin: 签发失败: %v\n", err)
		return 1
	}

	// 码打到 stdout、其余信息打到 stderr：这样 `useradmin invite | pbcopy` 拿到的
	// 恰好是码本身，不用手工去掉说明文字。
	fmt.Println(inv.Code)
	fmt.Fprintf(os.Stderr, "已签发邀请码：可用 %d 次", inv.MaxUses)
	if inv.ExpiresAt != nil {
		fmt.Fprintf(os.Stderr, "，%s 过期", inv.ExpiresAt.Local().Format("2006-01-02 15:04"))
	} else {
		fmt.Fprint(os.Stderr, "，永不过期")
	}
	fmt.Fprintln(os.Stderr)
	return 0
}
