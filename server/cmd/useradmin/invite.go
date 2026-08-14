package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/YouToco/vane/auth"
	"github.com/YouToco/vane/config"
	"github.com/YouToco/vane/store"
)

// runInvite 签发一个邀请码并打印。
//
// 码的生成与默认有效期都在 auth 包（auth.NewInviteCode / auth.DefaultInviteExpireDays）：
// 那是 CLI 与管理 API（POST /api/admin/invites）共用的同一份实现，字母表、熵、
// 无偏取样的讲究见彼处注释。
//
// 本命令保留的价值：能自定义 -uses/-expires-days/-code，且不依赖 API 服务在线——
// API 挂了、忘了 Dashboard 密码时，能登上 VPS 就能发码自救。
func runInvite(args []string) int {
	fs := flag.NewFlagSet("invite", flag.ExitOnError)
	uses := fs.Int("uses", 1, "该码可用次数")
	expireDays := fs.Int("expires-days", auth.DefaultInviteExpireDays, "有效期天数；0 表示永不过期（不推荐）")
	custom := fs.String("code", "", "自定义邀请码；留空则生成密码学随机码（推荐）")
	_ = fs.Parse(args)

	if *uses < 1 {
		fmt.Fprintln(os.Stderr, "useradmin: -uses 至少为 1")
		return 2
	}

	code := strings.TrimSpace(*custom)
	if code == "" {
		var err error
		if code, err = auth.NewInviteCode(); err != nil {
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
