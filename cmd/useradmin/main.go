// cmd/useradmin 是账号管理的本机工具。
//
// 目前只有一个用途：**存量 owner 接管**。Dashboard 从「共享密码」换成「邮箱+密码
// 账号体系」后，那位 owner 已带着租户 1 与全部历史数据存在于 users 表，却没有
// 邮箱和密码——重新注册会得到一个空的新租户，历史数据看不见。本工具把邮箱+密码
// 挂到既有 owner 行上，租户归属与数据一概不动。
//
// 用法（**密码从 stdin 读，不做命令行参数**）：
//
//	echo -n 'your-password' | useradmin set-password -email you@example.com
//	useradmin set-password -email you@example.com -user 3   # 显式指定用户
//
// 密码为何不能做参数：命令行参数会进 shell 历史，也会出现在同机任何用户可见的
// `ps aux` 里——那等于把密码写在公告板上。stdin 不经过这两处。
//
// 刻意不做成 HTTP 端点：它能给任意用户设密码，是纯管理员操作，
// 只应由能登上 VPS 的人在本机执行。
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/YouToco/vane/auth"
	"github.com/YouToco/vane/config"
	"github.com/YouToco/vane/feishu"
	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
)

func main() { os.Exit(run()) }

func run() int {
	if len(os.Args) < 2 {
		usage()
		return 2
	}
	switch os.Args[1] {
	case "set-password":
		return runSetPassword(os.Args[2:])
	case "invite":
		return runInvite(os.Args[2:])
	default:
		usage()
		return 2
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "用法:")
	fmt.Fprintln(os.Stderr, "  useradmin set-password -email <邮箱> [-user <id>]   （密码从 stdin 读）")
	fmt.Fprintln(os.Stderr, "  useradmin invite [-uses N] [-expires-days D] [-code CODE]")
}

func runSetPassword(args []string) int {
	fs := flag.NewFlagSet("set-password", flag.ExitOnError)
	email := fs.String("email", "", "要绑定的邮箱")
	userID := fs.Int64("user", 0, "目标用户 ID；0 表示解析当前 owner")
	_ = fs.Parse(args)

	if strings.TrimSpace(*email) == "" {
		fmt.Fprintln(os.Stderr, "useradmin: 必须提供 -email")
		return 2
	}

	// 密码从 stdin 读（见文件头注释）。TrimRight 掉行尾换行——用 echo 传入时
	// 必然带一个 \n，不去掉会把它算进密码，导致「设置时有、登录时没有」的错配。
	raw, err := io.ReadAll(os.Stdin)
	if err != nil {
		fmt.Fprintf(os.Stderr, "useradmin: 读取密码失败: %v\n", err)
		return 2
	}
	password := strings.TrimRight(string(raw), "\r\n")
	if err := auth.ValidatePassword(password); err != nil {
		fmt.Fprintf(os.Stderr, "useradmin: %v\n", err)
		return 2
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

	target := *userID
	if target == 0 {
		p, err := auth.NewOwnerResolver(st, feishu.SettingKeyOwner).FromContext(ctx)
		if err != nil {
			var ae *types.AppError
			if errors.As(err, &ae) && ae.Code == types.CodeConflict {
				fmt.Fprintln(os.Stderr, "useradmin: 尚未捕获 owner——请先在飞书给机器人发一条消息，或用 -user 显式指定")
				return 2
			}
			fmt.Fprintf(os.Stderr, "useradmin: 解析 owner 失败: %v\n", err)
			return 2
		}
		target = p.UserID
	}

	hash, err := auth.HashPassword(password)
	if err != nil {
		fmt.Fprintf(os.Stderr, "useradmin: 哈希密码失败: %v\n", err)
		return 2
	}
	if err := st.SetUserCredentials(ctx, target, *email, hash); err != nil {
		fmt.Fprintf(os.Stderr, "useradmin: 设置凭据失败: %v\n", err)
		return 2
	}

	// 设密码后清掉该用户所有旧会话：改密码的常见动机就是「凭据可能已泄漏」，
	// 留着旧会话等于改了锁却没换掉已配出去的钥匙。
	n, err := st.DeleteSessionsByUser(ctx, target)
	if err != nil {
		fmt.Fprintf(os.Stderr, "useradmin: 警告——清理旧会话失败: %v\n", err)
	}
	fmt.Printf("已为用户 %d 绑定邮箱 %s 并设置密码；清理旧会话 %d 条。\n", target, store.NormalizeEmail(*email), n)
	return 0
}
