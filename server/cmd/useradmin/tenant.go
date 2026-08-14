package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"

	"github.com/YouToco/vane/config"
	"github.com/YouToco/vane/store"
)

// runTenant 处理租户注销与恢复（契约 §1.3 / D9 的软删除半边）。
//

// 与 set-password / invite 同理走 CLI 不走 HTTP：注销会让一个租户的全部功能停摆，
// 是纯管理员操作。
func runTenant(args []string) int {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "用法: useradmin tenant delete|restore <租户ID>")
		fmt.Fprintln(os.Stderr, "      useradmin tenant purge [-execute]   （默认试运行）")
		return 2
	}
	sub := args[0]
	if sub == "purge" {
		return runTenantPurge(args[1:])
	}
	fs := flag.NewFlagSet("tenant", flag.ExitOnError)
	_ = fs.Parse(args[1:])
	if fs.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "useradmin: 必须提供租户 ID")
		return 2
	}
	tenantID, err := strconv.ParseInt(fs.Arg(0), 10, 64)
	if err != nil || tenantID <= 0 {
		fmt.Fprintf(os.Stderr, "useradmin: 租户 ID 非法: %q\n", fs.Arg(0))
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

	switch sub {
	case "delete":
		t, err := st.SoftDeleteTenant(ctx, tenantID)
		if err != nil {
			fmt.Fprintf(os.Stderr, "useradmin: 注销失败: %v\n", err)
			return 1
		}
		fmt.Printf("租户 %d 已注销：登录被拒、推送与抓取停止。\n", t.ID)
		if t.PurgeAfter != nil {
			fmt.Printf("保留期至 %s，在此之前可用 `useradmin tenant restore %d` 无损恢复。\n",
				t.PurgeAfter.Local().Format("2006-01-02 15:04"), t.ID)
		}
	case "restore":
		t, err := st.RestoreTenant(ctx, tenantID)
		if err != nil {
			fmt.Fprintf(os.Stderr, "useradmin: 恢复失败: %v\n", err)
			return 1
		}
		fmt.Printf("租户 %d 已恢复（status=%s）。\n", t.ID, t.Status)
	default:
		fmt.Fprintf(os.Stderr, "useradmin: 未知子命令 %q（支持 delete / restore）\n", sub)
		return 2
	}
	return 0
}

// runTenantPurge 清理已过保留期的租户（D9 硬删）。
//
// **默认试运行**，必须显式 -execute 才真删。这是全仓唯一不可逆的批量删除，
// 而试运行本身会真正执行 DELETE 再回滚——所以它报的行数是真实的，
// 且外键顺序、约束都被验证过。先看一眼再决定，比看完文档再祈祷可靠。
func runTenantPurge(args []string) int {
	fs := flag.NewFlagSet("purge", flag.ExitOnError)
	execute := fs.Bool("execute", false, "真正删除（不加此参数只做试运行）")
	_ = fs.Parse(args)

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

	ids, err := st.ListPurgeableTenants(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "useradmin: 查询待清理租户失败: %v\n", err)
		return 1
	}
	if len(ids) == 0 {
		fmt.Println("没有已过保留期的租户，无需清理。")
		return 0
	}

	if !*execute {
		fmt.Printf("【试运行】%d 个租户已过保留期。以下为将要删除的行数（本次不会真删）：\n", len(ids))
	}
	for _, id := range ids {
		rep, err := st.PurgeTenant(ctx, id, !*execute)
		if err != nil {
			fmt.Fprintf(os.Stderr, "useradmin: 清理租户 %d 失败: %v\n", id, err)
			return 1
		}
		fmt.Printf("  租户 %d：共 %d 行", id, rep.Total)
		for tbl, n := range rep.Rows {
			fmt.Printf("  %s=%d", tbl, n)
		}
		fmt.Println()
	}
	if !*execute {
		fmt.Println("\n以上均未执行。确认无误后加 -execute 真正删除（**不可逆**）。")
	} else {
		fmt.Println("\n清理完成。跨租户客观事实（信源与内容）未受影响。")
	}
	return 0
}
