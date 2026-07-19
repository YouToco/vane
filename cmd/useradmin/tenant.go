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
// 硬删（保留期到期后真正删数据）**刻意不在这里**：它不可逆、且必须遵守红线 I-A3
// （只删租户所有表，sources/content_items/content_sources/page_snapshots 是跨租户
// 客观事实，删了会伤到别的租户），值得单独一轮设计与审查。
//
// 与 set-password / invite 同理走 CLI 不走 HTTP：注销会让一个租户的全部功能停摆，
// 是纯管理员操作。
func runTenant(args []string) int {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "用法: useradmin tenant delete|restore <租户ID>")
		return 2
	}
	sub := args[0]
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
