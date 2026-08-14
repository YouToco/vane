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

// runQuota 查看与调整租户额度（契约 §2.7）。
//
// 为什么这个子命令是必需的，而不是"以后有空再加"：额度用尽时用户收到的文案
// 里写着「请联系管理员调整额度」。在它存在之前，管理员唯一能做的是连进生产库
// 手写 UPDATE——一条没有任何校验、打错一个字就可能把某人的额度清零的语句。
// 一个承诺了救援通道的系统，必须真的有那条通道。
func runQuota(args []string) int {
	if len(args) < 1 {
		quotaUsage()
		return 2
	}
	sub := args[0]

	fs := flag.NewFlagSet("quota", flag.ExitOnError)
	bucket := fs.String("bucket", "", "桶名（set 时必填）：llm_tokens|exa_calls|tikhub_calls|push|fetch")
	perDay := fs.Float64("per-day", -1, "每日额度（set 时必填）。同时决定补充速率与桶容量")
	refill := fs.Bool("refill", false, "把余额直接补满到桶容量（救援用：立刻恢复服务）")
	if err := fs.Parse(args[1:]); err != nil {
		return 2
	}
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
	case "show":
		return quotaShow(ctx, st, tenantID)
	case "set":
		if *bucket == "" || (*perDay < 0 && !*refill) {
			fmt.Fprintln(os.Stderr, "useradmin: set 需要 -bucket，且需 -per-day 或 -refill 之一")
			return 2
		}
		if *refill {
			if err := st.RefillQuota(ctx, tenantID, store.QuotaBucket(*bucket)); err != nil {
				fmt.Fprintf(os.Stderr, "useradmin: 补满失败: %v\n", err)
				return 1
			}
			fmt.Printf("租户 %d 的 %s 已补满到桶容量。\n", tenantID, *bucket)
		}
		if *perDay >= 0 {
			if err := st.SetQuota(ctx, tenantID, store.QuotaBucket(*bucket), *perDay); err != nil {
				fmt.Fprintf(os.Stderr, "useradmin: 设置失败: %v\n", err)
				return 1
			}
			fmt.Printf("租户 %d 的 %s 已设为 %.0f/天。\n", tenantID, *bucket, *perDay)
		}
		return quotaShow(ctx, st, tenantID)
	default:
		fmt.Fprintf(os.Stderr, "useradmin: 未知子命令 %q\n", sub)
		quotaUsage()
		return 2
	}
}

func quotaShow(ctx context.Context, st *store.Store, tenantID int64) int {
	sts, err := st.ListQuota(ctx, tenantID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "useradmin: 查询失败: %v\n", err)
		return 1
	}
	if len(sts) == 0 {
		// 这不是"没配置"，是一个会让该租户什么都用不了的状态——必须说清楚怎么修。
		fmt.Printf("租户 %d **没有任何配额行**。\n", tenantID)
		fmt.Println("缺行 = 无额度（不是无限额度），该租户的全部 LLM 调用都会被拒、推送会静默停摆。")
		fmt.Println("修复：重启服务会自动 reconcile 补齐；或 useradmin quota set -bucket llm_tokens -per-day 1000000000 <租户ID>")
		return 1
	}
	fmt.Printf("租户 %d 的额度：\n", tenantID)
	fmt.Printf("  %-14s %14s %14s %14s\n", "桶", "当前可用", "容量", "每日额度")
	for _, q := range sts {
		avail := fmt.Sprintf("%.0f", q.Tokens)
		if q.Tokens < 0 {
			// 负数是欠账：实际用量超过了余额，已如实记下。说明白，免得被当成 bug。
			avail += "（欠账）"
		}
		fmt.Printf("  %-14s %14s %14.0f %14.0f\n", q.Bucket, avail, q.Burst, q.RatePerDay)
	}
	return 0
}

func quotaUsage() {
	fmt.Fprintln(os.Stderr, "用法:")
	fmt.Fprintln(os.Stderr, "  useradmin quota show <租户ID>")
	fmt.Fprintln(os.Stderr, "  useradmin quota set -bucket <桶> -per-day <N> <租户ID>   （调整额度）")
	fmt.Fprintln(os.Stderr, "  useradmin quota set -bucket <桶> -refill <租户ID>        （立刻补满，救援用）")
}
