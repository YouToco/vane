// Command feedbackrepair previews and explicitly applies historical bad-
// feedback cause recovery. It is intentionally database-direct and is not
// exposed through user HTTP, Agent, A2A, or Temporal surfaces.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/YouToco/vane/config"
	"github.com/YouToco/vane/store"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	fs := flag.NewFlagSet("feedbackrepair", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	mode := fs.String("mode", "preview", "preview 或 apply")
	tenantID := fs.Int64("tenant", 0, "exact tenant id")
	userID := fs.Int64("user", 0, "exact user id")
	digest := fs.String("plan-digest", "", "preview 输出的 exact digest")
	confirm := fs.Bool("confirm-apply", false, "确认恢复原因码并按预览重置演化游标")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 0 || *tenantID <= 0 || *userID <= 0 {
		fmt.Fprintln(os.Stderr, "feedbackrepair: 需要 exact -tenant 和 -user")
		return 2
	}
	if *mode != "preview" && *mode != "apply" {
		fmt.Fprintln(os.Stderr, "feedbackrepair: -mode 仅支持 preview 或 apply")
		return 2
	}
	if *mode == "apply" && (!*confirm || len(*digest) != 64) {
		fmt.Fprintln(os.Stderr,
			"feedbackrepair: apply 需要 -confirm-apply 和 preview 的 -plan-digest")
		return 2
	}

	cfg, err := config.Load("")
	if err != nil {
		fmt.Fprintln(os.Stderr, "feedbackrepair: 加载配置失败")
		return 2
	}
	ctx, stop := signal.NotifyContext(
		context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	st, err := store.New(ctx, cfg.DB.URL)
	if err != nil {
		fmt.Fprintln(os.Stderr, "feedbackrepair: 连接数据库失败")
		return 2
	}
	defer st.Close()

	var plan store.FeedbackRepairPlan
	if *mode == "apply" {
		plan, err = st.ApplyLegacyFeedbackRepair(
			ctx, *tenantID, *userID, *digest)
	} else {
		plan, err = st.PreviewLegacyFeedbackRepair(
			ctx, *tenantID, *userID)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "feedbackrepair:", err)
		return 2
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(plan); err != nil {
		fmt.Fprintln(os.Stderr, "feedbackrepair: 输出失败")
		return 2
	}
	return 0
}
