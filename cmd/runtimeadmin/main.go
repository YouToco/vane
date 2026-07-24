// cmd/runtimeadmin contains database-direct runtime maintenance operations.
// It is intentionally not mounted on HTTP, Agent, A2A, or Temporal ingress:
// only an operator with the deployment database configuration can run it.
//
// Baseline examples:
//
//	runtimeadmin baseline -mode dry-run -limit 100
//	runtimeadmin baseline -mode apply -confirm-apply -limit 100
//	runtimeadmin baseline -mode verify -after-tenant 1 -after-user 2 -after-task push-123
//
// Verify returns 0 only when every item in the page is verified and Next is
// absent. Exit 1 means a non-verified item, 2 means the command failed, and 3
// means the JSON Next cursor must be supplied to a subsequent verify call.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/YouToco/vane/config"
	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
)

const (
	exitOK              = 0
	exitVerifyFailed    = 1
	exitFailure         = 2
	exitVerifyMorePages = 3
	runTimeout          = 2 * time.Minute
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	if len(args) > 0 && args[0] == "snapshot-shadow" {
		return runSnapshotShadow(args[1:])
	}
	if len(args) == 0 || args[0] != "baseline" {
		fmt.Fprintln(os.Stderr, "用法: runtimeadmin baseline ... | snapshot-shadow ...")
		return exitFailure
	}
	fs := flag.NewFlagSet("runtimeadmin baseline", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	rawMode := fs.String("mode", "dry-run", "dry-run、apply 或 verify")
	limit := fs.Int("limit", 100, "每页任务数（1-1000）")
	afterTenant := fs.Int64("after-tenant", 0, "exclusive cursor tenant id")
	afterUser := fs.Int64("after-user", 0, "exclusive cursor user id")
	afterTask := fs.String("after-task", "", "exclusive cursor task id")
	confirmApply := fs.Bool("confirm-apply", false, "确认 apply 会写 immutable v1 head")
	if err := fs.Parse(args[1:]); err != nil {
		return exitFailure
	}

	mode, ok := parseBaselineMode(*rawMode)
	if !ok {
		fmt.Fprintln(os.Stderr, "runtimeadmin: -mode 仅支持 dry-run、apply、verify")
		return exitFailure
	}
	if mode == store.TaskDefinitionBaselineApply && !*confirmApply {
		fmt.Fprintln(os.Stderr, "runtimeadmin: apply 必须显式提供 -confirm-apply")
		return exitFailure
	}

	cfg, err := config.Load("")
	if err != nil {
		fmt.Fprintln(os.Stderr, "runtimeadmin: 加载配置失败")
		return exitFailure
	}
	ctx, stop := signal.NotifyContext(
		context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, runTimeout)
	defer cancel()

	st, err := store.New(ctx, cfg.DB.URL)
	if err != nil {
		fmt.Fprintln(os.Stderr, "runtimeadmin: 连接数据库失败")
		return exitFailure
	}
	defer st.Close()

	page, err := st.ReconcileTaskDefinitionBaselines(
		ctx, mode, store.TaskDefinitionBaselineCursor{
			TenantID: *afterTenant,
			UserID:   *afterUser,
			TaskID:   *afterTask,
		}, *limit)
	return finishBaselineRun(os.Stdout, os.Stderr, mode, page, err)
}

func runSnapshotShadow(args []string) int {
	fs := flag.NewFlagSet("runtimeadmin snapshot-shadow", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	taskID := fs.String("task", "", "exact canary task id")
	rawSince := fs.String("since", "", "inclusive RFC3339 canary start")
	throughID := fs.Int64("through-id", 0, "inclusive frozen snapshot id ceiling")
	expectedCount := fs.Int("expected-count", 0, "exact number of rows required")
	limit := fs.Int("limit", 100, "page size (1-1000)")
	if err := fs.Parse(args); err != nil {
		return exitFailure
	}
	since, err := time.Parse(time.RFC3339, *rawSince)
	if err != nil || *taskID == "" || *throughID <= 0 || *expectedCount <= 0 {
		fmt.Fprintln(os.Stderr,
			"runtimeadmin: snapshot-shadow requires -task, RFC3339 -since, "+
				"-through-id and -expected-count")
		return exitFailure
	}
	cfg, err := config.Load("")
	if err != nil {
		fmt.Fprintln(os.Stderr, "runtimeadmin: 加载配置失败")
		return exitFailure
	}
	ctx, stop := signal.NotifyContext(
		context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, runTimeout)
	defer cancel()
	st, err := store.New(ctx, cfg.DB.URL)
	if err != nil {
		fmt.Fprintln(os.Stderr, "runtimeadmin: 连接数据库失败")
		return exitFailure
	}
	defer st.Close()
	page, err := collectSnapshotShadowAudit(
		ctx, *taskID, since, *throughID, *limit, *expectedCount,
		st.AuditTaskRunSnapshotShadowsV2Through)
	return finishSnapshotShadowRun(
		os.Stdout, os.Stderr, page, *expectedCount, err)
}

type snapshotShadowPageLoader func(
	context.Context, string, time.Time, int64, int64, int,
) (store.TaskRunSnapshotShadowAuditPage, error)

// collectSnapshotShadowAudit always starts at zero and follows Store-issued
// cursors through the complete frozen interval. The CLI deliberately exposes
// no after-id escape hatch: an operator cannot present a clean suffix while
// skipping a bad prefix.
func collectSnapshotShadowAudit(
	ctx context.Context,
	taskID string,
	since time.Time,
	throughID int64,
	limit int,
	expectedCount int,
	load snapshotShadowPageLoader,
) (store.TaskRunSnapshotShadowAuditPage, error) {
	collected := store.TaskRunSnapshotShadowAuditPage{
		Items: make([]store.TaskRunSnapshotShadowAuditItem, 0, expectedCount),
	}
	var afterID int64
	for {
		page, err := load(ctx, taskID, since, afterID, throughID, limit)
		if err != nil {
			return store.TaskRunSnapshotShadowAuditPage{}, err
		}
		collected.Items = append(collected.Items, page.Items...)
		if len(collected.Items) > expectedCount {
			return collected, nil
		}
		if page.Next == nil {
			return collected, nil
		}
		if *page.Next <= afterID || *page.Next >= throughID {
			return store.TaskRunSnapshotShadowAuditPage{},
				types.NewAppError(types.CodeValidation,
					"snapshot shadow audit cursor is invalid", nil)
		}
		afterID = *page.Next
	}
}

func finishSnapshotShadowRun(
	stdout io.Writer,
	stderr io.Writer,
	page store.TaskRunSnapshotShadowAuditPage,
	expectedCount int,
	runErr error,
) int {
	if runErr != nil {
		fmt.Fprintln(stderr, "runtimeadmin: "+safeError(
			runErr, "snapshot shadow verify failed"))
		return exitFailure
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(page); err != nil {
		fmt.Fprintln(stderr, "runtimeadmin: 输出结果失败")
		return exitFailure
	}
	if len(page.Items) == 0 || len(page.Items) != expectedCount {
		return exitVerifyFailed
	}
	for _, item := range page.Items {
		if item.Status != store.TaskRunSnapshotShadowMatch ||
			item.TypedAuditStatus != store.CompiledRunSnapshotV2AuditMatch ||
			!item.TypedEqual {
			return exitVerifyFailed
		}
	}
	if page.Next != nil {
		return exitVerifyMorePages
	}
	return exitOK
}

func finishBaselineRun(
	stdout io.Writer,
	stderr io.Writer,
	mode store.TaskDefinitionBaselineMode,
	page store.TaskDefinitionBaselinePage,
	runErr error,
) int {
	if runErr != nil {
		fmt.Fprintln(stderr, "runtimeadmin: "+safeError(
			runErr, "任务 definition baseline 执行失败"))
		return exitFailure
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(page); err != nil {
		fmt.Fprintln(stderr, "runtimeadmin: 输出结果失败")
		return exitFailure
	}
	return baselineExitCode(mode, page)
}

func baselineExitCode(
	mode store.TaskDefinitionBaselineMode,
	page store.TaskDefinitionBaselinePage,
) int {
	if mode != store.TaskDefinitionBaselineVerify {
		return exitOK
	}
	for _, item := range page.Items {
		if item.Status != store.TaskDefinitionBaselineVerified {
			return exitVerifyFailed
		}
	}
	if page.Next != nil {
		return exitVerifyMorePages
	}
	return exitOK
}

func parseBaselineMode(raw string) (store.TaskDefinitionBaselineMode, bool) {
	switch raw {
	case "dry-run":
		return store.TaskDefinitionBaselineDryRun, true
	case "apply":
		return store.TaskDefinitionBaselineApply, true
	case "verify":
		return store.TaskDefinitionBaselineVerify, true
	default:
		return "", false
	}
}

func safeError(err error, fallback string) string {
	var appErr *types.AppError
	if errors.As(err, &appErr) {
		return appErr.Message
	}
	return fallback
}
