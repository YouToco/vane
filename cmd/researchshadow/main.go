// Command researchshadow starts one exact, delivery-dark Research V3 shadow
// run. It is an operator control plane: it never edits or triggers the task's
// Temporal Schedule and cannot target a task outside the configured shadow ID.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"go.temporal.io/sdk/client"

	"github.com/YouToco/vane/config"
	"github.com/YouToco/vane/scheduler"
	"github.com/YouToco/vane/store"
)

type shadowArgs struct {
	taskID         string
	userID         int64
	idempotencyKey string
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "vane-research-shadow:", err)
		os.Exit(1)
	}
}

func run(arguments []string) error {
	args, err := parseShadowArgs(arguments)
	if err != nil {
		return err
	}
	cfg, err := config.Load("")
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	if cfg.Pipeline.ResearchV3ShadowCanaryScheduleID == "" ||
		args.taskID != cfg.Pipeline.ResearchV3ShadowCanaryScheduleID {
		return errors.New("task is not the exact configured Research V3 shadow canary")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	st, err := store.NewServerRuntime(ctx, cfg.DB.URL)
	if err != nil {
		return fmt.Errorf("open server runtime store: %w", err)
	}
	defer st.Close()
	temporalClient, err := client.Dial(client.Options{
		HostPort:  cfg.Temporal.Host,
		Namespace: cfg.Temporal.Namespace,
	})
	if err != nil {
		return fmt.Errorf("connect Temporal: %w", err)
	}
	defer temporalClient.Close()
	sched := scheduler.New(temporalClient, cfg.Temporal.TaskQueue, st,
		scheduler.WithResearchRuntimeV3ShadowCanary(
			cfg.Pipeline.ResearchV3ShadowCanaryScheduleID))
	if err := sched.TriggerResearchShadowNow(
		ctx, args.taskID, args.userID, args.idempotencyKey); err != nil {
		return fmt.Errorf("start shadow: %w", err)
	}
	fmt.Printf("research V3 shadow accepted: task=%s user=%d key=%s\n",
		args.taskID, args.userID, args.idempotencyKey)
	return nil
}

func parseShadowArgs(arguments []string) (shadowArgs, error) {
	set := flag.NewFlagSet("researchshadow", flag.ContinueOnError)
	set.SetOutput(os.Stderr)
	var args shadowArgs
	set.StringVar(&args.taskID, "task-id", "", "exact configured schedule ID")
	set.Int64Var(&args.userID, "user-id", 0, "owning Vane user ID")
	set.StringVar(&args.idempotencyKey, "idempotency-key", "", "stable retry key")
	if err := set.Parse(arguments); err != nil {
		return shadowArgs{}, err
	}
	if set.NArg() != 0 || args.taskID == "" || strings.TrimSpace(args.taskID) != args.taskID ||
		args.userID <= 0 || args.idempotencyKey == "" ||
		strings.TrimSpace(args.idempotencyKey) != args.idempotencyKey {
		return shadowArgs{}, errors.New(
			"require -task-id, positive -user-id and -idempotency-key")
	}
	return args, nil
}
