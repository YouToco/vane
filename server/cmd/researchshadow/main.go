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

	"github.com/YouToco/vane/server/internal/researchoperator"
	"github.com/YouToco/vane/server/scheduler"
	"github.com/YouToco/vane/server/store"
)

type shadowArgs struct {
	taskID         string
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
	if err := researchoperator.RequireExactTask(args.taskID); err != nil {
		return err
	}
	temporalConfig, err := researchoperator.LoadTemporalConfig()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	url, err := researchoperator.MigrationDatabaseURL()
	if err != nil {
		return err
	}
	st, err := store.New(ctx, url)
	if err != nil {
		return fmt.Errorf("open migration-owner operator store: %w", err)
	}
	defer st.Close()
	scope, err := st.ResolveResearchV3OperatorScope(ctx, args.taskID)
	if err != nil {
		return fmt.Errorf("resolve exact owner task: %w", err)
	}
	temporalClient, err := client.Dial(client.Options{
		HostPort: temporalConfig.Host, Namespace: temporalConfig.Namespace,
	})
	if err != nil {
		return fmt.Errorf("connect Temporal: %w", err)
	}
	defer temporalClient.Close()
	sched := scheduler.New(temporalClient, temporalConfig.TaskQueue, st,
		scheduler.WithResearchRuntimeV3ShadowCanary(
			args.taskID))
	if err := sched.TriggerResearchShadowNowAndWait(
		ctx, args.taskID, scope.UserID, args.idempotencyKey); err != nil {
		return fmt.Errorf("run shadow: %w", err)
	}
	head, err := st.LoadPreparedResearchApprovedDefinitionV3Head(
		ctx, scope.TenantID, scope.UserID, scope.TaskID)
	if err != nil {
		return fmt.Errorf("load shadow definition proof: %w", err)
	}
	if err := st.RequireSuccessfulResearchV3ShadowPreflight(
		ctx, scope.TenantID, scope.UserID, scope.TaskID, head); err != nil {
		return fmt.Errorf("verify delivery-dark shadow: %w", err)
	}
	fmt.Printf("research V3 shadow verified delivery-dark: task=%s user=%d key=%s\n",
		args.taskID, scope.UserID, args.idempotencyKey)
	return nil
}

func parseShadowArgs(arguments []string) (shadowArgs, error) {
	set := flag.NewFlagSet("researchshadow", flag.ContinueOnError)
	set.SetOutput(os.Stderr)
	var args shadowArgs
	set.StringVar(&args.taskID, "task-id", "", "exact configured schedule ID")
	set.StringVar(&args.idempotencyKey, "idempotency-key", "", "stable retry key")
	if err := set.Parse(arguments); err != nil {
		return shadowArgs{}, err
	}
	if set.NArg() != 0 || args.taskID == "" || strings.TrimSpace(args.taskID) != args.taskID ||
		args.idempotencyKey == "" ||
		strings.TrimSpace(args.idempotencyKey) != args.idempotencyKey {
		return shadowArgs{}, errors.New(
			"require -task-id and -idempotency-key")
	}
	return args, nil
}
