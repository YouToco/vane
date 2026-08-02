// Command researchcutover performs the durable exact-task V3 cutover or
// rollback saga. It is an operator-only control plane and is not exposed to
// HTTP, Feishu, or the Agent tool surface.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"go.temporal.io/sdk/client"

	"github.com/YouToco/vane/config"
	"github.com/YouToco/vane/scheduler"
	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
)

const (
	migrationOwnerDatabaseURLEnv      = "VANE_MIGRATION_DB_URL"
	migrationOwnerCredentialDirectory = "CREDENTIALS_DIRECTORY"
	migrationOwnerDatabaseCredential  = "migration_db_url"
)

type cutoverArgs struct {
	operation      string
	taskID         string
	userID         int64
	idempotencyKey string
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "vane-research-cutover:", err)
		os.Exit(1)
	}
}

func run(arguments []string) error {
	args, err := parseCutoverArgs(arguments)
	if err != nil {
		return err
	}
	cfg, err := config.Load("")
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	if cfg.Pipeline.ResearchV3AuthorityCanaryScheduleID == "" ||
		args.taskID != cfg.Pipeline.ResearchV3AuthorityCanaryScheduleID {
		return errors.New("task is not the exact configured Research V3 authority canary")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	operatorDatabaseURL, err := migrationOwnerDatabaseURL()
	if err != nil {
		return err
	}
	// Cutover authority deliberately does not belong to the long-lived
	// vane_server_runtime login. This one-shot command authenticates with the
	// migration owner, which migration 101 alone makes a member of the NOLOGIN
	// cutover operator role, and then Store methods still SET LOCAL ROLE for
	// every scoped mutation.
	st, err := store.New(ctx, operatorDatabaseURL)
	if err != nil {
		return fmt.Errorf("open migration-owner operator store: %w", err)
	}
	defer st.Close()
	temporalClient, err := client.Dial(client.Options{
		HostPort: cfg.Temporal.Host, Namespace: cfg.Temporal.Namespace,
	})
	if err != nil {
		return fmt.Errorf("connect Temporal: %w", err)
	}
	defer temporalClient.Close()
	sched := scheduler.New(temporalClient, cfg.Temporal.TaskQueue, st,
		scheduler.WithTaskScheduleNamespace(cfg.Temporal.Namespace),
		scheduler.WithResearchRuntimeV3AuthorityCanary(
			cfg.Pipeline.ResearchV3AuthorityCanaryScheduleID))

	var operation types.ResearchV3CutoverOperation
	if args.operation == "cutover" {
		operation, err = sched.CutoverResearchV3(
			ctx, args.taskID, args.userID, args.idempotencyKey)
	} else {
		operation, err = sched.RollbackResearchV3(
			ctx, args.taskID, args.userID, args.idempotencyKey)
	}
	if err != nil {
		return fmt.Errorf("%s Research V3 task: %w", args.operation, err)
	}
	fmt.Printf("research V3 %s complete: task=%s user=%d generation=%d phase=%s\n",
		args.operation, operation.TaskID, operation.UserID,
		operation.Generation, operation.Phase)
	return nil
}

func parseCutoverArgs(arguments []string) (cutoverArgs, error) {
	set := flag.NewFlagSet("researchcutover", flag.ContinueOnError)
	set.SetOutput(os.Stderr)
	var args cutoverArgs
	set.StringVar(&args.operation, "operation", "", "cutover or rollback")
	set.StringVar(&args.taskID, "task-id", "", "exact configured schedule ID")
	set.Int64Var(&args.userID, "user-id", 0, "owning Vane user ID")
	set.StringVar(&args.idempotencyKey, "idempotency-key", "", "stable saga retry key")
	if err := set.Parse(arguments); err != nil {
		return cutoverArgs{}, err
	}
	if set.NArg() != 0 || (args.operation != "cutover" && args.operation != "rollback") ||
		args.taskID == "" || strings.TrimSpace(args.taskID) != args.taskID ||
		args.userID <= 0 || args.idempotencyKey == "" ||
		strings.TrimSpace(args.idempotencyKey) != args.idempotencyKey ||
		len(args.idempotencyKey) > 512 {
		return cutoverArgs{}, errors.New(
			"require -operation cutover|rollback, -task-id, positive -user-id and -idempotency-key")
	}
	return args, nil
}

func migrationOwnerDatabaseURL() (string, error) {
	if value := strings.TrimSpace(os.Getenv(migrationOwnerDatabaseURLEnv)); value != "" {
		return value, nil
	}
	directory := strings.TrimSpace(os.Getenv(migrationOwnerCredentialDirectory))
	if directory == "" {
		return "", errors.New("migration-owner database credential is unavailable")
	}
	payload, err := os.ReadFile(filepath.Join(directory, migrationOwnerDatabaseCredential))
	if err != nil {
		return "", errors.New("read migration-owner database credential")
	}
	value := strings.TrimSpace(string(payload))
	if value == "" {
		return "", errors.New("migration-owner database credential is empty")
	}
	return value, nil
}
