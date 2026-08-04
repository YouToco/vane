// Command researchcutover performs the durable exact-task V3 cutover or
// rollback saga. It is an operator-only control plane and is not exposed to
// HTTP, Feishu, or the Agent tool surface.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"go.temporal.io/sdk/client"

	"github.com/YouToco/vane/internal/researchoperator"
	"github.com/YouToco/vane/scheduler"
	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
)

type cutoverArgs struct {
	operation      string
	taskID         string
	idempotencyKey string
	planDigest     string
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
	if err := researchoperator.RequireExactTask(args.taskID); err != nil {
		return err
	}
	temporalConfig, err := researchoperator.LoadTemporalConfig()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	operatorDatabaseURL, err := researchoperator.MigrationDatabaseURL()
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
		scheduler.WithTaskScheduleNamespace(temporalConfig.Namespace),
		scheduler.WithResearchRuntimeV3AuthorityCanary(
			args.taskID))

	var inspection types.ResearchV3CutoverInspection
	switch args.operation {
	case "preflight":
		inspection, err = sched.PreflightResearchV3(ctx, scope, args.idempotencyKey)
	case "status":
		inspection, err = sched.StatusResearchV3(ctx, scope, args.idempotencyKey)
	case "verify":
		inspection, err = sched.VerifyResearchV3(ctx, scope, args.idempotencyKey)
	case "cutover":
		_, err = sched.CutoverResearchV3(
			ctx, scope, args.idempotencyKey, args.planDigest)
		if err == nil {
			inspection, err = sched.VerifyResearchV3(ctx, scope, args.idempotencyKey)
		}
	case "rollback":
		_, err = sched.RollbackResearchV3(
			ctx, args.taskID, scope.UserID, args.idempotencyKey)
		if err == nil {
			inspection, err = sched.VerifyResearchV3(ctx, scope, args.idempotencyKey)
		}
	}
	if err != nil {
		return fmt.Errorf("%s Research V3 task: %w", args.operation, err)
	}
	payload, err := json.MarshalIndent(inspection, "", "  ")
	if err != nil {
		return errors.New("encode Research V3 operator result")
	}
	fmt.Println(string(payload))
	return nil
}

func parseCutoverArgs(arguments []string) (cutoverArgs, error) {
	set := flag.NewFlagSet("researchcutover", flag.ContinueOnError)
	set.SetOutput(os.Stderr)
	var args cutoverArgs
	set.StringVar(&args.operation, "operation", "", "preflight, status, cutover, verify or rollback")
	set.StringVar(&args.taskID, "task-id", "", "exact configured schedule ID")
	set.StringVar(&args.idempotencyKey, "idempotency-key", "", "stable saga retry key")
	set.StringVar(&args.planDigest, "plan-digest", "", "exact preflight digest (cutover only)")
	if err := set.Parse(arguments); err != nil {
		return cutoverArgs{}, err
	}
	validOperation := args.operation == "preflight" || args.operation == "status" ||
		args.operation == "cutover" || args.operation == "verify" ||
		args.operation == "rollback"
	validPlan := (args.operation == "cutover" && validDigest(args.planDigest)) ||
		(args.operation != "cutover" && args.planDigest == "")
	if set.NArg() != 0 || !validOperation || !validPlan ||
		args.taskID == "" || strings.TrimSpace(args.taskID) != args.taskID ||
		args.idempotencyKey == "" ||
		strings.TrimSpace(args.idempotencyKey) != args.idempotencyKey ||
		len(args.idempotencyKey) > 512 {
		return cutoverArgs{}, errors.New(
			"require -operation preflight|status|cutover|verify|rollback, -task-id, -idempotency-key, and -plan-digest only for cutover")
	}
	return args, nil
}

func validDigest(value string) bool {
	if len(value) != 64 || value != strings.ToLower(value) {
		return false
	}
	for _, char := range value {
		if !((char >= '0' && char <= '9') || (char >= 'a' && char <= 'f')) {
			return false
		}
	}
	return true
}
