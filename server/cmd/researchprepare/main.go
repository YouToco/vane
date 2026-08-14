// Command researchprepare compiles the exact configured task's current owner
// projection into an immutable, delivery-dark V3 sidecar head, or rolls that
// sidecar operation back. It never connects to Temporal.
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

	"github.com/YouToco/vane/server/internal/researchoperator"
	"github.com/YouToco/vane/server/internal/strictjson"
	"github.com/YouToco/vane/server/store"
	"github.com/YouToco/vane/server/taskstate"
	"github.com/YouToco/vane/server/types"
)

type argsV3 struct {
	operation, taskID, idempotencyKey, policyFile string
}

type policyV3 struct {
	Notification  taskstate.NotificationPolicyV3 `json:"notification"`
	Output        taskstate.OutputPreferenceV3   `json:"output"`
	PlannerBudget types.PlannerBudget            `json:"planner_budget"`
	ResearchScope *taskstate.ResearchScopeV3     `json:"research_scope,omitempty"`
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "vane-research-prepare:", err)
		os.Exit(1)
	}
}

func run(arguments []string) error {
	a, err := parseArgs(arguments)
	if err != nil {
		return err
	}
	if err := researchoperator.RequireExactTask(a.taskID); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
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
	scope, err := st.ResolveResearchV3OperatorScope(ctx, a.taskID)
	if err != nil {
		return fmt.Errorf("resolve exact owner task: %w", err)
	}
	var op types.ResearchV3DefinitionPrepareOperation
	if a.operation == "prepare" {
		policy, err := loadPolicy(a.policyFile)
		if err != nil {
			return err
		}
		op, err = st.PrepareResearchV3Definition(ctx, taskstate.ResearchV3DefinitionPrepareParams{
			TenantID: scope.TenantID, UserID: scope.UserID, TaskID: a.taskID,
			IdempotencyKey: a.idempotencyKey, Notification: policy.Notification,
			Output: policy.Output, PlannerBudget: policy.PlannerBudget,
			ResearchScope: policy.ResearchScope,
		})
	} else {
		op, err = st.RollbackResearchV3DefinitionPrepare(
			ctx, scope.TenantID, scope.UserID, a.taskID, a.idempotencyKey)
	}
	if err != nil {
		return fmt.Errorf("%s Research V3 definition: %w", a.operation, err)
	}
	fmt.Printf("research V3 definition %s complete: task=%s user=%d version=%d digest=%s phase=%s\n",
		a.operation, op.TaskID, op.UserID, op.Target.Version, op.Target.Digest, op.Phase)
	return nil
}

func parseArgs(arguments []string) (argsV3, error) {
	set := flag.NewFlagSet("researchprepare", flag.ContinueOnError)
	set.SetOutput(os.Stderr)
	var a argsV3
	set.StringVar(&a.operation, "operation", "", "prepare or rollback")
	set.StringVar(&a.taskID, "task-id", "", "exact configured schedule ID")
	set.StringVar(&a.idempotencyKey, "idempotency-key", "", "stable operation retry key")
	set.StringVar(&a.policyFile, "policy-file", "", "explicit V3 policy JSON (prepare only)")
	if err := set.Parse(arguments); err != nil {
		return argsV3{}, err
	}
	validPolicy := (a.operation == "prepare" && a.policyFile != "") || (a.operation == "rollback" && a.policyFile == "")
	if set.NArg() != 0 || (a.operation != "prepare" && a.operation != "rollback") || a.taskID == "" ||
		strings.TrimSpace(a.taskID) != a.taskID || a.idempotencyKey == "" ||
		strings.TrimSpace(a.idempotencyKey) != a.idempotencyKey || len(a.idempotencyKey) > 512 || !validPolicy {
		return argsV3{}, errors.New("require -operation prepare|rollback, -task-id, -idempotency-key, and -policy-file only for prepare")
	}
	return a, nil
}

func loadPolicy(path string) (policyV3, error) {
	payload, err := os.ReadFile(path)
	if err != nil {
		return policyV3{}, errors.New("read V3 policy file")
	}
	var policy policyV3
	if err := strictjson.DecodeExact(payload, &policy); err != nil {
		return policyV3{}, errors.New("V3 policy file is invalid")
	}
	return policy, nil
}
