package scheduler

import (
	"testing"

	"github.com/YouToco/vane/server/types"
	"github.com/YouToco/vane/server/workflow"
)

func TestCompiledToolRuntimeCanary_IsExactAndSelectorCanDisable(t *testing.T) {
	s := &Scheduler{}
	WithCompiledRuntimeRollout(true, "", true)(s)
	WithCompiledToolRuntimeCanary("  task-tool  ")(s)

	if got := s.runtimeVersionFor(
		"task-tool", types.ExecutionModeCompiled,
	); got != workflow.CompiledRuntimeToolSnapshotV2 {
		t.Fatalf("Tool canary runtime=%q", got)
	}
	if got := s.runtimeVersionFor(
		"task-other", types.ExecutionModeCompiled,
	); got != workflow.CompiledRuntimeSnapshotV1 {
		t.Fatalf("non-canary runtime=%q", got)
	}

	WithCompiledToolRuntimeCanary("")(s)
	if got := s.runtimeVersionFor(
		"task-tool", types.ExecutionModeCompiled,
	); got != workflow.CompiledRuntimeSnapshotV1 {
		t.Fatalf("rolled-back Tool canary runtime=%q", got)
	}
}

func TestCompiledToolRuntimeCanary_RejectsNonCompiledMode(t *testing.T) {
	s := &Scheduler{}
	WithCompiledRuntimeRollout(true, "", true)(s)
	WithCompiledToolRuntimeCanary("task-tool")(s)
	if got := s.runtimeVersionFor(
		"task-tool", types.ExecutionModeDiscoverAtRun,
	); got != "" {
		t.Fatalf("legacy task selected Tool runtime %q", got)
	}
}

func TestCompiledToolRuntimeCanary_RequiresCompiledRollout(t *testing.T) {
	s := &Scheduler{}
	WithCompiledToolRuntimeCanary("task-tool")(s)
	if got := s.runtimeVersionFor(
		"task-tool", types.ExecutionModeCompiled,
	); got != "" {
		t.Fatalf("Tool canary escaped compiled rollout with runtime %q", got)
	}
}
