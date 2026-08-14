package scheduler

import (
	"testing"

	"github.com/YouToco/vane/server/types"
	"github.com/YouToco/vane/server/workflow"
)

func TestRunOutcomeRolloutIsNestedInsideCompiledRuntime(t *testing.T) {
	cases := []struct {
		name     string
		enabled  bool
		canary   string
		allowAll bool
		taskID   string
		runtime  string
		want     string
	}{
		{
			name: "off preserves compiled", taskID: "task-a",
			runtime: workflow.CompiledRuntimeSnapshotV1,
			want:    workflow.CompiledRuntimeSnapshotV1,
		},
		{
			name: "canary match", enabled: true, canary: "task-a",
			taskID: "task-a", runtime: workflow.CompiledRuntimeSnapshotV1,
			want: workflow.CompiledRuntimeRunOutcomeV1,
		},
		{
			name: "canary miss", enabled: true, canary: "task-a",
			taskID: "task-b", runtime: workflow.CompiledRuntimeSnapshotV1,
			want: workflow.CompiledRuntimeSnapshotV1,
		},
		{
			name: "allow all", enabled: true, allowAll: true,
			taskID: "task-b", runtime: workflow.CompiledRuntimeSnapshotV1,
			want: workflow.CompiledRuntimeRunOutcomeV1,
		},
		{
			name: "legacy cannot enter", enabled: true, allowAll: true,
			taskID: "task-b", runtime: "", want: "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := &Scheduler{}
			WithRunOutcomeRollout(
				tc.enabled, tc.canary, tc.allowAll)(s)
			if got := s.runOutcome.runtimeVersionFor(
				tc.taskID, tc.runtime); got != tc.want {
				t.Fatalf("runtime = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestRunOutcomeRolloutInvalidCombinationFailsClosed(t *testing.T) {
	for _, apply := range []SchedulerOption{
		WithRunOutcomeRollout(true, "", false),
		WithRunOutcomeRollout(true, "task-a", true),
		WithRunOutcomeRollout(false, "task-a", false),
	} {
		s := &Scheduler{}
		apply(s)
		if got := s.runOutcome.runtimeVersionFor(
			"task-a", workflow.CompiledRuntimeSnapshotV1,
		); got != workflow.CompiledRuntimeSnapshotV1 {
			t.Fatalf("invalid rollout expanded runtime to %q", got)
		}
	}
}

func TestSchedulerRuntimeResolverComposesAndRejectsDynamicMode(t *testing.T) {
	s := &Scheduler{}
	WithCompiledRuntimeRollout(true, "task-a", false)(s)
	WithRunOutcomeRollout(true, "task-a", false)(s)
	if got := s.runtimeVersionFor(
		"task-a", types.ExecutionModeCompiled,
	); got != workflow.CompiledRuntimeRunOutcomeV1 {
		t.Fatalf("composed runtime = %q", got)
	}
	if got := s.runtimeVersionFor(
		"task-a", types.ExecutionModeDiscoverAtRun,
	); got != "" {
		t.Fatalf("dynamic task received compiled runtime %q", got)
	}
}
