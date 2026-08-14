package scheduler

import (
	"testing"

	"github.com/YouToco/vane/server/types"
	"github.com/YouToco/vane/server/workflow"
)

func TestCanonicalBriefRolloutIsNestedInsideRunOutcome(t *testing.T) {
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
			name: "off preserves outcome", taskID: "task-a",
			runtime: workflow.CompiledRuntimeRunOutcomeV1,
			want:    workflow.CompiledRuntimeRunOutcomeV1,
		},
		{
			name: "canary match", enabled: true, canary: "task-a",
			taskID: "task-a", runtime: workflow.CompiledRuntimeRunOutcomeV1,
			want: workflow.CompiledRuntimeCanonicalBriefV1,
		},
		{
			name: "canary miss", enabled: true, canary: "task-a",
			taskID: "task-b", runtime: workflow.CompiledRuntimeRunOutcomeV1,
			want: workflow.CompiledRuntimeRunOutcomeV1,
		},
		{
			name: "allow all", enabled: true, allowAll: true,
			taskID: "task-b", runtime: workflow.CompiledRuntimeRunOutcomeV1,
			want: workflow.CompiledRuntimeCanonicalBriefV1,
		},
		{
			name:    "compiled without outcome cannot enter",
			enabled: true, allowAll: true, taskID: "task-b",
			runtime: workflow.CompiledRuntimeSnapshotV1,
			want:    workflow.CompiledRuntimeSnapshotV1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := &Scheduler{}
			WithCanonicalBriefRollout(
				tc.enabled, tc.canary, tc.allowAll)(s)
			if got := s.canonicalBrief.runtimeVersionFor(
				tc.taskID, tc.runtime); got != tc.want {
				t.Fatalf("runtime = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestCanonicalBriefRolloutComposesThroughScheduler(t *testing.T) {
	s := &Scheduler{}
	WithCompiledRuntimeRollout(true, "task-a", false)(s)
	WithRunOutcomeRollout(true, "task-a", false)(s)
	WithCanonicalBriefRollout(true, "task-a", false)(s)
	if got := s.runtimeVersionFor(
		"task-a", types.ExecutionModeCompiled,
	); got != workflow.CompiledRuntimeCanonicalBriefV1 {
		t.Fatalf("composed runtime = %q", got)
	}
	if got := s.runtimeVersionFor(
		"task-b", types.ExecutionModeCompiled,
	); got != "" {
		t.Fatalf("outside compiled canary received runtime %q", got)
	}
}

func TestCanonicalBriefRolloutInvalidCombinationFailsClosed(t *testing.T) {
	for _, apply := range []SchedulerOption{
		WithCanonicalBriefRollout(true, "", false),
		WithCanonicalBriefRollout(true, "task-a", true),
		WithCanonicalBriefRollout(false, "task-a", false),
	} {
		s := &Scheduler{}
		apply(s)
		if got := s.canonicalBrief.runtimeVersionFor(
			"task-a", workflow.CompiledRuntimeRunOutcomeV1,
		); got != workflow.CompiledRuntimeRunOutcomeV1 {
			t.Fatalf("invalid rollout expanded runtime to %q", got)
		}
	}
}
