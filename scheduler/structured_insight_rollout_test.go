package scheduler

import (
	"testing"

	"github.com/YouToco/vane/types"
	"github.com/YouToco/vane/workflow"
)

func TestStructuredInsightRolloutIsNestedInsideCanonicalBrief(t *testing.T) {
	s := &Scheduler{}
	WithStructuredInsightRollout(true, "task-a", false)(s)
	if got := s.structuredInsight.runtimeVersionFor(
		"task-a", workflow.CompiledRuntimeRunOutcomeV1,
	); got != workflow.CompiledRuntimeRunOutcomeV1 {
		t.Fatalf("non-Brief runtime expanded to %q", got)
	}
	if got := s.structuredInsight.runtimeVersionFor(
		"task-a", workflow.CompiledRuntimeCanonicalBriefV1,
	); got != workflow.CompiledRuntimeStructuredInsightV1 {
		t.Fatalf("Brief runtime = %q", got)
	}
}

func TestStructuredInsightRolloutComposesThroughScheduler(t *testing.T) {
	s := &Scheduler{}
	WithCompiledRuntimeRollout(true, "task-a", false)(s)
	WithRunOutcomeRollout(true, "task-a", false)(s)
	WithCanonicalBriefRollout(true, "task-a", false)(s)
	WithStructuredInsightRollout(true, "task-a", false)(s)
	if got := s.runtimeVersionFor(
		"task-a", types.ExecutionModeCompiled,
	); got != workflow.CompiledRuntimeStructuredInsightV1 {
		t.Fatalf("composed runtime = %q", got)
	}
	if got := s.runtimeVersionFor(
		"task-b", types.ExecutionModeCompiled,
	); got != "" {
		t.Fatalf("outside canary received runtime %q", got)
	}
}

func TestStructuredInsightRolloutInvalidCombinationFailsClosed(t *testing.T) {
	for _, apply := range []SchedulerOption{
		WithStructuredInsightRollout(true, "", false),
		WithStructuredInsightRollout(true, "task-a", true),
		WithStructuredInsightRollout(false, "task-a", false),
	} {
		s := &Scheduler{}
		apply(s)
		if got := s.structuredInsight.runtimeVersionFor(
			"task-a", workflow.CompiledRuntimeCanonicalBriefV1,
		); got != workflow.CompiledRuntimeCanonicalBriefV1 {
			t.Fatalf("invalid rollout expanded runtime to %q", got)
		}
	}
}
