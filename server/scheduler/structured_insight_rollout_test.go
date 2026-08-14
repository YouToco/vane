package scheduler

import (
	"testing"

	"github.com/YouToco/vane/types"
	"github.com/YouToco/vane/workflow"
)

func TestStructuredInsightRolloutIsNestedInsideCanonicalBrief(t *testing.T) {
	s := &Scheduler{}
	WithStructuredInsightRollout(
		true, "task-a", false, true, "task-a", false)(s)
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
	WithStructuredInsightRollout(
		true, "task-a", false, true, "task-a", false)(s)
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
		WithStructuredInsightRollout(
			true, "", false, true, "task-a", false),
		WithStructuredInsightRollout(
			true, "task-a", true, true, "task-a", false),
		WithStructuredInsightRollout(
			false, "task-a", false, true, "task-a", false),
		WithStructuredInsightRollout(
			true, "task-a", false, false, "", false),
		WithStructuredInsightRollout(
			true, "task-a", false, true, "task-b", false),
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

func TestStructuredEventEvidenceRolloutIsNestedInsideStructuredInsight(
	t *testing.T,
) {
	s := New(nil, "queue", nil,
		WithCompiledRuntimeRollout(true, "task-a", false),
		WithRunOutcomeRollout(true, "task-a", false),
		WithCanonicalBriefRollout(true, "task-a", false),
		WithStructuredInsightRollout(
			true, "task-a", false, true, "task-a", false),
		WithStructuredEventEvidenceRollout(
			true, "task-a", false, "task-a"),
	)
	if got := s.runtimeVersionFor(
		"task-a", types.ExecutionModeCompiled,
	); got != workflow.CompiledRuntimeStructuredEventEvidenceV1 {
		t.Fatalf("event evidence runtime=%q", got)
	}
	if got := s.runtimeVersionFor(
		"task-b", types.ExecutionModeCompiled,
	); got == workflow.CompiledRuntimeStructuredEventEvidenceV1 {
		t.Fatalf("out-of-scope task gained event evidence runtime=%q", got)
	}

	withoutStructured := New(nil, "queue", nil,
		WithCompiledRuntimeRollout(true, "task-a", false),
		WithRunOutcomeRollout(true, "task-a", false),
		WithCanonicalBriefRollout(true, "task-a", false),
		WithStructuredEventEvidenceRollout(
			true, "task-a", false, "task-a"),
	)
	if got := withoutStructured.runtimeVersionFor(
		"task-a", types.ExecutionModeCompiled,
	); got != workflow.CompiledRuntimeCanonicalBriefV1 {
		t.Fatalf("event evidence escaped structured nesting: %q", got)
	}

	defaultOff := New(nil, "queue", nil,
		WithCompiledRuntimeRollout(true, "task-a", false),
		WithRunOutcomeRollout(true, "task-a", false),
		WithCanonicalBriefRollout(true, "task-a", false),
		WithStructuredInsightRollout(
			true, "task-a", false, true, "task-a", false),
	)
	if got := defaultOff.runtimeVersionFor(
		"task-a", types.ExecutionModeCompiled,
	); got != workflow.CompiledRuntimeStructuredInsightV1 {
		t.Fatalf("default-off event evidence changed runtime to %q", got)
	}

	for _, option := range []SchedulerOption{
		WithStructuredEventEvidenceRollout(true, "", false, "task-a"),
		WithStructuredEventEvidenceRollout(
			true, "task-a", true, "task-a"),
		WithStructuredEventEvidenceRollout(
			false, "task-a", false, "task-a"),
		WithStructuredEventEvidenceRollout(
			true, "task-a", false, "task-b"),
	} {
		invalid := New(nil, "queue", nil,
			WithCompiledRuntimeRollout(true, "task-a", false),
			WithRunOutcomeRollout(true, "task-a", false),
			WithCanonicalBriefRollout(true, "task-a", false),
			WithStructuredInsightRollout(
				true, "task-a", false, true, "task-a", false),
			option,
		)
		if got := invalid.runtimeVersionFor(
			"task-a", types.ExecutionModeCompiled,
		); got != workflow.CompiledRuntimeStructuredInsightV1 {
			t.Fatalf("invalid event evidence rollout changed runtime to %q", got)
		}
	}
}
