package scheduler

import (
	"testing"

	"github.com/YouToco/vane/workflow"
)

func TestExecutiveBriefRolloutIsNestedInsideStructuredEventEvidence(
	t *testing.T,
) {
	s := &Scheduler{}
	WithExecutiveBriefRollout(true, "task-a", false)(s)
	if got := s.executiveBrief.runtimeVersionForExecutiveBrief(
		"task-a", workflow.CompiledRuntimeStructuredInsightV1,
	); got != workflow.CompiledRuntimeStructuredInsightV1 {
		t.Fatalf("non-event runtime advanced to %q", got)
	}
	if got := s.executiveBrief.runtimeVersionForExecutiveBrief(
		"task-b", workflow.CompiledRuntimeStructuredEventEvidenceV1,
	); got != workflow.CompiledRuntimeStructuredEventEvidenceV1 {
		t.Fatalf("non-canary runtime advanced to %q", got)
	}
	if got := s.executiveBrief.runtimeVersionForExecutiveBrief(
		"task-a", workflow.CompiledRuntimeStructuredEventEvidenceV1,
	); got != workflow.CompiledRuntimeExecutiveBriefV1 {
		t.Fatalf("canary runtime = %q", got)
	}
}

func TestExecutiveBriefRolloutInvalidScopesFailClosed(t *testing.T) {
	for _, configure := range []func(*Scheduler){
		func(s *Scheduler) {
			WithExecutiveBriefRollout(false, "task-a", false)(s)
		},
		func(s *Scheduler) {
			WithExecutiveBriefRollout(true, "", false)(s)
		},
		func(s *Scheduler) {
			WithExecutiveBriefRollout(true, "task-a", true)(s)
		},
	} {
		s := &Scheduler{}
		configure(s)
		if got := s.executiveBrief.runtimeVersionForExecutiveBrief(
			"task-a", workflow.CompiledRuntimeStructuredEventEvidenceV1,
		); got != workflow.CompiledRuntimeStructuredEventEvidenceV1 {
			t.Fatalf("invalid rollout advanced to %q", got)
		}
	}
}
