package scheduler

import (
	"strings"

	"github.com/YouToco/vane/types"
	"github.com/YouToco/vane/workflow"
)

type runOutcomeRollout struct {
	enabled  bool
	canaryID string
	allowAll bool
}

// WithRunOutcomeRollout freezes the independent P1-B decision into a durable
// compiled-runtime Action. Invalid direct options fail closed.
func WithRunOutcomeRollout(
	enabled bool, canaryScheduleID string, allowAll bool,
) SchedulerOption {
	return func(s *Scheduler) {
		canaryScheduleID = strings.TrimSpace(canaryScheduleID)
		valid := enabled && ((canaryScheduleID != "") != allowAll)
		if !valid {
			s.runOutcome = runOutcomeRollout{}
			return
		}
		s.runOutcome = runOutcomeRollout{
			enabled: true, canaryID: canaryScheduleID, allowAll: allowAll,
		}
	}
}

func (r runOutcomeRollout) runtimeVersionFor(
	taskID, runtimeVersion string,
) string {
	if !r.enabled || runtimeVersion != workflow.CompiledRuntimeSnapshotV1 ||
		strings.TrimSpace(taskID) == "" {
		return runtimeVersion
	}
	if r.allowAll || taskID == r.canaryID {
		return workflow.CompiledRuntimeRunOutcomeV1
	}
	return runtimeVersion
}

// runtimeVersionFor composes the independent rollout layers in one place so
// every durable Action constructor, repair, and rename path makes the same
// decision. RunOutcome can only decorate an already-selected compiled runtime.
func (s *Scheduler) runtimeVersionFor(
	taskID string, executionMode types.ExecutionMode,
) string {
	if executionMode != "" && executionMode != types.ExecutionModeCompiled {
		return ""
	}
	compiled := s.compiledRuntime.runtimeVersionFor(taskID)
	outcome := s.runOutcome.runtimeVersionFor(taskID, compiled)
	brief := s.canonicalBrief.runtimeVersionFor(taskID, outcome)
	structured := s.structuredInsight.runtimeVersionFor(taskID, brief)
	eventEvidence := s.structuredEventEvidence.runtimeVersionForEventEvidence(
		taskID, structured)
	return s.executiveBrief.runtimeVersionForExecutiveBrief(
		taskID, eventEvidence)
}
