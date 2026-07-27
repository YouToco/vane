package scheduler

import (
	"strings"

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
