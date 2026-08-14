package scheduler

import (
	"strings"

	"github.com/YouToco/vane/server/workflow"
)

type canonicalBriefRollout struct {
	enabled  bool
	canaryID string
	allowAll bool
}

// WithCanonicalBriefRollout freezes P1-C selection into a durable Action.
// It can decorate only a task already selected for the P1-B RunOutcome
// runtime; invalid direct options fail closed.
func WithCanonicalBriefRollout(
	enabled bool, canaryScheduleID string, allowAll bool,
) SchedulerOption {
	return func(s *Scheduler) {
		canaryScheduleID = strings.TrimSpace(canaryScheduleID)
		valid := enabled && ((canaryScheduleID != "") != allowAll)
		if !valid {
			s.canonicalBrief = canonicalBriefRollout{}
			return
		}
		s.canonicalBrief = canonicalBriefRollout{
			enabled: true, canaryID: canaryScheduleID, allowAll: allowAll,
		}
	}
}

func (r canonicalBriefRollout) runtimeVersionFor(
	taskID, runtimeVersion string,
) string {
	if !r.enabled ||
		runtimeVersion != workflow.CompiledRuntimeRunOutcomeV1 ||
		strings.TrimSpace(taskID) == "" {
		return runtimeVersion
	}
	if r.allowAll || taskID == r.canaryID {
		return workflow.CompiledRuntimeCanonicalBriefV1
	}
	return runtimeVersion
}
