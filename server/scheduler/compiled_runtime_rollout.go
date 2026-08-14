package scheduler

import (
	"strings"

	"github.com/YouToco/vane/workflow"
)

type compiledRuntimeRollout struct {
	enabled  bool
	canaryID string
	allowAll bool
}

// WithCompiledRuntimeRollout freezes the process rollout decision into each
// durable Schedule Action. Invalid direct option combinations fail safe to
// disabled; production config rejects them earlier with a useful error.
func WithCompiledRuntimeRollout(enabled bool, canaryScheduleID string, allowAll bool) SchedulerOption {
	return func(s *Scheduler) {
		canaryScheduleID = strings.TrimSpace(canaryScheduleID)
		valid := enabled && ((canaryScheduleID != "") != allowAll)
		if !valid {
			s.compiledRuntime = compiledRuntimeRollout{}
			return
		}
		s.compiledRuntime = compiledRuntimeRollout{
			enabled: true, canaryID: canaryScheduleID, allowAll: allowAll,
		}
	}
}

func (r compiledRuntimeRollout) runtimeVersionFor(taskID string) string {
	if !r.enabled || strings.TrimSpace(taskID) == "" {
		return ""
	}
	if r.allowAll || taskID == r.canaryID {
		return workflow.CompiledRuntimeSnapshotV1
	}
	return ""
}
