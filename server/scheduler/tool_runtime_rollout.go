package scheduler

import (
	"strings"

	"github.com/YouToco/vane/server/workflow"
)

// toolRuntimeRollout is intentionally single-task only. A Tool task is not
// dual-written for V1; removing the exact ID is therefore admitted only after
// the task is paused and excluded from active Action reconciliation.
type toolRuntimeRollout struct {
	canaryID string
}

func WithCompiledToolRuntimeCanary(
	canaryScheduleID string,
) SchedulerOption {
	return func(s *Scheduler) {
		s.toolRuntime = toolRuntimeRollout{
			canaryID: strings.TrimSpace(canaryScheduleID),
		}
	}
}

func (r toolRuntimeRollout) runtimeVersionFor(taskID string) string {
	if r.canaryID == "" || taskID != r.canaryID {
		return ""
	}
	return workflow.CompiledRuntimeToolSnapshotV2
}
