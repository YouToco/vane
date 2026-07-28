package scheduler

import (
	"strings"

	"github.com/YouToco/vane/workflow"
)

type structuredInsightRollout struct {
	enabled  bool
	canaryID string
	allowAll bool
}

func WithStructuredInsightRollout(
	enabled bool, canaryScheduleID string, allowAll bool,
) SchedulerOption {
	return func(s *Scheduler) {
		canaryScheduleID = strings.TrimSpace(canaryScheduleID)
		if !enabled || ((canaryScheduleID != "") == allowAll) {
			s.structuredInsight = structuredInsightRollout{}
			return
		}
		s.structuredInsight = structuredInsightRollout{
			enabled: true, canaryID: canaryScheduleID, allowAll: allowAll,
		}
	}
}

func (r structuredInsightRollout) runtimeVersionFor(
	taskID, runtimeVersion string,
) string {
	if !r.enabled ||
		runtimeVersion != workflow.CompiledRuntimeCanonicalBriefV1 ||
		strings.TrimSpace(taskID) == "" {
		return runtimeVersion
	}
	if r.allowAll || taskID == r.canaryID {
		return workflow.CompiledRuntimeStructuredInsightV1
	}
	return runtimeVersion
}
