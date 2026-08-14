package scheduler

import (
	"strings"

	"github.com/YouToco/vane/server/workflow"
)

// WithExecutiveBriefRollout selects P2-D for one exact structured-event task
// or an explicitly enabled all-task scope. The nesting check keeps synthesis
// unreachable from legacy, ad-hoc and less capable compiled runtimes.
func WithExecutiveBriefRollout(
	enabled bool, canaryScheduleID string, allowAll bool,
) SchedulerOption {
	return func(s *Scheduler) {
		canaryScheduleID = strings.TrimSpace(canaryScheduleID)
		if !enabled || ((canaryScheduleID != "") == allowAll) {
			s.executiveBrief = rolloutScopeV1{}
			return
		}
		s.executiveBrief = rolloutScopeV1{
			enabled: true, canaryID: canaryScheduleID, allowAll: allowAll,
		}
	}
}

func (r rolloutScopeV1) runtimeVersionForExecutiveBrief(
	taskID, runtimeVersion string,
) string {
	if !r.allows(taskID) ||
		runtimeVersion != workflow.CompiledRuntimeStructuredEventEvidenceV1 ||
		strings.TrimSpace(taskID) == "" {
		return runtimeVersion
	}
	return workflow.CompiledRuntimeExecutiveBriefV1
}
