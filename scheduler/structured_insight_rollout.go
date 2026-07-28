package scheduler

import (
	"strings"

	"github.com/YouToco/vane/workflow"
)

type structuredInsightRollout struct {
	generation rolloutScopeV1
	renderer   rolloutScopeV1
}

type rolloutScopeV1 struct {
	enabled  bool
	canaryID string
	allowAll bool
}

func (r rolloutScopeV1) allows(taskID string) bool {
	return r.enabled && (r.allowAll || taskID == r.canaryID)
}

func WithStructuredInsightRollout(
	enabled bool, canaryScheduleID string, allowAll bool,
	rendererEnabled bool, rendererCanaryScheduleID string,
	rendererAllowAll bool,
) SchedulerOption {
	return func(s *Scheduler) {
		canaryScheduleID = strings.TrimSpace(canaryScheduleID)
		rendererCanaryScheduleID =
			strings.TrimSpace(rendererCanaryScheduleID)
		if !enabled || ((canaryScheduleID != "") == allowAll) ||
			!rendererEnabled ||
			((rendererCanaryScheduleID != "") == rendererAllowAll) {
			s.structuredInsight = structuredInsightRollout{}
			return
		}
		s.structuredInsight = structuredInsightRollout{
			generation: rolloutScopeV1{
				enabled: true, canaryID: canaryScheduleID, allowAll: allowAll,
			},
			renderer: rolloutScopeV1{
				enabled: true, canaryID: rendererCanaryScheduleID,
				allowAll: rendererAllowAll,
			},
		}
	}
}

func (r structuredInsightRollout) runtimeVersionFor(
	taskID, runtimeVersion string,
) string {
	if !r.generation.allows(taskID) ||
		!r.renderer.allows(taskID) ||
		runtimeVersion != workflow.CompiledRuntimeCanonicalBriefV1 ||
		strings.TrimSpace(taskID) == "" {
		return runtimeVersion
	}
	return workflow.CompiledRuntimeStructuredInsightV1
}

func WithStructuredEventEvidenceRollout(
	enabled bool, canaryScheduleID string, allowAll bool,
) SchedulerOption {
	return func(s *Scheduler) {
		canaryScheduleID = strings.TrimSpace(canaryScheduleID)
		if !enabled || ((canaryScheduleID != "") == allowAll) {
			s.structuredEventEvidence = rolloutScopeV1{}
			return
		}
		s.structuredEventEvidence = rolloutScopeV1{
			enabled: true, canaryID: canaryScheduleID, allowAll: allowAll,
		}
	}
}

func (r rolloutScopeV1) runtimeVersionForEventEvidence(
	taskID, runtimeVersion string,
) string {
	if !r.allows(taskID) ||
		runtimeVersion != workflow.CompiledRuntimeStructuredInsightV1 ||
		strings.TrimSpace(taskID) == "" {
		return runtimeVersion
	}
	return workflow.CompiledRuntimeStructuredEventEvidenceV1
}
