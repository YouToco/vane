// Package workflowruntime owns the dependency-light catalog of durable
// PushParams runtime labels. Both the live scheduler writer and retained
// checkpoint readers must use these predicates so a rollout cannot create
// bytes that the recovery path rejects.
package workflowruntime

const (
	CompiledSnapshotV1  = "compiled-snapshot/v1"
	RunOutcomeV1        = "compiled-snapshot/v1+run-outcome/v1"
	CanonicalBriefV1    = "compiled-snapshot/v1+run-outcome/v1+brief/v1"
	StructuredInsightV1 = "compiled-snapshot/v1+run-outcome/v1+brief/v1+" +
		"structured-insight/v1"
	StructuredEventEvidenceV1 = "compiled-snapshot/v1+run-outcome/v1+brief/v1+" +
		"structured-insight/v1+event-evidence/v1"
	ExecutiveBriefV1 = "compiled-snapshot/v1+run-outcome/v1+brief/v1+" +
		"structured-insight/v1+event-evidence/v1+executive-brief/v1"
	CompiledToolSnapshotV2 = "compiled-tool-snapshot/v2"
)

func IsCompiledV1(version string) bool {
	switch version {
	case CompiledSnapshotV1,
		RunOutcomeV1,
		CanonicalBriefV1,
		StructuredInsightV1,
		StructuredEventEvidenceV1,
		ExecutiveBriefV1:
		return true
	default:
		return false
	}
}

func IsCompiledToolV2(version string) bool {
	return version == CompiledToolSnapshotV2
}

// IsDurableActionRuntime includes the empty legacy selector retained by
// pre-rollout schedule Actions. Unknown future labels remain fail-closed until
// this shared catalog is intentionally extended.
func IsDurableActionRuntime(version string) bool {
	return version == "" || IsCompiledV1(version) || IsCompiledToolV2(version)
}
