package workflowruntime

import "testing"

func TestDurableActionRuntimeCatalog(t *testing.T) {
	for _, version := range []string{
		"",
		CompiledSnapshotV1,
		RunOutcomeV1,
		CanonicalBriefV1,
		StructuredInsightV1,
		StructuredEventEvidenceV1,
		ExecutiveBriefV1,
		ResearchRunV3,
		CompiledToolSnapshotV2,
	} {
		if !IsDurableActionRuntime(version) {
			t.Fatalf("current durable runtime %q rejected", version)
		}
	}
	if IsDurableActionRuntime("compiled-snapshot/v1+future/v1") {
		t.Fatal("unknown future runtime accepted")
	}
}
