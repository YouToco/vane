package types

import "testing"

func TestRunSnapshotReferenceProtocolsRemainTypeSeparated(t *testing.T) {
	base := RunSnapshotRef{
		SchemaVersion: RunSnapshotSchemaVersionV2,
		SnapshotID:    1, TemporalWorkflowID: "wf-task", TemporalRunID: "run",
		RunKind: RunSnapshotKindScheduled, TenantID: 1, UserID: 2, TaskID: "task",
		Mode:             ExecutionModeCompiled,
		DefinitionDigest: runSnapshotTestDigestA,
		PlanDigest:       runSnapshotTestDigestA, AdaptiveVersion: 1,
		Policy: RuntimePolicyDigests{
			CapabilityCatalogDigest: runSnapshotTestDigestA,
			ToolPolicyDigest:        runSnapshotTestDigestA,
			PromptPolicyDigest:      runSnapshotTestDigestA,
			ModelPolicyDigest:       runSnapshotTestDigestA,
			QuotaPolicyDigest:       runSnapshotTestDigestA,
		},
		PayloadDigest: runSnapshotTestDigestA,
	}
	if _, err := base.Seal(); err == nil {
		t.Fatal("retained RunSnapshotRef V1 accepted the V2 schema")
	}
	v2 := RunSnapshotRefV2{
		SchemaVersion: base.SchemaVersion, SnapshotID: base.SnapshotID,
		TemporalWorkflowID: base.TemporalWorkflowID,
		TemporalRunID:      base.TemporalRunID, RunKind: base.RunKind,
		TenantID: base.TenantID, UserID: base.UserID, TaskID: base.TaskID,
		Mode: base.Mode, DefinitionDigest: base.DefinitionDigest,
		PlanDigest: base.PlanDigest, AdaptiveVersion: base.AdaptiveVersion,
		Policy: base.Policy, PayloadDigest: base.PayloadDigest,
	}
	sealed, err := v2.Seal()
	if err != nil || sealed.Validate() != nil {
		t.Fatalf("V2 reference could not be sealed: ref=%+v err=%v", sealed, err)
	}
}
