package runcontext

import (
	"strings"
	"testing"

	"github.com/YouToco/vane/types"
)

func TestResearchExecutionTraceV3BindsExactPlanStep(t *testing.T) {
	identity := types.RunIdentity{
		TemporalWorkflowID: "workflow-trace", TemporalRunID: "run-trace",
		RunKind: types.RunSnapshotKindScheduled, TenantID: 7, UserID: 9,
		TaskID: "task-trace",
	}
	digest := strings.Repeat("a", 64)
	first, err := ResearchExecutionTraceV3(identity, 11, digest, 0, "same-invocation")
	if err != nil {
		t.Fatal(err)
	}
	relabeled, err := ResearchExecutionTraceV3(identity, 11, digest, 1, "same-invocation")
	if err != nil {
		t.Fatal(err)
	}
	if first == relabeled {
		t.Fatal("execution trace did not bind ordinal")
	}
}
