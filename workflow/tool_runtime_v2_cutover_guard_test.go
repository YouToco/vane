package workflow

import (
	"os"
	"strings"
	"testing"
)

// The command sequence is replay-safe and callable only by an explicitly
// versioned Action. Production selection remains a single-task canary rather
// than an allow-all rollout; rollback pauses the task before removing the ID.
func TestCompiledToolPipelineV2CutoverGuards(t *testing.T) {
	workflowSource, err := os.ReadFile("workflow.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		`"compiled-tool-pipeline-v2"`,
		"IsCompiledToolRuntimeV2(p.RuntimeVersion)",
		"runCompiledToolPipelineV2(ctx, p, traceID, a)",
	} {
		if !strings.Contains(string(workflowSource), required) {
			t.Fatalf("Tool V2 workflow cutover lacks %q", required)
		}
	}

	pipelineSource, err := os.ReadFile("tool_workflow_v2.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"a.ExecuteToolInvocationV2",
		"workflow.GetVersion(",
		"toolObservationQualificationVersionID",
		"toolRunOutcomeVersionID",
		"workflow.DefaultVersion, 1",
		"a.BeginToolRunOutcomeV2",
		"a.FinalizeToolRunOutcomeV2",
		"a.QualifyToolCandidatesV2",
		"a.ScoreToolCandidatesV2",
		"a.CardGenToolCandidatesV2",
		"MaximumAttempts:        1",
	} {
		if !strings.Contains(string(pipelineSource), required) {
			t.Fatalf("Tool V2 side-effect guard lacks %q", required)
		}
	}

	rolloutSource, err := os.ReadFile(
		"../scheduler/tool_runtime_rollout.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"canaryID string",
		`if r.canaryID == "" || taskID != r.canaryID`,
		"CompiledRuntimeToolSnapshotV2",
	} {
		if !strings.Contains(string(rolloutSource), required) {
			t.Fatalf("Tool V2 production canary lacks %q", required)
		}
	}
	if strings.Contains(string(rolloutSource), "allowAll") {
		t.Fatal("Tool V2 production canary gained an allow-all path")
	}
}
