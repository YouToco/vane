package workflow

import (
	"os"
	"strings"
	"testing"
)

// The command sequence is now replay-safe and callable by an explicitly
// versioned Action, but production Action writers remain on V1 until the
// dedicated canary selector is deliberately enabled.
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
		"a.ScoreToolCandidatesV2",
		"a.CardGenToolCandidatesV2",
		"MaximumAttempts:        1",
	} {
		if !strings.Contains(string(pipelineSource), required) {
			t.Fatalf("Tool V2 side-effect guard lacks %q", required)
		}
	}

	for _, name := range []string{
		"../scheduler/scheduler.go",
		"../scheduler/task_schedule.go",
	} {
		source, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(
			string(source), "CompiledRuntimeToolSnapshotV2",
		) {
			t.Fatalf("production Action writer enabled Tool V2 before canary: %s",
				name)
		}
	}
}
