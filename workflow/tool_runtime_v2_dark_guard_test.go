package workflow

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Until Tool execution and invocation-scoped observation writes land, the V2
// run-start Activity may be registered but must remain unreachable from every
// durable Workflow and Schedule Action writer.
func TestPrepareToolRunV2RemainsDark(t *testing.T) {
	workflowSource, err := os.ReadFile("workflow.go")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(workflowSource), "PrepareToolRunV2") {
		t.Fatal("PushPipelineWorkflow selected dark PrepareToolRunV2")
	}
	err = filepath.WalkDir("..", func(
		name string,
		entry os.DirEntry,
		walkErr error,
	) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "node_modules", "vendor":
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(name) != ".go" ||
			strings.HasSuffix(name, "_test.go") ||
			(filepath.Base(name) == "types.go" &&
				filepath.Base(filepath.Dir(name)) == "workflow") {
			return nil
		}
		source, err := os.ReadFile(name)
		if err != nil {
			return err
		}
		if strings.Contains(
			string(source), "CompiledRuntimeToolSnapshotV2",
		) {
			t.Fatalf("production writer selected dark Tool runtime: %s", name)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
