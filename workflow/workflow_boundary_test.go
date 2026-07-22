package workflow

import (
	"go/parser"
	"go/token"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
)

// TestPushPipelineWorkflow_ImportsOnlyDeterministicDependencies keeps the
// orchestration file on Temporal's deterministic side of the Activity
// boundary. An allow-list is intentional: merely denying today's database,
// network, and LLM packages would let a new I/O wrapper bypass the guard.
func TestPushPipelineWorkflow_ImportsOnlyDeterministicDependencies(t *testing.T) {
	t.Parallel()
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate workflow boundary test")
	}
	workflowFile := filepath.Join(filepath.Dir(currentFile), "workflow.go")
	parsed, err := parser.ParseFile(token.NewFileSet(), workflowFile, nil, parser.ImportsOnly)
	if err != nil {
		t.Fatalf("parse workflow.go imports: %v", err)
	}

	allowed := map[string]struct{}{
		"errors":                        {},
		"strings":                       {},
		"time":                          {},
		"github.com/google/uuid":        {},
		"go.temporal.io/sdk/temporal":   {},
		"go.temporal.io/sdk/workflow":   {},
		"github.com/YouToco/vane/types": {},
	}
	for _, spec := range parsed.Imports {
		path, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			t.Fatalf("decode workflow.go import %q: %v", spec.Path.Value, err)
		}
		if _, ok := allowed[path]; !ok {
			t.Fatalf("workflow.go imports %q outside the deterministic allow-list; put I/O and uncertainty in an Activity", path)
		}
	}
}
