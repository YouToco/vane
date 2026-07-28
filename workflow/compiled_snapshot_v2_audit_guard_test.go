package workflow

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestCompiledSnapshotV2AuditRouterCoversEveryCompiledActivity(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve audit guard source")
	}
	path := filepath.Join(filepath.Dir(thisFile), "activities.go")
	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, path, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	functions := make(map[string]*ast.FuncDecl)
	for _, declaration := range file.Decls {
		if function, ok := declaration.(*ast.FuncDecl); ok {
			functions[function.Name.Name] = function
		}
	}
	for _, name := range []string{
		"EvolveProfile", "Fetch", "Dedup", "Score", "Select", "cardGen",
		"RecordEmptyBatch", "Push", "NotifyEmptyResult",
	} {
		if got := countCallsInFunction(
			functions[name], "loadAuthoritativeCompiledRun"); got != 1 {
			t.Errorf("%s loadAuthoritativeCompiledRun calls = %d, want 1",
				name, got)
		}
	}
	for _, name := range []string{"CardGen", "CardGenOutcomeV2"} {
		if got := countCallsInFunction(functions[name], "cardGen"); got != 1 {
			t.Errorf("%s cardGen router calls = %d, want 1", name, got)
		}
	}
	if got := countCallsInFunction(
		functions["PrepareRun"],
		"LoadAuthoritativeCompiledTaskRunSnapshot",
	); got != 1 {
		t.Errorf("PrepareRun authoritative Store loads = %d, want 1", got)
	}
	if got := countCallsInFunction(
		functions["PrepareRun"], "auditCompiledSnapshotV2"); got != 1 {
		t.Errorf("PrepareRun audit router calls = %d, want 1", got)
	}
	if got := countCallsInFunction(
		functions["loadAuthoritativeCompiledRun"],
		"LoadAuthoritativeCompiledTaskRunSnapshot",
	); got != 1 {
		t.Errorf("common consumer authoritative Store loads = %d, want 1", got)
	}
	if got := countCallsInFunction(
		functions["loadAuthoritativeCompiledRun"],
		"auditCompiledSnapshotV2",
	); got != 1 {
		t.Errorf("common consumer audit router calls = %d, want 1", got)
	}
	if got := countCallsInFunction(
		functions["auditCompiledSnapshotV2"], "AuditCompiledTaskRunSnapshotV2"); got != 1 {
		t.Errorf("audit router Store calls = %d, want 1", got)
	}

	workflowSource, err := os.ReadFile(filepath.Join(filepath.Dir(thisFile), "workflow.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range [][]byte{
		[]byte("AuditCompiledTaskRunSnapshotV2"),
		[]byte("snapshot_v2_read_audit"),
		[]byte("LoadCompiledTaskRunSnapshotV1"),
	} {
		if bytes.Contains(workflowSource, forbidden) {
			t.Errorf("workflow history path contains audit-only symbol %q", forbidden)
		}
	}
}

func countCallsInFunction(function *ast.FuncDecl, target string) int {
	if function == nil || function.Body == nil {
		return 0
	}
	count := 0
	ast.Inspect(function.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		switch callee := call.Fun.(type) {
		case *ast.Ident:
			if callee.Name == target {
				count++
			}
		case *ast.SelectorExpr:
			if callee.Sel.Name == target {
				count++
			}
		}
		return true
	})
	return count
}
