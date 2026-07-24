package agent

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestDefinitionEditProposalCannotDowngradeToGenericPending(t *testing.T) {
	file := parseAgentLoopForDefinitionEditGuard(t)
	fn := findAgentFunction(file, "runToolCalls")
	branch := findStringEqualityBranch(fn, "edit_task_definition")
	if branch == nil {
		t.Fatal("runToolCalls is missing the edit_task_definition branch")
	}
	if got := countSelectorCalls(branch.Body, "Propose"); got != 1 {
		t.Fatalf("definition edit proposal Propose calls = %d, want 1", got)
	}
	for _, forbidden := range []string{
		"CreatePendingAction", "PrepareAndSealProposal",
		"UpdatePush", "UpsertSchedulePlaybook", "SetScheduleStrictness",
	} {
		if got := countSelectorCalls(branch.Body, forbidden); got != 0 {
			t.Fatalf("definition edit proposal branch reaches %s", forbidden)
		}
	}
}

func TestDefinitionEditConfirmOnlyNarrowNotFoundMayFallThrough(t *testing.T) {
	assertDefinitionEditCallbackGuard(t, "ExecuteAction", "Confirm")
}

func TestDefinitionEditCancelOnlyNarrowNotFoundMayFallThrough(t *testing.T) {
	assertDefinitionEditCallbackGuard(t, "CancelAction", "Cancel")
}

func assertDefinitionEditCallbackGuard(
	t *testing.T,
	functionName string,
	method string,
) {
	t.Helper()
	file := parseAgentLoopForDefinitionEditGuard(t)
	fn := findAgentFunction(file, functionName)
	if fn == nil || fn.Body == nil || len(fn.Body.List) == 0 {
		t.Fatalf("%s is missing", functionName)
	}
	creationIndex, editIndex := -1, -1
	var editBranch *ast.IfStmt
	for index, statement := range fn.Body.List {
		branch, ok := statement.(*ast.IfStmt)
		if !ok {
			continue
		}
		switch {
		case expressionContains(branch.Cond, "taskCreation"):
			creationIndex = index
		case expressionContains(branch.Cond, "taskDefinitionEdit"):
			editIndex = index
			editBranch = branch
		}
	}
	if creationIndex < 0 || editIndex <= creationIndex || editBranch == nil {
		t.Fatalf("%s must route creation → definition edit → v0 in order",
			functionName)
	}
	if got := countSelectorCalls(editBranch.Body, method); got != 1 {
		t.Fatalf("%s definition edit %s calls = %d, want 1",
			functionName, method, got)
	}
	if got := countIdentifiers(editBranch.Body, "ErrDefinitionEditOperationNotFound"); got != 1 {
		t.Fatalf("%s narrow not-found checks = %d, want 1", functionName, got)
	}
	for _, forbidden := range []string{
		"ClaimPendingAction", "CancelPendingAction",
		"CreatePendingAction", "PrepareAndSealProposal",
	} {
		if got := countSelectorCalls(editBranch.Body, forbidden); got != 0 {
			t.Fatalf("%s definition edit branch reaches %s", functionName, forbidden)
		}
	}
}

func parseAgentLoopForDefinitionEditGuard(t *testing.T) *ast.File {
	t.Helper()
	_, current, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate definition edit guard")
	}
	path := filepath.Join(filepath.Dir(current), "loop.go")
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	return file
}

func findAgentFunction(file *ast.File, name string) *ast.FuncDecl {
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if ok && fn.Name.Name == name {
			return fn
		}
	}
	return nil
}

func findStringEqualityBranch(
	fn *ast.FuncDecl,
	value string,
) *ast.IfStmt {
	var found *ast.IfStmt
	if fn == nil || fn.Body == nil {
		return nil
	}
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		if found != nil {
			return false
		}
		branch, ok := node.(*ast.IfStmt)
		if !ok || !expressionContains(branch.Cond, value) {
			return true
		}
		found = branch
		return false
	})
	return found
}

func countSelectorCalls(node ast.Node, name string) int {
	count := 0
	ast.Inspect(node, func(candidate ast.Node) bool {
		call, ok := candidate.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if ok && selector.Sel.Name == name {
			count++
		}
		return true
	})
	return count
}

func countIdentifiers(node ast.Node, name string) int {
	count := 0
	ast.Inspect(node, func(candidate ast.Node) bool {
		identifier, ok := candidate.(*ast.Ident)
		if ok && identifier.Name == name {
			count++
		}
		return true
	})
	return count
}

func expressionContains(node ast.Node, fragment string) bool {
	found := false
	ast.Inspect(node, func(candidate ast.Node) bool {
		switch value := candidate.(type) {
		case *ast.Ident:
			found = found || strings.Contains(value.Name, fragment)
		case *ast.BasicLit:
			found = found || strings.Contains(value.Value, fragment)
		}
		return !found
	})
	return found
}
