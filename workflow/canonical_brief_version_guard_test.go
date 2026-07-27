package workflow

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
)

// The result wire-version marker must be recorded before Prepare is scheduled.
// Otherwise a pre-v2 execution whose history frontier is the completed
// Prepare Activity is no longer replaying when it reaches GetVersion and will
// decode the old result as v2.
func TestCanonicalBriefPrepareResultVersionPrecedesActivityCommand(
	t *testing.T,
) {
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve test file path")
	}
	path := filepath.Join(filepath.Dir(testFile), "workflow.go")
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatalf("parse workflow.go: %v", err)
	}
	var workflowBody *ast.BlockStmt
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if ok && function.Name.Name == "PushPipelineWorkflow" {
			workflowBody = function.Body
			break
		}
	}
	if workflowBody == nil {
		t.Fatal("PushPipelineWorkflow not found")
	}
	var versionPosition, preparePosition token.Pos
	ast.Inspect(workflowBody, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		switch selector.Sel.Name {
		case "GetVersion":
			for _, argument := range call.Args {
				literal, ok := argument.(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					continue
				}
				value, err := strconv.Unquote(literal.Value)
				if err == nil &&
					value == "canonical-brief-prepare-result-v2" {
					versionPosition = call.Pos()
				}
			}
		case "ExecuteActivity":
			for _, argument := range call.Args {
				field, ok := argument.(*ast.SelectorExpr)
				if ok && field.Sel.Name == "PrepareCanonicalBriefV1" {
					preparePosition = call.Pos()
				}
			}
		}
		return true
	})
	if versionPosition == token.NoPos || preparePosition == token.NoPos {
		t.Fatalf(
			"version/Prepare command positions = %v/%v",
			versionPosition, preparePosition,
		)
	}
	if versionPosition >= preparePosition {
		t.Fatal("Prepare result version marker follows Activity command")
	}
}
