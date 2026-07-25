package workflow

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestPushAuthorityGuard(t *testing.T) {
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve test file path")
	}
	path := filepath.Join(filepath.Dir(testFile), "activities.go")
	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, path, nil, 0)
	if err != nil {
		t.Fatalf("parse activities.go: %v", err)
	}

	var push *ast.FuncDecl
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if ok && function.Name.Name == "Push" && function.Recv != nil {
			push = function
			break
		}
	}
	if push == nil {
		t.Fatal("Push method not found")
	}

	claimCalls := 0
	effectCalls := []string{}
	ast.Inspect(push.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		switch {
		case selector.Sel.Name == "ClaimPushBatchDeliveryAuthority":
			claimCalls++
		case strings.Contains(selector.Sel.Name, "PushEffect"):
			effectCalls = append(effectCalls, selector.Sel.Name)
		}
		return true
	})
	if claimCalls != 1 {
		t.Fatalf("Push authority claims = %d, want exactly 1", claimCalls)
	}
	if len(effectCalls) != 0 {
		t.Fatalf("authority-only Push must not call effect APIs: %v", effectCalls)
	}
}
