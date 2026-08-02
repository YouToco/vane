package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

func TestGateUsesNonOwnerServerRuntimeStore(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "main.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var runtimeCalls, ownerEraCalls int
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		pkg, ok := selector.X.(*ast.Ident)
		if !ok || pkg.Name != "store" {
			return true
		}
		switch selector.Sel.Name {
		case "NewServerRuntime":
			runtimeCalls++
		case "New":
			ownerEraCalls++
		}
		return true
	})
	if runtimeCalls != 1 || ownerEraCalls != 0 {
		t.Fatalf("gate Store constructors: NewServerRuntime=%d New=%d",
			runtimeCalls, ownerEraCalls)
	}
}
