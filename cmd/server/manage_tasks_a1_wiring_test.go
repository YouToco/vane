package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"runtime"
	"testing"
)

// TestAgentFirstManageTasksDoesNotReuseLegacyEditController pins the A1
// composition boundary. Native V3 create is deliberately unwired in this
// checkpoint; the server must not fill that gap with the V1 create or edit
// coordinators, and edit must not re-enter manage_tasks as a hidden dependency.
func TestAgentFirstManageTasksDoesNotReuseLegacyEditController(t *testing.T) {
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate manage_tasks wiring test")
	}
	file, err := parser.ParseFile(token.NewFileSet(),
		filepath.Join(filepath.Dir(testFile), "main.go"), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var literals []*ast.CompositeLit
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok || !isPackageSelector(call.Fun, "agent", "NewManageTasksTool") ||
			len(call.Args) != 1 {
			return true
		}
		literal, ok := call.Args[0].(*ast.CompositeLit)
		if !ok {
			t.Fatalf("NewManageTasksTool argument=%T, want composite literal", call.Args[0])
		}
		literals = append(literals, literal)
		return true
	})
	if len(literals) != 1 {
		t.Fatalf("NewManageTasksTool production calls=%d, want 1", len(literals))
	}
	fields := make(map[string]bool)
	for _, element := range literals[0].Elts {
		pair, ok := element.(*ast.KeyValueExpr)
		if !ok {
			t.Fatalf("unkeyed ManageTasksDeps field %T", element)
		}
		key, ok := pair.Key.(*ast.Ident)
		if !ok {
			t.Fatalf("ManageTasksDeps key=%T", pair.Key)
		}
		fields[key.Name] = true
		if key.Name == "Creator" || key.Name == "Edits" {
			t.Fatalf("A1 server wired forbidden/unready manage_tasks dependency %s", key.Name)
		}
	}
	for _, required := range []string{"Queries", "Runner", "Deleter", "Authorizer"} {
		if !fields[required] {
			t.Errorf("ManageTasksDeps missing %s: %v", required, fields)
		}
	}
}
