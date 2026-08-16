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
// composition boundary. Create must enter through the narrow native V3
// adapter, never through V1 create_schedule or the definition-edit controller.
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
		if !ok || !isPackageSelector(call.Fun, "agent", "BuildOwnerTools") ||
			len(call.Args) != 6 {
			return true
		}
		literal, ok := call.Args[2].(*ast.CompositeLit)
		if !ok {
			t.Fatalf("BuildOwnerTools manage argument=%T, want composite literal", call.Args[2])
		}
		literals = append(literals, literal)
		return true
	})
	if len(literals) != 1 {
		t.Fatalf("BuildOwnerTools production calls=%d, want 1", len(literals))
	}
	fields := make(map[string]bool)
	creatorWired := false
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
		if key.Name == "Edits" {
			t.Fatalf("A1 server wired retired manage_tasks dependency %s", key.Name)
		}
		if key.Name == "Creator" {
			call, ok := pair.Value.(*ast.CallExpr)
			if !ok || !isPackageSelector(call.Fun, "agent", "NewResearchTaskCreationV3Executor") ||
				len(call.Args) != 1 {
				t.Fatalf("Creator=%T, want native V3 adapter call", pair.Value)
			}
			controller, ok := call.Args[0].(*ast.Ident)
			creatorWired = ok && controller.Name == "creationCoordinator"
		}
	}
	for _, required := range []string{"Queries", "Creator", "Runner", "Deleter", "Authorizer"} {
		if !fields[required] {
			t.Errorf("ManageTasksDeps missing %s: %v", required, fields)
		}
	}
	if !creatorWired {
		t.Fatal("manage_tasks Creator is not bound to creationCoordinator through the native V3 adapter")
	}
}

func TestNativeV3CreationPolicyIsInjectedIntoProductionCoordinator(t *testing.T) {
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate manage_tasks wiring test")
	}
	file, err := parser.ParseFile(token.NewFileSet(),
		filepath.Join(filepath.Dir(testFile), "main.go"), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	wired := false
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok || !isPackageSelector(call.Fun, "task", "NewResearchCreationCoordinatorV3") {
			return true
		}
		if len(call.Args) != 4 {
			t.Fatalf("NewResearchCreationCoordinatorV3 args=%d, want trusted V3 policy", len(call.Args))
		}
		policy, ok := call.Args[3].(*ast.CallExpr)
		if !ok || len(policy.Args) != 0 {
			t.Fatalf("V3 creation policy=%T, want zero-arg trusted server policy", call.Args[3])
		}
		name, ok := policy.Fun.(*ast.Ident)
		wired = ok && name.Name == "nativeResearchV3CreationPolicy"
		return true
	})
	if !wired {
		t.Fatal("production CreationCoordinator lacks trusted native V3 creation policy")
	}
}
