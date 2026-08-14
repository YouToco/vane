package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

func TestLegacyAdmissionFenceOwnsEveryRetainedCoordinator(t *testing.T) {
	source, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "main.go", source, 0)
	if err != nil {
		t.Fatal(err)
	}
	type callRecord struct {
		name string
		pos  token.Pos
		args []ast.Expr
	}
	var calls []callRecord
	compositeFence := false
	ast.Inspect(file, func(node ast.Node) bool {
		switch typed := node.(type) {
		case *ast.CallExpr:
			selector, ok := typed.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			pkg, ok := selector.X.(*ast.Ident)
			if !ok {
				return true
			}
			calls = append(calls, callRecord{
				name: pkg.Name + "." + selector.Sel.Name,
				pos:  typed.Pos(), args: typed.Args,
			})
		case *ast.CompositeLit:
			selector, ok := typed.Type.(*ast.SelectorExpr)
			if ok && selector.Sel.Name == "LegacyAdmissionFencedStore" {
				compositeFence = true
			}
		}
		return true
	})
	if compositeFence {
		t.Fatal("legacy admission fence bypasses its fail-closed constructor")
	}
	find := func(name string) []callRecord {
		var found []callRecord
		for _, call := range calls {
			if call.name == name {
				found = append(found, call)
			}
		}
		return found
	}
	fences := find("store.NewLegacyAdmissionFencedStore")
	if len(fences) != 1 || len(fences[0].args) != 1 ||
		!isIdentNamed(fences[0].args[0], "st") {
		t.Fatalf("legacy fence constructors=%+v, want one over primary st", fences)
	}
	for _, target := range []string{
		"task.NewResearchCreationCoordinatorV3",
	} {
		found := find(target)
		if len(found) != 1 || len(found[0].args) == 0 ||
			!isIdentNamed(found[0].args[0], "legacyStore") {
			t.Fatalf("%s must receive the sole legacyStore: %+v", target, found)
		}
		if found[0].pos <= fences[0].pos {
			t.Fatalf("%s is constructed before the admission fence", target)
		}
	}
	if old := find("task.NewCreationCoordinator"); len(old) != 0 {
		t.Fatalf("production server constructs retained V1 creation coordinator: %+v", old)
	}
	if old := find("task.NewTaskDefinitionEditCoordinator"); len(old) != 0 {
		t.Fatalf("production server constructs retained V1 edit coordinator: %+v", old)
	}
	v3Edits := find("task.NewResearchTaskDefinitionEditCoordinatorV3")
	if len(v3Edits) != 1 || len(v3Edits[0].args) == 0 ||
		!isIdentNamed(v3Edits[0].args[0], "researchControlStore") {
		t.Fatalf("native V3 edit coordinator lost restricted control Store: %+v", v3Edits)
	}

	text := string(source)
	start := strings.Index(text, "legacyStore, err := store.NewLegacyAdmissionFencedStore(st)")
	end := strings.Index(text, "gatewayClient, err :=")
	if start < 0 || end <= start {
		t.Fatal("cannot locate fail-closed legacy fence startup block")
	}
	block := text[start:end]
	for _, required := range []string{"if err != nil", "closeStores()", "return fmt.Errorf"} {
		if !strings.Contains(block, required) {
			t.Fatalf("legacy fence startup failure is not fail-closed: missing %q", required)
		}
	}
}

func isIdentNamed(expr ast.Expr, want string) bool {
	ident, ok := expr.(*ast.Ident)
	return ok && ident.Name == want
}
