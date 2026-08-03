package scheduler

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"testing"
)

func TestResearchV3StaticAuthorityIsControlPlaneOnly(t *testing.T) {
	for _, name := range []string{"reconcileOne", "applyScheduleCommandRemote", "UpdatePush"} {
		if got := selectorCountInMethod(t, "scheduler.go", name, "authorityID"); got != 0 {
			t.Fatalf("runtime method %s still reads static authorityID %d times", name, got)
		}
	}
	for _, name := range []string{"CutoverResearchV3", "RollbackResearchV3"} {
		if got := selectorCountInMethod(t, "research_v3_cutover.go", name, "authorityID"); got == 0 {
			t.Fatalf("control-plane method %s lost exact authorityID guard", name)
		}
	}
	if got := identifierCountInMethod(
		t, "research_v3_rollout.go", "TriggerResearchShadowNow", "shadowID",
	); got == 0 {
		if helper := identifierCountInMethod(
			t, "research_v3_rollout.go", "TriggerResearchShadowNow", "shadowIDMatch",
		); helper == 0 {
			t.Fatal("shadow trigger lost exact shadowID guard")
		}
	}
}

func identifierCountInMethod(t *testing.T, file, method, identifier string) int {
	t.Helper()
	fset := token.NewFileSet()
	parsed, err := parser.ParseFile(fset, filepath.Clean(file), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, declaration := range parsed.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if !ok || fn.Recv == nil || fn.Name.Name != method {
			continue
		}
		count := 0
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			if ident, ok := node.(*ast.Ident); ok && ident.Name == identifier {
				count++
			}
			return true
		})
		return count
	}
	t.Fatalf("method %s not found in %s", method, file)
	return 0
}

func selectorCountInMethod(t *testing.T, file, method, selector string) int {
	t.Helper()
	fset := token.NewFileSet()
	parsed, err := parser.ParseFile(fset, filepath.Clean(file), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, declaration := range parsed.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if !ok || fn.Recv == nil || fn.Name.Name != method {
			continue
		}
		count := 0
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			if selected, ok := node.(*ast.SelectorExpr); ok && selected.Sel.Name == selector {
				count++
			}
			return true
		})
		return count
	}
	t.Fatalf("method %s not found in %s", method, file)
	return 0
}
