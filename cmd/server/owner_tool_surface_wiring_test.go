package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"runtime"
	"testing"
)

// TestOwnerToolSurfaceIsACompositionInvariant prevents a deployment flag or
// catalog fallback from restoring the retired owner tools. The one remaining
// BuildTools consumer is named and isolated as the Dashboard compatibility
// loop; A2A starts from the public-only catalog.
func TestOwnerToolSurfaceIsACompositionInvariant(t *testing.T) {
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate owner tool surface wiring test")
	}
	file, err := parser.ParseFile(token.NewFileSet(),
		filepath.Join(filepath.Dir(testFile), "main.go"), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var ownerBuilders, compatibilityBuilders, publicBuilders, ownerLanes int
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		switch {
		case isPackageSelector(call.Fun, "agent", "BuildOwnerTools"):
			ownerBuilders++
		case isPackageSelector(call.Fun, "agent", "BuildTools"):
			compatibilityBuilders++
		case isPackageSelector(call.Fun, "agent", "BuildPublicResearchTools"):
			publicBuilders++
		case isPackageSelector(call.Fun, "agent", "NewChecked") && len(call.Args) == 1:
			literal, ok := call.Args[0].(*ast.CompositeLit)
			if !ok {
				return true
			}
			for _, element := range literal.Elts {
				pair, ok := element.(*ast.KeyValueExpr)
				if !ok {
					continue
				}
				key, keyOK := pair.Key.(*ast.Ident)
				value, valueOK := pair.Value.(*ast.Ident)
				if keyOK && valueOK && key.Name == "OwnerAgent" && value.Name == "true" {
					ownerLanes++
				}
			}
		}
		return true
	})
	if ownerBuilders != 1 || ownerLanes != 1 {
		t.Fatalf("owner builders/lanes=%d/%d, want one explicit invariant",
			ownerBuilders, ownerLanes)
	}
	if compatibilityBuilders != 1 {
		t.Fatalf("retained BuildTools consumers=%d, want one explicit Web compatibility loop",
			compatibilityBuilders)
	}
	if publicBuilders != 1 {
		t.Fatalf("A2A public catalog builders=%d, want one", publicBuilders)
	}
}
