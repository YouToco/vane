package store

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPushEffectStoreCallPointsStayInFencedCoordinator(t *testing.T) {
	methods := map[string]bool{
		"CreatePushEffect":                   true,
		"LoadPushEffect":                     true,
		"ListRecoverablePushEffectTenantIDs": true,
		"ListRecoverablePushEffects":         true,
		"ClaimPushEffect":                    true,
		"ClaimPushEffectReconciliation":      true,
		"TakeOverStalePushEffect":            true,
		"RecordPushEffectDefiniteFailure":    true,
		"RecordPushEffectAmbiguous":          true,
		"RecordPushEffectSent":               true,
		"RecordPushEffectSentWithDeliveries": true,
		"BlockPushEffect":                    true,
	}
	expected := map[string]int{
		"CreatePushEffect":                   1,
		"ClaimPushEffect":                    1,
		"ClaimPushEffectReconciliation":      1,
		"RecordPushEffectDefiniteFailure":    1,
		"RecordPushEffectAmbiguous":          1,
		"RecordPushEffectSentWithDeliveries": 2,
	}
	found := make(map[string]int)
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	allowedFile := filepath.Join(root, "workflow", "activities.go")
	fset := token.NewFileSet()
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			name := entry.Name()
			if name == ".git" || name == "vendor" || name == "third_party" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") ||
			strings.HasSuffix(path, "_test.go") ||
			filepath.Clean(path) == filepath.Join(root, "store", "push_effects.go") {
			return nil
		}
		file, parseErr := parser.ParseFile(fset, path, nil, 0)
		if parseErr != nil {
			return parseErr
		}
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			ast.Inspect(function.Body, func(node ast.Node) bool {
				selector, ok := node.(*ast.SelectorExpr)
				if !ok || !methods[selector.Sel.Name] {
					return true
				}
				position := fset.Position(selector.Pos())
				if filepath.Clean(path) != allowedFile ||
					function.Name.Name != "sendDurablePushChunk" ||
					!isPushEffectCoordinatorReceiver(selector.X) {
					t.Errorf(
						"push effect Store API escaped fenced coordinator: %s:%d (%s)",
						position.Filename, position.Line, selector.Sel.Name,
					)
					return true
				}
				found[selector.Sel.Name]++
				return true
			})
		}
		return nil
	})
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	for method, want := range expected {
		if got := found[method]; got != want {
			t.Errorf("fenced call count %s=%d, want %d", method, got, want)
		}
	}
	for method, got := range found {
		if _, ok := expected[method]; !ok && got != 0 {
			t.Errorf("unexpected fenced call %s=%d", method, got)
		}
	}
}

func isPushEffectCoordinatorReceiver(expression ast.Expr) bool {
	selector, ok := expression.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != "pushEffectStore" {
		return false
	}
	identifier, ok := selector.X.(*ast.Ident)
	return ok && identifier.Name == "a"
}
