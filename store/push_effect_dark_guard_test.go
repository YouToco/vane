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

func TestPushEffectStoreHasZeroProductionCallPoints(t *testing.T) {
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
		"BlockPushEffect":                    true,
	}
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
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
			strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, parseErr := parser.ParseFile(fset, path, nil, 0)
		if parseErr != nil {
			return parseErr
		}
		ast.Inspect(file, func(node ast.Node) bool {
			selector, ok := node.(*ast.SelectorExpr)
			if ok && methods[selector.Sel.Name] {
				position := fset.Position(selector.Pos())
				t.Errorf("push effect Store API is wired before its coordinator Gate: %s:%d",
					position.Filename, position.Line)
			}
			return true
		})
		return nil
	})
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
}
