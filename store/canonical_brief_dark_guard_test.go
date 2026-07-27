package store

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// P1-A is schema + Store only. Any production call site silently turns the
// migration into a rollout and must be introduced by a later versioned batch.
func TestCanonicalBriefP1AHasZeroProductionCallPoints(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate guard source")
	}
	root := filepath.Dir(filepath.Dir(thisFile))
	protected := map[string]bool{
		"CreatePendingRunOutcomeV1": true,
		"FinalizeRunOutcomeV1":      true,
		"FreezeBriefV1":             true,
		"LoadBriefV1":               true,
	}
	var calls []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if entry.Name() == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") ||
			strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if ok && protected[selector.Sel.Name] {
				relative, relErr := filepath.Rel(root, path)
				if relErr != nil {
					relative = path
				}
				calls = append(calls, relative+":"+selector.Sel.Name)
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(calls) != 0 {
		t.Fatalf("P1-A gained production call points: %v", calls)
	}
}
