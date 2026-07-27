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
		for _, name := range canonicalBriefProtectedSelectors(file, protected) {
			relative, relErr := filepath.Rel(root, path)
			if relErr != nil {
				relative = path
			}
			calls = append(calls, relative+":"+name)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(calls) != 0 {
		t.Fatalf("P1-A gained production call points: %v", calls)
	}
}

func TestCanonicalBriefP1AGuardDetectsMethodValueAliases(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "alias.go", `package p
var freezeDark = (*Store).FreezeBriefV1
`, 0)
	if err != nil {
		t.Fatal(err)
	}
	found := canonicalBriefProtectedSelectors(
		file, map[string]bool{"FreezeBriefV1": true})
	if len(found) != 1 || found[0] != "FreezeBriefV1" {
		t.Fatalf("method-value alias escaped guard: %v", found)
	}
}

func canonicalBriefProtectedSelectors(
	file *ast.File, protected map[string]bool,
) []string {
	var found []string
	// Inspect every selector reference, not only direct CallExpr functions:
	// method values and method expressions can otherwise alias a dark method.
	ast.Inspect(file, func(node ast.Node) bool {
		selector, ok := node.(*ast.SelectorExpr)
		if ok && protected[selector.Sel.Name] {
			found = append(found, selector.Sel.Name)
		}
		return true
	})
	return found
}
