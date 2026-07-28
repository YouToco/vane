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

func TestObservedEventProvenanceV1HasZeroProductionCallers(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve observed-event provenance guard")
	}
	root := filepath.Dir(filepath.Dir(thisFile))
	var callers []string
	err := filepath.WalkDir(root,
		func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() {
				if entry.Name() == ".git" || entry.Name() == "vendor" {
					return filepath.SkipDir
				}
				return nil
			}
			if filepath.Ext(path) != ".go" ||
				strings.HasSuffix(path, "_test.go") {
				return nil
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			parsed, err := parser.ParseFile(
				token.NewFileSet(), path, raw, 0)
			if err != nil {
				return err
			}
			relative, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			ast.Inspect(parsed, func(node ast.Node) bool {
				selector, ok := node.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				if selector.Sel.Name ==
					"ReserveObservedEventProvenanceV1" {
					callers = append(
						callers, filepath.ToSlash(relative))
				}
				return true
			})
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if len(callers) != 0 {
		t.Fatalf(
			"observed-event provenance production callers=%v want none",
			callers)
	}
}
