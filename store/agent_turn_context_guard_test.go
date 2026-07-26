package store

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

func TestAgentTurnContextSnapshotProductionWiringIsNarrow(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	root := filepath.Dir(filepath.Dir(thisFile))
	fset := token.NewFileSet()
	var calls []string
	var inserts []string
	err := filepath.WalkDir(root, func(
		path string,
		entry os.DirEntry,
		walkErr error,
	) error {
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
		file, parseErr := parser.ParseFile(fset, path, nil, 0)
		if parseErr != nil {
			return parseErr
		}
		ast.Inspect(file, func(node ast.Node) bool {
			switch value := node.(type) {
			case *ast.CallExpr:
				selector, selectorOK := value.Fun.(*ast.SelectorExpr)
				if selectorOK &&
					selector.Sel.Name == "SealAgentTurnContextSnapshot" {
					calls = append(calls, filepath.Clean(path))
				}
			case *ast.BasicLit:
				if value.Kind != token.STRING {
					break
				}
				literal, unquoteErr := strconv.Unquote(value.Value)
				if unquoteErr == nil &&
					strings.Contains(
						literal,
						"INSERT INTO public.agent_turn_context_snapshots",
					) {
					inserts = append(inserts, filepath.Clean(path))
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	wantCall := filepath.Join(root, "agent", "context_shadow.go")
	if len(calls) != 2 {
		t.Fatalf("snapshot seal production calls=%v, want two shadow calls", calls)
	}
	for _, path := range calls {
		if path != wantCall {
			t.Fatalf("snapshot seal escaped shadow adapter: %s", path)
		}
	}
	wantInsert := filepath.Join(
		root, "store", "agent_turn_context_snapshots.go",
	)
	if len(inserts) != 1 || inserts[0] != wantInsert {
		t.Fatalf("snapshot raw inserts=%v, want only %s", inserts, wantInsert)
	}
}
