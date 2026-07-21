package task

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
)

// TestTaskCreationSaga_HasZeroProductionCallPoints keeps A4 parked safely.
// The providers and protocol types may exist, but no API, agent, worker, or
// startup path may construct or call them until A5 supplies the cross-system
// recovery loop. Identifier references count too, so method values and DI
// wiring cannot bypass the guard merely by avoiding a direct CallExpr.
func TestTaskCreationSaga_HasZeroProductionCallPoints(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller could not locate the A4 wiring guard")
	}
	taskDir := filepath.Dir(thisFile)
	repoRoot := filepath.Dir(taskDir)
	allowedProviders := map[string]struct{}{
		filepath.Clean(filepath.Join(taskDir, "creation_prepare.go")):                   {},
		filepath.Clean(filepath.Join(repoRoot, "store", "task_creation_operations.go")): {},
		filepath.Clean(filepath.Join(repoRoot, "types", "task_creation.go")):            {},
	}
	watched := map[string]struct{}{
		"TaskCreationExecutionVersionV1":      {},
		"CreationPreparer":                    {},
		"NewCreationPreparer":                 {},
		"AcquireTaskCreationOperation":        {},
		"RenewTaskCreationLease":              {},
		"SealTaskCreationCommand":             {},
		"BeginTaskCreationTranslation":        {},
		"CheckpointTaskCreationDefinition":    {},
		"CheckpointTaskCreationSchedule":      {},
		"CheckpointTaskCreationEnsureReceipt": {},
		"BlockTaskCreationOperation":          {},
		"FailTaskCreationOperation":           {},
		"CompleteTaskCreationOperation":       {},
		"ListStaleTaskCreationOperations":     {},
	}

	fset := token.NewFileSet()
	var references []string
	v1SQL := regexp.MustCompile(`(?i)execution_version\s*=\s*1`)
	err := filepath.WalkDir(repoRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if entry.Name() == ".git" || entry.Name() == "vendor" {
				return filepath.SkipDir
			}
			return nil
		}
		cleanPath := filepath.Clean(path)
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		source, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		if v1SQL.Match(source) {
			references = append(references, path+":literal execution_version=1")
		}
		file, parseErr := parser.ParseFile(fset, path, source, 0)
		if parseErr != nil {
			return parseErr
		}
		if _, allowed := allowedProviders[cleanPath]; allowed {
			for _, declaration := range file.Decls {
				function, isFunction := declaration.(*ast.FuncDecl)
				if isFunction && function.Recv == nil && function.Name.Name == "init" {
					position := fset.Position(function.Pos())
					references = append(references,
						fmt.Sprintf("%s:%d:init", position.Filename, position.Line))
				}
			}
			return nil
		}
		ast.Inspect(file, func(node ast.Node) bool {
			identifier, isIdentifier := node.(*ast.Ident)
			if !isIdentifier {
				return true
			}
			if _, forbidden := watched[identifier.Name]; forbidden {
				position := fset.Position(identifier.Pos())
				references = append(references,
					fmt.Sprintf("%s:%d:%s", position.Filename, position.Line, identifier.Name))
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("scan A4 production call points: %v", err)
	}
	if len(references) != 0 {
		t.Fatalf("A4 must retain zero production call points until A5; found %v", references)
	}
}
