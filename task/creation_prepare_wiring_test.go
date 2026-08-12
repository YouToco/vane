package task

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestTaskCreationSaga_LowLevelLifecycleIsControllerPrivate replaces A4's
// zero-call-point parking guard. A5 intentionally has one production caller,
// but Agent/API/startup may depend only on CreationCoordinator; no package may
// stitch lease, checkpoint, Temporal, or aggregate steps into a second saga.
func TestTaskCreationSaga_LowLevelLifecycleIsControllerPrivate(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller could not locate the A5 wiring guard")
	}
	taskDir := filepath.Dir(thisFile)
	repoRoot := filepath.Dir(taskDir)
	allowedProviders := map[string]struct{}{
		filepath.Clean(filepath.Join(taskDir, "creation_prepare.go")):                    {},
		filepath.Clean(filepath.Join(taskDir, "creation_saga.go")):                       {},
		filepath.Clean(filepath.Join(repoRoot, "store", "task_creation_operations.go")):  {},
		filepath.Clean(filepath.Join(repoRoot, "store", "task_creation_commits.go")):     {},
		filepath.Clean(filepath.Join(repoRoot, "store", "compiled_task_definitions.go")): {},
		filepath.Clean(filepath.Join(repoRoot, "store", "schedules.go")):                 {},
		filepath.Clean(filepath.Join(repoRoot, "types", "task_creation.go")):             {},
	}
	watched := map[string]struct{}{
		"CreateTaskCreationOperation":                   {},
		"AcquireTaskCreationOperation":                  {},
		"RenewTaskCreationLease":                        {},
		"SealTaskCreationCommand":                       {},
		"BeginTaskCreationTranslation":                  {},
		"CheckpointTaskCreationDefinition":              {},
		"CheckpointTaskCreationSchedule":                {},
		"CheckpointTaskCreationEnsureReceipt":           {},
		"BlockTaskCreationOperation":                    {},
		"FailTaskCreationOperation":                     {},
		"CompleteTaskCreationOperation":                 {},
		"CommitPausedCompiledTaskDefinitionForCreation": {},
		"BeginTaskCreationActivation":                   {},
		"CommitTaskCreationActivation":                  {},
		"BeginTaskCreationCleanup":                      {},
		"FinishTaskCreationCleanup":                     {},
		"BlockTaskCreationOperationAfterSideEffect":     {},
		"ListStaleTaskCreationOperations":               {},
	}

	fset := token.NewFileSet()
	var references []string
	var controllerWiring []string
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
			if identifier.Name == "NewCreationCoordinator" {
				position := fset.Position(identifier.Pos())
				controllerWiring = append(controllerWiring,
					fmt.Sprintf("%s:%d", position.Filename, position.Line))
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("scan A5 production call points: %v", err)
	}
	if len(references) != 0 {
		t.Fatalf("A5 low-level creation lifecycle escaped CreationCoordinator: %v", references)
	}
	mainFile := filepath.Clean(filepath.Join(repoRoot, "cmd", "server", "main.go"))
	if len(controllerWiring) != 1 || !strings.HasPrefix(
		filepath.Clean(controllerWiring[0]), mainFile+":",
	) {
		t.Fatalf("A5 must have exactly one production coordinator wiring in cmd/server/main.go: %v",
			controllerWiring)
	}
}
