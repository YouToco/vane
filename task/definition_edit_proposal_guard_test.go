package task

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
)

func TestDefinitionEditProposalCodecRemainsDark(t *testing.T) {
	t.Parallel()

	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate definition edit proposal guard")
	}
	taskDir := filepath.Clean(filepath.Dir(testFile))
	repoRoot := filepath.Clean(filepath.Dir(taskDir))
	provider := filepath.Join(taskDir, "definition_edit_proposal.go")
	fset := token.NewFileSet()
	var violations []string
	err := filepath.WalkDir(repoRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			base := entry.Name()
			if filepath.Clean(path) != repoRoot &&
				(base == "vendor" || base == "third_party" || base == "testdata" ||
					strings.HasPrefix(base, ".") || strings.HasPrefix(base, "_")) {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		var allowed map[token.Pos]struct{}
		if filepath.Clean(path) == provider {
			allowed, err = definitionEditProviderAllowedReferences(file)
			if err != nil {
				return err
			}
		}
		violations = append(violations, definitionEditDarkReferences(fset, file, allowed)...)
		return nil
	})
	if err != nil {
		t.Fatalf("scan production definition edit calls: %v", err)
	}
	slices.Sort(violations)
	if len(violations) != 0 {
		t.Fatalf("C2b3-2a proposal codec must remain dark:\n%s", strings.Join(violations, "\n"))
	}
}

func TestDefinitionEditProposalDarkGuardCatchesFunctionValues(t *testing.T) {
	t.Parallel()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "function_value_probe.go", `package probe
import taskpkg "github.com/YouToco/vane/task"
var build = taskpkg.BuildFrozenTaskDefinitionEditProposal
func probe() { decode := taskpkg.DecodeFrozenTaskDefinitionEditProposal; _ = decode }
`, 0)
	if err != nil {
		t.Fatal(err)
	}
	violations := definitionEditDarkReferences(fset, file, nil)
	if len(violations) != 2 {
		t.Fatalf("function-value references escaped dark guard: %v", violations)
	}
}

func definitionEditProviderAllowedReferences(file *ast.File) (map[token.Pos]struct{}, error) {
	functions := make(map[string]*ast.FuncDecl, 2)
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Recv != nil {
			continue
		}
		if function.Name.Name == "BuildFrozenTaskDefinitionEditProposal" ||
			function.Name.Name == "DecodeFrozenTaskDefinitionEditProposal" {
			if functions[function.Name.Name] != nil {
				return nil, fmt.Errorf("duplicate proposal codec declaration %s", function.Name.Name)
			}
			functions[function.Name.Name] = function
		}
	}
	build := functions["BuildFrozenTaskDefinitionEditProposal"]
	decode := functions["DecodeFrozenTaskDefinitionEditProposal"]
	if build == nil || decode == nil {
		return nil, fmt.Errorf("proposal codec declarations are incomplete")
	}
	allowed := map[token.Pos]struct{}{build.Name.Pos(): {}, decode.Name.Pos(): {}}
	decodeCalls := 0
	writeGateCalls := 0
	ast.Inspect(build.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		identifier, ok := call.Fun.(*ast.Ident)
		if ok && identifier.Name == "DecodeFrozenTaskDefinitionEditProposal" {
			decodeCalls++
			allowed[identifier.Pos()] = struct{}{}
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if ok && selector.Sel.Name == "ValidatePreparedTaskDefinitionEditRequestForWrite" {
			writeGateCalls++
		}
		return true
	})
	if decodeCalls != 1 {
		return nil, fmt.Errorf(
			"BuildFrozenTaskDefinitionEditProposal must directly call Decode exactly once, got %d",
			decodeCalls,
		)
	}
	if writeGateCalls != 1 {
		return nil, fmt.Errorf(
			"BuildFrozenTaskDefinitionEditProposal must call the current-writer gate exactly once, got %d",
			writeGateCalls,
		)
	}
	return allowed, nil
}

func definitionEditDarkReferences(
	fset *token.FileSet,
	file *ast.File,
	allowed map[token.Pos]struct{},
) []string {
	guarded := map[string]struct{}{
		"BuildFrozenTaskDefinitionEditProposal":  {},
		"DecodeFrozenTaskDefinitionEditProposal": {},
	}
	var violations []string
	ast.Inspect(file, func(node ast.Node) bool {
		identifier, ok := node.(*ast.Ident)
		if !ok {
			return true
		}
		if _, watched := guarded[identifier.Name]; watched {
			if _, ok := allowed[identifier.Pos()]; ok {
				return true
			}
			violations = append(violations, fset.Position(identifier.Pos()).String()+
				": production reference "+identifier.Name)
		}
		return true
	})
	return violations
}
