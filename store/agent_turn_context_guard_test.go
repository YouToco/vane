package store

import (
	"fmt"
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
	wantAdapter := filepath.Join(root, "agent", "context_shadow.go")
	wantStore := filepath.Join(
		root, "store", "agent_turn_context_snapshots.go",
	)
	fset := token.NewFileSet()
	var violations []string
	var sealSelectors int
	var rawInsertFunctions int
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
		fileViolations, selectors, inserts :=
			agentTurnContextProductionViolations(
				fset, file, filepath.Clean(path),
				wantAdapter, wantStore,
			)
		violations = append(violations, fileViolations...)
		sealSelectors += selectors
		rawInsertFunctions += inserts
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if sealSelectors != 1 || rawInsertFunctions != 1 {
		violations = append(violations, fmt.Sprintf(
			"exact snapshot selectors/inserts=%d/%d, want 1/1",
			sealSelectors, rawInsertFunctions,
		))
	}
	if len(violations) != 0 {
		t.Fatalf(
			"7.8-A snapshot boundary escaped exact adapter/Store path:\n%s",
			strings.Join(violations, "\n"),
		)
	}
}

func TestAgentTurnContextSnapshotGuardRejectsMutations(t *testing.T) {
	mutations := map[string]string{
		"method value": `package agent
func escape(sealer interface{ SealAgentTurnContextSnapshot() }) {
	call := sealer.SealAgentTurnContextSnapshot
	_ = call
}`,
		"wrapper": `package agent
func hidden(sealer interface{ SealAgentTurnContextSnapshot() }) {
	sealer.SealAgentTurnContextSnapshot()
}`,
		"dynamic raw insert": `package escape
func write(tx interface{ Exec(string) }) {
	query := "INSERT INTO public." + "agent_turn_context_snapshots"
	tx.Exec(query)
}`,
		"direct raw insert": `package escape
func write(tx interface{ Exec(string) }) {
	tx.Exec("INSERT INTO public.agent_turn_context_snapshots DEFAULT VALUES")
}`,
		"reflect method name": `package escape
import "reflect"
func write(value any) {
	reflect.ValueOf(value).MethodByName("SealAgentTurnContextSnapshot").Call(nil)
}`,
	}
	for name, source := range mutations {
		t.Run(name, func(t *testing.T) {
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, "escape.go", source, 0)
			if err != nil {
				t.Fatal(err)
			}
			violations, _, _ := agentTurnContextProductionViolations(
				fset, file, "escape.go", "context_shadow.go",
				"agent_turn_context_snapshots.go",
			)
			if len(violations) == 0 {
				t.Fatal("mutated snapshot boundary unexpectedly passed")
			}
		})
	}
}

func agentTurnContextProductionViolations(
	fset *token.FileSet,
	file *ast.File,
	path string,
	wantAdapter string,
	wantStore string,
) ([]string, int, int) {
	var violations []string
	var sealSelectors int
	var rawInsertFunctions int
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok {
			continue
		}
		var selectors, directCalls int
		var literals strings.Builder
		var fullInsertLiterals, dynamicSealNames int
		ast.Inspect(function, func(node ast.Node) bool {
			switch value := node.(type) {
			case *ast.SelectorExpr:
				if value.Sel.Name == "SealAgentTurnContextSnapshot" {
					selectors++
				}
			case *ast.CallExpr:
				selector, ok := value.Fun.(*ast.SelectorExpr)
				if ok && selector.Sel.Name ==
					"SealAgentTurnContextSnapshot" {
					directCalls++
				}
			case *ast.BasicLit:
				if value.Kind != token.STRING {
					return true
				}
				literal, err := strconv.Unquote(value.Value)
				if err != nil {
					return true
				}
				literals.WriteString(strings.ToLower(literal))
				if literal == "SealAgentTurnContextSnapshot" {
					dynamicSealNames++
				}
				if strings.Contains(
					strings.ToLower(literal),
					"insert into public.agent_turn_context_snapshots",
				) {
					fullInsertLiterals++
				}
			}
			return true
		})
		if dynamicSealNames > 0 {
			violations = append(violations, fmt.Sprintf(
				"%s:%s has forbidden dynamic snapshot method reference",
				path, function.Name.Name,
			))
		}
		if selectors > 0 {
			sealSelectors += selectors
			if path != wantAdapter ||
				function.Name.Name !=
					"sealPreparedAgentContextShadow" ||
				selectors != 1 || directCalls != 1 {
				violations = append(violations, fmt.Sprintf(
					"%s:%s has snapshot selector/direct calls=%d/%d",
					path, function.Name.Name, selectors, directCalls,
				))
			}
		}
		combined := strings.Join(strings.Fields(literals.String()), " ")
		if strings.Contains(combined, "insert") &&
			strings.Contains(combined, "agent_turn_context_snapshots") {
			rawInsertFunctions++
			if path != wantStore ||
				function.Name.Name != "SealAgentTurnContextSnapshot" ||
				fullInsertLiterals != 1 {
				violations = append(violations, fmt.Sprintf(
					"%s:%s has forbidden raw/dynamic snapshot INSERT",
					path, function.Name.Name,
				))
			}
		}
	}
	return violations, sealSelectors, rawInsertFunctions
}
