package store

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

var legacyBatch63RepairGuardedMethods = map[string]struct{}{
	"PreviewLegacyBatch63Repair":  {},
	"FinalizeLegacyBatch63Repair": {},
	"VerifyLegacyBatch63Repair":   {},
	"AbortLegacyBatch63Repair":    {},
}

var legacyBatch63RepairAllowedReferences = map[legacyBatch63RepairGuardKey]int{
	{
		path:     "cmd/runtimeadmin/main.go",
		function: "executeLegacyBatch63Repair",
		method:   "PreviewLegacyBatch63Repair",
	}: 1,
	{
		path:     "cmd/runtimeadmin/main.go",
		function: "executeLegacyBatch63Repair",
		method:   "FinalizeLegacyBatch63Repair",
	}: 1,
	{
		path:     "cmd/runtimeadmin/main.go",
		function: "executeLegacyBatch63Repair",
		method:   "VerifyLegacyBatch63Repair",
	}: 1,
	{
		path:     "cmd/runtimeadmin/main.go",
		function: "executeLegacyBatch63Repair",
		method:   "AbortLegacyBatch63Repair",
	}: 1,
	{
		path:     "store/legacy_push_batch_63_repair.go",
		function: "FinalizeLegacyBatch63Repair",
		method:   "VerifyLegacyBatch63Repair",
	}: 1,
	{
		path:     "store/legacy_push_batch_63_repair.go",
		function: "verifyLegacyBatch63FinalizeReplay",
		method:   "VerifyLegacyBatch63Repair",
	}: 1,
	{
		path:     "store/legacy_push_batch_63_repair.go",
		function: "AbortLegacyBatch63Repair",
		method:   "VerifyLegacyBatch63Repair",
	}: 1,
}

type legacyBatch63RepairGuardKey struct {
	path     string
	function string
	method   string
}

func TestLegacyBatch63RepairProductionEntryIsRuntimeAdminOnly(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve legacy batch 63 repair guard")
	}
	root := filepath.Dir(filepath.Dir(thisFile))
	got := make(map[legacyBatch63RepairGuardKey]int)
	var violations []string
	err := filepath.WalkDir(root,
		func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() {
				switch entry.Name() {
				case ".git", "vendor":
					return filepath.SkipDir
				default:
					return nil
				}
			}
			if filepath.Ext(path) != ".go" ||
				strings.HasSuffix(path, "_test.go") {
				return nil
			}
			relative, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			relative = filepath.ToSlash(relative)
			file, err := parser.ParseFile(
				token.NewFileSet(), path, nil, 0)
			if err != nil {
				return err
			}
			fileViolations, fileAllowed := legacyBatch63RepairInspectFile(
				file, relative)
			violations = append(violations, fileViolations...)
			for key, count := range fileAllowed {
				got[key] += count
			}
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if len(violations) != 0 {
		t.Fatalf("legacy batch 63 repair API escaped runtimeadmin: %v",
			violations)
	}
	for key, want := range legacyBatch63RepairAllowedReferences {
		if got[key] != want {
			t.Errorf("%s:%s references %s = %d, want %d",
				key.path, key.function, key.method, got[key], want)
		}
	}
	if len(got) != len(legacyBatch63RepairAllowedReferences) {
		t.Errorf("allowed legacy batch 63 references = %v, want exactly %v",
			got, legacyBatch63RepairAllowedReferences)
	}
}

func TestLegacyBatch63RepairGuardRejectsProductionEntryMutations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		path   string
		source string
	}{
		{
			name: "HTTP server call",
			path: "cmd/server/main.go",
			source: `package main
func serve(st interface{ PreviewLegacyBatch63Repair() }) {
	st.PreviewLegacyBatch63Repair()
}`,
		},
		{
			name: "Agent method value",
			path: "agent/tools.go",
			source: `package agent
func register(st interface{ FinalizeLegacyBatch63Repair() }) {
	run := st.FinalizeLegacyBatch63Repair
	_ = run
}`,
		},
		{
			name: "Temporal activity call",
			path: "workflow/activities.go",
			source: `package workflow
func recover(st interface{ VerifyLegacyBatch63Repair() }) {
	st.VerifyLegacyBatch63Repair()
}`,
		},
		{
			name: "worker call",
			path: "worker/recovery.go",
			source: `package worker
func abort(st interface{ AbortLegacyBatch63Repair() }) {
	st.AbortLegacyBatch63Repair()
}`,
		},
		{
			name: "provider call",
			path: "provider/feishu.go",
			source: `package provider
func send(st interface{ PreviewLegacyBatch63Repair() }) {
	st.PreviewLegacyBatch63Repair()
}`,
		},
		{
			name: "Store wrapper call",
			path: "store/legacy_push_batch_63_repair.go",
			source: `package store
type Store struct{}
func (s *Store) leakedWrapper() {
	s.VerifyLegacyBatch63Repair()
}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			file, err := parser.ParseFile(
				token.NewFileSet(), tt.path, tt.source, 0)
			if err != nil {
				t.Fatal(err)
			}
			violations, allowed := legacyBatch63RepairInspectFile(
				file, tt.path)
			if len(violations) != 1 || len(allowed) != 0 {
				t.Fatalf("mutation violations=%v allowed=%v, want one violation",
					violations, allowed)
			}
		})
	}
}

func TestLegacyBatch63RepairGuardIgnoresMethodDefinitions(t *testing.T) {
	t.Parallel()

	file, err := parser.ParseFile(
		token.NewFileSet(), "store/legacy_push_batch_63_repair.go", `package store
type Store struct{}
func (s *Store) PreviewLegacyBatch63Repair() {}
func (s *Store) FinalizeLegacyBatch63Repair() {}
func (s *Store) VerifyLegacyBatch63Repair() {}
func (s *Store) AbortLegacyBatch63Repair() {}
`, 0)
	if err != nil {
		t.Fatal(err)
	}
	violations, allowed := legacyBatch63RepairInspectFile(
		file, "store/legacy_push_batch_63_repair.go")
	if len(violations) != 0 || len(allowed) != 0 {
		t.Fatalf("Store method definitions counted as calls: violations=%v allowed=%v",
			violations, allowed)
	}
}

func legacyBatch63RepairInspectFile(
	file *ast.File,
	path string,
) ([]string, map[legacyBatch63RepairGuardKey]int) {
	functions := make(map[token.Pos]string)
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Body == nil {
			continue
		}
		ast.Inspect(function.Body, func(node ast.Node) bool {
			if selector, ok := node.(*ast.SelectorExpr); ok {
				functions[selector.Sel.Pos()] = function.Name.Name
			}
			return true
		})
	}

	path = filepath.ToSlash(path)
	allowed := make(map[legacyBatch63RepairGuardKey]int)
	var violations []string
	ast.Inspect(file, func(node ast.Node) bool {
		selector, ok := node.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if _, guarded := legacyBatch63RepairGuardedMethods[selector.Sel.Name]; !guarded {
			return true
		}
		function := functions[selector.Sel.Pos()]
		if function == "" {
			function = "<package>"
		}
		key := legacyBatch63RepairGuardKey{
			path:     path,
			function: function,
			method:   selector.Sel.Name,
		}
		if _, ok := legacyBatch63RepairAllowedReferences[key]; ok {
			allowed[key]++
			return true
		}
		violations = append(violations, fmt.Sprintf(
			"%s:%s references %s", path, function, selector.Sel.Name))
		return true
	})
	return violations, allowed
}
