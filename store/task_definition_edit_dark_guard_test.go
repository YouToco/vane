package store

import (
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

// C2b3-2b publishes only the durable Store substrate. No production package
// may acquire an edit lease, mutate a phase, or dispatch its terminal receipt
// until the coordinator lands with the authenticated task.BuildFrozen ->
// Store.Create wiring and one-remote-phase-per-attempt guard in C2b3-2c.
func TestTaskDefinitionEditStoreAPIsRemainDark(t *testing.T) {
	t.Parallel()
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate definition edit Store dark guard")
	}
	storeDir := filepath.Clean(filepath.Dir(testFile))
	repoRoot := filepath.Clean(filepath.Dir(storeDir))
	providers := map[string]struct{}{
		filepath.Join(storeDir, "task_definition_edit_operations.go"): {},
		filepath.Join(storeDir, "task_definition_edit_receipts.go"):   {},
	}
	guarded := taskDefinitionEditDarkStoreMethods()
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
		if _, provider := providers[filepath.Clean(path)]; provider {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		ast.Inspect(file, func(node ast.Node) bool {
			selector, ok := node.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if _, watched := guarded[selector.Sel.Name]; watched {
				violations = append(violations, fset.Position(selector.Sel.Pos()).String()+
					": production reference "+selector.Sel.Name)
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("scan production definition edit Store calls: %v", err)
	}
	slices.Sort(violations)
	if len(violations) != 0 {
		t.Fatalf("C2b3-2b Store APIs must remain dark:\n%s", strings.Join(violations, "\n"))
	}
}

func TestTaskDefinitionEditStoreDarkGuardCatchesMethodValues(t *testing.T) {
	t.Parallel()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "probe.go", `package probe
func probe(s interface{ AcquireTaskDefinitionEditOperation() }) {
	_ = s.AcquireTaskDefinitionEditOperation
	_ = s.CompleteTaskDefinitionEditOperation
}
`, 0)
	if err != nil {
		t.Fatal(err)
	}
	guarded := taskDefinitionEditDarkStoreMethods()
	var got []string
	ast.Inspect(file, func(node ast.Node) bool {
		selector, ok := node.(*ast.SelectorExpr)
		if ok {
			if _, watched := guarded[selector.Sel.Name]; watched {
				got = append(got, selector.Sel.Name)
			}
		}
		return true
	})
	slices.Sort(got)
	if !slices.Equal(got, []string{
		"AcquireTaskDefinitionEditOperation",
		"CompleteTaskDefinitionEditOperation",
	}) {
		t.Fatalf("method-value references escaped dark guard: %v", got)
	}
}

func TestTaskDefinitionEditStoreDarkGuardCoversEveryExportedMethod(t *testing.T) {
	t.Parallel()
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate definition edit Store dark guard")
	}
	storeDir := filepath.Clean(filepath.Dir(testFile))
	fset := token.NewFileSet()
	guarded := taskDefinitionEditDarkStoreMethods()
	for _, name := range []string{
		"task_definition_edit_operations.go",
		"task_definition_edit_receipts.go",
	} {
		file, err := parser.ParseFile(fset, filepath.Join(storeDir, name), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Recv == nil || !function.Name.IsExported() ||
				!strings.Contains(function.Name.Name, "TaskDefinitionEdit") {
				continue
			}
			if _, covered := guarded[function.Name.Name]; !covered {
				t.Fatalf("exported Store method %s is missing from the dark guard",
					function.Name.Name)
			}
		}
	}
}

func taskDefinitionEditDarkStoreMethods() map[string]struct{} {
	names := []string{
		"CreateTaskDefinitionEditOperation",
		"LoadTaskDefinitionEditOperation",
		"CancelTaskDefinitionEditOperation",
		"ExpireTaskDefinitionEditOperation",
		"AcquireTaskDefinitionEditOperation",
		"RenewTaskDefinitionEditLease",
		"ListStaleTaskDefinitionEditTenantIDs",
		"ListStaleTaskDefinitionEditOperations",
		"QuiesceTaskDefinitionEdit",
		"AuthorizeTaskDefinitionEditRemotePhase",
		"CheckpointTaskDefinitionEditBasePaused",
		"CommitTaskDefinitionEditDefinition",
		"CheckpointTaskDefinitionEditTargetApplied",
		"CheckpointTaskDefinitionEditTargetRestored",
		"BlockTaskDefinitionEditOperation",
		"SupersedeTaskDefinitionEditOperation",
		"CompleteTaskDefinitionEditOperation",
		"LoadTaskDefinitionEditReceiptByOperation",
		"ListDueTaskDefinitionEditReceiptTenantIDs",
		"ListDueTaskDefinitionEditReceipts",
		"AcquireTaskDefinitionEditReceipt",
		"CheckpointTaskDefinitionEditReceiptPayload",
		"RecordTaskDefinitionEditReceiptSessionMessages",
		"MarkTaskDefinitionEditReceiptSent",
		"RecordTaskDefinitionEditReceiptSendFailure",
	}
	guarded := make(map[string]struct{}, len(names))
	for _, name := range names {
		guarded[name] = struct{}{}
	}
	return guarded
}
