package store

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

// C2b3-2c gives exactly one production file access to the durable operation
// substrate. Receipt delivery remains dark until the authenticated C2b3-2d
// wiring lands.
func TestTaskDefinitionEditStoreAPIsHaveOneCoordinator(t *testing.T) {
	t.Parallel()
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate definition edit Store guard")
	}
	storeDir := filepath.Clean(filepath.Dir(testFile))
	repoRoot := filepath.Clean(filepath.Dir(storeDir))
	providerFunctions := taskDefinitionEditStoreProviderFunctionSymbols(storeDir)
	coordinatorPath := filepath.Join(repoRoot, "task", "definition_edit_coordinator.go")
	operationMethods := taskDefinitionEditOperationStoreMethods()
	receiptMethods := taskDefinitionEditReceiptStoreMethods()
	mutationGraph := taskDefinitionEditStoreMutationGraphSymbols()
	fset := token.NewFileSet()
	productionFiles, err := taskDefinitionEditStoreProductionFiles(repoRoot, fset)
	if err != nil {
		t.Fatalf("parse production definition edit Store calls: %v", err)
	}
	providerFiles := make(map[string]*ast.File, len(providerFunctions))
	for providerPath := range providerFunctions {
		file, ok := productionFiles[providerPath]
		if !ok {
			t.Fatalf("definition edit Store provider %s is missing", providerPath)
		}
		providerFiles[providerPath] = file
	}
	providerGraphAllowed, providerGraphViolations :=
		taskDefinitionEditStoreProviderGraphReferences(
			providerFiles,
			providerFunctions,
			mutationGraph,
			taskDefinitionEditStoreMutationGraphExpectations(),
			fset,
		)
	storeAliases := taskDefinitionEditStoreAliases(
		productionFiles,
		storeDir,
		providerFunctions,
		taskDefinitionEditStoreTaintSymbols(
			operationMethods, receiptMethods, mutationGraph,
		),
	)
	violations := append([]string(nil), providerGraphViolations...)
	for path, file := range productionFiles {
		cleanPath := filepath.Clean(path)
		allowed := map[token.Pos]struct{}{}
		if cleanPath == coordinatorPath {
			var allowErr error
			allowed, allowErr = taskDefinitionEditCoordinatorStoreCalls(file)
			if allowErr != nil {
				t.Fatalf("validate exact coordinator Store calls: %v", allowErr)
			}
		}
		violations = append(violations, taskDefinitionEditStoreReferenceViolations(
			fset, file, allowed, operationMethods, receiptMethods, storeAliases,
		)...)
		violations = append(violations, taskDefinitionEditStoreGraphBoundaryViolations(
			cleanPath,
			file,
			storeDir,
			mutationGraph,
			providerGraphAllowed,
			storeAliases,
			fset,
		)...)
	}
	slices.Sort(violations)
	if len(violations) != 0 {
		t.Fatalf("C2b3-2c Store boundary escaped its coordinator:\n%s",
			strings.Join(violations, "\n"))
	}
}

func TestTaskDefinitionEditStoreGuardCatchesMethodValuesAndReceiptCalls(t *testing.T) {
	t.Parallel()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "probe.go", `package probe
func probe(s interface {
	AcquireTaskDefinitionEditOperation()
	MarkTaskDefinitionEditReceiptSent()
}) {
	_ = s.AcquireTaskDefinitionEditOperation
	s.MarkTaskDefinitionEditReceiptSent()
}
func reflectProbe(v interface{ MethodByName(string) }) {
	_ = v.MethodByName("AcquireTaskDefinitionEditOperation")
}
func dynamicReflectProbe(v interface{ MethodByName(string) }, method string) {
	_ = v.MethodByName(method)
}
`, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	violations := taskDefinitionEditStoreReferenceViolations(
		fset,
		file,
		nil,
		taskDefinitionEditOperationStoreMethods(),
		taskDefinitionEditReceiptStoreMethods(),
		nil,
	)
	if len(violations) != 4 {
		t.Fatalf("method-value or receipt references escaped Store guard: %v", violations)
	}
}

func TestTaskDefinitionEditStoreGuardRejectsProviderAliasAndRenamedWrapper(t *testing.T) {
	t.Parallel()
	fset := token.NewFileSet()
	providerPath := filepath.Clean("/repo/store/task_definition_edit_operations.go")
	provider, err := parser.ParseFile(fset, providerPath, `package store
type Store struct{}
func (s *Store) AcquireTaskDefinitionEditOperation() {}
var hiddenDefinitionEdit = (*Store).AcquireTaskDefinitionEditOperation
var EscapeDefinitionEditOperation = hiddenDefinitionEdit
func (s *Store) AdvanceEdit() { EscapeDefinitionEditOperation(s) }
`, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	operationMethods := taskDefinitionEditOperationStoreMethods()
	receiptMethods := taskDefinitionEditReceiptStoreMethods()
	providerFunctions := map[string]map[string]struct{}{
		providerPath: {"AcquireTaskDefinitionEditOperation": {}},
	}
	aliases := taskDefinitionEditStoreAliases(
		map[string]*ast.File{providerPath: provider},
		filepath.Dir(providerPath),
		providerFunctions,
		taskDefinitionEditStoreTaintSymbols(
			operationMethods,
			receiptMethods,
			taskDefinitionEditStoreMutationGraphSymbols(),
		),
	)
	for _, name := range []string{
		"hiddenDefinitionEdit",
		"EscapeDefinitionEditOperation",
		"AdvanceEdit",
	} {
		if _, ok := aliases[name]; !ok {
			t.Fatalf("provider alias/wrapper %s escaped transitive taint", name)
		}
	}
	providerViolations := taskDefinitionEditStoreReferenceViolations(
		fset, provider, nil, operationMethods, receiptMethods, aliases,
	)
	if len(providerViolations) == 0 {
		t.Fatal("provider method expression/alias/wrapper escaped Store guard")
	}

	consumer, err := parser.ParseFile(fset, "cmd/server/main.go", `package main
import storepkg "github.com/YouToco/vane/store"
func run(s *storepkg.Store) {
	storepkg.EscapeDefinitionEditOperation(s)
	s.AdvanceEdit()
}
`, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	consumerViolations := taskDefinitionEditStoreReferenceViolations(
		fset, consumer, nil, operationMethods, receiptMethods, aliases,
	)
	for _, name := range []string{"EscapeDefinitionEditOperation", "AdvanceEdit"} {
		if !slices.ContainsFunc(consumerViolations, func(violation string) bool {
			return strings.Contains(violation, name)
		}) {
			t.Fatalf("consumer call through %s escaped fail-closed alias guard: %v",
				name, consumerViolations)
		}
	}
}

func TestTaskDefinitionEditStoreGuardRejectsPrivateMutatorWrapper(t *testing.T) {
	t.Parallel()
	fset := token.NewFileSet()
	providerPath := filepath.Clean("/repo/store/task_definition_edit_operations.go")
	provider, err := parser.ParseFile(fset, providerPath, `package store
type Store struct{}
func (s *Store) checkpointTaskDefinitionEditSnapshot() error { return nil }
func (s *Store) AdvanceEdit() error {
	return s.checkpointTaskDefinitionEditSnapshot()
}
`, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	graph := map[string]struct{}{"checkpointTaskDefinitionEditSnapshot": {}}
	providerFunctions := map[string]map[string]struct{}{
		providerPath: {"checkpointTaskDefinitionEditSnapshot": {}},
	}
	allowed, providerViolations := taskDefinitionEditStoreProviderGraphReferences(
		map[string]*ast.File{providerPath: provider},
		providerFunctions,
		graph,
		nil,
		fset,
	)
	if len(providerViolations) == 0 {
		t.Fatal("renamed provider method directly wrapping a private mutator escaped graph guard")
	}
	aliases := taskDefinitionEditStoreAliases(
		map[string]*ast.File{providerPath: provider},
		filepath.Dir(providerPath),
		providerFunctions,
		graph,
	)
	if _, ok := aliases["AdvanceEdit"]; !ok {
		t.Fatal("private-mutator wrapper escaped transitive Store taint")
	}
	providerBoundary := taskDefinitionEditStoreGraphBoundaryViolations(
		providerPath,
		provider,
		filepath.Dir(providerPath),
		graph,
		allowed,
		aliases,
		fset,
	)
	if len(providerBoundary) == 0 {
		t.Fatal("private-mutator provider wrapper escaped Store boundary")
	}

	consumerPath := filepath.Clean("/repo/cmd/server/main.go")
	consumer, err := parser.ParseFile(fset, consumerPath, `package main
import storepkg "github.com/YouToco/vane/store"
func run(s *storepkg.Store) { _ = s.AdvanceEdit() }
`, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	consumerViolations := taskDefinitionEditStoreGraphBoundaryViolations(
		consumerPath,
		consumer,
		filepath.Dir(providerPath),
		graph,
		allowed,
		aliases,
		fset,
	)
	if !slices.ContainsFunc(consumerViolations, func(violation string) bool {
		return strings.Contains(violation, "AdvanceEdit")
	}) {
		t.Fatalf("consumer private-mutator wrapper call escaped Store boundary: %v",
			consumerViolations)
	}
}

func TestTaskDefinitionEditStoreGuardCoversEveryExportedMethod(t *testing.T) {
	t.Parallel()
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate definition edit Store guard")
	}
	storeDir := filepath.Clean(filepath.Dir(testFile))
	fset := token.NewFileSet()
	operationMethods := taskDefinitionEditOperationStoreMethods()
	receiptMethods := taskDefinitionEditReceiptStoreMethods()
	providerFiles := make(map[string]*ast.File, 3)
	for _, name := range []string{
		"task_definition_edit_operations.go",
		"task_definition_edit_receipts.go",
		"task_definition_edit_tx.go",
	} {
		file, err := parser.ParseFile(fset, filepath.Join(storeDir, name), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		providerFiles[name] = file
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Recv == nil || !function.Name.IsExported() ||
				!strings.Contains(function.Name.Name, "TaskDefinitionEdit") {
				continue
			}
			_, operationCovered := operationMethods[function.Name.Name]
			_, receiptCovered := receiptMethods[function.Name.Name]
			if operationCovered == receiptCovered {
				t.Fatalf("exported Store method %s must be covered by exactly one boundary set",
					function.Name.Name)
			}
		}
	}
	aliases := taskDefinitionEditStoreAliases(
		providerFiles,
		".",
		taskDefinitionEditStoreProviderFunctionSymbols("."),
		taskDefinitionEditStoreTaintSymbols(
			operationMethods,
			receiptMethods,
			taskDefinitionEditStoreMutationGraphSymbols(),
		),
	)
	if len(aliases) != 0 {
		names := make([]string, 0, len(aliases))
		for name := range aliases {
			names = append(names, name)
		}
		slices.Sort(names)
		t.Fatalf("definition edit Store providers expose method aliases/wrappers: %s",
			strings.Join(names, ", "))
	}
}

type taskDefinitionEditStoreCallExpectation struct {
	method   string
	function string
	count    int
}

func taskDefinitionEditCoordinatorStoreCalls(file *ast.File) (map[token.Pos]struct{}, error) {
	expectations := []taskDefinitionEditStoreCallExpectation{
		{"CreateTaskDefinitionEditOperation", "sealTaskDefinitionEditProposal", 2},
		{"LoadTaskDefinitionEditOperation", "sealTaskDefinitionEditProposal", 1},
		{"LoadTaskDefinitionEditOperation", "Confirm", 1},
		{"CancelTaskDefinitionEditOperation", "Cancel", 1},
		{"ExpireTaskDefinitionEditOperation", "Expire", 1},
		{"AcquireTaskDefinitionEditOperation", "acquireTaskDefinitionEdit", 2},
		{"LoadTaskDefinitionEditOperation", "acquireTaskDefinitionEdit", 1},
		{"RenewTaskDefinitionEditLease", "runTaskDefinitionEditAttempt", 1},
		{"LoadTaskDefinitionEditOperation", "runTaskDefinitionEditAttempt", 1},
		{"QuiesceTaskDefinitionEdit", "runTaskDefinitionEditAttempt", 1},
		{"CommitTaskDefinitionEditDefinition", "runTaskDefinitionEditAttempt", 1},
		{"CompleteTaskDefinitionEditOperation", "runTaskDefinitionEditAttempt", 1},
		{"CheckpointTaskDefinitionEditBasePaused", "runTaskDefinitionEditPauseAttempt", 1},
		{"CheckpointTaskDefinitionEditTargetApplied", "runTaskDefinitionEditApplyAttempt", 1},
		{"CheckpointTaskDefinitionEditTargetRestored", "runTaskDefinitionEditRestoreAttempt", 1},
		{"AuthorizeTaskDefinitionEditRemotePhase", "authorizeTaskDefinitionEditRemote", 1},
		{"BlockTaskDefinitionEditOperation", "handleTaskDefinitionEditRemoteError", 1},
		{"BlockTaskDefinitionEditOperation", "blockTaskDefinitionEditCheckpoint", 1},
		{"LoadTaskDefinitionEditOperation", "reloadTaskDefinitionEditProgress", 1},
		{"LoadTaskDefinitionEditOperation", "loadTaskDefinitionEditConvergent", 1},
		{"ListStaleTaskDefinitionEditTenantIDs", "RecoverStaleOnce", 2},
		{"ListStaleTaskDefinitionEditOperations", "RecoverStaleOnce", 1},
	}
	type expectationKey struct{ method, function string }
	want := make(map[expectationKey]int, len(expectations))
	for _, expectation := range expectations {
		key := expectationKey{expectation.method, expectation.function}
		if _, duplicate := want[key]; duplicate {
			return nil, fmt.Errorf("duplicate Store call expectation %s in %s",
				expectation.method, expectation.function)
		}
		want[key] = expectation.count
	}

	allowed := make(map[token.Pos]struct{})
	got := make(map[expectationKey]int, len(want))
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Body == nil {
			continue
		}
		ast.Inspect(function.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := taskDefinitionEditStoreUnparen(call.Fun).(*ast.SelectorExpr)
			if !ok || !taskDefinitionEditIsCoordinatorStoreSelector(selector) {
				return true
			}
			key := expectationKey{selector.Sel.Name, function.Name.Name}
			if _, expected := want[key]; expected {
				got[key]++
				allowed[selector.Sel.Pos()] = struct{}{}
			}
			return true
		})
	}
	for key, expected := range want {
		if got[key] != expected {
			return nil, fmt.Errorf("coordinator %s must directly call c.store.%s exactly %d time(s), got %d",
				key.function, key.method, expected, got[key])
		}
	}
	return allowed, nil
}

func taskDefinitionEditStoreReferenceViolations(
	fset *token.FileSet,
	file *ast.File,
	allowed map[token.Pos]struct{},
	operationMethods map[string]struct{},
	receiptMethods map[string]struct{},
	providerAliases map[string]struct{},
) []string {
	var violations []string
	importsReflect := taskDefinitionEditStoreImports(file, "reflect")
	calledMethodSelectors := make(map[token.Pos]struct{})
	for _, group := range file.Comments {
		for _, comment := range group.List {
			if taskDefinitionEditStoreIsGoLinkname(comment.Text) {
				violations = append(violations,
					fset.Position(comment.Pos()).String()+": go:linkname is forbidden in production")
			}
		}
	}
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := taskDefinitionEditStoreUnparen(call.Fun).(*ast.SelectorExpr)
		if ok && selector.Sel.Name == "Method" {
			calledMethodSelectors[selector.Sel.Pos()] = struct{}{}
			violations = append(violations,
				fset.Position(selector.Sel.Pos()).String()+
					": dynamic method reflection is forbidden in production")
		}
		return true
	})
	ast.Inspect(file, func(node ast.Node) bool {
		selector, ok := node.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		_, alreadyReported := calledMethodSelectors[selector.Sel.Pos()]
		if selector.Sel.Name == "MethodByName" ||
			(selector.Sel.Name == "Method" && importsReflect && !alreadyReported) {
			violations = append(violations,
				fset.Position(selector.Sel.Pos()).String()+
					": dynamic method reflection is forbidden in production")
		}
		return true
	})
	ast.Inspect(file, func(node ast.Node) bool {
		selector, ok := node.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if _, operation := operationMethods[selector.Sel.Name]; operation {
			if _, ok := allowed[selector.Sel.Pos()]; ok {
				return true
			}
			violations = append(violations, fset.Position(selector.Sel.Pos()).String()+
				": operation Store reference outside its exact coordinator call "+selector.Sel.Name)
			return true
		}
		if _, receipt := receiptMethods[selector.Sel.Name]; receipt {
			violations = append(violations, fset.Position(selector.Sel.Pos()).String()+
				": definition edit receipt Store API must remain dark "+selector.Sel.Name)
		}
		return true
	})
	ast.Inspect(file, func(node ast.Node) bool {
		identifier, ok := node.(*ast.Ident)
		if !ok {
			return true
		}
		if _, alias := providerAliases[identifier.Name]; alias {
			violations = append(violations, fset.Position(identifier.Pos()).String()+
				": definition edit Store provider alias/wrapper reference "+identifier.Name)
		}
		return true
	})
	return violations
}

func taskDefinitionEditStoreProviderGraphReferences(
	files map[string]*ast.File,
	expectedFunctions map[string]map[string]struct{},
	mutationGraph map[string]struct{},
	expectations []taskDefinitionEditStoreCallExpectation,
	fset *token.FileSet,
) (map[token.Pos]struct{}, []string) {
	type expectationKey struct{ method, function string }
	want := make(map[expectationKey]int, len(expectations))
	for _, expectation := range expectations {
		key := expectationKey{expectation.method, expectation.function}
		if _, duplicate := want[key]; duplicate {
			return nil, []string{fmt.Sprintf(
				"duplicate Store graph expectation %s in %s",
				expectation.method,
				expectation.function,
			)}
		}
		want[key] = expectation.count
	}

	allowed := make(map[token.Pos]struct{})
	got := make(map[expectationKey]int, len(want))
	var violations []string
	for path, expected := range expectedFunctions {
		file := files[path]
		if file == nil {
			violations = append(violations, "missing Store provider "+path)
			continue
		}
		declared := make(map[string]int, len(expected))
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok {
				continue
			}
			if _, controlled := expected[function.Name.Name]; !controlled {
				violations = append(violations, fset.Position(function.Name.Pos()).String()+
					": definition edit Store provider has unexpected function/method "+
					function.Name.Name)
			} else {
				declared[function.Name.Name]++
			}
			if _, sensitive := mutationGraph[function.Name.Name]; sensitive {
				allowed[function.Name.Pos()] = struct{}{}
			}
			if function.Body == nil {
				continue
			}
			ast.Inspect(function.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				callee := taskDefinitionEditStoreUnparen(call.Fun)
				var identifier *ast.Ident
				switch typed := callee.(type) {
				case *ast.Ident:
					identifier = typed
				case *ast.SelectorExpr:
					identifier = typed.Sel
				}
				if identifier == nil {
					return true
				}
				if _, sensitive := mutationGraph[identifier.Name]; !sensitive {
					return true
				}
				key := expectationKey{identifier.Name, function.Name.Name}
				if _, expected := want[key]; !expected {
					violations = append(violations, fset.Position(identifier.Pos()).String()+
						": private Store mutation graph call is not allowlisted "+
						function.Name.Name+" -> "+identifier.Name)
					return true
				}
				got[key]++
				allowed[identifier.Pos()] = struct{}{}
				return true
			})
		}
		for name := range expected {
			if declared[name] != 1 {
				violations = append(violations, fmt.Sprintf(
					"definition edit Store provider %s must declare %s exactly once, got %d",
					path,
					name,
					declared[name],
				))
			}
		}
	}
	for expected, count := range want {
		if got[expected] != count {
			violations = append(violations, fmt.Sprintf(
				"Store provider %s must directly call %s exactly %d time(s), got %d",
				expected.function,
				expected.method,
				count,
				got[expected],
			))
		}
	}
	return allowed, violations
}

func taskDefinitionEditStoreGraphBoundaryViolations(
	path string,
	file *ast.File,
	storeDir string,
	mutationGraph map[string]struct{},
	providerAllowed map[token.Pos]struct{},
	aliases map[string]struct{},
	fset *token.FileSet,
) []string {
	insideStore := filepath.Clean(filepath.Dir(path)) == filepath.Clean(storeDir)
	var violations []string
	ast.Inspect(file, func(node ast.Node) bool {
		identifier, ok := node.(*ast.Ident)
		if !ok {
			return true
		}
		if _, alias := aliases[identifier.Name]; alias {
			violations = append(violations, fset.Position(identifier.Pos()).String()+
				": renamed definition edit Store mutation wrapper/alias "+identifier.Name)
			return true
		}
		if !insideStore {
			return true
		}
		if _, sensitive := mutationGraph[identifier.Name]; !sensitive {
			return true
		}
		if _, allowed := providerAllowed[identifier.Pos()]; allowed {
			return true
		}
		violations = append(violations, fset.Position(identifier.Pos()).String()+
			": private definition edit Store mutation graph reference outside its exact provider edge "+
			identifier.Name)
		return true
	})
	return violations
}

func taskDefinitionEditStoreTaintSymbols(sets ...map[string]struct{}) map[string]struct{} {
	merged := make(map[string]struct{})
	for _, set := range sets {
		for name := range set {
			merged[name] = struct{}{}
		}
	}
	return merged
}

func taskDefinitionEditStoreMutationGraphSymbols() map[string]struct{} {
	return taskDefinitionEditStoreMethodSet([]string{
		"terminatePendingTaskDefinitionEdit",
		"expireTaskDefinitionEditDuringAcquire",
		"confirmAndTerminateTaskDefinitionEditDuringAcquire",
		"acquirePendingTaskDefinitionEdit",
		"terminateAcquiredTaskDefinitionEditAssessment",
		"takeOverTaskDefinitionEdit",
		"checkpointTaskDefinitionEditSnapshot",
		"blockInvalidTaskDefinitionEditCheckpoint",
		"terminateTaskDefinitionEditTx",
		"insertTaskDefinitionEditReceiptForTerminal",
		"beginTaskDefinitionEditTx",
		"beginTaskDefinitionEditReceiptTx",
		"beginTaskDefinitionEditRoleTx",
	})
}

func taskDefinitionEditStoreMutationGraphExpectations() []taskDefinitionEditStoreCallExpectation {
	return []taskDefinitionEditStoreCallExpectation{
		{"terminatePendingTaskDefinitionEdit", "CancelTaskDefinitionEditOperation", 1},
		{"terminatePendingTaskDefinitionEdit", "ExpireTaskDefinitionEditOperation", 1},
		{"expireTaskDefinitionEditDuringAcquire", "AcquireTaskDefinitionEditOperation", 1},
		{"confirmAndTerminateTaskDefinitionEditDuringAcquire", "AcquireTaskDefinitionEditOperation", 1},
		{"acquirePendingTaskDefinitionEdit", "AcquireTaskDefinitionEditOperation", 1},
		{"terminateAcquiredTaskDefinitionEditAssessment", "AcquireTaskDefinitionEditOperation", 2},
		{"terminateAcquiredTaskDefinitionEditAssessment", "RenewTaskDefinitionEditLease", 1},
		{"terminateAcquiredTaskDefinitionEditAssessment", "QuiesceTaskDefinitionEdit", 1},
		{"terminateAcquiredTaskDefinitionEditAssessment", "AuthorizeTaskDefinitionEditRemotePhase", 1},
		{"terminateAcquiredTaskDefinitionEditAssessment", "checkpointTaskDefinitionEditSnapshot", 1},
		{"terminateAcquiredTaskDefinitionEditAssessment", "CommitTaskDefinitionEditDefinition", 1},
		{"takeOverTaskDefinitionEdit", "AcquireTaskDefinitionEditOperation", 1},
		{"checkpointTaskDefinitionEditSnapshot", "CheckpointTaskDefinitionEditBasePaused", 1},
		{"checkpointTaskDefinitionEditSnapshot", "CheckpointTaskDefinitionEditTargetApplied", 1},
		{"checkpointTaskDefinitionEditSnapshot", "CheckpointTaskDefinitionEditTargetRestored", 1},
		{"blockInvalidTaskDefinitionEditCheckpoint", "checkpointTaskDefinitionEditSnapshot", 2},
		{"terminateTaskDefinitionEditTx", "terminateAcquiredTaskDefinitionEditAssessment", 1},
		{"terminateTaskDefinitionEditTx", "blockInvalidTaskDefinitionEditCheckpoint", 1},
		{"terminateTaskDefinitionEditTx", "BlockTaskDefinitionEditOperation", 1},
		{"terminateTaskDefinitionEditTx", "SupersedeTaskDefinitionEditOperation", 1},
		{"terminateTaskDefinitionEditTx", "CompleteTaskDefinitionEditOperation", 1},
		{"insertTaskDefinitionEditReceiptForTerminal", "terminatePendingTaskDefinitionEdit", 1},
		{"insertTaskDefinitionEditReceiptForTerminal", "expireTaskDefinitionEditDuringAcquire", 1},
		{"insertTaskDefinitionEditReceiptForTerminal", "confirmAndTerminateTaskDefinitionEditDuringAcquire", 1},
		{"insertTaskDefinitionEditReceiptForTerminal", "terminateTaskDefinitionEditTx", 1},
		{"beginTaskDefinitionEditTx", "CreateTaskDefinitionEditOperation", 1},
		{"beginTaskDefinitionEditTx", "LoadTaskDefinitionEditOperation", 1},
		{"beginTaskDefinitionEditTx", "terminatePendingTaskDefinitionEdit", 1},
		{"beginTaskDefinitionEditTx", "AcquireTaskDefinitionEditOperation", 1},
		{"beginTaskDefinitionEditTx", "RenewTaskDefinitionEditLease", 1},
		{"beginTaskDefinitionEditTx", "ListStaleTaskDefinitionEditOperations", 1},
		{"beginTaskDefinitionEditTx", "QuiesceTaskDefinitionEdit", 1},
		{"beginTaskDefinitionEditTx", "AuthorizeTaskDefinitionEditRemotePhase", 1},
		{"beginTaskDefinitionEditTx", "checkpointTaskDefinitionEditSnapshot", 1},
		{"beginTaskDefinitionEditTx", "BlockTaskDefinitionEditOperation", 1},
		{"beginTaskDefinitionEditTx", "SupersedeTaskDefinitionEditOperation", 1},
		{"beginTaskDefinitionEditTx", "CompleteTaskDefinitionEditOperation", 1},
		{"beginTaskDefinitionEditTx", "CommitTaskDefinitionEditDefinition", 1},
		{"beginTaskDefinitionEditReceiptTx", "LoadTaskDefinitionEditReceiptByOperation", 1},
		{"beginTaskDefinitionEditReceiptTx", "ListDueTaskDefinitionEditReceipts", 1},
		{"beginTaskDefinitionEditReceiptTx", "AcquireTaskDefinitionEditReceipt", 1},
		{"beginTaskDefinitionEditReceiptTx", "CheckpointTaskDefinitionEditReceiptPayload", 1},
		{"beginTaskDefinitionEditReceiptTx", "RecordTaskDefinitionEditReceiptSessionMessages", 1},
		{"beginTaskDefinitionEditReceiptTx", "MarkTaskDefinitionEditReceiptSent", 1},
		{"beginTaskDefinitionEditReceiptTx", "RecordTaskDefinitionEditReceiptSendFailure", 1},
		{"beginTaskDefinitionEditRoleTx", "beginTaskDefinitionEditTx", 1},
		{"beginTaskDefinitionEditRoleTx", "beginTaskDefinitionEditReceiptTx", 1},
	}
}

func taskDefinitionEditStoreProviderFunctionSymbols(
	storeDir string,
) map[string]map[string]struct{} {
	return map[string]map[string]struct{}{
		filepath.Clean(filepath.Join(storeDir, "task_definition_edit_operations.go")): taskDefinitionEditStoreMethodSet([]string{
			"scanTaskDefinitionEditOperation",
			"cloneTaskDefinitionEditOperation",
			"taskDefinitionEditDatabaseClock",
			"taskDefinitionEditValidation",
			"taskDefinitionEditConflict",
			"taskDefinitionEditIntegrity",
			"taskDefinitionEditDatabaseError",
			"taskDefinitionEditNotFound",
			"taskDefinitionEditBusy",
			"taskDefinitionEditTerminal",
			"taskDefinitionEditLeaseLost",
			"validTaskDefinitionEditScope",
			"validTaskDefinitionEditReference",
			"validateTaskDefinitionEditAcquire",
			"validateTaskDefinitionEditLease",
			"taskDefinitionEditOperationIsTerminal",
			"taskDefinitionEditScopeMatches",
			"lockTaskDefinitionEditScheduleForUpdate",
			"loadTaskDefinitionEditOperationForUpdate",
			"loadLeasedTaskDefinitionEditOperation",
			"taskDefinitionEditOriginalStatus",
			"validateTaskDefinitionEditCreationScope",
			"validateTaskDefinitionEditCreationProvenance",
			"CreateTaskDefinitionEditOperation",
			"taskDefinitionEditCreationReplayEqual",
			"sha256HexTaskDefinitionEdit",
			"LoadTaskDefinitionEditOperation",
			"CancelTaskDefinitionEditOperation",
			"ExpireTaskDefinitionEditOperation",
			"terminatePendingTaskDefinitionEdit",
			"pendingTaskDefinitionEditPristine",
			"pendingTaskDefinitionEditTerminalComplete",
			"assessTaskDefinitionEditSchedule",
			"AcquireTaskDefinitionEditOperation",
			"expireTaskDefinitionEditDuringAcquire",
			"confirmAndTerminateTaskDefinitionEditDuringAcquire",
			"acquirePendingTaskDefinitionEdit",
			"terminateAcquiredTaskDefinitionEditAssessment",
			"takeOverTaskDefinitionEdit",
			"RenewTaskDefinitionEditLease",
			"ListStaleTaskDefinitionEditTenantIDs",
			"ListStaleTaskDefinitionEditOperations",
			"QuiesceTaskDefinitionEdit",
			"AuthorizeTaskDefinitionEditRemotePhase",
			"CheckpointTaskDefinitionEditBasePaused",
			"CheckpointTaskDefinitionEditTargetApplied",
			"CheckpointTaskDefinitionEditTargetRestored",
			"checkpointTaskDefinitionEditSnapshot",
			"blockInvalidTaskDefinitionEditCheckpoint",
			"validateLoadedTaskDefinitionEditLease",
			"taskDefinitionEditBlockText",
			"taskDefinitionEditMarkerShapeExact",
			"terminateTaskDefinitionEditTx",
			"BlockTaskDefinitionEditOperation",
			"taskDefinitionEditBlockedScheduleExact",
			"SupersedeTaskDefinitionEditOperation",
			"canonicalTaskDefinitionEditResult",
			"CompleteTaskDefinitionEditOperation",
			"taskDefinitionEditCompletedScheduleExact",
			"CommitTaskDefinitionEditDefinition",
			"taskDefinitionEditPhaseHasCommittedDefinition",
			"verifyCommittedTaskDefinitionEditTx",
			"cloneOptionalTaskDefinitionEditString",
		}),
		filepath.Clean(filepath.Join(storeDir, "task_definition_edit_receipts.go")): taskDefinitionEditStoreMethodSet([]string{
			"scanTaskDefinitionEditReceipt",
			"taskDefinitionEditReceiptSelect",
			"LoadTaskDefinitionEditReceiptByOperation",
			"ListDueTaskDefinitionEditReceiptTenantIDs",
			"ListDueTaskDefinitionEditReceipts",
			"AcquireTaskDefinitionEditReceipt",
			"CheckpointTaskDefinitionEditReceiptPayload",
			"RecordTaskDefinitionEditReceiptSessionMessages",
			"MarkTaskDefinitionEditReceiptSent",
			"RecordTaskDefinitionEditReceiptSendFailure",
			"insertTaskDefinitionEditReceiptForTerminal",
			"verifyTaskDefinitionEditReceiptForTerminal",
			"loadTaskDefinitionEditReceiptForUpdate",
			"loadLeasedTaskDefinitionEditReceipt",
			"validateActiveTaskDefinitionEditReceiptLease",
			"validateAcquireTaskDefinitionEditReceiptParams",
			"validateTaskDefinitionEditReceiptLease",
			"validateTaskDefinitionEditReceiptTarget",
			"validTaskDefinitionEditReceiptReference",
			"validTaskDefinitionEditReceiptDigest",
			"validTaskDefinitionEditReceiptFailureClass",
			"taskDefinitionEditReceiptValidation",
			"taskDefinitionEditReceiptConflict",
			"taskDefinitionEditReceiptNotFound",
			"taskDefinitionEditReceiptDatabaseError",
			"taskDefinitionEditReceiptBusy",
			"taskDefinitionEditReceiptTerminal",
			"taskDefinitionEditReceiptLeaseLost",
		}),
		filepath.Clean(filepath.Join(storeDir, "task_definition_edit_tx.go")): taskDefinitionEditStoreMethodSet([]string{
			"beginTaskDefinitionEditTx",
			"beginTaskDefinitionEditReceiptTx",
			"beginTaskDefinitionEditRoleTx",
			"rollbackTaskDefinitionEditTx",
		}),
	}
}

func taskDefinitionEditStoreProductionFiles(
	repoRoot string,
	fset *token.FileSet,
) (map[string]*ast.File, error) {
	files := make(map[string]*ast.File)
	err := filepath.WalkDir(repoRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			base := entry.Name()
			if filepath.Clean(path) != filepath.Clean(repoRoot) &&
				(base == "vendor" || base == "third_party" || base == "testdata" ||
					strings.HasPrefix(base, ".") || strings.HasPrefix(base, "_")) {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		cleanPath := filepath.Clean(path)
		file, err := parser.ParseFile(fset, cleanPath, nil, parser.ParseComments)
		if err != nil {
			return err
		}
		files[cleanPath] = file
		return nil
	})
	return files, err
}

func taskDefinitionEditStoreAliases(
	files map[string]*ast.File,
	storeDir string,
	providerFunctions map[string]map[string]struct{},
	taintSymbols map[string]struct{},
) map[string]struct{} {
	type providerDeclaration struct {
		node              ast.Node
		names             []*ast.Ident
		declaredPositions map[token.Pos]struct{}
	}
	declarations := make([]providerDeclaration, 0)
	for path, file := range files {
		cleanPath := filepath.Clean(path)
		if filepath.Clean(filepath.Dir(cleanPath)) != filepath.Clean(storeDir) {
			continue
		}
		for _, declaration := range file.Decls {
			candidate := providerDeclaration{
				node:              declaration,
				declaredPositions: make(map[token.Pos]struct{}),
			}
			switch typed := declaration.(type) {
			case *ast.FuncDecl:
				if controlled := providerFunctions[cleanPath]; controlled != nil {
					if _, ok := controlled[typed.Name.Name]; ok {
						continue
					}
				}
				candidate.names = append(candidate.names, typed.Name)
				candidate.declaredPositions[typed.Name.Pos()] = struct{}{}
			case *ast.GenDecl:
				for _, specification := range typed.Specs {
					value, ok := specification.(*ast.ValueSpec)
					if !ok {
						continue
					}
					for _, name := range value.Names {
						candidate.names = append(candidate.names, name)
						candidate.declaredPositions[name.Pos()] = struct{}{}
					}
				}
			}
			if len(candidate.names) != 0 {
				declarations = append(declarations, candidate)
			}
		}
	}

	aliases := make(map[string]struct{})
	for changed := true; changed; {
		changed = false
		for _, declaration := range declarations {
			tainted := false
			ast.Inspect(declaration.node, func(node ast.Node) bool {
				if tainted {
					return false
				}
				identifier, ok := node.(*ast.Ident)
				if !ok {
					return true
				}
				if _, declarationName := declaration.declaredPositions[identifier.Pos()]; declarationName {
					return true
				}
				if _, sensitive := taintSymbols[identifier.Name]; sensitive {
					tainted = true
					return false
				}
				_, tainted = aliases[identifier.Name]
				return !tainted
			})
			if !tainted {
				continue
			}
			for _, name := range declaration.names {
				if _, exists := aliases[name.Name]; exists {
					continue
				}
				aliases[name.Name] = struct{}{}
				changed = true
			}
		}
	}
	return aliases
}

func taskDefinitionEditIsCoordinatorStoreSelector(selector *ast.SelectorExpr) bool {
	store, ok := taskDefinitionEditStoreUnparen(selector.X).(*ast.SelectorExpr)
	if !ok || store.Sel.Name != "store" {
		return false
	}
	receiver, ok := taskDefinitionEditStoreUnparen(store.X).(*ast.Ident)
	return ok && receiver.Name == "c"
}

func taskDefinitionEditStoreImports(file *ast.File, importPath string) bool {
	for _, specification := range file.Imports {
		path := strings.Trim(specification.Path.Value, "`\"")
		if path == importPath {
			return true
		}
	}
	return false
}

func taskDefinitionEditStoreUnparen(expression ast.Expr) ast.Expr {
	for {
		parenthesized, ok := expression.(*ast.ParenExpr)
		if !ok {
			return expression
		}
		expression = parenthesized.X
	}
}

func taskDefinitionEditStoreIsGoLinkname(text string) bool {
	const directive = "//go:linkname"
	text = strings.TrimSpace(text)
	if !strings.HasPrefix(text, directive) {
		return false
	}
	rest := strings.TrimPrefix(text, directive)
	return rest == "" || strings.HasPrefix(rest, " ") || strings.HasPrefix(rest, "\t")
}

func taskDefinitionEditOperationStoreMethods() map[string]struct{} {
	return taskDefinitionEditStoreMethodSet([]string{
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
	})
}

func taskDefinitionEditReceiptStoreMethods() map[string]struct{} {
	return taskDefinitionEditStoreMethodSet([]string{
		"LoadTaskDefinitionEditReceiptByOperation",
		"ListDueTaskDefinitionEditReceiptTenantIDs",
		"ListDueTaskDefinitionEditReceipts",
		"AcquireTaskDefinitionEditReceipt",
		"CheckpointTaskDefinitionEditReceiptPayload",
		"RecordTaskDefinitionEditReceiptSessionMessages",
		"MarkTaskDefinitionEditReceiptSent",
		"RecordTaskDefinitionEditReceiptSendFailure",
	})
}

func taskDefinitionEditStoreMethodSet(names []string) map[string]struct{} {
	guarded := make(map[string]struct{}, len(names))
	for _, name := range names {
		guarded[name] = struct{}{}
	}
	return guarded
}
