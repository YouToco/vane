package task

import (
	"errors"
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

// C2b3-2c activates recovery only. The composition root may construct the
// coordinator and run recovery, but it must not pass that value to an ingress
// or invoke proposal/confirmation/cancellation/receipt behavior.
func TestDefinitionEditCoordinatorWiringIsRecoveryOnly(t *testing.T) {
	t.Parallel()
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate definition edit coordinator guard")
	}
	taskDir := filepath.Clean(filepath.Dir(testFile))
	repoRoot := filepath.Clean(filepath.Dir(taskDir))
	providerPath := filepath.Join(taskDir, "definition_edit_coordinator.go")
	wiringPath := filepath.Join(repoRoot, "cmd", "server", "main.go")
	fset := token.NewFileSet()
	productionFiles, err := definitionEditCoordinatorProductionFiles(repoRoot, fset)
	if err != nil {
		t.Fatalf("parse production files: %v", err)
	}
	provider, ok := productionFiles[providerPath]
	if !ok {
		t.Fatalf("definition edit coordinator provider %s is missing", providerPath)
	}
	wiring, ok := productionFiles[wiringPath]
	if !ok {
		t.Fatalf("definition edit recovery wiring %s is missing", wiringPath)
	}
	providerFunctions := definitionEditCoordinatorProviderFunctionSymbols()
	privateGraph := definitionEditCoordinatorPrivateGraphSymbols()
	providerGraphAllowed, providerGraphViolations :=
		definitionEditCoordinatorProviderGraphReferences(
			provider,
			providerFunctions,
			privateGraph,
			definitionEditCoordinatorProviderGraphExpectations(),
			fset,
		)
	coordinatorAliases := definitionEditCoordinatorAliases(
		productionFiles,
		taskDir,
		providerPath,
		providerFunctions,
		definitionEditCoordinatorTaintSymbols(privateGraph),
	)

	allowed := make(map[token.Pos]struct{})
	providerAllowed, err := definitionEditCoordinatorInternalPublicCalls(provider)
	if err != nil {
		t.Fatalf("validate coordinator internal calls: %v", err)
	}
	for position := range providerAllowed {
		allowed[position] = struct{}{}
	}
	wiringAllowed, err := definitionEditCoordinatorRecoveryWiring(wiring)
	if err != nil {
		t.Fatalf("validate recovery-only composition root: %v", err)
	}
	for position := range wiringAllowed {
		allowed[position] = struct{}{}
	}

	violations, err := definitionEditCoordinatorProviderEscapeViolations(provider, fset)
	if err != nil {
		t.Fatalf("validate sealed coordinator provider: %v", err)
	}
	violations = append(violations, providerGraphViolations...)
	for path, file := range productionFiles {
		violations = append(violations, definitionEditCoordinatorWiringViolations(
			path, file, providerPath, allowed, fset,
		)...)
		violations = append(violations, definitionEditCoordinatorGraphBoundaryViolations(
			path,
			file,
			taskDir,
			providerPath,
			privateGraph,
			providerGraphAllowed,
			coordinatorAliases,
			fset,
		)...)
	}
	slices.Sort(violations)
	if len(violations) != 0 {
		t.Fatalf("C2b3-2c coordinator escaped recovery-only wiring:\n%s",
			strings.Join(violations, "\n"))
	}
}

// One invocation of runTaskDefinitionEditAttempt may select one of the three
// remote wrappers. Raw Scheduler calls themselves are pinned by the scheduler
// package guard, so this test locks the only production call graph into them.
func TestDefinitionEditAttemptHasOneRemoteBranchBoundary(t *testing.T) {
	t.Parallel()
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate definition edit coordinator guard")
	}
	fset := token.NewFileSet()
	providerPath := filepath.Join(filepath.Dir(testFile), "definition_edit_coordinator.go")
	file, err := parser.ParseFile(fset, providerPath, nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	wrapperCalls, attemptCall, err := definitionEditCoordinatorAttemptCalls(file)
	if err != nil {
		t.Fatal(err)
	}
	allowed := make(map[token.Pos]struct{}, len(wrapperCalls)+1)
	for position := range wrapperCalls {
		allowed[position] = struct{}{}
	}
	allowed[attemptCall] = struct{}{}
	guarded := map[string]struct{}{
		"runTaskDefinitionEditAttempt":        {},
		"runTaskDefinitionEditPauseAttempt":   {},
		"runTaskDefinitionEditApplyAttempt":   {},
		"runTaskDefinitionEditRestoreAttempt": {},
	}
	var violations []string
	ast.Inspect(file, func(node ast.Node) bool {
		selector, ok := node.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if _, watched := guarded[selector.Sel.Name]; !watched {
			return true
		}
		if _, ok := allowed[selector.Sel.Pos()]; ok {
			return true
		}
		violations = append(violations, fset.Position(selector.Sel.Pos()).String()+
			": coordinator attempt/wrapper reference is not an exact direct call "+
			selector.Sel.Name)
		return true
	})
	if len(violations) != 0 {
		slices.Sort(violations)
		t.Fatalf("definition edit attempt boundary escaped:\n%s",
			strings.Join(violations, "\n"))
	}
}

func TestDefinitionEditCoordinatorGuardCatchesValueEscape(t *testing.T) {
	t.Parallel()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "probe.go", `package probe
import taskpkg "github.com/YouToco/vane/task"
var constructor = taskpkg.NewTaskDefinitionEditCoordinator
func probe(c *taskpkg.TaskDefinitionEditCoordinator) { _ = c.Confirm }
func probeInterface(c interface {
	PrepareAndSealProposal()
	Confirm()
}) { _ = c.Confirm }
func reflectProbe(v interface{ MethodByName(string) }) {
	_ = v.MethodByName("Confirm")
}
`, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	violations := definitionEditCoordinatorWiringViolations(
		"probe.go", file, "provider.go", nil, fset,
	)
	if len(violations) != 5 {
		t.Fatalf("constructor/method/type value escaped coordinator guard: %v", violations)
	}
}

func TestDefinitionEditCoordinatorGuardRejectsFactoryToIngressMutation(t *testing.T) {
	t.Parallel()
	fset := token.NewFileSet()
	provider, err := parser.ParseFile(fset, "definition_edit_coordinator.go", `package task
type TaskDefinitionEditCoordinator struct{}
func NewTaskDefinitionEditCoordinator() *TaskDefinitionEditCoordinator {
	return &TaskDefinitionEditCoordinator{}
}
func ExportDefinitionEditCoordinator() *TaskDefinitionEditCoordinator {
	return NewTaskDefinitionEditCoordinator()
}
func (c *TaskDefinitionEditCoordinator) Confirm() {}
`, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	providerViolations, err := definitionEditCoordinatorProviderEscapeViolations(provider, fset)
	if err != nil {
		t.Fatal(err)
	}
	if len(providerViolations) == 0 {
		t.Fatal("exported coordinator factory escaped sealed-provider guard")
	}

	wiring, err := parser.ParseFile(fset, "cmd/server/main.go", `package main
import taskpkg "github.com/YouToco/vane/task"
func run() {
	ingress := taskpkg.ExportDefinitionEditCoordinator()
	ingress.Confirm()
}
`, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	wiringViolations := definitionEditCoordinatorWiringViolations(
		"cmd/server/main.go",
		wiring,
		"definition_edit_coordinator.go",
		nil,
		fset,
	)
	if !slices.ContainsFunc(wiringViolations, func(violation string) bool {
		return strings.Contains(violation, "Confirm")
	}) {
		t.Fatalf("factory-derived Confirm receiver escaped fail-closed selector guard: %v",
			wiringViolations)
	}
}

func TestDefinitionEditCoordinatorProviderRejectsAliasGlobalAndReceiverEscape(t *testing.T) {
	t.Parallel()
	fset := token.NewFileSet()
	provider, err := parser.ParseFile(fset, "definition_edit_coordinator.go", `package task
type TaskDefinitionEditCoordinator struct{}
func NewTaskDefinitionEditCoordinator() *TaskDefinitionEditCoordinator {
	return &TaskDefinitionEditCoordinator{}
}
type coordinatorAlias = TaskDefinitionEditCoordinator
var globalCoordinator any = NewTaskDefinitionEditCoordinator()
var globalCallback func()
func (c *TaskDefinitionEditCoordinator) Leak() any { return c }
func (c *TaskDefinitionEditCoordinator) Capture() {
	globalCallback = func() { c.Confirm() }
}
`, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	violations, err := definitionEditCoordinatorProviderEscapeViolations(provider, fset)
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{
		"type reference",
		"constructor reference",
		"receiver escapes",
		"escaping closure",
	} {
		if !slices.ContainsFunc(violations, func(violation string) bool {
			return strings.Contains(violation, fragment)
		}) {
			t.Fatalf("provider %s mutation escaped guard: %v", fragment, violations)
		}
	}
}

func TestDefinitionEditCoordinatorGuardRejectsBareConstructorAndPrivateExecutionBackdoor(
	t *testing.T,
) {
	t.Parallel()
	fset := token.NewFileSet()
	providerPath := filepath.Clean("/repo/task/definition_edit_coordinator.go")
	backdoorPath := filepath.Clean("/repo/task/backdoor.go")
	backdoor, err := parser.ParseFile(fset, backdoorPath, `package task
func backdoor() {
	c := NewTaskDefinitionEditCoordinator()
	c.runTaskDefinitionEditAttempt()
}
`, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	wiringViolations := definitionEditCoordinatorWiringViolations(
		backdoorPath,
		backdoor,
		providerPath,
		nil,
		fset,
	)
	if !slices.ContainsFunc(wiringViolations, func(violation string) bool {
		return strings.Contains(violation, "bare coordinator constructor")
	}) {
		t.Fatalf("same-package bare constructor escaped full-tree guard: %v",
			wiringViolations)
	}

	privateGraph := map[string]struct{}{"runTaskDefinitionEditAttempt": {}}
	aliases := definitionEditCoordinatorAliases(
		map[string]*ast.File{backdoorPath: backdoor},
		filepath.Dir(backdoorPath),
		providerPath,
		nil,
		definitionEditCoordinatorTaintSymbols(privateGraph),
	)
	if _, ok := aliases["backdoor"]; !ok {
		t.Fatal("private execution backdoor escaped transitive coordinator taint")
	}
	graphViolations := definitionEditCoordinatorGraphBoundaryViolations(
		backdoorPath,
		backdoor,
		filepath.Dir(backdoorPath),
		providerPath,
		privateGraph,
		nil,
		aliases,
		fset,
	)
	if !slices.ContainsFunc(graphViolations, func(violation string) bool {
		return strings.Contains(violation, "runTaskDefinitionEditAttempt")
	}) {
		t.Fatalf("same-package private execution call escaped graph guard: %v",
			graphViolations)
	}
}

func TestDefinitionEditCoordinatorGuardRejectsWrongPrivateExecutionEdge(t *testing.T) {
	t.Parallel()
	fset := token.NewFileSet()
	provider, err := parser.ParseFile(fset, "definition_edit_coordinator.go", `package task
type TaskDefinitionEditCoordinator struct{}
func (c *TaskDefinitionEditCoordinator) RecoverStaleOnce() {
	c.runTaskDefinitionEditAcquired()
}
func (c *TaskDefinitionEditCoordinator) runTaskDefinitionEditAcquired() {}
`, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	functions := map[string]struct{}{
		"RecoverStaleOnce":              {},
		"runTaskDefinitionEditAcquired": {},
	}
	graph := map[string]struct{}{"runTaskDefinitionEditAcquired": {}}
	_, violations := definitionEditCoordinatorProviderGraphReferences(
		provider,
		functions,
		graph,
		nil,
		fset,
	)
	if !slices.ContainsFunc(violations, func(violation string) bool {
		return strings.Contains(violation,
			"RecoverStaleOnce -> runTaskDefinitionEditAcquired")
	}) {
		t.Fatalf("wrong coordinator private execution edge escaped exact guard: %v",
			violations)
	}
}

func definitionEditCoordinatorProductionFiles(
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

func definitionEditCoordinatorProviderFunctionSymbols() map[string]struct{} {
	return definitionEditCoordinatorStringSet([]string{
		"NewTaskDefinitionEditCoordinator",
		"PrepareAndSealProposal",
		"prepareTaskDefinitionEditProposal",
		"validateTaskDefinitionEditPrepareInput",
		"sealTaskDefinitionEditProposal",
		"Confirm",
		"Cancel",
		"Expire",
		"validateDependencies",
		"acquireTaskDefinitionEdit",
		"deterministicTaskDefinitionEditStoreFailure",
		"deterministicTaskDefinitionEditAcquireFailure",
		"runTaskDefinitionEditAcquired",
		"runTaskDefinitionEditAttempt",
		"runTaskDefinitionEditPauseAttempt",
		"runTaskDefinitionEditApplyAttempt",
		"runTaskDefinitionEditRestoreAttempt",
		"authorizeTaskDefinitionEditRemote",
		"handleTaskDefinitionEditRemoteError",
		"blockTaskDefinitionEditCheckpoint",
		"reloadTaskDefinitionEditProgress",
		"reloadTaskDefinitionEditAfterStoreError",
		"taskDefinitionEditPhaseDirectlyFollows",
		"loadTaskDefinitionEditConvergent",
		"RunRecovery",
		"recoverTaskDefinitionEditsAndLog",
		"RecoverStaleOnce",
		"recoverTaskDefinitionEditOperation",
		"marshalTaskDefinitionEditSuccess",
		"decodeTaskDefinitionEditOperation",
		"validateTaskDefinitionEditOperationCheckpoints",
		"validateTaskDefinitionEditPhaseCheckpoint",
		"definitionEditCreateParams",
		"definitionEditProposalScope",
		"definitionEditLeaseScope",
		"definitionEditOperationMatchesFrozen",
		"taskDefinitionEditOperationTerminal",
		"taskDefinitionEditOutcome",
	})
}

func definitionEditCoordinatorPrivateGraphSymbols() map[string]struct{} {
	return definitionEditCoordinatorStringSet([]string{
		"prepareTaskDefinitionEditProposal",
		"sealTaskDefinitionEditProposal",
		"validateDependencies",
		"acquireTaskDefinitionEdit",
		"runTaskDefinitionEditAcquired",
		"runTaskDefinitionEditAttempt",
		"runTaskDefinitionEditPauseAttempt",
		"runTaskDefinitionEditApplyAttempt",
		"runTaskDefinitionEditRestoreAttempt",
		"authorizeTaskDefinitionEditRemote",
		"handleTaskDefinitionEditRemoteError",
		"blockTaskDefinitionEditCheckpoint",
		"reloadTaskDefinitionEditProgress",
		"reloadTaskDefinitionEditAfterStoreError",
		"loadTaskDefinitionEditConvergent",
		"recoverTaskDefinitionEditsAndLog",
		"recoverTaskDefinitionEditOperation",
	})
}

type definitionEditCoordinatorGraphExpectation struct {
	caller string
	callee string
	count  int
}

func definitionEditCoordinatorProviderGraphExpectations() []definitionEditCoordinatorGraphExpectation {
	return []definitionEditCoordinatorGraphExpectation{
		{"PrepareAndSealProposal", "validateDependencies", 1},
		{"PrepareAndSealProposal", "prepareTaskDefinitionEditProposal", 1},
		{"PrepareAndSealProposal", "sealTaskDefinitionEditProposal", 1},
		{"sealTaskDefinitionEditProposal", "validateDependencies", 1},
		{"Confirm", "validateDependencies", 1},
		{"Confirm", "acquireTaskDefinitionEdit", 1},
		{"Confirm", "runTaskDefinitionEditAcquired", 1},
		{"Confirm", "loadTaskDefinitionEditConvergent", 3},
		{"Cancel", "validateDependencies", 1},
		{"Expire", "validateDependencies", 1},
		{"runTaskDefinitionEditAcquired", "runTaskDefinitionEditAttempt", 1},
		{"runTaskDefinitionEditAttempt", "runTaskDefinitionEditPauseAttempt", 1},
		{"runTaskDefinitionEditAttempt", "runTaskDefinitionEditApplyAttempt", 1},
		{"runTaskDefinitionEditAttempt", "runTaskDefinitionEditRestoreAttempt", 1},
		{"runTaskDefinitionEditAttempt", "blockTaskDefinitionEditCheckpoint", 3},
		{"runTaskDefinitionEditAttempt", "reloadTaskDefinitionEditProgress", 2},
		{"runTaskDefinitionEditAttempt", "reloadTaskDefinitionEditAfterStoreError", 3},
		{"runTaskDefinitionEditAttempt", "loadTaskDefinitionEditConvergent", 2},
		{"runTaskDefinitionEditPauseAttempt", "authorizeTaskDefinitionEditRemote", 1},
		{"runTaskDefinitionEditPauseAttempt", "handleTaskDefinitionEditRemoteError", 1},
		{"runTaskDefinitionEditPauseAttempt", "blockTaskDefinitionEditCheckpoint", 1},
		{"runTaskDefinitionEditPauseAttempt", "reloadTaskDefinitionEditProgress", 1},
		{"runTaskDefinitionEditPauseAttempt", "reloadTaskDefinitionEditAfterStoreError", 1},
		{"runTaskDefinitionEditApplyAttempt", "authorizeTaskDefinitionEditRemote", 1},
		{"runTaskDefinitionEditApplyAttempt", "handleTaskDefinitionEditRemoteError", 1},
		{"runTaskDefinitionEditApplyAttempt", "blockTaskDefinitionEditCheckpoint", 2},
		{"runTaskDefinitionEditApplyAttempt", "reloadTaskDefinitionEditProgress", 1},
		{"runTaskDefinitionEditApplyAttempt", "reloadTaskDefinitionEditAfterStoreError", 1},
		{"runTaskDefinitionEditRestoreAttempt", "authorizeTaskDefinitionEditRemote", 1},
		{"runTaskDefinitionEditRestoreAttempt", "handleTaskDefinitionEditRemoteError", 1},
		{"runTaskDefinitionEditRestoreAttempt", "blockTaskDefinitionEditCheckpoint", 2},
		{"runTaskDefinitionEditRestoreAttempt", "reloadTaskDefinitionEditProgress", 1},
		{"runTaskDefinitionEditRestoreAttempt", "reloadTaskDefinitionEditAfterStoreError", 1},
		{"authorizeTaskDefinitionEditRemote", "blockTaskDefinitionEditCheckpoint", 1},
		{"authorizeTaskDefinitionEditRemote", "loadTaskDefinitionEditConvergent", 1},
		{"handleTaskDefinitionEditRemoteError", "reloadTaskDefinitionEditAfterStoreError", 1},
		{"handleTaskDefinitionEditRemoteError", "loadTaskDefinitionEditConvergent", 1},
		{"blockTaskDefinitionEditCheckpoint", "reloadTaskDefinitionEditAfterStoreError", 1},
		{"blockTaskDefinitionEditCheckpoint", "loadTaskDefinitionEditConvergent", 1},
		{"reloadTaskDefinitionEditAfterStoreError", "loadTaskDefinitionEditConvergent", 1},
		{"RunRecovery", "validateDependencies", 1},
		{"RunRecovery", "recoverTaskDefinitionEditsAndLog", 2},
		{"RecoverStaleOnce", "validateDependencies", 1},
		{"RecoverStaleOnce", "recoverTaskDefinitionEditOperation", 1},
		{"recoverTaskDefinitionEditOperation", "acquireTaskDefinitionEdit", 1},
		{"recoverTaskDefinitionEditOperation", "runTaskDefinitionEditAcquired", 1},
	}
}

func definitionEditCoordinatorTaintSymbols(
	privateGraph map[string]struct{},
) map[string]struct{} {
	symbols := make(map[string]struct{}, len(privateGraph)+1)
	for name := range privateGraph {
		symbols[name] = struct{}{}
	}
	symbols["NewTaskDefinitionEditCoordinator"] = struct{}{}
	return symbols
}

func definitionEditCoordinatorProviderGraphReferences(
	provider *ast.File,
	expectedFunctions map[string]struct{},
	privateGraph map[string]struct{},
	expectations []definitionEditCoordinatorGraphExpectation,
	fset *token.FileSet,
) (map[token.Pos]struct{}, []string) {
	type expectationKey struct{ caller, callee string }
	want := make(map[expectationKey]int, len(expectations))
	for _, expectation := range expectations {
		key := expectationKey{expectation.caller, expectation.callee}
		if _, duplicate := want[key]; duplicate {
			return nil, []string{fmt.Sprintf(
				"duplicate coordinator provider graph expectation %s -> %s",
				expectation.caller,
				expectation.callee,
			)}
		}
		want[key] = expectation.count
	}
	got := make(map[expectationKey]int, len(want))
	declared := make(map[string]int, len(expectedFunctions))
	allowed := make(map[token.Pos]struct{})
	var violations []string
	for _, declaration := range provider.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok {
			continue
		}
		if _, expected := expectedFunctions[function.Name.Name]; !expected {
			violations = append(violations, fset.Position(function.Name.Pos()).String()+
				": coordinator provider has unexpected function/method "+function.Name.Name)
		} else {
			declared[function.Name.Name]++
		}
		if _, sensitive := privateGraph[function.Name.Name]; sensitive {
			allowed[function.Name.Pos()] = struct{}{}
		}
	}
	for name := range expectedFunctions {
		if declared[name] != 1 {
			violations = append(violations, fmt.Sprintf(
				"coordinator provider must declare %s exactly once, got %d",
				name,
				declared[name],
			))
		}
	}
	for _, declaration := range provider.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Body == nil {
			continue
		}
		ast.Inspect(function.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			callee := definitionEditCoordinatorUnparen(call.Fun)
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
			if _, sensitive := privateGraph[identifier.Name]; !sensitive {
				return true
			}
			key := expectationKey{function.Name.Name, identifier.Name}
			if _, expected := want[key]; !expected {
				violations = append(violations, fset.Position(identifier.Pos()).String()+
					": coordinator provider graph edge is not allowlisted "+
					function.Name.Name+" -> "+identifier.Name)
				return true
			}
			got[key]++
			allowed[identifier.Pos()] = struct{}{}
			return true
		})
	}
	for expected, count := range want {
		if got[expected] != count {
			violations = append(violations, fmt.Sprintf(
				"coordinator provider graph edge %s -> %s must occur exactly %d time(s), got %d",
				expected.caller,
				expected.callee,
				count,
				got[expected],
			))
		}
	}
	ast.Inspect(provider, func(node ast.Node) bool {
		identifier, ok := node.(*ast.Ident)
		if !ok {
			return true
		}
		if _, sensitive := privateGraph[identifier.Name]; !sensitive {
			return true
		}
		if _, exact := allowed[identifier.Pos()]; exact {
			return true
		}
		violations = append(violations, fset.Position(identifier.Pos()).String()+
			": coordinator private execution symbol must only be declared or directly called "+
			identifier.Name)
		return true
	})
	return allowed, violations
}

func definitionEditCoordinatorAliases(
	files map[string]*ast.File,
	taskDir string,
	providerPath string,
	providerFunctions map[string]struct{},
	taintSymbols map[string]struct{},
) map[string]struct{} {
	type declarationInfo struct {
		node              ast.Node
		names             []*ast.Ident
		declaredPositions map[token.Pos]struct{}
	}
	declarations := make([]declarationInfo, 0)
	for path, file := range files {
		cleanPath := filepath.Clean(path)
		if filepath.Clean(filepath.Dir(cleanPath)) != filepath.Clean(taskDir) {
			continue
		}
		for _, declaration := range file.Decls {
			candidate := declarationInfo{
				node:              declaration,
				declaredPositions: make(map[token.Pos]struct{}),
			}
			switch typed := declaration.(type) {
			case *ast.FuncDecl:
				if cleanPath == filepath.Clean(providerPath) {
					if _, controlled := providerFunctions[typed.Name.Name]; controlled {
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

func definitionEditCoordinatorGraphBoundaryViolations(
	path string,
	file *ast.File,
	taskDir string,
	providerPath string,
	privateGraph map[string]struct{},
	providerAllowed map[token.Pos]struct{},
	aliases map[string]struct{},
	fset *token.FileSet,
) []string {
	cleanPath := filepath.Clean(path)
	insideTask := filepath.Clean(filepath.Dir(cleanPath)) == filepath.Clean(taskDir)
	var violations []string
	ast.Inspect(file, func(node ast.Node) bool {
		identifier, ok := node.(*ast.Ident)
		if !ok {
			return true
		}
		if _, alias := aliases[identifier.Name]; alias {
			violations = append(violations, fset.Position(identifier.Pos()).String()+
				": renamed coordinator constructor/execution wrapper "+identifier.Name)
			return true
		}
		if !insideTask {
			return true
		}
		if _, sensitive := privateGraph[identifier.Name]; !sensitive {
			return true
		}
		if cleanPath == filepath.Clean(providerPath) {
			if _, exact := providerAllowed[identifier.Pos()]; exact {
				return true
			}
		}
		violations = append(violations, fset.Position(identifier.Pos()).String()+
			": coordinator private execution graph reference outside its provider "+
			identifier.Name)
		return true
	})
	return violations
}

func definitionEditCoordinatorStringSet(names []string) map[string]struct{} {
	set := make(map[string]struct{}, len(names))
	for _, name := range names {
		set[name] = struct{}{}
	}
	return set
}

func definitionEditCoordinatorProviderEscapeViolations(
	file *ast.File,
	fset *token.FileSet,
) ([]string, error) {
	var coordinatorType *ast.TypeSpec
	var constructor *ast.FuncDecl
	for _, declaration := range file.Decls {
		switch typed := declaration.(type) {
		case *ast.GenDecl:
			if typed.Tok != token.TYPE {
				continue
			}
			for _, specification := range typed.Specs {
				typeSpec, ok := specification.(*ast.TypeSpec)
				if !ok || typeSpec.Name.Name != "TaskDefinitionEditCoordinator" {
					continue
				}
				if coordinatorType != nil {
					return nil, errors.New("duplicate TaskDefinitionEditCoordinator type declaration")
				}
				coordinatorType = typeSpec
			}
		case *ast.FuncDecl:
			if typed.Name.Name != "NewTaskDefinitionEditCoordinator" {
				continue
			}
			if constructor != nil {
				return nil, errors.New("duplicate NewTaskDefinitionEditCoordinator declaration")
			}
			constructor = typed
		}
	}
	if coordinatorType == nil || coordinatorType.Assign.IsValid() {
		return nil, errors.New("TaskDefinitionEditCoordinator must be one non-alias declaration")
	}
	if _, ok := definitionEditCoordinatorUnparen(coordinatorType.Type).(*ast.StructType); !ok {
		return nil, errors.New("TaskDefinitionEditCoordinator must remain a concrete struct")
	}
	if constructor == nil || constructor.Recv != nil {
		return nil, errors.New("NewTaskDefinitionEditCoordinator must be one free function")
	}
	if constructor.Type.Results == nil || len(constructor.Type.Results.List) != 1 ||
		len(constructor.Type.Results.List[0].Names) != 0 {
		return nil, errors.New("NewTaskDefinitionEditCoordinator must have one unnamed result")
	}
	resultType, ok := definitionEditCoordinatorPointerTypeIdent(
		constructor.Type.Results.List[0].Type,
	)
	if !ok {
		return nil, errors.New("NewTaskDefinitionEditCoordinator must return *TaskDefinitionEditCoordinator")
	}

	allowedTypes := map[token.Pos]struct{}{
		coordinatorType.Name.Pos(): {},
		resultType.Pos():           {},
	}
	allowedConstructors := map[token.Pos]struct{}{constructor.Name.Pos(): {}}
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Recv == nil {
			continue
		}
		if len(function.Recv.List) != 1 {
			return nil, fmt.Errorf("coordinator method %s must have one receiver", function.Name.Name)
		}
		receiverType, ok := definitionEditCoordinatorPointerTypeIdent(
			function.Recv.List[0].Type,
		)
		if !ok {
			return nil, fmt.Errorf("coordinator provider method %s has an unexpected receiver",
				function.Name.Name)
		}
		allowedTypes[receiverType.Pos()] = struct{}{}
	}

	constructorReturns := 0
	ast.Inspect(constructor.Body, func(node ast.Node) bool {
		statement, ok := node.(*ast.ReturnStmt)
		if !ok || len(statement.Results) != 1 {
			return true
		}
		address, ok := definitionEditCoordinatorUnparen(statement.Results[0]).(*ast.UnaryExpr)
		if !ok || address.Op != token.AND {
			return true
		}
		literal, ok := definitionEditCoordinatorUnparen(address.X).(*ast.CompositeLit)
		if !ok {
			return true
		}
		typeName, ok := definitionEditCoordinatorTypeIdent(literal.Type)
		if !ok {
			return true
		}
		constructorReturns++
		allowedTypes[typeName.Pos()] = struct{}{}
		return true
	})
	if constructorReturns != 1 {
		return nil, fmt.Errorf("NewTaskDefinitionEditCoordinator must directly return one sealed composite, got %d",
			constructorReturns)
	}

	parents := definitionEditCoordinatorParentMap(file)
	var violations []string
	ast.Inspect(file, func(node ast.Node) bool {
		identifier, ok := node.(*ast.Ident)
		if !ok {
			return true
		}
		switch identifier.Name {
		case "TaskDefinitionEditCoordinator":
			if _, allowed := allowedTypes[identifier.Pos()]; !allowed {
				violations = append(violations, fset.Position(identifier.Pos()).String()+
					": coordinator concrete type reference escapes the sealed provider surface")
			}
		case "NewTaskDefinitionEditCoordinator":
			if _, allowed := allowedConstructors[identifier.Pos()]; !allowed {
				violations = append(violations, fset.Position(identifier.Pos()).String()+
					": coordinator constructor reference outside its declaration")
			}
		}
		return true
	})

	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Recv == nil || len(function.Recv.List) != 1 ||
			len(function.Recv.List[0].Names) != 1 || function.Body == nil {
			continue
		}
		receiver := function.Recv.List[0].Names[0]
		if receiver.Obj == nil {
			return nil, fmt.Errorf("coordinator method %s receiver object is unresolved",
				function.Name.Name)
		}
		ast.Inspect(function.Body, func(node ast.Node) bool {
			identifier, ok := node.(*ast.Ident)
			if !ok || identifier.Obj != receiver.Obj {
				return true
			}
			if definitionEditCoordinatorReceiverUseIsInternal(identifier, parents) {
				if definitionEditCoordinatorReceiverCaptureEscapes(identifier, parents) {
					violations = append(violations, fset.Position(identifier.Pos()).String()+
						": coordinator receiver is captured by an escaping closure in "+function.Name.Name)
				}
				return true
			}
			violations = append(violations, fset.Position(identifier.Pos()).String()+
				": coordinator receiver escapes as a first-class value from "+function.Name.Name)
			return true
		})
	}
	slices.Sort(violations)
	return violations, nil
}

func definitionEditCoordinatorReceiverCaptureEscapes(
	receiver *ast.Ident,
	parents map[ast.Node]ast.Node,
) bool {
	var closure *ast.FuncLit
	for node := ast.Node(receiver); parents[node] != nil; node = parents[node] {
		if function, ok := parents[node].(*ast.FuncLit); ok {
			closure = function
			break
		}
	}
	if closure == nil {
		return false
	}
	parent := parents[closure]
	call, ok := parent.(*ast.CallExpr)
	return !ok || definitionEditCoordinatorUnparen(call.Fun) != closure
}

func definitionEditCoordinatorPointerTypeIdent(expression ast.Expr) (*ast.Ident, bool) {
	pointer, ok := definitionEditCoordinatorUnparen(expression).(*ast.StarExpr)
	if !ok {
		return nil, false
	}
	return definitionEditCoordinatorTypeIdent(pointer.X)
}

func definitionEditCoordinatorTypeIdent(expression ast.Expr) (*ast.Ident, bool) {
	identifier, ok := definitionEditCoordinatorUnparen(expression).(*ast.Ident)
	return identifier, ok && identifier.Name == "TaskDefinitionEditCoordinator"
}

func definitionEditCoordinatorParentMap(root ast.Node) map[ast.Node]ast.Node {
	parents := make(map[ast.Node]ast.Node)
	stack := make([]ast.Node, 0, 32)
	ast.Inspect(root, func(node ast.Node) bool {
		if node == nil {
			stack = stack[:len(stack)-1]
			return false
		}
		if len(stack) != 0 {
			parents[node] = stack[len(stack)-1]
		}
		stack = append(stack, node)
		return true
	})
	return parents
}

func definitionEditCoordinatorReceiverUseIsInternal(
	receiver *ast.Ident,
	parents map[ast.Node]ast.Node,
) bool {
	node := ast.Node(receiver)
	parent := parents[node]
	for {
		parenthesized, ok := parent.(*ast.ParenExpr)
		if !ok {
			break
		}
		node = parenthesized
		parent = parents[node]
	}
	if selector, ok := parent.(*ast.SelectorExpr); ok && selector.X == node {
		return true
	}
	binary, ok := parent.(*ast.BinaryExpr)
	if !ok || (binary.Op != token.EQL && binary.Op != token.NEQ) {
		return false
	}
	other := binary.X
	if other == node {
		other = binary.Y
	} else if binary.Y != node {
		return false
	}
	identifier, ok := definitionEditCoordinatorUnparen(other).(*ast.Ident)
	return ok && identifier.Name == "nil"
}

func definitionEditCoordinatorInternalPublicCalls(
	file *ast.File,
) (map[token.Pos]struct{}, error) {
	type expectation struct {
		method   string
		function string
	}
	expectations := []expectation{
		{"RecoverStaleOnce", "recoverTaskDefinitionEditsAndLog"},
	}
	type expectationKey struct{ method, function string }
	want := make(map[expectationKey]struct{}, len(expectations))
	for _, expected := range expectations {
		want[expectationKey(expected)] = struct{}{}
	}
	got := make(map[expectationKey]int, len(expectations))
	allowed := make(map[token.Pos]struct{}, len(expectations))
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
			selector, ok := definitionEditCoordinatorUnparen(call.Fun).(*ast.SelectorExpr)
			if !ok || !definitionEditCoordinatorReceiverIsC(selector) {
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
	for expected := range want {
		if got[expected] != 1 {
			return nil, fmt.Errorf("coordinator %s must directly call c.%s exactly once, got %d",
				expected.function, expected.method, got[expected])
		}
	}
	publicMethods := map[string]struct{}{
		"PrepareAndSealProposal": {},
		"Confirm":                {},
		"Cancel":                 {},
		"Expire":                 {},
		"RunRecovery":            {},
		"RecoverStaleOnce":       {},
	}
	var unexpected []string
	ast.Inspect(file, func(node ast.Node) bool {
		selector, ok := node.(*ast.SelectorExpr)
		if !ok || !definitionEditCoordinatorReceiverIsC(selector) {
			return true
		}
		if _, watched := publicMethods[selector.Sel.Name]; !watched {
			return true
		}
		if _, ok := allowed[selector.Sel.Pos()]; !ok {
			unexpected = append(unexpected, selector.Sel.Name)
		}
		return true
	})
	if len(unexpected) != 0 {
		slices.Sort(unexpected)
		return nil, fmt.Errorf("coordinator public methods may not call one another outside the exact recovery edge: %s",
			strings.Join(unexpected, ", "))
	}
	return allowed, nil
}

func definitionEditCoordinatorRecoveryWiring(
	file *ast.File,
) (map[token.Pos]struct{}, error) {
	mainFunction := definitionEditCoordinatorFindFunction(file, "run")
	if mainFunction == nil || mainFunction.Body == nil {
		return nil, errors.New("cmd/server run function is missing")
	}

	taskImportName, err := definitionEditCoordinatorImportName(
		file, "github.com/YouToco/vane/task",
	)
	if err != nil {
		return nil, err
	}
	type constructorSite struct {
		selector *ast.SelectorExpr
		variable *ast.Ident
	}
	var constructors []constructorSite
	ast.Inspect(mainFunction.Body, func(node ast.Node) bool {
		assignment, ok := node.(*ast.AssignStmt)
		if !ok || len(assignment.Lhs) != len(assignment.Rhs) {
			return true
		}
		for i, expression := range assignment.Rhs {
			call, ok := definitionEditCoordinatorUnparen(expression).(*ast.CallExpr)
			if !ok {
				continue
			}
			selector, ok := definitionEditCoordinatorUnparen(call.Fun).(*ast.SelectorExpr)
			if !ok || selector.Sel.Name != "NewTaskDefinitionEditCoordinator" {
				continue
			}
			pkg, ok := definitionEditCoordinatorUnparen(selector.X).(*ast.Ident)
			variable, variableOK := definitionEditCoordinatorUnparen(assignment.Lhs[i]).(*ast.Ident)
			if !ok || pkg.Name != taskImportName || !variableOK || variable.Obj == nil {
				continue
			}
			constructors = append(constructors, constructorSite{
				selector: selector,
				variable: variable,
			})
		}
		return true
	})
	if len(constructors) != 1 {
		return nil, fmt.Errorf("cmd/server run must assign exactly one task.NewTaskDefinitionEditCoordinator call, got %d",
			len(constructors))
	}
	site := constructors[0]
	allowed := map[token.Pos]struct{}{
		site.selector.Sel.Pos(): {},
	}
	allowedVariablePositions := map[token.Pos]struct{}{site.variable.Pos(): {}}
	runRecoveryCalls := 0
	ast.Inspect(mainFunction.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := definitionEditCoordinatorUnparen(call.Fun).(*ast.SelectorExpr)
		if !ok || selector.Sel.Name != "RunRecovery" {
			return true
		}
		receiver, ok := definitionEditCoordinatorUnparen(selector.X).(*ast.Ident)
		if !ok || receiver.Obj != site.variable.Obj {
			return true
		}
		runRecoveryCalls++
		allowed[selector.Sel.Pos()] = struct{}{}
		allowedVariablePositions[receiver.Pos()] = struct{}{}
		return true
	})
	if runRecoveryCalls != 1 {
		return nil, fmt.Errorf("cmd/server run must directly call definition edit RunRecovery exactly once, got %d",
			runRecoveryCalls)
	}

	var escaped []string
	ast.Inspect(mainFunction.Body, func(node ast.Node) bool {
		identifier, ok := node.(*ast.Ident)
		if !ok || identifier.Obj != site.variable.Obj {
			return true
		}
		if _, ok := allowedVariablePositions[identifier.Pos()]; !ok {
			escaped = append(escaped, identifier.Name)
		}
		return true
	})
	if len(escaped) != 0 {
		return nil, fmt.Errorf("definition edit coordinator value may only be assigned and used as the RunRecovery receiver; extra uses=%d",
			len(escaped))
	}
	return allowed, nil
}

func definitionEditCoordinatorAttemptCalls(
	file *ast.File,
) (map[token.Pos]struct{}, token.Pos, error) {
	attempt := definitionEditCoordinatorFindFunction(file, "runTaskDefinitionEditAttempt")
	loop := definitionEditCoordinatorFindFunction(file, "runTaskDefinitionEditAcquired")
	if attempt == nil || attempt.Body == nil || loop == nil || loop.Body == nil {
		return nil, token.NoPos, errors.New(
			"definition edit attempt functions are incomplete",
		)
	}
	wrapperNames := map[string]struct{}{
		"runTaskDefinitionEditPauseAttempt":   {},
		"runTaskDefinitionEditApplyAttempt":   {},
		"runTaskDefinitionEditRestoreAttempt": {},
	}
	wrapperCalls := make(map[token.Pos]struct{}, len(wrapperNames))
	counts := make(map[string]int, len(wrapperNames))
	ast.Inspect(attempt.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := definitionEditCoordinatorUnparen(call.Fun).(*ast.SelectorExpr)
		if !ok || !definitionEditCoordinatorReceiverIsC(selector) {
			return true
		}
		if _, expected := wrapperNames[selector.Sel.Name]; expected {
			counts[selector.Sel.Name]++
			wrapperCalls[selector.Sel.Pos()] = struct{}{}
		}
		return true
	})
	for name := range wrapperNames {
		if counts[name] != 1 {
			return nil, token.NoPos, fmt.Errorf("runTaskDefinitionEditAttempt must directly call c.%s exactly once, got %d",
				name, counts[name])
		}
	}

	attemptCalls := 0
	attemptPosition := token.NoPos
	ast.Inspect(loop.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := definitionEditCoordinatorUnparen(call.Fun).(*ast.SelectorExpr)
		if !ok || selector.Sel.Name != "runTaskDefinitionEditAttempt" ||
			!definitionEditCoordinatorReceiverIsC(selector) {
			return true
		}
		attemptCalls++
		attemptPosition = selector.Sel.Pos()
		return true
	})
	if attemptCalls != 1 {
		return nil, token.NoPos, fmt.Errorf("runTaskDefinitionEditAcquired must directly call c.runTaskDefinitionEditAttempt exactly once, got %d",
			attemptCalls)
	}
	return wrapperCalls, attemptPosition, nil
}

func definitionEditCoordinatorWiringViolations(
	path string,
	file *ast.File,
	providerPath string,
	allowed map[token.Pos]struct{},
	fset *token.FileSet,
) []string {
	publicSelectors := map[string]struct{}{
		"NewTaskDefinitionEditCoordinator": {},
		"PrepareAndSealProposal":           {},
		"Confirm":                          {},
		"Cancel":                           {},
		"Expire":                           {},
		"RunRecovery":                      {},
		"RecoverStaleOnce":                 {},
	}
	var violations []string
	directCalls := definitionEditCoordinatorDirectCallSelectors(file)
	unrelatedCalls := definitionEditCoordinatorProvenUnrelatedCalls(file)
	parents := definitionEditCoordinatorParentMap(file)
	for _, group := range file.Comments {
		for _, comment := range group.List {
			if definitionEditCoordinatorIsGoLinkname(comment.Text) {
				violations = append(violations,
					fset.Position(comment.Pos()).String()+": go:linkname is forbidden in production")
			}
		}
	}
	strictReflection := filepath.Clean(path) == filepath.Clean(providerPath)
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := definitionEditCoordinatorUnparen(call.Fun).(*ast.SelectorExpr)
		if !ok || (selector.Sel.Name != "Method" && selector.Sel.Name != "MethodByName") {
			return true
		}
		if strictReflection || definitionEditCoordinatorReflectsPublicMethod(call, publicSelectors) {
			violations = append(violations, fset.Position(selector.Sel.Pos()).String()+
				": dynamic reflection over definition edit coordinator is forbidden")
		}
		return true
	})
	ast.Inspect(file, func(node ast.Node) bool {
		switch typed := node.(type) {
		case *ast.SelectorExpr:
			if _, watched := publicSelectors[typed.Sel.Name]; !watched {
				return true
			}
			if _, ok := allowed[typed.Sel.Pos()]; ok {
				return true
			}
			if _, direct := directCalls[typed.Sel.Pos()]; direct {
				if _, unrelated := unrelatedCalls[typed.Sel.Pos()]; unrelated {
					return true
				}
				violations = append(violations, fset.Position(typed.Sel.Pos()).String()+
					": coordinator-like call has no exact allowed receiver source "+
					typed.Sel.Name)
				return true
			}
			if typed.Sel.Name != "NewTaskDefinitionEditCoordinator" &&
				typed.Sel.Name != "PrepareAndSealProposal" &&
				!definitionEditCoordinatorHasConcreteReceiver(typed) {
				return true
			}
			violations = append(violations, fset.Position(typed.Sel.Pos()).String()+
				": coordinator public reference is not an exact recovery-only call "+
				typed.Sel.Name)
		case *ast.Ident:
			if typed.Name == "NewTaskDefinitionEditCoordinator" {
				if selector, ok := parents[typed].(*ast.SelectorExpr); ok &&
					selector.Sel == typed {
					return true
				}
				if filepath.Clean(path) != filepath.Clean(providerPath) {
					violations = append(violations, fset.Position(typed.Pos()).String()+
						": bare coordinator constructor is forbidden outside its provider")
				}
				return true
			}
			if filepath.Clean(path) == filepath.Clean(providerPath) ||
				typed.Name != "TaskDefinitionEditCoordinator" {
				return true
			}
			violations = append(violations, fset.Position(typed.Pos()).String()+
				": concrete definition edit coordinator type must stay in its provider")
		}
		return true
	})
	return violations
}

func definitionEditCoordinatorDirectCallSelectors(file *ast.File) map[token.Pos]struct{} {
	positions := make(map[token.Pos]struct{})
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := definitionEditCoordinatorUnparen(call.Fun).(*ast.SelectorExpr)
		if ok {
			positions[selector.Sel.Pos()] = struct{}{}
		}
		return true
	})
	return positions
}

func definitionEditCoordinatorProvenUnrelatedCalls(file *ast.File) map[token.Pos]struct{} {
	genericMethods := map[string]struct{}{
		"Confirm":          {},
		"Cancel":           {},
		"Expire":           {},
		"RunRecovery":      {},
		"RecoverStaleOnce": {},
	}
	allowed := make(map[token.Pos]struct{})
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Body == nil {
			continue
		}
		var methodReceiver *ast.Ident
		methodReceiverUnrelated := false
		if function.Recv != nil && len(function.Recv.List) == 1 &&
			len(function.Recv.List[0].Names) == 1 {
			methodReceiver = function.Recv.List[0].Names[0]
			methodReceiverUnrelated = !definitionEditCoordinatorTypeExpression(
				function.Recv.List[0].Type,
			)
		}
		ast.Inspect(function.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := definitionEditCoordinatorUnparen(call.Fun).(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if _, generic := genericMethods[selector.Sel.Name]; !generic {
				return true
			}
			if definitionEditCoordinatorReceiverComesFromOtherMethod(
				selector.X, methodReceiver, methodReceiverUnrelated,
			) || definitionEditCoordinatorReceiverComesFromCreationConstructor(selector.X) {
				allowed[selector.Sel.Pos()] = struct{}{}
			}
			return true
		})
	}
	return allowed
}

func definitionEditCoordinatorReceiverComesFromOtherMethod(
	expression ast.Expr,
	methodReceiver *ast.Ident,
	methodReceiverUnrelated bool,
) bool {
	if methodReceiver == nil || methodReceiver.Obj == nil || !methodReceiverUnrelated {
		return false
	}
	root := definitionEditCoordinatorUnparen(expression)
	for {
		selector, ok := root.(*ast.SelectorExpr)
		if !ok {
			break
		}
		root = definitionEditCoordinatorUnparen(selector.X)
	}
	identifier, ok := root.(*ast.Ident)
	return ok && identifier.Obj == methodReceiver.Obj
}

func definitionEditCoordinatorReceiverComesFromCreationConstructor(
	expression ast.Expr,
) bool {
	receiver, ok := definitionEditCoordinatorUnparen(expression).(*ast.Ident)
	if !ok || receiver.Obj == nil {
		return false
	}
	assignment, ok := receiver.Obj.Decl.(*ast.AssignStmt)
	if !ok {
		return false
	}
	for i, lhs := range assignment.Lhs {
		identifier, ok := definitionEditCoordinatorUnparen(lhs).(*ast.Ident)
		if !ok || identifier.Obj != receiver.Obj || i >= len(assignment.Rhs) {
			continue
		}
		call, ok := definitionEditCoordinatorUnparen(assignment.Rhs[i]).(*ast.CallExpr)
		if !ok {
			return false
		}
		callee := definitionEditCoordinatorUnparen(call.Fun)
		switch typed := callee.(type) {
		case *ast.Ident:
			return typed.Name == "NewCreationCoordinator"
		case *ast.SelectorExpr:
			return typed.Sel.Name == "NewCreationCoordinator"
		}
	}
	return false
}

func definitionEditCoordinatorReflectsPublicMethod(
	call *ast.CallExpr,
	publicSelectors map[string]struct{},
) bool {
	if len(call.Args) != 1 {
		return false
	}
	literal, ok := definitionEditCoordinatorUnparen(call.Args[0]).(*ast.BasicLit)
	if !ok || literal.Kind != token.STRING {
		return false
	}
	name := strings.Trim(literal.Value, "`\"")
	if _, watched := publicSelectors[name]; watched {
		return true
	}
	return false
}

func definitionEditCoordinatorHasConcreteReceiver(selector *ast.SelectorExpr) bool {
	receiver, ok := definitionEditCoordinatorUnparen(selector.X).(*ast.Ident)
	if !ok || receiver.Obj == nil {
		return false
	}
	switch declaration := receiver.Obj.Decl.(type) {
	case *ast.Field:
		return definitionEditCoordinatorTypeExpression(declaration.Type)
	case *ast.ValueSpec:
		return declaration.Type != nil &&
			definitionEditCoordinatorTypeExpression(declaration.Type)
	case *ast.AssignStmt:
		for i, lhs := range declaration.Lhs {
			identifier, ok := definitionEditCoordinatorUnparen(lhs).(*ast.Ident)
			if !ok || identifier.Obj != receiver.Obj || i >= len(declaration.Rhs) {
				continue
			}
			call, ok := definitionEditCoordinatorUnparen(declaration.Rhs[i]).(*ast.CallExpr)
			if !ok {
				return false
			}
			constructor, ok := definitionEditCoordinatorUnparen(call.Fun).(*ast.SelectorExpr)
			return ok && constructor.Sel.Name == "NewTaskDefinitionEditCoordinator"
		}
	}
	return false
}

func definitionEditCoordinatorTypeExpression(expression ast.Expr) bool {
	expression = definitionEditCoordinatorUnparen(expression)
	if pointer, ok := expression.(*ast.StarExpr); ok {
		expression = definitionEditCoordinatorUnparen(pointer.X)
	}
	switch typed := expression.(type) {
	case *ast.Ident:
		return typed.Name == "TaskDefinitionEditCoordinator"
	case *ast.SelectorExpr:
		return typed.Sel.Name == "TaskDefinitionEditCoordinator"
	case *ast.InterfaceType:
		for _, field := range typed.Methods.List {
			for _, name := range field.Names {
				if name.Name == "PrepareAndSealProposal" {
					return true
				}
			}
		}
		return false
	default:
		return false
	}
}

func definitionEditCoordinatorFindFunction(file *ast.File, name string) *ast.FuncDecl {
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if ok && function.Name.Name == name {
			return function
		}
	}
	return nil
}

func definitionEditCoordinatorImportName(file *ast.File, importPath string) (string, error) {
	for _, specification := range file.Imports {
		if strings.Trim(specification.Path.Value, "`\"") != importPath {
			continue
		}
		if specification.Name == nil {
			parts := strings.Split(importPath, "/")
			return parts[len(parts)-1], nil
		}
		if specification.Name.Name == "." || specification.Name.Name == "_" {
			return "", fmt.Errorf("definition edit recovery wiring forbids %q task import",
				specification.Name.Name)
		}
		return specification.Name.Name, nil
	}
	return "", errors.New("cmd/server must import the task package")
}

func definitionEditCoordinatorReceiverIsC(selector *ast.SelectorExpr) bool {
	receiver, ok := definitionEditCoordinatorUnparen(selector.X).(*ast.Ident)
	return ok && receiver.Name == "c"
}

func definitionEditCoordinatorUnparen(expression ast.Expr) ast.Expr {
	for {
		parenthesized, ok := expression.(*ast.ParenExpr)
		if !ok {
			return expression
		}
		expression = parenthesized.X
	}
}

func definitionEditCoordinatorIsGoLinkname(text string) bool {
	const directive = "//go:linkname"
	text = strings.TrimSpace(text)
	if !strings.HasPrefix(text, directive) {
		return false
	}
	rest := strings.TrimPrefix(text, directive)
	return rest == "" || strings.HasPrefix(rest, " ") || strings.HasPrefix(rest, "\t")
}
