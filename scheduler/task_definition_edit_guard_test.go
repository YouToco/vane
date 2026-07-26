package scheduler

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

type taskDefinitionEditMethodSource struct {
	path     string
	position string
}

func TestTaskDefinitionEditPrimitivesHaveOneRawCoordinator(t *testing.T) {
	t.Parallel()

	const providerName = "task_definition_edit.go"
	guarded := taskDefinitionEditGuardedMethods()
	forbiddenProviderSelectors := map[string]struct{}{
		"ScheduleClient": {},
		"GetHandle":      {},
		"Update":         {},
		"Pause":          {},
		"Unpause":        {},
		"CreateSchedule": {},
		"PatchSchedule":  {},
		"DeleteSchedule": {},
	}
	allowedProviderSchedulerSelectors := map[string]struct{}{
		"c":                                     {},
		"acquireTaskScheduleGate":               {},
		"buildTaskDefinitionEditRuntime":        {},
		"describeTaskDefinitionEdit":            {},
		"describeTaskDefinitionEditForRecovery": {},
		"observeTaskDefinitionEditSource":       {},
		"resolveTaskScheduleNamespaceID":        {},
		"taskDefinitionEditEnvironment":         {},
		"taskScheduleDecoder":                   {},
		"taskScheduleEnvironment":               {},
		"transitionTaskDefinitionEdit":          {},
	}

	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate task definition edit guard")
	}
	schedulerDir := filepath.Clean(filepath.Dir(testFile))
	repoRoot := filepath.Clean(filepath.Dir(schedulerDir))
	providerPath := filepath.Clean(filepath.Join(schedulerDir, providerName))
	coordinatorPath := filepath.Clean(filepath.Join(
		repoRoot, "task", "definition_edit_coordinator.go",
	))
	fset := token.NewFileSet()
	productionFiles, err := taskDefinitionEditProductionFiles(repoRoot, fset)
	if err != nil {
		t.Fatalf("parse c2b3-1 production files: %v", err)
	}
	provider, ok := productionFiles[providerPath]
	if !ok {
		t.Fatalf("task definition edit provider %s is not a production Go file", providerPath)
	}
	coordinator, ok := productionFiles[coordinatorPath]
	if !ok {
		t.Fatalf("task definition edit coordinator %s is not a production Go file", coordinatorPath)
	}
	coordinatorCalls, err := taskDefinitionEditCoordinatorRawCalls(coordinator)
	if err != nil {
		t.Fatalf("validate exact coordinator raw calls: %v", err)
	}
	providerFunctions := taskDefinitionEditProviderFunctionSymbols()
	transitionGraph := taskDefinitionEditTransitionGraphSymbols()
	providerGraphAllowed, providerGraphViolations := taskDefinitionEditProviderGraphReferences(
		provider,
		providerFunctions,
		transitionGraph,
		taskDefinitionEditProviderGraphExpectations(),
		fset,
	)
	schedulerAliases := taskDefinitionEditSchedulerAliases(
		productionFiles, schedulerDir, providerPath, providerFunctions, transitionGraph,
	)
	rawTransportViolations := taskDefinitionEditRawTransportViolations(
		productionFiles,
		taskDefinitionEditRawTransportExpectations(schedulerDir),
		fset,
	)

	nonProviderSchedulerMethods := make(map[string]struct{})
	relatedMethods := make(map[string][]taskDefinitionEditMethodSource)
	violations := append([]string(nil), providerGraphViolations...)
	violations = append(violations, rawTransportViolations...)
	for path, file := range productionFiles {
		if filepath.Clean(filepath.Dir(path)) != schedulerDir {
			continue
		}
		for _, declaration := range file.Decls {
			function, isFunction := declaration.(*ast.FuncDecl)
			if !isFunction || function.Recv == nil ||
				!taskDefinitionEditReceiverIsScheduler(function) {
				continue
			}
			name := function.Name.Name
			if filepath.Clean(path) != providerPath {
				nonProviderSchedulerMethods[name] = struct{}{}
			}
			if function.Name.IsExported() && strings.Contains(name, "TaskDefinitionEdit") {
				position := fset.Position(function.Name.Pos())
				relatedMethods[name] = append(relatedMethods[name], taskDefinitionEditMethodSource{
					path:     filepath.Clean(path),
					position: position.String(),
				})
			}
		}
	}
	violations = append(violations, taskDefinitionEditAPIViolations(
		provider, providerPath, guarded, relatedMethods, fset,
	)...)
	violations = append(violations, taskDefinitionEditProviderViolations(
		provider, guarded, nonProviderSchedulerMethods, forbiddenProviderSelectors,
		allowedProviderSchedulerSelectors, fset,
	)...)

	for path, file := range productionFiles {
		violations = append(violations, taskDefinitionEditProductionViolations(
			path, file, providerPath, guarded, coordinatorCalls, fset,
		)...)
		violations = append(violations, taskDefinitionEditGraphBoundaryViolations(
			path, file, schedulerDir, providerPath, transitionGraph,
			providerGraphAllowed, schedulerAliases, fset,
		)...)
	}
	slices.Sort(violations)
	if len(violations) != 0 {
		t.Fatalf("c2b3-2c must keep one exact raw CAS coordinator path:\n%s",
			strings.Join(violations, "\n"))
	}
}

func TestTaskDefinitionEditPrimitiveGuardCatchesMethodValues(t *testing.T) {
	t.Parallel()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "probe.go", `package probe
func probe(s interface {
	PauseTaskDefinitionEdit()
	DescribeTaskDefinitionEdit()
}) {
	_ = s.PauseTaskDefinitionEdit
	s.DescribeTaskDefinitionEdit()
}
func reflectProbe(v interface{ MethodByName(string) }) {
	_ = v.MethodByName("PauseTaskDefinitionEdit")
}
`, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	violations := taskDefinitionEditProductionViolations(
		"probe.go",
		file,
		"provider.go",
		taskDefinitionEditGuardedMethods(),
		nil,
		fset,
	)
	if len(violations) != 3 {
		t.Fatalf("raw method-value/direct calls escaped Scheduler guard: %v", violations)
	}
}

func TestTaskDefinitionEditPrimitiveGuardRejectsRenamedTransitionWrapper(t *testing.T) {
	t.Parallel()
	fset := token.NewFileSet()
	wrapperPath := filepath.Clean("/repo/scheduler/escape.go")
	providerPath := filepath.Clean("/repo/scheduler/task_definition_edit.go")
	schedulerDir := filepath.Dir(wrapperPath)
	wrapper, err := parser.ParseFile(fset, wrapperPath, `package scheduler
type Scheduler struct{}
var hiddenTransition = (*Scheduler).transitionTaskDefinitionEdit
func (s *Scheduler) AdvanceEdit() {
	s.buildTaskDefinitionEditRuntime()
	hiddenTransition(s)
}
`, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	graph := map[string]struct{}{
		"buildTaskDefinitionEditRuntime": {},
		"transitionTaskDefinitionEdit":   {},
	}
	aliases := taskDefinitionEditSchedulerAliases(
		map[string]*ast.File{wrapperPath: wrapper},
		schedulerDir,
		providerPath,
		nil,
		graph,
	)
	for _, name := range []string{"hiddenTransition", "AdvanceEdit"} {
		if _, ok := aliases[name]; !ok {
			t.Fatalf("renamed transition wrapper %s escaped transitive taint", name)
		}
	}
	wrapperViolations := taskDefinitionEditGraphBoundaryViolations(
		wrapperPath, wrapper, schedulerDir, providerPath, graph, nil, aliases, fset,
	)
	if len(wrapperViolations) == 0 {
		t.Fatal("non-provider Scheduler transition wrapper escaped graph guard")
	}

	consumerPath := filepath.Clean("/repo/cmd/server/main.go")
	consumer, err := parser.ParseFile(fset, consumerPath, `package main
import schedulerpkg "github.com/YouToco/vane/scheduler"
func run(s *schedulerpkg.Scheduler) { s.AdvanceEdit() }
`, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	consumerViolations := taskDefinitionEditGraphBoundaryViolations(
		consumerPath, consumer, schedulerDir, providerPath, graph, nil, aliases, fset,
	)
	if !slices.ContainsFunc(consumerViolations, func(violation string) bool {
		return strings.Contains(violation, "AdvanceEdit")
	}) {
		t.Fatalf("consumer call through renamed transition wrapper escaped guard: %v",
			consumerViolations)
	}
}

func TestTaskDefinitionEditPrimitiveGuardRejectsNewRawTransportCallAndAlias(t *testing.T) {
	t.Parallel()
	fset := token.NewFileSet()
	path := filepath.Clean("/repo/scheduler/escape.go")
	file, err := parser.ParseFile(fset, path, `package scheduler
import (
	"context"
	workflowservice "go.temporal.io/api/workflowservice/v1"
)
func (s *Scheduler) AdvanceDefinitionEdit(
	ctx context.Context,
	req *workflowservice.UpdateScheduleRequest,
) error {
	_, err := s.c.WorkflowService().UpdateSchedule(ctx, req)
	return err
}
func (s *Scheduler) AliasDefinitionEditTransport() {
	update := s.c.WorkflowService().UpdateSchedule
	_ = update
}
`, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	crossPath := filepath.Clean("/repo/cmd/server/backdoor.go")
	crossPackage, err := parser.ParseFile(fset, crossPath, `package main
func crossPackageRawWrite(c *Client, ctx, req any) {
	_, _ = c.WorkflowService().UpdateSchedule(ctx, req)
}
`, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	violations := taskDefinitionEditRawTransportViolations(
		map[string]*ast.File{path: file, crossPath: crossPackage},
		nil,
		fset,
	)
	for _, fragment := range []string{"AdvanceDefinitionEdit", "UpdateSchedule"} {
		if !slices.ContainsFunc(violations, func(violation string) bool {
			return strings.Contains(violation, fragment)
		}) {
			t.Fatalf("raw transport %s mutation escaped exact call-site guard: %v",
				fragment, violations)
		}
	}
	if !slices.ContainsFunc(violations, func(violation string) bool {
		return strings.Contains(violation, crossPath) &&
			strings.Contains(violation, "UpdateSchedule")
	}) {
		t.Fatalf("cross-package raw Temporal write escaped full-tree guard: %v", violations)
	}
}

func TestTaskDefinitionEditPrimitiveGuardRejectsWrongProviderGraphEdge(t *testing.T) {
	t.Parallel()
	fset := token.NewFileSet()
	provider, err := parser.ParseFile(fset, "task_definition_edit.go", `package scheduler
type Scheduler struct{}
func (s *Scheduler) PrepareTaskDefinitionEdit() {
	s.PauseTaskDefinitionEdit()
}
func (s *Scheduler) PauseTaskDefinitionEdit() {}
`, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	functions := map[string]struct{}{
		"PrepareTaskDefinitionEdit": {},
		"PauseTaskDefinitionEdit":   {},
	}
	graph := map[string]struct{}{
		"PrepareTaskDefinitionEdit": {},
		"PauseTaskDefinitionEdit":   {},
	}
	_, violations := taskDefinitionEditProviderGraphReferences(
		provider,
		functions,
		graph,
		nil,
		fset,
	)
	if !slices.ContainsFunc(violations, func(violation string) bool {
		return strings.Contains(violation,
			"PrepareTaskDefinitionEdit -> PauseTaskDefinitionEdit")
	}) {
		t.Fatalf("wrong provider graph edge escaped exact caller/callee guard: %v",
			violations)
	}
}

func taskDefinitionEditGuardedMethods() map[string]struct{} {
	return map[string]struct{}{
		"PrepareTaskDefinitionEdit":             {},
		"DescribeTaskDefinitionEdit":            {},
		"PauseTaskDefinitionEdit":               {},
		"ApplyTaskDefinitionEdit":               {},
		"RestoreTaskDefinitionEdit":             {},
		"ValidateTaskDefinitionEditEnvironment": {},
	}
}

func taskDefinitionEditProviderFunctionSymbols() map[string]struct{} {
	names := []string{
		"PrepareTaskDefinitionEdit",
		"DescribeTaskDefinitionEdit",
		"PauseTaskDefinitionEdit",
		"ApplyTaskDefinitionEdit",
		"RestoreTaskDefinitionEdit",
		"ValidateTaskDefinitionEditEnvironment",
		"transitionTaskDefinitionEdit",
		"observeTaskDefinitionEditSource",
		"buildTaskDefinitionEditRuntime",
		"taskDefinitionEditEnvironment",
		"validateTaskDefinitionEditRequest",
		"validateTaskDefinitionEditRequestIdentityV1",
		"validateTaskDefinitionEditScheduleSpecV1",
		"parseTaskDefinitionEditAnchorV1",
		"validateTaskDefinitionEditDefinition",
		"validateTaskDefinitionEditHead",
		"freezeTaskDefinitionEditBase",
		"decodeTaskDefinitionEditAction",
		"verifyTaskDefinitionEditPayloadRoundTrip",
		"validateTaskDefinitionEditBaseFingerprint",
		"validateTaskDefinitionEditPolicies",
		"classifyTaskDefinitionEditDescription",
		"taskDefinitionEditDescriptionMatches",
		"buildTaskDefinitionEditUpdateRequest",
		"taskDefinitionEditProtoSchedule",
		"validatePreparedTaskDefinitionEdit",
		"validatePreparedTaskDefinitionEditSchedule",
		"validateTaskDefinitionEditPreparedPhases",
		"validateTaskDefinitionEditActionParams",
		"taskDefinitionEditSchedulesShareDefinition",
		"taskDefinitionEditSchedulesShareExecutionEnvelope",
		"validateTaskDefinitionEditPhaseFingerprint",
		"validateTaskDefinitionEditSnapshot",
		"rejectTaskDefinitionEditUnknownFields",
		"taskDefinitionEditCoreFingerprint",
		"taskDefinitionEditCreationFingerprint",
		"taskDefinitionEditFingerprintFor",
		"taskDefinitionEditNote",
		"taskDefinitionEditOperationSeedFromPrepared",
		"digestTaskDefinitionEditOperationSeed",
		"digestTaskDefinitionEditProjectionV1",
		"digestPreparedTaskDefinitionEditSchedule",
		"digestPreparedTaskDefinitionEdit",
		"digestTaskDefinitionEditJSON",
		"clonePreparedTaskDefinitionEdit",
		"clonePreparedTaskDefinitionEditSchedule",
		"cloneTaskDefinitionEditAction",
		"clonePreparedTaskScheduleTiming",
		"cloneTaskDefinitionEditDefinition",
		"cloneTaskDefinitionEditScope",
		"taskDefinitionEditSnapshot",
		"taskDefinitionEditSnapshotFromRevision",
		"taskDefinitionEditRepresentation",
		"describeTaskDefinitionEdit",
		"classifyTaskDefinitionEditReadError",
		"isTaskDefinitionEditScheduleNotFound",
		"describeTaskDefinitionEditForRecovery",
	}
	symbols := make(map[string]struct{}, len(names))
	for _, name := range names {
		symbols[name] = struct{}{}
	}
	return symbols
}

func taskDefinitionEditTransitionGraphSymbols() map[string]struct{} {
	return map[string]struct{}{
		"PrepareTaskDefinitionEdit":             {},
		"DescribeTaskDefinitionEdit":            {},
		"PauseTaskDefinitionEdit":               {},
		"ApplyTaskDefinitionEdit":               {},
		"RestoreTaskDefinitionEdit":             {},
		"ValidateTaskDefinitionEditEnvironment": {},
		"transitionTaskDefinitionEdit":          {},
		"observeTaskDefinitionEditSource":       {},
		"buildTaskDefinitionEditRuntime":        {},
		"taskDefinitionEditEnvironment":         {},
		"describeTaskDefinitionEdit":            {},
		"describeTaskDefinitionEditForRecovery": {},
	}
}

type taskDefinitionEditGraphExpectation struct {
	caller string
	callee string
	count  int
}

func taskDefinitionEditProviderGraphExpectations() []taskDefinitionEditGraphExpectation {
	return []taskDefinitionEditGraphExpectation{
		{"ValidateTaskDefinitionEditEnvironment", "buildTaskDefinitionEditRuntime", 1},
		{"PrepareTaskDefinitionEdit", "taskDefinitionEditEnvironment", 1},
		{"PrepareTaskDefinitionEdit", "describeTaskDefinitionEdit", 1},
		{"DescribeTaskDefinitionEdit", "buildTaskDefinitionEditRuntime", 1},
		{"DescribeTaskDefinitionEdit", "describeTaskDefinitionEdit", 1},
		{"PauseTaskDefinitionEdit", "buildTaskDefinitionEditRuntime", 1},
		{"PauseTaskDefinitionEdit", "observeTaskDefinitionEditSource", 1},
		{"PauseTaskDefinitionEdit", "transitionTaskDefinitionEdit", 1},
		{"ApplyTaskDefinitionEdit", "buildTaskDefinitionEditRuntime", 1},
		{"ApplyTaskDefinitionEdit", "transitionTaskDefinitionEdit", 1},
		{"RestoreTaskDefinitionEdit", "buildTaskDefinitionEditRuntime", 1},
		{"RestoreTaskDefinitionEdit", "observeTaskDefinitionEditSource", 1},
		{"RestoreTaskDefinitionEdit", "transitionTaskDefinitionEdit", 1},
		{"transitionTaskDefinitionEdit", "describeTaskDefinitionEdit", 1},
		{"transitionTaskDefinitionEdit", "describeTaskDefinitionEditForRecovery", 1},
		{"observeTaskDefinitionEditSource", "describeTaskDefinitionEdit", 1},
		{"buildTaskDefinitionEditRuntime", "taskDefinitionEditEnvironment", 1},
		{"describeTaskDefinitionEditForRecovery", "describeTaskDefinitionEdit", 1},
	}
}

func taskDefinitionEditProviderGraphReferences(
	provider *ast.File,
	expectedFunctions map[string]struct{},
	transitionGraph map[string]struct{},
	expectations []taskDefinitionEditGraphExpectation,
	fset *token.FileSet,
) (map[token.Pos]struct{}, []string) {
	type expectationKey struct{ caller, callee string }
	want := make(map[expectationKey]int, len(expectations))
	for _, expectation := range expectations {
		key := expectationKey{expectation.caller, expectation.callee}
		if _, duplicate := want[key]; duplicate {
			return nil, []string{fmt.Sprintf(
				"duplicate Scheduler provider graph expectation %s -> %s",
				expectation.caller,
				expectation.callee,
			)}
		}
		want[key] = expectation.count
	}
	got := make(map[expectationKey]int, len(want))
	declared := make(map[string]int, len(expectedFunctions))
	allowed := make(map[token.Pos]struct{}, len(transitionGraph)*2)
	var violations []string
	for _, declaration := range provider.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok {
			continue
		}
		if _, expected := expectedFunctions[function.Name.Name]; !expected {
			violations = append(violations, fset.Position(function.Name.Pos()).String()+
				": task-definition-edit provider has an unexpected function/method "+
				function.Name.Name)
			continue
		}
		declared[function.Name.Name]++
		allowed[function.Name.Pos()] = struct{}{}
	}
	for name := range expectedFunctions {
		if declared[name] != 1 {
			violations = append(violations, fmt.Sprintf(
				"task-definition-edit provider graph symbol %s must be declared exactly once, got %d",
				name, declared[name],
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
			var callee *ast.Ident
			switch typed := taskDefinitionEditUnparen(call.Fun).(type) {
			case *ast.Ident:
				callee = typed
			case *ast.SelectorExpr:
				callee = typed.Sel
			}
			if callee == nil {
				return true
			}
			if _, sensitive := transitionGraph[callee.Name]; !sensitive {
				return true
			}
			key := expectationKey{function.Name.Name, callee.Name}
			if _, expected := want[key]; !expected {
				violations = append(violations, fset.Position(callee.Pos()).String()+
					": Scheduler provider graph edge is not allowlisted "+
					function.Name.Name+" -> "+callee.Name)
				return true
			}
			got[key]++
			allowed[callee.Pos()] = struct{}{}
			return true
		})
	}
	for expected, count := range want {
		if got[expected] != count {
			violations = append(violations, fmt.Sprintf(
				"Scheduler provider graph edge %s -> %s must occur exactly %d time(s), got %d",
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
		if _, sensitive := transitionGraph[identifier.Name]; !sensitive {
			return true
		}
		if _, ok := allowed[identifier.Pos()]; ok {
			return true
		}
		violations = append(violations, fset.Position(identifier.Pos()).String()+
			": provider graph symbol must only be declared or directly called "+identifier.Name)
		return true
	})
	return allowed, violations
}

func taskDefinitionEditSchedulerAliases(
	files map[string]*ast.File,
	schedulerDir string,
	providerPath string,
	providerFunctions map[string]struct{},
	transitionGraph map[string]struct{},
) map[string]struct{} {
	type graphDeclaration struct {
		node              ast.Node
		names             []*ast.Ident
		declaredPositions map[token.Pos]struct{}
	}
	declarations := make([]graphDeclaration, 0)
	for path, file := range files {
		if filepath.Clean(filepath.Dir(path)) != filepath.Clean(schedulerDir) {
			continue
		}
		for _, declaration := range file.Decls {
			candidate := graphDeclaration{
				node:              declaration,
				declaredPositions: make(map[token.Pos]struct{}),
			}
			switch typed := declaration.(type) {
			case *ast.FuncDecl:
				if filepath.Clean(path) == filepath.Clean(providerPath) {
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
				if _, sensitive := transitionGraph[identifier.Name]; sensitive {
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

func taskDefinitionEditGraphBoundaryViolations(
	path string,
	file *ast.File,
	schedulerDir string,
	providerPath string,
	graph map[string]struct{},
	providerAllowed map[token.Pos]struct{},
	aliases map[string]struct{},
	fset *token.FileSet,
) []string {
	cleanPath := filepath.Clean(path)
	insideScheduler := filepath.Clean(filepath.Dir(cleanPath)) == filepath.Clean(schedulerDir)
	var violations []string
	ast.Inspect(file, func(node ast.Node) bool {
		identifier, ok := node.(*ast.Ident)
		if !ok {
			return true
		}
		if _, alias := aliases[identifier.Name]; alias {
			violations = append(violations, fset.Position(identifier.Pos()).String()+
				": renamed task-definition-edit transition wrapper/alias "+identifier.Name)
			return true
		}
		if !insideScheduler || cleanPath == filepath.Clean(providerPath) {
			return true
		}
		if _, sensitive := graph[identifier.Name]; !sensitive {
			return true
		}
		if _, allowed := providerAllowed[identifier.Pos()]; allowed {
			return true
		}
		violations = append(violations, fset.Position(identifier.Pos()).String()+
			": task-definition-edit private/raw graph reference outside its provider "+
			identifier.Name)
		return true
	})
	return violations
}

func taskDefinitionEditProductionFiles(
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
		file, parseErr := parser.ParseFile(fset, cleanPath, nil, parser.ParseComments)
		if parseErr != nil {
			return parseErr
		}
		files[cleanPath] = file
		return nil
	})
	return files, err
}

func taskDefinitionEditAPIViolations(
	provider *ast.File,
	providerPath string,
	guarded map[string]struct{},
	relatedMethods map[string][]taskDefinitionEditMethodSource,
	fset *token.FileSet,
) []string {
	declared := make(map[string]struct{}, len(guarded))
	var violations []string
	for _, declaration := range provider.Decls {
		switch typed := declaration.(type) {
		case *ast.FuncDecl:
			if !typed.Name.IsExported() {
				continue
			}
			if typed.Recv != nil && taskDefinitionEditReceiverIsScheduler(typed) {
				declared[typed.Name.Name] = struct{}{}
				if _, allowed := guarded[typed.Name.Name]; !allowed {
					position := fset.Position(typed.Name.Pos())
					violations = append(violations,
						position.String()+": provider has unguarded exported Scheduler method "+typed.Name.Name)
				}
				continue
			}
			position := fset.Position(typed.Name.Pos())
			violations = append(violations,
				position.String()+": provider must not expose exported function or non-Scheduler method "+typed.Name.Name)
		case *ast.GenDecl:
			if typed.Tok != token.VAR {
				continue
			}
			for _, specification := range typed.Specs {
				value, isValue := specification.(*ast.ValueSpec)
				if !isValue {
					continue
				}
				for _, name := range value.Names {
					if !name.IsExported() {
						continue
					}
					position := fset.Position(name.Pos())
					violations = append(violations,
						position.String()+": provider must not expose exported variable "+name.Name)
				}
			}
		}
	}

	for name := range guarded {
		if _, ok := declared[name]; !ok {
			violations = append(violations, "provider is missing guarded method "+name)
		}
		locations := relatedMethods[name]
		if len(locations) != 1 || locations[0].path != providerPath {
			violations = append(violations,
				"guarded method "+name+" must be declared exactly once in provider, found "+
					taskDefinitionEditMethodPositions(locations))
		}
	}
	for name, locations := range relatedMethods {
		if _, ok := guarded[name]; ok {
			continue
		}
		violations = append(violations,
			"unguarded exported TaskDefinitionEdit Scheduler method "+name+" at "+
				taskDefinitionEditMethodPositions(locations))
	}
	return violations
}

type taskDefinitionEditRawTransportExpectation struct {
	path     string
	function string
	method   string
	count    int
}

func taskDefinitionEditRawTransportExpectations(
	schedulerDir string,
) []taskDefinitionEditRawTransportExpectation {
	return []taskDefinitionEditRawTransportExpectation{
		{
			filepath.Clean(filepath.Join(schedulerDir, "task_definition_edit.go")),
			"transitionTaskDefinitionEdit",
			"UpdateSchedule",
			1,
		},
		{
			filepath.Clean(filepath.Join(schedulerDir, "task_definition_edit.go")),
			"describeTaskDefinitionEdit",
			"DescribeSchedule",
			1,
		},
		{
			filepath.Clean(filepath.Join(schedulerDir, "task_schedule.go")),
			"ActivateTask",
			"UpdateSchedule",
			1,
		},
		{
			filepath.Clean(filepath.Join(schedulerDir, "task_schedule.go")),
			"describeTaskSchedule",
			"DescribeSchedule",
			1,
		},
		{
			filepath.Clean(filepath.Join(schedulerDir, "task_schedule.go")),
			"DeleteTask",
			"DeleteSchedule",
			1,
		},
		{
			filepath.Clean(filepath.Join(schedulerDir, "scheduler.go")),
			"applyScheduleCommandRemote",
			"PatchSchedule",
			2,
		},
		{
			filepath.Clean(filepath.Join(schedulerDir, "scheduler.go")),
			"applyScheduleCommandRemote",
			"DeleteSchedule",
			1,
		},
		{
			filepath.Clean(filepath.Join(schedulerDir, "scheduler.go")),
			"applyScheduleCommandRemote",
			"DescribeSchedule",
			1,
		},
		{
			filepath.Clean(filepath.Join(schedulerDir, "scheduler.go")),
			"schedulePausedFact",
			"DescribeSchedule",
			1,
		},
	}
}

func taskDefinitionEditRawTransportViolations(
	files map[string]*ast.File,
	expectations []taskDefinitionEditRawTransportExpectation,
	fset *token.FileSet,
) []string {
	type expectationKey struct {
		path, function, method string
	}
	want := make(map[expectationKey]int, len(expectations))
	for _, expectation := range expectations {
		key := expectationKey{
			filepath.Clean(expectation.path),
			expectation.function,
			expectation.method,
		}
		if _, duplicate := want[key]; duplicate {
			return []string{fmt.Sprintf(
				"duplicate raw Scheduler transport expectation %s in %s",
				expectation.method,
				expectation.function,
			)}
		}
		want[key] = expectation.count
	}

	got := make(map[expectationKey]int, len(want))
	allowed := make(map[token.Pos]struct{})
	var violations []string
	for path, file := range files {
		cleanPath := filepath.Clean(path)
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
				method, raw := taskDefinitionEditRawWorkflowServiceCall(call)
				if !raw {
					return true
				}
				selector := taskDefinitionEditUnparen(call.Fun).(*ast.SelectorExpr)
				key := expectationKey{cleanPath, function.Name.Name, method}
				if _, watched := want[key]; !watched &&
					method != "UpdateSchedule" && method != "DescribeSchedule" &&
					method != "PatchSchedule" && method != "DeleteSchedule" {
					return true
				}
				if _, expected := want[key]; !expected {
					violations = append(violations, fset.Position(selector.Sel.Pos()).String()+
						": raw Scheduler transport call is not an exact existing site "+
						function.Name.Name+" -> "+method)
					return true
				}
				got[key]++
				allowed[selector.Sel.Pos()] = struct{}{}
				return true
			})
		}
	}
	for expected, count := range want {
		if got[expected] != count {
			violations = append(violations, fmt.Sprintf(
				"raw Scheduler transport %s:%s -> %s must occur exactly %d time(s), got %d",
				expected.path,
				expected.function,
				expected.method,
				count,
				got[expected],
			))
		}
	}
	for _, file := range files {
		ast.Inspect(file, func(node ast.Node) bool {
			selector, ok := node.(*ast.SelectorExpr)
			if !ok || (selector.Sel.Name != "UpdateSchedule" &&
				selector.Sel.Name != "DescribeSchedule") {
				return true
			}
			if _, exact := allowed[selector.Sel.Pos()]; exact {
				return true
			}
			violations = append(violations, fset.Position(selector.Sel.Pos()).String()+
				": raw Scheduler transport selector/method value is not allowlisted "+
				selector.Sel.Name)
			return true
		})
	}
	return violations
}

func taskDefinitionEditProviderViolations(
	provider *ast.File,
	guarded map[string]struct{},
	nonProviderSchedulerMethods map[string]struct{},
	forbiddenSelectors map[string]struct{},
	allowedSchedulerSelectors map[string]struct{},
	fset *token.FileSet,
) []string {
	type rawUsage struct {
		workflowServiceSelectors int
		describeSelectors        int
		updateSelectors          int
		describeCalls            int
		updateCalls              int
	}
	var raw rawUsage
	var violations []string
	ast.Inspect(provider, func(node ast.Node) bool {
		switch typed := node.(type) {
		case *ast.CallExpr:
			method, ok := taskDefinitionEditRawWorkflowServiceCall(typed)
			if !ok {
				return true
			}
			switch method {
			case "DescribeSchedule":
				raw.describeCalls++
			case "UpdateSchedule":
				raw.updateCalls++
			default:
				position := fset.Position(typed.Fun.Pos())
				violations = append(violations,
					position.String()+": raw WorkflowService call is not allowed: "+method)
			}
		case *ast.SelectorExpr:
			name := typed.Sel.Name
			switch name {
			case "WorkflowService":
				raw.workflowServiceSelectors++
			case "DescribeSchedule":
				raw.describeSelectors++
			case "UpdateSchedule":
				raw.updateSelectors++
			}
			if _, forbidden := forbiddenSelectors[name]; forbidden {
				position := fset.Position(typed.Sel.Pos())
				violations = append(violations,
					position.String()+": forbidden provider selector "+name)
			}
			if _, outsideProvider := nonProviderSchedulerMethods[name]; outsideProvider {
				if _, allowed := allowedSchedulerSelectors[name]; allowed {
					return true
				}
				position := fset.Position(typed.Sel.Pos())
				violations = append(violations,
					position.String()+": provider must not call non-allowlisted Scheduler method "+name)
			}
		}
		return true
	})
	for _, declaration := range provider.Decls {
		function, isFunction := declaration.(*ast.FuncDecl)
		if !isFunction || function.Body == nil || !taskDefinitionEditReceiverIsScheduler(function) ||
			len(function.Recv.List[0].Names) != 1 {
			continue
		}
		receiverName := function.Recv.List[0].Names[0].Name
		ast.Inspect(function.Body, func(node ast.Node) bool {
			selector, ok := node.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if receiver, ok := taskDefinitionEditUnparen(selector.X).(*ast.Ident); ok &&
				receiver.Name == receiverName {
				if _, allowed := allowedSchedulerSelectors[selector.Sel.Name]; allowed {
					return true
				}
				if _, allowed := guarded[selector.Sel.Name]; allowed {
					return true
				}
				position := fset.Position(selector.Sel.Pos())
				violations = append(violations,
					position.String()+": provider Scheduler selector is not allowlisted: "+selector.Sel.Name)
				return true
			}
			client, ok := taskDefinitionEditUnparen(selector.X).(*ast.SelectorExpr)
			if !ok || client.Sel.Name != "c" {
				return true
			}
			receiver, ok := taskDefinitionEditUnparen(client.X).(*ast.Ident)
			if !ok || receiver.Name != receiverName || selector.Sel.Name == "WorkflowService" {
				return true
			}
			position := fset.Position(selector.Sel.Pos())
			violations = append(violations,
				position.String()+": provider may only reach Temporal through s.c.WorkflowService")
			return true
		})
	}

	if raw.describeCalls != 1 {
		violations = append(violations, "provider must contain exactly one direct s.c.WorkflowService().DescribeSchedule call")
	}
	if raw.updateCalls != 1 {
		violations = append(violations, "provider must contain exactly one direct s.c.WorkflowService().UpdateSchedule call")
	}
	if raw.describeSelectors != raw.describeCalls {
		violations = append(violations, "DescribeSchedule must only appear as the pinned direct raw call")
	}
	if raw.updateSelectors != raw.updateCalls {
		violations = append(violations, "UpdateSchedule must only appear as the pinned direct raw call")
	}
	if raw.workflowServiceSelectors != raw.describeCalls+raw.updateCalls {
		violations = append(violations, "WorkflowService must only be used by the pinned raw DescribeSchedule and UpdateSchedule calls")
	}
	return violations
}

func taskDefinitionEditProductionViolations(
	path string,
	file *ast.File,
	providerPath string,
	guarded map[string]struct{},
	coordinatorCalls map[token.Pos]struct{},
	fset *token.FileSet,
) []string {
	var violations []string
	importsReflect := taskDefinitionEditImports(file, "reflect")
	calledMethodSelectors := make(map[token.Pos]struct{})
	for _, group := range file.Comments {
		for _, comment := range group.List {
			if !taskDefinitionEditIsGoLinkname(comment.Text) {
				continue
			}
			position := fset.Position(comment.Pos())
			violations = append(violations, position.String()+": go:linkname is forbidden in production")
		}
	}

	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := taskDefinitionEditUnparen(call.Fun).(*ast.SelectorExpr)
		if ok && selector.Sel.Name == "Method" {
			calledMethodSelectors[selector.Sel.Pos()] = struct{}{}
			position := fset.Position(selector.Sel.Pos())
			violations = append(violations,
				position.String()+": dynamic method reflection is forbidden in production")
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
			position := fset.Position(selector.Sel.Pos())
			violations = append(violations,
				position.String()+": dynamic method reflection is forbidden in production")
		}
		return true
	})

	skippedBodies := make(map[*ast.BlockStmt]struct{}, len(guarded))
	if filepath.Clean(path) == providerPath {
		for _, declaration := range file.Decls {
			function, isFunction := declaration.(*ast.FuncDecl)
			if !isFunction || function.Body == nil ||
				!taskDefinitionEditReceiverIsScheduler(function) {
				continue
			}
			if _, skip := guarded[function.Name.Name]; skip {
				skippedBodies[function.Body] = struct{}{}
			}
		}
	}
	ast.Inspect(file, func(node ast.Node) bool {
		if body, isBody := node.(*ast.BlockStmt); isBody {
			if _, skip := skippedBodies[body]; skip {
				return false
			}
		}
		selector, ok := node.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if _, watched := guarded[selector.Sel.Name]; watched {
			if _, allowed := coordinatorCalls[selector.Sel.Pos()]; allowed {
				return true
			}
			position := fset.Position(selector.Sel.Pos())
			violations = append(violations,
				position.String()+": production reference "+selector.Sel.Name)
		}
		return true
	})
	return violations
}

func taskDefinitionEditCoordinatorRawCalls(
	file *ast.File,
) (map[token.Pos]struct{}, error) {
	type expectation struct {
		method   string
		function string
	}
	expectations := []expectation{
		{"ValidateTaskDefinitionEditEnvironment", "ValidateRuntimeEnvironment"},
		{"PrepareTaskDefinitionEdit", "prepareTaskDefinitionEditProposal"},
		{"PauseTaskDefinitionEdit", "runTaskDefinitionEditPauseAttempt"},
		{"ApplyTaskDefinitionEdit", "runTaskDefinitionEditApplyAttempt"},
		{"RestoreTaskDefinitionEdit", "runTaskDefinitionEditRestoreAttempt"},
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
			selector, ok := taskDefinitionEditUnparen(call.Fun).(*ast.SelectorExpr)
			if !ok || !taskDefinitionEditIsCoordinatorSchedulerSelector(selector) {
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
			return nil, fmt.Errorf("coordinator %s must directly call c.scheduler.%s exactly once, got %d",
				expected.function, expected.method, got[expected])
		}
	}
	return allowed, nil
}

func taskDefinitionEditIsCoordinatorSchedulerSelector(selector *ast.SelectorExpr) bool {
	schedules, ok := taskDefinitionEditUnparen(selector.X).(*ast.SelectorExpr)
	if !ok || schedules.Sel.Name != "scheduler" {
		return false
	}
	receiver, ok := taskDefinitionEditUnparen(schedules.X).(*ast.Ident)
	return ok && receiver.Name == "c"
}

func taskDefinitionEditMethodPositions(sources []taskDefinitionEditMethodSource) string {
	positions := make([]string, 0, len(sources))
	for _, source := range sources {
		positions = append(positions, source.position)
	}
	slices.Sort(positions)
	return strings.Join(positions, ", ")
}

func taskDefinitionEditImports(file *ast.File, importPath string) bool {
	for _, specification := range file.Imports {
		path := strings.Trim(specification.Path.Value, "`\"")
		if path == importPath {
			return true
		}
	}
	return false
}

func taskDefinitionEditRawWorkflowServiceCall(call *ast.CallExpr) (string, bool) {
	method, ok := taskDefinitionEditUnparen(call.Fun).(*ast.SelectorExpr)
	if !ok {
		return "", false
	}
	serviceCall, ok := taskDefinitionEditUnparen(method.X).(*ast.CallExpr)
	if !ok || len(serviceCall.Args) != 0 || serviceCall.Ellipsis.IsValid() {
		return "", false
	}
	workflowService, ok := taskDefinitionEditUnparen(serviceCall.Fun).(*ast.SelectorExpr)
	if !ok || workflowService.Sel.Name != "WorkflowService" {
		return "", false
	}
	client, ok := taskDefinitionEditUnparen(workflowService.X).(*ast.SelectorExpr)
	if !ok || client.Sel.Name != "c" {
		return "", false
	}
	receiver, ok := taskDefinitionEditUnparen(client.X).(*ast.Ident)
	if !ok || receiver.Name != "s" {
		return "", false
	}
	return method.Sel.Name, true
}

func taskDefinitionEditUnparen(expression ast.Expr) ast.Expr {
	for {
		parenthesized, ok := expression.(*ast.ParenExpr)
		if !ok {
			return expression
		}
		expression = parenthesized.X
	}
}

func taskDefinitionEditIsGoLinkname(text string) bool {
	const directive = "//go:linkname"
	text = strings.TrimSpace(text)
	if !strings.HasPrefix(text, directive) {
		return false
	}
	rest := strings.TrimPrefix(text, directive)
	return rest == "" || strings.HasPrefix(rest, " ") || strings.HasPrefix(rest, "\t")
}

func taskDefinitionEditReceiverIsScheduler(function *ast.FuncDecl) bool {
	if function.Recv == nil || len(function.Recv.List) != 1 {
		return false
	}
	receiver := function.Recv.List[0].Type
	if pointer, ok := receiver.(*ast.StarExpr); ok {
		receiver = pointer.X
	}
	identifier, ok := receiver.(*ast.Ident)
	return ok && identifier.Name == "Scheduler"
}
