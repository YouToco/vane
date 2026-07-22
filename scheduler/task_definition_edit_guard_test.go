package scheduler

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

type taskDefinitionEditMethodSource struct {
	path     string
	position string
}

func TestTaskDefinitionEditPrimitivesRemainDarkAndRawOnly(t *testing.T) {
	t.Parallel()

	const providerName = "task_definition_edit.go"
	guarded := map[string]struct{}{
		"PrepareTaskDefinitionEdit":  {},
		"DescribeTaskDefinitionEdit": {},
		"PauseTaskDefinitionEdit":    {},
		"ApplyTaskDefinitionEdit":    {},
		"RestoreTaskDefinitionEdit":  {},
	}
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
	fset := token.NewFileSet()
	productionFiles, err := taskDefinitionEditProductionFiles(repoRoot, fset)
	if err != nil {
		t.Fatalf("parse c2b3-1 production files: %v", err)
	}
	provider, ok := productionFiles[providerPath]
	if !ok {
		t.Fatalf("task definition edit provider %s is not a production Go file", providerPath)
	}

	nonProviderSchedulerMethods := make(map[string]struct{})
	relatedMethods := make(map[string][]taskDefinitionEditMethodSource)
	var violations []string
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
			path, file, providerPath, guarded, fset,
		)...)
	}
	slices.Sort(violations)
	if len(violations) != 0 {
		t.Fatalf("c2b3-1 must stay dark and use one raw CAS path only:\n%s",
			strings.Join(violations, "\n"))
	}
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
			position := fset.Position(selector.Sel.Pos())
			violations = append(violations,
				position.String()+": production reference "+selector.Sel.Name)
		}
		return true
	})
	return violations
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
