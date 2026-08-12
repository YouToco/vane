package main

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

func TestAgentContinuationRuntimeIsRemoved(
	t *testing.T,
) {
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate continuation wiring test")
	}
	mainPath := filepath.Join(filepath.Dir(testFile), "main.go")
	raw, err := os.ReadFile(mainPath)
	if err != nil {
		t.Fatal(err)
	}
	source := string(raw)
	for _, forbidden := range []string{
		"agentcontinuation", "continuationDispatcher",
		"agent_session_fact_outbox",
	} {
		if strings.Contains(source, forbidden) {
			t.Fatalf("production server retains Agent continuation runtime %q", forbidden)
		}
	}
	file, err := parser.ParseFile(token.NewFileSet(), mainPath, raw, 0)
	if err != nil {
		t.Fatal(err)
	}
	newCalls, exactBindings := feedbackNotifierAssemblyCounts(file)
	if newCalls != 1 || exactBindings != 1 {
		t.Fatalf(
			"feedback.New calls/exact Notifier=agentLoop bindings=%d/%d, want 1/1",
			newCalls,
			exactBindings,
		)
	}
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(testFile), "..", ".."))
	feedbackSource, err := os.ReadFile(filepath.Join(repoRoot, "store", "feedbacks.go"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(feedbackSource), "agent_session_fact_outbox") {
		t.Fatal("new feedback still produces retired Agent session facts")
	}
	continuationSources, err := filepath.Glob(filepath.Join(
		repoRoot, "agentcontinuation", "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	if len(continuationSources) != 0 {
		t.Fatalf("retired agentcontinuation sources remain: %v", continuationSources)
	}
}

func TestFeedbackSessionProjectionCallBoundary(t *testing.T) {
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate continuation wiring test")
	}
	feedbackDir := filepath.Clean(
		filepath.Join(filepath.Dir(testFile), "..", "..", "feedback"),
	)
	packages, err := parser.ParseDir(
		token.NewFileSet(),
		feedbackDir,
		func(info os.FileInfo) bool {
			return !strings.HasSuffix(info.Name(), "_test.go")
		},
		0,
	)
	if err != nil {
		t.Fatal(err)
	}
	feedbackPackage, ok := packages["feedback"]
	if !ok {
		t.Fatal("feedback package not found")
	}
	files := make([]*ast.File, 0, len(feedbackPackage.Files))
	for _, file := range feedbackPackage.Files {
		files = append(files, file)
	}
	if violations := feedbackSessionBoundaryViolations(files); len(violations) != 0 {
		t.Fatalf(
			"feedback session projection boundary escaped:\n%s",
			strings.Join(violations, "\n"),
		)
	}
}

func TestFeedbackSessionProjectionGuardRejectsMutations(t *testing.T) {
	mutations := map[string]struct {
		source string
		want   string
	}{
		"same-name receiver": {
			want: "Service.handleAttitude cutoff",
			source: `package feedback
type Store struct{}
type Other struct{}
type Service struct { deps struct { Store Store } }
func (Store) InsertFeedbackWithSessionCutoff() {}
func (Store) InsertDeepDiveFeedback() {}
func (Other) InsertFeedbackWithSessionCutoff() {}
func (s *Service) notifyClick() {}
func (s *Service) handleAttitude() {
	var other Other
	other.InsertFeedbackWithSessionCutoff()
}
func (s *Service) HandleReasonSubmit() {
	s.deps.Store.InsertFeedbackWithSessionCutoff()
}
func (s *Service) generateDeepDive() {
	s.deps.Store.InsertDeepDiveFeedback()
	s.notifyClick()
}`,
		},
		"method-value notify": {
			want: "Service.handleAttitude reaches Service.notifyClick",
			source: `package feedback
type Store struct{}
type Service struct { deps struct { Store Store } }
func (Store) InsertFeedbackWithSessionCutoff() {}
func (Store) InsertDeepDiveFeedback() {}
func (s *Service) notifyClick() {}
func (s *Service) handleAttitude() {
	s.deps.Store.InsertFeedbackWithSessionCutoff()
	callback := s.notifyClick
	_ = callback
}
func (s *Service) HandleReasonSubmit() {
	s.deps.Store.InsertFeedbackWithSessionCutoff()
}
func (s *Service) generateDeepDive() {
	s.deps.Store.InsertDeepDiveFeedback()
	s.notifyClick()
}`,
		},
		"reachable wrapper": {
			want: "package.hiddenNotify reaches Service.notifyClick",
			source: `package feedback
type Store struct{}
type Service struct { deps struct { Store Store } }
func (Store) InsertFeedbackWithSessionCutoff() {}
func (Store) InsertDeepDiveFeedback() {}
func (s *Service) notifyClick() {}
func hiddenNotify(s *Service) { s.notifyClick() }
func (s *Service) handleAttitude() {
	s.deps.Store.InsertFeedbackWithSessionCutoff()
	hiddenNotify(s)
}
func (s *Service) HandleReasonSubmit() {
	s.deps.Store.InsertFeedbackWithSessionCutoff()
}
func (s *Service) generateDeepDive() {
	s.deps.Store.InsertDeepDiveFeedback()
	s.notifyClick()
}`,
		},
		"package helper function value": {
			want: "Service.handleAttitude has forbidden package helper function value package.hiddenNotify",
			source: `package feedback
type Store struct{}
type Service struct { deps struct { Store Store } }
func (Store) InsertFeedbackWithSessionCutoff() {}
func (Store) InsertDeepDiveFeedback() {}
func (s *Service) notifyClick() {}
func hiddenNotify(s *Service) { s.notifyClick() }
func (s *Service) handleAttitude() {
	s.deps.Store.InsertFeedbackWithSessionCutoff()
	callback := hiddenNotify
	callback(s)
}
func (s *Service) HandleReasonSubmit() {
	s.deps.Store.InsertFeedbackWithSessionCutoff()
}
func (s *Service) generateDeepDive() {
	s.deps.Store.InsertDeepDiveFeedback()
	s.notifyClick()
}`,
		},
	}
	for name, mutation := range mutations {
		t.Run(name, func(t *testing.T) {
			file, err := parser.ParseFile(
				token.NewFileSet(),
				"feedback_mutation.go",
				mutation.source,
				0,
			)
			if err != nil {
				t.Fatal(err)
			}
			violations := feedbackSessionBoundaryViolations(
				[]*ast.File{file},
			)
			if len(violations) != 1 ||
				!strings.Contains(violations[0], mutation.want) {
				t.Fatalf(
					"mutation violations=%v, want exactly %q",
					violations,
					mutation.want,
				)
			}
		})
	}
}

func TestFeedbackSessionProjectionGuardAllowsUnrelatedNamesAndLocalShadows(
	t *testing.T,
) {
	allowed := map[string]string{
		"local package-helper shadow": `package feedback
type Store struct{}
type Service struct { deps struct { Store Store } }
func (Store) InsertFeedbackWithSessionCutoff() {}
func (Store) InsertDeepDiveFeedback() {}
func (s *Service) notifyClick() {}
func hiddenNotify(s *Service) { s.notifyClick() }
func (s *Service) handleAttitude() {
	s.deps.Store.InsertFeedbackWithSessionCutoff()
	hiddenNotify := func(*Service) {}
	hiddenNotify(s)
}
func (s *Service) HandleReasonSubmit() {
	s.deps.Store.InsertFeedbackWithSessionCutoff()
}
func (s *Service) generateDeepDive() {
	s.deps.Store.InsertDeepDiveFeedback()
	s.notifyClick()
}`,
		"unrelated same-name receiver": `package feedback
type Store struct{}
type Other struct{}
type Service struct { deps struct { Store Store } }
func (Store) InsertFeedbackWithSessionCutoff() {}
func (Store) InsertDeepDiveFeedback() {}
func (Other) InsertFeedbackWithSessionCutoff() {}
func (s *Service) notifyClick() {}
func (s *Service) handleAttitude() {
	s.deps.Store.InsertFeedbackWithSessionCutoff()
	var other Other
	other.InsertFeedbackWithSessionCutoff()
}
func (s *Service) HandleReasonSubmit() {
	s.deps.Store.InsertFeedbackWithSessionCutoff()
}
func (s *Service) generateDeepDive() {
	s.deps.Store.InsertDeepDiveFeedback()
	s.notifyClick()
}`,
	}
	for name, source := range allowed {
		t.Run(name, func(t *testing.T) {
			file, err := parser.ParseFile(
				token.NewFileSet(), "feedback_allowed.go", source, 0,
			)
			if err != nil {
				t.Fatal(err)
			}
			if violations := feedbackSessionBoundaryViolations(
				[]*ast.File{file},
			); len(violations) != 0 {
				t.Fatalf("valid feedback boundary rejected: %v", violations)
			}
		})
	}
}

func feedbackNotifierAssemblyCounts(file *ast.File) (int, int) {
	var newCalls int
	var exactBindings int
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok || !feedbackSelectorHasPath(call.Fun, "feedback", "New") {
			return true
		}
		newCalls++
		if len(call.Args) == 0 {
			return true
		}
		deps, ok := feedbackUnparen(call.Args[0]).(*ast.CompositeLit)
		if !ok ||
			!feedbackSelectorHasPath(deps.Type, "feedback", "Deps") {
			return true
		}
		notifierFields := 0
		exactNotifierFields := 0
		for _, element := range deps.Elts {
			keyValue, ok := element.(*ast.KeyValueExpr)
			if !ok {
				continue
			}
			key, ok := keyValue.Key.(*ast.Ident)
			if !ok || key.Name != "Notifier" {
				continue
			}
			notifierFields++
			value, ok := feedbackUnparen(keyValue.Value).(*ast.Ident)
			if ok && value.Name == "agentLoop" {
				exactNotifierFields++
			}
		}
		if notifierFields == 1 && exactNotifierFields == 1 {
			exactBindings++
		}
		return true
	})
	return newCalls, exactBindings
}

type feedbackBoundaryNode struct {
	key            string
	function       *ast.FuncDecl
	receiverObject *ast.Object
	serviceObjects map[*ast.Object]struct{}
}

type feedbackSelectorStats struct {
	references int
	direct     int
}

func feedbackSessionBoundaryViolations(files []*ast.File) []string {
	serviceMethods := make(map[string]*feedbackBoundaryNode)
	packageFunctions := make(map[string]*feedbackBoundaryNode)
	packageFunctionObjects := make(
		map[*ast.Object]*feedbackBoundaryNode,
	)
	var violations []string
	for _, file := range files {
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			receiverObject, serviceMethod := feedbackServiceReceiver(function)
			node := &feedbackBoundaryNode{
				function:       function,
				receiverObject: receiverObject,
				serviceObjects: feedbackServiceObjects(function),
			}
			if serviceMethod {
				node.key = "Service." + function.Name.Name
				if _, exists := serviceMethods[function.Name.Name]; exists {
					violations = append(
						violations,
						"duplicate Service method "+function.Name.Name,
					)
				}
				serviceMethods[function.Name.Name] = node
				continue
			}
			if function.Recv == nil {
				node.key = "package." + function.Name.Name
				packageFunctions[function.Name.Name] = node
				if function.Name.Obj != nil {
					packageFunctionObjects[function.Name.Obj] = node
				}
			}
		}
	}

	ordinary := make([]*feedbackBoundaryNode, 0, 2)
	for _, name := range []string{"handleAttitude", "HandleReasonSubmit"} {
		node := serviceMethods[name]
		if node == nil {
			violations = append(violations, "missing Service."+name)
			continue
		}
		ordinary = append(ordinary, node)
		stats := feedbackTargetSelectorStats(
			node,
			"InsertFeedbackWithSessionCutoff",
			"deps", "Store", "InsertFeedbackWithSessionCutoff",
		)
		if stats != (feedbackSelectorStats{references: 1, direct: 1}) {
			violations = append(violations, fmt.Sprintf(
				"%s cutoff target references/direct=%d/%d, want 1/1",
				node.key, stats.references, stats.direct,
			))
		}
	}

	deepDive := serviceMethods["generateDeepDive"]
	if deepDive == nil {
		violations = append(violations, "missing Service.generateDeepDive")
	} else {
		insertStats := feedbackTargetSelectorStats(
			deepDive,
			"InsertDeepDiveFeedback",
			"deps", "Store", "InsertDeepDiveFeedback",
		)
		if insertStats !=
			(feedbackSelectorStats{references: 1, direct: 1}) {
			violations = append(violations, fmt.Sprintf(
				"%s deep-dive insert target references/direct=%d/%d, want 1/1",
				deepDive.key,
				insertStats.references,
				insertStats.direct,
			))
		}
		notifyStats := feedbackTargetSelectorStats(
			deepDive,
			"notifyClick",
			"notifyClick",
		)
		if notifyStats !=
			(feedbackSelectorStats{references: 1, direct: 1}) {
			violations = append(violations, fmt.Sprintf(
				"%s notify target references/direct=%d/%d, want 1/1",
				deepDive.key,
				notifyStats.references,
				notifyStats.direct,
			))
		}
		cutoffStats := feedbackTargetSelectorStats(
			deepDive,
			"InsertFeedbackWithSessionCutoff",
			"deps", "Store", "InsertFeedbackWithSessionCutoff",
		)
		if cutoffStats.references != 0 {
			violations = append(violations, fmt.Sprintf(
				"%s cutoff target references=%d, want 0",
				deepDive.key,
				cutoffStats.references,
			))
		}
	}
	violations = append(
		violations,
		feedbackReachableNotifyViolations(
			ordinary,
			serviceMethods,
			packageFunctions,
			packageFunctionObjects,
		)...,
	)
	return violations
}

func feedbackTargetSelectorStats(
	node *feedbackBoundaryNode,
	method string,
	pathAfterReceiver ...string,
) feedbackSelectorStats {
	var stats feedbackSelectorStats
	ast.Inspect(node.function.Body, func(candidate ast.Node) bool {
		selector, ok := candidate.(*ast.SelectorExpr)
		if !ok || selector.Sel.Name != method ||
			!feedbackSelectorTargetsReceiver(
				selector,
				node.receiverObject,
				pathAfterReceiver...,
			) {
			return true
		}
		stats.references++
		return true
	})
	ast.Inspect(node.function.Body, func(candidate ast.Node) bool {
		call, ok := candidate.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := feedbackUnparen(call.Fun).(*ast.SelectorExpr)
		if ok && selector.Sel.Name == method &&
			feedbackSelectorTargetsReceiver(
				selector,
				node.receiverObject,
				pathAfterReceiver...,
			) {
			stats.direct++
		}
		return true
	})
	return stats
}

func feedbackReachableNotifyViolations(
	start []*feedbackBoundaryNode,
	serviceMethods map[string]*feedbackBoundaryNode,
	packageFunctions map[string]*feedbackBoundaryNode,
	packageFunctionObjects map[*ast.Object]*feedbackBoundaryNode,
) []string {
	queue := slices.Clone(start)
	visited := make(map[string]struct{}, len(queue))
	var violations []string
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		if _, seen := visited[node.key]; seen {
			continue
		}
		visited[node.key] = struct{}{}
		ast.Inspect(node.function.Body, func(candidate ast.Node) bool {
			selector, ok := candidate.(*ast.SelectorExpr)
			if !ok || !feedbackSelectorTargetsAnyService(
				selector,
				node.serviceObjects,
			) {
				return true
			}
			if selector.Sel.Name == "notifyClick" {
				violations = append(violations, fmt.Sprintf(
					"%s reaches Service.notifyClick by selector or method value",
					node.key,
				))
			}
			if next := serviceMethods[selector.Sel.Name]; next != nil {
				queue = append(queue, next)
			}
			return true
		})
		directPackageCalls := make(map[token.Pos]struct{})
		parents := feedbackASTParents(node.function.Body)
		ast.Inspect(node.function.Body, func(candidate ast.Node) bool {
			call, ok := candidate.(*ast.CallExpr)
			if !ok {
				return true
			}
			identifier, ok := feedbackUnparen(call.Fun).(*ast.Ident)
			if ok {
				directPackageCalls[identifier.Pos()] = struct{}{}
			}
			return true
		})
		ast.Inspect(node.function.Body, func(candidate ast.Node) bool {
			identifier, ok := candidate.(*ast.Ident)
			if !ok {
				return true
			}
			switch parent := parents[identifier].(type) {
			case *ast.SelectorExpr:
				if parent.Sel == identifier {
					return true
				}
			case *ast.KeyValueExpr:
				if parent.Key == identifier {
					return true
				}
			}
			next := packageFunctionObjects[identifier.Obj]
			if next == nil && identifier.Obj == nil {
				// parser resolves local shadows within their file. A nil
				// object with an exact package declaration name is the
				// cross-file package reference that parser.ParseDir leaves
				// unresolved.
				next = packageFunctions[identifier.Name]
			}
			if next == nil {
				return true
			}
			if _, direct := directPackageCalls[identifier.Pos()]; direct {
				queue = append(queue, next)
				return true
			}
			violations = append(violations, fmt.Sprintf(
				"%s has forbidden package helper function value %s",
				node.key,
				next.key,
			))
			return true
		})
	}
	return violations
}

func feedbackASTParents(root ast.Node) map[ast.Node]ast.Node {
	parents := make(map[ast.Node]ast.Node)
	stack := make([]ast.Node, 0, 16)
	ast.Inspect(root, func(node ast.Node) bool {
		if node == nil {
			stack = stack[:len(stack)-1]
			return false
		}
		if len(stack) > 0 {
			parents[node] = stack[len(stack)-1]
		}
		stack = append(stack, node)
		return true
	})
	return parents
}

func feedbackServiceReceiver(
	function *ast.FuncDecl,
) (*ast.Object, bool) {
	if function.Recv == nil || len(function.Recv.List) != 1 {
		return nil, false
	}
	field := function.Recv.List[0]
	if feedbackTypeName(field.Type) != "Service" || len(field.Names) != 1 {
		return nil, false
	}
	return field.Names[0].Obj, true
}

func feedbackServiceObjects(
	function *ast.FuncDecl,
) map[*ast.Object]struct{} {
	objects := make(map[*ast.Object]struct{})
	if receiver, ok := feedbackServiceReceiver(function); ok &&
		receiver != nil {
		objects[receiver] = struct{}{}
	}
	if function.Type.Params == nil {
		return objects
	}
	for _, field := range function.Type.Params.List {
		if feedbackTypeName(field.Type) != "Service" {
			continue
		}
		for _, name := range field.Names {
			if name.Obj != nil {
				objects[name.Obj] = struct{}{}
			}
		}
	}
	return objects
}

func feedbackTypeName(expression ast.Expr) string {
	switch value := feedbackUnparen(expression).(type) {
	case *ast.Ident:
		return value.Name
	case *ast.StarExpr:
		return feedbackTypeName(value.X)
	default:
		return ""
	}
}

func feedbackSelectorTargetsReceiver(
	selector *ast.SelectorExpr,
	receiver *ast.Object,
	pathAfterReceiver ...string,
) bool {
	root, path, ok := feedbackSelectorPath(selector)
	return ok &&
		receiver != nil &&
		root.Obj == receiver &&
		slices.Equal(path[1:], pathAfterReceiver)
}

func feedbackSelectorTargetsAnyService(
	selector *ast.SelectorExpr,
	serviceObjects map[*ast.Object]struct{},
) bool {
	root, path, ok := feedbackSelectorPath(selector)
	if !ok || len(path) != 2 {
		return false
	}
	_, ok = serviceObjects[root.Obj]
	return ok
}

func feedbackSelectorHasPath(expression ast.Expr, want ...string) bool {
	_, path, ok := feedbackSelectorPath(expression)
	return ok && slices.Equal(path, want)
}

func feedbackSelectorPath(
	expression ast.Expr,
) (*ast.Ident, []string, bool) {
	switch value := feedbackUnparen(expression).(type) {
	case *ast.Ident:
		return value, []string{value.Name}, true
	case *ast.SelectorExpr:
		root, path, ok := feedbackSelectorPath(value.X)
		if !ok {
			return nil, nil, false
		}
		return root, append(path, value.Sel.Name), true
	default:
		return nil, nil, false
	}
}

func feedbackUnparen(expression ast.Expr) ast.Expr {
	for {
		parenthesized, ok := expression.(*ast.ParenExpr)
		if !ok {
			return expression
		}
		expression = parenthesized.X
	}
}
