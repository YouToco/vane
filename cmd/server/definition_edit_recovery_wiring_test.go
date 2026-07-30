package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"runtime"
	"slices"
	"testing"
)

// TestTaskDefinitionEditWiringUsesFlaggedController pins the C2b3-2d process
// boundary: one controller always drains historical card callbacks, while the
// default-off flag alone controls whether the proposal tool is registered.
func TestTaskDefinitionEditWiringUsesFlaggedController(t *testing.T) {
	t.Helper()
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate definition-edit recovery wiring test")
	}
	mainFile := filepath.Join(filepath.Dir(testFile), "main.go")
	fset := token.NewFileSet()
	parsed, err := parser.ParseFile(fset, mainFile, nil, 0)
	if err != nil {
		t.Fatalf("parse cmd/server/main.go: %v", err)
	}

	var (
		coordinatorName       string
		constructorCalls      int
		constructorPos        token.Pos
		roleGateCalls         int
		roleGatePos           token.Pos
		environmentGateCalls  int
		environmentGatePos    token.Pos
		firstRecoveryCalls    int
		firstRecoveryPos      token.Pos
		reconcileCalls        int
		reconcilePos          token.Pos
		runRecoveryCalls      int
		runRecoveryPos        token.Pos
		runRecoveryInGoStmt   bool
		workerStartPos        token.Pos
		managerStartPos       token.Pos
		coordinatorIdentCount int
		stopCalls             int
		controllerCalls       int
		controllerPos         token.Pos
		flaggedController     bool
	)

	ast.Inspect(parsed, func(node ast.Node) bool {
		assign, ok := node.(*ast.AssignStmt)
		if !ok {
			return true
		}
		for i, rhs := range assign.Rhs {
			call, ok := rhs.(*ast.CallExpr)
			if !ok || !isPackageSelector(call.Fun, "task", "NewTaskDefinitionEditCoordinator") {
				continue
			}
			constructorCalls++
			constructorPos = call.Pos()
			if i >= len(assign.Lhs) {
				t.Fatalf("definition-edit coordinator constructor has no assignment target")
			}
			ident, ok := assign.Lhs[i].(*ast.Ident)
			if !ok {
				t.Fatalf("definition-edit coordinator assigned to %T, want identifier", assign.Lhs[i])
			}
			coordinatorName = ident.Name
		}
		return true
	})
	ast.Inspect(parsed, func(node ast.Node) bool {
		branch, ok := node.(*ast.IfStmt)
		if !ok || !isSelectorPath(
			branch.Cond, "cfg", "Agent", "DefinitionEditEnabled",
		) {
			return true
		}
		ast.Inspect(branch.Body, func(child ast.Node) bool {
			assignment, ok := child.(*ast.AssignStmt)
			if !ok {
				return true
			}
			for _, rhs := range assignment.Rhs {
				ident, ok := rhs.(*ast.Ident)
				if ok && ident.Name == "definitionEditController" {
					flaggedController = true
				}
			}
			return true
		})
		return true
	})
	if constructorCalls != 1 || coordinatorName == "" {
		t.Fatalf("definition-edit coordinator constructors = %d (%q), want one named binding",
			constructorCalls, coordinatorName)
	}

	ast.Inspect(parsed, func(node ast.Node) bool {
		switch value := node.(type) {
		case *ast.Ident:
			if value.Name == coordinatorName {
				coordinatorIdentCount++
			}
		case *ast.CallExpr:
			if isReceiverSelector(value.Fun, "st", "ValidateTaskDefinitionEditRuntimeRoles") {
				roleGateCalls++
				roleGatePos = value.Pos()
			}
			if isReceiverSelector(value.Fun, coordinatorName, "ValidateRuntimeEnvironment") {
				environmentGateCalls++
				environmentGatePos = value.Pos()
			}
			if isReceiverSelector(value.Fun, coordinatorName, "RecoverStaleOnce") {
				firstRecoveryCalls++
				firstRecoveryPos = value.Pos()
			}
			if isReceiverSelector(value.Fun, "sched", "ReconcileActions") {
				reconcileCalls++
				reconcilePos = value.Pos()
			}
			if isReceiverSelector(value.Fun, coordinatorName, "RunRecovery") {
				runRecoveryCalls++
				runRecoveryPos = value.Pos()
			}
			if isReceiverSelector(value.Fun, "manager", "Start") {
				managerStartPos = value.Pos()
			}
			if isReceiverSelector(value.Fun, "w", "Start") {
				workerStartPos = value.Pos()
			}
			if isPackageSelector(value.Fun, "task", "NewDefinitionEditController") &&
				len(value.Args) == 3 {
				if coordinator, ok := value.Args[1].(*ast.Ident); ok &&
					coordinator.Name == coordinatorName &&
					isPackageCall(value.Args[2], "agent", "NewPlaybookTranslator") {
					controllerCalls++
					controllerPos = value.Pos()
				}
			}
			if ident, ok := value.Fun.(*ast.Ident); ok && ident.Name == "stopDefinitionEditRecovery" {
				stopCalls++
			}
		case *ast.GoStmt:
			ast.Inspect(value.Call, func(child ast.Node) bool {
				call, ok := child.(*ast.CallExpr)
				if ok && isReceiverSelector(call.Fun, coordinatorName, "RunRecovery") {
					runRecoveryInGoStmt = true
				}
				return true
			})
		}
		return true
	})

	if coordinatorIdentCount != 5 || controllerCalls != 1 ||
		!flaggedController {
		t.Fatalf("%s production references/controller edges = %d/%d, want assignment plus three recovery/Gate receivers and one narrow controller edge",
			coordinatorName, coordinatorIdentCount, controllerCalls)
	}
	if roleGateCalls != 1 || environmentGateCalls != 1 ||
		firstRecoveryCalls != 1 || reconcileCalls != 1 {
		t.Fatalf("startup role/environment/first recovery/reconcile calls = %d/%d/%d/%d, want one each",
			roleGateCalls, environmentGateCalls, firstRecoveryCalls, reconcileCalls)
	}
	if runRecoveryCalls != 1 || !runRecoveryInGoStmt {
		t.Fatalf("RunRecovery calls = %d, background=%v; want one background loop",
			runRecoveryCalls, runRecoveryInGoStmt)
	}
	if workerStartPos == token.NoPos || managerStartPos == token.NoPos ||
		constructorPos >= roleGatePos || roleGatePos >= environmentGatePos ||
		environmentGatePos >= firstRecoveryPos ||
		firstRecoveryPos >= reconcilePos || reconcilePos >= runRecoveryPos ||
		runRecoveryPos >= workerStartPos ||
		workerStartPos >= managerStartPos {
		t.Fatalf("startup order must be constructor→role Gate→environment Gate→first recovery→reconcile→periodic recovery→worker ingress→manager ingress: constructor=%d role=%d environment=%d first=%d reconcile=%d worker.Start=%d periodic=%d manager.Start=%d",
			constructorPos, roleGatePos, environmentGatePos, firstRecoveryPos,
			reconcilePos, workerStartPos, runRecoveryPos, managerStartPos)
	}
	if controllerPos == token.NoPos || controllerPos >= managerStartPos {
		t.Fatalf("flagged definition-edit controller must be constructed before manager ingress: controller=%d manager=%d",
			controllerPos, managerStartPos)
	}
	if stopCalls != 3 {
		t.Fatalf("stopDefinitionEditRecovery calls = %d, want worker/A2A startup-failure and normal-shutdown drains",
			stopCalls)
	}
}

func isPackageCall(node ast.Expr, pkg, name string) bool {
	call, ok := node.(*ast.CallExpr)
	return ok && isPackageSelector(call.Fun, pkg, name)
}

func isSelectorPath(expr ast.Expr, path ...string) bool {
	if len(path) == 0 {
		return false
	}
	current := expr
	for i := len(path) - 1; i > 0; i-- {
		selector, ok := current.(*ast.SelectorExpr)
		if !ok || selector.Sel.Name != path[i] {
			return false
		}
		current = selector.X
	}
	identifier, ok := current.(*ast.Ident)
	return ok && identifier.Name == path[0]
}

func TestTaskDefinitionEditStartupGatesAreTopLevelAndOrdered(t *testing.T) {
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate definition-edit startup wiring test")
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(
		fset, filepath.Join(filepath.Dir(testFile), "main.go"), nil, 0,
	)
	if err != nil {
		t.Fatal(err)
	}
	run := startupTopLevelFunction(t, file, "run")
	objects := make(map[string]*ast.Object)
	for _, name := range []string{"st", "definitionEditCoordinator", "sched", "w", "manager"} {
		objects[name] = startupTopLevelObject(t, run, name)
	}

	constructor := startupDirectPackageAssignmentIndex(
		run, "task", "NewTaskDefinitionEditCoordinator",
	)
	role := startupDirectAssignmentIndex(
		run, objects["st"], "ValidateTaskDefinitionEditRuntimeRoles",
	)
	roleResult := startupAssignmentResultObject(run, role)
	roleGuard := startupGuardReturnIndex(run, roleResult)
	environment := startupDirectAssignmentIndex(
		run, objects["definitionEditCoordinator"], "ValidateRuntimeEnvironment",
	)
	environmentResult := startupAssignmentResultObject(run, environment)
	environmentGuard := startupGuardReturnIndex(run, environmentResult)
	firstRecovery := startupFailClosedIfInitCallIndex(
		run, objects["definitionEditCoordinator"], "RecoverStaleOnce",
	)
	reconcile := startupFailClosedIfInitCallIndex(
		run, objects["sched"], "ReconcileActions",
	)
	worker := startupFailClosedIfInitCallIndex(run, objects["w"], "Start")
	periodic := startupGoCallIndex(
		run, objects["definitionEditCoordinator"], "RunRecovery",
	)
	manager := startupExpressionCallIndex(run, objects["manager"], "Start")

	ordered := []int{
		constructor, role, environment, firstRecovery, reconcile,
		periodic, worker, manager,
	}
	for index, statementIndex := range ordered {
		if statementIndex < 0 {
			t.Fatalf("startup Gate step %d is not in its required top-level form", index)
		}
		if index > 0 && ordered[index-1] >= statementIndex {
			t.Fatalf("startup Gate top-level order = %v", ordered)
		}
	}
	if roleGuard <= role || roleGuard >= environment ||
		environmentGuard <= environment || environmentGuard >= firstRecovery {
		t.Fatalf("role/environment Gate results are not fail-closed before the next step: role=%d guard=%d environment=%d guard=%d first=%d",
			role, roleGuard, environment, environmentGuard, firstRecovery)
	}
	for _, call := range []struct {
		object *ast.Object
		method string
	}{
		{objects["st"], "ValidateTaskDefinitionEditRuntimeRoles"},
		{objects["definitionEditCoordinator"], "ValidateRuntimeEnvironment"},
		{objects["definitionEditCoordinator"], "RecoverStaleOnce"},
		{objects["sched"], "ReconcileActions"},
		{objects["w"], "Start"},
		{objects["definitionEditCoordinator"], "RunRecovery"},
		{objects["manager"], "Start"},
	} {
		if count := startupReceiverCallCount(run.Body, call.object, call.method); count != 1 {
			t.Fatalf("startup receiver call %s count = %d, want 1",
				call.method, count)
		}
	}
}

func TestStartupDirectGateHelperRejectsClosureTokenTrick(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "main.go", `package main
func run() error {
	st, _ := store.New()
	dead := func() { _ = st.ValidateTaskDefinitionEditRuntimeRoles(nil) }
	_ = dead
	return nil
}`, 0)
	if err != nil {
		t.Fatal(err)
	}
	run := startupTopLevelFunction(t, file, "run")
	storeObject := startupTopLevelObject(t, run, "st")
	if count := startupReceiverCallCount(
		run.Body, storeObject, "ValidateTaskDefinitionEditRuntimeRoles",
	); count != 1 {
		t.Fatalf("fixture call count = %d, want 1", count)
	}
	if index := startupDirectAssignmentIndex(
		run, storeObject, "ValidateTaskDefinitionEditRuntimeRoles",
	); index != -1 {
		t.Fatalf("dead closure accepted as direct startup Gate at statement %d", index)
	}
}

func startupTopLevelFunction(t *testing.T, file *ast.File, name string) *ast.FuncDecl {
	t.Helper()
	var found *ast.FuncDecl
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Recv != nil || function.Name.Name != name {
			continue
		}
		if found != nil {
			t.Fatalf("multiple top-level %s functions", name)
		}
		found = function
	}
	if found == nil || found.Body == nil {
		t.Fatalf("top-level %s function is missing", name)
	}
	return found
}

func startupTopLevelObject(t *testing.T, run *ast.FuncDecl, name string) *ast.Object {
	t.Helper()
	var found *ast.Object
	for _, statement := range run.Body.List {
		assignment, ok := statement.(*ast.AssignStmt)
		if !ok {
			continue
		}
		for _, lhs := range assignment.Lhs {
			ident, ok := lhs.(*ast.Ident)
			if !ok || ident.Name != name || ident.Obj == nil {
				continue
			}
			if found != nil && found != ident.Obj {
				t.Fatalf("multiple top-level bindings for %s", name)
			}
			found = ident.Obj
		}
	}
	if found == nil {
		t.Fatalf("top-level binding %s is missing", name)
	}
	return found
}

func startupDirectPackageAssignmentIndex(
	run *ast.FuncDecl,
	packageName, method string,
) int {
	for index, statement := range run.Body.List {
		assignment, ok := statement.(*ast.AssignStmt)
		if !ok {
			continue
		}
		for _, rhs := range assignment.Rhs {
			call, ok := rhs.(*ast.CallExpr)
			if ok && isPackageSelector(call.Fun, packageName, method) {
				return index
			}
		}
	}
	return -1
}

func startupDirectAssignmentIndex(
	run *ast.FuncDecl,
	object *ast.Object,
	method string,
) int {
	for index, statement := range run.Body.List {
		assignment, ok := statement.(*ast.AssignStmt)
		if !ok {
			continue
		}
		for _, rhs := range assignment.Rhs {
			if startupReceiverCall(rhs, object, method) {
				return index
			}
		}
	}
	return -1
}

func startupIfInitCallIndex(
	run *ast.FuncDecl,
	object *ast.Object,
	method string,
) int {
	for index, statement := range run.Body.List {
		ifStatement, ok := statement.(*ast.IfStmt)
		if !ok {
			continue
		}
		assignment, ok := ifStatement.Init.(*ast.AssignStmt)
		if !ok {
			continue
		}
		for _, rhs := range assignment.Rhs {
			if startupReceiverCall(rhs, object, method) {
				return index
			}
		}
	}
	return -1
}

func startupFailClosedIfInitCallIndex(
	run *ast.FuncDecl,
	object *ast.Object,
	method string,
) int {
	for index, statement := range run.Body.List {
		ifStatement, ok := statement.(*ast.IfStmt)
		if !ok || !startupBlockHasDirectReturn(ifStatement.Body) {
			continue
		}
		assignment, ok := ifStatement.Init.(*ast.AssignStmt)
		if !ok {
			continue
		}
		for _, rhs := range assignment.Rhs {
			if startupReceiverCall(rhs, object, method) {
				return index
			}
		}
	}
	return -1
}

func startupAssignmentResultObject(run *ast.FuncDecl, index int) *ast.Object {
	if index < 0 || index >= len(run.Body.List) {
		return nil
	}
	assignment, ok := run.Body.List[index].(*ast.AssignStmt)
	if !ok || len(assignment.Lhs) != 1 {
		return nil
	}
	ident, ok := assignment.Lhs[0].(*ast.Ident)
	if !ok {
		return nil
	}
	return ident.Obj
}

func startupGuardReturnIndex(run *ast.FuncDecl, result *ast.Object) int {
	if result == nil {
		return -1
	}
	for index, statement := range run.Body.List {
		ifStatement, ok := statement.(*ast.IfStmt)
		if !ok || !startupBlockHasDirectReturn(ifStatement.Body) {
			continue
		}
		referencesResult := false
		ast.Inspect(ifStatement.Cond, func(node ast.Node) bool {
			ident, ok := node.(*ast.Ident)
			if ok && ident.Obj == result {
				referencesResult = true
			}
			return true
		})
		if referencesResult {
			return index
		}
	}
	return -1
}

func startupBlockHasDirectReturn(block *ast.BlockStmt) bool {
	if block == nil {
		return false
	}
	return slices.ContainsFunc(block.List, func(statement ast.Stmt) bool {
		_, ok := statement.(*ast.ReturnStmt)
		return ok
	})
}

func startupExpressionCallIndex(
	run *ast.FuncDecl,
	object *ast.Object,
	method string,
) int {
	for index, statement := range run.Body.List {
		expression, ok := statement.(*ast.ExprStmt)
		if ok && startupReceiverCall(expression.X, object, method) {
			return index
		}
	}
	return -1
}

func startupGoCallIndex(
	run *ast.FuncDecl,
	object *ast.Object,
	method string,
) int {
	for index, statement := range run.Body.List {
		goStatement, ok := statement.(*ast.GoStmt)
		if !ok {
			continue
		}
		if startupReceiverCallCount(goStatement.Call, object, method) == 1 {
			return index
		}
	}
	return -1
}

func startupReceiverCall(expr ast.Expr, object *ast.Object, method string) bool {
	call, ok := expr.(*ast.CallExpr)
	if !ok {
		return false
	}
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != method {
		return false
	}
	receiver, ok := selector.X.(*ast.Ident)
	return ok && receiver.Obj == object
}

func startupReceiverCallCount(node ast.Node, object *ast.Object, method string) int {
	count := 0
	ast.Inspect(node, func(child ast.Node) bool {
		call, ok := child.(*ast.CallExpr)
		if ok && startupReceiverCall(call, object, method) {
			count++
		}
		return true
	})
	return count
}

func isPackageSelector(expr ast.Expr, packageName, method string) bool {
	selector, ok := expr.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != method {
		return false
	}
	receiver, ok := selector.X.(*ast.Ident)
	return ok && receiver.Name == packageName
}

func isReceiverSelector(expr ast.Expr, receiverName, method string) bool {
	return isPackageSelector(expr, receiverName, method)
}
