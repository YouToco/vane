package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"testing"
)

func TestPushEffectRecoveryStartupAndDrainOrdering(t *testing.T) {
	source, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	file, err := parser.ParseFile(
		token.NewFileSet(), "main.go", source, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	run := startupTopLevelFunction(t, file, "run")
	objects := map[string]*ast.Object{
		"w":       startupTopLevelObject(t, run, "w"),
		"manager": startupTopLevelObject(t, run, "manager"),
	}
	workerStart := startupFailClosedIfInitCallIndex(
		run, objects["w"], "Start")
	managerStart := startupExpressionCallIndex(
		run, objects["manager"], "Start")
	if workerStart < 0 || managerStart < 0 || workerStart >= managerStart {
		t.Fatalf("worker/manager ingress order=%d/%d",
			workerStart, managerStart)
	}

	recoveryGate := -1
	for index, statement := range run.Body.List {
		ifStatement, ok := statement.(*ast.IfStmt)
		if !ok ||
			!isExactRecoveryCanaryEnabled(ifStatement.Cond) {
			continue
		}
		recoveryGate = index
		if countSelectorCalls(ifStatement.Body, "PrepareOutbound") != 1 ||
			countSelectorCalls(ifStatement.Body, "RunStartup") != 1 {
			t.Fatal("exact-task recovery Gate must prepare outbound and run startup once")
		}
	}
	if recoveryGate < 0 || recoveryGate >= workerStart {
		t.Fatalf("recovery startup Gate=%d worker ingress=%d",
			recoveryGate, workerStart)
	}

	periodicGo := -1
	for index, statement := range run.Body.List {
		found := false
		ast.Inspect(statement, func(node ast.Node) bool {
			goStatement, ok := node.(*ast.GoStmt)
			if ok && astContainsSelector(goStatement, "Run") &&
				astContainsIdent(goStatement, "pushRecoveryRunner") {
				found = true
				return false
			}
			return !found
		})
		if found {
			periodicGo = index
			break
		}
	}
	if periodicGo < 0 || periodicGo >= workerStart {
		t.Fatalf("periodic recovery=%d worker ingress=%d",
			periodicGo, workerStart)
	}
	if calls := countIdentCalls(run.Body, "stopPushRecovery"); calls < 3 {
		t.Fatalf("stopPushRecovery calls=%d, want startup/A2A/final drains", calls)
	}
}

func isExactRecoveryCanaryEnabled(expression ast.Expr) bool {
	binary, ok := expression.(*ast.BinaryExpr)
	if !ok || binary.Op != token.NEQ {
		return false
	}
	literal, ok := binary.Y.(*ast.BasicLit)
	return ok && literal.Kind == token.STRING && literal.Value == `""` &&
		astContainsSelector(
			binary.X, "PushEffectRecoveryCanaryScheduleID")
}

func astContainsSelector(node ast.Node, name string) bool {
	found := false
	ast.Inspect(node, func(child ast.Node) bool {
		selector, ok := child.(*ast.SelectorExpr)
		if ok && selector.Sel.Name == name {
			found = true
		}
		return !found
	})
	return found
}

func astContainsIdent(node ast.Node, name string) bool {
	found := false
	ast.Inspect(node, func(child ast.Node) bool {
		ident, ok := child.(*ast.Ident)
		if ok && ident.Name == name {
			found = true
		}
		return !found
	})
	return found
}

func countSelectorCalls(node ast.Node, name string) int {
	count := 0
	ast.Inspect(node, func(child ast.Node) bool {
		call, ok := child.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if ok && selector.Sel.Name == name {
			count++
		}
		return true
	})
	return count
}

func countIdentCalls(node ast.Node, name string) int {
	count := 0
	ast.Inspect(node, func(child ast.Node) bool {
		call, ok := child.(*ast.CallExpr)
		if !ok {
			return true
		}
		ident, ok := call.Fun.(*ast.Ident)
		if ok && ident.Name == name {
			count++
		}
		return true
	})
	return count
}
