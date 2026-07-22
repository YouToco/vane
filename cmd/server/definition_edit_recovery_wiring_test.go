package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"runtime"
	"testing"
)

// TestTaskDefinitionEditRecoveryWiringStaysDark pins the C2b3-2c process
// boundary: startup may construct the coordinator and run only its recovery
// loop. The coordinator must not escape into Agent/API/Feishu or gain a live
// proposal/confirmation/cancellation entry point before C2b3-2d.
func TestTaskDefinitionEditRecoveryWiringStaysDark(t *testing.T) {
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
		runRecoveryCalls      int
		runRecoveryPos        token.Pos
		runRecoveryInGoStmt   bool
		managerStartPos       token.Pos
		coordinatorIdentCount int
		stopCalls             int
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
			if isReceiverSelector(value.Fun, coordinatorName, "RunRecovery") {
				runRecoveryCalls++
				runRecoveryPos = value.Pos()
			}
			if isReceiverSelector(value.Fun, "manager", "Start") {
				managerStartPos = value.Pos()
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

	if coordinatorIdentCount != 2 {
		t.Fatalf("%s production references = %d, want assignment plus RunRecovery receiver only",
			coordinatorName, coordinatorIdentCount)
	}
	if runRecoveryCalls != 1 || !runRecoveryInGoStmt {
		t.Fatalf("RunRecovery calls = %d, background=%v; want one background loop",
			runRecoveryCalls, runRecoveryInGoStmt)
	}
	if managerStartPos == token.NoPos || constructorPos >= runRecoveryPos ||
		runRecoveryPos >= managerStartPos {
		t.Fatalf("definition-edit recovery must start before ingress: constructor=%d recovery=%d manager.Start=%d",
			constructorPos, runRecoveryPos, managerStartPos)
	}
	if stopCalls != 2 {
		t.Fatalf("stopDefinitionEditRecovery calls = %d, want startup-failure and normal-shutdown drains",
			stopCalls)
	}
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
