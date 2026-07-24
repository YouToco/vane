package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"runtime"
	"testing"
)

func TestDefinitionEditReceiptDispatcherStartsBeforeEveryIngressAndDrainsInOrder(
	t *testing.T,
) {
	_, current, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate definition edit receipt wiring test")
	}
	file, err := parser.ParseFile(
		token.NewFileSet(),
		filepath.Join(filepath.Dir(current), "main.go"),
		nil,
		0,
	)
	if err != nil {
		t.Fatal(err)
	}

	var dispatcherObject *ast.Object
	ast.Inspect(file, func(node ast.Node) bool {
		assignment, ok := node.(*ast.AssignStmt)
		if !ok {
			return true
		}
		for index, expression := range assignment.Rhs {
			call, ok := expression.(*ast.CallExpr)
			if !ok || !isPackageSelector(
				call.Fun, "task",
				"NewDefinitionEditReceiptDispatcher",
			) || index >= len(assignment.Lhs) {
				continue
			}
			identifier, ok := assignment.Lhs[index].(*ast.Ident)
			if ok {
				dispatcherObject = identifier.Obj
			}
		}
		return true
	})
	if dispatcherObject == nil {
		t.Fatal("definition edit receipt dispatcher is not constructed")
	}

	var (
		runPosition          token.Pos
		runInGoroutine       bool
		workerStartPosition  token.Pos
		managerStartPosition token.Pos
		apiMountPosition     token.Pos
		listenPosition       token.Pos
		stopCalls            int
		lastStopPosition     token.Pos
		lastRecoveryStop     token.Pos
		lastSessionDrain     token.Pos
	)
	ast.Inspect(file, func(node ast.Node) bool {
		switch value := node.(type) {
		case *ast.CallExpr:
			if selector, ok := value.Fun.(*ast.SelectorExpr); ok {
				if receiver, ok := selector.X.(*ast.Ident); ok &&
					receiver.Obj == dispatcherObject &&
					selector.Sel.Name == "Run" {
					runPosition = value.Pos()
				}
			}
			switch {
			case isReceiverSelector(value.Fun, "w", "Start"):
				workerStartPosition = value.Pos()
			case isReceiverSelector(value.Fun, "manager", "Start"):
				managerStartPosition = value.Pos()
			case isPackageSelector(value.Fun, "api", "Mount"):
				apiMountPosition = value.Pos()
			case isReceiverSelector(value.Fun, "srv", "ListenAndServe"):
				listenPosition = value.Pos()
			}
			if identifier, ok := value.Fun.(*ast.Ident); ok {
				switch identifier.Name {
				case "stopDefinitionEditReceiptDispatch":
					stopCalls++
					lastStopPosition = value.Pos()
				case "stopDefinitionEditRecovery":
					lastRecoveryStop = value.Pos()
				case "drainAgentSessions":
					lastSessionDrain = value.Pos()
				}
			}
		case *ast.GoStmt:
			ast.Inspect(value.Call, func(child ast.Node) bool {
				call, ok := child.(*ast.CallExpr)
				if !ok {
					return true
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || selector.Sel.Name != "Run" {
					return true
				}
				receiver, ok := selector.X.(*ast.Ident)
				if ok && receiver.Obj == dispatcherObject {
					runInGoroutine = true
				}
				return true
			})
		}
		return true
	})
	if runPosition == token.NoPos || !runInGoroutine {
		t.Fatalf("definition edit receipt Run=%d background=%v",
			runPosition, runInGoroutine)
	}
	for name, ingress := range map[string]token.Pos{
		"Temporal worker": workerStartPosition,
		"Feishu manager":  managerStartPosition,
		"HTTP API mount":  apiMountPosition,
		"HTTP listener":   listenPosition,
	} {
		if ingress == token.NoPos || runPosition >= ingress {
			t.Fatalf("dispatcher Run=%d must precede %s ingress=%d",
				runPosition, name, ingress)
		}
	}
	if stopCalls < 2 || lastRecoveryStop == token.NoPos ||
		lastSessionDrain == token.NoPos ||
		lastStopPosition <= lastRecoveryStop ||
		lastStopPosition >= lastSessionDrain {
		t.Fatalf("shutdown order recovery=%d edit_receipt=%d session=%d calls=%d",
			lastRecoveryStop, lastStopPosition, lastSessionDrain, stopCalls)
	}
}
