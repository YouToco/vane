package workflow

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// TestDurablePushUsesOnlyAtomicProviderTarget is a mutation trap for the
// provider-generation boundary. The legacy Push path may still read the owner
// alone, but the durable sender must freeze owner/chat/App through the single
// atomic Manager snapshot and must never reconstruct it from separate reads.
func TestDurablePushUsesOnlyAtomicProviderTarget(t *testing.T) {
	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, "activities.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	var durable *ast.FuncDecl
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if ok && function.Name.Name == "sendDurablePushChunk" {
			durable = function
			break
		}
	}
	if durable == nil || durable.Body == nil {
		t.Fatal("sendDurablePushChunk production coordinator not found")
	}

	atomicCalls := 0
	forbidden := make(map[string]int)
	ast.Inspect(durable.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		method, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || !isActivitiesFeishuReceiver(method.X) {
			return true
		}
		switch method.Sel.Name {
		case "PushEffectTarget":
			atomicCalls++
		case "OwnerOpenID", "OwnerChatID", "AppIdentity":
			forbidden[method.Sel.Name]++
		}
		return true
	})

	if atomicCalls != 1 {
		t.Fatalf("durable provider target atomic calls=%d, want exactly 1",
			atomicCalls)
	}
	for method, count := range forbidden {
		if count != 0 {
			t.Errorf("durable provider target escaped through %s (%d calls)",
				method, count)
		}
	}
}

// TestDurablePushBackoffIsDefiniteOnly locks the Store contract into the
// production coordinator: ambiguous evidence must carry RetryAfter=0, while a
// definite "not sent" response retains the bounded retry delay.
func TestDurablePushBackoffIsDefiniteOnly(t *testing.T) {
	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, "activities.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	durable := findWorkflowFunction(file, "sendDurablePushChunk")
	if durable == nil || durable.Body == nil {
		t.Fatal("sendDurablePushChunk production coordinator not found")
	}

	compositeRetryFields := 0
	totalRetryAssignments := 0
	definiteRetryAssignments := 0
	ast.Inspect(durable.Body, func(node ast.Node) bool {
		switch typed := node.(type) {
		case *ast.CompositeLit:
			typeName, ok := typed.Type.(*ast.SelectorExpr)
			if !ok || typeName.Sel.Name != "FailureParams" {
				return true
			}
			for _, element := range typed.Elts {
				field, ok := element.(*ast.KeyValueExpr)
				if !ok {
					continue
				}
				if key, keyOK := field.Key.(*ast.Ident); keyOK &&
					key.Name == "RetryAfter" {
					compositeRetryFields++
				}
			}
		case *ast.AssignStmt:
			totalRetryAssignments += retryAfterAssignments(typed)
		case *ast.IfStmt:
			condition, ok := typed.Cond.(*ast.Ident)
			if ok && condition.Name == "definite" {
				ast.Inspect(typed.Body, func(child ast.Node) bool {
					assignment, ok := child.(*ast.AssignStmt)
					if ok {
						definiteRetryAssignments +=
							retryAfterAssignments(assignment)
					}
					return true
				})
			}
		}
		return true
	})

	if compositeRetryFields != 0 ||
		totalRetryAssignments != 1 ||
		definiteRetryAssignments != 1 {
		t.Fatalf(
			"push failure retry boundary composite=%d total=%d definite=%d, want 0/1/1",
			compositeRetryFields,
			totalRetryAssignments,
			definiteRetryAssignments,
		)
	}
}

func findWorkflowFunction(file *ast.File, name string) *ast.FuncDecl {
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if ok && function.Name.Name == name {
			return function
		}
	}
	return nil
}

func retryAfterAssignments(assignment *ast.AssignStmt) int {
	count := 0
	for _, expression := range assignment.Lhs {
		field, ok := expression.(*ast.SelectorExpr)
		if !ok || field.Sel.Name != "RetryAfter" {
			continue
		}
		receiver, ok := field.X.(*ast.Ident)
		if ok && receiver.Name == "failure" {
			count++
		}
	}
	return count
}

func isActivitiesFeishuReceiver(expression ast.Expr) bool {
	field, ok := expression.(*ast.SelectorExpr)
	if !ok || field.Sel.Name != "feishu" {
		return false
	}
	receiver, ok := field.X.(*ast.Ident)
	return ok && receiver.Name == "a"
}
