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

func isActivitiesFeishuReceiver(expression ast.Expr) bool {
	field, ok := expression.(*ast.SelectorExpr)
	if !ok || field.Sel.Name != "feishu" {
		return false
	}
	receiver, ok := field.X.(*ast.Ident)
	return ok && receiver.Name == "a"
}
