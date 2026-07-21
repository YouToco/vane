package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"runtime"
	"testing"
)

// TestPlaybookPromptPolicyWiring guards the composition root: leaving the
// option off NewActivities would compile and silently keep every task on the
// legacy prompt even when operators enable the rollout configuration.
func TestPlaybookPromptPolicyWiring(t *testing.T) {
	_, self, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller 定位不到本测试文件")
	}
	file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(filepath.Dir(self), "main.go"), nil, 0)
	if err != nil {
		t.Fatalf("解析 main.go: %v", err)
	}

	var constructors []*ast.CallExpr
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if ok && selectorPath(call.Fun) == "workflow.NewActivities" {
			constructors = append(constructors, call)
		}
		return true
	})
	if len(constructors) != 1 {
		t.Fatalf("workflow.NewActivities 调用数 = %d，期望 1", len(constructors))
	}

	args := constructors[0].Args
	if len(args) == 0 {
		t.Fatal("workflow.NewActivities 缺少参数")
	}
	policy, ok := args[len(args)-1].(*ast.CallExpr)
	if !ok || selectorPath(policy.Fun) != "workflow.WithPlaybookPromptPolicy" {
		t.Fatal("NewActivities 最后一参必须是 workflow.WithPlaybookPromptPolicy")
	}
	if len(policy.Args) != 2 {
		t.Fatalf("WithPlaybookPromptPolicy 参数数 = %d，期望 2", len(policy.Args))
	}
	if got := selectorPath(policy.Args[0]); got != "cfg.Pipeline.PlaybookPromptsEnabled" {
		t.Fatalf("rollout enabled 接线 = %q，期望 cfg.Pipeline.PlaybookPromptsEnabled", got)
	}
	if got := selectorPath(policy.Args[1]); got != "cfg.Pipeline.PlaybookPromptCanaryScheduleID" {
		t.Fatalf("canary schedule 接线 = %q，期望 cfg.Pipeline.PlaybookPromptCanaryScheduleID", got)
	}
}

func selectorPath(expr ast.Expr) string {
	switch expr := expr.(type) {
	case *ast.Ident:
		return expr.Name
	case *ast.SelectorExpr:
		prefix := selectorPath(expr.X)
		if prefix == "" {
			return expr.Sel.Name
		}
		return prefix + "." + expr.Sel.Name
	default:
		return ""
	}
}
