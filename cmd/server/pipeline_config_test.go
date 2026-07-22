package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"runtime"
	"testing"
)

// TestPipelinePolicyWiring guards both policy options at the composition root.
// Leaving either option off NewActivities would compile and silently keep the
// corresponding runtime behavior on its legacy path.
func TestPipelinePolicyWiring(t *testing.T) {
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
	options := make(map[string][]*ast.CallExpr)
	for _, arg := range args {
		call, ok := arg.(*ast.CallExpr)
		if !ok {
			continue
		}
		options[selectorPath(call.Fun)] = append(options[selectorPath(call.Fun)], call)
	}

	playbookPolicies := options["workflow.WithPlaybookPromptPolicy"]
	if len(playbookPolicies) != 1 {
		t.Fatalf("WithPlaybookPromptPolicy 接线数 = %d，期望 1", len(playbookPolicies))
	}
	policy := playbookPolicies[0]
	if len(policy.Args) != 2 {
		t.Fatalf("WithPlaybookPromptPolicy 参数数 = %d，期望 2", len(policy.Args))
	}
	if got := selectorPath(policy.Args[0]); got != "cfg.Pipeline.PlaybookPromptsEnabled" {
		t.Fatalf("rollout enabled 接线 = %q，期望 cfg.Pipeline.PlaybookPromptsEnabled", got)
	}
	if got := selectorPath(policy.Args[1]); got != "cfg.Pipeline.PlaybookPromptCanaryScheduleID" {
		t.Fatalf("canary schedule 接线 = %q，期望 cfg.Pipeline.PlaybookPromptCanaryScheduleID", got)
	}

	compiledPolicies := options["workflow.WithCompiledRuntimeV1"]
	if len(compiledPolicies) != 1 {
		t.Fatalf("WithCompiledRuntimeV1 接线数 = %d，期望 1", len(compiledPolicies))
	}
	compiled := compiledPolicies[0]
	if len(compiled.Args) != 3 {
		t.Fatalf("WithCompiledRuntimeV1 参数数 = %d，期望 3", len(compiled.Args))
	}
	if got := selectorPath(compiled.Args[0]); got != "st" {
		t.Fatalf("snapshot store 接线 = %q，期望 st", got)
	}
	if _, ok := compiled.Args[1].(*ast.FuncLit); !ok {
		t.Fatalf("compiled policy builder 接线类型 = %T，期望函数", compiled.Args[1])
	}
	if got := selectorPath(compiled.Args[2]); got != "compiledModelResolver" {
		t.Fatalf("compiled LLM 接线 = %q，期望 compiledModelResolver", got)
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
