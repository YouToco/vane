package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"runtime"
	"testing"
)

func TestResearchV3RolloutWiring(t *testing.T) {
	_, self, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller could not locate this test")
	}
	file, err := parser.ParseFile(
		token.NewFileSet(), filepath.Join(filepath.Dir(self), "main.go"), nil, 0,
	)
	if err != nil {
		t.Fatal(err)
	}
	options := make(map[string][]*ast.CallExpr)
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if ok {
			name := selectorPath(call.Fun)
			if name == "scheduler.WithResearchRuntimeV3ShadowCanary" ||
				name == "scheduler.WithResearchRuntimeV3AuthorityCanary" {
				options[name] = append(options[name], call)
			}
		}
		return true
	})
	for option, configField := range map[string]string{
		"scheduler.WithResearchRuntimeV3ShadowCanary":    "cfg.Pipeline.ResearchV3ShadowCanaryScheduleID",
		"scheduler.WithResearchRuntimeV3AuthorityCanary": "cfg.Pipeline.ResearchV3AuthorityCanaryScheduleID",
	} {
		calls := options[option]
		if len(calls) != 1 || len(calls[0].Args) != 1 ||
			selectorPath(calls[0].Args[0]) != configField {
			t.Fatalf("%s is not wired exactly once to %s", option, configField)
		}
	}
}
