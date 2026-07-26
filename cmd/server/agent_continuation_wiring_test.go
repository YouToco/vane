package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestAgentContinuationWiringOwnsFeedbackSessionProjection(
	t *testing.T,
) {
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate continuation wiring test")
	}
	raw, err := os.ReadFile(filepath.Join(filepath.Dir(testFile), "main.go"))
	if err != nil {
		t.Fatal(err)
	}
	source := string(raw)
	for fragment, want := range map[string]int{
		"agentcontinuation.New(":          1,
		"continuationDispatcher.Run(ctx)": 1,
	} {
		if got := strings.Count(source, fragment); got != want {
			t.Fatalf("%q count=%d want=%d", fragment, got, want)
		}
	}
	if got := strings.Count(source, "Notifier:      agentLoop,"); got != 1 {
		t.Fatalf(
			"deep-dive legacy session notifier wiring count=%d want=1",
			got,
		)
	}
	runAt := strings.Index(source, "continuationDispatcher.Run(ctx)")
	workerAt := strings.Index(source, "if err := w.Start(); err != nil")
	managerAt := strings.Index(source, "manager.Start(ctx)")
	if runAt < 0 || workerAt < 0 || managerAt < 0 ||
		!(runAt < workerAt && workerAt < managerAt) {
		t.Fatalf(
			"continuation startup order invalid: run=%d worker=%d manager=%d",
			runAt, workerAt, managerAt)
	}
	if !strings.Contains(
		source,
		"maintenanceErr := waitMaintenance(maintenanceCtx)",
	) {
		t.Fatal("continuation Run must participate in graceful maintenance drain")
	}
}

func TestFeedbackSessionProjectionCallBoundary(t *testing.T) {
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate continuation wiring test")
	}
	repoRoot := filepath.Clean(
		filepath.Join(filepath.Dir(testFile), "..", ".."),
	)
	servicePath := filepath.Join(repoRoot, "feedback", "service.go")
	deepDivePath := filepath.Join(repoRoot, "feedback", "deepdive.go")

	for _, functionName := range []string{
		"handleAttitude",
		"HandleReasonSubmit",
	} {
		if got := countSelectorCallsInFunction(
			t,
			servicePath,
			functionName,
			"InsertFeedbackWithSessionCutoff",
		); got != 1 {
			t.Fatalf(
				"%s InsertFeedbackWithSessionCutoff calls=%d want=1",
				functionName,
				got,
			)
		}
		if got := countSelectorCallsInFunction(
			t,
			servicePath,
			functionName,
			"notifyClick",
		); got != 0 {
			t.Fatalf("%s notifyClick calls=%d want=0", functionName, got)
		}
	}

	if got := countSelectorCallsInFunction(
		t,
		deepDivePath,
		"generateDeepDive",
		"InsertDeepDiveFeedback",
	); got != 1 {
		t.Fatalf("generateDeepDive InsertDeepDiveFeedback calls=%d want=1", got)
	}
	if got := countSelectorCallsInFunction(
		t,
		deepDivePath,
		"generateDeepDive",
		"notifyClick",
	); got != 1 {
		t.Fatalf("generateDeepDive notifyClick calls=%d want=1", got)
	}
	if got := countSelectorCallsInFunction(
		t,
		deepDivePath,
		"generateDeepDive",
		"InsertFeedbackWithSessionCutoff",
	); got != 0 {
		t.Fatalf(
			"generateDeepDive InsertFeedbackWithSessionCutoff calls=%d want=0",
			got,
		)
	}
}

func countSelectorCallsInFunction(
	t *testing.T,
	path, functionName, selectorName string,
) int {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Name.Name != functionName {
			continue
		}
		count := 0
		ast.Inspect(function.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if ok && selector.Sel.Name == selectorName {
				count++
			}
			return true
		})
		return count
	}
	t.Fatalf("function %s not found in %s", functionName, path)
	return 0
}
