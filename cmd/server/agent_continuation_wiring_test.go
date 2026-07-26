package main

import (
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
	if strings.Contains(source, "Notifier:") {
		t.Fatal("feedback service must not retain legacy session notifier wiring")
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
