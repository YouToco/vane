package main

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestResearchV3DefinitionEditIsGatedRecoveredAndInjectedBeforeIngress(t *testing.T) {
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate Research V3 edit wiring test")
	}
	payload, err := os.ReadFile(filepath.Join(filepath.Dir(testFile), "main.go"))
	if err != nil {
		t.Fatal(err)
	}
	source := string(payload)
	ordered := []string{
		"task.NewResearchTaskDefinitionEditCoordinatorV3(",
		"researchDefinitionEditCoordinator.RecoverStaleOnceV3(ctx)",
		"Editor: agent.NewResearchTaskDefinitionEditV3Executor(",
		"researchDefinitionEditCoordinator.RunRecoveryV3(",
		"w.Start()",
		"manager.Start(ctx)",
	}
	previous := -1
	for _, needle := range ordered {
		index := strings.Index(source, needle)
		if index < 0 || index <= previous {
			t.Fatalf("Research V3 edit startup order missing %q: previous=%d current=%d",
				needle, previous, index)
		}
		if strings.Count(source, needle) != 1 {
			t.Fatalf("Research V3 edit wiring %q count=%d, want 1",
				needle, strings.Count(source, needle))
		}
		previous = index
	}
	if !strings.Contains(source,
		"researchControlStore, sched, slog.Default())") {
		t.Fatal("Research V3 edit coordinator is not bound to the isolated control Store")
	}
}
