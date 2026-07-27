package main

import (
	"os"
	"strings"
	"testing"
)

func TestRunOutcomeRecoveryStartsBeforeIngressAndDrains(t *testing.T) {
	sourceBytes, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(sourceBytes)
	startup := strings.Index(
		source, "outcomeRecoveryRunner.RunStartup(ctx)")
	periodic := strings.Index(
		source, "outcomeRecoveryRunner.Run(outcomeRecoveryCtx)")
	worker := strings.Index(source, "if err := w.Start(); err != nil")
	manager := strings.Index(source, "manager.Start(ctx)")
	if startup < 0 || periodic < 0 || worker < 0 || manager < 0 ||
		startup > periodic || periodic > worker || worker > manager {
		t.Fatalf(
			"run outcome recovery ordering startup=%d periodic=%d worker=%d manager=%d",
			startup, periodic, worker, manager)
	}
	if calls := strings.Count(source, "stopOutcomeRecovery()"); calls < 3 {
		t.Fatalf("stopOutcomeRecovery calls = %d, want startup/A2A/final drains",
			calls)
	}
}
