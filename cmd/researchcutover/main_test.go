package main

import (
	"os"
	"strings"
	"testing"
)

func TestParseCutoverArgs(t *testing.T) {
	digest := strings.Repeat("a", 64)
	for _, operation := range []string{"preflight", "status", "verify", "rollback", "cutover"} {
		arguments := []string{"-operation", operation, "-task-id", "task-v3", "-idempotency-key", "gate-1-attempt"}
		if operation == "cutover" {
			arguments = append(arguments, "-plan-digest", digest)
		}
		got, err := parseCutoverArgs(arguments)
		if err != nil || got.operation != operation || got.taskID != "task-v3" ||
			got.idempotencyKey != "gate-1-attempt" {
			t.Fatalf("operation=%s got=%+v err=%v", operation, got, err)
		}
	}
}

func TestParseCutoverArgsRejectsUnsafeScope(t *testing.T) {
	tests := [][]string{
		{"-operation", "delete", "-task-id", "task-v3", "-idempotency-key", "key"},
		{"-operation", "cutover", "-task-id", "task-other ", "-idempotency-key", "key", "-plan-digest", strings.Repeat("a", 64)},
		{"-operation", "cutover", "-task-id", "task-v3", "-idempotency-key", "key"},
		{"-operation", "rollback", "-task-id", "task-v3", "-user-id", "42", "-idempotency-key", "key"},
		{"-operation", "rollback", "-task-id", "task-v3", "-idempotency-key", " "},
	}
	for _, args := range tests {
		if _, err := parseCutoverArgs(args); err == nil {
			t.Fatalf("unsafe args accepted: %v", args)
		}
	}
}

func TestCutoverCommandUsesMinimalOneShotRuntime(t *testing.T) {
	payload, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(payload)
	for _, forbidden := range []string{"store.NewServerRuntime", "config.Load", "-user-id"} {
		if strings.Contains(source, forbidden) {
			t.Fatalf("one-shot cutover command contains forbidden runtime surface %q", forbidden)
		}
	}
	for _, required := range []string{
		"researchoperator.MigrationDatabaseURL()", "store.New(ctx, operatorDatabaseURL)",
		"ResolveResearchV3OperatorScope", "researchoperator.LoadTemporalConfig()",
	} {
		if !strings.Contains(source, required) {
			t.Fatalf("one-shot cutover boundary is missing %q", required)
		}
	}
}
