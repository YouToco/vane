package main

import (
	"os"
	"strings"
	"testing"
)

func TestParseShadowArgsRequiresExactOperatorScope(t *testing.T) {
	got, err := parseShadowArgs([]string{
		"-task-id", "task-v3",
		"-idempotency-key", "shadow-2026-08-01-1",
	})
	if err != nil || got.taskID != "task-v3" ||
		got.idempotencyKey != "shadow-2026-08-01-1" {
		t.Fatalf("args=%+v err=%v", got, err)
	}
	for _, arguments := range [][]string{
		{},
		{"-task-id", "task-v3", "-user-id", "42", "-idempotency-key", "key"},
		{"-task-id", " task-v3", "-idempotency-key", "key"},
		{"-task-id", "task-v3", "-idempotency-key", " key"},
	} {
		if _, err := parseShadowArgs(arguments); err == nil {
			t.Fatalf("invalid arguments accepted: %v", arguments)
		}
	}
}

func TestShadowCommandWaitsForDurableDeliveryDarkProof(t *testing.T) {
	payload, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(payload)
	for _, required := range []string{
		"TriggerResearchShadowNowAndWait", "RequireSuccessfulResearchV3ShadowPreflight",
		"ResolveResearchV3OperatorScope", "researchoperator.RequireExactTask",
	} {
		if !strings.Contains(source, required) {
			t.Fatalf("shadow closure is missing %q", required)
		}
	}
	for _, forbidden := range []string{"config.Load", "store.NewServerRuntime", "-user-id"} {
		if strings.Contains(source, forbidden) {
			t.Fatalf("shadow command contains forbidden runtime surface %q", forbidden)
		}
	}
}
