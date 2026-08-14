package main

import (
	"os"
	"strings"
	"testing"
)

func TestParseArgsRequiresExplicitPreparePolicy(t *testing.T) {
	_, err := parseArgs([]string{"-operation", "prepare", "-task-id", "task", "-idempotency-key", "k"})
	if err == nil {
		t.Fatal("prepare without explicit policy succeeded")
	}
	a, err := parseArgs([]string{"-operation", "prepare", "-task-id", "task", "-idempotency-key", "k", "-policy-file", "policy.json"})
	if err != nil || a.policyFile != "policy.json" {
		t.Fatalf("args=%+v err=%v", a, err)
	}
	if _, err := parseArgs([]string{"-operation", "rollback", "-task-id", "task", "-idempotency-key", "k", "-policy-file", "policy.json"}); err == nil {
		t.Fatal("rollback accepted policy mutation")
	}
	if _, err := parseArgs([]string{"-operation", "prepare", "-task-id", "task", "-user-id", "7", "-idempotency-key", "k", "-policy-file", "policy.json"}); err == nil {
		t.Fatal("operator command accepted caller-supplied user scope")
	}
}

func TestPrepareCommandUsesExactDatabaseResolvedScopeOnly(t *testing.T) {
	payload, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(payload)
	for _, required := range []string{
		"researchoperator.RequireExactTask", "ResolveResearchV3OperatorScope",
		"researchoperator.MigrationDatabaseURL()",
	} {
		if !strings.Contains(source, required) {
			t.Fatalf("prepare boundary is missing %q", required)
		}
	}
	for _, forbidden := range []string{"config.Load", "store.NewServerRuntime", "-user-id"} {
		if strings.Contains(source, forbidden) {
			t.Fatalf("prepare command contains forbidden runtime surface %q", forbidden)
		}
	}
}
