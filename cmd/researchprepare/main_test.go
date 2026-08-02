package main

import "testing"

func TestParseArgsRequiresExplicitPreparePolicy(t *testing.T) {
	_, err := parseArgs([]string{"-operation", "prepare", "-task-id", "task", "-user-id", "7", "-idempotency-key", "k"})
	if err == nil {
		t.Fatal("prepare without explicit policy succeeded")
	}
	a, err := parseArgs([]string{"-operation", "prepare", "-task-id", "task", "-user-id", "7", "-idempotency-key", "k", "-policy-file", "policy.json"})
	if err != nil || a.policyFile != "policy.json" {
		t.Fatalf("args=%+v err=%v", a, err)
	}
	if _, err := parseArgs([]string{"-operation", "rollback", "-task-id", "task", "-user-id", "7", "-idempotency-key", "k", "-policy-file", "policy.json"}); err == nil {
		t.Fatal("rollback accepted policy mutation")
	}
}
