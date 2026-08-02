package main

import "testing"

func TestParseShadowArgsRequiresExactOperatorScope(t *testing.T) {
	got, err := parseShadowArgs([]string{
		"-task-id", "task-v3", "-user-id", "42",
		"-idempotency-key", "shadow-2026-08-01-1",
	})
	if err != nil || got.taskID != "task-v3" || got.userID != 42 ||
		got.idempotencyKey != "shadow-2026-08-01-1" {
		t.Fatalf("args=%+v err=%v", got, err)
	}
	for _, arguments := range [][]string{
		{},
		{"-task-id", "task-v3", "-user-id", "0", "-idempotency-key", "key"},
		{"-task-id", " task-v3", "-user-id", "42", "-idempotency-key", "key"},
		{"-task-id", "task-v3", "-user-id", "42", "-idempotency-key", " key"},
	} {
		if _, err := parseShadowArgs(arguments); err == nil {
			t.Fatalf("invalid arguments accepted: %v", arguments)
		}
	}
}
