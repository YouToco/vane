package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseCutoverArgs(t *testing.T) {
	for _, operation := range []string{"cutover", "rollback"} {
		got, err := parseCutoverArgs([]string{
			"-operation", operation, "-task-id", "task-v3",
			"-user-id", "42", "-idempotency-key", "gate-1-attempt",
		})
		if err != nil || got.operation != operation || got.taskID != "task-v3" ||
			got.userID != 42 || got.idempotencyKey != "gate-1-attempt" {
			t.Fatalf("operation=%s got=%+v err=%v", operation, got, err)
		}
	}
}

func TestParseCutoverArgsRejectsUnsafeScope(t *testing.T) {
	tests := [][]string{
		{"-operation", "delete", "-task-id", "task-v3", "-user-id", "42", "-idempotency-key", "key"},
		{"-operation", "cutover", "-task-id", "task-other ", "-user-id", "42", "-idempotency-key", "key"},
		{"-operation", "cutover", "-task-id", "task-v3", "-user-id", "0", "-idempotency-key", "key"},
		{"-operation", "rollback", "-task-id", "task-v3", "-user-id", "42", "-idempotency-key", " "},
	}
	for _, args := range tests {
		if _, err := parseCutoverArgs(args); err == nil {
			t.Fatalf("unsafe args accepted: %v", args)
		}
	}
}

func TestMigrationOwnerDatabaseURL(t *testing.T) {
	t.Run("explicit environment wins", func(t *testing.T) {
		t.Setenv(migrationOwnerDatabaseURLEnv, " postgres://owner/explicit ")
		t.Setenv(migrationOwnerCredentialDirectory, "")
		got, err := migrationOwnerDatabaseURL()
		if err != nil || got != "postgres://owner/explicit" {
			t.Fatalf("url=%q err=%v", got, err)
		}
	})

	t.Run("systemd credential fallback", func(t *testing.T) {
		t.Setenv(migrationOwnerDatabaseURLEnv, " ")
		directory := t.TempDir()
		if err := os.WriteFile(filepath.Join(directory, migrationOwnerDatabaseCredential),
			[]byte(" postgres://owner/credential\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		t.Setenv(migrationOwnerCredentialDirectory, directory)
		got, err := migrationOwnerDatabaseURL()
		if err != nil || got != "postgres://owner/credential" {
			t.Fatalf("url=%q err=%v", got, err)
		}
	})

	for _, test := range []struct {
		name      string
		directory string
		write     bool
		payload   string
	}{
		{name: "missing directory"},
		{name: "missing file", directory: "temp"},
		{name: "empty credential", directory: "temp", write: true, payload: " \n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv(migrationOwnerDatabaseURLEnv, "")
			directory := test.directory
			if directory == "temp" {
				directory = t.TempDir()
			}
			if test.write {
				if err := os.WriteFile(filepath.Join(directory, migrationOwnerDatabaseCredential),
					[]byte(test.payload), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			t.Setenv(migrationOwnerCredentialDirectory, directory)
			if _, err := migrationOwnerDatabaseURL(); err == nil {
				t.Fatal("unsafe migration-owner credential was accepted")
			}
		})
	}
}

func TestCutoverCommandNeverUsesLongLivedServerRuntime(t *testing.T) {
	payload, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(payload)
	if strings.Contains(source, "store.NewServerRuntime") {
		t.Fatal("one-shot cutover command regained long-lived server runtime authority")
	}
	for _, required := range []string{
		"migrationOwnerDatabaseURL()", "store.New(ctx, operatorDatabaseURL)",
		"VANE_MIGRATION_DB_URL", "migration_db_url",
	} {
		if !strings.Contains(source, required) {
			t.Fatalf("one-shot cutover credential boundary is missing %q", required)
		}
	}
}
