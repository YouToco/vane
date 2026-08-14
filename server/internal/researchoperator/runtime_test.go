package researchoperator

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRequireExactTaskIsTransientAndExact(t *testing.T) {
	t.Setenv(ExactTaskIDEnv, "task-one")
	if err := RequireExactTask("task-one"); err != nil {
		t.Fatal(err)
	}
	for _, taskID := range []string{"task-two", " task-one", ""} {
		if err := RequireExactTask(taskID); err == nil {
			t.Fatalf("task %q escaped exact process authority", taskID)
		}
	}
}

func TestMigrationDatabaseURLUsesOnlyOperatorCredential(t *testing.T) {
	t.Run("explicit", func(t *testing.T) {
		t.Setenv(MigrationDatabaseURLEnv, " postgres://owner/explicit ")
		got, err := MigrationDatabaseURL()
		if err != nil || got != "postgres://owner/explicit" {
			t.Fatalf("url=%q err=%v", got, err)
		}
	})
	t.Run("credential", func(t *testing.T) {
		t.Setenv(MigrationDatabaseURLEnv, "")
		directory := t.TempDir()
		if err := os.WriteFile(filepath.Join(directory, MigrationDatabaseCredential),
			[]byte("postgres://owner/credential\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		t.Setenv(CredentialDirectoryEnv, directory)
		got, err := MigrationDatabaseURL()
		if err != nil || got != "postgres://owner/credential" {
			t.Fatalf("url=%q err=%v", got, err)
		}
	})
	t.Run("missing", func(t *testing.T) {
		t.Setenv(MigrationDatabaseURLEnv, "")
		t.Setenv(CredentialDirectoryEnv, "")
		if _, err := MigrationDatabaseURL(); err == nil {
			t.Fatal("missing operator credential succeeded")
		}
	})
}

func TestLoadTemporalConfigRejectsBroadOrMalformedValues(t *testing.T) {
	for _, name := range []string{TemporalHostEnv, TemporalNamespaceEnv, TemporalTaskQueueEnv} {
		t.Run(name, func(t *testing.T) {
			t.Setenv(name, " value ")
			if _, err := LoadTemporalConfig(); err == nil {
				t.Fatal("malformed Temporal operator configuration succeeded")
			}
		})
	}
}
