package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/YouToco/vane/server/store"
)

func TestParseOptionsRequiresPhaseAndCanonicalAuthority(t *testing.T) {
	directory := t.TempDir()
	arguments := []string{
		"baseline", "--temporal-host", "127.0.0.1:7233",
		"--temporal-namespace", "vane", "--temporal-task-queue", "vane",
		"--operation-id", "123e4567-e89b-42d3-a456-426614174000",
		"--release-receipt", filepath.Join(directory, "receipt.json"),
		"--evidence-directory", filepath.Join(directory, "evidence"),
		"--live-vane-binary", filepath.Join(directory, "vane"),
	}
	if _, err := parseOptions(arguments); err != nil {
		t.Fatal(err)
	}
	prime := append([]string{"prime-clock"}, arguments[1:]...)
	if parsed, err := parseOptions(prime); err != nil || parsed.command != "prime-clock" {
		t.Fatalf("prime=%+v err=%v", parsed, err)
	}
	prepared := append([]string{"prepared"}, arguments[1:]...)
	prepared = append(prepared, "--parent-digest", strings.Repeat("a", 64))
	if parsed, err := parseOptions(prepared); err != nil ||
		parsed.command != "prepared" || parsed.parentDigest != strings.Repeat("a", 64) {
		t.Fatalf("prepared=%+v err=%v", parsed, err)
	}
	for _, mutation := range [][]string{
		append([]string(nil), arguments[1:]...),
		append(append([]string(nil), arguments...), "extra"),
		append([]string(nil), arguments[:6]...),
		prepared[:len(prepared)-2],
		append(append([]string(nil), arguments...), "--parent-digest", strings.Repeat("b", 64)),
	} {
		if _, err := parseOptions(mutation); err == nil {
			t.Fatal("invalid retention options accepted")
		}
	}
}

func TestRetentionNotBeforeUsesLaterDatabaseAndTemporalClock(t *testing.T) {
	issued := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	for name, witness := range map[string]time.Time{
		"database": issued.Add(-10 * time.Minute),
		"temporal": issued.Add(5 * time.Second),
	} {
		t.Run(name, func(t *testing.T) {
			event := &store.AgentFirstRetentionAttestationEvent{
				IssuedAt: issued, TemporalServerWitness: witness, RetentionSeconds: 86400,
			}
			expected := issued.Add(24 * time.Hour)
			if witness.After(issued) {
				expected = witness.Add(24 * time.Hour)
			}
			if got := retentionNotBefore(event); !got.Equal(expected) {
				t.Fatalf("not before=%s expected=%s", got, expected)
			}
		})
	}
}

func TestMigrationDatabaseURLUsesOnlyCredentialDirectory(t *testing.T) {
	directory := t.TempDir()
	t.Setenv(migrationCredentialDirectoryEnv, directory)
	t.Setenv("VANE_MIGRATION_DB_URL", "postgres://must-not-be-used")
	if err := os.WriteFile(filepath.Join(directory, migrationDatabaseCredential),
		[]byte("postgres://owner\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	value, err := migrationDatabaseURL()
	if err != nil || value != "postgres://owner" {
		t.Fatalf("value=%q err=%v", value, err)
	}
	credential := filepath.Join(directory, migrationDatabaseCredential)
	for _, mode := range []os.FileMode{0o644, 0o444, 0o666} {
		if err := os.Chmod(credential, mode); err != nil {
			t.Fatal(err)
		}
		if _, err := migrationDatabaseURL(); err == nil {
			t.Fatalf("unsafe credential mode accepted: %04o", mode)
		}
	}
	if err := os.Chmod(credential, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(directory, 0o777); err != nil {
		t.Fatal(err)
	}
	if _, err := migrationDatabaseURL(); err == nil {
		t.Fatal("unsafe credential directory mode accepted")
	}
}

func TestCanonicalUUIDRejectsNonCanonicalOrWrongVariant(t *testing.T) {
	if !canonicalUUID("123e4567-e89b-42d3-a456-426614174000") {
		t.Fatal("canonical UUID rejected")
	}
	for _, value := range []string{
		"123E4567-E89B-42D3-A456-426614174000",
		"123e4567-e89b-02d3-a456-426614174000",
		strings.Repeat("a", 36),
	} {
		if canonicalUUID(value) {
			t.Fatalf("invalid UUID accepted: %q", value)
		}
	}
}
