package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseOptionsRequiresBaselineAndCanonicalAuthority(t *testing.T) {
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
	for _, mutation := range [][]string{
		append([]string(nil), arguments[1:]...),
		append(append([]string(nil), arguments...), "extra"),
		append([]string(nil), arguments[:6]...),
	} {
		if _, err := parseOptions(mutation); err == nil {
			t.Fatal("invalid baseline options accepted")
		}
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
	if err := os.Chmod(filepath.Join(directory, migrationDatabaseCredential), 0o666); err != nil {
		t.Fatal(err)
	}
	if _, err := migrationDatabaseURL(); err == nil {
		t.Fatal("unsafe credential mode accepted")
	}
	if err := os.Chmod(filepath.Join(directory, migrationDatabaseCredential), 0o600); err != nil {
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
