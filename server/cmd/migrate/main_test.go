package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestRunRejectsUnknownSubcommandBeforeDatabaseAccess(t *testing.T) {
	if err := run([]string{"unknown"}); err == nil ||
		!strings.Contains(err.Error(), "unsupported vane-migrate subcommand") {
		t.Fatalf("unknown subcommand err=%v", err)
	}
}

func TestMigrationDatabaseURLUsesEnvironmentThenSystemdCredential(t *testing.T) {
	t.Setenv(migrationDatabaseURLEnv, " postgres://owner/environment ")
	if got, err := migrationDatabaseURL(); err != nil || got != "postgres://owner/environment" {
		t.Fatalf("environment URL=%q err=%v", got, err)
	}
	t.Setenv(migrationDatabaseURLEnv, "")
	directory := t.TempDir()
	t.Setenv(migrationDatabaseCredentialEnv, directory)
	if err := os.WriteFile(filepath.Join(directory, migrationDatabaseCredential),
		[]byte(" postgres://owner/credential \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got, err := migrationDatabaseURL(); err != nil || got != "postgres://owner/credential" {
		t.Fatalf("credential URL=%q err=%v", got, err)
	}
	if err := os.WriteFile(filepath.Join(directory, migrationDatabaseCredential), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := migrationDatabaseURL(); err == nil {
		t.Fatal("empty migration credential passed")
	}
}

func TestMigrateAndProvisionOrdersClusterIdentityAfterSchema(t *testing.T) {
	var calls []string
	step := func(name string, result error) func(context.Context, string) error {
		return func(_ context.Context, databaseURL string) error {
			if databaseURL != "postgres://owner/target" {
				t.Fatalf("unexpected database URL %q", databaseURL)
			}
			calls = append(calls, name)
			return result
		}
	}

	if err := migrateAndProvision(t.Context(), "postgres://owner/target",
		step("schema", nil), step("provision", nil)); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(calls, []string{"schema", "provision"}) {
		t.Fatalf("steps=%v", calls)
	}

	calls = nil
	schemaErr := errors.New("schema failed")
	err := migrateAndProvision(t.Context(), "postgres://owner/target",
		step("schema", schemaErr), step("provision", nil))
	if !errors.Is(err, schemaErr) || !reflect.DeepEqual(calls, []string{"schema"}) {
		t.Fatalf("schema failure err=%v calls=%v", err, calls)
	}

	calls = nil
	provisionErr := errors.New("provision failed")
	err = migrateAndProvision(t.Context(), "postgres://owner/target",
		step("schema", nil), step("provision", provisionErr))
	if !errors.Is(err, provisionErr) ||
		!strings.Contains(err.Error(), "server runtime provision failed") ||
		!reflect.DeepEqual(calls, []string{"schema", "provision"}) {
		t.Fatalf("provision failure err=%v calls=%v", err, calls)
	}
}
