package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/YouToco/vane/server/store"
)

const (
	migrationDatabaseURLEnv        = "VANE_MIGRATION_DB_URL"
	migrationDatabaseCredentialEnv = "CREDENTIALS_DIRECTORY"
	migrationDatabaseCredential    = "migration_db_url"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "vane-migrate:", err)
		os.Exit(1)
	}
}

func run() error {
	databaseURL, err := migrationDatabaseURL()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	return migrateAndProvision(
		ctx, databaseURL, store.Migrate, provisionRuntimeBoundaries)
}

func provisionRuntimeBoundaries(ctx context.Context, databaseURL string) error {
	if err := store.ProvisionServerRuntime(ctx, databaseURL); err != nil {
		return err
	}
	return store.ProvisionNativeV3EditRecoveryRuntime(ctx, databaseURL)
}

func migrateAndProvision(
	ctx context.Context, databaseURL string,
	migrate func(context.Context, string) error,
	provision func(context.Context, string) error,
) error {
	if err := migrate(ctx, databaseURL); err != nil {
		return fmt.Errorf("database migration failed: %w", err)
	}
	// Cluster-global runtime membership is intentionally installed only after
	// the complete target schema succeeds. Ordinary store.Migrate callers and
	// scratch databases must remain schema-only.
	if err := provision(ctx, databaseURL); err != nil {
		return fmt.Errorf("server runtime provision failed: %w", err)
	}
	return nil
}

func migrationDatabaseURL() (string, error) {
	if value := strings.TrimSpace(os.Getenv(migrationDatabaseURLEnv)); value != "" {
		return value, nil
	}
	directory := strings.TrimSpace(os.Getenv(migrationDatabaseCredentialEnv))
	if directory == "" {
		return "", errors.New("migration database credential is unavailable")
	}
	payload, err := os.ReadFile(directory + string(os.PathSeparator) + migrationDatabaseCredential)
	if err != nil {
		return "", errors.New("read migration database credential")
	}
	value := strings.TrimSpace(string(payload))
	if value == "" {
		return "", errors.New("migration database credential is empty")
	}
	return value, nil
}
