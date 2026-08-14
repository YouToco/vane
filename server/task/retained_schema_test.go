package task

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

var retainedTaskSchemaURL string

// TestMain gives only the retained V1 compatibility cases an isolated
// schema-131 database whenever PostgreSQL integration tests are enabled. The
// V3 cases continue to run against the latest production schema. The package
// intentionally retains V1 creation fixtures to prove replay and V3/V1
// journal separation, while migration 132 correctly makes constructing those
// historical rows impossible in the production schema. Dedicated store
// migration tests cover the frozen schema; these retained compatibility tests
// must never weaken that fence.
func TestMain(m *testing.M) {
	databaseURL := os.Getenv("VANE_TEST_DATABASE_URL")
	if databaseURL == "" {
		databaseURL = os.Getenv("DATABASE_URL")
	}
	if databaseURL == "" {
		os.Exit(m.Run())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	scratchURL, cleanup, err := retainedTaskSchemaDatabase(ctx, databaseURL)
	cancel()
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "prepare retained task schema: %v\n", err)
		os.Exit(1)
	}
	retainedTaskSchemaURL = scratchURL

	code := m.Run()
	cleanup()
	os.Exit(code)
}

func retainedTaskSchemaDatabase(
	ctx context.Context,
	databaseURL string,
) (string, func(), error) {
	admin, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return "", func() {}, fmt.Errorf("open database admin connection: %w", err)
	}
	defer admin.Close()

	parsed, err := url.Parse(databaseURL)
	if err != nil {
		return "", func() {}, fmt.Errorf("parse database URL: %w", err)
	}
	name := "vane_task_retained_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.ExecContext(ctx, `CREATE DATABASE `+quoteTaskTestIdentifier(name)); err != nil {
		return "", func() {}, fmt.Errorf("create scratch database: %w", err)
	}

	parsed.Path = "/" + name
	scratchURL := parsed.String()
	cleanup := func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cleanupDB, openErr := sql.Open("pgx", databaseURL)
		if openErr != nil {
			_, _ = fmt.Fprintf(os.Stderr, "open retained task cleanup connection: %v\n", openErr)
			return
		}
		defer cleanupDB.Close()
		if _, dropErr := cleanupDB.ExecContext(cleanupCtx,
			`DROP DATABASE IF EXISTS `+quoteTaskTestIdentifier(name)+` WITH (FORCE)`); dropErr != nil {
			_, _ = fmt.Fprintf(os.Stderr, "drop retained task database %s: %v\n", name, dropErr)
		}
	}

	database, err := sql.Open("pgx", scratchURL)
	if err != nil {
		cleanup()
		return "", func() {}, fmt.Errorf("open scratch database: %w", err)
	}
	defer database.Close()
	migrations, err := fs.Sub(os.DirFS("../store"), "migrations")
	if err != nil {
		cleanup()
		return "", func() {}, fmt.Errorf("open Store migrations: %w", err)
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, database, migrations)
	if err != nil {
		cleanup()
		return "", func() {}, fmt.Errorf("initialize migration provider: %w", err)
	}
	if _, err := provider.UpTo(ctx, 131); err != nil {
		cleanup()
		return "", func() {}, fmt.Errorf("migrate retained task schema to 131: %w", err)
	}
	return scratchURL, cleanup, nil
}

func quoteTaskTestIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}
