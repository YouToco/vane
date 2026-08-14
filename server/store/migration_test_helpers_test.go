package store

import (
	"context"
	"database/sql"
	"net/url"
	"strings"
	"testing"

	"github.com/google/uuid"
)

// createScratchDB creates an isolated database for migration integration tests.
func createScratchDB(
	ctx context.Context,
	t *testing.T,
	databaseURL string,
) (string, func()) {
	t.Helper()
	admin, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatalf("open migration admin connection: %v", err)
	}
	defer admin.Close()

	name := "vane_migration_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.ExecContext(
		ctx, `CREATE DATABASE `+pgQuoteIdent(name),
	); err != nil {
		requireCreateDatabaseCapability(t, err)
	}

	parsed, err := url.Parse(databaseURL)
	if err != nil {
		t.Fatalf("parse DATABASE_URL: %v", err)
	}
	parsed.Path = "/" + name

	return parsed.String(), func() {
		cleanup, err := sql.Open("pgx", databaseURL)
		if err != nil {
			t.Logf("open cleanup connection for %s: %v", name, err)
			return
		}
		defer cleanup.Close()
		if _, err := cleanup.ExecContext(
			context.WithoutCancel(ctx),
			`DROP DATABASE IF EXISTS `+pgQuoteIdent(name)+` WITH (FORCE)`,
		); err != nil {
			t.Logf("drop scratch database %s: %v", name, err)
		}
	}
}

func pgQuoteIdent(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

func mustQueryRow(
	ctx context.Context,
	t *testing.T,
	database *sql.DB,
	query string,
	destination any,
	args ...any,
) {
	t.Helper()
	if err := database.QueryRowContext(
		ctx, query, args...,
	).Scan(destination); err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
}

func mustExec(
	ctx context.Context,
	t *testing.T,
	database *sql.DB,
	query string,
	args ...any,
) {
	t.Helper()
	if _, err := database.ExecContext(ctx, query, args...); err != nil {
		t.Fatalf("execute %q: %v", query, err)
	}
}
