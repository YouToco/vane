package store

import (
	"context"
	"database/sql"
	"io/fs"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pressly/goose/v3"
)

// Runtime projection was retired by migration 128, but migration 056 remains
// immutable history. Keep its durable-row downgrade fence under a real
// PostgreSQL test so deleting the Go consumer never weakens old schema replay.
func TestMigration056DownStillRefusesDurableFactsPostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		requireDatabaseCapability(t)
	}
	scratchURL, drop := createScratchDB(t.Context(), t, databaseURL)
	t.Cleanup(drop)
	database, err := sql.Open("pgx", scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	dir, err := fs.Sub(migrationsFS, "migrations")
	if err != nil {
		t.Fatal(err)
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, database, dir,
		goose.WithAllowOutofOrder(true))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.UpTo(t.Context(), 56); err != nil {
		t.Fatal(err)
	}
	var tenantID, userID, sessionID int64
	if err := database.QueryRowContext(t.Context(),
		`INSERT INTO tenants DEFAULT VALUES RETURNING id`).Scan(&tenantID); err != nil {
		t.Fatal(err)
	}
	if err := database.QueryRowContext(t.Context(), `
		INSERT INTO users(feishu_open_id,name)
		VALUES('migration-056-retained','owner') RETURNING id`).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	if err := database.QueryRowContext(t.Context(), `
		INSERT INTO agent_sessions(tenant_id,user_id)
		VALUES($1,$2) RETURNING id`, tenantID, userID).Scan(&sessionID); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), `
		INSERT INTO agent_session_fact_outbox(
		 tenant_id,user_id,fact_type,fact_id,source_identity,session_id,
		 session_messages,payload_digest,status)
		VALUES($1,$2,'feedback',1,'feedback-click:1',$3,
		       '[{"role":"user","content":"x"}]'::bytea,
		       repeat('a',64),'pending')`, tenantID, userID, sessionID); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.DownTo(t.Context(), 55); err == nil ||
		!strings.Contains(err.Error(),
			"refusing downgrade while Agent continuation facts exist") {
		t.Fatalf("migration 056 durable history fence err=%v", err)
	}
}

func TestMigration056EmptyDownStillWaitsForProfileTransitionLockPostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		requireDatabaseCapability(t)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	scratchURL, drop := createScratchDB(ctx, t, databaseURL)
	t.Cleanup(drop)
	migrationDB, err := sql.Open("pgx", scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = migrationDB.Close() })
	dir, err := fs.Sub(migrationsFS, "migrations")
	if err != nil {
		t.Fatal(err)
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, migrationDB, dir,
		goose.WithAllowOutofOrder(true))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.UpTo(ctx, 56); err != nil {
		t.Fatal(err)
	}
	lockTx, err := migrationDB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lockTx.ExecContext(ctx,
		`SELECT pg_advisory_xact_lock($1,$2)`,
		agentSessionFactAdmissionClass, agentSessionFactAdmissionKey); err != nil {
		_ = lockTx.Rollback()
		t.Fatal(err)
	}
	defer lockTx.Rollback() //nolint:errcheck

	downDB, err := sql.Open("pgx", scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = downDB.Close() })
	downProvider, err := goose.NewProvider(goose.DialectPostgres, downDB, dir,
		goose.WithAllowOutofOrder(true))
	if err != nil {
		t.Fatal(err)
	}
	downDone := make(chan error, 1)
	go func() {
		_, err := downProvider.DownTo(ctx, 53)
		downDone <- err
	}()
	select {
	case err := <-downDone:
		t.Fatalf("migration 056 Down bypassed retained admission lock: %v", err)
	case <-time.After(150 * time.Millisecond):
	}
	if err := lockTx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := <-downDone; err != nil {
		t.Fatalf("migration 056 Down after admission release: %v", err)
	}
}
