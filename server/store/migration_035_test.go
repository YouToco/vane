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

func migration035Scratch(t *testing.T) (*sql.DB, *goose.Provider) {
	t.Helper()
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		requireDatabaseCapability(t)
	}
	scratchURL, drop := createScratchDB(t.Context(), t, dbURL)
	t.Cleanup(drop)
	db, err := sql.Open("pgx", scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	dir, err := fs.Sub(migrationsFS, "migrations")
	if err != nil {
		t.Fatal(err)
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, db, dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.UpTo(t.Context(), 35); err != nil {
		t.Fatalf("迁移到 035 失败: %v", err)
	}
	return db, provider
}

func TestMigration035EmptyLedgerCanDowngrade(t *testing.T) {
	db, provider := migration035Scratch(t)
	var payloadInsert, idInsert, createdAtInsert, sequenceUsage, sequenceSelect bool
	if err := db.QueryRowContext(t.Context(), `
		SELECT
		  has_column_privilege('vane_app','agent_events','payload','INSERT'),
		  has_column_privilege('vane_app','agent_events','id','INSERT'),
		  has_column_privilege('vane_app','agent_events','created_at','INSERT'),
		  has_sequence_privilege('vane_app','agent_events_id_seq','USAGE'),
		  has_sequence_privilege('vane_app','agent_events_id_seq','SELECT')`,
	).Scan(
		&payloadInsert, &idInsert, &createdAtInsert,
		&sequenceUsage, &sequenceSelect,
	); err != nil {
		t.Fatal(err)
	}
	if !payloadInsert || idInsert || createdAtInsert ||
		!sequenceUsage || sequenceSelect {
		t.Fatalf("append-only grants drifted: payload_insert=%v id_insert=%v "+
			"created_at_insert=%v sequence_usage=%v sequence_select=%v",
			payloadInsert, idInsert, createdAtInsert,
			sequenceUsage, sequenceSelect)
	}
	if _, err := provider.Down(t.Context()); err != nil {
		t.Fatalf("空 agent_events 应可回滚 035: %v", err)
	}
	var exists bool
	if err := db.QueryRowContext(t.Context(),
		`SELECT to_regclass('public.agent_events') IS NOT NULL`,
	).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("035 Down 后 agent_events 仍存在")
	}
}

func TestMigration035RefusesEventDataDowngrade(t *testing.T) {
	db, provider := migration035Scratch(t)
	ctx := t.Context()
	payload := `{"schema_version":"vane.agent-event/v1","kind":"user_message","body":{"text":"keep"}}`
	if _, err := db.ExecContext(ctx, `
		WITH created_user AS (
			INSERT INTO users (feishu_open_id, name)
			VALUES ('migration-035-user', 'migration 035') RETURNING id
		), created_session AS (
			INSERT INTO agent_sessions (tenant_id, user_id)
			SELECT 1, id FROM created_user
			RETURNING id, user_id
		)
		INSERT INTO agent_events (
			tenant_id, user_id, session_id, sequence,
			batch_idempotency_key, batch_index, batch_size,
			kind, schema_version, payload, payload_digest, batch_digest
		)
		SELECT 1, user_id, id, 1, 'migration-035-event', 0, 1,
		       'user_message', 'vane.agent-event/v1',
		       convert_to($1, 'UTF8'),
		       encode(sha256(convert_to($1, 'UTF8')), 'hex'),
		       repeat('a', 64)
		  FROM created_session`, payload); err != nil {
		t.Fatal(err)
	}

	if _, err := provider.Down(ctx); err == nil ||
		!strings.Contains(err.Error(), "refusing downgrade") {
		t.Fatalf("非空 event ledger 必须拒绝回滚: %v", err)
	}
	var version, rows int
	var exists bool
	if err := db.QueryRowContext(ctx, `
		SELECT
		  (SELECT COALESCE(max(version_id), 0)
		     FROM goose_db_version WHERE is_applied),
		  (SELECT count(*) FROM agent_events),
		  to_regclass('public.agent_events') IS NOT NULL`,
	).Scan(&version, &rows, &exists); err != nil {
		t.Fatal(err)
	}
	if version != 35 || rows != 1 || !exists {
		t.Fatalf("拒绝回滚必须原子保留版本/表/行: version=%d rows=%d exists=%v",
			version, rows, exists)
	}
	if _, err := db.ExecContext(ctx,
		`UPDATE agent_events SET batch_idempotency_key='bad key'`); err == nil {
		t.Fatal("database accepted idempotency bytes rejected by the Store grammar")
	}

	if _, err := db.ExecContext(ctx, `DELETE FROM agent_events`); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Down(ctx); err != nil {
		t.Fatalf("清空 event ledger 后应可回滚 035: %v", err)
	}
}

func TestMigration035DownSerializesBeforeEmptyCheck(t *testing.T) {
	db, provider := migration035Scratch(t)
	ctx := t.Context()

	var userID, sessionID int64
	if err := db.QueryRowContext(ctx, `
		INSERT INTO users (feishu_open_id, name)
		VALUES ('migration-035-race-user', 'migration 035 race')
		RETURNING id`,
	).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `
		INSERT INTO agent_sessions (tenant_id, user_id)
		VALUES (1, $1) RETURNING id`, userID,
	).Scan(&sessionID); err != nil {
		t.Fatal(err)
	}

	// Keep a valid append uncommitted on one physical connection. It holds a
	// RowExclusiveLock but is invisible to another transaction's EXISTS.
	appendTx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	appendDone := false
	defer func() {
		if !appendDone {
			_ = appendTx.Rollback()
		}
	}()
	payload := `{"schema_version":"vane.agent-event/v1","kind":"user_message","body":{"text":"concurrent"}}`
	if _, err := appendTx.ExecContext(ctx, `
		INSERT INTO agent_events (
			tenant_id, user_id, session_id, sequence,
			batch_idempotency_key, batch_index, batch_size,
			kind, schema_version, payload, payload_digest, batch_digest
		) VALUES (
			1, $1, $2, 1, 'migration-035-race', 0, 1,
			'user_message', 'vane.agent-event/v1',
			convert_to($3, 'UTF8'),
			encode(sha256(convert_to($3, 'UTF8')), 'hex'),
			repeat('b', 64)
		)`, userID, sessionID, payload); err != nil {
		t.Fatal(err)
	}

	downDone := make(chan error, 1)
	go func() {
		_, downErr := provider.Down(ctx)
		downDone <- downErr
	}()

	// The fixed migration must block at its explicit pre-check LOCK, not later
	// at DROP TABLE. Without that statement the old migration reaches the empty
	// EXISTS, then blocks at DROP; committing below would let it silently delete
	// the newly visible row.
	lockObserved := waitForMigration035DowngradeFence(ctx, db, 5*time.Second)
	if err := appendTx.Commit(); err != nil {
		t.Fatal(err)
	}
	appendDone = true

	var downErr error
	select {
	case downErr = <-downDone:
	case <-time.After(10 * time.Second):
		t.Fatal("035 Down did not converge after concurrent append committed")
	}
	if !lockObserved {
		t.Fatal("035 Down never waited at the pre-check ACCESS EXCLUSIVE fence")
	}
	if downErr == nil || !strings.Contains(downErr.Error(), "refusing downgrade") {
		t.Fatalf("035 Down silently accepted a concurrent append: %v", downErr)
	}

	var version, rows int
	var exists bool
	if err := db.QueryRowContext(ctx, `
		SELECT
		  (SELECT COALESCE(max(version_id), 0)
		     FROM goose_db_version WHERE is_applied),
		  (SELECT count(*) FROM agent_events),
		  to_regclass('public.agent_events') IS NOT NULL`,
	).Scan(&version, &rows, &exists); err != nil {
		t.Fatal(err)
	}
	if version != 35 || rows != 1 || !exists {
		t.Fatalf("concurrent append was not atomically preserved: version=%d rows=%d exists=%v",
			version, rows, exists)
	}

	if _, err := db.ExecContext(ctx, `DELETE FROM agent_events`); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Down(ctx); err != nil {
		t.Fatalf("empty ledger should downgrade after concurrent Gate: %v", err)
	}
}

func waitForMigration035DowngradeFence(
	ctx context.Context,
	db *sql.DB,
	timeout time.Duration,
) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var waiting bool
		err := db.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT 1
				  FROM pg_stat_activity
				 WHERE datname=current_database()
				   AND pid<>pg_backend_pid()
				   AND wait_event_type='Lock'
				   AND query LIKE '%migration 035 downgrade fence%'
			)`,
		).Scan(&waiting)
		if err == nil && waiting {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}
