package store

import (
	"context"
	"database/sql"
	"io/fs"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"
)

func TestMigration079KeepsManualAuthorityNarrowAndFailClosed(t *testing.T) {
	raw, err := os.ReadFile("migrations/079_manual_task_run_authority.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(raw)
	required := []string{
		"CREATE FUNCTION authorize_manual_task_run_v1(",
		"SECURITY DEFINER",
		"expected_workflow_id = 'wf-manual-' || c.id::text",
		"c.tenant_id = expected_tenant_id",
		"c.user_id = expected_user_id",
		"c.task_id = expected_task_id",
		"c.kind = 'run'",
		"c.status IN ('pending', 'completed')",
		"REVOKE ALL ON FUNCTION authorize_manual_task_run_v1(",
		"TO vane_app, vane_push_effect_coordinator",
		"CREATE OR REPLACE FUNCTION task_run_snapshot_v2_admission_fence()",
		"schedule_status = 'paused'",
		"public.authorize_manual_task_run_v1(",
		"079: manual task run authority migration is irreversible",
	}
	for _, fragment := range required {
		if !strings.Contains(sql, fragment) {
			t.Fatalf("migration 079 omitted %q", fragment)
		}
	}
	for _, forbidden := range []string{
		"GRANT SELECT ON schedule_commands TO vane_app",
		"GRANT SELECT ON schedule_commands TO vane_push_effect_coordinator",
	} {
		if strings.Contains(sql, forbidden) {
			t.Fatalf("migration 079 widened command-table access with %q",
				forbidden)
		}
	}
}

func TestMigration079ManualAuthorityMatrix(t *testing.T) {
	st := tenantTestStore(t)
	ctx := t.Context()
	var tenantID, otherTenantID, userID, otherUserID int64
	if err := st.pool.QueryRow(ctx,
		`INSERT INTO tenants DEFAULT VALUES RETURNING id`,
	).Scan(&tenantID); err != nil {
		t.Fatal(err)
	}
	if err := st.pool.QueryRow(ctx,
		`INSERT INTO tenants DEFAULT VALUES RETURNING id`,
	).Scan(&otherTenantID); err != nil {
		t.Fatal(err)
	}
	if err := st.pool.QueryRow(ctx,
		`INSERT INTO users (feishu_open_id,name)
		 VALUES ($1,'migration 079 owner') RETURNING id`,
		"migration-079-"+uuid.NewString(),
	).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	if err := st.pool.QueryRow(ctx,
		`INSERT INTO users (feishu_open_id,name)
		 VALUES ($1,'migration 079 other') RETURNING id`,
		"migration-079-"+uuid.NewString(),
	).Scan(&otherUserID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupExec(context.Background(), t, st,
			`DELETE FROM schedule_commands WHERE tenant_id IN ($1,$2)`,
			tenantID, otherTenantID)
		cleanupExec(context.Background(), t, st,
			`DELETE FROM users WHERE id IN ($1,$2)`, userID, otherUserID)
		cleanupExec(context.Background(), t, st,
			`DELETE FROM tenants WHERE id IN ($1,$2)`,
			tenantID, otherTenantID)
	})

	type commandFixture struct {
		id, kind, status, taskID string
	}
	commands := []commandFixture{
		{uuid.NewString(), "run", "pending", "task-pending"},
		{uuid.NewString(), "run", "completed", "task-completed"},
		{uuid.NewString(), "pause", "completed", "task-pause"},
		{uuid.NewString(), "resume", "completed", "task-resume"},
		{uuid.NewString(), "delete", "completed", "task-delete"},
		{uuid.NewString(), "run", "blocked", "task-blocked"},
	}
	for _, command := range commands {
		phase, errorCode, errorMessage := "intent", "", ""
		terminal := false
		if command.status == "completed" {
			phase, terminal = "completed", true
		} else if command.status == "blocked" {
			phase, terminal = "blocked", true
			errorCode, errorMessage = "test_blocked", "blocked by test"
		}
		if _, err := st.pool.Exec(ctx,
			`INSERT INTO schedule_commands (
			    id,tenant_id,user_id,task_id,idempotency_key,kind,
			    payload_digest,remote_request_id,status,phase,completed_at,
			    error_code,error_message
			 ) VALUES (
			    $1,$2,$3,$4,$5,$6,repeat('a',64),repeat('b',64),$7,$8,
			    CASE WHEN $9 THEN clock_timestamp() ELSE NULL END,$10,$11
			 )`,
			command.id, tenantID, userID, command.taskID,
			"migration-079-"+command.id, command.kind, command.status,
			phase, terminal, errorCode, errorMessage,
		); err != nil {
			t.Fatal(err)
		}
	}

	tx, err := st.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_app`); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name       string
		tenantID   int64
		userID     int64
		taskID     string
		workflowID string
		want       bool
	}{
		{"pending run", tenantID, userID, commands[0].taskID, "wf-manual-" + commands[0].id, true},
		{"completed run", tenantID, userID, commands[1].taskID, "wf-manual-" + commands[1].id, true},
		{"wrong tenant", otherTenantID, userID, commands[0].taskID, "wf-manual-" + commands[0].id, false},
		{"wrong user", tenantID, otherUserID, commands[0].taskID, "wf-manual-" + commands[0].id, false},
		{"wrong task", tenantID, userID, "task-other", "wf-manual-" + commands[0].id, false},
		{"pause kind", tenantID, userID, commands[2].taskID, "wf-manual-" + commands[2].id, false},
		{"resume kind", tenantID, userID, commands[3].taskID, "wf-manual-" + commands[3].id, false},
		{"delete kind", tenantID, userID, commands[4].taskID, "wf-manual-" + commands[4].id, false},
		{"blocked run", tenantID, userID, commands[5].taskID, "wf-manual-" + commands[5].id, false},
		{"malformed workflow", tenantID, userID, commands[0].taskID, "wf-manual-attacker", false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var got bool
			if err := tx.QueryRow(ctx,
				`SELECT authorize_manual_task_run_v1($1,$2,$3,$4)`,
				test.tenantID, test.userID, test.taskID, test.workflowID,
			).Scan(&got); err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("authorization=%v, want %v", got, test.want)
			}
		})
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	enumerationTx, err := st.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = enumerationTx.Rollback(ctx) }()
	if _, err := enumerationTx.Exec(ctx, `SET LOCAL ROLE vane_app`); err != nil {
		t.Fatal(err)
	}
	if _, err := enumerationTx.Exec(
		ctx, `SELECT id FROM schedule_commands LIMIT 1`,
	); err == nil || !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("vane_app command-table enumeration err=%v", err)
	}
}

func TestMigration079DownRefusesOnPostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required for migration 079 integration tests")
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
	provider, err := goose.NewProvider(goose.DialectPostgres, database, dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.UpTo(t.Context(), 79); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.DownTo(t.Context(), 78); err == nil ||
		!strings.Contains(
			err.Error(),
			"manual task run authority migration is irreversible",
		) {
		t.Fatalf("DownTo(78) err=%v", err)
	}
	var version int64
	if err := database.QueryRowContext(t.Context(),
		`SELECT COALESCE(MAX(version_id), 0)
		   FROM goose_db_version
		  WHERE is_applied`,
	).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 79 {
		t.Fatalf("version after refused Down=%d, want 79", version)
	}
}
