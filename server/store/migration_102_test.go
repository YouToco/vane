package store

import (
	"database/sql"
	"io/fs"
	"os"
	"strings"
	"testing"

	"github.com/pressly/goose/v3"
)

func TestMigration102KeepsPreparationDarkAndPromotionRecoverable(t *testing.T) {
	payload, err := migrationsFS.ReadFile("migrations/102_research_v3_definition_prepare.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.ReplaceAll(string(payload), "\r\n", "\n")
	required := []string{
		"CREATE TABLE research_v3_definition_prepare_operations",
		"CREATE TABLE research_v3_prepared_definition_heads",
		"original_execution_mode",
		"definition_promoted",
		"definition_restored",
		"^research-v3-shadow-[0-9a-f]{64}$",
		"NEW.definition_digest IS DISTINCT FROM selected_definition_digest",
		"GRANT UPDATE (execution_mode,approved_definition_version,approved_definition_digest)",
		"protect_research_v3_definition_prepare_transition",
		"non-terminal migration 101 V3 cutover journal or live authority exists",
		"source_baseline_digest",
		"enforce_research_v3_prepared_binding",
		"resolve_owned_schedule_tenant_v1",
		"register_research_run_capability_registration_v1",
		"FOR SHARE OF schedule,tenant,membership,head,definition",
		"lock_research_v3_membership_authorization_write",
		"lock_research_v3_tenant_authorization_write",
	}
	for _, fragment := range required {
		if !strings.Contains(sql, fragment) {
			t.Fatalf("migration 102 missing %q", fragment)
		}
	}
	forbidden := []string{
		"GRANT UPDATE ON schedules TO vane_research_v3_cutover_operator",
		"GRANT SELECT ON research_v3_definition_prepare_operations TO vane_app",
		"GRANT INSERT ON research_v3_prepared_definition_heads TO vane_app",
		"GRANT SELECT ON research_run_capabilities TO vane_app",
		"GRANT INSERT ON research_run_capabilities TO vane_app",
	}
	for _, fragment := range forbidden {
		if strings.Contains(sql, fragment) {
			t.Fatalf("migration 102 exposes forbidden capability %q", fragment)
		}
	}
}

func TestMigration102RejectsEveryLiveMigration101PhasePostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		requireDatabaseCapability(t)
	}
	scratchURL, drop := createScratchDB(t.Context(), t, databaseURL)
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
	provider, err := goose.NewProvider(goose.DialectPostgres, db, dir,
		goose.WithAllowOutofOrder(true))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.UpTo(t.Context(), 101); err != nil {
		t.Fatal(err)
	}
	digest := strings.Repeat("a", 64)
	insertJournal := func(phase, task string) {
		t.Helper()
		status := "staged"
		var revokedAt any
		if phase == "rolled_back" || phase == "aborted" || phase == "manual_intervention" {
			status, revokedAt = "revoked", "2026-01-01T00:00:00Z"
		}
		if _, err := db.ExecContext(t.Context(), `INSERT INTO research_v3_delivery_authorities
			(tenant_id,user_id,task_id,generation,definition_version,definition_digest,
			 target_action_digest,action_authorization_digest,status,revoked_at)
			VALUES(1,1,$1,1,1,$2,$2,$2,$3,$4)`, task, digest, status, revokedAt); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(t.Context(), `INSERT INTO research_v3_cutover_operations
			(tenant_id,user_id,task_id,idempotency_key,generation,definition_version,
			 definition_digest,frozen_schedule,frozen_schedule_digest,frozen_conflict_token,
			 conflict_token_digest,target_action,target_action_digest,
			 action_authorization_digest,original_paused,phase)
			VALUES(1,1,$1,$1,1,1,$2,'frozen',$2,'token',$2,'target',$2,$2,false,$3)`,
			task, digest, phase); err != nil {
			t.Fatal(err)
		}
	}
	live := []string{"prepared", "pause_requested", "paused", "action_swapped", "active",
		"rollback_pause_requested", "rollback_paused"}
	for i, phase := range live {
		task := "live-" + phase
		insertJournal(phase, task)
		if _, err := provider.UpTo(t.Context(), 102); err == nil ||
			!strings.Contains(err.Error(), "non-terminal migration 101") {
			t.Fatalf("phase %s upgrade err=%v", phase, err)
		}
		var version int64
		if err := db.QueryRowContext(t.Context(), `SELECT max(version_id) FROM goose_db_version
			WHERE is_applied`).Scan(&version); err != nil || version != 101 {
			t.Fatalf("phase %s version=%d err=%v", phase, version, err)
		}
		if _, err := db.ExecContext(t.Context(),
			`DELETE FROM research_v3_cutover_operations WHERE task_id=$1`, task); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(t.Context(),
			`DELETE FROM research_v3_delivery_authorities WHERE task_id=$1`, task); err != nil {
			t.Fatal(err)
		}
		_ = i
	}
	for _, phase := range []string{"rolled_back", "aborted", "manual_intervention"} {
		insertJournal(phase, "terminal-"+phase)
	}
	if _, err := provider.UpTo(t.Context(), 102); err != nil {
		t.Fatalf("terminal journals rejected: %v", err)
	}
}
