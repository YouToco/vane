package store

import (
	"database/sql"
	"io/fs"
	"os"
	"strings"
	"testing"

	"github.com/pressly/goose/v3"
)

func TestMigration110ProtocolIsolationAndACLPostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required")
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
	if _, err := provider.UpTo(t.Context(), 110); err != nil {
		t.Fatal(err)
	}
	var defaultValue, nullable, definition string
	if err := db.QueryRowContext(t.Context(), `
		SELECT column_default,is_nullable FROM information_schema.columns
		 WHERE table_schema='public' AND table_name='task_definition_edit_operations'
		   AND column_name='operation_protocol'`).Scan(&defaultValue, &nullable); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(defaultValue, "1") || nullable != "NO" {
		t.Fatalf("unsafe protocol column default=%q nullable=%q", defaultValue, nullable)
	}
	if err := db.QueryRowContext(t.Context(), `
		SELECT pg_get_constraintdef(oid) FROM pg_constraint
		 WHERE conname='task_definition_edit_operation_protocol_valid'`).Scan(&definition); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(definition, "ANY (ARRAY[1, 3])") {
		t.Fatalf("protocol constraint does not retain only legacy/V3: %s", definition)
	}
	var appInsert, appUpdate, coordinatorInsert, coordinatorUpdate bool
	if err := db.QueryRowContext(t.Context(), `
		SELECT has_table_privilege('vane_app','research_v3_delivery_authorities','INSERT'),
		       has_table_privilege('vane_app','research_v3_delivery_authorities','UPDATE'),
		       has_column_privilege('vane_edit_coordinator','research_v3_delivery_authorities','generation','INSERT'),
		       has_column_privilege('vane_edit_coordinator','research_v3_delivery_authorities','status','UPDATE')`).Scan(
		&appInsert, &appUpdate, &coordinatorInsert, &coordinatorUpdate); err != nil {
		t.Fatal(err)
	}
	if appInsert || appUpdate || !coordinatorInsert || !coordinatorUpdate {
		t.Fatalf("native V3 edit ACL differs app_insert=%v app_update=%v coordinator_insert=%v coordinator_update=%v",
			appInsert, appUpdate, coordinatorInsert, coordinatorUpdate)
	}
	if _, err := provider.DownTo(t.Context(), 109); err != nil {
		t.Fatal(err)
	}
	var exists bool
	if err := db.QueryRowContext(t.Context(), `
		SELECT EXISTS (SELECT 1 FROM information_schema.columns
		 WHERE table_schema='public' AND table_name='task_definition_edit_operations'
		   AND column_name='operation_protocol')`).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("migration 110 downgrade retained protocol discriminator")
	}
}

func TestMigration110ContainsNoRetiredExecutionState(t *testing.T) {
	raw, err := fs.ReadFile(migrationsFS,
		"migrations/110_native_research_task_definition_edit.sql")
	if err != nil {
		t.Fatal(err)
	}
	sqlText := strings.ToLower(string(raw))
	for _, forbidden := range []string{
		"tool_calls", "fetch_targets", "schedule_sources", "source_catalog",
	} {
		if strings.Contains(sqlText, forbidden) {
			t.Fatalf("migration 110 retained retired execution state %q", forbidden)
		}
	}
	for _, required := range []string{
		"operation_protocol", "check (operation_protocol in (1,3))",
		"refusing downgrade while native v3 edits exist",
	} {
		if !strings.Contains(sqlText, required) {
			t.Fatalf("migration 110 is missing %q", required)
		}
	}
}
