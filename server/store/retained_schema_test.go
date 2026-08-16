package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
)

// TestMain gives the broad Store suite the current production schema while
// retaining its historical V1/protocol-1 fixture builders. The production
// migration keeps all physical fences ENABLE ALWAYS; only this package's
// disposable test database disables the four legacy write triggers. Dedicated
// migration-132 scratch-database tests still exercise the exact production
// fence, catalog descriptor, roles, races and V3 pass-through unchanged.
func TestMain(m *testing.M) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL != "" {
		if err := Migrate(context.Background(), databaseURL); err != nil {
			fmt.Fprintf(os.Stderr, "prepare current Store test schema: %v\n", err)
			os.Exit(1)
		}
		database, err := sql.Open("pgx", databaseURL)
		if err != nil {
			fmt.Fprintf(os.Stderr, "open Store test schema: %v\n", err)
			os.Exit(1)
		}
		if err := disableRetainedFixtureWriteFences(
			context.Background(), database,
		); err != nil {
			_ = database.Close()
			fmt.Fprintf(os.Stderr, "prepare retained Store fixtures: %v\n", err)
			os.Exit(1)
		}
		for _, statement := range []string{
			// Retained personal-memory tests historically attach several
			// synthetic owners to tenant 1. Migration 134 correctly forbids
			// that production shape; workspace-isolation tests use independent
			// scratch databases with the exact constraint intact.
			`ALTER TABLE tenants DROP CONSTRAINT ck_tenants_personal_owner`,
			`UPDATE tenants SET workspace_kind='personal',personal_owner_user_id=NULL,seat_limit=1 WHERE id=1`,
		} {
			if _, err := database.ExecContext(context.Background(), statement); err != nil {
				_ = database.Close()
				fmt.Fprintf(os.Stderr, "prepare retained personal fixture: %v\n", err)
				os.Exit(1)
			}
		}
		if err := database.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "close Store test schema: %v\n", err)
			os.Exit(1)
		}
	}
	os.Exit(m.Run())
}

func disableRetainedFixtureWriteFences(
	ctx context.Context,
	database *sql.DB,
) error {
	for _, statement := range []string{
		`ALTER TABLE task_creation_operations DISABLE TRIGGER agent_first_legacy_creation_root_fence_v132`,
		`ALTER TABLE task_creation_receipts DISABLE TRIGGER agent_first_legacy_creation_receipt_fence_v132`,
		`ALTER TABLE task_definition_edit_operations DISABLE TRIGGER agent_first_protocol1_edit_root_fence_v132`,
		`ALTER TABLE task_definition_edit_receipts DISABLE TRIGGER agent_first_protocol1_edit_receipt_fence_v132`,
	} {
		if _, err := database.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}
