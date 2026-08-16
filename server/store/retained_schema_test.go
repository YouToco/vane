package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"
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

// restoreCurrentRetainedFixtureSchema repairs the package-owned disposable
// database after a downgrade-refusal test. Goose correctly restores the
// production ENABLE ALWAYS fence when a multi-migration DownTo aborts; the
// broad retained-history suite then needs its test-only fixture exception
// reinstalled before the next top-level test starts.
func restoreCurrentRetainedFixtureSchema(t *testing.T, databaseURL string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := Migrate(ctx, databaseURL); err != nil {
		t.Fatalf("restore current Store test schema: %v", err)
	}
	database, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatalf("open restored Store test schema: %v", err)
	}
	defer database.Close()
	if err := disableRetainedFixtureWriteFences(ctx, database); err != nil {
		t.Fatalf("restore retained Store fixtures: %v", err)
	}
}
