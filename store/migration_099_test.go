package store

import (
	"database/sql"
	"io/fs"
	"os"
	"strings"
	"testing"

	"github.com/pressly/goose/v3"
)

func TestMigration099BinderGrantAndDownGuardPostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL required")
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
	if _, err := provider.UpTo(t.Context(), 99); err != nil {
		t.Fatal(err)
	}
	if err := callServerRuntimeProvisioner(
		t.Context(), scratchURL, "provision_vane_server_runtime_v1"); err != nil {
		t.Fatal(err)
	}
	if err := callServerRuntimeProvisioner(
		t.Context(), scratchURL, "provision_vane_server_runtime_research_binder_v1"); err != nil {
		t.Fatal(err)
	}
	deprovisioned := false
	t.Cleanup(func() {
		if !deprovisioned {
			_ = callServerRuntimeProvisioner(t.Context(), scratchURL,
				"deprovision_vane_server_runtime_research_binder_v1")
			_ = callServerRuntimeProvisioner(t.Context(), scratchURL,
				"deprovision_vane_server_runtime_v1")
		}
	})
	var runtimeCanExecute, appCanExecute bool
	if err := db.QueryRowContext(t.Context(), `SELECT
		has_function_privilege('vane_server_runtime',
		 'bind_research_llm_process_gateway_v1(bigint,bigint,text,text,text,bigint,text,bigint)','EXECUTE'),
		has_function_privilege('vane_app',
		 'bind_research_llm_process_gateway_v1(bigint,bigint,text,text,text,bigint,text,bigint)','EXECUTE')`).Scan(
		&runtimeCanExecute, &appCanExecute); err != nil {
		t.Fatal(err)
	}
	if !runtimeCanExecute || appCanExecute {
		t.Fatalf("binder grants runtime=%v app=%v",
			runtimeCanExecute, appCanExecute)
	}
	if _, err := provider.DownTo(t.Context(), 98); err == nil ||
		!strings.Contains(err.Error(), "deprovision vane_server_runtime") {
		t.Fatalf("099 Down did not guard a provisioned runtime: %v", err)
	}
	if err := callServerRuntimeProvisioner(t.Context(), scratchURL,
		"deprovision_vane_server_runtime_research_binder_v1"); err != nil {
		t.Fatal(err)
	}
	if err := callServerRuntimeProvisioner(t.Context(), scratchURL,
		"deprovision_vane_server_runtime_v1"); err != nil {
		t.Fatal(err)
	}
	deprovisioned = true
	if _, err := provider.DownTo(t.Context(), 98); err != nil {
		t.Fatalf("099 Down after deprovision: %v", err)
	}
}
