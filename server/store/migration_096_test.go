package store

import (
	"database/sql"
	"errors"
	"io/fs"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pressly/goose/v3"
)

func TestMigration096RemovesToolQuotaRefundAuthorityPostgres(t *testing.T) {
	if testing.Short() {
		requireLongRunningCapability(t)
	}
	st := tenantTestStore(t)
	ctx := t.Context()

	var refundFunction, refundTrigger, executorCanSetLegacyMarker bool
	if err := st.pool.QueryRow(ctx, `SELECT
		to_regprocedure('public.refund_unattempted_research_quota_v3()') IS NOT NULL,
		EXISTS (SELECT 1 FROM pg_catalog.pg_trigger
		         WHERE tgrelid='research_run_step_spend_settlements'::regclass
		           AND tgname='refund_unattempted_research_quota_v3' AND NOT tgisinternal),
		has_column_privilege('vane_research_v3_executor',
		 'research_run_step_spend_settlements','quota_floor_policy_version','INSERT')`,
	).Scan(&refundFunction, &refundTrigger, &executorCanSetLegacyMarker); err != nil {
		t.Fatal(err)
	}
	if refundFunction || refundTrigger || executorCanSetLegacyMarker {
		t.Fatalf("refund boundary function=%v trigger=%v legacy_marker_insert=%v",
			refundFunction, refundTrigger, executorCanSetLegacyMarker)
	}

	tx, err := st.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_research_v3_executor`); err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(ctx, `SELECT refund_unattempted_research_quota_v3()`)
	if err == nil {
		t.Fatal("executor invoked removed Tool quota refund")
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "42883" {
		t.Fatalf("removed refund call=%v, want undefined_function", err)
	}
}

func TestMigration096DownRefusesQuotaFloorSettlementPostgres(t *testing.T) {
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
	if _, err := provider.UpTo(t.Context(), 96); err != nil {
		t.Fatal(err)
	}
	// This test exercises only the downgrade marker. Disable relational
	// triggers in the disposable superuser scratch DB so no production fixture
	// or provider receipt is fabricated merely to reach the Down guard.
	if _, err := db.ExecContext(t.Context(), `
		SET session_replication_role=replica;
		INSERT INTO research_run_step_spend_settlements (
		 tenant_id,user_id,task_id,run_snapshot_id,plan_id,reservation_id,
		 terminal_step_id,tool_call_id,temporal_run_id,plan_digest,step_ordinal,
		 invocation_id,tool_name,request_digest,outcome,actual_quota_units,
		 actual_cost_micro_usd,pricing_status,cost_currency,schema_version
		) VALUES (1,1,'down-guard',1,1,1,1,NULL,'run',repeat('a',64),0,
		 'invoke','web_search',repeat('b',64),'failed',0,0,'unpriced','USD',
		 'vane.research-run-step-spend-settlement/v1');
		SET session_replication_role=origin`); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Down(t.Context()); err == nil ||
		!strings.Contains(err.Error(), "quota-floor or unsettled reservations") {
		t.Fatalf("096 Down did not fail closed: %v", err)
	}
}

func TestMigration096DownRefusesUnsettledQuotaReservationPostgres(t *testing.T) {
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
	if _, err := provider.UpTo(t.Context(), 96); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `
		SET session_replication_role=replica;
		INSERT INTO research_run_step_spend_reservations(
		 id,tenant_id,user_id,task_id,run_snapshot_id,plan_id,started_step_id,
		 temporal_run_id,plan_digest,step_ordinal,invocation_id,tool_name,
		 request_digest,tool_policy_digest,quota_bucket,reserved_quota_units,
		 reserved_cost_micro_usd,schema_version)
		VALUES(1,1,1,'down-guard',1,1,1,'run',repeat('a',64),0,'invoke',
		 'web_search',repeat('b',64),repeat('c',64),'exa_calls',1,1,
		 'vane.research-run-step-spend-reservation/v1');
		SET session_replication_role=origin`); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Down(t.Context()); err == nil ||
		!strings.Contains(err.Error(), "quota-floor or unsettled reservations") {
		t.Fatalf("096 Down did not preserve unsettled admission: %v", err)
	}
	var reservations int
	var refundExists bool
	if err := db.QueryRowContext(t.Context(), `SELECT
		(SELECT count(*) FROM research_run_step_spend_reservations),
		to_regprocedure('public.refund_unattempted_research_quota_v3()') IS NOT NULL`,
	).Scan(&reservations, &refundExists); err != nil {
		t.Fatal(err)
	}
	if reservations != 1 || refundExists {
		t.Fatalf("failed Down changed quota floor reservation=%d refund=%v",
			reservations, refundExists)
	}
}
