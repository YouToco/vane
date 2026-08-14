package store

import (
	"context"
	"database/sql"
	"io/fs"
	"os"
	"strings"
	"testing"

	"github.com/pressly/goose/v3"
)

// newMigration094GatewayStore creates the explicit historical schema fixture
// needed by tests for the writable migration-094 gateway. It never points
// legacy assertions at the current shared schema (097+ intentionally revokes
// this authority), and the scratch database is dropped as one unit.
func newMigration094GatewayStore(t *testing.T) *Store {
	t.Helper()
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
	dir, err := fs.Sub(migrationsFS, "migrations")
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, database, dir,
		goose.WithAllowOutofOrder(true))
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	if _, err := provider.UpTo(t.Context(), 94); err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := New(context.WithoutCancel(t.Context()), scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	return store
}

func TestMigration094GatewayAuthorityPostgres(t *testing.T) {
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
	if _, err := provider.UpTo(t.Context(), 94); err != nil {
		t.Fatal(err)
	}

	var executorOld, executorSigned, gatewayOld, gatewaySigned, gatewaySecret,
		gatewayCalls, activeKey, attemptTable bool
	err = db.QueryRowContext(t.Context(), `SELECT
		has_function_privilege('vane_research_v3_executor',
		 'settle_research_run_llm_spend_v3(bigint,bigint,text,bigint,bigint,text,text,text,text,integer,integer,integer,integer,integer,integer,boolean,real,integer,boolean,text,boolean,boolean,boolean,text,text)','EXECUTE'),
		has_function_privilege('vane_research_v3_executor',
		 'settle_signed_research_run_llm_spend_v3(bigint,bigint,text,bigint,bigint,text,text,text,text,text,text,integer,integer,integer,integer,integer,integer,boolean,real,integer,boolean,text,boolean,boolean,boolean,text,text,text,bigint,bytea)','EXECUTE'),
		has_function_privilege('vane_research_llm_gateway',
		 'settle_research_run_llm_spend_v3(bigint,bigint,text,bigint,bigint,text,text,text,text,integer,integer,integer,integer,integer,integer,boolean,real,integer,boolean,text,boolean,boolean,boolean,text,text)','EXECUTE'),
		has_function_privilege('vane_research_llm_gateway',
		 'settle_signed_research_run_llm_spend_v3(bigint,bigint,text,bigint,bigint,text,text,text,text,text,text,integer,integer,integer,integer,integer,integer,boolean,real,integer,boolean,text,boolean,boolean,boolean,text,text,text,bigint,bytea)','EXECUTE'),
		has_column_privilege('vane_research_llm_gateway','research_llm_gateway_verifier_keys','secret','SELECT'),
		has_table_privilege('vane_research_llm_gateway','llm_calls','SELECT'),
		EXISTS(SELECT 1 FROM research_llm_gateway_verifier_keys WHERE status='active' AND octet_length(secret)>=32),
		to_regclass('public.research_llm_gateway_attempts') IS NOT NULL`,
	).Scan(&executorOld, &executorSigned, &gatewayOld, &gatewaySigned, &gatewaySecret,
		&gatewayCalls, &activeKey, &attemptTable)
	if err != nil {
		t.Fatal(err)
	}
	if executorOld || executorSigned || gatewayOld || !gatewaySigned || gatewaySecret ||
		gatewayCalls || !activeKey || !attemptTable {
		t.Fatalf("gateway ACL executor=%v/%v gateway=%v/%v secret=%v calls=%v key=%v attempt=%v",
			executorOld, executorSigned, gatewayOld, gatewaySigned, gatewaySecret, gatewayCalls,
			activeKey, attemptTable)
	}
}

func TestMigration094DownRefusesUnsettledGatewayAttemptPostgres(t *testing.T) {
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
	if _, err := provider.UpTo(t.Context(), 94); err != nil {
		t.Fatal(err)
	}
	// The guard itself is under test. In the disposable superuser database,
	// bypass relational triggers so no fake business fixture is needed.
	if _, err := db.ExecContext(t.Context(), `
		SET session_replication_role=replica;
		INSERT INTO research_llm_gateway_attempts(
		 reservation_id,request_digest,system_prompt,user_prompt,provider,model,
		 temperature,max_tokens,disable_thinking,attempt_state,schema_version)
		VALUES(1,repeat('a',64),'system','user','provider','model',0,1,false,
		 'send_started','vane.research-llm-gateway-attempt/v1');
		SET session_replication_role=origin`); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Down(t.Context()); err == nil ||
		!strings.Contains(err.Error(), "gateway attempts or receipts exist") {
		t.Fatalf("094 Down did not preserve unsettled gateway attempt: %v", err)
	}
	var attempts int
	if err := db.QueryRowContext(t.Context(),
		`SELECT count(*) FROM research_llm_gateway_attempts`).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 {
		t.Fatalf("failed 094 Down lost attempt: %d", attempts)
	}
}
