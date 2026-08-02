package store

import (
	"database/sql"
	"encoding/hex"
	"io/fs"
	"os"
	"strings"
	"testing"

	"github.com/pressly/goose/v3"
)

func TestMigration097DownRefusesClaimedUnsettledProviderEffectPostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required")
	}
	scratchURL, drop := createScratchDB(t.Context(), t, databaseURL)
	t.Cleanup(drop)
	t.Setenv("DATABASE_URL", scratchURL)

	// Build a real V3 reservation and use the production process-gateway claim
	// function. This represents the response-loss window after a Provider send
	// has started but before its settlement has committed.
	seed := tenantTestStore(t)
	ensureResearchLLMPriceV3(t, seed)
	f := newResearchRunSpendFixtureWithToolBudgetV3(t, 1_000_000, 16, false)
	useOwnerResearchRuntimeForTest(f.store)
	reservation, err := f.store.BeginResearchRunLLMSpendV3(t.Context(),
		researchPlannerBeginV3(f, 1, "097 down must preserve claimed provider effect"))
	if err != nil {
		t.Fatal(err)
	}
	capability, err := f.store.resolveResearchRunCapabilityV1(t.Context(), f.snapshotRef)
	if err != nil {
		t.Fatal(err)
	}
	var firstWriter bool
	if err := f.store.pool.QueryRow(t.Context(), `SELECT out_first_writer
		FROM claim_research_llm_gateway_request_v2($1,$2,$3)`,
		reservation.ReservationID, reservation.RequestDigest,
		hex.EncodeToString(capability.raw[:])).Scan(&firstWriter); err != nil {
		t.Fatal(err)
	}
	if !firstWriter {
		t.Fatal("first process-gateway claim was not the first writer")
	}

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
	// Migration 102 is deliberately irreversible because its cutover recovery
	// state cannot be reconstructed by DDL rollback. On a latest-schema binary,
	// that stronger outer fence may reject DownTo before migration 097 gets a
	// chance to evaluate its own claimed-effect fence; either rejection must
	// leave the 097 recovery rows and gateway privilege untouched below.
	if _, err := provider.DownTo(t.Context(), 96); err == nil ||
		(!strings.Contains(err.Error(), "cannot remove frozen or issued process gateway effects") &&
			!strings.Contains(err.Error(), "irreversible V3 prepare/cutover recovery migration") &&
			!strings.Contains(err.Error(), "refusing downgrade after shadow Tool admission authority")) {
		t.Fatalf("097 Down did not fail closed after unsettled claim: %v", err)
	}

	var frozen, attempts int
	var gatewayCanClaim bool
	if err := db.QueryRowContext(t.Context(), `SELECT
		(SELECT count(*) FROM research_llm_gateway_frozen_requests WHERE reservation_id=$1),
		(SELECT count(*) FROM research_llm_gateway_attempts WHERE reservation_id=$1),
		has_function_privilege('vane_research_llm_gateway',
		 'claim_research_llm_gateway_request_v2(bigint,text,text)','EXECUTE')`,
		reservation.ReservationID).Scan(&frozen, &attempts, &gatewayCanClaim); err != nil {
		t.Fatal(err)
	}
	if frozen != 1 || attempts != 1 || !gatewayCanClaim {
		t.Fatalf("failed Down changed recovery state frozen=%d attempts=%d can_claim=%v",
			frozen, attempts, gatewayCanClaim)
	}
}
