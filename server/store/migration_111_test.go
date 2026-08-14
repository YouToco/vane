package store

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"io/fs"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pressly/goose/v3"

	"github.com/YouToco/vane/server/runcontext"
	"github.com/YouToco/vane/server/types"
)

func TestMigration111FencesConcurrentTenantRegistrationPostgres(t *testing.T) {
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
	provider, err := goose.NewProvider(goose.DialectPostgres, db, dir, goose.WithAllowOutofOrder(true))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.UpTo(t.Context(), 110); err != nil {
		t.Fatal(err)
	}

	registration, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	var tenantID int64
	if err := registration.QueryRowContext(t.Context(),
		`INSERT INTO tenants(status,plan) VALUES('active','free') RETURNING id`).Scan(&tenantID); err != nil {
		t.Fatal(err)
	}
	migrated := make(chan error, 1)
	go func() { _, err := provider.UpTo(t.Context(), 111); migrated <- err }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var waiting bool
		err := db.QueryRowContext(t.Context(), `SELECT EXISTS(
			SELECT 1 FROM pg_locks lock JOIN pg_class rel ON rel.oid=lock.relation
			 WHERE rel.relname='tenants' AND lock.mode='AccessExclusiveLock' AND NOT lock.granted)`).Scan(&waiting)
		if err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("migration did not fence concurrent tenant registration")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := registration.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := <-migrated; err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.QueryRowContext(t.Context(), `SELECT count(*) FROM tenant_quota
		WHERE tenant_id=$1 AND bucket='official_calls'`, tenantID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("concurrent tenant missed official_calls backfill: %d", count)
	}
}

func TestMigration111OfficialResearchRouteAndDowngradeGuardPostgres(t *testing.T) {
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
	if _, err := provider.UpTo(t.Context(), 111); err != nil {
		t.Fatal(err)
	}

	var admission, binding, settlement string
	if err := db.QueryRowContext(t.Context(), `SELECT pg_get_functiondef(
		'admit_research_run_tool_step_cap_v1(bigint,bigint,integer)'::regprocedure)`).Scan(&admission); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(t.Context(), `SELECT pg_get_functiondef(
		'protect_bound_research_tool_call_v1()'::regprocedure)`).Scan(&binding); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(t.Context(), `SELECT pg_get_functiondef(
		'enforce_research_run_step_spend_settlement_v1()'::regprocedure)`).Scan(&settlement); err != nil {
		t.Fatal(err)
	}
	for label, pair := range map[string]struct{ got, want string }{
		"admission":            {admission, "official_calls"},
		"binding":              {binding, "kimi:goods_list"},
		"settlement provider":  {settlement, "THEN 'kimi'"},
		"settlement operation": {settlement, "kimi:goods_list"},
	} {
		if !strings.Contains(pair.got, pair.want) {
			t.Fatalf("%s did not retain %q", label, pair.want)
		}
	}
	var missing int
	if err := db.QueryRowContext(t.Context(), `
		SELECT count(*) FROM tenants tenant
		 WHERE NOT EXISTS (SELECT 1 FROM tenant_quota quota
		  WHERE quota.tenant_id=tenant.id AND quota.bucket='official_calls'
		    AND quota.tokens=500 AND quota.burst=500)`).Scan(&missing); err != nil {
		t.Fatal(err)
	}
	if missing != 0 {
		t.Fatalf("official_calls backfill missing for %d tenants", missing)
	}

	if _, err := provider.DownTo(t.Context(), 110); err == nil ||
		!strings.Contains(err.Error(), "refusing downgrade after official Research Tool admission") {
		t.Fatalf("111 downgrade unexpectedly succeeded or lost guard: %v", err)
	}
}

func TestOfficialResearchStepFirstWriterEvidenceAndRawLedgerPostgres(t *testing.T) {
	f := newResearchRunSpendFixtureWithToolBudgetV3(t, 1_000_000, 4, false)
	arguments := json.RawMessage(`{"page_url":"https://www.kimi.com/membership/pricing"}`)
	plan, err := runcontext.BuildResearchExecutionPlanV3(
		f.definitionDigest, f.snapshotRef.CapabilityCatalogDigest,
		f.snapshotRef.ToolPolicyDigest,
		[]runcontext.ResearchPlanStepV3{{
			InvocationID: "kimi-status", ToolName: "web_product_status",
			Arguments: arguments,
		}},
		func(_ string, raw json.RawMessage) (json.RawMessage, error) { return raw, nil },
	)
	if err != nil {
		t.Fatal(err)
	}
	f.planRef, _ = createResearchPlanFromReceiptV3(
		t, f.store, f.identity, f.snapshotRef, plan)
	if _, err := f.store.pool.Exec(t.Context(), `UPDATE tenant_quota
		SET tokens=10,rate=0,burst=10,updated_at=now()
		WHERE tenant_id=$1 AND bucket='official_calls'`, f.tenantID); err != nil {
		t.Fatal(err)
	}

	first, err := f.begin(t, 0)
	if err != nil || !first.FirstWriter || first.ReservedCostMicroUSD != 1 ||
		first.ReservedQuotaUnits != 1 || first.ToolName != "web_product_status" {
		t.Fatalf("official first writer=%+v err=%v", first, err)
	}
	replay, err := f.begin(t, 0)
	if err != nil || replay.FirstWriter || replay.StepID != first.StepID ||
		replay.SpendReservationID != first.SpendReservationID || replay.Arguments != nil {
		t.Fatalf("official response-loss recovery=%+v err=%v", replay, err)
	}
	var tokens float64
	if err := f.store.pool.QueryRow(t.Context(), `SELECT tokens FROM tenant_quota
		WHERE tenant_id=$1 AND bucket='official_calls'`, f.tenantID).Scan(&tokens); err != nil {
		t.Fatal(err)
	}
	if tokens != 9 {
		t.Fatalf("official_calls debited more than once: %v", tokens)
	}

	result := []byte(`{"schema_version":"vane.product-status-result/v1","provider":"kimi","official_page":"https://www.kimi.com/membership/pricing","purchase_status":"reservation_only","plans":[]}`)
	status := 200
	trace := f.trace(t, 0, "kimi-status")
	receipt, err := f.store.CommitResearchRunStepEvidenceV3(t.Context(),
		CommitResearchRunStepEvidenceV3Params{
			Identity: f.identity, RunSnapshotID: f.snapshotID, PlanRef: f.planRef,
			Ordinal: 0, Result: result, OriginalSize: len(result),
			TrustType: "official", CostMicroUSD: 0,
			ProviderCall: ResearchProviderCallV3{
				TraceID: trace, Provider: "kimi", UsageQuantity: 1, QuotaUnits: 1,
				HTTPStatus: &status, DurationMS: 12, Attempted: true, CostKnown: true,
				CostMicroUSD: 0, PricingStatus: "calculated", CostCurrency: "USD",
			},
		})
	if err != nil || receipt.TrustType != "official" || receipt.CostMicroUSD != 0 {
		t.Fatalf("official evidence=%+v err=%v", receipt, err)
	}
	var toolName, toolKind, provider, pricing, endpoint string
	var cost float64
	if err := f.store.pool.QueryRow(t.Context(), `SELECT tool_name,tool_kind,provider,
		pricing_status,cost_amount::double precision,endpoint_path FROM tool_calls
		WHERE research_run_step_spend_reservation_id=$1`, first.SpendReservationID).Scan(
		&toolName, &toolKind, &provider, &pricing, &cost, &endpoint); err != nil {
		t.Fatal(err)
	}
	if toolName != "kimi:goods_list" || toolKind != string(types.ToolCallKindOfficialFetch) ||
		provider != "kimi" || pricing != "calculated" || cost != 0 ||
		endpoint != "/apiv2/kimi.gateway.order.v1.GoodsService/ListGoods" {
		t.Fatalf("raw official ledger=%q/%q provider=%q pricing=%q cost=%v endpoint=%q",
			toolName, toolKind, provider, pricing, cost, endpoint)
	}
	sum := sha256.Sum256(result)
	if receipt.ResultDigest != hex.EncodeToString(sum[:]) {
		t.Fatalf("official evidence digest=%q", receipt.ResultDigest)
	}
}
