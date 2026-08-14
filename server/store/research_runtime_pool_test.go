package store

import (
	"errors"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/YouToco/vane/server/runcontext"
)

func TestResearchRuntimeFailsClosedWithoutDedicatedPool(t *testing.T) {
	for _, st := range []*Store{nil, &Store{}} {
		if _, err := st.beginResearchTransaction(t.Context(), pgx.TxOptions{}); !errors.Is(
			err, errResearchRuntimeUnavailable,
		) {
			t.Fatalf("missing V3 runtime returned %v, want fail-closed error", err)
		}
	}
}

func TestResearchV3TransactionsUseDedicatedRuntimeGuard(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"research_run_v3.go", "research_brief_v3.go"} {
		source, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		text := string(source)
		if strings.Contains(text, "setResearchRunScopeV3(") {
			t.Fatalf("%s retains caller-selected tenant/user authority", name)
		}
		if !strings.Contains(text, "beginScopedResearchRunTransactionV3(") {
			t.Fatalf("%s has no per-run capability transaction", name)
		}
	}
}

func TestNewWithResearchRuntimeEmptyURLKeepsLegacyStoreAndFailsV3Postgres(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("DATABASE_URL is required for research runtime constructor test")
	}
	if err := Migrate(t.Context(), dbURL); err != nil {
		t.Fatal(err)
	}
	st, err := NewWithResearchRuntime(t.Context(), dbURL, "  ")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	if st.researchPool != nil {
		t.Fatal("empty runtime URL unexpectedly created a research pool")
	}
	if err := st.Ping(t.Context()); err != nil {
		t.Fatalf("legacy store is not ready: %v", err)
	}
	if _, err := st.beginResearchTransaction(t.Context(), pgx.TxOptions{}); !errors.Is(
		err, errResearchRuntimeUnavailable,
	) {
		t.Fatalf("empty runtime URL did not fail V3 closed: %v", err)
	}
}

func TestNewWithResearchRuntimeRejectsOwnerSessionPostgres(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("DATABASE_URL is required for research runtime authority test")
	}
	if err := Migrate(t.Context(), dbURL); err != nil {
		t.Fatal(err)
	}
	if st, err := NewWithResearchRuntime(t.Context(), dbURL, dbURL); err == nil {
		st.Close()
		t.Fatal("schema-owner research runtime was accepted")
	} else if !strings.Contains(err.Error(), "unsafe role attributes") {
		t.Fatalf("unexpected owner rejection: %v", err)
	}
}

func TestNewWithResearchRuntimeAcceptsRestrictedLoginPostgres(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("DATABASE_URL is required for research runtime login test")
	}
	fixture := newResearchRunSpendFixtureV3(t, 20_000)
	admin := fixture.store
	started, err := fixture.begin(t, 0)
	if err != nil {
		t.Fatal(err)
	}
	bound, err := admin.CommitResearchRunStepEvidenceV3(t.Context(),
		CommitResearchRunStepEvidenceV3Params{
			Identity: fixture.identity, RunSnapshotID: fixture.snapshotID,
			PlanRef: fixture.planRef, Ordinal: 0, Result: []byte(`{"ok":true}`),
			OriginalSize: 11, TrustType: "external", CostMicroUSD: 4_000,
			ProviderCall: researchProviderCallV3ForTest(
				researchExecutionTraceV3ForTest(t, fixture.identity, fixture.snapshotID,
					fixture.planRef, 0, started.InvocationID), 4_000),
		})
	if err != nil {
		t.Fatal(err)
	}
	if !started.FirstWriter || bound.EvidenceID <= 0 {
		t.Fatalf("bound evidence was not created: start=%+v evidence=%+v", started, bound)
	}
	var toolCallID int64
	if err := admin.pool.QueryRow(t.Context(),
		`SELECT call.id
		   FROM tool_calls call
		   JOIN research_run_step_spend_settlements settlement
		     ON settlement.tool_call_id=call.id
		  WHERE settlement.tenant_id=$1 AND settlement.terminal_step_id=$2`,
		fixture.tenantID, bound.StepID).Scan(&toolCallID); err != nil {
		t.Fatal(err)
	}

	const password = "vane_research_runtime_test_only_20260801"
	if _, err := admin.pool.Exec(t.Context(),
		`ALTER ROLE vane_research_runtime LOGIN PASSWORD '`+password+`'
		 NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		if _, err := admin.pool.Exec(ctx, `ALTER ROLE vane_research_runtime NOLOGIN`); err != nil {
			t.Errorf("disable test research runtime login: %v", err)
		}
	})

	researchConfig, err := url.Parse(dbURL)
	if err != nil {
		t.Fatal(err)
	}
	if researchConfig.Scheme != "postgres" && researchConfig.Scheme != "postgresql" {
		t.Skip("research runtime login test requires a PostgreSQL URL")
	}
	researchConfig.User = url.UserPassword("vane_research_runtime", password)
	researchURL := researchConfig.String()
	st, err := NewWithResearchRuntimeCapability(t.Context(), dbURL, researchURL,
		ResearchRunCapabilityConfigV1{
			ActiveKeyID:  "runtime-test-active",
			ActiveKeyHex: strings.Repeat("51", 32),
		})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	if st.researchPool == nil {
		t.Fatal("restricted runtime login did not create a research pool")
	}
	if err := st.Ping(t.Context()); err != nil {
		t.Fatalf("research runtime is not ready: %v", err)
	}
	var (
		primarySessionUser string
		primaryCurrentUser string
		primaryOwnsStore   bool
	)
	if err := st.pool.QueryRow(t.Context(), `
		SELECT session_user,current_user,class.relowner=role.oid
		  FROM pg_catalog.pg_class class
		  JOIN pg_catalog.pg_namespace namespace
		    ON namespace.oid=class.relnamespace
		  JOIN pg_catalog.pg_roles role ON role.rolname=current_user
		 WHERE namespace.nspname='public' AND class.relname='schedules'`,
	).Scan(&primarySessionUser, &primaryCurrentUser, &primaryOwnsStore); err != nil {
		t.Fatal(err)
	}
	if primarySessionUser != primaryCurrentUser || !primaryOwnsStore {
		t.Fatalf("primary compatibility Store is not the schema owner: session=%q current=%q owns=%t",
			primarySessionUser, primaryCurrentUser, primaryOwnsStore)
	}

	tx, err := st.beginResearchTransaction(t.Context(), pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(t.Context()) }()
	var sessionUser, currentUser string
	if err := tx.QueryRow(t.Context(), `SELECT session_user,current_user`).Scan(
		&sessionUser, &currentUser,
	); err != nil {
		t.Fatal(err)
	}
	if sessionUser != "vane_research_runtime" || currentUser != "vane_research_runtime" {
		t.Fatalf("unexpected runtime identity session=%q current=%q", sessionUser, currentUser)
	}
	if err := tx.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}

	assertDenied := func(name string, attempt func(pgx.Tx) error) {
		t.Helper()
		t.Run(name, func(t *testing.T) {
			tx, err := st.beginResearchTransaction(t.Context(), pgx.TxOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback(t.Context()) }()
			err = attempt(tx)
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
				t.Fatalf("escape attempt returned %v, want SQLSTATE 42501", err)
			}
		})
	}
	assertDenied("legacy vane_app role is unreachable", func(tx pgx.Tx) error {
		_, err := tx.Exec(t.Context(), `SET LOCAL ROLE vane_app`)
		return err
	})
	assertDenied("executor cannot delete bound Tool evidence", func(tx pgx.Tx) error {
		if err := setResearchRunScopeV3(t.Context(), tx, fixture.tenantID, fixture.userID); err != nil {
			return err
		}
		_, err := tx.Exec(t.Context(), `DELETE FROM tool_calls WHERE id=$1`, toolCallID)
		return err
	})
	assertDenied("executor cannot delete spend evidence", func(tx pgx.Tx) error {
		if err := setResearchRunScopeV3(t.Context(), tx, fixture.tenantID, fixture.userID); err != nil {
			return err
		}
		_, err := tx.Exec(t.Context(),
			`DELETE FROM research_run_step_spend_reservations WHERE tenant_id=$1`,
			fixture.tenantID)
		return err
	})
	assertDenied("executor cannot delete tenant root", func(tx pgx.Tx) error {
		if err := setResearchRunScopeV3(t.Context(), tx, fixture.tenantID, fixture.userID); err != nil {
			return err
		}
		_, err := tx.Exec(t.Context(), `DELETE FROM tenants WHERE id=$1`, fixture.tenantID)
		return err
	})
	assertDenied("RESET ROLE cannot recover delete privilege", func(tx pgx.Tx) error {
		if _, err := tx.Exec(t.Context(), `RESET ROLE`); err != nil {
			return err
		}
		_, err := tx.Exec(t.Context(), `DELETE FROM tool_calls WHERE id=$1`, toolCallID)
		return err
	})

	var ownerRole string
	if err := admin.pool.QueryRow(t.Context(),
		`SELECT pg_catalog.pg_get_userbyid(class.relowner)
		   FROM pg_catalog.pg_class class
		   JOIN pg_catalog.pg_namespace namespace ON namespace.oid=class.relnamespace
		  WHERE namespace.nspname='public' AND class.relname='tool_calls'`,
	).Scan(&ownerRole); err != nil {
		t.Fatal(err)
	}
	assertDenied("cannot SET ROLE schema owner", func(tx pgx.Tx) error {
		_, err := tx.Exec(t.Context(), `SET LOCAL ROLE `+pgx.Identifier{ownerRole}.Sanitize())
		return err
	})
	assertDenied("cannot disable immutable trigger", func(tx pgx.Tx) error {
		_, err := tx.Exec(t.Context(),
			`ALTER TABLE public.tool_calls DISABLE TRIGGER protect_bound_research_tool_call_v1`)
		return err
	})

	var remaining int
	if err := admin.pool.QueryRow(t.Context(),
		`SELECT count(*) FROM tool_calls WHERE id=$1`, toolCallID,
	).Scan(&remaining); err != nil || remaining != 1 {
		t.Fatalf("bound Tool evidence changed after attacks: count=%d err=%v", remaining, err)
	}

	otherUserID := testUser(t, admin)
	if _, err := admin.pool.Exec(t.Context(),
		`INSERT INTO memberships (tenant_id,user_id,role) VALUES ($1,$2,'member')`,
		fixture.tenantID, otherUserID); err != nil {
		t.Fatal(err)
	}
	scopeTx, err := st.beginResearchTransaction(t.Context(), pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := setResearchRunScopeV3(
		t.Context(), scopeTx, fixture.tenantID, otherUserID); err != nil {
		_ = scopeTx.Rollback(t.Context())
		t.Fatal(err)
	}
	var leakedRows int
	if err := scopeTx.QueryRow(t.Context(),
		`SELECT count(*) FROM task_run_snapshots`).Scan(&leakedRows); err != nil {
		_ = scopeTx.Rollback(t.Context())
		t.Fatal(err)
	}
	if leakedRows != 0 {
		_ = scopeTx.Rollback(t.Context())
		t.Fatalf("same-tenant other user can read V3 snapshots: %d", leakedRows)
	}
	if err := scopeTx.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.pool.Exec(t.Context(),
		`DELETE FROM memberships WHERE tenant_id=$1 AND user_id=$2`,
		fixture.tenantID, otherUserID); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.pool.Exec(t.Context(),
		`DELETE FROM users WHERE id=$1`, otherUserID); err != nil {
		t.Fatal(err)
	}

	// Prove the same restricted pool can execute the paid V3 begin/evidence
	// transaction pair; this catches missing column, sequence, function and
	// row-lock capabilities without granting a broad application role.
	runtimeFixture := fixture
	runtimeFixture.store = st
	runtimeFixture.identity.TemporalWorkflowID += "-restricted"
	runtimeFixture.identity.TemporalRunID += "-restricted"
	runtimeSnapshot, err := st.CreateOrGetResearchRunSnapshotV3(
		t.Context(), runtimeFixture.identity, testCompiledRunPolicyV1(t),
		testResearchToolPolicyStoreV3(t), testResearchModelPolicyStoreV3(t))
	if err != nil {
		t.Fatalf("restricted runtime freeze V3 snapshot: %v", err)
	}
	runtimeFixture.snapshotID = runtimeSnapshot.SnapshotID
	runtimeFixture.snapshotRef = runtimeSnapshot
	runtimePlan := researchRunPlanFixtureV3(
		t, runtimeFixture.definitionDigest, runtimeSnapshot.CapabilityCatalogDigest,
		runtimeSnapshot.ToolPolicyDigest, "restricted runtime Kimi pricing")
	runtimePlanRef, _ := createResearchPlanFromReceiptV3(
		t, st, runtimeFixture.identity, runtimeSnapshot, runtimePlan)
	runtimeFixture.planRef = runtimePlanRef
	execution, err := runtimeFixture.begin(t, 0)
	if err != nil {
		t.Fatalf("restricted runtime begin V3 step: %v", err)
	}
	if !execution.FirstWriter || execution.SpendReservationID <= 0 {
		t.Fatalf("restricted runtime did not reserve exact effect: %+v", execution)
	}
	traceID, err := runcontext.ResearchExecutionTraceV3(
		runtimeFixture.identity, runtimeFixture.snapshotID,
		runtimeFixture.planRef.PlanDigest, 0, "search-official")
	if err != nil {
		t.Fatal(err)
	}
	providerCall := researchProviderCallV3ForTest(traceID, 4_000)
	receipt, err := st.CommitResearchRunStepEvidenceV3(t.Context(),
		CommitResearchRunStepEvidenceV3Params{
			Identity: runtimeFixture.identity, RunSnapshotID: runtimeFixture.snapshotID,
			PlanRef: runtimeFixture.planRef, Ordinal: 0, Result: []byte(`{"ok":true}`),
			OriginalSize: 11, TrustType: "external", CostMicroUSD: 4_000,
			ProviderCall: providerCall,
		})
	if err != nil {
		t.Fatalf("restricted runtime commit V3 evidence: %v", err)
	}
	if receipt.EvidenceID <= 0 || receipt.StepID <= 0 {
		t.Fatalf("restricted runtime returned incomplete evidence receipt: %+v", receipt)
	}
}
