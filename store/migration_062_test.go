package store

import (
	"database/sql"
	"io/fs"
	"strings"
	"testing"

	"github.com/pressly/goose/v3"
)

func TestMigration062DowngradeFailsClosedWithLedger(t *testing.T) {
	freshURL := freshMigrationDatabase(t, "vane_profile_claim_062")
	db, err := sql.Open("pgx", freshURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	dir, err := fs.Sub(migrationsFS, "migrations")
	if err != nil {
		t.Fatal(err)
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, db, dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.UpTo(t.Context(), 60); err != nil {
		t.Fatalf("migrate to 060: %v", err)
	}
	var userID int64
	if err := db.QueryRowContext(t.Context(),
		`INSERT INTO users(feishu_open_id,name)
		 VALUES ('claim-062-user','claim') RETURNING id`).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(),
		`INSERT INTO memberships(tenant_id,user_id,role) VALUES (1,$1,'owner')`,
		userID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(),
		`INSERT INTO profiles(tenant_id,user_id,industry,tags,summary)
		 VALUES (1,$1,'AI',ARRAY['safe'],$2)`,
		userID, strings.Repeat("历史句子。", 100)); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.UpTo(t.Context(), 62); err != nil {
		t.Fatalf("migrate to 062: %v", err)
	}

	var sourceState string
	if err := db.QueryRowContext(t.Context(),
		`SELECT source_state FROM profile_claims
		  WHERE user_id=$1 AND field_name='industry'`, userID,
	).Scan(&sourceState); err != nil {
		t.Fatal(err)
	}
	if sourceState != "source_unavailable" {
		t.Fatalf("backfill invented provenance %q", sourceState)
	}
	var summaryClaims, maxSummaryRunes int
	if err := db.QueryRowContext(t.Context(),
		`SELECT count(*),COALESCE(max(char_length(claim_value)),0)
		   FROM profile_claims
		  WHERE user_id=$1 AND field_name='summary'`, userID,
	).Scan(&summaryClaims, &maxSummaryRunes); err != nil {
		t.Fatal(err)
	}
	if summaryClaims < 2 || maxSummaryRunes > 240 {
		t.Fatalf("summary backfill claims=%d max_runes=%d",
			summaryClaims, maxSummaryRunes)
	}
	if _, err := provider.Down(t.Context()); err == nil ||
		!strings.Contains(err.Error(), "refusing to drop non-empty profile claim authority ledger") {
		t.Fatalf("062 Down accepted non-empty ledger: %v", err)
	}
	var version int64
	if err := db.QueryRowContext(t.Context(),
		`SELECT max(version_id) FROM goose_db_version WHERE is_applied`,
	).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 62 {
		t.Fatalf("failed Down changed version=%d", version)
	}
}

func TestMigration062LeastPrivilegeAndExactUserRLS(t *testing.T) {
	dbURL := freshMigrationDatabase(t, "vane_profile_claim_rls")
	if err := Migrate(t.Context(), dbURL); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("pgx", dbURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	var (
		claimsRead, claimsInsert, claimsUpdate, claimsDelete bool
		eventInsert, receiptInsert, stateUpdate              bool
		noLogin, noInherit, noBypass                         bool
		oldEditorSummaryUpdate                               bool
		ownerCanSet, appCanSet, oldEditorCanSet              bool
		appClaimsAccess, oldEditorClaimsAccess               bool
		oldEditorProfileInsert, oldEditorProfileUpdate       bool
	)
	if err := db.QueryRowContext(t.Context(), `
		SELECT
		  has_table_privilege('vane_profile_claim_editor','profile_claims','SELECT'),
		  has_table_privilege('vane_profile_claim_editor','profile_claims','INSERT'),
		  has_table_privilege('vane_profile_claim_editor','profile_claims','UPDATE'),
		  has_table_privilege('vane_profile_claim_editor','profile_claims','DELETE'),
		  has_table_privilege('vane_profile_claim_editor','profile_claim_events','INSERT'),
		  has_table_privilege('vane_profile_claim_editor','profile_claim_receipts','INSERT'),
		  has_table_privilege('vane_profile_claim_editor','profile_claim_states','UPDATE'),
		  NOT rolcanlogin,NOT rolinherit,NOT rolbypassrls,
		  has_column_privilege('vane_profile_editor','profiles','summary','UPDATE'),
		  pg_has_role(current_user,'vane_profile_claim_editor','SET'),
		  pg_has_role('vane_app','vane_profile_claim_editor','SET'),
		  pg_has_role('vane_profile_editor','vane_profile_claim_editor','SET'),
		  has_table_privilege('vane_app','profile_claims','SELECT,INSERT,UPDATE,DELETE'),
		  has_table_privilege('vane_profile_editor','profile_claims','SELECT,INSERT,UPDATE,DELETE'),
		  has_column_privilege('vane_profile_editor','profiles','industry','INSERT'),
		  has_column_privilege('vane_profile_editor','profiles','industry','UPDATE')
		FROM pg_roles WHERE rolname='vane_profile_claim_editor'`,
	).Scan(
		&claimsRead, &claimsInsert, &claimsUpdate, &claimsDelete,
		&eventInsert, &receiptInsert, &stateUpdate,
		&noLogin, &noInherit, &noBypass, &oldEditorSummaryUpdate,
		&ownerCanSet, &appCanSet, &oldEditorCanSet,
		&appClaimsAccess, &oldEditorClaimsAccess,
		&oldEditorProfileInsert, &oldEditorProfileUpdate,
	); err != nil {
		t.Fatal(err)
	}
	if !claimsRead || !claimsInsert || claimsUpdate || claimsDelete ||
		!eventInsert || !receiptInsert || !stateUpdate ||
		!noLogin || !noInherit || !noBypass || oldEditorSummaryUpdate ||
		!ownerCanSet || appCanSet || oldEditorCanSet ||
		appClaimsAccess || oldEditorClaimsAccess ||
		oldEditorProfileInsert || oldEditorProfileUpdate {
		t.Fatalf("unsafe 062 privileges read/insert/update/delete=%t/%t/%t/%t events=%t receipts=%t state=%t role=%t/%t/%t",
			claimsRead, claimsInsert, claimsUpdate, claimsDelete,
			eventInsert, receiptInsert, stateUpdate,
			noLogin, noInherit, noBypass)
	}

	var policyCount int
	if err := db.QueryRowContext(t.Context(), `
		SELECT count(*) FROM pg_policies
		 WHERE tablename IN (
		   'profile_claim_states','profile_claims',
		   'profile_claim_events','profile_claim_receipts'
		 )
		   AND qual LIKE '%app.tenant_id%'
		   AND qual LIKE '%app.user_id%'
		   AND qual LIKE '%NULLIF%'
		   AND with_check LIKE '%app.tenant_id%'
		   AND with_check LIKE '%app.user_id%'
		   AND with_check LIKE '%NULLIF%'`,
	).Scan(&policyCount); err != nil {
		t.Fatal(err)
	}
	if policyCount != 4 {
		t.Fatalf("exact-user RLS policies=%d want 4", policyCount)
	}

	var identityPolicyCount int
	if err := db.QueryRowContext(t.Context(), `
		SELECT count(*) FROM pg_policies
		 WHERE policyname='profile_claim_editor_identity'
		   AND tablename IN ('profiles','memberships')
		   AND qual LIKE '%app.tenant_id%'
		   AND qual LIKE '%app.user_id%'
		   AND qual LIKE '%NULLIF%'`,
	).Scan(&identityPolicyCount); err != nil {
		t.Fatal(err)
	}
	if identityPolicyCount != 2 {
		t.Fatalf("claim identity fail-closed policies=%d want 2", identityPolicyCount)
	}
	var profileTenantPolicySafe bool
	if err := db.QueryRowContext(t.Context(), `
		SELECT qual LIKE '%NULLIF%' AND with_check LIKE '%NULLIF%'
		  FROM pg_policies
		 WHERE tablename='profiles' AND policyname='tenant_isolation'`,
	).Scan(&profileTenantPolicySafe); err != nil {
		t.Fatal(err)
	}
	if !profileTenantPolicySafe {
		t.Fatal("profiles general tenant policy still has an empty-GUC bare cast")
	}

	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(t.Context(), `DISCARD ALL`); err != nil {
		t.Fatal(err)
	}
	assertNoClaimRows := func(tx *sql.Tx, state string) {
		t.Helper()
		var visible int
		if err := tx.QueryRowContext(t.Context(), `
			SELECT
			  (SELECT count(*) FROM profile_claim_states) +
			  (SELECT count(*) FROM profile_claims) +
			  (SELECT count(*) FROM profile_claim_events) +
			  (SELECT count(*) FROM profile_claim_receipts) +
			  (SELECT count(*) FROM profiles) +
			  (SELECT count(*) FROM memberships)`,
		).Scan(&visible); err != nil {
			t.Fatalf("%s GUC read raised instead of failing closed: %v", state, err)
		}
		if visible != 0 {
			t.Fatalf("%s GUC exposed %d scoped rows", state, visible)
		}
		if _, err := tx.ExecContext(t.Context(), `
			INSERT INTO profile_claim_states(tenant_id,user_id) VALUES(1,1)`,
		); err == nil {
			t.Fatalf("%s GUC allowed scoped insert", state)
		} else if strings.Contains(err.Error(), "invalid input syntax") {
			t.Fatalf("%s GUC raised bigint cast error: %v", state, err)
		}
	}

	tx, err := conn.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `SET LOCAL ROLE vane_profile_claim_editor`); err != nil {
		t.Fatal(err)
	}
	assertNoClaimRows(tx, "missing")
	_ = tx.Rollback()

	tx, err = conn.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `
		SELECT set_config('app.tenant_id','',true),
		       set_config('app.user_id','',true)`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `SET LOCAL ROLE vane_profile_claim_editor`); err != nil {
		t.Fatal(err)
	}
	assertNoClaimRows(tx, "empty")
	_ = tx.Rollback()
}
