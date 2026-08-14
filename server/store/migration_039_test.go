package store

import (
	"database/sql"
	"io/fs"
	"os"
	"testing"

	"github.com/pressly/goose/v3"
)

func migration039Scratch(t *testing.T) (string, *sql.DB, *goose.Provider) {
	t.Helper()
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("DATABASE_URL is not set; skipping migration 039 test")
	}
	scratchURL, drop := createScratchDB(t.Context(), t, dbURL)
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
	provider, err := goose.NewProvider(goose.DialectPostgres, db, dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.UpTo(t.Context(), 39); err != nil {
		t.Fatalf("migrate to 039: %v", err)
	}
	return scratchURL, db, provider
}

func TestMigration039RestrictedRoleMatrix(t *testing.T) {
	_, db, _ := migration039Scratch(t)
	for _, role := range []string{
		"vane_push_effect_coordinator",
		"vane_push_effect_receipt",
		"vane_push_effect_operator",
	} {
		var (
			noLogin, noSuper, noCreateDB, noCreateRole bool
			noReplication, noBypassRLS, noInherit      bool
			ownerMember, appToRole, roleToApp          bool
		)
		if err := db.QueryRowContext(t.Context(), `
			SELECT NOT r.rolcanlogin, NOT r.rolsuper, NOT r.rolcreatedb,
			       NOT r.rolcreaterole, NOT r.rolreplication,
			       NOT r.rolbypassrls, NOT r.rolinherit,
			       pg_has_role(current_user,r.oid,'MEMBER'),
			       pg_has_role('vane_app',r.oid,'MEMBER'),
			       pg_has_role(r.oid,'vane_app','MEMBER')
			  FROM pg_roles r WHERE r.rolname=$1`, role,
		).Scan(
			&noLogin, &noSuper, &noCreateDB, &noCreateRole,
			&noReplication, &noBypassRLS, &noInherit,
			&ownerMember, &appToRole, &roleToApp,
		); err != nil {
			t.Fatal(err)
		}
		if !noLogin || !noSuper || !noCreateDB || !noCreateRole ||
			!noReplication || !noBypassRLS || !noInherit ||
			!ownerMember || appToRole || roleToApp {
			t.Fatalf("unsafe role %s attrs/membership", role)
		}
	}

	var (
		coordinatorInsert, coordinatorIdentityUpdate   bool
		receiptSentUpdate, receiptCardUpdate           bool
		operatorBlockUpdate, operatorMessageUpdate     bool
		coordinatorTenantRoot, coordinatorTenantStatus bool
		appSelect                                      bool
	)
	if err := db.QueryRowContext(t.Context(), `
		SELECT
		  has_column_privilege('vane_push_effect_coordinator','push_effects','canonical_payload','INSERT'),
		  has_column_privilege('vane_push_effect_coordinator','push_effects','canonical_payload','UPDATE'),
		  has_column_privilege('vane_push_effect_receipt','push_effects','sent_at','UPDATE'),
		  has_column_privilege('vane_push_effect_receipt','push_effects','card_payload','UPDATE'),
		  has_column_privilege('vane_push_effect_operator','push_effects','blocked_at','UPDATE'),
		  has_column_privilege('vane_push_effect_operator','push_effects','provider_message_id','UPDATE'),
		  has_column_privilege('vane_push_effect_coordinator','tenants','id','SELECT'),
		  has_column_privilege('vane_push_effect_coordinator','tenants','status','SELECT'),
		  has_table_privilege('vane_app','push_effects','SELECT')`,
	).Scan(
		&coordinatorInsert, &coordinatorIdentityUpdate,
		&receiptSentUpdate, &receiptCardUpdate,
		&operatorBlockUpdate, &operatorMessageUpdate,
		&coordinatorTenantRoot, &coordinatorTenantStatus, &appSelect,
	); err != nil {
		t.Fatal(err)
	}
	if !coordinatorInsert || coordinatorIdentityUpdate ||
		!receiptSentUpdate || receiptCardUpdate ||
		!operatorBlockUpdate || operatorMessageUpdate ||
		!coordinatorTenantRoot || coordinatorTenantStatus || appSelect {
		t.Fatalf("role matrix drifted: coordinator=%v/%v receipt=%v/%v operator=%v/%v tenant=%v/%v app=%v",
			coordinatorInsert, coordinatorIdentityUpdate,
			receiptSentUpdate, receiptCardUpdate,
			operatorBlockUpdate, operatorMessageUpdate,
			coordinatorTenantRoot, coordinatorTenantStatus, appSelect)
	}
}

func TestMigration039EmptyDowngrade(t *testing.T) {
	_, db, provider := migration039Scratch(t)
	if _, err := provider.Down(t.Context()); err != nil {
		t.Fatalf("empty 039 downgrade: %v", err)
	}
	var version int
	var exists bool
	if err := db.QueryRowContext(t.Context(), `
		SELECT
		  (SELECT COALESCE(max(version_id),0)
		     FROM goose_db_version WHERE is_applied),
		  to_regclass('public.push_effects') IS NOT NULL`,
	).Scan(&version, &exists); err != nil {
		t.Fatal(err)
	}
	if version != 38 || exists {
		t.Fatalf("039 down version/table=%d/%v", version, exists)
	}
}
