package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pressly/goose/v3"
)

const profileEditorRole = "vane_profile_editor"

func TestMigration060NormalizesRestrictedRoleAndPrivileges(t *testing.T) {
	db, provider, roleName, _ := isolatedMigration060(t, "normalize")
	ctx := t.Context()
	if _, err := db.ExecContext(ctx, fmt.Sprintf(`
		CREATE ROLE %s;
		ALTER ROLE %s
		  LOGIN INHERIT CREATEDB CREATEROLE BYPASSRLS;
		ALTER ROLE %s SET statement_timeout='1s'`,
		roleName, roleName, roleName)); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.UpTo(ctx, 60); err != nil {
		t.Fatalf("migrate to 060: %v", err)
	}

	var (
		noLogin, noInherit, noBypass, noSuper                bool
		noCreateDB, noCreateRole, noReplication, ownerCanSet bool
		ownsTables, onlyOwnerMember, editorHasParent         bool
		appCanSet, appRevisionAccess, appReceiptAccess       bool
		configNormalized                                     bool
	)
	if err := db.QueryRowContext(ctx, `
		SELECT
		  NOT r.rolcanlogin, NOT r.rolinherit, NOT r.rolbypassrls,
		  NOT r.rolsuper, NOT r.rolcreatedb, NOT r.rolcreaterole,
		  NOT r.rolreplication,
		  pg_has_role(current_user,r.oid,'SET'),
		  EXISTS (
		    SELECT 1 FROM pg_class c
		     WHERE c.relname IN (
		       'profile_edit_revisions','profile_edit_receipts'
		     ) AND c.relowner=r.oid
		  ),
		  NOT EXISTS (
		    SELECT 1
		      FROM pg_auth_members am
		      JOIN pg_roles member_role ON member_role.oid=am.member
		     WHERE am.roleid=r.oid
		       AND member_role.rolname<>current_user
		  ),
		  EXISTS (
		    SELECT 1
		      FROM pg_auth_members am
		     WHERE am.member=r.oid
		  ),
		  pg_has_role('vane_app',r.oid,'SET'),
		  has_table_privilege(
		    'vane_app','profile_edit_revisions',
		    'SELECT,INSERT,UPDATE,DELETE,TRUNCATE'
		  ),
		  has_table_privilege(
		    'vane_app','profile_edit_receipts',
		    'SELECT,INSERT,UPDATE,DELETE,TRUNCATE'
		  ),
		  cardinality(COALESCE(r.rolconfig,ARRAY[]::text[]))=1
		    AND r.rolconfig[1]='search_path=pg_catalog, public, pg_temp'
		  FROM pg_roles r WHERE r.rolname=$1`,
		roleName,
	).Scan(
		&noLogin, &noInherit, &noBypass, &noSuper,
		&noCreateDB, &noCreateRole, &noReplication, &ownerCanSet,
		&ownsTables, &onlyOwnerMember, &editorHasParent,
		&appCanSet, &appRevisionAccess, &appReceiptAccess,
		&configNormalized,
	); err != nil {
		t.Fatal(err)
	}
	if !noLogin || !noInherit || !noBypass || !noSuper ||
		!noCreateDB || !noCreateRole || !noReplication || !ownerCanSet ||
		ownsTables || !onlyOwnerMember || editorHasParent || appCanSet ||
		appRevisionAccess || appReceiptAccess || !configNormalized {
		t.Fatalf(
			"unsafe editor role attrs=%t/%t/%t/%t/%t/%t/%t set=%t "+
				"owner=%t only_owner=%t parent=%t app=%t/%t/%t config=%t",
			noLogin, noInherit, noBypass, noSuper,
			noCreateDB, noCreateRole, noReplication, ownerCanSet,
			ownsTables, onlyOwnerMember, editorHasParent,
			appCanSet, appRevisionAccess, appReceiptAccess,
			configNormalized,
		)
	}
}

func TestMigration060RoleCreationHandlesCrossDatabaseRace(t *testing.T) {
	raw, err := fs.ReadFile(
		migrationsFS, "migrations/060_profile_manual_authority.sql")
	if err != nil {
		t.Fatal(err)
	}
	sqlText := string(raw)
	for _, fragment := range []string{
		"WHEN duplicate_object OR unique_violation THEN NULL",
		"CREATE ROLE vane_profile_editor",
	} {
		if !strings.Contains(sqlText, fragment) {
			t.Fatalf("060 role creation race guard missing %q", fragment)
		}
	}
}

func TestMigration060RejectsUnsafePreexistingMembershipAtomically(t *testing.T) {
	db, provider, roleName, unsafeRole := isolatedMigration060(t, "unsafe")
	ctx := t.Context()
	if _, err := db.ExecContext(ctx, fmt.Sprintf(`
		CREATE ROLE %s NOLOGIN;
		CREATE ROLE %s NOLOGIN;
		GRANT %s TO %s`,
		roleName, unsafeRole, roleName, unsafeRole)); err != nil {
		t.Fatal(err)
	}

	if _, err := provider.UpTo(ctx, 60); err == nil ||
		!strings.Contains(err.Error(), "only migration owner") {
		t.Fatalf("060 accepted unsafe pre-existing membership: %v", err)
	}
	var version int64
	if err := db.QueryRowContext(ctx,
		`SELECT COALESCE(max(version_id),0)
		   FROM goose_db_version WHERE is_applied`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 59 {
		t.Fatalf("failed 060 changed migration version to %d", version)
	}
	var revisionExists, receiptExists bool
	var profileUpdate, membershipSelect, sequenceUsage bool
	if err := db.QueryRowContext(ctx, `
		SELECT
		  to_regclass('public.profile_edit_revisions') IS NOT NULL,
		  to_regclass('public.profile_edit_receipts') IS NOT NULL,
		  has_column_privilege(
		    $1,'profiles','industry','UPDATE'
		  ),
		  has_column_privilege(
		    $1,'memberships','user_id','SELECT'
		  ),
		  has_sequence_privilege(
		    $1,'profiles_id_seq','USAGE'
		  )`, roleName,
	).Scan(
		&revisionExists, &receiptExists,
		&profileUpdate, &membershipSelect, &sequenceUsage,
	); err != nil {
		t.Fatal(err)
	}
	if revisionExists || receiptExists ||
		profileUpdate || membershipSelect || sequenceUsage {
		t.Fatalf(
			"failed 060 leaked state: tables=%t/%t grants=%t/%t/%t",
			revisionExists, receiptExists,
			profileUpdate, membershipSelect, sequenceUsage,
		)
	}
}

func isolatedMigration060(
	t *testing.T, suffix string,
) (*sql.DB, *goose.Provider, string, string) {
	t.Helper()
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL")
	}
	roleName := fmt.Sprintf(
		"vane_profile_editor_%s_%d", suffix, time.Now().UnixNano())
	unsafeRole := roleName + "_member"
	admin, err := sql.Open("pgx", dbURL)
	if err != nil {
		t.Fatal(err)
	}
	// Register cluster cleanup before the database cleanup below. Cleanup is
	// LIFO, so all database-local dependencies disappear before DROP ROLE.
	t.Cleanup(func() {
		defer admin.Close()
		if _, err := admin.ExecContext(context.Background(), fmt.Sprintf(`
			DROP ROLE IF EXISTS %s;
			DROP ROLE IF EXISTS %s`, roleName, unsafeRole)); err != nil {
			t.Errorf("cleanup isolated 060 roles: %v", err)
		}
	})
	freshURL := freshMigrationDatabase(t, "vane_profile_"+suffix)
	db, err := sql.Open("pgx", freshURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	dir, err := fs.Sub(migrationsFS, "migrations")
	if err != nil {
		t.Fatal(err)
	}
	baseProvider, err := goose.NewProvider(goose.DialectPostgres, db, dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := baseProvider.UpTo(t.Context(), 59); err != nil {
		t.Fatalf("migrate isolated database to 059: %v", err)
	}
	provider, err := goose.NewProvider(
		goose.DialectPostgres, db, isolatedMigration060FS(t, roleName))
	if err != nil {
		t.Fatal(err)
	}
	return db, provider, roleName, unsafeRole
}

func isolatedMigration060FS(t *testing.T, roleName string) fs.FS {
	t.Helper()
	raw, err := fs.ReadFile(
		migrationsFS, "migrations/060_profile_manual_authority.sql")
	if err != nil {
		t.Fatal(err)
	}
	return fstest.MapFS{
		"060_profile_manual_authority.sql": &fstest.MapFile{
			Data: []byte(strings.ReplaceAll(
				string(raw), profileEditorRole, roleName)),
		},
	}
}

func TestMigration060ConcurrentAcrossIndependentDatabases(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL")
	}
	roleName := fmt.Sprintf("vane_profile_editor_race_%d", time.Now().UnixNano())
	admin, err := sql.Open("pgx", dbURL)
	if err != nil {
		t.Fatal(err)
	}
	// Register this first: the database cleanups registered below run first,
	// releasing database-local dependencies before this cluster role is dropped.
	t.Cleanup(func() {
		defer admin.Close()
		if _, err := admin.ExecContext(
			context.Background(), "DROP ROLE IF EXISTS "+roleName,
		); err != nil {
			t.Errorf("cleanup concurrent migration role: %v", err)
		}
	})
	var roleExists bool
	if err := admin.QueryRowContext(t.Context(),
		`SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=$1)`,
		roleName).Scan(&roleExists); err != nil {
		t.Fatal(err)
	}
	if roleExists {
		t.Fatalf("isolated race role unexpectedly exists: %s", roleName)
	}

	firstURL := freshMigrationDatabase(t, "vane_profile_role_race_a")
	secondURL := freshMigrationDatabase(t, "vane_profile_role_race_b")
	dir, err := fs.Sub(migrationsFS, "migrations")
	if err != nil {
		t.Fatal(err)
	}
	raceFS := isolatedMigration060FS(t, roleName)
	type target struct {
		db       *sql.DB
		provider *goose.Provider
	}
	targets := make([]target, 2)
	for i, dbURL := range []string{firstURL, secondURL} {
		db, err := sql.Open("pgx", dbURL)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		provider, err := goose.NewProvider(goose.DialectPostgres, db, dir)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := provider.UpTo(t.Context(), 59); err != nil {
			t.Fatalf("database %d migrate to 059: %v", i, err)
		}
		raceProvider, err := goose.NewProvider(
			goose.DialectPostgres, db, raceFS)
		if err != nil {
			t.Fatal(err)
		}
		targets[i] = target{db: db, provider: raceProvider}
	}

	start := make(chan struct{})
	errs := make([]error, len(targets))
	var wg sync.WaitGroup
	for i := range targets {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, errs[i] = targets[i].provider.UpTo(t.Context(), 60)
		}()
	}
	close(start)
	wg.Wait()
	if err := admin.QueryRowContext(t.Context(),
		`SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=$1)`,
		roleName).Scan(&roleExists); err != nil {
		t.Fatal(err)
	}
	if !roleExists {
		t.Fatalf("concurrent migration did not create isolated role %s", roleName)
	}
	for i, err := range errs {
		if err != nil {
			t.Fatalf("database %d concurrent UpTo(60): %v", i, err)
		}
		var version int64
		var revisionExists, receiptExists bool
		if err := targets[i].db.QueryRowContext(t.Context(), `
			SELECT
			  (SELECT COALESCE(max(version_id),0)
			     FROM goose_db_version WHERE is_applied),
			  to_regclass('public.profile_edit_revisions') IS NOT NULL,
			  to_regclass('public.profile_edit_receipts') IS NOT NULL`,
		).Scan(&version, &revisionExists, &receiptExists); err != nil {
			t.Fatal(err)
		}
		if version != 60 || !revisionExists || !receiptExists {
			t.Fatalf(
				"database %d incomplete migration: version=%d tables=%t/%t",
				i, version, revisionExists, receiptExists,
			)
		}
	}
}

func TestMigration060RejectsUnsafeJSONBShapes(t *testing.T) {
	db, provider := migration035Scratch(t)
	ctx := t.Context()
	if _, err := provider.UpTo(ctx, 60); err != nil {
		t.Fatalf("migrate to 060: %v", err)
	}
	var tenantID, userID int64
	if err := db.QueryRowContext(ctx,
		`INSERT INTO tenants DEFAULT VALUES RETURNING id`,
	).Scan(&tenantID); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx,
		`INSERT INTO users (feishu_open_id,name)
		 VALUES ($1,'migration 060 JSON') RETURNING id`,
		fmt.Sprintf("migration-060-json-%d", time.Now().UnixNano()),
	).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO memberships (tenant_id,user_id,role)
		 VALUES ($1,$2,'owner')`, tenantID, userID); err != nil {
		t.Fatal(err)
	}

	valid := `{"exists":true,"industry":"AI","occupation":"Engineer",` +
		`"tags":["safe"],"removed_tags":[]}`
	invalidShapes := map[string]string{
		"extra summary": `{"exists":true,"industry":"AI",` +
			`"occupation":"Engineer","tags":[],"removed_tags":[],` +
			`"summary":"must not enter audit"}`,
		"missing key": `{"exists":true,"industry":"AI",` +
			`"occupation":"Engineer","tags":[]}`,
		"non-string tag": `{"exists":true,"industry":"AI",` +
			`"occupation":"Engineer","tags":["safe",7],"removed_tags":[]}`,
	}
	for name, invalid := range invalidShapes {
		t.Run(name, func(t *testing.T) {
			_, err := db.ExecContext(ctx, `
				INSERT INTO profile_edit_revisions (
				  tenant_id,user_id,actor_user_id,kind,
				  before_fields,after_fields,result_updated_at
				) VALUES ($1,$2,$2,'edit',$3::jsonb,$4::jsonb,clock_timestamp())`,
				tenantID, userID, invalid, valid)
			assertSQLState(t, err, "23514")
		})
	}

	var revisionID int64
	if err := db.QueryRowContext(ctx, `
		INSERT INTO profile_edit_revisions (
		  tenant_id,user_id,actor_user_id,kind,
		  before_fields,after_fields,result_updated_at
		) VALUES ($1,$2,$2,'edit',$3::jsonb,$3::jsonb,clock_timestamp())
		RETURNING id`,
		tenantID, userID, valid,
	).Scan(&revisionID); err != nil {
		t.Fatal(err)
	}
	leakingResponse := `{"industry":"AI","occupation":"Engineer",` +
		`"tags":[],"removed_tags":[],"summary":"safe summary",` +
		`"created_at":"2026-01-01T00:00:00Z",` +
		`"updated_at":"2026-01-01T00:00:00Z","user_id":123}`
	_, err := db.ExecContext(ctx, `
		INSERT INTO profile_edit_receipts (
		  tenant_id,user_id,idempotency_key,request_digest,
		  revision_id,response_profile
		) VALUES ($1,$2,'leaking-response',$3,$4,$5::jsonb)`,
		tenantID, userID, strings.Repeat("a", 64),
		revisionID, leakingResponse)
	assertSQLState(t, err, "23514")
}

func assertSQLState(t *testing.T, err error, want string) {
	t.Helper()
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != want {
		t.Fatalf("SQLSTATE=%v, want %s (err=%v)", pgErr, want, err)
	}
}
