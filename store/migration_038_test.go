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

func migration038Scratch(t *testing.T) (*sql.DB, *goose.Provider) {
	t.Helper()
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("DATABASE_URL is not set; skipping migration 038 test")
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
	if _, err := provider.UpTo(t.Context(), 38); err != nil {
		t.Fatalf("migrate to 038: %v", err)
	}
	return db, provider
}

func TestMigration038RestrictedOperatorAndDefinerBoundary(t *testing.T) {
	db, _ := migration038Scratch(t)
	var (
		version                                        int
		noLogin, noSuper, noCreateDB, noCreateRole     bool
		noReplication, noBypassRLS, noInherit          bool
		ownerMember, appToOperator, operatorToApp      bool
		controlDefiner, controlPublic, controlOperator bool
		helperDefiner, helperPublic, helperOperator    bool
		operatorTableGrants, operatorSequenceGrants    int
	)
	if err := db.QueryRowContext(t.Context(), `
		SELECT
		  (SELECT COALESCE(max(version_id),0)
		     FROM goose_db_version WHERE is_applied),
		  NOT r.rolcanlogin, NOT r.rolsuper, NOT r.rolcreatedb,
		  NOT r.rolcreaterole, NOT r.rolreplication, NOT r.rolbypassrls,
		  NOT r.rolinherit,
		  pg_has_role(current_user, r.oid, 'MEMBER'),
		  pg_has_role('vane_app', r.oid, 'MEMBER'),
		  pg_has_role(r.oid, 'vane_app', 'MEMBER'),
		  control.prosecdef,
		  EXISTS (
		      SELECT 1 FROM aclexplode(control.proacl) acl
		       WHERE acl.grantee=0 AND acl.privilege_type='EXECUTE'
		  ),
		  has_function_privilege(
		      r.oid,
		      'task_run_snapshot_v2_cutover_control(bigint,bigint,text,text)',
		      'EXECUTE'),
		  helper.prosecdef,
		  EXISTS (
		      SELECT 1 FROM aclexplode(helper.proacl) acl
		       WHERE acl.grantee=0 AND acl.privilege_type='EXECUTE'
		  ),
		  has_function_privilege(
		      r.oid,
		      'task_run_snapshot_v2_cutover_row_exact(bigint)',
		      'EXECUTE'),
		  (SELECT count(*) FROM information_schema.role_table_grants
		    WHERE grantee='vane_snapshot_cutover_operator'),
		  (SELECT count(*) FROM information_schema.role_usage_grants
		    WHERE grantee='vane_snapshot_cutover_operator'
		      AND object_type='SEQUENCE')
		 FROM pg_roles r
		 JOIN pg_proc control
		   ON control.oid =
		      'task_run_snapshot_v2_cutover_control(bigint,bigint,text,text)'::regprocedure
		 JOIN pg_proc helper
		   ON helper.oid =
		      'task_run_snapshot_v2_cutover_row_exact(bigint)'::regprocedure
		WHERE r.rolname='vane_snapshot_cutover_operator'`,
	).Scan(
		&version,
		&noLogin, &noSuper, &noCreateDB, &noCreateRole,
		&noReplication, &noBypassRLS, &noInherit,
		&ownerMember, &appToOperator, &operatorToApp,
		&controlDefiner, &controlPublic, &controlOperator,
		&helperDefiner, &helperPublic, &helperOperator,
		&operatorTableGrants, &operatorSequenceGrants,
	); err != nil {
		t.Fatal(err)
	}
	if version != 38 || !noLogin || !noSuper || !noCreateDB ||
		!noCreateRole || !noReplication || !noBypassRLS || !noInherit ||
		!ownerMember || appToOperator || operatorToApp ||
		!controlDefiner || controlPublic || !controlOperator ||
		!helperDefiner || helperPublic || helperOperator ||
		operatorTableGrants != 0 || operatorSequenceGrants != 0 {
		t.Fatalf("038 role/definer boundary is not minimal: "+
			"version=%d attrs=%v/%v/%v/%v/%v/%v/%v memberships=%v/%v/%v "+
			"control=%v/%v/%v helper=%v/%v/%v grants=%d/%d",
			version, noLogin, noSuper, noCreateDB, noCreateRole,
			noReplication, noBypassRLS, noInherit,
			ownerMember, appToOperator, operatorToApp,
			controlDefiner, controlPublic, controlOperator,
			helperDefiner, helperPublic, helperOperator,
			operatorTableGrants, operatorSequenceGrants)
	}

	if _, err := db.ExecContext(t.Context(),
		`SELECT * FROM task_run_snapshot_v2_cutover_control(
		    1,1,'migration-038-owner-direct','activate')`); err == nil {
		t.Fatal("function owner directly entered cutover definer")
	} else {
		requireSQLState038(t, err, "42501")
	}
	appTx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := appTx.ExecContext(t.Context(),
		`SET LOCAL ROLE vane_app`); err != nil {
		_ = appTx.Rollback()
		t.Fatal(err)
	}
	if _, err := appTx.ExecContext(t.Context(),
		`SELECT * FROM task_run_snapshot_v2_cutover_control(
		    1,1,'migration-038-app-direct','activate')`); err == nil {
		_ = appTx.Rollback()
		t.Fatal("vane_app directly entered cutover definer")
	} else {
		requireSQLState038(t, err, "42501")
	}
	if err := appTx.Rollback(); err != nil {
		t.Fatal(err)
	}

	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(t.Context(),
		`SET LOCAL ROLE vane_snapshot_cutover_operator`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(),
		`SELECT count(*) FROM task_run_snapshot_v2_cutover_events`); err == nil {
		t.Fatal("operator read raw cutover events")
	} else {
		requireSQLState038(t, err, "42501")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	for name, args := range map[string][4]any{
		"null tenant": {nil, int64(1), "migration-038-task", "activate"},
		"null user":   {int64(1), nil, "migration-038-task", "activate"},
		"null task":   {int64(1), int64(1), nil, "activate"},
		"empty task":  {int64(1), int64(1), "", "activate"},
		"null action": {int64(1), int64(1), "migration-038-task", nil},
		"forged":      {int64(1), int64(1), "migration-038-task", "forged"},
	} {
		t.Run(name, func(t *testing.T) {
			tx, err := db.BeginTx(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback() }()
			if _, err := tx.ExecContext(t.Context(),
				`SET LOCAL ROLE vane_snapshot_cutover_operator`); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.ExecContext(t.Context(),
				`SELECT * FROM task_run_snapshot_v2_cutover_control(
				    $1,$2,$3,$4)`,
				args[0], args[1], args[2], args[3]); err == nil {
				t.Fatal("control primitive accepted invalid argument")
			} else {
				requireSQLState038(t, err, "22023")
			}
		})
	}
	var events, pointers int
	if err := db.QueryRowContext(t.Context(), `
		SELECT
		  (SELECT count(*) FROM task_run_snapshot_v2_cutover_events),
		  (SELECT count(*) FROM schedules
		    WHERE run_snapshot_cutover_event_id IS NOT NULL)`,
	).Scan(&events, &pointers); err != nil {
		t.Fatal(err)
	}
	if events != 0 || pointers != 0 {
		t.Fatalf("invalid control mutated events/pointers = %d/%d",
			events, pointers)
	}
}

func TestMigration038EmptyDowngradeRemovesDatabaseCapabilities(t *testing.T) {
	db, provider := migration038Scratch(t)
	if _, err := provider.Down(t.Context()); err != nil {
		t.Fatalf("empty 038 downgrade: %v", err)
	}
	var version int
	var control, helper *string
	var directSchemaUsage bool
	if err := db.QueryRowContext(t.Context(), `
		SELECT
		  (SELECT COALESCE(max(version_id),0)
		     FROM goose_db_version WHERE is_applied),
		  to_regprocedure(
		      'task_run_snapshot_v2_cutover_control(bigint,bigint,text,text)'
		  )::text,
		  to_regprocedure(
		      'task_run_snapshot_v2_cutover_row_exact(bigint)'
		  )::text,
		  EXISTS (
		      SELECT 1
		        FROM pg_namespace n, aclexplode(n.nspacl) acl
		        JOIN pg_roles r ON r.oid=acl.grantee
		       WHERE n.nspname='public'
		         AND r.rolname='vane_snapshot_cutover_operator'
		         AND acl.privilege_type='USAGE'
		  )`,
	).Scan(&version, &control, &helper, &directSchemaUsage); err != nil {
		t.Fatal(err)
	}
	if version != 37 || control != nil || helper != nil || directSchemaUsage {
		t.Fatalf("038 down version/functions/schema usage = %d/%v/%v/%v",
			version, control, helper, directSchemaUsage)
	}
}

func TestMigration038RefusesDurableStateDowngrade(t *testing.T) {
	db, provider := migration038Scratch(t)
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(),
		`SELECT set_config('app.tenant_id','1',true)`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `
		INSERT INTO task_run_snapshot_v2_cutover_events (
		    tenant_id,user_id,task_id,generation,action,
		    approved_definition_version,approved_definition_digest,
		    snapshot_high_watermark,audit_from_snapshot_id,
		    audit_count,audit_through_id
		) VALUES (
		    1,1,'migration-038-retained',1,'activate',
		    1,repeat('a',64),1,1,1,1
		)`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	_, err = provider.Down(t.Context())
	if err == nil || !strings.Contains(err.Error(), "refusing downgrade") {
		t.Fatalf("038 downgrade accepted durable state: %v", err)
	}
	var version, events int
	if err := db.QueryRowContext(t.Context(), `
		SELECT
		  (SELECT COALESCE(max(version_id),0)
		     FROM goose_db_version WHERE is_applied),
		  (SELECT count(*) FROM task_run_snapshot_v2_cutover_events)`,
	).Scan(&version, &events); err != nil {
		t.Fatal(err)
	}
	if version != 38 || events != 1 {
		t.Fatalf("failed 038 down version/events = %d/%d",
			version, events)
	}
}

func requireSQLState038(t *testing.T, err error, want string) {
	t.Helper()
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != want {
		t.Fatalf("SQLSTATE=%q want=%q err=%v", func() string {
			if pgErr == nil {
				return ""
			}
			return pgErr.Code
		}(), want, err)
	}
}
