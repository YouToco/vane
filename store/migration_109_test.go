package store

import (
	"context"
	"database/sql"
	"errors"
	"io/fs"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pressly/goose/v3"
)

func TestMigration109NativeResearchCreationBoundaryPostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required")
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
	if _, err := provider.UpTo(t.Context(), 109); err != nil {
		t.Fatal(err)
	}

	for _, signature := range []string{
		"commit_native_research_task_creation_v3_v1(text,bigint,bigint,text,bigint,text,text,bytea,bytea,bytea,bytea,text,text,smallint)",
		"begin_native_research_task_activation_v3_v1(text,bigint,bigint,text,bigint,text,text,text,text,smallint)",
		"commit_native_research_task_activation_v3_v1(text,bigint,bigint,text,bigint,text,text,text,text,smallint)",
		"cleanup_native_research_task_creation_v3_v1(text,bigint,bigint,text,bigint,text,text,text,smallint)",
	} {
		var publicExecute, appExecute, coordinatorExecute, securityDefiner, safePath bool
		var owner string
		if err := db.QueryRowContext(t.Context(), `
			SELECT has_function_privilege('public',p.oid,'EXECUTE'),
			       has_function_privilege('vane_app',p.oid,'EXECUTE'),
			       has_function_privilege('vane_native_v3_creation_coordinator',p.oid,'EXECUTE'),
			       p.prosecdef,p.proowner::regrole::text,
			       p.proconfig=ARRAY['search_path=pg_catalog, public, pg_temp']::text[]
			  FROM pg_proc p WHERE p.oid=$1::regprocedure`, signature).Scan(
			&publicExecute, &appExecute, &coordinatorExecute,
			&securityDefiner, &owner, &safePath); err != nil {
			t.Fatal(err)
		}
		if publicExecute || appExecute || !coordinatorExecute || !securityDefiner ||
			owner == "vane_app" || !safePath {
			t.Fatalf("unsafe native V3 function %s public=%v app=%v coordinator=%v definer=%v owner=%q path=%v",
				signature, publicExecute, appExecute, coordinatorExecute,
				securityDefiner, owner, safePath)
		}
	}
	const maturitySignature = "native_research_schedule_mature_v3_v1(bigint,bigint,text)"
	var publicMaturity, appMaturity, executorMaturity, maturityDefiner, maturitySafePath bool
	if err := db.QueryRowContext(t.Context(), `
		SELECT has_function_privilege('public',p.oid,'EXECUTE'),
		       has_function_privilege('vane_app',p.oid,'EXECUTE'),
		       has_function_privilege('vane_research_v3_executor',p.oid,'EXECUTE'),
		       p.prosecdef,
		       p.proconfig=ARRAY['search_path=pg_catalog, public, pg_temp']::text[]
		  FROM pg_proc p WHERE p.oid=$1::regprocedure`, maturitySignature).Scan(
		&publicMaturity, &appMaturity, &executorMaturity,
		&maturityDefiner, &maturitySafePath); err != nil {
		t.Fatal(err)
	}
	if publicMaturity || appMaturity || !executorMaturity ||
		!maturityDefiner || !maturitySafePath {
		t.Fatalf("unsafe native maturity predicate public=%v app=%v executor=%v definer=%v path=%v",
			publicMaturity, appMaturity, executorMaturity,
			maturityDefiner, maturitySafePath)
	}

	var constraintDefinition string
	if err := db.QueryRowContext(t.Context(), `
		SELECT pg_get_constraintdef(oid)
		  FROM pg_constraint
		 WHERE conname='task_creation_operations_execution_version_current'`,
	).Scan(&constraintDefinition); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(constraintDefinition, "ANY (ARRAY[1, 2])") {
		t.Fatalf("migration 109 did not retain only V1/V2 creation protocols: %s",
			constraintDefinition)
	}
	if err := db.QueryRowContext(t.Context(), `
		SELECT pg_get_constraintdef(oid)
		  FROM pg_constraint
		 WHERE conname='task_creation_operations_protocol_tool_binding'`,
	).Scan(&constraintDefinition); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(constraintDefinition, "execution_version = 1") ||
		!strings.Contains(constraintDefinition, "tool_name = 'create_schedule'") ||
		!strings.Contains(constraintDefinition, "execution_version = 2") ||
		!strings.Contains(constraintDefinition, "tool_name = 'manage_tasks'") {
		t.Fatalf("migration 109 protocol/tool binding differs: %s", constraintDefinition)
	}

	var appAuthorityInsert, appAuthorityUpdate bool
	if err := db.QueryRowContext(t.Context(), `
		SELECT has_table_privilege('vane_app','research_v3_delivery_authorities','INSERT'),
		       has_table_privilege('vane_app','research_v3_delivery_authorities','UPDATE')`,
	).Scan(&appAuthorityInsert, &appAuthorityUpdate); err != nil {
		t.Fatal(err)
	}
	if appAuthorityInsert || appAuthorityUpdate {
		t.Fatalf("vane_app received direct authority mutation insert=%v update=%v",
			appAuthorityInsert, appAuthorityUpdate)
	}
	roleTx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := roleTx.ExecContext(t.Context(), `SET LOCAL ROLE vane_app`); err != nil {
		t.Fatal(err)
	}
	_, directErr := roleTx.ExecContext(t.Context(), `
		INSERT INTO research_v3_delivery_authorities
		 (tenant_id,user_id,task_id,generation,definition_version,definition_digest,
		  target_action_digest,action_authorization_digest,status)
		 VALUES (1,1,'forbidden',1,1,repeat('0',64),repeat('0',64),repeat('0',64),'staged')`)
	if directErr == nil || !postgresCodeIs(directErr, "42501") {
		t.Fatalf("vane_app direct authority insert err=%v", directErr)
	}
	if err := roleTx.Rollback(); err != nil {
		t.Fatal(err)
	}
	roleTx, err = db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := roleTx.ExecContext(t.Context(), `SET LOCAL ROLE vane_app`); err != nil {
		t.Fatal(err)
	}
	_, executeErr := roleTx.ExecContext(t.Context(), `
		SELECT begin_native_research_task_activation_v3_v1(
		 NULL::text,NULL::bigint,NULL::bigint,NULL::text,NULL::bigint,
		 NULL::text,NULL::text,NULL::text,NULL::text,2::smallint)`)
	if executeErr == nil || !postgresCodeIs(executeErr, "42501") {
		t.Fatalf("vane_app reached native V3 write function: %v", executeErr)
	}
	_ = roleTx.Rollback()
	roleTx, err = db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := roleTx.ExecContext(t.Context(),
		`SET LOCAL ROLE vane_native_v3_creation_coordinator`); err != nil {
		t.Fatal(err)
	}
	_, directErr = roleTx.ExecContext(t.Context(), `
		INSERT INTO research_v3_delivery_authorities
		 (tenant_id,user_id,task_id,generation,definition_version,definition_digest,
		  target_action_digest,action_authorization_digest,status)
		 VALUES (1,1,'forbidden',1,1,repeat('0',64),repeat('0',64),repeat('0',64),'staged')`)
	if directErr == nil || !postgresCodeIs(directErr, "42501") {
		t.Fatalf("coordinator gained direct authority insert: %v", directErr)
	}
	_ = roleTx.Rollback()
	roleTx, err = db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := roleTx.ExecContext(t.Context(),
		`SET LOCAL ROLE vane_native_v3_creation_coordinator`); err != nil {
		t.Fatal(err)
	}
	_, executeErr = roleTx.ExecContext(t.Context(), `
		SELECT begin_native_research_task_activation_v3_v1(
		 NULL::text,NULL::bigint,NULL::bigint,NULL::text,NULL::bigint,
		 NULL::text,NULL::text,NULL::text,NULL::text,2::smallint)`)
	if executeErr == nil || postgresCodeIs(executeErr, "42501") {
		t.Fatalf("coordinator did not reach guarded function body: %v", executeErr)
	}
	_ = roleTx.Rollback()

	var appMember, serverMember, coordinatorLogin, coordinatorInherit, coordinatorBypass bool
	if err := db.QueryRowContext(t.Context(), `
		SELECT pg_has_role('vane_app','vane_native_v3_creation_coordinator','MEMBER'),
		       EXISTS (
		         SELECT 1 FROM pg_roles runtime,pg_roles coordinator
		          WHERE runtime.rolname='vane_server_runtime'
		            AND coordinator.rolname='vane_native_v3_creation_coordinator'
		            AND pg_has_role(runtime.oid,coordinator.oid,'MEMBER')
		       ),
		       rolcanlogin,rolinherit,rolbypassrls
		  FROM pg_roles WHERE rolname='vane_native_v3_creation_coordinator'`).Scan(
		&appMember, &serverMember, &coordinatorLogin, &coordinatorInherit,
		&coordinatorBypass); err != nil {
		t.Fatal(err)
	}
	if appMember || serverMember || coordinatorLogin || coordinatorInherit || coordinatorBypass {
		t.Fatalf("unsafe coordinator role app_member=%v server_member=%v login=%v inherit=%v bypass=%v",
			appMember, serverMember, coordinatorLogin, coordinatorInherit, coordinatorBypass)
	}
	if err := ProvisionServerRuntime(t.Context(), scratchURL); err != nil {
		t.Fatalf("provision exact server runtime V2: %v", err)
	}
	if err := ProvisionServerRuntime(t.Context(), scratchURL); err != nil {
		t.Fatalf("replay exact server runtime V2 provision: %v", err)
	}
	if err := db.QueryRowContext(t.Context(), `
		SELECT EXISTS (
		  SELECT 1 FROM pg_roles runtime,pg_roles coordinator
		   WHERE runtime.rolname='vane_server_runtime'
		     AND coordinator.rolname='vane_native_v3_creation_coordinator'
		     AND pg_has_role(runtime.oid,coordinator.oid,'MEMBER'))`,
	).Scan(&serverMember); err != nil {
		t.Fatal(err)
	}
	if serverMember {
		t.Fatal("provisioned server runtime can enter native V3 coordinator")
	}
	const runtimePassword = "migration-109-runtime-password"
	if _, err := db.ExecContext(t.Context(),
		`ALTER ROLE vane_server_runtime LOGIN PASSWORD '`+runtimePassword+`'`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = db.ExecContext(cleanupCtx,
			`ALTER ROLE vane_server_runtime NOLOGIN PASSWORD NULL`)
	})
	runtimeStore, err := NewServerRuntime(
		t.Context(), serverRuntimeTestURL(t, scratchURL, runtimePassword))
	if err != nil {
		t.Fatalf("NewServerRuntime after migration 109 provisioning: %v", err)
	}
	runtimeTx, err := runtimeStore.pool.Begin(t.Context())
	if err != nil {
		runtimeStore.Close()
		t.Fatal(err)
	}
	if _, err := runtimeTx.Exec(t.Context(),
		`SET LOCAL ROLE vane_native_v3_creation_coordinator`); err == nil ||
		!postgresCodeIs(err, "42501") {
		_ = runtimeTx.Rollback(t.Context())
		runtimeStore.Close()
		t.Fatalf("server runtime entered native V3 coordinator: %v", err)
	}
	_ = runtimeTx.Rollback(t.Context())
	runtimeStore.Close()
	if _, err := db.ExecContext(t.Context(),
		`ALTER ROLE vane_server_runtime NOLOGIN PASSWORD NULL`); err != nil {
		t.Fatal(err)
	}
	for _, signature := range []string{
		"commit_native_research_task_creation_v3_v1(text,bigint,bigint,text,bigint,text,text,bytea,bytea,bytea,bytea,text,text,smallint)",
		"begin_native_research_task_activation_v3_v1(text,bigint,bigint,text,bigint,text,text,text,text,smallint)",
		"commit_native_research_task_activation_v3_v1(text,bigint,bigint,text,bigint,text,text,text,text,smallint)",
		"cleanup_native_research_task_creation_v3_v1(text,bigint,bigint,text,bigint,text,text,text,smallint)",
	} {
		var runtimeExecute bool
		if err := db.QueryRowContext(t.Context(), `
			SELECT has_function_privilege('vane_server_runtime',$1::regprocedure,'EXECUTE')`,
			signature).Scan(&runtimeExecute); err != nil {
			t.Fatal(err)
		}
		if runtimeExecute {
			t.Fatalf("server runtime can execute native V3 function %s", signature)
		}
	}

	migration, err := fs.ReadFile(dir, "109_native_research_task_creation.sql")
	if err != nil {
		t.Fatal(err)
	}
	up := strings.Split(string(migration), "-- +goose Down")[0]
	for _, forbidden := range []string{"task_fetch_targets", "fetch_targets", "tool_calls"} {
		if strings.Contains(strings.ToLower(up), forbidden) {
			t.Fatalf("native V3 creation migration references retired execution state %q", forbidden)
		}
	}
	for _, signature := range []string{
		"commit_native_research_task_creation_v3_v1(text,bigint,bigint,text,bigint,text,text,bytea,bytea,bytea,bytea,text,text,smallint)",
		"begin_native_research_task_activation_v3_v1(text,bigint,bigint,text,bigint,text,text,text,text,smallint)",
		"commit_native_research_task_activation_v3_v1(text,bigint,bigint,text,bigint,text,text,text,text,smallint)",
		"cleanup_native_research_task_creation_v3_v1(text,bigint,bigint,text,bigint,text,text,text,smallint)",
	} {
		var definition string
		if err := db.QueryRowContext(t.Context(),
			`SELECT pg_get_functiondef($1::regprocedure)`, signature).Scan(&definition); err != nil {
			t.Fatal(err)
		}
		advisory := strings.Index(definition, "pg_advisory_xact_lock(hashtextextended")
		rowLock := firstPositiveIndex(definition, "FOR UPDATE", "FOR SHARE")
		if advisory < 0 || rowLock < 0 || advisory >= rowLock {
			t.Fatalf("native V3 lock order differs for %s advisory=%d row=%d",
				signature, advisory, rowLock)
		}
	}

	var userID, tenantID int64
	if err := db.QueryRowContext(t.Context(), `
		INSERT INTO users(feishu_open_id,name) VALUES ('migration-109-owner','owner') RETURNING id`,
	).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(t.Context(), `
		INSERT INTO tenants(status,plan) VALUES ('active','free') RETURNING id`,
	).Scan(&tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `
		INSERT INTO memberships(tenant_id,user_id,role) VALUES($1,$2,'owner')`,
		tenantID, userID); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []struct {
		id      string
		tool    string
		version int
	}{
		{"migration-109-invalid-v1", "manage_tasks", 1},
		{"migration-109-invalid-v2", "create_schedule", 2},
	} {
		_, err := db.ExecContext(t.Context(), `
			INSERT INTO task_creation_operations
			 (id,tenant_id,user_id,tool_name,args,summary,status,expires_at,execution_version)
			 VALUES ($1,$2,$3,$4,'{}','invalid','pending',$5,$6)`,
			invalid.id, tenantID, userID, invalid.tool, time.Now().Add(time.Hour), invalid.version)
		if err == nil || !postgresCodeIs(err, "23514") {
			t.Fatalf("invalid protocol/tool binding %+v err=%v", invalid, err)
		}
	}
	if _, err := db.ExecContext(t.Context(), `
		INSERT INTO task_creation_operations
		 (id,tenant_id,user_id,tool_name,args,summary,status,expires_at,execution_version)
		 VALUES ('migration-109-v1',$1,$2,'create_schedule','{}','v1','pending',$3,1)`,
		tenantID, userID, time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("V1/create_schedule binding rejected: %v", err)
	}
	if _, err := db.ExecContext(t.Context(),
		`DELETE FROM task_creation_operations WHERE id='migration-109-v1'`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `
		INSERT INTO task_creation_operations
		 (id,tenant_id,user_id,tool_name,args,summary,status,expires_at,execution_version)
		 VALUES ('migration-109-v2',$1,$2,'manage_tasks','{}','v3','pending',$3,2)`,
		tenantID, userID, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.DownTo(t.Context(), 108); err == nil {
		t.Fatal("migration 109 downgraded with a native V2 operation")
	}
	if _, err := db.ExecContext(t.Context(),
		`DELETE FROM task_creation_operations WHERE id='migration-109-v2'`); err != nil {
		t.Fatal(err)
	}
	if err := DeprovisionServerRuntime(t.Context(), scratchURL); err != nil {
		t.Fatalf("deprovision exact server runtime V2: %v", err)
	}

	if _, err := provider.DownTo(t.Context(), 108); err != nil {
		t.Fatal(err)
	}
}

func TestMigration109UpgradesProvisionedLoginRuntimeWithoutLosingLogin(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required")
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
	if _, err := provider.UpTo(t.Context(), 108); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `SELECT provision_vane_server_runtime_v1()`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `ALTER ROLE vane_server_runtime LOGIN`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = db.ExecContext(cleanupCtx, `ALTER ROLE vane_server_runtime NOLOGIN`)
		_, _ = db.ExecContext(cleanupCtx, `SELECT deprovision_vane_server_runtime_v2()`)
	})
	if _, err := provider.UpTo(t.Context(), 109); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `SELECT provision_vane_server_runtime_v2()`); err != nil {
		t.Fatalf("upgrade provisioned runtime to exact V2 set: %v", err)
	}
	var login, coordinatorMember bool
	if err := db.QueryRowContext(t.Context(), `
		SELECT runtime.rolcanlogin,
		       pg_has_role(runtime.oid,coordinator.oid,'MEMBER')
		  FROM pg_roles runtime CROSS JOIN pg_roles coordinator
		 WHERE runtime.rolname='vane_server_runtime'
		   AND coordinator.rolname='vane_native_v3_creation_coordinator'`).Scan(
		&login, &coordinatorMember); err != nil {
		t.Fatal(err)
	}
	if !login || coordinatorMember {
		t.Fatalf("V2 upgrade lost deployed runtime state login=%v coordinator=%v",
			login, coordinatorMember)
	}
	if _, err := db.ExecContext(t.Context(), `ALTER ROLE vane_server_runtime NOLOGIN`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `SELECT deprovision_vane_server_runtime_v2()`); err != nil {
		t.Fatalf("deprovision upgraded runtime: %v", err)
	}
	var runtimeExists bool
	if err := db.QueryRowContext(t.Context(), `
		SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='vane_server_runtime')`).Scan(
		&runtimeExists); err != nil {
		t.Fatal(err)
	}
	if runtimeExists {
		t.Fatal("upgraded runtime deprovision left the cluster-global login role")
	}
}

func postgresCodeIs(err error, code string) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == code
}

func firstPositiveIndex(value string, candidates ...string) int {
	best := -1
	for _, candidate := range candidates {
		if index := strings.Index(value, candidate); index >= 0 && (best < 0 || index < best) {
			best = index
		}
	}
	return best
}
