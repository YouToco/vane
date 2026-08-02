package store

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pressly/goose/v3"
)

func TestMigration101KeepsExactCutoverAuthorityDarkAndTransitionGuarded(t *testing.T) {
	payload, err := migrationsFS.ReadFile("migrations/101_research_v3_exact_cutover.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.ReplaceAll(string(payload), "\r\n", "\n")
	required := []string{
		"vane_research_v3_cutover_operator NOLOGIN NOSUPERUSER",
		"definition_digest,target_action_digest,action_authorization_digest,status\n) ON research_v3_delivery_authorities TO vane_app",
		"GRANT UPDATE (status,enabled_at,revoked_at)",
		"GRANT UPDATE (phase,rollback_conflict_token,rollback_token_digest)",
		"ALTER ROLE vane_research_v3_cutover_operator NOLOGIN NOSUPERUSER",
		"exclude_definition_edit_during_research_v3_cutover",
		"exclude_research_v3_cutover_during_definition_edit",
		"NEW.updated_at := clock_timestamp()",
		"protect_research_v3_authority_transition",
		"protect_research_v3_cutover_transition",
		"OLD.status='revoked'",
		"OLD.phase='rollback_paused' AND NEW.phase IN",
		"('rolled_back','manual_intervention')",
		"REVOKE ALL ON FUNCTION protect_research_v3_authority_transition() FROM PUBLIC",
		"REVOKE ALL ON FUNCTION protect_research_v3_cutover_transition() FROM PUBLIC",
		"fence_research_v3_schedule_claims",
		"fence_research_v3_schedule_delete",
		"SET search_path=pg_catalog,pg_temp",
		"FROM public.research_v3_cutover_operations",
		"FROM public.task_definition_edit_operations",
		"pg_advisory_xact_lock(hashtextextended(",
		"approved_definition_version,approved_definition_digest",
	}
	for _, fragment := range required {
		if !strings.Contains(sql, fragment) {
			t.Fatalf("migration 101 is missing %q", fragment)
		}
	}
	forbidden := []string{
		"GRANT SELECT ON research_v3_delivery_authorities TO vane_app",
		"GRANT SELECT,INSERT,UPDATE ON research_v3_delivery_authorities TO vane_app",
		"GRANT SELECT,INSERT,UPDATE ON research_v3_cutover_operations TO vane_app",
		"research_v3_cutover_operations TO vane_app",
		"GRANT vane_research_v3_cutover_operator TO vane_server_runtime",
		"REFERENCES schedules (tenant_id,user_id,id) ON DELETE RESTRICT",
		"RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER\nSET search_path=pg_catalog,public,pg_temp",
		"require_research_v3_delivery_authority_for_claim_v1",
	}
	for _, fragment := range forbidden {
		if strings.Contains(sql, fragment) {
			t.Fatalf("migration 101 exposes forbidden authority: %q", fragment)
		}
	}
}

func TestMigration101CutoverJournalACLPostgres(t *testing.T) {
	if os.Getenv("DATABASE_URL") == "" {
		t.Skip("DATABASE_URL is required")
	}
	if err := Migrate(t.Context(), os.Getenv("DATABASE_URL")); err != nil {
		t.Fatal(err)
	}
	st, err := New(t.Context(), os.Getenv("DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	var (
		appCanReadWholeAuthority, appCanReadAuthorityScope      bool
		appCanReadAuthorityDigest, appCanReadAuthorityTimestamp bool
		appCanReadJournal, appCanReadTarget, appCanReadConflict bool
		operatorCanReadJournal, serverIsOperator                bool
	)
	if err := st.pool.QueryRow(t.Context(), `
		SELECT
		 has_table_privilege('vane_app','public.research_v3_delivery_authorities','SELECT'),
		 has_column_privilege('vane_app','public.research_v3_delivery_authorities','task_id','SELECT'),
		 has_column_privilege('vane_app','public.research_v3_delivery_authorities','action_authorization_digest','SELECT'),
		 has_column_privilege('vane_app','public.research_v3_delivery_authorities','enabled_at','SELECT'),
		 has_table_privilege('vane_app','public.research_v3_cutover_operations','SELECT'),
		 has_column_privilege('vane_app','public.research_v3_cutover_operations','target_action','SELECT'),
		 has_column_privilege('vane_app','public.research_v3_cutover_operations','frozen_conflict_token','SELECT'),
		 has_table_privilege('vane_research_v3_cutover_operator',
		                     'public.research_v3_cutover_operations','SELECT'),
		 COALESCE(pg_has_role(to_regrole('vane_server_runtime'),
		                      to_regrole('vane_research_v3_cutover_operator'),
		                      'MEMBER'),false)
	`).Scan(&appCanReadWholeAuthority, &appCanReadAuthorityScope,
		&appCanReadAuthorityDigest, &appCanReadAuthorityTimestamp,
		&appCanReadJournal, &appCanReadTarget, &appCanReadConflict,
		&operatorCanReadJournal, &serverIsOperator); err != nil {
		t.Fatal(err)
	}
	if appCanReadWholeAuthority || !appCanReadAuthorityScope ||
		!appCanReadAuthorityDigest || appCanReadAuthorityTimestamp ||
		appCanReadJournal || appCanReadTarget || appCanReadConflict ||
		!operatorCanReadJournal || serverIsOperator {
		t.Fatalf("unsafe migration 101 ACL: app_whole_authority=%t "+
			"app_authority_scope=%t app_authority_digest=%t app_authority_timestamp=%t "+
			"app_journal=%t app_target=%t app_conflict=%t operator_journal=%t "+
			"server_operator=%t", appCanReadWholeAuthority, appCanReadAuthorityScope,
			appCanReadAuthorityDigest, appCanReadAuthorityTimestamp,
			appCanReadJournal, appCanReadTarget, appCanReadConflict,
			operatorCanReadJournal, serverIsOperator)
	}
}

func TestMigration101ScheduleDeleteSerializesWithExactTaskClaimPostgres(t *testing.T) {
	if os.Getenv("DATABASE_URL") == "" {
		t.Skip("DATABASE_URL is required")
	}
	f := newResearchRunSpendFixtureWithToolBudgetV3(t, 20_000, 1, false)
	ctx := t.Context()
	digest := func(char string) string { return strings.Repeat(char, 64) }
	if _, err := f.store.pool.Exec(ctx,
		`INSERT INTO research_v3_delivery_authorities
		 (tenant_id,user_id,task_id,generation,definition_version,definition_digest,
		  target_action_digest,action_authorization_digest,status,enabled_at)
		 VALUES ($1,$2,$3,1,1,$4,$5,$6,'enabled',clock_timestamp())`,
		f.tenantID, f.userID, f.identity.TaskID, f.definitionDigest,
		digest("a"), digest("b")); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.pool.Exec(ctx,
		`INSERT INTO research_v3_cutover_operations
		 (tenant_id,user_id,task_id,idempotency_key,generation,definition_version,
		  definition_digest,frozen_schedule,frozen_schedule_digest,
		  frozen_conflict_token,conflict_token_digest,target_action,
		  target_action_digest,action_authorization_digest,original_paused,phase,
		  original_execution_mode,original_definition_version,original_definition_digest,
		  source_baseline_digest)
		 VALUES ($1,$2,$3,$4,1,1,$5,'frozen',$6,'token',$7,'target',$8,$9,false,'active',
		         'discover_at_run',1,$5,$5)`,
		f.tenantID, f.userID, f.identity.TaskID, "delete-race-"+f.identity.TemporalRunID,
		f.definitionDigest, digest("c"), digest("d"), digest("a"), digest("b")); err != nil {
		t.Fatal(err)
	}

	claimTx, err := f.store.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = claimTx.Rollback(context.Background()) }()
	lockKey := fmt.Sprintf("%d/%d/%s", f.tenantID, f.userID, f.identity.TaskID)
	if _, err := claimTx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1,101))`, lockKey); err != nil {
		t.Fatal(err)
	}
	var enabled bool
	if err := claimTx.QueryRow(ctx,
		`SELECT EXISTS (
		   SELECT 1 FROM research_v3_delivery_authorities authority
		   JOIN schedules schedule ON schedule.tenant_id=authority.tenant_id
		    AND schedule.user_id=authority.user_id AND schedule.id=authority.task_id
		   WHERE authority.tenant_id=$1 AND authority.user_id=$2 AND authority.task_id=$3
		     AND authority.status='enabled')`,
		f.tenantID, f.userID, f.identity.TaskID).Scan(&enabled); err != nil || !enabled {
		t.Fatalf("claim admission err=%v enabled=%t", err, enabled)
	}

	deleteDone := make(chan error, 1)
	go func() {
		_, deleteErr := f.store.pool.Exec(context.Background(),
			`DELETE FROM schedules WHERE tenant_id=$1 AND user_id=$2 AND id=$3`,
			f.tenantID, f.userID, f.identity.TaskID)
		deleteDone <- deleteErr
	}()
	select {
	case err := <-deleteDone:
		t.Fatalf("schedule delete bypassed claim lock: %v", err)
	case <-time.After(150 * time.Millisecond):
	}
	if err := claimTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-deleteDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("schedule delete did not resume after claim committed")
	}

	var scheduleCount int
	var status, phase string
	if err := f.store.pool.QueryRow(ctx,
		`SELECT count(*) FROM schedules WHERE tenant_id=$1 AND user_id=$2 AND id=$3`,
		f.tenantID, f.userID, f.identity.TaskID).Scan(&scheduleCount); err != nil {
		t.Fatal(err)
	}
	if err := f.store.pool.QueryRow(ctx,
		`SELECT authority.status,operation.phase
		   FROM research_v3_delivery_authorities authority
		   JOIN research_v3_cutover_operations operation
		     ON operation.tenant_id=authority.tenant_id AND operation.user_id=authority.user_id
		    AND operation.task_id=authority.task_id AND operation.generation=authority.generation
		  WHERE authority.tenant_id=$1 AND authority.user_id=$2 AND authority.task_id=$3`,
		f.tenantID, f.userID, f.identity.TaskID).Scan(&status, &phase); err != nil {
		t.Fatal(err)
	}
	if scheduleCount != 0 || status != "revoked" || phase != "manual_intervention" {
		t.Fatalf("schedule=%d authority=%s phase=%s", scheduleCount, status, phase)
	}

	afterDeleteTx, err := f.store.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = afterDeleteTx.Rollback(context.Background()) }()
	if _, err := afterDeleteTx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1,101))`, lockKey); err != nil {
		t.Fatal(err)
	}
	if err := afterDeleteTx.QueryRow(ctx,
		`SELECT EXISTS (
		   SELECT 1 FROM research_v3_delivery_authorities authority
		   JOIN schedules schedule ON schedule.tenant_id=authority.tenant_id
		    AND schedule.user_id=authority.user_id AND schedule.id=authority.task_id
		   WHERE authority.tenant_id=$1 AND authority.user_id=$2 AND authority.task_id=$3
		     AND authority.status='enabled')`,
		f.tenantID, f.userID, f.identity.TaskID).Scan(&enabled); err != nil {
		t.Fatal(err)
	}
	if enabled {
		t.Fatal("delete-wins ordering admitted a later delivery claim")
	}
}

// This test intentionally mutates cluster-global roles and rolls migration 101
// down/up. Run it only against a disposable PostgreSQL instance.
func TestMigration101HardensPreexistingUnsafeOperatorPostgres(t *testing.T) {
	if os.Getenv("VANE_RUN_DESTRUCTIVE_MIGRATION101_ROLE_TEST") != "1" {
		t.Skip("set VANE_RUN_DESTRUCTIVE_MIGRATION101_ROLE_TEST=1 on disposable PostgreSQL")
	}
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required")
	}
	if err := Migrate(t.Context(), databaseURL); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	dir, err := fs.Sub(migrationsFS, "migrations")
	if err != nil {
		t.Fatal(err)
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, db, dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.DownTo(t.Context(), 100); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	_, _ = db.ExecContext(ctx, `DROP ROLE IF EXISTS migration_101_unsafe_member`)
	_, _ = db.ExecContext(ctx, `DROP ROLE IF EXISTS migration_101_unsafe_parent`)
	if _, err := db.ExecContext(ctx, `
		CREATE ROLE migration_101_unsafe_member NOLOGIN;
		CREATE ROLE migration_101_unsafe_parent NOLOGIN;
		ALTER ROLE vane_research_v3_cutover_operator LOGIN SUPERUSER
		    CREATEDB CREATEROLE INHERIT REPLICATION BYPASSRLS;
		GRANT vane_research_v3_cutover_operator TO migration_101_unsafe_member;
		GRANT migration_101_unsafe_parent TO vane_research_v3_cutover_operator`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = db.ExecContext(ctx, `
			REVOKE vane_research_v3_cutover_operator FROM migration_101_unsafe_member;
			REVOKE migration_101_unsafe_parent FROM vane_research_v3_cutover_operator;
			DROP ROLE IF EXISTS migration_101_unsafe_member;
			DROP ROLE IF EXISTS migration_101_unsafe_parent`)
	}()
	if _, err := provider.Up(t.Context()); err != nil {
		t.Fatal(err)
	}
	var safe bool
	if err := db.QueryRowContext(ctx, `
		SELECT NOT rolcanlogin AND NOT rolsuper AND NOT rolcreatedb AND
		       NOT rolcreaterole AND NOT rolinherit AND NOT rolreplication AND
		       NOT rolbypassrls
		  FROM pg_roles WHERE rolname='vane_research_v3_cutover_operator'`).Scan(&safe); err != nil {
		t.Fatal(err)
	}
	if !safe {
		t.Fatal("migration 101 did not harden preexisting operator role")
	}
	var unsafeMemberships int
	if err := db.QueryRowContext(ctx, `
		SELECT count(*)
		  FROM pg_auth_members edge
		  JOIN pg_roles granted ON granted.oid=edge.roleid
		  JOIN pg_roles member ON member.oid=edge.member
		 WHERE (granted.rolname='vane_research_v3_cutover_operator'
		        AND member.rolname<>current_user)
		    OR member.rolname='vane_research_v3_cutover_operator'`).Scan(&unsafeMemberships); err != nil {
		t.Fatal(err)
	}
	if unsafeMemberships != 0 {
		t.Fatalf("unsafe operator memberships remain: %d", unsafeMemberships)
	}
}
