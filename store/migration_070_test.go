package store

import (
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestMigration071SynthesisRolesAreLeastPrivilegeAndEmptyDown(
	t *testing.T,
) {
	_, db, provider := openMigration066Database(
		t, "vane_executive_brief_071_acl")
	if _, err := provider.UpTo(t.Context(), 71); err != nil {
		t.Fatal(err)
	}
	for _, role := range []string{
		"vane_brief_synthesis_writer",
		"vane_brief_synthesis_recovery",
	} {
		var (
			canLogin, inherit, bypassRLS, superuser bool
		)
		if err := db.QueryRowContext(t.Context(), `
			SELECT rolcanlogin,rolinherit,rolbypassrls,rolsuper
			  FROM pg_roles WHERE rolname=$1`, role,
		).Scan(&canLogin, &inherit, &bypassRLS, &superuser); err != nil {
			t.Fatal(err)
		}
		if canLogin || inherit || bypassRLS || superuser {
			t.Fatalf("unsafe synthesis role %s: %t/%t/%t/%t",
				role, canLogin, inherit, bypassRLS, superuser)
		}
	}
	checkPrivilege := func(role, object, privilege string, want bool) {
		t.Helper()
		var got bool
		if err := db.QueryRowContext(t.Context(),
			`SELECT has_table_privilege($1,$2,$3)`,
			role, object, privilege).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("%s %s on %s = %t, want %t",
				role, privilege, object, got, want)
		}
	}
	checkPrivilege("vane_app",
		"executive_brief_synthesis_receipts", "SELECT", false)
	checkPrivilege("vane_app",
		"executive_brief_artifacts", "INSERT", false)
	checkPrivilege("vane_brief_synthesis_writer",
		"executive_brief_synthesis_receipts", "INSERT", true)
	checkPrivilege("vane_brief_synthesis_writer",
		"executive_brief_synthesis_receipts", "DELETE", false)
	var recoveryCanReadAny bool
	if err := db.QueryRowContext(t.Context(),
		`SELECT has_any_column_privilege(
		    'vane_brief_synthesis_recovery',
		    'executive_brief_synthesis_receipts','SELECT')`,
	).Scan(&recoveryCanReadAny); err != nil {
		t.Fatal(err)
	}
	if !recoveryCanReadAny {
		t.Fatal("recovery role cannot read receipt columns")
	}
	checkPrivilege("vane_brief_synthesis_recovery",
		"executive_brief_synthesis_receipts", "INSERT", false)
	var insertIdentity, insertStatus bool
	if err := db.QueryRowContext(t.Context(), `
		SELECT has_column_privilege(
		           'vane_brief_synthesis_recovery',
		           'executive_brief_synthesis_receipts',
		           'run_outcome_id','INSERT'),
		       has_column_privilege(
		           'vane_brief_synthesis_recovery',
		           'executive_brief_synthesis_receipts',
		           'status','INSERT')`,
	).Scan(&insertIdentity, &insertStatus); err != nil {
		t.Fatal(err)
	}
	if !insertIdentity || insertStatus {
		t.Fatalf("recovery receipt INSERT boundary = %t/%t",
			insertIdentity, insertStatus)
	}
	checkPrivilege("vane_brief_synthesis_recovery",
		"executive_brief_artifacts", "SELECT", false)
	var recoveryArtifactRead, recoveryArtifactInsert,
		recoveryArtifactDelete bool
	if err := db.QueryRowContext(t.Context(), `
		SELECT has_column_privilege(
		           'vane_brief_synthesis_recovery',
		           'executive_brief_artifacts','payload','SELECT'),
		       has_column_privilege(
		           'vane_brief_synthesis_recovery',
		           'executive_brief_artifacts','id','INSERT'),
		       has_table_privilege(
		           'vane_brief_synthesis_recovery',
		           'executive_brief_artifacts','DELETE')`,
	).Scan(
		&recoveryArtifactRead, &recoveryArtifactInsert,
		&recoveryArtifactDelete); err != nil {
		t.Fatal(err)
	}
	if !recoveryArtifactRead || !recoveryArtifactInsert ||
		recoveryArtifactDelete {
		t.Fatalf("recovery artifact boundary = %t/%t/%t",
			recoveryArtifactRead, recoveryArtifactInsert,
			recoveryArtifactDelete)
	}

	if _, err := provider.DownTo(t.Context(), 70); err != nil {
		t.Fatalf("empty 071 Down failed: %v", err)
	}
	for _, object := range []string{
		"executive_brief_synthesis_receipts",
		"executive_brief_artifacts",
	} {
		var exists bool
		if err := db.QueryRowContext(t.Context(),
			`SELECT to_regclass($1) IS NOT NULL`,
			"public."+object).Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if exists {
			t.Fatalf("071 Down left %s", object)
		}
	}
	var roleCount, privilegedRoleCount int
	if err := db.QueryRowContext(t.Context(), `
		SELECT count(*),
		       count(*) FILTER (
		           WHERE has_any_column_privilege(
		                     rolname,'brief_snapshots','SELECT')
		       )
		  FROM pg_roles
		 WHERE rolname = ANY($1)`,
		[]string{
			"vane_brief_synthesis_writer",
			"vane_brief_synthesis_recovery",
		},
	).Scan(&roleCount, &privilegedRoleCount); err != nil {
		t.Fatal(err)
	}
	if roleCount != 2 || privilegedRoleCount != 0 {
		t.Fatalf("071 Down role boundary roles=%d privileged=%d",
			roleCount, privilegedRoleCount)
	}
}

func TestMigration071DownRefusalTextIsExplicit(t *testing.T) {
	body, err := migrationsFS.ReadFile(
		"migrations/071_executive_brief_artifacts.sql")
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	for _, want := range []string{
		"NOLOGIN NOINHERIT",
		"NOBYPASSRLS",
		"converge to deterministic fallback",
		"refusing Down while executive Brief state exists",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("migration 071 lacks %q", want)
		}
	}
}

func TestMigration070PrivilegesAndDownRefusal(t *testing.T) {
	db, provider := migration035Scratch(t)
	ctx := t.Context()
	if _, err := provider.UpTo(ctx, 70); err != nil {
		t.Fatalf("migrate to 070: %v", err)
	}
	var (
		continuatorDelete bool
		continuatorInsert bool
		proposerToolRead  bool
		proposerDelete    bool
	)
	if err := db.QueryRowContext(ctx, `
		SELECT
		  has_table_privilege(
		    'vane_agent_action_continuator','subscriptions','DELETE'),
		  has_table_privilege(
		    'vane_agent_action_continuator','subscriptions','INSERT'),
		  has_column_privilege(
		    'vane_agent_action_proposer','agent_action_continuations',
		    'tool_name','SELECT'),
		  has_table_privilege(
		    'vane_agent_action_proposer','subscriptions','DELETE')`,
	).Scan(
		&continuatorDelete, &continuatorInsert,
		&proposerToolRead, &proposerDelete,
	); err != nil {
		t.Fatal(err)
	}
	if !continuatorDelete || continuatorInsert ||
		!proposerToolRead || proposerDelete {
		t.Fatalf(
			"070 role matrix continuator(delete/insert)=%v/%v proposer(tool/delete)=%v/%v",
			continuatorDelete, continuatorInsert,
			proposerToolRead, proposerDelete,
		)
	}
	if _, err := provider.DownTo(ctx, 69); err != nil {
		t.Fatalf("empty 070 Down: %v", err)
	}
	if _, err := provider.UpTo(ctx, 70); err != nil {
		t.Fatalf("reapply 070: %v", err)
	}

	var tenantID, userID, sessionID int64
	if err := db.QueryRowContext(ctx,
		`INSERT INTO tenants DEFAULT VALUES RETURNING id`,
	).Scan(&tenantID); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `
		INSERT INTO users (feishu_open_id,name)
		VALUES ('migration-070-user','migration 070') RETURNING id`,
	).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO memberships (tenant_id,user_id,role)
		VALUES ($1,$2,'owner')`,
		tenantID, userID,
	); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `
		INSERT INTO agent_sessions (tenant_id,user_id)
		VALUES ($1,$2) RETURNING id`,
		tenantID, userID,
	).Scan(&sessionID); err != nil {
		t.Fatal(err)
	}
	const actionID = "migration-070-remove-source"
	if _, err := db.ExecContext(ctx, `
		INSERT INTO pending_actions (
		  id,tenant_id,user_id,session_id,tool_name,args,summary,
		  expires_at,execution_version
		) VALUES (
		  $1,$2,$3,$4,'remove_source','{"source_ids":[1]}',
		  'remove source',clock_timestamp()+interval '1 hour',2
		)`,
		actionID, tenantID, userID, sessionID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_action_continuations (
		  action_id,tenant_id,user_id,session_id,tool_name,source_id,
		  canonical_args,args_digest,tool_spec_version,tool_spec,
		  tool_spec_digest,tool_policy_version,tool_policy,
		  tool_policy_digest,adapter_version,success_messages,
		  success_digest,not_found_messages,not_found_digest
		) VALUES (
		  $1,$2,$3,$4,'remove_source',1,
		  convert_to('{"source_ids":[1]}','UTF8'),
		  encode(sha256(convert_to('{"source_ids":[1]}','UTF8')),'hex'),
		  'vane.agent-tool-spec/v1',convert_to('{}','UTF8'),
		  encode(sha256(convert_to('{}','UTF8')),'hex'),
		  'vane.agent-tool-policy/v1',convert_to('{}','UTF8'),
		  encode(sha256(convert_to('{}','UTF8')),'hex'),
		  'vane.remove-source/postgres/v1',convert_to('[]','UTF8'),
		  encode(sha256(convert_to('[]','UTF8')),'hex'),
		  convert_to('[]','UTF8'),
		  encode(sha256(convert_to('[]','UTF8')),'hex')
		)`,
		actionID, tenantID, userID, sessionID,
	); err != nil {
		t.Fatal(err)
	}
	_, err := provider.DownTo(ctx, 69)
	if err == nil || !strings.Contains(
		err.Error(),
		"refusing downgrade while remove_source durable actions exist",
	) {
		t.Fatalf("070 Down accepted durable remove_source: %v", err)
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "P0001" {
		t.Fatalf("070 Down refusal SQLSTATE=%v error=%v", pgErr, err)
	}
}
