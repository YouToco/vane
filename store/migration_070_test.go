package store

import (
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

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
