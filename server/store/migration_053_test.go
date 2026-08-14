package store

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

const agentProjectionOperatorRole = "vane_agent_session_projection_operator"

func TestMigration053NormalizesPreexistingClusterRole(t *testing.T) {
	db, provider := migration035Scratch(t)
	ctx := t.Context()
	if _, err := db.ExecContext(ctx, `
		DO $$
		BEGIN
		  IF NOT EXISTS (
		    SELECT 1 FROM pg_roles
		     WHERE rolname='vane_agent_session_projection_operator'
		  ) THEN
		    CREATE ROLE vane_agent_session_projection_operator;
		  END IF;
		END $$;
		ALTER ROLE vane_agent_session_projection_operator
		  LOGIN INHERIT CREATEDB CREATEROLE BYPASSRLS;
		ALTER ROLE vane_agent_session_projection_operator
		  SET statement_timeout='1s'`); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.UpTo(ctx, 53); err != nil {
		t.Fatalf("053 must normalize a pre-existing cluster role: %v", err)
	}
	var safe bool
	if err := db.QueryRowContext(ctx, `
		SELECT NOT rolcanlogin AND NOT rolinherit AND NOT rolcreatedb AND
		       NOT rolcreaterole AND NOT rolbypassrls AND
		       rolconfig =
		         ARRAY['search_path=pg_catalog, public']::TEXT[]
		  FROM pg_roles WHERE rolname=$1`,
		agentProjectionOperatorRole,
	).Scan(&safe); err != nil {
		t.Fatal(err)
	}
	if !safe {
		t.Fatal("053 left the pre-existing cluster role unsafe")
	}
}

func TestMigration053RejectsPreexistingUnsafeMembership(t *testing.T) {
	db, provider := migration035Scratch(t)
	ctx := t.Context()
	if _, err := db.ExecContext(ctx, `
		DO $$
		BEGIN
		  IF NOT EXISTS (
		    SELECT 1 FROM pg_roles
		     WHERE rolname='vane_agent_projection_unsafe_member'
		  ) THEN
		    CREATE ROLE vane_agent_projection_unsafe_member NOLOGIN;
		  END IF;
		  IF NOT EXISTS (
		    SELECT 1 FROM pg_roles
		     WHERE rolname='vane_agent_session_projection_operator'
		  ) THEN
		    CREATE ROLE vane_agent_session_projection_operator NOLOGIN;
		  END IF;
		END $$;
		GRANT vane_agent_session_projection_operator
		  TO vane_agent_projection_unsafe_member`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := db.ExecContext(
			context.WithoutCancel(ctx), `
			REVOKE vane_agent_session_projection_operator
			  FROM vane_agent_projection_unsafe_member;
			DROP ROLE IF EXISTS vane_agent_projection_unsafe_member`,
		); err != nil {
			t.Errorf("cleanup unsafe membership fixture: %v", err)
		}
	})
	if _, err := provider.UpTo(ctx, 53); err == nil ||
		!strings.Contains(err.Error(), "only migration owner") {
		t.Fatalf("053 accepted an unsafe pre-existing membership: %v", err)
	}
}

func TestMigration053OperatorMinimumPrivilegesAndTenantRLS(t *testing.T) {
	db, provider := migration035Scratch(t)
	ctx := t.Context()
	if _, err := provider.UpTo(ctx, 53); err != nil {
		t.Fatalf("migrate to 053: %v", err)
	}

	var (
		noLogin, noInherit, noBypass, ownerCanSet              bool
		sessionRead, sessionStatusRead, sessionUpdate          bool
		eventRead, eventInsert, eventUpdate, eventDelete       bool
		authorityRead, actionInsert, idInsert, createdAtInsert bool
		authorityUpdate, authorityDelete, authorityTruncate    bool
		sequenceUsage, sequenceSelect, sequenceUpdate          bool
		appRead, appInsert, receiptRead, receiptInsert         bool
	)
	if err := db.QueryRowContext(ctx, `
		SELECT
		  NOT rolcanlogin, NOT rolinherit, NOT rolbypassrls,
		  pg_has_role(current_user, oid, 'SET'),
		  has_column_privilege(
		    $1,'agent_sessions','messages','SELECT'),
		  has_column_privilege(
		    $1,'agent_sessions','status','SELECT'),
		  has_table_privilege($1,'agent_sessions','UPDATE'),
		  has_table_privilege($1,'agent_events','SELECT'),
		  has_table_privilege($1,'agent_events','INSERT'),
		  has_table_privilege($1,'agent_events','UPDATE'),
		  has_table_privilege($1,'agent_events','DELETE'),
		  has_table_privilege(
		    $1,'agent_session_projection_authority_events','SELECT'),
		  has_column_privilege(
		    $1,'agent_session_projection_authority_events','action','INSERT'),
		  has_column_privilege(
		    $1,'agent_session_projection_authority_events','id','INSERT'),
		  has_column_privilege(
		    $1,'agent_session_projection_authority_events','created_at','INSERT'),
		  has_table_privilege(
		    $1,'agent_session_projection_authority_events','UPDATE'),
		  has_table_privilege(
		    $1,'agent_session_projection_authority_events','DELETE'),
		  has_table_privilege(
		    $1,'agent_session_projection_authority_events','TRUNCATE'),
		  has_sequence_privilege(
		    $1,'agent_session_projection_authority_events_id_seq','USAGE'),
		  has_sequence_privilege(
		    $1,'agent_session_projection_authority_events_id_seq','SELECT'),
		  has_sequence_privilege(
		    $1,'agent_session_projection_authority_events_id_seq','UPDATE'),
		  has_table_privilege(
		    'vane_app','agent_session_projection_authority_events','SELECT'),
		  has_column_privilege(
		    'vane_app','agent_session_projection_authority_events',
		    'action','INSERT'),
		  has_table_privilege(
		    'vane_edit_receipt',
		    'agent_session_projection_authority_events','SELECT'),
		  has_column_privilege(
		    'vane_edit_receipt',
		    'agent_session_projection_authority_events','action','INSERT')
		  FROM pg_roles WHERE rolname=$1`,
		agentProjectionOperatorRole,
	).Scan(
		&noLogin, &noInherit, &noBypass, &ownerCanSet,
		&sessionRead, &sessionStatusRead, &sessionUpdate,
		&eventRead, &eventInsert, &eventUpdate, &eventDelete,
		&authorityRead, &actionInsert, &idInsert, &createdAtInsert,
		&authorityUpdate, &authorityDelete, &authorityTruncate,
		&sequenceUsage, &sequenceSelect, &sequenceUpdate,
		&appRead, &appInsert, &receiptRead, &receiptInsert,
	); err != nil {
		t.Fatal(err)
	}
	if !noLogin || !noInherit || !noBypass || !ownerCanSet ||
		!sessionRead || sessionStatusRead || sessionUpdate ||
		!eventRead || eventInsert || eventUpdate || eventDelete ||
		!authorityRead || !actionInsert || idInsert || createdAtInsert ||
		authorityUpdate || authorityDelete || authorityTruncate ||
		!sequenceUsage || sequenceSelect || sequenceUpdate ||
		!appRead || appInsert || !receiptRead || receiptInsert {
		t.Fatalf(
			"053 privilege drift role=%v/%v/%v/%v "+
				"session=%v/%v/%v event=%v/%v/%v/%v "+
				"authority=%v/%v/%v/%v/%v/%v/%v "+
				"sequence=%v/%v/%v app=%v/%v receipt=%v/%v",
			noLogin, noInherit, noBypass, ownerCanSet,
			sessionRead, sessionStatusRead, sessionUpdate,
			eventRead, eventInsert, eventUpdate, eventDelete,
			authorityRead, actionInsert, idInsert, createdAtInsert,
			authorityUpdate, authorityDelete, authorityTruncate,
			sequenceUsage, sequenceSelect, sequenceUpdate,
			appRead, appInsert, receiptRead, receiptInsert,
		)
	}

	type scope struct {
		tenantID, userID, sessionID int64
	}
	createScope := func(label string) scope {
		t.Helper()
		var result scope
		if err := db.QueryRowContext(ctx,
			`INSERT INTO tenants DEFAULT VALUES RETURNING id`,
		).Scan(&result.tenantID); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRowContext(ctx, `
			INSERT INTO users (feishu_open_id,name)
			VALUES ($1,$2) RETURNING id`,
			"migration-053-"+label, "migration 053 "+label,
		).Scan(&result.userID); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, `
			INSERT INTO memberships (tenant_id,user_id,role)
			VALUES ($1,$2,'owner')`,
			result.tenantID, result.userID,
		); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRowContext(ctx, `
			INSERT INTO agent_sessions (tenant_id,user_id)
			VALUES ($1,$2) RETURNING id`,
			result.tenantID, result.userID,
		).Scan(&result.sessionID); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, `
			INSERT INTO agent_session_projection_authority_events (
			    tenant_id,user_id,session_id,generation,action,
			    ledger_head_sequence,legacy_digest,ledger_digest
			) VALUES ($1,$2,$3,1,'activate',1,repeat('a',64),repeat('a',64))`,
			result.tenantID, result.userID, result.sessionID,
		); err != nil {
			t.Fatal(err)
		}
		return result
	}
	a := createScope("a")
	b := createScope("b")

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx,
		`SET LOCAL ROLE vane_agent_session_projection_operator`,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx,
		`SELECT set_config('app.tenant_id',$1,true)`,
		fmt.Sprintf("%d", a.tenantID),
	); err != nil {
		t.Fatal(err)
	}
	var visibleA, visibleB int
	if err := tx.QueryRowContext(ctx, `
		SELECT count(*) FILTER (WHERE tenant_id=$1),
		       count(*) FILTER (WHERE tenant_id=$2)
		  FROM agent_session_projection_authority_events`,
		a.tenantID, b.tenantID,
	).Scan(&visibleA, &visibleB); err != nil {
		t.Fatal(err)
	}
	if visibleA != 1 || visibleB != 0 {
		t.Fatalf("operator visibility A/B=%d/%d", visibleA, visibleB)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO agent_session_projection_authority_events (
		    tenant_id,user_id,session_id,generation,action,
		    ledger_head_sequence,legacy_digest,ledger_digest
		) VALUES ($1,$2,$3,2,'rollback',1,repeat('b',64),repeat('b',64))`,
		a.tenantID, a.userID, a.sessionID,
	); err != nil {
		t.Fatalf("same-tenant operator append: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	attempt := func(statement string, args ...any) error {
		t.Helper()
		attemptTx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = attemptTx.Rollback() }()
		if _, err := attemptTx.ExecContext(ctx,
			`SET LOCAL ROLE vane_agent_session_projection_operator`,
		); err != nil {
			t.Fatal(err)
		}
		if _, err := attemptTx.ExecContext(ctx,
			`SELECT set_config('app.tenant_id',$1,true)`,
			fmt.Sprintf("%d", a.tenantID),
		); err != nil {
			t.Fatal(err)
		}
		_, err = attemptTx.ExecContext(ctx, statement, args...)
		return err
	}
	insert := `
		INSERT INTO agent_session_projection_authority_events (
		    tenant_id,user_id,session_id,generation,action,
		    ledger_head_sequence,legacy_digest,ledger_digest
		) VALUES ($1,$2,$3,2,'rollback',1,repeat('c',64),repeat('c',64))`
	if err := attempt(insert, b.tenantID, b.userID, b.sessionID); err == nil {
		t.Fatal("cross-tenant authority append unexpectedly succeeded")
	}
	if err := attempt(`
		INSERT INTO agent_session_projection_authority_events (
		    id,tenant_id,user_id,session_id,generation,action,
		    ledger_head_sequence,legacy_digest,ledger_digest
		) VALUES (999999999,$1,$2,$3,2,'rollback',1,
		          repeat('d',64),repeat('d',64))`,
		a.tenantID, a.userID, a.sessionID,
	); err == nil {
		t.Fatal("operator explicit event id unexpectedly succeeded")
	}
	for name, statement := range map[string]string{
		"update": `UPDATE agent_session_projection_authority_events
		              SET action='rollback'`,
		"delete":   `DELETE FROM agent_session_projection_authority_events`,
		"truncate": `TRUNCATE agent_session_projection_authority_events`,
	} {
		if err := attempt(statement); err == nil {
			t.Fatalf("operator %s unexpectedly succeeded", name)
		}
	}
}

func TestMigration053NonEmptyDownFenceAndSharedRole(t *testing.T) {
	db, provider := migration035Scratch(t)
	ctx := t.Context()
	if _, err := provider.UpTo(ctx, 53); err != nil {
		t.Fatalf("migrate to 053: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		WITH created_user AS (
			INSERT INTO users (feishu_open_id,name)
			VALUES ('migration-053-down','migration 053 down')
			RETURNING id
		), created_session AS (
			INSERT INTO agent_sessions (tenant_id,user_id)
			SELECT 1,id FROM created_user RETURNING id,user_id
		)
		INSERT INTO agent_session_projection_authority_events (
		    tenant_id,user_id,session_id,generation,action,
		    ledger_head_sequence,legacy_digest,ledger_digest
		)
		SELECT 1,user_id,id,1,'activate',1,repeat('a',64),repeat('a',64)
		  FROM created_session`); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Down(ctx); err == nil ||
		!strings.Contains(err.Error(), "refusing downgrade") {
		t.Fatalf("non-empty authority must fence 053 Down: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		`DELETE FROM agent_session_projection_authority_events`,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Down(ctx); err != nil {
		t.Fatalf("empty authority should allow 053 Down: %v", err)
	}
	var tableExists, roleExists, ownerMembership bool
	if err := db.QueryRowContext(ctx, `
		SELECT
		  to_regclass('public.agent_session_projection_authority_events')
		    IS NOT NULL,
		  EXISTS (SELECT 1 FROM pg_roles WHERE rolname=$1),
		  pg_has_role(current_user,$1,'SET')`,
		agentProjectionOperatorRole,
	).Scan(&tableExists, &roleExists, &ownerMembership); err != nil {
		t.Fatal(err)
	}
	if tableExists || !roleExists || !ownerMembership {
		t.Fatalf("Down table/role/membership=%v/%v/%v",
			tableExists, roleExists, ownerMembership)
	}
}
