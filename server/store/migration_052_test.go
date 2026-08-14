package store

import (
	"fmt"
	"strings"
	"testing"
)

func TestMigration052MinimumReceiptAuthorityAndDownFence(t *testing.T) {
	db, provider := migration035Scratch(t)
	ctx := t.Context()
	if _, err := provider.UpTo(ctx, 52); err != nil {
		t.Fatalf("migrate to 052: %v", err)
	}
	var (
		messagesRead, turnRead, toolsRead, turnUpdate bool
		eventRead, payloadInsert, idInsert            bool
		eventUpdate, eventDelete                      bool
		sequenceUsage, sequenceSelect                 bool
	)
	if err := db.QueryRowContext(ctx, `
		SELECT
		  has_column_privilege(
		    'vane_edit_receipt','agent_sessions','messages','SELECT'),
		  has_column_privilege(
		    'vane_edit_receipt','agent_sessions','turn_count','SELECT'),
		  has_column_privilege(
		    'vane_edit_receipt','agent_sessions','activated_tools','SELECT'),
		  has_column_privilege(
		    'vane_edit_receipt','agent_sessions','turn_count','UPDATE'),
		  has_table_privilege(
		    'vane_edit_receipt','agent_events','SELECT'),
		  has_column_privilege(
		    'vane_edit_receipt','agent_events','payload','INSERT'),
		  has_column_privilege(
		    'vane_edit_receipt','agent_events','id','INSERT'),
		  has_table_privilege(
		    'vane_edit_receipt','agent_events','UPDATE'),
		  has_table_privilege(
		    'vane_edit_receipt','agent_events','DELETE'),
		  has_sequence_privilege(
		    'vane_edit_receipt','agent_events_id_seq','USAGE'),
		  has_sequence_privilege(
		    'vane_edit_receipt','agent_events_id_seq','SELECT')`,
	).Scan(
		&messagesRead, &turnRead, &toolsRead, &turnUpdate,
		&eventRead, &payloadInsert, &idInsert,
		&eventUpdate, &eventDelete, &sequenceUsage, &sequenceSelect,
	); err != nil {
		t.Fatal(err)
	}
	if !messagesRead || !turnRead || !toolsRead || turnUpdate ||
		!eventRead || !payloadInsert || idInsert ||
		eventUpdate || eventDelete || !sequenceUsage || sequenceSelect {
		t.Fatalf(
			"052 authority drift session=%v/%v/%v/%v "+
				"event=%v/%v/%v/%v/%v sequence=%v/%v",
			messagesRead, turnRead, toolsRead, turnUpdate,
			eventRead, payloadInsert, idInsert, eventUpdate, eventDelete,
			sequenceUsage, sequenceSelect,
		)
	}

	payload := `{"schema_version":"vane.agent-event/v1","kind":"user_message","body":{"text":"keep"}}`
	if _, err := db.ExecContext(ctx, `
		WITH created_user AS (
			INSERT INTO users (feishu_open_id, name)
			VALUES ('migration-052-user', 'migration 052') RETURNING id
		), created_session AS (
			INSERT INTO agent_sessions (tenant_id, user_id)
			SELECT 1, id FROM created_user
			RETURNING id, user_id
		)
		INSERT INTO agent_events (
			tenant_id, user_id, session_id, sequence,
			batch_idempotency_key, batch_index, batch_size,
			kind, schema_version, payload, payload_digest, batch_digest
		)
		SELECT 1, user_id, id, 1,
		       'side.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
		       0, 1, 'user_message', 'vane.agent-event/v1',
		       convert_to($1, 'UTF8'),
		       encode(sha256(convert_to($1, 'UTF8')), 'hex'),
		       repeat('a', 64)
		  FROM created_session`, payload); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Down(ctx); err == nil ||
		!strings.Contains(err.Error(), "refusing downgrade") {
		t.Fatalf("side-writer history must fence 052 Down: %v", err)
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM agent_events`); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Down(ctx); err != nil {
		t.Fatalf("empty B2 ledger should allow 052 Down: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT
		  has_column_privilege(
		    'vane_edit_receipt','agent_sessions','turn_count','SELECT'),
		  has_table_privilege(
		    'vane_edit_receipt','agent_events','SELECT')`,
	).Scan(&turnRead, &eventRead); err != nil {
		t.Fatal(err)
	}
	if turnRead || eventRead {
		t.Fatalf("052 Down left grants: turn_read=%v event_read=%v",
			turnRead, eventRead)
	}
}

func TestMigration052ReceiptRoleTenantRLSAndImmutableEventBoundary(
	t *testing.T,
) {
	db, provider := migration035Scratch(t)
	ctx := t.Context()
	if _, err := provider.UpTo(ctx, 52); err != nil {
		t.Fatalf("migrate to 052: %v", err)
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
			"migration-052-role-"+label,
			"migration 052 role "+label,
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
		return result
	}
	a := createScope("a")
	b := createScope("b")
	payload := `{"schema_version":"vane.agent-event/v1","kind":"user_message","body":{"text":"role boundary"}}`
	insertOwnerEvent := func(target scope, key string) {
		t.Helper()
		if _, err := db.ExecContext(ctx, `
			INSERT INTO agent_events (
				tenant_id,user_id,session_id,sequence,
				batch_idempotency_key,batch_index,batch_size,
				kind,schema_version,payload,payload_digest,batch_digest
			) VALUES (
				$1,$2,$3,1,$4,0,1,'user_message',
				'vane.agent-event/v1',convert_to($5,'UTF8'),
				encode(sha256(convert_to($5,'UTF8')),'hex'),
				repeat('a',64)
			)`,
			target.tenantID,
			target.userID,
			target.sessionID,
			key,
			payload,
		); err != nil {
			t.Fatal(err)
		}
	}
	insertOwnerEvent(a, "migration-052-role-a")
	insertOwnerEvent(b, "migration-052-role-b")

	roleTx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = roleTx.Rollback() }()
	if _, err := roleTx.ExecContext(ctx, `SET LOCAL ROLE vane_edit_receipt`); err != nil {
		t.Fatal(err)
	}
	if _, err := roleTx.ExecContext(ctx,
		`SELECT set_config('app.tenant_id',$1,true)`,
		fmt.Sprintf("%d", a.tenantID),
	); err != nil {
		t.Fatal(err)
	}
	var visible, visibleA, visibleB int
	if err := roleTx.QueryRowContext(ctx, `
		SELECT count(*),
		       count(*) FILTER (WHERE tenant_id=$1),
		       count(*) FILTER (WHERE tenant_id=$2)
		  FROM agent_events`,
		a.tenantID, b.tenantID,
	).Scan(&visible, &visibleA, &visibleB); err != nil {
		t.Fatal(err)
	}
	if visible != 1 || visibleA != 1 || visibleB != 0 {
		t.Fatalf("receipt role visibility total/A/B=%d/%d/%d",
			visible, visibleA, visibleB)
	}
	if _, err := roleTx.ExecContext(ctx, `
		INSERT INTO agent_events (
			tenant_id,user_id,session_id,sequence,
			batch_idempotency_key,batch_index,batch_size,
			kind,schema_version,payload,payload_digest,batch_digest
		) VALUES (
			$1,$2,$3,2,'migration-052-role-allowed',0,1,
			'user_message','vane.agent-event/v1',convert_to($4,'UTF8'),
			encode(sha256(convert_to($4,'UTF8')),'hex'),repeat('b',64)
		)`,
		a.tenantID, a.userID, a.sessionID, payload,
	); err != nil {
		t.Fatalf("same-tenant immutable insert failed: %v", err)
	}
	if err := roleTx.Rollback(); err != nil {
		t.Fatal(err)
	}

	attempt := func(statement string, args ...any) error {
		t.Helper()
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback() }()
		if _, err := tx.ExecContext(
			ctx, `SET LOCAL ROLE vane_edit_receipt`,
		); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(ctx,
			`SELECT set_config('app.tenant_id',$1,true)`,
			fmt.Sprintf("%d", a.tenantID),
		); err != nil {
			t.Fatal(err)
		}
		_, err = tx.ExecContext(ctx, statement, args...)
		return err
	}
	insertSQL := `
		INSERT INTO agent_events (
			tenant_id,user_id,session_id,sequence,
			batch_idempotency_key,batch_index,batch_size,
			kind,schema_version,payload,payload_digest,batch_digest
		) VALUES (
			$1,$2,$3,2,$4,0,1,'user_message',
			'vane.agent-event/v1',convert_to($5,'UTF8'),
			encode(sha256(convert_to($5,'UTF8')),'hex'),repeat('c',64)
		)`
	if err := attempt(
		insertSQL,
		b.tenantID, b.userID, b.sessionID,
		"migration-052-cross-tenant", payload,
	); err == nil {
		t.Fatal("cross-tenant receipt event insert unexpectedly succeeded")
	}
	if err := attempt(
		insertSQL,
		a.tenantID, b.userID, b.sessionID,
		"migration-052-forged-scope", payload,
	); err == nil {
		t.Fatal("forged user/session scope insert unexpectedly succeeded")
	}
	if err := attempt(`
		INSERT INTO agent_events (
			id,tenant_id,user_id,session_id,sequence,
			batch_idempotency_key,batch_index,batch_size,
			kind,schema_version,payload,payload_digest,batch_digest
		) VALUES (
			999999999,$1,$2,$3,2,'migration-052-explicit-id',0,1,
			'user_message','vane.agent-event/v1',convert_to($4,'UTF8'),
			encode(sha256(convert_to($4,'UTF8')),'hex'),repeat('d',64)
		)`,
		a.tenantID, a.userID, a.sessionID, payload,
	); err == nil {
		t.Fatal("receipt role explicit event id insert unexpectedly succeeded")
	}
	for name, statement := range map[string]string{
		"update":   `UPDATE agent_events SET kind='assistant_message'`,
		"delete":   `DELETE FROM agent_events`,
		"truncate": `TRUNCATE agent_events`,
	} {
		if err := attempt(statement); err == nil {
			t.Fatalf("receipt role %s unexpectedly succeeded", name)
		}
	}
}
