package store

import (
	"strings"
	"testing"
)

func TestMigration058EmptyDownAndOrphanV2Fence(t *testing.T) {
	db, provider := migration035Scratch(t)
	ctx := t.Context()
	if _, err := provider.UpTo(ctx, 58); err != nil {
		t.Fatalf("migrate to 058: %v", err)
	}
	if _, err := provider.DownTo(ctx, 57); err != nil {
		t.Fatalf("empty 058 Down: %v", err)
	}
	if _, err := provider.UpTo(ctx, 58); err != nil {
		t.Fatalf("reapply 058: %v", err)
	}
	var tenantID, userID, sessionID int64
	if err := db.QueryRowContext(ctx,
		`INSERT INTO tenants DEFAULT VALUES RETURNING id`,
	).Scan(&tenantID); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx,
		`INSERT INTO users (feishu_open_id,name)
		 VALUES ('migration-058-user','migration 058') RETURNING id`,
	).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO memberships (tenant_id,user_id,role)
		 VALUES ($1,$2,'owner')`,
		tenantID, userID,
	); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx,
		`INSERT INTO agent_sessions (tenant_id,user_id)
		 VALUES ($1,$2) RETURNING id`,
		tenantID, userID,
	).Scan(&sessionID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO pending_actions (
		     id,tenant_id,user_id,session_id,tool_name,args,
		     expires_at,execution_version
		 ) VALUES (
		     'migration-058-orphan',$1,$2,$3,'enable_source',
		     '{"source_id":1}',clock_timestamp()+interval '1 hour',2
		 )`,
		tenantID, userID, sessionID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.DownTo(ctx, 57); err == nil ||
		!strings.Contains(
			err.Error(),
			"refusing downgrade while durable Agent actions exist",
		) {
		t.Fatalf("058 Down accepted orphan v2 root: %v", err)
	}
}
