package store

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"io/fs"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pressly/goose/v3"
)

type migration033Fixture struct {
	db        *sql.DB
	provider  *goose.Provider
	tenantA   int64
	userA     int64
	sessionA  int64
	userA2    int64
	sessionA2 int64
	taskA     string
	tenantB   int64
	userB     int64
	sessionB  int64
	taskB     string
}

func migration033Scratch(t *testing.T) migration033Fixture {
	t.Helper()
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		requireDatabaseCapability(t)
	}
	ctx := t.Context()
	scratchURL, drop := createScratchDB(ctx, t, dbURL)
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
	if _, err := provider.UpTo(ctx, 32); err != nil {
		t.Fatalf("迁移到 032 失败: %v", err)
	}

	f := migration033Fixture{
		db: db, provider: provider, tenantA: 1,
		taskA: "migration-033-task-a", taskB: "migration-033-task-b",
	}
	if err := db.QueryRowContext(ctx, `
		INSERT INTO users (feishu_open_id, name)
		VALUES ('migration-033-user-a', 'migration 033 A') RETURNING id`,
	).Scan(&f.userA); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO memberships (tenant_id, user_id, role) VALUES (1, $1, 'owner')`,
		f.userA); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `
		INSERT INTO agent_sessions (tenant_id, user_id)
		VALUES (1, $1) RETURNING id`, f.userA,
	).Scan(&f.sessionA); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `
		INSERT INTO users (feishu_open_id, name)
		VALUES ('migration-033-user-a2', 'migration 033 A2') RETURNING id`,
	).Scan(&f.userA2); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO memberships (tenant_id, user_id, role) VALUES (1, $1, 'member')`,
		f.userA2); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `
		INSERT INTO agent_sessions (tenant_id, user_id)
		VALUES (1, $1) RETURNING id`, f.userA2,
	).Scan(&f.sessionA2); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO schedules (id, tenant_id, user_id, nl_description, status)
		VALUES ($2, 1, $1, 'definition edit A', 'paused')`,
		f.userA, f.taskA); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `
		INSERT INTO tenants (status, plan) VALUES ('active', 'free') RETURNING id`,
	).Scan(&f.tenantB); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `
		INSERT INTO users (feishu_open_id, name)
		VALUES ('migration-033-user-b', 'migration 033 B') RETURNING id`,
	).Scan(&f.userB); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO memberships (tenant_id, user_id, role) VALUES ($1, $2, 'owner')`,
		f.tenantB, f.userB); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `
		INSERT INTO agent_sessions (tenant_id, user_id)
		VALUES ($1, $2) RETURNING id`, f.tenantB, f.userB,
	).Scan(&f.sessionB); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO schedules (id, tenant_id, user_id, nl_description, status)
		VALUES ($3, $1, $2, 'definition edit B', 'active')`,
		f.tenantB, f.userB, f.taskB); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.UpTo(ctx, 33); err != nil {
		t.Fatalf("迁移到 033 失败（PG18 SHA-256/FK 形状须可执行）: %v", err)
	}
	return f
}

func digest033(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func baseDefinition033(operationID string) []byte {
	return []byte(`{"schema":"approved-definition/v1","kind":"base","operation":"` +
		operationID + `"}`)
}

func insertPendingEdit033(
	t *testing.T,
	f migration033Fixture,
	id string,
	tenantID, userID int64,
	sessionID int64,
	taskID string,
) {
	t.Helper()
	baseDefinition := baseDefinition033(id)
	targetDefinition := []byte(`{"schema":"approved-definition/v1","operation":"` + id + `"}`)
	proposal := []byte(`{"schema":"frozen-task-definition-edit-proposal/v1","operation":"` + id + `"}`)
	prepared := []byte(`{"schema":"prepared-task-definition-edit/v1","operation":"` + id + `"}`)
	baseSnapshot := []byte(`{"schema":"task-definition-edit-snapshot/v1","operation":"` + id + `"}`)
	if _, err := f.db.ExecContext(t.Context(), `
		INSERT INTO task_definition_edit_operations (
			id, tenant_id, user_id, target_tenant_id, target_user_id,
			task_id, session_id, approval_ref, expires_at, original_status,
			base_definition_version, base_definition_digest, base_definition,
			target_definition_version, target_definition_digest,
			target_definition,
			canonical_proposal, proposal_digest,
			prepared_edit, prepared_edit_digest,
			base_snapshot, base_snapshot_digest
		) VALUES (
			$1, $2, $3, $2, $3, $4, $5, $6,
			clock_timestamp() + interval '1 day', 'paused',
			1, $7, $8, 2, $9, $10, $11, $12, $13, $14, $15, $16
		)`,
		id, tenantID, userID, taskID, sessionID, "approval:"+id,
		digest033(baseDefinition), baseDefinition, digest033(targetDefinition), targetDefinition,
		proposal, digest033(proposal), prepared, digest033(prepared),
		baseSnapshot, digest033(baseSnapshot)); err != nil {
		t.Fatalf("插入 pending definition edit %s 失败: %v", id, err)
	}
}

func TestMigration033ConstraintsPermissionsRLSAndIndexes(t *testing.T) {
	f := migration033Scratch(t)
	ctx := t.Context()

	var (
		operationSelect, operationInsert, operationUpdate, operationDelete bool
		receiptSelect, receiptInsert, receiptUpdate, receiptDelete         bool
		sequenceUsage                                                      bool
		ownerOperationInsert, ownerOperationUpdate                         bool
		markerOperationUpdate, markerFenceUpdate, legacyStatusUpdate       bool
		markerOperationInsert, markerFenceInsert, legacyStatusInsert       bool
	)
	if err := f.db.QueryRowContext(ctx, `
		SELECT
		  has_table_privilege('vane_app', 'task_definition_edit_operations', 'SELECT'),
		  has_table_privilege('vane_app', 'task_definition_edit_operations', 'INSERT'),
		  has_table_privilege('vane_app', 'task_definition_edit_operations', 'UPDATE'),
		  has_table_privilege('vane_app', 'task_definition_edit_operations', 'DELETE'),
		  has_table_privilege('vane_app', 'task_definition_edit_receipts', 'SELECT'),
		  has_table_privilege('vane_app', 'task_definition_edit_receipts', 'INSERT'),
		  has_table_privilege('vane_app', 'task_definition_edit_receipts', 'UPDATE'),
		  has_table_privilege('vane_app', 'task_definition_edit_receipts', 'DELETE'),
		  has_sequence_privilege(
		      'vane_app', 'task_definition_edit_receipts_id_seq', 'USAGE'),
		  has_table_privilege(
		      current_user, 'task_definition_edit_operations', 'INSERT'),
		  has_table_privilege(
		      current_user, 'task_definition_edit_operations', 'UPDATE'),
		  has_column_privilege(
		      'vane_app', 'schedules', 'definition_edit_operation_id', 'UPDATE'),
		  has_column_privilege(
		      'vane_app', 'schedules', 'definition_edit_fence', 'UPDATE'),
		  has_column_privilege(
		      'vane_app', 'schedules', 'status', 'UPDATE'),
		  has_column_privilege(
		      'vane_app', 'schedules', 'definition_edit_operation_id', 'INSERT'),
		  has_column_privilege(
		      'vane_app', 'schedules', 'definition_edit_fence', 'INSERT'),
		  has_column_privilege(
		      'vane_app', 'schedules', 'status', 'INSERT')`,
	).Scan(
		&operationSelect, &operationInsert, &operationUpdate, &operationDelete,
		&receiptSelect, &receiptInsert, &receiptUpdate, &receiptDelete,
		&sequenceUsage, &ownerOperationInsert, &ownerOperationUpdate,
		&markerOperationUpdate, &markerFenceUpdate, &legacyStatusUpdate,
		&markerOperationInsert, &markerFenceInsert, &legacyStatusInsert,
	); err != nil {
		t.Fatal(err)
	}
	if operationSelect || operationInsert || operationUpdate || operationDelete {
		t.Fatalf("operation foundation unexpectedly granted vane_app DML: select=%v insert=%v update=%v delete=%v",
			operationSelect, operationInsert, operationUpdate, operationDelete)
	}
	if receiptSelect || receiptInsert || receiptUpdate || receiptDelete || sequenceUsage {
		t.Fatalf("receipt foundation unexpectedly granted vane_app DML: select=%v insert=%v update=%v delete=%v seq=%v",
			receiptSelect, receiptInsert, receiptUpdate, receiptDelete, sequenceUsage)
	}
	if !ownerOperationInsert || !ownerOperationUpdate {
		t.Fatal("migration/table owner risk model drifted: owner should retain implicit DML")
	}
	if markerOperationUpdate || markerFenceUpdate || !legacyStatusUpdate {
		t.Fatalf("vane_app schedule UPDATE column grants drifted: marker=%v/%v legacy_status=%v",
			markerOperationUpdate, markerFenceUpdate, legacyStatusUpdate)
	}
	if markerOperationInsert || markerFenceInsert || !legacyStatusInsert {
		t.Fatalf("vane_app schedule INSERT column grants drifted: marker=%v/%v legacy_status=%v",
			markerOperationInsert, markerFenceInsert, legacyStatusInsert)
	}
	// Exercise the policies in this scratch database without changing the 2a
	// production grant surface. C2b3-2b will own its own privilege tests.
	if _, err := f.db.ExecContext(ctx, `
		GRANT SELECT, INSERT, UPDATE ON task_definition_edit_operations TO vane_app;
		GRANT SELECT, INSERT, UPDATE ON task_definition_edit_receipts TO vane_app;
		GRANT USAGE, SELECT ON SEQUENCE task_definition_edit_receipts_id_seq TO vane_app`); err != nil {
		t.Fatalf("grant scratch-only RLS privileges: %v", err)
	}

	for _, table := range []string{
		"task_definition_edit_operations", "task_definition_edit_receipts",
	} {
		var enabled bool
		var policies int
		if err := f.db.QueryRowContext(ctx, `
			SELECT c.relrowsecurity,
			       (SELECT count(*) FROM pg_policies
			         WHERE schemaname='public' AND tablename=$1)
			  FROM pg_class c
			  JOIN pg_namespace n ON n.oid=c.relnamespace
			 WHERE n.nspname='public' AND c.relname=$1`, table,
		).Scan(&enabled, &policies); err != nil {
			t.Fatal(err)
		}
		if !enabled || policies != 2 {
			t.Fatalf("%s RLS incomplete: enabled=%v policies=%d", table, enabled, policies)
		}
	}

	var staleDef, dueDef string
	if err := f.db.QueryRowContext(ctx, `
		SELECT pg_get_indexdef('idx_task_definition_edit_operations_stale'::regclass),
		       pg_get_indexdef('idx_task_definition_edit_receipts_due'::regclass)`,
	).Scan(&staleDef, &dueDef); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(staleDef, "(tenant_id, takeover_not_before, id)") ||
		!strings.Contains(staleDef, "status = 'executing'") {
		t.Fatalf("stale index must be tenant-first and partial: %s", staleDef)
	}
	if !strings.Contains(dueDef, "(tenant_id, next_attempt_at, id)") ||
		!strings.Contains(dueDef, "status = 'pending'") {
		t.Fatalf("due index must be tenant-first and partial: %s", dueDef)
	}

	insertPendingEdit033(t, f, "migration-033-op-a", f.tenantA, f.userA, f.sessionA, f.taskA)
	var initialPhase, targetDigest, proposalDigest string
	if err := f.db.QueryRowContext(ctx, `
		SELECT phase, target_definition_digest, proposal_digest
		  FROM task_definition_edit_operations
		 WHERE id='migration-033-op-a'`,
	).Scan(&initialPhase, &targetDigest, &proposalDigest); err != nil {
		t.Fatal(err)
	}
	if initialPhase != "proposal_sealed" {
		t.Fatalf("new operation must start proposal_sealed, got %q", initialPhase)
	}
	if targetDigest == proposalDigest {
		t.Fatal("small approval envelope and canonical target definition need independent digests")
	}
	if _, err := f.db.ExecContext(ctx, `
		UPDATE task_definition_edit_operations SET session_id=$2 WHERE id=$1`,
		"migration-033-op-a", f.sessionB); err == nil ||
		!strings.Contains(err.Error(), "fk_task_definition_edit_operation_session_scope") {
		t.Fatalf("operation session must match exact tenant/user scope: %v", err)
	}
	if _, err := f.db.ExecContext(ctx, `
		UPDATE task_definition_edit_operations SET session_id=$2 WHERE id=$1`,
		"migration-033-op-a", f.sessionA2); err == nil ||
		!strings.Contains(err.Error(), "fk_task_definition_edit_operation_session_scope") {
		t.Fatalf("operation session must not cross users within one tenant: %v", err)
	}
	if _, err := f.db.ExecContext(ctx, `
		UPDATE task_definition_edit_operations
		   SET base_definition=convert_to('{"forged":true}', 'UTF8')
		 WHERE id='migration-033-op-a'`); err == nil ||
		!strings.Contains(err.Error(), "base_definition_valid") {
		t.Fatalf("base definition bytes must remain bound to their SHA-256 head: %v", err)
	}
	targetDefinition := []byte(`{"schema":"approved-definition/v1","invalid":"scope"}`)
	proposal := []byte(`{"schema":"frozen-task-definition-edit-proposal/v1","invalid":"scope"}`)
	prepared := []byte(`{"prepared":true}`)
	baseSnapshot := []byte(`{"base":true}`)
	baseDefinition := baseDefinition033("invalid")
	insertInvalid := func(id string, tenantID, userID, targetTenantID, targetUserID int64,
		sessionID int64, taskID string, targetVersion int64, targetDigest, proposalDigest string,
	) error {
		_, err := f.db.ExecContext(ctx, `
			INSERT INTO task_definition_edit_operations (
				id, tenant_id, user_id, target_tenant_id, target_user_id,
				task_id, session_id, approval_ref, expires_at, original_status,
				base_definition_version, base_definition_digest, base_definition,
				target_definition_version, target_definition_digest,
				target_definition,
				canonical_proposal, proposal_digest,
				prepared_edit, prepared_edit_digest,
				base_snapshot, base_snapshot_digest
			) VALUES (
				$1, $2, $3, $4, $5, $6, $7, 'approval:' || $1,
				clock_timestamp() + interval '1 day', 'active',
				1, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18
			)`, id, tenantID, userID, targetTenantID, targetUserID, taskID, sessionID,
			digest033(baseDefinition), baseDefinition, targetVersion, targetDigest,
			targetDefinition, proposal, proposalDigest, prepared, digest033(prepared),
			baseSnapshot, digest033(baseSnapshot))
		return err
	}
	if err := insertInvalid(
		"migration-033-cross-scope", f.tenantA, f.userA, f.tenantB, f.userB,
		f.sessionA, "migration-033-cross-scope-task", 2,
		digest033(targetDefinition), digest033(proposal),
	); err == nil || !strings.Contains(err.Error(), "actor_is_target") {
		t.Fatalf("actor/target scope drift must be rejected by named CHECK: %v", err)
	}
	if err := insertInvalid(
		"migration-033-version-skip", f.tenantA, f.userA, f.tenantA, f.userA,
		f.sessionA, "migration-033-version-skip-task", 3,
		digest033(targetDefinition), digest033(proposal),
	); err == nil || !strings.Contains(err.Error(), "versions_valid") {
		t.Fatalf("target must be exactly base+1: %v", err)
	}
	if err := insertInvalid(
		"migration-033-task-too-long", f.tenantA, f.userA, f.tenantA, f.userA,
		f.sessionA, strings.Repeat("t", 256), 2,
		digest033(targetDefinition), digest033(proposal),
	); err == nil || !strings.Contains(err.Error(), "task_id_valid") {
		t.Fatalf("task id must match ApprovedDefinitionV1's 255-byte bound: %v", err)
	}
	if err := insertInvalid(
		"migration-033-digest-drift", f.tenantA, f.userA, f.tenantA, f.userA,
		f.sessionA, "migration-033-digest-drift-task", 2,
		strings.Repeat("f", 64), digest033(proposal),
	); err == nil || !strings.Contains(err.Error(), "target_definition_valid") {
		t.Fatalf("target definition SHA-256 drift must be rejected: %v", err)
	}
	if err := insertInvalid(
		"migration-033-proposal-drift", f.tenantA, f.userA, f.tenantA, f.userA,
		f.sessionA, "migration-033-proposal-drift-task", 2,
		digest033(targetDefinition), strings.Repeat("e", 64),
	); err == nil || !strings.Contains(err.Error(), "proposal_valid") {
		t.Fatalf("canonical proposal SHA-256 drift must be rejected: %v", err)
	}
	if _, err := f.db.ExecContext(ctx, `
		UPDATE task_definition_edit_operations
		   SET pause_snapshot=convert_to('{"paused":true}', 'UTF8')
		 WHERE id='migration-033-op-a'`); err == nil ||
		!strings.Contains(err.Error(), "pause_snapshot_complete") {
		t.Fatalf("half phase checkpoint must be rejected: %v", err)
	}
	pauseSnapshot := []byte(`{"phase":"base_paused"}`)
	if _, err := f.db.ExecContext(ctx, `
		UPDATE task_definition_edit_operations
		   SET pause_snapshot=$2, pause_snapshot_digest=$3
		 WHERE id=$1`, "migration-033-op-a", pauseSnapshot,
		digest033(pauseSnapshot)); err == nil ||
		!strings.Contains(err.Error(), "phase_checkpoint_valid") {
		t.Fatalf("proposal phase must reject a future complete checkpoint: %v", err)
	}
	if _, err := f.db.ExecContext(ctx, `
		UPDATE task_definition_edit_operations
		   SET status='executing', phase='temporal_target_applied',
		       confirmed_at=clock_timestamp(), receipt_provider='feishu_card_patch:app-a',
		       receipt_target='om_a', lease_owner='phase-forger',
		       lease_until=clock_timestamp()+interval '20 seconds',
		       takeover_not_before=clock_timestamp()+interval '50 seconds',
		       fence=1, attempt=1
		 WHERE id='migration-033-op-a'`); err == nil ||
		!strings.Contains(err.Error(), "phase_checkpoint_valid") {
		t.Fatalf("phase may not advance without exact checkpoint prefix: %v", err)
	}
	if _, err := f.db.ExecContext(ctx, `
		UPDATE task_definition_edit_operations
		   SET status='completed', phase='temporal_target_restored',
		       confirmed_at=clock_timestamp(), tombstoned_at=clock_timestamp(),
		       receipt_provider='feishu_card_patch:app-a', receipt_target='om_a',
		       fence=1, attempt=1
		 WHERE id='migration-033-op-a'`); err == nil ||
		!strings.Contains(err.Error(), "phase_checkpoint_valid") {
		t.Fatalf("completed status requires every remote checkpoint: %v", err)
	}
	if _, err := f.db.ExecContext(ctx, `
		UPDATE task_definition_edit_operations
		   SET status='expired', phase='definition_committed',
		       tombstoned_at=clock_timestamp(),
		       pause_snapshot=$2, pause_snapshot_digest=$3
		 WHERE id=$1`, "migration-033-op-a", pauseSnapshot,
		digest033(pauseSnapshot)); err == nil ||
		!strings.Contains(err.Error(), "status_phase_valid") {
		t.Fatalf("unconfirmed expired/cancelled status must not hide progressed state: %v", err)
	}
	if _, err := f.db.ExecContext(ctx, `
		UPDATE task_definition_edit_operations
		   SET receipt_provider='feishu_card_patch'
		 WHERE id='migration-033-op-a'`); err == nil ||
		!strings.Contains(err.Error(), "receipt_target_complete") {
		t.Fatalf("half receipt target must be rejected: %v", err)
	}
	if err := insertInvalid(
		"migration-033-op-a-conflict", f.tenantA, f.userA, f.tenantA, f.userA,
		f.sessionA, f.taskA, 2, digest033(targetDefinition), digest033(proposal),
	); err == nil || !strings.Contains(err.Error(), "nonterminal") {
		t.Fatalf("one scoped task may have at most one non-terminal operation: %v", err)
	}
	if _, err := f.db.ExecContext(ctx, `
		UPDATE task_definition_edit_operations
		   SET receipt_provider='feishu_card_patch:app-a', receipt_target='om_a'
		 WHERE id='migration-033-op-a'`); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.ExecContext(ctx, `
		UPDATE task_definition_edit_operations
		   SET status='cancelled', tombstoned_at=clock_timestamp()
		 WHERE id='migration-033-op-a'`); err != nil {
		t.Fatal(err)
	}
	insertPendingEdit033(t, f, "migration-033-op-a-next", f.tenantA, f.userA, f.sessionA, f.taskA)

	insertPendingEdit033(t, f, "migration-033-op-b", f.tenantB, f.userB, f.sessionB, f.taskB)
	if _, err := f.db.ExecContext(ctx, `
		UPDATE task_definition_edit_operations
		   SET status='executing', phase='proposal_sealed', confirmed_at=clock_timestamp(),
		       lease_owner='worker-b', lease_until=clock_timestamp()+interval '20 seconds',
		       takeover_not_before=clock_timestamp()+interval '50 seconds',
		       fence=1, attempt=1,
		       receipt_provider='feishu_card_patch:app-b', receipt_target='om_b'
		 WHERE id='migration-033-op-b'`); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.ExecContext(ctx, `
		UPDATE schedules
		   SET definition_edit_operation_id='migration-033-op-b',
		       definition_edit_fence=1
		 WHERE id=$1`, f.taskB); err != nil {
		t.Fatalf("exact operation marker should bind: %v", err)
	}
	if _, err := f.db.ExecContext(ctx, `
		UPDATE schedules
		   SET definition_edit_operation_id='migration-033-op-b',
		       definition_edit_fence=1
		 WHERE id=$1`, f.taskA); err == nil ||
		!strings.Contains(err.Error(), "fk_schedules_definition_edit_operation") {
		t.Fatalf("schedule marker must prove exact target scope: %v", err)
	}
	if _, err := f.db.ExecContext(ctx, `
		UPDATE schedules SET definition_edit_operation_id='migration-033-op-b'
		 WHERE id=$1`, f.taskA); err == nil ||
		!strings.Contains(err.Error(), "marker_complete") {
		t.Fatalf("half schedule marker must be rejected: %v", err)
	}
	markerTx, err := f.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := markerTx.ExecContext(ctx, `
		UPDATE task_definition_edit_operations
		   SET fence=2, attempt=2,
		       lease_until=clock_timestamp()+interval '20 seconds',
		       takeover_not_before=clock_timestamp()+interval '50 seconds'
		 WHERE id='migration-033-op-b'`); err != nil {
		_ = markerTx.Rollback()
		t.Fatal(err)
	}
	if _, err := markerTx.ExecContext(ctx, `
		UPDATE schedules SET definition_edit_fence=2 WHERE id=$1`, f.taskB); err != nil {
		_ = markerTx.Rollback()
		t.Fatal(err)
	}
	if err := markerTx.Commit(); err != nil {
		t.Fatalf("operation+schedule fence takeover must be atomically deferrable: %v", err)
	}

	receiptPayload := []byte(`{"status":"cancelled"}`)
	if _, err := f.db.ExecContext(ctx, `
		INSERT INTO task_definition_edit_receipts (
			operation_id, tenant_id, user_id, session_id,
			provider, target, provider_key,
			payload, payload_digest
		) VALUES (
			'migration-033-op-a', $1, $2, $3,
			'feishu_card_patch:app-a', 'om_a',
			'00000000-0000-0000-0000-000000000033', $4, $5
		)`, f.tenantA, f.userA, f.sessionA,
		receiptPayload, digest033(receiptPayload)); err != nil {
		t.Fatalf("insert exact receipt failed: %v", err)
	}
	if _, err := f.db.ExecContext(ctx, `
		UPDATE task_definition_edit_receipts
		   SET lease_owner='zero-fence-worker',
		       lease_until=clock_timestamp()+interval '20 seconds',
		       takeover_not_before=clock_timestamp()+interval '50 seconds'
		 WHERE operation_id='migration-033-op-a'`); err == nil ||
		!strings.Contains(err.Error(), "lease_status") {
		t.Fatalf("active receipt lease requires a positive fence and attempt: %v", err)
	}
	if _, err := f.db.ExecContext(ctx, `
		UPDATE task_definition_edit_receipts SET failure_class='permanent'
		 WHERE operation_id='migration-033-op-a'`); err == nil ||
		!strings.Contains(err.Error(), "failure_state_valid") {
		t.Fatalf("pending receipt must reject permanent failure: %v", err)
	}
	if _, err := f.db.ExecContext(ctx, `
		UPDATE task_definition_edit_receipts SET failure_class='target_unbound'
		 WHERE operation_id='migration-033-op-a'`); err == nil ||
		!strings.Contains(err.Error(), "failure_state_valid") {
		t.Fatalf("bound pending receipt must reject target-unbound failure: %v", err)
	}
	if _, err := f.db.ExecContext(ctx, `
		UPDATE task_definition_edit_receipts SET failure_class='ambiguous'
		 WHERE operation_id='migration-033-op-a'`); err == nil ||
		!strings.Contains(err.Error(), "failure_state_valid") {
		t.Fatalf("ambiguous failure requires its first-observed checkpoint: %v", err)
	}
	if _, err := f.db.ExecContext(ctx, `
		UPDATE task_definition_edit_receipts
		   SET failure_class='retryable', ambiguous_since=clock_timestamp(),
		       fence=1, attempt=1
		 WHERE operation_id='migration-033-op-a'`); err == nil ||
		!strings.Contains(err.Error(), "failure_state_valid") {
		t.Fatalf("retryable failure must not downgrade ambiguous send evidence: %v", err)
	}
	if _, err := f.db.ExecContext(ctx, `
		UPDATE task_definition_edit_receipts
		   SET status='sent', sent_at=clock_timestamp(), provider_message_id='om_sent',
		       fence=1, attempt=1
		 WHERE operation_id='migration-033-op-a'`); err == nil ||
		!strings.Contains(err.Error(), "status_checkpoint_valid") {
		t.Fatalf("sent receipt requires a session checkpoint: %v", err)
	}
	if _, err := f.db.ExecContext(ctx, `
		UPDATE task_definition_edit_receipts
		   SET status='sent', sent_at=clock_timestamp(), provider_message_id='om_sent',
		       session_recorded_at=clock_timestamp(), session_messages_digest=repeat('a',64),
		       lease_owner='stale-worker',
		       lease_until=clock_timestamp()+interval '20 seconds',
		       takeover_not_before=clock_timestamp()+interval '50 seconds',
		       fence=9, attempt=9
		 WHERE operation_id='migration-033-op-a'`); err == nil ||
		!strings.Contains(err.Error(), "lease_status") {
		t.Fatalf("terminal receipt must not retain an active lease: %v", err)
	}
	if _, err := f.db.ExecContext(ctx, `
		UPDATE task_definition_edit_receipts
		   SET status='sent', sent_at=clock_timestamp(), provider_message_id='om_sent',
		       session_recorded_at=clock_timestamp(), session_messages_digest=repeat('a',64),
		       fence=1, attempt=1, failure_class='permanent',
		       ambiguous_since=clock_timestamp()
		 WHERE operation_id='migration-033-op-a'`); err == nil ||
		!strings.Contains(err.Error(), "failure_state_valid") {
		t.Fatalf("sent receipt must clear all failure evidence: %v", err)
	}
	if _, err := f.db.ExecContext(ctx, `
		UPDATE task_definition_edit_receipts
		   SET status='blocked', blocked_at=clock_timestamp(),
		       fence=1, attempt=1, failure_class='retryable'
		 WHERE operation_id='migration-033-op-a'`); err == nil ||
		!strings.Contains(err.Error(), "failure_state_valid") {
		t.Fatalf("blocked receipt must carry only a permanent failure: %v", err)
	}
	if _, err := f.db.ExecContext(ctx, `
		UPDATE task_definition_edit_receipts
		   SET status='suppressed', provider='', target='',
		       failure_class='target_unbound', sent_at=clock_timestamp(),
		       provider_message_id='target-unbound-suppressed'
		 WHERE operation_id='migration-033-op-a'`); err == nil ||
		!strings.Contains(err.Error(), "fk_task_definition_edit_receipts_operation_scope") {
		t.Fatalf("receipt target must remain the exact operation target: %v", err)
	}
	if _, err := f.db.ExecContext(ctx, `
		UPDATE task_definition_edit_receipts SET payload_digest=repeat('f',64)
		 WHERE operation_id='migration-033-op-a'`); err == nil ||
		!strings.Contains(err.Error(), "payload_checkpoint_complete") {
		t.Fatalf("receipt payload SHA-256 drift must be rejected: %v", err)
	}
	if _, err := f.db.ExecContext(ctx, `
		INSERT INTO task_definition_edit_receipts (
			operation_id, tenant_id, user_id, session_id, provider, target, provider_key
		) VALUES (
			'migration-033-op-b', $1, $2, $3, 'feishu_card_patch:app-a', 'om_foreign',
			'00000000-0000-0000-0000-000000000034'
		)`, f.tenantA, f.userA, f.sessionA); err == nil ||
		!strings.Contains(err.Error(), "fk_task_definition_edit_receipts_operation_scope") {
		t.Fatalf("receipt must not bind a foreign operation scope: %v", err)
	}
	if _, err := f.db.ExecContext(ctx, `
		INSERT INTO task_definition_edit_receipts (
			operation_id, tenant_id, user_id, session_id, provider, target,
			provider_key, status, failure_class, sent_at, provider_message_id
		) VALUES (
			'migration-033-op-a-next', $1, $2, $3, '', '',
			'00000000-0000-0000-0000-000000000037', 'suppressed',
			'target_unbound', clock_timestamp(), 'target-unbound-suppressed'
		)`, f.tenantA, f.userA, f.sessionA); err != nil {
		t.Fatalf("exact target-unbound suppression should remain representable: %v", err)
	}
	if _, err := f.db.ExecContext(ctx, `
		UPDATE task_definition_edit_receipts
		   SET failure_class='permanent'
		 WHERE operation_id='migration-033-op-a-next'`); err == nil ||
		!strings.Contains(err.Error(), "failure_state_valid") {
		t.Fatalf("suppressed receipt must retain target-unbound evidence: %v", err)
	}
	if _, err := f.db.ExecContext(ctx, `
		DELETE FROM task_definition_edit_receipts
		 WHERE operation_id='migration-033-op-a-next'`); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.ExecContext(ctx, `
		UPDATE task_definition_edit_operations
		   SET receipt_provider='feishu_card_patch:app-a', receipt_target='om_a-next'
		 WHERE id='migration-033-op-a-next'`); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.ExecContext(ctx, `
		INSERT INTO task_definition_edit_receipts (
			operation_id, tenant_id, user_id, session_id, provider, target, provider_key
		) VALUES (
			'migration-033-op-a-next', $1, $2, $3,
			'feishu_card_patch:app-a', 'om_a-next',
			'00000000-0000-0000-0000-000000000036'
		)`, f.tenantA, f.userA, f.sessionB); err == nil ||
		(!strings.Contains(err.Error(), "fk_task_definition_edit_receipts_session_scope") &&
			!strings.Contains(err.Error(), "fk_task_definition_edit_receipts_operation_scope")) {
		t.Fatalf("receipt session must match operation and exact tenant/user scope: %v", err)
	}
	if _, err := f.db.ExecContext(ctx, `
		INSERT INTO task_definition_edit_receipts (
			operation_id, tenant_id, user_id, session_id, provider, target, provider_key
		) VALUES (
			'migration-033-op-b', $1, $2, $3, 'feishu_card_patch:app-b', 'om_b',
			'00000000-0000-0000-0000-000000000035'
		)`, f.tenantB, f.userB, f.sessionB); err != nil {
		t.Fatal(err)
	}

	assertVisible := func(tenantID int64, table string, want int) {
		t.Helper()
		tx, err := f.db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback() }()
		if tenantID > 0 {
			if _, err := tx.ExecContext(ctx,
				`SELECT set_config('app.tenant_id', $1, true)`,
				strconv.FormatInt(tenantID, 10)); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := tx.ExecContext(ctx, `SET LOCAL ROLE vane_app`); err != nil {
			t.Fatal(err)
		}
		var count int
		if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM "+table).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != want {
			t.Fatalf("RLS %s tenant=%d count=%d want=%d", table, tenantID, count, want)
		}
	}
	assertVisible(f.tenantA, "task_definition_edit_operations", 2)
	assertVisible(f.tenantB, "task_definition_edit_operations", 1)
	assertVisible(0, "task_definition_edit_operations", 0)
	assertVisible(f.tenantA, "task_definition_edit_receipts", 1)
	assertVisible(f.tenantB, "task_definition_edit_receipts", 1)
	assertVisible(0, "task_definition_edit_receipts", 0)

	tx, err := f.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx,
		`SELECT set_config('app.tenant_id', $1, true)`,
		strconv.FormatInt(f.tenantA, 10)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `SET LOCAL ROLE vane_app`); err != nil {
		t.Fatal(err)
	}
	foreignTarget := []byte(`{"target":"foreign"}`)
	foreignProposal := []byte(`{"proposal":"foreign"}`)
	foreignPrepared := []byte(`{"prepared":"foreign"}`)
	foreignBaseSnapshot := []byte(`{"base_snapshot":"foreign"}`)
	foreignBaseDefinition := baseDefinition033("migration-033-rls-foreign")
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO task_definition_edit_operations (
			id, tenant_id, user_id, target_tenant_id, target_user_id,
			task_id, session_id, approval_ref, expires_at, original_status,
			base_definition_version, base_definition_digest, base_definition,
			target_definition_version, target_definition_digest,
			target_definition,
			canonical_proposal, proposal_digest,
			prepared_edit, prepared_edit_digest,
			base_snapshot, base_snapshot_digest
		) VALUES (
			'migration-033-rls-foreign', $1, $2, $1, $2,
			'migration-033-rls-foreign-task', $3, 'approval:rls-foreign',
			clock_timestamp()+interval '1 day', 'paused', 1, $4, $5, 2, $6,
			$7, $8, $9, $10, $11, $12, $13
		)`, f.tenantB, f.userB, f.sessionB,
		digest033(foreignBaseDefinition), foreignBaseDefinition,
		digest033(foreignTarget), foreignTarget, foreignProposal,
		digest033(foreignProposal), foreignPrepared, digest033(foreignPrepared),
		foreignBaseSnapshot, digest033(foreignBaseSnapshot)); err == nil ||
		!strings.Contains(err.Error(), "row-level security") {
		t.Fatalf("tenant A must not insert an actor=B operation: %v", err)
	}
	if _, err := f.db.ExecContext(ctx,
		`DELETE FROM task_definition_edit_receipts WHERE operation_id='migration-033-op-b'`); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.ExecContext(ctx,
		`DELETE FROM task_definition_edit_operations WHERE id='migration-033-op-b'`); err != nil {
		t.Fatalf("owner-only operation purge should clear its schedule marker: %v", err)
	}
	var markerCleared bool
	if err := f.db.QueryRowContext(ctx, `
		SELECT definition_edit_operation_id IS NULL AND definition_edit_fence IS NULL
		  FROM schedules WHERE id=$1`, f.taskB,
	).Scan(&markerCleared); err != nil {
		t.Fatal(err)
	}
	if !markerCleared {
		t.Fatal("owner-only operation purge must clear the complete schedule marker pair")
	}
}

func TestMigration033DowngradeGuardAndCleanup(t *testing.T) {
	t.Run("empty foundation can downgrade", func(t *testing.T) {
		f := migration033Scratch(t)
		if _, err := f.provider.Down(t.Context()); err != nil {
			t.Fatalf("空 C2b3-2a 地基应可回滚: %v", err)
		}
		var tables, columns int
		var scheduleInsertRestored, scheduleUpdateRestored bool
		if err := f.db.QueryRowContext(t.Context(), `
			SELECT
			  (SELECT count(*) FROM information_schema.tables
			    WHERE table_schema='public' AND table_name IN
			          ('task_definition_edit_operations','task_definition_edit_receipts')),
			  (SELECT count(*) FROM information_schema.columns
			    WHERE table_schema='public' AND table_name='schedules'
			      AND column_name IN
			          ('definition_edit_operation_id','definition_edit_fence')),
			  has_table_privilege('vane_app', 'schedules', 'INSERT'),
			  has_table_privilege('vane_app', 'schedules', 'UPDATE')`,
		).Scan(&tables, &columns, &scheduleInsertRestored, &scheduleUpdateRestored); err != nil {
			t.Fatal(err)
		}
		if tables != 0 || columns != 0 || !scheduleInsertRestored || !scheduleUpdateRestored {
			t.Fatalf("033 Down 留下 schema/privilege 漂移: tables=%d columns=%d insert=%v update=%v",
				tables, columns, scheduleInsertRestored, scheduleUpdateRestored)
		}
	})

	f := migration033Scratch(t)
	ctx := t.Context()
	insertPendingEdit033(t, f, "migration-033-down", f.tenantA, f.userA, f.sessionA, f.taskA)
	if _, err := f.provider.Down(ctx); err == nil ||
		!strings.Contains(err.Error(), "refusing downgrade") {
		t.Fatalf("任意 durable edit operation 都必须拒绝回滚: %v", err)
	}
	var version, operations, tables int
	if err := f.db.QueryRowContext(ctx, `
		SELECT
		  (SELECT max(version_id) FROM goose_db_version WHERE is_applied),
		  (SELECT count(*) FROM task_definition_edit_operations),
		  (SELECT count(*) FROM information_schema.tables
		    WHERE table_schema='public' AND table_name IN
		          ('task_definition_edit_operations','task_definition_edit_receipts'))`,
	).Scan(&version, &operations, &tables); err != nil {
		t.Fatal(err)
	}
	if version != 33 || operations != 1 || tables != 2 {
		t.Fatalf("拒绝 Down 必须原子保留状态: version=%d operations=%d tables=%d",
			version, operations, tables)
	}
	if _, err := f.db.ExecContext(ctx,
		`DELETE FROM task_definition_edit_operations WHERE id='migration-033-down'`); err != nil {
		t.Fatal(err)
	}
	if _, err := f.provider.Down(ctx); err != nil {
		t.Fatalf("清空 edit state 后应可安全回滚: %v", err)
	}
}

func TestMigration033ScheduleDeletionRetainsSelfContainedEditOperation(t *testing.T) {
	f := migration033Scratch(t)
	ctx := t.Context()
	const operationID = "migration-033-delete-wins"
	baseDefinition := baseDefinition033(operationID)
	baseDigest := digest033(baseDefinition)
	if _, err := f.db.ExecContext(ctx, `
		INSERT INTO task_approved_definition_versions (
			tenant_id, user_id, task_id, version, schema_version,
			execution_mode, definition_digest, payload, approval_ref
		) VALUES (1, $1, $2, 1, 'vane.task-approved-definition/v1',
		          'compiled', $3, $4, 'migration-033-delete-base')`,
		f.userA, f.taskA, baseDigest, baseDefinition); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.ExecContext(ctx, `
		UPDATE schedules
		   SET approved_definition_version=1, approved_definition_digest=$2
		 WHERE id=$1`, f.taskA, baseDigest); err != nil {
		t.Fatal(err)
	}
	insertPendingEdit033(
		t, f, operationID, f.tenantA, f.userA, f.sessionA, f.taskA,
	)

	tx, err := f.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx,
		`SELECT set_config('app.tenant_id', $1, true)`,
		strconv.FormatInt(f.tenantA, 10)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `SET LOCAL ROLE vane_app`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM schedules WHERE id=$1`, f.taskA); err != nil {
		t.Fatalf("existing owner delete-wins path must remain available: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	var schedules, versions, operations int
	var retainedBase []byte
	var retainedDigest string
	if err := f.db.QueryRowContext(ctx, `
		SELECT
		  (SELECT count(*) FROM schedules WHERE id=$1),
		  (SELECT count(*) FROM task_approved_definition_versions WHERE task_id=$1),
		  (SELECT count(*) FROM task_definition_edit_operations WHERE id=$2),
		  (SELECT base_definition FROM task_definition_edit_operations WHERE id=$2),
		  (SELECT base_definition_digest FROM task_definition_edit_operations WHERE id=$2)`,
		f.taskA, operationID,
	).Scan(&schedules, &versions, &operations, &retainedBase, &retainedDigest); err != nil {
		t.Fatal(err)
	}
	if schedules != 0 || versions != 0 || operations != 1 ||
		!bytes.Equal(retainedBase, baseDefinition) || retainedDigest != baseDigest {
		t.Fatalf(
			"delete-wins left a non-self-contained operation: schedules=%d versions=%d operations=%d base=%s digest=%s",
			schedules, versions, operations, retainedBase, retainedDigest,
		)
	}
}

func TestMigration033DowngradeSerializesWithEditWriter(t *testing.T) {
	f := migration033Scratch(t)
	ctx := t.Context()

	writer, err := f.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	writerOpen := true
	downDone := make(chan error, 1)
	downFinished := false
	t.Cleanup(func() {
		if writerOpen {
			_ = writer.Rollback()
		}
		if downFinished {
			return
		}
		select {
		case <-downDone:
		case <-time.After(5 * time.Second):
			t.Error("033 Down did not finish after writer cleanup")
		}
	})

	var lockedTask string
	if err := writer.QueryRowContext(ctx,
		`SELECT id FROM schedules WHERE id=$1 FOR UPDATE`, f.taskA,
	).Scan(&lockedTask); err != nil {
		t.Fatal(err)
	}
	go func() {
		_, downErr := f.provider.Down(ctx)
		downDone <- downErr
	}()

	deadline := time.Now().Add(5 * time.Second)
	for {
		var waiting bool
		if err := f.db.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT 1
				  FROM pg_locks l
				  JOIN pg_class c ON c.oid=l.relation
				 WHERE c.relname='schedules'
				   AND l.mode='AccessExclusiveLock'
				   AND NOT l.granted
			)`,
		).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case downErr := <-downDone:
			downFinished = true
			t.Fatalf("033 Down completed before serializing with writer: %v", downErr)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("033 Down never waited for the schedule-first writer")
		}
		time.Sleep(10 * time.Millisecond)
	}

	id := "migration-033-down-race"
	baseDefinition := baseDefinition033(id)
	targetDefinition := []byte(`{"target":"down"}`)
	proposal := []byte(`{"race":"down"}`)
	prepared := []byte(`{"prepared":"down"}`)
	baseSnapshot := []byte(`{"base":"down"}`)
	if _, err := writer.ExecContext(ctx, `
		INSERT INTO task_definition_edit_operations (
			id, tenant_id, user_id, target_tenant_id, target_user_id,
				task_id, session_id, approval_ref, expires_at, original_status,
			base_definition_version, base_definition_digest, base_definition,
			target_definition_version, target_definition_digest,
			target_definition,
			canonical_proposal, proposal_digest,
			prepared_edit, prepared_edit_digest,
			base_snapshot, base_snapshot_digest
		) VALUES (
				$1, $2, $3, $2, $3, $4, $5, 'approval:' || $1,
				clock_timestamp()+interval '1 day', 'paused', 1, $6, $7, 2, $8,
				$9, $10, $11, $12, $13, $14, $15
			)`, id, f.tenantA, f.userA, f.taskA, f.sessionA,
		digest033(baseDefinition), baseDefinition,
		digest033(targetDefinition), targetDefinition, proposal, digest033(proposal),
		prepared, digest033(prepared),
		baseSnapshot, digest033(baseSnapshot)); err != nil {
		t.Fatal(err)
	}
	if err := writer.Commit(); err != nil {
		t.Fatal(err)
	}
	writerOpen = false

	select {
	case downErr := <-downDone:
		downFinished = true
		if downErr == nil || !strings.Contains(downErr.Error(), "refusing downgrade") {
			t.Fatalf("033 Down must observe and preserve committed edit state: %v", downErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("033 Down did not resume after writer commit")
	}

	var operations, migrationVersion int
	if err := f.db.QueryRowContext(ctx, `
		SELECT
		  (SELECT count(*) FROM task_definition_edit_operations WHERE id=$1),
		  (SELECT max(version_id) FROM goose_db_version WHERE is_applied)`, id,
	).Scan(&operations, &migrationVersion); err != nil {
		t.Fatal(err)
	}
	if operations != 1 || migrationVersion != 33 {
		t.Fatalf("refused concurrent Down lost state: operations=%d migration=%d",
			operations, migrationVersion)
	}
}
