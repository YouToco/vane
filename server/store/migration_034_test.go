package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pressly/goose/v3"
)

type migration034Fixture struct {
	db       *sql.DB
	provider *goose.Provider
	store    *Store
	tenantA  int64
	userA    int64
	sessionA int64
	taskA    string
	tenantB  int64
	userB    int64
	sessionB int64
	taskB    string
}

func migration034Scratch(t *testing.T) migration034Fixture {
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
	if _, err := provider.UpTo(ctx, 33); err != nil {
		t.Fatalf("迁移到 033 失败: %v", err)
	}

	f := migration034Fixture{
		db: db, provider: provider, tenantA: 1,
		taskA: "migration-034-task-a", taskB: "migration-034-task-b",
	}
	if err := db.QueryRowContext(ctx, `
		INSERT INTO users (feishu_open_id, name)
		VALUES ('migration-034-user-a', 'migration 034 A') RETURNING id`,
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
		VALUES ('migration-034-user-b', 'migration 034 B') RETURNING id`,
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
		VALUES ($3, $1, $2, 'definition edit B', 'paused')`,
		f.tenantB, f.userB, f.taskB); err != nil {
		t.Fatal(err)
	}

	if _, err := provider.UpTo(ctx, 34); err != nil {
		t.Fatalf("迁移到 034 失败: %v", err)
	}
	st, err := New(ctx, scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	f.store = st
	return f
}

const insertPendingEdit034SQL = `
	INSERT INTO task_definition_edit_operations (
		id, tenant_id, user_id, target_tenant_id, target_user_id,
		task_id, session_id, approval_ref, expires_at, original_status,
		base_definition_version, base_definition_digest, base_definition,
		target_definition_version, target_definition_digest, target_definition,
		canonical_proposal, proposal_digest,
		prepared_edit, prepared_edit_digest,
		base_snapshot, base_snapshot_digest
	) VALUES (
		$1, $2, $3, $2, $3, $4, $5, $6,
		clock_timestamp() + interval '1 day', 'paused',
		1, $7, $8, 2, $9, $10, $11, $12, $13, $14, $15, $16
	)`

func pendingEditArgs034(
	id string,
	tenantID, userID, sessionID int64,
	taskID string,
) []any {
	base := []byte(`{"schema":"approved-definition/v1","kind":"base","id":"` + id + `"}`)
	target := []byte(`{"schema":"approved-definition/v1","kind":"target","id":"` + id + `"}`)
	proposal := []byte(`{"schema":"frozen-task-definition-edit-proposal/v1","id":"` + id + `"}`)
	prepared := []byte(`{"schema":"prepared-task-definition-edit/v1","id":"` + id + `"}`)
	snapshot := []byte(`{"schema":"task-definition-edit-snapshot/v1","id":"` + id + `"}`)
	return []any{
		id, tenantID, userID, taskID, sessionID, "approval:" + id,
		digest034(base), base, digest034(target), target,
		proposal, digest034(proposal), prepared, digest034(prepared),
		snapshot, digest034(snapshot),
	}
}

func insertPendingEdit034(
	t *testing.T,
	f migration034Fixture,
	id string,
	tenantID, userID, sessionID int64,
	taskID string,
) {
	t.Helper()
	if _, err := f.db.ExecContext(t.Context(), insertPendingEdit034SQL,
		pendingEditArgs034(id, tenantID, userID, sessionID, taskID)...); err != nil {
		t.Fatalf("插入 034 edit operation %s 失败: %v", id, err)
	}
}

func digest034(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func beginSQLRole034(
	t *testing.T,
	db *sql.DB,
	tenantID int64,
	role string,
) *sql.Tx {
	t.Helper()
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	tenantContext := ""
	if tenantID > 0 {
		tenantContext = strconv.FormatInt(tenantID, 10)
	}
	if _, err := tx.ExecContext(t.Context(),
		`SELECT set_config('app.tenant_id', $1, true)`, tenantContext); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	var setRole string
	switch role {
	case "vane_app":
		setRole = `SET LOCAL ROLE vane_app`
	case "vane_edit_coordinator":
		setRole = `SET LOCAL ROLE vane_edit_coordinator`
	case "vane_edit_receipt":
		setRole = `SET LOCAL ROLE vane_edit_receipt`
	default:
		_ = tx.Rollback()
		t.Fatalf("未知测试角色 %q", role)
	}
	if _, err := tx.ExecContext(t.Context(), setRole); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	return tx
}

func requireSQLState034(t *testing.T, err error, want string) {
	t.Helper()
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != want {
		t.Fatalf("SQLSTATE=%v, want %s (err=%v)", func() string {
			if pgErr == nil {
				return ""
			}
			return pgErr.Code
		}(), want, err)
	}
}

func makeExecutingEdit034(
	t *testing.T,
	f migration034Fixture,
	operationID string,
	tenantID, userID int64,
	taskID string,
) {
	t.Helper()
	if _, err := f.db.ExecContext(t.Context(), `
		UPDATE task_definition_edit_operations
		   SET status='executing', confirmed_at=clock_timestamp(),
		       lease_owner='migration-034-owner',
		       lease_until=clock_timestamp()+interval '5 minutes',
		       takeover_not_before=clock_timestamp()+interval '6 minutes',
		       fence=1, attempt=1,
		       receipt_provider='feishu_card_patch', receipt_target='om_034'
		 WHERE id=$1 AND tenant_id=$2 AND user_id=$3`,
		operationID, tenantID, userID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.ExecContext(t.Context(), `
		UPDATE schedules
		   SET status='paused', definition_edit_operation_id=$2,
		       definition_edit_fence=1
		 WHERE id=$1`, taskID, operationID); err != nil {
		t.Fatal(err)
	}
}

func insertReceipt034(
	t *testing.T,
	f migration034Fixture,
	operationID string,
	tenantID, userID, sessionID int64,
	providerKey string,
) {
	t.Helper()
	if _, err := f.db.ExecContext(t.Context(), `
		INSERT INTO task_definition_edit_receipts (
			operation_id, tenant_id, user_id, session_id,
			provider, target, provider_key
		) VALUES ($1, $2, $3, $4, 'feishu_card_patch', 'om_034', $5)`,
		operationID, tenantID, userID, sessionID, providerKey); err != nil {
		t.Fatal(err)
	}
}

func TestMigration034RestrictedRolesAndRLS(t *testing.T) {
	f := migration034Scratch(t)
	ctx := t.Context()

	var (
		coordinatorLogin, coordinatorInherit, coordinatorBypass      bool
		coordinatorSuper, coordinatorCreateDB, coordinatorCreateRole bool
		coordinatorReplication                                       bool
		receiptLogin, receiptInherit, receiptBypass                  bool
		receiptSuper, receiptCreateDB, receiptCreateRole             bool
		receiptReplication                                           bool
		coordinatorIsApp, receiptIsApp                               bool
		appIsCoordinator, appIsReceipt                               bool
		foreignRoleMembers, outgoingRoleMemberships                  int
	)
	if err := f.db.QueryRowContext(ctx, `
		SELECT c.rolcanlogin, c.rolinherit, c.rolbypassrls,
		       c.rolsuper, c.rolcreatedb, c.rolcreaterole, c.rolreplication,
		       r.rolcanlogin, r.rolinherit, r.rolbypassrls,
		       r.rolsuper, r.rolcreatedb, r.rolcreaterole, r.rolreplication,
		       pg_has_role('vane_edit_coordinator', 'vane_app', 'MEMBER'),
		       pg_has_role('vane_edit_receipt', 'vane_app', 'MEMBER'),
		       pg_has_role('vane_app', 'vane_edit_coordinator', 'MEMBER'),
		       pg_has_role('vane_app', 'vane_edit_receipt', 'MEMBER'),
		       (SELECT count(*)
		          FROM pg_auth_members am
		          JOIN pg_roles granted_role ON granted_role.oid=am.roleid
		          JOIN pg_roles member_role ON member_role.oid=am.member
		         WHERE granted_role.rolname IN (
		                   'vane_edit_coordinator', 'vane_edit_receipt'
		               )
		           AND member_role.rolname <> CURRENT_USER),
		       (SELECT count(*)
		          FROM pg_auth_members am
		          JOIN pg_roles member_role ON member_role.oid=am.member
		         WHERE member_role.rolname IN (
		                   'vane_edit_coordinator', 'vane_edit_receipt'
		               ))
		  FROM pg_roles c CROSS JOIN pg_roles r
		 WHERE c.rolname='vane_edit_coordinator'
		   AND r.rolname='vane_edit_receipt'`,
	).Scan(
		&coordinatorLogin, &coordinatorInherit, &coordinatorBypass,
		&coordinatorSuper, &coordinatorCreateDB, &coordinatorCreateRole,
		&coordinatorReplication,
		&receiptLogin, &receiptInherit, &receiptBypass,
		&receiptSuper, &receiptCreateDB, &receiptCreateRole, &receiptReplication,
		&coordinatorIsApp, &receiptIsApp,
		&appIsCoordinator, &appIsReceipt,
		&foreignRoleMembers, &outgoingRoleMemberships,
	); err != nil {
		t.Fatal(err)
	}
	if coordinatorLogin || coordinatorInherit || coordinatorBypass ||
		coordinatorSuper || coordinatorCreateDB || coordinatorCreateRole ||
		coordinatorReplication || receiptLogin || receiptInherit || receiptBypass ||
		receiptSuper || receiptCreateDB || receiptCreateRole || receiptReplication ||
		coordinatorIsApp || receiptIsApp || appIsCoordinator || appIsReceipt ||
		foreignRoleMembers != 0 || outgoingRoleMemberships != 0 {
		t.Fatalf("restricted role 属性/继承漂移: coordinator=%v/%v/%v dangerous=%v/%v/%v/%v to_app=%v receipt=%v/%v/%v dangerous=%v/%v/%v/%v to_app=%v app_to_roles=%v/%v foreign_members=%d outgoing=%d",
			coordinatorLogin, coordinatorInherit, coordinatorBypass,
			coordinatorSuper, coordinatorCreateDB, coordinatorCreateRole,
			coordinatorReplication, coordinatorIsApp,
			receiptLogin, receiptInherit, receiptBypass,
			receiptSuper, receiptCreateDB, receiptCreateRole, receiptReplication,
			receiptIsApp,
			appIsCoordinator, appIsReceipt, foreignRoleMembers, outgoingRoleMemberships)
	}

	var (
		appMarkerUpdate, coordinatorStatusUpdate, coordinatorApprovalUpdate bool
		coordinatorScheduleDelete, coordinatorOperationDelete               bool
		coordinatorReceiptDelete, coordinatorProvenanceRead                 bool
		coordinatorLockCapability, receiptLockCapability                    bool
		coordinatorTenantStatusUpdate                                       bool
		receiptMessagesUpdate, receiptImmutableUpdate                       bool
		receiptScheduleRead, receiptScheduleUpdate, receiptOperationUpdate  bool
		receiptOperationStatusRead, receiptOperationPayloadRead             bool
		receiptTenantRead                                                   bool
		receiptDefinitionRead, receiptDefinitionInsert                      bool
	)
	if err := f.db.QueryRowContext(ctx, `
		SELECT
		  has_column_privilege('vane_app', 'schedules',
		                       'definition_edit_operation_id', 'UPDATE'),
		  has_column_privilege('vane_edit_coordinator',
		                       'task_definition_edit_operations', 'status', 'UPDATE'),
		  has_column_privilege('vane_edit_coordinator',
		                       'task_definition_edit_operations', 'approval_ref', 'UPDATE'),
		  has_table_privilege('vane_edit_coordinator', 'schedules', 'DELETE'),
		  has_table_privilege('vane_edit_coordinator',
		                      'task_definition_edit_operations', 'DELETE'),
		  has_table_privilege('vane_edit_coordinator',
		                      'task_definition_edit_receipts', 'DELETE'),
		  has_column_privilege('vane_edit_coordinator', 'pending_actions',
		                       'compiled_definition', 'SELECT'),
		  has_column_privilege('vane_edit_coordinator', 'tenants',
		                       'definition_edit_lock_capability', 'UPDATE'),
		  has_column_privilege('vane_edit_receipt', 'tenants',
		                       'definition_edit_lock_capability', 'UPDATE'),
		  has_column_privilege('vane_edit_coordinator', 'tenants',
		                       'status', 'UPDATE'),
		  has_column_privilege('vane_edit_receipt', 'agent_sessions',
		                       'messages', 'UPDATE'),
		  has_column_privilege('vane_edit_receipt',
		                       'task_definition_edit_receipts', 'operation_id', 'UPDATE'),
		  has_table_privilege('vane_edit_receipt', 'schedules', 'SELECT'),
		  has_table_privilege('vane_edit_receipt', 'schedules', 'UPDATE'),
		  has_table_privilege('vane_edit_receipt',
		                      'task_definition_edit_operations', 'UPDATE'),
		  has_column_privilege('vane_edit_receipt',
		                       'task_definition_edit_operations', 'status', 'SELECT'),
		  has_column_privilege('vane_edit_receipt',
		                       'task_definition_edit_operations', 'target_definition', 'SELECT'),
		  has_column_privilege('vane_edit_receipt', 'tenants', 'id', 'SELECT'),
		  has_table_privilege('vane_edit_receipt',
		                      'task_approved_definition_versions', 'SELECT'),
		  has_table_privilege('vane_edit_receipt',
		                      'task_approved_definition_versions', 'INSERT')`,
	).Scan(
		&appMarkerUpdate, &coordinatorStatusUpdate, &coordinatorApprovalUpdate,
		&coordinatorScheduleDelete, &coordinatorOperationDelete,
		&coordinatorReceiptDelete, &coordinatorProvenanceRead,
		&coordinatorLockCapability, &receiptLockCapability,
		&coordinatorTenantStatusUpdate,
		&receiptMessagesUpdate, &receiptImmutableUpdate,
		&receiptScheduleRead, &receiptScheduleUpdate, &receiptOperationUpdate,
		&receiptOperationStatusRead, &receiptOperationPayloadRead, &receiptTenantRead,
		&receiptDefinitionRead, &receiptDefinitionInsert,
	); err != nil {
		t.Fatal(err)
	}
	if appMarkerUpdate || !coordinatorStatusUpdate || coordinatorApprovalUpdate ||
		coordinatorScheduleDelete || coordinatorOperationDelete || coordinatorReceiptDelete ||
		!coordinatorProvenanceRead || !coordinatorLockCapability || receiptLockCapability ||
		coordinatorTenantStatusUpdate || !receiptMessagesUpdate || receiptImmutableUpdate ||
		receiptScheduleRead || receiptScheduleUpdate || receiptOperationUpdate ||
		!receiptOperationStatusRead || receiptOperationPayloadRead || receiptTenantRead ||
		receiptDefinitionRead || receiptDefinitionInsert {
		t.Fatalf("034 privilege matrix drifted: app_marker=%v coord_status/approval=%v/%v deletes=%v/%v/%v provenance=%v lock_capability=%v/%v tenant_status=%v receipt_messages/immutable=%v/%v schedule=%v/%v operation_update/status/payload=%v/%v/%v tenant_read=%v definition=%v/%v",
			appMarkerUpdate, coordinatorStatusUpdate, coordinatorApprovalUpdate,
			coordinatorScheduleDelete, coordinatorOperationDelete, coordinatorReceiptDelete,
			coordinatorProvenanceRead, coordinatorLockCapability, receiptLockCapability,
			coordinatorTenantStatusUpdate, receiptMessagesUpdate, receiptImmutableUpdate,
			receiptScheduleRead, receiptScheduleUpdate, receiptOperationUpdate,
			receiptOperationStatusRead, receiptOperationPayloadRead, receiptTenantRead,
			receiptDefinitionRead, receiptDefinitionInsert)
	}

	if _, err := f.db.ExecContext(ctx, `
		INSERT INTO schedule_playbooks (schedule_id, content)
		VALUES ($1, 'tenant A playbook'), ($2, 'tenant B playbook')`,
		f.taskA, f.taskB); err != nil {
		t.Fatal(err)
	}
	insertPendingEdit034(t, f, "migration-034-op-a", f.tenantA, f.userA, f.sessionA, f.taskA)
	insertPendingEdit034(t, f, "migration-034-op-b", f.tenantB, f.userB, f.sessionB, f.taskB)

	coordinatorTx, err := f.store.beginTaskDefinitionEditTx(ctx, f.tenantA)
	if err != nil {
		t.Fatal(err)
	}
	var currentUser, tenantContext string
	var operationCount, playbookCount, tenantCount, membershipCount int
	if err := coordinatorTx.QueryRow(ctx, `SELECT current_user,
		current_setting('app.tenant_id', true)`).Scan(&currentUser, &tenantContext); err != nil {
		t.Fatal(err)
	}
	if err := coordinatorTx.QueryRow(ctx,
		`SELECT count(*) FROM task_definition_edit_operations`).Scan(&operationCount); err != nil {
		t.Fatal(err)
	}
	if err := coordinatorTx.QueryRow(ctx,
		`SELECT count(*) FROM schedule_playbooks`).Scan(&playbookCount); err != nil {
		t.Fatal(err)
	}
	if err := coordinatorTx.QueryRow(ctx,
		`SELECT count(*) FROM tenants`).Scan(&tenantCount); err != nil {
		t.Fatal(err)
	}
	if err := coordinatorTx.QueryRow(ctx,
		`SELECT count(*) FROM memberships`).Scan(&membershipCount); err != nil {
		t.Fatal(err)
	}
	foreignTag, err := coordinatorTx.Exec(ctx, `
		UPDATE schedule_playbooks SET content='cross-tenant-write'
		 WHERE schedule_id=$1`, f.taskB)
	if err != nil || foreignTag.RowsAffected() != 0 {
		t.Fatalf("coordinator cross-tenant legacy UPDATE 应不可见: tag=%v err=%v",
			foreignTag, err)
	}
	rollbackTaskDefinitionEditTx(ctx, coordinatorTx)
	if currentUser != "vane_edit_coordinator" ||
		tenantContext != strconv.FormatInt(f.tenantA, 10) ||
		operationCount != 1 || playbookCount != 1 || tenantCount != 1 ||
		membershipCount != 1 {
		t.Fatalf("coordinator role/RLS mismatch: user=%q tenant=%q operations=%d playbooks=%d tenants=%d memberships=%d",
			currentUser, tenantContext, operationCount, playbookCount,
			tenantCount, membershipCount)
	}

	if _, err := f.store.beginTaskDefinitionEditTx(ctx, 0); err == nil {
		t.Fatal("coordinator transaction accepted an empty tenant context")
	}

	crossTx, err := f.store.beginTaskDefinitionEditTx(ctx, f.tenantA)
	if err != nil {
		t.Fatal(err)
	}
	_, crossErr := crossTx.Exec(ctx, insertPendingEdit034SQL,
		pendingEditArgs034("migration-034-cross", f.tenantB, f.userB,
			f.sessionB, "migration-034-cross-task")...)
	requireSQLState034(t, crossErr, "42501")
	rollbackTaskDefinitionEditTx(ctx, crossTx)

	const coordinatorTask = "migration-034-coordinator-task"
	if _, err := f.db.ExecContext(ctx, `
		INSERT INTO schedules (id, tenant_id, user_id, nl_description, status)
		VALUES ($1, $2, $3, 'coordinator task', 'paused')`,
		coordinatorTask, f.tenantA, f.userA); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.ExecContext(ctx, `
		INSERT INTO schedule_playbooks (schedule_id, content)
		VALUES ($1, 'coordinator playbook')`,
		coordinatorTask); err != nil {
		t.Fatal(err)
	}
	var sourceID int64
	if err := f.db.QueryRowContext(ctx, `
		INSERT INTO sources (url, platform, capability)
		VALUES ('https://migration-034.example/source', 'web', 'feed') RETURNING id`,
	).Scan(&sourceID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.ExecContext(ctx, `
		INSERT INTO pending_actions (
			id, tenant_id, user_id, session_id, tool_name, args, summary,
			status, expires_at, execution_version, phase,
			compiled_definition, compiled_digest, prepared_schedule,
			ensure_receipt, task_id
		) VALUES (
			'migration-034-create-provenance', $1, $2, $3,
			'create_schedule', '{}', 'sealed create provenance',
			'executed', clock_timestamp()+interval '1 day', 1, 'committed',
			$4, $5, $6, $7, $8
		)`, f.tenantA, f.userA, f.sessionA,
		[]byte("compiled-definition"), digest034([]byte("compiled-definition")),
		[]byte("prepared-schedule"), []byte("ensure-receipt"), coordinatorTask); err != nil {
		t.Fatal(err)
	}

	const coordinatorOperation = "migration-034-coordinator-op"
	coordinatorTx, err = f.store.beginTaskDefinitionEditTx(ctx, f.tenantA)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinatorTx.Exec(ctx, insertPendingEdit034SQL,
		pendingEditArgs034(coordinatorOperation, f.tenantA, f.userA,
			f.sessionA, coordinatorTask)...); err != nil {
		t.Fatalf("coordinator INSERT operation 失败: %v", err)
	}
	if _, err := coordinatorTx.Exec(ctx, `
		UPDATE task_definition_edit_operations
		   SET status='executing', confirmed_at=clock_timestamp(),
		       lease_owner='coordinator-worker',
		       lease_until=clock_timestamp()+interval '5 minutes',
		       takeover_not_before=clock_timestamp()+interval '6 minutes',
		       fence=1, attempt=1,
		       receipt_provider='feishu_card_patch', receipt_target='om_034'
		 WHERE id=$1`, coordinatorOperation); err != nil {
		t.Fatalf("coordinator UPDATE operation checkpoint 失败: %v", err)
	}
	if _, err := coordinatorTx.Exec(ctx, `
		UPDATE schedules
		   SET status='paused', definition_edit_operation_id=$2,
		       definition_edit_fence=1
		 WHERE id=$1`, coordinatorTask, coordinatorOperation); err != nil {
		t.Fatalf("coordinator schedule marker 失败: %v", err)
	}
	definitionPayload := []byte(`{"schema":"migration-034-approved/v1"}`)
	definitionDigest := digest034(definitionPayload)
	if _, err := coordinatorTx.Exec(ctx, `
		INSERT INTO task_approved_definition_versions (
			tenant_id, user_id, task_id, version, schema_version,
			execution_mode, definition_digest, payload, approval_ref
		) VALUES ($1, $2, $3, 1, 'migration-034/v1',
		          'compiled', $4, $5, 'migration-034-approval')`,
		f.tenantA, f.userA, coordinatorTask, definitionDigest, definitionPayload); err != nil {
		t.Fatalf("coordinator append Approved Definition 失败: %v", err)
	}
	if _, err := coordinatorTx.Exec(ctx, `
		UPDATE schedules
		   SET nl_description='coordinator updated', spec_json='{}', scope_json='{}',
		       push_strictness='loose', execution_mode='compiled',
		       approved_definition_version=1, approved_definition_digest=$2,
		       updated_at=clock_timestamp()
		 WHERE id=$1`, coordinatorTask, definitionDigest); err != nil {
		t.Fatalf("coordinator advance definition head 失败: %v", err)
	}
	if _, err := coordinatorTx.Exec(ctx, `
		UPDATE schedule_playbooks
		   SET content='coordinator updated', fetch_plan='{}',
		       updated_at=clock_timestamp()
		 WHERE schedule_id=$1`, coordinatorTask); err != nil {
		t.Fatalf("coordinator update playbook 失败: %v", err)
	}
	if _, err := coordinatorTx.Exec(ctx,
		`INSERT INTO schedule_sources (schedule_id, source_id) VALUES ($1, $2)`,
		coordinatorTask, sourceID); err != nil {
		t.Fatalf("coordinator insert source link 失败: %v", err)
	}
	for _, lockProbe := range []struct {
		name  string
		query string
		args  []any
	}{
		{
			name: "tenant and membership",
			query: `SELECT true FROM tenants AS t
			          JOIN memberships AS m ON m.tenant_id=t.id
			         WHERE t.id=$1 AND m.user_id=$2
			           FOR SHARE OF t, m`,
			args: []any{f.tenantA, f.userA},
		},
		{
			name:  "agent session",
			query: `SELECT true FROM agent_sessions WHERE id=$1 FOR SHARE`,
			args:  []any{f.sessionA},
		},
		{
			name: "creation provenance",
			query: `SELECT true FROM pending_actions
			         WHERE id='migration-034-create-provenance' FOR SHARE`,
		},
		{
			name:  "source",
			query: `SELECT true FROM sources WHERE id=$1 FOR KEY SHARE`,
			args:  []any{sourceID},
		},
		{
			name: "source link",
			query: `SELECT true FROM schedule_sources
			         WHERE schedule_id=$1 AND source_id=$2 FOR SHARE`,
			args: []any{coordinatorTask, sourceID},
		},
	} {
		var locked bool
		if err := coordinatorTx.QueryRow(ctx, lockProbe.query, lockProbe.args...).Scan(&locked); err != nil {
			t.Fatalf("coordinator %s row lock 失败: %v", lockProbe.name, err)
		}
		if !locked {
			t.Fatalf("coordinator %s row lock returned false", lockProbe.name)
		}
	}
	if _, err := coordinatorTx.Exec(ctx,
		`DELETE FROM schedule_sources WHERE schedule_id=$1`, coordinatorTask); err != nil {
		t.Fatalf("coordinator clear source links 失败: %v", err)
	}
	if _, err := coordinatorTx.Exec(ctx,
		`INSERT INTO schedule_sources (schedule_id, source_id) VALUES ($1, $2)`,
		coordinatorTask, sourceID); err != nil {
		t.Fatalf("coordinator reinsert source link 失败: %v", err)
	}
	var provenanceCount int
	if err := coordinatorTx.QueryRow(ctx, `
		SELECT count(compiled_definition) FROM pending_actions
		 WHERE id='migration-034-create-provenance'
		   AND execution_version=1 AND tool_name='create_schedule'`,
	).Scan(&provenanceCount); err != nil {
		t.Fatalf("coordinator provenance SELECT 失败: %v", err)
	}
	if provenanceCount != 1 {
		t.Fatalf("coordinator provenance count=%d, want 1", provenanceCount)
	}
	var receiptID int64
	if err := coordinatorTx.QueryRow(ctx, `
		INSERT INTO task_definition_edit_receipts (
			operation_id, tenant_id, user_id, session_id,
			provider, target, provider_key
		) VALUES ($1, $2, $3, $4, 'feishu_card_patch', 'om_034',
		          '00000000-0000-0000-0000-000000000034')
		RETURNING id`, coordinatorOperation, f.tenantA, f.userA, f.sessionA,
	).Scan(&receiptID); err != nil {
		t.Fatalf("coordinator insert outbox 失败: %v", err)
	}
	if receiptID <= 0 {
		t.Fatalf("coordinator receipt id=%d", receiptID)
	}
	if err := coordinatorTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	var tenantBusinessBefore, tenantBusinessAfter string
	if err := f.db.QueryRowContext(ctx, `
		SELECT (to_jsonb(t) - 'definition_edit_lock_capability')::text
		  FROM tenants AS t WHERE id=$1`, f.tenantA,
	).Scan(&tenantBusinessBefore); err != nil {
		t.Fatal(err)
	}
	lockCapabilityTx := beginSQLRole034(t, f.db, f.tenantA, "vane_edit_coordinator")
	result, err := lockCapabilityTx.ExecContext(ctx, `
		UPDATE tenants
		   SET definition_edit_lock_capability=DEFAULT
		 WHERE id=$1`, f.tenantA)
	if err != nil {
		t.Fatalf("coordinator lock capability DEFAULT update 失败: %v", err)
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		t.Fatalf("coordinator lock capability affected=%d err=%v, want 1", affected, err)
	}
	if err := lockCapabilityTx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := f.db.QueryRowContext(ctx, `
		SELECT (to_jsonb(t) - 'definition_edit_lock_capability')::text
		  FROM tenants AS t WHERE id=$1`, f.tenantA,
	).Scan(&tenantBusinessAfter); err != nil {
		t.Fatal(err)
	}
	if tenantBusinessAfter != tenantBusinessBefore {
		t.Fatalf("lock capability DEFAULT mutated tenant: before=%s after=%s",
			tenantBusinessBefore, tenantBusinessAfter)
	}
	crossCapabilityTx := beginSQLRole034(
		t, f.db, f.tenantA, "vane_edit_coordinator")
	crossResult, err := crossCapabilityTx.ExecContext(ctx, `
		UPDATE tenants
		   SET definition_edit_lock_capability=DEFAULT
		 WHERE id=$1`, f.tenantB)
	if err != nil {
		_ = crossCapabilityTx.Rollback()
		t.Fatalf("cross-tenant lock capability probe failed: %v", err)
	}
	if affected, rowsErr := crossResult.RowsAffected(); rowsErr != nil || affected != 0 {
		_ = crossCapabilityTx.Rollback()
		t.Fatalf("cross-tenant lock capability affected=%d err=%v, want 0",
			affected, rowsErr)
	}
	if err := crossCapabilityTx.Commit(); err != nil {
		t.Fatal(err)
	}

	nonDefaultCapabilityTx := beginSQLRole034(t, f.db, f.tenantA, "vane_edit_coordinator")
	_, err = nonDefaultCapabilityTx.ExecContext(ctx, `
		UPDATE tenants
		   SET definition_edit_lock_capability=false
		 WHERE id=$1`, f.tenantA)
	requireSQLState034(t, err, "428C9")
	_ = nonDefaultCapabilityTx.Rollback()

	tenantBusinessTx := beginSQLRole034(t, f.db, f.tenantA, "vane_edit_coordinator")
	_, err = tenantBusinessTx.ExecContext(ctx,
		`UPDATE tenants SET status='suspended' WHERE id=$1`, f.tenantA)
	requireSQLState034(t, err, "42501")
	_ = tenantBusinessTx.Rollback()

	for _, tc := range []struct {
		name string
		sql  string
	}{
		{name: "immutable operation identity", sql: `UPDATE task_definition_edit_operations SET approval_ref='changed' WHERE id='migration-034-coordinator-op'`},
		{name: "schedule delete", sql: `DELETE FROM schedules WHERE id='migration-034-coordinator-task'`},
		{name: "operation delete", sql: `DELETE FROM task_definition_edit_operations WHERE id='migration-034-coordinator-op'`},
		{name: "receipt delete", sql: `DELETE FROM task_definition_edit_receipts WHERE operation_id='migration-034-coordinator-op'`},
	} {
		t.Run("coordinator rejects "+tc.name, func(t *testing.T) {
			tx := beginSQLRole034(t, f.db, f.tenantA, "vane_edit_coordinator")
			_, err := tx.ExecContext(t.Context(), tc.sql)
			requireSQLState034(t, err, "42501")
			_ = tx.Rollback()
		})
	}

	appMarkerTx := beginSQLRole034(t, f.db, f.tenantA, "vane_app")
	_, err = appMarkerTx.ExecContext(ctx, `
		UPDATE schedules
		   SET definition_edit_operation_id=NULL, definition_edit_fence=NULL
		 WHERE id=$1`, coordinatorTask)
	requireSQLState034(t, err, "42501")
	_ = appMarkerTx.Rollback()

	appActivateTx := beginSQLRole034(t, f.db, f.tenantA, "vane_app")
	_, err = appActivateTx.ExecContext(ctx,
		`UPDATE schedules SET status='active' WHERE id=$1`, coordinatorTask)
	requireSQLState034(t, err, "23514")
	_ = appActivateTx.Rollback()

	makeExecutingEdit034(t, f, "migration-034-op-b", f.tenantB, f.userB, f.taskB)
	insertReceipt034(t, f, "migration-034-op-b", f.tenantB, f.userB, f.sessionB,
		"00000000-0000-0000-0000-000000000035")

	receiptTx, err := f.store.beginTaskDefinitionEditReceiptTx(ctx, f.tenantA)
	if err != nil {
		t.Fatal(err)
	}
	if err := receiptTx.QueryRow(ctx, `SELECT current_user`).Scan(&currentUser); err != nil {
		t.Fatal(err)
	}
	if err := receiptTx.QueryRow(ctx,
		`SELECT count(*) FROM task_definition_edit_receipts`).Scan(&operationCount); err != nil {
		t.Fatal(err)
	}
	if currentUser != "vane_edit_receipt" || operationCount != 1 {
		t.Fatalf("receipt role/RLS mismatch: user=%q receipts=%d", currentUser, operationCount)
	}
	tag, err := receiptTx.Exec(ctx, `
		UPDATE task_definition_edit_receipts
		   SET lease_owner='receipt-worker',
		       lease_until=clock_timestamp()+interval '5 minutes',
		       takeover_not_before=clock_timestamp()+interval '6 minutes',
		       fence=fence+1, attempt=attempt+1, updated_at=clock_timestamp()
		 WHERE id=$1`, receiptID)
	if err != nil || tag.RowsAffected() != 1 {
		t.Fatalf("receipt lease update: tag=%v err=%v", tag, err)
	}
	tag, err = receiptTx.Exec(ctx, `
		UPDATE agent_sessions SET messages=messages || '[{"role":"assistant","content":"receipt"}]'::jsonb
		 WHERE id=$1 AND tenant_id=$2 AND user_id=$3`, f.sessionA, f.tenantA, f.userA)
	if err != nil || tag.RowsAffected() != 1 {
		t.Fatalf("receipt session messages update: tag=%v err=%v", tag, err)
	}
	tag, err = receiptTx.Exec(ctx, `
		UPDATE task_definition_edit_receipts SET updated_at=clock_timestamp()
		 WHERE operation_id='migration-034-op-b'`)
	if err != nil || tag.RowsAffected() != 0 {
		t.Fatalf("receipt cross-tenant UPDATE 应不可见: tag=%v err=%v", tag, err)
	}
	if err := receiptTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name string
		sql  string
	}{
		{name: "operation write", sql: `UPDATE task_definition_edit_operations SET updated_at=clock_timestamp() WHERE id='migration-034-coordinator-op'`},
		{name: "schedule write", sql: `UPDATE schedules SET status='paused' WHERE id='migration-034-coordinator-task'`},
		{name: "definition write", sql: `UPDATE task_approved_definition_versions SET approval_ref='changed' WHERE task_id='migration-034-coordinator-task'`},
		{name: "receipt immutable identity", sql: `UPDATE task_definition_edit_receipts SET operation_id='changed' WHERE id=1`},
		{name: "receipt delete", sql: `DELETE FROM task_definition_edit_receipts WHERE id=1`},
	} {
		t.Run("receipt rejects "+tc.name, func(t *testing.T) {
			tx := beginSQLRole034(t, f.db, f.tenantA, "vane_edit_receipt")
			_, err := tx.ExecContext(t.Context(), tc.sql)
			requireSQLState034(t, err, "42501")
			_ = tx.Rollback()
		})
	}
}

func TestMigration034ConcurrentFirstRoleCreationIsIdempotent(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		requireDatabaseCapability(t)
	}
	rawMigration, err := fs.ReadFile(
		migrationsFS, "migrations/034_task_definition_edit_store.sql")
	if err != nil {
		t.Fatal(err)
	}
	const conflictHandler = "WHEN duplicate_object OR unique_violation THEN NULL;"
	if count := strings.Count(string(rawMigration), conflictHandler); count != 2 {
		t.Fatalf("034 concurrent role conflict handlers=%d, want 2", count)
	}

	f := migration034Scratch(t)
	otherDB, err := sql.Open("pgx", dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = otherDB.Close() }()
	roleName := fmt.Sprintf(
		"vane_edit_concurrency_%d", time.Now().UnixNano())
	quotedRole := pgx.Identifier{roleName}.Sanitize()
	defer func() {
		_, _ = otherDB.ExecContext(context.Background(),
			"DROP ROLE IF EXISTS "+quotedRole)
	}()

	txA, err := f.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = txA.Rollback() }()
	txB, err := otherDB.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = txB.Rollback() }()
	createRoleSQL := fmt.Sprintf(`
		DO $body$
		BEGIN
		    BEGIN
		        CREATE ROLE %s
		            NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
		            NOLOGIN NOINHERIT NOBYPASSRLS;
		    EXCEPTION
		        WHEN duplicate_object OR unique_violation THEN NULL;
		    END;
		END
		$body$`, quotedRole)
	type roleCreateResult struct {
		index int
		err   error
	}
	results := make(chan roleCreateResult, 2)
	go func() {
		_, execErr := txA.ExecContext(t.Context(), createRoleSQL)
		results <- roleCreateResult{index: 0, err: execErr}
	}()
	go func() {
		_, execErr := txB.ExecContext(t.Context(), createRoleSQL)
		results <- roleCreateResult{index: 1, err: execErr}
	}()

	var first roleCreateResult
	select {
	case first = <-results:
	case <-time.After(5 * time.Second):
		t.Fatal("neither concurrent CREATE ROLE transaction made progress")
	}
	if first.err != nil {
		t.Fatalf("first concurrent CREATE ROLE failed: %v", first.err)
	}
	transactions := []*sql.Tx{txA, txB}
	if err := transactions[first.index].Commit(); err != nil {
		t.Fatalf("commit first CREATE ROLE winner: %v", err)
	}
	var second roleCreateResult
	select {
	case second = <-results:
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent CREATE ROLE loser did not recover after winner commit")
	}
	if second.err != nil {
		t.Fatalf("concurrent CREATE ROLE loser was not idempotent: %v", second.err)
	}
	if err := transactions[second.index].Commit(); err != nil {
		t.Fatalf("commit recovered CREATE ROLE transaction: %v", err)
	}

	var count int
	var login, inherit, bypass, super, createDB, createRole, replication bool
	if err := otherDB.QueryRowContext(t.Context(), `
		SELECT count(*), bool_or(rolcanlogin), bool_or(rolinherit),
		       bool_or(rolbypassrls), bool_or(rolsuper), bool_or(rolcreatedb),
		       bool_or(rolcreaterole), bool_or(rolreplication)
		  FROM pg_roles WHERE rolname=$1`, roleName,
	).Scan(&count, &login, &inherit, &bypass, &super, &createDB,
		&createRole, &replication); err != nil {
		t.Fatal(err)
	}
	if count != 1 || login || inherit || bypass || super || createDB ||
		createRole || replication {
		t.Fatalf("concurrent role result count=%d attrs=%v/%v/%v/%v/%v/%v/%v",
			count, login, inherit, bypass, super, createDB, createRole, replication)
	}
}

var errInjectedEditRoleSetup = errors.New("injected edit role setup failure")

type taskDefinitionEditSetupFaultTx struct {
	pgx.Tx
	failContains       string
	cancel             context.CancelFunc
	rollbackCalls      int
	rollbackContextErr error
}

func (tx *taskDefinitionEditSetupFaultTx) Exec(
	ctx context.Context,
	sql string,
	arguments ...any,
) (pgconn.CommandTag, error) {
	if strings.Contains(sql, tx.failContains) {
		if tx.cancel != nil {
			tx.cancel()
		}
		return pgconn.CommandTag{}, errInjectedEditRoleSetup
	}
	return tx.Tx.Exec(ctx, sql, arguments...)
}

func (tx *taskDefinitionEditSetupFaultTx) Rollback(ctx context.Context) error {
	tx.rollbackCalls++
	tx.rollbackContextErr = ctx.Err()
	return tx.Tx.Rollback(ctx)
}

func TestBeginTaskDefinitionEditRoleTxRollsBackSetupFailures(t *testing.T) {
	f := migration034Scratch(t)

	for _, tc := range []struct {
		name         string
		failContains string
		receipt      bool
	}{
		{name: "tenant context", failContains: "set_config"},
		{name: "coordinator role", failContains: "SET LOCAL ROLE vane_edit_coordinator"},
		{name: "receipt role", failContains: "SET LOCAL ROLE vane_edit_receipt", receipt: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			faultStore := *f.store
			var wrapped *taskDefinitionEditSetupFaultTx
			faultStore.beginTx = func(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error) {
				realTx, err := f.store.beginTx(ctx, opts)
				if err != nil {
					return nil, err
				}
				wrapped = &taskDefinitionEditSetupFaultTx{
					Tx: realTx, failContains: tc.failContains, cancel: cancel,
				}
				return wrapped, nil
			}
			var err error
			if tc.receipt {
				_, err = faultStore.beginTaskDefinitionEditReceiptTx(ctx, f.tenantA)
			} else {
				_, err = faultStore.beginTaskDefinitionEditTx(ctx, f.tenantA)
			}
			if !errors.Is(err, errInjectedEditRoleSetup) {
				t.Fatalf("setup error=%v, want injected", err)
			}
			if wrapped == nil || wrapped.rollbackCalls != 1 || wrapped.rollbackContextErr != nil {
				t.Fatalf("setup failure 未 detached rollback: %+v", wrapped)
			}

			var user, tenantContext string
			if err := f.store.pool.QueryRow(t.Context(), `
				SELECT current_user,
				       COALESCE(current_setting('app.tenant_id', true), '')`,
			).Scan(&user, &tenantContext); err != nil {
				t.Fatal(err)
			}
			if user == "vane_edit_coordinator" || user == "vane_edit_receipt" ||
				tenantContext != "" {
				t.Fatalf("setup failure 污染连接池: user=%q tenant=%q", user, tenantContext)
			}
		})
	}

	beginCalled := false
	faultStore := *f.store
	faultStore.beginTx = func(context.Context, pgx.TxOptions) (pgx.Tx, error) {
		beginCalled = true
		return nil, fmt.Errorf("unexpected begin")
	}
	for _, invalidTenantID := range []int64{-1, 0} {
		if _, err := faultStore.beginTaskDefinitionEditTx(
			t.Context(), invalidTenantID); err == nil {
			t.Fatalf("tenant id %d must fail before BEGIN", invalidTenantID)
		}
	}
	if beginCalled {
		t.Fatal("non-positive tenant id unexpectedly opened a transaction")
	}
}

func TestMigration034DowngradeGuardAndCleanup(t *testing.T) {
	t.Run("empty foundation can downgrade", func(t *testing.T) {
		f := migration034Scratch(t)
		if _, err := f.provider.Down(t.Context()); err != nil {
			t.Fatalf("空 C2b3-2b 地基应可回滚: %v", err)
		}
		var version, constraintCount, capabilityColumns int
		var coordinatorOperationSelect, receiptUpdate bool
		var playbookRLS, sourceLinkRLS, tenantRLS, membershipRLS bool
		var definitionEditPolicyCount int
		if err := f.db.QueryRowContext(t.Context(), `
			SELECT
			  (SELECT max(version_id) FROM goose_db_version WHERE is_applied),
			  (SELECT count(*) FROM pg_constraint
			    WHERE conname='schedules_definition_edit_marker_requires_paused'),
			  (SELECT count(*) FROM information_schema.columns
			    WHERE table_schema=current_schema()
			      AND column_name='definition_edit_lock_capability'
			      AND table_name IN (
			        'tenants', 'memberships', 'agent_sessions',
			        'pending_actions', 'sources', 'schedule_sources'
			      )),
			  has_table_privilege('vane_edit_coordinator',
			                      'task_definition_edit_operations', 'SELECT'),
			  has_column_privilege('vane_edit_receipt',
			                       'task_definition_edit_receipts', 'status', 'UPDATE'),
			  (SELECT relrowsecurity FROM pg_class
			    WHERE oid='schedule_playbooks'::regclass),
			  (SELECT relrowsecurity FROM pg_class
			    WHERE oid='schedule_sources'::regclass),
			  (SELECT relrowsecurity FROM pg_class
			    WHERE oid='tenants'::regclass),
			  (SELECT relrowsecurity FROM pg_class
			    WHERE oid='memberships'::regclass),
			  (SELECT count(*) FROM pg_policy
			    WHERE polname IN (
			      'definition_edit_existing_visibility',
			      'definition_edit_tenant_isolation'
			    ) AND polrelid IN ('tenants'::regclass, 'memberships'::regclass))`,
		).Scan(&version, &constraintCount, &capabilityColumns, &coordinatorOperationSelect,
			&receiptUpdate, &playbookRLS, &sourceLinkRLS, &tenantRLS,
			&membershipRLS, &definitionEditPolicyCount); err != nil {
			t.Fatal(err)
		}
		if version != 33 || constraintCount != 0 || capabilityColumns != 0 ||
			coordinatorOperationSelect ||
			receiptUpdate || playbookRLS || sourceLinkRLS || tenantRLS ||
			membershipRLS || definitionEditPolicyCount != 0 {
			t.Fatalf("034 Down 留下本库权限/schema: version=%d constraint=%d lock_columns=%d coord_select=%v receipt_update=%v rls=%v/%v/%v/%v policies=%d",
				version, constraintCount, capabilityColumns, coordinatorOperationSelect, receiptUpdate,
				playbookRLS, sourceLinkRLS, tenantRLS, membershipRLS,
				definitionEditPolicyCount)
		}
		var roles int
		if err := f.db.QueryRowContext(t.Context(), `
			SELECT count(*) FROM pg_roles
			 WHERE rolname IN ('vane_edit_coordinator', 'vane_edit_receipt')`,
		).Scan(&roles); err != nil {
			t.Fatal(err)
		}
		if roles != 2 {
			t.Fatalf("共享角色应保留，实得 %d", roles)
		}
	})

	f := migration034Scratch(t)
	insertPendingEdit034(t, f, "migration-034-down", f.tenantA, f.userA, f.sessionA, f.taskA)
	if _, err := f.provider.Down(t.Context()); err == nil ||
		!strings.Contains(err.Error(), "refusing downgrade") {
		t.Fatalf("durable edit state 必须拒绝 034 Down: %v", err)
	}
	var version, operations int
	if err := f.db.QueryRowContext(t.Context(), `
		SELECT (SELECT max(version_id) FROM goose_db_version WHERE is_applied),
		       (SELECT count(*) FROM task_definition_edit_operations)`,
	).Scan(&version, &operations); err != nil {
		t.Fatal(err)
	}
	if version != 34 || operations != 1 {
		t.Fatalf("拒绝 Down 必须原子保留状态: version=%d operations=%d", version, operations)
	}
	if _, err := f.db.ExecContext(t.Context(),
		`DELETE FROM task_definition_edit_operations WHERE id='migration-034-down'`); err != nil {
		t.Fatal(err)
	}
	if _, err := f.provider.Down(t.Context()); err != nil {
		t.Fatalf("清空 durable edit state 后应可回滚: %v", err)
	}
}
