package store

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io/fs"
	"os"
	"strings"
	"testing"

	"github.com/pressly/goose/v3"
)

func TestFetchTargetCutoverMigrationIsExplicitAndIrreversible(t *testing.T) {
	raw, err := fs.ReadFile(
		migrationsFS,
		"migrations/074_fetch_target_cutover.sql",
	)
	if err != nil {
		t.Fatal(err)
	}
	sqlText := string(raw)
	for _, required := range []string{
		"-- +goose StatementBegin",
		"active task % lacks an exact approved definition head",
		"approved plan and target projection differ",
		"WHERE status IN ('pending','confirmed','blocked')",
		"unknown action version exists",
		"source action parent/child state differs",
		"DELETE FROM pending_actions WHERE execution_version IN (0,2)",
		"DROP TABLE agent_action_continuation_authority_events",
		"DROP TABLE agent_action_continuations",
		"UPDATE pending_actions p",
		"UPDATE task_creation_receipts",
		"UPDATE task_definition_edit_operations o",
		"UPDATE task_definition_edit_receipts",
		"retired receipt adapter adoption did not converge",
		"DROP TABLE subscriptions",
		"ALTER TABLE sources RENAME TO fetch_targets",
		"ALTER TABLE schedule_sources RENAME TO task_fetch_targets",
		"RENAME COLUMN source_id TO fetch_target_id",
		"GRANT SELECT (platform, capability, config)",
		"'targets'",
		"task_creation_operations_execution_version_current",
		"CHECK (execution_version = 1)",
		"regexp_replace(",
		"RENAME COLUMN approval_ref TO operation_ref",
		"RENAME COLUMN confirmed_at TO execution_started_at",
		"DROP FUNCTION IF EXISTS abort_legacy_push_batch_63_v1",
		"legacy batch 63 has a live push effect",
		"DROP ROLE %I",
		"074: fetch-target cutover is irreversible",
	} {
		if !strings.Contains(sqlText, required) {
			t.Errorf("migration 074 missing %q", required)
		}
	}
	for _, forbidden := range []string{
		"DROP TABLE sources",
		"DROP TABLE schedule_sources",
		"CASCADE",
		"CREATE VIEW sources",
		"CREATE VIEW schedule_sources",
		"CREATE VIEW subscriptions",
	} {
		if strings.Contains(sqlText, forbidden) {
			t.Errorf("migration 074 contains forbidden compatibility residue %q", forbidden)
		}
	}
}

func migration074Scratch(t *testing.T) (*sql.DB, *goose.Provider) {
	t.Helper()
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required for migration 074 integration tests")
	}
	scratchURL, drop := createScratchDB(t.Context(), t, databaseURL)
	t.Cleanup(drop)
	database, err := sql.Open("pgx", scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	dir, err := fs.Sub(migrationsFS, "migrations")
	if err != nil {
		t.Fatal(err)
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, database, dir)
	if err != nil {
		t.Fatal(err)
	}
	return database, provider
}

type migration074Fixture struct {
	tenantID  int64
	userID    int64
	sessionID int64
	taskID    string
	targetID  int64
}

func migration074Identity(t *testing.T, database *sql.DB, suffix string) migration074Fixture {
	t.Helper()
	ctx := t.Context()
	var fixture migration074Fixture
	if err := database.QueryRowContext(ctx,
		`INSERT INTO tenants DEFAULT VALUES RETURNING id`,
	).Scan(&fixture.tenantID); err != nil {
		t.Fatal(err)
	}
	if err := database.QueryRowContext(ctx,
		`INSERT INTO users (feishu_open_id,name)
		 VALUES ($1,$2) RETURNING id`,
		"migration-074-"+suffix, "migration 074 "+suffix,
	).Scan(&fixture.userID); err != nil {
		t.Fatal(err)
	}
	mustExec(ctx, t, database,
		`INSERT INTO memberships (tenant_id,user_id,role)
		 VALUES ($1,$2,'owner')`,
		fixture.tenantID, fixture.userID)
	if err := database.QueryRowContext(ctx,
		`INSERT INTO agent_sessions (tenant_id,user_id)
		 VALUES ($1,$2) RETURNING id`,
		fixture.tenantID, fixture.userID,
	).Scan(&fixture.sessionID); err != nil {
		t.Fatal(err)
	}
	fixture.taskID = "migration-074-" + suffix
	return fixture
}

func seedMigration074ApprovedTask(
	t *testing.T,
	database *sql.DB,
	fixture *migration074Fixture,
) {
	t.Helper()
	ctx := t.Context()
	targetURL := "vane://web/search?q=migration-074-" + fixture.taskID
	targetConfig := fmt.Sprintf(`{"query":%q}`, fixture.taskID)
	if err := database.QueryRowContext(ctx,
		`INSERT INTO sources (platform,capability,url,title,config)
		 VALUES ('web','search',$1,'migration 074 target',$2::jsonb)
		 RETURNING id`,
		targetURL, targetConfig,
	).Scan(&fixture.targetID); err != nil {
		t.Fatal(err)
	}
	mustExec(ctx, t, database,
		`INSERT INTO schedules (
		     id,tenant_id,user_id,nl_description,status,execution_mode
		 ) VALUES ($1,$2,$3,'migration 074 exact task','active','compiled')`,
		fixture.taskID, fixture.tenantID, fixture.userID)
	plan := fmt.Sprintf(
		`{"sources":[{"platform":"web","capability":"search",`+
			`"title":"migration 074 target","url":%q,"config":%s}]}`,
		targetURL, targetConfig,
	)
	mustExec(ctx, t, database,
		`INSERT INTO schedule_playbooks (schedule_id,content,fetch_plan)
		 VALUES ($1,'migration 074 exact task',$2::jsonb)`,
		fixture.taskID, plan)
	mustExec(ctx, t, database,
		`INSERT INTO schedule_sources (schedule_id,source_id) VALUES ($1,$2)`,
		fixture.taskID, fixture.targetID)

	payload := fmt.Sprintf(
		`{"schema_version":"vane.task-approved-definition/v1",`+
			`"tenant_id":%d,"user_id":%d,"task_id":%q,`+
			`"source_scope":"approved_plan","execution_mode":"compiled",`+
			`"fetch_plan":%s,`+
			`"sources":[{"source_id":%d,"platform":"web",`+
			`"capability":"search","title":"migration 074 target",`+
			`"url":%q,"config":%s}]}`,
		fixture.tenantID, fixture.userID, fixture.taskID, plan,
		fixture.targetID, targetURL, targetConfig,
	)
	sum := sha256.Sum256([]byte(payload))
	digest := hex.EncodeToString(sum[:])
	mustExec(ctx, t, database,
		`INSERT INTO task_approved_definition_versions (
		     tenant_id,user_id,task_id,version,schema_version,execution_mode,
		     definition_digest,payload,approval_ref
		 ) VALUES (
		     $1,$2,$3,1,'vane.task-approved-definition/v1','compiled',
		     $4,$5,'migration-074-approved'
		 )`,
		fixture.tenantID, fixture.userID, fixture.taskID,
		digest, []byte(payload))
	mustExec(ctx, t, database,
		`UPDATE schedules
		    SET approved_definition_version=1,approved_definition_digest=$4
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3`,
		fixture.taskID, fixture.tenantID, fixture.userID, digest)
}

func seedMigration074CreationV1(
	t *testing.T,
	database *sql.DB,
	fixture migration074Fixture,
) {
	t.Helper()
	mustExec(t.Context(), t, database,
		`INSERT INTO pending_actions (
		     id,tenant_id,user_id,session_id,tool_name,args,summary,status,
		     expires_at,execution_version,receipt_provider,receipt_target
		 ) VALUES (
		     'migration-074-create-v1',$1,$2,$3,'create_schedule','{}',
		     'migration 074 v1','pending',clock_timestamp()+interval '1 hour',
		     1,'agent_auto/v1','migration-074-create-v1'
		 )`,
		fixture.tenantID, fixture.userID, fixture.sessionID)
}

func seedMigration074TerminalV2(
	t *testing.T,
	database *sql.DB,
	fixture migration074Fixture,
) {
	t.Helper()
	ctx := t.Context()
	mustExec(ctx, t, database,
		`INSERT INTO pending_actions (
		     id,tenant_id,user_id,session_id,tool_name,args,summary,status,
		     expires_at,execution_version
		 ) VALUES (
		     'migration-074-source-v2',$1,$2,$3,'enable_source',
		     jsonb_build_object('source_id',$4::bigint),'retired terminal source action',
		     'cancelled',clock_timestamp()+interval '1 hour',2
		 )`,
		fixture.tenantID, fixture.userID, fixture.sessionID, fixture.targetID)
	mustExec(ctx, t, database,
		`INSERT INTO agent_action_continuations (
		     action_id,tenant_id,user_id,session_id,tool_name,source_id,
		     canonical_args,args_digest,tool_spec_version,tool_spec,
		     tool_spec_digest,tool_policy_version,tool_policy,
		     tool_policy_digest,adapter_version,success_messages,
		     success_digest,not_found_messages,not_found_digest,status
		 ) VALUES (
		     'migration-074-source-v2',$1,$2,$3,'enable_source',$4::bigint,
		     convert_to(jsonb_build_object('source_id',$4::bigint)::text,'UTF8'),
		     repeat('a',64),'vane.agent-tool-spec/v1',convert_to('{}','UTF8'),
		     repeat('b',64),'vane.agent-tool-policy/v1',convert_to('{}','UTF8'),
		     repeat('c',64),'vane.enable-source/postgres/v1',
		     convert_to('[]','UTF8'),repeat('d',64),convert_to('[]','UTF8'),
		     repeat('e',64),'cancelled'
		 )`,
		fixture.tenantID, fixture.userID, fixture.sessionID, fixture.targetID)
}

func TestMigration074RejectsActiveTaskWithoutApprovedPlan(t *testing.T) {
	database, provider := migration074Scratch(t)
	if _, err := provider.UpTo(t.Context(), 73); err != nil {
		t.Fatalf("migrate to 073: %v", err)
	}
	fixture := migration074Identity(t, database, "headless")
	mustExec(t.Context(), t, database,
		`INSERT INTO schedules (
		     id,tenant_id,user_id,nl_description,status,execution_mode
		 ) VALUES ($1,$2,$3,'headless active task','active','compiled')`,
		fixture.taskID, fixture.tenantID, fixture.userID)

	if _, err := provider.UpTo(t.Context(), 74); err == nil ||
		!strings.Contains(err.Error(),
			"lacks an exact approved definition head") {
		t.Fatalf("migration 074 accepted a headless active task: %v", err)
	}
	var oldTables bool
	mustQueryRow(t.Context(), t, database,
		`SELECT to_regclass('sources') IS NOT NULL
		    AND to_regclass('subscriptions') IS NOT NULL`,
		&oldTables)
	if !oldTables {
		t.Fatal("failed cutover performed destructive DDL")
	}
}

func TestMigration074RejectsUnknownActionVersion(t *testing.T) {
	database, provider := migration074Scratch(t)
	if _, err := provider.UpTo(t.Context(), 73); err != nil {
		t.Fatalf("migrate to 073: %v", err)
	}
	fixture := migration074Identity(t, database, "unknown-action")
	mustExec(t.Context(), t, database,
		`INSERT INTO pending_actions (
		     id,tenant_id,user_id,session_id,tool_name,args,summary,status,
		     expires_at,execution_version
		 ) VALUES (
		     'migration-074-unknown',$1,$2,$3,'retired_unknown','{}',
		     'unknown version','cancelled',
		     clock_timestamp()+interval '1 hour',3
		 )`,
		fixture.tenantID, fixture.userID, fixture.sessionID)

	if _, err := provider.UpTo(t.Context(), 74); err == nil ||
		!strings.Contains(err.Error(), "unknown action version exists") {
		t.Fatalf("migration 074 accepted an unknown action version: %v", err)
	}
}

func TestMigration074RejectsApprovedTargetSemanticDrift(t *testing.T) {
	database, provider := migration074Scratch(t)
	if _, err := provider.UpTo(t.Context(), 73); err != nil {
		t.Fatalf("migrate to 073: %v", err)
	}
	fixture := migration074Identity(t, database, "semantic-drift")
	seedMigration074ApprovedTask(t, database, &fixture)
	mustExec(t.Context(), t, database,
		`UPDATE sources
		    SET config='{"query":"drifted-after-approval"}'::jsonb
		  WHERE id=$1`,
		fixture.targetID)

	if _, err := provider.UpTo(t.Context(), 74); err == nil ||
		!strings.Contains(err.Error(),
			"approved plan and target projection differ") {
		t.Fatalf("migration 074 accepted fetch-target semantic drift: %v", err)
	}
}

func TestMigration074CleansTerminalV2AndOldConstraintNames(t *testing.T) {
	database, provider := migration074Scratch(t)
	if _, err := provider.UpTo(t.Context(), 73); err != nil {
		t.Fatalf("migrate to 073: %v", err)
	}
	fixture := migration074Identity(t, database, "exact")
	seedMigration074ApprovedTask(t, database, &fixture)
	seedMigration074CreationV1(t, database, fixture)
	seedMigration074TerminalV2(t, database, fixture)
	mustExec(t.Context(), t, database,
		`UPDATE pending_actions
		    SET receipt_provider='feishu_card_patch:retired',
		        receipt_target='om_retired'
		  WHERE id='migration-074-create-v1'`)
	mustExec(t.Context(), t, database,
		`INSERT INTO task_creation_receipts (
		     operation_id,tenant_id,user_id,session_id,provider,target,
		     provider_key,status,lease_owner,lease_until,takeover_not_before,
		     fence,attempt,next_attempt_at,payload,payload_digest,
		     session_recorded_at,session_messages_digest,failure_class
		 ) VALUES (
		     'migration-074-create-v1',$1,$2,$3,
		     'feishu_card_patch:retired','om_retired',
		     md5('migration-074-retired-receipt')::uuid,'pending',
		     'retired-worker',clock_timestamp()+interval '1 minute',
		     clock_timestamp()+interval '2 minutes',1,1,
		     clock_timestamp()+interval '1 minute',
		     convert_to('retired-card-payload','UTF8'),
		     encode(sha256(convert_to('retired-card-payload','UTF8')),'hex'),
		     clock_timestamp(),repeat('a',64),'retryable'
		 )`,
		fixture.tenantID, fixture.userID, fixture.sessionID)

	if _, err := provider.UpTo(t.Context(), 74); err != nil {
		t.Fatalf("migrate exact fixture to 074: %v", err)
	}
	var operationCount int
	mustQueryRow(t.Context(), t, database,
		`SELECT count(*) FROM task_creation_operations`,
		&operationCount)
	if operationCount != 1 {
		t.Fatalf("task_creation_operations count=%d, want only current v1", operationCount)
	}
	var v2Exists bool
	mustQueryRow(t.Context(), t, database,
		`SELECT EXISTS (
		     SELECT 1 FROM task_creation_operations WHERE execution_version<>1
		 )`,
		&v2Exists)
	if v2Exists {
		t.Fatal("retired source-action root survived the cutover")
	}
	var operationProvider, operationTarget, receiptProvider, receiptTarget string
	var receiptCheckpointCleared bool
	if err := database.QueryRowContext(t.Context(),
		`SELECT o.receipt_provider,o.receipt_target,r.provider,r.target,
		        r.payload IS NULL AND r.payload_digest=''
		        AND r.session_recorded_at IS NULL
		        AND r.session_messages_digest=''
		        AND r.lease_owner='' AND r.lease_until IS NULL
		        AND r.takeover_not_before IS NULL
		        AND r.failure_class=''
		   FROM task_creation_operations o
		   JOIN task_creation_receipts r ON r.operation_id=o.id
		  WHERE o.id='migration-074-create-v1'`).Scan(
		&operationProvider, &operationTarget,
		&receiptProvider, &receiptTarget, &receiptCheckpointCleared,
	); err != nil {
		t.Fatal(err)
	}
	if operationProvider != "agent_auto/v1" ||
		operationTarget != "migration-074-create-v1" ||
		receiptProvider != operationProvider ||
		receiptTarget != operationTarget ||
		!receiptCheckpointCleared {
		t.Fatalf("retired receipt was not adopted exactly: operation=%q/%q receipt=%q/%q cleared=%v",
			operationProvider, operationTarget,
			receiptProvider, receiptTarget, receiptCheckpointCleared)
	}
	if _, err := database.ExecContext(t.Context(),
		`UPDATE task_creation_operations
		    SET execution_version=2
		  WHERE id='migration-074-create-v1'`); err == nil {
		t.Fatal("current task creation table accepted execution_version=2")
	}

	var oldConstraintCount int
	mustQueryRow(t.Context(), t, database,
		`SELECT count(*)
		   FROM pg_constraint
		  WHERE (
		          conrelid='task_creation_operations'::regclass
		          AND conname LIKE 'pending_actions\_%' ESCAPE '\'
		        )
		     OR (
		          conrelid='fetch_targets'::regclass
		          AND conname LIKE 'sources\_%' ESCAPE '\'
		        )
		     OR (
		          conrelid='task_fetch_targets'::regclass
		          AND conname LIKE 'schedule_sources\_%' ESCAPE '\'
		        )`,
		&oldConstraintCount)
	if oldConstraintCount != 0 {
		t.Fatalf("retired constraint prefixes remain: %d", oldConstraintCount)
	}
	var coordinatorCanReadIdentity bool
	mustQueryRow(t.Context(), t, database,
		`SELECT
		    has_column_privilege(
		        'vane_edit_coordinator','fetch_targets','id','SELECT'
		    )
		    AND has_column_privilege(
		        'vane_edit_coordinator','fetch_targets','platform','SELECT'
		    )
		    AND has_column_privilege(
		        'vane_edit_coordinator','fetch_targets','capability','SELECT'
		    )
		    AND has_column_privilege(
		        'vane_edit_coordinator','fetch_targets','url','SELECT'
		    )
		    AND has_column_privilege(
		        'vane_edit_coordinator','fetch_targets','config','SELECT'
		    )`,
		&coordinatorCanReadIdentity)
	if !coordinatorCanReadIdentity {
		t.Fatal("definition edit coordinator cannot read the exact acquisition identity")
	}
}
