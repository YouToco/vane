package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/server/taskstate"
	"github.com/YouToco/vane/server/types"
)

func TestMigration132FreezesOnlyRetainedLegacyProtocolWrites(t *testing.T) {
	payload, err := migrationsFS.ReadFile(
		"migrations/132_agent_first_legacy_protocol_write_fence.sql")
	if err != nil {
		t.Fatal(err)
	}
	sqlText := string(payload)
	for _, fragment := range []string{
		"LOCK TABLE task_creation_operations IN ACCESS EXCLUSIVE MODE",
		"LOCK TABLE task_definition_edit_operations IN ACCESS EXCLUSIVE MODE",
		"agent_first_legacy_db_snapshot_v130()",
		"retained legacy protocol is not quiescent",
		"OLD.execution_version=1 AND OLD.tool_name='create_schedule'",
		"NEW.execution_version=1 AND NEW.tool_name='create_schedule'",
		"OLD.operation_protocol=1 OR NEW.operation_protocol=1",
		"agent_first_legacy_creation_root_fence_v132",
		"agent_first_legacy_creation_receipt_fence_v132",
		"agent_first_protocol1_edit_root_fence_v132",
		"agent_first_protocol1_edit_receipt_fence_v132",
		"agent_first_retention_append_fence_v132",
		"retention evidence must use the fenced append path",
		"ENABLE ALWAYS TRIGGER",
		"assert_agent_first_legacy_write_fence_v132",
		"retention evidence depends on the legacy write fence",
	} {
		if !strings.Contains(sqlText, fragment) {
			t.Errorf("migration 132 lost protocol fence %q", fragment)
		}
	}
	for _, forbidden := range []string{
		"DELETE FROM task_creation_operations",
		"DELETE FROM task_definition_edit_operations",
		"DROP TABLE task_creation_operations",
		"DROP TABLE task_definition_edit_operations",
		"PushPipelineWorkflow",
		"ALTER ROLE",
		"GRANT vane_",
	} {
		if strings.Contains(sqlText, forbidden) {
			t.Errorf("migration 132 changed retained history/runtime via %q", forbidden)
		}
	}
}

func TestMigration132LegacyFenceAndV3PassThroughPostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		requireDatabaseCapability(t)
	}
	database, provider, scratchURL, drop := migration128Scratch(t, databaseURL)
	t.Cleanup(drop)
	if _, err := provider.UpTo(t.Context(), 131); err != nil {
		t.Fatal(err)
	}
	st, err := New(t.Context(), scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)

	userID, tenantID := migration129Identity(t, database, "migration-132")
	var sessionID int64
	if err := database.QueryRowContext(t.Context(), `
		INSERT INTO agent_sessions(tenant_id,user_id) VALUES($1,$2) RETURNING id`,
		tenantID, userID).Scan(&sessionID); err != nil {
		t.Fatal(err)
	}
	const taskID = "migration-132-task"
	definitionPayload := []byte(`{"schema":"migration-132-approved/v1"}`)
	definitionSum := sha256.Sum256(definitionPayload)
	definitionDigest := hex.EncodeToString(definitionSum[:])
	if _, err := database.ExecContext(t.Context(), `
		INSERT INTO schedules(id,tenant_id,user_id,nl_description,status)
		VALUES($1,$2,$3,'migration 132','paused')`, taskID, tenantID, userID); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), `
		INSERT INTO task_approved_definition_versions(
		 tenant_id,user_id,task_id,version,schema_version,execution_mode,
		 definition_digest,payload,operation_ref)
		VALUES($1,$2,$3,1,'migration-132/v1','discover_at_run',$4,$5,
		 'migration-132-approval')`, tenantID, userID, taskID, definitionDigest,
		definitionPayload); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), `
		UPDATE schedules SET execution_mode='discover_at_run',
		 approved_definition_version=1,approved_definition_digest=$2
		 WHERE id=$1`, taskID, definitionDigest); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), `
		INSERT INTO research_v3_delivery_authorities(
		 tenant_id,user_id,task_id,generation,definition_version,
		 definition_digest,target_action_digest,action_authorization_digest,
		 status,enabled_at)
		VALUES($1,$2,$3,1,1,$4,$5,$6,'enabled',clock_timestamp())`,
		tenantID, userID, taskID, definitionDigest, strings.Repeat("b", 64),
		strings.Repeat("c", 64)); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), `
		INSERT INTO task_creation_operations(
		 id,tenant_id,user_id,tool_name,args,summary,status,expires_at,
		 execution_version,phase,receipt_provider,receipt_target,tombstoned_at)
		VALUES('migration-132-v1',$1,$2,'create_schedule','{}','terminal',
		 'expired',clock_timestamp()-interval '1 hour',1,'expired',
		 'agent_auto/v1','migration-132-v1',clock_timestamp())`, tenantID, userID); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), `
		INSERT INTO task_creation_receipts(
		 operation_id,tenant_id,user_id,provider,target,provider_key,status,
		 next_attempt_at,provider_message_id,sent_at)
		VALUES('migration-132-v1',$1,$2,'agent_auto/v1','migration-132-v1',
		 md5('migration-132-v1')::uuid,'suppressed',clock_timestamp(),
		 'legacy-suppressed',clock_timestamp())`, tenantID, userID); err != nil {
		t.Fatal(err)
	}

	editArgs := pendingEditArgs034(
		"migration-132-edit-v1", tenantID, userID, sessionID, taskID)
	if _, err := database.ExecContext(t.Context(), `
		INSERT INTO task_definition_edit_operations (
		 id,tenant_id,user_id,target_tenant_id,target_user_id,task_id,session_id,
		 operation_ref,expires_at,original_status,base_definition_version,
		 base_definition_digest,base_definition,target_definition_version,
		 target_definition_digest,target_definition,canonical_proposal,proposal_digest,
		 prepared_edit,prepared_edit_digest,base_snapshot,base_snapshot_digest)
		VALUES($1,$2,$3,$2,$3,$4,$5,$6,clock_timestamp()+interval '1 day','paused',
		 1,$7,$8,2,$9,$10,$11,$12,$13,$14,$15,$16)`, editArgs...); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), `
		UPDATE task_definition_edit_operations
		   SET status='cancelled',phase='proposal_sealed',
		       tombstoned_at=clock_timestamp()
		 WHERE id='migration-132-edit-v1'`); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), `
		INSERT INTO task_definition_edit_receipts(
		 operation_id,tenant_id,user_id,session_id,provider,target,provider_key,
		 status,failure_class,sent_at,provider_message_id)
		VALUES('migration-132-edit-v1',$1,$2,$3,'','',md5('migration-132-edit-v1')::uuid,
		 'suppressed','target_unbound',clock_timestamp(),'target-unbound-suppressed')`,
		tenantID, userID, sessionID); err != nil {
		t.Fatal(err)
	}
	preFenceInput := agentFirstRetentionTestInput(
		AgentFirstRetentionPhaseBaseline, "", time.Now().UTC())
	preFenceBaseline, err := st.AppendAgentFirstRetentionAttestation(
		t.Context(), preFenceInput)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := provider.UpTo(t.Context(), 132); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AssertAgentFirstLegacyWriteFence(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := callServerRuntimeProvisioner(
		t.Context(), scratchURL, "provision_vane_server_runtime_v129",
	); err != nil {
		t.Fatalf("provision schema-132 runtime: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = callServerRuntimeProvisioner(
			ctx, scratchURL, "deprovision_vane_server_runtime_v129")
	})
	const runtimePassword = "migration-132-server-runtime-password"
	if _, err := database.ExecContext(t.Context(),
		`ALTER ROLE vane_server_runtime LOGIN PASSWORD '`+runtimePassword+`'`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, _ = database.ExecContext(ctx,
			`ALTER ROLE vane_server_runtime NOLOGIN PASSWORD NULL`)
	})
	runtimeURL := serverRuntimeTestURL(t, scratchURL, runtimePassword)
	// Current binaries require the migration-138 capability set and therefore
	// deliberately reject a schema-132 runtime. This historical migration test
	// enters only the schema-132 vane_app capability; the current full runtime
	// boundary is exercised separately on the current schema.
	runtimePool, err := newStorePool(
		t.Context(), runtimeURL,
		func(ctx context.Context, conn *pgx.Conn) error {
			_, err := conn.Exec(ctx, `SET ROLE vane_app`)
			return err
		},
	)
	if err != nil {
		t.Fatalf("open schema-132 application capability: %v", err)
	}
	runtimeStore := newStore(runtimePool, nil)
	if _, err := runtimeStore.AssertAgentFirstLegacyWriteFence(t.Context()); err != nil {
		runtimeStore.Close()
		t.Fatalf("schema-132 runtime fence assertion: %v", err)
	}
	runtimeStore.Close()
	var originalRootFunction string
	if err := database.QueryRowContext(t.Context(), `SELECT pg_get_functiondef(
		'public.reject_legacy_creation_root_write_v132()'::regprocedure)`).Scan(
		&originalRootFunction); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), `CREATE OR REPLACE FUNCTION
		public.reject_legacy_creation_root_write_v132() RETURNS TRIGGER
		LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog,public
		AS $$ BEGIN RETURN NEW; END $$`); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AssertAgentFirstLegacyWriteFence(t.Context()); err == nil {
		t.Fatal("replacement legacy fence function body passed catalog descriptor")
	}
	if _, err := database.ExecContext(t.Context(), originalRootFunction); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AssertAgentFirstLegacyWriteFence(t.Context()); err != nil {
		t.Fatalf("restored legacy fence descriptor failed: %v", err)
	}
	var originalDescriptorFunction string
	if err := database.QueryRowContext(t.Context(), `SELECT pg_get_functiondef(
		'public.agent_first_legacy_write_fence_descriptor_v132()'::regprocedure)`).Scan(
		&originalDescriptorFunction); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), `CREATE OR REPLACE FUNCTION
		public.agent_first_legacy_write_fence_descriptor_v132() RETURNS TEXT
		LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog,public
		AS $$ SELECT descriptor_digest FROM
		public.agent_first_legacy_protocol_write_fence_v132 WHERE singleton $$`); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AssertAgentFirstLegacyWriteFence(t.Context()); err == nil ||
		!strings.Contains(err.Error(), "verifier catalog drifted") {
		t.Fatalf("self-approving database descriptor bypassed binary bootstrap: %v", err)
	}
	if _, err := database.ExecContext(t.Context(), originalDescriptorFunction); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AssertAgentFirstLegacyWriteFence(t.Context()); err != nil {
		t.Fatalf("restored descriptor verifier failed: %v", err)
	}
	var originalAssertionFunction string
	if err := database.QueryRowContext(t.Context(), `SELECT pg_get_functiondef(
		'public.assert_agent_first_legacy_write_fence_v132()'::regprocedure)`).Scan(
		&originalAssertionFunction); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), `CREATE OR REPLACE FUNCTION
		public.assert_agent_first_legacy_write_fence_v132()
		RETURNS TABLE(installed_at TIMESTAMPTZ,
		 preexisting_attestation_max_id BIGINT,descriptor_digest TEXT)
		LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog,public AS $$
		SELECT installed_at,preexisting_attestation_max_id,descriptor_digest
		FROM public.agent_first_legacy_protocol_write_fence_v132 WHERE singleton $$`); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AssertAgentFirstLegacyWriteFence(t.Context()); err == nil ||
		!strings.Contains(err.Error(), "verifier catalog drifted") {
		t.Fatalf("self-approving assertion bypassed binary bootstrap: %v", err)
	}
	if _, err := database.ExecContext(t.Context(), originalAssertionFunction); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AssertAgentFirstLegacyWriteFence(t.Context()); err != nil {
		t.Fatalf("restored assertion verifier failed: %v", err)
	}
	var originalSnapshotFunction string
	if err := database.QueryRowContext(t.Context(), `SELECT pg_get_functiondef(
		'public.agent_first_legacy_db_snapshot_v130()'::regprocedure)`).Scan(
		&originalSnapshotFunction); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), `CREATE OR REPLACE FUNCTION
		public.agent_first_legacy_db_snapshot_v130() RETURNS BYTEA
		LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog,public AS $$
		SELECT '{}'::text::bytea $$`); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AssertAgentFirstLegacyWriteFence(t.Context()); err == nil {
		t.Fatal("replaced v130 semantic snapshot passed transitive descriptor")
	}
	if _, err := database.ExecContext(t.Context(), originalSnapshotFunction); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AssertAgentFirstLegacyWriteFence(t.Context()); err != nil {
		t.Fatalf("restored v130 snapshot verifier failed: %v", err)
	}
	for name, statements := range map[string][2]string{
		"creation delete ACL": {
			`GRANT DELETE ON task_creation_receipts TO vane_app`,
			`REVOKE DELETE ON task_creation_receipts FROM vane_app`,
		},
		"edit discriminator ACL": {
			`REVOKE SELECT(operation_protocol) ON task_definition_edit_operations FROM vane_edit_receipt`,
			`GRANT SELECT(operation_protocol) ON task_definition_edit_operations TO vane_edit_receipt`,
		},
		"immutable epoch trigger": {
			`ALTER TABLE agent_first_legacy_protocol_write_fence_v132 DISABLE TRIGGER agent_first_legacy_write_fence_immutable_v132`,
			`ALTER TABLE agent_first_legacy_protocol_write_fence_v132 ENABLE ALWAYS TRIGGER agent_first_legacy_write_fence_immutable_v132`,
		},
		"retained ledger immutable trigger": {
			`ALTER TABLE agent_first_retention_attestation_events DISABLE TRIGGER agent_first_retention_history_immutable_v130`,
			`ALTER TABLE agent_first_retention_attestation_events ENABLE ALWAYS TRIGGER agent_first_retention_history_immutable_v130`,
		},
		"definition edit force RLS": {
			`ALTER TABLE task_definition_edit_operations FORCE ROW LEVEL SECURITY`,
			`ALTER TABLE task_definition_edit_operations NO FORCE ROW LEVEL SECURITY`,
		},
		"schedule force RLS": {
			`ALTER TABLE schedules FORCE ROW LEVEL SECURITY`,
			`ALTER TABLE schedules NO FORCE ROW LEVEL SECURITY`,
		},
		"delivery authority force RLS": {
			`ALTER TABLE research_v3_delivery_authorities FORCE ROW LEVEL SECURITY`,
			`ALTER TABLE research_v3_delivery_authorities NO FORCE ROW LEVEL SECURITY`,
		},
		"schedule policy": {
			`ALTER POLICY tenant_isolation ON schedules RENAME TO tenant_isolation_drifted`,
			`ALTER POLICY tenant_isolation_drifted ON schedules RENAME TO tenant_isolation`,
		},
		"schedule column ACL": {
			`GRANT UPDATE(nl_description) ON schedules TO vane_edit_receipt`,
			`REVOKE UPDATE(nl_description) ON schedules FROM vane_edit_receipt`,
		},
	} {
		t.Run("catalog drift "+name, func(t *testing.T) {
			if _, err := database.ExecContext(t.Context(), statements[0]); err != nil {
				t.Fatal(err)
			}
			if _, err := st.AssertAgentFirstLegacyWriteFence(t.Context()); err == nil {
				t.Fatalf("%s passed catalog descriptor", name)
			}
			if _, err := database.ExecContext(t.Context(), statements[1]); err != nil {
				t.Fatal(err)
			}
			if _, err := st.AssertAgentFirstLegacyWriteFence(t.Context()); err != nil {
				t.Fatalf("restore %s: %v", name, err)
			}
		})
	}
	var enabled int
	if err := database.QueryRowContext(t.Context(), `
		SELECT count(*) FROM pg_trigger
		 WHERE tgname LIKE 'agent_first_%_fence_v132' AND tgenabled='A'`).Scan(&enabled); err != nil {
		t.Fatal(err)
	}
	if enabled != 5 {
		t.Fatalf("enabled legacy fences=%d want 5", enabled)
	}
	v3OperationID := "migration-132-v3-store"
	v3Definition := nativeV3EditDefinition110(t, tenantID, userID,
		nativeResearchTaskIDV3Test(tenantID, userID, v3OperationID),
		"schema 132 V3", "exercise the production V3 creation entrypoint")
	v3Payload, err := taskstate.EncodeApprovedDefinitionV3(v3Definition)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.CreateResearchTaskCreationOperationV3(t.Context(),
		types.CreateResearchTaskCreationOperationV3Params{
			ID: v3OperationID, TenantID: tenantID, UserID: userID,
			Args: v3Payload, Summary: v3Definition.TaskName,
			ExpiresAt: time.Now().Add(time.Hour),
		}); err != nil {
		t.Fatalf("schema-132 production V3 creation entrypoint: %v", err)
	}
	postFenceDirect := agentFirstRetentionTestInput(
		AgentFirstRetentionPhaseBaseline, "", time.Now().UTC())
	if _, err := st.AppendAgentFirstRetentionAttestation(
		t.Context(), postFenceDirect); err == nil ||
		!strings.Contains(err.Error(), "must use the fenced append path") {
		t.Fatalf("retained v130 append bypassed the v132 snapshot CAS: %v", err)
	}
	if _, err := st.LoadAgentFirstRetentionAttestation(
		t.Context(), preFenceInput); !errors.Is(err, ErrAgentFirstRetentionAttestationStale) {
		t.Fatalf("pre-fence baseline remained adoptable: %v", err)
	}
	snapshot, err := st.ReadAgentFirstRetentionAuditSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	preFencePrepared := agentFirstRetentionTestInput(
		AgentFirstRetentionPhasePrepared, preFenceBaseline.PayloadDigest, time.Now().UTC())
	if _, err := st.AppendAgentFirstRetentionAttestationV132(
		t.Context(), preFencePrepared, snapshot.LegacyDBSnapshotDigest); err == nil ||
		!strings.Contains(err.Error(), "predates legacy write fence") {
		t.Fatalf("prepared accepted a pre-fence parent: %v", err)
	}

	for name, statement := range map[string]string{
		"new creation": `INSERT INTO task_creation_operations(
		 id,tenant_id,user_id,tool_name,args,summary,status,expires_at,execution_version)
		 VALUES('migration-132-new-v1',` + itoa(tenantID) + `,` + itoa(userID) + `,
		 'create_schedule','{}','blocked','pending',clock_timestamp()+interval '1 hour',1)`,
		"creation update": `UPDATE task_creation_operations SET summary='blocked'
		 WHERE id='migration-132-v1'`,
		"creation receipt": `UPDATE task_creation_receipts SET updated_at=clock_timestamp()
		 WHERE operation_id='migration-132-v1'`,
		"edit update": `UPDATE task_definition_edit_operations SET error_message='blocked'
		 WHERE id='migration-132-edit-v1'`,
		"edit receipt": `UPDATE task_definition_edit_receipts SET updated_at=clock_timestamp()
		 WHERE operation_id='migration-132-edit-v1'`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := database.ExecContext(t.Context(), statement); err == nil ||
				!postgresCodeIs(err, "55000") {
				t.Fatalf("legacy write was not fenced: %v", err)
			}
		})
	}
	replicaTx, err := database.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := replicaTx.ExecContext(t.Context(),
		`SET LOCAL session_replication_role=replica`); err != nil {
		_ = replicaTx.Rollback()
		t.Fatal(err)
	}
	if _, err := replicaTx.ExecContext(t.Context(), `INSERT INTO task_creation_operations(
		id,tenant_id,user_id,tool_name,args,summary,status,expires_at,execution_version)
		VALUES('migration-132-replica-v1',$1,$2,'create_schedule','{}','blocked',
		'pending',clock_timestamp()+interval '1 hour',1)`, tenantID, userID); err == nil ||
		!postgresCodeIs(err, "55000") {
		_ = replicaTx.Rollback()
		t.Fatalf("ENABLE ALWAYS fence was bypassed in replica mode: %v", err)
	}
	if err := replicaTx.Rollback(); err != nil {
		t.Fatal(err)
	}

	if _, err := database.ExecContext(t.Context(), `
		INSERT INTO task_creation_operations(
		 id,tenant_id,user_id,tool_name,args,summary,status,expires_at,execution_version)
		VALUES('migration-132-v3',$1,$2,'manage_tasks','{}','v3','pending',
		 clock_timestamp()+interval '1 hour',2)`, tenantID, userID); err != nil {
		t.Fatalf("V3 creation root was fenced: %v", err)
	}
	if _, err := database.ExecContext(t.Context(), `
		UPDATE task_creation_operations SET summary='v3-pass'
		 WHERE id='migration-132-v3'`); err != nil {
		t.Fatalf("V3 creation update was fenced: %v", err)
	}
	for name, statement := range map[string]string{
		"creation legacy to current": `UPDATE task_creation_operations
		 SET execution_version=2,tool_name='manage_tasks' WHERE id='migration-132-v1'`,
		"creation current to legacy": `UPDATE task_creation_operations
		 SET execution_version=1,tool_name='create_schedule' WHERE id='migration-132-v3'`,
		"creation receipt reparent": `UPDATE task_creation_receipts
		 SET operation_id='migration-132-v3' WHERE operation_id='migration-132-v1'`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := database.ExecContext(t.Context(), statement); err == nil ||
				!postgresCodeIs(err, "55000") {
				t.Fatalf("legacy identity escape was not fenced: %v", err)
			}
		})
	}
	if _, err := database.ExecContext(t.Context(), `
		INSERT INTO task_definition_edit_operations(
		 id,tenant_id,user_id,target_tenant_id,target_user_id,task_id,session_id,
		 operation_ref,status,phase,expires_at,execution_started_at,original_status,
		 base_definition_version,base_definition_digest,base_definition,
		 target_definition_version,target_definition_digest,target_definition,
		 canonical_proposal,proposal_digest,prepared_edit,prepared_edit_digest,
		 base_snapshot,base_snapshot_digest,pause_snapshot,pause_snapshot_digest,
		 apply_snapshot,apply_snapshot_digest,restore_snapshot,restore_snapshot_digest,
		 lease_owner,lease_until,takeover_not_before,fence,attempt,receipt_provider,
		 receipt_target,result,error_code,error_message,created_at,updated_at,tombstoned_at,
		 operation_protocol)
		SELECT 'migration-132-edit-v3',tenant_id,user_id,target_tenant_id,target_user_id,
		 task_id,session_id,operation_ref||'-v3',status,phase,expires_at,execution_started_at,
		 original_status,base_definition_version,base_definition_digest,base_definition,
		 target_definition_version,target_definition_digest,target_definition,
		 canonical_proposal,proposal_digest,prepared_edit,prepared_edit_digest,
		 base_snapshot,base_snapshot_digest,pause_snapshot,pause_snapshot_digest,
		 apply_snapshot,apply_snapshot_digest,restore_snapshot,restore_snapshot_digest,
		 lease_owner,lease_until,takeover_not_before,fence,attempt,receipt_provider,
		 receipt_target,result,error_code,error_message,created_at,updated_at,tombstoned_at,3
		FROM task_definition_edit_operations WHERE id='migration-132-edit-v1'`); err != nil {
		t.Fatalf("protocol-3 edit root was fenced: %v", err)
	}
	if _, err := database.ExecContext(t.Context(), `
		UPDATE task_definition_edit_operations SET error_message='v3-pass'
		 WHERE id='migration-132-edit-v3'`); err != nil {
		t.Fatalf("protocol-3 edit update was fenced: %v", err)
	}
	if _, err := database.ExecContext(t.Context(), `
		INSERT INTO task_definition_edit_receipts(
		 operation_id,tenant_id,user_id,session_id,provider,target,provider_key,
		 status,failure_class,sent_at,provider_message_id)
		VALUES('migration-132-edit-v3',$1,$2,$3,'','',md5('migration-132-edit-v3')::uuid,
		 'suppressed','target_unbound',clock_timestamp(),'target-unbound-suppressed')`,
		tenantID, userID, sessionID); err != nil {
		t.Fatalf("protocol-3 receipt was fenced: %v", err)
	}
	for name, statement := range map[string]string{
		"edit legacy to current": `UPDATE task_definition_edit_operations
		 SET operation_protocol=3 WHERE id='migration-132-edit-v1'`,
		"edit current to legacy": `UPDATE task_definition_edit_operations
		 SET operation_protocol=1 WHERE id='migration-132-edit-v3'`,
		"edit receipt reparent": `UPDATE task_definition_edit_receipts
		 SET operation_id='migration-132-edit-v3' WHERE operation_id='migration-132-edit-v1'`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := database.ExecContext(t.Context(), statement); err == nil ||
				!postgresCodeIs(err, "55000") {
				t.Fatalf("legacy edit identity escape was not fenced: %v", err)
			}
		})
	}
	tx, err := database.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `SELECT set_config('app.tenant_id',$1,true)`,
		itoa(tenantID)); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `SET LOCAL ROLE vane_edit_receipt`); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `UPDATE task_definition_edit_receipts
		 SET updated_at=clock_timestamp() WHERE operation_id='migration-132-edit-v3'`); err != nil {
		_ = tx.Rollback()
		t.Fatalf("real V3 receipt dispatcher was broken by fence: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	if _, err := database.ExecContext(t.Context(), `
		ALTER TABLE task_creation_operations
		 DISABLE TRIGGER agent_first_legacy_creation_root_fence_v132`); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AssertAgentFirstLegacyWriteFence(t.Context()); err == nil {
		t.Fatal("disabled legacy fence passed collector assertion")
	}
	if _, err := database.ExecContext(t.Context(), `
		ALTER TABLE task_creation_operations
		 ENABLE ALWAYS TRIGGER agent_first_legacy_creation_root_fence_v132`); err != nil {
		t.Fatal(err)
	}
	deleteTx, err := database.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := deleteTx.ExecContext(t.Context(), `SET LOCAL ROLE vane_app`); err != nil {
		_ = deleteTx.Rollback()
		t.Fatal(err)
	}
	if _, err := deleteTx.ExecContext(t.Context(), `
		DELETE FROM task_creation_receipts WHERE operation_id='migration-132-v1'`); err == nil ||
		!postgresCodeIs(err, "42501") {
		_ = deleteTx.Rollback()
		t.Fatalf("server runtime could delete retained creation receipt: %v", err)
	}
	if err := deleteTx.Rollback(); err != nil {
		t.Fatal(err)
	}
	// PurgeTenant is the current-schema operator contract. Retain the exact
	// migration-132 fence assertions above, then advance this same retained
	// history to the current schema before exercising the current purge list.
	if _, err := database.ExecContext(t.Context(),
		`ALTER ROLE vane_server_runtime NOLOGIN PASSWORD NULL`); err != nil {
		t.Fatal(err)
	}
	if err := callServerRuntimeProvisioner(
		t.Context(), scratchURL, "deprovision_vane_server_runtime_v129",
	); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Up(t.Context()); err != nil {
		t.Fatal(err)
	}

	dryRun, err := st.PurgeTenant(t.Context(), tenantID, true)
	if err != nil {
		t.Fatalf("schema-132 tenant purge dry-run: %v", err)
	}
	for _, table := range []string{"task_creation_operations", "task_creation_receipts",
		"task_definition_edit_operations", "task_definition_edit_receipts"} {
		if dryRun.Rows[table] == 0 {
			t.Fatalf("schema-132 dry-run missed retained table %s", table)
		}
	}
	if _, err := st.PurgeTenant(t.Context(), tenantID, false); err != nil {
		t.Fatalf("schema-132 tenant purge was fenced: %v", err)
	}
	if _, err := provider.DownTo(t.Context(), 131); err != nil {
		t.Fatalf("empty evidence should allow 132 down: %v", err)
	}
	var creationDelete, receiptDelete, editProtocolRead bool
	if err := database.QueryRowContext(t.Context(), `SELECT
		has_table_privilege('vane_app','public.task_creation_operations','DELETE'),
		has_table_privilege('vane_app','public.task_creation_receipts','DELETE'),
		has_column_privilege('vane_edit_receipt',
		 'public.task_definition_edit_operations','operation_protocol','SELECT')`).Scan(
		&creationDelete, &receiptDelete, &editProtocolRead); err != nil {
		t.Fatal(err)
	}
	if !creationDelete || !receiptDelete || editProtocolRead {
		t.Fatalf("schema-131 ACLs not exactly restored: creation_delete=%t receipt_delete=%t edit_protocol_read=%t",
			creationDelete, receiptDelete, editProtocolRead)
	}
}

func TestMigration132FencedAppendCASPostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		requireDatabaseCapability(t)
	}
	database, provider, scratchURL, drop := migration128Scratch(t, databaseURL)
	t.Cleanup(drop)
	if _, err := provider.UpTo(t.Context(), 132); err != nil {
		t.Fatal(err)
	}
	st, err := New(t.Context(), scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	fence, err := st.AssertAgentFirstLegacyWriteFence(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := st.ReadAgentFirstRetentionAuditSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	input := agentFirstRetentionTestInput(
		AgentFirstRetentionPhaseBaseline, "", time.Now().UTC())
	if _, err := st.AppendAgentFirstRetentionAttestationV132(
		t.Context(), input, strings.Repeat("9", 64)); err == nil ||
		!strings.Contains(err.Error(), "snapshot drifted") {
		t.Fatalf("fenced append accepted wrong expected snapshot: %v", err)
	}
	event, err := st.AppendAgentFirstRetentionAttestationV132(
		t.Context(), input, snapshot.LegacyDBSnapshotDigest)
	if err != nil {
		t.Fatal(err)
	}
	if event.ID <= fence.PreexistingAttestationMaxID || event.IssuedAt.Before(fence.InstalledAt) {
		t.Fatalf("post-fence baseline=%+v fence=%+v", event, fence)
	}
	if _, err := st.LoadAgentFirstRetentionAttestationV132(
		t.Context(), input, snapshot.LegacyDBSnapshotDigest); err != nil {
		t.Fatalf("fenced exact adoption failed: %v", err)
	}
	userID, tenantID := migration129Identity(t, database, "migration-132-adoption")
	if _, err := database.ExecContext(t.Context(), `INSERT INTO schedules(
		id,tenant_id,user_id,nl_description,status)
		VALUES('migration-132-adoption-drift',$1,$2,'drift','paused')`,
		tenantID, userID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.LoadAgentFirstRetentionAttestationV132(
		t.Context(), input, snapshot.LegacyDBSnapshotDigest); !errors.Is(
		err, ErrAgentFirstRetentionAttestationStale) {
		t.Fatalf("fenced adoption accepted database drift: %v", err)
	}
	if _, err := provider.DownTo(t.Context(), 131); err == nil ||
		!strings.Contains(err.Error(), "evidence depends") {
		t.Fatalf("post-fence evidence did not block 132 down: %v", err)
	}
}

func TestMigration132LostResponseAdoptionSeesAppendCommittedAfterLockWaitPostgres(
	t *testing.T,
) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		requireDatabaseCapability(t)
	}
	database, provider, scratchURL, drop := migration128Scratch(t, databaseURL)
	t.Cleanup(drop)
	if _, err := provider.UpTo(t.Context(), 132); err != nil {
		t.Fatal(err)
	}
	st, err := New(t.Context(), scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	snapshot, err := st.ReadAgentFirstRetentionAuditSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	input := agentFirstRetentionTestInput(
		AgentFirstRetentionPhaseBaseline, "", time.Now().UTC())

	appendTx, err := database.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = appendTx.Rollback() }()
	if _, err := appendTx.ExecContext(t.Context(),
		`SELECT pg_advisory_xact_lock(6215335020355474130)`); err != nil {
		t.Fatal(err)
	}
	var descriptor string
	if err := appendTx.QueryRowContext(t.Context(), `SELECT descriptor_digest
		FROM public.agent_first_legacy_protocol_write_fence_v132 WHERE singleton`).Scan(
		&descriptor); err != nil {
		t.Fatal(err)
	}
	if _, err := appendTx.ExecContext(t.Context(), `SELECT set_config(
		'app.agent_first_retention_fence_v132',$1,true)`, descriptor); err != nil {
		t.Fatal(err)
	}
	var inserted int
	if err := appendTx.QueryRowContext(t.Context(), `SELECT count(*) FROM
		public.append_agent_first_retention_attestation_v130(
		 $1,NULL,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)`,
		string(input.Phase), input.TemporalClusterID, input.TemporalNamespace,
		input.TemporalNamespaceID, input.RetentionSeconds,
		string(input.HistoryArchivalState), input.HistoryArchiveURIDigest,
		string(input.VisibilityArchivalState), input.VisibilityArchiveURIDigest,
		input.TemporalServerWitness, input.WorkflowInventoryDigest,
		input.ScheduleInventoryDigest, input.ArchiveInventoryDigest,
		input.TemporalEvidenceDigest, input.SourceRevision, input.DeployDigest).Scan(
		&inserted); err != nil {
		t.Fatal(err)
	}
	if inserted != 1 {
		t.Fatalf("uncommitted response-loss fixture rows=%d", inserted)
	}

	type adoptionResult struct {
		event *AgentFirstRetentionAttestationEvent
		err   error
	}
	result := make(chan adoptionResult, 1)
	go func() {
		event, err := st.LoadAgentFirstRetentionAttestationV132(
			t.Context(), input, snapshot.LegacyDBSnapshotDigest)
		result <- adoptionResult{event: event, err: err}
	}()
	waitForBlockedMigration132Query(t, database, "pg_advisory_xact_lock(6215335020355474130)")
	if err := appendTx.Commit(); err != nil {
		t.Fatal(err)
	}
	select {
	case adopted := <-result:
		if adopted.err != nil || adopted.event == nil ||
			adopted.event.TemporalEvidenceDigest != input.TemporalEvidenceDigest {
			t.Fatalf("post-wait adoption=%+v", adopted)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("adoption remained blocked after append commit")
	}
}

func TestMigration132AdoptionRejectsBusyReverseOrderV3WriterWithoutDeadlockPostgres(
	t *testing.T,
) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		requireDatabaseCapability(t)
	}
	database, provider, scratchURL, drop := migration128Scratch(t, databaseURL)
	t.Cleanup(drop)
	if _, err := provider.UpTo(t.Context(), 132); err != nil {
		t.Fatal(err)
	}
	st, err := New(t.Context(), scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	userID, tenantID := migration129Identity(t, database, "migration-132-lock-order")
	if _, err := database.ExecContext(t.Context(), `INSERT INTO schedules(
		id,tenant_id,user_id,nl_description,status)
		VALUES('migration-132-lock-schedule',$1,$2,'before','paused')`,
		tenantID, userID); err != nil {
		t.Fatal(err)
	}
	definitionPayload := []byte(`{"schema":"migration-132-lock/v1"}`)
	definitionSum := sha256.Sum256(definitionPayload)
	definitionDigest := hex.EncodeToString(definitionSum[:])
	if _, err := database.ExecContext(t.Context(), `INSERT INTO
		task_approved_definition_versions(
		 tenant_id,user_id,task_id,version,schema_version,execution_mode,
		 definition_digest,payload,operation_ref)
		VALUES($1,$2,'migration-132-lock-schedule',1,'migration-132-lock/v1',
		 'discover_at_run',$3,$4,'migration-132-lock-approval')`,
		tenantID, userID, definitionDigest, definitionPayload); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), `UPDATE schedules
		SET execution_mode='discover_at_run',approved_definition_version=1,
		 approved_definition_digest=$1 WHERE id='migration-132-lock-schedule'`,
		definitionDigest); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), `INSERT INTO
		research_v3_delivery_authorities(
		 tenant_id,user_id,task_id,generation,definition_version,
		 definition_digest,target_action_digest,action_authorization_digest,
		 status,enabled_at)
		VALUES($1,$2,'migration-132-lock-schedule',1,1,$3,$4,$5,'enabled',
		 clock_timestamp())`, tenantID, userID, definitionDigest,
		strings.Repeat("b", 64), strings.Repeat("c", 64)); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), `INSERT INTO task_creation_operations(
		id,tenant_id,user_id,tool_name,args,summary,status,expires_at,execution_version)
		VALUES('migration-132-lock-v3',$1,$2,'manage_tasks','{}','before','pending',
		clock_timestamp()+interval '1 hour',2)`, tenantID, userID); err != nil {
		t.Fatal(err)
	}
	snapshot, err := st.ReadAgentFirstRetentionAuditSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	input := agentFirstRetentionTestInput(
		AgentFirstRetentionPhaseBaseline, "", time.Now().UTC())
	if _, err := st.AppendAgentFirstRetentionAttestationV132(
		t.Context(), input, snapshot.LegacyDBSnapshotDigest); err != nil {
		t.Fatal(err)
	}

	writer, err := database.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = writer.Rollback() }()
	if _, err := writer.ExecContext(t.Context(), `UPDATE research_v3_delivery_authorities
		SET status='revoked',revoked_at=clock_timestamp()
		 WHERE tenant_id=$1 AND user_id=$2
		 AND task_id='migration-132-lock-schedule'`, tenantID, userID); err != nil {
		t.Fatal(err)
	}
	assertAuditBusy := func(label string) {
		t.Helper()
		appendCtx, appendCancel := context.WithTimeout(t.Context(), 2*time.Second)
		_, appendErr := st.AppendAgentFirstRetentionAttestationV132(
			appendCtx, input, snapshot.LegacyDBSnapshotDigest)
		appendCancel()
		if !postgresCodeIs(appendErr, "55P03") {
			t.Fatalf("busy %s did not fail append without waiting: %v", label, appendErr)
		}
		loadCtx, loadCancel := context.WithTimeout(t.Context(), 2*time.Second)
		_, loadErr := st.LoadAgentFirstRetentionAttestationV132(
			loadCtx, input, snapshot.LegacyDBSnapshotDigest)
		loadCancel()
		if !postgresCodeIs(loadErr, "55P03") {
			t.Fatalf("busy %s did not fail adoption without waiting: %v", label, loadErr)
		}
	}
	assertAuditBusy("authority")
	updateCtx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	if _, err := writer.ExecContext(updateCtx, `UPDATE schedules
		SET status='active' WHERE id='migration-132-lock-schedule'`); err != nil {
		cancel()
		t.Fatalf("failed adoption retained schedule lock against authority-first V3 writer: %v", err)
	}
	cancel()
	if err := writer.Rollback(); err != nil {
		t.Fatal(err)
	}

	reverseWriter, err := database.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reverseWriter.Rollback() }()
	if _, err := reverseWriter.ExecContext(t.Context(), `UPDATE schedules
		SET status='active' WHERE id='migration-132-lock-schedule'`); err != nil {
		t.Fatal(err)
	}
	assertAuditBusy("schedule")
	reverseCtx, reverseCancel := context.WithTimeout(t.Context(), 2*time.Second)
	if _, err := reverseWriter.ExecContext(reverseCtx, `UPDATE research_v3_delivery_authorities
		SET status='revoked',revoked_at=clock_timestamp()
		 WHERE tenant_id=$1 AND user_id=$2
		 AND task_id='migration-132-lock-schedule'`, tenantID, userID); err != nil {
		reverseCancel()
		t.Fatalf("failed audit retained authority lock against schedule-first V3 writer: %v", err)
	}
	reverseCancel()
	if err := reverseWriter.Commit(); err != nil {
		t.Fatal(err)
	}
}

func waitForBlockedMigration132Query(t *testing.T, database *sql.DB, fragment string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var blocked int
		if err := database.QueryRowContext(t.Context(), `SELECT count(*)
			FROM pg_stat_activity
			WHERE datname=current_database() AND pid<>pg_backend_pid()
			  AND wait_event_type='Lock' AND query LIKE '%'||$1||'%'`, fragment).Scan(
			&blocked); err != nil {
			t.Fatal(err)
		}
		if blocked > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("query never reached blocked lock barrier: %s", fragment)
}

func itoa(value int64) string {
	return strconv.FormatInt(value, 10)
}
