package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/pressly/goose/v3"

	"github.com/YouToco/vane/probe"
	"github.com/YouToco/vane/types"
)

func TestServerRuntimeBoundaryPostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required")
	}
	scratchURL, drop := createScratchDB(t.Context(), t, databaseURL)
	t.Cleanup(drop)
	owner, err := sql.Open("pgx", scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Close() })
	dir, err := fs.Sub(migrationsFS, "migrations")
	if err != nil {
		t.Fatal(err)
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, owner, dir,
		goose.WithAllowOutofOrder(true))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.UpTo(t.Context(), 99); err != nil {
		t.Fatal(err)
	}
	var roleCount int
	if err := owner.QueryRowContext(t.Context(), `SELECT count(*)
		FROM pg_catalog.pg_roles WHERE rolname='vane_server_runtime'`).Scan(
		&roleCount); err != nil {
		t.Fatal(err)
	}
	if roleCount != 0 {
		t.Fatal("migration 098 provisioned a cluster-global runtime implicitly")
	}
	if _, err := provider.UpTo(t.Context(), latestMigrationVersion); err != nil {
		t.Fatal(err)
	}
	if err := ProvisionServerRuntime(t.Context(), scratchURL); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := DeprovisionServerRuntime(ctx, scratchURL); err != nil {
			t.Errorf("deprovision server runtime test role: %v", err)
		}
	})

	const password = "server-runtime-test-password"
	if _, err := owner.ExecContext(t.Context(),
		`ALTER ROLE vane_server_runtime LOGIN PASSWORD '`+password+`'`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		_, _ = owner.ExecContext(ctx,
			`ALTER ROLE vane_server_runtime NOLOGIN PASSWORD NULL`)
	})
	runtimeURL := serverRuntimeTestURL(t, scratchURL, password)
	const researchPassword = "research-runtime-test-password"
	if _, err := owner.ExecContext(t.Context(),
		`ALTER ROLE vane_research_runtime LOGIN PASSWORD '`+researchPassword+`'`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		_, _ = owner.ExecContext(ctx,
			`ALTER ROLE vane_research_runtime NOLOGIN PASSWORD NULL`)
	})
	researchURL := roleTestURL(t, scratchURL, researchRuntimeLoginRole,
		researchPassword)
	if _, err := provider.UpTo(t.Context(), 108); err != nil {
		t.Fatal(err)
	}

	t.Run("schema 108 retains the unfinished V1 research admission fence", func(t *testing.T) {
		st, err := NewServerRuntime(t.Context(), runtimeURL)
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close()
		user, err := st.UpsertUserByOpenID(t.Context(),
			"ou-server-runtime-v1-research-fence", "runtime V1 fence")
		if err != nil {
			t.Fatal(err)
		}
		const tenantID int64 = 1
		if _, err := owner.ExecContext(t.Context(),
			`INSERT INTO memberships (tenant_id,user_id,role) VALUES ($1,$2,'owner')`,
			tenantID, user.ID); err != nil {
			t.Fatal(err)
		}
		tx, err := st.beginTx(t.Context(), pgx.TxOptions{})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(t.Context()) }()
		if err := bindResearchV3AppScopeTx(t.Context(), tx, tenantID, user.ID); err != nil {
			t.Fatal(err)
		}
		taskID := "schema108-v1-cutover-" + uuid.NewString()
		operationID := "schema108-v1-operation-" + uuid.NewString()
		if _, err := tx.Exec(t.Context(), `
			INSERT INTO schedules
			    (id,tenant_id,user_id,nl_description,spec_json,scope_json,status,execution_mode)
			VALUES ($1,$2,$3,'V1 cutover fence','{}','{}','active','compiled')`,
			taskID, tenantID, user.ID); err != nil {
			t.Fatal(err)
		}
		var definitionDigest string
		if err := tx.QueryRow(t.Context(), `
			INSERT INTO task_approved_definition_versions
			    (tenant_id,user_id,task_id,version,schema_version,execution_mode,
			     definition_digest,payload,operation_ref)
			VALUES ($1,$2,$3,1,'vane.task-approved-definition/v3','discover_at_run',
			        encode(sha256($4),'hex'),$4,$5)
			RETURNING definition_digest`, tenantID, user.ID, taskID, []byte(`{}`),
			"schema108-v1-cutover/"+operationID).Scan(&definitionDigest); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(t.Context(), `
			UPDATE schedules SET execution_mode='discover_at_run',
			       approved_definition_version=1,approved_definition_digest=$2
			 WHERE id=$1`, taskID, definitionDigest); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(t.Context(), `
			INSERT INTO task_creation_operations
			    (id,tenant_id,user_id,tool_name,args,summary,status,expires_at,
			     execution_version,task_id)
			VALUES ($1,$2,$3,'create_schedule','{}','V1 cutover fence','pending',
			        clock_timestamp()+interval '1 hour',1,$4)`,
			operationID, tenantID, user.ID, taskID); err != nil {
			t.Fatal(err)
		}
		clause, err := nativeResearchScheduleMaturityClause(t.Context(), tx)
		if err != nil {
			t.Fatal(err)
		}
		var visible int
		if err := tx.QueryRow(t.Context(),
			`SELECT count(*) FROM schedules schedule WHERE schedule.id=$1`+clause,
			taskID).Scan(&visible); err != nil {
			t.Fatal(err)
		}
		if visible != 0 {
			t.Fatal("schema 108 admitted a V3-cutover task with unfinished V1 creation")
		}
		if _, err := tx.Exec(t.Context(),
			`DELETE FROM task_creation_operations WHERE id=$1`, operationID); err != nil {
			t.Fatal(err)
		}
		if err := tx.QueryRow(t.Context(),
			`SELECT count(*) FROM schedules schedule WHERE schedule.id=$1`+clause,
			taskID).Scan(&visible); err != nil {
			t.Fatal(err)
		}
		if visible != 1 {
			t.Fatal("schema 108 rejected a research task after the V1 fence cleared")
		}
	})

	t.Run("valid runtime supports default and explicit Store capabilities", func(t *testing.T) {
		st, err := NewServerRuntime(t.Context(), runtimeURL)
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close()
		if err := st.Ping(t.Context()); err != nil {
			t.Fatal(err)
		}
		user, err := st.UpsertUserByOpenID(t.Context(),
			"ou-server-runtime-smoke", "runtime smoke")
		if err != nil || user.ID <= 0 {
			t.Fatalf("default vane_app Store smoke user=%+v err=%v", user, err)
		}
		tx, err := st.pool.Begin(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(t.Context()) }()
		var sessionUser, currentUser string
		if err := tx.QueryRow(t.Context(), `SELECT session_user,current_user`).Scan(
			&sessionUser, &currentUser); err != nil {
			t.Fatal(err)
		}
		if sessionUser != serverRuntimeLoginRole || currentUser != "vane_app" {
			t.Fatalf("runtime identity=%s/%s", sessionUser, currentUser)
		}
		if _, err := tx.Exec(t.Context(),
			`SET LOCAL ROLE vane_intelligence_reader`); err != nil {
			t.Fatalf("explicit allowlisted Store role: %v", err)
		}
	})

	// The compatibility cases above deliberately exercise schema 108. The
	// recovery catalog and every remaining runtime boundary use the current
	// contract, including the durable schedule-command recovery cursor.
	if _, err := provider.UpTo(t.Context(), latestMigrationVersion); err != nil {
		t.Fatal(err)
	}

	t.Run("memory capability requires every exact ACL at startup", func(t *testing.T) {
		mutations := []struct {
			name, revoke, restore string
		}{
			{"schema usage", `REVOKE USAGE ON SCHEMA public FROM vane_memory_editor`, `GRANT USAGE ON SCHEMA public TO vane_memory_editor`},
			{"authorizations select", `REVOKE SELECT ON memory_authorizations FROM vane_memory_editor`, `GRANT SELECT ON memory_authorizations TO vane_memory_editor`},
			{"authorizations insert", `REVOKE INSERT ON memory_authorizations FROM vane_memory_editor`, `GRANT INSERT ON memory_authorizations TO vane_memory_editor`},
			{"records select", `REVOKE SELECT ON memory_records FROM vane_memory_editor`, `GRANT SELECT ON memory_records TO vane_memory_editor`},
			{"records insert", `REVOKE INSERT ON memory_records FROM vane_memory_editor`, `GRANT INSERT ON memory_records TO vane_memory_editor`},
			{"events select", `REVOKE SELECT ON memory_events FROM vane_memory_editor`, `GRANT SELECT ON memory_events TO vane_memory_editor`},
			{"events insert", `REVOKE INSERT ON memory_events FROM vane_memory_editor`, `GRANT INSERT ON memory_events TO vane_memory_editor`},
			{"receipts select", `REVOKE SELECT ON memory_receipts FROM vane_memory_editor`, `GRANT SELECT ON memory_receipts TO vane_memory_editor`},
			{"receipts insert", `REVOKE INSERT ON memory_receipts FROM vane_memory_editor`, `GRANT INSERT ON memory_receipts TO vane_memory_editor`},
			{"records sequence usage", `REVOKE USAGE ON SEQUENCE memory_records_id_seq FROM vane_memory_editor`, `GRANT USAGE ON SEQUENCE memory_records_id_seq TO vane_memory_editor`},
			{"records sequence select", `REVOKE SELECT ON SEQUENCE memory_records_id_seq FROM vane_memory_editor`, `GRANT SELECT ON SEQUENCE memory_records_id_seq TO vane_memory_editor`},
			{"events sequence usage", `REVOKE USAGE ON SEQUENCE memory_events_id_seq FROM vane_memory_editor`, `GRANT USAGE ON SEQUENCE memory_events_id_seq TO vane_memory_editor`},
			{"events sequence select", `REVOKE SELECT ON SEQUENCE memory_events_id_seq FROM vane_memory_editor`, `GRANT SELECT ON SEQUENCE memory_events_id_seq TO vane_memory_editor`},
			{"authorization consume", `REVOKE UPDATE (consumed_event_id) ON memory_authorizations FROM vane_memory_editor`, `GRANT UPDATE (consumed_event_id) ON memory_authorizations TO vane_memory_editor`},
		}
		for _, mutation := range mutations {
			if _, err := owner.ExecContext(t.Context(), mutation.revoke); err != nil {
				t.Fatalf("revoke %s: %v", mutation.name, err)
			}
			st, err := NewServerRuntime(t.Context(), runtimeURL)
			if err == nil {
				st.Close()
				t.Fatalf("startup accepted missing %s", mutation.name)
			}
			if !strings.Contains(err.Error(), "required authorities") {
				t.Fatalf("missing %s returned unexpected error: %v", mutation.name, err)
			}
			if _, err := owner.ExecContext(t.Context(), mutation.restore); err != nil {
				t.Fatalf("restore %s: %v", mutation.name, err)
			}
		}
		st, err := NewServerRuntime(t.Context(), runtimeURL)
		if err != nil {
			t.Fatalf("restored memory authority rejected: %v", err)
		}
		st.Close()
	})

	t.Run("memory capability rejects extra ACL at startup", func(t *testing.T) {
		mutations := []struct {
			name, grant, revoke string
		}{
			{"mutable history", `GRANT UPDATE ON memory_records TO vane_memory_editor`, `REVOKE UPDATE ON memory_records FROM vane_memory_editor`},
			{"foreign session read", `GRANT SELECT ON agent_sessions TO vane_memory_editor`, `REVOKE SELECT ON agent_sessions FROM vane_memory_editor`},
			{"delegable required read", `GRANT SELECT ON memory_records TO vane_memory_editor WITH GRANT OPTION`, `REVOKE GRANT OPTION FOR SELECT ON memory_records FROM vane_memory_editor; GRANT SELECT ON memory_records TO vane_memory_editor`},
		}
		for _, mutation := range mutations {
			if _, err := owner.ExecContext(t.Context(), mutation.grant); err != nil {
				t.Fatalf("grant %s: %v", mutation.name, err)
			}
			st, err := NewServerRuntime(t.Context(), runtimeURL)
			if err == nil {
				st.Close()
				t.Fatalf("startup accepted %s", mutation.name)
			}
			if !strings.Contains(err.Error(), "unexpected authorities") {
				t.Fatalf("extra %s returned unexpected error: %v", mutation.name, err)
			}
			if _, err := owner.ExecContext(t.Context(), mutation.revoke); err != nil {
				t.Fatalf("revoke %s: %v", mutation.name, err)
			}
		}
		st, err := NewServerRuntime(t.Context(), runtimeURL)
		if err != nil {
			t.Fatalf("restored exact memory authority rejected: %v", err)
		}
		st.Close()
	})

	t.Run("recovery catalog separates discovery from tenant payload reads", func(t *testing.T) {
		st, err := NewServerRuntime(t.Context(), runtimeURL)
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close()
		user, err := st.UpsertUserByOpenID(t.Context(),
			"ou-recovery-catalog-"+uuid.NewString(), "recovery catalog")
		if err != nil {
			t.Fatal(err)
		}
		var tenantA, tenantB int64
		if err := owner.QueryRowContext(t.Context(),
			`INSERT INTO tenants(status,plan) VALUES('active','free') RETURNING id`).Scan(&tenantA); err != nil {
			t.Fatal(err)
		}
		if err := owner.QueryRowContext(t.Context(),
			`INSERT INTO tenants(status,plan) VALUES('active','free') RETURNING id`).Scan(&tenantB); err != nil {
			t.Fatal(err)
		}
		if _, err := owner.ExecContext(t.Context(), `
			INSERT INTO memberships(tenant_id,user_id,role)
			VALUES($1,$3,'owner'),($2,$3,'owner')`, tenantA, tenantB, user.ID); err != nil {
			t.Fatal(err)
		}
		taskA := "runtime-recovery-a-" + uuid.NewString()
		taskB := "runtime-recovery-b-" + uuid.NewString()
		if _, err := owner.ExecContext(t.Context(), `
			INSERT INTO schedules
			  (id,tenant_id,user_id,nl_description,spec_json,scope_json,status,execution_mode)
			VALUES
			  ($1,$3,$5,'tenant A','{}','{}','active','compiled'),
			  ($2,$4,$5,'tenant B','{}','{}','active','compiled')`,
			taskA, taskB, tenantA, tenantB, user.ID); err != nil {
			t.Fatal(err)
		}
		commandID := uuid.New()
		if _, err := owner.ExecContext(t.Context(), `
			INSERT INTO schedule_commands
			  (id,tenant_id,user_id,task_id,idempotency_key,kind,
			   payload_digest,remote_request_id)
			VALUES($1,$2,$3,$4,$5,'run',$6,$7)`,
			commandID, tenantA, user.ID, taskA,
			"runtime-recovery-"+uuid.NewString(), strings.Repeat("a", 64),
			strings.Repeat("b", 64)); err != nil {
			t.Fatal(err)
		}

		catalog, err := st.ListRecoveryTenantCatalogPage(
			t.Context(), tenantA-1, maxRecoveryTenantCatalogPage)
		if err != nil {
			t.Fatalf("runtime recovery tenant catalog: %v", err)
		}
		contains := func(ids []int64, want int64) bool {
			for _, id := range ids {
				if id == want {
					return true
				}
			}
			return false
		}
		if !contains(catalog, tenantA) || !contains(catalog, tenantB) {
			t.Fatalf("recovery catalog=%v, want tenants %d/%d", catalog, tenantA, tenantB)
		}
		activeA, err := st.ListActiveSchedules(t.Context(), tenantA)
		if err != nil || len(activeA) != 1 || activeA[0].ID != taskA {
			t.Fatalf("tenant A active schedules=%+v err=%v", activeA, err)
		}
		activeB, err := st.ListActiveSchedules(t.Context(), tenantB)
		if err != nil || len(activeB) != 1 || activeB[0].ID != taskB {
			t.Fatalf("tenant B active schedules=%+v err=%v", activeB, err)
		}
		commandsA, err := st.ListPendingScheduleCommands(t.Context(), tenantA, "")
		if err != nil || len(commandsA) != 1 || commandsA[0].ID != commandID.String() {
			t.Fatalf("tenant A commands=%+v err=%v", commandsA, err)
		}
		commandsB, err := st.ListPendingScheduleCommands(t.Context(), tenantB, "")
		if err != nil || len(commandsB) != 0 {
			t.Fatalf("tenant B saw tenant A commands=%+v err=%v", commandsB, err)
		}
		cursorTenant, cursorCommand, err := st.LoadScheduleCommandRecoveryCursor(t.Context())
		if err != nil || cursorTenant != 0 || cursorCommand != "" {
			t.Fatalf("initial durable command cursor=%d/%q err=%v",
				cursorTenant, cursorCommand, err)
		}
		if err := st.SaveScheduleCommandRecoveryCursor(
			t.Context(), tenantA, commandID.String(),
		); err != nil {
			t.Fatalf("save durable command cursor: %v", err)
		}
		cursorTenant, cursorCommand, err = st.LoadScheduleCommandRecoveryCursor(t.Context())
		if err != nil || cursorTenant != tenantA || cursorCommand != commandID.String() {
			t.Fatalf("durable command cursor=%d/%q err=%v",
				cursorTenant, cursorCommand, err)
		}
		if err := st.SaveScheduleCommandRecoveryCursor(t.Context(), 0, ""); err != nil {
			t.Fatalf("clear durable command cursor: %v", err)
		}
		reconciled, release, err := st.AcquireScheduleReconcile(
			t.Context(), tenantA, taskA)
		if err != nil || reconciled == nil || reconciled.ID != taskA || release == nil {
			t.Fatalf("tenant A reconcile=%+v release=%v err=%v",
				reconciled, release != nil, err)
		}
		if err := release(t.Context()); err != nil {
			t.Fatalf("release tenant A reconcile: %v", err)
		}
		future := time.Now().Add(time.Hour)
		assertEmpty := func(name string, count int, err error) {
			t.Helper()
			if err != nil || count != 0 {
				t.Fatalf("%s count=%d err=%v", name, count, err)
			}
		}
		creationOps, err := st.ListStaleTaskCreationOperations(
			t.Context(), tenantA, future, 10)
		assertEmpty("task creation recovery", len(creationOps), err)
		creationReceipts, err := st.ListDueTaskCreationReceipts(
			t.Context(), tenantA, future, 10)
		assertEmpty("task creation receipt recovery", len(creationReceipts), err)
		editOps, err := st.ListStaleTaskDefinitionEditOperations(
			t.Context(), tenantA, future, 10)
		assertEmpty("definition edit recovery", len(editOps), err)
		nonterminalEdits, err := st.ListNonterminalTaskDefinitionEditOperations(
			t.Context(), tenantA, "", 10)
		assertEmpty("definition edit preflight", len(nonterminalEdits), err)
		editReceipts, err := st.ListDueTaskDefinitionEditReceipts(
			t.Context(), tenantA, future, 10)
		assertEmpty("definition edit receipt recovery", len(editReceipts), err)
		pushEffects, err := st.ListRecoverablePushEffects(
			t.Context(), taskA, tenantA, future, "", 10)
		assertEmpty("push effect recovery", len(pushEffects), err)
		// Prove the non-empty task-creation recovery chain through the actual
		// server runtime role. Empty discovery assertions would not catch an
		// RLS-scoped UPDATE that silently affected zero rows.
		operationID := "runtime-creation-recovery-" + uuid.NewString()
		created, err := st.CreateTaskCreationOperation(t.Context(),
			types.CreateTaskCreationOperationParams{
				ID: operationID, TenantID: tenantA, UserID: user.ID,
				Args:    json.RawMessage(`{"intent":"runtime recovery proof"}`),
				Summary: "runtime recovery proof", ExpiresAt: future,
			})
		if err != nil {
			t.Fatalf("runtime create task creation operation: %v", err)
		}
		acquired, err := st.AcquireTaskCreationOperation(t.Context(),
			types.AcquireTaskCreationOperationParams{
				ID: created.ID, TenantID: tenantA, UserID: user.ID,
				LeaseOwner: "runtime-recovery-worker", LeaseDuration: time.Minute,
				ReceiptProvider: "feishu_message_patch",
				ReceiptTarget:   "om_runtime_recovery",
			})
		if err != nil {
			t.Fatalf("runtime acquire task creation operation: %v", err)
		}
		if err := st.SealTaskCreationCommand(
			t.Context(), acquired.Lease(), []byte(`{"intent":"runtime recovery proof"}`),
		); err != nil {
			t.Fatalf("runtime checkpoint task creation operation: %v", err)
		}
		if err := st.FailTaskCreationOperation(
			t.Context(), acquired.Lease(), "RUNTIME_RECOVERY_PROOF", "expected test terminal",
		); err != nil {
			t.Fatalf("runtime terminal task creation operation: %v", err)
		}
		if _, err := owner.ExecContext(t.Context(), `
			UPDATE task_creation_receipts
			   SET next_attempt_at=clock_timestamp()-interval '1 second'
			 WHERE operation_id=$1`, operationID); err != nil {
			t.Fatalf("age runtime receipt fixture: %v", err)
		}
		dueA, err := st.ListDueTaskCreationReceipts(
			t.Context(), tenantA, future, 10)
		if err != nil || len(dueA) != 1 || dueA[0].OperationID != operationID {
			t.Fatalf("runtime tenant A due receipts=%+v err=%v", dueA, err)
		}
		dueB, err := st.ListDueTaskCreationReceipts(
			t.Context(), tenantB, future, 10)
		if err != nil || len(dueB) != 0 {
			t.Fatalf("runtime tenant B saw tenant A receipt=%+v err=%v", dueB, err)
		}
		receipt, err := st.AcquireTaskCreationReceipt(t.Context(),
			types.AcquireTaskCreationReceiptParams{
				ID: dueA[0].ID, TenantID: tenantA, UserID: user.ID,
				LeaseOwner: "runtime-receipt-worker", LeaseDuration: time.Minute,
			})
		if err != nil {
			t.Fatalf("runtime acquire task creation receipt: %v", err)
		}
		payload := []byte(`{"text":"runtime receipt proof"}`)
		sum := sha256.Sum256(payload)
		digest := hex.EncodeToString(sum[:])
		if err := st.CheckpointTaskCreationReceiptPayload(
			t.Context(), receipt.Lease(), payload, digest,
		); err != nil {
			t.Fatalf("runtime checkpoint task creation receipt: %v", err)
		}
		if err := st.MarkTaskCreationReceiptSent(
			t.Context(), receipt.Lease(), "om_runtime_receipt_sent",
		); err != nil {
			t.Fatalf("runtime terminal task creation receipt: %v", err)
		}
		loadedReceipt, err := st.LoadTaskCreationReceiptByOperation(
			t.Context(), operationID, tenantA, user.ID)
		if err != nil || loadedReceipt.Status != types.TaskCreationReceiptStatusSent {
			t.Fatalf("runtime terminal receipt=%+v err=%v", loadedReceipt, err)
		}

		// Exercise the production first read (operation id + user, no tenant)
		// and every activation/cleanup commit under NewServerRuntime. Fixture
		// setup uses the owner Store only to create tenant/user roots.
		ownerStore, err := New(t.Context(), scratchURL)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(ownerStore.Close)
		creationFixture := newCompiledTaskFixture(t, ownerStore)
		t.Cleanup(func() {
			cleanupCtx, cancel := cleanupContext()
			defer cancel()
			cleanupExec(cleanupCtx, t, ownerStore,
				`DELETE FROM task_creation_receipts WHERE tenant_id=$1`,
				creationFixture.tenantID)
			cleanupExec(cleanupCtx, t, ownerStore,
				`DELETE FROM task_creation_operations WHERE tenant_id=$1`,
				creationFixture.tenantID)
		})
		activation := preparedA5Commit(t, st, creationFixture, "runtime-activation")
		resolved, err := st.LoadTaskCreationOperationByUser(
			t.Context(), activation.Lease.ID, activation.Lease.UserID,
		)
		if err != nil || resolved.TenantID != activation.Lease.TenantID {
			t.Fatalf("runtime production operation resolver=%+v err=%v", resolved, err)
		}
		if err := st.CommitPausedCompiledTaskDefinitionForCreation(
			t.Context(), activation,
		); err != nil {
			t.Fatalf("runtime definition commit: %v", err)
		}
		started, err := st.BeginTaskCreationActivation(
			t.Context(), activation.Lease, activation.Definition.TaskID,
		)
		if err != nil || !started {
			t.Fatalf("runtime activation begin=%t err=%v", started, err)
		}
		if err := st.CommitTaskCreationActivation(
			t.Context(), activation.Lease, activation.Definition.TaskID,
		); err != nil {
			t.Fatalf("runtime activation commit: %v", err)
		}

		cleanup := preparedA5Commit(t, st, creationFixture, "runtime-cleanup")
		if err := st.CommitPausedCompiledTaskDefinitionForCreation(
			t.Context(), cleanup,
		); err != nil {
			t.Fatalf("runtime cleanup definition commit: %v", err)
		}
		started, err = st.BeginTaskCreationCleanup(
			t.Context(), cleanup.Lease, cleanup.Definition.TaskID,
			"RUNTIME_CLEANUP_PROOF", "expected cleanup test",
		)
		if err != nil || !started {
			t.Fatalf("runtime cleanup begin=%t err=%v", started, err)
		}
		if err := st.FinishTaskCreationCleanup(
			t.Context(), cleanup.Lease, cleanup.Definition.TaskID,
			types.TaskOperationStatusFailed,
		); err != nil {
			t.Fatalf("runtime cleanup finish: %v", err)
		}

		quarantine := preparedA5Commit(t, st, creationFixture, "runtime-quarantine")
		if err := st.BlockTaskCreationOperationAfterSideEffect(
			t.Context(), quarantine.Lease, quarantine.Definition.TaskID,
			"RUNTIME_QUARANTINE_PROOF", "expected quarantine test",
		); err != nil {
			t.Fatalf("runtime side-effect quarantine: %v", err)
		}
	})

	t.Run("exact profile and observability reads are tenant scoped", func(t *testing.T) {
		st, err := NewServerRuntime(t.Context(), runtimeURL)
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close()
		user, err := st.UpsertUserByOpenID(t.Context(),
			"ou-server-runtime-profile", "runtime profile")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := owner.ExecContext(t.Context(),
			`INSERT INTO memberships (tenant_id,user_id,role)
			 VALUES (1,$1,'owner') ON CONFLICT DO NOTHING`, user.ID); err != nil {
			t.Fatal(err)
		}
		industry := "software"
		if _, err := st.UpsertProfileFields(t.Context(), user.ID,
			&industry, nil, []string{"agent"}); err != nil {
			t.Fatal(err)
		}
		profile, err := st.GetProfileForTenant(t.Context(), 1, user.ID)
		if err != nil {
			t.Fatalf("scoped profile read through server runtime: %v", err)
		}
		if profile.ID <= 0 || profile.UserID != user.ID || profile.Industry != industry ||
			profile.ProfileEpoch != 0 || profile.ProfileVersion != 0 {
			t.Fatalf("unexpected scoped profile: %+v", profile)
		}
		if _, err := st.GetProfileForTenant(t.Context(), 2, user.ID); !errors.Is(err, types.ErrNotFound) {
			t.Fatalf("cross-tenant profile read must fail closed, got %v", err)
		}
		now := time.Now().UTC()
		if _, err := owner.ExecContext(t.Context(),
			`INSERT INTO llm_calls
			    (trace_id,span_name,user_id,tenant_id,provider,model,
			     user_prompt,completion,completion_tokens,error,created_at)
			 VALUES ('server-runtime-bad-score','score',$1,1,'test','test-model',
			         '用户画像：行业：software','',16,'',$2)`,
			user.ID, now.Add(-time.Minute)); err != nil {
			t.Fatalf("insert tenant bad score fixture: %v", err)
		}
		report, err := probe.Run(t.Context(), st, 1, user.ID,
			now, probe.DefaultWindow)
		if err != nil {
			t.Fatalf("full gate read surface through server runtime: %v", err)
		}
		if len(report.Results) != 9 {
			t.Fatalf("full gate result count=%d", len(report.Results))
		}
		emptyOutputStatus := probe.Status("")
		for _, result := range report.Results {
			if result.ID == "empty_completion" {
				emptyOutputStatus = result.Status
				break
			}
		}
		if report.Quality.EmptyNoError != 1 || emptyOutputStatus != probe.StatusRed ||
			report.Worst() != probe.StatusRed {
			t.Fatalf("tenant bad score must make Gate red, quality=%+v worst=%s",
				report.Quality, report.Worst())
		}
		crossTenantReport, err := probe.Run(t.Context(), st, 2, user.ID,
			now, probe.DefaultWindow)
		if err != nil {
			t.Fatalf("cross-tenant Gate read: %v", err)
		}
		if crossTenantReport.Quality.EmptyNoError != 0 ||
			crossTenantReport.Worst() == probe.StatusRed {
			t.Fatalf("tenant 2 must not see tenant 1 bad score, quality=%+v worst=%s",
				crossTenantReport.Quality, crossTenantReport.Worst())
		}
		stats, err := st.ListSpanRunStats(t.Context(), 1, now.Add(-probe.DefaultWindow))
		if err != nil {
			t.Fatalf("tenant runstats: %v", err)
		}
		if len(stats) != 1 || stats[0].SpanName != "score" || stats[0].Calls != 1 {
			t.Fatalf("tenant runstats must include bad score, got %+v", stats)
		}
		crossTenantStats, err := st.ListSpanRunStats(
			t.Context(), 2, now.Add(-probe.DefaultWindow))
		if err != nil {
			t.Fatalf("cross-tenant runstats: %v", err)
		}
		if len(crossTenantStats) != 0 {
			t.Fatalf("tenant 2 must not see tenant 1 runstats, got %+v", crossTenantStats)
		}
	})

	t.Run("constructor keeps paid research on separate restricted pool", func(t *testing.T) {
		st, err := NewServerRuntimeWithResearchRuntimeCapability(
			t.Context(), runtimeURL, researchURL, ResearchRunCapabilityConfigV1{
				ActiveKeyID:  "server-runtime-constructor-smoke",
				ActiveKeyHex: strings.Repeat("51", 32),
			})
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close()
		if st.researchPool == nil || st.gatewayPool != nil ||
			!st.researchCapabilityConfigured {
			t.Fatalf("constructor pools/capability are unsafe: research=%v gateway=%v capability=%v",
				st.researchPool != nil, st.gatewayPool != nil,
				st.researchCapabilityConfigured)
		}
		tx, err := st.beginResearchTransaction(t.Context(), pgx.TxOptions{})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(t.Context()) }()
		var sessionUser, currentUser string
		if err := tx.QueryRow(t.Context(), `SELECT session_user,current_user`).Scan(
			&sessionUser, &currentUser); err != nil {
			t.Fatal(err)
		}
		if sessionUser != researchRuntimeLoginRole || currentUser != researchRuntimeLoginRole {
			t.Fatalf("research identity=%s/%s", sessionUser, currentUser)
		}
	})

	t.Run("constructor rejects missing synthesis admission capability", func(t *testing.T) {
		if _, err := owner.ExecContext(t.Context(),
			`REVOKE EXECUTE ON FUNCTION authorize_research_run_effect_cap_v1(BIGINT)
			 FROM vane_research_v3_executor`); err != nil {
			t.Fatal(err)
		}
		defer func() {
			ctx, cancel := cleanupContext()
			defer cancel()
			if _, err := owner.ExecContext(ctx,
				`GRANT EXECUTE ON FUNCTION authorize_research_run_effect_cap_v1(BIGINT)
				 TO vane_research_v3_executor`); err != nil {
				t.Errorf("restore synthesis admission capability: %v", err)
			}
		}()

		st, err := NewServerRuntimeWithResearchRuntimeCapability(
			t.Context(), runtimeURL, researchURL, ResearchRunCapabilityConfigV1{
				ActiveKeyID:  "server-runtime-missing-synthesis-admission",
				ActiveKeyHex: strings.Repeat("52", 32),
			})
		if st != nil {
			st.Close()
		}
		if err == nil || !strings.Contains(err.Error(), "destructive privileges") {
			t.Fatalf("constructor accepted missing synthesis admission capability: %v", err)
		}
	})

	t.Run("quota readiness fails closed on live privilege drift", func(t *testing.T) {
		for name, drift := range map[string]struct {
			apply   string
			restore string
		}{
			"resolver missing": {
				apply: `ALTER FUNCTION resolve_research_quota_rule_v1(BIGINT,BIGINT,TEXT,TEXT)
					RENAME TO resolve_research_quota_rule_v1_drifted`,
				restore: `ALTER FUNCTION resolve_research_quota_rule_v1_drifted(BIGINT,BIGINT,TEXT,TEXT)
					RENAME TO resolve_research_quota_rule_v1`,
			},
			"token column granted": {
				apply:   `GRANT SELECT(tokens) ON tenant_quota TO vane_app`,
				restore: `REVOKE SELECT(tokens) ON tenant_quota FROM vane_app`,
			},
			"runtime login token column granted": {
				apply:   `GRANT SELECT(tokens) ON tenant_quota TO vane_server_runtime`,
				restore: `REVOKE SELECT(tokens) ON tenant_quota FROM vane_server_runtime`,
			},
			"capability rate column granted": {
				apply:   `GRANT SELECT(rate) ON tenant_quota TO vane_brief_reader`,
				restore: `REVOKE SELECT(rate) ON tenant_quota FROM vane_brief_reader`,
			},
		} {
			t.Run(name, func(t *testing.T) {
				runtimeStore, err := NewServerRuntime(t.Context(), runtimeURL)
				if err != nil {
					t.Fatal(err)
				}
				defer runtimeStore.Close()
				ownerCompat, err := New(t.Context(), scratchURL)
				if err != nil {
					t.Fatal(err)
				}
				defer ownerCompat.Close()
				if _, err := owner.ExecContext(t.Context(), drift.apply); err != nil {
					t.Fatal(err)
				}
				defer func() {
					ctx, cancel := cleanupContext()
					defer cancel()
					if _, err := owner.ExecContext(ctx, drift.restore); err != nil {
						t.Errorf("restore quota readiness drift: %v", err)
					}
				}()
				if err := runtimeStore.Ping(t.Context()); err == nil {
					t.Fatal("restricted research control readiness accepted quota privilege drift")
				}
				if err := ownerCompat.Ping(t.Context()); err != nil {
					t.Fatalf("owner-compatible primary readiness was coupled to V3 projection: %v", err)
				}
			})
		}
	})

	t.Run("V3 shadow binds exact app tenant and user scope", func(t *testing.T) {
		ownerStore, err := New(t.Context(), scratchURL)
		if err != nil {
			t.Fatal(err)
		}
		defer ownerStore.Close()
		user, err := ownerStore.UpsertUserByOpenID(t.Context(),
			"ou-v3-runtime-"+uuid.NewString(), "V3 runtime owner")
		if err != nil {
			t.Fatal(err)
		}
		var tenantID int64
		if err := owner.QueryRowContext(t.Context(),
			`INSERT INTO tenants(status,plan) VALUES('active','free') RETURNING id`).Scan(&tenantID); err != nil {
			t.Fatal(err)
		}
		if _, err := owner.ExecContext(t.Context(),
			`INSERT INTO memberships(tenant_id,user_id,role) VALUES($1,$2,'owner')`,
			tenantID, user.ID); err != nil {
			t.Fatal(err)
		}
		if err := ownerStore.SeedTenantQuota(t.Context(), tenantID); err != nil {
			t.Fatal(err)
		}
		taskID := "v3-runtime-" + uuid.NewString()
		if _, err := owner.ExecContext(t.Context(), `INSERT INTO schedules
			(id,tenant_id,user_id,nl_description,spec_json,scope_json,status,push_strictness)
			VALUES($1,$2,$3,'runtime V3','{"cron":"0 9 * * 1","tz":"Asia/Shanghai"}',
			'{}','active','strict')`, taskID, tenantID, user.ID); err != nil {
			t.Fatal(err)
		}
		if _, err := owner.ExecContext(t.Context(), `INSERT INTO schedule_playbooks
			(schedule_id,content,fetch_plan) VALUES($1,'runtime RLS shadow test','{}')`,
			taskID); err != nil {
			t.Fatal(err)
		}
		p := researchV3PreparePolicyForTest()
		p.TenantID, p.UserID, p.TaskID, p.IdempotencyKey = tenantID, user.ID, taskID, "runtime-prepare"
		if _, err := ownerStore.PrepareResearchV3Definition(t.Context(), p); err != nil {
			t.Fatal(err)
		}
		runtimeStore, err := NewServerRuntimeWithResearchRuntimeCapability(
			t.Context(), runtimeURL, researchURL, ResearchRunCapabilityConfigV1{
				ActiveKeyID:  "server-runtime-v3-shadow",
				ActiveKeyHex: strings.Repeat("53", 32),
			})
		if err != nil {
			t.Fatal(err)
		}
		defer runtimeStore.Close()
		mirror, err := runtimeStore.GetSchedule(t.Context(), taskID, user.ID)
		if err != nil || mirror.TenantID != tenantID {
			t.Fatalf("runtime GetSchedule=%+v err=%v", mirror, err)
		}
		if _, err := runtimeStore.GetSchedule(t.Context(), taskID, user.ID+1); types.CodeOf(err) != types.CodeNotFound {
			t.Fatalf("runtime cross-user GetSchedule err=%v", err)
		}
		available, err := runtimeStore.HasCurrentResearchApprovedDefinitionV3(
			t.Context(), tenantID, user.ID, taskID)
		if err != nil || !available {
			t.Fatalf("runtime V3 preflight=%t err=%v", available, err)
		}
		foreign, err := runtimeStore.HasCurrentResearchApprovedDefinitionV3(
			t.Context(), tenantID+1, user.ID, taskID)
		if err != nil || foreign {
			t.Fatalf("runtime cross-tenant V3 preflight=%t err=%v", foreign, err)
		}
		identity := types.RunIdentity{TemporalWorkflowID: "research-v3-shadow-" + strings.Repeat("2", 64),
			TemporalRunID: "runtime-run-" + uuid.NewString(), RunKind: types.RunSnapshotKindScheduled,
			TenantID: tenantID, UserID: user.ID, TaskID: taskID}
		quota, err := runtimeStore.LoadResearchQuotaRuleV3(
			t.Context(), identity, QuotaLLMTokens)
		if err != nil || quota.Rate <= 0 || quota.Burst <= 0 {
			t.Fatalf("runtime V3 quota projection=%+v err=%v", quota, err)
		}
		for name, mutate := range map[string]func(*types.RunIdentity){
			"tenant": func(value *types.RunIdentity) { value.TenantID++ },
			"user":   func(value *types.RunIdentity) { value.UserID++ },
			"task":   func(value *types.RunIdentity) { value.TaskID = "foreign-task" },
		} {
			t.Run("quota projection rejects foreign "+name, func(t *testing.T) {
				foreign := identity
				mutate(&foreign)
				if _, err := runtimeStore.LoadResearchQuotaRuleV3(
					t.Context(), foreign, QuotaLLMTokens,
				); !errors.Is(err, ErrQuotaExceeded) {
					t.Fatalf("foreign %s quota err=%v", name, err)
				}
			})
		}
		if _, err := runtimeStore.pool.Exec(t.Context(),
			`SELECT rate,burst FROM tenant_quota LIMIT 1`); err == nil {
			t.Fatal("vane_app read tenant_quota directly")
		}
		if _, err := runtimeStore.pool.Exec(t.Context(),
			`SELECT tokens FROM tenant_quota LIMIT 1`); err == nil {
			t.Fatal("vane_app read tenant_quota token balance directly")
		}
		unbound, err := runtimeStore.pool.Begin(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := unbound.Exec(t.Context(), `SELECT * FROM
			resolve_research_quota_rule_v1($1,$2,$3,$4)`, tenantID, user.ID,
			taskID, string(QuotaLLMTokens)); err == nil {
			t.Fatal("quota projection accepted an unbound transaction")
		}
		_ = unbound.Rollback(t.Context())
		executor, err := pgx.Connect(t.Context(), researchURL)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = executor.Close(t.Context()) }()
		if _, err := executor.Exec(t.Context(), `SELECT * FROM
			resolve_research_quota_rule_v1($1,$2,$3,$4)`, tenantID, user.ID,
			taskID, string(QuotaLLMTokens)); err == nil {
			t.Fatal("research executor invoked the control quota projection")
		}
		if _, err := executor.Exec(t.Context(),
			`SELECT tokens FROM tenant_quota LIMIT 1`); err == nil {
			t.Fatal("research executor read tenant_quota token balance directly")
		}
		var publicCanExecute bool
		if err := owner.QueryRowContext(t.Context(), `
			SELECT COALESCE(bool_or(acl.grantee=0 AND acl.privilege_type='EXECUTE'),false)
			  FROM pg_catalog.pg_proc proc
			 CROSS JOIN LATERAL aclexplode(COALESCE(proc.proacl,
			       acldefault('f',proc.proowner))) acl
			 WHERE proc.oid='public.resolve_research_quota_rule_v1(bigint,bigint,text,text)'::regprocedure`,
		).Scan(&publicCanExecute); err != nil {
			t.Fatal(err)
		}
		if publicCanExecute {
			t.Fatal("PUBLIC can execute the research control quota projection")
		}

		for name, mutation := range map[string]struct {
			apply   string
			restore string
		}{
			"paused task": {
				apply:   `UPDATE schedules SET status='paused' WHERE id=$1`,
				restore: `UPDATE schedules SET status='active' WHERE id=$1`,
			},
			"suspended tenant": {
				apply:   `UPDATE tenants SET status='suspended' WHERE id=$1`,
				restore: `UPDATE tenants SET status='active' WHERE id=$1`,
			},
			"soft-deleted tenant": {
				apply:   `UPDATE tenants SET deleted_at=now() WHERE id=$1`,
				restore: `UPDATE tenants SET deleted_at=NULL WHERE id=$1`,
			},
			"missing membership": {
				apply:   `DELETE FROM memberships WHERE tenant_id=$1 AND user_id=$2`,
				restore: `INSERT INTO memberships(tenant_id,user_id,role) VALUES($1,$2,'owner')`,
			},
			"non-owner membership": {
				apply:   `UPDATE memberships SET role='member' WHERE tenant_id=$1 AND user_id=$2`,
				restore: `UPDATE memberships SET role='owner' WHERE tenant_id=$1 AND user_id=$2`,
			},
		} {
			t.Run("quota projection rejects "+name, func(t *testing.T) {
				args := []any{tenantID, user.ID}
				if name == "paused task" {
					args = []any{taskID}
				} else if name == "suspended tenant" || name == "soft-deleted tenant" {
					args = []any{tenantID}
				}
				if _, err := owner.ExecContext(t.Context(), mutation.apply, args...); err != nil {
					t.Fatal(err)
				}
				defer func() {
					ctx, cancel := cleanupContext()
					defer cancel()
					if _, err := owner.ExecContext(ctx, mutation.restore, args...); err != nil {
						t.Errorf("restore %s: %v", name, err)
					}
				}()
				if _, err := runtimeStore.LoadResearchQuotaRuleV3(
					t.Context(), identity, QuotaLLMTokens,
				); !errors.Is(err, ErrQuotaExceeded) {
					t.Fatalf("%s quota err=%v", name, err)
				}
			})
		}
		pausedTaskID := "v3-runtime-paused-" + uuid.NewString()
		if _, err := owner.ExecContext(t.Context(), `INSERT INTO schedules
			(id,tenant_id,user_id,nl_description,spec_json,scope_json,status,push_strictness)
			VALUES($1,$2,$3,'paused runtime V3',
			'{"cron":"0 9 * * 1","tz":"Asia/Shanghai"}','{}','paused','strict')`,
			pausedTaskID, tenantID, user.ID); err != nil {
			t.Fatal(err)
		}
		if _, err := owner.ExecContext(t.Context(), `INSERT INTO schedule_playbooks
			(schedule_id,content,fetch_plan) VALUES($1,'paused runtime shadow test','{}')`,
			pausedTaskID); err != nil {
			t.Fatal(err)
		}
		pausedPolicy := researchV3PreparePolicyForTest()
		pausedPolicy.TenantID, pausedPolicy.UserID, pausedPolicy.TaskID,
			pausedPolicy.IdempotencyKey = tenantID, user.ID, pausedTaskID, "runtime-paused-prepare"
		if _, err := ownerStore.PrepareResearchV3Definition(t.Context(), pausedPolicy); err != nil {
			t.Fatal(err)
		}
		pausedIdentity := identity
		pausedIdentity.TaskID = pausedTaskID
		pausedIdentity.TemporalRunID = "runtime-paused-run-" + uuid.NewString()
		for _, bucket := range []QuotaBucket{QuotaLLMTokens, QuotaExaCalls} {
			quota, err := runtimeStore.LoadResearchQuotaRuleV3(
				t.Context(), pausedIdentity, bucket)
			if err != nil || quota.Rate <= 0 || quota.Burst <= 0 {
				t.Fatalf("paused prepared V3 %s quota=%+v err=%v", bucket, quota, err)
			}
		}
		if _, err := owner.ExecContext(t.Context(),
			`DELETE FROM research_v3_prepared_definition_heads
			  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3`,
			tenantID, user.ID, pausedTaskID); err != nil {
			t.Fatal(err)
		}
		if _, err := runtimeStore.LoadResearchQuotaRuleV3(
			t.Context(), pausedIdentity, QuotaLLMTokens,
		); !errors.Is(err, ErrQuotaExceeded) {
			t.Fatalf("paused task without prepared sidecar quota err=%v", err)
		}
		ref, err := runtimeStore.CreateOrGetResearchRunSnapshotV3(t.Context(), identity,
			testCompiledRunPolicyV1(t), testResearchToolPolicyStoreV3(t),
			testResearchModelPolicyStoreV3(t))
		if err != nil {
			t.Fatalf("runtime V3 snapshot: %v", err)
		}
		loaded, err := runtimeStore.loadControlResearchRunSnapshotRefV3(
			t.Context(), identity, ref.SnapshotID)
		if err != nil || loaded != ref {
			t.Fatalf("runtime control snapshot=%+v err=%v", loaded, err)
		}
		tx, err := runtimeStore.pool.Begin(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(t.Context()) }()
		if _, err := tx.Exec(t.Context(), `SELECT set_config('app.tenant_id',$1,true),
			set_config('app.user_id',$2,true)`, fmt.Sprint(tenantID+1), fmt.Sprint(user.ID)); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(t.Context(), `SELECT * FROM
			resolve_research_run_capability_registration_v1($1,$2,$3,$4,$5,$6,$7)`,
			ref.SnapshotID, tenantID, user.ID, taskID, ref.TemporalWorkflowID,
			ref.TemporalRunID, ref.ReferenceDigest); err == nil {
			t.Fatal("cross-tenant capability resolver bypassed bound app scope")
		}
		_ = tx.Rollback(t.Context())
		tx2, err := runtimeStore.pool.Begin(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx2.Rollback(t.Context()) }()
		if _, err := tx2.Exec(t.Context(), `SELECT set_config('app.tenant_id',$1,true),
			set_config('app.user_id',$2,true)`, fmt.Sprint(tenantID+1), fmt.Sprint(user.ID)); err != nil {
			t.Fatal(err)
		}
		if _, err := tx2.Exec(t.Context(), `SELECT * FROM
			register_research_run_capability_registration_v1(
			$1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`, ref.SnapshotID, tenantID, user.ID,
			taskID, ref.TemporalWorkflowID, ref.TemporalRunID, ref.ReferenceDigest,
			"poison-key", make([]byte, 32), time.Now().Add(24*time.Hour)); err == nil {
			t.Fatal("cross-tenant capability registrar bypassed bound app scope")
		}
	})

	t.Run("schema owner URL is rejected", func(t *testing.T) {
		if st, err := NewServerRuntime(t.Context(), scratchURL); err == nil {
			st.Close()
			t.Fatal("schema-owner URL entered the server runtime constructor")
		}
	})

	t.Run("deprovision refuses a live login", func(t *testing.T) {
		if err := DeprovisionServerRuntime(t.Context(), scratchURL); err == nil {
			t.Fatal("deprovision dropped a LOGIN runtime")
		} else if !strings.Contains(err.Error(), "can login") {
			t.Fatalf("unexpected live-login deprovision error: %v", err)
		}
	})

	t.Run("provision control functions are owner only", func(t *testing.T) {
		runtime, err := sql.Open("pgx", runtimeURL)
		if err != nil {
			t.Fatal(err)
		}
		defer runtime.Close()
		for _, function := range []string{
			"provision_vane_server_runtime_v1",
			"provision_vane_server_runtime_v2",
			"provision_vane_server_runtime_v128",
			"provision_vane_server_runtime_v129",
			"provision_vane_server_runtime_research_binder_v1",
			"retire_agent_session_fact_projector_v128",
			"deprovision_vane_server_runtime_v1",
			"deprovision_vane_server_runtime_v2",
			"deprovision_vane_server_runtime_v128",
			"deprovision_vane_server_runtime_v129",
			"deprovision_vane_server_runtime_research_binder_v1",
			"restore_agent_session_fact_projector_v128",
		} {
			if _, err := runtime.ExecContext(t.Context(),
				"SELECT public."+function+"()"); err == nil {
				t.Fatalf("runtime executed owner-only function %s", function)
			}
		}
	})

	t.Run("extra provider gateway membership is rejected", func(t *testing.T) {
		if _, err := owner.ExecContext(t.Context(),
			`GRANT vane_research_llm_gateway TO vane_server_runtime`); err != nil {
			t.Fatal(err)
		}
		defer func() {
			_, _ = owner.ExecContext(t.Context(),
				`REVOKE vane_research_llm_gateway FROM vane_server_runtime`)
		}()
		if st, err := NewServerRuntime(t.Context(), runtimeURL); err == nil {
			st.Close()
			t.Fatal("server runtime accepted provider gateway membership")
		} else if !strings.Contains(err.Error(), "memberships differ") &&
			!strings.Contains(err.Error(), "forbidden role") {
			t.Fatalf("unexpected gateway membership error: %v", err)
		}
	})

	t.Run("direct protected mutation grant is rejected", func(t *testing.T) {
		if _, err := owner.ExecContext(t.Context(),
			`GRANT INSERT ON research_llm_gateway_attempts TO vane_server_runtime`); err != nil {
			t.Fatal(err)
		}
		defer func() {
			_, _ = owner.ExecContext(t.Context(),
				`REVOKE INSERT ON research_llm_gateway_attempts FROM vane_server_runtime`)
		}()
		if st, err := NewServerRuntime(t.Context(), runtimeURL); err == nil {
			st.Close()
			t.Fatal("server runtime accepted protected direct mutation")
		} else if !strings.Contains(err.Error(), "protected data privileges") {
			t.Fatalf("unexpected protected mutation error: %v", err)
		}
	})

	t.Run("protected mutation drift on default app role is rejected", func(t *testing.T) {
		if _, err := owner.ExecContext(t.Context(),
			`GRANT INSERT ON research_llm_gateway_attempts TO vane_app`); err != nil {
			t.Fatal(err)
		}
		defer func() {
			_, _ = owner.ExecContext(t.Context(),
				`REVOKE INSERT ON research_llm_gateway_attempts FROM vane_app`)
		}()
		if st, err := NewServerRuntime(t.Context(), runtimeURL); err == nil {
			st.Close()
			t.Fatal("server runtime accepted protected vane_app mutation")
		} else if !strings.Contains(err.Error(), "vane_app has direct protected") {
			t.Fatalf("unexpected default-role drift error: %v", err)
		}
	})

	t.Run("gateway function grant is rejected", func(t *testing.T) {
		if _, err := owner.ExecContext(t.Context(),
			`GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO vane_server_runtime`); err != nil {
			t.Fatal(err)
		}
		defer func() {
			_, _ = owner.ExecContext(t.Context(),
				`REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA public FROM vane_server_runtime`)
		}()
		if st, err := NewServerRuntime(t.Context(), runtimeURL); err == nil {
			st.Close()
			t.Fatal("server runtime accepted provider gateway finalizer grant")
		} else if !strings.Contains(err.Error(), "provider gateway functions") {
			t.Fatalf("unexpected gateway function error: %v", err)
		}
	})

	t.Run("owned object is rejected", func(t *testing.T) {
		if _, err := owner.ExecContext(t.Context(),
			`CREATE SCHEMA server_runtime_drift AUTHORIZATION vane_server_runtime`); err != nil {
			t.Fatal(err)
		}
		defer func() {
			_, _ = owner.ExecContext(t.Context(), `DROP SCHEMA server_runtime_drift`)
		}()
		if st, err := NewServerRuntime(t.Context(), runtimeURL); err == nil {
			st.Close()
			t.Fatal("server runtime accepted owned database object")
		} else if !strings.Contains(err.Error(), "is unsafe") {
			t.Fatalf("unexpected ownership error: %v", err)
		}
	})
}

func TestSchemaMigrationsCoexistWithoutServerRuntimeProvisionPostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required")
	}
	admin, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	var existing int
	if err := admin.QueryRowContext(t.Context(), `SELECT count(*)
		FROM pg_catalog.pg_roles WHERE rolname='vane_server_runtime'`).Scan(
		&existing); err != nil {
		t.Fatal(err)
	}
	if existing != 0 {
		t.Skip("cluster must be explicitly deprovisioned before fresh-database migration")
	}

	for _, name := range []string{"first schema", "second schema"} {
		t.Run(name, func(t *testing.T) {
			scratchURL, drop := createScratchDB(t.Context(), t, databaseURL)
			defer drop()
			if err := Migrate(t.Context(), scratchURL); err != nil {
				t.Fatal(err)
			}
			var count int
			if err := admin.QueryRowContext(t.Context(), `SELECT count(*)
				FROM pg_catalog.pg_roles WHERE rolname='vane_server_runtime'`).Scan(
				&count); err != nil {
				t.Fatal(err)
			}
			if count != 0 {
				t.Fatal("ordinary schema migration leaked server runtime into cluster")
			}
		})
	}
}

func serverRuntimeTestURL(t *testing.T, databaseURL, password string) string {
	return roleTestURL(t, databaseURL, serverRuntimeLoginRole, password)
}

func roleTestURL(t *testing.T, databaseURL, role, password string) string {
	t.Helper()
	parsed, err := url.Parse(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	parsed.User = url.UserPassword(role, password)
	return parsed.String()
}
