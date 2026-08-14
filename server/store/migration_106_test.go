package store

import (
	"database/sql"
	"io/fs"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"

	"github.com/YouToco/vane/server/types"
)

func TestMigration106SnapshotFenceACLAndIrreversibleDowngradePostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required")
	}
	scratchURL, drop := createScratchDB(t.Context(), t, databaseURL)
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
	provider, err := goose.NewProvider(goose.DialectPostgres, db, dir,
		goose.WithAllowOutofOrder(true))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.UpTo(t.Context(), 106); err != nil {
		t.Fatal(err)
	}

	var publicExecute, securityDefiner, safeConfig, triggerEnabled, formalRowLock bool
	if err := db.QueryRowContext(t.Context(), `SELECT
		has_function_privilege('public',p.oid,'EXECUTE'),p.prosecdef,
		p.proconfig=ARRAY['search_path=pg_catalog, public, pg_temp']::text[],
		EXISTS (SELECT 1 FROM pg_trigger t
		         WHERE t.tgrelid='task_run_snapshots'::regclass
		           AND t.tgfoid=p.oid AND t.tgenabled='O'),
		position('FOR SHARE OF schedule,tenant,membership,definition'
		         IN pg_get_functiondef(p.oid))>0
		FROM pg_proc p
		WHERE p.oid='task_run_snapshot_v3_admission_fence()'::regprocedure`,
	).Scan(&publicExecute, &securityDefiner, &safeConfig, &triggerEnabled,
		&formalRowLock); err != nil {
		t.Fatal(err)
	}
	if publicExecute || !securityDefiner || !safeConfig || !triggerEnabled || !formalRowLock {
		t.Fatalf("unsafe 106 fence public=%v definer=%v config=%v trigger=%v formal=%v",
			publicExecute, securityDefiner, safeConfig, triggerEnabled, formalRowLock)
	}

	if _, err := provider.DownTo(t.Context(), 105); err == nil ||
		!strings.Contains(err.Error(),
			"refusing downgrade to row-locking shadow snapshot admission") {
		t.Fatalf("106 downgrade unexpectedly succeeded or lost guard: %v", err)
	}
	var version int64
	if err := db.QueryRowContext(t.Context(),
		`SELECT max(version_id) FROM goose_db_version WHERE is_applied`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 106 {
		t.Fatalf("failed 106 downgrade changed version to %d", version)
	}
}

func TestResearchShadowSnapshotSerializesRevocationWithoutDeadlockPostgres(t *testing.T) {
	if os.Getenv("DATABASE_URL") == "" {
		t.Skip("DATABASE_URL is required")
	}
	type mutation struct {
		name      string
		lockSQL   string
		revokeSQL string
		args      func(int64, int64, string) []any
	}
	mutations := []mutation{
		{"membership", `SELECT 1 FROM memberships WHERE tenant_id=$1 AND user_id=$2 FOR UPDATE`,
			`UPDATE memberships SET role='member' WHERE tenant_id=$1 AND user_id=$2`,
			func(tenantID, userID int64, _ string) []any { return []any{tenantID, userID} }},
		{"tenant", `SELECT 1 FROM tenants WHERE id=$1 FOR NO KEY UPDATE`,
			`UPDATE tenants SET status='suspended' WHERE id=$1`,
			func(tenantID, _ int64, _ string) []any { return []any{tenantID} }},
		{"schedule", `SELECT 1 FROM schedules WHERE id=$1 FOR UPDATE`,
			`UPDATE schedules SET status='paused' WHERE id=$1`,
			func(_, _ int64, taskID string) []any { return []any{taskID} }},
		{"prepared head", `SELECT 1 FROM research_v3_prepared_definition_heads
			WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3 FOR UPDATE`,
			`DELETE FROM research_v3_prepared_definition_heads
			WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3`,
			func(tenantID, userID int64, taskID string) []any {
				return []any{tenantID, userID, taskID}
			}},
	}
	for index, test := range mutations {
		t.Run(test.name, func(t *testing.T) {
			st, tenantID, userID, taskID := researchV3PrepareFixture(t)
			policy := testCompiledRunPolicyV1(t)
			tools := testResearchToolPolicyStoreV3(t)
			model := testResearchModelPolicyStoreV3(t)
			prepare := researchV3PreparePolicyForTest()
			prepare.TenantID, prepare.UserID, prepare.TaskID, prepare.IdempotencyKey =
				tenantID, userID, taskID, "snapshot-deadlock-"+strconv.Itoa(index)
			if _, err := st.PrepareResearchV3Definition(t.Context(), prepare); err != nil {
				t.Fatal(err)
			}

			identity := types.RunIdentity{
				TemporalWorkflowID: "research-v3-shadow-" +
					strings.Repeat(strconv.Itoa(index+1), 64),
				TemporalRunID: "snapshot-deadlock-" + uuid.NewString(),
				RunKind:       types.RunSnapshotKindScheduled, TenantID: tenantID,
				UserID: userID, TaskID: taskID,
			}
			ctx := t.Context()
			gate, err := st.pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer gate.Rollback(ctx)
			if _, err := gate.Exec(ctx,
				`SELECT pg_advisory_xact_lock(hashtextextended($1,$2))`,
				identity.TemporalRunID, taskRunSnapshotLockSeed); err != nil {
				t.Fatal(err)
			}
			var gatePID int
			if err := gate.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&gatePID); err != nil {
				t.Fatal(err)
			}

			revoke, err := st.pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer revoke.Rollback(ctx)
			var revokePID int
			if err := revoke.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&revokePID); err != nil {
				t.Fatal(err)
			}
			args := test.args(tenantID, userID, taskID)
			var one int
			if err := revoke.QueryRow(ctx, test.lockSQL, args...).Scan(&one); err != nil {
				t.Fatal(err)
			}

			type snapshotResult struct {
				ref types.ResearchRunSnapshotRefV3
				err error
			}
			created := make(chan snapshotResult, 1)
			go func() {
				ref, createErr := st.CreateOrGetResearchRunSnapshotV3(
					ctx, identity, policy, tools, model)
				created <- snapshotResult{ref: ref, err: createErr}
			}()
			waitForResearchSnapshotRunLockV3(t, st, gatePID, identity.TemporalRunID)

			revoked := make(chan error, 1)
			go func() {
				_, revokeErr := revoke.Exec(ctx, test.revokeSQL, args...)
				revoked <- revokeErr
			}()
			waitForBackendAdvisoryLockV3(t, st, revokePID,
				test.name+" snapshot revocation")
			if err := gate.Rollback(ctx); err != nil {
				t.Fatal(err)
			}

			select {
			case got := <-created:
				if got.err != nil || got.ref.SnapshotID <= 0 || got.ref.AuthorityGeneration != 0 {
					t.Fatalf("shadow snapshot during %s revocation=%+v err=%v",
						test.name, got.ref, got.err)
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("shadow snapshot deadlocked with %s revocation", test.name)
			}
			select {
			case err := <-revoked:
				if err != nil {
					t.Fatalf("%s snapshot revocation deadlocked: %v", test.name, err)
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("%s snapshot revocation did not serialize", test.name)
			}
			if err := revoke.Commit(ctx); err != nil {
				t.Fatal(err)
			}

			identity.TemporalWorkflowID = "research-v3-shadow-" +
				strings.Repeat(strconv.Itoa(index+5), 64)
			identity.TemporalRunID = "snapshot-revoked-" + uuid.NewString()
			if ref, err := st.CreateOrGetResearchRunSnapshotV3(
				ctx, identity, policy, tools, model); err == nil || ref.SnapshotID != 0 {
				t.Fatalf("%s revocation admitted later snapshot=%+v err=%v",
					test.name, ref, err)
			}
			var count int
			if err := st.pool.QueryRow(ctx, `SELECT count(*) FROM task_run_snapshots
				WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3`,
				tenantID, userID, taskID).Scan(&count); err != nil || count != 1 {
				t.Fatalf("%s revocation snapshot count=%d err=%v", test.name, count, err)
			}
		})
	}
}

func TestMigration106RawRestrictedSnapshotInsertUsesTriggerFencePostgres(t *testing.T) {
	if os.Getenv("DATABASE_URL") == "" {
		t.Skip("DATABASE_URL is required")
	}
	st, tenantID, userID, taskID := researchV3PrepareFixture(t)
	prepare := researchV3PreparePolicyForTest()
	prepare.TenantID, prepare.UserID, prepare.TaskID, prepare.IdempotencyKey =
		tenantID, userID, taskID, "raw-trigger-fence"
	if _, err := st.PrepareResearchV3Definition(t.Context(), prepare); err != nil {
		t.Fatal(err)
	}
	policy := testCompiledRunPolicyV1(t)
	tools := testResearchToolPolicyStoreV3(t)
	model := testResearchModelPolicyStoreV3(t)
	templateIdentity := types.RunIdentity{
		TemporalWorkflowID: "research-v3-shadow-" + strings.Repeat("a", 64),
		TemporalRunID:      "raw-template-" + uuid.NewString(),
		RunKind:            types.RunSnapshotKindScheduled,
		TenantID:           tenantID, UserID: userID, TaskID: taskID,
	}
	template, err := st.CreateOrGetResearchRunSnapshotV3(
		t.Context(), templateIdentity, policy, tools, model)
	if err != nil {
		t.Fatal(err)
	}

	const gateKey = "test/migration106/raw-shadow-snapshot-gate"
	if _, err := st.pool.Exec(t.Context(), `
		CREATE FUNCTION test_pause_raw_shadow_snapshot_106() RETURNS trigger
		LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
		BEGIN
		    PERFORM pg_advisory_xact_lock(hashtextextended(
		        'test/migration106/raw-shadow-snapshot-gate',0));
		    RETURN NEW;
		END $$;
		CREATE TRIGGER zz_test_pause_raw_shadow_snapshot_106
		BEFORE INSERT ON task_run_snapshots
		FOR EACH ROW WHEN (NEW.reference_schema_version=
		    'vane.research-run-snapshot-ref/v3')
		EXECUTE FUNCTION test_pause_raw_shadow_snapshot_106()`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		if _, err := st.pool.Exec(ctx, `
			DROP TRIGGER IF EXISTS zz_test_pause_raw_shadow_snapshot_106
			    ON task_run_snapshots;
			DROP FUNCTION IF EXISTS test_pause_raw_shadow_snapshot_106()`); err != nil {
			t.Errorf("drop raw snapshot gate: %v", err)
		}
	})

	ctx := t.Context()
	gate, err := st.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer gate.Rollback(ctx)
	if _, err := gate.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, gateKey); err != nil {
		t.Fatal(err)
	}
	var gatePID int
	if err := gate.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&gatePID); err != nil {
		t.Fatal(err)
	}

	revoke, err := st.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer revoke.Rollback(ctx)
	var revokePID int
	if err := revoke.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&revokePID); err != nil {
		t.Fatal(err)
	}
	var one int
	if err := revoke.QueryRow(ctx, `SELECT 1 FROM memberships
		WHERE tenant_id=$1 AND user_id=$2 FOR UPDATE`, tenantID, userID).Scan(&one); err != nil {
		t.Fatal(err)
	}

	raw, err := st.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Rollback(ctx)
	if _, err := raw.Exec(ctx, `SELECT set_config('app.tenant_id',$1,true),
		set_config('app.user_id',$2,true)`, strconv.FormatInt(tenantID, 10),
		strconv.FormatInt(userID, 10)); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(ctx, `SET LOCAL ROLE vane_app`); err != nil {
		t.Fatal(err)
	}
	var rawPID int
	if err := raw.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&rawPID); err != nil {
		t.Fatal(err)
	}
	rawWorkflow := "research-v3-shadow-" + strings.Repeat("b", 64)
	rawRun := "raw-trigger-" + uuid.NewString()
	rawInserted := make(chan error, 1)
	go func() {
		_, insertErr := raw.Exec(ctx, `INSERT INTO task_run_snapshots (
			tenant_id,user_id,task_id,temporal_workflow_id,temporal_run_id,
			run_kind,execution_mode,adaptive_version,capability_catalog_digest,
			tool_policy_digest,prompt_policy_digest,model_policy_digest,
			quota_policy_digest,definition_digest,plan_digest,payload_digest,
			reference_digest,reference_schema_version,payload,budget,v2_cutover_event_id
		) SELECT tenant_id,user_id,task_id,$1,$2,run_kind,execution_mode,
			adaptive_version,capability_catalog_digest,tool_policy_digest,
			prompt_policy_digest,model_policy_digest,quota_policy_digest,
			definition_digest,plan_digest,payload_digest,reference_digest,
			reference_schema_version,payload,budget,v2_cutover_event_id
		FROM task_run_snapshots WHERE id=$3`, rawWorkflow, rawRun, template.SnapshotID)
		rawInserted <- insertErr
	}()
	waitForBackendBlockedByPIDV3(t, st, rawPID, gatePID,
		"restricted raw snapshot trigger gate")

	revoked := make(chan error, 1)
	go func() {
		_, revokeErr := revoke.Exec(ctx, `UPDATE memberships SET role='member'
			WHERE tenant_id=$1 AND user_id=$2`, tenantID, userID)
		revoked <- revokeErr
	}()
	waitForBackendAdvisoryLockV3(t, st, revokePID,
		"restricted raw snapshot revocation")
	if err := gate.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-rawInserted:
		if err != nil {
			t.Fatalf("restricted raw snapshot insert: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("restricted raw snapshot insert did not leave its gate")
	}
	if err := raw.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-revoked:
		if err != nil {
			t.Fatalf("restricted raw snapshot revocation: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("restricted raw snapshot revocation did not serialize")
	}
	if err := revoke.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	var count int
	if err := st.pool.QueryRow(ctx, `SELECT count(*) FROM task_run_snapshots
		WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3 AND temporal_run_id=$4`,
		tenantID, userID, taskID, rawRun).Scan(&count); err != nil || count != 1 {
		t.Fatalf("restricted raw snapshot count=%d err=%v", count, err)
	}
	denied, err := st.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer denied.Rollback(ctx)
	if _, err := denied.Exec(ctx, `SELECT set_config('app.tenant_id',$1,true),
		set_config('app.user_id',$2,true)`, strconv.FormatInt(tenantID, 10),
		strconv.FormatInt(userID, 10)); err != nil {
		t.Fatal(err)
	}
	if _, err := denied.Exec(ctx, `SET LOCAL ROLE vane_app`); err != nil {
		t.Fatal(err)
	}
	if _, err := denied.Exec(ctx, `INSERT INTO task_run_snapshots (
		tenant_id,user_id,task_id,temporal_workflow_id,temporal_run_id,
		run_kind,execution_mode,adaptive_version,capability_catalog_digest,
		tool_policy_digest,prompt_policy_digest,model_policy_digest,
		quota_policy_digest,definition_digest,plan_digest,payload_digest,
		reference_digest,reference_schema_version,payload,budget,v2_cutover_event_id
	) SELECT tenant_id,user_id,task_id,$1,$2,run_kind,execution_mode,
		adaptive_version,capability_catalog_digest,tool_policy_digest,
		prompt_policy_digest,model_policy_digest,quota_policy_digest,
		definition_digest,plan_digest,payload_digest,reference_digest,
		reference_schema_version,payload,budget,v2_cutover_event_id
	FROM task_run_snapshots WHERE id=$3`,
		"research-v3-shadow-"+strings.Repeat("c", 64),
		"raw-denied-"+uuid.NewString(), template.SnapshotID); err == nil {
		t.Fatal("restricted raw snapshot escaped committed owner revocation")
	}
}

func waitForBackendBlockedByPIDV3(
	t *testing.T, st *Store, pid, blockerPID int, label string,
) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var waiting bool
		if err := st.pool.QueryRow(t.Context(), `SELECT EXISTS (
			SELECT 1 FROM pg_stat_activity
			 WHERE datname=current_database() AND pid=$1
			   AND wait_event_type='Lock' AND wait_event='advisory'
			   AND $2=ANY(pg_blocking_pids(pid))
		)`, pid, blockerPID).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%s did not reach its advisory gate", label)
}

func waitForResearchSnapshotRunLockV3(
	t *testing.T, st *Store, blockerPID int, temporalRunID string,
) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var waiting bool
		if err := st.pool.QueryRow(t.Context(), `SELECT EXISTS (
			SELECT 1 FROM pg_stat_activity
			 WHERE datname=current_database()
			   AND query LIKE '%pg_advisory_xact_lock(hashtextextended($1,$2))%'
			   AND query NOT LIKE '%pg_stat_activity%'
			   AND wait_event_type='Lock' AND wait_event='advisory'
			   AND $1=ANY(pg_blocking_pids(pid))
		)`, blockerPID).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("research snapshot for %s did not reach its run lock", temporalRunID)
}

func TestMigration106SQLKeepsFormalAndShadowSnapshotAuthorizationOrthogonal(t *testing.T) {
	payload, err := fs.ReadFile(migrationsFS,
		"migrations/106_research_shadow_snapshot_admission.sql")
	if err != nil {
		t.Fatal(err)
	}
	sqlText := string(payload)
	for _, required := range []string{
		"pg_advisory_xact_lock(6215335020355474248)",
		"LOCK TABLE task_run_snapshots IN ACCESS EXCLUSIVE MODE",
		"^research-v3-shadow-[0-9a-f]{64}$",
		"pg_advisory_xact_lock_shared(hashtextextended(",
		"membership.role='owner'",
		"tenant.status='active'",
		"research_v3_prepared_definition_heads",
		"FOR SHARE OF schedule,tenant,membership,definition",
		"REVOKE ALL ON FUNCTION task_run_snapshot_v3_admission_fence() FROM PUBLIC",
		"refusing downgrade to row-locking shadow snapshot admission",
	} {
		if !strings.Contains(sqlText, required) {
			t.Fatalf("106 migration omitted %q", required)
		}
	}
	shadowStart := strings.Index(sqlText, "IF is_shadow THEN")
	formalStart := strings.Index(sqlText[shadowStart:], "ELSE")
	if shadowStart < 0 || formalStart < 0 {
		t.Fatal("106 migration lost shadow/formal branches")
	}
	shadowBlock := sqlText[shadowStart : shadowStart+formalStart]
	if strings.Contains(shadowBlock, "FOR SHARE") {
		t.Fatal("shadow snapshot authorization reintroduced advisory-to-row lock inversion")
	}
	legacyPayload, err := fs.ReadFile(migrationsFS,
		"migrations/102_research_v3_definition_prepare.sql")
	if err != nil {
		t.Fatal(err)
	}
	formalSelectStart := "        SELECT schedule.status,schedule.execution_mode," +
		"schedule.approved_definition_digest,"
	formalEnd := "    RETURN NEW;"
	extractFormal := func(label, payload string) string {
		t.Helper()
		start := strings.Index(payload, formalSelectStart)
		if start < 0 {
			t.Fatalf("%s lost formal snapshot SELECT", label)
		}
		end := strings.Index(payload[start:], formalEnd)
		if end < 0 {
			t.Fatalf("%s lost formal snapshot admission tail", label)
		}
		return strings.ReplaceAll(
			payload[start:start+end+len(formalEnd)], "\r\n", "\n")
	}
	if oldFormal, newFormal := extractFormal("102", string(legacyPayload)),
		extractFormal("106", sqlText); oldFormal != newFormal {
		t.Fatal("106 changed migration 102's formal snapshot authorization or error branch")
	}
}
