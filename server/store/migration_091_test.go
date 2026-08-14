package store

import (
	"database/sql"
	"io/fs"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/pressly/goose/v3"
)

func TestMigration091LLMSpendBoundaryAndEmptyDown(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required for migration 091 integration tests")
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
	provider, err := goose.NewProvider(goose.DialectPostgres, database, dir,
		goose.WithAllowOutofOrder(true))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.UpTo(t.Context(), 91); err != nil {
		t.Fatal(err)
	}

	for _, table := range []string{
		"research_run_llm_spend_reservations",
		"research_run_llm_spend_settlements",
	} {
		var rls, tableInsert, update, deletePrivilege, truncate bool
		if err := database.QueryRowContext(t.Context(), `
			SELECT c.relrowsecurity,
			       has_table_privilege('vane_research_v3_executor',$1,'INSERT'),
			       has_table_privilege('vane_research_v3_executor',$1,'UPDATE'),
			       has_table_privilege('vane_research_v3_executor',$1,'DELETE'),
			       has_table_privilege('vane_research_v3_executor',$1,'TRUNCATE')
			  FROM pg_class c WHERE c.oid=$1::regclass`, table,
		).Scan(&rls, &tableInsert, &update, &deletePrivilege, &truncate); err != nil {
			t.Fatal(err)
		}
		if !rls || tableInsert || update || deletePrivilege || truncate {
			t.Fatalf("%s privilege boundary rls=%v insert=%v update=%v delete=%v truncate=%v",
				table, rls, tableInsert, update, deletePrivilege, truncate)
		}
	}

	var reserveInsert, settleInsert, llmBindInsert, llmBinding, llmBindingUnique bool
	if err := database.QueryRowContext(t.Context(), `
		SELECT has_column_privilege(
		           'vane_research_v3_executor','research_run_llm_spend_reservations',
		           'reserved_quota_tokens','INSERT'),
		       has_column_privilege(
		           'vane_research_v3_executor','research_run_llm_spend_settlements',
		           'actual_prompt_tokens','INSERT'),
		       has_column_privilege(
		           'vane_research_v3_executor','llm_calls',
		           'research_run_llm_spend_reservation_id','INSERT'),
		       EXISTS (SELECT 1 FROM information_schema.columns
		                WHERE table_schema='public' AND table_name='llm_calls'
		                  AND column_name='research_run_llm_spend_reservation_id'),
		       to_regclass('public.uq_llm_calls_research_llm_spend_reservation') IS NOT NULL`,
	).Scan(&reserveInsert, &settleInsert, &llmBindInsert, &llmBinding, &llmBindingUnique); err != nil {
		t.Fatal(err)
	}
	if reserveInsert || settleInsert || llmBindInsert || !llmBinding || !llmBindingUnique {
		t.Fatalf("narrow LLM spend grants reserve=%v settle=%v bind_insert=%v binding=%v unique=%v",
			reserveInsert, settleInsert, llmBindInsert, llmBinding, llmBindingUnique)
	}
	var appTableInsert, appBindingInsert, appLegacyTraceInsert, llmScoped bool
	if err := database.QueryRowContext(t.Context(), `
		SELECT has_table_privilege('vane_app','llm_calls','INSERT'),
		       has_column_privilege('vane_app','llm_calls',
		           'research_run_llm_spend_reservation_id','INSERT'),
		       has_column_privilege('vane_app','llm_calls','trace_id','INSERT'),
		       EXISTS (
		           SELECT 1
		             FROM pg_catalog.pg_policy policy
		             JOIN pg_catalog.pg_class class ON class.oid=policy.polrelid
		             JOIN pg_catalog.pg_roles role ON role.oid=ANY(policy.polroles)
		            WHERE class.oid='llm_calls'::regclass
		              AND policy.polname='research_v3_scope'
		              AND policy.polpermissive=false
		              AND role.rolname='vane_research_v3_executor'
		       )`,
	).Scan(&appTableInsert, &appBindingInsert, &appLegacyTraceInsert,
		&llmScoped); err != nil {
		t.Fatal(err)
	}
	if appTableInsert || appBindingInsert || !appLegacyTraceInsert || !llmScoped {
		t.Fatalf("legacy/V3 llm_calls boundary table_insert=%v binding_insert=%v legacy_trace_insert=%v scoped=%v",
			appTableInsert, appBindingInsert, appLegacyTraceInsert, llmScoped)
	}

	rows, err := database.QueryContext(t.Context(), `
		SELECT p.proname,has_function_privilege('public',p.oid,'EXECUTE'),
		       p.proowner::regrole::text,
		       p.proconfig=ARRAY['search_path=pg_catalog, public, pg_temp']::text[]
		  FROM pg_proc p
		 WHERE p.proname IN (
		     'enforce_research_run_llm_spend_reservation_v1',
		     'enforce_research_run_shared_planner_budget_v3',
		     'admit_research_run_llm_spend_v3',
		     'protect_bound_research_llm_call_v1',
		     'enforce_research_run_llm_spend_settlement_v1',
		     'reconcile_research_run_llm_quota_v3',
		     'protect_research_run_llm_spend_v1'
		     ,'settle_research_run_llm_spend_v3'
		 ) ORDER BY p.proname`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var name, owner string
		var publicExecute, safeConfig bool
		if err := rows.Scan(&name, &publicExecute, &owner, &safeConfig); err != nil {
			t.Fatal(err)
		}
		if publicExecute || owner == "vane_app" || !safeConfig {
			t.Fatalf("unsafe 091 function %s public=%v owner=%q config=%v",
				name, publicExecute, owner, safeConfig)
		}
		count++
	}
	if err := rows.Err(); err != nil || count != 8 {
		t.Fatalf("091 function count=%d err=%v", count, err)
	}

	var admitExecute, settleExecute, oldReserveAbsent, quotaUpdate, appRead bool
	var priceInsert, priceUpdate, priceDelete, priceTruncate bool
	if err := database.QueryRowContext(t.Context(), `
		SELECT has_function_privilege(
		           'vane_research_v3_executor',
		           'admit_research_run_llm_spend_v3(bigint,bigint,text,bigint,text,integer,bigint,text,text,text,text)',
		           'EXECUTE'),
		       has_function_privilege(
		           'vane_research_v3_executor',
		           'settle_research_run_llm_spend_v3(bigint,bigint,text,bigint,bigint,text,text,text,text,integer,integer,integer,integer,integer,integer,boolean,real,integer,boolean,text,boolean,boolean,boolean,text,text)',
		           'EXECUTE'),
		       to_regprocedure('reserve_research_run_llm_quota_v3(bigint,bigint)') IS NULL,
		       has_table_privilege('vane_research_v3_executor','tenant_quota','UPDATE'),
		       has_table_privilege('vane_app','research_run_llm_spend_reservations','SELECT'),
		       has_table_privilege('vane_research_v3_executor','provider_price_rules','INSERT'),
		       has_table_privilege('vane_research_v3_executor','provider_price_rules','UPDATE'),
		       has_table_privilege('vane_research_v3_executor','provider_price_rules','DELETE'),
		       has_table_privilege('vane_research_v3_executor','provider_price_rules','TRUNCATE')`,
	).Scan(&admitExecute, &settleExecute, &oldReserveAbsent, &quotaUpdate, &appRead,
		&priceInsert, &priceUpdate, &priceDelete, &priceTruncate); err != nil {
		t.Fatal(err)
	}
	if !admitExecute || !settleExecute || !oldReserveAbsent || quotaUpdate || !appRead ||
		priceInsert || priceUpdate || priceDelete || priceTruncate {
		t.Fatalf("spend boundary admit=%v settle=%v old_absent=%v direct_quota_update=%v legacy_read=%v price_dml=%v/%v/%v/%v",
			admitExecute, settleExecute, oldReserveAbsent, quotaUpdate, appRead,
			priceInsert, priceUpdate, priceDelete, priceTruncate)
	}

	if _, err := provider.DownTo(t.Context(), 90); err != nil {
		t.Fatalf("empty 091 Down: %v", err)
	}
	var removed bool
	if err := database.QueryRowContext(t.Context(), `
		SELECT to_regclass('public.research_run_llm_spend_reservations') IS NULL
		   AND to_regclass('public.research_run_llm_spend_settlements') IS NULL
		   AND NOT EXISTS (
		       SELECT 1 FROM information_schema.columns
		        WHERE table_schema='public' AND table_name='llm_calls'
		          AND column_name IN ('research_run_llm_spend_reservation_id','disable_thinking')
		   )`,
	).Scan(&removed); err != nil {
		t.Fatal(err)
	}
	if !removed {
		t.Fatal("091 Down retained LLM spend schema")
	}
	rolledBackStore, err := New(t.Context(), scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rolledBackStore.Close)
	tenantID := seedPurgeTenant(t, rolledBackStore)
	var legacyUserID int64
	if err := rolledBackStore.pool.QueryRow(t.Context(), `
		SELECT user_id FROM memberships WHERE tenant_id=$1 ORDER BY user_id LIMIT 1`,
		tenantID).Scan(&legacyUserID); err != nil {
		t.Fatal(err)
	}
	var legacyCallID int64
	if err := rolledBackStore.pool.QueryRow(t.Context(), `
		INSERT INTO llm_calls(trace_id,span_name,user_id,provider,model,tenant_id)
		VALUES('m091-rollback-legacy','legacy', $1,'legacy','legacy',$2)
		RETURNING id`, legacyUserID, tenantID).Scan(&legacyCallID); err != nil {
		t.Fatalf("seed legacy llm_call after 091 rollback: %v", err)
	}
	report, err := rolledBackStore.PurgeTenant(t.Context(), tenantID, false)
	if err != nil {
		t.Fatalf("current binary cannot purge after 091 rollback: %v", err)
	}
	if report.Rows["llm_calls"] != 1 {
		t.Fatalf("rollback purge llm_calls=%d, want 1", report.Rows["llm_calls"])
	}
	var legacyRemaining int
	if err := rolledBackStore.pool.QueryRow(t.Context(),
		`SELECT count(*) FROM llm_calls WHERE id=$1`, legacyCallID,
	).Scan(&legacyRemaining); err != nil || legacyRemaining != 0 {
		t.Fatalf("legacy llm_call survived rollback purge count=%d err=%v",
			legacyRemaining, err)
	}
}

func TestMigration091SQLContainsAtomicFences(t *testing.T) {
	payload, err := fs.ReadFile(migrationsFS,
		"migrations/091_research_run_llm_spend.sql")
	if err != nil {
		t.Fatal(err)
	}
	sqlText := string(payload)
	for _, required := range []string{
		"pg_advisory_xact_lock(6215335020355474248)",
		"FOR UPDATE",
		"research_run_llm_spend_reservations",
		"research_run_llm_spend_settlements",
		"research_run_llm_spend_reservation_id",
		"uq_llm_calls_research_llm_spend_reservation",
		"enforce_research_run_shared_planner_budget_v3",
		"reserved_planner_tokens",
		"reserved_completion_tokens",
		"pricing.resource=reservation.model",
		"admit_research_run_llm_spend_v3",
		"settle_research_run_llm_spend_v3",
		"system_prompt_digest",
		"user_prompt_digest",
		"disable_thinking",
		"reconcile_research_run_llm_quota_v3",
		"V3-bound LLM call is immutable",
		"ON DELETE CASCADE",
		"AS RESTRICTIVE FOR ALL TO vane_research_v3_executor",
		"CREATE POLICY research_v3_scope ON llm_calls",
		"GREATEST(",
		"can never refund the conservative reservation",
		"synthesis prompt differs from frozen context",
		"octet_length(requested_completion)>8192",
		"octet_length(requested_error)>4096",
		"octet_length(requested_provider_reported_model) NOT BETWEEN 1 AND 255",
		"refusing downgrade while V3 LLM spend authority or evidence exists",
	} {
		if !strings.Contains(sqlText, required) {
			t.Fatalf("091 migration omitted %q", required)
		}
	}
}

func TestMigration091ExecutorCanInsertExactBoundLLMCall(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required for migration 091 integration tests")
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
	provider, err := goose.NewProvider(goose.DialectPostgres, database, dir,
		goose.WithAllowOutofOrder(true))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.UpTo(t.Context(), 91); err != nil {
		t.Fatal(err)
	}

	var pricingProvider, model string
	if err := database.QueryRowContext(t.Context(), `
		SELECT provider,resource
		  FROM provider_price_rules
		 WHERE meter='llm_tokens' AND currency='USD'
		 ORDER BY id LIMIT 1`,
	).Scan(&pricingProvider, &model); err != nil {
		t.Fatal(err)
	}
	sha := strings.Repeat("a", 64)
	tx, err := database.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `SET LOCAL session_replication_role=replica`); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	var tenantID, userID, otherUserID, snapshotID, synthesisID, reservationID int64
	if err := tx.QueryRowContext(t.Context(),
		`INSERT INTO tenants(status,plan) VALUES('active','free') RETURNING id`,
	).Scan(&tenantID); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.QueryRowContext(t.Context(), `
		INSERT INTO users(feishu_open_id,name)
		VALUES('m091-executor','m091') RETURNING id`,
	).Scan(&userID); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `
		INSERT INTO memberships(tenant_id,user_id,role) VALUES($1,$2,'owner')`,
		tenantID, userID); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.QueryRowContext(t.Context(), `
		INSERT INTO users(feishu_open_id,name)
		VALUES('m091-executor-other','m091-other') RETURNING id`,
	).Scan(&otherUserID); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `
		INSERT INTO memberships(tenant_id,user_id,role) VALUES($1,$2,'member')`,
		tenantID, otherUserID); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `
		INSERT INTO tenant_quota(tenant_id,bucket,tokens,rate,burst)
		VALUES($1,'llm_tokens',1000000,0,1000000)`, tenantID); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.QueryRowContext(t.Context(), `
		INSERT INTO task_run_snapshots(
		    tenant_id,user_id,task_id,temporal_workflow_id,temporal_run_id,
		    run_kind,execution_mode,adaptive_version,capability_catalog_digest,
		    tool_policy_digest,prompt_policy_digest,model_policy_digest,
		    quota_policy_digest,definition_digest,plan_digest,payload_digest,
		    reference_digest,reference_schema_version,payload,budget
		) VALUES($1,$2,'m091-task','m091-wf','m091-run','scheduled',
		         'discover_at_run',0,$3,$3,$3,$3,$3,$3,'',$3,$3,
		         'vane.research-run-snapshot-ref/v3',
		         convert_to(jsonb_build_object(
		           'research_model',jsonb_build_object(
		             'provider',$4::text,'quota_bucket','llm_tokens',
		             'planner',jsonb_build_object(
		               'model',$5::text,'max_tokens',100,'system_prompt','system',
		               'temperature',0.1,'disable_thinking',true),
		             'synthesis',jsonb_build_object(
		               'model',$5::text,'max_tokens',100,'system_prompt','system',
		               'temperature',0.1,'disable_thinking',true)),
		           'planner_budget',jsonb_build_object(
		             'max_planner_rounds',8,'max_tool_calls',16,
		             'max_tokens',32768,'max_cost_micro_usd',1000000)
		         )::text,'UTF8'),'{}') RETURNING id`,
		tenantID, userID, sha, pricingProvider, model).Scan(&snapshotID); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.QueryRowContext(t.Context(), `
		INSERT INTO research_brief_syntheses(
		    tenant_id,user_id,task_id,run_snapshot_id,plan_id,
		    temporal_workflow_id,temporal_run_id,definition_digest,plan_digest,
		    notification_threshold,request_digest,context_payload,context_digest,
		    evidence_manifest,evidence_digest,history_manifest,history_digest,
		    schema_version
		) VALUES (
		    $1,$2,'m091-task',$3,424242,'m091-wf','m091-synthesis-run',$4,$4,
		    'major_updates_only',$4,convert_to('{"frozen":true}','UTF8'),
		    encode(sha256(convert_to('{"frozen":true}','UTF8')),'hex'),
		    convert_to('{}','UTF8'),encode(sha256(convert_to('{}','UTF8')),'hex'),
		    convert_to('{}','UTF8'),encode(sha256(convert_to('{}','UTF8')),'hex'),
		    'vane.research-brief-synthesis/v3'
		) RETURNING id`, tenantID, userID, snapshotID, sha,
	).Scan(&synthesisID); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `
		INSERT INTO llm_calls(trace_id,span_name,user_id,provider,model,tenant_id)
		VALUES('m091-other-user','legacy',$1,'legacy','legacy',$2)`,
		otherUserID, tenantID); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	type admitResult struct {
		id    int64
		first bool
		err   error
	}
	results := make(chan admitResult, 8)
	shaC := strings.Repeat("c", 64)
	for range 8 {
		go func() {
			workerTx, beginErr := database.BeginTx(t.Context(), nil)
			if beginErr != nil {
				results <- admitResult{err: beginErr}
				return
			}
			defer func() { _ = workerTx.Rollback() }()
			if _, roleErr := workerTx.ExecContext(t.Context(),
				`SELECT set_config('app.tenant_id',$1,true),set_config('app.user_id',$2,true)`,
				strconv.FormatInt(tenantID, 10), strconv.FormatInt(userID, 10)); roleErr != nil {
				results <- admitResult{err: roleErr}
				return
			}
			if _, roleErr := workerTx.ExecContext(t.Context(),
				`SET LOCAL ROLE vane_research_v3_executor`); roleErr != nil {
				results <- admitResult{err: roleErr}
				return
			}
			var result admitResult
			result.err = workerTx.QueryRowContext(t.Context(), `
				SELECT out_reservation_id,out_first_writer
				  FROM admit_research_run_llm_spend_v3(
				    $1,$2,'m091-task',$3,'planner',2,0,$4,$4,'m091-trace-3','user3')`,
				tenantID, userID, snapshotID, shaC,
			).Scan(&result.id, &result.first)
			if result.err == nil {
				result.err = workerTx.Commit()
			}
			results <- result
		}()
	}
	var concurrentReservationID int64
	firstWriters := 0
	for range 8 {
		result := <-results
		if result.err != nil {
			t.Fatal(result.err)
		}
		if concurrentReservationID == 0 {
			concurrentReservationID = result.id
		} else if result.id != concurrentReservationID {
			t.Fatalf("concurrent admission ids differ: %d vs %d",
				result.id, concurrentReservationID)
		}
		if result.first {
			firstWriters++
		}
	}
	if firstWriters != 1 {
		t.Fatalf("concurrent admission first writers=%d, want 1", firstWriters)
	}

	tx, err = database.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(t.Context(), `SELECT set_config('app.tenant_id',$1,true),set_config('app.user_id',$2,true)`,
		strconv.FormatInt(tenantID, 10), strconv.FormatInt(userID, 10)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `SET LOCAL ROLE vane_research_v3_executor`); err != nil {
		t.Fatal(err)
	}
	var crossUserVisible int
	if err := tx.QueryRowContext(t.Context(), `
		SELECT count(user_id) FROM llm_calls WHERE user_id=$1`, otherUserID,
	).Scan(&crossUserVisible); err != nil {
		t.Fatal(err)
	}
	if crossUserVisible != 0 {
		t.Fatalf("executor read %d cross-user llm_calls", crossUserVisible)
	}
	if _, err := tx.ExecContext(t.Context(), `SAVEPOINT direct_reservation_insert`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `
		INSERT INTO research_run_llm_spend_reservations(tenant_id)
		VALUES($1)`, tenantID); err == nil || !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("direct reservation insert err=%v", err)
	}
	if _, err := tx.ExecContext(t.Context(), `ROLLBACK TO SAVEPOINT direct_reservation_insert`); err != nil {
		t.Fatal(err)
	}
	var firstWriter bool
	if err := tx.QueryRowContext(t.Context(), `
		SELECT out_reservation_id,out_first_writer
		  FROM admit_research_run_llm_spend_v3(
		    $1,$2,'m091-task',$3,'planner',0,0,$4,$4,'m091-trace',
		    'user')`,
		tenantID, userID, snapshotID, sha,
	).Scan(&reservationID, &firstWriter); err != nil {
		t.Fatal(err)
	}
	if !firstWriter {
		t.Fatal("first atomic admission did not win first-writer authority")
	}
	if _, err := tx.ExecContext(t.Context(), `SAVEPOINT legacy_binding_dos`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `SET LOCAL ROLE vane_app`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `
		INSERT INTO llm_calls(
		    trace_id,span_name,user_id,provider,model,tenant_id,
		    research_run_llm_spend_reservation_id
		) VALUES ('legacy-dos','legacy',$1,'legacy','legacy',$2,$3)`,
		userID, tenantID, reservationID,
	); err == nil || !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("vane_app can preempt V3 llm binding: %v", err)
	}
	if _, err := tx.ExecContext(t.Context(), `ROLLBACK TO SAVEPOINT legacy_binding_dos`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `SET LOCAL ROLE vane_research_v3_executor`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `SAVEPOINT synthesis_prompt_substitution`); err != nil {
		t.Fatal(err)
	}
	var rejectedSynthesisID int64
	err = tx.QueryRowContext(t.Context(), `
		SELECT out_reservation_id
		  FROM admit_research_run_llm_spend_v3(
		    $1,$2,'m091-task',$3,'synthesis',0,$4,$5,$5,
		    'm091-synthesis-trace','{"tampered":true}')`,
		tenantID, userID, snapshotID, synthesisID, strings.Repeat("d", 64),
	).Scan(&rejectedSynthesisID)
	if err == nil || !strings.Contains(err.Error(), "differs from frozen context") {
		t.Fatalf("arbitrary synthesis prompt admission err=%v id=%d", err, rejectedSynthesisID)
	}
	if _, err := tx.ExecContext(t.Context(), `ROLLBACK TO SAVEPOINT synthesis_prompt_substitution`); err != nil {
		t.Fatal(err)
	}
	var replayReservationID int64
	if err := tx.QueryRowContext(t.Context(), `
		SELECT out_reservation_id,out_first_writer
		  FROM admit_research_run_llm_spend_v3(
		    $1,$2,'m091-task',$3,'planner',0,0,$4,$4,'m091-trace',
		    'user')`,
		tenantID, userID, snapshotID, sha,
	).Scan(&replayReservationID, &firstWriter); err != nil {
		t.Fatal(err)
	}
	if firstWriter || replayReservationID != reservationID {
		t.Fatalf("admission replay id=%d first=%v, want %d/false",
			replayReservationID, firstWriter, reservationID)
	}
	for name, statement := range map[string]string{
		"llm_call":     `INSERT INTO llm_calls(trace_id) VALUES('forged-zero')`,
		"settlement":   `INSERT INTO research_run_llm_spend_settlements(tenant_id) VALUES(1)`,
		"price_update": `UPDATE provider_price_rules SET note='forged' WHERE id=1`,
	} {
		if _, err := tx.ExecContext(t.Context(), `SAVEPOINT forged_direct_insert`); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(t.Context(), statement); err == nil ||
			!strings.Contains(err.Error(), "permission denied") {
			t.Fatalf("direct forged %s insert err=%v", name, err)
		}
		if _, err := tx.ExecContext(t.Context(), `ROLLBACK TO SAVEPOINT forged_direct_insert`); err != nil {
			t.Fatal(err)
		}
	}
	const settleLLM = `
		SELECT out_llm_call_id,out_settlement_id
		  FROM settle_research_run_llm_spend_v3(
		    $1,$2,'m091-task',$3,$4,'system',$5,'{}',$6,
		    1,1,0,1,0,1,false,0.1::real,100,true,'',
		    true,true,false,'completed','')`
	if _, err := tx.ExecContext(t.Context(), `SAVEPOINT prompt_mismatch`); err != nil {
		t.Fatal(err)
	}
	var rejectedCallID, rejectedSettlementID int64
	err = tx.QueryRowContext(t.Context(), settleLLM,
		tenantID, userID, snapshotID, reservationID, "tampered", model+"-reported",
	).Scan(&rejectedCallID, &rejectedSettlementID)
	if err == nil || !strings.Contains(err.Error(), "differs from reservation") {
		t.Fatalf("bound request prompt mismatch err=%v ids=%d/%d",
			err, rejectedCallID, rejectedSettlementID)
	}
	if _, err := tx.ExecContext(t.Context(), `ROLLBACK TO SAVEPOINT prompt_mismatch`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `SAVEPOINT partial_cache_breakdown`); err != nil {
		t.Fatal(err)
	}
	err = tx.QueryRowContext(t.Context(), `
		SELECT out_llm_call_id,out_settlement_id
		  FROM settle_research_run_llm_spend_v3(
		    $1,$2,'m091-task',$3,$4,'system','user','{}',$5,
		    1,1,NULL,1,0,1,false,0.1::real,100,true,'',
		    true,true,false,'completed','')`,
		tenantID, userID, snapshotID, reservationID, model+"-reported",
	).Scan(&rejectedCallID, &rejectedSettlementID)
	if err == nil || !strings.Contains(err.Error(), "known LLM usage is incomplete") {
		t.Fatalf("partial cache breakdown err=%v ids=%d/%d",
			err, rejectedCallID, rejectedSettlementID)
	}
	if _, err := tx.ExecContext(t.Context(), `ROLLBACK TO SAVEPOINT partial_cache_breakdown`); err != nil {
		t.Fatal(err)
	}
	var callID, settlementID int64
	if err := tx.QueryRowContext(t.Context(), settleLLM,
		tenantID, userID, snapshotID, reservationID, "user", model+"-reported",
	).Scan(&callID, &settlementID); err != nil {
		t.Fatal(err)
	}
	if callID <= 0 || settlementID <= 0 {
		t.Fatalf("settled LLM ids call=%d settlement=%d", callID, settlementID)
	}
	var replayCallID, replaySettlementID int64
	if err := tx.QueryRowContext(t.Context(), settleLLM,
		tenantID, userID, snapshotID, reservationID, "user", model+"-reported",
	).Scan(&replayCallID, &replaySettlementID); err != nil {
		t.Fatal(err)
	}
	if replayCallID != callID || replaySettlementID != settlementID {
		t.Fatalf("settlement replay ids=%d/%d, want %d/%d",
			replayCallID, replaySettlementID, callID, settlementID)
	}
	shaB := strings.Repeat("b", 64)
	var estimatedReservationID int64
	if err := tx.QueryRowContext(t.Context(), `
		SELECT out_reservation_id
		  FROM admit_research_run_llm_spend_v3(
		    $1,$2,'m091-task',$3,'planner',1,0,$4,$4,'m091-trace-2','user2')`,
		tenantID, userID, snapshotID, shaB,
	).Scan(&estimatedReservationID); err != nil {
		t.Fatal(err)
	}
	var estimatedCallID, estimatedSettlementID int64
	if err := tx.QueryRowContext(t.Context(), `
		SELECT out_llm_call_id,out_settlement_id
		  FROM settle_research_run_llm_spend_v3(
		    $1,$2,'m091-task',$3,$4,'system','user2','{}',$5,
		    1,1,NULL,NULL,NULL,1,NULL,0.1::real,100,true,'',
		    true,true,false,'completed','')`,
		tenantID, userID, snapshotID, estimatedReservationID, model+"-reported",
	).Scan(&estimatedCallID, &estimatedSettlementID); err != nil {
		t.Fatal(err)
	}
	var estimatedPricing string
	if err := tx.QueryRowContext(t.Context(), `
		SELECT pricing_status FROM research_run_llm_spend_settlements WHERE id=$1`,
		estimatedSettlementID,
	).Scan(&estimatedPricing); err != nil {
		t.Fatal(err)
	}
	if estimatedCallID <= 0 || estimatedPricing != "estimated" {
		t.Fatalf("missing-cache settlement call=%d pricing=%q",
			estimatedCallID, estimatedPricing)
	}
	var unattemptedCallID sql.NullInt64
	var unattemptedSettlementID int64
	if err := tx.QueryRowContext(t.Context(), `
		SELECT out_llm_call_id,out_settlement_id
		  FROM settle_research_run_llm_spend_v3(
		    $1,$2,'m091-task',$3,$4,'system','user3','',$5,
		    0,0,NULL,NULL,NULL,0,NULL,0.1::real,100,true,'',
		    false,false,true,'failed','not_attempted')`,
		tenantID, userID, snapshotID, concurrentReservationID, model+"-reported",
	).Scan(&unattemptedCallID, &unattemptedSettlementID); err != nil {
		t.Fatal(err)
	}
	if unattemptedCallID.Valid || unattemptedSettlementID <= 0 {
		t.Fatalf("unattempted settlement call=%v settlement=%d",
			unattemptedCallID, unattemptedSettlementID)
	}
	var overageReservationID int64
	if err := tx.QueryRowContext(t.Context(), `
		SELECT out_reservation_id FROM admit_research_run_llm_spend_v3(
		    $1,$2,'m091-task',$3,'planner',3,0,$4,$4,'m091-trace-4','user4')`,
		tenantID, userID, snapshotID, strings.Repeat("e", 64),
	).Scan(&overageReservationID); err != nil {
		t.Fatal(err)
	}
	var overageCallID, overageSettlementID int64
	if err := tx.QueryRowContext(t.Context(), `
		SELECT out_llm_call_id,out_settlement_id
		  FROM settle_research_run_llm_spend_v3(
		    $1,$2,'m091-task',$3,$4,'system','user4','{}',$5,
		    300,100,0,300,0,1,false,0.1::real,100,true,'',
		    true,true,false,'completed','')`,
		tenantID, userID, snapshotID, overageReservationID, model+"-reported",
	).Scan(&overageCallID, &overageSettlementID); err != nil {
		t.Fatal(err)
	}
	if overageCallID <= 0 || overageSettlementID <= 0 {
		t.Fatalf("overage settlement ids=%d/%d", overageCallID, overageSettlementID)
	}
	var zeroReservationID int64
	if err := tx.QueryRowContext(t.Context(), `
		SELECT out_reservation_id FROM admit_research_run_llm_spend_v3(
		    $1,$2,'m091-task',$3,'planner',4,0,$4,$4,'m091-trace-5','user5')`,
		tenantID, userID, snapshotID, strings.Repeat("f", 64),
	).Scan(&zeroReservationID); err != nil {
		t.Fatal(err)
	}
	var zeroCallID, zeroSettlementID int64
	if err := tx.QueryRowContext(t.Context(), `
		SELECT out_llm_call_id,out_settlement_id
		  FROM settle_research_run_llm_spend_v3(
		    $1,$2,'m091-task',$3,$4,'system','user5','',$5,
		    0,0,NULL,NULL,NULL,1,NULL,0.1::real,100,true,'provider rejected',
		    true,false,true,'failed','provider_rejected')`,
		tenantID, userID, snapshotID, zeroReservationID, model+"-reported",
	).Scan(&zeroCallID, &zeroSettlementID); err != nil {
		t.Fatal(err)
	}
	if zeroCallID <= 0 || zeroSettlementID <= 0 {
		t.Fatalf("confirmed-zero settlement ids=%d/%d", zeroCallID, zeroSettlementID)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var actualCost int64
	var callCost float64
	var storedTemperature float32
	var storedDisableThinking bool
	var storedReportedModel string
	if err := database.QueryRowContext(t.Context(), `
		SELECT settlement.actual_cost_micro_usd,call.cost_usd,
		       call.temperature,call.disable_thinking,call.model
		  FROM research_run_llm_spend_settlements settlement
		  JOIN llm_calls call ON call.id=settlement.llm_call_id
		 WHERE settlement.id=$1`, settlementID,
	).Scan(&actualCost, &callCost, &storedTemperature, &storedDisableThinking,
		&storedReportedModel); err != nil {
		t.Fatal(err)
	}
	if actualCost <= 0 || callCost <= 0 || storedTemperature != float32(0.1) ||
		!storedDisableThinking || storedReportedModel != model+"-reported" {
		t.Fatalf("database-priced call cost=%d/%v temperature=%v thinking=%v model=%q",
			actualCost, callCost, storedTemperature, storedDisableThinking, storedReportedModel)
	}
	var tokens, expectedTokens float64
	if err := database.QueryRowContext(t.Context(), `
		SELECT quota.tokens,
		       1000000::double precision-
		       COALESCE(sum(reservation.reserved_quota_tokens),0)::double precision-
		       COALESCE(sum(GREATEST(
		           settlement.actual_prompt_tokens+settlement.actual_completion_tokens-
		             reservation.reserved_quota_tokens,0)),0)::double precision
		  FROM tenant_quota quota
		  LEFT JOIN research_run_llm_spend_reservations reservation
		    ON reservation.tenant_id=quota.tenant_id
		  LEFT JOIN research_run_llm_spend_settlements settlement
		    ON settlement.reservation_id=reservation.id
		 WHERE quota.tenant_id=$1 AND quota.bucket='llm_tokens'
		 GROUP BY quota.tokens`,
		tenantID,
	).Scan(&tokens, &expectedTokens); err != nil {
		t.Fatal(err)
	}
	if tokens != expectedTokens {
		t.Fatalf("settlement minted refund tokens=%v, authoritative floor/debt=%v",
			tokens, expectedTokens)
	}
}
