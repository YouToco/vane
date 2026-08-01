package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"sync"
	"testing"

	"github.com/YouToco/vane/types"
)

func TestMigration085AgentIntelligenceEvidenceAndIsolation(t *testing.T) {
	dbURL, db, provider := openMigration066Database(t, "vane_agent_intelligence_085")
	if _, err := provider.UpTo(t.Context(), 85); err != nil {
		t.Fatal(err)
	}
	var roleCanLogin, roleSuper, roleBypassRLS, roleInherit, roleCreateRole, roleCreateDB bool
	if err := db.QueryRowContext(t.Context(), `
		SELECT rolcanlogin,rolsuper,rolbypassrls,rolinherit,rolcreaterole,rolcreatedb
		  FROM pg_roles WHERE rolname='vane_intelligence_reader'`,
	).Scan(&roleCanLogin, &roleSuper, &roleBypassRLS, &roleInherit,
		&roleCreateRole, &roleCreateDB); err != nil {
		t.Fatal(err)
	}
	if roleCanLogin || roleSuper || roleBypassRLS || roleInherit || roleCreateRole || roleCreateDB {
		t.Fatalf("unsafe intelligence reader attributes login=%v super=%v bypass=%v inherit=%v createrole=%v createdb=%v",
			roleCanLogin, roleSuper, roleBypassRLS, roleInherit, roleCreateRole, roleCreateDB)
	}

	var tenantB int64
	if err := db.QueryRowContext(t.Context(), `
		INSERT INTO tenants(status,plan) VALUES('active','free') RETURNING id`,
	).Scan(&tenantB); err != nil {
		t.Fatal(err)
	}
	createUser := func(openID string, tenantID int64) (userID, sessionID, callID int64) {
		t.Helper()
		if err := db.QueryRowContext(t.Context(), `
			INSERT INTO users(feishu_open_id,name) VALUES($1,$1) RETURNING id`, openID,
		).Scan(&userID); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(t.Context(), `
			INSERT INTO memberships(tenant_id,user_id,role) VALUES($1,$2,'owner')`,
			tenantID, userID); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRowContext(t.Context(), `
			INSERT INTO agent_sessions(tenant_id,user_id) VALUES($1,$2) RETURNING id`,
			tenantID, userID).Scan(&sessionID); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRowContext(t.Context(), `
			INSERT INTO tool_calls(
			    tenant_id,user_id,session_id,trace_id,tool_name,arguments,result_preview,result_size
			) VALUES($1,$2,$3,$4,'query_fixture','{}','legacy-visible',14)
			RETURNING id`, tenantID, userID, sessionID, "trace-"+openID,
		).Scan(&callID); err != nil {
			t.Fatal(err)
		}
		return
	}
	userA, sessionA, callA := createUser("agent-intel-a-085", 1)
	userSameTenant, _, _ := createUser("agent-intel-c-085", 1)
	userB, _, _ := createUser("agent-intel-b-085", tenantB)
	if _, err := db.ExecContext(t.Context(), `DELETE FROM tool_calls WHERE id=$1`, callA); err != nil {
		t.Fatal(err)
	}

	st, err := New(t.Context(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	for i := 1; i <= 2; i++ {
		taskID := fmt.Sprintf("task-agent-intel-085-%d", i)
		if _, err := db.ExecContext(t.Context(), `
			INSERT INTO schedules(
			    id,tenant_id,user_id,nl_description,spec_json,scope_json,status
			) VALUES($1,1,$2,$3,'{"cron":"0 9 * * 1","timezone":"Asia/Shanghai"}','{}','paused')`,
			taskID, userA, fmt.Sprintf("Kimi task %d", i)); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(t.Context(), `
			INSERT INTO schedule_playbooks(schedule_id,content)
			VALUES($1,$2)`, taskID, fmt.Sprintf("monitor Kimi %d", i)); err != nil {
			t.Fatal(err)
		}
	}
	evidenceResult := []byte(`{"available":false,"source":"official"}`)
	record := AgentTurnRecordV1{
		SessionID: sessionA, TurnID: "turn-a-085", TraceID: "trace-agent-intel-a-085",
		UserMessage: "昨天 Kimi 查到什么？", AssistantMessage: "昨天的证据显示尚不可购买。",
		ActionReceipts: json.RawMessage(`[]`),
		ToolEvidence: []AgentToolEvidenceV1{{
			InvocationID: "invoke-a-085",
			ToolName:     "query_fixture", Arguments: json.RawMessage(`{"topic":"kimi"}`),
			Result: evidenceResult, OriginalSize: len(evidenceResult), TrustType: "local",
			ToolCall: types.ToolCall{
				TenantID: ptrInt64(1), UserID: &userA, SessionID: &sessionA,
				TraceID: "trace-agent-intel-a-085", ToolName: "query_fixture",
				Arguments:     json.RawMessage(`{"topic":"kimi"}`),
				ResultPreview: string(evidenceResult), ResultSize: len(evidenceResult),
			},
		}},
	}
	if err := st.CommitAgentTurnRecordV1(t.Context(), 1, userA, record); err != nil {
		t.Fatal(err)
	}
	if err := st.CommitAgentTurnRecordV1(t.Context(), 1, userA, record); err != nil {
		t.Fatalf("exact replay: %v", err)
	}
	concurrent := record
	concurrent.TurnID = "turn-concurrent-085"
	concurrent.TraceID = "trace-concurrent-085"
	concurrent.ToolEvidence = append([]AgentToolEvidenceV1(nil), record.ToolEvidence...)
	concurrent.ToolEvidence[0].InvocationID = "invoke-concurrent-085"
	concurrent.ToolEvidence[0].ToolCall.TraceID = concurrent.TraceID
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			candidate := concurrent
			candidate.ToolEvidence = append([]AgentToolEvidenceV1(nil), concurrent.ToolEvidence...)
			candidate.ToolEvidence[0].ToolCall = concurrent.ToolEvidence[0].ToolCall
			errs <- st.CommitAgentTurnRecordV1(t.Context(), 1, userA, candidate)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent exact replay: %v", err)
		}
	}
	var concurrentCalls, concurrentEvidence, concurrentTurns int
	if err := db.QueryRowContext(t.Context(), `
		SELECT
		 (SELECT count(*) FROM tool_calls WHERE tenant_id=1 AND user_id=$1 AND trace_id=$2),
		 (SELECT count(*) FROM agent_tool_evidence WHERE tenant_id=1 AND user_id=$1 AND trace_id=$2),
		 (SELECT count(*) FROM agent_turn_records WHERE tenant_id=1 AND user_id=$1 AND trace_id=$2)`,
		userA, concurrent.TraceID,
	).Scan(&concurrentCalls, &concurrentEvidence, &concurrentTurns); err != nil {
		t.Fatal(err)
	}
	if concurrentCalls != 1 || concurrentEvidence != 1 || concurrentTurns != 1 {
		t.Fatalf("concurrent idempotency calls=%d evidence=%d turns=%d",
			concurrentCalls, concurrentEvidence, concurrentTurns)
	}
	changed := record
	changed.AssistantMessage = "different"
	if err := st.CommitAgentTurnRecordV1(t.Context(), 1, userA, changed); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("divergent replay error=%v", err)
	}

	result, err := st.QueryMyIntelligence(t.Context(), IntelligenceScope{
		TenantID: 1, UserID: userA, SessionID: &sessionA,
	}, IntelligenceQuery{
		Dataset: IntelligenceToolCalls,
		Select:  []string{"tool_name", "model_visible_result", "evidence_coverage"},
		Filters: []IntelligenceFilter{{
			Field: "invocation_id", Op: "eq", Value: json.RawMessage(`"invoke-a-085"`),
		}},
		OrderBy: []IntelligenceOrder{{Field: "created_at", Direction: "asc"}},
		Limit:   10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["evidence_coverage"] != "exact" ||
		result.Rows[0]["model_visible_result"] != `{"available":false,"source":"official"}` {
		t.Fatalf("tenant/user exact result=%+v", result.Rows)
	}
	if _, err := db.ExecContext(t.Context(), `
		INSERT INTO tool_calls(
		    tenant_id,user_id,session_id,trace_id,tool_name,arguments,
		    result_preview,result_size
		) VALUES(1,$1,$2,'trace-legacy-preview-085','legacy_fixture','{}',
		         'legacy visible bytes',9000)`, userA, sessionA); err != nil {
		t.Fatal(err)
	}
	legacy, err := st.QueryMyIntelligence(t.Context(), IntelligenceScope{
		TenantID: 1, UserID: userA, SessionID: &sessionA,
	}, IntelligenceQuery{
		Dataset: IntelligenceToolCalls,
		Select:  []string{"model_visible_result", "evidence_coverage", "truncated"},
		Filters: []IntelligenceFilter{{
			Field: "trace_id", Op: "eq", Value: json.RawMessage(`"trace-legacy-preview-085"`),
		}},
	})
	if err != nil || len(legacy.Rows) != 1 ||
		legacy.Rows[0]["evidence_coverage"] != "legacy_preview" ||
		legacy.Rows[0]["truncated"] != true {
		t.Fatalf("legacy preview coverage=%+v err=%v", legacy, err)
	}
	turnCoverage, err := st.QueryMyIntelligence(t.Context(), IntelligenceScope{
		TenantID: 1, UserID: userA, SessionID: &sessionA,
	}, IntelligenceQuery{Dataset: IntelligenceAgentTurns, Limit: 5})
	if err != nil || turnCoverage.Coverage.Status != "partial" ||
		!strings.Contains(turnCoverage.Coverage.Note, "unavailable") {
		t.Fatalf("legacy turn gap coverage=%+v err=%v", turnCoverage, err)
	}

	for _, dataset := range []IntelligenceDataset{
		IntelligenceTasks, IntelligenceRuns, IntelligenceObservations,
		IntelligenceBriefs, IntelligenceAgentTurns, IntelligenceToolCalls,
		IntelligenceProfile,
	} {
		if _, err := st.QueryMyIntelligence(t.Context(), IntelligenceScope{
			TenantID: 1, UserID: userA, SessionID: &sessionA,
		}, IntelligenceQuery{Dataset: dataset, Limit: 5}); err != nil {
			t.Fatalf("dataset %s smoke: %v", dataset, err)
		}
	}
	if _, err := st.QueryMyIntelligence(t.Context(), IntelligenceScope{
		TenantID: 1, UserID: userA, SessionID: &sessionA,
		TaskID: "task-agent-intel-085-1",
	}, IntelligenceQuery{Dataset: IntelligenceProfile}); err != nil {
		t.Fatalf("scheduled Agent profile read: %v", err)
	}
	crossTask, err := st.QueryMyIntelligence(t.Context(), IntelligenceScope{
		TenantID: 1, UserID: userA, SessionID: &sessionA,
		TaskID: "task-agent-intel-085-1",
	}, IntelligenceQuery{
		Dataset: IntelligenceTasks, Select: []string{"task_ref"},
		Filters: []IntelligenceFilter{{
			Field: "task_ref", Op: "eq", Value: json.RawMessage(`"task-agent-intel-085-2"`),
		}},
	})
	if err != nil || len(crossTask.Rows) != 0 {
		t.Fatalf("scheduled Agent crossed task fence rows=%+v err=%v", crossTask, err)
	}
	if _, err := st.QueryMyIntelligence(t.Context(), IntelligenceScope{
		TenantID: tenantB, UserID: userA,
	}, IntelligenceQuery{Dataset: IntelligenceTasks}); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("cross tenant/user scope error=%v", err)
	}
	var denialCount int
	if err := db.QueryRowContext(t.Context(), `
		SELECT count(*) FROM agent_intelligence_access_denials
		 WHERE presented_tenant_id=$1 AND presented_user_id=$2
		   AND dataset='tasks' AND reason='membership_mismatch'`,
		tenantB, userA).Scan(&denialCount); err != nil {
		t.Fatal(err)
	}
	if denialCount != 1 {
		t.Fatalf("cross tenant/user denial audit count=%d", denialCount)
	}
	const denialConcurrency = 12
	startDenials := make(chan struct{})
	denialErrors := make(chan error, denialConcurrency)
	for range denialConcurrency {
		go func() {
			<-startDenials
			_, err := st.QueryMyIntelligence(t.Context(), IntelligenceScope{
				TenantID: tenantB, UserID: userA,
			}, IntelligenceQuery{Dataset: IntelligenceTasks})
			if !errors.Is(err, types.ErrValidation) {
				denialErrors <- fmt.Errorf("concurrent denial: %w", err)
				return
			}
			denialErrors <- nil
		}()
	}
	close(startDenials)
	for range denialConcurrency {
		if err := <-denialErrors; err != nil {
			t.Fatal(err)
		}
	}
	if err := db.QueryRowContext(t.Context(), `
		SELECT count(*) FROM agent_intelligence_access_denials
		 WHERE presented_tenant_id=$1 AND presented_user_id=$2
		   AND dataset='tasks' AND reason='membership_mismatch'`,
		tenantB, userA).Scan(&denialCount); err != nil {
		t.Fatal(err)
	}
	if denialCount != 1+denialConcurrency {
		t.Fatalf("concurrent denial audit count=%d", denialCount)
	}
	if _, err := db.ExecContext(t.Context(), `
		INSERT INTO schedules(
		    id,tenant_id,user_id,nl_description,spec_json,scope_json,status
		)
		SELECT 'task-agent-intel-bulk-085-'||i,1,$1,'bulk task '||i,
		       '{"cron":"0 9 * * 1","timezone":"Asia/Shanghai"}','{}','paused'
		  FROM generate_series(1,99) AS i`, userA); err != nil {
		t.Fatal(err)
	}
	boundary, err := st.QueryMyIntelligence(t.Context(), IntelligenceScope{
		TenantID: 1, UserID: userA, SessionID: &sessionA,
	}, IntelligenceQuery{
		Dataset: IntelligenceTasks, Select: []string{"task_ref"}, Limit: 100,
	})
	encodedBoundary, _ := json.Marshal(boundary)
	if err != nil || len(boundary.Rows) != 100 || !boundary.Truncated ||
		boundary.NextCursor == "" || len(encodedBoundary) > maxIntelligenceBytes {
		t.Fatalf("100-row boundary rows=%d truncated=%v cursor=%t bytes=%d err=%v",
			len(boundary.Rows), boundary.Truncated, boundary.NextCursor != "",
			len(encodedBoundary), err)
	}
	if _, err := st.QueryMyIntelligence(t.Context(), IntelligenceScope{
		TenantID: 1, UserID: userA, SessionID: &sessionA,
	}, IntelligenceQuery{Dataset: IntelligenceTasks, Limit: 101}); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("limit 101 error=%v", err)
	}
	aggregateQuery := IntelligenceQuery{
		Dataset: IntelligenceTasks,
		GroupBy: []string{"created_at", "record_id"},
		Metrics: []IntelligenceMetric{{Function: "count", As: "items"}},
		OrderBy: []IntelligenceOrder{{Field: "created_at", Direction: "desc"}},
		Limit:   1,
	}
	aggregateFirst, err := st.QueryMyIntelligence(t.Context(), IntelligenceScope{
		TenantID: 1, UserID: userA, SessionID: &sessionA,
	}, aggregateQuery)
	if err != nil || len(aggregateFirst.Rows) != 1 || aggregateFirst.NextCursor == "" {
		t.Fatalf("aggregate first page=%+v err=%v", aggregateFirst, err)
	}
	aggregateQuery.Cursor = aggregateFirst.NextCursor
	aggregateSecond, err := st.QueryMyIntelligence(t.Context(), IntelligenceScope{
		TenantID: 1, UserID: userA, SessionID: &sessionA,
	}, aggregateQuery)
	if err != nil || len(aggregateSecond.Rows) != 1 {
		t.Fatalf("aggregate second page=%+v err=%v", aggregateSecond, err)
	}
	if aggregateFirst.Rows[0]["created_at"] != aggregateSecond.Rows[0]["created_at"] ||
		aggregateFirst.Rows[0]["record_id"] == aggregateSecond.Rows[0]["record_id"] {
		t.Fatalf("aggregate keyset lost equal leading groups first=%+v second=%+v",
			aggregateFirst.Rows, aggregateSecond.Rows)
	}
	if _, err := st.QueryMyIntelligence(t.Context(), IntelligenceScope{
		TenantID: 1, UserID: userA, SessionID: &sessionA,
	}, IntelligenceQuery{
		Dataset: IntelligenceTasks, Limit: 1,
		OrderBy: []IntelligenceOrder{{Field: "task_name", Direction: "asc"}},
	}); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("mutable ordering pagination error=%v", err)
	}

	firstPage, err := st.QueryMyIntelligence(t.Context(), IntelligenceScope{
		TenantID: 1, UserID: userA, SessionID: &sessionA,
	}, IntelligenceQuery{
		Dataset: IntelligenceTasks, Select: []string{"task_ref"}, Limit: 1,
		OrderBy: []IntelligenceOrder{{Field: "created_at", Direction: "asc"}},
	})
	if err != nil || firstPage.NextCursor == "" {
		t.Fatalf("first page=%+v err=%v", firstPage, err)
	}
	firstTask, _ := firstPage.Rows[0]["task_ref"].(string)
	if firstTask != "task-agent-intel-085-1" {
		t.Fatalf("first keyset task=%q", firstTask)
	}
	if _, err := db.ExecContext(t.Context(),
		`DELETE FROM schedule_playbooks WHERE schedule_id=$1`, firstTask); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(),
		`DELETE FROM schedules WHERE id=$1`, firstTask); err != nil {
		t.Fatal(err)
	}
	secondStore, err := New(t.Context(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(secondStore.Close)
	secondPage, err := secondStore.QueryMyIntelligence(t.Context(), IntelligenceScope{
		TenantID: 1, UserID: userA, SessionID: &sessionA,
	}, IntelligenceQuery{
		Dataset: IntelligenceTasks, Select: []string{"task_ref"}, Limit: 1,
		OrderBy: []IntelligenceOrder{{Field: "created_at", Direction: "asc"}},
		Cursor:  firstPage.NextCursor,
	})
	if err != nil || len(secondPage.Rows) != 1 {
		t.Fatalf("cross-process cursor page=%+v err=%v", secondPage, err)
	}
	if secondPage.Rows[0]["task_ref"] != "task-agent-intel-085-2" {
		t.Fatalf("keyset pagination skipped after delete: %+v", secondPage.Rows)
	}
	if _, err := db.ExecContext(t.Context(), `
		UPDATE schedule_playbooks SET content=repeat('x',70000)
		 WHERE schedule_id='task-agent-intel-085-2'`); err != nil {
		t.Fatal(err)
	}
	if _, err := st.QueryMyIntelligence(t.Context(), IntelligenceScope{
		TenantID: 1, UserID: userA, SessionID: &sessionA,
	}, IntelligenceQuery{
		Dataset: IntelligenceTasks, Select: []string{"playbook"},
		Filters: []IntelligenceFilter{{
			Field: "task_ref", Op: "eq", Value: json.RawMessage(`"task-agent-intel-085-2"`),
		}},
	}); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("oversize row error=%v", err)
	}

	lockTx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lockTx.ExecContext(t.Context(),
		`LOCK TABLE schedule_playbooks IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	if _, err := st.QueryMyIntelligence(t.Context(), IntelligenceScope{
		TenantID: 1, UserID: userA, SessionID: &sessionA,
	}, IntelligenceQuery{Dataset: IntelligenceTasks}); !errors.Is(err, context.DeadlineExceeded) {
		_ = lockTx.Rollback()
		t.Fatalf("statement timeout error=%v", err)
	}
	if err := lockTx.Rollback(); err != nil {
		t.Fatal(err)
	}

	var mismatchCallID int64
	if err := db.QueryRowContext(t.Context(), `
		INSERT INTO tool_calls(
		    tenant_id,user_id,session_id,trace_id,tool_name,arguments,
		    result_preview,result_size
		) VALUES(1,$1,$2,'trace-digest-085','digest_fixture','{}','x',1)
		RETURNING id`, userA, sessionA).Scan(&mismatchCallID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `
		INSERT INTO agent_tool_evidence(
		    tenant_id,user_id,session_id,trace_id,invocation_id,tool_call_id,
		    tool_name,arguments,result_bytes,result_digest,original_size,
		    truncated,trust_type,schema_version
		) VALUES(1,$1,$2,'trace-digest-085','invoke-digest-085',$3,
		         'digest_fixture','{}',convert_to('x','UTF8'),repeat('0',64),1,
		         false,'local','vane.agent-tool-evidence/v1')`,
		userA, sessionA, mismatchCallID); err == nil || !strings.Contains(err.Error(), "digest mismatch") {
		t.Fatalf("mismatched evidence digest error=%v", err)
	}
	if _, err := db.ExecContext(t.Context(), `
		INSERT INTO agent_tool_evidence(
		    tenant_id,user_id,session_id,trace_id,invocation_id,tool_call_id,
		    tool_name,arguments,result_bytes,result_digest,original_size,
		    truncated,trust_type,schema_version
		) VALUES(1,$1,$2,'trace-digest-085','invoke-utf8-085',$3,
		         'digest_fixture','{}',decode('ff','hex'),repeat('0',64),1,
		         false,'local','vane.agent-tool-evidence/v1')`,
		userA, sessionA, mismatchCallID); err == nil {
		t.Fatal("invalid UTF-8 evidence unexpectedly committed")
	}

	for _, probe := range []struct {
		tenantID int64
		userID   int64
		want     int
	}{
		{1, userA, 2},
		{1, userSameTenant, 0},
		{tenantB, userB, 0},
	} {
		var count int
		tx, err := db.BeginTx(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(t.Context(), `
			SELECT set_config('app.tenant_id',$1,true),set_config('app.user_id',$2,true)`,
			fmt.Sprint(probe.tenantID), fmt.Sprint(probe.userID)); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(t.Context(), `SET LOCAL ROLE vane_app`); err != nil {
			t.Fatal(err)
		}
		if err := tx.QueryRowContext(t.Context(), `SELECT count(*) FROM agent_turn_records`).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		if count != probe.want {
			t.Fatalf("scope tenant=%d user=%d count=%d want=%d", probe.tenantID, probe.userID, count, probe.want)
		}
	}

	var canUpdate, canDelete bool
	if err := db.QueryRowContext(t.Context(), `
		SELECT has_table_privilege('vane_app','agent_turn_records','UPDATE'),
		       has_table_privilege('vane_app','agent_turn_records','DELETE')`,
	).Scan(&canUpdate, &canDelete); err != nil {
		t.Fatal(err)
	}
	if canUpdate || canDelete {
		t.Fatalf("vane_app mutable privileges update=%v delete=%v", canUpdate, canDelete)
	}

	var auditCount int
	if err := db.QueryRowContext(t.Context(), `
		SELECT count(*) FROM agent_intelligence_query_audits
		 WHERE tenant_id=1 AND user_id=$1 AND dataset='tool_calls'
		   AND query_summary ? 'has_cursor'
		   AND NOT (query_summary::text LIKE '%kimi%')`, userA,
	).Scan(&auditCount); err != nil {
		t.Fatal(err)
	}
	if auditCount != 3 {
		t.Fatalf("metadata-only audit count=%d", auditCount)
	}
	if _, err := provider.DownTo(t.Context(), 84); err == nil ||
		!strings.Contains(err.Error(), "refusing downgrade while Agent intelligence evidence exists") {
		t.Fatalf("nonempty 085 downgrade fence error=%v", err)
	}
}

func TestMigration085EmptyDowngradeAndLockOrder(t *testing.T) {
	raw, err := fs.ReadFile(migrationsFS, "migrations/085_agent_intelligence_evidence.sql")
	if err != nil {
		t.Fatal(err)
	}
	sqlText := string(raw)
	lock := "LOCK TABLE agent_tool_evidence,agent_turn_records,"
	if !strings.Contains(sqlText, lock) {
		t.Fatalf("085 downgrade must lock producer order %q", lock)
	}
	_, db, provider := openMigration066Database(t, "vane_agent_intel_down_085")
	if _, err := provider.UpTo(t.Context(), 85); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.DownTo(t.Context(), 84); err != nil {
		t.Fatalf("empty 85->84 downgrade: %v", err)
	}
	for _, object := range []string{
		"agent_tool_evidence", "agent_turn_records",
		"agent_intelligence_query_audits", "agent_intelligence_cursor_keys",
		"agent_intelligence_access_denials",
	} {
		var exists bool
		if err := db.QueryRowContext(t.Context(),
			`SELECT to_regclass('public.'||$1) IS NOT NULL`, object).Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if exists {
			t.Fatalf("downgrade left table %s", object)
		}
	}
	var functionExists bool
	if err := db.QueryRowContext(t.Context(), `
		SELECT to_regprocedure('public.enforce_agent_tool_evidence_call_v1()') IS NOT NULL`,
	).Scan(&functionExists); err != nil {
		t.Fatal(err)
	}
	if functionExists {
		t.Fatal("downgrade left SECURITY DEFINER evidence function")
	}
}

func TestMigration085RejectsPollutedReaderACL(t *testing.T) {
	_, db, provider := openMigration066Database(t, "vane_agent_intel_polluted_085")
	if _, err := provider.UpTo(t.Context(), 84); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `
		DO $$
		BEGIN
		    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='vane_intelligence_reader') THEN
		        CREATE ROLE vane_intelligence_reader
		            NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
		            NOLOGIN NOINHERIT NOBYPASSRLS;
		    END IF;
		END $$;
		GRANT SELECT ON schedules TO vane_intelligence_reader`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(),
			`REVOKE ALL ON schedules FROM vane_intelligence_reader`)
	})
	if _, err := provider.UpTo(t.Context(), 85); err == nil ||
		!strings.Contains(err.Error(), "preexisting ACL") {
		t.Fatalf("polluted reader migration error=%v", err)
	}
}
