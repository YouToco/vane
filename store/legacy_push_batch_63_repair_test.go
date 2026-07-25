package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pressly/goose/v3"

	"github.com/YouToco/vane/pusheffect"
	"github.com/YouToco/vane/types"
)

type legacyBatch63Fixture struct {
	st        *Store
	db        *sql.DB
	provider  *goose.Provider
	evidence  LegacyBatch63RepairEvidence
	expiresAt time.Time
	buildCard LegacyBatch63CardBuilder
}

func newLegacyBatch63Fixture(t *testing.T) legacyBatch63Fixture {
	t.Helper()
	dbURL, db, provider := migration039Scratch(t)
	if _, err := provider.UpTo(t.Context(), 50); err != nil {
		t.Fatalf("migrate to 050: %v", err)
	}
	snapshotPayload := strings.ReplaceAll(
		goldenTaskRunSnapshotPayloadV1, `"tenant_id":7`, `"tenant_id":1`)
	snapshotPayload = strings.ReplaceAll(
		snapshotPayload, `"user_id":11`, `"user_id":1`)
	snapshotPayload = strings.ReplaceAll(
		snapshotPayload, "golden-task", legacyBatch63TaskID)
	snapshotPayload = strings.ReplaceAll(
		snapshotPayload, "monitor status", "batch63 frozen title")
	decodedSnapshot, err := readTaskRunSnapshotPayload([]byte(snapshotPayload))
	if err != nil {
		t.Fatal(err)
	}
	snapshotRow := taskRunSnapshot{
		ID: 3, TenantID: 1, UserID: 1, TaskID: legacyBatch63TaskID,
		TemporalWorkflowID:      legacyBatch63WorkflowID,
		TemporalRunID:           legacyBatch63RunID,
		RunKind:                 types.RunSnapshotKindScheduled,
		Mode:                    types.ExecutionModeCompiled,
		AdaptiveVersion:         0,
		CapabilityCatalogDigest: decodedSnapshot.PolicyDigests.CapabilityCatalog,
		ToolPolicyDigest:        decodedSnapshot.PolicyDigests.ToolPolicy,
		PromptPolicyDigest:      decodedSnapshot.PolicyDigests.PromptPolicy,
		ModelPolicyDigest:       decodedSnapshot.PolicyDigests.ModelPolicy,
		QuotaPolicyDigest:       decodedSnapshot.PolicyDigests.QuotaPolicy,
		DefinitionDigest:        decodedSnapshot.DefinitionDigest,
		PlanDigest:              decodedSnapshot.PlanDigest,
		PayloadDigest:           legacyBatch63Digest(decodedSnapshot.Canonical),
		ReferenceSchemaVersion:  taskRunReferenceSchemaVersionV1,
		BudgetJSON: []byte(
			`{"max_planner_rounds":0,"max_tool_calls":0,"max_tokens":0,` +
				`"max_cost_micro_usd":0,"duration_ms":0}`),
	}
	sealedRef, err := sealTaskRunSnapshotReferenceV1(
		&snapshotRow, taskRunBudgetV1{})
	if err != nil {
		t.Fatal(err)
	}
	snapshotRow.ReferenceDigest = sealedRef.ReferenceDigest
	if _, err := snapshotRow.safeRef(); err != nil {
		t.Fatalf("fixture snapshot reference: %v", err)
	}
	if _, err := db.ExecContext(t.Context(), `
		INSERT INTO users (id,feishu_open_id,name)
		VALUES (1,'ou_batch63_owner','batch63 owner');
		INSERT INTO memberships (tenant_id,user_id,role)
		VALUES (1,1,'owner');
		`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `
		INSERT INTO settings (key,value) VALUES
		  ('feishu',$1::jsonb),('feishu_owner',$2::jsonb)`,
		`{"app_id":"cli_batch63","app_secret":"not-real","enabled":true}`,
		`{"open_id":"ou_batch63_owner","name":"owner","app_identity":`+
			`"cli_batch63","chat_id":"oc_batch63",`+
			`"captured_at":"2026-07-24T20:00:00Z"}`,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `
		INSERT INTO sources (
		  id,url,title,status,platform,capability,config
		) VALUES (
		  42,'https://example.test/status','live drifted title',
		  'active','rss','read','{}'
		)`,
	); err != nil {
		t.Fatal(err)
	}
	contentIDs := []int64{1715, 1710, 1775, 1713, 1708}
	for _, id := range contentIDs {
		if _, err := db.ExecContext(t.Context(), `
			INSERT INTO content_items (
			  id,source_id,external_id,url,title,content,content_hash,
			  canonical_key
			) VALUES (
			  $1,42,$2,$3,$4,'body',$2,$5
			)`,
			id, "hash-"+strconv.FormatInt(id, 10),
			"https://example.test/"+strconv.FormatInt(id, 10),
			"title "+strconv.FormatInt(id, 10),
			"legacy-batch63://"+strconv.FormatInt(id, 10),
		); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(t.Context(), `
			INSERT INTO content_sources (
			  content_item_id,source_id,external_id,url
			) VALUES ($1,42,$2,$3)`,
			id, "hash-"+strconv.FormatInt(id, 10),
			"https://example.test/"+strconv.FormatInt(id, 10),
		); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.ExecContext(t.Context(), `
		INSERT INTO schedules (
		  id,tenant_id,user_id,nl_description,spec_json,scope_json,
		  status,execution_mode
		) VALUES ($1,1,1,'batch63','{}','{}','active','compiled')`,
		legacyBatch63TaskID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `
		INSERT INTO task_run_snapshots (
		  id,tenant_id,user_id,task_id,temporal_workflow_id,temporal_run_id,
		  run_kind,execution_mode,adaptive_version,
		  capability_catalog_digest,tool_policy_digest,prompt_policy_digest,
		  model_policy_digest,quota_policy_digest,definition_digest,plan_digest,
		  payload_digest,reference_digest,reference_schema_version,payload,budget
		) VALUES (
		  3,1,1,$1,$2,$3,'scheduled','compiled',0,
		  $4,$5,$6,$7,$8,$9,$10,$11,$12,'vane.run-snapshot-ref/v1',$13,$14
		)`,
		legacyBatch63TaskID, legacyBatch63WorkflowID, legacyBatch63RunID,
		snapshotRow.CapabilityCatalogDigest, snapshotRow.ToolPolicyDigest,
		snapshotRow.PromptPolicyDigest, snapshotRow.ModelPolicyDigest,
		snapshotRow.QuotaPolicyDigest, snapshotRow.DefinitionDigest,
		snapshotRow.PlanDigest, snapshotRow.PayloadDigest,
		snapshotRow.ReferenceDigest, snapshotPayload, snapshotRow.BudgetJSON,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `
		INSERT INTO push_batches (
		  id,tenant_id,user_id,scheduled_at,status,idempotency_key,run_snapshot_id
		) VALUES (63,1,1,'2026-07-24T20:28:32Z','failed',
		          'compiled-run/v1/3/trace88070ddd',3)`); err != nil {
		t.Fatal(err)
	}
	for id := int64(202); id <= 206; id++ {
		if _, err := db.ExecContext(t.Context(), `
			INSERT INTO deliveries (
			  id,tenant_id,batch_id,user_id,content_item_id,score,body_md,
			  card_json,status
			) VALUES ($1,1,63,1,$2,85,$3,'{}','pending')`,
			id, contentIDs[id-202],
			"durable body "+time.Unix(id, 0).UTC().Format(time.RFC3339),
		); err != nil {
			t.Fatal(err)
		}
	}
	st, err := New(t.Context(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	var databaseNow time.Time
	if err := db.QueryRowContext(t.Context(),
		`SELECT clock_timestamp()`).Scan(&databaseNow); err != nil {
		t.Fatal(err)
	}
	expiresAt := databaseNow.UTC().
		Truncate(time.Microsecond).Add(50 * time.Minute)
	codeExcerpt := "client := m.api()\n" +
		"\tif client == nil {\n" +
		"\t\treturn \"\", types.NewAppError(types.CodeConflict, " +
		"\"飞书通道未连接，无法主动推送\", nil)\n" +
		"\t}\n\n" +
		"\tresp, err := client.Im.Message.Create(ctx, " +
		"larkim.NewCreateMessageReqBuilder()."
	journalLines := []string{
		"2026/07/24 16:28:36 DEBUG ExecuteActivity Namespace default " +
			"TaskQueue vane-push WorkerID 2310323@yzl1t1ywk8.bytevirt.com@ " +
			"WorkflowType PushPipelineWorkflow WorkflowID " +
			legacyBatch63WorkflowID + " RunID " + legacyBatch63RunID +
			" Attempt 1 ActivityID 52 ActivityType Push",
		`{"time":"2026-07-24T16:28:37.042121633-04:00","level":"INFO",` +
			`"msg":"compiled snapshot authority selected","stage":` +
			`"activity_consumer","activity_type":"Push","activity_attempt":1,` +
			`"task_id":"` + legacyBatch63TaskID +
			`","temporal_workflow_id":"` + legacyBatch63WorkflowID +
			`","temporal_run_id":"` + legacyBatch63RunID +
			`","snapshot_id":3,"authority":"retained_v2"}`,
		`{"time":"2026-07-24T16:28:37.102733202-04:00","level":"WARN",` +
			`"msg":"push: 聚合卡推送失败，跳过该块","trace_id":` +
			`"88070ddd-1101-4fe1-a50f-4c97bb8f311b","items":5,` +
			`"err":"CONFLICT: 飞书通道未连接，无法主动推送"}`,
		"2026/07/24 16:28:37 ERROR Activity error. Namespace default " +
			"TaskQueue vane-push WorkerID 2310323@yzl1t1ywk8.bytevirt.com@ " +
			"WorkflowID " + legacyBatch63WorkflowID + " RunID " +
			legacyBatch63RunID +
			" ActivityType Push Attempt 1 Error 本批次全部推送失败 " +
			"(type: PUSH_FAILED, retryable: false): PUSH_FAILED: 本批次全部推送失败",
	}
	wire := legacyBatch63EvidenceWire{
		SchemaVersion: "vane.legacy-batch63-repair-evidence/v1",
		BatchID:       63, TaskID: legacyBatch63TaskID,
		TemporalWorkflowID:         legacyBatch63WorkflowID,
		TemporalRunID:              legacyBatch63RunID,
		TemporalHistoryDisposition: "expired_not_found",
		ServiceRevision:            legacyBatch63Revision,
		ActivityID:                 "52", Attempt: 1, ItemCount: 5,
		ErrorCode:    "CONFLICT",
		ErrorMessage: "飞书通道未连接，无法主动推送",
		Retryable:    false, CodePath: "feishu/push.go",
		CodeExcerpt:       codeExcerpt,
		CodeExcerptSHA256: legacyBatch63Digest([]byte(codeExcerpt)),
		JournalLines:      journalLines,
		JournalSHA256: legacyBatch63Digest(
			[]byte(strings.Join(journalLines, "\n"))),
	}
	evidenceBytes, err := json.Marshal(wire)
	if err != nil {
		t.Fatal(err)
	}
	return legacyBatch63Fixture{
		st: st, db: db, provider: provider,
		evidence: LegacyBatch63RepairEvidence{
			CanonicalBytes: evidenceBytes,
		},
		expiresAt: expiresAt,
		buildCard: buildLegacyBatch63TestCard,
	}
}

func TestMigration050RestrictedOneShotControlPlane(t *testing.T) {
	f := newLegacyBatch63Fixture(t)
	var (
		roleSecure, functionDefiner, fixedPath bool
		roleExecute, publicExecute, appExecute bool
		publicTable, appTable, roleTable       bool
	)
	if err := f.db.QueryRowContext(t.Context(), `
		SELECT
		  (SELECT rolcanlogin=false AND rolsuper=false AND rolcreatedb=false AND
		          rolcreaterole=false AND rolreplication=false AND
		          rolbypassrls=false AND rolinherit=false
		     FROM pg_roles WHERE rolname='vane_legacy_batch63_repair'),
		  p.prosecdef,
		  p.proconfig @> ARRAY['search_path=pg_catalog, public']::text[],
		  has_function_privilege(
		    'vane_legacy_batch63_repair',p.oid,'EXECUTE'),
		  has_function_privilege('public',p.oid,'EXECUTE'),
		  has_function_privilege('vane_app',p.oid,'EXECUTE'),
		  has_table_privilege(
		    'public','legacy_batch63_repair_events','SELECT'),
		  has_table_privilege(
		    'vane_app','legacy_batch63_repair_events','SELECT'),
		  has_table_privilege(
		    'vane_legacy_batch63_repair',
		    'legacy_batch63_repair_events','INSERT')
		  FROM pg_proc p
		 WHERE p.proname='finalize_legacy_push_batch_63_v1'
		   AND p.pronargs=22`,
	).Scan(&roleSecure, &functionDefiner, &fixedPath,
		&roleExecute, &publicExecute, &appExecute,
		&publicTable, &appTable, &roleTable); err != nil {
		t.Fatal(err)
	}
	if !roleSecure || !functionDefiner || !fixedPath || !roleExecute ||
		publicExecute || appExecute || publicTable || appTable || roleTable {
		t.Fatalf(
			"unsafe 050 ACL role=%v definer/path=%v/%v execute=%v/%v/%v table=%v/%v/%v",
			roleSecure, functionDefiner, fixedPath,
			roleExecute, publicExecute, appExecute,
			publicTable, appTable, roleTable)
	}
	var (
		abortSecure, claimSecure                             bool
		abortRepair, abortPublic, abortApp, abortCoordinator bool
		claimCoordinator, claimPublic, claimApp, claimRepair bool
		rlsEnabled, publicSequence, appSequence              bool
	)
	if err := f.db.QueryRowContext(t.Context(), `
		SELECT
		  abort_fn.prosecdef AND
		    abort_fn.proconfig @> ARRAY['search_path=pg_catalog, public']::text[],
		  claim_fn.prosecdef AND
		    claim_fn.proconfig @> ARRAY['search_path=pg_catalog, public']::text[],
		  has_function_privilege(
		    'vane_legacy_batch63_repair',abort_fn.oid,'EXECUTE'),
		  has_function_privilege('public',abort_fn.oid,'EXECUTE'),
		  has_function_privilege('vane_app',abort_fn.oid,'EXECUTE'),
		  has_function_privilege(
		    'vane_push_effect_coordinator',abort_fn.oid,'EXECUTE'),
		  has_function_privilege(
		    'vane_push_effect_coordinator',claim_fn.oid,'EXECUTE'),
		  has_function_privilege('public',claim_fn.oid,'EXECUTE'),
		  has_function_privilege('vane_app',claim_fn.oid,'EXECUTE'),
		  has_function_privilege(
		    'vane_legacy_batch63_repair',claim_fn.oid,'EXECUTE'),
		  (SELECT relrowsecurity FROM pg_class
		    WHERE oid='legacy_batch63_repair_events'::regclass),
		  has_sequence_privilege(
		    'public','legacy_batch63_repair_events_id_seq','USAGE'),
		  has_sequence_privilege(
		    'vane_app','legacy_batch63_repair_events_id_seq','USAGE')
		  FROM pg_proc abort_fn,pg_proc claim_fn
		 WHERE abort_fn.oid=
		       'abort_legacy_push_batch_63_v1(text)'::regprocedure
		   AND claim_fn.oid=
		       'legacy_push_batch_63_claim_ready_v1(text,bigint,bigint,text,bigint,text,text)'::regprocedure`,
	).Scan(&abortSecure, &claimSecure,
		&abortRepair, &abortPublic, &abortApp, &abortCoordinator,
		&claimCoordinator, &claimPublic, &claimApp, &claimRepair,
		&rlsEnabled, &publicSequence, &appSequence); err != nil {
		t.Fatal(err)
	}
	if !abortSecure || !claimSecure || !abortRepair || abortPublic ||
		abortApp || abortCoordinator || !claimCoordinator || claimPublic ||
		claimApp || claimRepair || !rlsEnabled || publicSequence || appSequence {
		t.Fatalf("unsafe 050 secondary ACL abort=%v/%v/%v/%v/%v claim=%v/%v/%v/%v/%v rls/seq=%v/%v/%v",
			abortSecure, abortRepair, abortPublic, abortApp, abortCoordinator,
			claimSecure, claimCoordinator, claimPublic, claimApp, claimRepair,
			rlsEnabled, publicSequence, appSequence)
	}
	var freshSecure, freshCoordinator, freshPublic, freshApp, freshRepair bool
	if err := f.db.QueryRowContext(t.Context(), `
		SELECT
		  p.prosecdef AND
		    p.proconfig @> ARRAY['search_path=pg_catalog, public']::text[],
		  has_function_privilege(
		    'vane_push_effect_coordinator',p.oid,'EXECUTE'),
		  has_function_privilege('public',p.oid,'EXECUTE'),
		  has_function_privilege('vane_app',p.oid,'EXECUTE'),
		  has_function_privilege(
		    'vane_legacy_batch63_repair',p.oid,'EXECUTE')
		  FROM pg_proc p
		 WHERE p.oid=
		   'legacy_push_batch_63_fresh_claim_ready_v1(text,bigint,bigint,text,bigint,text,text)'::regprocedure`,
	).Scan(&freshSecure, &freshCoordinator, &freshPublic,
		&freshApp, &freshRepair); err != nil {
		t.Fatal(err)
	}
	if !freshSecure || !freshCoordinator || freshPublic ||
		freshApp || freshRepair {
		t.Fatalf("unsafe 050 fresh admission ACL=%v/%v/%v/%v/%v",
			freshSecure, freshCoordinator, freshPublic,
			freshApp, freshRepair)
	}
}

func TestLegacyBatch63RepairSettingsDriftFailsBeforeEffect(t *testing.T) {
	tests := []struct {
		name   string
		mutate string
	}{
		{name: "owner deleted",
			mutate: `DELETE FROM settings WHERE key='feishu_owner'`},
		{name: "app changed",
			mutate: `UPDATE settings
			         SET value=jsonb_set(value,'{app_id}','"cli_other"')
			       WHERE key='feishu'`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newLegacyBatch63Fixture(t)
			plan, err := f.st.PreviewLegacyBatch63Repair(
				t.Context(), f.evidence, f.expiresAt, f.buildCard)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.db.ExecContext(t.Context(), tt.mutate); err != nil {
				t.Fatal(err)
			}
			if _, err := f.st.FinalizeLegacyBatch63Repair(
				t.Context(), plan.PlanDigest, f.evidence, f.expiresAt,
				f.buildCard); err == nil {
				t.Fatal("settings drift unexpectedly finalized")
			}
			var effects, events int
			var status string
			var authority *string
			if err := f.db.QueryRowContext(t.Context(), `
				SELECT
				  (SELECT count(*) FROM push_effects WHERE batch_id=63),
				  (SELECT count(*) FROM legacy_batch63_repair_events),
				  status,delivery_authority
				  FROM push_batches WHERE id=63`,
			).Scan(&effects, &events, &status, &authority); err != nil {
				t.Fatal(err)
			}
			if effects != 0 || events != 0 || status != "failed" ||
				authority != nil {
				t.Fatalf("partial finalize effects/events=%d/%d batch=%s/%v",
					effects, events, status, authority)
			}
		})
	}
}

func buildLegacyBatch63TestCard(in LegacyBatch63CardInput) string {
	markers := make([]map[string]any, 0, len(in.Items))
	for _, item := range in.Items {
		markers = append(markers, map[string]any{
			"effect_id": in.EffectID, "delivery_id": item.DeliveryID,
		})
	}
	card, _ := json.Marshal(map[string]any{
		"schema": "2.0", "config": map[string]any{},
		"body": map[string]any{"markers": markers},
	})
	return string(card)
}

func TestLegacyBatch63RepairPreviewFinalizeReplayAndAbort(t *testing.T) {
	f := newLegacyBatch63Fixture(t)
	plan, err := f.st.PreviewLegacyBatch63Repair(
		t.Context(), f.evidence, f.expiresAt, f.buildCard)
	if err != nil {
		t.Fatal(err)
	}
	if !legacyBatch63ValidDigest(plan.PlanDigest) ||
		!legacyBatch63ValidDigest(plan.MaterialDigest) ||
		!legacyBatch63ValidDigest(plan.EvidenceDigest) {
		t.Fatalf("invalid plan digests: %+v", plan)
	}
	status, err := f.st.FinalizeLegacyBatch63Repair(
		t.Context(), plan.PlanDigest, f.evidence, f.expiresAt, f.buildCard)
	if err != nil {
		t.Fatal(err)
	}
	if status.Phase != "finalized" ||
		status.EffectStatus != "prepared" ||
		status.BatchStatus != "pending" ||
		status.Authority != "effect" ||
		status.EnableBy == nil || status.ExpiresAt == nil {
		t.Fatalf("unexpected finalized status: %+v", status)
	}
	if _, err := f.st.FinalizeLegacyBatch63Repair(
		t.Context(), plan.PlanDigest, f.evidence, f.expiresAt,
		f.buildCard); err != nil {
		t.Fatalf("exact response-lost replay: %v", err)
	}
	status, err = f.st.AbortLegacyBatch63Repair(
		t.Context(), plan.PlanDigest)
	if err != nil {
		t.Fatal(err)
	}
	if status.Phase != "blocked" || status.EffectStatus != "blocked" ||
		status.BatchStatus != "failed" || status.Authority != "effect" {
		t.Fatalf("unexpected abort status: %+v", status)
	}
}

func TestLegacyBatch63RepairConcurrentFinalizeHasOneExactEffect(t *testing.T) {
	f := newLegacyBatch63Fixture(t)
	plan, err := f.st.PreviewLegacyBatch63Repair(
		t.Context(), f.evidence, f.expiresAt, f.buildCard)
	if err != nil {
		t.Fatal(err)
	}
	const contenders = 24
	errs := make(chan error, contenders)
	var wg sync.WaitGroup
	for range contenders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := f.st.FinalizeLegacyBatch63Repair(
				t.Context(), plan.PlanDigest, f.evidence, f.expiresAt,
				f.buildCard)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Errorf("concurrent exact replay: %v", err)
		}
	}
	status, err := f.st.VerifyLegacyBatch63Repair(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if status.Phase != "finalized" || status.EffectStatus != "prepared" {
		t.Fatalf("status=%+v", status)
	}
}

func TestLegacyBatch63RepairRejectsEvidenceCardAndPlanDrift(t *testing.T) {
	f := newLegacyBatch63Fixture(t)
	withNewline := f.evidence
	withNewline.CanonicalBytes = append(
		append([]byte(nil), f.evidence.CanonicalBytes...), '\n')
	newlinePlan, err := f.st.PreviewLegacyBatch63Repair(
		t.Context(), withNewline, f.expiresAt, f.buildCard)
	if err != nil {
		t.Fatalf("canonical evidence artifact with trailing newline: %v", err)
	}
	if newlinePlan.EvidenceDigest != legacyBatch63EvidenceDigest {
		t.Fatalf("newline changed canonical evidence digest: %s",
			newlinePlan.EvidenceDigest)
	}
	badEvidence := f.evidence
	badEvidence.CanonicalBytes = append([]byte(nil), f.evidence.CanonicalBytes...)
	badEvidence.CanonicalBytes[len(badEvidence.CanonicalBytes)-2] ^= 1
	if _, err := f.st.PreviewLegacyBatch63Repair(
		t.Context(), badEvidence, f.expiresAt,
		f.buildCard); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("evidence drift error=%v", err)
	}
	if _, err := f.st.PreviewLegacyBatch63Repair(
		t.Context(), f.evidence, f.expiresAt,
		func(in LegacyBatch63CardInput) string {
			return `{"effect_id":"` + in.EffectID + `","delivery_id":202}`
		}); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("card drift error=%v", err)
	}
	var internallyConsistent legacyBatch63EvidenceWire
	if err := json.Unmarshal(
		f.evidence.CanonicalBytes, &internallyConsistent); err != nil {
		t.Fatal(err)
	}
	internallyConsistent.JournalLines[0] += " tampered"
	internallyConsistent.JournalSHA256 = legacyBatch63Digest([]byte(
		strings.Join(internallyConsistent.JournalLines, "\n")))
	consistentBytes, err := json.Marshal(internallyConsistent)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.st.PreviewLegacyBatch63Repair(
		t.Context(), LegacyBatch63RepairEvidence{
			CanonicalBytes: consistentBytes,
		}, f.expiresAt, f.buildCard,
	); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("internally consistent evidence mutation error=%v", err)
	}
	plan, err := f.st.PreviewLegacyBatch63Repair(
		t.Context(), f.evidence, f.expiresAt, f.buildCard)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.st.FinalizeLegacyBatch63Repair(
		t.Context(), strings.Repeat("b", 64), f.evidence,
		f.expiresAt, f.buildCard,
	); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("plan drift error=%v (valid=%s)", err, plan.PlanDigest)
	}
}

func TestLegacyBatch63RepairExpiryAndDownFences(t *testing.T) {
	f := newLegacyBatch63Fixture(t)
	var databaseNow time.Time
	if err := f.db.QueryRowContext(t.Context(),
		`SELECT clock_timestamp()`).Scan(&databaseNow); err != nil {
		t.Fatal(err)
	}
	for _, expiry := range []time.Time{
		databaseNow.UTC().Truncate(time.Microsecond).Add(44 * time.Minute),
		databaseNow.UTC().Truncate(time.Microsecond).Add(61 * time.Minute),
	} {
		if _, err := f.st.PreviewLegacyBatch63Repair(
			t.Context(), f.evidence, expiry, f.buildCard,
		); !errors.Is(err, types.ErrConflict) {
			t.Fatalf("unsafe expiry %s error=%v", expiry, err)
		}
	}
	plan, err := f.st.PreviewLegacyBatch63Repair(
		t.Context(), f.evidence, f.expiresAt, f.buildCard)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.st.FinalizeLegacyBatch63Repair(
		t.Context(), plan.PlanDigest, f.evidence, f.expiresAt,
		f.buildCard); err != nil {
		t.Fatal(err)
	}
	if _, err := f.provider.DownTo(t.Context(), 49); err == nil {
		t.Fatal("migration 050 downgrade crossed finalized repair fence")
	}
}

func TestLegacyBatch63RepairAbortAfterProviderExpiry(t *testing.T) {
	f := newLegacyBatch63Fixture(t)
	plan, err := f.st.PreviewLegacyBatch63Repair(
		t.Context(), f.evidence, f.expiresAt, f.buildCard)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.st.FinalizeLegacyBatch63Repair(
		t.Context(), plan.PlanDigest, f.evidence, f.expiresAt,
		f.buildCard); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.ExecContext(t.Context(), `
		UPDATE push_effects
		   SET created_at=clock_timestamp()-interval '2 hours',
		       idempotency_expires_at=clock_timestamp()-interval '1 hour'
		 WHERE batch_id=63;
		UPDATE legacy_batch63_repair_events
		   SET created_at=clock_timestamp()-interval '2 hours',
		       idempotency_expires_at=clock_timestamp()-interval '1 hour',
		       enable_by=clock_timestamp()-interval '61 minutes'
		 WHERE batch_id=63 AND phase='finalized'`); err != nil {
		t.Fatal(err)
	}
	status, err := f.st.AbortLegacyBatch63Repair(
		t.Context(), plan.PlanDigest)
	if err != nil {
		t.Fatal(err)
	}
	if status.Phase != "blocked" || status.EffectStatus != "blocked" {
		t.Fatalf("expired abort status=%+v", status)
	}
}

func TestLegacyBatch63RepairVerifyAcceptsGenericExpiredNoSendTerminal(
	t *testing.T,
) {
	f := newLegacyBatch63Fixture(t)
	if _, err := f.provider.UpTo(t.Context(), 51); err != nil {
		t.Fatalf("migrate to 051: %v", err)
	}
	plan, err := f.st.PreviewLegacyBatch63Repair(
		t.Context(), f.evidence, f.expiresAt, f.buildCard)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.st.FinalizeLegacyBatch63Repair(
		t.Context(), plan.PlanDigest, f.evidence, f.expiresAt,
		f.buildCard); err != nil {
		t.Fatal(err)
	}
	scope := pusheffect.Scope{
		ID: legacyBatch63EffectID, TenantID: 1, UserID: 1,
	}
	claimed, decision, err := f.st.ClaimAuthorizedPushEffect(
		t.Context(),
		pusheffect.AuthorizedClaimParams{
			ClaimParams: pusheffect.ClaimParams{
				Scope: scope, LeaseOwner: "batch63-expiry-regression",
				LeaseDuration: time.Minute,
			},
			ExpectedTaskID:   legacyBatch63TaskID,
			DenialRetryAfter: time.Minute,
		},
	)
	if err != nil || decision != pusheffect.AuthorizedClaimed ||
		claimed == nil {
		t.Fatalf("claim=%+v/%q/%v", claimed, decision, err)
	}
	if err := f.st.RecordPushEffectDefiniteFailure(
		t.Context(),
		pusheffect.FailureParams{
			Lease: pusheffect.Lease{
				Scope: scope, LeaseOwner: claimed.LeaseOwner,
				Fence: claimed.Fence,
			},
			Class: "provider_definite_rejection", RetryAfter: time.Minute,
		},
	); err != nil {
		t.Fatal(err)
	}
	// Move both sealed operator timestamps together to model the real UUID
	// window elapsing after a definite no-send result. The immutable canonical
	// payload remains byte-identical on the effect and repair event.
	var databaseNow time.Time
	if err := f.db.QueryRowContext(t.Context(),
		`SELECT clock_timestamp()`).Scan(&databaseNow); err != nil {
		t.Fatal(err)
	}
	expiredAt := databaseNow.UTC().Truncate(time.Microsecond).Add(-time.Hour)
	createdAt := expiredAt.Add(-time.Hour)
	if _, err := f.db.ExecContext(t.Context(), `
		UPDATE push_effects
		   SET created_at=$2,idempotency_expires_at=$3
		 WHERE id=$1`, legacyBatch63EffectID, createdAt, expiredAt,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.ExecContext(t.Context(), `
		UPDATE legacy_batch63_repair_events
		   SET created_at=$1,idempotency_expires_at=$2,
		       enable_by=clock_timestamp()-interval '61 minutes'
		 WHERE batch_id=63 AND phase='finalized'`,
		createdAt, expiredAt,
	); err != nil {
		t.Fatal(err)
	}
	blocked, err := f.st.BlockExpiredUnclaimedPushEffect(
		t.Context(),
		pusheffect.ExpiryResolution{
			Scope: scope, ExpectedFence: claimed.Fence,
			ExpectedTaskID: legacyBatch63TaskID,
			RequiredWindow: time.Minute,
		},
	)
	if err != nil || !blocked {
		t.Fatalf("generic expiry block=%v/%v", blocked, err)
	}
	status, err := f.st.VerifyLegacyBatch63Repair(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if status.Phase != "finalized" ||
		status.EffectStatus != string(pusheffect.StatusBlocked) ||
		status.BatchStatus != "pending" ||
		status.Authority != "effect" {
		t.Fatalf("expired finalized verify=%+v", status)
	}
	if _, err := f.db.ExecContext(t.Context(), `
		UPDATE push_effects
		   SET failure_class='provider_history_conflict'
		 WHERE id=$1`,
		legacyBatch63EffectID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := f.st.VerifyLegacyBatch63Repair(
		t.Context()); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("other blocked cause verify error=%v", err)
	}
}

func TestMigration050UnusedDownSucceeds(t *testing.T) {
	f := newLegacyBatch63Fixture(t)
	if _, err := f.provider.DownTo(t.Context(), 49); err != nil {
		t.Fatalf("unused migration 050 must be reversible: %v", err)
	}
}
