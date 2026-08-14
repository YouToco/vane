package store

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/taskstate"
)

func TestMigration118ProjectionMatchesStoreAndRejectsForgedContextPostgres(t *testing.T) {
	f := newResearchBriefFixtureWithResultV3(t,
		taskstate.NotificationThresholdMajorV3, true,
		[]byte(strings.Repeat("证据", 32<<10)))
	prepared, err := f.st.PrepareOrGetResearchBriefSynthesisV3(
		t.Context(), researchBriefPrepareParamsV3(f))
	if err != nil {
		t.Fatal(err)
	}
	var exact researchSynthesisContextV3
	if err := json.Unmarshal(prepared.Synthesis.ContextPayload, &exact); err != nil {
		t.Fatal(err)
	}
	if exact.SchemaVersion != researchSynthesisContextSchemaV32 ||
		len(exact.CurrentEvidence) != 1 ||
		!exact.CurrentEvidence[0].ContextTruncated {
		t.Fatalf("projected context=%+v", exact)
	}

	for _, test := range []struct {
		name  string
		text  string
		count int
	}{
		{name: "ascii", text: strings.Repeat("a", 20<<10), count: 1},
		{name: "unicode boundary", text: strings.Repeat("证", 4096), count: 16},
		{name: "json special characters", text: "official \"status\"\\path\nsecond\tline", count: 4},
		{name: "short exact", text: "official status: direct_purchase", count: 4},
	} {
		t.Run(test.name, func(t *testing.T) {
			want := projectResearchEvidenceTextV32(test.text,
				min(researchEvidenceItemMaxBytesV32,
					researchEvidenceContextBytesV32/test.count))
			var got string
			if err := f.st.pool.QueryRow(t.Context(),
				`SELECT public.project_research_evidence_context_v118($1,$2)`,
				[]byte(test.text), test.count).Scan(&got); err != nil {
				t.Fatal(err)
			}
			if got != want {
				t.Fatalf("SQL projection bytes=%d want bytes=%d", len(got), len(want))
			}
		})
	}

	forged := exact
	forged.CurrentEvidence[0].SynthesisVisibleText = "forged current Evidence"
	forged.CurrentEvidence[0].ContextVisibleSize = len(forged.CurrentEvidence[0].SynthesisVisibleText)
	forged.CurrentEvidence[0].ContextVisibleDigest =
		researchRunSHA256([]byte(forged.CurrentEvidence[0].SynthesisVisibleText))
	forged.CurrentEvidence[0].ContextTruncated = true
	forgedPayload, err := json.Marshal(forged)
	if err != nil {
		t.Fatal(err)
	}
	forgedContextDigest := researchRunSHA256(forgedPayload)
	forgedRequestDigest := digestResearchBriefRequestV3(researchBriefRequestDigestV3{
		SchemaVersion: researchBriefSynthesisSchemaV3,
		RunSnapshotID: f.snapshotRef.SnapshotID, PlanID: f.planRef.PlanID,
		DefinitionDigest:      f.snapshotRef.DefinitionDigest,
		PlanDigest:            f.planRef.PlanDigest,
		NotificationThreshold: string(taskstate.NotificationThresholdMajorV3),
		ContextDigest:         forgedContextDigest,
		EvidenceDigest:        prepared.Synthesis.EvidenceDigest,
		HistoryDigest:         prepared.Synthesis.HistoryDigest,
	})
	updateTx, err := f.st.pool.BeginTx(t.Context(), pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := updateTx.Exec(t.Context(),
		`SELECT set_config('app.tenant_id',$1,true),set_config('app.user_id',$2,true)`,
		fmtInt64V3(f.tenantID), fmtInt64V3(f.userID)); err != nil {
		_ = updateTx.Rollback(t.Context())
		t.Fatal(err)
	}
	_, updateErr := updateTx.Exec(t.Context(),
		`UPDATE research_brief_syntheses
		    SET context_payload=$2,context_digest=$3,request_digest=$4
		  WHERE id=$1`,
		prepared.Synthesis.ID, forgedPayload, forgedContextDigest, forgedRequestDigest)
	_ = updateTx.Rollback(t.Context())
	if updateErr == nil || !strings.Contains(updateErr.Error(),
		"research Brief synthesis identity is immutable") {
		t.Fatalf("forged synthesis update err=%v", updateErr)
	}

	if _, err := f.st.pool.Exec(t.Context(),
		`DELETE FROM research_brief_syntheses WHERE id=$1`,
		prepared.Synthesis.ID); err != nil {
		t.Fatal(err)
	}
	tx, err := f.st.pool.BeginTx(t.Context(), pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(t.Context()) }()
	if _, err := tx.Exec(t.Context(),
		`SELECT set_config('app.tenant_id',$1,true),set_config('app.user_id',$2,true)`,
		fmtInt64V3(f.tenantID), fmtInt64V3(f.userID)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(t.Context(), `SET LOCAL ROLE vane_app`); err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(t.Context(),
		`INSERT INTO research_brief_syntheses (
		     tenant_id,user_id,task_id,run_snapshot_id,plan_id,
		     temporal_workflow_id,temporal_run_id,definition_digest,plan_digest,
		     notification_threshold,request_digest,context_payload,context_digest,
		     evidence_manifest,evidence_digest,history_manifest,history_digest,schema_version
		 ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)`,
		f.tenantID, f.userID, f.taskID, f.snapshotRef.SnapshotID, f.planRef.PlanID,
		f.identity.TemporalWorkflowID, f.identity.TemporalRunID,
		f.snapshotRef.DefinitionDigest, f.planRef.PlanDigest,
		string(taskstate.NotificationThresholdMajorV3), forgedRequestDigest,
		forgedPayload, forgedContextDigest,
		prepared.Synthesis.EvidenceManifest, prepared.Synthesis.EvidenceDigest,
		prepared.Synthesis.HistoryManifest, prepared.Synthesis.HistoryDigest,
		researchBriefSynthesisSchemaV3)
	if err == nil || !strings.Contains(err.Error(),
		"research Brief projection is not exact terminal Tool outcomes") {
		t.Fatalf("forged synthesis projection err=%v", err)
	}
}
