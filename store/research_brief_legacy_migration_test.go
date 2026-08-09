package store

import (
	"encoding/json"
	"testing"

	"github.com/jackc/pgx/v5"
)

// prepareLegacyResearchBriefSynthesisForMigrationTest writes the exact
// historical v3/v3.1 context bytes required by migration upgrade tests. The
// production Store has no legacy writer: all current calls use context v3.2.
func prepareLegacyResearchBriefSynthesisForMigrationTest(
	t *testing.T, f researchBriefFixtureV3,
) PrepareResearchBriefSynthesisV3Result {
	t.Helper()
	ctx := t.Context()
	tx, scopedRef, err := f.st.beginScopedResearchRunTransactionV3(
		ctx, pgx.TxOptions{}, f.identity, f.snapshotRef.SnapshotID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if scopedRef != f.snapshotRef {
		t.Fatal("legacy synthesis fixture scope drifted")
	}
	snapshotSeal, err := loadAndValidateResearchBriefSnapshotV3(
		ctx, tx, f.identity, f.snapshotRef)
	if err != nil {
		t.Fatal(err)
	}
	_, planRow, err := loadAndValidateResearchRunPlanV3(
		ctx, tx, f.identity, f.snapshotRef.SnapshotID, f.planRef)
	if err != nil {
		t.Fatal(err)
	}
	evidencePayload, evidenceContext, toolFailures, err :=
		buildResearchEvidenceManifestV3(ctx, tx, f.identity, f.planRef, nil)
	if err != nil {
		t.Fatal(err)
	}
	for index := range evidenceContext {
		item := &evidenceContext[index]
		var fullText string
		if err := tx.QueryRow(ctx,
			`SELECT convert_from(result_bytes,'UTF8')
			   FROM research_run_evidence
			  WHERE id=$1 AND tenant_id=$2 AND user_id=$3 AND task_id=$4`,
			item.EvidenceID, f.tenantID, f.userID, f.taskID).Scan(&fullText); err != nil {
			t.Fatal(err)
		}
		item.SynthesisVisibleText = fullText
		item.ContextStoredSize = len(fullText)
		item.ContextVisibleSize = len(fullText)
		item.ContextVisibleDigest = researchRunSHA256([]byte(fullText))
		item.ContextTruncated = false
	}
	historyPayload, historyContext, err := buildResearchHistoryManifestV3(
		ctx, tx, f.identity, f.snapshotRef, f.planRef.PlanID)
	if err != nil {
		t.Fatal(err)
	}
	contextSchema := researchSynthesisContextSchemaV3
	if len(toolFailures) > 0 {
		contextSchema = researchSynthesisContextSchemaV31
	}
	contextPayload, err := json.Marshal(researchSynthesisContextV3{
		SchemaVersion: contextSchema,
		Definition: researchSynthesisDefinitionContextV3{
			TaskName:     snapshotSeal.Payload.Definition.TaskName,
			TaskManual:   snapshotSeal.Payload.Definition.TaskManual,
			Output:       snapshotSeal.Payload.Definition.Output,
			Notification: snapshotSeal.Payload.Definition.Notification,
		},
		CurrentEvidence: evidenceContext,
		ToolFailures:    toolFailures,
		History:         historyContext,
	})
	if err != nil {
		t.Fatal(err)
	}
	contextDigest := researchRunSHA256(contextPayload)
	evidenceDigest := researchRunSHA256(evidencePayload)
	historyDigest := researchRunSHA256(historyPayload)
	threshold := string(snapshotSeal.Payload.Definition.Notification.MinimumSignificance)
	requestDigest := digestResearchBriefRequestV3(researchBriefRequestDigestV3{
		SchemaVersion:         researchBriefSynthesisSchemaV3,
		RunSnapshotID:         f.snapshotRef.SnapshotID,
		PlanID:                f.planRef.PlanID,
		DefinitionDigest:      f.snapshotRef.DefinitionDigest,
		PlanDigest:            f.planRef.PlanDigest,
		NotificationThreshold: threshold,
		ContextDigest:         contextDigest,
		EvidenceDigest:        evidenceDigest,
		HistoryDigest:         historyDigest,
	})
	row, err := scanResearchBriefSynthesisV3(tx.QueryRow(ctx,
		`INSERT INTO research_brief_syntheses (
		     tenant_id,user_id,task_id,run_snapshot_id,plan_id,
		     temporal_workflow_id,temporal_run_id,definition_digest,plan_digest,
		     notification_threshold,request_digest,context_payload,context_digest,
		     evidence_manifest,evidence_digest,history_manifest,history_digest,schema_version
		 ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
		 RETURNING `+researchBriefSynthesisColumnsV3,
		f.tenantID, f.userID, f.taskID, f.snapshotRef.SnapshotID, planRow.ID,
		f.identity.TemporalWorkflowID, f.identity.TemporalRunID,
		f.snapshotRef.DefinitionDigest, f.planRef.PlanDigest,
		threshold, requestDigest, contextPayload, contextDigest,
		evidencePayload, evidenceDigest, historyPayload, historyDigest,
		researchBriefSynthesisSchemaV3))
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	return PrepareResearchBriefSynthesisV3Result{
		Synthesis: row, FirstWriter: true, PartialCoverage: len(toolFailures) > 0,
	}
}
