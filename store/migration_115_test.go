package store

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

func TestIntelligenceCatalogV3ProjectsNativeResearchArtifactsPostgres(t *testing.T) {
	const evidenceSize = 70_000
	largePlainTextEvidence := []byte(strings.Repeat("x", evidenceSize))
	exact := newResearchBriefFixtureWithResultV3(
		t, taskstate.NotificationThresholdMajorV3, true, largePlainTextEvidence,
	)
	_, exactState := finalizeResearchBriefFixtureV3(
		t, exact, types.ResearchBriefSignificanceNoneV3,
	)
	scope := IntelligenceScope{
		TenantID: exact.tenantID, UserID: exact.userID, TaskID: exact.taskID,
	}

	var observationRows []map[string]any
	var evidenceText strings.Builder
	var observationCursor string
	for page := 0; page < 16; page++ {
		observations, err := exact.st.QueryMyIntelligence(t.Context(), scope,
			IntelligenceQuery{Dataset: IntelligenceObservations, Cursor: observationCursor})
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range observations.Rows {
			observationRows = append(observationRows, row)
			value, _ := row["model_visible_result"].(string)
			evidenceText.WriteString(value)
		}
		if observations.NextCursor == "" {
			break
		}
		if !observations.Truncated {
			t.Fatal("V3 observation returned a cursor without truncation")
		}
		observationCursor = observations.NextCursor
	}
	if evidenceText.String() != string(largePlainTextEvidence) || len(observationRows) < 2 {
		t.Fatalf("V3 observation windows=%d bytes=%d", len(observationRows), evidenceText.Len())
	}
	observation := observationRows[0]
	if observation["lineage"] != "research_tool_evidence_v3" ||
		observation["evidence_coverage"] != "exact" ||
		observation["payload_coverage"] != "window" ||
		observation["source_truncated"] != false ||
		observation["model_visible_result"] != strings.Repeat("x", 8192) ||
		observation["payload_offset"].(json.Number).String() != "0" ||
		observation["payload_complete"] != false ||
		observation["original_size"].(json.Number).String() != "70000" {
		t.Fatalf("V3 observation projection=%+v", observation)
	}
	lastObservation := observationRows[len(observationRows)-1]
	if lastObservation["payload_complete"] != true ||
		lastObservation["payload_total_chars"].(json.Number).String() != "70000" {
		t.Fatalf("V3 final observation window=%+v", lastObservation)
	}

	briefs, err := exact.st.QueryMyIntelligence(t.Context(), scope,
		IntelligenceQuery{Dataset: IntelligenceBriefs})
	if err != nil {
		t.Fatal(err)
	}
	if len(briefs.Rows) != 1 {
		t.Fatalf("V3 briefs rows=%d: %+v", len(briefs.Rows), briefs.Rows)
	}
	brief := briefs.Rows[0]
	if brief["lineage"] != "research_brief_v3" || brief["status"] != "finalized" ||
		brief["truth_coverage"] != "exact" || brief["payload_coverage"] != "full" ||
		brief["decision"] != "quiet" || brief["delivery_required"] != false ||
		brief["delivery_status"] != "not_required" || brief["brief_preview"] == "" ||
		brief["payload_total_bytes"].(json.Number).String() != strconv.Itoa(len(exactState.BriefPayload)) {
		t.Fatalf("V3 Brief projection=%+v", brief)
	}

	runs, err := exact.st.QueryMyIntelligence(t.Context(), scope,
		IntelligenceQuery{Dataset: IntelligenceRuns})
	if err != nil {
		t.Fatal(err)
	}
	if len(runs.Rows) != 1 || runs.Rows[0]["runtime_generation"] != "research_v3" ||
		runs.Rows[0]["outcome_status"] != "finalized" || runs.Rows[0]["result"] != "quiet" ||
		runs.Rows[0]["source_coverage"] != "unavailable" ||
		runs.Rows[0]["processing"] != "unavailable" {
		t.Fatalf("V3 run projection=%+v", runs.Rows)
	}
	if exactState.FinalizedAt == nil {
		t.Fatal("exact fixture did not finalize")
	}
	poisonTx, err := exact.st.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = poisonTx.Rollback(t.Context()) }()
	if _, err := poisonTx.Exec(t.Context(), `
		SELECT set_config('app.tenant_id',$1,true),
		       set_config('app.user_id',$2,true)`,
		strconv.FormatInt(exact.tenantID, 10), strconv.FormatInt(exact.userID, 10)); err != nil {
		t.Fatal(err)
	}
	if _, err := poisonTx.Exec(t.Context(), `
		INSERT INTO task_run_outcomes (
			tenant_id,user_id,task_id,run_snapshot_id,schema_version
		) SELECT tenant_id,user_id,task_id,id,'vane.run-outcome/v1'
		    FROM task_run_snapshots WHERE id=$1`,
		exact.snapshotRef.SnapshotID); err != nil {
		t.Fatal(err)
	}
	if err := poisonTx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	poisonedRuns, err := exact.st.QueryMyIntelligence(t.Context(), scope,
		IntelligenceQuery{Dataset: IntelligenceRuns})
	if err != nil {
		t.Fatal(err)
	}
	if len(poisonedRuns.Rows) != 1 ||
		poisonedRuns.Rows[0]["runtime_generation"] != "research_v3" ||
		poisonedRuns.Rows[0]["outcome_status"] != "finalized" ||
		poisonedRuns.Rows[0]["result"] != "quiet" {
		t.Fatalf("legacy outcome poisoned V3 run semantics: %+v", poisonedRuns.Rows)
	}

	wrongTask, err := exact.st.QueryMyIntelligence(t.Context(), IntelligenceScope{
		TenantID: exact.tenantID, UserID: exact.userID, TaskID: exact.taskID + "-other",
	}, IntelligenceQuery{Dataset: IntelligenceObservations})
	if err != nil {
		t.Fatal(err)
	}
	if len(wrongTask.Rows) != 0 {
		t.Fatalf("scheduled exact-task fence leaked rows: %+v", wrongTask.Rows)
	}
	assertReaderCannotSee := func(name string, tenantID, userID int64) {
		t.Helper()
		t.Run(name, func(t *testing.T) {
			tx, err := exact.st.pool.Begin(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback(t.Context()) }()
			if _, err := tx.Exec(t.Context(), `
				SELECT set_config('app.tenant_id',$1,true),
				       set_config('app.user_id',$2,true)`,
				strconv.FormatInt(tenantID, 10), strconv.FormatInt(userID, 10)); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Exec(t.Context(), `SET LOCAL ROLE vane_intelligence_reader`); err != nil {
				t.Fatal(err)
			}
			for _, check := range []struct {
				query    string
				argument any
			}{
				{`SELECT count(*) FROM task_run_snapshots WHERE id=$1`, exact.snapshotRef.SnapshotID},
				{`SELECT count(*) FROM research_run_evidence WHERE task_id=$1`, exact.taskID},
				{`SELECT count(*) FROM research_brief_syntheses WHERE task_id=$1`, exact.taskID},
			} {
				var count int
				if err := tx.QueryRow(t.Context(), check.query, check.argument).Scan(&count); err != nil {
					t.Fatal(err)
				}
				if count != 0 {
					t.Fatalf("reader identity fence leaked via %q", check.query)
				}
			}
		})
	}
	assertReaderCannotSee("same tenant other user", exact.tenantID, exact.userID+99)
	assertReaderCannotSee("other tenant same user", exact.tenantID+99, exact.userID)

	failed := newResearchBriefFixtureV3(
		t, taskstate.NotificationThresholdMajorV3, true,
	)
	prepared, err := failed.st.PrepareOrGetResearchBriefSynthesisV3(
		t.Context(), researchBriefPrepareParamsV3(failed))
	if err != nil {
		t.Fatal(err)
	}
	handle := ClaimResearchBriefSynthesisV3Params{
		Identity: failed.identity, SnapshotRef: failed.snapshotRef, PlanRef: failed.planRef,
		SynthesisID: prepared.Synthesis.ID, RequestDigest: prepared.Synthesis.RequestDigest,
	}
	if _, err := failed.st.FailResearchBriefSynthesisV3(t.Context(),
		FailResearchBriefSynthesisV3Params{
			ClaimResearchBriefSynthesisV3Params: handle,
			Status:                              ResearchBriefSynthesisFailedV3, FailureCode: "prompt_unavailable",
		}); err != nil {
		t.Fatal(err)
	}
	failedBriefs, err := failed.st.QueryMyIntelligence(t.Context(), IntelligenceScope{
		TenantID: failed.tenantID, UserID: failed.userID, TaskID: failed.taskID,
	}, IntelligenceQuery{Dataset: IntelligenceBriefs})
	if err != nil {
		t.Fatal(err)
	}
	if len(failedBriefs.Rows) != 1 || failedBriefs.Rows[0]["status"] != "failed" ||
		failedBriefs.Rows[0]["truth_coverage"] != "unavailable" ||
		failedBriefs.Rows[0]["payload_coverage"] != "unavailable" ||
		failedBriefs.Rows[0]["failure_code"] != "prompt_unavailable" ||
		failedBriefs.Rows[0]["brief_preview"] != nil {
		t.Fatalf("failed V3 Brief projection=%+v", failedBriefs.Rows)
	}
}
