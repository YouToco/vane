package store

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/pressly/goose/v3"

	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

func TestMigration107UpgradesLegacyV3PreparedAndSpendingArtifactsPostgres(t *testing.T) {
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
	// This fixture intentionally remains at schema 106 while it exercises the
	// migration 107 byte-preservation contract. The current Store binary reads
	// the schedule-status binding introduced by migration 116, so provide only
	// that compatibility column without running or weakening migration 107.
	if _, err := db.ExecContext(t.Context(), `ALTER TABLE research_v3_prepared_definition_heads
		ADD COLUMN prepared_schedule_status TEXT NOT NULL DEFAULT 'active'`); err != nil {
		t.Fatal(err)
	}

	st, err := New(t.Context(), scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	if err := st.configureResearchRunCapabilityV1(ResearchRunCapabilityConfigV1{
		ActiveKeyID:  "migration-107-active",
		ActiveKeyHex: strings.Repeat("42", 32),
		RetiredKeys:  "migration-107-retired=" + strings.Repeat("24", 32),
	}); err != nil {
		t.Fatal(err)
	}

	preparedFixture := newResearchBriefFixtureWithStoreAndWorkflowV3(
		t, st, taskstate.NotificationThresholdMajorV3, true, nil, "", "")
	prepared := prepareLegacyResearchBriefSynthesisForMigrationTest(t, preparedFixture)
	if !prepared.FirstWriter ||
		prepared.Synthesis.Status != ResearchBriefSynthesisPreparedV3 {
		t.Fatalf("legacy prepared=%+v", prepared)
	}
	pre108ShadowIdentity := preparedFixture.identity
	pre108ShadowIdentity.TemporalWorkflowID =
		"research-v3-shadow-" + strings.Repeat("a", 64)
	pre108ShadowIdentity.TemporalRunID = "pre-108-shadow-run"
	pre108ShadowRef, err := st.CreateOrGetResearchRunSnapshotV3(
		t.Context(), pre108ShadowIdentity, testCompiledRunPolicyV1(t),
		testResearchToolPolicyStoreV3(t), testResearchModelPolicyStoreV3(t))
	if err != nil || pre108ShadowRef.SnapshotID <= 0 {
		t.Fatalf("pre-108 shadow snapshot=%+v err=%v", pre108ShadowRef, err)
	}
	if _, err := st.pool.Exec(t.Context(),
		`UPDATE schedules SET execution_mode='compiled',
		 approved_definition_version=NULL,approved_definition_digest=NULL
		 WHERE id=$1 AND tenant_id=$2 AND user_id=$3`,
		preparedFixture.taskID, preparedFixture.tenantID,
		preparedFixture.userID); err != nil {
		t.Fatal(err)
	}
	pre108Tx, _, err := st.beginScopedResearchRunTransactionV3(
		t.Context(), pgx.TxOptions{}, pre108ShadowIdentity,
		pre108ShadowRef.SnapshotID)
	if err != nil {
		t.Fatal(err)
	}
	if err := lockPushEffectSchemaWriter(t.Context(), pre108Tx); err != nil {
		_ = pre108Tx.Rollback(t.Context())
		t.Fatal(err)
	}
	pre108AuthErr := authorizeResearchRunEffectV3(t.Context(), pre108Tx,
		pre108ShadowIdentity, pre108ShadowRef.SnapshotID)
	if err := pre108Tx.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(pre108AuthErr, types.ErrValidation) {
		t.Fatalf("pre-108 compatibility fallback admitted compiled shadow: %v",
			pre108AuthErr)
	}
	assertResearchBriefSynthesisStatusV3(t, preparedFixture,
		prepared.Synthesis.ID, ResearchBriefSynthesisPreparedV3)
	if _, err := st.pool.Exec(t.Context(),
		`UPDATE schedules SET execution_mode='discover_at_run',
		 approved_definition_version=1,approved_definition_digest=$2
		 WHERE id=$1 AND tenant_id=$3 AND user_id=$4`,
		preparedFixture.taskID, preparedFixture.snapshotRef.DefinitionDigest,
		preparedFixture.tenantID, preparedFixture.userID); err != nil {
		t.Fatal(err)
	}

	spendingFixture := newResearchBriefFixtureWithStoreAndWorkflowV3(
		t, st, taskstate.NotificationThresholdMajorV3, true, nil, "", "")
	spendingPrepared := prepareLegacyResearchBriefSynthesisForMigrationTest(t, spendingFixture)
	if !spendingPrepared.FirstWriter {
		t.Fatalf("legacy spending prepare=%+v", spendingPrepared)
	}
	spendingHandle, spendingReservation := claimResearchBriefWithPendingReceiptV3(
		t, spendingFixture, spendingPrepared.Synthesis)
	spendingBefore, err := st.LoadResearchBriefSynthesisV3(t.Context(),
		spendingFixture.identity, spendingFixture.snapshotRef, spendingFixture.planRef)
	if err != nil || spendingBefore.Status != ResearchBriefSynthesisSpendingV3 {
		t.Fatalf("legacy spending=%+v err=%v", spendingBefore, err)
	}

	type frozenLegacyV3 struct {
		requestDigest, contextDigest, evidenceDigest, historyDigest string
		context, evidence, history                                  []byte
	}
	freeze := func(row ResearchBriefSynthesisV3) frozenLegacyV3 {
		t.Helper()
		var context researchSynthesisContextV3
		var evidence researchEvidenceManifestV3
		if err := json.Unmarshal(row.ContextPayload, &context); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(row.EvidenceManifest, &evidence); err != nil {
			t.Fatal(err)
		}
		if context.SchemaVersion != researchSynthesisContextSchemaV3 ||
			evidence.SchemaVersion != researchEvidenceManifestSchemaV3 ||
			len(context.ToolFailures) != 0 || len(evidence.ToolFailures) != 0 {
			t.Fatalf("pre-107 artifact is not retained complete-coverage v3: context=%+v evidence=%+v",
				context, evidence)
		}
		return frozenLegacyV3{
			requestDigest: row.RequestDigest, contextDigest: row.ContextDigest,
			evidenceDigest: row.EvidenceDigest, historyDigest: row.HistoryDigest,
			context:  append([]byte(nil), row.ContextPayload...),
			evidence: append([]byte(nil), row.EvidenceManifest...),
			history:  append([]byte(nil), row.HistoryManifest...),
		}
	}
	assertFrozen := func(want frozenLegacyV3, got ResearchBriefSynthesisV3) {
		t.Helper()
		if got.RequestDigest != want.requestDigest ||
			got.ContextDigest != want.contextDigest ||
			got.EvidenceDigest != want.evidenceDigest ||
			got.HistoryDigest != want.historyDigest ||
			!bytes.Equal(got.ContextPayload, want.context) ||
			!bytes.Equal(got.EvidenceManifest, want.evidence) ||
			!bytes.Equal(got.HistoryManifest, want.history) {
			t.Fatalf("legacy v3 frozen bytes drifted after migration: got=%+v", got)
		}
	}
	preparedFrozen := freeze(prepared.Synthesis)
	spendingFrozen := freeze(spendingBefore)

	if _, err := provider.UpTo(t.Context(), 107); err != nil {
		t.Fatal(err)
	}

	preparedReplay, err := st.PrepareOrGetResearchBriefSynthesisV3(
		t.Context(), researchBriefPrepareParamsV3(preparedFixture))
	if err != nil || preparedReplay.FirstWriter ||
		preparedReplay.Synthesis.Status != ResearchBriefSynthesisPreparedV3 {
		t.Fatalf("post-107 prepared replay=%+v err=%v", preparedReplay, err)
	}
	assertFrozen(preparedFrozen, preparedReplay.Synthesis)
	spendingReplay, err := st.PrepareOrGetResearchBriefSynthesisV3(
		t.Context(), researchBriefPrepareParamsV3(spendingFixture))
	if err != nil || spendingReplay.FirstWriter ||
		spendingReplay.Synthesis.Status != ResearchBriefSynthesisSpendingV3 {
		t.Fatalf("post-107 spending replay=%+v err=%v", spendingReplay, err)
	}
	assertFrozen(spendingFrozen, spendingReplay.Synthesis)

	spendingClaimReplay, err := st.ClaimResearchBriefSynthesisV3(
		t.Context(), spendingHandle)
	if err != nil || spendingClaimReplay.Claimed ||
		spendingClaimReplay.ReceiptState != ResearchBriefLLMReceiptPendingV3 {
		t.Fatalf("post-107 spending claim replay=%+v err=%v",
			spendingClaimReplay, err)
	}
	preparedHandle, preparedReservation := claimResearchBriefWithPendingReceiptV3(
		t, preparedFixture, preparedReplay.Synthesis)

	finalizeAndReplay := func(
		fixture researchBriefFixtureV3, synthesis ResearchBriefSynthesisV3,
		handle ClaimResearchBriefSynthesisV3Params,
		reservation ResearchRunLLMSpendReservationV3, summary string,
	) {
		t.Helper()
		payload := researchBriefPayloadV3(t, synthesis,
			types.ResearchBriefSignificanceNoneV3, summary)
		settleResearchBriefReceiptV3(t, fixture, reservation, synthesis, payload)
		params := FinalizeResearchBriefSynthesisV3Params{
			ClaimResearchBriefSynthesisV3Params: handle,
			BriefPayload:                        payload,
		}
		ref, err := st.FinalizeResearchBriefSynthesisV3(t.Context(), params)
		if err != nil || ref.Decision != types.ResearchBriefDecisionQuietV3 ||
			ref.DeliveryRequired {
			t.Fatalf("post-107 legacy finalization ref=%+v err=%v", ref, err)
		}
		replay, err := st.FinalizeResearchBriefSynthesisV3(t.Context(), params)
		if err != nil || replay != ref {
			t.Fatalf("post-107 legacy finalization replay=%+v want=%+v err=%v",
				replay, ref, err)
		}
		finalized, err := st.LoadResearchBriefSynthesisV3(t.Context(),
			fixture.identity, fixture.snapshotRef, fixture.planRef)
		if err != nil || finalized.Status != ResearchBriefSynthesisFinalizedV3 {
			t.Fatalf("post-107 legacy finalized=%+v err=%v", finalized, err)
		}
	}
	finalizeAndReplay(preparedFixture, preparedReplay.Synthesis,
		preparedHandle, preparedReservation, "prepared v3 completed after migration 107")
	finalizeAndReplay(spendingFixture, spendingReplay.Synthesis,
		spendingHandle, spendingReservation, "spending v3 completed after migration 107")
}

func TestMigration107ACLTriggerRoutingAndIrreversibleDowngradePostgres(t *testing.T) {
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
	if _, err := provider.UpTo(t.Context(), 107); err != nil {
		t.Fatal(err)
	}

	rows, err := db.QueryContext(t.Context(), `
		SELECT p.proname,
		       has_function_privilege('public',p.oid,'EXECUTE'),
		       p.prosecdef,
		       p.proowner::regrole::text,
		       p.proconfig=ARRAY['search_path=pg_catalog, public, pg_temp']::text[]
		  FROM pg_proc p
		 WHERE p.proname IN (
		     'enforce_research_brief_synthesis_admission_v31',
		     'reject_research_brief_synthesis_schema_v31'
		 ) ORDER BY p.proname`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	wantDefiner := map[string]bool{
		"enforce_research_brief_synthesis_admission_v31": true,
		"reject_research_brief_synthesis_schema_v31":     true,
	}
	seen := map[string]bool{}
	for rows.Next() {
		var name, owner string
		var publicExecute, securityDefiner, safeConfig bool
		if err := rows.Scan(&name, &publicExecute, &securityDefiner, &owner,
			&safeConfig); err != nil {
			t.Fatal(err)
		}
		if publicExecute || securityDefiner != wantDefiner[name] ||
			owner == "vane_app" || !safeConfig {
			t.Fatalf("unsafe 107 function %s public=%v definer=%v owner=%q config=%v",
				name, publicExecute, securityDefiner, owner, safeConfig)
		}
		seen[name] = true
	}
	if err := rows.Err(); err != nil || len(seen) != len(wantDefiner) {
		t.Fatalf("107 functions seen=%v err=%v", seen, err)
	}

	triggerRows, err := db.QueryContext(t.Context(), `
		SELECT t.tgname,p.proname,t.tgenabled,pg_get_triggerdef(t.oid)
		  FROM pg_trigger t
		  JOIN pg_proc p ON p.oid=t.tgfoid
		 WHERE t.tgrelid='research_brief_syntheses'::regclass
		   AND NOT t.tgisinternal
		   AND t.tgname IN (
		       'research_brief_synthesis_admission_v3',
		       'research_brief_synthesis_admission_v31',
		       'research_brief_synthesis_reject_unknown_v31'
		   ) ORDER BY t.tgname`)
	if err != nil {
		t.Fatal(err)
	}
	defer triggerRows.Close()
	wantRoute := map[string]struct {
		function string
		markers  []string
	}{
		"research_brief_synthesis_admission_v3": {
			"enforce_research_brief_synthesis_admission_v3",
			[]string{"vane.research-synthesis-context/v3"},
		},
		"research_brief_synthesis_admission_v31": {
			"enforce_research_brief_synthesis_admission_v31",
			[]string{"vane.research-synthesis-context/v3.1"},
		},
		"research_brief_synthesis_reject_unknown_v31": {
			"reject_research_brief_synthesis_schema_v31",
			[]string{
				"vane.research-synthesis-context/v3",
				"vane.research-synthesis-context/v3.1",
			},
		},
	}
	seenRoutes := map[string]bool{}
	for triggerRows.Next() {
		var name, function, enabled, definition string
		if err := triggerRows.Scan(&name, &function, &enabled, &definition); err != nil {
			t.Fatal(err)
		}
		want := wantRoute[name]
		routeMatches := true
		for _, marker := range want.markers {
			routeMatches = routeMatches && strings.Contains(definition, marker)
		}
		if name == "research_brief_synthesis_admission_v3" &&
			strings.Contains(definition, "vane.research-synthesis-context/v3.1") {
			routeMatches = false
		}
		if enabled != "O" || function != want.function || !routeMatches {
			t.Fatalf("unsafe 107 trigger %s function=%s enabled=%s definition=%s",
				name, function, enabled, definition)
		}
		seenRoutes[name] = true
	}
	if err := triggerRows.Err(); err != nil || len(seenRoutes) != len(wantRoute) {
		t.Fatalf("107 trigger routes seen=%v err=%v", seenRoutes, err)
	}

	var updateIdentity, updateStatus, deletePrivilege, truncate bool
	if err := db.QueryRowContext(t.Context(), `SELECT
		has_column_privilege('vane_app','research_brief_syntheses','task_id','UPDATE'),
		has_column_privilege('vane_app','research_brief_syntheses','status','UPDATE'),
		has_table_privilege('vane_app','research_brief_syntheses','DELETE'),
		has_table_privilege('vane_app','research_brief_syntheses','TRUNCATE')`,
	).Scan(&updateIdentity, &updateStatus, &deletePrivilege, &truncate); err != nil {
		t.Fatal(err)
	}
	if updateIdentity || !updateStatus || deletePrivilege || truncate {
		t.Fatalf("107 changed synthesis ACL identity=%v status=%v delete=%v truncate=%v",
			updateIdentity, updateStatus, deletePrivilege, truncate)
	}

	if _, err := provider.DownTo(t.Context(), 106); err == nil ||
		!strings.Contains(err.Error(),
			"irreversible partial-coverage Brief evidence may exist") {
		t.Fatalf("107 downgrade unexpectedly succeeded or lost guard: %v", err)
	}
	var version int64
	if err := db.QueryRowContext(t.Context(),
		`SELECT max(version_id) FROM goose_db_version WHERE is_applied`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 107 {
		t.Fatalf("failed 107 downgrade changed version to %d", version)
	}
}

func TestMigration107PartialAndUnknownBriefAdmissionPostgres(t *testing.T) {
	for _, test := range []struct {
		name                string
		completedOrdinals   map[int]bool
		wantEvidence        int
		includeEvidenceLink bool
	}{
		{
			name:              "partial evidence is unknown quiet and delivery dark",
			completedOrdinals: map[int]bool{0: true}, wantEvidence: 1,
			includeEvidenceLink: true,
		},
		{
			name:              "no evidence is unknown quiet and delivery dark",
			completedOrdinals: map[int]bool{}, wantEvidence: 0,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture, synthesis := migration107PreparedBriefFixture(
				t, test.completedOrdinals)
			var manifest researchEvidenceManifestV3
			if err := json.Unmarshal(synthesis.EvidenceManifest, &manifest); err != nil {
				t.Fatal(err)
			}
			if manifest.SchemaVersion != researchEvidenceManifestSchemaV31 ||
				len(manifest.Items) != test.wantEvidence ||
				len(manifest.Items)+len(manifest.ToolFailures) != fixture.planRef.StepCount {
				t.Fatalf("partial manifest=%+v step_count=%d",
					manifest, fixture.planRef.StepCount)
			}
			var contextPayload researchSynthesisContextV3
			if err := json.Unmarshal(synthesis.ContextPayload, &contextPayload); err != nil {
				t.Fatal(err)
			}
			if contextPayload.SchemaVersion != researchSynthesisContextSchemaV32 ||
				!equalResearchToolFailuresV31(
					contextPayload.ToolFailures, manifest.ToolFailures) {
				t.Fatalf("partial context=%+v failures=%+v",
					contextPayload, manifest.ToolFailures)
			}

			citations := []types.ResearchBriefCitationV3{}
			if test.includeEvidenceLink {
				citations = []types.ResearchBriefCitationV3{{
					Kind: types.ResearchBriefCitationCurrentEvidenceV3,
					Ref:  strconv.FormatInt(manifest.Items[0].EvidenceID, 10),
				}}
			}
			payload, err := types.EncodeResearchBriefPayloadV3(
				types.ResearchBriefPayloadV3{
					SchemaVersion: types.ResearchBriefPayloadSchemaV31,
					Assessment:    types.ResearchBriefAssessmentUnknownV31,
					Headline:      "本次检查证据不足",
					Summary:       "一个或多个公开读取步骤失败，无法可靠判断是否有更新。",
					Significance:  types.ResearchBriefSignificanceNoneV3,
					Citations:     citations,
				})
			if err != nil {
				t.Fatal(err)
			}
			handle, reservation := claimResearchBriefWithPendingReceiptV3(
				t, fixture, synthesis)
			settleResearchBriefReceiptV3(
				t, fixture, reservation, synthesis, payload)
			ref, err := fixture.st.FinalizeResearchBriefSynthesisV3(t.Context(),
				FinalizeResearchBriefSynthesisV3Params{
					ClaimResearchBriefSynthesisV3Params: handle,
					BriefPayload:                        payload,
				})
			if err != nil {
				t.Fatal(err)
			}
			if ref.Significance != types.ResearchBriefSignificanceNoneV3 ||
				ref.Decision != types.ResearchBriefDecisionQuietV3 ||
				ref.DeliveryRequired {
				t.Fatalf("partial Brief escaped quiet gate: %+v", ref)
			}
		})
	}
}

func TestMigration107DatabaseRejectsForgedPartialCoverageDeliveryPostgres(t *testing.T) {
	fixture, synthesis := migration107PreparedBriefFixture(
		t, map[int]bool{0: true})
	var manifest researchEvidenceManifestV3
	if err := json.Unmarshal(synthesis.EvidenceManifest, &manifest); err != nil ||
		len(manifest.Items) != 1 || len(manifest.ToolFailures) != 1 {
		t.Fatalf("partial manifest=%+v err=%v", manifest, err)
	}
	legal, err := types.EncodeResearchBriefPayloadV3(types.ResearchBriefPayloadV3{
		SchemaVersion: types.ResearchBriefPayloadSchemaV31,
		Assessment:    types.ResearchBriefAssessmentUnknownV31,
		Headline:      "本次检查证据不足",
		Summary:       "一个公开读取步骤失败，无法可靠判断是否有更新。",
		Significance:  types.ResearchBriefSignificanceNoneV3,
		Citations: []types.ResearchBriefCitationV3{{
			Kind: types.ResearchBriefCitationCurrentEvidenceV3,
			Ref:  strconv.FormatInt(manifest.Items[0].EvidenceID, 10),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, reservation := claimResearchBriefWithPendingReceiptV3(t, fixture, synthesis)
	settleResearchBriefReceiptV3(t, fixture, reservation, synthesis, legal)

	tx, err := fixture.st.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(t.Context())
	if _, err := tx.Exec(t.Context(),
		`SELECT set_config('app.tenant_id',$1,true),
		        set_config('app.user_id',$2,true)`,
		strconv.FormatInt(fixture.tenantID, 10),
		strconv.FormatInt(fixture.userID, 10)); err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(t.Context(), `
		UPDATE research_brief_syntheses
		   SET status='finalized',significance='major',decision='deliver',
		       delivery_required=true,brief_payload=$2,
		       brief_digest=encode(sha256($2),'hex'),
		       finalized_at=clock_timestamp(),updated_at=clock_timestamp()
		 WHERE id=$1`, synthesis.ID, legal)
	if err == nil || !strings.Contains(err.Error(),
		"118: partial Brief must be unknown and quiet") {
		t.Fatalf("database admitted forged partial delivery or wrong fence: %v", err)
	}
}

func TestMigration107RejectsUnknownContextSchemaPostgres(t *testing.T) {
	fixture, synthesis := migration107PreparedBriefFixture(
		t, map[int]bool{0: true})
	tx, err := fixture.st.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(t.Context())
	if _, err := tx.Exec(t.Context(),
		`SELECT set_config('app.tenant_id',$1,true),
		        set_config('app.user_id',$2,true)`,
		strconv.FormatInt(fixture.tenantID, 10),
		strconv.FormatInt(fixture.userID, 10)); err != nil {
		t.Fatal(err)
	}
	unknownContext := []byte(`{"schema_version":"vane.research-synthesis-context/v999"}`)
	_, err = tx.Exec(t.Context(), `
		UPDATE research_brief_syntheses
		   SET context_payload=$2,context_digest=encode(sha256($2),'hex')
		 WHERE id=$1`, synthesis.ID, unknownContext)
	if err == nil || !strings.Contains(err.Error(),
		"107: research Brief context schema is unavailable") {
		t.Fatalf("database admitted unknown context schema or wrong route: %v", err)
	}
}

func TestMigration107SQLRetainsLegacyRouteAndRejectsUnknownSchemas(t *testing.T) {
	payload, err := fs.ReadFile(migrationsFS,
		"migrations/107_research_brief_partial_coverage.sql")
	if err != nil {
		t.Fatal(err)
	}
	sqlText := string(payload)
	for _, required := range []string{
		"EXECUTE FUNCTION enforce_research_brief_synthesis_admission_v3()",
		"vane.research-synthesis-context/v3.1",
		"vane.research-evidence-manifest/v3.1",
		"jsonb_array_length(evidence_json->'tool_failures')<1",
		"terminal.phase IN ('failed','indeterminate')",
		"brief_json->>'assessment' IS DISTINCT FROM 'unknown'",
		"NEW.decision IS DISTINCT FROM 'quiet'",
		"NEW.delivery_required IS DISTINCT FROM false",
		"EXECUTE FUNCTION reject_research_brief_synthesis_schema_v31()",
		"irreversible partial-coverage Brief evidence may exist",
	} {
		if !strings.Contains(sqlText, required) {
			t.Fatalf("107 migration omitted %q", required)
		}
	}
	if strings.Contains(sqlText,
		"DROP FUNCTION enforce_research_brief_synthesis_admission_v3") {
		t.Fatal("107 migration removed the retained complete-evidence admission function")
	}
}

func migration107PreparedBriefFixture(
	t *testing.T, completedOrdinals map[int]bool,
) (researchBriefFixtureV3, ResearchBriefSynthesisV3) {
	return migrationPreparedBriefFixtureV31(t, completedOrdinals, "external")
}

func migrationPreparedBriefFixtureV31(
	t *testing.T, completedOrdinals map[int]bool, trustType string,
) (researchBriefFixtureV3, ResearchBriefSynthesisV3) {
	t.Helper()
	f := newResearchRunSpendFixtureV3(t, 1_000_000)
	for ordinal := 0; ordinal < f.planRef.StepCount; ordinal++ {
		started, err := f.begin(t, ordinal)
		if err != nil {
			t.Fatal(err)
		}
		if completedOrdinals[ordinal] {
			result := []byte(`{"url":"https://www.kimi.com/membership/pricing","state":"reservation_only"}`)
			if _, err := f.store.CommitResearchRunStepEvidenceV3(t.Context(),
				CommitResearchRunStepEvidenceV3Params{
					Identity: f.identity, RunSnapshotID: f.snapshotID,
					PlanRef: f.planRef, Ordinal: ordinal,
					Result: result, OriginalSize: len(result), TrustType: trustType,
					CostMicroUSD: 100,
					ProviderCall: researchProviderCallV3ForTest(
						f.trace(t, ordinal, started.InvocationID), 100),
				}); err != nil {
				t.Fatal(err)
			}
			continue
		}
		terminal := CommitResearchRunStepV3Params{
			Identity: f.identity, RunSnapshotID: f.snapshotID,
			PlanRef: f.planRef, Ordinal: ordinal,
			Phase: ResearchRunStepFailedV3, ErrorCode: "route_unavailable",
		}
		if ordinal%2 == 1 {
			terminal.Phase = ResearchRunStepIndeterminateV3
			terminal.ErrorCode = "provider_outcome_uncertain"
			terminal.CostMicroUSD = 10_000
			terminal.ProviderCall = ResearchProviderCallV3{
				TraceID:  f.trace(t, ordinal, started.InvocationID),
				Provider: "exa", QuotaUnits: researchRunQuotaUnitsV3,
				Attempted: true, PricingStatus: "unpriced",
			}
		}
		if _, err := f.store.CommitResearchRunStepV3(t.Context(), terminal); err != nil {
			t.Fatal(err)
		}
	}
	fixture := researchBriefFixtureV3{
		st: f.store, tenantID: f.tenantID, userID: f.userID,
		taskID: f.identity.TaskID, identity: f.identity,
		snapshotRef: f.snapshotRef, planRef: f.planRef,
	}
	prepared, err := fixture.st.PrepareOrGetResearchBriefSynthesisV3(
		t.Context(), researchBriefPrepareParamsV3(fixture))
	if err != nil {
		t.Fatal(err)
	}
	if !prepared.FirstWriter || prepared.Synthesis.Status != ResearchBriefSynthesisPreparedV3 {
		t.Fatalf("partial synthesis prepare=%+v", prepared)
	}
	return fixture, prepared.Synthesis
}
