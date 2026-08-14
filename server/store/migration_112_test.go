package store

import (
	"database/sql"
	"encoding/json"
	"io/fs"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/YouToco/vane/server/runcontext"
	"github.com/YouToco/vane/server/runtimepolicy"
	"github.com/YouToco/vane/server/taskstate"
	"github.com/YouToco/vane/server/types"
	"github.com/pressly/goose/v3"
)

func TestMigration112GroundedOfficialPartialCoverageFinalizesQuietPostgres(t *testing.T) {
	fixture, synthesis := migration112PreparedOfficialBriefFixture(t)
	var manifest researchEvidenceManifestV3
	if err := json.Unmarshal(synthesis.EvidenceManifest, &manifest); err != nil ||
		len(manifest.Items) != 1 || len(manifest.ToolFailures) != 1 {
		t.Fatalf("partial manifest=%+v err=%v", manifest, err)
	}
	payload, err := types.EncodeResearchBriefPayloadV3(types.ResearchBriefPayloadV3{
		SchemaVersion: types.ResearchBriefPayloadSchemaV32,
		Assessment:    types.ResearchBriefAssessmentGroundedV31,
		Headline:      "Kimi 套餐仍需预约",
		Summary:       "成功的官方结构化状态表明套餐当前仍不能直接购买。",
		Significance:  types.ResearchBriefSignificanceNoneV3,
		Citations: []types.ResearchBriefCitationV3{{
			Kind: types.ResearchBriefCitationCurrentEvidenceV3,
			Ref:  strconv.FormatInt(manifest.Items[0].EvidenceID, 10),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	handle, reservation := claimResearchBriefWithPendingReceiptV3(t, fixture, synthesis)
	settleResearchBriefReceiptV3(t, fixture, reservation, synthesis, payload)
	ref, err := fixture.st.FinalizeResearchBriefSynthesisV3(t.Context(),
		FinalizeResearchBriefSynthesisV3Params{
			ClaimResearchBriefSynthesisV3Params: handle,
			BriefPayload:                        payload,
		})
	if err != nil {
		t.Fatal(err)
	}
	if ref.Significance != types.ResearchBriefSignificanceNoneV3 ||
		ref.Decision != types.ResearchBriefDecisionQuietV3 || ref.DeliveryRequired {
		t.Fatalf("grounded partial Brief escaped quiet gate: %+v", ref)
	}
}

func TestMigration112DatabaseRejectsExternalOnlyAndDeliveryForgeryPostgres(t *testing.T) {
	for _, test := range []struct {
		name         string
		trustType    string
		significance string
		decision     string
		deliver      bool
		want         string
	}{
		{
			name: "external-only citation", trustType: "external",
			significance: "none", decision: "quiet", deliver: false,
			want: "118: grounded partial Brief must cite official Evidence and stay quiet",
		},
		{
			name: "official citation with forged delivery", trustType: "official",
			significance: "major", decision: "deliver", deliver: true,
			want: "118: grounded partial Brief must cite official Evidence and stay quiet",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var fixture researchBriefFixtureV3
			var synthesis ResearchBriefSynthesisV3
			if test.trustType == "official" {
				fixture, synthesis = migration112PreparedOfficialBriefFixture(t)
			} else {
				fixture, synthesis = migrationPreparedBriefFixtureV31(
					t, map[int]bool{0: true}, test.trustType)
			}
			var manifest researchEvidenceManifestV3
			if err := json.Unmarshal(synthesis.EvidenceManifest, &manifest); err != nil {
				t.Fatal(err)
			}
			payload, err := types.EncodeResearchBriefPayloadV3(types.ResearchBriefPayloadV3{
				SchemaVersion: types.ResearchBriefPayloadSchemaV32,
				Assessment:    types.ResearchBriefAssessmentGroundedV31,
				Headline:      "Kimi status is grounded",
				Summary:       "The current evidence reports the purchase status.",
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
			settleResearchBriefReceiptV3(t, fixture, reservation, synthesis, payload)

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
				   SET status='finalized',significance=$2,decision=$3,
				       delivery_required=$4,brief_payload=$5,
				       brief_digest=encode(sha256($5),'hex'),
				       finalized_at=clock_timestamp(),updated_at=clock_timestamp()
				 WHERE id=$1`, synthesis.ID, test.significance, test.decision,
				test.deliver, payload)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("database admitted forged grounded Brief or wrong fence: %v", err)
			}
		})
	}
}

func TestMigration112DatabaseRejectsGroundedFrozenInputMutationPostgres(t *testing.T) {
	fixture, synthesis := migration112PreparedOfficialBriefFixture(t)
	var manifest researchEvidenceManifestV3
	if err := json.Unmarshal(synthesis.EvidenceManifest, &manifest); err != nil {
		t.Fatal(err)
	}
	payload, err := types.EncodeResearchBriefPayloadV3(types.ResearchBriefPayloadV3{
		SchemaVersion: types.ResearchBriefPayloadSchemaV32,
		Assessment:    types.ResearchBriefAssessmentGroundedV31,
		Headline:      "Kimi status is grounded",
		Summary:       "The official status reports the current purchase state.",
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
	settleResearchBriefReceiptV3(t, fixture, reservation, synthesis, payload)
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
		   SET status='finalized',notification_threshold='all_qualified_updates',
		       significance='none',decision='quiet',delivery_required=false,
		       brief_payload=$2,brief_digest=encode(sha256($2),'hex'),
		       finalized_at=clock_timestamp(),updated_at=clock_timestamp()
		 WHERE id=$1`, synthesis.ID, payload)
	if err == nil || !strings.Contains(err.Error(),
		"118: research Brief manifest shape is invalid") {
		t.Fatalf("database admitted grounded frozen-input mutation: %v", err)
	}
}

func TestMigration112FunctionACLAndTriggerRoutingPostgres(t *testing.T) {
	if os.Getenv("DATABASE_URL") == "" {
		requireDatabaseCapability(t)
	}
	st := tenantTestStore(t)
	var publicExecute, securityDefiner, safeConfig bool
	var owner string
	if err := st.pool.QueryRow(t.Context(), `
		SELECT has_function_privilege('public',p.oid,'EXECUTE'),p.prosecdef,
		       p.proowner::regrole::text,
		       p.proconfig=ARRAY['search_path=pg_catalog, public, pg_temp']::text[]
		  FROM pg_proc p
		 WHERE p.proname='enforce_research_brief_grounded_partial_v32'`,
	).Scan(&publicExecute, &securityDefiner, &owner, &safeConfig); err != nil {
		t.Fatal(err)
	}
	if publicExecute || !securityDefiner || owner == "vane_app" || !safeConfig {
		t.Fatalf("unsafe 112 function public=%v definer=%v owner=%q config=%v",
			publicExecute, securityDefiner, owner, safeConfig)
	}
	var enabled, function, definition string
	if err := st.pool.QueryRow(t.Context(), `
		SELECT t.tgenabled,p.proname,pg_get_triggerdef(t.oid)
		  FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid
		 WHERE t.tgrelid='research_brief_syntheses'::regclass
		   AND t.tgname='research_brief_synthesis_grounded_partial_v32'
		   AND NOT t.tgisinternal`,
	).Scan(&enabled, &function, &definition); err != nil {
		t.Fatal(err)
	}
	if enabled != "O" || function != "enforce_research_brief_grounded_partial_v32" ||
		!strings.Contains(definition, "vane.research-brief/v3.2") ||
		!strings.Contains(definition, "vane.research-synthesis-context/v3.1") {
		t.Fatalf("unsafe 112 trigger enabled=%q function=%q definition=%q",
			enabled, function, definition)
	}
	var constraint string
	if err := st.pool.QueryRow(t.Context(), `
		SELECT pg_get_constraintdef(oid)
		  FROM pg_constraint
		 WHERE conrelid='research_run_evidence'::regclass
		   AND conname='ck_research_run_evidence_official_tool_v112'`,
	).Scan(&constraint); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(constraint, "web_product_status") ||
		!strings.Contains(constraint, "official") {
		t.Fatalf("unsafe 112 Evidence constraint=%q", constraint)
	}
}

func migration112PreparedOfficialBriefFixture(
	t *testing.T,
) (researchBriefFixtureV3, ResearchBriefSynthesisV3) {
	t.Helper()
	f := newResearchRunSpendFixtureWithToolBudgetV3(t, 1_000_000, 4, false)
	plan, err := runcontext.BuildResearchExecutionPlanV3(
		f.definitionDigest, f.snapshotRef.CapabilityCatalogDigest,
		f.snapshotRef.ToolPolicyDigest,
		[]runcontext.ResearchPlanStepV3{
			{
				InvocationID: "kimi-status", ToolName: "web_product_status",
				Arguments: json.RawMessage(
					`{"page_url":"https://www.kimi.com/membership/pricing"}`),
			},
			{
				InvocationID: "read-kimi-page", ToolName: "web_contents",
				Arguments: json.RawMessage(
					`{"urls":["https://www.kimi.com/membership/pricing"]}`),
			},
		},
		func(_ string, raw json.RawMessage) (json.RawMessage, error) { return raw, nil },
	)
	if err != nil {
		t.Fatal(err)
	}
	f.planRef, _ = createResearchPlanFromReceiptV3(
		t, f.store, f.identity, f.snapshotRef, plan)

	officialStart, err := f.begin(t, 0)
	if err != nil {
		t.Fatal(err)
	}
	result := []byte(`{"schema_version":"vane.product-status-result/v1","provider":"kimi","official_page":"https://www.kimi.com/membership/pricing","purchase_status":"reservation_only","plans":[]}`)
	status := 200
	if _, err := f.store.CommitResearchRunStepEvidenceV3(t.Context(),
		CommitResearchRunStepEvidenceV3Params{
			Identity: f.identity, RunSnapshotID: f.snapshotID, PlanRef: f.planRef,
			Ordinal: 0, Result: result, OriginalSize: len(result),
			TrustType: "official", CostMicroUSD: 0,
			ProviderCall: ResearchProviderCallV3{
				TraceID:  f.trace(t, 0, officialStart.InvocationID),
				Provider: "kimi", UsageQuantity: 1, QuotaUnits: 1,
				HTTPStatus: &status, DurationMS: 12, Attempted: true, CostKnown: true,
				CostMicroUSD: 0, PricingStatus: "calculated", CostCurrency: "USD",
			},
		}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.begin(t, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.CommitResearchRunStepV3(t.Context(),
		CommitResearchRunStepV3Params{
			Identity: f.identity, RunSnapshotID: f.snapshotID, PlanRef: f.planRef,
			Ordinal: 1, Phase: ResearchRunStepFailedV3,
			ErrorCode: "provider_reported_failure",
		}); err != nil {
		t.Fatal(err)
	}
	fixture := researchBriefFixtureV3{
		st: f.store, tenantID: f.tenantID, userID: f.userID,
		taskID: f.identity.TaskID, identity: f.identity,
		snapshotRef: f.snapshotRef, planRef: f.planRef,
	}
	prepared, err := fixture.st.PrepareOrGetResearchBriefSynthesisV3(
		t.Context(), researchBriefPrepareParamsV3(fixture))
	if err != nil || !prepared.FirstWriter || !prepared.PartialCoverage {
		t.Fatalf("official partial synthesis=%+v err=%v", prepared, err)
	}
	return fixture, prepared.Synthesis
}

func TestMigration112SQLHasVersionedRouteAndRollbackGuard(t *testing.T) {
	payload, err := fs.ReadFile(migrationsFS,
		"migrations/112_research_brief_grounded_partial_coverage.sql")
	if err != nil {
		t.Fatal(err)
	}
	sqlText := string(payload)
	for _, required := range []string{
		"vane.research-brief/v3.2",
		"assessment' IS DISTINCT FROM 'grounded'",
		"item->>'trust_type'='official'",
		"item->>'tool_name'='web_product_status'",
		"NEW.significance IS DISTINCT FROM 'none'",
		"NEW.decision IS DISTINCT FROM 'quiet'",
		"NEW.delivery_required IS DISTINCT FROM false",
		"ck_research_run_evidence_official_tool_v112",
		"(trust_type='official')=(tool_name='web_product_status')",
		"LOCK TABLE task_run_snapshots,research_run_evidence,research_brief_syntheses",
		"research-synthesis.render/v3.2",
		"112: v3.2 research artifacts exist; restore from backup",
	} {
		if !strings.Contains(sqlText, required) {
			t.Fatalf("112 migration omitted %q", required)
		}
	}
}

func TestMigration112DowngradeRejectsFrozenV32SnapshotPostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		requireDatabaseCapability(t)
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
	if _, err := provider.UpTo(t.Context(), 112); err != nil {
		t.Fatal(err)
	}
	st, err := New(t.Context(), scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	if err := st.configureResearchRunCapabilityV1(ResearchRunCapabilityConfigV1{
		ActiveKeyID: "migration-112-active", ActiveKeyHex: strings.Repeat("42", 32),
	}); err != nil {
		t.Fatal(err)
	}
	// This downgrade test exercises snapshot retention, not provider pricing.
	// Its synthetic strong-model must still satisfy the migration-091 paid-step
	// admission that the fixture crosses before freezing the v3.2 snapshot.
	ensureResearchLLMPriceV3(t, st)
	model := testResearchModelPolicyStoreV3(t)
	model.Synthesis.RendererVersion = runtimepolicy.ResearchSynthesisRendererVersionV32
	model, err = runtimepolicy.BuildResearchModelPolicyV3(model)
	if err != nil {
		t.Fatal(err)
	}
	_ = newResearchBriefFixtureWithStoreWorkflowAndModelV3(
		t, st, taskstate.NotificationThresholdMajorV3, false, nil, "", "", model)

	if _, err := provider.DownTo(t.Context(), 111); err == nil ||
		!strings.Contains(err.Error(),
			"112: v3.2 research artifacts exist; restore from backup") {
		t.Fatalf("112 downgrade ignored frozen v3.2 snapshot: %v", err)
	}
	var version int64
	if err := db.QueryRowContext(t.Context(),
		`SELECT max(version_id) FROM goose_db_version WHERE is_applied`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 112 {
		t.Fatalf("failed 112 downgrade changed version to %d", version)
	}
}

func TestMigration112RetainsInFlightV31FinalizationPostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		requireDatabaseCapability(t)
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
	if _, err := provider.UpTo(t.Context(), 111); err != nil {
		t.Fatal(err)
	}
	st, err := New(t.Context(), scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	if err := st.configureResearchRunCapabilityV1(ResearchRunCapabilityConfigV1{
		ActiveKeyID: "migration-112-v31", ActiveKeyHex: strings.Repeat("42", 32),
	}); err != nil {
		t.Fatal(err)
	}
	fixture := newResearchBriefFixtureWithStoreAndWorkflowV3(
		t, st, taskstate.NotificationThresholdMajorV3, false, nil, "", "")
	if _, err := st.CommitResearchRunStepV3(t.Context(), CommitResearchRunStepV3Params{
		Identity: fixture.identity, RunSnapshotID: fixture.snapshotRef.SnapshotID,
		PlanRef: fixture.planRef, Ordinal: 0, Phase: ResearchRunStepFailedV3,
		ErrorCode: "provider_reported_failure",
	}); err != nil {
		t.Fatal(err)
	}
	prepared := prepareLegacyResearchBriefSynthesisForMigrationTest(t, fixture)
	if !prepared.PartialCoverage {
		t.Fatalf("pre-112 v3.1 prepare=%+v", prepared)
	}
	payload, err := types.EncodeResearchBriefPayloadV3(types.ResearchBriefPayloadV3{
		SchemaVersion: types.ResearchBriefPayloadSchemaV31,
		Assessment:    types.ResearchBriefAssessmentUnknownV31,
		Headline:      "本次检查证据不足",
		Summary:       "官方读取步骤失败，无法可靠判断是否有更新。",
		Significance:  types.ResearchBriefSignificanceNoneV3,
		Citations:     []types.ResearchBriefCitationV3{},
	})
	if err != nil {
		t.Fatal(err)
	}
	handle, reservation := claimResearchBriefWithPendingReceiptV3(
		t, fixture, prepared.Synthesis)
	settleResearchBriefReceiptV3(t, fixture, reservation, prepared.Synthesis, payload)
	if _, err := provider.UpTo(t.Context(), 112); err != nil {
		t.Fatal(err)
	}
	ref, err := st.FinalizeResearchBriefSynthesisV3(t.Context(),
		FinalizeResearchBriefSynthesisV3Params{
			ClaimResearchBriefSynthesisV3Params: handle,
			BriefPayload:                        payload,
		})
	if err != nil {
		t.Fatal(err)
	}
	if ref.Decision != types.ResearchBriefDecisionQuietV3 || ref.DeliveryRequired {
		t.Fatalf("retained v3.1 finalization escaped quiet gate: %+v", ref)
	}
}
