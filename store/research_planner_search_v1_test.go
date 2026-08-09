package store

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/YouToco/vane/acquisitiontool"
	"github.com/YouToco/vane/runcontext"
	"github.com/YouToco/vane/runtimepolicy"
)

func TestResearchPlannerToolSearchReceiptV1BindsSearchAndFinalPlanPostgres(t *testing.T) {
	model := testResearchModelPolicyStoreV3(t)
	model.Planner.RendererVersion = runtimepolicy.ResearchPlannerRendererVersionV32
	var err error
	model, err = runtimepolicy.BuildResearchModelPolicyV3(model)
	if err != nil {
		t.Fatal(err)
	}
	model, err = runtimepolicy.WithPlannerToolSearchV33(model)
	if err != nil {
		t.Fatal(err)
	}
	f := newResearchRunSpendFixtureWithModelPolicyV3(t, 1_000_000, 4, false, model)
	ensureResearchLLMPriceV3(t, f.store)
	seal, err := f.store.LoadResearchRunSnapshotV3(t.Context(), f.identity, f.snapshotRef)
	if err != nil {
		t.Fatal(err)
	}
	settlePlanner := func(round int, prompt, completion string) ResearchRunLLMSpendReservationV3 {
		t.Helper()
		reservation, err := f.store.BeginResearchRunLLMSpendV3(t.Context(),
			BeginResearchRunLLMSpendV3Params{
				Identity: f.identity, SnapshotRef: f.snapshotRef,
				Stage: ResearchRunLLMStagePlannerV3, RoundOrdinal: round,
				SystemPrompt: seal.ResearchModel.Planner.SystemPrompt, UserPrompt: prompt,
			})
		if err != nil {
			t.Fatal(err)
		}
		call := researchLLMCallForTestV3(f.identity, f.snapshotRef, reservation,
			seal.ResearchModel.Planner.SystemPrompt, prompt)
		call.Completion = completion
		call.PromptTokens, call.CompletionTokens = 1, 1
		if _, _, err := commitResearchRunLLMReceiptForTestV3(t, f.store,
			CommitResearchRunLLMReceiptV3Params{
				Identity: f.identity, SnapshotRef: f.snapshotRef,
				ReservationID: reservation.ReservationID, Call: call,
				DisableThinking: reservation.DisableThinking,
				Attempted:       true, UsageKnown: true, Outcome: ResearchRunLLMCompletedV3,
			}); err != nil {
			t.Fatal(err)
		}
		return reservation
	}
	searchCompletion := `{"schema_version":"vane.research-planner-output/v3.3","action":"tool_search","tool_search":{"query":"official web release","limit":1}}`
	searchReservation := settlePlanner(0, "search the frozen catalog", searchCompletion)
	webSearch, found := frozenResearchToolByNameForTestV126(seal, "web_search")
	if !found {
		t.Fatal("fixture lost web_search")
	}
	receipt, err := runcontext.BuildResearchPlannerToolSearchReceiptV1(
		0, f.snapshotRef.ToolPolicyDigest, "official web release", 1,
		[]runcontext.ResearchPlannerToolSearchMatchV1{{
			Name: webSearch.Name, SchemaDigest: webSearch.SchemaDigest,
			Score: "1.250000000",
		}})
	if err != nil {
		t.Fatal(err)
	}
	stored, err := f.store.CreateOrGetResearchPlannerToolSearchReceiptV1(t.Context(),
		CreateOrGetResearchPlannerToolSearchReceiptV1Params{
			Identity: f.identity, SnapshotRef: f.snapshotRef,
			PlannerLLMReservationID: searchReservation.ReservationID,
			Receipt:                 receipt,
		})
	if err != nil || stored.RoundOrdinal != 0 || len(stored.Matches) != 1 {
		t.Fatalf("stored=%+v err=%v", stored, err)
	}
	replay, err := f.store.CreateOrGetResearchPlannerToolSearchReceiptV1(t.Context(),
		CreateOrGetResearchPlannerToolSearchReceiptV1Params{
			Identity: f.identity, SnapshotRef: f.snapshotRef,
			PlannerLLMReservationID: searchReservation.ReservationID,
			Receipt:                 receipt,
		})
	if err != nil || replay.RoundOrdinal != stored.RoundOrdinal {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}
	loaded, err := f.store.LoadResearchPlannerToolSearchReceiptsV1(
		t.Context(), f.identity, f.snapshotRef)
	if err != nil || len(loaded) != 1 || loaded[0].Matches[0].Name != "web_search" {
		t.Fatalf("loaded=%+v err=%v", loaded, err)
	}
	if _, err := f.store.pool.Exec(t.Context(), `
		UPDATE research_planner_tool_search_receipts SET catalog_digest=$1
		 WHERE run_snapshot_id=$2`, strings.Repeat("f", 64), f.snapshotRef.SnapshotID); err == nil {
		t.Fatal("database allowed planner search receipt mutation")
	}

	secondCompletion := `{"schema_version":"vane.research-planner-output/v3.3","action":"tool_search","tool_search":{"query":"second search","limit":1}}`
	secondReservation := settlePlanner(1, "search again", secondCompletion)
	unknown, err := runcontext.BuildResearchPlannerToolSearchReceiptV1(
		1, f.snapshotRef.ToolPolicyDigest, "second search", 1,
		[]runcontext.ResearchPlannerToolSearchMatchV1{{
			Name: "unknown_tool", SchemaDigest: strings.Repeat("e", 64),
			Score: "1.000000000",
		}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.CreateOrGetResearchPlannerToolSearchReceiptV1(t.Context(),
		CreateOrGetResearchPlannerToolSearchReceiptV1Params{
			Identity: f.identity, SnapshotRef: f.snapshotRef,
			PlannerLLMReservationID: secondReservation.ReservationID,
			Receipt:                 unknown,
		}); err == nil {
		t.Fatal("database admitted an unknown search result")
	}

	steps := []runcontext.ResearchPlanStepV3{{
		InvocationID: "official-search", ToolName: "web_search",
		Arguments: json.RawMessage(`{"query":"OpenAI official release"}`),
	}}
	plan, err := runcontext.BuildResearchExecutionPlanV3(
		f.snapshotRef.DefinitionDigest, f.snapshotRef.CapabilityCatalogDigest,
		f.snapshotRef.ToolPolicyDigest, steps,
		acquisitiontool.CanonicalizeToolArgumentsV1)
	if err != nil {
		t.Fatal(err)
	}
	finalPayload, err := json.Marshal(struct {
		SchemaVersion string                          `json:"schema_version"`
		Action        string                          `json:"action"`
		Steps         []runcontext.ResearchPlanStepV3 `json:"steps"`
	}{
		SchemaVersion: "vane.research-planner-output/v3.3",
		Action:        "final", Steps: plan.Steps,
	})
	if err != nil {
		t.Fatal(err)
	}
	finalReservation := settlePlanner(2, "finalize loaded tools", string(finalPayload))
	ref, err := f.store.CreateOrGetResearchRunPlanV3(t.Context(),
		CreateOrGetResearchRunPlanV3Params{
			Identity: f.identity, RunSnapshotID: f.snapshotRef.SnapshotID,
			PlannerLLMReservationID: finalReservation.ReservationID,
			Plan:                    plan,
		})
	if err != nil || ref.PlanID <= 0 || ref.StepCount != 1 {
		t.Fatalf("plan=%+v err=%v", ref, err)
	}
}

func frozenResearchToolByNameForTestV126(
	seal runcontext.ResearchSnapshotSealV3, name string,
) (runtimepolicy.ResearchToolDefinitionV3, bool) {
	for _, tool := range seal.ResearchTools.AllowedTools {
		if tool.Name == name {
			return tool, true
		}
	}
	return runtimepolicy.ResearchToolDefinitionV3{}, false
}
