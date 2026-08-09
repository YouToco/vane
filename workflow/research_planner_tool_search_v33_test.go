package workflow

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/YouToco/vane/runcontext"
	"github.com/YouToco/vane/runtimepolicy"
	storepkg "github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
)

func plannerToolSearchSealV33(t *testing.T) (
	types.ResearchRunSnapshotRefV3, runcontext.ResearchSnapshotSealV3,
) {
	t.Helper()
	_, snapshot, _, _ := researchBridgeFixtureV3(t)
	snapshot.PlannerBudget.MaxPlannerRounds = 3
	snapshot.PlannerBudget.MaxToolCalls = 2
	tools := coordinatorResearchToolsV3(t)
	model := coordinatorResearchModelV3(t)
	model.Planner.RendererVersion = runtimepolicy.ResearchPlannerRendererVersionV32
	model, err := runtimepolicy.WithPlannerToolSearchV33(model)
	if err != nil {
		t.Fatal(err)
	}
	seal := runcontext.ResearchSnapshotSealV3{
		ResearchToolPolicyDigest: snapshot.ToolPolicyDigest,
		Payload: runcontext.ResearchSnapshotPayloadV3{
			HistoryThroughUTC: "2026-08-09T00:00:00Z",
			PlannerBudget:     snapshot.PlannerBudget,
		},
		ResearchTools: tools, ResearchModel: model,
	}
	seal.Payload.Definition.TaskName = "OpenAI release watch"
	seal.Payload.Definition.TaskManual = "Find current official release evidence."
	return snapshot, seal
}

func TestResearchPlannerToolSearchV33SearchThenFinalIsRecoverySafe(t *testing.T) {
	snapshot, seal := plannerToolSearchSealV33(t)
	searchCompletion := `{"schema_version":"vane.research-planner-output/v3.3","action":"tool_search","tool_search":{"query":"official web release","limit":1}}`
	finalCompletion := `{"schema_version":"vane.research-planner-output/v3.3","action":"final","steps":[{"invocation_id":"search-official","tool_name":"web_search","arguments":{"query":"OpenAI official release"}},{"invocation_id":"search-confirmation","tool_name":"web_search","arguments":{"query":"OpenAI official confirmation"}}]}`
	settled := map[int]storepkg.ResearchRunLLMReceiptV3{}
	providerCalls := 0
	execute := func(_ context.Context, round int, prompt string) (
		storepkg.ResearchRunLLMReceiptV3,
		storepkg.ResearchRunLLMSpendReservationV3, error,
	) {
		reservation := storepkg.ResearchRunLLMSpendReservationV3{ReservationID: int64(round + 1)}
		if receipt, ok := settled[round]; ok {
			return receipt, reservation, nil
		}
		providerCalls++
		completion := searchCompletion
		if round == 0 {
			if strings.Contains(prompt, "Search public web pages") ||
				strings.Contains(prompt, `"allowed_tools"`) ||
				!strings.Contains(prompt, snapshot.ToolPolicyDigest) {
				t.Fatalf("initial prompt leaked schemas or missed frozen catalog: %s", prompt)
			}
		} else {
			completion = finalCompletion
			if !strings.Contains(prompt, "Search public web pages") ||
				!strings.Contains(prompt, `"name":"web_search"`) ||
				!strings.Contains(prompt, `"schema_digest"`) {
				t.Fatalf("next prompt did not load the exact hit: %s", prompt)
			}
		}
		receipt := storepkg.ResearchRunLLMReceiptV3{Call: types.LLMCall{Completion: completion}}
		settled[round] = receipt
		return receipt, reservation, nil
	}
	stored := map[int]runcontext.ResearchPlannerToolSearchReceiptV1{}
	persistCalls := 0
	persist := func(_ context.Context, reservation storepkg.ResearchRunLLMSpendReservationV3,
		receipt runcontext.ResearchPlannerToolSearchReceiptV1,
	) (runcontext.ResearchPlannerToolSearchReceiptV1, error) {
		persistCalls++
		if existing, ok := stored[receipt.RoundOrdinal]; ok {
			return existing, nil
		}
		if reservation.ReservationID != int64(receipt.RoundOrdinal+1) {
			t.Fatalf("receipt/reservation round mismatch: %+v %+v", receipt, reservation)
		}
		stored[receipt.RoundOrdinal] = receipt
		return receipt, nil
	}
	for attempt := 0; attempt < 2; attempt++ {
		plan, reservation, err := executeResearchPlannerToolSearchRoundsV33(
			t.Context(), snapshot, seal, execute, persist)
		if err != nil || len(plan.Steps) != 2 || plan.Steps[0].ToolName != "web_search" ||
			reservation.ReservationID != 2 {
			t.Fatalf("attempt=%d plan=%+v reservation=%+v err=%v", attempt, plan, reservation, err)
		}
	}
	if providerCalls != 2 || len(settled) != 2 || len(stored) != 1 || persistCalls != 2 {
		t.Fatalf("provider=%d settled=%d stored=%d persist=%d", providerCalls, len(settled), len(stored), persistCalls)
	}
}

func TestResearchPlannerToolSearchV33RejectsGuessedOrMalformedTool(t *testing.T) {
	snapshot, seal := plannerToolSearchSealV33(t)
	completions := []string{
		`{"schema_version":"vane.research-planner-output/v3.3","action":"final","steps":[{"invocation_id":"guess","tool_name":"web_search","arguments":{"query":"x"}}]}`,
		`{"schema_version":"vane.research-planner-output/v3.3","action":"tool_search","tool_search":{"query":"x","limit":1},"steps":[]}`,
	}
	for _, completion := range completions {
		round := 0
		execute := func(context.Context, int, string) (
			storepkg.ResearchRunLLMReceiptV3, storepkg.ResearchRunLLMSpendReservationV3, error,
		) {
			round++
			return storepkg.ResearchRunLLMReceiptV3{Call: types.LLMCall{Completion: completion}},
				storepkg.ResearchRunLLMSpendReservationV3{ReservationID: int64(round)}, nil
		}
		persist := func(context.Context, storepkg.ResearchRunLLMSpendReservationV3,
			runcontext.ResearchPlannerToolSearchReceiptV1,
		) (runcontext.ResearchPlannerToolSearchReceiptV1, error) {
			return runcontext.ResearchPlannerToolSearchReceiptV1{}, nil
		}
		if _, _, err := executeResearchPlannerToolSearchRoundsV33(
			t.Context(), snapshot, seal, execute, persist); err == nil {
			t.Fatalf("malformed/guessed completion crossed planner: %s", completion)
		}
	}
}

func TestDecodeResearchPlannerDecisionV33ExactActionShapes(t *testing.T) {
	valid := []string{
		`{"schema_version":"vane.research-planner-output/v3.3","action":"tool_search","tool_search":{"query":"中文 工具","limit":8}}`,
		`{"schema_version":"vane.research-planner-output/v3.3","action":"final","steps":[{"invocation_id":"one","tool_name":"web_search","arguments":{"query":"x"}},{"invocation_id":"two","tool_name":"web_search","arguments":{"query":"y"}}]}`,
	}
	for _, raw := range valid {
		if _, err := decodeResearchPlannerDecisionV33([]byte(raw), 2); err != nil {
			t.Fatalf("valid decision rejected: %s: %v", raw, err)
		}
	}
	for _, raw := range []string{
		`{"schema_version":"vane.research-planner-output/v3.3","action":"tool_search","tool_search":{"query":"x","limit":9}}`,
		`{"schema_version":"vane.research-planner-output/v3.3","action":"tool_search","tool_search":{"query":" x","limit":1}}`,
		`{"schema_version":"vane.research-planner-output/v3.3","action":"final","steps":[],"extra":true}`,
		`{"schema_version":"vane.research-planner-output/v3.3","action":"final","steps":[{"invocation_id":"one","tool_name":"web_search","arguments":{"query":"x"}}]}`,
		`{"schema_version":"bad","schema_version":"vane.research-planner-output/v3.3","action":"final","steps":[]}`,
	} {
		if _, err := decodeResearchPlannerDecisionV33([]byte(raw), 2); err == nil {
			t.Fatalf("invalid decision accepted: %s", raw)
		}
	}
}

func TestBuildResearchPlannerPromptV33ReceiptBytesAreCanonical(t *testing.T) {
	_, seal := plannerToolSearchSealV33(t)
	receipt, err := runcontext.BuildResearchPlannerToolSearchReceiptV1(
		0, seal.ResearchToolPolicyDigest, "official web", 1,
		[]runcontext.ResearchPlannerToolSearchMatchV1{{
			Name: "web_search", SchemaDigest: seal.ResearchTools.AllowedTools[0].SchemaDigest,
			Score: "1.250000000",
		}})
	if err != nil {
		t.Fatal(err)
	}
	prompt, err := buildResearchPlannerPromptV33(seal,
		[]runcontext.ResearchPlannerToolSearchReceiptV1{receipt})
	if err != nil {
		t.Fatal(err)
	}
	var decoded researchPlannerPromptV33
	if err := json.Unmarshal([]byte(prompt), &decoded); err != nil ||
		len(decoded.SearchHistory) != 1 || len(decoded.LoadedTools) != 1 {
		t.Fatalf("prompt=%s err=%v", prompt, err)
	}
}
