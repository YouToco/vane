package workflow

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/YouToco/vane/runcontext"
	"github.com/YouToco/vane/runtimeconfig"
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

func TestResearchPlannerToolSearchV33ForcesFinalAfterLoadedTools(t *testing.T) {
	for name, systemPrompt := range map[string]string{
		"retained-final-only": runtimepolicy.ResearchPlannerSystemPromptV33FinalOnly,
		"compact-loaded":      runtimepolicy.ResearchPlannerSystemPromptV33CompactLoadedTools,
		"compact-loaded-v2":   runtimepolicy.ResearchPlannerSystemPromptV33CompactLoadedToolsV2,
		"multi-entity-v3":     runtimepolicy.ResearchPlannerSystemPromptV33MultiEntityWindowV3,
	} {
		t.Run(name, func(t *testing.T) {
			snapshot, seal := plannerToolSearchSealV33(t)
			seal.ResearchModel.Planner.SystemPrompt = systemPrompt
			completions := []string{
				`{"schema_version":"vane.research-planner-output/v3.3","action":"tool_search","tool_search":{"query":"official web release","limit":8}}`,
				`{"schema_version":"vane.research-planner-output/v3.3","action":"tool_search","tool_search":{"query":"redundant search","limit":8}}`,
				`{"schema_version":"vane.research-planner-output/v3.3","action":"final","steps":[{"invocation_id":"official","tool_name":"web_search","arguments":{"query":"official status"}},{"invocation_id":"confirmation","tool_name":"web_search","arguments":{"query":"official status confirmation"}}]}`,
			}
			persistCalls := 0
			execute := func(_ context.Context, round int, _ string) (
				storepkg.ResearchRunLLMReceiptV3,
				storepkg.ResearchRunLLMSpendReservationV3, error,
			) {
				return storepkg.ResearchRunLLMReceiptV3{
						Call: types.LLMCall{Completion: completions[round]},
					}, storepkg.ResearchRunLLMSpendReservationV3{
						ReservationID: int64(round + 1),
					}, nil
			}
			persist := func(_ context.Context, _ storepkg.ResearchRunLLMSpendReservationV3,
				receipt runcontext.ResearchPlannerToolSearchReceiptV1,
			) (runcontext.ResearchPlannerToolSearchReceiptV1, error) {
				persistCalls++
				return receipt, nil
			}
			plan, reservation, err := executeResearchPlannerToolSearchRoundsV33(
				t.Context(), snapshot, seal, execute, persist)
			if err != nil || len(plan.Steps) != 2 || reservation.ReservationID != 3 ||
				persistCalls != 1 {
				t.Fatalf("plan=%+v reservation=%+v persist=%d err=%v",
					plan, reservation, persistCalls, err)
			}
		})
	}
}

func TestResearchPlannerToolSearchV33RetainsOldPolicyMultiSearchReplay(t *testing.T) {
	snapshot, seal := plannerToolSearchSealV33(t)
	seal.ResearchModel.Planner.MaxTokens = 4096
	seal.ResearchModel.Planner.SystemPrompt = "retained v3.3 planner policy"
	completions := []string{
		`{"schema_version":"vane.research-planner-output/v3.3","action":"tool_search","tool_search":{"query":"official web release","limit":1}}`,
		`{"schema_version":"vane.research-planner-output/v3.3","action":"tool_search","tool_search":{"query":"official web confirmation","limit":1}}`,
		`{"schema_version":"vane.research-planner-output/v3.3","action":"final","steps":[{"invocation_id":"official","tool_name":"web_search","arguments":{"query":"official status"}},{"invocation_id":"confirmation","tool_name":"web_search","arguments":{"query":"official status confirmation"}}]}`,
	}
	persistCalls := 0
	execute := func(_ context.Context, round int, _ string) (
		storepkg.ResearchRunLLMReceiptV3,
		storepkg.ResearchRunLLMSpendReservationV3, error,
	) {
		return storepkg.ResearchRunLLMReceiptV3{
			Call: types.LLMCall{Completion: completions[round]},
		}, storepkg.ResearchRunLLMSpendReservationV3{ReservationID: int64(round + 1)}, nil
	}
	persist := func(_ context.Context, _ storepkg.ResearchRunLLMSpendReservationV3,
		receipt runcontext.ResearchPlannerToolSearchReceiptV1,
	) (runcontext.ResearchPlannerToolSearchReceiptV1, error) {
		persistCalls++
		return receipt, nil
	}
	plan, _, err := executeResearchPlannerToolSearchRoundsV33(
		t.Context(), snapshot, seal, execute, persist)
	if err != nil || len(plan.Steps) != 2 || persistCalls != 2 {
		t.Fatalf("plan=%+v persist=%d err=%v", plan, persistCalls, err)
	}
}

func TestResearchPlannerToolSearchV33ProductionRecoveryFitsRetainedTokenBudget(t *testing.T) {
	_, seal := plannerToolSearchSealV33(t)
	production, err := runtimeconfig.BuildResearchRuntimeV3(
		runtimeconfig.CurrentCompiledV1Input{
			Model: "cheap-model", ResearchModel: "deepseek-v4-pro",
			ModelEndpointGeneration: 1, ModelCredentialGeneration: 1,
			ExaCredentialGeneration: 1, TikHubCredentialGeneration: 1,
		})
	if err != nil {
		t.Fatal(err)
	}
	seal.ResearchTools = production.Tools
	seal.ResearchModel = production.Model
	seal.ResearchToolPolicyDigest, err = runtimepolicy.DigestResearchToolPolicyV3(
		production.Tools)
	if err != nil {
		t.Fatal(err)
	}
	seal.Payload.PlannerBudget.MaxPlannerRounds = 4
	seal.Payload.PlannerBudget.MaxToolCalls = 8
	seal.Payload.PlannerBudget.MaxTokens = 16000
	seal.Payload.Definition.TaskName = "每小时检查 Kimi 会员定价页面的购买状态，状态变化时推送"
	seal.Payload.Definition.TaskManual = "持续监测 Kimi 会员定价页面的套餐购买状态。使用官方套餐状态工具读取 https://www.kimi.com/membership/pricing 的第一方结构化购买状态。目标：判断页面是否出现可购买、立即订阅、购买或支付入口，并区分以下状态：1. 可购买/开放购买（页面存在购买、订阅、支付等入口）；2. 售罄（页面显示售罄、无库存等）；3. 暂不可购买（页面显示暂不可购买、未开放、敬请期待等）。推送规则：仅当状态从\"不可购买或售罄\"变为\"可购买\"，或者购买条件（价格、购买门槛等）发生实质变化时立即推送。无变化不推送。第一次运行建立基线；若当前已可购买则直接推送。"
	initial, err := buildResearchPlannerPromptV33(seal, nil)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := buildResearchPlannerCatalogV33(seal)
	if err != nil {
		t.Fatal(err)
	}
	// The planner searches for capabilities, but production models can include
	// the research subjects in that query. Keep the generic real-time search
	// discoverable for the exact formal-B shape; otherwise the English query
	// only matches the unrelated Kimi product-status adapter and the scoped run
	// reaches synthesis with zero eligible evidence.
	formalBQuery := "OpenAI Anthropic Google DeepMind foundation model API agent major update official announcement past week"
	formalBMatches, err := catalog.Search(formalBQuery, 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(formalBMatches) == 0 || formalBMatches[0].Entry.Name != "web_search" {
		t.Fatalf("formal B query matches=%#v, want web_search first", formalBMatches)
	}
	productionQuery := "official structured tool to read first-party purchase status of Kimi membership pricing page https://www.kimi.com/membership/pricing"
	productionQuery += strings.Repeat(" x", 6)
	if len(productionQuery) != 144 {
		t.Fatalf("production max query bytes=%d", len(productionQuery))
	}
	matches, err := catalog.Search(productionQuery, 8)
	if err != nil || len(matches) != 3 {
		t.Fatalf("production query matches=%d err=%v", len(matches), err)
	}
	receiptMatches := make([]runcontext.ResearchPlannerToolSearchMatchV1, 0, len(matches))
	for _, match := range matches {
		tool, ok := frozenResearchToolV3(seal.ResearchTools, match.Entry.Name)
		if !ok {
			t.Fatalf("production search returned unknown tool %q", match.Entry.Name)
		}
		score, scoreErr := runcontext.CanonicalResearchPlannerSearchScoreV1(match.Score)
		if scoreErr != nil {
			t.Fatal(scoreErr)
		}
		receiptMatches = append(receiptMatches, runcontext.ResearchPlannerToolSearchMatchV1{
			Name: tool.Name, SchemaDigest: tool.SchemaDigest, Score: score,
		})
	}
	receipt, err := runcontext.BuildResearchPlannerToolSearchReceiptV1(
		0, seal.ResearchToolPolicyDigest, productionQuery, 8, receiptMatches)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := buildResearchPlannerPromptV33(
		seal, []runcontext.ResearchPlannerToolSearchReceiptV1{receipt})
	if err != nil {
		t.Fatal(err)
	}
	reserve := func(prompt string) int {
		return len(seal.ResearchModel.Planner.SystemPrompt) + len(prompt) + 64 +
			seal.ResearchModel.Planner.MaxTokens
	}
	// The final-only policy makes the normal production path one search plus one
	// final plan. Preserve the original 4096-token final capacity and prove both
	// paid rounds fit the exact retained 16k task budget with real tool schemas.
	total := reserve(initial) + reserve(loaded)
	if total > seal.Payload.PlannerBudget.MaxTokens {
		t.Fatalf("search/final reserves %d bytes over budget %d", total,
			seal.Payload.PlannerBudget.MaxTokens)
	}
	if remaining := seal.Payload.PlannerBudget.MaxTokens - total; remaining < 256 {
		t.Fatalf("search/final reserves leave only %d bytes of production margin", remaining)
	}
	legacySeal := seal
	legacySeal.ResearchModel.Planner.SystemPrompt =
		runtimepolicy.ResearchPlannerSystemPromptV33FinalOnly
	legacyInitial, err := buildResearchPlannerPromptV33(legacySeal, nil)
	if err != nil {
		t.Fatal(err)
	}
	legacyLoaded, err := buildResearchPlannerPromptV33(
		legacySeal, []runcontext.ResearchPlannerToolSearchReceiptV1{receipt})
	if err != nil {
		t.Fatal(err)
	}
	legacyReserve := func(prompt string) int {
		return len(legacySeal.ResearchModel.Planner.SystemPrompt) + len(prompt) + 64 +
			legacySeal.ResearchModel.Planner.MaxTokens
	}
	if legacyReserve(legacyInitial)+legacyReserve(legacyLoaded) <=
		seal.Payload.PlannerBudget.MaxTokens {
		t.Fatal("production three-hit fixture no longer reproduces the duplicate-schema overflow")
	}
	var decoded researchPlannerPromptV33
	if err := json.Unmarshal([]byte(loaded), &decoded); err != nil ||
		len(decoded.SearchHistory) != 1 || len(decoded.SearchHistory[0].Tools) != 0 ||
		len(decoded.LoadedTools) != 3 {
		t.Fatalf("compact loaded prompt projection is invalid: err=%v prompt=%s", err, loaded)
	}
}

func TestBuildResearchPlannerPromptV33RetainsVersionedSchemaProjection(t *testing.T) {
	_, seal := plannerToolSearchSealV33(t)
	tool := seal.ResearchTools.AllowedTools[0]
	receipt, err := runcontext.BuildResearchPlannerToolSearchReceiptV1(
		0, seal.ResearchToolPolicyDigest, "official web", 1,
		[]runcontext.ResearchPlannerToolSearchMatchV1{{
			Name: tool.Name, SchemaDigest: tool.SchemaDigest, Score: "1.000000000",
		}})
	if err != nil {
		t.Fatal(err)
	}
	for name, fixture := range map[string]struct {
		prompt       string
		historyTools int
		queryBytes   int
	}{
		"retained-final-only": {runtimepolicy.ResearchPlannerSystemPromptV33FinalOnly, 1, 512},
		"compact-loaded":      {runtimepolicy.ResearchPlannerSystemPromptV33CompactLoadedTools, 0, 512},
		"compact-loaded-v2":   {runtimepolicy.ResearchPlannerSystemPromptV33CompactLoadedToolsV2, 0, 144},
		"multi-entity-v3":     {runtimepolicy.ResearchPlannerSystemPromptV33MultiEntityWindowV3, 0, 144},
	} {
		t.Run(name, func(t *testing.T) {
			versionedSeal := seal
			versionedSeal.ResearchModel.Planner.SystemPrompt = fixture.prompt
			prompt, err := buildResearchPlannerPromptV33(
				versionedSeal, []runcontext.ResearchPlannerToolSearchReceiptV1{receipt})
			if err != nil {
				t.Fatal(err)
			}
			var decoded researchPlannerPromptV33
			if err := json.Unmarshal([]byte(prompt), &decoded); err != nil ||
				len(decoded.SearchHistory) != 1 ||
				len(decoded.SearchHistory[0].Tools) != fixture.historyTools ||
				len(decoded.LoadedTools) != 1 ||
				decoded.ToolSearch.QueryMaxBytes != fixture.queryBytes {
				t.Fatalf("versioned schema projection changed: err=%v prompt=%s", err, prompt)
			}
			query145 := strings.Repeat("x", 145)
			decision145 := `{"schema_version":"vane.research-planner-output/v3.3","action":"tool_search","tool_search":{"query":"` + query145 + `","limit":1}}`
			_, decodeErr := decodeResearchPlannerDecisionV33(
				[]byte(decision145), 2,
				researchPlannerSearchQueryMaxBytesV33(fixture.prompt))
			if (decodeErr == nil) != (fixture.queryBytes == 512) {
				t.Fatalf("versioned query admission mismatch: max=%d err=%v",
					fixture.queryBytes, decodeErr)
			}
		})
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
		if _, err := decodeResearchPlannerDecisionV33([]byte(raw), 2, 512); err != nil {
			t.Fatalf("valid decision rejected: %s: %v", raw, err)
		}
	}
	for _, raw := range []string{
		`{"schema_version":"vane.research-planner-output/v3.3","action":"tool_search","tool_search":{"query":"x","limit":9}}`,
		`{"schema_version":"vane.research-planner-output/v3.3","action":"tool_search","tool_search":{"query":" x","limit":1}}`,
		"{\"schema_version\":\"vane.research-planner-output/v3.3\",\"action\":\"tool_search\",\"tool_search\":{\"query\":\"x\\u0000y\",\"limit\":1}}",
		`{"schema_version":"vane.research-planner-output/v3.3","action":"final","steps":[],"extra":true}`,
		`{"schema_version":"vane.research-planner-output/v3.3","action":"final","steps":[{"invocation_id":"one","tool_name":"web_search","arguments":{"query":"x"}}]}`,
		`{"schema_version":"bad","schema_version":"vane.research-planner-output/v3.3","action":"final","steps":[]}`,
	} {
		if _, err := decodeResearchPlannerDecisionV33([]byte(raw), 2, 512); err == nil {
			t.Fatalf("invalid decision accepted: %s", raw)
		}
	}
	query144 := strings.Repeat("x", 144)
	valid144 := `{"schema_version":"vane.research-planner-output/v3.3","action":"tool_search","tool_search":{"query":"` + query144 + `","limit":1}}`
	if _, err := decodeResearchPlannerDecisionV33([]byte(valid144), 2, 144); err != nil {
		t.Fatalf("exact V2 query limit rejected: %v", err)
	}
	query145 := strings.Repeat("x", 145)
	invalid145 := `{"schema_version":"vane.research-planner-output/v3.3","action":"tool_search","tool_search":{"query":"` + query145 + `","limit":1}}`
	if _, err := decodeResearchPlannerDecisionV33([]byte(invalid145), 2, 144); err == nil {
		t.Fatal("over-limit V2 query accepted")
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
