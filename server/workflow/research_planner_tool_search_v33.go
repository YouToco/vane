package workflow

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/YouToco/vane/acquisitiontool"
	"github.com/YouToco/vane/internal/strictjson"
	"github.com/YouToco/vane/runcontext"
	"github.com/YouToco/vane/runtimepolicy"
	storepkg "github.com/YouToco/vane/store"
	"github.com/YouToco/vane/toolsearch"
	"github.com/YouToco/vane/types"
)

const researchPlannerOutputSchemaV33 = "vane.research-planner-output/v3.3"

const (
	researchPlannerActionSearchV33 = "tool_search"
	researchPlannerActionFinalV33  = "final"
)

type researchPlannerToolSearchRequestV33 struct {
	Query string `json:"query"`
	Limit int    `json:"limit"`
}

type researchPlannerDecisionV33 struct {
	SchemaVersion string                               `json:"schema_version"`
	Action        string                               `json:"action"`
	ToolSearch    *researchPlannerToolSearchRequestV33 `json:"tool_search,omitempty"`
	Steps         []runcontext.ResearchPlanStepV3      `json:"steps,omitempty"`
}

type researchPlannerToolSearchContractV33 struct {
	Name            string `json:"name"`
	CatalogDigest   string `json:"catalog_digest"`
	QueryMaxBytes   int    `json:"query_max_bytes"`
	LimitDefault    int    `json:"limit_default"`
	LimitMaximum    int    `json:"limit_maximum"`
	NetworkEffect   bool   `json:"network_effect"`
	ResultAuthority string `json:"result_authority"`
}

type researchPlannerResponseContractV33 struct {
	SchemaVersionLiteral string   `json:"schema_version_literal"`
	Actions              []string `json:"actions"`
	SearchFields         []string `json:"tool_search_fields"`
	FinalFields          []string `json:"final_fields"`
	RequiredStepFields   []string `json:"required_step_fields"`
	MinSteps             int      `json:"min_steps"`
	MaxSteps             int      `json:"max_steps"`
	LoadedToolRule       string   `json:"loaded_tool_rule"`
	ExtraFieldsAllowed   bool     `json:"extra_fields_allowed"`
	SingleJSONObject     bool     `json:"single_json_object"`
}

type researchPlannerSearchHistoryV33 struct {
	Receipt runcontext.ResearchPlannerToolSearchReceiptV1 `json:"receipt"`
	Tools   []researchPlannerPromptToolV3                 `json:"tools"`
}

type researchPlannerPromptV33 struct {
	SchemaVersion     string                               `json:"schema_version"`
	TaskName          string                               `json:"task_name"`
	TaskManual        string                               `json:"task_manual"`
	HistoryThroughUTC string                               `json:"history_through_utc"`
	MaxToolCalls      int                                  `json:"max_tool_calls"`
	ToolSearch        researchPlannerToolSearchContractV33 `json:"tool_search"`
	SearchHistory     []researchPlannerSearchHistoryV33    `json:"search_history"`
	LoadedTools       []researchPlannerPromptToolV3        `json:"loaded_tools"`
	ResponseContract  researchPlannerResponseContractV33   `json:"response_contract"`
}

type researchPlannerSearchReceiptWriterV33 func(
	context.Context,
	storepkg.ResearchRunLLMSpendReservationV3,
	runcontext.ResearchPlannerToolSearchReceiptV1,
) (runcontext.ResearchPlannerToolSearchReceiptV1, error)

func buildResearchPlannerCatalogV33(
	seal runcontext.ResearchSnapshotSealV3,
) (*toolsearch.Catalog, error) {
	if seal.ResearchTools.Validate() != nil || seal.ResearchToolPolicyDigest == "" {
		return nil, researchCoordinatorValidationV3("research planner search catalog is invalid")
	}
	entries := make([]toolsearch.Entry, 0, len(seal.ResearchTools.AllowedTools))
	for _, tool := range seal.ResearchTools.AllowedTools {
		tags := make([]string, 0, len(tool.Effects)+1)
		for _, effect := range tool.Effects {
			tags = append(tags, string(effect))
		}
		tags = append(tags, string(tool.ResultTrust))
		entries = append(entries, toolsearch.Entry{
			Namespace: "research/" + tool.Provider,
			Name:      tool.Name, Description: tool.Description,
			Parameters: append(json.RawMessage(nil), tool.Parameters...),
			Tags:       tags,
		})
	}
	catalog, err := toolsearch.NewCatalog(entries)
	if err != nil {
		return nil, researchCoordinatorValidationV3("research planner search catalog is unavailable")
	}
	return catalog, nil
}

func decodeResearchPlannerDecisionV33(
	raw []byte, maxToolCalls, maxSearchQueryBytes int,
) (researchPlannerDecisionV33, error) {
	if len(raw) < 2 || len(raw) > 256<<10 || maxToolCalls <= 0 || maxToolCalls > 16 ||
		maxSearchQueryBytes <= 0 || maxSearchQueryBytes > 512 {
		return researchPlannerDecisionV33{}, researchCoordinatorValidationV3("research planner decision is invalid")
	}
	var fields map[string]json.RawMessage
	var decision researchPlannerDecisionV33
	if strictjson.DecodeExact(raw, &fields) != nil || strictjson.DecodeExact(raw, &decision) != nil ||
		decision.SchemaVersion != researchPlannerOutputSchemaV33 {
		return researchPlannerDecisionV33{}, researchCoordinatorValidationV3("research planner decision is invalid")
	}
	var expected []string
	switch decision.Action {
	case researchPlannerActionSearchV33:
		expected = []string{"action", "schema_version", "tool_search"}
		if decision.ToolSearch == nil || decision.Steps != nil ||
			decision.ToolSearch.Query == "" ||
			len(decision.ToolSearch.Query) > maxSearchQueryBytes ||
			!utf8.ValidString(decision.ToolSearch.Query) ||
			strings.IndexByte(decision.ToolSearch.Query, 0) >= 0 ||
			strings.TrimSpace(decision.ToolSearch.Query) != decision.ToolSearch.Query ||
			decision.ToolSearch.Limit < 1 || decision.ToolSearch.Limit > 8 {
			return researchPlannerDecisionV33{}, researchCoordinatorValidationV3("research planner tool search request is invalid")
		}
	case researchPlannerActionFinalV33:
		expected = []string{"action", "schema_version", "steps"}
		if decision.ToolSearch != nil ||
			len(decision.Steps) < minimumResearchPlannerStepsV33(maxToolCalls) ||
			len(decision.Steps) > maxToolCalls {
			return researchPlannerDecisionV33{}, researchCoordinatorValidationV3("research planner final decision is invalid")
		}
	default:
		return researchPlannerDecisionV33{}, researchCoordinatorValidationV3("research planner action is invalid")
	}
	actual := make([]string, 0, len(fields))
	for key := range fields {
		actual = append(actual, key)
	}
	sort.Strings(actual)
	if !equalResearchPlannerStringsV33(actual, expected) {
		return researchPlannerDecisionV33{}, researchCoordinatorValidationV3("research planner decision fields are invalid")
	}
	return decision, nil
}

func equalResearchPlannerStringsV33(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func minimumResearchPlannerStepsV33(maxToolCalls int) int {
	if maxToolCalls >= 2 {
		return 2
	}
	return 1
}

func buildResearchPlannerPromptV33(
	seal runcontext.ResearchSnapshotSealV3,
	receipts []runcontext.ResearchPlannerToolSearchReceiptV1,
) (string, error) {
	compactLoadedTools := seal.ResearchModel.Planner.SystemPrompt ==
		runtimepolicy.ResearchPlannerSystemPromptV33CompactLoadedTools ||
		seal.ResearchModel.Planner.SystemPrompt ==
			runtimepolicy.ResearchPlannerSystemPromptV33CompactLoadedToolsV2 ||
		seal.ResearchModel.Planner.SystemPrompt ==
			runtimepolicy.ResearchPlannerSystemPromptV33MultiEntityWindowV3
	loaded := make(map[string]researchPlannerPromptToolV3)
	history := make([]researchPlannerSearchHistoryV33, 0, len(receipts))
	for index, receipt := range receipts {
		if receipt.Validate() != nil || receipt.RoundOrdinal >= seal.Payload.PlannerBudget.MaxPlannerRounds ||
			receipt.CatalogDigest != seal.ResearchToolPolicyDigest ||
			(index > 0 && receipts[index-1].RoundOrdinal >= receipt.RoundOrdinal) {
			return "", researchCoordinatorValidationV3("research planner search history is invalid")
		}
		tools := make([]researchPlannerPromptToolV3, 0, len(receipt.Matches))
		for _, match := range receipt.Matches {
			tool, ok := frozenResearchToolV3(seal.ResearchTools, match.Name)
			if !ok || tool.SchemaDigest != match.SchemaDigest {
				return "", researchCoordinatorValidationV3("research planner search history differs from frozen catalog")
			}
			promptTool := researchPlannerPromptToolV3{
				Name: tool.Name, Description: tool.Description,
				Parameters: append(json.RawMessage(nil), tool.Parameters...),
			}
			tools = append(tools, promptTool)
			loaded[tool.Name] = promptTool
		}
		historyTools := tools
		if compactLoadedTools {
			// The immutable receipt already binds the ordered names, schema
			// digests, and scores. New frozen policies project each full schema
			// once through loaded_tools instead of duplicating it here.
			historyTools = []researchPlannerPromptToolV3{}
		}
		history = append(history, researchPlannerSearchHistoryV33{
			Receipt: receipt, Tools: historyTools,
		})
	}
	loadedNames := make([]string, 0, len(loaded))
	for name := range loaded {
		loadedNames = append(loadedNames, name)
	}
	sort.Strings(loadedNames)
	loadedTools := make([]researchPlannerPromptToolV3, 0, len(loadedNames))
	for _, name := range loadedNames {
		loadedTools = append(loadedTools, loaded[name])
	}
	payload, err := json.Marshal(researchPlannerPromptV33{
		SchemaVersion:     "vane.research-planner-input/v3.3",
		TaskName:          seal.Payload.Definition.TaskName,
		TaskManual:        seal.Payload.Definition.TaskManual,
		HistoryThroughUTC: seal.Payload.HistoryThroughUTC,
		MaxToolCalls:      seal.Payload.PlannerBudget.MaxToolCalls,
		ToolSearch: researchPlannerToolSearchContractV33{
			Name: "tool_search", CatalogDigest: seal.ResearchToolPolicyDigest,
			QueryMaxBytes: researchPlannerSearchQueryMaxBytesV33(
				seal.ResearchModel.Planner.SystemPrompt),
			LimitDefault: 8, LimitMaximum: 8,
			NetworkEffect:   false,
			ResultAuthority: "only tools returned in search_history may appear in final steps",
		},
		SearchHistory: history,
		LoadedTools:   loadedTools,
		ResponseContract: researchPlannerResponseContractV33{
			SchemaVersionLiteral: researchPlannerOutputSchemaV33,
			Actions:              []string{researchPlannerActionSearchV33, researchPlannerActionFinalV33},
			SearchFields:         []string{"schema_version", "action", "tool_search"},
			FinalFields:          []string{"schema_version", "action", "steps"},
			RequiredStepFields:   []string{"invocation_id", "tool_name", "arguments"},
			MinSteps:             minimumResearchPlannerStepsV33(seal.Payload.PlannerBudget.MaxToolCalls),
			MaxSteps:             seal.Payload.PlannerBudget.MaxToolCalls,
			LoadedToolRule:       "final steps[].tool_name must exactly equal a loaded_tools[].name",
			ExtraFieldsAllowed:   false, SingleJSONObject: true,
		},
	})
	if err != nil || len(payload) < 2 || len(payload) > 2<<20 {
		return "", researchCoordinatorValidationV3("research planner tool search prompt is invalid")
	}
	return string(payload), nil
}

func executeResearchPlannerToolSearchRoundsV33(
	ctx context.Context,
	snapshot types.ResearchRunSnapshotRefV3,
	seal runcontext.ResearchSnapshotSealV3,
	execute researchPlannerRoundExecutorV3,
	persist researchPlannerSearchReceiptWriterV33,
) (runcontext.ResearchExecutionPlanV3, storepkg.ResearchRunLLMSpendReservationV3, error) {
	maxRounds := snapshot.PlannerBudget.MaxPlannerRounds
	if execute == nil || persist == nil || maxRounds <= 1 || maxRounds > 8 ||
		seal.ResearchModel.Planner.RendererVersion != runtimepolicy.ResearchPlannerRendererVersionV33 {
		return runcontext.ResearchExecutionPlanV3{}, storepkg.ResearchRunLLMSpendReservationV3{},
			researchCoordinatorValidationV3("research planner tool search budget is invalid")
	}
	catalog, err := buildResearchPlannerCatalogV33(seal)
	if err != nil {
		return runcontext.ResearchExecutionPlanV3{}, storepkg.ResearchRunLLMSpendReservationV3{}, err
	}
	receipts := make([]runcontext.ResearchPlannerToolSearchReceiptV1, 0, maxRounds-1)
	prompt, err := buildResearchPlannerPromptV33(seal, receipts)
	if err != nil {
		return runcontext.ResearchExecutionPlanV3{}, storepkg.ResearchRunLLMSpendReservationV3{}, err
	}
	for round := 0; round < maxRounds; round++ {
		modelReceipt, reservation, callErr := execute(ctx, round, prompt)
		if callErr != nil {
			return runcontext.ResearchExecutionPlanV3{}, storepkg.ResearchRunLLMSpendReservationV3{}, callErr
		}
		decision, decisionErr := decodeResearchPlannerDecisionV33(
			[]byte(modelReceipt.Call.Completion), snapshot.PlannerBudget.MaxToolCalls,
			researchPlannerSearchQueryMaxBytesV33(seal.ResearchModel.Planner.SystemPrompt))
		if decisionErr != nil {
			prompt, err = buildResearchPlannerCorrectionPromptV3(
				prompt, runtimepolicy.ResearchPlannerRendererVersionV33)
			if err != nil {
				return runcontext.ResearchExecutionPlanV3{}, storepkg.ResearchRunLLMSpendReservationV3{}, err
			}
			continue
		}
		if decision.Action == researchPlannerActionFinalV33 {
			loaded := make(map[string]struct{})
			for _, receipt := range receipts {
				for _, match := range receipt.Matches {
					loaded[match.Name] = struct{}{}
				}
			}
			for _, step := range decision.Steps {
				if _, ok := loaded[step.ToolName]; !ok {
					return runcontext.ResearchExecutionPlanV3{}, storepkg.ResearchRunLLMSpendReservationV3{},
						researchCoordinatorValidationV3("research planner selected a tool without a search receipt")
				}
			}
			plan, planErr := runcontext.BuildResearchExecutionPlanV3(
				snapshot.DefinitionDigest, snapshot.CapabilityCatalogDigest,
				snapshot.ToolPolicyDigest, decision.Steps,
				canonicalResearchPlannerArgumentsV33)
			if planErr != nil {
				return runcontext.ResearchExecutionPlanV3{}, storepkg.ResearchRunLLMSpendReservationV3{}, planErr
			}
			return plan, reservation, nil
		}
		// New v3.3 policies make a non-empty loaded_tools set final-only. Enforce
		// that frozen policy in code as well as in prose: a redundant model search
		// cannot create another catalog authority receipt after schemas are loaded.
		// Older v3.3 snapshots lack this exact frozen policy marker and retain
		// their historical multi-search replay semantics unchanged.
		if researchPlannerFinalOnlyV33(seal.ResearchModel.Planner.SystemPrompt) &&
			plannerHasLoadedToolsV33(receipts) {
			prompt, err = buildResearchPlannerCorrectionPromptV3(
				prompt, runtimepolicy.ResearchPlannerRendererVersionV33)
			if err != nil {
				return runcontext.ResearchExecutionPlanV3{},
					storepkg.ResearchRunLLMSpendReservationV3{}, err
			}
			continue
		}
		matches, searchErr := catalog.Search(decision.ToolSearch.Query, decision.ToolSearch.Limit)
		if searchErr != nil {
			return runcontext.ResearchExecutionPlanV3{}, storepkg.ResearchRunLLMSpendReservationV3{},
				researchCoordinatorValidationV3("research planner tool search failed")
		}
		receiptMatches := make([]runcontext.ResearchPlannerToolSearchMatchV1, 0, len(matches))
		for _, match := range matches {
			frozen, ok := frozenResearchToolV3(seal.ResearchTools, match.Entry.Name)
			if !ok {
				return runcontext.ResearchExecutionPlanV3{}, storepkg.ResearchRunLLMSpendReservationV3{},
					researchCoordinatorValidationV3("research planner search returned an unknown tool")
			}
			score, scoreErr := runcontext.CanonicalResearchPlannerSearchScoreV1(match.Score)
			if scoreErr != nil {
				return runcontext.ResearchExecutionPlanV3{}, storepkg.ResearchRunLLMSpendReservationV3{}, scoreErr
			}
			receiptMatches = append(receiptMatches, runcontext.ResearchPlannerToolSearchMatchV1{
				Name: frozen.Name, SchemaDigest: frozen.SchemaDigest, Score: score,
			})
		}
		built, buildErr := runcontext.BuildResearchPlannerToolSearchReceiptV1(
			round, seal.ResearchToolPolicyDigest, decision.ToolSearch.Query,
			decision.ToolSearch.Limit, receiptMatches)
		if buildErr != nil {
			return runcontext.ResearchExecutionPlanV3{}, storepkg.ResearchRunLLMSpendReservationV3{}, buildErr
		}
		stored, persistErr := persist(ctx, reservation, built)
		if persistErr != nil {
			return runcontext.ResearchExecutionPlanV3{}, storepkg.ResearchRunLLMSpendReservationV3{}, persistErr
		}
		builtBytes, _ := runcontext.EncodeResearchPlannerToolSearchReceiptV1(built)
		storedBytes, storedErr := runcontext.EncodeResearchPlannerToolSearchReceiptV1(stored)
		if storedErr != nil || !bytes.Equal(builtBytes, storedBytes) {
			return runcontext.ResearchExecutionPlanV3{}, storepkg.ResearchRunLLMSpendReservationV3{},
				researchCoordinatorValidationV3("research planner stored search receipt differs")
		}
		receipts = append(receipts, stored)
		prompt, err = buildResearchPlannerPromptV33(seal, receipts)
		if err != nil {
			return runcontext.ResearchExecutionPlanV3{}, storepkg.ResearchRunLLMSpendReservationV3{}, err
		}
	}
	return runcontext.ResearchExecutionPlanV3{}, storepkg.ResearchRunLLMSpendReservationV3{},
		researchCoordinatorValidationV3("research planner exhausted its tool search budget")
}

func researchPlannerFinalOnlyV33(systemPrompt string) bool {
	return systemPrompt == runtimepolicy.ResearchPlannerSystemPromptV33FinalOnly ||
		systemPrompt == runtimepolicy.ResearchPlannerSystemPromptV33CompactLoadedTools ||
		systemPrompt == runtimepolicy.ResearchPlannerSystemPromptV33CompactLoadedToolsV2 ||
		systemPrompt == runtimepolicy.ResearchPlannerSystemPromptV33MultiEntityWindowV3
}

func researchPlannerSearchQueryMaxBytesV33(systemPrompt string) int {
	if systemPrompt == runtimepolicy.ResearchPlannerSystemPromptV33CompactLoadedToolsV2 ||
		systemPrompt == runtimepolicy.ResearchPlannerSystemPromptV33MultiEntityWindowV3 {
		return 144
	}
	return 512
}

func plannerHasLoadedToolsV33(
	receipts []runcontext.ResearchPlannerToolSearchReceiptV1,
) bool {
	for _, receipt := range receipts {
		if len(receipt.Matches) > 0 {
			return true
		}
	}
	return false
}

func canonicalResearchPlannerArgumentsV33(toolName string, arguments json.RawMessage) (json.RawMessage, error) {
	canonical, err := acquisitiontool.CanonicalizeToolArgumentsV1(toolName, arguments)
	if err != nil {
		return nil, fmt.Errorf("research planner arguments: %w", err)
	}
	return canonical, nil
}
