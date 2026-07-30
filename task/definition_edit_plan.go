package task

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/YouToco/vane/acquisitiontool"
	"github.com/YouToco/vane/internal/strictjson"
	"github.com/YouToco/vane/observation"
	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

// DefinitionEditPlanCompiler turns the user-facing task manual into an
// internal, materializable fetch plan. The manual remains the only user-owned
// source of truth; callers never expose targets or source IDs to the user.
type DefinitionEditPlanCompiler interface {
	Translate(
		ctx context.Context,
		userID int64,
		content string,
	) (json.RawMessage, error)
}

type definitionEditPlanMaterializer interface {
	GetOrCreateFetchTarget(
		context.Context,
		*types.FetchTarget,
	) (int64, bool, error)
}

func (c *DefinitionEditController) recompileDefinitionEditPlan(
	ctx context.Context,
	userID int64,
	target taskstate.ApprovedDefinitionV1,
) (taskstate.ApprovedDefinitionV1, error) {
	if c.planCompiler == nil {
		return taskstate.ApprovedDefinitionV1{},
			errors.New("task: definition edit plan compiler is not configured")
	}
	materializer, ok := c.store.(definitionEditPlanMaterializer)
	if !ok {
		return taskstate.ApprovedDefinitionV1{},
			errors.New("task: definition edit plan materializer is not configured")
	}
	raw, err := c.planCompiler.Translate(ctx, userID, target.PlaybookContent)
	if err != nil {
		return taskstate.ApprovedDefinitionV1{},
			definitionEditControllerValidation("任务手册暂时无法编译，请稍后重试")
	}
	canonical, err := canonicalizeFetchPlan(raw)
	if err != nil {
		return taskstate.ApprovedDefinitionV1{},
			definitionEditControllerValidation("任务手册没有生成可执行的抓取计划")
	}
	var plan compiledFetchPlan
	if err := strictjson.DecodeExact(canonical, &plan); err != nil {
		return taskstate.ApprovedDefinitionV1{},
			definitionEditControllerValidation("任务手册抓取计划无效")
	}
	policy, err := definitionEditObservationPolicy(target.ScopeJSON)
	if err != nil {
		return taskstate.ApprovedDefinitionV1{},
			definitionEditControllerValidation("任务的新鲜度策略无效")
	}
	if policy != nil && policy.Mode == observation.ModeEvent &&
		policy.Evidence.Requirement == observation.EvidenceOfficialRequired {
		plan, err = alignOfficialEventFetchPlan(*policy, plan)
		if err != nil {
			return taskstate.ApprovedDefinitionV1{},
				definitionEditControllerValidation(
					"任务手册与官方证据范围无法对齐")
		}
	}
	return materializeDefinitionEditPlan(ctx, materializer, target, plan)
}

func definitionEditObservationPolicy(
	raw json.RawMessage,
) (*observation.PolicyV1, error) {
	scope, err := decodeDefinitionEditApprovedScope(raw)
	if err != nil {
		return nil, err
	}
	return scope.Observation, nil
}

func alignOfficialEventFetchPlan(
	policy observation.PolicyV1,
	plan compiledFetchPlan,
) (compiledFetchPlan, error) {
	allowed := make(map[string]struct{}, len(policy.Evidence.OfficialDomains))
	for _, domain := range policy.Evidence.OfficialDomains {
		allowed[domain] = struct{}{}
	}
	covered := make(map[string]struct{}, len(allowed))
	aligned := make([]compiledFetchTarget, 0, len(plan.Targets)+1)
	unassigned := make([]compiledFetchTarget, 0)
	for _, target := range plan.Targets {
		if types.Platform(target.Platform) != types.PlatformWeb ||
			types.Capability(target.Capability) != types.CapSearch {
			aligned = append(aligned, target)
			continue
		}
		config, err := decodeDefinitionEditSearchConfig(target.Config)
		if err != nil {
			return compiledFetchPlan{}, err
		}
		domains := make([]string, 0, len(config.IncludeDomains))
		for _, domain := range config.IncludeDomains {
			if _, ok := allowed[domain]; !ok {
				continue
			}
			domains = append(domains, domain)
			covered[domain] = struct{}{}
		}
		if len(domains) == 0 {
			unassigned = append(unassigned, target)
			continue
		}
		rebuilt, err := rebuildDefinitionEditSearchTarget(
			target, config.Query, config.Category, domains,
		)
		if err != nil {
			return compiledFetchPlan{}, err
		}
		aligned = append(aligned, rebuilt)
	}
	missing := make([]string, 0, len(allowed))
	for _, domain := range policy.Evidence.OfficialDomains {
		if _, ok := covered[domain]; !ok {
			missing = append(missing, domain)
		}
	}
	if len(missing) > 0 && len(unassigned) > 0 {
		config, err := decodeDefinitionEditSearchConfig(unassigned[0].Config)
		if err != nil {
			return compiledFetchPlan{}, err
		}
		rebuilt, err := rebuildDefinitionEditSearchTarget(
			unassigned[0], config.Query, config.Category, missing,
		)
		if err != nil {
			return compiledFetchPlan{}, err
		}
		aligned = append(aligned, rebuilt)
		missing = nil
	}
	if len(missing) > 0 {
		query := strings.TrimSpace(
			policy.Event.Subject + " " + policy.Event.EventKind,
		)
		rebuilt, err := rebuildDefinitionEditSearchTarget(
			compiledFetchTarget{}, query, "", missing,
		)
		if err != nil {
			return compiledFetchPlan{}, err
		}
		aligned = append(aligned, rebuilt)
	}
	if len(aligned) == 0 {
		return compiledFetchPlan{}, errors.New(
			"official event fetch plan has no admissible targets")
	}
	return compiledFetchPlan{Targets: aligned}, nil
}

type definitionEditSearchConfig struct {
	Query          string   `json:"query"`
	Category       string   `json:"category,omitempty"`
	IncludeDomains []string `json:"include_domains,omitempty"`
}

func decodeDefinitionEditSearchConfig(
	raw json.RawMessage,
) (definitionEditSearchConfig, error) {
	var config definitionEditSearchConfig
	if err := strictjson.DecodeExact(raw, &config); err != nil ||
		strings.TrimSpace(config.Query) == "" {
		return definitionEditSearchConfig{},
			errors.New("official event search config is invalid")
	}
	return config, nil
}

func rebuildDefinitionEditSearchTarget(
	original compiledFetchTarget,
	query string,
	category string,
	domains []string,
) (compiledFetchTarget, error) {
	encodedDomains, err := json.Marshal(domains)
	if err != nil {
		return compiledFetchTarget{}, err
	}
	params := map[string]string{
		"query": query, "include_domains": string(encodedDomains),
	}
	if category != "" {
		params["category"] = category
	}
	materialized, message := acquisitiontool.BuildTarget(
		acquisitiontool.Requirement{
			Platform: string(types.PlatformWeb), Capability: string(types.CapSearch),
			Title: original.Title, Params: params,
		},
	)
	if message != "" || materialized == nil {
		if message == "" {
			message = "official event search target could not be rebuilt"
		}
		return compiledFetchTarget{}, errors.New(message)
	}
	return compiledFetchTarget{
		Platform: string(materialized.Platform), Capability: string(materialized.Capability),
		Title: materialized.Title, URL: materialized.URL,
		Config: materialized.Config,
	}, nil
}

func materializeDefinitionEditPlan(
	ctx context.Context,
	materializer definitionEditPlanMaterializer,
	target taskstate.ApprovedDefinitionV1,
	plan compiledFetchPlan,
) (taskstate.ApprovedDefinitionV1, error) {
	legacyPlan := taskstate.FetchPlanV1{
		Sources: make([]taskstate.PlanSourceV1, 0, len(plan.Targets)),
	}
	approved := make([]taskstate.ApprovedSourceV1, 0, len(plan.Targets))
	for _, source := range plan.Targets {
		fetchTarget := &types.FetchTarget{
			Platform: types.Platform(source.Platform),
			Capability: types.Capability(source.Capability),
			Title: source.Title, URL: source.URL, Config: source.Config,
			Status: types.FetchTargetStatusActive,
		}
		sourceID, _, err := materializer.GetOrCreateFetchTarget(ctx, fetchTarget)
		if err != nil {
			return taskstate.ApprovedDefinitionV1{}, err
		}
		if sourceID <= 0 {
			return taskstate.ApprovedDefinitionV1{},
				errors.New("task: materialized definition edit target has no ID")
		}
		legacyPlan.Sources = append(legacyPlan.Sources, taskstate.PlanSourceV1{
			Platform: fetchTarget.Platform, Capability: fetchTarget.Capability,
			Title: fetchTarget.Title, URL: fetchTarget.URL,
			Config: fetchTarget.Config,
		})
		approved = append(approved, taskstate.ApprovedSourceV1{
			SourceID: sourceID, Platform: fetchTarget.Platform,
			Capability: fetchTarget.Capability, Title: fetchTarget.Title,
			URL: fetchTarget.URL, Config: fetchTarget.Config,
		})
	}
	fetchPlan, err := json.Marshal(legacyPlan)
	if err != nil {
		return taskstate.ApprovedDefinitionV1{},
			fmt.Errorf("marshal definition edit fetch plan: %w", err)
	}
	return taskstate.BuildApprovedDefinitionV1(
		taskstate.ApprovedDefinitionInputV1{
			TenantID: target.TenantID, UserID: target.UserID, TaskID: target.TaskID,
			Intent: target.Intent, NLDescription: target.NLDescription,
			SpecJSON: target.SpecJSON, ScopeJSON: target.ScopeJSON,
			PlaybookContent: target.PlaybookContent,
			SourceScope: taskstate.SourceScopeApprovedPlan,
			FetchPlan: fetchPlan, Strictness: target.Strictness,
			Sources: approved, ExecutionMode: target.ExecutionMode,
			DeliveryPolicy: target.DeliveryPolicy,
			BudgetPolicy: target.BudgetPolicy,
		},
	)
}
