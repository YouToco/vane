package cardgen

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/YouToco/vane/server/llm"
	"github.com/YouToco/vane/server/runtimepolicy"
	"github.com/YouToco/vane/server/types"
)

// RendererVersionV1 identifies the deterministic card-generation user-prompt
// renderer retained for historical V1 run snapshots.
const (
	RendererVersionV1 = "cardgen.render/v1"
	RendererVersionV2 = "cardgen.render/v2"
)

// PolicyV1 is a validated card-generation execution policy. Private fields
// prevent callers from bypassing PreparePolicyV1 with a partial policy.
type PolicyV1 struct {
	isPrepared bool
	execution  cardExecutionV1
}

type PolicyV2 struct {
	isPrepared bool
	execution  cardExecutionV1
}

type cardExecutionV1 struct {
	client                 *llm.Client
	systemPrompt           string
	model                  string
	temperature            float32
	maxTokens              int
	disableThinking        bool
	taskInstructionEnabled bool
	quotaRule              *runtimepolicy.QuotaBucketV1
}

// PrepareCompiledPolicyV1 additionally binds the immutable quota rule used by
// the compiled LLM call.
func PrepareCompiledPolicyV1(
	prompts runtimepolicy.PromptPolicyV1,
	models runtimepolicy.ModelPolicyV1,
	quotas runtimepolicy.QuotaPolicyV1,
	client *llm.Client,
) (PolicyV1, error) {
	if client == nil {
		return PolicyV1{}, fmt.Errorf("%w: cardgen model executor is missing", runtimepolicy.ErrInvalidPolicy)
	}
	policy, err := PreparePolicyV1(prompts, models)
	if err != nil {
		return PolicyV1{}, err
	}
	if err := quotas.Validate(); err != nil {
		return PolicyV1{}, fmt.Errorf("cardgen: validate quota policy v1: %w", err)
	}
	quota, ok := quotas.Bucket("llm_tokens")
	if !ok {
		return PolicyV1{}, fmt.Errorf("%w: cardgen llm quota is missing", runtimepolicy.ErrInvalidPolicy)
	}
	policy.execution.quotaRule = &quota
	policy.execution.client = client
	return policy, nil
}

func PrepareCompiledPolicyV2(
	prompts runtimepolicy.PromptPolicyV1,
	models runtimepolicy.ModelPolicyV1,
	quotas runtimepolicy.QuotaPolicyV1,
	client *llm.Client,
) (PolicyV2, error) {
	if client == nil {
		return PolicyV2{}, fmt.Errorf("%w: cardgen model executor is missing", runtimepolicy.ErrInvalidPolicy)
	}
	policy, err := PreparePolicyV2(prompts, models)
	if err != nil {
		return PolicyV2{}, err
	}
	if err := quotas.Validate(); err != nil {
		return PolicyV2{}, fmt.Errorf("cardgen: validate quota policy v2: %w", err)
	}
	quota, ok := quotas.Bucket("llm_tokens")
	if !ok {
		return PolicyV2{}, fmt.Errorf("%w: cardgen llm quota is missing", runtimepolicy.ErrInvalidPolicy)
	}
	policy.execution.quotaRule = &quota
	policy.execution.client = client
	return policy, nil
}

// CurrentPromptStageV1 returns the exact prompt and renderer used by legacy
// card generation today.
func CurrentPromptStageV1() runtimepolicy.PromptStageV1 {
	return runtimepolicy.PromptStageV1{
		SystemPrompt:    cardSystemPrompt,
		RendererVersion: RendererVersionV1,
	}
}

// CurrentModelCallV1 returns the current card-generation request parameters.
func CurrentModelCallV1(model string) runtimepolicy.ModelCallV1 {
	return runtimepolicy.ModelCallV1{
		Stage:           runtimepolicy.ModelStageCardGen,
		Model:           model,
		Temperature:     0.7,
		MaxTokens:       400,
		DisableThinking: true,
	}
}

func StructuredPromptStageV2() runtimepolicy.PromptStageV1 {
	return runtimepolicy.PromptStageV1{
		SystemPrompt:    structuredSystemPromptV1,
		RendererVersion: RendererVersionV2,
	}
}

func StructuredModelCallV2(model string) runtimepolicy.ModelCallV1 {
	return runtimepolicy.ModelCallV1{
		Stage: runtimepolicy.ModelStageCardGen, Model: model,
		Temperature: 0.2, MaxTokens: 900, DisableThinking: true,
	}
}

func PreparePolicyV2(
	prompts runtimepolicy.PromptPolicyV1,
	models runtimepolicy.ModelPolicyV1,
) (PolicyV2, error) {
	if err := prompts.Validate(); err != nil {
		return PolicyV2{}, fmt.Errorf("cardgen: validate prompt policy v2: %w", err)
	}
	if err := models.Validate(); err != nil {
		return PolicyV2{}, fmt.Errorf("cardgen: validate model policy v2: %w", err)
	}
	if prompts.CardGen.RendererVersion != RendererVersionV2 {
		return PolicyV2{}, fmt.Errorf("%w: cardgen renderer version is unsupported",
			runtimepolicy.ErrInvalidPolicy)
	}
	call, ok := models.Call(runtimepolicy.ModelStageCardGen)
	if !ok {
		return PolicyV2{}, fmt.Errorf("%w: cardgen model stage is missing",
			runtimepolicy.ErrInvalidPolicy)
	}
	return PolicyV2{
		isPrepared: true,
		execution: cardExecutionV1{
			systemPrompt: prompts.CardGen.SystemPrompt, model: call.Model,
			temperature: float32(call.Temperature), maxTokens: call.MaxTokens,
			disableThinking:        call.DisableThinking,
			taskInstructionEnabled: prompts.TaskInstructionEnabled,
		},
	}, nil
}

// PreparePolicyV1 validates and narrows a complete snapshot policy to the
// card-generation fields supported by this worker generation.
func PreparePolicyV1(
	prompts runtimepolicy.PromptPolicyV1,
	models runtimepolicy.ModelPolicyV1,
) (PolicyV1, error) {
	if err := prompts.Validate(); err != nil {
		return PolicyV1{}, fmt.Errorf("cardgen: validate prompt policy v1: %w", err)
	}
	if err := models.Validate(); err != nil {
		return PolicyV1{}, fmt.Errorf("cardgen: validate model policy v1: %w", err)
	}
	if prompts.CardGen.RendererVersion != RendererVersionV1 {
		return PolicyV1{}, fmt.Errorf(
			"%w: cardgen renderer version is unsupported",
			runtimepolicy.ErrInvalidPolicy,
		)
	}
	call, ok := models.Call(runtimepolicy.ModelStageCardGen)
	if !ok {
		return PolicyV1{}, fmt.Errorf(
			"%w: cardgen model stage is missing",
			runtimepolicy.ErrInvalidPolicy,
		)
	}
	return PolicyV1{
		isPrepared: true,
		execution: cardExecutionV1{
			systemPrompt:           prompts.CardGen.SystemPrompt,
			model:                  call.Model,
			temperature:            float32(call.Temperature),
			maxTokens:              call.MaxTokens,
			disableThinking:        call.DisableThinking,
			taskInstructionEnabled: prompts.TaskInstructionEnabled,
		},
	}, nil
}

// GenerateWithPolicyV1 executes card generation from a validated immutable
// run policy. A zero policy is rejected before profile reads or LLM calls.
func (cg *CardGen) GenerateWithPolicyV1(
	ctx context.Context,
	tenantID int64,
	userID int64,
	item types.ScoredItem,
	traceID string,
	taskInstruction string,
	policy PolicyV1,
	beforeSpend func(context.Context, float64) error,
) (string, error) {
	if !policy.isPrepared {
		return "", fmt.Errorf("%w: cardgen policy v1 is not prepared", runtimepolicy.ErrInvalidPolicy)
	}
	return cg.generate(ctx, tenantID, userID, item, traceID, taskInstruction, policy.execution, beforeSpend)
}

// GenerateWithEvidencePolicyV1 uses the same frozen V1 model/prompt policy,
// but renders the exact ordered evidence bundle admitted by the current run.
// It remains one CardGen call and returns the same markdown body contract.
func (cg *CardGen) GenerateWithEvidencePolicyV1(
	ctx context.Context,
	tenantID int64,
	userID int64,
	item types.ScoredItem,
	sources []EventEvidenceSourceV1,
	traceID string,
	taskInstruction string,
	policy PolicyV1,
	beforeSpend func(context.Context, float64) error,
) (string, error) {
	if !policy.isPrepared {
		return "", fmt.Errorf(
			"%w: cardgen evidence policy v1 is not prepared",
			runtimepolicy.ErrInvalidPolicy)
	}
	if _, err := EventEvidenceCorpusV1(sources); err != nil ||
		item.Item.ID <= 0 ||
		sources[0].ContentItemID != item.Item.ID {
		return "", types.NewAppError(types.CodeValidation,
			"cardgen evidence input is invalid", err)
	}
	body, err := cg.generateResponse(
		ctx, tenantID, userID, item, traceID, taskInstruction,
		policy.execution, beforeSpend,
		func(hint string, _ types.ContentItem) string {
			return buildEvidenceCardUserV1(hint, sources)
		},
	)
	if err != nil {
		return "", err
	}
	if body == "" {
		return "", types.NewAppError(types.CodeValidation,
			"cardgen evidence output is empty", nil)
	}
	return renderGroundedEvidenceBodyV1(
		body, taskInstruction, sources)
}

var evidenceOutputFieldV1 = regexp.MustCompile(
	`(?mi)^\s*(?:\*\*)?(变化|影响判断|change|what changed|impact|impact assessment)\s*[：:](?:\*\*)?\s*(.+?)\s*$`,
)
var evidenceOutputLabelV1 = regexp.MustCompile(
	`(?mi)^\s*(?:\*\*)?(?:变化|官方原文|交叉证据|影响判断|change|what changed|official source|cross evidence|impact|impact assessment)\s*[：:](?:\*\*)?\s*`,
)
var unsafeSemanticLinkV1 = regexp.MustCompile(
	`(?i)(?:[a-z][a-z0-9+.-]*:|//|\[[^\]]*\]\s*\(|!\[|<[^>]*>)`,
)

func renderGroundedEvidenceBodyV1(
	body string,
	taskInstruction string,
	sources []EventEvidenceSourceV1,
) (string, error) {
	if unsafeSemanticLinkV1.MatchString(
		evidenceOutputLabelV1.ReplaceAllString(body, ""),
	) {
		return "", types.NewAppError(types.CodeValidation,
			"cardgen evidence output contains model-authored link syntax", nil)
	}
	if !manualRequiresEvidenceOutputV1(taskInstruction) {
		return body, nil
	}
	if len(sources) < 2 {
		return "", types.NewAppError(types.CodeValidation,
			"cardgen evidence output lacks a cross-evidence source", nil)
	}
	fields := make(map[string]string, 2)
	for _, match := range evidenceOutputFieldV1.FindAllStringSubmatch(body, -1) {
		if len(match) != 3 {
			continue
		}
		value := strings.TrimSpace(match[2])
		if value != "" && !unsafeSemanticLinkV1.MatchString(value) {
			fields[canonicalEvidenceFieldV1(match[1])] =
				plainSemanticFieldV1(value)
		}
	}
	if fields["变化"] == "" || fields["影响判断"] == "" {
		return "", types.NewAppError(types.CodeValidation,
			"cardgen evidence output lacks required semantic fields", nil)
	}
	rendered, err := renderGroundedEvidenceFieldsV1(
		fields["变化"], fields["影响判断"], taskInstruction, sources)
	if err != nil {
		return "", err
	}
	if err := ValidateGroundedEvidenceBodyV1(
		rendered, taskInstruction, sources,
	); err != nil {
		return "", err
	}
	return rendered, nil
}

// RenderGroundedEvidenceInsightV1 turns a validated structured model result
// into the user-requested four-field body. The model supplies only semantics;
// code owns field labels and exact current-run evidence links.
func RenderGroundedEvidenceInsightV1(
	insight types.StructuredInsightV1,
	taskInstruction string,
	sources []EventEvidenceSourceV1,
) (string, error) {
	if _, err := EventEvidenceCorpusV1(sources); err != nil {
		return "", types.NewAppError(types.CodeValidation,
			"structured card evidence is invalid", err)
	}
	if !manualRequiresEvidenceOutputV1(taskInstruction) {
		body := strings.TrimSpace(insight.BodyMD)
		if body == "" || unsafeSemanticLinkV1.MatchString(body) {
			return "", types.NewAppError(types.CodeValidation,
				"structured card body is invalid", nil)
		}
		return body, nil
	}
	return renderGroundedEvidenceFieldsV1(
		insight.WhatChanged, insight.WhyItMatters,
		taskInstruction, sources)
}

func renderGroundedEvidenceFieldsV1(
	change string,
	impact string,
	taskInstruction string,
	sources []EventEvidenceSourceV1,
) (string, error) {
	change = plainSemanticFieldV1(change)
	impact = plainSemanticFieldV1(impact)
	if change == "" || impact == "" ||
		unsafeSemanticLinkV1.MatchString(change) ||
		unsafeSemanticLinkV1.MatchString(impact) ||
		len(sources) < 2 {
		return "", types.NewAppError(types.CodeValidation,
			"cardgen evidence output lacks required semantic fields", nil)
	}
	official := groundedEvidenceLinkV1(sources[0])
	cross := make([]string, 0, len(sources)-1)
	for _, source := range sources[1:] {
		cross = append(cross, groundedEvidenceLinkV1(source))
	}
	labels := evidenceOutputLabelsV1(taskInstruction)
	rendered := labels.change + change +
		"\n\n" + labels.official + official +
		"\n\n" + labels.cross + strings.Join(cross, "；") +
		"\n\n" + labels.impact + impact
	if err := ValidateGroundedEvidenceBodyV1(
		rendered, taskInstruction, sources,
	); err != nil {
		return "", err
	}
	return rendered, nil
}

func manualRequiresEvidenceOutputV1(taskInstruction string) bool {
	manual := strings.ToLower(taskInstruction)
	return manualRequiresChineseEvidenceOutputV1(manual) ||
		manualRequiresEnglishEvidenceOutputV1(manual)
}

func manualRequiresChineseEvidenceOutputV1(manual string) bool {
	return strings.Contains(manual, "变化") &&
		strings.Contains(manual, "官方原文") &&
		strings.Contains(manual, "交叉证据") &&
		strings.Contains(manual, "影响判断")
}

func manualRequiresEnglishEvidenceOutputV1(manual string) bool {
	return strings.Contains(manual, "change") &&
		strings.Contains(manual, "official") &&
		strings.Contains(manual, "cross") &&
		strings.Contains(manual, "impact")
}

func canonicalEvidenceFieldV1(field string) string {
	switch strings.ToLower(strings.TrimSpace(field)) {
	case "变化", "change", "what changed":
		return "变化"
	case "影响判断", "impact", "impact assessment":
		return "影响判断"
	default:
		return ""
	}
}

type evidenceOutputLabels struct {
	change   string
	official string
	cross    string
	impact   string
}

func evidenceOutputLabelsV1(taskInstruction string) evidenceOutputLabels {
	manual := strings.ToLower(taskInstruction)
	if manualRequiresEnglishEvidenceOutputV1(manual) &&
		!manualRequiresChineseEvidenceOutputV1(manual) {
		return evidenceOutputLabels{
			change:   "**Change:** ",
			official: "**Official source:** ",
			cross:    "**Cross evidence:** ",
			impact:   "**Impact:** ",
		}
	}
	return evidenceOutputLabels{
		change:   "**变化：** ",
		official: "**官方原文：** ",
		cross:    "**交叉证据：** ",
		impact:   "**影响判断：** ",
	}
}

func plainSemanticFieldV1(value string) string {
	return strings.TrimSpace(strings.NewReplacer(
		"*", "", "_", "", "`", "", "[", "〔", "]", "〕",
		"<", "＜", ">", "＞",
	).Replace(value))
}

func groundedEvidenceLinkV1(source EventEvidenceSourceV1) string {
	label := strings.TrimSpace(source.Metadata.Title)
	if label == "" {
		label = source.Metadata.SourceTitle
	}
	label = strings.NewReplacer(
		"[", "〔", "]", "〕", "(", "（", ")", "）",
	).Replace(label)
	return "[" + label + "](" + source.Metadata.SourceURL + ")"
}

// ValidateGroundedEvidenceBodyV1 is repeated at the push boundary so a
// forged Activity result cannot bypass the CardGen renderer.
func ValidateGroundedEvidenceBodyV1(
	body string,
	taskInstruction string,
	sources []EventEvidenceSourceV1,
) error {
	if _, err := EventEvidenceCorpusV1(sources); err != nil {
		return err
	}
	linkFree := stripGroundedEvidenceLinksV1(body, sources)
	linkFree = evidenceOutputLabelV1.ReplaceAllString(linkFree, "")
	if unsafeSemanticLinkV1.MatchString(linkFree) {
		return types.NewAppError(types.CodeValidation,
			"grounded evidence body contains an unowned link", nil)
	}
	if manualRequiresEvidenceOutputV1(taskInstruction) {
		if len(sources) < 2 {
			return types.NewAppError(types.CodeValidation,
				"grounded evidence body lacks cross evidence", nil)
		}
		labels := evidenceOutputLabelsV1(taskInstruction)
		for _, required := range []string{
			labels.change, labels.official,
			labels.cross, labels.impact,
		} {
			if strings.Count(body, required) != 1 {
				return types.NewAppError(types.CodeValidation,
					"grounded evidence body lacks a required field", nil)
			}
		}
		officialLine := labels.official +
			groundedEvidenceLinkV1(sources[0])
		cross := make([]string, 0, len(sources)-1)
		for _, source := range sources[1:] {
			cross = append(cross, groundedEvidenceLinkV1(source))
		}
		crossLine := labels.cross + strings.Join(cross, "；")
		if strings.Count(body, officialLine) != 1 ||
			strings.Count(body, crossLine) != 1 {
			return types.NewAppError(types.CodeValidation,
				"grounded evidence body differs from owned evidence links", nil)
		}
	}
	return nil
}

func stripGroundedEvidenceLinksV1(
	body string,
	sources []EventEvidenceSourceV1,
) string {
	for _, source := range sources {
		body = strings.ReplaceAll(
			body, groundedEvidenceLinkV1(source), "")
	}
	return body
}

func legacyCardExecutionV1() cardExecutionV1 {
	return cardExecutionV1{
		systemPrompt:           cardSystemPrompt,
		temperature:            0.7,
		maxTokens:              400,
		disableThinking:        true,
		taskInstructionEnabled: true,
	}
}
