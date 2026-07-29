package taskstate

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"slices"
	"sort"

	"github.com/YouToco/vane/fetchspec"
	"github.com/YouToco/vane/internal/strictjson"
	"github.com/YouToco/vane/types"
)

const (
	// ApprovedDefinitionSchemaVersionV1 identifies the immutable current
	// approved-definition wire. Future versions must retain a dedicated reader.
	ApprovedDefinitionSchemaVersionV1 = "vane.task-approved-definition/v1"

	// DeliveryPolicyOwnerFeishu means delivery resolves the task owner's
	// current Feishu binding at send time; no account or credential enters the
	// definition payload.
	DeliveryPolicyOwnerFeishu DeliveryPolicy = "owner_feishu"
	// BudgetPolicyInheritTenantQuota means live quota state is authorized at
	// spend time. Rates, balances, and credentials are deliberately absent.
	BudgetPolicyInheritTenantQuota BudgetPolicy = "inherit_tenant_quota"

	// SourceScopeApprovedPlan means FetchPlan and Sources are the exact,
	// user-approved long-term source set.
	SourceScopeApprovedPlan SourceScope = "approved_plan"
	// SourceScopeLegacySubscriptions is a frozen reader-only marker for rows
	// written before account subscriptions were retired.
	SourceScopeLegacySubscriptions SourceScope = "legacy_subscriptions"
)

// DeliveryPolicy is an approved non-secret delivery policy identifier.
type DeliveryPolicy string

// BudgetPolicy is an approved non-secret budget policy identifier.
type BudgetPolicy string

// SourceScope distinguishes an exact approved plan from historical v1 rows.
type SourceScope string

// PlanSourceV1 is one exact, approved source in execution-plan order. Config
// is a strict JSON object; the current writer additionally validates its
// capability-specific schema before persistence.
type PlanSourceV1 struct {
	Platform   types.Platform   `json:"platform"`
	Capability types.Capability `json:"capability"`
	Title      string           `json:"title"`
	URL        string           `json:"url"`
	Config     json.RawMessage  `json:"config"`
}

// FetchPlanV1 preserves the user's approved execution order. Its source set
// must exactly match ApprovedDefinitionV1.Sources.
type FetchPlanV1 struct {
	Sources []PlanSourceV1 `json:"sources"`
}

// ApprovedSourceV1 binds an approved plan source to its stable database ID.
// Entries are canonicalized by URL, then SourceID.
type ApprovedSourceV1 struct {
	SourceID   int64            `json:"source_id"`
	Platform   types.Platform   `json:"platform"`
	Capability types.Capability `json:"capability"`
	Title      string           `json:"title"`
	URL        string           `json:"url"`
	Config     json.RawMessage  `json:"config"`
}

// ApprovedDefinitionV1 is the complete user-confirmed task definition. It is
// physically independent from AdaptiveStateV1: no automatic update should be
// able to change these fields.
type ApprovedDefinitionV1 struct {
	SchemaVersion   string               `json:"schema_version"`
	TenantID        int64                `json:"tenant_id"`
	UserID          int64                `json:"user_id"`
	TaskID          string               `json:"task_id"`
	Intent          string               `json:"intent"`
	NLDescription   string               `json:"nl_description"`
	SpecJSON        json.RawMessage      `json:"spec_json"`
	ScopeJSON       json.RawMessage      `json:"scope_json"`
	PlaybookContent string               `json:"playbook_content"`
	SourceScope     SourceScope          `json:"source_scope"`
	FetchPlan       json.RawMessage      `json:"fetch_plan"`
	Strictness      types.PushStrictness `json:"strictness"`
	Sources         []ApprovedSourceV1   `json:"sources"`
	ExecutionMode   types.ExecutionMode  `json:"execution_mode"`
	DeliveryPolicy  DeliveryPolicy       `json:"delivery_policy"`
	BudgetPolicy    BudgetPolicy         `json:"budget_policy"`
}

// ApprovedDefinitionInputV1 is the explicit construction boundary. The
// current writer accepts only compiled mode; decoding can still recognize a
// structurally valid discover_at_run V1 for forward-safe inspection.
type ApprovedDefinitionInputV1 struct {
	TenantID        int64
	UserID          int64
	TaskID          string
	Intent          string
	NLDescription   string
	SpecJSON        json.RawMessage
	ScopeJSON       json.RawMessage
	PlaybookContent string
	SourceScope     SourceScope
	FetchPlan       json.RawMessage
	Strictness      types.PushStrictness
	Sources         []ApprovedSourceV1
	ExecutionMode   types.ExecutionMode
	DeliveryPolicy  DeliveryPolicy
	BudgetPolicy    BudgetPolicy
}

type approvedDefinitionV1Wire ApprovedDefinitionV1

// BuildApprovedDefinitionV1 constructs the only currently writable V1 shape.
// DiscoverAtRun remains behind the later control-plane gate.
func BuildApprovedDefinitionV1(input ApprovedDefinitionInputV1) (ApprovedDefinitionV1, error) {
	definition := ApprovedDefinitionV1{
		SchemaVersion:   ApprovedDefinitionSchemaVersionV1,
		TenantID:        input.TenantID,
		UserID:          input.UserID,
		TaskID:          input.TaskID,
		Intent:          input.Intent,
		NLDescription:   input.NLDescription,
		SpecJSON:        input.SpecJSON,
		ScopeJSON:       input.ScopeJSON,
		PlaybookContent: input.PlaybookContent,
		SourceScope:     input.SourceScope,
		FetchPlan:       input.FetchPlan,
		Strictness:      input.Strictness,
		Sources:         input.Sources,
		ExecutionMode:   input.ExecutionMode,
		DeliveryPolicy:  input.DeliveryPolicy,
		BudgetPolicy:    input.BudgetPolicy,
	}
	normalized, err := normalizeApprovedDefinitionV1(definition)
	if err != nil {
		return ApprovedDefinitionV1{}, err
	}
	if err := validateApprovedDefinitionV1CurrentWriter(normalized); err != nil {
		return ApprovedDefinitionV1{}, err
	}
	return normalized, nil
}

// Validate verifies the V1 reader semantics without enabling a writer for a
// future execution mode.
func (d ApprovedDefinitionV1) Validate() error {
	_, err := normalizeApprovedDefinitionV1(d)
	return err
}

// ValidateApprovedDefinitionV1ForWrite applies the current writer gate after
// the frozen V1 rules. Stores accepting a hand-built value must call this
// before persistence; Decode and ordinary Encode deliberately do not call a
// registry whose future evolution could reinterpret retained V1 bytes.
func ValidateApprovedDefinitionV1ForWrite(definition ApprovedDefinitionV1) error {
	normalized, err := normalizeApprovedDefinitionV1(definition)
	if err != nil {
		return err
	}
	return validateApprovedDefinitionV1CurrentWriter(normalized)
}

// MarshalJSON validates and canonically orders the frozen V1 wire. It does
// not consult the current capability registry.
func (d ApprovedDefinitionV1) MarshalJSON() ([]byte, error) {
	normalized, err := normalizeApprovedDefinitionV1(d)
	if err != nil {
		return nil, err
	}
	return marshalBounded(approvedDefinitionV1Wire(normalized), maxDefinitionBytes,
		"approved definition")
}

// UnmarshalJSON uses the frozen V1 exact reader. It rejects duplicate,
// unknown, missing, case-folded, and null fields.
func (d *ApprovedDefinitionV1) UnmarshalJSON(payload []byte) error {
	if d == nil || len(payload) == 0 || len(payload) > maxDefinitionBytes {
		return invalidState("approved definition json size is invalid")
	}
	var wire approvedDefinitionV1Wire
	if err := strictjson.DecodeExact(payload, &wire); err != nil {
		return invalidState("approved definition json is invalid")
	}
	normalized, err := normalizeApprovedDefinitionV1(ApprovedDefinitionV1(wire))
	if err != nil {
		return err
	}
	*d = normalized
	return nil
}

// EncodeApprovedDefinitionV1 returns canonical bytes using only the frozen V1
// reader rules. A current write path must additionally call
// ValidateApprovedDefinitionV1ForWrite.
func EncodeApprovedDefinitionV1(definition ApprovedDefinitionV1) ([]byte, error) {
	return json.Marshal(definition)
}

// DecodeApprovedDefinitionV1 strictly decodes the retained V1 wire.
func DecodeApprovedDefinitionV1(payload []byte) (ApprovedDefinitionV1, error) {
	if len(payload) == 0 || len(payload) > maxDefinitionBytes {
		return ApprovedDefinitionV1{}, invalidState("approved definition json size is invalid")
	}
	var definition ApprovedDefinitionV1
	if err := json.Unmarshal(payload, &definition); err != nil {
		return ApprovedDefinitionV1{}, invalidState("approved definition json is invalid")
	}
	return definition, nil
}

// DigestApprovedDefinitionV1 returns the lowercase SHA-256 digest of the
// canonical frozen V1 bytes.
func DigestApprovedDefinitionV1(definition ApprovedDefinitionV1) (string, error) {
	canonical, err := EncodeApprovedDefinitionV1(definition)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}

func normalizeApprovedDefinitionV1(
	definition ApprovedDefinitionV1,
) (ApprovedDefinitionV1, error) {
	if definition.SchemaVersion != ApprovedDefinitionSchemaVersionV1 {
		return ApprovedDefinitionV1{}, invalidState("approved definition schema version is unsupported")
	}
	if definition.TenantID <= 0 || definition.UserID <= 0 ||
		!validIdentifier(definition.TaskID, maxTaskIDBytes) {
		return ApprovedDefinitionV1{}, invalidState("approved definition identity is invalid")
	}
	if !validMultilineText(definition.Intent, maxIntentBytes, false) ||
		!validMultilineText(definition.NLDescription, maxDescriptionBytes, false) ||
		!validMultilineText(definition.PlaybookContent, maxPlaybookBytes, true) {
		return ApprovedDefinitionV1{}, invalidState("approved definition text is invalid")
	}
	if definition.Strictness == "" {
		definition.Strictness = types.PushStrictness("loose")
	}
	if !validStrictnessV1(definition.Strictness) {
		return ApprovedDefinitionV1{}, invalidState("approved definition strictness is invalid")
	}
	if !validExecutionModeV1(definition.ExecutionMode) {
		return ApprovedDefinitionV1{}, invalidState("approved definition execution mode is invalid")
	}
	if definition.SourceScope == SourceScopeLegacySubscriptions &&
		definition.ExecutionMode != types.ExecutionModeCompiled {
		return ApprovedDefinitionV1{}, invalidState("legacy source scope requires compiled execution")
	}
	if definition.DeliveryPolicy != DeliveryPolicyOwnerFeishu ||
		definition.BudgetPolicy != BudgetPolicyInheritTenantQuota {
		return ApprovedDefinitionV1{}, invalidState("approved definition policy is unsupported")
	}

	var err error
	definition.SpecJSON, err = canonicalJSONObject(definition.SpecJSON, "spec")
	if err != nil {
		return ApprovedDefinitionV1{}, err
	}
	definition.ScopeJSON, err = canonicalJSONObject(definition.ScopeJSON, "scope")
	if err != nil {
		return ApprovedDefinitionV1{}, err
	}

	plan, canonicalPlan, err := normalizeFetchPlanV1(
		definition.FetchPlan,
		definition.SourceScope,
	)
	if err != nil {
		return ApprovedDefinitionV1{}, err
	}
	definition.FetchPlan = canonicalPlan
	definition.Sources, err = normalizeApprovedSourcesV1(definition.Sources)
	if err != nil {
		return ApprovedDefinitionV1{}, err
	}
	if err := verifyPlanSourceIdentityV1(plan, definition.Sources); err != nil {
		return ApprovedDefinitionV1{}, err
	}
	if _, err := marshalBounded(approvedDefinitionV1Wire(definition), maxDefinitionBytes,
		"approved definition"); err != nil {
		return ApprovedDefinitionV1{}, err
	}
	return definition, nil
}

func normalizeFetchPlanV1(
	raw json.RawMessage,
	sourceScope SourceScope,
) (FetchPlanV1, json.RawMessage, error) {
	switch sourceScope {
	case SourceScopeLegacySubscriptions:
		var empty map[string]json.RawMessage
		if err := strictjson.DecodeExact(raw, &empty); err != nil || empty == nil || len(empty) != 0 {
			return FetchPlanV1{}, nil, invalidState("legacy fetch plan must be an empty object")
		}
		return FetchPlanV1{Sources: []PlanSourceV1{}}, json.RawMessage("{}"), nil
	case SourceScopeApprovedPlan:
		// Continue below.
	default:
		return FetchPlanV1{}, nil, invalidState("approved definition source scope is invalid")
	}

	var plan FetchPlanV1
	if err := strictjson.DecodeExact(raw, &plan); err != nil || plan.Sources == nil ||
		len(plan.Sources) == 0 || len(plan.Sources) > maxSourceCount {
		return FetchPlanV1{}, nil, invalidState("approved fetch plan source count is invalid")
	}
	plan.Sources = slices.Clone(plan.Sources)
	seenURLs := make(map[string]struct{}, len(plan.Sources))
	for i := range plan.Sources {
		source, err := normalizePlanSourceV1(plan.Sources[i])
		if err != nil {
			return FetchPlanV1{}, nil, err
		}
		if _, duplicate := seenURLs[source.URL]; duplicate {
			return FetchPlanV1{}, nil, invalidState("approved fetch plan url is duplicated")
		}
		seenURLs[source.URL] = struct{}{}
		plan.Sources[i] = source
	}
	canonical, err := marshalBounded(plan, maxJSONObjectBytes, "approved fetch plan")
	if err != nil {
		return FetchPlanV1{}, nil, err
	}
	return plan, canonical, nil
}

func normalizePlanSourceV1(source PlanSourceV1) (PlanSourceV1, error) {
	if !validReadCapability(source.Platform, source.Capability) ||
		!validIdentifier(source.URL, maxSourceURLBytes) ||
		!validOptionalSingleLineText(source.Title, maxSourceTextBytes) {
		return PlanSourceV1{}, invalidState("approved fetch plan source is invalid")
	}
	config, err := canonicalJSONObject(source.Config, "source config")
	if err != nil {
		return PlanSourceV1{}, err
	}
	source.Config = config
	return source, nil
}

func validateApprovedDefinitionV1CurrentWriter(definition ApprovedDefinitionV1) error {
	if definition.ExecutionMode != types.ExecutionModeCompiled {
		return invalidState("approved definition execution mode is not writable")
	}
	if definition.SourceScope == SourceScopeLegacySubscriptions {
		return invalidState("legacy subscription scope is reader-only")
	}
	var plan FetchPlanV1
	if err := strictjson.DecodeExact(definition.FetchPlan, &plan); err != nil {
		return invalidState("approved fetch plan is invalid")
	}
	for _, source := range plan.Sources {
		if !validCurrentMaterializedSourceV1(source.Platform, source.Capability,
			source.Title, source.URL, source.Config) {
			return invalidState("approved plan source is not a registered materialized capability")
		}
	}
	for _, source := range definition.Sources {
		if !validCurrentMaterializedSourceV1(source.Platform, source.Capability,
			source.Title, source.URL, source.Config) {
			return invalidState("approved source is not a registered materialized capability")
		}
	}
	return nil
}

func validCurrentMaterializedSourceV1(
	platform types.Platform,
	capability types.Capability,
	title string,
	url string,
	config json.RawMessage,
) bool {
	return fetchspec.ValidateMaterialized(&types.FetchTarget{
		Platform: platform, Capability: capability, Title: title, URL: url, Config: config,
	}) == ""
}

func normalizeApprovedSourcesV1(sources []ApprovedSourceV1) ([]ApprovedSourceV1, error) {
	if sources == nil || len(sources) > maxSourceCount {
		return nil, invalidState("approved source count is invalid")
	}
	sources = slices.Clone(sources)
	seenIDs := make(map[int64]struct{}, len(sources))
	seenURLs := make(map[string]struct{}, len(sources))
	for i := range sources {
		source := sources[i]
		if source.SourceID <= 0 {
			return nil, invalidState("approved source id is invalid")
		}
		planSource, err := normalizePlanSourceV1(PlanSourceV1{
			Platform: source.Platform, Capability: source.Capability,
			Title: source.Title, URL: source.URL, Config: source.Config,
		})
		if err != nil {
			return nil, err
		}
		if _, duplicate := seenIDs[source.SourceID]; duplicate {
			return nil, invalidState("approved source id is duplicated")
		}
		if _, duplicate := seenURLs[source.URL]; duplicate {
			return nil, invalidState("approved source url is duplicated")
		}
		seenIDs[source.SourceID] = struct{}{}
		seenURLs[source.URL] = struct{}{}
		source.Platform = planSource.Platform
		source.Capability = planSource.Capability
		source.Title = planSource.Title
		source.URL = planSource.URL
		source.Config = planSource.Config
		sources[i] = source
	}
	sort.Slice(sources, func(i, j int) bool {
		if sources[i].URL == sources[j].URL {
			return sources[i].SourceID < sources[j].SourceID
		}
		return sources[i].URL < sources[j].URL
	})
	return sources, nil
}

func verifyPlanSourceIdentityV1(plan FetchPlanV1, sources []ApprovedSourceV1) error {
	if len(plan.Sources) != len(sources) {
		return invalidState("approved fetch plan and sources differ")
	}
	byURL := make(map[string]ApprovedSourceV1, len(sources))
	for _, source := range sources {
		byURL[source.URL] = source
	}
	for _, planned := range plan.Sources {
		materialized, ok := byURL[planned.URL]
		if !ok || materialized.Platform != planned.Platform ||
			materialized.Capability != planned.Capability ||
			materialized.Title != planned.Title ||
			!bytes.Equal(materialized.Config, planned.Config) {
			return invalidState("approved fetch plan and sources differ")
		}
	}
	return nil
}
