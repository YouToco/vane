package store

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/YouToco/vane/internal/strictjson"
	"github.com/YouToco/vane/observation"
	"github.com/YouToco/vane/types"
)

type taskRunBudget = types.PlannerBudget

type taskRunPolicyPayloads struct {
	CapabilityCatalog json.RawMessage `json:"capability_catalog"`
	ToolPolicy        json.RawMessage `json:"tool_policy"`
	PromptPolicy      json.RawMessage `json:"prompt_policy"`
	ModelPolicy       json.RawMessage `json:"model_policy"`
	QuotaPolicy       json.RawMessage `json:"quota_policy"`
}

type taskRunPolicyDigestSet struct {
	CapabilityCatalog string
	ToolPolicy        string
	PromptPolicy      string
	ModelPolicy       string
	QuotaPolicy       string
}

type taskRunPolicyDigestEnvelope struct {
	Version string          `json:"version"`
	Kind    string          `json:"kind"`
	Payload json.RawMessage `json:"payload"`
}

type taskRunSourceIdentity struct {
	SourceID   int64           `json:"source_id"`
	Platform   string          `json:"platform"`
	Capability string          `json:"capability"`
	Title      string          `json:"title"`
	URL        string          `json:"url"`
	Config     json.RawMessage `json:"config"`
}

type taskRunDefinitionPayload struct {
	TaskID          string                  `json:"task_id"`
	TenantID        int64                   `json:"tenant_id"`
	UserID          int64                   `json:"user_id"`
	NLDescription   string                  `json:"nl_description"`
	SpecJSON        json.RawMessage         `json:"spec_json"`
	ScopeJSON       json.RawMessage         `json:"scope_json"`
	PlaybookContent string                  `json:"playbook_content"`
	Strictness      types.PushStrictness    `json:"strictness"`
	SourceScope     string                  `json:"source_scope"`
	FetchPlan       json.RawMessage         `json:"fetch_plan"`
	Sources         []taskRunSourceIdentity `json:"sources"`
}

type taskRunSnapshotPayload struct {
	SchemaVersion          string                   `json:"schema_version"`
	TenantID               int64                    `json:"tenant_id"`
	UserID                 int64                    `json:"user_id"`
	TaskID                 string                   `json:"task_id"`
	RunKind                types.RunSnapshotKind    `json:"run_kind"`
	Mode                   types.ExecutionMode      `json:"mode"`
	AdaptiveVersion        int64                    `json:"adaptive_version"`
	ObservationRollout     observation.RolloutMode  `json:"observation_rollout,omitempty"`
	Policies               taskRunPolicyPayloads    `json:"policies"`
	Budget                 taskRunBudget            `json:"budget"`
	Definition             taskRunDefinitionPayload `json:"definition"`
	ReferenceSchemaVersion string                   `json:"reference_schema_version"`
}

type taskRunLegacyDefinitionEnvelope struct {
	Version    string                   `json:"version"`
	Definition taskRunDefinitionPayload `json:"definition"`
}

type taskRunPlanDigestEnvelope struct {
	Version     string                  `json:"version"`
	SourceScope string                  `json:"source_scope"`
	FetchPlan   json.RawMessage         `json:"fetch_plan"`
	Sources     []taskRunSourceIdentity `json:"sources"`
}

// The v1 reader owns a complete copy of the persisted wire schema and every
// validation limit that gives that schema meaning. Current task-definition,
// planner, and enum helpers must be free to evolve without reinterpreting
// already persisted BYTEA rows.
const (
	taskRunSnapshotPayloadSchemaV1    = "vane.task-run-snapshot-payload/v1"
	taskRunReferenceSchemaV1          = "vane.run-snapshot-ref/v1"
	taskRunApprovedDefinitionDigestV1 = "vane.paused-compiled-task-definition/v1"
	taskRunLegacyDefinitionDigestV1   = "vane.task-run-legacy-definition/v1"
	taskRunPlanDigestV1               = "vane.task-run-execution-plan/v1"
	taskRunPolicyDigestV1             = "vane.runtime-policy-digest/v1"
	taskRunApprovedSourceScopeV1      = "approved_plan"
	taskRunLegacySourceScopeV1        = "legacy_subscriptions"

	maxTaskRunPayloadBytesV1     = 2 << 20
	maxTaskRunJSONBytesV1        = 256 << 10
	maxTaskRunPlaybookBytesV1    = 256 << 10
	maxTaskRunDescriptionBytesV1 = 16 << 10
	maxTaskRunSourcesV1          = 64
	maxTaskRunSourceURLBytesV1   = 4096
	maxTaskRunSourceTextBytesV1  = 4096
	maxTaskRunTaskIDBytesV1      = 255
)

type taskRunBudgetV1 struct {
	MaxPlannerRounds int   `json:"max_planner_rounds"`
	MaxToolCalls     int   `json:"max_tool_calls"`
	MaxTokens        int   `json:"max_tokens"`
	MaxCostMicroUSD  int64 `json:"max_cost_micro_usd"`
	DurationMs       int64 `json:"duration_ms"`
}

type taskRunPolicyPayloadsV1 struct {
	CapabilityCatalog json.RawMessage `json:"capability_catalog"`
	ToolPolicy        json.RawMessage `json:"tool_policy"`
	PromptPolicy      json.RawMessage `json:"prompt_policy"`
	ModelPolicy       json.RawMessage `json:"model_policy"`
	QuotaPolicy       json.RawMessage `json:"quota_policy"`
}

type taskRunPolicyDigestEnvelopeV1 struct {
	Version string          `json:"version"`
	Kind    string          `json:"kind"`
	Payload json.RawMessage `json:"payload"`
}

type taskRunSourceIdentityV1 struct {
	SourceID   int64           `json:"source_id"`
	Platform   string          `json:"platform"`
	Capability string          `json:"capability"`
	Title      string          `json:"title"`
	URL        string          `json:"url"`
	Config     json.RawMessage `json:"config"`
}

type taskRunPlanSourceV1 struct {
	Platform   string          `json:"platform"`
	Capability string          `json:"capability"`
	Title      string          `json:"title,omitempty"`
	URL        string          `json:"url"`
	Config     json.RawMessage `json:"config,omitempty"`
}

type taskRunFetchPlanV1 struct {
	Sources []taskRunPlanSourceV1 `json:"sources"`
}

type taskRunDefinitionPayloadV1 struct {
	TaskID          string                    `json:"task_id"`
	TenantID        int64                     `json:"tenant_id"`
	UserID          int64                     `json:"user_id"`
	NLDescription   string                    `json:"nl_description"`
	SpecJSON        json.RawMessage           `json:"spec_json"`
	ScopeJSON       json.RawMessage           `json:"scope_json"`
	PlaybookContent string                    `json:"playbook_content"`
	Strictness      string                    `json:"strictness"`
	SourceScope     string                    `json:"source_scope"`
	FetchPlan       json.RawMessage           `json:"fetch_plan"`
	Sources         []taskRunSourceIdentityV1 `json:"sources"`
}

type taskRunSnapshotPayloadV1 struct {
	SchemaVersion          string                      `json:"schema_version"`
	TenantID               int64                       `json:"tenant_id"`
	UserID                 int64                       `json:"user_id"`
	TaskID                 string                      `json:"task_id"`
	RunKind                string                      `json:"run_kind"`
	Mode                   string                      `json:"mode"`
	AdaptiveVersion        int64                       `json:"adaptive_version"`
	ObservationRollout     string                      `json:"observation_rollout,omitempty"`
	Policies               *taskRunPolicyPayloadsV1    `json:"policies"`
	Budget                 *taskRunBudgetV1            `json:"budget"`
	Definition             *taskRunDefinitionPayloadV1 `json:"definition"`
	ReferenceSchemaVersion string                      `json:"reference_schema_version"`
}

type taskRunApprovedDefinitionDigestEnvelopeV1 struct {
	Version         string          `json:"version"`
	TaskID          string          `json:"task_id"`
	TenantID        int64           `json:"tenant_id"`
	UserID          int64           `json:"user_id"`
	NLDescription   string          `json:"nl_description"`
	SpecJSON        json.RawMessage `json:"spec_json"`
	ScopeJSON       json.RawMessage `json:"scope_json"`
	PlaybookContent string          `json:"playbook_content"`
	FetchPlan       json.RawMessage `json:"fetch_plan"`
	Strictness      string          `json:"strictness"`
}

type taskRunLegacyDefinitionEnvelopeV1 struct {
	Version    string                     `json:"version"`
	Definition taskRunDefinitionPayloadV1 `json:"definition"`
}

type taskRunPlanDigestEnvelopeV1 struct {
	Version     string                    `json:"version"`
	SourceScope string                    `json:"source_scope"`
	FetchPlan   json.RawMessage           `json:"fetch_plan"`
	Sources     []taskRunSourceIdentityV1 `json:"sources"`
}

type taskRunSnapshotPayloadRead struct {
	Payload          *taskRunSnapshotPayloadV1
	Canonical        []byte
	DefinitionDigest string
	PlanDigest       string
	PolicyDigests    taskRunPolicyDigestSet
}

func canonicalizeTaskRunPayload(
	payload *taskRunSnapshotPayload,
) ([]byte, string, string, error) {
	decoded, err := canonicalizeTaskRunPayloadForWrite(payload)
	if err != nil {
		return nil, "", "", err
	}
	return decoded.Canonical, decoded.DefinitionDigest, decoded.PlanDigest, nil
}

func canonicalizeTaskRunPayloadForWrite(
	payload *taskRunSnapshotPayload,
) (taskRunSnapshotPayloadRead, error) {
	if payload == nil {
		return taskRunSnapshotPayloadRead{}, errors.New("payload is missing")
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return taskRunSnapshotPayloadRead{}, errors.New("payload cannot be encoded")
	}
	return readTaskRunSnapshotPayload(raw)
}

// readTaskRunSnapshotPayload is the only raw persisted-payload entry point.
// It validates representation-level integrity before inspecting the version,
// then dispatches to a reader whose types and rules are frozen for that wire.
func readTaskRunSnapshotPayload(raw []byte) (taskRunSnapshotPayloadRead, error) {
	if len(raw) == 0 || len(raw) > maxTaskRunPayloadBytesV1 ||
		strictjson.Validate(raw) != nil {
		return taskRunSnapshotPayloadRead{}, errors.New("task run payload is invalid")
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(raw, &envelope); err != nil || envelope == nil {
		return taskRunSnapshotPayloadRead{}, errors.New("task run payload is not an object")
	}
	var schemaVersion string
	if err := strictjson.Decode(envelope["schema_version"], &schemaVersion); err != nil {
		return taskRunSnapshotPayloadRead{}, errors.New("task run payload version is invalid")
	}
	switch schemaVersion {
	case taskRunSnapshotPayloadSchemaV1:
		var payload *taskRunSnapshotPayloadV1
		if err := strictjson.DecodeExact(raw, &payload); err != nil || payload == nil {
			return taskRunSnapshotPayloadRead{}, errors.New("task run v1 payload is invalid")
		}
		return canonicalizeTaskRunPayloadV1(payload)
	default:
		return taskRunSnapshotPayloadRead{}, errors.New("task run payload version is unsupported")
	}
}

func canonicalizeTaskRunPayloadV1(
	payload *taskRunSnapshotPayloadV1,
) (taskRunSnapshotPayloadRead, error) {
	if payload == nil || payload.SchemaVersion != taskRunSnapshotPayloadSchemaV1 ||
		payload.ReferenceSchemaVersion != taskRunReferenceSchemaV1 ||
		payload.RunKind != "scheduled" || payload.Mode != "compiled" ||
		payload.AdaptiveVersion != 0 || payload.TenantID <= 0 || payload.UserID <= 0 ||
		!validTaskRunObservationRolloutV1(payload.ObservationRollout) ||
		!validTaskRunTaskIDV1(payload.TaskID) || payload.Policies == nil ||
		payload.Budget == nil || *payload.Budget != (taskRunBudgetV1{}) ||
		payload.Definition == nil {
		return taskRunSnapshotPayloadRead{}, errors.New("invalid v1 payload envelope")
	}
	policyDigests, err := canonicalizeTaskRunPolicyPayloadsV1(payload.Policies)
	if err != nil {
		return taskRunSnapshotPayloadRead{}, err
	}
	definition := payload.Definition
	if definition.TaskID != payload.TaskID || definition.TenantID != payload.TenantID ||
		definition.UserID != payload.UserID || !validTaskRunStrictnessV1(definition.Strictness) {
		return taskRunSnapshotPayloadRead{}, errors.New("invalid v1 definition identity")
	}
	definition.SpecJSON, err = canonicalTaskRunJSONObjectV1(definition.SpecJSON)
	if err != nil {
		return taskRunSnapshotPayloadRead{}, err
	}
	definition.ScopeJSON, err = canonicalTaskRunJSONObjectV1(definition.ScopeJSON)
	if err != nil {
		return taskRunSnapshotPayloadRead{}, err
	}

	seenSourceIDs := make(map[int64]struct{}, len(definition.Sources))
	seenSourceURLs := make(map[string]struct{}, len(definition.Sources))
	for i := range definition.Sources {
		source := &definition.Sources[i]
		if !validTaskRunSourceIdentityV1(*source) {
			return taskRunSnapshotPayloadRead{}, errors.New("invalid v1 source identity")
		}
		if _, duplicate := seenSourceIDs[source.SourceID]; duplicate {
			return taskRunSnapshotPayloadRead{}, errors.New("v1 source identity is duplicated")
		}
		if _, duplicate := seenSourceURLs[source.URL]; duplicate {
			return taskRunSnapshotPayloadRead{}, errors.New("v1 source url is duplicated")
		}
		seenSourceIDs[source.SourceID] = struct{}{}
		seenSourceURLs[source.URL] = struct{}{}
		source.Config, err = canonicalTaskRunJSONObjectV1(source.Config)
		if err != nil {
			return taskRunSnapshotPayloadRead{}, err
		}
		if i > 0 && definition.Sources[i-1].URL >= source.URL {
			return taskRunSnapshotPayloadRead{}, errors.New("v1 source identities are not canonical")
		}
	}

	var definitionDigest string
	switch definition.SourceScope {
	case taskRunApprovedSourceScopeV1:
		plan, validateErr := validateTaskRunApprovedDefinitionV1(definition)
		if validateErr != nil {
			return taskRunSnapshotPayloadRead{}, errors.New("approved v1 definition is invalid")
		}
		definition.FetchPlan, err = canonicalTaskRunCompiledPlanV1(plan)
		if err != nil || len(definition.Sources) != len(plan.Sources) {
			return taskRunSnapshotPayloadRead{}, errors.New("approved v1 plan is not canonical")
		}
		planURLs := make(map[string]struct{}, len(plan.Sources))
		for _, source := range plan.Sources {
			planURLs[source.URL] = struct{}{}
		}
		for _, source := range definition.Sources {
			if _, ok := planURLs[source.URL]; !ok {
				return taskRunSnapshotPayloadRead{}, errors.New("approved v1 plan links differ")
			}
		}
		if len(planURLs) != len(seenSourceURLs) {
			return taskRunSnapshotPayloadRead{}, errors.New("approved v1 plan links differ")
		}
		definitionDigest, err = digestTaskRunApprovedDefinitionV1(*definition)
	case taskRunLegacySourceScopeV1:
		var planObject map[string]json.RawMessage
		if strictjson.Decode(definition.FetchPlan, &planObject) != nil || planObject == nil ||
			len(planObject) != 0 {
			return taskRunSnapshotPayloadRead{}, errors.New("legacy v1 plan is not empty")
		}
		var canonicalEmpty json.RawMessage
		canonicalEmpty, err = canonicalTaskRunJSONObjectV1(definition.FetchPlan)
		if err != nil || !bytes.Equal(canonicalEmpty, []byte("{}")) {
			return taskRunSnapshotPayloadRead{}, errors.New("legacy v1 plan is not canonical")
		}
		definition.FetchPlan = canonicalEmpty
		definitionDigest, err = digestTaskRunLegacyDefinitionV1(*definition)
	default:
		return taskRunSnapshotPayloadRead{}, errors.New("v1 source scope is invalid")
	}
	if err != nil {
		return taskRunSnapshotPayloadRead{}, err
	}
	planDigest, err := digestTaskRunPlanV1(*definition)
	if err != nil {
		return taskRunSnapshotPayloadRead{}, err
	}
	canonical, err := json.Marshal(payload)
	if err != nil || len(canonical) > maxTaskRunPayloadBytesV1 {
		return taskRunSnapshotPayloadRead{}, errors.New("v1 payload cannot be canonicalized")
	}
	return taskRunSnapshotPayloadRead{
		Payload: payload, Canonical: canonical, DefinitionDigest: definitionDigest,
		PlanDigest: planDigest, PolicyDigests: policyDigests,
	}, nil
}

func validTaskRunObservationRolloutV1(value string) bool {
	switch observation.RolloutMode(value) {
	case "", observation.RolloutOff,
		observation.RolloutShadow, observation.RolloutAuthority:
		return true
	default:
		return false
	}
}

func validateTaskRunApprovedDefinitionV1(
	definition *taskRunDefinitionPayloadV1,
) (*taskRunFetchPlanV1, error) {
	if definition == nil || strings.TrimSpace(definition.TaskID) == "" ||
		strings.TrimSpace(definition.TaskID) != definition.TaskID ||
		len(definition.TaskID) > maxTaskRunTaskIDBytesV1 ||
		!utf8.ValidString(definition.TaskID) || definition.TenantID <= 0 ||
		definition.UserID <= 0 ||
		len(definition.NLDescription) > maxTaskRunDescriptionBytesV1 ||
		!utf8.ValidString(definition.NLDescription) ||
		len(definition.PlaybookContent) > maxTaskRunPlaybookBytesV1 ||
		!utf8.ValidString(definition.PlaybookContent) ||
		!validTaskRunStrictnessV1(definition.Strictness) {
		return nil, errors.New("approved v1 definition fields are invalid")
	}
	if _, err := canonicalTaskRunJSONObjectV1(definition.SpecJSON); err != nil {
		return nil, err
	}
	if _, err := canonicalTaskRunJSONObjectV1(definition.ScopeJSON); err != nil {
		return nil, err
	}
	raw := bytes.TrimSpace(definition.FetchPlan)
	if len(raw) == 0 || len(raw) > maxTaskRunJSONBytesV1 || bytes.Equal(raw, []byte("null")) {
		return nil, errors.New("approved v1 fetch plan is missing")
	}
	var plan *taskRunFetchPlanV1
	if err := strictjson.DecodeExact(raw, &plan); err != nil || plan == nil ||
		len(plan.Sources) == 0 || len(plan.Sources) > maxTaskRunSourcesV1 {
		return nil, errors.New("approved v1 fetch plan is invalid")
	}
	seenURLs := make(map[string]struct{}, len(plan.Sources))
	for i := range plan.Sources {
		source := &plan.Sources[i]
		if len(source.Platform) > maxTaskRunSourceTextBytesV1 ||
			len(source.Capability) > maxTaskRunSourceTextBytesV1 ||
			len(source.Title) > maxTaskRunSourceTextBytesV1 ||
			!utf8.ValidString(source.Platform) || !utf8.ValidString(source.Capability) ||
			!utf8.ValidString(source.Title) || !utf8.ValidString(source.URL) ||
			strings.TrimSpace(source.Platform) == "" ||
			strings.TrimSpace(source.Platform) != source.Platform ||
			strings.TrimSpace(source.Capability) == "" ||
			strings.TrimSpace(source.Capability) != source.Capability ||
			strings.TrimSpace(source.URL) == "" || strings.TrimSpace(source.URL) != source.URL ||
			len(source.URL) > maxTaskRunSourceURLBytesV1 {
			return nil, errors.New("approved v1 fetch plan source is invalid")
		}
		if _, duplicate := seenURLs[source.URL]; duplicate {
			return nil, errors.New("approved v1 fetch plan url is duplicated")
		}
		seenURLs[source.URL] = struct{}{}
		if len(bytes.TrimSpace(source.Config)) == 0 {
			source.Config = json.RawMessage("{}")
			continue
		}
		if _, err := canonicalTaskRunJSONObjectV1(source.Config); err != nil {
			return nil, err
		}
	}
	return plan, nil
}

func canonicalTaskRunCompiledPlanV1(plan *taskRunFetchPlanV1) (json.RawMessage, error) {
	if plan == nil {
		return nil, errors.New("v1 compiled plan is missing")
	}
	for i := range plan.Sources {
		canonical, err := canonicalTaskRunJSONObjectV1(plan.Sources[i].Config)
		if err != nil {
			return nil, err
		}
		plan.Sources[i].Config = canonical
	}
	canonical, err := json.Marshal(plan)
	if err != nil {
		return nil, errors.New("v1 compiled plan cannot be canonicalized")
	}
	return canonical, nil
}

func canonicalizeTaskRunPolicyPayloadsV1(
	policies *taskRunPolicyPayloadsV1,
) (taskRunPolicyDigestSet, error) {
	if policies == nil {
		return taskRunPolicyDigestSet{}, errors.New("v1 policy payloads are missing")
	}
	fields := []*json.RawMessage{
		&policies.CapabilityCatalog,
		&policies.ToolPolicy,
		&policies.PromptPolicy,
		&policies.ModelPolicy,
		&policies.QuotaPolicy,
	}
	for _, field := range fields {
		canonical, err := canonicalTaskRunJSONObjectV1(*field)
		if err != nil {
			return taskRunPolicyDigestSet{}, err
		}
		*field = canonical
	}
	return digestTaskRunPoliciesV1(*policies)
}

func digestTaskRunPoliciesV1(
	policies taskRunPolicyPayloadsV1,
) (taskRunPolicyDigestSet, error) {
	digest := func(kind string, payload json.RawMessage) (string, error) {
		return digestTaskRunValueV1(taskRunPolicyDigestEnvelopeV1{
			Version: taskRunPolicyDigestV1, Kind: kind, Payload: payload,
		})
	}
	capabilityCatalog, err := digest("capability_catalog", policies.CapabilityCatalog)
	if err != nil {
		return taskRunPolicyDigestSet{}, err
	}
	toolPolicy, err := digest("tool_policy", policies.ToolPolicy)
	if err != nil {
		return taskRunPolicyDigestSet{}, err
	}
	promptPolicy, err := digest("prompt_policy", policies.PromptPolicy)
	if err != nil {
		return taskRunPolicyDigestSet{}, err
	}
	modelPolicy, err := digest("model_policy", policies.ModelPolicy)
	if err != nil {
		return taskRunPolicyDigestSet{}, err
	}
	quotaPolicy, err := digest("quota_policy", policies.QuotaPolicy)
	if err != nil {
		return taskRunPolicyDigestSet{}, err
	}
	return taskRunPolicyDigestSet{
		CapabilityCatalog: capabilityCatalog,
		ToolPolicy:        toolPolicy,
		PromptPolicy:      promptPolicy,
		ModelPolicy:       modelPolicy,
		QuotaPolicy:       quotaPolicy,
	}, nil
}

func canonicalTaskRunJSONObjectV1(raw json.RawMessage) (json.RawMessage, error) {
	if len(raw) == 0 || len(raw) > maxTaskRunJSONBytesV1 {
		return nil, errors.New("v1 json object size is invalid")
	}
	var object map[string]any
	if err := strictjson.Decode(raw, &object); err != nil || object == nil {
		return nil, errors.New("v1 json value is not a strict object")
	}
	canonical, err := json.Marshal(object)
	if err != nil {
		return nil, errors.New("v1 json object cannot be canonicalized")
	}
	return canonical, nil
}

func digestTaskRunApprovedDefinitionV1(
	definition taskRunDefinitionPayloadV1,
) (string, error) {
	return digestTaskRunValueV1(taskRunApprovedDefinitionDigestEnvelopeV1{
		Version: taskRunApprovedDefinitionDigestV1,
		TaskID:  definition.TaskID, TenantID: definition.TenantID, UserID: definition.UserID,
		NLDescription: definition.NLDescription, SpecJSON: definition.SpecJSON,
		ScopeJSON: definition.ScopeJSON, PlaybookContent: definition.PlaybookContent,
		FetchPlan: definition.FetchPlan, Strictness: definition.Strictness,
	})
}

func digestTaskRunLegacyDefinitionV1(
	definition taskRunDefinitionPayloadV1,
) (string, error) {
	return digestTaskRunValueV1(taskRunLegacyDefinitionEnvelopeV1{
		Version: taskRunLegacyDefinitionDigestV1, Definition: definition,
	})
}

func digestTaskRunPlanV1(definition taskRunDefinitionPayloadV1) (string, error) {
	return digestTaskRunValueV1(taskRunPlanDigestEnvelopeV1{
		Version: taskRunPlanDigestV1, SourceScope: definition.SourceScope,
		FetchPlan: definition.FetchPlan, Sources: definition.Sources,
	})
}

func digestTaskRunValueV1(value any) (string, error) {
	canonical, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return sha256Hex(canonical), nil
}

func validTaskRunTaskIDV1(value string) bool {
	return value != "" && strings.TrimSpace(value) == value &&
		len(value) <= maxTaskRunTaskIDBytesV1 && utf8.ValidString(value) &&
		!containsUnsafeTaskRunRuneV1(value)
}

func validTaskRunStrictnessV1(value string) bool {
	switch value {
	case "", "loose", "normal", "strict":
		return true
	default:
		return false
	}
}

func validTaskRunSourceIdentityV1(source taskRunSourceIdentityV1) bool {
	return source.SourceID > 0 &&
		validTaskRunSourceTextV1(source.Platform, maxTaskRunSourceTextBytesV1) &&
		validTaskRunSourceTextV1(source.Capability, maxTaskRunSourceTextBytesV1) &&
		validTaskRunSourceTextV1(source.URL, maxTaskRunSourceURLBytesV1) &&
		len(source.Title) <= maxTaskRunSourceTextBytesV1 && utf8.ValidString(source.Title)
}

func validTaskRunSourceTextV1(value string, maxBytes int) bool {
	return value != "" && strings.TrimSpace(value) == value && len(value) <= maxBytes &&
		utf8.ValidString(value) && !containsUnsafeTaskRunRuneV1(value)
}

func containsUnsafeTaskRunRuneV1(value string) bool {
	for _, r := range value {
		if unicode.IsControl(r) || unicode.In(r, unicode.Cf) {
			return true
		}
	}
	return false
}

func canonicalTaskRunCompiledPlan(plan *compiledFetchPlan) (json.RawMessage, error) {
	if plan == nil {
		return nil, errors.New("compiled plan is missing")
	}
	for i := range plan.Targets {
		canonical, err := canonicalTaskRunJSONObject(plan.Targets[i].Config)
		if err != nil {
			return nil, err
		}
		plan.Targets[i].Config = canonical
	}
	canonical, err := json.Marshal(plan)
	if err != nil {
		return nil, err
	}
	return canonical, nil
}

func canonicalTaskRunBudget(raw json.RawMessage) (taskRunBudget, []byte, error) {
	if len(raw) == 0 || len(raw) > maxTaskRunJSONBytes {
		return taskRunBudget{}, nil, taskRunValidationError("task run budget is invalid")
	}
	var budget *taskRunBudget
	if err := strictjson.Decode(raw, &budget); err != nil || budget == nil ||
		*budget != (taskRunBudget{}) {
		return taskRunBudget{}, nil, taskRunValidationError(
			"compiled task run budget must be an all-zero object")
	}
	canonical, err := json.Marshal(budget)
	if err != nil {
		return taskRunBudget{}, nil, taskRunValidationError("task run budget is invalid")
	}
	return *budget, canonical, nil
}

func canonicalTaskRunPolicies(
	p CreateOrGetTaskRunSnapshotParams,
) (taskRunPolicyPayloads, taskRunPolicyDigestSet, error) {
	policies := taskRunPolicyPayloads{
		CapabilityCatalog: p.CapabilityCatalogJSON,
		ToolPolicy:        p.ToolPolicyJSON,
		PromptPolicy:      p.PromptPolicyJSON,
		ModelPolicy:       p.ModelPolicyJSON,
		QuotaPolicy:       p.QuotaPolicyJSON,
	}
	digests, err := canonicalizeTaskRunPolicyPayloads(&policies)
	if err != nil {
		return taskRunPolicyPayloads{}, taskRunPolicyDigestSet{},
			taskRunValidationError("task run policy payloads are invalid")
	}
	return policies, digests, nil
}

func canonicalizeTaskRunPolicyPayloads(
	policies *taskRunPolicyPayloads,
) (taskRunPolicyDigestSet, error) {
	if policies == nil {
		return taskRunPolicyDigestSet{}, errors.New("policy payloads are missing")
	}
	fields := []*json.RawMessage{
		&policies.CapabilityCatalog,
		&policies.ToolPolicy,
		&policies.PromptPolicy,
		&policies.ModelPolicy,
		&policies.QuotaPolicy,
	}
	for _, field := range fields {
		canonical, err := canonicalTaskRunJSONObject(*field)
		if err != nil {
			return taskRunPolicyDigestSet{}, err
		}
		*field = canonical
	}
	return digestTaskRunPolicies(*policies)
}

func digestTaskRunPolicies(
	policies taskRunPolicyPayloads,
) (taskRunPolicyDigestSet, error) {
	digest := func(kind string, payload json.RawMessage) (string, error) {
		return digestTaskRunValue(taskRunPolicyDigestEnvelope{
			Version: taskRunPolicyDigestVersion,
			Kind:    kind,
			Payload: payload,
		})
	}
	capabilityCatalog, err := digest("capability_catalog", policies.CapabilityCatalog)
	if err != nil {
		return taskRunPolicyDigestSet{}, err
	}
	toolPolicy, err := digest("tool_policy", policies.ToolPolicy)
	if err != nil {
		return taskRunPolicyDigestSet{}, err
	}
	promptPolicy, err := digest("prompt_policy", policies.PromptPolicy)
	if err != nil {
		return taskRunPolicyDigestSet{}, err
	}
	modelPolicy, err := digest("model_policy", policies.ModelPolicy)
	if err != nil {
		return taskRunPolicyDigestSet{}, err
	}
	quotaPolicy, err := digest("quota_policy", policies.QuotaPolicy)
	if err != nil {
		return taskRunPolicyDigestSet{}, err
	}
	return taskRunPolicyDigestSet{
		CapabilityCatalog: capabilityCatalog,
		ToolPolicy:        toolPolicy,
		PromptPolicy:      promptPolicy,
		ModelPolicy:       modelPolicy,
		QuotaPolicy:       quotaPolicy,
	}, nil
}

func canonicalTaskRunJSONObject(raw json.RawMessage) (json.RawMessage, error) {
	if len(raw) == 0 || len(raw) > maxTaskRunJSONBytes {
		return nil, errors.New("json object size is invalid")
	}
	var object map[string]any
	if err := strictjson.Decode(raw, &object); err != nil || object == nil {
		return nil, errors.New("json value is not a strict object")
	}
	canonical, err := json.Marshal(object)
	if err != nil {
		return nil, errors.New("json object cannot be canonicalized")
	}
	return canonical, nil
}

func digestTaskRunLegacyDefinition(definition taskRunDefinitionPayload) (string, error) {
	return digestTaskRunValue(taskRunLegacyDefinitionEnvelope{
		Version: taskRunLegacyDefinitionVersion, Definition: definition,
	})
}

func digestTaskRunPlan(definition taskRunDefinitionPayload) (string, error) {
	return digestTaskRunValue(taskRunPlanDigestEnvelope{
		Version: taskRunPlanDigestVersion, SourceScope: definition.SourceScope,
		FetchPlan: definition.FetchPlan, Sources: definition.Sources,
	})
}

func digestTaskRunValue(value any) (string, error) {
	canonical, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return sha256Hex(canonical), nil
}

func sha256Hex(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func constantTimeDigestEqual(left, right string) bool {
	return len(left) == sha256.Size*2 && len(right) == sha256.Size*2 &&
		subtle.ConstantTimeCompare([]byte(left), []byte(right)) == 1
}
