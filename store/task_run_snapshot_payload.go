package store

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"

	"github.com/YouToco/vane/internal/strictjson"
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

func canonicalizeTaskRunPayload(
	payload *taskRunSnapshotPayload,
) ([]byte, string, string, error) {
	if payload == nil || payload.SchemaVersion != taskRunSnapshotPayloadVersion ||
		payload.ReferenceSchemaVersion != types.RunSnapshotSchemaVersion ||
		payload.RunKind != types.RunSnapshotKindScheduled ||
		payload.Mode != types.ExecutionModeCompiled || payload.AdaptiveVersion != 0 ||
		payload.TenantID <= 0 || payload.UserID <= 0 ||
		!validTaskRunTaskID(payload.TaskID) ||
		payload.Budget != (taskRunBudget{}) {
		return nil, "", "", errors.New("invalid payload envelope")
	}
	if _, err := canonicalizeTaskRunPolicyPayloads(&payload.Policies); err != nil {
		return nil, "", "", err
	}
	definition := &payload.Definition
	if definition.TaskID != payload.TaskID || definition.TenantID != payload.TenantID ||
		definition.UserID != payload.UserID ||
		(definition.Strictness != "" && !definition.Strictness.Valid()) {
		return nil, "", "", errors.New("invalid definition identity")
	}
	var err error
	definition.SpecJSON, err = canonicalTaskRunJSONObject(definition.SpecJSON)
	if err != nil {
		return nil, "", "", err
	}
	definition.ScopeJSON, err = canonicalTaskRunJSONObject(definition.ScopeJSON)
	if err != nil {
		return nil, "", "", err
	}
	seenSourceIDs := make(map[int64]struct{}, len(definition.Sources))
	seenSourceURLs := make(map[string]struct{}, len(definition.Sources))
	for i := range definition.Sources {
		if !validTaskRunSourceIdentity(definition.Sources[i]) {
			return nil, "", "", errors.New("invalid source identity")
		}
		if _, duplicate := seenSourceIDs[definition.Sources[i].SourceID]; duplicate {
			return nil, "", "", errors.New("source identity is duplicated")
		}
		if _, duplicate := seenSourceURLs[definition.Sources[i].URL]; duplicate {
			return nil, "", "", errors.New("source url is duplicated")
		}
		seenSourceIDs[definition.Sources[i].SourceID] = struct{}{}
		seenSourceURLs[definition.Sources[i].URL] = struct{}{}
		definition.Sources[i].Config, err = canonicalTaskRunJSONObject(
			definition.Sources[i].Config)
		if err != nil {
			return nil, "", "", err
		}
		if i > 0 && definition.Sources[i-1].URL >= definition.Sources[i].URL {
			return nil, "", "", errors.New("source identities are not canonical")
		}
	}

	var definitionDigest string
	switch definition.SourceScope {
	case taskRunSourceScopeApproved:
		compiledDef := types.PausedCompiledTaskDefinition{
			TaskID: definition.TaskID, TenantID: definition.TenantID, UserID: definition.UserID,
			NLDescription: definition.NLDescription, SpecJSON: definition.SpecJSON,
			ScopeJSON: definition.ScopeJSON, PlaybookContent: definition.PlaybookContent,
			FetchPlan: definition.FetchPlan, Strictness: definition.Strictness,
		}
		plan, validateErr := validatePausedCompiledTaskDefinition(compiledDef)
		if validateErr != nil {
			return nil, "", "", errors.New("approved definition is invalid")
		}
		compiledDef.FetchPlan, err = canonicalTaskRunCompiledPlan(plan)
		if err != nil || len(definition.Sources) != len(plan.Sources) {
			return nil, "", "", errors.New("approved plan is not canonical")
		}
		definition.FetchPlan = compiledDef.FetchPlan
		planURLs := make(map[string]struct{}, len(plan.Sources))
		for _, source := range plan.Sources {
			planURLs[source.URL] = struct{}{}
		}
		for _, source := range definition.Sources {
			if _, ok := planURLs[source.URL]; !ok {
				return nil, "", "", errors.New("approved plan links differ")
			}
		}
		if len(planURLs) != len(seenSourceURLs) {
			return nil, "", "", errors.New("approved plan links differ")
		}
		definitionDigest, err = types.DigestPausedCompiledTaskDefinition(compiledDef)
	case taskRunSourceScopeLegacy:
		var planObject map[string]json.RawMessage
		if strictjson.Decode(definition.FetchPlan, &planObject) != nil || planObject == nil ||
			len(planObject) != 0 {
			return nil, "", "", errors.New("legacy plan is not empty")
		}
		canonicalEmpty, canonicalErr := canonicalTaskRunJSONObject(definition.FetchPlan)
		if canonicalErr != nil || !bytes.Equal(canonicalEmpty, []byte("{}")) {
			return nil, "", "", errors.New("legacy plan is not canonical")
		}
		definition.FetchPlan = canonicalEmpty
		definitionDigest, err = digestTaskRunLegacyDefinition(*definition)
	default:
		return nil, "", "", errors.New("source scope is invalid")
	}
	if err != nil {
		return nil, "", "", err
	}
	planDigest, err := digestTaskRunPlan(*definition)
	if err != nil {
		return nil, "", "", err
	}
	canonical, err := json.Marshal(payload)
	if err != nil || len(canonical) > maxTaskRunPayloadBytes {
		return nil, "", "", errors.New("payload cannot be canonicalized")
	}
	return canonical, definitionDigest, planDigest, nil
}

func canonicalTaskRunCompiledPlan(plan *compiledFetchPlan) (json.RawMessage, error) {
	if plan == nil {
		return nil, errors.New("compiled plan is missing")
	}
	for i := range plan.Sources {
		canonical, err := canonicalTaskRunJSONObject(plan.Sources[i].Config)
		if err != nil {
			return nil, err
		}
		plan.Sources[i].Config = canonical
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
