// Package runtimepolicy defines versioned, non-secret runtime policy payloads.
//
// The package deliberately accepts credential references only. Credential
// values belong to the controlled resolver and must never enter these DTOs or
// their canonical JSON bytes.
package runtimepolicy

const (
	// PrimaryGenerationV1 is the first controlled resolver generation for the
	// V1 endpoint and credential aliases. A future rotation that changes an
	// alias's meaning must add a new generation and keep the old resolver while
	// retained run snapshots may still consume it.
	PrimaryGenerationV1 int64 = 1

	// BundleSchemaVersionV1 identifies the complete C1 compiled policy bundle.
	BundleSchemaVersionV1 = "vane.runtime-policy-bundle/v1"

	// CapabilityCatalogSchemaVersionV1 identifies the compiled capability allowlist.
	CapabilityCatalogSchemaVersionV1 = "vane.runtime-capability-catalog/v1"
	// ToolPolicySchemaVersionV1 identifies the compiled tool policy. Its allowlist is empty.
	ToolPolicySchemaVersionV1 = "vane.runtime-tool-policy/v1"
	// PromptPolicySchemaVersionV1 identifies the three compiled prompt renderers.
	PromptPolicySchemaVersionV1 = "vane.runtime-prompt-policy/v1"
	// ModelPolicySchemaVersionV1 identifies model routing and request parameters.
	ModelPolicySchemaVersionV1 = "vane.runtime-model-policy/v1"
	// QuotaPolicySchemaVersionV1 identifies immutable quota rules for one run.
	QuotaPolicySchemaVersionV1 = "vane.runtime-quota-policy/v1"
	// QuotaEnforcementLLMPrechargeV1 identifies the retained precharge plus
	// actual-token reconciliation algorithm used by compiled LLM calls.
	QuotaEnforcementLLMPrechargeV1 = "precharge-reconcile/v1"
)

// CapabilityImplementationIDV1 names a read-only worker implementation whose
// credential purpose is frozen by V1 validation. It is not an arbitrary
// executable, package path, URL, or plugin identifier.
type CapabilityImplementationIDV1 string

const (
	// CapabilityImplementationRSSV1 is the credentialless RSS/Atom reader;
	// its paid Exa enrichment route is frozen as a dependency credential.
	CapabilityImplementationRSSV1 CapabilityImplementationIDV1 = "fetcher.rss/v1"
	// CapabilityImplementationExaV1 is the Exa search/content reader.
	CapabilityImplementationExaV1 CapabilityImplementationIDV1 = "fetcher.exa/v1"
	// CapabilityImplementationBindingV1 is the vetted TikHub binding engine.
	CapabilityImplementationBindingV1 CapabilityImplementationIDV1 = "fetcher.binding/v1"
)

// ModelProviderIDV1 names a controlled model-provider adapter.
type ModelProviderIDV1 string

const (
	// ModelProviderDeepSeekV1 selects the DeepSeek-compatible model adapter.
	ModelProviderDeepSeekV1 ModelProviderIDV1 = "deepseek"
)

const (
	// ModelStageScore is the relevance-scoring LLM call.
	ModelStageScore = "score"
	// ModelStageCardGen is the delivery-card generation LLM call.
	ModelStageCardGen = "cardgen"
	// ModelStageProfileEvolve is the pre-run profile evolution LLM call.
	ModelStageProfileEvolve = "profile_evolve"
)

// CredentialIDV1 is a controlled logical alias resolved by the worker's
// credential registry. An alias names its purpose/provider so validation can
// prevent cross-purpose use, but never carries an environment variable name or
// credential value.
type CredentialIDV1 string

const (
	// CredentialIDLLMPrimaryV1 is the primary compiled-pipeline LLM credential.
	CredentialIDLLMPrimaryV1 CredentialIDV1 = "llm-primary"
	// CredentialIDExaPrimaryV1 is the primary Exa credential.
	CredentialIDExaPrimaryV1 CredentialIDV1 = "exa-primary"
	// CredentialIDTikHubPrimaryV1 is the primary TikHub credential.
	CredentialIDTikHubPrimaryV1 CredentialIDV1 = "tikhub-primary"
	// CredentialIDFeishuPrimaryV1 is the primary Feishu application credential.
	CredentialIDFeishuPrimaryV1 CredentialIDV1 = "feishu-primary"
)

// EndpointIDV1 is a controlled logical alias resolved by the worker's endpoint
// registry. Endpoint URLs never enter a runtime policy snapshot.
type EndpointIDV1 string

const (
	// EndpointIDDeepSeekCompatiblePrimaryV1 selects the primary
	// OpenAI-compatible DeepSeek endpoint.
	EndpointIDDeepSeekCompatiblePrimaryV1 EndpointIDV1 = "deepseek-compatible-primary"
)

// BundleV1 is the complete set of independently content-addressed compiled
// runtime policies. Every nested policy carries its own schema version because
// the durable snapshot stores and digests the five bodies separately.
type BundleV1 struct {
	SchemaVersion     string              `json:"schema_version"`
	CapabilityCatalog CapabilityCatalogV1 `json:"capability_catalog"`
	ToolPolicy        ToolPolicyV1        `json:"tool_policy"`
	PromptPolicy      PromptPolicyV1      `json:"prompt_policy"`
	ModelPolicy       ModelPolicyV1       `json:"model_policy"`
	QuotaPolicy       QuotaPolicyV1       `json:"quota_policy"`
}

// CredentialRefV1 identifies a controlled credential generation without
// carrying provider-controlled names, a credential value, or a digest derived
// from that value. The zero value is reserved for capabilities that need no
// credential.
type CredentialRefV1 struct {
	ID         CredentialIDV1 `json:"id"`
	Generation int64          `json:"generation"`
}

// EndpointRefV1 identifies a controlled endpoint generation. The resolver
// maps this opaque ID to a URL out of band, immediately before use.
type EndpointRefV1 struct {
	ID         EndpointIDV1 `json:"id"`
	Generation int64        `json:"generation"`
}

// CapabilityCatalogV1 is the exact capability allowlist available to a
// compiled run. Entries are canonically ordered by platform and capability.
type CapabilityCatalogV1 struct {
	SchemaVersion string         `json:"schema_version"`
	Allowed       []CapabilityV1 `json:"allowed"`
}

// CapabilityV1 pins a registered read-only fetch capability and the worker
// implementation generation that knows how to execute it.
type CapabilityV1 struct {
	Platform                 string                       `json:"platform"`
	Capability               string                       `json:"capability"`
	Kind                     string                       `json:"kind"`
	ImplementationVersion    CapabilityImplementationIDV1 `json:"implementation_version"`
	CredentialRef            CredentialRefV1              `json:"credential_ref"`
	DependencyCredentialRefs []CredentialRefV1            `json:"dependency_credential_refs"`
}

// ToolPolicyV1 is intentionally empty for compiled execution. Planner and
// conversational agent tools are not reachable in C1 compiled runs.
type ToolPolicyV1 struct {
	SchemaVersion string   `json:"schema_version"`
	AllowedTools  []string `json:"allowed_tools"`
}

// PromptPolicyV1 freezes prompt bodies, renderer generations, and the
// per-task result of the playbook rollout decision.
type PromptPolicyV1 struct {
	SchemaVersion          string        `json:"schema_version"`
	Score                  PromptStageV1 `json:"score"`
	CardGen                PromptStageV1 `json:"cardgen"`
	ProfileEvolve          PromptStageV1 `json:"profile_evolve"`
	TaskInstructionEnabled bool          `json:"task_instruction_enabled"`
}

// PromptStageV1 pins one system prompt and the deterministic renderer used to
// construct its dynamic user message.
type PromptStageV1 struct {
	SystemPrompt    string `json:"system_prompt"`
	RendererVersion string `json:"renderer_version"`
}

// ModelPolicyV1 freezes the non-secret upstream route and every compiled LLM
// call's model parameters. CredentialRef is resolved immediately before use.
type ModelPolicyV1 struct {
	SchemaVersion string            `json:"schema_version"`
	Provider      ModelProviderIDV1 `json:"provider"`
	Endpoint      EndpointRefV1     `json:"endpoint"`
	CredentialRef CredentialRefV1   `json:"credential_ref"`
	Calls         []ModelCallV1     `json:"calls"`
}

// Call returns the immutable parameters for stage.
func (p ModelPolicyV1) Call(stage string) (ModelCallV1, bool) {
	for _, call := range p.Calls {
		if call.Stage == stage {
			return call, true
		}
	}
	return ModelCallV1{}, false
}

// ModelCallV1 is the complete model-visible parameter set for one compiled
// pipeline stage.
type ModelCallV1 struct {
	Stage           string  `json:"stage"`
	Model           string  `json:"model"`
	Temperature     float64 `json:"temperature"`
	MaxTokens       int     `json:"max_tokens"`
	DisableThinking bool    `json:"disable_thinking"`
}

// QuotaPolicyV1 freezes the identity and enforcement generation of financial
// gates used by a run. Mutable tenant balance, rate and burst are live
// authorization state: freezing those values while mutating one shared bucket
// would make concurrent policy generations mathematically inconsistent.
type QuotaPolicyV1 struct {
	SchemaVersion string          `json:"schema_version"`
	Buckets       []QuotaBucketV1 `json:"buckets"`
}

// Bucket returns the immutable rule named name.
func (p QuotaPolicyV1) Bucket(name string) (QuotaBucketV1, bool) {
	for _, bucket := range p.Buckets {
		if bucket.Name == name {
			return bucket, true
		}
	}
	return QuotaBucketV1{}, false
}

// QuotaBucketV1 freezes one bucket identity and its enforcement algorithm.
// Per-call/model hard limits live in ModelPolicyV1; tenant rate/burst/tokens
// are deliberately resolved from the exact tenant immediately before spend.
type QuotaBucketV1 struct {
	Name               string `json:"name"`
	Financial          bool   `json:"financial"`
	EnforcementVersion string `json:"enforcement_version"`
}

// BuildInputV1 is the explicit non-secret input boundary for BuildV1. It has
// no generic config or credential-value slot. Tool policy is omitted entirely
// because compiled V1 always has an empty tool allowlist.
type BuildInputV1 struct {
	AllowedCapabilities    []CapabilityV1    `json:"-"`
	ScorePrompt            PromptStageV1     `json:"-"`
	CardGenPrompt          PromptStageV1     `json:"-"`
	ProfileEvolvePrompt    PromptStageV1     `json:"-"`
	TaskInstructionEnabled bool              `json:"-"`
	ModelProvider          ModelProviderIDV1 `json:"-"`
	ModelEndpoint          EndpointRefV1     `json:"-"`
	ModelCredentialRef     CredentialRefV1   `json:"-"`
	ModelCalls             []ModelCallV1     `json:"-"`
	QuotaBuckets           []QuotaBucketV1   `json:"-"`
}

// BuildV1 constructs a validated, canonically ordered compiled policy bundle.
// Caller-owned slices are copied and are never mutated.
func BuildV1(input BuildInputV1) (BundleV1, error) {
	bundle := BundleV1{
		SchemaVersion: BundleSchemaVersionV1,
		CapabilityCatalog: CapabilityCatalogV1{
			SchemaVersion: CapabilityCatalogSchemaVersionV1,
			Allowed:       input.AllowedCapabilities,
		},
		ToolPolicy: ToolPolicyV1{
			SchemaVersion: ToolPolicySchemaVersionV1,
			AllowedTools:  []string{},
		},
		PromptPolicy: PromptPolicyV1{
			SchemaVersion:          PromptPolicySchemaVersionV1,
			Score:                  input.ScorePrompt,
			CardGen:                input.CardGenPrompt,
			ProfileEvolve:          input.ProfileEvolvePrompt,
			TaskInstructionEnabled: input.TaskInstructionEnabled,
		},
		ModelPolicy: ModelPolicyV1{
			SchemaVersion: ModelPolicySchemaVersionV1,
			Provider:      input.ModelProvider,
			Endpoint:      input.ModelEndpoint,
			CredentialRef: input.ModelCredentialRef,
			Calls:         input.ModelCalls,
		},
		QuotaPolicy: QuotaPolicyV1{
			SchemaVersion: QuotaPolicySchemaVersionV1,
			Buckets:       input.QuotaBuckets,
		},
	}
	return normalizeBundleV1(bundle)
}
