package runtimepolicy

import (
	"cmp"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	maxShortTextBytes = 512
	maxPromptBytes    = 64 << 10
	maxCapabilities   = 256
	maxQuotaBuckets   = 64
	maxModelTokens    = 32_768
)

// ErrInvalidPolicy is returned for malformed, unsupported, or non-canonical
// runtime policy input.
var ErrInvalidPolicy = errors.New("runtimepolicy: invalid policy")

// Validate verifies the complete V1 bundle without mutating it.
func (b BundleV1) Validate() error {
	if b.SchemaVersion != BundleSchemaVersionV1 {
		return invalidPolicy("bundle schema version is unsupported")
	}
	if err := b.CapabilityCatalog.Validate(); err != nil {
		return err
	}
	if err := b.ToolPolicy.Validate(); err != nil {
		return err
	}
	if err := b.PromptPolicy.Validate(); err != nil {
		return err
	}
	if err := b.ModelPolicy.Validate(); err != nil {
		return err
	}
	return b.QuotaPolicy.Validate()
}

// Validate verifies the V1 capability catalog.
func (p CapabilityCatalogV1) Validate() error {
	if p.SchemaVersion != CapabilityCatalogSchemaVersionV1 {
		return invalidPolicy("capability catalog schema version is unsupported")
	}
	if len(p.Allowed) == 0 || len(p.Allowed) > maxCapabilities {
		return invalidPolicy("capability allowlist size is invalid")
	}
	seen := make(map[string]struct{}, len(p.Allowed))
	for _, capability := range p.Allowed {
		if !validShortText(capability.Platform) || !validShortText(capability.Capability) ||
			!validShortText(capability.Kind) {
			return invalidPolicy("capability entry is invalid")
		}
		if err := capability.validateImplementationCredential(); err != nil {
			return err
		}
		key := capability.Platform + "\x00" + capability.Capability
		if _, duplicate := seen[key]; duplicate {
			return invalidPolicy("capability entry is duplicated")
		}
		seen[key] = struct{}{}
	}
	return nil
}

// Validate verifies that compiled V1 exposes no tools.
func (p ToolPolicyV1) Validate() error {
	if p.SchemaVersion != ToolPolicySchemaVersionV1 {
		return invalidPolicy("tool policy schema version is unsupported")
	}
	if p.AllowedTools == nil || len(p.AllowedTools) != 0 {
		return invalidPolicy("compiled tool allowlist must be an empty array")
	}
	return nil
}

// Validate verifies the V1 prompt policy.
func (p PromptPolicyV1) Validate() error {
	if p.SchemaVersion != PromptPolicySchemaVersionV1 {
		return invalidPolicy("prompt policy schema version is unsupported")
	}
	for _, stage := range []PromptStageV1{p.Score, p.CardGen, p.ProfileEvolve} {
		if !validPrompt(stage.SystemPrompt) || !validShortText(stage.RendererVersion) {
			return invalidPolicy("prompt stage is invalid")
		}
	}
	return nil
}

// Validate verifies the V1 model policy.
func (p ModelPolicyV1) Validate() error {
	if p.SchemaVersion != ModelPolicySchemaVersionV1 {
		return invalidPolicy("model policy schema version is unsupported")
	}
	if p.Provider != ModelProviderDeepSeekV1 || !p.Endpoint.valid() {
		return invalidPolicy("model route is invalid")
	}
	if err := p.CredentialRef.validateFor(CredentialIDLLMPrimaryV1); err != nil {
		return err
	}
	if len(p.Calls) != 3 {
		return invalidPolicy("compiled model policy must contain exactly three stages")
	}
	seen := make(map[string]struct{}, len(p.Calls))
	for _, call := range p.Calls {
		if !validModelStage(call.Stage) || !validShortText(call.Model) ||
			math.IsNaN(call.Temperature) || math.IsInf(call.Temperature, 0) ||
			call.Temperature < 0 || call.Temperature > 2 ||
			call.MaxTokens <= 0 || call.MaxTokens > maxModelTokens || !call.DisableThinking {
			return invalidPolicy("model call is invalid")
		}
		if _, duplicate := seen[call.Stage]; duplicate {
			return invalidPolicy("model stage is duplicated")
		}
		seen[call.Stage] = struct{}{}
	}
	for _, stage := range []string{ModelStageScore, ModelStageCardGen, ModelStageProfileEvolve} {
		if _, ok := seen[stage]; !ok {
			return invalidPolicy("compiled model stage is missing")
		}
	}
	return nil
}

// Validate verifies the immutable V1 quota rules.
func (p QuotaPolicyV1) Validate() error {
	if p.SchemaVersion != QuotaPolicySchemaVersionV1 {
		return invalidPolicy("quota policy schema version is unsupported")
	}
	if len(p.Buckets) == 0 || len(p.Buckets) > maxQuotaBuckets {
		return invalidPolicy("quota bucket count is invalid")
	}
	seen := make(map[string]struct{}, len(p.Buckets))
	for _, bucket := range p.Buckets {
		if !validShortText(bucket.Name) || !validShortText(bucket.EnforcementVersion) {
			return invalidPolicy("quota bucket is invalid")
		}
		if _, duplicate := seen[bucket.Name]; duplicate {
			return invalidPolicy("quota bucket is duplicated")
		}
		seen[bucket.Name] = struct{}{}
	}
	return nil
}

func (r CredentialRefV1) validateOptional() error {
	empty := r == (CredentialRefV1{})
	if empty {
		return nil
	}
	return r.validateRequired()
}

func (r CredentialRefV1) validateRequired() error {
	if !validCredentialIDV1(r.ID) || r.Generation <= 0 {
		return invalidPolicy("credential reference is invalid")
	}
	return nil
}

func (r CredentialRefV1) validateFor(expected CredentialIDV1) error {
	if r.ID != expected {
		return invalidPolicy("credential reference purpose is invalid")
	}
	return r.validateRequired()
}

func (c CapabilityV1) validateImplementationCredential() error {
	switch c.ImplementationVersion {
	case CapabilityImplementationRSSV1:
		if c.CredentialRef != (CredentialRefV1{}) {
			return invalidPolicy("credentialless capability has a credential")
		}
		if len(c.DependencyCredentialRefs) != 1 {
			return invalidPolicy("rss capability must freeze one enrichment credential")
		}
		return c.DependencyCredentialRefs[0].validateFor(CredentialIDExaPrimaryV1)
	case CapabilityImplementationExaV1:
		if len(c.DependencyCredentialRefs) != 0 {
			return invalidPolicy("exa capability has unexpected dependency credentials")
		}
		return c.CredentialRef.validateFor(CredentialIDExaPrimaryV1)
	case CapabilityImplementationBindingV1:
		if len(c.DependencyCredentialRefs) != 0 {
			return invalidPolicy("binding capability has unexpected dependency credentials")
		}
		return c.CredentialRef.validateFor(CredentialIDTikHubPrimaryV1)
	default:
		return invalidPolicy("capability implementation is unsupported")
	}
}

func (r EndpointRefV1) valid() bool {
	return validEndpointIDV1(r.ID) && r.Generation > 0
}

func normalizeBundleV1(bundle BundleV1) (BundleV1, error) {
	capabilities, err := normalizeCapabilityCatalogV1(bundle.CapabilityCatalog)
	if err != nil {
		return BundleV1{}, err
	}
	tools, err := normalizeToolPolicyV1(bundle.ToolPolicy)
	if err != nil {
		return BundleV1{}, err
	}
	if err := bundle.PromptPolicy.Validate(); err != nil {
		return BundleV1{}, err
	}
	model, err := normalizeModelPolicyV1(bundle.ModelPolicy)
	if err != nil {
		return BundleV1{}, err
	}
	quota, err := normalizeQuotaPolicyV1(bundle.QuotaPolicy)
	if err != nil {
		return BundleV1{}, err
	}
	bundle.CapabilityCatalog = capabilities
	bundle.ToolPolicy = tools
	bundle.ModelPolicy = model
	bundle.QuotaPolicy = quota
	if err := bundle.Validate(); err != nil {
		return BundleV1{}, err
	}
	return bundle, nil
}

func normalizeCapabilityCatalogV1(policy CapabilityCatalogV1) (CapabilityCatalogV1, error) {
	policy.Allowed = slices.Clone(policy.Allowed)
	for i := range policy.Allowed {
		refs := slices.Clone(policy.Allowed[i].DependencyCredentialRefs)
		if refs == nil {
			refs = []CredentialRefV1{}
		}
		slices.SortFunc(refs, func(left, right CredentialRefV1) int {
			if n := strings.Compare(string(left.ID), string(right.ID)); n != 0 {
				return n
			}
			return cmp.Compare(left.Generation, right.Generation)
		})
		for j := 1; j < len(refs); j++ {
			if refs[j] == refs[j-1] {
				return CapabilityCatalogV1{}, invalidPolicy(
					"capability dependency credential is duplicated")
			}
		}
		policy.Allowed[i].DependencyCredentialRefs = refs
	}
	if err := policy.Validate(); err != nil {
		return CapabilityCatalogV1{}, err
	}
	slices.SortFunc(policy.Allowed, func(left, right CapabilityV1) int {
		if n := strings.Compare(left.Platform, right.Platform); n != 0 {
			return n
		}
		return strings.Compare(left.Capability, right.Capability)
	})
	return policy, nil
}

func normalizeToolPolicyV1(policy ToolPolicyV1) (ToolPolicyV1, error) {
	if policy.AllowedTools != nil {
		policy.AllowedTools = slices.Clone(policy.AllowedTools)
	}
	if err := policy.Validate(); err != nil {
		return ToolPolicyV1{}, err
	}
	return policy, nil
}

func normalizeModelPolicyV1(policy ModelPolicyV1) (ModelPolicyV1, error) {
	policy.Calls = slices.Clone(policy.Calls)
	if err := policy.Validate(); err != nil {
		return ModelPolicyV1{}, err
	}
	slices.SortFunc(policy.Calls, func(left, right ModelCallV1) int {
		return strings.Compare(left.Stage, right.Stage)
	})
	return policy, nil
}

func normalizeQuotaPolicyV1(policy QuotaPolicyV1) (QuotaPolicyV1, error) {
	policy.Buckets = slices.Clone(policy.Buckets)
	if err := policy.Validate(); err != nil {
		return QuotaPolicyV1{}, err
	}
	slices.SortFunc(policy.Buckets, func(left, right QuotaBucketV1) int {
		return strings.Compare(left.Name, right.Name)
	})
	return policy, nil
}

func validModelStage(stage string) bool {
	switch stage {
	case ModelStageScore, ModelStageCardGen, ModelStageProfileEvolve:
		return true
	default:
		return false
	}
}

func validCredentialIDV1(id CredentialIDV1) bool {
	switch id {
	case CredentialIDLLMPrimaryV1,
		CredentialIDExaPrimaryV1,
		CredentialIDTikHubPrimaryV1,
		CredentialIDFeishuPrimaryV1:
		return true
	default:
		return false
	}
}

func validEndpointIDV1(id EndpointIDV1) bool {
	switch id {
	case EndpointIDDeepSeekCompatiblePrimaryV1:
		return true
	default:
		return false
	}
}

func validShortText(value string) bool {
	return validText(value, maxShortTextBytes, false)
}

func validPrompt(value string) bool {
	return validText(value, maxPromptBytes, true)
}

func validText(value string, maxBytes int, allowLayoutControls bool) bool {
	if value == "" || strings.TrimSpace(value) != value || len(value) > maxBytes ||
		!utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if unicode.In(r, unicode.Cf) {
			return false
		}
		if unicode.IsControl(r) && !(allowLayoutControls && (r == '\n' || r == '\r' || r == '\t')) {
			return false
		}
	}
	return true
}

func invalidPolicy(message string) error {
	return fmt.Errorf("%w: %s", ErrInvalidPolicy, message)
}
