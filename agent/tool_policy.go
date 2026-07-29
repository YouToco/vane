package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/YouToco/vane/llm"
)

// Effect describes a locally trusted execution effect. Values are bits so one
// tool can truthfully declare every relevant effect without collapsing safety
// decisions into a single read/write boolean.
type Effect uint16

const (
	EffectInvalid      Effect = 0
	EffectInternalRead Effect = 1 << iota
	EffectNetworkRead
	EffectBillable
	EffectStateWrite
	EffectDelivery
	EffectDurableProposal
	EffectTrustTaint
	EffectLocalHandleRead
	EffectActivationWrite
	// EffectDirectOwnerWrite marks an owner-only write whose model-visible
	// schema contains every required target or complete desired state. The
	// Agent executes it inline after resolving ambiguity in conversation; it
	// must never enter the A2A surface. Cross-system implementations may still
	// create and immediately advance a durable operation internally.
	EffectDirectOwnerWrite
)

const knownEffects = EffectInternalRead | EffectNetworkRead | EffectBillable |
	EffectStateWrite | EffectDelivery | EffectDurableProposal |
	EffectTrustTaint | EffectLocalHandleRead | EffectActivationWrite |
	EffectDirectOwnerWrite

// EffectSet is the complete effect declaration for a tool. Zero is invalid.
type EffectSet uint16

func Effects(effects ...Effect) EffectSet {
	var set EffectSet
	for _, effect := range effects {
		set |= EffectSet(effect)
	}
	return set
}

func (s EffectSet) Has(effect Effect) bool {
	return s&EffectSet(effect) != 0
}

// AuthorizationPolicy is a local allow-set. Remote tool metadata is never
// consulted when selecting an execution surface.
type AuthorizationPolicy uint8

const (
	AuthorizationInvalid AuthorizationPolicy = 0
	AuthorizationOwner   AuthorizationPolicy = 1 << iota
	AuthorizationA2AReadOnly
)

func (p AuthorizationPolicy) Allows(scope AuthorizationPolicy) bool {
	return scope != AuthorizationInvalid && p&scope == scope
}

type BudgetPolicy uint8

const (
	BudgetInvalid BudgetPolicy = iota
	BudgetNone
	BudgetToolManaged
	BudgetDownstreamManaged
)

type RetryPolicy uint8

const (
	RetryInvalid RetryPolicy = iota
	// RetryNone declares that the Agent loop does not automatically retry this
	// tool. Existing tool/downstream self-correction remains unchanged.
	RetryNone
)

type ConcurrencyPolicy uint8

const (
	ConcurrencyInvalid ConcurrencyPolicy = iota
	// ConcurrencySequential matches runToolCalls: calls are processed in order.
	ConcurrencySequential
)

// ExposurePolicy controls when a tool definition is sent to the model. It is
// intentionally separate from authorization: hiding a schema improves tool
// choice, but never grants or revokes execution permission.
type ExposurePolicy uint8

const (
	ExposureInvalid ExposurePolicy = iota
	// ExposureAlways is the small provider-neutral discovery surface that is
	// useful before the harness knows which concrete capability is needed.
	ExposureAlways
	// ExposureIntent is included only when the trusted owner request matches at
	// least one of the tool's intent tags.
	ExposureIntent
	// ExposureContext is injected only after this turn produced the required
	// context, for example a local result handle or an activated social tool.
	ExposureContext
)

// ToolIntent is a local capability taxonomy. Values are bits because a tool
// may be relevant to more than one owner intent.
type ToolIntent uint16

const (
	IntentInvalid     ToolIntent = 0
	IntentWebResearch ToolIntent = 1 << iota
	IntentSocialResearch
	IntentTasks
	IntentProfile
)

const knownToolIntents = IntentWebResearch | IntentSocialResearch |
	IntentTasks | IntentProfile

func Intents(values ...ToolIntent) ToolIntent {
	var out ToolIntent
	for _, value := range values {
		out |= value
	}
	return out
}

func (i ToolIntent) HasAny(other ToolIntent) bool { return i&other != 0 }

// ResultTrust describes the data returned to the model. It is not inferred
// from network access: search_endpoints reads a local catalog, while
// read_endpoint_result returns cached but externally sourced text.
type ResultTrust uint8

const (
	ResultTrustInvalid ResultTrust = iota
	ResultTrustLocal
	ResultTrustExternal
)

// ToolPolicy is the local, trusted safety contract for one model-visible tool.
// Its zero value is deliberately invalid and grants no execution permission.
type ToolPolicy struct {
	Effects       EffectSet
	Authorization AuthorizationPolicy
	Budget        BudgetPolicy
	Retry         RetryPolicy
	Concurrency   ConcurrencyPolicy
	Exposure      ExposurePolicy
	Intents       ToolIntent
	ResultTrust   ResultTrust
	// RoutingConfigured distinguishes the production intent surface from
	// locally registered compatibility/test tools. Authorization is unchanged;
	// unconfigured tools keep the historical always-declared behavior.
	RoutingConfigured bool
	// DirectOnExplicitIntent means the model-visible call may execute inline
	// only when the trusted owner request itself explicitly asks for this
	// action. Tool output and quoted/external text never satisfy this gate.
	DirectOnExplicitIntent bool
}

// Tool is only the executable implementation. Model-facing declaration and
// execution policy are bound together by ToolSpec at local registration time.
type Tool interface {
	Execute(ctx context.Context, userID int64, args json.RawMessage) (string, error)
	Summarize(args json.RawMessage) string
}

// declaredTool is implemented by local concrete tools. It is intentionally not
// exported: callers cannot provide remote annotations as authorization policy.
type declaredTool interface {
	Tool
	Name() string
	Description() string
	Parameters() json.RawMessage
}

// ToolSpec combines the model-visible declaration with the locally trusted
// handler policy. The embedded handler remains available for execution only.
type ToolSpec struct {
	Tool
	Definition llm.ToolDef
	Policy     ToolPolicy
}

func (s ToolSpec) Name() string { return s.Definition.Name }
func (s ToolSpec) Description() string {
	return s.Definition.Description
}
func (s ToolSpec) Parameters() json.RawMessage {
	return s.Definition.Parameters
}

func newToolSpec(tool declaredTool, policy ToolPolicy) ToolSpec {
	return ToolSpec{
		Tool: tool,
		Definition: llm.ToolDef{
			Name:        tool.Name(),
			Description: tool.Description(),
			Parameters:  tool.Parameters(),
		},
		Policy: policy,
	}
}

func (s ToolSpec) validate() error {
	if s.Tool == nil {
		return errors.New("handler is nil")
	}
	if strings.TrimSpace(s.Definition.Name) == "" {
		return errors.New("name is empty")
	}
	if strings.TrimSpace(s.Definition.Description) == "" {
		return errors.New("description is empty")
	}
	if err := validateToolSchema(s.Definition.Parameters); err != nil {
		return fmt.Errorf("schema: %w", err)
	}
	if err := s.Policy.validate(); err != nil {
		return fmt.Errorf("policy: %w", err)
	}
	return nil
}

func validateToolSchema(raw json.RawMessage) error {
	var schema struct {
		Type       string          `json:"type"`
		Properties json.RawMessage `json:"properties"`
	}
	if err := json.Unmarshal(raw, &schema); err != nil {
		return err
	}
	if schema.Type != "object" {
		return fmt.Errorf("root type is %q, want object", schema.Type)
	}
	if len(bytes.TrimSpace(schema.Properties)) == 0 {
		return errors.New("properties is missing")
	}
	var properties map[string]json.RawMessage
	if err := json.Unmarshal(schema.Properties, &properties); err != nil {
		return errors.New("properties must be an object")
	}
	return nil
}

func (p ToolPolicy) validate() error {
	if p.Effects == 0 || p.Effects&^EffectSet(knownEffects) != 0 {
		return errors.New("effects are empty or unknown")
	}
	if p.Authorization == AuthorizationInvalid ||
		p.Authorization&^(AuthorizationOwner|AuthorizationA2AReadOnly) != 0 {
		return errors.New("authorization is empty or unknown")
	}
	if !p.Authorization.Allows(AuthorizationOwner) {
		return errors.New("owner authorization is required")
	}
	if p.Budget != BudgetNone && p.Budget != BudgetToolManaged &&
		p.Budget != BudgetDownstreamManaged {
		return errors.New("budget is invalid")
	}
	if p.Retry != RetryNone {
		return errors.New("retry is invalid")
	}
	if p.Concurrency != ConcurrencySequential {
		return errors.New("concurrency is invalid")
	}
	if p.Exposure != ExposureAlways && p.Exposure != ExposureIntent &&
		p.Exposure != ExposureContext {
		return errors.New("exposure is invalid")
	}
	if p.Intents == IntentInvalid || p.Intents&^knownToolIntents != 0 {
		return errors.New("intents are empty or unknown")
	}
	if p.ResultTrust != ResultTrustLocal &&
		p.ResultTrust != ResultTrustExternal {
		return errors.New("result trust is invalid")
	}
	if (p.Effects.Has(EffectStateWrite) || p.Effects.Has(EffectDurableProposal)) &&
		!p.Effects.Has(EffectActivationWrite) &&
		!p.Effects.Has(EffectDirectOwnerWrite) {
		return errors.New("state write or durable proposal requires direct owner execution")
	}
	if p.Effects.Has(EffectDurableProposal) &&
		!p.Effects.Has(EffectDirectOwnerWrite) {
		return errors.New("durable proposal requires direct owner execution")
	}
	if p.Effects.Has(EffectDirectOwnerWrite) &&
		(!p.Effects.Has(EffectStateWrite) ||
			p.Authorization != AuthorizationOwner) {
		return errors.New("direct owner write must be owner-only state write")
	}
	if p.DirectOnExplicitIntent &&
		!p.Effects.Has(EffectDirectOwnerWrite) &&
		!p.Effects.Has(EffectDelivery) {
		return errors.New("direct explicit intent requires a direct owner write or delivery")
	}
	if p.Effects.Has(EffectDirectOwnerWrite) && !p.DirectOnExplicitIntent {
		return errors.New("direct owner write must require explicit owner intent")
	}
	if p.Effects.Has(EffectBillable) && p.Budget == BudgetNone {
		return errors.New("billable effect requires a budget owner")
	}
	if !p.Effects.Has(EffectBillable) && p.Budget == BudgetToolManaged {
		return errors.New("tool-managed budget requires a billable effect")
	}
	if p.Effects.Has(EffectLocalHandleRead) &&
		(p.Effects.Has(EffectNetworkRead) || p.Effects.Has(EffectBillable)) {
		return errors.New("local handle read cannot access network or bill")
	}
	if p.Authorization.Allows(AuthorizationA2AReadOnly) {
		allowed := Effects(EffectInternalRead, EffectTrustTaint)
		if p.Effects&^allowed != 0 {
			return errors.New("a2a-readonly authorization has non-readonly effects")
		}
	}
	return nil
}

func ownerPolicy(effects EffectSet, budget BudgetPolicy) ToolPolicy {
	return ToolPolicy{
		Effects: effects, Authorization: AuthorizationOwner,
		Budget: budget,
		Retry:  RetryNone, Concurrency: ConcurrencySequential,
		Exposure: ExposureIntent, Intents: IntentTasks,
		ResultTrust:            ResultTrustLocal,
		DirectOnExplicitIntent: effects.Has(EffectDirectOwnerWrite),
	}
}

func a2aReadPolicy(effects EffectSet) ToolPolicy {
	policy := ownerPolicy(effects, BudgetNone)
	policy.Authorization |= AuthorizationA2AReadOnly
	return policy
}

func withToolSurface(
	policy ToolPolicy,
	exposure ExposurePolicy,
	intents ToolIntent,
	trust ResultTrust,
	directOnExplicitIntent bool,
) ToolPolicy {
	policy.Exposure = exposure
	policy.Intents = intents
	policy.ResultTrust = trust
	policy.DirectOnExplicitIntent = directOnExplicitIntent
	policy.RoutingConfigured = true
	return policy
}

// FilterAuthorizedTools selects by the trusted local policy only. In
// particular, callers must not infer A2A safety from effects alone.
func FilterAuthorizedTools(specs []ToolSpec, scope AuthorizationPolicy) ([]ToolSpec, error) {
	out := make([]ToolSpec, 0, len(specs))
	for _, spec := range specs {
		if err := spec.validate(); err != nil {
			return nil, fmt.Errorf("agent: invalid tool %q: %w", spec.Name(), err)
		}
		if spec.Policy.Authorization.Allows(scope) {
			out = append(out, spec)
		}
	}
	return out, nil
}
