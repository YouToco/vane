// Package capabilityruntime defines the immutable wire boundary shared by
// Vane-owned capability adapters. It deliberately contains no registry,
// authorization, network, database, or production runtime wiring.
package capabilityruntime

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/YouToco/vane/server/internal/strictjson"
	"github.com/YouToco/vane/server/types"
	"github.com/google/uuid"
)

const (
	InvocationSchemaVersionV1 = "vane.capability-invocation/v1"
	PolicySchemaVersionV1     = "vane.capability-policy/v1"
	ReceiptSchemaVersionV1    = "vane.capability-receipt/v1"

	maxArgumentsBytes  = 256 << 10
	maxResultBytes     = 16 << 20
	maxTimeoutMillis   = 15 * 60 * 1000
	maxIdentifierBytes = 255
	maxMediaTypeBytes  = 127
)

var ErrInvalidContract = errors.New("capabilityruntime: invalid contract")

type CapabilityKind string

const (
	CapabilityKindBuiltinTool      CapabilityKind = "builtin_tool"
	CapabilityKindDeclarativeSkill CapabilityKind = "declarative_skill"
	CapabilityKindRemoteMCP        CapabilityKind = "remote_mcp"
	CapabilityKindSandboxScript    CapabilityKind = "sandbox_script"
)

func (k CapabilityKind) valid() bool {
	switch k {
	case CapabilityKindBuiltinTool, CapabilityKindDeclarativeSkill,
		CapabilityKindRemoteMCP, CapabilityKindSandboxScript:
		return true
	default:
		return false
	}
}

type EffectV1 string

const (
	EffectInternalRead  EffectV1 = "internal_read"
	EffectNetworkRead   EffectV1 = "network_read"
	EffectBillable      EffectV1 = "billable"
	EffectStateWrite    EffectV1 = "state_write"
	EffectDelivery      EffectV1 = "delivery"
	EffectCodeExecution EffectV1 = "code_execution"
)

func (e EffectV1) valid() bool {
	switch e {
	case EffectInternalRead, EffectNetworkRead, EffectBillable,
		EffectStateWrite, EffectDelivery, EffectCodeExecution:
		return true
	default:
		return false
	}
}

type NetworkPolicyV1 string

const (
	NetworkPolicyNone                NetworkPolicyV1 = "none"
	NetworkPolicyPublicHTTPSReadOnly NetworkPolicyV1 = "public_https_read_only"
)

type IsolationV1 string

const (
	IsolationInProcess   IsolationV1 = "in_process"
	IsolationRemoteHTTPS IsolationV1 = "remote_https"
	IsolationFirecracker IsolationV1 = "firecracker"
)

type CapabilityScopeV1 string

const (
	CapabilityScopePlatform  CapabilityScopeV1 = "platform"
	CapabilityScopePersonal  CapabilityScopeV1 = "personal"
	CapabilityScopeWorkspace CapabilityScopeV1 = "workspace"
)

type CredentialScopeV1 string

const (
	CredentialScopePlatform CredentialScopeV1 = "platform"
	CredentialScopeTenant   CredentialScopeV1 = "tenant"
	CredentialScopeUser     CredentialScopeV1 = "user"
)

// PrincipalV1 is an explicit execution identity, never a user-to-tenant
// lookup hint. Role is observational; Prepare must live-reprove membership
// generation and service-token authority before an adapter can execute.
type PrincipalV1 struct {
	TenantID                          types.TenantID       `json:"tenant_id"`
	UserID                            int64                `json:"user_id"`
	Role                              types.MembershipRole `json:"role"`
	ActorType                         types.ActorType      `json:"actor_type"`
	MembershipAuthorizationGeneration int64                `json:"membership_authorization_generation"`
	A2ATokenAuthorityID               string               `json:"a2a_token_authority_id"`
	RequiredA2AScope                  types.A2AScope       `json:"required_a2a_scope"`
}

type CapabilityRefV1 struct {
	Kind                  CapabilityKind    `json:"kind"`
	Scope                 CapabilityScopeV1 `json:"scope"`
	OwnerUserID           int64             `json:"owner_user_id"`
	ID                    string            `json:"id"`
	VersionID             string            `json:"version_id"`
	VersionDigest         string            `json:"version_digest"`
	OperationSchemaDigest string            `json:"operation_schema_digest"`
}

// CredentialRefV1 freezes already-resolved, non-secret vault metadata. It
// cannot express "latest": every credentialful invocation pins generation
// and fingerprint under one exact scope/provider/purpose authority.
type CredentialRefV1 struct {
	Provider    string            `json:"provider"`
	Purpose     string            `json:"purpose"`
	Scope       CredentialScopeV1 `json:"scope"`
	UserID      int64             `json:"user_id"`
	Generation  int64             `json:"generation"`
	Fingerprint string            `json:"fingerprint"`
}

// PolicyV1 is the closed, locally-authoritative effect envelope frozen into
// every invocation. Remote annotations cannot widen it.
type PolicyV1 struct {
	SchemaVersion  string          `json:"schema_version"`
	Effects        []EffectV1      `json:"effects"`
	ReadOnly       bool            `json:"read_only"`
	Network        NetworkPolicyV1 `json:"network"`
	Isolation      IsolationV1     `json:"isolation"`
	TimeoutMillis  int64           `json:"timeout_millis"`
	MaxAttempts    int64           `json:"max_attempts"`
	MaxInputBytes  int64           `json:"max_input_bytes"`
	MaxOutputBytes int64           `json:"max_output_bytes"`
}

// InvocationV1 binds one explicit principal, one immutable capability
// version, one frozen schema/policy/credential generation, canonical input,
// and one scoped idempotency identity.
type InvocationV1 struct {
	SchemaVersion     string          `json:"schema_version"`
	Principal         PrincipalV1     `json:"principal"`
	Capability        CapabilityRefV1 `json:"capability"`
	Operation         string          `json:"operation"`
	Policy            PolicyV1        `json:"policy"`
	PolicyDigest      string          `json:"policy_digest"`
	Credential        CredentialRefV1 `json:"credential"`
	Arguments         json.RawMessage `json:"arguments"`
	IdempotencyKey    string          `json:"idempotency_key"`
	IdempotencyDigest string          `json:"idempotency_digest"`
	InvocationDigest  string          `json:"invocation_digest"`
}

type InvocationInputV1 struct {
	Principal      PrincipalV1
	Capability     CapabilityRefV1
	Operation      string
	Policy         PolicyV1
	Credential     CredentialRefV1
	Arguments      json.RawMessage
	IdempotencyKey string
}

func NewInvocationV1(input InvocationInputV1) (InvocationV1, error) {
	policy, err := normalizePolicyV1(input.Policy)
	if err != nil {
		return InvocationV1{}, err
	}
	arguments, err := canonicalJSONObject(input.Arguments)
	if err != nil {
		return InvocationV1{}, err
	}
	invocation := InvocationV1{
		SchemaVersion: InvocationSchemaVersionV1,
		Principal:     input.Principal, Capability: input.Capability,
		Operation: input.Operation, Policy: policy, Credential: input.Credential,
		Arguments: arguments, IdempotencyKey: input.IdempotencyKey,
	}
	if err := invocation.validateFields(); err != nil {
		return InvocationV1{}, err
	}
	invocation.PolicyDigest, err = digestJSON(policy)
	if err != nil {
		return InvocationV1{}, invalid("policy cannot be encoded")
	}
	invocation.IdempotencyDigest, err = invocation.expectedIdempotencyDigest()
	if err != nil {
		return InvocationV1{}, err
	}
	invocation.InvocationDigest, err = invocation.expectedInvocationDigest()
	if err != nil {
		return InvocationV1{}, err
	}
	return invocation, nil
}

func (i InvocationV1) Validate() error {
	if err := i.validateFields(); err != nil {
		return err
	}
	canonical, err := canonicalJSONObject(i.Arguments)
	if err != nil || !bytes.Equal(canonical, i.Arguments) {
		return invalid("arguments are not canonical")
	}
	policyDigest, err := digestJSON(i.Policy)
	if err != nil || i.PolicyDigest != policyDigest {
		return invalid("policy digest differs")
	}
	idempotencyDigest, err := i.expectedIdempotencyDigest()
	if err != nil || i.IdempotencyDigest != idempotencyDigest {
		return invalid("idempotency digest differs")
	}
	invocationDigest, err := i.expectedInvocationDigest()
	if err != nil || i.InvocationDigest != invocationDigest {
		return invalid("invocation digest differs")
	}
	return nil
}

func (i InvocationV1) validateFields() error {
	if i.SchemaVersion != InvocationSchemaVersionV1 {
		return invalid("invocation schema version is invalid")
	}
	if i.Principal.TenantID <= 0 || i.Principal.UserID <= 0 ||
		!i.Principal.Role.Valid() || !i.Principal.ActorType.Valid() ||
		i.Principal.MembershipAuthorizationGeneration <= 0 {
		return invalid("principal is invalid")
	}
	if err := i.Principal.validateAuthority(); err != nil {
		return err
	}
	if !i.Capability.Kind.valid() || !validIdentifier(i.Capability.ID) ||
		!validIdentifier(i.Capability.VersionID) ||
		!validSHA256(i.Capability.VersionDigest) ||
		!validSHA256(i.Capability.OperationSchemaDigest) {
		return invalid("capability reference is invalid")
	}
	if err := i.Capability.validateScope(i.Principal); err != nil {
		return err
	}
	if !validIdentifier(i.Operation) || !validIdentifier(i.IdempotencyKey) {
		return invalid("operation or idempotency key is invalid")
	}
	if err := i.Policy.Validate(); err != nil {
		return err
	}
	if err := i.Capability.validatePolicy(i.Policy); err != nil {
		return err
	}
	if err := i.Credential.validateFor(i.Principal); err != nil {
		return err
	}
	if len(i.Arguments) == 0 || len(i.Arguments) > maxArgumentsBytes {
		return invalid("arguments size is invalid")
	}
	if int64(len(i.Arguments)) > i.Policy.MaxInputBytes {
		return invalid("arguments exceed frozen input budget")
	}
	return nil
}

func (p PolicyV1) Validate() error {
	if p.SchemaVersion != PolicySchemaVersionV1 || p.Effects == nil ||
		p.TimeoutMillis <= 0 || p.TimeoutMillis > maxTimeoutMillis ||
		p.MaxAttempts <= 0 || p.MaxAttempts > 10 ||
		p.MaxInputBytes <= 0 || p.MaxInputBytes > maxArgumentsBytes ||
		p.MaxOutputBytes <= 0 || p.MaxOutputBytes > maxResultBytes {
		return invalid("policy fields are invalid")
	}
	sorted := slices.IsSortedFunc(p.Effects, func(a, b EffectV1) int {
		return strings.Compare(string(a), string(b))
	})
	seen := make(map[EffectV1]struct{}, len(p.Effects))
	for _, effect := range p.Effects {
		if !effect.valid() {
			return invalid("policy effect is invalid")
		}
		if _, duplicate := seen[effect]; duplicate {
			return invalid("policy effect is duplicated")
		}
		seen[effect] = struct{}{}
	}
	if !sorted {
		return invalid("policy effects are not canonical")
	}
	switch p.Network {
	case NetworkPolicyNone:
		if _, present := seen[EffectNetworkRead]; present {
			return invalid("network effect exceeds policy")
		}
	case NetworkPolicyPublicHTTPSReadOnly:
		if _, present := seen[EffectNetworkRead]; !present {
			return invalid("network policy lacks network effect")
		}
	default:
		return invalid("network policy is invalid")
	}
	switch p.Isolation {
	case IsolationInProcess:
	case IsolationRemoteHTTPS:
		if p.Network != NetworkPolicyPublicHTTPSReadOnly {
			return invalid("remote adapter lacks public HTTPS policy")
		}
	case IsolationFirecracker:
		if _, present := seen[EffectCodeExecution]; !present {
			return invalid("firecracker policy lacks code execution effect")
		}
	default:
		return invalid("isolation policy is invalid")
	}
	if p.ReadOnly {
		if _, present := seen[EffectStateWrite]; present {
			return invalid("read-only policy contains state write")
		}
		if _, present := seen[EffectDelivery]; present {
			return invalid("read-only policy contains delivery")
		}
	}
	return nil
}

func normalizePolicyV1(policy PolicyV1) (PolicyV1, error) {
	policy.Effects = slices.Clone(policy.Effects)
	if policy.Effects == nil {
		policy.Effects = []EffectV1{}
	}
	slices.SortFunc(policy.Effects, func(a, b EffectV1) int {
		return strings.Compare(string(a), string(b))
	})
	if err := policy.Validate(); err != nil {
		return PolicyV1{}, err
	}
	return policy, nil
}

func (r CredentialRefV1) validateFor(principal PrincipalV1) error {
	if r == (CredentialRefV1{}) {
		return nil
	}
	if !validVaultIdentifier(r.Provider) || !validVaultIdentifier(r.Purpose) ||
		r.Generation <= 0 || r.UserID < 0 || !validSHA256(r.Fingerprint) {
		return invalid("credential reference is invalid")
	}
	switch r.Scope {
	case CredentialScopePlatform, CredentialScopeTenant:
		if r.UserID != 0 {
			return invalid("shared credential contains a user")
		}
	case CredentialScopeUser:
		if r.UserID != principal.UserID {
			return invalid("user credential belongs to another principal")
		}
	default:
		return invalid("credential scope is invalid")
	}
	return nil
}

func (p PrincipalV1) validateAuthority() error {
	switch p.ActorType {
	case types.ActorTypeUser:
		if p.A2ATokenAuthorityID != "" || p.RequiredA2AScope != "" {
			return invalid("user principal contains service authority")
		}
	case types.ActorTypeServiceAccount:
		parsed, err := uuid.Parse(p.A2ATokenAuthorityID)
		if err != nil || parsed.String() != p.A2ATokenAuthorityID ||
			!p.RequiredA2AScope.Valid() {
			return invalid("service principal authority is invalid")
		}
	default:
		return invalid("principal actor type is invalid")
	}
	return nil
}

func (r CapabilityRefV1) validateScope(principal PrincipalV1) error {
	switch r.Scope {
	case CapabilityScopePlatform:
		if r.Kind != CapabilityKindBuiltinTool || r.OwnerUserID != 0 {
			return invalid("platform capability scope is invalid")
		}
	case CapabilityScopePersonal:
		if r.Kind == CapabilityKindBuiltinTool || r.OwnerUserID != principal.UserID {
			return invalid("personal capability belongs to another principal")
		}
	case CapabilityScopeWorkspace:
		if r.Kind == CapabilityKindBuiltinTool || r.OwnerUserID <= 0 {
			return invalid("workspace capability scope is invalid")
		}
	default:
		return invalid("capability scope is invalid")
	}
	return nil
}

func (r CapabilityRefV1) validatePolicy(policy PolicyV1) error {
	effects := make(map[EffectV1]struct{}, len(policy.Effects))
	for _, effect := range policy.Effects {
		effects[effect] = struct{}{}
	}
	switch r.Kind {
	case CapabilityKindBuiltinTool:
		if policy.Isolation != IsolationInProcess {
			return invalid("builtin capability isolation is invalid")
		}
	case CapabilityKindDeclarativeSkill:
		_, codeExecution := effects[EffectCodeExecution]
		if policy.Isolation != IsolationInProcess || codeExecution {
			return invalid("declarative skill policy is invalid")
		}
	case CapabilityKindRemoteMCP:
		_, stateWrite := effects[EffectStateWrite]
		_, delivery := effects[EffectDelivery]
		_, codeExecution := effects[EffectCodeExecution]
		if policy.Isolation != IsolationRemoteHTTPS ||
			policy.Network != NetworkPolicyPublicHTTPSReadOnly || !policy.ReadOnly ||
			stateWrite || delivery || codeExecution {
			return invalid("remote MCP policy is not read-only HTTPS")
		}
	case CapabilityKindSandboxScript:
		if policy.Isolation != IsolationFirecracker {
			return invalid("sandbox script is not firecracker-isolated")
		}
	default:
		return invalid("capability kind is invalid")
	}
	return nil
}

func (i InvocationV1) expectedIdempotencyDigest() (string, error) {
	// Role and membership generation are deliberately excluded: a live
	// membership change must make the frozen invocation payload conflict, not
	// create a second side-effect key.
	return digestJSON(struct {
		SchemaVersion       string          `json:"schema_version"`
		TenantID            types.TenantID  `json:"tenant_id"`
		UserID              int64           `json:"user_id"`
		ActorType           types.ActorType `json:"actor_type"`
		A2ATokenAuthorityID string          `json:"a2a_token_authority_id"`
		RequiredA2AScope    types.A2AScope  `json:"required_a2a_scope"`
		Capability          CapabilityRefV1 `json:"capability"`
		Operation           string          `json:"operation"`
		Key                 string          `json:"key"`
	}{"vane.capability-idempotency/v1", i.Principal.TenantID, i.Principal.UserID,
		i.Principal.ActorType, i.Principal.A2ATokenAuthorityID,
		i.Principal.RequiredA2AScope, i.Capability, i.Operation, i.IdempotencyKey})
}

func (i InvocationV1) expectedInvocationDigest() (string, error) {
	return digestJSON(struct {
		SchemaVersion     string          `json:"schema_version"`
		Principal         PrincipalV1     `json:"principal"`
		Capability        CapabilityRefV1 `json:"capability"`
		Operation         string          `json:"operation"`
		Policy            PolicyV1        `json:"policy"`
		PolicyDigest      string          `json:"policy_digest"`
		Credential        CredentialRefV1 `json:"credential"`
		Arguments         json.RawMessage `json:"arguments"`
		IdempotencyKey    string          `json:"idempotency_key"`
		IdempotencyDigest string          `json:"idempotency_digest"`
	}{i.SchemaVersion, i.Principal, i.Capability, i.Operation, i.Policy,
		i.PolicyDigest, i.Credential, i.Arguments, i.IdempotencyKey, i.IdempotencyDigest})
}

func canonicalJSONObject(raw json.RawMessage) (json.RawMessage, error) {
	if len(raw) == 0 || len(raw) > maxArgumentsBytes {
		return nil, invalid("arguments size is invalid")
	}
	var object map[string]any
	if err := strictjson.Decode(raw, &object); err != nil || object == nil {
		return nil, invalid("arguments must be a strict JSON object")
	}
	canonical, err := json.Marshal(object)
	if err != nil {
		return nil, invalid("arguments cannot be canonicalized")
	}
	return canonical, nil
}

func digestJSON(value any) (string, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func validSHA256(value string) bool {
	if len(value) != sha256.Size*2 || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func validIdentifier(value string) bool {
	if value == "" || len(value) > maxIdentifierBytes || !utf8.ValidString(value) ||
		strings.TrimSpace(value) != value {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) || unicode.In(r, unicode.Cf, unicode.Zl, unicode.Zp) {
			return false
		}
	}
	return true
}

func validMediaType(value string) bool {
	if value == "" || len(value) > maxMediaTypeBytes || strings.TrimSpace(value) != value {
		return false
	}
	_, _, err := mime.ParseMediaType(value)
	return err == nil
}

func validErrorClass(value string) bool {
	if value == "" || len(value) > maxIdentifierBytes || value[0] < 'a' || value[0] > 'z' {
		return false
	}
	for _, char := range value[1:] {
		if (char < 'a' || char > 'z') && (char < '0' || char > '9') &&
			char != '_' && char != '.' && char != '-' {
			return false
		}
	}
	return true
}

func validVaultIdentifier(value string) bool {
	if value == "" || len(value) > 64 || value[0] < 'a' || value[0] > 'z' {
		return false
	}
	for _, char := range value[1:] {
		if (char < 'a' || char > 'z') && (char < '0' || char > '9') &&
			char != '_' && char != '-' {
			return false
		}
	}
	return true
}

func invalid(message string) error { return fmt.Errorf("%w: %s", ErrInvalidContract, message) }
