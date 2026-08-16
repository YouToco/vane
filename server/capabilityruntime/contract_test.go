package capabilityruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/YouToco/vane/server/types"
)

func TestInvocationV1CanonicalRoundTrip(t *testing.T) {
	t.Parallel()

	invocation := testInvocationV1(t)
	if got, want := string(invocation.Arguments), `{"a":"first","z":"last"}`; got != want {
		t.Fatalf("canonical arguments = %s, want %s", got, want)
	}
	encoded, err := EncodeInvocationV1(invocation)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeInvocationV1(encoded)
	if err != nil {
		t.Fatal(err)
	}
	reencoded, err := EncodeInvocationV1(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(reencoded, encoded) {
		t.Fatalf("canonical round trip drifted:\n got %s\nwant %s", reencoded, encoded)
	}
}

func TestInvocationV1RequiresExplicitTenantAndUser(t *testing.T) {
	t.Parallel()

	base := testInvocationInputV1()
	base.Principal.TenantID = 0
	if _, err := NewInvocationV1(base); !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("zero tenant error = %v, want ErrInvalidContract", err)
	}
	base = testInvocationInputV1()
	base.Principal.UserID = 0
	if _, err := NewInvocationV1(base); !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("zero user error = %v, want ErrInvalidContract", err)
	}
	base = testInvocationInputV1()
	base.Principal.MembershipAuthorizationGeneration = 0
	if _, err := NewInvocationV1(base); !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("zero membership generation error = %v, want ErrInvalidContract", err)
	}
}

func TestInvocationV1ServiceAccountRequiresExactA2AAuthority(t *testing.T) {
	t.Parallel()

	valid := testServiceInvocationInputV1()
	if _, err := NewInvocationV1(valid); err != nil {
		t.Fatalf("valid service authority error = %v", err)
	}
	userWithToken := valid
	userWithToken.Principal.ActorType = types.ActorTypeUser
	userWithToken.Principal.A2ATokenAuthorityID = valid.Principal.A2ATokenAuthorityID
	userWithToken.Principal.RequiredA2AScope = valid.Principal.RequiredA2AScope
	if _, err := NewInvocationV1(userWithToken); !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("user with service authority error = %v, want ErrInvalidContract", err)
	}
	cases := map[string]func(*InvocationInputV1){
		"missing token":      func(v *InvocationInputV1) { v.Principal.A2ATokenAuthorityID = "" },
		"nil token":          func(v *InvocationInputV1) { v.Principal.A2ATokenAuthorityID = "00000000-0000-0000-0000-000000000000" },
		"noncanonical token": func(v *InvocationInputV1) { v.Principal.A2ATokenAuthorityID = "11111111-1111-4111-8111-11111111111A" },
		"missing scope":      func(v *InvocationInputV1) { v.Principal.RequiredA2AScope = "" },
		"remote scope":       func(v *InvocationInputV1) { v.Principal.RequiredA2AScope = types.A2AScope("tools.write") },
		"content query cannot invoke tools": func(v *InvocationInputV1) {
			v.Principal.RequiredA2AScope = types.A2AScopeContentQuery
			v.Operation = "web_search"
		},
		"private capability": func(v *InvocationInputV1) {
			private := testInvocationInputV1()
			v.Capability, v.Policy = private.Capability, private.Policy
		},
	}
	for name, mutate := range cases {
		candidate := valid
		mutate(&candidate)
		if _, err := NewInvocationV1(candidate); !errors.Is(err, ErrInvalidContract) {
			t.Fatalf("%s: error = %v, want ErrInvalidContract", name, err)
		}
	}
	for _, kind := range []CapabilityKind{
		CapabilityKindDeclarativeSkill, CapabilityKindRemoteMCP, CapabilityKindSandboxScript,
	} {
		candidate := valid
		candidate.Capability.Kind = kind
		candidate.Capability.Scope = CapabilityScopeWorkspace
		candidate.Capability.OwnerUserID = 12
		candidate.Policy = testPolicyForKind(kind)
		if _, err := NewInvocationV1(candidate); !errors.Is(err, ErrInvalidContract) {
			t.Fatalf("service account private %s error = %v, want ErrInvalidContract", kind, err)
		}
	}

	service, err := NewInvocationV1(valid)
	if err != nil {
		t.Fatal(err)
	}
	rotated := valid
	rotated.Principal.A2ATokenAuthorityID = "22222222-2222-4222-8222-222222222222"
	changed, err := NewInvocationV1(rotated)
	if err != nil {
		t.Fatal(err)
	}
	if changed.IdempotencyDigest != service.IdempotencyDigest ||
		changed.InvocationDigest == service.InvocationDigest {
		t.Fatal("token rotation did not become a same-key invocation conflict")
	}
	for _, operation := range []string{"web_search", "recall_memory"} {
		candidate := valid
		candidate.Operation = operation
		if _, err := NewInvocationV1(candidate); err != nil {
			t.Fatalf("assistant.chat builtin operation %q error = %v", operation, err)
		}
	}
}

func TestInvocationV1StrictCodecRejectsRepresentationDrift(t *testing.T) {
	t.Parallel()

	encoded, err := EncodeInvocationV1(testInvocationV1(t))
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string][]byte{
		"unknown field": bytes.Replace(encoded,
			[]byte(`"operation":"tools/call"`),
			[]byte(`"unknown":true,"operation":"tools/call"`), 1),
		"duplicate field": bytes.Replace(encoded,
			[]byte(`"operation":"tools/call"`),
			[]byte(`"operation":"other","operation":"tools/call"`), 1),
		"duplicate argument": bytes.Replace(encoded,
			[]byte(`"arguments":{"a":"first","z":"last"}`),
			[]byte(`"arguments":{"a":"first","a":"second","z":"last"}`), 1),
		"noncanonical argument order": bytes.Replace(encoded,
			[]byte(`"arguments":{"a":"first","z":"last"}`),
			[]byte(`"arguments":{"z":"last","a":"first"}`), 1),
		"noncanonical whitespace": append([]byte(" "), encoded...),
		"trailing value":          append(bytes.Clone(encoded), []byte(`{}`)...),
		"missing required field": bytes.Replace(encoded,
			[]byte(`"operation":"tools/call",`), nil, 1),
	}
	for name, payload := range cases {
		name, payload := name, payload
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if _, err := DecodeInvocationV1(payload); !errors.Is(err, ErrInvalidContract) {
				t.Fatalf("DecodeInvocationV1() error = %v, want ErrInvalidContract", err)
			}
		})
	}
}

func TestInvocationV1AcceptsOnlyCanonicalInt64JSONNumbers(t *testing.T) {
	t.Parallel()

	for _, spelling := range []string{
		"0", "1", "-1", "9223372036854775807", "-9223372036854775808",
	} {
		input := testInvocationInputV1()
		input.Arguments = json.RawMessage(`{"value":` + spelling + `}`)
		invocation, err := NewInvocationV1(input)
		if err != nil {
			t.Fatalf("canonical integer %q error = %v", spelling, err)
		}
		if string(invocation.Arguments) != `{"value":`+spelling+`}` {
			t.Fatalf("canonical integer %q drifted to %s", spelling, invocation.Arguments)
		}
	}
	for _, spelling := range []string{
		"1.0", "1e0", "1E0", "-0", "2.5e-3", "01",
		"9223372036854775808", "-9223372036854775809",
	} {
		input := testInvocationInputV1()
		input.Arguments = json.RawMessage(`{"value":` + spelling + `}`)
		if _, err := NewInvocationV1(input); !errors.Is(err, ErrInvalidContract) {
			t.Fatalf("noncanonical integer %q error = %v, want ErrInvalidContract", spelling, err)
		}
	}
	input := testInvocationInputV1()
	input.Arguments = json.RawMessage(`{"nested":[0,{"value":-2}]}`)
	if _, err := NewInvocationV1(input); err != nil {
		t.Fatalf("nested canonical integers error = %v", err)
	}
	input.Arguments = json.RawMessage(`{"nested":[0,{"value":2.0}]}`)
	if _, err := NewInvocationV1(input); !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("nested noncanonical number error = %v, want ErrInvalidContract", err)
	}
}

func TestInvocationV1EveryAuthorityFieldIsDigestBound(t *testing.T) {
	t.Parallel()

	base := testInvocationV1(t)
	otherDigest := strings.Repeat("b", 64)
	mutations := map[string]func(*InvocationV1){
		"tenant":                func(v *InvocationV1) { v.Principal.TenantID++ },
		"user":                  func(v *InvocationV1) { v.Principal.UserID++ },
		"role":                  func(v *InvocationV1) { v.Principal.Role = types.MembershipRoleAdmin },
		"actor":                 func(v *InvocationV1) { v.Principal.ActorType = types.ActorTypeServiceAccount },
		"membership generation": func(v *InvocationV1) { v.Principal.MembershipAuthorizationGeneration++ },
		"kind":                  func(v *InvocationV1) { v.Capability.Kind = CapabilityKindDeclarativeSkill },
		"capability scope": func(v *InvocationV1) {
			v.Capability.Scope = CapabilityScopePersonal
			v.Capability.OwnerUserID = v.Principal.UserID
		},
		"capability owner":       func(v *InvocationV1) { v.Capability.OwnerUserID++ },
		"capability id":          func(v *InvocationV1) { v.Capability.ID = "workspace.skill.other" },
		"version id":             func(v *InvocationV1) { v.Capability.VersionID = "version-8" },
		"version digest":         func(v *InvocationV1) { v.Capability.VersionDigest = otherDigest },
		"schema digest":          func(v *InvocationV1) { v.Capability.OperationSchemaDigest = otherDigest },
		"operation":              func(v *InvocationV1) { v.Operation = "resources/read" },
		"policy":                 func(v *InvocationV1) { v.Policy.TimeoutMillis++ },
		"policy digest":          func(v *InvocationV1) { v.PolicyDigest = otherDigest },
		"credential provider":    func(v *InvocationV1) { v.Credential.Provider = "mcp_other" },
		"credential purpose":     func(v *InvocationV1) { v.Credential.Purpose = "connection_other" },
		"credential opaque ref":  func(v *InvocationV1) { v.Credential.OpaqueRef = "vault:mcp-other" },
		"credential ref digest":  func(v *InvocationV1) { v.Credential.OpaqueRefDigest = otherDigest },
		"credential scope":       func(v *InvocationV1) { v.Credential.Scope = CredentialScopeTenant; v.Credential.UserID = 0 },
		"credential user":        func(v *InvocationV1) { v.Credential.UserID++ },
		"credential gen":         func(v *InvocationV1) { v.Credential.Generation++ },
		"credential fingerprint": func(v *InvocationV1) { v.Credential.Fingerprint = otherDigest },
		"arguments":              func(v *InvocationV1) { v.Arguments = json.RawMessage(`{"a":"changed","z":"last"}`) },
		"idempotency key":        func(v *InvocationV1) { v.IdempotencyKey = "run-44/tool-8" },
		"idempotency sum":        func(v *InvocationV1) { v.IdempotencyDigest = otherDigest },
		"invocation sum":         func(v *InvocationV1) { v.InvocationDigest = otherDigest },
	}
	for name, mutate := range mutations {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			candidate := base
			candidate.Policy.Effects = append([]EffectV1(nil), base.Policy.Effects...)
			candidate.Arguments = bytes.Clone(base.Arguments)
			mutate(&candidate)
			if err := candidate.Validate(); !errors.Is(err, ErrInvalidContract) {
				t.Fatalf("mutated invocation validated: %v", err)
			}
		})
	}
}

func TestInvocationV1IdempotencyIdentitySeparatesScopeFromPayloadConflict(t *testing.T) {
	t.Parallel()

	base := testInvocationV1(t)
	identityChanges := map[string]func(*InvocationInputV1){
		"tenant":           func(v *InvocationInputV1) { v.Principal.TenantID++ },
		"user":             func(v *InvocationInputV1) { v.Principal.UserID++; v.Credential.UserID++ },
		"capability id":    func(v *InvocationInputV1) { v.Capability.ID = "workspace.mcp.other" },
		"capability owner": func(v *InvocationInputV1) { v.Capability.OwnerUserID++ },
		"capability kind": func(v *InvocationInputV1) {
			v.Capability.Kind = CapabilityKindSandboxScript
			v.Policy = testPolicyForKind(CapabilityKindSandboxScript)
		},
		"capability scope": func(v *InvocationInputV1) {
			v.Capability.Scope = CapabilityScopePersonal
			v.Capability.OwnerUserID = v.Principal.UserID
		},
		"operation": func(v *InvocationInputV1) { v.Operation = "resources/read" },
		"key":       func(v *InvocationInputV1) { v.IdempotencyKey = "run-44/tool-8" },
	}
	for name, mutate := range identityChanges {
		input := testInvocationInputV1()
		mutate(&input)
		changed, err := NewInvocationV1(input)
		if err != nil {
			t.Fatalf("%s: NewInvocationV1() error = %v", name, err)
		}
		if changed.IdempotencyDigest == base.IdempotencyDigest {
			t.Fatalf("%s did not change idempotency identity", name)
		}
	}

	payloadChanges := map[string]func(*InvocationInputV1){
		"version id":     func(v *InvocationInputV1) { v.Capability.VersionID = "version-8" },
		"version digest": func(v *InvocationInputV1) { v.Capability.VersionDigest = strings.Repeat("e", 64) },
		"schema digest":  func(v *InvocationInputV1) { v.Capability.OperationSchemaDigest = strings.Repeat("f", 64) },
		"arguments":      func(v *InvocationInputV1) { v.Arguments = json.RawMessage(`{"a":"changed"}`) },
		"policy":         func(v *InvocationInputV1) { v.Policy.TimeoutMillis++ },
		"credential rotation": func(v *InvocationInputV1) {
			v.Credential.Generation++
			v.Credential.Fingerprint = strings.Repeat("e", 64)
		},
		"credential ref": func(v *InvocationInputV1) {
			v.Credential.OpaqueRef = "vault:mcp-secondary"
			v.Credential.OpaqueRefDigest = rawSHA256([]byte(v.Credential.OpaqueRef))
		},
		"live role evidence":    func(v *InvocationInputV1) { v.Principal.Role = types.MembershipRoleAdmin },
		"membership generation": func(v *InvocationInputV1) { v.Principal.MembershipAuthorizationGeneration++ },
	}
	for name, mutate := range payloadChanges {
		input := testInvocationInputV1()
		mutate(&input)
		changed, err := NewInvocationV1(input)
		if err != nil {
			t.Fatalf("%s: NewInvocationV1() error = %v", name, err)
		}
		if changed.IdempotencyDigest != base.IdempotencyDigest {
			t.Fatalf("%s unexpectedly changed idempotency identity", name)
		}
		if changed.InvocationDigest == base.InvocationDigest {
			t.Fatalf("%s did not create an idempotency payload conflict", name)
		}
	}

	userBuiltin := testServiceInvocationInputV1()
	userBuiltin.Principal.ActorType = types.ActorTypeUser
	userBuiltin.Principal.A2ATokenAuthorityID = ""
	userBuiltin.Principal.RequiredA2AScope = ""
	userInvocation, err := NewInvocationV1(userBuiltin)
	if err != nil {
		t.Fatal(err)
	}
	serviceInvocation, err := NewInvocationV1(testServiceInvocationInputV1())
	if err != nil {
		t.Fatal(err)
	}
	if userInvocation.IdempotencyDigest != serviceInvocation.IdempotencyDigest ||
		userInvocation.InvocationDigest == serviceInvocation.InvocationDigest {
		t.Fatal("actor authority drift did not become a same-key invocation conflict")
	}
}

func TestPolicyV1RejectsEffectEscalationAndNoncanonicalValues(t *testing.T) {
	t.Parallel()

	base := testInvocationInputV1().Policy
	cases := map[string]func(*PolicyV1){
		"nil effects":      func(v *PolicyV1) { v.Effects = nil },
		"unknown effect":   func(v *PolicyV1) { v.Effects = []EffectV1{"root"} },
		"duplicate effect": func(v *PolicyV1) { v.Effects = []EffectV1{EffectBillable, EffectBillable} },
		"unsorted effects": func(v *PolicyV1) { v.Effects = []EffectV1{EffectNetworkRead, EffectBillable} },
		"read-only write":  func(v *PolicyV1) { v.Effects = []EffectV1{EffectStateWrite} },
		"network without grant": func(v *PolicyV1) {
			v.Effects = []EffectV1{EffectBillable}
			v.Network = NetworkPolicyPublicHTTPSReadOnly
		},
		"remote without network":   func(v *PolicyV1) { v.Effects = []EffectV1{}; v.Network = NetworkPolicyNone },
		"firecracker without code": func(v *PolicyV1) { v.Isolation = IsolationFirecracker },
	}
	for name, mutate := range cases {
		candidate := base
		candidate.Effects = append([]EffectV1(nil), base.Effects...)
		mutate(&candidate)
		if err := candidate.Validate(); !errors.Is(err, ErrInvalidContract) {
			t.Fatalf("%s: Validate() error = %v, want ErrInvalidContract", name, err)
		}
	}
}

func testInvocationV1(t *testing.T) InvocationV1 {
	t.Helper()
	invocation, err := NewInvocationV1(testInvocationInputV1())
	if err != nil {
		t.Fatal(err)
	}
	return invocation
}

func testInvocationInputV1() InvocationInputV1 {
	return InvocationInputV1{
		Principal: PrincipalV1{
			TenantID: 41, UserID: 73, Role: types.MembershipRoleMember,
			ActorType: types.ActorTypeUser, MembershipAuthorizationGeneration: 9,
		},
		Capability: CapabilityRefV1{
			Kind: CapabilityKindRemoteMCP, ID: "workspace.mcp.readonly",
			Scope: CapabilityScopeWorkspace, OwnerUserID: 12,
			VersionID: "version-7", VersionDigest: strings.Repeat("a", 64),
			OperationSchemaDigest: strings.Repeat("c", 64),
		},
		Operation: "tools/call",
		Policy: PolicyV1{
			SchemaVersion: PolicySchemaVersionV1,
			Effects:       []EffectV1{EffectNetworkRead, EffectBillable},
			ReadOnly:      true, Network: NetworkPolicyPublicHTTPSReadOnly,
			Isolation: IsolationRemoteHTTPS, TimeoutMillis: 5_000,
			MaxAttempts:   2,
			MaxInputBytes: 64 << 10, MaxOutputBytes: 256 << 10,
		},
		Credential: CredentialRefV1{
			OpaqueRef: "vault:mcp-primary", OpaqueRefDigest: rawSHA256([]byte("vault:mcp-primary")),
			Provider: "mcp", Purpose: "connection_primary",
			Scope: CredentialScopeUser, UserID: 73, Generation: 7,
			Fingerprint: strings.Repeat("d", 64),
		},
		Arguments:      json.RawMessage(` { "z": "last", "a": "first" } `),
		IdempotencyKey: "run-44/tool-7",
	}
}

func testServiceInvocationInputV1() InvocationInputV1 {
	input := testInvocationInputV1()
	input.Principal.ActorType = types.ActorTypeServiceAccount
	input.Principal.A2ATokenAuthorityID = "11111111-1111-4111-8111-111111111111"
	input.Principal.RequiredA2AScope = types.A2AScopeAssistantChat
	input.Capability = CapabilityRefV1{
		Kind: CapabilityKindBuiltinTool, Scope: CapabilityScopePlatform,
		ID: "agent.builtin.tools", VersionID: "version-1",
		VersionDigest:         strings.Repeat("a", 64),
		OperationSchemaDigest: strings.Repeat("c", 64),
	}
	input.Operation = "web_search"
	input.Policy = testPolicyForKind(CapabilityKindBuiltinTool)
	input.Credential = CredentialRefV1{}
	return input
}

// Compile-time proof that adapters share the one invocation/result boundary.
var _ Adapter = testAdapterV1{}

type testAdapterV1 struct{}

func (testAdapterV1) Kind() CapabilityKind { return CapabilityKindRemoteMCP }

func (testAdapterV1) Invoke(_ context.Context, invocation InvocationV1) (AdapterResultV1, error) {
	output := []byte(`{"ok":true}`)
	receipt, err := NewReceiptV1(
		invocation, ReceiptStatusSucceeded, 1, "application/json", output, "", false,
	)
	return AdapterResultV1{Receipt: receipt, SanitizedOutput: output}, err
}
