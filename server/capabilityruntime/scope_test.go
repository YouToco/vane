package capabilityruntime

import (
	"errors"
	"strings"
	"testing"
)

func TestCapabilityRefV1ScopeMatrix(t *testing.T) {
	t.Parallel()

	base := testInvocationInputV1()
	cases := []struct {
		name    string
		kind    CapabilityKind
		scope   CapabilityScopeV1
		owner   int64
		wantErr bool
	}{
		{"builtin platform", CapabilityKindBuiltinTool, CapabilityScopePlatform, 0, false},
		{"personal skill", CapabilityKindDeclarativeSkill, CapabilityScopePersonal, 73, false},
		{"workspace mcp", CapabilityKindRemoteMCP, CapabilityScopeWorkspace, 12, false},
		{"workspace script", CapabilityKindSandboxScript, CapabilityScopeWorkspace, 12, false},
		{"builtin workspace", CapabilityKindBuiltinTool, CapabilityScopeWorkspace, 12, true},
		{"user platform", CapabilityKindDeclarativeSkill, CapabilityScopePlatform, 0, true},
		{"foreign personal", CapabilityKindDeclarativeSkill, CapabilityScopePersonal, 74, true},
		{"ownerless workspace", CapabilityKindRemoteMCP, CapabilityScopeWorkspace, 0, true},
		{"unknown scope", CapabilityKindRemoteMCP, CapabilityScopeV1("foreign"), 12, true},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			input := base
			input.Capability.Kind, input.Capability.Scope = tt.kind, tt.scope
			input.Capability.OwnerUserID = tt.owner
			input.Policy = testPolicyForKind(tt.kind)
			_, err := NewInvocationV1(input)
			if tt.wantErr != errors.Is(err, ErrInvalidContract) {
				t.Fatalf("NewInvocationV1() error = %v, wantErr=%v", err, tt.wantErr)
			}
		})
	}
}

func testPolicyForKind(kind CapabilityKind) PolicyV1 {
	policy := testInvocationInputV1().Policy
	switch kind {
	case CapabilityKindBuiltinTool:
		policy.Effects = []EffectV1{}
		policy.Network = NetworkPolicyNone
		policy.Isolation = IsolationInProcess
	case CapabilityKindDeclarativeSkill:
		policy.Effects = []EffectV1{EffectInternalRead}
		policy.ReadOnly = true
		policy.Network = NetworkPolicyNone
		policy.Isolation = IsolationInProcess
	case CapabilityKindSandboxScript:
		policy.Effects = []EffectV1{EffectCodeExecution}
		policy.Network = NetworkPolicyNone
		policy.Isolation = IsolationFirecracker
	}
	return policy
}

func TestCredentialRefV1ScopeMatrix(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		credential CredentialRefV1
		wantErr    bool
	}{
		{"credentialless", CredentialRefV1{}, false},
		{"platform", testCredentialV1(CredentialScopePlatform, 0, 1), false},
		{"tenant", testCredentialV1(CredentialScopeTenant, 0, 2), false},
		{"current user", testCredentialV1(CredentialScopeUser, 73, 3), false},
		{"foreign user", testCredentialV1(CredentialScopeUser, 74, 3), true},
		{"user on shared", testCredentialV1(CredentialScopeTenant, 73, 2), true},
		{"latest generation", testCredentialV1(CredentialScopeUser, 73, 0), true},
		{"missing fingerprint", func() CredentialRefV1 {
			v := testCredentialV1(CredentialScopeUser, 73, 1)
			v.Fingerprint = ""
			return v
		}(), true},
		{"uncontrolled provider", func() CredentialRefV1 {
			v := testCredentialV1(CredentialScopeUser, 73, 1)
			v.Provider = "MCP"
			return v
		}(), true},
		{"partial zero", CredentialRefV1{OpaqueRef: "vault:mcp-primary"}, true},
		{"opaque digest mismatch", func() CredentialRefV1 {
			v := testCredentialV1(CredentialScopeUser, 73, 1)
			v.OpaqueRefDigest = strings.Repeat("a", 64)
			return v
		}(), true},
		{"unknown", testCredentialV1(CredentialScopeV1("foreign"), 0, 1), true},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			input := testInvocationInputV1()
			input.Credential = tt.credential
			_, err := NewInvocationV1(input)
			if tt.wantErr != errors.Is(err, ErrInvalidContract) {
				t.Fatalf("NewInvocationV1() error = %v, wantErr=%v", err, tt.wantErr)
			}
		})
	}
}

func testCredentialV1(scope CredentialScopeV1, userID, generation int64) CredentialRefV1 {
	return CredentialRefV1{
		OpaqueRef: "vault:mcp-primary", OpaqueRefDigest: rawSHA256([]byte("vault:mcp-primary")),
		Provider: "mcp", Purpose: "connection_primary", Scope: scope,
		UserID: userID, Generation: generation, Fingerprint: strings.Repeat("d", 64),
	}
}

func TestCredentialRefV1TenantScopeUsesExplicitPrincipalTenant(t *testing.T) {
	t.Parallel()

	firstInput := testInvocationInputV1()
	firstInput.Credential = testCredentialV1(CredentialScopeTenant, 0, 2)
	first, err := NewInvocationV1(firstInput)
	if err != nil {
		t.Fatal(err)
	}
	secondInput := firstInput
	secondInput.Principal.TenantID++
	second, err := NewInvocationV1(secondInput)
	if err != nil {
		t.Fatal(err)
	}
	if first.Credential != second.Credential {
		t.Fatal("tenant credential metadata unexpectedly inferred a tenant")
	}
	if first.IdempotencyDigest == second.IdempotencyDigest ||
		first.InvocationDigest == second.InvocationDigest {
		t.Fatal("explicit Principal tenant is not authority-bound")
	}
}

func TestCapabilityRefV1RejectsPolicyKindMismatch(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		kind   CapabilityKind
		mutate func(*PolicyV1)
	}{
		{"builtin outside process", CapabilityKindBuiltinTool, func(p *PolicyV1) { p.Isolation = IsolationFirecracker; p.Effects = []EffectV1{EffectCodeExecution} }},
		{"declarative skill executes code", CapabilityKindDeclarativeSkill, func(p *PolicyV1) { p.Isolation = IsolationFirecracker; p.Effects = []EffectV1{EffectCodeExecution} }},
		{"declarative skill network", CapabilityKindDeclarativeSkill, func(p *PolicyV1) {
			p.Network = NetworkPolicyPublicHTTPSReadOnly
			p.Effects = []EffectV1{EffectInternalRead, EffectNetworkRead}
		}},
		{"declarative skill writable", CapabilityKindDeclarativeSkill, func(p *PolicyV1) { p.ReadOnly = false }},
		{"declarative skill empty effects", CapabilityKindDeclarativeSkill, func(p *PolicyV1) { p.Effects = []EffectV1{} }},
		{"declarative skill billable", CapabilityKindDeclarativeSkill, func(p *PolicyV1) { p.Effects = []EffectV1{EffectBillable, EffectInternalRead} }},
		{"declarative skill state write", CapabilityKindDeclarativeSkill, func(p *PolicyV1) { p.Effects = []EffectV1{EffectInternalRead, EffectStateWrite} }},
		{"declarative skill delivery", CapabilityKindDeclarativeSkill, func(p *PolicyV1) { p.Effects = []EffectV1{EffectDelivery, EffectInternalRead} }},
		{"remote mcp not read-only", CapabilityKindRemoteMCP, func(p *PolicyV1) { p.ReadOnly = false }},
		{"script outside microvm", CapabilityKindSandboxScript, func(p *PolicyV1) { p.Isolation = IsolationInProcess }},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			input := testInvocationInputV1()
			input.Capability.Kind = tt.kind
			if tt.kind == CapabilityKindBuiltinTool {
				input.Capability.Scope, input.Capability.OwnerUserID = CapabilityScopePlatform, 0
			}
			input.Policy = testPolicyForKind(tt.kind)
			tt.mutate(&input.Policy)
			if _, err := NewInvocationV1(input); !errors.Is(err, ErrInvalidContract) {
				t.Fatalf("NewInvocationV1() error = %v, want ErrInvalidContract", err)
			}
		})
	}
}
