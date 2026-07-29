package agent

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/YouToco/vane/scheduler"
	"github.com/YouToco/vane/store"
)

type expectedToolPolicy struct {
	name          string
	effects       EffectSet
	auth          AuthorizationPolicy
	confirmation  ConfirmationPolicy
	budget        BudgetPolicy
	a2aAuthorized bool
}

func TestToolPolicyZeroValueIsInvalid(t *testing.T) {
	var policy ToolPolicy
	if err := policy.validate(); err == nil {
		t.Fatal("zero ToolPolicy must fail closed")
	}
	var set EffectSet
	if set.Has(EffectInternalRead) {
		t.Fatal("zero EffectSet must not grant effects")
	}
}

func TestProductionToolPolicyGolden(t *testing.T) {
	ep := NewEndpointTools(nil, nil, 10, 200, 128_000)
	exa := NewExaTools(nil, nil, nil, 5, 100)
	tools := BuildTools(
		&store.Store{}, &scheduler.Scheduler{}, nil, nil, ep, nil, exa,
		&fakeDefinitionEditController{},
	)

	want := []expectedToolPolicy{
		{
			name:         "list_sources",
			effects:      Effects(EffectInternalRead),
			auth:         AuthorizationOwner | AuthorizationA2AReadOnly,
			confirmation: ConfirmationNone, budget: BudgetNone,
			a2aAuthorized: true,
		},
		{
			name:    "add_source",
			effects: Effects(EffectNetworkRead, EffectBillable, EffectStateWrite, EffectTrustTaint, EffectDirectOwnerWrite),
			auth:    AuthorizationOwner, confirmation: ConfirmationNone,
			budget: BudgetDownstreamManaged,
		},
		{
			name:    "remove_source",
			effects: Effects(EffectStateWrite, EffectDirectOwnerWrite),
			auth:    AuthorizationOwner, confirmation: ConfirmationNone, budget: BudgetNone,
		},
		{
			name: "enable_source", effects: Effects(EffectStateWrite, EffectDirectOwnerWrite),
			auth: AuthorizationOwner, confirmation: ConfirmationNone, budget: BudgetNone,
		},
		{
			name: "list_schedules", effects: Effects(EffectInternalRead),
			auth:         AuthorizationOwner | AuthorizationA2AReadOnly,
			confirmation: ConfirmationNone, budget: BudgetNone, a2aAuthorized: true,
		},
		{
			name:    "create_schedule",
			effects: Effects(EffectDurableProposal, EffectStateWrite, EffectDirectOwnerWrite),
			auth:    AuthorizationOwner, confirmation: ConfirmationNone, budget: BudgetNone,
		},
		{
			name:    "remove_schedule",
			effects: Effects(EffectStateWrite, EffectDirectOwnerWrite),
			auth:    AuthorizationOwner, confirmation: ConfirmationNone, budget: BudgetNone,
		},
		{
			name: "push_now", effects: Effects(EffectDelivery),
			auth: AuthorizationOwner, confirmation: ConfirmationNone,
			budget: BudgetDownstreamManaged,
		},
		{
			name: "view_profile", effects: Effects(EffectInternalRead),
			auth: AuthorizationOwner, confirmation: ConfirmationNone, budget: BudgetNone,
		},
		{
			name: "update_profile", effects: Effects(EffectStateWrite, EffectDirectOwnerWrite),
			auth: AuthorizationOwner, confirmation: ConfirmationNone, budget: BudgetNone,
		},
		{
			name: "view_task_playbook", effects: Effects(EffectInternalRead),
			auth: AuthorizationOwner, confirmation: ConfirmationNone, budget: BudgetNone,
		},
		{
			name:    "edit_task_definition",
			effects: Effects(EffectDurableProposal, EffectStateWrite, EffectDirectOwnerWrite),
			auth:    AuthorizationOwner, confirmation: ConfirmationNone, budget: BudgetNone,
		},
		{
			name:    "search_endpoints",
			effects: Effects(EffectNetworkRead, EffectBillable, EffectActivationWrite),
			auth:    AuthorizationOwner, confirmation: ConfirmationNone, budget: BudgetToolManaged,
		},
		{
			name:    "read_endpoint_result",
			effects: Effects(EffectLocalHandleRead, EffectTrustTaint),
			auth:    AuthorizationOwner, confirmation: ConfirmationNone, budget: BudgetNone,
		},
		{
			name:    "web_search",
			effects: Effects(EffectNetworkRead, EffectBillable, EffectTrustTaint),
			auth:    AuthorizationOwner, confirmation: ConfirmationNone, budget: BudgetToolManaged,
		},
		{
			name:    "read_page",
			effects: Effects(EffectNetworkRead, EffectBillable, EffectTrustTaint),
			auth:    AuthorizationOwner, confirmation: ConfirmationNone, budget: BudgetToolManaged,
		},
	}

	byName := make(map[string]ToolSpec, len(tools))
	for _, spec := range tools {
		if _, exists := byName[spec.Name()]; exists {
			t.Fatalf("duplicate production tool %q", spec.Name())
		}
		byName[spec.Name()] = spec
		if err := spec.validate(); err != nil {
			t.Fatalf("production tool %q invalid: %v", spec.Name(), err)
		}
		modelContract := spec.Definition.Description + string(spec.Definition.Parameters)
		for _, forbidden := range []string{
			"待用户确认",
			"先发原确认卡",
			"点击确认",
			"确认后由",
		} {
			if strings.Contains(modelContract, forbidden) {
				t.Fatalf("production tool %q leaks retired confirmation UX %q", spec.Name(), forbidden)
			}
		}
	}
	if len(byName) != len(want) {
		t.Fatalf("BuildTools registered %d tools, golden expects %d; update the matrix when adding tools",
			len(byName), len(want))
	}

	for _, tc := range want {
		spec, ok := byName[tc.name]
		if !ok {
			t.Fatalf("missing production tool %q", tc.name)
		}
		p := spec.Policy
		if p.Effects != tc.effects {
			t.Fatalf("%s effects = %#x, want %#x", tc.name, p.Effects, tc.effects)
		}
		if p.Authorization != tc.auth {
			t.Fatalf("%s authorization = %#x, want %#x", tc.name, p.Authorization, tc.auth)
		}
		if p.Confirmation != tc.confirmation {
			t.Fatalf("%s confirmation = %d, want %d", tc.name, p.Confirmation, tc.confirmation)
		}
		if p.Budget != tc.budget {
			t.Fatalf("%s budget = %d, want %d", tc.name, p.Budget, tc.budget)
		}
		if p.Retry != RetryNone || p.Concurrency != ConcurrencySequential {
			t.Fatalf("%s retry/concurrency drifted: %+v", tc.name, p)
		}
		if got := p.Authorization.Allows(AuthorizationA2AReadOnly); got != tc.a2aAuthorized {
			t.Fatalf("%s a2a authorized = %v, want %v", tc.name, got, tc.a2aAuthorized)
		}
	}
}

func TestFilterAuthorizedTools_A2AExactWhitelist(t *testing.T) {
	ep := NewEndpointTools(nil, nil, 10, 200, 128_000)
	exa := NewExaTools(nil, nil, nil, 5, 100)
	tools := BuildTools(
		&store.Store{}, &scheduler.Scheduler{}, nil, nil, ep, nil, exa,
		&fakeDefinitionEditController{},
	)
	filtered, err := FilterAuthorizedTools(tools, AuthorizationA2AReadOnly)
	if err != nil {
		t.Fatal(err)
	}
	if len(filtered) != 2 {
		t.Fatalf("A2A surface = %d tools, want exact 2", len(filtered))
	}
	names := map[string]bool{}
	for _, spec := range filtered {
		names[spec.Name()] = true
		if spec.Policy.Confirmation != ConfirmationNone {
			t.Fatalf("A2A tool %s unexpectedly requires confirmation", spec.Name())
		}
		if spec.Policy.Effects.Has(EffectDelivery) ||
			spec.Policy.Effects.Has(EffectStateWrite) ||
			spec.Policy.Effects.Has(EffectNetworkRead) {
			t.Fatalf("A2A tool %s has non-readonly effects %#x", spec.Name(), spec.Policy.Effects)
		}
	}
	if !names["list_sources"] || !names["list_schedules"] {
		t.Fatalf("A2A surface drifted: %v", names)
	}
}

func TestPushNowRemainsInlineDespiteDeliveryEffect(t *testing.T) {
	spec := newToolSpec(&pushNowTool{}, ownerPolicy(
		Effects(EffectDelivery), ConfirmationNone, BudgetDownstreamManaged))
	if err := spec.validate(); err != nil {
		t.Fatal(err)
	}
	if spec.Policy.Confirmation != ConfirmationNone {
		t.Fatal("push_now must stay confirmation-none to preserve current UX")
	}
	if !spec.Policy.Effects.Has(EffectDelivery) {
		t.Fatal("push_now must declare delivery effect")
	}
}

func TestDirectOwnerWritePolicyIsNarrow(t *testing.T) {
	valid := ownerPolicy(
		Effects(EffectStateWrite, EffectDirectOwnerWrite),
		ConfirmationNone,
		BudgetNone,
	)
	if err := valid.validate(); err != nil {
		t.Fatalf("valid direct owner write rejected: %v", err)
	}

	tests := []ToolPolicy{
		ownerPolicy(Effects(EffectDirectOwnerWrite), ConfirmationNone, BudgetNone),
		ownerPolicy(
			Effects(EffectStateWrite, EffectDirectOwnerWrite),
			ConfirmationRequired,
			BudgetNone,
		),
	}
	a2a := valid
	a2a.Authorization |= AuthorizationA2AReadOnly
	tests = append(tests, a2a)
	for i, policy := range tests {
		if err := policy.validate(); err == nil {
			t.Fatalf("invalid direct owner policy %d accepted: %+v", i, policy)
		}
	}
}

func TestNewCheckedRejectsInvalidAndDuplicateTools(t *testing.T) {
	valid := newToolSpec(&listSchedulesTool{}, a2aReadPolicy(Effects(EffectInternalRead)))
	if _, err := NewChecked(Deps{Tools: []ToolSpec{valid, valid}}); err == nil {
		t.Fatal("duplicate tool names must fail closed")
	}

	bad := ToolSpec{
		Tool:       &listSchedulesTool{},
		Definition: valid.Definition,
		Policy:     ToolPolicy{}, // zero policy
	}
	if _, err := NewChecked(Deps{Tools: []ToolSpec{bad}}); err == nil {
		t.Fatal("invalid zero policy must fail closed")
	}

	badSchema := newToolSpec(&listSchedulesTool{}, a2aReadPolicy(Effects(EffectInternalRead)))
	badSchema.Definition.Parameters = json.RawMessage(`{"type":"string"}`)
	if _, err := NewChecked(Deps{Tools: []ToolSpec{badSchema}}); err == nil {
		t.Fatal("invalid schema must fail closed")
	}
}

func TestDynamicEndpointPolicyMatchesNetworkBudgetSurface(t *testing.T) {
	const name = "xiaohongshu_app_v2_search_notes"
	ep := NewEndpointTools(nil, nil, 10, 200, 128_000)
	act := &activationState{}
	act.activate(name)
	spec, ok := ep.Resolve(name, act)
	if !ok {
		t.Fatalf("Resolve(%q) failed; catalog entry required for policy golden", name)
	}
	if err := spec.validate(); err != nil {
		t.Fatal(err)
	}
	want := Effects(EffectNetworkRead, EffectBillable, EffectTrustTaint)
	if spec.Policy.Effects != want {
		t.Fatalf("dynamic endpoint effects = %#x, want %#x", spec.Policy.Effects, want)
	}
	if spec.Policy.Confirmation != ConfirmationNone ||
		spec.Policy.Budget != BudgetToolManaged {
		t.Fatalf("dynamic endpoint confirmation/budget drifted: %+v", spec.Policy)
	}
}
