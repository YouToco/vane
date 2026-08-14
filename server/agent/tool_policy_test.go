package agent

import "testing"

func TestToolPolicyZeroValueIsInvalid(t *testing.T) {
	if err := (ToolPolicy{}).validate(); err == nil {
		t.Fatal("zero policy must remain invalid")
	}
}

func TestDirectOwnerWritePolicyIsNarrow(t *testing.T) {
	valid := ownerPolicy(Effects(
		EffectStateWrite,
		EffectDirectOwnerWrite,
	), BudgetNone)
	if err := valid.validate(); err != nil {
		t.Fatalf("direct owner policy: %v", err)
	}

	invalid := ownerPolicy(Effects(EffectStateWrite), BudgetNone)
	if err := invalid.validate(); err == nil {
		t.Fatal("state write without direct-owner execution was accepted")
	}

	a2a := valid
	a2a.Authorization |= AuthorizationA2AReadOnly
	if err := a2a.validate(); err == nil {
		t.Fatal("direct owner write leaked into A2A authorization")
	}
}

func TestDurableProposalExecutesOnlyAsDirectOwnerWrite(t *testing.T) {
	policy := ownerPolicy(Effects(
		EffectStateWrite,
		EffectDurableProposal,
		EffectDirectOwnerWrite,
	), BudgetNone)
	if err := policy.validate(); err != nil {
		t.Fatalf("durable direct execution policy: %v", err)
	}

	policy.Effects &^= EffectSet(EffectDirectOwnerWrite)
	if err := policy.validate(); err == nil {
		t.Fatal("detached durable proposal was accepted")
	}
}

func TestPublicResearchCatalogDoesNotImplicitlyGrantA2A(t *testing.T) {
	public := BuildPublicResearchTools(
		NewEndpointTools(nil, nil, 1, 1),
		NewExaTools(nil, nil, nil, 1),
	)
	if len(public) != 4 {
		t.Fatalf("public research catalog=%d, want four owner tools", len(public))
	}
	filtered, err := FilterAuthorizedTools(public, AuthorizationA2AReadOnly)
	if err != nil {
		t.Fatal(err)
	}
	if len(filtered) != 0 {
		names := make([]string, 0, len(filtered))
		for _, spec := range filtered {
			names = append(names, spec.Name())
		}
		t.Fatalf("owner network/billable tools leaked into A2A: %v", names)
	}
}
