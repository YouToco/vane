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
