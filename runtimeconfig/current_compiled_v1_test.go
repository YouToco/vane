package runtimeconfig

import (
	"errors"
	"reflect"
	"testing"

	"github.com/YouToco/vane/cardgen"
	"github.com/YouToco/vane/evolver"
	"github.com/YouToco/vane/runtimepolicy"
	"github.com/YouToco/vane/scorer"
	"github.com/YouToco/vane/sourcecatalog"
	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
)

func currentCompiledInput(model string) CurrentCompiledV1Input {
	return CurrentCompiledV1Input{
		Model: model, ModelEndpointGeneration: 1, ModelCredentialGeneration: 1,
		ExaCredentialGeneration: 2, TikHubCredentialGeneration: 4,
	}
}

func TestBuildCurrentCompiledV1ReconstructsCurrentExecution(t *testing.T) {
	input := currentCompiledInput("deepseek-v4-flash")
	input.TaskInstructionEnabled = true
	bundle, err := BuildCurrentCompiledV1(input)
	if err != nil {
		t.Fatalf("BuildCurrentCompiledV1() error = %v", err)
	}
	if err := bundle.Validate(); err != nil {
		t.Fatalf("BuildCurrentCompiledV1() returned invalid bundle: %v", err)
	}

	if bundle.PromptPolicy.Score != scorer.CurrentPromptStageV1() ||
		bundle.PromptPolicy.CardGen != cardgen.CurrentPromptStageV1() ||
		bundle.PromptPolicy.ProfileEvolve != evolver.CurrentPromptStageV1() ||
		!bundle.PromptPolicy.TaskInstructionEnabled {
		t.Fatalf("prompt policy does not match current execution: %+v", bundle.PromptPolicy)
	}

	wantCalls := map[string]runtimepolicy.ModelCallV1{
		runtimepolicy.ModelStageScore:         scorer.CurrentModelCallV1("deepseek-v4-flash"),
		runtimepolicy.ModelStageCardGen:       cardgen.CurrentModelCallV1("deepseek-v4-flash"),
		runtimepolicy.ModelStageProfileEvolve: evolver.CurrentModelCallV1("deepseek-v4-flash"),
	}
	for stage, want := range wantCalls {
		got, ok := bundle.ModelPolicy.Call(stage)
		if !ok || got != want {
			t.Errorf("model call %q = %+v, %v; want %+v", stage, got, ok, want)
		}
	}

	if _, err := scorer.PreparePolicyV1(bundle.PromptPolicy, bundle.ModelPolicy); err != nil {
		t.Errorf("scorer rejected current bundle: %v", err)
	}
	if _, err := cardgen.PreparePolicyV1(bundle.PromptPolicy, bundle.ModelPolicy); err != nil {
		t.Errorf("cardgen rejected current bundle: %v", err)
	}
	if _, err := evolver.PreparePolicyV1(bundle.PromptPolicy, bundle.ModelPolicy); err != nil {
		t.Errorf("evolver rejected current bundle: %v", err)
	}
}

func TestBuildStructuredInsightCompiledV1OnlyChangesCardGenPolicy(t *testing.T) {
	input := currentCompiledInput("model-v2")
	legacy, err := BuildCurrentCompiledV1(input)
	if err != nil {
		t.Fatal(err)
	}
	structured, err := BuildStructuredInsightCompiledV1(input)
	if err != nil {
		t.Fatal(err)
	}
	if structured.PromptPolicy.CardGen != cardgen.StructuredPromptStageV2() {
		t.Fatalf("structured card prompt = %+v", structured.PromptPolicy.CardGen)
	}
	call, ok := structured.ModelPolicy.Call(runtimepolicy.ModelStageCardGen)
	if !ok || call != cardgen.StructuredModelCallV2("model-v2") {
		t.Fatalf("structured card call = %+v, ok=%v", call, ok)
	}
	legacy.PromptPolicy.CardGen = structured.PromptPolicy.CardGen
	for index := range legacy.ModelPolicy.Calls {
		if legacy.ModelPolicy.Calls[index].Stage == runtimepolicy.ModelStageCardGen {
			legacy.ModelPolicy.Calls[index] = call
		}
	}
	if !reflect.DeepEqual(legacy, structured) {
		t.Fatal("structured runtime changed policy outside CardGen")
	}
}

func TestBuildCurrentCompiledV1IncludesEveryAvailableCapability(t *testing.T) {
	bundle, err := BuildCurrentCompiledV1(currentCompiledInput("model-v1"))
	if err != nil {
		t.Fatalf("BuildCurrentCompiledV1() error = %v", err)
	}

	got := make(map[string]runtimepolicy.CapabilityV1, len(bundle.CapabilityCatalog.Allowed))
	for _, capability := range bundle.CapabilityCatalog.Allowed {
		got[capability.Platform+"/"+capability.Capability] = capability
	}
	available := 0
	for _, entry := range sourcecatalog.List() {
		key := string(entry.Platform) + "/" + string(entry.Capability)
		capability, found := got[key]
		if !entry.Available() {
			if found {
				t.Errorf("unavailable capability %s entered compiled allowlist", key)
			}
			continue
		}
		available++
		if !found {
			t.Errorf("available capability %s is missing", key)
			continue
		}
		assertCurrentCapabilityV1(t, entry, capability)
	}
	if len(got) != available {
		t.Fatalf("compiled capability count = %d, available catalog count = %d", len(got), available)
	}
}

func TestBuildCurrentCompiledV1OnlyFreezesCurrentlyEnforcedQuota(t *testing.T) {
	input := currentCompiledInput("model-v1")
	bundle, err := BuildCurrentCompiledV1(input)
	if err != nil {
		t.Fatalf("BuildCurrentCompiledV1() error = %v", err)
	}
	if len(bundle.QuotaPolicy.Buckets) != 1 {
		t.Fatalf("quota buckets = %+v, want only llm_tokens", bundle.QuotaPolicy.Buckets)
	}
	got := bundle.QuotaPolicy.Buckets[0]
	if got.Name != string(store.QuotaLLMTokens) || !got.Financial ||
		got.EnforcementVersion != runtimepolicy.QuotaEnforcementLLMPrechargeV1 {
		t.Fatalf("llm quota identity = %+v", got)
	}
	for _, forbidden := range []string{
		string(store.QuotaExaCalls),
		string(store.QuotaTikHubCalls),
		string(store.QuotaPush),
		string(store.QuotaFetch),
	} {
		if got.Name == forbidden {
			t.Fatalf("unenforced quota %q entered the runtime policy", forbidden)
		}
	}
}

func TestBuildCurrentCompiledV1RejectsEmptyModel(t *testing.T) {
	_, err := BuildCurrentCompiledV1(currentCompiledInput(""))
	if !errors.Is(err, runtimepolicy.ErrInvalidPolicy) {
		t.Fatalf("BuildCurrentCompiledV1() error = %v, want ErrInvalidPolicy", err)
	}
}

func TestBuildCurrentCompiledV1FreezesTaskInstructionDecision(t *testing.T) {
	disabledInput := currentCompiledInput("model-v1")
	disabled, err := BuildCurrentCompiledV1(disabledInput)
	if err != nil {
		t.Fatal(err)
	}
	enabledInput := currentCompiledInput("model-v1")
	enabledInput.TaskInstructionEnabled = true
	enabled, err := BuildCurrentCompiledV1(enabledInput)
	if err != nil {
		t.Fatal(err)
	}
	if disabled.PromptPolicy.TaskInstructionEnabled ||
		!enabled.PromptPolicy.TaskInstructionEnabled {
		t.Fatal("task-instruction rollout decision was not frozen")
	}
}

func assertCurrentCapabilityV1(
	t *testing.T,
	entry sourcecatalog.Entry,
	capability runtimepolicy.CapabilityV1,
) {
	t.Helper()
	if capability.Kind != string(entry.Kind) {
		t.Errorf("%s/%s kind = %q, want %q",
			entry.Platform, entry.Capability, capability.Kind, entry.Kind)
	}
	switch {
	case entry.Platform == types.PlatformWeb && entry.Capability == types.CapFeed:
		wantDependency := []runtimepolicy.CredentialRefV1{{
			ID: runtimepolicy.CredentialIDExaPrimaryV1, Generation: 2,
		}}
		if capability.ImplementationVersion != runtimepolicy.CapabilityImplementationRSSV1 ||
			capability.CredentialRef != (runtimepolicy.CredentialRefV1{}) ||
			!reflect.DeepEqual(capability.DependencyCredentialRefs, wantDependency) {
			t.Errorf("web/feed binding = %+v", capability)
		}
	case entry.Platform == types.PlatformWeb:
		wantRef := runtimepolicy.CredentialRefV1{
			ID:         runtimepolicy.CredentialIDExaPrimaryV1,
			Generation: 2,
		}
		if capability.ImplementationVersion != runtimepolicy.CapabilityImplementationExaV1 ||
			capability.CredentialRef != wantRef {
			t.Errorf("web capability binding = %+v", capability)
		}
	default:
		wantRef := runtimepolicy.CredentialRefV1{
			ID:         runtimepolicy.CredentialIDTikHubPrimaryV1,
			Generation: 4,
		}
		if capability.ImplementationVersion != runtimepolicy.CapabilityImplementationBindingV1 ||
			capability.CredentialRef != wantRef {
			t.Errorf("binding-backed capability = %+v", capability)
		}
	}
}
