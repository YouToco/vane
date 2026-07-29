package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"go.temporal.io/sdk/testsuite"

	"github.com/YouToco/vane/observation"
	"github.com/YouToco/vane/runcontext"
	"github.com/YouToco/vane/runtimepolicy"
	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

type compiledToolRunStoreFake struct {
	mu sync.Mutex

	ref       types.RunSnapshotRefV2
	found     bool
	snapshot  runcontext.CompiledSnapshotV2
	authorize bool

	loadRefErr   error
	createErr    error
	loadErr      error
	authorizeErr error

	loadRefCalls   int
	createCalls    int
	loadCalls      int
	authorizeCalls int
	createPolicies []runtimepolicy.BundleV1
}

func (f *compiledToolRunStoreFake) LoadCompiledRunSnapshotRefV2(
	_ context.Context,
	_ types.RunIdentity,
) (types.RunSnapshotRefV2, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.loadRefCalls++
	return f.ref, f.found, f.loadRefErr
}

func (f *compiledToolRunStoreFake) CreateOrGetCompiledRunSnapshotV2(
	_ context.Context,
	_ types.RunIdentity,
	policy runtimepolicy.BundleV1,
	_ ...observation.RolloutMode,
) (types.RunSnapshotRefV2, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createCalls++
	f.createPolicies = append(f.createPolicies, policy)
	if f.createErr != nil {
		return types.RunSnapshotRefV2{}, f.createErr
	}
	f.found = true
	return f.ref, nil
}

func (f *compiledToolRunStoreFake) LoadCompiledTaskRunSnapshotV2(
	_ context.Context,
	_ types.RunIdentity,
	_ types.RunSnapshotRefV2,
) (runcontext.CompiledSnapshotV2, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.loadCalls++
	return f.snapshot, f.loadErr
}

func (f *compiledToolRunStoreFake) AuthorizeTaskRunSideEffectV2(
	_ context.Context,
	_ types.RunIdentity,
	_ types.RunSnapshotRefV2,
) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.authorizeCalls++
	return f.authorize, f.authorizeErr
}

func executePrepareToolRunV2(
	t *testing.T,
	env *testsuite.TestActivityEnvironment,
	a *Activities,
	p PushParams,
) (PrepareToolRunV2Result, error) {
	t.Helper()
	encoded, err := env.ExecuteActivity(a.PrepareToolRunV2, p)
	if err != nil {
		return PrepareToolRunV2Result{}, err
	}
	var result PrepareToolRunV2Result
	if err := encoded.Get(&result); err != nil {
		t.Fatalf("decode PrepareToolRunV2 result: %v", err)
	}
	return result, nil
}

func validToolRuntimePolicyV2(t *testing.T) runtimepolicy.BundleV1 {
	t.Helper()
	policy, err := runtimepolicy.BuildV1(runtimepolicy.BuildInputV1{
		AllowedCapabilities: []runtimepolicy.CapabilityV1{{
			Platform: "web", Capability: "search", Kind: "article",
			ImplementationVersion: runtimepolicy.CapabilityImplementationExaV1,
			CredentialRef: runtimepolicy.CredentialRefV1{
				ID: runtimepolicy.CredentialIDExaPrimaryV1, Generation: 1,
			},
		}},
		ScorePrompt: runtimepolicy.PromptStageV1{
			SystemPrompt: "score", RendererVersion: "scorer.render/v1",
		},
		CardGenPrompt: runtimepolicy.PromptStageV1{
			SystemPrompt: "card", RendererVersion: "cardgen.render/v1",
		},
		ProfileEvolvePrompt: runtimepolicy.PromptStageV1{
			SystemPrompt: "profile", RendererVersion: "evolver.render/v1",
		},
		TaskInstructionEnabled: true,
		ModelProvider:          runtimepolicy.ModelProviderDeepSeekV1,
		ModelEndpoint: runtimepolicy.EndpointRefV1{
			ID:         runtimepolicy.EndpointIDDeepSeekCompatiblePrimaryV1,
			Generation: 1,
		},
		ModelCredentialRef: runtimepolicy.CredentialRefV1{
			ID: runtimepolicy.CredentialIDLLMPrimaryV1, Generation: 1,
		},
		ModelCalls: []runtimepolicy.ModelCallV1{
			{
				Stage: runtimepolicy.ModelStageScore, Model: "model",
				MaxTokens: 16, DisableThinking: true,
			},
			{
				Stage: runtimepolicy.ModelStageCardGen, Model: "model",
				MaxTokens: 64, DisableThinking: true,
			},
			{
				Stage: runtimepolicy.ModelStageProfileEvolve, Model: "model",
				MaxTokens: 64, DisableThinking: true,
			},
		},
		QuotaBuckets: []runtimepolicy.QuotaBucketV1{{
			Name: "llm_tokens", Financial: true,
			EnforcementVersion: runtimepolicy.QuotaEnforcementLLMPrechargeV1,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return policy
}

func compiledToolActivityFixtureV2(
	t *testing.T,
	identity types.RunIdentity,
) (types.RunSnapshotRefV2, runcontext.CompiledSnapshotV2) {
	t.Helper()
	call, err := taskstate.BuildToolInvocationV1(
		"web_search", "v1", json.RawMessage(`{"query":"AI"}`))
	if err != nil {
		t.Fatal(err)
	}
	definition, err := taskstate.BuildApprovedDefinitionV2(
		taskstate.ApprovedDefinitionInputV2{
			TenantID: identity.TenantID, UserID: identity.UserID,
			TaskID: identity.TaskID, NLDescription: "monitor AI",
			SpecJSON: json.RawMessage(`{}`), ScopeJSON: json.RawMessage(`{}`),
			TaskManual: "monitor AI", Strictness: types.StrictnessNormal,
			ToolCalls:      []taskstate.ToolInvocationV1{call},
			ExecutionMode:  types.ExecutionModeCompiled,
			DeliveryPolicy: taskstate.DeliveryPolicyOwnerFeishu,
			BudgetPolicy:   taskstate.BudgetPolicyInheritTenantQuota,
		})
	if err != nil {
		t.Fatal(err)
	}
	definitionDigest, err :=
		taskstate.DigestApprovedDefinitionV2(definition)
	if err != nil {
		t.Fatal(err)
	}
	adaptive, err := taskstate.BuildAdaptiveStateV2(
		taskstate.AdaptiveStateInputV2{
			TenantID: identity.TenantID, UserID: identity.UserID,
			TaskID: identity.TaskID,
			InvocationStates: []taskstate.InvocationAdaptiveStateV1{{
				InvocationDigest: call.Digest, Cursor: json.RawMessage(`{}`),
				Status: taskstate.InvocationStatusActive,
			}},
		})
	if err != nil {
		t.Fatal(err)
	}
	policy := validToolRuntimePolicyV2(t)
	capability := policy.CapabilityCatalog.Allowed[0]
	snapshot := runcontext.CompiledSnapshotV2{
		Mode:              types.ExecutionModeCompiled,
		DefinitionVersion: 1, AdaptiveVersion: 1,
		AdaptiveBasisDefinitionVersion: 1,
		AdaptiveBasisDefinitionDigest:  definitionDigest,
		ObservationRollout:             observation.RolloutOff,
		Definition:                     definition,
		Adaptive:                       adaptive,
		ToolBindings: []runcontext.ToolBindingV1{{
			InvocationDigest: call.Digest,
			Contract: runcontext.ToolContractBindingV1{
				ToolName:            call.ToolName,
				ToolContractVersion: call.ToolContractVersion,
				Platform:            "web", Capability: "search", Kind: "article",
				ImplementationVersion: runtimepolicy.CapabilityImplementationExaV1,
			},
			Capability: capability,
		}},
		Policy: policy,
	}
	seal, err := runcontext.SealCompiledSnapshotV2(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if seal.DefinitionDigest != definitionDigest {
		t.Fatal("Tool fixture definition digest drifted")
	}
	snapshot.AdaptiveDigest = seal.AdaptiveDigest
	ref, err := (types.RunSnapshotRefV2{
		SchemaVersion: types.RunSnapshotSchemaVersionV2,
		SnapshotID:    91, TemporalWorkflowID: identity.TemporalWorkflowID,
		TemporalRunID: identity.TemporalRunID, RunKind: identity.RunKind,
		TenantID: identity.TenantID, UserID: identity.UserID,
		TaskID: identity.TaskID, Mode: types.ExecutionModeCompiled,
		DefinitionDigest: seal.DefinitionDigest,
		PlanDigest:       seal.PlanDigest, AdaptiveVersion: 1,
		Policy: seal.PolicyDigests, PayloadDigest: seal.PayloadDigest,
	}).Seal()
	if err != nil {
		t.Fatal(err)
	}
	snapshot.Ref = ref
	return ref, snapshot
}

func cloneCompiledToolSnapshotV2(
	t *testing.T,
	snapshot runcontext.CompiledSnapshotV2,
) runcontext.CompiledSnapshotV2 {
	t.Helper()
	raw, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	var cloned runcontext.CompiledSnapshotV2
	if err := json.Unmarshal(raw, &cloned); err != nil {
		t.Fatal(err)
	}
	return cloned
}

func TestCompiledToolSnapshotV2_SealRejectsExecutionViewTampering(
	t *testing.T,
) {
	identity := testActivityIdentity(7, 9, "task-tool-v2-seal")
	_, valid := compiledToolActivityFixtureV2(t, identity)
	if err := valid.ValidateFor(identity); err != nil {
		t.Fatalf("valid Tool snapshot: %v", err)
	}
	cases := []struct {
		name   string
		mutate func(*runcontext.CompiledSnapshotV2)
	}{
		{
			name: "model policy",
			mutate: func(snapshot *runcontext.CompiledSnapshotV2) {
				snapshot.Policy.ModelPolicy.Calls[0].MaxTokens++
			},
		},
		{
			name: "route and matching policy",
			mutate: func(snapshot *runcontext.CompiledSnapshotV2) {
				snapshot.ToolBindings[0].Capability.CredentialRef.Generation++
				snapshot.Policy.CapabilityCatalog.Allowed[0].
					CredentialRef.Generation++
			},
		},
		{
			name: "adaptive cursor with matching adaptive digest",
			mutate: func(snapshot *runcontext.CompiledSnapshotV2) {
				snapshot.Adaptive.InvocationStates[0].Cursor =
					json.RawMessage(`{"next":"changed"}`)
				digest, err :=
					taskstate.DigestAdaptiveStateV2(snapshot.Adaptive)
				if err != nil {
					t.Fatal(err)
				}
				snapshot.AdaptiveDigest = digest
			},
		},
		{
			name: "valid rollout",
			mutate: func(snapshot *runcontext.CompiledSnapshotV2) {
				snapshot.ObservationRollout = observation.RolloutAuthority
			},
		},
		{
			name: "definition and adaptive basis version",
			mutate: func(snapshot *runcontext.CompiledSnapshotV2) {
				snapshot.DefinitionVersion++
				snapshot.AdaptiveBasisDefinitionVersion++
			},
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			tampered := cloneCompiledToolSnapshotV2(t, valid)
			test.mutate(&tampered)
			if err := tampered.ValidateFor(identity); err == nil {
				t.Fatal("tampered execution view retained the sealed ref")
			}
		})
	}
}

func TestPrepareToolRunV2_CreatesThenRecoversBeforeMutablePolicy(
	t *testing.T,
) {
	identity := testActivityIdentity(7, 9, "task-tool-v2")
	ref, snapshot := compiledToolActivityFixtureV2(t, identity)
	st := &compiledToolRunStoreFake{
		ref: ref, snapshot: snapshot, authorize: true,
	}
	resolver := new(compiledModelResolverFake)
	var builderCalls atomic.Int32
	var builderErr atomic.Pointer[error]
	builder := func(
		context.Context, int64, bool,
	) (runtimepolicy.BundleV1, error) {
		builderCalls.Add(1)
		if err := builderErr.Load(); err != nil {
			return runtimepolicy.BundleV1{}, *err
		}
		return snapshot.Policy, nil
	}
	a := NewActivities(
		fakeFetcher{}, fakeScorer{}, fakeCardGen{}, &fakePusher{},
		&fakeStore{}, fakeFeishu{}, nil, nil, nil, nil,
		WithCompiledToolRuntimeV2(st, builder, resolver))
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(a.PrepareToolRunV2)
	params := PushParams{
		TenantID: identity.TenantID, UserID: identity.UserID,
		RunKind:        PushRunKindScheduled,
		ExecutionMode:  types.ExecutionModeCompiled,
		RuntimeVersion: CompiledRuntimeToolSnapshotV2,
		ScheduleID:     identity.TaskID,
	}
	first, err := executePrepareToolRunV2(t, env, a, params)
	if err != nil || !first.Authorized || first.Snapshot != ref {
		t.Fatalf("first Tool prepare = %+v err=%v", first, err)
	}
	policyFailure := errors.New("mutable policy must not be rebuilt")
	builderErr.Store(&policyFailure)
	recovered, err := executePrepareToolRunV2(t, env, a, params)
	if err != nil || recovered != first {
		t.Fatalf("recovered Tool prepare = %+v err=%v", recovered, err)
	}
	if builderCalls.Load() != 1 {
		t.Fatalf("Tool policy builder calls=%d, want 1", builderCalls.Load())
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.createCalls != 1 || st.loadRefCalls != 2 ||
		st.loadCalls != 2 || st.authorizeCalls != 2 {
		t.Fatalf("Tool prepare calls: loadRef=%d create=%d load=%d authorize=%d",
			st.loadRefCalls, st.createCalls, st.loadCalls, st.authorizeCalls)
	}
}

func TestPrepareToolRunV2_RejectsFrozenBindingDrift(t *testing.T) {
	identity := testActivityIdentity(7, 9, "task-tool-v2-drift")
	ref, snapshot := compiledToolActivityFixtureV2(t, identity)
	snapshot.ToolBindings[0].Contract.ImplementationVersion =
		runtimepolicy.CapabilityImplementationBindingV1
	st := &compiledToolRunStoreFake{
		ref: ref, found: true, snapshot: snapshot, authorize: true,
	}
	a := NewActivities(
		fakeFetcher{}, fakeScorer{}, fakeCardGen{}, &fakePusher{},
		&fakeStore{}, fakeFeishu{}, nil, nil, nil, nil,
		WithCompiledToolRuntimeV2(
			st,
			func(context.Context, int64, bool) (
				runtimepolicy.BundleV1, error,
			) {
				return snapshot.Policy, nil
			},
			new(compiledModelResolverFake)))
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(a.PrepareToolRunV2)
	_, err := executePrepareToolRunV2(t, env, a, PushParams{
		TenantID: identity.TenantID, UserID: identity.UserID,
		RunKind:        PushRunKindScheduled,
		ExecutionMode:  types.ExecutionModeCompiled,
		RuntimeVersion: CompiledRuntimeToolSnapshotV2,
		ScheduleID:     identity.TaskID,
	})
	if err == nil {
		t.Fatal("PrepareToolRunV2 accepted a frozen implementation drift")
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.authorizeCalls != 0 {
		t.Fatal("invalid frozen Tool snapshot reached live authorization")
	}
}
