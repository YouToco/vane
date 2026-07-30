package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"

	"github.com/YouToco/vane/feedback"
	"github.com/YouToco/vane/observation"
	"github.com/YouToco/vane/pusheffect"
	"github.com/YouToco/vane/runcontext"
	"github.com/YouToco/vane/runtimepolicy"
	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

type compiledToolRunStoreFake struct {
	mu sync.Mutex

	ref              types.RunSnapshotRefV2
	found            bool
	snapshot         runcontext.CompiledSnapshotV2
	authorize        bool
	observation      []types.ContentItem
	observationFound bool
	recentSimhashes  []int64

	loadRefErr           error
	createErr            error
	loadErr              error
	authorizeErr         error
	loadObservationErr   error
	commitObservationErr error
	recentSimhashErr     error
	quotaErr             error
	pushEffects          PushEffectStore
	pushRecoveryOnly     bool

	loadRefCalls             int
	createCalls              int
	loadCalls                int
	authorizeCalls           int
	loadObservationCalls     int
	commitObservationCalls   int
	listCandidateCalls       int
	recentSimhashCalls       int
	quotaCalls               int
	pushBatchCalls           int
	insertDeliveryCalls      int
	deliveryInvocation       string
	deliveryEvidence         json.RawMessage
	deliveryEvidenceRequired bool
	createPolicies           []runtimepolicy.BundleV1
}

func (f *compiledToolRunStoreFake) LoadContentObservationForTaskRunV2(
	_ context.Context,
	_ types.RunIdentity,
	_ types.RunSnapshotRefV2,
	_ string,
) ([]types.ContentItem, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.loadObservationCalls++
	return append([]types.ContentItem(nil), f.observation...),
		f.observationFound, f.loadObservationErr
}

func (f *compiledToolRunStoreFake) CommitContentObservationForTaskRunV2(
	_ context.Context,
	_ types.RunIdentity,
	_ types.RunSnapshotRefV2,
	_ string,
	items []types.ContentItem,
) ([]types.ContentItem, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.commitObservationCalls++
	if f.commitObservationErr != nil {
		return nil, f.commitObservationErr
	}
	for i := range items {
		if items[i].ID == 0 {
			items[i].ID = int64(100 + i)
		}
		items[i].SourceID = 0
	}
	f.observation = append([]types.ContentItem(nil), items...)
	f.observationFound = true
	return append([]types.ContentItem(nil), items...), nil
}

func (f *compiledToolRunStoreFake) ListContentCandidatesForTaskRunV2(
	_ context.Context,
	_ types.RunIdentity,
	_ types.RunSnapshotRefV2,
	_ int,
) ([]runcontext.ToolCandidateV1, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listCandidateCalls++
	out := make([]runcontext.ToolCandidateV1, len(f.observation))
	for i := range f.observation {
		out[i] = runcontext.ToolCandidateV1{
			InvocationDigest: f.snapshot.Definition.ToolCalls[0].Digest,
			Item:             f.observation[i],
		}
	}
	return out, nil
}

func (f *compiledToolRunStoreFake) ListRecentSimhashesForTaskRunV2(
	_ context.Context,
	_ types.RunIdentity,
	_ types.RunSnapshotRefV2,
	_ time.Time,
	_ []int64,
) ([]int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.recentSimhashCalls++
	return append([]int64(nil), f.recentSimhashes...), f.recentSimhashErr
}

func (f *compiledToolRunStoreFake) AuthorizeAndConsumeTaskRunLLMQuotaV2(
	_ context.Context,
	_ types.RunIdentity,
	_ types.RunSnapshotRefV2,
	_ runtimepolicy.QuotaBucketV1,
	_ float64,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.quotaCalls++
	return f.quotaErr
}

func (f *compiledToolRunStoreFake) CreateOrRecoverPushBatchForTaskRunV2(
	context.Context,
	types.RunIdentity,
	types.RunSnapshotRefV2,
	string,
) (int64, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pushBatchCalls++
	return 1, f.pushRecoveryOnly, nil
}

func (*compiledToolRunStoreFake) RecordEmptyPushBatchForTaskRunV2(
	context.Context,
	types.RunIdentity,
	types.RunSnapshotRefV2,
	string,
	types.BatchExitGate,
	types.PipelineCounts,
) (int64, bool, error) {
	return 1, false, nil
}

func (f *compiledToolRunStoreFake) InsertDeliveryForTaskRunV2(
	_ context.Context,
	_ types.RunIdentity,
	_ types.RunSnapshotRefV2,
	_ string,
	invocationDigest string,
	delivery *types.Delivery,
) (int64, bool, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.insertDeliveryCalls++
	f.deliveryInvocation = invocationDigest
	f.deliveryEvidence = append(
		json.RawMessage(nil), delivery.ToolEvidenceJSON...)
	f.deliveryEvidenceRequired = delivery.ToolEvidenceRequired
	return 1, false, false, nil
}

func (f *compiledToolRunStoreFake) ClaimPushBatchDeliveryAuthorityForTaskRunV2(
	ctx context.Context,
	_ types.RunIdentity,
	_ types.RunSnapshotRefV2,
	batchID int64,
) (types.PushBatchDeliveryAuthority, error) {
	if f.pushEffects != nil {
		return f.pushEffects.ClaimPushBatchDeliveryAuthority(
			ctx, types.PushBatchScope{
				TenantID: f.snapshot.Definition.TenantID,
				UserID:   f.snapshot.Definition.UserID,
				BatchID:  batchID,
			}, types.PushBatchDeliveryAuthorityEffect)
	}
	return types.PushBatchDeliveryAuthorityEffect, nil
}

func (f *compiledToolRunStoreFake) CreatePushEffectForTaskRunV2(
	ctx context.Context,
	_ types.RunIdentity,
	_ types.RunSnapshotRefV2,
	prepared pusheffect.Prepared,
) (*pusheffect.Effect, error) {
	if f.pushEffects != nil {
		return f.pushEffects.CreatePushEffect(ctx, prepared)
	}
	return &pusheffect.Effect{
		Prepared: prepared,
		Status:   pusheffect.StatusPrepared,
	}, nil
}

func (f *compiledToolRunStoreFake) ClaimPushEffectForTaskRunV2(
	ctx context.Context,
	_ types.RunIdentity,
	_ types.RunSnapshotRefV2,
	params pusheffect.ClaimParams,
) (*pusheffect.Effect, error) {
	if f.pushEffects != nil {
		return f.pushEffects.ClaimPushEffect(ctx, params)
	}
	return &pusheffect.Effect{
		Prepared: pusheffect.Prepared{
			ID: params.ID, TenantID: params.TenantID, UserID: params.UserID,
		},
		Status:     pusheffect.StatusSending,
		LeaseOwner: params.LeaseOwner,
		Fence:      1,
	}, nil
}

type compiledToolFetcherV2Fake struct {
	mu              sync.Mutex
	items           []types.ContentItem
	err             error
	validateErr     error
	validateCalls   int
	fetchCalls      int
	effectGateCalls int
}

func (f *compiledToolFetcherV2Fake) Fetch(
	context.Context,
	types.FetchTarget,
) ([]types.ContentItem, error) {
	return nil, errors.New("legacy Fetch must not execute a Tool V2 call")
}

func (f *compiledToolFetcherV2Fake) ValidateRuntimeFetchRouteV1(
	_ runtimepolicy.CapabilityV1,
	target types.FetchTarget,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.validateCalls++
	if target.ID != 0 {
		return errors.New("Tool V2 request carried a Source identity")
	}
	return f.validateErr
}

func (f *compiledToolFetcherV2Fake) FetchWithPolicyV1(
	ctx context.Context,
	target types.FetchTarget,
	_ runtimepolicy.CapabilityV1,
	beforeEffect func(context.Context) error,
) ([]types.ContentItem, error) {
	f.mu.Lock()
	f.fetchCalls++
	f.mu.Unlock()
	if target.ID != 0 {
		return nil, errors.New("Tool V2 request carried a Source ID")
	}
	if beforeEffect != nil {
		f.mu.Lock()
		f.effectGateCalls++
		f.mu.Unlock()
		if err := beforeEffect(ctx); err != nil {
			return nil, err
		}
	}
	return append([]types.ContentItem(nil), f.items...), f.err
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
			Request: runcontext.ToolFetchRequestV1{
				Platform: "web", Capability: "search",
				URL: "vane://web/search?q=AI", Title: "搜索: AI",
				Config: json.RawMessage(`{"query":"AI"}`),
			},
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
			name: "materialized request",
			mutate: func(snapshot *runcontext.CompiledSnapshotV2) {
				snapshot.ToolBindings[0].Request.Config =
					json.RawMessage(`{"query":"changed"}`)
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
	if err != nil || !first.Authorized || first.Snapshot != ref ||
		len(first.InvocationDigests) != 1 ||
		first.InvocationDigests[0] !=
			snapshot.Definition.ToolCalls[0].Digest {
		t.Fatalf("first Tool prepare = %+v err=%v", first, err)
	}
	policyFailure := errors.New("mutable policy must not be rebuilt")
	builderErr.Store(&policyFailure)
	recovered, err := executePrepareToolRunV2(t, env, a, params)
	if err != nil || !reflect.DeepEqual(recovered, first) {
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

func executeToolInvocationV2(
	t *testing.T,
	env *testsuite.TestActivityEnvironment,
	a *Activities,
	input ExecuteToolInvocationV2Input,
) (ToolInvocationReceiptV1, error) {
	t.Helper()
	encoded, err := env.ExecuteActivity(a.ExecuteToolInvocationV2, input)
	if err != nil {
		return ToolInvocationReceiptV1{}, err
	}
	var result ToolInvocationReceiptV1
	if err := encoded.Get(&result); err != nil {
		t.Fatalf("decode Tool invocation receipt: %v", err)
	}
	return result, nil
}

func TestExecuteToolInvocationV2_CommitsThenRecoversWithoutProviderReplay(
	t *testing.T,
) {
	identity := testActivityIdentity(7, 9, "task-tool-v2-execute")
	ref, snapshot := compiledToolActivityFixtureV2(t, identity)
	st := &compiledToolRunStoreFake{
		ref: ref, found: true, snapshot: snapshot, authorize: true,
	}
	frozenFetcher := &compiledToolFetcherV2Fake{
		items: []types.ContentItem{{
			ExternalID:   "result-1",
			CanonicalKey: "https://example.com/result-1",
			Kind:         types.KindArticle, URL: "https://example.com/result-1",
			Title: "Result", Content: "exact body",
			ContentHash: strings.Repeat("e", 64),
		}},
	}
	a := NewActivities(
		frozenFetcher, fakeScorer{}, fakeCardGen{}, &fakePusher{},
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
	env.RegisterActivity(a.ExecuteToolInvocationV2)
	input := ExecuteToolInvocationV2Input{
		TenantID: identity.TenantID, UserID: identity.UserID,
		TaskID: identity.TaskID, Snapshot: ref,
		InvocationDigest: snapshot.Definition.ToolCalls[0].Digest,
	}
	first, err := executeToolInvocationV2(t, env, a, input)
	if err != nil || first.ContentCount != 1 ||
		first.InvocationDigest != input.InvocationDigest ||
		len(first.ObservationDigest) != 64 {
		t.Fatalf("first Tool V2 receipt=%+v err=%v", first, err)
	}
	second, err := executeToolInvocationV2(t, env, a, input)
	if err != nil ||
		second.ObservationDigest != first.ObservationDigest ||
		second.ContentCount != first.ContentCount ||
		second != first {
		t.Fatalf("recovered Tool V2 receipt=%+v err=%v", second, err)
	}
	frozenFetcher.mu.Lock()
	validateCalls := frozenFetcher.validateCalls
	fetchCalls := frozenFetcher.fetchCalls
	effectCalls := frozenFetcher.effectGateCalls
	frozenFetcher.mu.Unlock()
	if validateCalls != 1 || fetchCalls != 1 || effectCalls != 1 {
		t.Fatalf("Tool V2 provider calls: validate=%d fetch=%d effect=%d",
			validateCalls, fetchCalls, effectCalls)
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.loadObservationCalls != 2 ||
		st.commitObservationCalls != 1 ||
		len(st.observation) != 1 ||
		st.observation[0].SourceID != 0 ||
		st.observation[0].ID <= 0 {
		t.Fatalf("Tool V2 observation calls/state: %+v", st)
	}
}

func TestToolCandidatePipelineV2_PreservesInvocationProvenance(
	t *testing.T,
) {
	identity := testActivityIdentity(7, 9, "task-tool-v2-pipeline")
	ref, snapshot := compiledToolActivityFixtureV2(t, identity)
	st := &compiledToolRunStoreFake{
		ref: ref, found: true, snapshot: snapshot, authorize: true,
	}
	scorer := new(compiledQuotaScorerFake)
	cardgen := new(compiledQuotaCardGenFake)
	a := NewActivities(
		fakeFetcher{}, scorer, cardgen, &fakePusher{},
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
	env.RegisterActivity(a.DedupToolCandidatesV2)
	env.RegisterActivity(a.QualifyToolCandidatesV2)
	env.RegisterActivity(a.ScoreToolCandidatesV2)
	env.RegisterActivity(a.SelectToolCandidatesV2)
	env.RegisterActivity(a.CardGenToolCandidatesV2)

	run := CompiledToolRunInputV2{
		TenantID: identity.TenantID,
		TaskID:   identity.TaskID,
		Snapshot: ref,
	}
	invocation := snapshot.Definition.ToolCalls[0].Digest
	candidates := []runcontext.ToolCandidateV1{
		{
			InvocationDigest: invocation,
			Item: types.ContentItem{
				ID: 101, Kind: types.KindArticle,
				Title: "same title", Content: "same body",
				FetchedAt: time.Now().UTC(),
			},
		},
		{
			InvocationDigest: invocation,
			Item: types.ContentItem{
				ID: 102, Kind: types.KindArticle,
				Title: "same title", Content: "same body",
				FetchedAt: time.Now().UTC().Add(-time.Minute),
			},
		},
	}
	st.observation = []types.ContentItem{
		candidates[0].Item,
		candidates[1].Item,
	}
	st.observationFound = true
	encoded, err := env.ExecuteActivity(
		a.DedupToolCandidatesV2,
		DedupToolCandidatesV2Input{
			UserID: identity.UserID, TraceID: "trace-tool-v2",
			Run: run, Candidates: candidates,
		})
	if err != nil {
		t.Fatalf("dedup Tool candidates: %v", err)
	}
	var deduped []runcontext.ToolCandidateV1
	if err := encoded.Get(&deduped); err != nil {
		t.Fatal(err)
	}
	if len(deduped) != 1 ||
		deduped[0].InvocationDigest != invocation ||
		deduped[0].Item.Simhash == nil {
		t.Fatalf("deduped candidates = %+v", deduped)
	}

	encoded, err = env.ExecuteActivity(
		a.QualifyToolCandidatesV2,
		QualifyToolCandidatesV2Input{
			UserID: identity.UserID, TraceID: "trace-tool-v2",
			Run: run, Candidates: deduped,
		})
	if err != nil {
		t.Fatalf("qualify Tool candidates: %v", err)
	}
	var qualified QualifyToolCandidatesV2Result
	if err := encoded.Get(&qualified); err != nil {
		t.Fatal(err)
	}
	if qualified.Outcome != "not_configured" ||
		len(qualified.Candidates) != 1 ||
		qualified.Candidates[0].InvocationDigest != invocation {
		t.Fatalf("qualified candidates = %+v", qualified)
	}

	encoded, err = env.ExecuteActivity(
		a.ScoreToolCandidatesV2,
		ScoreToolCandidatesV2Input{
			UserID: identity.UserID, TraceID: "trace-tool-v2",
			Run: run, Candidates: qualified.Candidates,
		})
	if err != nil {
		t.Fatalf("score Tool candidates: %v", err)
	}
	var scored []runcontext.ToolScoredCandidateV1
	if err := encoded.Get(&scored); err != nil {
		t.Fatal(err)
	}
	if len(scored) != 1 ||
		scored[0].InvocationDigest != invocation ||
		scored[0].Scored.Score != 88 {
		t.Fatalf("scored candidates = %+v", scored)
	}

	encoded, err = env.ExecuteActivity(
		a.SelectToolCandidatesV2,
		SelectToolCandidatesV2Input{
			UserID: identity.UserID, TraceID: "trace-tool-v2",
			Run: run, Candidates: scored,
		})
	if err != nil {
		t.Fatalf("select Tool candidates: %v", err)
	}
	var selected []runcontext.ToolScoredCandidateV1
	if err := encoded.Get(&selected); err != nil {
		t.Fatal(err)
	}
	if len(selected) != 1 ||
		selected[0].InvocationDigest != invocation {
		t.Fatalf("selected candidates = %+v", selected)
	}

	encoded, err = env.ExecuteActivity(
		a.CardGenToolCandidatesV2,
		CardGenToolCandidatesV2Input{
			UserID: identity.UserID, TraceID: "trace-tool-v2",
			Run: run, Candidates: selected,
		})
	if err != nil {
		t.Fatalf("cardgen Tool candidates: %v", err)
	}
	var cards []ToolGeneratedCardV1
	if err := encoded.Get(&cards); err != nil {
		t.Fatal(err)
	}
	if len(cards) != 1 ||
		cards[0].InvocationDigest != invocation ||
		cards[0].Card.BodyMD != "body" ||
		cards[0].Card.Scored.Item.SourceID != 0 {
		t.Fatalf("Tool cards = %+v", cards)
	}

	st.mu.Lock()
	defer st.mu.Unlock()
	if st.recentSimhashCalls != 1 || st.quotaCalls != 2 ||
		st.authorizeCalls != 2 {
		t.Fatalf("Tool pipeline gates: simhash=%d quota=%d authorize=%d",
			st.recentSimhashCalls, st.quotaCalls, st.authorizeCalls)
	}
}

func TestDedupToolCandidateBatchV2PreservesIndependentEvidence(t *testing.T) {
	candidates := []runcontext.ToolCandidateV1{
		{
			InvocationDigest: strings.Repeat("a", 64),
			Item: types.ContentItem{
				ID: 1, Kind: types.KindArticle,
				Title: "Claude Opus 5 release",
				URL:   "https://www.anthropic.com/news/claude-opus-5",
				Content: "Claude Opus 5 release with one million token " +
					"context.",
			},
		},
		{
			InvocationDigest: strings.Repeat("b", 64),
			Item: types.ContentItem{
				ID: 2, Kind: types.KindArticle,
				Title: "Claude Opus 5 release",
				URL:   "https://media.example/claude-opus-5",
				Content: "Claude Opus 5 release with one million token " +
					"context.",
			},
		},
	}
	if got := dedupToolCandidateBatchV2(candidates, nil, false); len(got) != 1 {
		t.Fatalf("ordinary near-duplicate count=%d, want 1", len(got))
	}
	got := dedupToolCandidateBatchV2(candidates, nil, true)
	if len(got) != 2 || got[0].Item.Simhash == nil ||
		got[1].Item.Simhash == nil {
		t.Fatalf("independent evidence was collapsed: %+v", got)
	}
}

func TestScoreToolCandidatesV2_RejectsTamperedPayloadBeforeSpend(
	t *testing.T,
) {
	identity := testActivityIdentity(7, 9, "task-tool-v2-provenance")
	ref, snapshot := compiledToolActivityFixtureV2(t, identity)
	st := &compiledToolRunStoreFake{
		ref: ref, found: true, snapshot: snapshot, authorize: true,
		observation: []types.ContentItem{{
			ID: 103, Kind: types.KindArticle,
			Title: "canonical", Content: "canonical",
		}},
	}
	scorer := new(compiledQuotaScorerFake)
	a := NewActivities(
		fakeFetcher{}, scorer, fakeCardGen{}, &fakePusher{},
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
	env.RegisterActivity(a.ScoreToolCandidatesV2)
	_, err := env.ExecuteActivity(
		a.ScoreToolCandidatesV2,
		ScoreToolCandidatesV2Input{
			UserID: identity.UserID, TraceID: "trace-tool-v2-invalid",
			Run: CompiledToolRunInputV2{
				TenantID: identity.TenantID,
				TaskID:   identity.TaskID,
				Snapshot: ref,
			},
			Candidates: []runcontext.ToolCandidateV1{{
				InvocationDigest: snapshot.Definition.ToolCalls[0].Digest,
				Item: types.ContentItem{
					ID: 103, Kind: types.KindArticle,
					Title: "tampered", Content: "tampered",
				},
			}},
		})
	if err == nil {
		t.Fatal("tampered Tool candidate reached score")
	}
	if scorer.calls.Load() != 0 {
		t.Fatal("tampered Tool candidate spent model quota")
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.quotaCalls != 0 || st.authorizeCalls != 0 {
		t.Fatalf("tampered candidate reached gates: %+v", st)
	}
}

func TestToolCardEvidenceSourcesV2UsesLiveCanonicalURLs(t *testing.T) {
	identity := testActivityIdentity(7, 9, "task-tool-v2-evidence")
	_, snapshot := compiledToolActivityFixtureV2(t, identity)
	now := time.Date(2026, 7, 30, 9, 0, 0, 0, time.UTC)
	invocation := snapshot.Definition.ToolCalls[0].Digest
	sources, err := toolCardEvidenceSourcesV2(
		snapshot,
		[]runcontext.ToolCandidateV1{
			{
				InvocationDigest: invocation,
				Item: types.ContentItem{
					ID: 171, Title: "official",
					URL:       "https://example.com/official",
					Content:   "official announcement",
					CreatedAt: now,
				},
			},
			{
				InvocationDigest: invocation,
				Item: types.ContentItem{
					ID: 172, Title: "cross-check",
					URL:       "https://example.net/cross-check",
					Content:   "independent evidence",
					CreatedAt: now,
				},
			},
		})
	if err != nil {
		t.Fatal(err)
	}
	if len(sources) != 2 ||
		sources[0].ContentItemID != 171 ||
		sources[0].Metadata.Ref != "source-1" ||
		sources[0].Metadata.SourceURL !=
			"https://example.com/official" ||
		sources[1].Metadata.Ref != "source-2" ||
		sources[1].Metadata.SourceURL !=
			"https://example.net/cross-check" {
		t.Fatalf("Tool evidence sources = %+v", sources)
	}
}

func TestPushToolCardsV2_UsesDurableEffectAndToolProvenance(
	t *testing.T,
) {
	identity := testActivityIdentity(7, 9, "task-tool-v2-push")
	ref, snapshot := compiledToolActivityFixtureV2(t, identity)
	now := time.Date(2026, 7, 30, 9, 0, 0, 0, time.UTC)
	st := &compiledToolRunStoreFake{
		ref: ref, found: true, snapshot: snapshot, authorize: true,
		observation: []types.ContentItem{{
			ID: 201, Kind: types.KindArticle,
			Title:     "Tool result",
			URL:       "https://example.com/tool-result",
			Content:   "Tool body",
			CreatedAt: now,
		}},
		observationFound: true,
	}
	effectStore := newPRBActivityEffectStore(nil, nil)
	st.pushEffects = effectStore
	pusher := &prbActivityPusher{chatID: "oc_tool_v2"}
	feishu := &prbEffectFeishu{
		owner: "ou_tool_v2", chat: "oc_tool_v2", app: "cli_tool_v2",
	}
	var captured []feedback.CardInput
	build := func(in feedback.AggregateCardInput) string {
		captured = append([]feedback.CardInput(nil), in.Items...)
		return prbEffectCard(in)
	}
	a := NewActivities(
		fakeFetcher{}, fakeScorer{}, fakeCardGen{}, pusher,
		&fakeStore{}, feishu, nil, nil, build, nil,
		WithCompiledToolRuntimeV2(
			st,
			func(context.Context, int64, bool) (
				runtimepolicy.BundleV1, error,
			) {
				return snapshot.Policy, nil
			},
			new(compiledModelResolverFake)),
		WithPushEffectCanary(effectStore, identity.TaskID))
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(a.PushToolCardsV2)
	invocation := snapshot.Definition.ToolCalls[0].Digest
	evidenceCandidate := runcontext.ToolCandidateV1{
		InvocationDigest: invocation,
		Item: types.ContentItem{
			ID: 201, Kind: types.KindArticle,
			Title:     "Tool result",
			URL:       "https://example.com/tool-result",
			Content:   "Tool body",
			CreatedAt: now,
		},
	}
	evidenceSources, err := toolCardEvidenceSourcesV2(
		snapshot, []runcontext.ToolCandidateV1{evidenceCandidate})
	if err != nil {
		t.Fatal(err)
	}
	_, err = env.ExecuteActivity(
		a.PushToolCardsV2,
		PushToolCardsV2Input{
			UserID: identity.UserID, TraceID: "trace-tool-v2-push",
			Run: CompiledToolRunInputV2{
				TenantID: identity.TenantID,
				TaskID:   identity.TaskID,
				Snapshot: ref,
			},
			EvidenceRequired: true,
			Cards: []ToolGeneratedCardV1{{
				InvocationDigest: invocation,
				Card: GeneratedCard{
					Scored: types.ScoredItem{
						Item: types.ContentItem{
							ID: 201, Kind: types.KindArticle,
							Title:     "Tool result",
							URL:       "https://example.com/tool-result",
							Content:   "Tool body",
							CreatedAt: now,
						},
						Score: 88,
					},
					BodyMD: "durable Tool insight",
				},
				Evidence: []ToolCardEvidenceV1{{
					Candidate: evidenceCandidate,
					Source:    evidenceSources[0],
				}},
			}},
		})
	if err != nil {
		t.Fatalf("PushToolCardsV2: %v", err)
	}
	calls := pusher.snapshot()
	prepared, claimed, receipts := effectStore.snapshot()
	if len(calls) != 1 || len(prepared) != 1 ||
		len(claimed) != 1 || len(receipts) != 1 {
		t.Fatalf("durable Tool push: calls=%d prepared=%d claimed=%d receipts=%d",
			len(calls), len(prepared), len(claimed), len(receipts))
	}
	if prepared[0].RunSnapshotID != ref.SnapshotID ||
		len(prepared[0].DeliveryIDs) != 1 ||
		prepared[0].DeliveryIDs[0] != 1 ||
		calls[0].uuid != prepared[0].ProviderUUID {
		t.Fatalf("durable Tool effect differs: prepared=%+v call=%+v",
			prepared[0], calls[0])
	}
	if len(captured) != 1 ||
		captured[0].SourceTitle !=
			snapshot.ToolBindings[0].Contract.ToolName ||
		captured[0].Platform !=
			types.Platform(snapshot.ToolBindings[0].Contract.Platform) {
		t.Fatalf("Tool display provenance = %+v", captured)
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.pushBatchCalls != 1 || st.insertDeliveryCalls != 1 ||
		st.deliveryInvocation != invocation ||
		len(st.deliveryEvidence) == 0 ||
		!st.deliveryEvidenceRequired ||
		st.authorizeCalls != 1 {
		t.Fatalf("Tool push Store gates: %+v", st)
	}
}

func TestPushToolCardsV2RejectsMissingRequiredEvidence(t *testing.T) {
	identity := testActivityIdentity(
		7, 9, "task-tool-v2-missing-evidence")
	ref, snapshot := compiledToolActivityFixtureV2(t, identity)
	now := time.Date(2026, 7, 30, 9, 0, 0, 0, time.UTC)
	st := &compiledToolRunStoreFake{
		ref: ref, found: true, snapshot: snapshot, authorize: true,
	}
	effectStore := newPRBActivityEffectStore(nil, nil)
	st.pushEffects = effectStore
	pusher := &prbActivityPusher{chatID: "oc_tool_v2"}
	a := NewActivities(
		fakeFetcher{}, fakeScorer{}, fakeCardGen{}, pusher,
		&fakeStore{}, fakeFeishu{}, nil, nil, prbEffectCard, nil,
		WithCompiledToolRuntimeV2(
			st,
			func(context.Context, int64, bool) (
				runtimepolicy.BundleV1, error,
			) {
				return snapshot.Policy, nil
			},
			new(compiledModelResolverFake)),
		WithPushEffectCanary(effectStore, identity.TaskID))
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(a.PushToolCardsV2)
	invocation := snapshot.Definition.ToolCalls[0].Digest
	_, err := env.ExecuteActivity(
		a.PushToolCardsV2,
		PushToolCardsV2Input{
			UserID: identity.UserID, TraceID: "trace-missing-evidence",
			Run: CompiledToolRunInputV2{
				TenantID: identity.TenantID,
				TaskID:   identity.TaskID,
				Snapshot: ref,
			},
			EvidenceRequired: true,
			Cards: []ToolGeneratedCardV1{{
				InvocationDigest: invocation,
				Card: GeneratedCard{
					Scored: types.ScoredItem{
						Item: types.ContentItem{
							ID: 211, Kind: types.KindArticle,
							Title: "Tool result", Content: "Tool body",
							URL:       "https://example.com/tool-result",
							CreatedAt: now,
						},
						Score: 88,
					},
					BodyMD: "[fake](https://fake.example)",
				},
			}},
		})
	if err == nil {
		t.Fatalf("missing required evidence err=%v", err)
	}
	if len(pusher.snapshot()) != 0 {
		t.Fatal("missing required evidence reached provider")
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.insertDeliveryCalls != 0 {
		t.Fatal("missing required evidence reached durable delivery")
	}
}

func TestPushToolCardsV2RejectsForgedSecondaryEvidence(t *testing.T) {
	identity := testActivityIdentity(
		7, 9, "task-tool-v2-forged-evidence")
	ref, snapshot := compiledToolActivityFixtureV2(t, identity)
	invocation := snapshot.Definition.ToolCalls[0].Digest
	now := time.Date(2026, 7, 30, 9, 0, 0, 0, time.UTC)
	official := types.ContentItem{
		ID: 211, Kind: types.KindArticle, Title: "official",
		URL:     "https://official.example/release",
		Content: "official body", CreatedAt: now,
	}
	cross := types.ContentItem{
		ID: 212, Kind: types.KindArticle, Title: "cross",
		URL:     "https://media.example/report",
		Content: "cross body", CreatedAt: now,
	}
	candidates := []runcontext.ToolCandidateV1{
		{InvocationDigest: invocation, Item: official},
		{InvocationDigest: invocation, Item: cross},
	}
	sources, err := toolCardEvidenceSourcesV2(snapshot, candidates)
	if err != nil {
		t.Fatal(err)
	}
	sources[1].Metadata.SourceURL = "https://fake.example/report"
	st := &compiledToolRunStoreFake{
		ref: ref, found: true, snapshot: snapshot, authorize: true,
		observation:      []types.ContentItem{official, cross},
		observationFound: true,
	}
	effectStore := newPRBActivityEffectStore(nil, nil)
	st.pushEffects = effectStore
	a := NewActivities(
		fakeFetcher{}, fakeScorer{}, fakeCardGen{}, &fakePusher{},
		&fakeStore{}, fakeFeishu{}, nil, nil,
		func(feedback.AggregateCardInput) string { return "{}" }, nil,
		WithCompiledToolRuntimeV2(
			st,
			func(context.Context, int64, bool) (
				runtimepolicy.BundleV1, error,
			) {
				return snapshot.Policy, nil
			},
			new(compiledModelResolverFake)),
		WithPushEffectCanary(effectStore, identity.TaskID))
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(a.PushToolCardsV2)
	_, err = env.ExecuteActivity(
		a.PushToolCardsV2,
		PushToolCardsV2Input{
			UserID:  identity.UserID,
			TraceID: "trace-tool-v2-forged-evidence",
			Run: CompiledToolRunInputV2{
				TenantID: identity.TenantID,
				TaskID:   identity.TaskID,
				Snapshot: ref,
			},
			Cards: []ToolGeneratedCardV1{{
				InvocationDigest: invocation,
				Card: GeneratedCard{
					Scored: types.ScoredItem{
						Item: official, Score: 88,
					},
					BodyMD: "safe",
				},
				Evidence: []ToolCardEvidenceV1{
					{Candidate: candidates[0], Source: sources[0]},
					{Candidate: candidates[1], Source: sources[1]},
				},
			}},
		})
	if err == nil {
		t.Fatal("forged secondary evidence reached Tool push")
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.insertDeliveryCalls != 0 {
		t.Fatal("forged secondary evidence reached durable delivery")
	}
}

func TestPushToolCardsV2_RecoveryOnlySettlesWithoutLiveAuthorization(
	t *testing.T,
) {
	identity := testActivityIdentity(7, 9, "task-tool-v2-recovery")
	ref, snapshot := compiledToolActivityFixtureV2(t, identity)
	effectStore := newPRBActivityEffectStore(nil, nil)
	st := &compiledToolRunStoreFake{
		ref: ref, found: true, snapshot: snapshot,
		authorize: false, pushRecoveryOnly: true,
		pushEffects: effectStore,
	}
	pusher := &prbActivityPusher{chatID: "oc_tool_v2"}
	a := NewActivities(
		fakeFetcher{}, fakeScorer{}, fakeCardGen{}, pusher,
		&fakeStore{}, fakeFeishu{}, nil, nil, nil, nil,
		WithCompiledToolRuntimeV2(
			st,
			func(context.Context, int64, bool) (
				runtimepolicy.BundleV1, error,
			) {
				return snapshot.Policy, nil
			},
			new(compiledModelResolverFake)),
		WithPushEffectCanary(effectStore, identity.TaskID))
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(a.PushToolCardsV2)
	_, err := env.ExecuteActivity(
		a.PushToolCardsV2,
		PushToolCardsV2Input{
			UserID: identity.UserID, TraceID: "trace-tool-v2-recovery",
			Run: CompiledToolRunInputV2{
				TenantID: identity.TenantID,
				TaskID:   identity.TaskID,
				Snapshot: ref,
			},
			// Receipt recovery must not depend on replayed card payloads.
			Cards: nil,
		})
	if err != nil {
		t.Fatalf("recover Tool push receipt: %v", err)
	}
	effectStore.mu.Lock()
	settles := effectStore.settles
	effectStore.mu.Unlock()
	if settles != 1 || len(pusher.snapshot()) != 0 {
		t.Fatalf("recovery-only Tool push: settles=%d provider_calls=%d",
			settles, len(pusher.snapshot()))
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.pushBatchCalls != 1 || st.authorizeCalls != 0 ||
		st.insertDeliveryCalls != 0 ||
		st.loadObservationCalls != 0 {
		t.Fatalf("recovery-only Tool push crossed live gates: %+v", st)
	}
}

func TestPushPipelineWorkflow_CompiledToolV2CommandSequence(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	identity := testActivityIdentity(7, 9, "task-tool-v2-workflow")
	ref, snapshot := compiledToolActivityFixtureV2(t, identity)
	invocation := snapshot.Definition.ToolCalls[0].Digest
	item := types.ContentItem{
		ID: 301, Kind: types.KindArticle,
		Title: "Tool result", URL: "https://example.com/tool-result",
	}
	candidate := runcontext.ToolCandidateV1{
		InvocationDigest: invocation, Item: item,
	}
	scored := runcontext.ToolScoredCandidateV1{
		InvocationDigest: invocation,
		Scored:           types.ScoredItem{Item: item, Score: 88},
	}
	card := ToolGeneratedCardV1{
		InvocationDigest: invocation,
		Card:             GeneratedCard{Scored: scored.Scored, BodyMD: "insight"},
	}
	var mu sync.Mutex
	var calls []string
	var cardGenEvidenceRequired bool
	var pushEvidenceRequired bool
	record := func(name string) {
		mu.Lock()
		defer mu.Unlock()
		calls = append(calls, name)
	}
	register := func(name string, fn any) {
		env.RegisterActivityWithOptions(
			fn, activity.RegisterOptions{Name: name})
	}
	register("PrepareToolRunV2",
		func(context.Context, PushParams) (PrepareToolRunV2Result, error) {
			record("prepare")
			return PrepareToolRunV2Result{
				Authorized: true, Snapshot: ref,
				InvocationDigests: []string{invocation},
			}, nil
		})
	register("ExecuteToolInvocationV2",
		func(
			_ context.Context,
			in ExecuteToolInvocationV2Input,
		) (ToolInvocationReceiptV1, error) {
			record("execute")
			return ToolInvocationReceiptV1{
				InvocationDigest:  in.InvocationDigest,
				ObservationDigest: strings.Repeat("b", 64),
				ContentCount:      1,
			}, nil
		})
	register("CollectToolRunContentV2",
		func(
			context.Context,
			CollectToolRunContentV2Input,
		) ([]runcontext.ToolCandidateV1, error) {
			record("collect")
			return []runcontext.ToolCandidateV1{candidate}, nil
		})
	register("DedupToolCandidatesV2",
		func(
			context.Context,
			DedupToolCandidatesV2Input,
		) ([]runcontext.ToolCandidateV1, error) {
			record("dedup")
			return []runcontext.ToolCandidateV1{candidate}, nil
		})
	register("QualifyToolCandidatesV2",
		func(
			context.Context,
			QualifyToolCandidatesV2Input,
		) (QualifyToolCandidatesV2Result, error) {
			record("qualify")
			return QualifyToolCandidatesV2Result{
				Candidates:       []runcontext.ToolCandidateV1{candidate},
				EvidenceRequired: true,
				Outcome:          "match",
			}, nil
		})
	register("ScoreToolCandidatesV2",
		func(
			context.Context,
			ScoreToolCandidatesV2Input,
		) ([]runcontext.ToolScoredCandidateV1, error) {
			record("score")
			return []runcontext.ToolScoredCandidateV1{scored}, nil
		})
	register("SelectToolCandidatesV2",
		func(
			context.Context,
			SelectToolCandidatesV2Input,
		) ([]runcontext.ToolScoredCandidateV1, error) {
			record("select")
			return []runcontext.ToolScoredCandidateV1{scored}, nil
		})
	register("CardGenToolCandidatesV2",
		func(
			_ context.Context,
			in CardGenToolCandidatesV2Input,
		) ([]ToolGeneratedCardV1, error) {
			record("cardgen")
			mu.Lock()
			cardGenEvidenceRequired = in.EvidenceRequired
			mu.Unlock()
			return []ToolGeneratedCardV1{card}, nil
		})
	register("PushToolCardsV2",
		func(_ context.Context, in PushToolCardsV2Input) error {
			record("push")
			mu.Lock()
			pushEvidenceRequired = in.EvidenceRequired
			mu.Unlock()
			return nil
		})

	env.ExecuteWorkflow(PushPipelineWorkflow, PushParams{
		TenantID: identity.TenantID, UserID: identity.UserID,
		RunKind:        PushRunKindScheduled,
		ExecutionMode:  types.ExecutionModeCompiled,
		RuntimeVersion: CompiledRuntimeToolSnapshotV2,
		ScheduleID:     identity.TaskID,
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("compiled Tool workflow: %v", err)
	}
	want := []string{
		"prepare", "execute", "collect", "dedup",
		"qualify", "score", "select", "cardgen", "push",
	}
	mu.Lock()
	defer mu.Unlock()
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("compiled Tool command sequence=%v, want %v", calls, want)
	}
	if !cardGenEvidenceRequired || !pushEvidenceRequired {
		t.Fatalf(
			"evidence requirement was not carried: cardgen=%t push=%t",
			cardGenEvidenceRequired, pushEvidenceRequired)
	}
}

func TestPushPipelineWorkflow_ToolV2NoMatchStopsBeforeScoreAndPush(
	t *testing.T,
) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	identity := testActivityIdentity(7, 9, "task-tool-v2-no-match")
	ref, snapshot := compiledToolActivityFixtureV2(t, identity)
	invocation := snapshot.Definition.ToolCalls[0].Digest
	item := types.ContentItem{
		ID: 302, Kind: types.KindArticle,
		Title: "media rumor", URL: "https://media.example/rumor",
	}
	candidate := runcontext.ToolCandidateV1{
		InvocationDigest: invocation, Item: item,
	}
	register := func(name string, fn any) {
		env.RegisterActivityWithOptions(
			fn, activity.RegisterOptions{Name: name})
	}
	register("PrepareToolRunV2",
		func(context.Context, PushParams) (PrepareToolRunV2Result, error) {
			return PrepareToolRunV2Result{
				Authorized: true, Snapshot: ref,
				InvocationDigests: []string{invocation},
			}, nil
		})
	register("ExecuteToolInvocationV2",
		func(
			context.Context,
			ExecuteToolInvocationV2Input,
		) (ToolInvocationReceiptV1, error) {
			return ToolInvocationReceiptV1{
				InvocationDigest:  invocation,
				ObservationDigest: strings.Repeat("c", 64),
				ContentCount:      1,
			}, nil
		})
	register("CollectToolRunContentV2",
		func(
			context.Context,
			CollectToolRunContentV2Input,
		) ([]runcontext.ToolCandidateV1, error) {
			return []runcontext.ToolCandidateV1{candidate}, nil
		})
	register("DedupToolCandidatesV2",
		func(
			context.Context,
			DedupToolCandidatesV2Input,
		) ([]runcontext.ToolCandidateV1, error) {
			return []runcontext.ToolCandidateV1{candidate}, nil
		})
	register("QualifyToolCandidatesV2",
		func(
			context.Context,
			QualifyToolCandidatesV2Input,
		) (QualifyToolCandidatesV2Result, error) {
			return QualifyToolCandidatesV2Result{
				Outcome: "no_match",
			}, nil
		})
	var got RecordEmptyToolRunV2Input
	register("RecordEmptyToolRunV2",
		func(_ context.Context, in RecordEmptyToolRunV2Input) error {
			got = in
			return nil
		})

	env.ExecuteWorkflow(PushPipelineWorkflow, PushParams{
		TenantID: identity.TenantID, UserID: identity.UserID,
		RunKind:        PushRunKindScheduled,
		ExecutionMode:  types.ExecutionModeCompiled,
		RuntimeVersion: CompiledRuntimeToolSnapshotV2,
		ScheduleID:     identity.TaskID,
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("compiled Tool no-match workflow: %v", err)
	}
	if got.Gate != types.BatchExitGateObservationNoMatch ||
		got.Counts.Fetched == nil || *got.Counts.Fetched != 1 ||
		got.Counts.Deduped == nil || *got.Counts.Deduped != 1 ||
		got.Counts.Qualified == nil || *got.Counts.Qualified != 0 {
		t.Fatalf("no-match receipt = %+v", got)
	}
}

func TestPushPipelineWorkflow_ToolProviderFailureIsNotRetried(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	identity := testActivityIdentity(7, 9, "task-tool-v2-no-retry")
	ref, snapshot := compiledToolActivityFixtureV2(t, identity)
	invocation := snapshot.Definition.ToolCalls[0].Digest
	var executeCalls atomic.Int32
	env.RegisterActivityWithOptions(
		func(context.Context, PushParams) (PrepareToolRunV2Result, error) {
			return PrepareToolRunV2Result{
				Authorized: true, Snapshot: ref,
				InvocationDigests: []string{invocation},
			}, nil
		}, activity.RegisterOptions{Name: "PrepareToolRunV2"})
	env.RegisterActivityWithOptions(
		func(
			context.Context,
			ExecuteToolInvocationV2Input,
		) (ToolInvocationReceiptV1, error) {
			executeCalls.Add(1)
			return ToolInvocationReceiptV1{}, errors.New("provider failed")
		}, activity.RegisterOptions{Name: "ExecuteToolInvocationV2"})

	env.ExecuteWorkflow(PushPipelineWorkflow, PushParams{
		TenantID: identity.TenantID, UserID: identity.UserID,
		RunKind:        PushRunKindScheduled,
		ExecutionMode:  types.ExecutionModeCompiled,
		RuntimeVersion: CompiledRuntimeToolSnapshotV2,
		ScheduleID:     identity.TaskID,
	})
	if env.GetWorkflowError() == nil {
		t.Fatal("Tool provider failure unexpectedly succeeded")
	}
	if executeCalls.Load() != 1 {
		t.Fatalf("Tool provider calls=%d, want exactly one",
			executeCalls.Load())
	}
}
