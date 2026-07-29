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

	"go.temporal.io/sdk/testsuite"

	"github.com/YouToco/vane/observation"
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

	loadRefCalls           int
	createCalls            int
	loadCalls              int
	authorizeCalls         int
	loadObservationCalls   int
	commitObservationCalls int
	listCandidateCalls     int
	recentSimhashCalls     int
	quotaCalls             int
	createPolicies         []runtimepolicy.BundleV1
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
		a.ScoreToolCandidatesV2,
		ScoreToolCandidatesV2Input{
			UserID: identity.UserID, TraceID: "trace-tool-v2",
			Run: run, Candidates: deduped,
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

func TestScoreToolCandidatesV2_RejectsUnknownInvocationBeforeSpend(
	t *testing.T,
) {
	identity := testActivityIdentity(7, 9, "task-tool-v2-provenance")
	ref, snapshot := compiledToolActivityFixtureV2(t, identity)
	st := &compiledToolRunStoreFake{
		ref: ref, found: true, snapshot: snapshot, authorize: true,
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
				InvocationDigest: strings.Repeat("f", 64),
				Item: types.ContentItem{
					ID: 103, Kind: types.KindArticle,
					Title: "tampered", Content: "tampered",
				},
			}},
		})
	if err == nil {
		t.Fatal("unknown Tool invocation reached score")
	}
	if scorer.calls.Load() != 0 {
		t.Fatal("unknown Tool invocation spent model quota")
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.quotaCalls != 0 || st.authorizeCalls != 0 {
		t.Fatalf("unknown invocation reached gates: %+v", st)
	}
}
