package workflow

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"

	"github.com/YouToco/vane/server/types"
)

type researchWorkflowV3Stubs struct {
	threshold       string
	significance    types.ResearchBriefSignificanceV3
	deliveryAllowed bool
	stepFailures    map[int]string
	mu              sync.Mutex
	calls           []string
}

type researchV3FailingRuntime struct{ err error }

func (r researchV3FailingRuntime) Prepare(
	_ context.Context, identity types.RunIdentity, _ string,
) (types.ResearchRunSnapshotRefV3, bool, bool, error) {
	digest := strings.Repeat("a", 64)
	ref, err := types.SealResearchRunSnapshotRefV3(types.ResearchRunSnapshotRefV3{
		SnapshotID: 11, TemporalWorkflowID: identity.TemporalWorkflowID,
		TemporalRunID: identity.TemporalRunID, RunKind: identity.RunKind,
		TenantID: identity.TenantID, UserID: identity.UserID, TaskID: identity.TaskID,
		DefinitionVersion: 1, DefinitionDigest: digest,
		CapabilityCatalogDigest: digest, ToolPolicyDigest: digest,
		PromptPolicyDigest: digest, ModelPolicyDigest: digest, QuotaPolicyDigest: digest,
		PlannerBudget: types.PlannerBudget{MaxPlannerRounds: 8, MaxToolCalls: 16,
			MaxTokens: 32768, MaxCostMicroUSD: 1_000_000, DurationMs: 300_000},
		HistoryThroughUTC: "2026-08-01T12:34:56Z", PayloadDigest: digest,
	})
	return ref, true, false, err
}

func (r researchV3FailingRuntime) Plan(
	context.Context, types.RunIdentity, types.ResearchRunSnapshotRefV3, string,
) (types.ResearchRunPlanRefV3, error) {
	return types.ResearchRunPlanRefV3{}, r.err
}

func (r researchV3FailingRuntime) ExecuteStep(
	context.Context, types.RunIdentity, types.ResearchRunSnapshotRefV3,
	types.ResearchRunPlanRefV3, int, string,
) (ResearchStepReceiptV3, error) {
	return ResearchStepReceiptV3{}, r.err
}

func (r researchV3FailingRuntime) Synthesize(
	context.Context, types.RunIdentity, types.ResearchRunSnapshotRefV3,
	types.ResearchRunPlanRefV3, string,
) (ResearchBriefRefV3, error) {
	return ResearchBriefRefV3{}, r.err
}

func (r researchV3FailingRuntime) Deliver(
	context.Context, types.RunIdentity, types.ResearchRunSnapshotRefV3,
	types.ResearchRunPlanRefV3, ResearchBriefRefV3, string,
) (ResearchDeliveryReceiptV3, error) {
	return ResearchDeliveryReceiptV3{}, r.err
}

func (s *researchWorkflowV3Stubs) record(call string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, call)
}

func (s *researchWorkflowV3Stubs) snapshot(
	ctx context.Context, p ResearchScheduledInputV3,
) types.ResearchRunSnapshotRefV3 {
	info := activity.GetInfo(ctx)
	digest := strings.Repeat("a", 64)
	ref, err := types.SealResearchRunSnapshotRefV3(types.ResearchRunSnapshotRefV3{
		SnapshotID: 11, TemporalWorkflowID: info.WorkflowExecution.ID,
		TemporalRunID: info.WorkflowExecution.RunID,
		RunKind:       types.RunSnapshotKindScheduled, TenantID: p.TenantID,
		UserID: p.UserID, TaskID: p.TaskID, DefinitionVersion: 1,
		DefinitionDigest: digest, CapabilityCatalogDigest: digest,
		ToolPolicyDigest: digest, PromptPolicyDigest: digest,
		ModelPolicyDigest: digest, QuotaPolicyDigest: digest,
		PlannerBudget: types.PlannerBudget{MaxPlannerRounds: 8, MaxToolCalls: 16,
			MaxTokens: 32768, MaxCostMicroUSD: 1_000_000, DurationMs: 300_000},
		HistoryThroughUTC: "2026-08-01T12:34:56Z", PayloadDigest: digest,
	})
	if err != nil {
		panic(err)
	}
	return ref
}

func (s *researchWorkflowV3Stubs) register(env *testsuite.TestWorkflowEnvironment) {
	reg := func(name string, fn any) {
		env.RegisterActivityWithOptions(fn, activity.RegisterOptions{Name: name})
	}
	reg("PrepareResearchRunV3", func(ctx context.Context, p ResearchScheduledInputV3) (PrepareResearchRunV3Result, error) {
		s.record("prepare")
		return PrepareResearchRunV3Result{
			Authorized: true, DeliveryAllowed: s.deliveryAllowed,
			Snapshot: s.snapshot(ctx, p),
		}, nil
	})
	reg("PlanResearchRunV3", func(_ context.Context, in ResearchRunV3Input) (PlanResearchRunV3Result, error) {
		s.record("plan")
		plan, err := types.SealResearchRunPlanRefV3(types.ResearchRunPlanRefV3{
			PlanID: 22, RunSnapshotID: in.Snapshot.SnapshotID,
			TemporalWorkflowID: in.Snapshot.TemporalWorkflowID,
			TemporalRunID:      in.Snapshot.TemporalRunID, TenantID: in.TenantID,
			UserID: in.UserID, TaskID: in.TaskID,
			DefinitionDigest:        in.Snapshot.DefinitionDigest,
			CapabilityCatalogDigest: in.Snapshot.CapabilityCatalogDigest,
			ToolPolicyDigest:        in.Snapshot.ToolPolicyDigest,
			PlanDigest:              strings.Repeat("b", 64),
			StepCount:               2,
		})
		return PlanResearchRunV3Result{Plan: plan}, err
	})
	reg("ExecuteResearchStepV3", func(_ context.Context, in ExecuteResearchStepV3Input) (ResearchStepReceiptV3, error) {
		s.record("step:" + string(rune('0'+in.Ordinal)))
		if code, failed := s.stepFailures[in.Ordinal]; failed {
			return ResearchStepReceiptV3{
				StepID: int64(30 + in.Ordinal), Ordinal: in.Ordinal,
				Phase: "indeterminate", InvocationID: "invocation",
				ToolName: "web_search", RequestDigest: strings.Repeat("c", 64),
				ErrorCode: code,
			}, nil
		}
		return ResearchStepReceiptV3{
			StepID: int64(30 + in.Ordinal), Ordinal: in.Ordinal,
			Phase:        "completed",
			InvocationID: "invocation", ToolName: "web_search",
			RequestDigest: strings.Repeat("c", 64),
			ResultDigest:  strings.Repeat("d", 64), EvidenceID: int64(40 + in.Ordinal),
		}, nil
	})
	reg("SynthesizeResearchBriefV3", func(_ context.Context, in SynthesizeResearchBriefV3Input) (ResearchBriefRefV3, error) {
		s.record("synthesize")
		threshold := s.threshold
		if threshold == "" {
			threshold = "major_updates_only"
		}
		significance := s.significance
		if significance == "" {
			significance = types.ResearchBriefSignificanceNoneV3
		}
		deliver := significance == types.ResearchBriefSignificanceMajorV3 ||
			(threshold == "all_qualified_updates" &&
				significance == types.ResearchBriefSignificanceQualifiedV3)
		decision := types.ResearchBriefDecisionQuietV3
		if deliver {
			decision = types.ResearchBriefDecisionDeliverV3
		}
		return types.SealResearchBriefRefV3(types.ResearchBriefRefV3{
			BriefID: 50, RunSnapshotID: in.Snapshot.SnapshotID,
			PlanID: in.Plan.PlanID, TenantID: in.TenantID,
			UserID: in.UserID, TaskID: in.TaskID,
			TemporalWorkflowID: in.Snapshot.TemporalWorkflowID,
			TemporalRunID:      in.Snapshot.TemporalRunID,
			DefinitionDigest:   in.Snapshot.DefinitionDigest,
			PlanDigest:         in.Plan.PlanDigest, RequestDigest: strings.Repeat("2", 64),
			BriefDigest: strings.Repeat("e", 64), EvidenceDigest: strings.Repeat("f", 64),
			HistoryDigest: strings.Repeat("3", 64), NotificationThreshold: threshold,
			Significance: significance, Decision: decision, DeliveryRequired: deliver,
		})
	})
	reg("DeliverResearchBriefV3", func(_ context.Context, in DeliverResearchBriefV3Input) (ResearchDeliveryReceiptV3, error) {
		s.record("deliver")
		return ResearchDeliveryReceiptV3{
			DeliveryID: 60, BriefID: in.Brief.BriefID,
			ReceiptDigest: strings.Repeat("1", 64),
		}, nil
	})
}

func TestResearchWorkflowV3ContinuesAfterSealedToolFailure(t *testing.T) {
	for _, test := range []struct {
		name         string
		stepFailures map[int]string
		want         []string
	}{
		{
			name:         "partial evidence still reaches synthesis",
			stepFailures: map[int]string{0: "provider_outcome_uncertain"},
			want:         []string{"prepare", "plan", "step:0", "step:1", "synthesize"},
		},
		{
			name: "zero evidence reaches unknown synthesis",
			stepFailures: map[int]string{
				0: "provider_outcome_uncertain", 1: "provider_rejected",
			},
			want: []string{"prepare", "plan", "step:0", "step:1", "synthesize"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			env.SetStartWorkflowOptions(client.StartWorkflowOptions{
				ID: "wf-research-v3-tool-failure-" + strings.ReplaceAll(test.name, " ", "-"),
			})
			stubs := &researchWorkflowV3Stubs{
				stepFailures: test.stepFailures, deliveryAllowed: false,
			}
			stubs.register(env)
			env.ExecuteWorkflow(ResearchShadowWorkflowV3, ResearchShadowInputV3{
				TenantID: 7, UserID: 42, TaskID: "task-v3",
			})
			if err := env.GetWorkflowError(); err != nil {
				t.Fatal(err)
			}
			stubs.mu.Lock()
			got := append([]string(nil), stubs.calls...)
			stubs.mu.Unlock()
			if strings.Join(got, ",") != strings.Join(test.want, ",") {
				t.Fatalf("calls=%v want=%v", got, test.want)
			}
		})
	}
}

func TestResearchWorkflowV3UsesSealedDeliveryDecision(t *testing.T) {
	for _, test := range []struct {
		name         string
		threshold    string
		significance types.ResearchBriefSignificanceV3
		want         []string
	}{
		{name: "quiet", want: []string{"prepare", "plan", "step:0", "step:1", "synthesize"}},
		{name: "qualified", threshold: "all_qualified_updates",
			significance: types.ResearchBriefSignificanceQualifiedV3,
			want:         []string{"prepare", "plan", "step:0", "step:1", "synthesize", "deliver"}},
		{name: "major", significance: types.ResearchBriefSignificanceMajorV3,
			want: []string{"prepare", "plan", "step:0", "step:1", "synthesize", "deliver"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: "wf-research-v3-" + test.name})
			stubs := &researchWorkflowV3Stubs{
				threshold: test.threshold, significance: test.significance,
				deliveryAllowed: true,
			}
			stubs.register(env)
			env.ExecuteWorkflow(ResearchScheduledWorkflowV3, ResearchScheduledInputV3{
				TenantID: 7, UserID: 42, TaskID: "task-v3",
				ActionAuthorizationToken: strings.Repeat("a", 64),
			})
			if err := env.GetWorkflowError(); err != nil {
				t.Fatal(err)
			}
			stubs.mu.Lock()
			got := append([]string(nil), stubs.calls...)
			stubs.mu.Unlock()
			if strings.Join(got, ",") != strings.Join(test.want, ",") {
				t.Fatalf("calls=%v want=%v", got, test.want)
			}
		})
	}
}

func TestResearchWorkflowV3HardNoDeliveryOverridesMajorBrief(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: "wf-research-v3-hard-no-delivery"})
	stubs := &researchWorkflowV3Stubs{
		significance: types.ResearchBriefSignificanceMajorV3,
		// The synthesized Brief deliberately requires delivery. This durable
		// shadow-run authority bit must prevent the delivery Activity itself.
		deliveryAllowed: false,
	}
	stubs.register(env)
	env.ExecuteWorkflow(ResearchScheduledWorkflowV3, ResearchScheduledInputV3{
		TenantID: 7, UserID: 42, TaskID: "task-v3",
		ActionAuthorizationToken: strings.Repeat("b", 64),
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	stubs.mu.Lock()
	got := append([]string(nil), stubs.calls...)
	stubs.mu.Unlock()
	want := []string{"prepare", "plan", "step:0", "step:1", "synthesize"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("hard-no-delivery calls=%v want=%v", got, want)
	}
}

func TestResearchShadowWorkflowV3NeverDeliversEvenIfCoordinatorAllows(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: "wf-research-v3-shadow"})
	stubs := &researchWorkflowV3Stubs{
		significance:    types.ResearchBriefSignificanceMajorV3,
		deliveryAllowed: true,
	}
	stubs.register(env)
	env.ExecuteWorkflow(ResearchShadowWorkflowV3, ResearchShadowInputV3{
		TenantID: 7, UserID: 42, TaskID: "task-v3",
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	stubs.mu.Lock()
	got := append([]string(nil), stubs.calls...)
	stubs.mu.Unlock()
	want := []string{"prepare", "plan", "step:0", "step:1", "synthesize"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("shadow calls=%v want=%v", got, want)
	}
}

func TestResearchScheduledWorkflowV3UsesIndependentAuthorizedWire(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: "wf-research-v3-formal"})
	stubs := &researchWorkflowV3Stubs{
		significance: types.ResearchBriefSignificanceMajorV3, deliveryAllowed: true,
	}
	stubs.register(env)
	env.ExecuteWorkflow(ResearchScheduledWorkflowV3, ResearchScheduledInputV3{
		TenantID: 7, UserID: 42, TaskID: "task-v3",
		ActionAuthorizationToken: strings.Repeat("a", 64),
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	stubs.mu.Lock()
	got := append([]string(nil), stubs.calls...)
	stubs.mu.Unlock()
	want := []string{"prepare", "plan", "step:0", "step:1", "synthesize", "deliver"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("formal V3 calls=%v want=%v", got, want)
	}
}

func TestResearchWorkflowV3InvalidEnvelopeHasNoActivity(t *testing.T) {
	for _, mutate := range []func(*PushParams){
		func(p *PushParams) { p.ExecutionMode = types.ExecutionModeCompiled },
		func(p *PushParams) { p.NLDesc = "must not enter history" },
		func(p *PushParams) { p.Scope.TopN = 1 },
		func(p *PushParams) { p.Scope.SourceIDs = []int64{1} },
	} {
		var suite testsuite.WorkflowTestSuite
		env := suite.NewTestWorkflowEnvironment()
		stubs := new(researchWorkflowV3Stubs)
		stubs.register(env)
		params := PushParams{
			TenantID: 7, UserID: 42, RunKind: PushRunKindScheduled,
			ExecutionMode:  types.ExecutionModeDiscoverAtRun,
			RuntimeVersion: ResearchRuntimeV3, ScheduleID: "task-v3",
		}
		mutate(&params)
		env.ExecuteWorkflow(PushPipelineWorkflow, params)
		if err := env.GetWorkflowError(); err == nil {
			t.Fatal("invalid research envelope passed")
		}
		stubs.mu.Lock()
		if len(stubs.calls) != 0 {
			t.Fatalf("invalid envelope scheduled activities: %v", stubs.calls)
		}
		stubs.mu.Unlock()
	}
}

func TestResearchWorkflowV3FailureHistorySanitizesCoordinatorError(t *testing.T) {
	const secret = "sentinel-secret-in-provider-response"
	runtimeErr := types.NewAppError(types.CodeLLMUnavailable,
		"provider response included "+secret, errors.New("credential="+secret))
	a := &Activities{}
	WithResearchRuntimeV3(researchV3FailingRuntime{err: runtimeErr})(a)

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterActivity(a.PrepareResearchRunV3)
	env.RegisterActivity(a.PlanResearchRunV3)
	env.ExecuteWorkflow(ResearchScheduledWorkflowV3, ResearchScheduledInputV3{
		TenantID: 7, UserID: 42, TaskID: "task-v3",
		ActionAuthorizationToken: strings.Repeat("c", 64),
	})
	err := env.GetWorkflowError()
	if err == nil {
		t.Fatal("coordinator failure unexpectedly passed")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("Temporal failure exposed coordinator payload: %v", err)
	}
	if !strings.Contains(err.Error(), string(types.CodeLLMUnavailable)) {
		t.Fatalf("sanitized failure lost controlled code: %v", err)
	}
}

func TestResearchV3ReceiptBackedStagesHaveOneStoreOnlyRecoveryAttempt(t *testing.T) {
	synthesis := researchV3SynthesisOptions()
	if synthesis.RetryPolicy == nil || synthesis.RetryPolicy.MaximumAttempts != 2 {
		t.Fatalf("synthesis retry policy=%+v", synthesis.RetryPolicy)
	}
	if synthesis.StartToCloseTimeout <= 10*time.Minute {
		t.Fatalf("synthesis timeout %v cannot reach gateway recovery fence",
			synthesis.StartToCloseTimeout)
	}
	recovery := researchV3StoreRecoveryOptions()
	if recovery.RetryPolicy == nil || recovery.RetryPolicy.MaximumAttempts != 2 {
		t.Fatalf("planner/tool recovery policy=%+v", recovery.RetryPolicy)
	}
	if recovery.StartToCloseTimeout <= 10*time.Minute {
		t.Fatalf("recovery timeout %v cannot reach gateway recovery fence",
			recovery.StartToCloseTimeout)
	}
	delivery := researchV3DeliveryOptions()
	if delivery.RetryPolicy == nil || delivery.RetryPolicy.MaximumAttempts != 2 {
		t.Fatalf("delivery retry policy=%+v", delivery.RetryPolicy)
	}
	if delivery.StartToCloseTimeout <= 10*time.Minute {
		t.Fatalf("delivery timeout %v cannot reach receipt recovery fence",
			delivery.StartToCloseTimeout)
	}
}
