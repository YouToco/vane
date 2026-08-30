package task

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/YouToco/vane/server/taskstate"
	"github.com/YouToco/vane/server/types"
)

func TestBuildResearchV3DefinitionEditTargetReplacesCompleteOwnerSurfaceOnly(
	t *testing.T,
) {
	base, err := taskstate.BuildApprovedDefinitionV3(
		taskstate.ApprovedDefinitionInputV3{
			TenantID: 7, UserID: 42, TaskID: "task-v1-edit-v3",
			TaskName: "旧任务", TaskManual: "监控旧目标",
			SpecJSON:      json.RawMessage(`{"cron":"0 9 * * *","tz":"Asia/Shanghai"}`),
			ExecutionMode: types.ExecutionModeDiscoverAtRun,
			Notification: taskstate.NotificationPolicyV3{
				MinimumSignificance: taskstate.NotificationThresholdMajorV3,
				SuppressEmpty:       true,
			},
			Output: taskstate.OutputPreferenceV3{
				Language:     taskstate.OutputLanguageZhCNV3,
				Format:       taskstate.OutputFormatExecutiveBriefV3,
				Instructions: "旧格式", IncludeEvidenceLinks: true,
			},
			PlannerBudget: types.PlannerBudget{
				MaxPlannerRounds: 4, MaxToolCalls: 8, MaxTokens: 4096,
				MaxCostMicroUSD: 10000, DurationMs: 60000,
			},
			DeliveryPolicy:     taskstate.DeliveryPolicyOwnerFeishu,
			TenantBudgetPolicy: taskstate.BudgetPolicyInheritTenantQuota,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	target, err := BuildResearchV3DefinitionEditTarget(base,
		ResearchV3DefinitionEditInput{
			TenantID: 7, UserID: 42, TaskID: "task-v1-edit-v3",
			TaskName:   "Kimi 套餐监控",
			TaskManual: "检查官方定价页，与历史结果比较；无重大更新不推送。",
			SpecJSON:   json.RawMessage(`{"every_seconds":7200,"tz":"Asia/Shanghai"}`),
			Notification: taskstate.NotificationPolicyV3{
				MinimumSignificance: taskstate.NotificationThresholdQualifiedV3,
				SuppressEmpty:       true,
			},
			Output: taskstate.OutputPreferenceV3{
				Language:     taskstate.OutputLanguageAutoV3,
				Format:       taskstate.OutputFormatConciseBriefV3,
				Instructions: "先结论后证据", IncludeEvidenceLinks: true,
			},
		})
	if err != nil {
		t.Fatal(err)
	}
	if target.TaskName != "Kimi 套餐监控" ||
		target.TaskManual == base.TaskManual || bytesEqual(target.SpecJSON, base.SpecJSON) {
		t.Fatalf("target owner surface was not replaced: %+v", target)
	}
	if target.TenantID != base.TenantID || target.UserID != base.UserID ||
		target.TaskID != base.TaskID || target.ExecutionMode != base.ExecutionMode ||
		target.DeliveryPolicy != base.DeliveryPolicy ||
		target.TenantBudgetPolicy != base.TenantBudgetPolicy {
		t.Fatalf("trusted V3 policy changed: base=%+v target=%+v", base, target)
	}
	if target.PlannerBudget != taskstate.ResearchPlannerBudgetPolicy() {
		t.Fatalf("edit did not refresh planner budget to deployment policy: %+v",
			target.PlannerBudget)
	}
	raw, err := taskstate.EncodeApprovedDefinitionV3(target)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{
		`"tool_calls":`, `"fetch_target":`, `"schedule_sources":`, `"source_catalog":`,
	} {
		if strings.Contains(string(raw), forbidden) {
			t.Fatalf("target definition contains retired state %q: %s", forbidden, raw)
		}
	}
}

func TestBuildResearchV3DefinitionEditTargetRequiresReprepareForScopedManualChange(t *testing.T) {
	base, err := taskstate.BuildApprovedDefinitionV3(taskstate.ApprovedDefinitionInputV3{
		TenantID: 7, UserID: 42, TaskID: "scoped-edit", TaskName: "scope",
		TaskManual:    "exact approved manual",
		SpecJSON:      json.RawMessage(`{"every_seconds":7200,"tz":"UTC"}`),
		ExecutionMode: types.ExecutionModeDiscoverAtRun,
		Notification: taskstate.NotificationPolicyV3{
			MinimumSignificance: taskstate.NotificationThresholdMajorV3, SuppressEmpty: true,
		},
		Output: taskstate.OutputPreferenceV3{Language: taskstate.OutputLanguageZhCNV3,
			Format: taskstate.OutputFormatExecutiveBriefV3, IncludeEvidenceLinks: true},
		PlannerBudget: types.PlannerBudget{MaxPlannerRounds: 4, MaxToolCalls: 8,
			MaxTokens: 4096, MaxCostMicroUSD: 10000, DurationMs: 60000},
		DeliveryPolicy:     taskstate.DeliveryPolicyOwnerFeishu,
		TenantBudgetPolicy: taskstate.BudgetPolicyInheritTenantQuota,
		ResearchScope: &taskstate.ResearchScopeV3{Mode: taskstate.ResearchScopeEventWindowV3,
			LookbackSeconds: taskstate.ResearchScopeWeekSecondsV3},
	})
	if err != nil {
		t.Fatal(err)
	}
	input := ResearchV3DefinitionEditInput{TenantID: 7, UserID: 42, TaskID: "scoped-edit",
		TaskName: "renamed", TaskManual: base.TaskManual, SpecJSON: base.SpecJSON,
		Notification: base.Notification, Output: base.Output}
	unchanged, err := BuildResearchV3DefinitionEditTarget(base, input)
	if err != nil || !reflect.DeepEqual(unchanged.ResearchScope, base.ResearchScope) {
		t.Fatalf("non-manual edit lost scope: scope=%+v err=%v", unchanged.ResearchScope, err)
	}
	input.TaskManual = "changed manual"
	if _, err := BuildResearchV3DefinitionEditTarget(base, input); err == nil ||
		!strings.Contains(err.Error(), "explicit operator prepare") {
		t.Fatalf("scoped manual edit err=%v", err)
	}
}

type researchV3RecoveryRestartStoreTest struct {
	ResearchTaskDefinitionEditStoreV3
	queue       []*types.TaskDefinitionEditOperation
	claimed     []string
	failNext    bool
	restartFail error
}

func (s *researchV3RecoveryRestartStoreTest) ClaimStaleResearchTaskDefinitionEditOperationV3(
	_ context.Context, _ time.Time, leaseOwner string, _ time.Duration,
) (*types.TaskDefinitionEditOperation, error) {
	if s.failNext && len(s.claimed) > 0 {
		s.failNext = false
		return nil, s.restartFail
	}
	if len(s.queue) == 0 {
		return nil, nil
	}
	op := *s.queue[0]
	s.queue = s.queue[1:]
	op.LeaseOwner = leaseOwner
	op.Fence++
	op.Attempt++
	s.claimed = append(s.claimed, op.ID)
	return &op, nil
}

func (s *researchV3RecoveryRestartStoreTest) LoadResearchTaskDefinitionEditOperationV3(
	_ context.Context, scope types.TaskDefinitionEditScope,
) (*types.TaskDefinitionEditOperation, error) {
	return &types.TaskDefinitionEditOperation{
		ID: scope.ID, Protocol: types.TaskDefinitionEditProtocolResearchV3,
		TenantID: scope.TenantID, UserID: scope.UserID,
		TargetTenantID: scope.TargetTenantID, TargetUserID: scope.TargetUserID,
		TaskID: scope.TaskID, Status: types.TaskDefinitionEditOperationStatusCompleted,
	}, nil
}

type researchV3RecoveryRestartSchedulerTest struct {
	ResearchTaskDefinitionEditSchedulerV3
}

func TestResearchTaskDefinitionEditRecoveryRestartUsesDurableClaimOrder(t *testing.T) {
	restartErr := errors.New("simulated coordinator restart")
	store := &researchV3RecoveryRestartStoreTest{
		restartFail: restartErr,
		queue: []*types.TaskDefinitionEditOperation{
			{ID: "low-tenant-oldest", Protocol: types.TaskDefinitionEditProtocolResearchV3,
				TenantID: 1, UserID: 11, TargetTenantID: 1, TargetUserID: 11,
				TaskID: "low", Status: types.TaskDefinitionEditOperationStatusExecuting,
				Phase: types.TaskDefinitionEditPhaseProposalSealed, LeaseOwner: "lost", Fence: 1, Attempt: 1},
			{ID: "high-tenant-next", Protocol: types.TaskDefinitionEditProtocolResearchV3,
				TenantID: 99, UserID: 22, TargetTenantID: 99, TargetUserID: 22,
				TaskID: "high", Status: types.TaskDefinitionEditOperationStatusExecuting,
				Phase: types.TaskDefinitionEditPhaseProposalSealed, LeaseOwner: "lost", Fence: 1, Attempt: 1},
		},
	}
	first := NewResearchTaskDefinitionEditCoordinatorV3(
		store, &researchV3RecoveryRestartSchedulerTest{}, nil)
	// The first durable claim succeeds; the following claim simulates process
	// loss. A new coordinator must continue with the next database-owned row.
	store.failNext = true
	if err := first.RecoverStaleOnceV3(t.Context()); !errors.Is(err, restartErr) {
		t.Fatalf("first recovery err=%v, want restart", err)
	}
	second := NewResearchTaskDefinitionEditCoordinatorV3(
		store, &researchV3RecoveryRestartSchedulerTest{}, nil)
	if err := second.RecoverStaleOnceV3(t.Context()); err != nil {
		t.Fatalf("restarted recovery: %v", err)
	}
	if !reflect.DeepEqual(store.claimed, []string{"low-tenant-oldest", "high-tenant-next"}) {
		t.Fatalf("durable claim order=%v", store.claimed)
	}
}

func TestResearchTaskDefinitionEditProposalMatchesV3AdoptsResponseLossStates(t *testing.T) {
	expires := time.Now().UTC().Truncate(time.Microsecond)
	p := types.CreateResearchTaskDefinitionEditOperationV3Params{
		ID: "edit-v3-response-loss", TenantID: 7, UserID: 42, TaskID: "task-v3",
		SessionID: 9, ExpiresAt: expires, BaseVersion: 1, TargetVersion: 2,
		BaseDefinition: []byte("base"), TargetDefinition: []byte("target"),
		PreparedEdit: []byte("prepared"), BaseSnapshot: []byte("snapshot"),
	}
	op := &types.TaskDefinitionEditOperation{
		Protocol: types.TaskDefinitionEditProtocolResearchV3, ID: p.ID,
		TenantID: p.TenantID, UserID: p.UserID, TargetTenantID: p.TenantID,
		TargetUserID: p.UserID, TaskID: p.TaskID, SessionID: p.SessionID,
		ExpiresAt: p.ExpiresAt, BaseDefinitionVersion: p.BaseVersion,
		TargetDefinitionVersion: p.TargetVersion, BaseDefinition: p.BaseDefinition,
		TargetDefinition: p.TargetDefinition, PreparedEdit: p.PreparedEdit,
		BaseSnapshot: p.BaseSnapshot,
	}
	// A real executor retry derives a fresh local TTL. Expiry is not part of the
	// immutable owner proposal, so response-loss convergence must adopt the
	// first persisted operation without extending or comparing its lease.
	p.ExpiresAt = expires.Add(17 * time.Minute)
	for _, status := range []types.TaskDefinitionEditOperationStatus{
		types.TaskDefinitionEditOperationStatusPending,
		types.TaskDefinitionEditOperationStatusExecuting,
		types.TaskDefinitionEditOperationStatusCompleted,
	} {
		op.Status = status
		if !researchTaskDefinitionEditProposalMatchesV3(op, p) {
			t.Fatalf("exact %s response-loss state was not adopted", status)
		}
	}
	op.Status = types.TaskDefinitionEditOperationStatusBlocked
	if researchTaskDefinitionEditProposalMatchesV3(op, p) {
		t.Fatal("blocked operation was adopted as a successful create response-loss replay")
	}
	op.Status = types.TaskDefinitionEditOperationStatusCompleted
	op.TargetDefinition = []byte("different")
	if researchTaskDefinitionEditProposalMatchesV3(op, p) {
		t.Fatal("completed operation with different immutable proposal was adopted")
	}
}

func TestBuildResearchV3DefinitionEditTargetFailsClosedOnScopeOrPartialTarget(
	t *testing.T,
) {
	base := validResearchV3DefinitionForEditTest(t)
	valid := ResearchV3DefinitionEditInput{
		TenantID: base.TenantID, UserID: base.UserID, TaskID: base.TaskID,
		TaskName: "新任务", TaskManual: "完整新手册",
		SpecJSON:     json.RawMessage(`{"cron":"15 9 * * *","tz":"Asia/Shanghai"}`),
		Notification: base.Notification, Output: base.Output,
	}
	wrongOwner := valid
	wrongOwner.UserID++
	if _, err := BuildResearchV3DefinitionEditTarget(base, wrongOwner); err == nil {
		t.Fatal("cross-user native V3 edit was accepted")
	}
	partial := valid
	partial.TaskManual = ""
	if _, err := BuildResearchV3DefinitionEditTarget(base, partial); err == nil {
		t.Fatal("partial native V3 edit inherited an omitted manual")
	}
}

func TestApplyResearchV3DefinitionChangesPreservesUnmentionedOwnerPolicy(t *testing.T) {
	base := validResearchV3DefinitionForEditTest(t)
	manual := "检查官方原文并与历史证据比较；无重大更新不推送。"
	target, err := ApplyResearchV3DefinitionChanges(base,
		ResearchV3DefinitionChanges{TaskManual: &manual})
	if err != nil {
		t.Fatal(err)
	}
	if target.TaskManual != manual || target.TaskName != base.TaskName ||
		!bytesEqual(target.SpecJSON, base.SpecJSON) ||
		!reflect.DeepEqual(target.Notification, base.Notification) ||
		!reflect.DeepEqual(target.Output, base.Output) ||
		target.DeliveryPolicy != base.DeliveryPolicy ||
		target.TenantBudgetPolicy != base.TenantBudgetPolicy {
		t.Fatalf("unmentioned V3 policy changed: base=%+v target=%+v", base, target)
	}
	if target.PlannerBudget != taskstate.ResearchPlannerBudgetPolicy() {
		t.Fatalf("edit did not refresh planner budget to deployment policy: %+v",
			target.PlannerBudget)
	}
	if _, err := ApplyResearchV3DefinitionChanges(
		base, ResearchV3DefinitionChanges{}); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("empty changes err=%v, want validation", err)
	}
	empty := ""
	if _, err := ApplyResearchV3DefinitionChanges(base,
		ResearchV3DefinitionChanges{TaskManual: &empty}); err == nil {
		t.Fatal("explicit invalid manual was inherited instead of rejected")
	}
}

type researchV3PrepareChangesStoreTest struct {
	ResearchTaskDefinitionEditStoreV3
	existing    *types.TaskDefinitionEditOperation
	existingErr error
	basis       *types.ResearchTaskDefinitionEditBasisV3
}

func (s *researchV3PrepareChangesStoreTest) LoadResearchTaskDefinitionEditOperationV3(
	context.Context, types.TaskDefinitionEditScope,
) (*types.TaskDefinitionEditOperation, error) {
	return s.existing, s.existingErr
}

func (s *researchV3PrepareChangesStoreTest) LoadResearchTaskDefinitionEditBasisV3(
	context.Context, int64, int64, string,
) (*types.ResearchTaskDefinitionEditBasisV3, error) {
	return s.basis, nil
}

func TestResearchTaskDefinitionEditPrepareChangesClassifiesDefinitePresealFailures(
	t *testing.T,
) {
	base := validResearchV3DefinitionForEditTest(t)
	basePayload, err := taskstate.EncodeApprovedDefinitionV3(base)
	if err != nil {
		t.Fatal(err)
	}
	basis := &types.ResearchTaskDefinitionEditBasisV3{
		TenantID: base.TenantID, UserID: base.UserID, TaskID: base.TaskID,
		DefinitionVersion: 1, DefinitionPayload: basePayload,
	}
	manual := base.TaskManual
	readFailure := types.NewAppError(
		types.CodeDBConnLost, "operation lookup unavailable", errors.New("connection lost"))
	unknownStore := &researchV3PrepareChangesStoreTest{existingErr: readFailure}
	coordinator := NewResearchTaskDefinitionEditCoordinatorV3(
		unknownStore, &researchV3RecoveryRestartSchedulerTest{}, nil)
	_, err = coordinator.PrepareChanges(t.Context(),
		ResearchTaskDefinitionEditChangesInputV3{
			ActionID: "manage-task-v1-lookup-unknown", TenantID: base.TenantID,
			UserID: base.UserID, TaskID: base.TaskID, SessionID: 9,
			Changes:   ResearchV3DefinitionChanges{TaskManual: &manual},
			ExpiresAt: time.Now().Add(time.Hour),
		})
	if err == nil || errors.Is(err, ErrResearchTaskDefinitionEditNotExecuted) {
		t.Fatalf("unavailable replay lookup was falsely classified not-executed: %v", err)
	}

	noOpStore := &researchV3PrepareChangesStoreTest{
		existingErr: types.ErrNotFound, basis: basis,
	}
	coordinator = NewResearchTaskDefinitionEditCoordinatorV3(
		noOpStore, &researchV3RecoveryRestartSchedulerTest{}, nil)
	_, err = coordinator.PrepareChanges(t.Context(),
		ResearchTaskDefinitionEditChangesInputV3{
			ActionID: "manage-task-v1-no-op", TenantID: base.TenantID,
			UserID: base.UserID, TaskID: base.TaskID, SessionID: 9,
			Changes:   ResearchV3DefinitionChanges{TaskManual: &manual},
			ExpiresAt: time.Now().Add(time.Hour),
		})
	if !errors.Is(err, ErrResearchTaskDefinitionEditNotExecuted) {
		t.Fatalf("no-op classification=%v", err)
	}

	different := "不同的变更"
	target, err := ApplyResearchV3DefinitionChanges(base,
		ResearchV3DefinitionChanges{TaskManual: &different})
	if err != nil {
		t.Fatal(err)
	}
	targetPayload, err := taskstate.EncodeApprovedDefinitionV3(target)
	if err != nil {
		t.Fatal(err)
	}
	conflictStore := &researchV3PrepareChangesStoreTest{existing: &types.TaskDefinitionEditOperation{
		ID:       "manage-task-v1-conflict",
		Protocol: types.TaskDefinitionEditProtocolResearchV3,
		TenantID: base.TenantID, UserID: base.UserID,
		TargetTenantID: base.TenantID, TargetUserID: base.UserID,
		TaskID: base.TaskID, SessionID: 9,
		Status:         types.TaskDefinitionEditOperationStatusCompleted,
		BaseDefinition: basePayload, TargetDefinition: targetPayload,
	}}
	coordinator = NewResearchTaskDefinitionEditCoordinatorV3(
		conflictStore, &researchV3RecoveryRestartSchedulerTest{}, nil)
	replayed, err := coordinator.PrepareChanges(t.Context(),
		ResearchTaskDefinitionEditChangesInputV3{
			ActionID: conflictStore.existing.ID, TenantID: base.TenantID,
			UserID: base.UserID, TaskID: base.TaskID, SessionID: 9,
			Changes:   ResearchV3DefinitionChanges{TaskManual: &different},
			ExpiresAt: time.Now().Add(24 * time.Hour),
		})
	if err != nil || replayed != conflictStore.existing {
		t.Fatalf("terminal whole-tool replay=%+v err=%v", replayed, err)
	}
	other := "另一个变更"
	_, err = coordinator.PrepareChanges(t.Context(),
		ResearchTaskDefinitionEditChangesInputV3{
			ActionID: conflictStore.existing.ID, TenantID: base.TenantID,
			UserID: base.UserID, TaskID: base.TaskID, SessionID: 9,
			Changes:   ResearchV3DefinitionChanges{TaskManual: &other},
			ExpiresAt: time.Now().Add(2 * time.Hour),
		})
	if !errors.Is(err, ErrResearchTaskDefinitionEditNotExecuted) ||
		!errors.Is(err, types.ErrConflict) {
		t.Fatalf("same action conflict classification=%v", err)
	}
}

func TestResearchTaskDefinitionEditSealClassificationKeepsResponseLossIndeterminate(
	t *testing.T,
) {
	for _, err := range []error{
		context.DeadlineExceeded,
		types.NewAppError(types.CodeDBConnLost, "commit response lost", errors.New("lost")),
	} {
		if researchTaskDefinitionEditSealDefinitelyRejected(err) {
			t.Fatalf("outcome-unknown seal was classified rejected: %v", err)
		}
	}
	for _, err := range []error{types.ErrValidation, types.ErrConflict, types.ErrNotFound} {
		if !researchTaskDefinitionEditSealDefinitelyRejected(err) {
			t.Fatalf("definite seal rejection was classified unknown: %v", err)
		}
	}
}

func TestResearchTaskDefinitionEditChangesReplayMatchesOriginalBaseAfterHeadAdvance(
	t *testing.T,
) {
	base := validResearchV3DefinitionForEditTest(t)
	manual := "只查官方原文并与历史证据比较；无重大更新不推送。"
	changes := ResearchV3DefinitionChanges{TaskManual: &manual}
	target, err := ApplyResearchV3DefinitionChanges(base, changes)
	if err != nil {
		t.Fatal(err)
	}
	basePayload, err := taskstate.EncodeApprovedDefinitionV3(base)
	if err != nil {
		t.Fatal(err)
	}
	targetPayload, err := taskstate.EncodeApprovedDefinitionV3(target)
	if err != nil {
		t.Fatal(err)
	}
	op := &types.TaskDefinitionEditOperation{
		ID:       "manage-task-v1-terminal-replay",
		Protocol: types.TaskDefinitionEditProtocolResearchV3,
		TenantID: base.TenantID, UserID: base.UserID,
		TargetTenantID: base.TenantID, TargetUserID: base.UserID,
		TaskID: base.TaskID, SessionID: 99,
		Status:         types.TaskDefinitionEditOperationStatusCompleted,
		BaseDefinition: basePayload, TargetDefinition: targetPayload,
		ExpiresAt: time.Now().Add(time.Hour),
	}
	in := ResearchTaskDefinitionEditChangesInputV3{
		ActionID: op.ID, TenantID: base.TenantID, UserID: base.UserID,
		TaskID: base.TaskID, SessionID: op.SessionID, Changes: changes,
		ExpiresAt: op.ExpiresAt.Add(24 * time.Hour),
	}
	if !researchTaskDefinitionEditChangesReplayMatchesV3(op, in) {
		t.Fatal("terminal replay with a fresh local TTL did not adopt original proposal")
	}
	different := "监控不同目标"
	in.Changes.TaskManual = &different
	if researchTaskDefinitionEditChangesReplayMatchesV3(op, in) {
		t.Fatal("same action ID with different owner changes was adopted")
	}
}

func validResearchV3DefinitionForEditTest(t *testing.T) taskstate.ApprovedDefinitionV3 {
	t.Helper()
	definition, err := taskstate.BuildApprovedDefinitionV3(
		taskstate.ApprovedDefinitionInputV3{
			TenantID: 7, UserID: 42, TaskID: "task-v1-edit-v3",
			TaskName: "旧任务", TaskManual: "旧手册",
			SpecJSON:      json.RawMessage(`{"cron":"0 9 * * *","tz":"Asia/Shanghai"}`),
			ExecutionMode: types.ExecutionModeDiscoverAtRun,
			Notification: taskstate.NotificationPolicyV3{
				MinimumSignificance: taskstate.NotificationThresholdMajorV3,
				SuppressEmpty:       true,
			},
			Output: taskstate.OutputPreferenceV3{
				Language:             taskstate.OutputLanguageZhCNV3,
				Format:               taskstate.OutputFormatExecutiveBriefV3,
				IncludeEvidenceLinks: true,
			},
			PlannerBudget: types.PlannerBudget{
				MaxPlannerRounds: 4, MaxToolCalls: 8, MaxTokens: 4096,
				MaxCostMicroUSD: 10000, DurationMs: 60000,
			},
			DeliveryPolicy:     taskstate.DeliveryPolicyOwnerFeishu,
			TenantBudgetPolicy: taskstate.BudgetPolicyInheritTenantQuota,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	return definition
}

func bytesEqual(left, right []byte) bool {
	return string(left) == string(right)
}
