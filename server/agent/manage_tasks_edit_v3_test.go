package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/YouToco/vane/task"
	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

type fakeResearchTaskDefinitionEditV3Controller struct {
	prepareInput task.ResearchTaskDefinitionEditChangesInputV3
	op           *types.TaskDefinitionEditOperation
	outcome      task.TaskDefinitionEditOutcome
	executeScope types.TaskDefinitionEditScope
	receipt      task.TaskDefinitionEditReceiptTarget
	prepareErr   error
	executeErr   error
}

func (f *fakeResearchTaskDefinitionEditV3Controller) PrepareChanges(
	_ context.Context, in task.ResearchTaskDefinitionEditChangesInputV3,
) (*types.TaskDefinitionEditOperation, error) {
	f.prepareInput = in
	return f.op, f.prepareErr
}

func (f *fakeResearchTaskDefinitionEditV3Controller) Execute(
	_ context.Context, scope types.TaskDefinitionEditScope,
	receipt task.TaskDefinitionEditReceiptTarget,
) (task.TaskDefinitionEditOutcome, error) {
	f.executeScope, f.receipt = scope, receipt
	return f.outcome, f.executeErr
}

func TestResearchTaskDefinitionEditV3ExecutorClassifiesOnlyPresealFailureAsNotExecuted(
	t *testing.T,
) {
	manual := "新手册"
	input := ResearchTaskDefinitionEditV3Input{
		ActionID: "manage-task-v1-1234", TenantID: 7, UserID: 42,
		SessionID: 9, TaskRef: "internal-kimi",
		Changes: ResearchTaskDefinitionEditV3Changes{Manual: &manual},
	}
	preseal := &fakeResearchTaskDefinitionEditV3Controller{prepareErr: fmt.Errorf(
		"%w: invalid schedule", task.ErrResearchTaskDefinitionEditNotExecuted)}
	_, err := NewResearchTaskDefinitionEditV3Executor(preseal).
		ExecuteResearchTaskDefinitionEditV3(t.Context(), input)
	if !errors.Is(err, errResearchTaskDefinitionEditNotExecuted) {
		t.Fatalf("preseal error classification=%v", err)
	}
	unknown := &fakeResearchTaskDefinitionEditV3Controller{
		prepareErr: errors.New("create response lost"),
	}
	_, err = NewResearchTaskDefinitionEditV3Executor(unknown).
		ExecuteResearchTaskDefinitionEditV3(t.Context(), input)
	if err == nil || errors.Is(err, errResearchTaskDefinitionEditNotExecuted) {
		t.Fatalf("post-seal uncertainty misclassified=%v", err)
	}
}

func TestResearchTaskDefinitionEditV3ExecutorBindsExactOwnerAndReceipt(t *testing.T) {
	definition, err := taskstate.BuildApprovedDefinitionV3(
		taskstate.ApprovedDefinitionInputV3{
			TenantID: 7, UserID: 42, TaskID: "internal-kimi",
			TaskName: "Kimi 套餐监控", TaskManual: "新手册",
			SpecJSON:      json.RawMessage(`{"cron":"0 9 * * 1","tz":"Asia/Shanghai"}`),
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
			PlannerBudget: types.PlannerBudget{MaxPlannerRounds: 1, MaxToolCalls: 1,
				MaxTokens: 1, MaxCostMicroUSD: 1, DurationMs: 1},
			DeliveryPolicy:     taskstate.DeliveryPolicyOwnerFeishu,
			TenantBudgetPolicy: taskstate.BudgetPolicyInheritTenantQuota,
		})
	if err != nil {
		t.Fatal(err)
	}
	target, _ := taskstate.EncodeApprovedDefinitionV3(definition)
	op := &types.TaskDefinitionEditOperation{
		ID: "manage-task-v1-1234", Protocol: types.TaskDefinitionEditProtocolResearchV3,
		TenantID: 7, UserID: 42, TargetTenantID: 7, TargetUserID: 42,
		TaskID: "internal-kimi", SessionID: 9, TargetDefinition: target,
	}
	controller := &fakeResearchTaskDefinitionEditV3Controller{
		op: op,
		outcome: task.TaskDefinitionEditOutcome{
			OperationID: op.ID, TaskID: op.TaskID, SessionID: op.SessionID,
			Status:       types.TaskDefinitionEditOperationStatusCompleted,
			ReceiptBound: true,
		},
	}
	executor := NewResearchTaskDefinitionEditV3Executor(controller)
	manual := "新手册"
	outcome, err := executor.ExecuteResearchTaskDefinitionEditV3(t.Context(),
		ResearchTaskDefinitionEditV3Input{
			ActionID: op.ID, TenantID: 7, UserID: 42, SessionID: 9,
			TaskRef: op.TaskID,
			Changes: ResearchTaskDefinitionEditV3Changes{Manual: &manual},
		})
	if err != nil {
		t.Fatal(err)
	}
	if controller.prepareInput.TenantID != 7 ||
		controller.prepareInput.UserID != 42 ||
		controller.prepareInput.TaskID != op.TaskID ||
		controller.prepareInput.Changes.TaskManual == nil ||
		*controller.prepareInput.Changes.TaskManual != manual ||
		controller.receipt.Provider != task.AgentAutoReceiptProvider ||
		controller.receipt.Target != op.ID || controller.executeScope != op.Scope() {
		t.Fatalf("binding mismatch input=%+v scope=%+v receipt=%+v",
			controller.prepareInput, controller.executeScope, controller.receipt)
	}
	if outcome.OperationID != op.ID || outcome.TaskRef != op.TaskID ||
		outcome.TaskName != definition.TaskName || outcome.Status != "completed" {
		t.Fatalf("outcome=%+v", outcome)
	}
}

func TestResearchTaskDefinitionEditV3ExecutorRejectsInvalidTarget(t *testing.T) {
	controller := &fakeResearchTaskDefinitionEditV3Controller{
		op: &types.TaskDefinitionEditOperation{
			ID: "manage-task-v1-1234", Protocol: types.TaskDefinitionEditProtocolResearchV3,
			TenantID: 7, UserID: 42, TargetTenantID: 7, TargetUserID: 42,
			TaskID: "internal-kimi", SessionID: 9,
			TargetDefinition: []byte(`{}`),
		},
	}
	manual := "新手册"
	_, err := NewResearchTaskDefinitionEditV3Executor(controller).
		ExecuteResearchTaskDefinitionEditV3(t.Context(), ResearchTaskDefinitionEditV3Input{
			ActionID: controller.op.ID, TenantID: 7, UserID: 42, SessionID: 9,
			TaskRef: controller.op.TaskID,
			Changes: ResearchTaskDefinitionEditV3Changes{Manual: &manual},
		})
	if err == nil {
		t.Fatal("invalid target/receipt binding was accepted")
	}
}
