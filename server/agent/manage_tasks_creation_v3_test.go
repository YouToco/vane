package agent

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/YouToco/vane/task"
	"github.com/YouToco/vane/types"
)

type fakeResearchTaskCreationV3Controller struct {
	prepared        task.ResearchV3CreationProposalInput
	proposal        task.CreationProposal
	result          task.CreationResult
	prepareErr      error
	executeErr      error
	executeUserID   int64
	executeActionID string
	receipt         task.CreationReceiptTarget
	prepareCalls    int
	executeCalls    int
}

func (f *fakeResearchTaskCreationV3Controller) PrepareResearchV3(
	_ context.Context,
	in task.ResearchV3CreationProposalInput,
) (task.CreationProposal, error) {
	f.prepareCalls++
	f.prepared = in
	return f.proposal, f.prepareErr
}

func (f *fakeResearchTaskCreationV3Controller) ExecuteResearchV3(
	_ context.Context,
	userID int64,
	actionID string,
	receipt task.CreationReceiptTarget,
) (task.CreationResult, error) {
	f.executeCalls++
	f.executeUserID = userID
	f.executeActionID = actionID
	f.receipt = receipt
	return f.result, f.executeErr
}

func nativeV3AdapterInput() ResearchTaskCreationV3Input {
	sessionID := int64(9)
	return ResearchTaskCreationV3Input{
		ActionID: "manage-task-v1-action", UserID: 42, SessionID: &sessionID,
		Name:     "Kimi 套餐监控",
		Manual:   "检查官方套餐可购买状态，与历史结论交叉核验；无重大更新不推送。",
		Schedule: json.RawMessage(`{"cron":"0 9 * * 1","tz":"Asia/Shanghai"}`),
		Notification: ResearchTaskNotificationV3Input{
			MinimumSignificance: "major_updates_only", SuppressEmpty: true,
		},
		Output: ResearchTaskOutputV3Input{
			Language: "zh-CN", Format: "executive_brief",
			Instructions: "先写结论", IncludeEvidenceLinks: true,
		},
	}
}

func TestResearchTaskCreationV3AdapterMapsOnlyDurableOwnerIntent(t *testing.T) {
	in := nativeV3AdapterInput()
	controller := &fakeResearchTaskCreationV3Controller{
		proposal: task.CreationProposal{ID: in.ActionID, Summary: in.Name},
		result: task.CreationResult{
			OperationID: in.ActionID, TaskID: "task-native-v3",
			Summary: in.Name, Status: types.TaskOperationStatusExecuted,
			ReceiptBound: true, SessionID: in.SessionID,
		},
	}
	executor := NewResearchTaskCreationV3Executor(controller)
	before := time.Now()
	outcome, err := executor.ExecuteResearchTaskCreationV3(t.Context(), in)
	if err != nil {
		t.Fatal(err)
	}
	if controller.prepareCalls != 1 || controller.executeCalls != 1 ||
		controller.prepared.ActionID != in.ActionID ||
		controller.prepared.UserID != in.UserID ||
		controller.prepared.SessionID == nil || *controller.prepared.SessionID != 9 ||
		controller.prepared.TaskName != in.Name ||
		controller.prepared.TaskManual != in.Manual ||
		string(controller.prepared.SpecJSON) != string(in.Schedule) ||
		string(controller.prepared.Notification.MinimumSignificance) != "major_updates_only" ||
		!controller.prepared.Notification.SuppressEmpty ||
		string(controller.prepared.Output.Language) != "zh-CN" ||
		string(controller.prepared.Output.Format) != "executive_brief" ||
		controller.prepared.Output.Instructions != "先写结论" ||
		!controller.prepared.Output.IncludeEvidenceLinks {
		t.Fatalf("native V3 mapping mismatch: %+v", controller.prepared)
	}
	if controller.prepared.ExpiresAt.Before(before.Add(23*time.Hour)) ||
		controller.prepared.ExpiresAt.After(time.Now().Add(25*time.Hour)) {
		t.Fatalf("proposal expiry=%s", controller.prepared.ExpiresAt)
	}
	if controller.executeUserID != in.UserID || controller.executeActionID != in.ActionID ||
		controller.receipt != task.AgentAutoReceiptTarget(in.ActionID) {
		t.Fatalf("execute binding mismatch: user=%d action=%q receipt=%+v",
			controller.executeUserID, controller.executeActionID, controller.receipt)
	}
	if outcome != (ResearchTaskCreationV3Outcome{
		TaskRef: "task-native-v3", TaskName: in.Name, Status: "completed",
	}) {
		t.Fatalf("outcome=%+v", outcome)
	}
}

func TestResearchTaskCreationV3AdapterMapsRecoveryWithoutFalseSuccess(t *testing.T) {
	in := nativeV3AdapterInput()
	controller := &fakeResearchTaskCreationV3Controller{
		proposal: task.CreationProposal{ID: in.ActionID, Summary: in.Name},
		result: task.CreationResult{
			OperationID: in.ActionID, Summary: in.Name,
			Status: types.TaskOperationStatusExecuting, Recovering: true,
			ReceiptBound: true, SessionID: in.SessionID,
		},
	}
	outcome, err := NewResearchTaskCreationV3Executor(controller).
		ExecuteResearchTaskCreationV3(t.Context(), in)
	if err != nil || outcome.Status != "executing" || outcome.TaskRef != "" {
		t.Fatalf("recovery outcome=%+v err=%v", outcome, err)
	}
}

func TestResearchTaskCreationV3AdapterFailsClosedOnIdentityOrControllerError(t *testing.T) {
	in := nativeV3AdapterInput()
	for name, controller := range map[string]*fakeResearchTaskCreationV3Controller{
		"prepare error": {
			prepareErr: errors.New("prepare failed"),
		},
		"proposal drift": {
			proposal: task.CreationProposal{ID: "other", Summary: in.Name},
		},
		"execute error": {
			proposal:   task.CreationProposal{ID: in.ActionID, Summary: in.Name},
			executeErr: errors.New("execute failed"),
		},
		"result drift": {
			proposal: task.CreationProposal{ID: in.ActionID, Summary: in.Name},
			result: task.CreationResult{
				OperationID: "other", Summary: in.Name,
				Status: types.TaskOperationStatusExecuted, TaskID: "task-v3",
			},
		},
		"receipt unbound": {
			proposal: task.CreationProposal{ID: in.ActionID, Summary: in.Name},
			result: task.CreationResult{
				OperationID: in.ActionID, Summary: in.Name,
				Status: types.TaskOperationStatusExecuting, SessionID: in.SessionID,
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NewResearchTaskCreationV3Executor(controller).
				ExecuteResearchTaskCreationV3(t.Context(), in); err == nil {
				t.Fatal("unsafe controller result was accepted")
			}
		})
	}
}
