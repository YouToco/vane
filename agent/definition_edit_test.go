package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/YouToco/vane/llm"
	"github.com/YouToco/vane/scheduler"
	"github.com/YouToco/vane/task"
	"github.com/YouToco/vane/types"
)

type fakeDefinitionEditController struct {
	proposeCalls []task.DefinitionEditProposalInput
	proposal     task.DefinitionEditProposal
	proposeErr   error

	confirmCalls []fakeDefinitionEditCall
	confirm      task.TaskDefinitionEditOutcome
	confirmErr   error

	cancelCalls []fakeDefinitionEditCall
	cancel      task.TaskDefinitionEditOutcome
	cancelErr   error
}

type fakeDefinitionEditCall struct {
	userID   int64
	actionID string
	receipt  task.TaskDefinitionEditReceiptTarget
}

func (f *fakeDefinitionEditController) Propose(
	_ context.Context,
	in task.DefinitionEditProposalInput,
) (task.DefinitionEditProposal, error) {
	f.proposeCalls = append(f.proposeCalls, in)
	if f.proposeErr != nil {
		return task.DefinitionEditProposal{}, f.proposeErr
	}
	result := f.proposal
	if result.ID == "" {
		result.ID = in.ActionID
	}
	if result.Summary == "" {
		result.Summary = "编辑任务 task-edit-1"
	}
	return result, nil
}

func (f *fakeDefinitionEditController) Confirm(
	_ context.Context,
	userID int64,
	actionID string,
	receipt task.TaskDefinitionEditReceiptTarget,
) (task.TaskDefinitionEditOutcome, error) {
	f.confirmCalls = append(f.confirmCalls, fakeDefinitionEditCall{
		userID: userID, actionID: actionID, receipt: receipt,
	})
	return f.confirm, f.confirmErr
}

func (f *fakeDefinitionEditController) Cancel(
	_ context.Context,
	userID int64,
	actionID string,
	receipt task.TaskDefinitionEditReceiptTarget,
) (task.TaskDefinitionEditOutcome, error) {
	f.cancelCalls = append(f.cancelCalls, fakeDefinitionEditCall{
		userID: userID, actionID: actionID, receipt: receipt,
	})
	return f.cancel, f.cancelErr
}

func TestBuildTools_DefinitionEditFlagBoundary(t *testing.T) {
	disabled := BuildTools(nil, nil, nil, nil, nil, nil, nil)
	if toolNamed(disabled, "edit_task_definition") != nil {
		t.Fatal("default BuildTools must keep definition editing absent")
	}
	explicitlyDisabled := BuildTools(
		nil, nil, nil, nil, nil, nil, nil, nil,
	)
	baselineLoop := New(Deps{Tools: disabled})
	disabledLoop := New(Deps{
		Tools: explicitlyDisabled,
		// Callback compatibility remains wired during a flag rollback, but it
		// must not expose a proposal tool to the model.
		TaskDefinitionEdit: &fakeDefinitionEditController{},
	})
	baselineDefs, err := json.Marshal(baselineLoop.toolDefs)
	if err != nil {
		t.Fatal(err)
	}
	disabledDefs, err := json.Marshal(disabledLoop.toolDefs)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(baselineDefs, disabledDefs) ||
		baselineLoop.sys != disabledLoop.sys {
		t.Fatalf("disabled flag changed Agent request bytes:\nbaseline tools=%s\n disabled tools=%s",
			baselineDefs, disabledDefs)
	}

	controller := &fakeDefinitionEditController{}
	enabled := BuildTools(nil, &scheduler.Scheduler{}, nil, nil, nil, nil, nil, controller)
	tool := toolNamed(enabled, "edit_task_definition")
	if tool == nil {
		t.Fatal("enabled BuildTools did not register edit_task_definition")
	}
	if !tool.Mutating() {
		t.Fatal("edit_task_definition must require a confirmation card")
	}
	var schema struct {
		Required []string `json:"required"`
	}
	if err := json.Unmarshal(tool.Parameters(), &schema); err != nil {
		t.Fatalf("tool schema is invalid: %v", err)
	}
	if len(schema.Required) != 1 || schema.Required[0] != "task_id" {
		t.Fatalf("schema required = %v, want task_id only", schema.Required)
	}
}

func TestLoop_DefinitionEditProposalUsesControllerNotGenericPending(t *testing.T) {
	store := newFakeStore()
	session, err := store.CreateAgentSession(t.Context(), 11)
	if err != nil {
		t.Fatal(err)
	}
	controller := &fakeDefinitionEditController{}
	tools := BuildTools(nil, nil, nil, nil, nil, nil, nil, controller)
	loop := New(Deps{
		Store: store, Tools: tools, TaskDefinitionEdit: controller,
	})
	loop.chatFn = func(context.Context, llm.ChatRequest) (*llm.ChatResponse, error) {
		return &llm.ChatResponse{ToolCalls: []llm.ToolCall{{
			ID: "call-edit", Name: "edit_task_definition",
			Arguments: `{"task_id":"task-edit-1","strictness":"strict"}`,
		}}}, nil
	}

	out, err := loop.HandleMessage(t.Context(), 11, "把这个任务改严格")
	if err != nil {
		t.Fatalf("HandleMessage() error = %v", err)
	}
	if out.Confirm == nil || out.Confirm.ActionID == "" ||
		!strings.Contains(out.Confirm.Summary, "edit_task_definition") {
		t.Fatalf("confirmation = %+v", out.Confirm)
	}
	if len(controller.proposeCalls) != 1 {
		t.Fatalf("controller Propose calls = %d", len(controller.proposeCalls))
	}
	call := controller.proposeCalls[0]
	if call.UserID != 11 || call.SessionID == nil || *call.SessionID != session.ID ||
		string(call.RawArgs) != `{"task_id":"task-edit-1","strictness":"strict"}` ||
		time.Until(call.ExpiresAt) < 23*time.Hour {
		t.Fatalf("proposal input drifted: %+v", call)
	}
	if store.createCalls != 0 {
		t.Fatalf("definition edit used generic pending action: %d calls", store.createCalls)
	}
}

func TestLoop_DefinitionEditConfirmAndCancelDoNotDowngradeErrors(t *testing.T) {
	receipt := task.CreationReceiptTarget{
		Provider: "feishu_card_patch/app", Target: "om_original",
	}
	target := task.TaskDefinitionEditReceiptTarget{
		Provider: receipt.Provider, Target: receipt.Target,
	}
	t.Run("confirm routes to edit controller", func(t *testing.T) {
		store := newFakeStore()
		session, err := store.CreateAgentSession(t.Context(), 11)
		if err != nil {
			t.Fatal(err)
		}
		controller := &fakeDefinitionEditController{
			confirm: task.TaskDefinitionEditOutcome{
				OperationID: "edit-1", TaskID: "task-edit-1",
				SessionID: session.ID, ReceiptBound: true,
				Status: types.TaskDefinitionEditOperationStatusCompleted,
			},
		}
		loop := New(Deps{Store: store, TaskDefinitionEdit: controller})
		out, err := loop.ExecuteActionWithReceipt(
			t.Context(), 11, "edit-1", receipt,
		)
		if err != nil {
			t.Fatalf("ExecuteActionWithReceipt() error = %v", err)
		}
		if len(controller.confirmCalls) != 1 ||
			controller.confirmCalls[0].receipt != target ||
			store.claimCalls != 0 || !strings.Contains(out.Text, "最终结果") ||
			!out.DurableReceipt || out.PreserveCard {
			t.Fatalf("confirm routing drifted: calls=%+v outcome=%+v claim=%d",
				controller.confirmCalls, out, store.claimCalls)
		}
		if store.appendCount() != 0 {
			t.Fatalf("terminal session fact must be owned by outbox, appends=%d",
				store.appendCount())
		}
	})

	t.Run("cancel routes to edit controller", func(t *testing.T) {
		store := newFakeStore()
		controller := &fakeDefinitionEditController{
			cancel: task.TaskDefinitionEditOutcome{
				OperationID: "edit-2", TaskID: "task-edit-1",
				Status:       types.TaskDefinitionEditOperationStatusCancelled,
				ReceiptBound: true,
			},
		}
		loop := New(Deps{Store: store, TaskDefinitionEdit: controller})
		out, err := loop.CancelActionWithReceipt(
			t.Context(), 11, "edit-2", receipt,
		)
		if err != nil {
			t.Fatalf("CancelActionWithReceipt() error = %v", err)
		}
		if len(controller.cancelCalls) != 1 ||
			controller.cancelCalls[0].receipt != target ||
			store.claimCalls != 0 || !strings.Contains(out.Text, "已取消") ||
			!out.DurableReceipt || out.PreserveCard {
			t.Fatalf("cancel routing drifted: calls=%+v outcome=%+v claim=%d",
				controller.cancelCalls, out, store.claimCalls)
		}
	})

	t.Run("infrastructure failure never reaches creation or legacy", func(t *testing.T) {
		store := newFakeStore()
		store.actions["edit-broken"] = &types.PendingAction{
			ID: "edit-broken", UserID: 11, ToolName: "remove_schedule",
			Status:    types.PendingActionStatusPending,
			ExpiresAt: time.Now().Add(time.Hour),
		}
		boom := types.NewAppError(types.CodeDatabase, "read failed", nil)
		controller := &fakeDefinitionEditController{confirmErr: boom}
		creation := &fakeCreationController{
			confirmErr: task.ErrCreationOperationNotFound,
		}
		loop := New(Deps{
			Store: store, TaskDefinitionEdit: controller, TaskCreation: creation,
		})
		if _, err := loop.ExecuteActionWithReceipt(
			t.Context(), 11, "edit-broken", receipt,
		); !errors.Is(err, boom) {
			t.Fatalf("error = %v, want infrastructure failure", err)
		}
		if len(creation.confirmCalls) != 1 || store.claimCalls != 0 {
			t.Fatalf("routing/downgrade drifted: creation=%d legacy=%d",
				len(creation.confirmCalls), store.claimCalls)
		}
	})

	t.Run("cancel infrastructure failure never reaches legacy", func(t *testing.T) {
		store := newFakeStore()
		store.actions["edit-cancel-broken"] = &types.PendingAction{
			ID: "edit-cancel-broken", UserID: 11,
			ToolName:  "remove_schedule",
			Status:    types.PendingActionStatusPending,
			ExpiresAt: time.Now().Add(time.Hour),
		}
		boom := types.NewAppError(types.CodeDatabase, "cancel read failed", nil)
		controller := &fakeDefinitionEditController{cancelErr: boom}
		creation := &fakeCreationController{
			cancelErr: task.ErrCreationOperationNotFound,
		}
		loop := New(Deps{
			Store: store, TaskDefinitionEdit: controller, TaskCreation: creation,
		})
		if _, err := loop.CancelActionWithReceipt(
			t.Context(), 11, "edit-cancel-broken", receipt,
		); !errors.Is(err, boom) {
			t.Fatalf("error = %v, want infrastructure failure", err)
		}
		if len(creation.cancelCalls) != 1 ||
			store.actions["edit-cancel-broken"].Status !=
				types.PendingActionStatusPending {
			t.Fatalf("cancel error downgraded: creation=%d status=%s",
				len(creation.cancelCalls),
				store.actions["edit-cancel-broken"].Status)
		}
	})
}

type fakeDefinitionEditReceiptSessionStore struct {
	*fakeStore
	mu sync.Mutex

	calls    int
	lease    types.TaskDefinitionEditReceiptLease
	messages json.RawMessage
}

func (s *fakeDefinitionEditReceiptSessionStore) RecordTaskDefinitionEditReceiptSessionMessages(
	_ context.Context,
	lease types.TaskDefinitionEditReceiptLease,
	messages json.RawMessage,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	s.lease = lease
	s.messages = bytes.Clone(messages)
	return nil
}

func TestRecordDefinitionEditReceiptSessionUsesAgentUserLock(t *testing.T) {
	base := newFakeStore()
	store := &fakeDefinitionEditReceiptSessionStore{fakeStore: base}
	loop := New(Deps{Store: store})
	receipt := types.TaskDefinitionEditReceipt{
		ID: 9, TenantID: 2, UserID: 7,
		LeaseOwner: "definition-edit-receipt-worker", Fence: 4,
	}
	messages := json.RawMessage(
		`[{"role":"user","content":"[卡片回调] fixed edit fact"}]`,
	)
	muValue, _ := loop.userMu.LoadOrStore(int64(7), &sync.Mutex{})
	userMu := muValue.(*sync.Mutex)
	userMu.Lock()
	started := time.Now()
	err := loop.RecordDefinitionEditReceiptSession(
		t.Context(), receipt, messages,
	)
	if !errors.Is(err, errCreationReceiptSessionBusy) {
		t.Fatalf("busy user lock error=%v", err)
	}
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("edit receipt recorder blocked dispatcher for %v", elapsed)
	}
	userMu.Unlock()
	if err := loop.RecordDefinitionEditReceiptSession(
		t.Context(), receipt, messages,
	); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.calls != 1 || store.lease != receipt.Lease() ||
		!bytes.Equal(store.messages, messages) {
		t.Fatalf("calls=%d lease=%+v messages=%s",
			store.calls, store.lease, store.messages)
	}
}

func toolNamed(tools []Tool, name string) Tool {
	for _, tool := range tools {
		if tool.Name() == name {
			return tool
		}
	}
	return nil
}
