package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/YouToco/vane/llm"
	"github.com/YouToco/vane/task"
	"github.com/YouToco/vane/types"
)

type fakeDefinitionEditController struct {
	proposeCalls []task.DefinitionEditProposalInput
	proposal     task.DefinitionEditProposal
	proposeErr   error

	executeCalls []fakeDefinitionEditCall
	execute      task.TaskDefinitionEditOutcome
	executeErr   error
}

type fakeDefinitionEditCall struct {
	userID   int64
	actionID string
	receipt  task.TaskDefinitionEditReceiptTarget
}

func (f *fakeDefinitionEditController) Prepare(
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

func (f *fakeDefinitionEditController) Execute(
	_ context.Context,
	userID int64,
	actionID string,
	receipt task.TaskDefinitionEditReceiptTarget,
) (task.TaskDefinitionEditOutcome, error) {
	f.executeCalls = append(f.executeCalls, fakeDefinitionEditCall{
		userID: userID, actionID: actionID, receipt: receipt,
	})
	return f.execute, f.executeErr
}

type fakeDefinitionEditReceiptSessionStore struct {
	*fakeStore
	mu sync.Mutex

	calls    int
	lease    types.TaskDefinitionEditReceiptLease
	messages json.RawMessage
}

func TestDirectTaskDefinitionEditPreservesTargetedClarification(t *testing.T) {
	fs := newFakeStore()
	controller := &fakeDefinitionEditController{}
	loop := New(Deps{
		Store:              fs,
		TaskDefinitionEdit: controller,
		Model:              "test-model",
		MaxTurns:           4,
		SessionTTL:         30 * time.Minute,
	})
	loop.chatFn = func(context.Context, llm.ChatRequest) (*llm.ChatResponse, error) {
		return &llm.ChatResponse{
			Content: "你说的“减少推送”是改为每周一次，还是保留频率但只推重大事件？",
		}, nil
	}
	out, err := loop.HandleTaskDefinitionEditMessage(
		t.Context(), 7,
		"beff990d-9ed6-4a5d-8f72-9e6987516dda",
		"task-edit-1",
		"减少这个任务的推送",
	)
	if err != nil {
		t.Fatal(err)
	}
	if out.Reply != "你说的“减少推送”是改为每周一次，还是保留频率但只推重大事件？" {
		t.Fatalf("针对性编辑澄清被改写: %q", out.Reply)
	}
	if len(controller.proposeCalls) != 0 || len(controller.executeCalls) != 0 {
		t.Fatal("澄清问题不得产生定义编辑副作用")
	}
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
	muValue, _ := loop.userMu.LoadOrStore(int64(7), newUserTurnLock())
	userMu := muValue.(*userTurnLock)
	if err := userMu.Lock(t.Context()); err != nil {
		t.Fatal(err)
	}
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

func toolNamed(tools []ToolSpec, name string) *ToolSpec {
	for i := range tools {
		if tools[i].Name() == name {
			return &tools[i]
		}
	}
	return nil
}
