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

func TestNaturalTaskDefinitionEditResolvesNameThenEditsOnce(t *testing.T) {
	fs := newFakeStore()
	list := &fakeTool{
		name:   "list_schedules",
		result: "- id=task-edit-1; 名称=每周一上午9:00推送AI官方重大更新",
	}
	controller := &fakeDefinitionEditController{
		execute: task.TaskDefinitionEditOutcome{
			Status: types.TaskDefinitionEditOperationStatusCompleted,
			TaskID: "task-edit-1",
		},
	}
	chat := &scriptedChat{responses: []*llm.ChatResponse{
		{Content: replyMaxTurns},
		{
			FinishReason: "tool_calls",
			ToolCalls: []llm.ToolCall{{
				ID: "list-1", Name: "list_schedules", Arguments: `{}`,
			}},
		},
		{
			FinishReason: "tool_calls",
			ToolCalls: []llm.ToolCall{{
				ID:   "edit-1",
				Name: "edit_task_definition",
				Arguments: `{
					"task_id":"task-edit-1",
					"intent":"每周一检查三家官方博客；未来运行时打开官方原文并交叉核验；没有重大更新就不推送"
				}`,
			}},
		},
	}}
	loop := New(Deps{
		Store: fs,
		Tools: []ToolSpec{
			newToolSpec(list, ownerPolicy(
				Effects(EffectInternalRead), BudgetNone)),
			newToolSpec(&editTaskDefinitionTool{}, ownerPolicy(
				Effects(
					EffectDurableProposal,
					EffectStateWrite,
					EffectDirectOwnerWrite,
				),
				BudgetNone,
			)),
		},
		TaskDefinitionEdit: controller,
		Model:              "test-model",
		MaxTurns:           20,
		SessionTTL:         30 * time.Minute,
	})
	loop.chatFn = chat.fn

	out, err := loop.HandleMessage(
		t.Context(),
		7,
		"请把“每周一上午9:00推送AI官方重大更新”更新为：任务手册写清三家官方博客、交叉核验和无更新不推送，无需确认，直接更新。",
	)
	if err != nil {
		t.Fatal(err)
	}
	if out.Reply != "已修改定时推送任务（id=task-edit-1）。" {
		t.Fatalf("Reply=%q", out.Reply)
	}
	if len(chat.requests) != 3 {
		t.Fatalf("model calls=%d, want 3 including one bounded retry",
			len(chat.requests))
	}
	if got := toolDefNames(chat.requests[0].Tools); len(got) != 1 ||
		got[0] != "list_schedules" {
		t.Fatalf("first tools=%v, want list_schedules only", got)
	}
	if got := toolDefNames(chat.requests[1].Tools); len(got) != 1 ||
		got[0] != "list_schedules" {
		t.Fatalf("retry tools=%v, want list_schedules only", got)
	}
	if got := toolDefNames(chat.requests[2].Tools); len(got) != 1 ||
		got[0] != "edit_task_definition" {
		t.Fatalf("third tools=%v, want edit_task_definition only", got)
	}
	if len(list.calls) != 1 {
		t.Fatalf("list calls=%d, want 1", len(list.calls))
	}
	if len(controller.proposeCalls) != 1 ||
		len(controller.executeCalls) != 1 {
		t.Fatalf("prepare=%d execute=%d, want 1/1",
			len(controller.proposeCalls), len(controller.executeCalls))
	}
	if bytes.Contains(
		[]byte(chat.requests[0].Messages[0].Content),
		[]byte("请把需求拆小"),
	) {
		t.Fatal("natural edit lane must not instruct the user to split one edit")
	}
}

func TestNaturalTaskDefinitionEditClassifier(t *testing.T) {
	for _, test := range []struct {
		text string
		want bool
	}{
		{"请把“每周 AI 更新”任务更新为只看官方博客", true},
		{"把日报改成每周一推送", true},
		{"如何修改这个任务？", false},
		{"不要修改这个任务", false},
		{"创建任务：每天看官方博客", false},
	} {
		if got := isNaturalTaskDefinitionEditRequest(test.text); got != test.want {
			t.Errorf("isNaturalTaskDefinitionEditRequest(%q)=%v, want %v",
				test.text, got, test.want)
		}
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

func toolDefNames(defs []llm.ToolDef) []string {
	out := make([]string, 0, len(defs))
	for _, def := range defs {
		out = append(out, def.Name)
	}
	return out
}
