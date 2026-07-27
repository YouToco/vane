package agent

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/YouToco/vane/agentcontinuation"
	"github.com/YouToco/vane/llm"
	"github.com/YouToco/vane/task"
)

type fakeActionContinuationController struct {
	confirmOutcome agentcontinuation.ActionOutcome
	confirmErr     error
	cancelOutcome  agentcontinuation.ActionOutcome
	cancelErr      error
	confirmCalls   int
	cancelCalls    int
	onConfirm      func()
	onCancel       func()
}

type fakeActionProposalController struct {
	input   agentcontinuation.ActionProposalInput
	outcome agentcontinuation.ActionProposal
	err     error
	calls   int
}

func (f *fakeActionProposalController) Propose(
	_ context.Context,
	in agentcontinuation.ActionProposalInput,
) (agentcontinuation.ActionProposal, error) {
	f.calls++
	f.input = in
	if f.outcome.ID == "" {
		f.outcome = agentcontinuation.ActionProposal{
			ID: in.ActionID, Summary: in.Summary,
		}
	}
	return f.outcome, f.err
}

func TestRunToolCalls_EnableSourceUsesOnlyAtomicDurableProposal(
	t *testing.T,
) {
	fs := newFakeStore()
	tool := &fakeTool{
		name: "enable_source", mutating: true,
		result: "must not execute",
	}
	loop := newTestLoop(t, fs, (&scriptedChat{}).fn, tool)
	proposer := &fakeActionProposalController{}
	loop.actionProposal = proposer
	sessionID := int64(19)

	pending, replies, err := loop.runToolCalls(
		t.Context(), 7, &sessionID, []llm.ToolCall{{
			ID: "call", Name: "enable_source",
			Arguments: `{"source_id":11}`,
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if proposer.calls != 1 || proposer.input.UserID != 7 ||
		proposer.input.SessionID != sessionID ||
		proposer.input.ToolName != "enable_source" ||
		string(proposer.input.RawArgs) != `{"source_id":11}` ||
		time.Until(proposer.input.ExpiresAt) < 23*time.Hour {
		t.Fatalf("proposal call drifted: %+v", proposer)
	}
	if pending == nil || pending.ID != proposer.outcome.ID ||
		len(replies) != 1 || len(fs.actions) != 0 ||
		len(tool.calls) != 0 {
		t.Fatalf(
			"pending=%+v replies=%+v legacy=%d executes=%d",
			pending, replies, len(fs.actions), len(tool.calls),
		)
	}
}

func TestRunToolCalls_EnableSourceMiswireDoesNotFallBackToLegacyRoot(
	t *testing.T,
) {
	fs := newFakeStore()
	tool := &fakeTool{name: "enable_source", mutating: true}
	loop := newTestLoop(t, fs, (&scriptedChat{}).fn, tool)
	sessionID := int64(19)
	pending, _, err := loop.runToolCalls(
		t.Context(), 7, &sessionID, []llm.ToolCall{{
			ID: "call", Name: "enable_source",
			Arguments: `{"source_id":11}`,
		}},
	)
	if err == nil || pending != nil || len(fs.actions) != 0 ||
		len(tool.calls) != 0 {
		t.Fatalf(
			"miswire fell back: pending=%+v legacy=%d executes=%d err=%v",
			pending, len(fs.actions), len(tool.calls), err,
		)
	}
}

func (f *fakeActionContinuationController) Confirm(
	_ context.Context,
	_ int64,
	_ string,
) (agentcontinuation.ActionOutcome, error) {
	f.confirmCalls++
	if f.onConfirm != nil {
		f.onConfirm()
	}
	return f.confirmOutcome, f.confirmErr
}

func (f *fakeActionContinuationController) Cancel(
	_ context.Context,
	_ int64,
	_ string,
) (agentcontinuation.ActionOutcome, error) {
	f.cancelCalls++
	if f.onCancel != nil {
		f.onCancel()
	}
	return f.cancelOutcome, f.cancelErr
}

func TestExecuteActionWithReceipt_DurableV2RoutesBeforeEveryLegacyLane(
	t *testing.T,
) {
	fs := newFakeStore()
	session, err := fs.CreateAgentSession(t.Context(), 7)
	if err != nil {
		t.Fatal(err)
	}
	fs.actions["action"] = newPendingAction(
		"action", 7, &session.ID, "enable_source", "legacy")
	tool := &fakeTool{
		name: "enable_source", mutating: true, result: "must not execute",
	}
	creation := &fakeCreationController{}
	definition := &fakeDefinitionEditController{}
	continuation := &fakeActionContinuationController{
		confirmOutcome: agentcontinuation.ActionOutcome{
			Text: "已确认，系统将可靠继续执行，无需重复点击。",
		},
		onConfirm: func() {
			if fs.claimCalls != 0 || len(creation.confirmCalls) != 0 ||
				len(definition.confirmCalls) != 0 || len(tool.calls) != 0 {
				t.Fatal("durable v2 was not the first confirmation router")
			}
		},
	}
	loop := newTestLoop(t, fs, (&scriptedChat{}).fn, tool)
	loop.actionContinuation = continuation
	loop.taskCreation = creation
	loop.taskDefinitionEdit = definition

	out, err := loop.ExecuteActionWithReceipt(
		t.Context(), 7, "action", task.CreationReceiptTarget{})
	if err != nil {
		t.Fatal(err)
	}
	if out.Text != continuation.confirmOutcome.Text ||
		out.DurableReceipt || out.PreserveCard {
		t.Fatalf("outcome = %+v", out)
	}
	if continuation.confirmCalls != 1 || fs.claimCalls != 0 ||
		len(creation.confirmCalls) != 0 ||
		len(definition.confirmCalls) != 0 || len(tool.calls) != 0 {
		t.Fatalf(
			"v2 leaked into an older lane: v2=%d claim=%d create=%d edit=%d tool=%d",
			continuation.confirmCalls, fs.claimCalls,
			len(creation.confirmCalls), len(definition.confirmCalls),
			len(tool.calls))
	}
	waitAppends(t, fs, 0)
}

func TestExecuteAction_DurableV2FallsThroughOnlyOnErrNotRouted(
	t *testing.T,
) {
	tests := []struct {
		name         string
		continuation error
		wantLegacy   bool
	}{
		{
			name:         "explicit not routed",
			continuation: agentcontinuation.ErrNotRouted,
			wantLegacy:   true,
		},
		{
			name: "wrapped explicit not routed",
			continuation: errors.Join(
				errors.New("lookup"), agentcontinuation.ErrNotRouted),
			wantLegacy: true,
		},
		{
			name:         "database ambiguity",
			continuation: errors.New("database unavailable"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := newFakeStore()
			fs.actions["action"] = newPendingAction(
				"action", 7, nil, "enable_source", "legacy")
			tool := &fakeTool{
				name: "enable_source", mutating: true, result: "legacy",
			}
			continuation := &fakeActionContinuationController{
				confirmErr: tt.continuation,
				onConfirm: func() {
					if fs.claimCalls != 0 || len(tool.calls) != 0 {
						t.Fatal("legacy ran before the v2 routing decision")
					}
				},
			}
			loop := newTestLoop(t, fs, (&scriptedChat{}).fn, tool)
			loop.actionContinuation = continuation
			loop.taskCreation = &fakeCreationController{
				confirmErr: task.ErrCreationOperationNotFound,
			}

			got, err := loop.ExecuteAction(t.Context(), 7, "action")
			if tt.wantLegacy {
				if err != nil || got != "legacy" || fs.claimCalls != 1 ||
					len(tool.calls) != 1 {
					t.Fatalf(
						"explicit fallthrough failed: got=%q err=%v claim=%d tool=%d",
						got, err, fs.claimCalls, len(tool.calls))
				}
				return
			}
			if err == nil || got != "" || fs.claimCalls != 0 ||
				len(tool.calls) != 0 {
				t.Fatalf(
					"ambiguous v2 failure fell through: got=%q err=%v claim=%d tool=%d",
					got, err, fs.claimCalls, len(tool.calls))
			}
		})
	}
}

func TestExecuteActionWithReceipt_DurableV2FailurePreservesCard(
	t *testing.T,
) {
	fs := newFakeStore()
	loop := newTestLoop(t, fs, (&scriptedChat{}).fn)
	loop.actionContinuation = &fakeActionContinuationController{
		confirmErr: errors.New("integrity proof differs"),
	}

	out, err := loop.ExecuteActionWithReceipt(
		t.Context(), 7, "action", task.CreationReceiptTarget{})
	if err == nil || !out.PreserveCard || out.DurableReceipt ||
		fs.claimCalls != 0 {
		t.Fatalf("outcome=%+v err=%v claim=%d",
			out, err, fs.claimCalls)
	}
	waitAppends(t, fs, 0)
}

func TestCancelActionWithReceipt_DurableV2DoesNotWriteLegacyCallback(
	t *testing.T,
) {
	fs := newFakeStore()
	session, err := fs.CreateAgentSession(t.Context(), 7)
	if err != nil {
		t.Fatal(err)
	}
	fs.actions["action"] = newPendingAction(
		"action", 7, &session.ID, "enable_source", "legacy")
	creation := &fakeCreationController{}
	definition := &fakeDefinitionEditController{}
	continuation := &fakeActionContinuationController{
		cancelOutcome: agentcontinuation.ActionOutcome{
			Text: "已取消，本次操作不会执行。",
		},
		onCancel: func() {
			if len(creation.cancelCalls) != 0 ||
				len(definition.cancelCalls) != 0 {
				t.Fatal("durable v2 was not the first cancellation router")
			}
		},
	}
	loop := newTestLoop(t, fs, (&scriptedChat{}).fn)
	loop.actionContinuation = continuation
	loop.taskCreation = creation
	loop.taskDefinitionEdit = definition

	out, err := loop.CancelActionWithReceipt(
		t.Context(), 7, "action", task.CreationReceiptTarget{})
	if err != nil || out.Text != continuation.cancelOutcome.Text ||
		out.DurableReceipt || out.PreserveCard {
		t.Fatalf("outcome=%+v err=%v", out, err)
	}
	if got := fs.actions["action"].Status; got != "pending" {
		t.Fatalf("legacy pending action changed to %q", got)
	}
	if len(creation.cancelCalls) != 0 ||
		len(definition.cancelCalls) != 0 {
		t.Fatalf("older cancellation lane called: create=%d edit=%d",
			len(creation.cancelCalls), len(definition.cancelCalls))
	}
	waitAppends(t, fs, 0)
}

func TestCancelActionWithReceipt_DurableV2FailurePreservesCard(
	t *testing.T,
) {
	fs := newFakeStore()
	loop := newTestLoop(t, fs, (&scriptedChat{}).fn)
	loop.actionContinuation = &fakeActionContinuationController{
		cancelErr: errors.New("database unavailable"),
	}

	out, err := loop.CancelActionWithReceipt(
		t.Context(), 7, "action", task.CreationReceiptTarget{})
	if err == nil || !out.PreserveCard || out.DurableReceipt {
		t.Fatalf("outcome=%+v err=%v", out, err)
	}
	waitAppends(t, fs, 0)
}

func TestCancelAction_DurableV2FallsThroughOnlyOnErrNotRouted(
	t *testing.T,
) {
	tests := []struct {
		name         string
		continuation error
		wantLegacy   bool
	}{
		{
			name:         "explicit not routed",
			continuation: agentcontinuation.ErrNotRouted,
			wantLegacy:   true,
		},
		{
			name:         "database ambiguity",
			continuation: errors.New("database unavailable"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := newFakeStore()
			fs.actions["action"] = newPendingAction(
				"action", 7, nil, "enable_source", "legacy")
			continuation := &fakeActionContinuationController{
				cancelErr: tt.continuation,
			}
			loop := newTestLoop(t, fs, (&scriptedChat{}).fn)
			loop.actionContinuation = continuation
			loop.taskCreation = &fakeCreationController{
				cancelErr: task.ErrCreationOperationNotFound,
			}

			got, err := loop.CancelAction(t.Context(), 7, "action")
			if tt.wantLegacy {
				if err != nil || got != "已取消，本次操作不会执行。" {
					t.Fatalf("explicit fallthrough: got=%q err=%v", got, err)
				}
				if status := fs.actions["action"].Status; status != "cancelled" {
					t.Fatalf("legacy status = %q", status)
				}
				waitAppends(t, fs, 0)
				return
			}
			if err == nil || got != "" ||
				fs.actions["action"].Status != "pending" {
				t.Fatalf("ambiguous failure fell through: got=%q err=%v status=%q",
					got, err, fs.actions["action"].Status)
			}
			waitAppends(t, fs, 0)
		})
	}
}
