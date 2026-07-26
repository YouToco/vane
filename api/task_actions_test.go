package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/YouToco/vane/agent"
	"github.com/YouToco/vane/task"
)

type fakeTaskActionAgent struct {
	outcome agent.Outcome
	card    agent.CardActionOutcome
	err     error

	gotUserID  int64
	gotText    string
	gotAction  string
	gotReceipt task.CreationReceiptTarget
}

func (f *fakeTaskActionAgent) HandleMessage(
	_ context.Context,
	userID int64,
	text string,
) (agent.Outcome, error) {
	f.gotUserID = userID
	f.gotText = text
	return f.outcome, f.err
}

func (f *fakeTaskActionAgent) ExecuteActionWithReceipt(
	_ context.Context,
	userID int64,
	actionID string,
	receipt task.CreationReceiptTarget,
) (agent.CardActionOutcome, error) {
	f.gotUserID = userID
	f.gotAction = actionID
	f.gotReceipt = receipt
	return f.card, f.err
}

func (f *fakeTaskActionAgent) CancelActionWithReceipt(
	_ context.Context,
	userID int64,
	actionID string,
	receipt task.CreationReceiptTarget,
) (agent.CardActionOutcome, error) {
	f.gotUserID = userID
	f.gotAction = actionID
	f.gotReceipt = receipt
	return f.card, f.err
}

func TestWebTaskActionProposalUsesConfirmedCreationControlPlane(t *testing.T) {
	actionAgent := &fakeTaskActionAgent{outcome: agent.Outcome{
		Reply: "请确认",
		Confirm: &agent.Confirm{
			ActionID: "action-web-1",
			Summary:  "每天追踪官方更新",
		},
	}}
	deps, cookie := authedDeps(t, Deps{TaskAgent: actionAgent})
	mux := http.NewServeMux()
	Mount(mux, deps)

	req := httptest.NewRequest(
		http.MethodPost,
		"/api/task-actions/propose",
		strings.NewReader(`{"text":"每天追踪官方更新"}`),
	)
	req.AddCookie(cookie)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK ||
		!strings.Contains(rec.Body.String(), `"id":"action-web-1"`) {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if actionAgent.gotUserID != 1 ||
		!strings.HasPrefix(
			actionAgent.gotText,
			"确认创建，直接生成确认卡，不要再次搜索。",
		) ||
		!strings.Contains(actionAgent.gotText, "每天追踪官方更新") {
		t.Fatalf(
			"agent handoff user=%d text=%q",
			actionAgent.gotUserID,
			actionAgent.gotText,
		)
	}
}

func TestWebTaskActionConfirmOwnsReceiptIdentity(t *testing.T) {
	actionAgent := &fakeTaskActionAgent{
		card: agent.CardActionOutcome{Text: "已受理"},
	}
	deps, cookie := authedDeps(t, Deps{TaskAgent: actionAgent})
	mux := http.NewServeMux()
	Mount(mux, deps)

	req := httptest.NewRequest(
		http.MethodPost,
		"/api/task-actions/action-web-2/confirm",
		nil,
	)
	req.AddCookie(cookie)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if actionAgent.gotAction != "action-web-2" ||
		actionAgent.gotReceipt != task.WebActionReceiptTarget("action-web-2") {
		t.Fatalf(
			"action=%q receipt=%+v",
			actionAgent.gotAction,
			actionAgent.gotReceipt,
		)
	}
}

func TestWebTaskActionRequiresSession(t *testing.T) {
	mux := http.NewServeMux()
	Mount(mux, Deps{TaskAgent: &fakeTaskActionAgent{}})
	req := httptest.NewRequest(
		http.MethodPost,
		"/api/task-actions/propose",
		strings.NewReader(`{"text":"track updates"}`),
	)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d, want 401", rec.Code)
	}
}
