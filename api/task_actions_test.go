package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/YouToco/vane/agent"
	"github.com/YouToco/vane/task"
	"github.com/YouToco/vane/types"
)

type fakeTaskActionAgent struct {
	outcome agent.Outcome
	card    agent.CardActionOutcome
	err     error

	gotUserID    int64
	gotText      string
	gotAction    string
	gotTaskID    string
	proposeCalls int
	executeCalls int
	cancelCalls  int
	gotReceipt   task.CreationReceiptTarget

	onEdit func(actionID string)
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

func (f *fakeTaskActionAgent) HandleTaskCreationMessage(
	_ context.Context,
	userID int64,
	actionID string,
	text string,
) (agent.Outcome, error) {
	f.proposeCalls++
	f.gotUserID = userID
	f.gotAction = actionID
	f.gotText = text
	outcome := f.outcome
	if outcome.Confirm != nil {
		outcome.Confirm.ActionID = actionID
	}
	return outcome, f.err
}

func (f *fakeTaskActionAgent) HandleTaskDefinitionEditMessage(
	_ context.Context,
	userID int64,
	actionID string,
	taskID string,
	text string,
) (agent.Outcome, error) {
	f.proposeCalls++
	f.gotUserID = userID
	f.gotAction = actionID
	f.gotTaskID = taskID
	f.gotText = text
	if f.onEdit != nil {
		f.onEdit(actionID)
	}
	outcome := f.outcome
	if outcome.Confirm != nil {
		outcome.Confirm.ActionID = actionID
	}
	return outcome, f.err
}

func (f *fakeTaskActionAgent) ExecuteActionWithReceipt(
	_ context.Context,
	userID int64,
	actionID string,
	receipt task.CreationReceiptTarget,
) (agent.CardActionOutcome, error) {
	f.executeCalls++
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
	f.cancelCalls++
	f.gotUserID = userID
	f.gotAction = actionID
	f.gotReceipt = receipt
	return f.card, f.err
}

type fakeTaskActionStore struct {
	schedules map[string]*types.Schedule
	creations map[string]*types.TaskCreationOperation
	edits     map[string]*types.TaskDefinitionEditOperation
}

func newFakeTaskActionStore() *fakeTaskActionStore {
	return &fakeTaskActionStore{
		schedules: make(map[string]*types.Schedule),
		creations: make(map[string]*types.TaskCreationOperation),
		edits:     make(map[string]*types.TaskDefinitionEditOperation),
	}
}

func (f *fakeTaskActionStore) GetSchedule(
	_ context.Context,
	id string,
	userID int64,
) (*types.Schedule, error) {
	schedule := f.schedules[id]
	if schedule == nil || schedule.UserID != userID {
		return nil, taskActionNotFound()
	}
	copy := *schedule
	return &copy, nil
}

func (f *fakeTaskActionStore) LoadTaskCreationOperationByUser(
	_ context.Context,
	id string,
	userID int64,
) (*types.TaskCreationOperation, error) {
	op := f.creations[id]
	if op == nil || op.UserID != userID {
		return nil, taskActionNotFound()
	}
	copy := *op
	return &copy, nil
}

func (f *fakeTaskActionStore) LoadTaskDefinitionEditOperationByActor(
	_ context.Context,
	id string,
	userID int64,
) (*types.TaskDefinitionEditOperation, error) {
	op := f.edits[id]
	if op == nil || op.UserID != userID {
		return nil, taskActionNotFound()
	}
	copy := *op
	return &copy, nil
}

func taskActionNotFound() error {
	return types.NewAppError(
		types.CodeNotFound,
		"任务动作不存在",
		types.ErrNotFound,
	)
}

func testWebTaskActionRequestID(
	t *testing.T,
	mode string,
	taskID string,
	text string,
) string {
	t.Helper()
	digest, ok := webTaskActionPayloadDigest(mode, taskID, text)
	if !ok {
		t.Fatal("build request digest")
	}
	return uuid.NewString() + "." + digest
}

func TestWebTaskActionProposalUsesConfirmedCreationControlPlane(t *testing.T) {
	const text = "每天追踪官方更新"
	requestID := testWebTaskActionRequestID(t, "create", "", text)
	actionAgent := &fakeTaskActionAgent{outcome: agent.Outcome{
		Reply: "请确认",
		Confirm: &agent.Confirm{
			Summary: "每天追踪官方更新",
		},
	}}
	actionStore := newFakeTaskActionStore()
	deps, cookie := authedDeps(t, Deps{
		TaskAgent: actionAgent, TaskActions: actionStore,
	})
	mux := http.NewServeMux()
	Mount(mux, deps)

	body, err := json.Marshal(proposeTaskActionReq{
		RequestID: requestID, Text: text,
	})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(
		http.MethodPost,
		"/api/task-actions/propose",
		strings.NewReader(string(body)),
	)
	req.AddCookie(cookie)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	wantActionID := webTaskActionID(1, 1, "create", "", requestID)
	if rec.Code != http.StatusOK ||
		!strings.Contains(rec.Body.String(), `"id":"`+wantActionID+`"`) ||
		!strings.Contains(rec.Body.String(), `"kind":"create"`) {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if actionAgent.gotUserID != 1 ||
		actionAgent.gotAction != wantActionID ||
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
	actionStore := newFakeTaskActionStore()
	actionStore.creations["action-web-2"] = &types.TaskCreationOperation{
		ID: "action-web-2", UserID: 1,
	}
	deps, cookie := authedDeps(t, Deps{
		TaskAgent: actionAgent, TaskActions: actionStore,
	})
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
		strings.NewReader(`{"request_id":"invalid","text":"track updates"}`),
	)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d, want 401", rec.Code)
	}
}

func TestWebTaskActionRequestIDBindsCanonicalPayload(t *testing.T) {
	const (
		prefix = "014a4c61-2d14-4e00-a7db-cb046ee51d59"
		text   = "追踪 <AI> 更新\u2028只看官方"
	)
	digest, ok := webTaskActionPayloadDigest("edit", "task-1", text)
	if !ok {
		t.Fatal("digest failed")
	}
	// Shared browser/backend vector. The frontend applies JSON.stringify with
	// fixed key order and canonicalizes U+2028/U+2029 to their JSON escapes.
	const wantDigest = "66ceaf7617ecc99ca6bdf53d3ef88733015eb93505505a6253e08e2195079d8d"
	if digest != wantDigest {
		t.Fatalf("digest=%s, want=%s", digest, wantDigest)
	}
	requestID := prefix + "." + digest
	if !validWebTaskActionRequestID(requestID, "edit", "task-1", text) {
		t.Fatal("matching self-binding request id rejected")
	}
	if validWebTaskActionRequestID(
		requestID, "edit", "task-1", text+" changed",
	) {
		t.Fatal("same request id accepted a different payload")
	}
}

func TestWebTaskActionEditUsesIsolatedLaneAndVerifiesDurableScope(
	t *testing.T,
) {
	const (
		taskID = "task-edit-1"
		text   = "改成严格模式"
	)
	requestID := testWebTaskActionRequestID(t, "edit", taskID, text)
	actionStore := newFakeTaskActionStore()
	actionStore.schedules[taskID] = &types.Schedule{
		ID: taskID, UserID: 1,
	}
	actionAgent := &fakeTaskActionAgent{outcome: agent.Outcome{
		Reply: "请确认",
		Confirm: &agent.Confirm{
			Summary: "严格模式",
		},
	}}
	actionAgent.onEdit = func(actionID string) {
		actionStore.edits[actionID] = &types.TaskDefinitionEditOperation{
			ID: actionID, TenantID: 1, UserID: 1, TaskID: taskID,
		}
	}
	deps, cookie := authedDeps(t, Deps{
		TaskAgent: actionAgent, TaskActions: actionStore,
		DefinitionEditEnabled: true,
	})
	mux := http.NewServeMux()
	Mount(mux, deps)
	body, err := json.Marshal(proposeTaskActionReq{
		RequestID: requestID, Text: text, TaskID: taskID,
	})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(
		http.MethodPost,
		"/api/task-actions/propose",
		strings.NewReader(string(body)),
	)
	req.AddCookie(cookie)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK ||
		!strings.Contains(rec.Body.String(), `"kind":"edit"`) ||
		!strings.Contains(rec.Body.String(), `"task_id":"`+taskID+`"`) {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if actionAgent.proposeCalls != 1 ||
		actionAgent.gotTaskID != taskID ||
		actionAgent.gotText != text {
		t.Fatalf("agent=%+v", actionAgent)
	}
}

func TestWebTaskActionEditCapabilityDisabledFailsBeforeAgent(t *testing.T) {
	const (
		taskID = "task-edit-disabled"
		text   = "改成每天执行"
	)
	actionStore := newFakeTaskActionStore()
	actionStore.schedules[taskID] = &types.Schedule{
		ID: taskID, UserID: 1,
	}
	actionAgent := &fakeTaskActionAgent{}
	deps, cookie := authedDeps(t, Deps{
		TaskAgent: actionAgent, TaskActions: actionStore,
	})
	mux := http.NewServeMux()
	Mount(mux, deps)
	body, err := json.Marshal(proposeTaskActionReq{
		RequestID: testWebTaskActionRequestID(
			t, "edit", taskID, text,
		),
		Text: text, TaskID: taskID,
	})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(
		http.MethodPost,
		"/api/task-actions/propose",
		strings.NewReader(string(body)),
	)
	req.AddCookie(cookie)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable ||
		actionAgent.proposeCalls != 0 {
		t.Fatalf(
			"status=%d calls=%d body=%s",
			rec.Code, actionAgent.proposeCalls, rec.Body.String(),
		)
	}
}

func TestWebTaskActionDurableReplaySkipsModel(t *testing.T) {
	const text = "每周整理竞品更新"
	requestID := testWebTaskActionRequestID(t, "create", "", text)
	actionID := webTaskActionID(1, 1, "create", "", requestID)
	actionStore := newFakeTaskActionStore()
	actionStore.creations[actionID] = &types.TaskCreationOperation{
		ID: actionID, TenantID: 1, UserID: 1,
		Summary: "已有方案",
	}
	actionAgent := &fakeTaskActionAgent{}
	deps, cookie := authedDeps(t, Deps{
		TaskAgent: actionAgent, TaskActions: actionStore,
	})
	mux := http.NewServeMux()
	Mount(mux, deps)
	body, err := json.Marshal(proposeTaskActionReq{
		RequestID: requestID, Text: text,
	})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(
		http.MethodPost,
		"/api/task-actions/propose",
		strings.NewReader(string(body)),
	)
	req.AddCookie(cookie)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK ||
		!strings.Contains(rec.Body.String(), actionID) ||
		!strings.Contains(rec.Body.String(), "已有方案") ||
		actionAgent.proposeCalls != 0 {
		t.Fatalf(
			"status=%d calls=%d body=%s",
			rec.Code, actionAgent.proposeCalls, rec.Body.String(),
		)
	}
}

func TestWebTaskActionGenericPendingCannotBeConfirmed(t *testing.T) {
	actionAgent := &fakeTaskActionAgent{}
	actionStore := newFakeTaskActionStore()
	deps, cookie := authedDeps(t, Deps{
		TaskAgent: actionAgent, TaskActions: actionStore,
	})
	mux := http.NewServeMux()
	Mount(mux, deps)
	req := httptest.NewRequest(
		http.MethodPost,
		"/api/task-actions/generic-pending/confirm",
		nil,
	)
	req.AddCookie(cookie)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound ||
		actionAgent.executeCalls != 0 {
		t.Fatalf(
			"status=%d execute=%d body=%s",
			rec.Code, actionAgent.executeCalls, rec.Body.String(),
		)
	}
}
