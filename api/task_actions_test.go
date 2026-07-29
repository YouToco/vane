package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"

	"github.com/YouToco/vane/agent"
)

type fakeTaskActionAgent struct {
	createCalls int
	editCalls   int
	userID      int64
	actionID    string
	taskID      string
	text        string
	outcome     agent.Outcome
	err         error
}

func (f *fakeTaskActionAgent) HandleMessage(
	_ context.Context,
	userID int64,
	text string,
) (agent.Outcome, error) {
	f.userID, f.text = userID, text
	return f.result()
}

func (f *fakeTaskActionAgent) HandleTaskCreationMessage(
	_ context.Context,
	userID int64,
	actionID string,
	text string,
) (agent.Outcome, error) {
	f.createCalls++
	f.userID, f.actionID, f.text = userID, actionID, text
	return f.result()
}

func (f *fakeTaskActionAgent) HandleTaskDefinitionEditMessage(
	_ context.Context,
	userID int64,
	actionID string,
	taskID string,
	text string,
) (agent.Outcome, error) {
	f.editCalls++
	f.userID, f.actionID, f.taskID, f.text = userID, actionID, taskID, text
	return f.result()
}

func (f *fakeTaskActionAgent) result() (agent.Outcome, error) {
	if f.outcome.Reply == "" {
		f.outcome.Reply = "已执行"
	}
	return f.outcome, f.err
}

func TestWebTaskActionExecutesNaturalLanguageDirectly(t *testing.T) {
	taskAgent := &fakeTaskActionAgent{}
	deps, cookie := authedDeps(t, Deps{
		TaskAgent:             taskAgent,
		DefinitionEditEnabled: true,
	})
	mux := http.NewServeMux()
	Mount(mux, deps)

	text := "每天九点汇总官方更新"
	requestID := taskActionRequestID(t, "create", "", text)
	body, err := json.Marshal(executeTaskActionRequest{
		RequestID: requestID,
		Text:      text,
	})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/task-actions", bytes.NewReader(body))
	req.AddCookie(cookie)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if taskAgent.createCalls != 1 || taskAgent.editCalls != 0 ||
		taskAgent.text != text || taskAgent.actionID == "" {
		t.Fatalf("direct creation call=%+v", taskAgent)
	}
	var response executeTaskActionResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Message != "已执行" {
		t.Fatalf("message=%q", response.Message)
	}
}

func TestWebTaskActionRejectsPayloadReplayWithDifferentText(t *testing.T) {
	taskAgent := &fakeTaskActionAgent{}
	deps, cookie := authedDeps(t, Deps{TaskAgent: taskAgent})
	mux := http.NewServeMux()
	Mount(mux, deps)

	requestID := taskActionRequestID(t, "create", "", "原请求")
	body, err := json.Marshal(executeTaskActionRequest{
		RequestID: requestID,
		Text:      "篡改后的请求",
	})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/task-actions", bytes.NewReader(body))
	req.AddCookie(cookie)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusConflict || taskAgent.createCalls != 0 {
		t.Fatalf("status=%d calls=%d body=%s",
			rec.Code, taskAgent.createCalls, rec.Body.String())
	}
}

func TestRetiredTaskActionConfirmationRoutesAreAbsent(t *testing.T) {
	deps, cookie := authedDeps(t, Deps{TaskAgent: &fakeTaskActionAgent{}})
	mux := http.NewServeMux()
	Mount(mux, deps)
	for _, path := range []string{
		"/api/task-actions/propose",
		"/api/task-actions/action-1/confirm",
		"/api/task-actions/action-1/cancel",
	} {
		req := httptest.NewRequest(http.MethodPost, path, nil)
		req.AddCookie(cookie)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("%s status=%d want=404", path, rec.Code)
		}
	}
}

func TestRetiredSourceCRUDRoutesAreAbsent(t *testing.T) {
	deps, cookie := authedDeps(t, Deps{TaskAgent: &fakeTaskActionAgent{}})
	mux := http.NewServeMux()
	Mount(mux, deps)
	for _, testCase := range []struct {
		method string
		path   string
	}{
		{method: http.MethodGet, path: "/api/sources"},
		{method: http.MethodPost, path: "/api/sources"},
		{method: http.MethodPatch, path: "/api/sources/1"},
		{method: http.MethodDelete, path: "/api/sources/1"},
	} {
		req := httptest.NewRequest(testCase.method, testCase.path, nil)
		req.AddCookie(cookie)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusNotFound {
			t.Fatalf(
				"%s %s status=%d want=404",
				testCase.method, testCase.path, rec.Code,
			)
		}
	}
}

func taskActionRequestID(
	t *testing.T,
	mode string,
	taskID string,
	text string,
) string {
	t.Helper()
	digest, ok := webTaskActionPayloadDigest(mode, taskID, text)
	if !ok {
		t.Fatal("payload digest failed")
	}
	return uuid.NewString() + "." + digest
}
