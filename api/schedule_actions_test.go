package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/YouToco/vane/workflow"
)

type fakeScheduleActionController struct {
	command string
	id      string
	userID  int64
	err     error
}

func (f *fakeScheduleActionController) PushNow(
	context.Context,
	int64,
	workflow.PushScope,
) (string, error) {
	return "", nil
}

func (f *fakeScheduleActionController) DeletePush(
	context.Context,
	string,
	int64,
) error {
	return nil
}

func (f *fakeScheduleActionController) TriggerScheduleNow(
	_ context.Context,
	id string,
	userID int64,
) error {
	f.command, f.id, f.userID = "run", id, userID
	return f.err
}

func (f *fakeScheduleActionController) PausePush(
	_ context.Context,
	id string,
	userID int64,
) error {
	f.command, f.id, f.userID = "pause", id, userID
	return f.err
}

func (f *fakeScheduleActionController) ResumePush(
	_ context.Context,
	id string,
	userID int64,
) error {
	f.command, f.id, f.userID = "resume", id, userID
	return f.err
}

func TestScheduleActionsUseSessionPrincipalAndSelectedTask(t *testing.T) {
	for _, tc := range []struct {
		path       string
		want       string
		wantStatus int
	}{
		{path: "/api/schedules/task-1/run", want: "run", wantStatus: http.StatusAccepted},
		{path: "/api/schedules/task-1/pause", want: "pause", wantStatus: http.StatusOK},
		{path: "/api/schedules/task-1/resume", want: "resume", wantStatus: http.StatusOK},
	} {
		t.Run(tc.want, func(t *testing.T) {
			controller := &fakeScheduleActionController{}
			deps, cookie := authedDeps(t, Deps{Scheduler: controller})
			mux := http.NewServeMux()
			Mount(mux, deps)
			req := httptest.NewRequest(http.MethodPost, tc.path, nil)
			req.AddCookie(cookie)
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			if rec.Code != tc.wantStatus {
				t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
			}
			if controller.command != tc.want ||
				controller.id != "task-1" ||
				controller.userID != 1 {
				t.Fatalf("controller=%+v", controller)
			}
		})
	}
}
