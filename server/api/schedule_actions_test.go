package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type fakeScheduleActionController struct {
	command string
	id      string
	userID  int64
	key     string
	err     error
}

func (f *fakeScheduleActionController) DeletePush(
	context.Context,
	string,
	int64,
) error {
	return nil
}

func (f *fakeScheduleActionController) TriggerScheduleNowIdempotent(
	_ context.Context,
	id string,
	userID int64,
	key string,
) error {
	f.command, f.id, f.userID, f.key = "run", id, userID, key
	return f.err
}

func (f *fakeScheduleActionController) PausePushIdempotent(
	_ context.Context,
	id string,
	userID int64,
	key string,
) error {
	f.command, f.id, f.userID, f.key = "pause", id, userID, key
	return f.err
}

func (f *fakeScheduleActionController) ResumePushIdempotent(
	_ context.Context,
	id string,
	userID int64,
	key string,
) error {
	f.command, f.id, f.userID, f.key = "resume", id, userID, key
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
			req.Header.Set("Idempotency-Key", "web-command-test-1")
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
			if controller.key != "web-command-test-1" {
				t.Fatalf("idempotency key=%q", controller.key)
			}
		})
	}
}

func TestScheduleActionsRequireBoundedIdempotencyKey(t *testing.T) {
	for _, key := range []string{"", " bad", "bad key", strings.Repeat("x", 129)} {
		t.Run(key, func(t *testing.T) {
			controller := &fakeScheduleActionController{}
			deps, cookie := authedDeps(t, Deps{Scheduler: controller})
			mux := http.NewServeMux()
			Mount(mux, deps)
			req := httptest.NewRequest(
				http.MethodPost, "/api/schedules/task-1/run", nil,
			)
			req.Header.Set("Idempotency-Key", key)
			req.AddCookie(cookie)
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("key=%q status=%d body=%s",
					key, rec.Code, rec.Body.String())
			}
			if controller.command != "" {
				t.Fatalf("invalid key reached controller: %+v", controller)
			}
		})
	}
}
