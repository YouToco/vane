package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestTaskBriefAPIRejectsInvalidPageBeforeStore(t *testing.T) {
	for _, target := range []string{
		"/api/schedules/task-1/briefs?page_size=0",
		"/api/schedules/task-1/briefs?page_size=21",
		"/api/schedules/task-1/briefs?page_size=not-a-number",
	} {
		req := httptest.NewRequest(http.MethodGet, target, nil)
		req.SetPathValue("id", "task-1")
		rec := httptest.NewRecorder()
		(&server{}).handleListTaskBriefs(rec, req)
		if rec.Code != http.StatusBadRequest ||
			!strings.Contains(rec.Body.String(), "1 到 20") {
			t.Fatalf("%s: status=%d body=%s",
				target, rec.Code, rec.Body.String())
		}
	}
}

func TestTaskBriefAPIRouteRequiresSession(t *testing.T) {
	mux := http.NewServeMux()
	Mount(mux, Deps{})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(
		http.MethodGet, "/api/schedules/task-1/briefs", nil)
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}
