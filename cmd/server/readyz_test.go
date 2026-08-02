package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

type readyzStoreFunc func(context.Context) error

func (f readyzStoreFunc) Ping(ctx context.Context) error { return f(ctx) }

func TestHandleReadyzRequiresEveryConfiguredStore(t *testing.T) {
	for _, test := range []struct {
		name       string
		primaryErr error
		controlErr error
		wantStatus int
	}{
		{name: "both healthy", wantStatus: http.StatusOK},
		{name: "primary unavailable", primaryErr: errors.New("primary down"),
			wantStatus: http.StatusServiceUnavailable},
		{name: "V3 control or research unavailable", controlErr: errors.New("control down"),
			wantStatus: http.StatusServiceUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			var primaryCalls, controlCalls atomic.Int32
			handler := handleReadyz(
				readyzStoreFunc(func(context.Context) error {
					primaryCalls.Add(1)
					return test.primaryErr
				}),
				readyzStoreFunc(func(context.Context) error {
					controlCalls.Add(1)
					return test.controlErr
				}),
			)
			response := httptest.NewRecorder()
			handler(response, httptest.NewRequest(http.MethodGet, "/readyz", nil))
			if response.Code != test.wantStatus {
				t.Fatalf("status=%d want=%d", response.Code, test.wantStatus)
			}
			if primaryCalls.Load() != 1 || controlCalls.Load() != 1 {
				t.Fatalf("Ping calls primary=%d control=%d",
					primaryCalls.Load(), controlCalls.Load())
			}
		})
	}
}

func TestHandleReadyzWithoutV3UsesOnlyPrimaryStore(t *testing.T) {
	var calls atomic.Int32
	handler := handleReadyz(readyzStoreFunc(func(context.Context) error {
		calls.Add(1)
		return nil
	}))
	response := httptest.NewRecorder()
	handler(response, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if response.Code != http.StatusOK || calls.Load() != 1 {
		t.Fatalf("status=%d primary calls=%d", response.Code, calls.Load())
	}
}

func TestHandleReadyzFailsClosedWithoutStore(t *testing.T) {
	response := httptest.NewRecorder()
	handleReadyz()(response, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d want=%d", response.Code, http.StatusServiceUnavailable)
	}
}
