package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestTemporalWorkerOptionsCoverLongestActivity(t *testing.T) {
	const longestActivity = 120 * time.Second
	got := temporalWorkerOptions().WorkerStopTimeout
	if got <= longestActivity {
		t.Fatalf("WorkerStopTimeout = %v, must exceed longest Activity timeout %v", got, longestActivity)
	}
}

func TestHTTPShutdownUnownedHandlerDoesNotReleaseDependencies(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	handlerDone := make(chan struct{})
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(entered)
		<-release
		w.WriteHeader(http.StatusNoContent)
		close(handlerDone)
	})}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- srv.Serve(ln) }()

	requestDone := make(chan error, 1)
	go func() {
		resp, err := http.Get("http://" + ln.Addr().String())
		if err == nil {
			resp.Body.Close()
		}
		requestDone <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("blocking handler did not start")
	}

	initial := beginHTTPShutdown(srv, 25*time.Millisecond)
	shutdownErr := completeHTTPShutdown(srv, initial, 25*time.Millisecond)
	if !errors.Is(shutdownErr, context.DeadlineExceeded) {
		t.Fatalf("HTTP drain proof error = %v, want DeadlineExceeded", shutdownErr)
	}
	released := false
	if err := releaseAfterSafeDrain(shutdownErr, func() { released = true }); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("releaseAfterSafeDrain error = %v, want DeadlineExceeded", err)
	}
	if released {
		t.Fatal("dependencies released while HTTP handler was still running")
	}

	close(release)
	select {
	case <-handlerDone:
	case <-time.After(time.Second):
		t.Fatal("handler did not exit after release")
	}
	if err := <-requestDone; err != nil {
		t.Fatalf("request failed after handler release: %v", err)
	}
	if err := <-serveDone; !errors.Is(err, http.ErrServerClosed) {
		t.Fatalf("Serve error = %v, want ErrServerClosed", err)
	}
}

func TestHTTPShutdownCanBeReprovedAfterOwnedDrain(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	handlerDone := make(chan struct{})
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(entered)
		<-release
		w.WriteHeader(http.StatusNoContent)
		close(handlerDone)
	})}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- srv.Serve(ln) }()
	requestDone := make(chan error, 1)
	go func() {
		resp, callErr := http.Get("http://" + ln.Addr().String())
		if callErr == nil {
			resp.Body.Close()
		}
		requestDone <- callErr
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("blocking handler did not start")
	}

	initialAttempt := beginHTTPShutdown(srv, 25*time.Millisecond)
	// Prove the admission-closing attempt actually expired while the
	// subsystem-owned work remained in flight. Sleeping for a nominally longer
	// duration is scheduler-dependent under the race detector.
	initialErr := <-initialAttempt
	if !errors.Is(initialErr, context.DeadlineExceeded) {
		t.Fatalf("initial HTTP shutdown error = %v, want deadline exceeded", initialErr)
	}
	initial := make(chan error, 1)
	initial <- initialErr
	close(release)
	select {
	case <-handlerDone:
	case <-time.After(time.Second):
		t.Fatal("owned handler did not exit after drain")
	}
	shutdownErr := completeHTTPShutdown(srv, initial, time.Second)
	if shutdownErr != nil {
		t.Fatalf("final HTTP drain proof failed: %v", shutdownErr)
	}
	released := false
	if err := releaseAfterSafeDrain(shutdownErr, func() { released = true }); err != nil {
		t.Fatalf("releaseAfterSafeDrain: %v", err)
	}
	if !released {
		t.Fatal("dependencies were not released after final drain proof")
	}
	if err := <-requestDone; err != nil {
		t.Fatalf("request failed after owned drain: %v", err)
	}
	if err := <-serveDone; !errors.Is(err, http.ErrServerClosed) {
		t.Fatalf("Serve error = %v, want ErrServerClosed", err)
	}
}

func TestReleaseAfterSafeDrainReleasesOnSuccess(t *testing.T) {
	released := false
	if err := releaseAfterSafeDrain(nil, func() { released = true }); err != nil {
		t.Fatalf("releaseAfterSafeDrain error: %v", err)
	}
	if !released {
		t.Fatal("successful drain did not release dependencies")
	}
}
