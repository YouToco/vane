//go:build integration

package main

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	workflowSDK "go.temporal.io/sdk/workflow"
)

const workerShutdownActivityName = "vane-test-worker-shutdown-activity"

type workerShutdownProbe struct {
	started   chan struct{}
	release   chan struct{}
	tailWrite chan struct{}
	closed    atomic.Bool
	once      sync.Once
}

func (p *workerShutdownProbe) Run(context.Context) error {
	p.once.Do(func() { close(p.started) })
	<-p.release
	if p.closed.Load() {
		return errors.New("dependency closed before activity tail write")
	}
	close(p.tailWrite)
	return nil
}

func workerShutdownWorkflow(ctx workflowSDK.Context) error {
	ctx = workflowSDK.WithActivityOptions(ctx, workflowSDK.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
	})
	return workflowSDK.ExecuteActivity(ctx, workerShutdownActivityName).Get(ctx, nil)
}

func TestTemporalWorkerStopWaitsForActivityTail(t *testing.T) {
	const namespace = "vane-worker-shutdown-integration"
	startCtx, cancelStart := context.WithTimeout(t.Context(), 2*time.Minute)
	server, err := testsuite.StartDevServer(startCtx, testsuite.DevServerOptions{
		ClientOptions: &client.Options{Namespace: namespace},
		LogLevel:      "error",
	})
	cancelStart()
	if err != nil {
		t.Fatalf("start Temporal dev server: %v", err)
	}
	t.Cleanup(func() {
		server.Client().Close()
		if err := server.Stop(); err != nil {
			t.Errorf("stop Temporal dev server: %v", err)
		}
	})

	probe := &workerShutdownProbe{
		started:   make(chan struct{}),
		release:   make(chan struct{}),
		tailWrite: make(chan struct{}),
	}
	var releaseOnce sync.Once
	tq := "vane-worker-shutdown-test"
	w := worker.New(server.Client(), tq, temporalWorkerOptions())
	w.RegisterWorkflow(workerShutdownWorkflow)
	w.RegisterActivityWithOptions(probe.Run, activity.RegisterOptions{Name: workerShutdownActivityName})
	if err := w.Start(); err != nil {
		t.Fatalf("start worker: %v", err)
	}
	stopDone := make(chan struct{})
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(probe.release) })
		select {
		case <-stopDone:
		case <-time.After(5 * time.Second):
			t.Errorf("worker did not stop during cleanup")
		}
	})

	if _, err := server.Client().ExecuteWorkflow(t.Context(), client.StartWorkflowOptions{
		ID:        "vane-worker-shutdown-test-run",
		TaskQueue: tq,
	}, workerShutdownWorkflow); err != nil {
		t.Fatalf("start workflow: %v", err)
	}
	select {
	case <-probe.started:
	case <-time.After(10 * time.Second):
		t.Fatal("activity did not start")
	}

	go func() {
		w.Stop()
		close(stopDone)
	}()
	select {
	case <-stopDone:
		t.Fatal("Worker.Stop returned while Activity was still blocked")
	case <-time.After(100 * time.Millisecond):
	}

	releaseOnce.Do(func() { close(probe.release) })
	select {
	case <-probe.tailWrite:
	case <-time.After(5 * time.Second):
		t.Fatal("activity tail write did not complete")
	}
	select {
	case <-stopDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Worker.Stop did not return after Activity completed")
	}
	probe.closed.Store(true)
}
