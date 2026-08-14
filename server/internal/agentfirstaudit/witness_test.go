package agentfirstaudit

import (
	"context"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	vaneworkflow "github.com/YouToco/vane/workflow"
)

type historyReaderFunc func(*workflowservice.GetWorkflowExecutionHistoryRequest) (
	*workflowservice.GetWorkflowExecutionHistoryResponse, error)

func (function historyReaderFunc) GetWorkflowExecutionHistory(
	_ context.Context,
	request *workflowservice.GetWorkflowExecutionHistoryRequest,
	_ ...grpc.CallOption,
) (*workflowservice.GetWorkflowExecutionHistoryResponse, error) {
	return function(request)
}

func TestReadRetentionClockEvidenceBindsServerTimeAndWorkerBuild(t *testing.T) {
	expect, events := retentionClockHistoryFixture(t)
	calls := 0
	reader := historyReaderFunc(func(request *workflowservice.GetWorkflowExecutionHistoryRequest) (
		*workflowservice.GetWorkflowExecutionHistoryResponse, error) {
		calls++
		if calls == 1 {
			return &workflowservice.GetWorkflowExecutionHistoryResponse{
				History: &historypb.History{Events: events[:3]}, NextPageToken: []byte("next"),
			}, nil
		}
		if string(request.NextPageToken) != "next" {
			t.Fatalf("next token=%q", request.NextPageToken)
		}
		return &workflowservice.GetWorkflowExecutionHistoryResponse{
			History: &historypb.History{Events: events[3:]},
		}, nil
	})
	evidence, err := ReadRetentionClockEvidence(context.Background(), reader, expect)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 2 || evidence.EventCount != 5 || evidence.HistoryDigest == "" ||
		evidence.WorkerBuildID != expect.WorkerBuildID ||
		evidence.Namespace != expect.Namespace || evidence.WorkflowID != expect.WorkflowID ||
		evidence.RunID != expect.RunID || evidence.TaskQueue != expect.TaskQueue ||
		!evidence.ObservedAtUTC.Equal(events[2].EventTime.AsTime()) {
		t.Fatalf("calls=%d evidence=%+v", calls, evidence)
	}
}

func TestRetentionClockEvidenceRejectsRepresentationDrift(t *testing.T) {
	expect, events := retentionClockHistoryFixture(t)
	tests := map[string]func(){
		"wrong worker": func() {
			events[3].GetWorkflowTaskCompletedEventAttributes().WorkerVersion.BuildId = "vane/other"
		},
		"side effect event": func() {
			events = append(events, events[len(events)-1])
		},
		"forged result time": func() {
			completed := events[4].GetWorkflowExecutionCompletedEventAttributes()
			result := vaneworkflow.AgentFirstRetentionClockResultV1{
				Nonce: expect.Nonce, SourceRevision: expect.SourceRevision,
				Namespace: expect.Namespace, WorkflowID: expect.WorkflowID,
				RunID: expect.RunID, ObservedAtUTC: time.Now().UTC().Format(time.RFC3339Nano),
			}
			completed.Result, _ = converter.GetDefaultDataConverter().ToPayloads(result)
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			expect, events = retentionClockHistoryFixture(t)
			mutate()
			_, err := validateRetentionClockEvents(events, expect)
			if err == nil {
				t.Fatal("drift accepted")
			}
		})
	}
}

func TestReadRetentionClockEvidenceRejectsArchivedHistory(t *testing.T) {
	expect, events := retentionClockHistoryFixture(t)
	reader := historyReaderFunc(func(*workflowservice.GetWorkflowExecutionHistoryRequest) (
		*workflowservice.GetWorkflowExecutionHistoryResponse, error) {
		return &workflowservice.GetWorkflowExecutionHistoryResponse{
			History: &historypb.History{Events: events}, Archived: true,
		}, nil
	})
	if _, err := ReadRetentionClockEvidence(context.Background(), reader, expect); err == nil {
		t.Fatal("archived fresh witness accepted")
	}
}

func TestReadRetentionClockEvidenceRejectsNonCanonicalRevision(t *testing.T) {
	expect, events := retentionClockHistoryFixture(t)
	expect.SourceRevision = "ABCDEF6789abcdef0123456789abcdef01234567"
	expect.WorkerBuildID = "vane/" + expect.SourceRevision
	reader := historyReaderFunc(func(*workflowservice.GetWorkflowExecutionHistoryRequest) (
		*workflowservice.GetWorkflowExecutionHistoryResponse, error) {
		return &workflowservice.GetWorkflowExecutionHistoryResponse{
			History: &historypb.History{Events: events},
		}, nil
	})
	if _, err := ReadRetentionClockEvidence(context.Background(), reader, expect); err == nil {
		t.Fatal("non-canonical source revision accepted")
	}
}

func retentionClockHistoryFixture(t *testing.T) (
	RetentionClockExpectation, []*historypb.HistoryEvent) {
	t.Helper()
	revision := "0123456789abcdef0123456789abcdef01234567"
	expect := RetentionClockExpectation{
		Namespace: "vane", WorkflowID: "retention-clock-1", RunID: "run-1",
		TaskQueue: "vane-push", Nonce: "nonce-1", SourceRevision: revision,
		WorkerBuildID: "vane/" + revision,
	}
	requestPayload, err := converter.GetDefaultDataConverter().ToPayloads(
		vaneworkflow.AgentFirstRetentionClockRequestV1{
			Nonce: expect.Nonce, SourceRevision: revision,
		})
	if err != nil {
		t.Fatal(err)
	}
	serverTime := time.Date(2026, 8, 13, 1, 2, 3, 456000000, time.UTC)
	resultPayload, err := converter.GetDefaultDataConverter().ToPayloads(
		vaneworkflow.AgentFirstRetentionClockResultV1{
			Nonce: expect.Nonce, SourceRevision: revision, Namespace: expect.Namespace,
			WorkflowID: expect.WorkflowID, RunID: expect.RunID,
			ObservedAtUTC: serverTime.Format(time.RFC3339Nano),
		})
	if err != nil {
		t.Fatal(err)
	}
	event := func(id int64, eventType enumspb.EventType) *historypb.HistoryEvent {
		return &historypb.HistoryEvent{
			EventId: id, EventType: eventType,
			EventTime: timestamppb.New(serverTime.Add(time.Duration(id-3) * time.Millisecond)),
		}
	}
	events := []*historypb.HistoryEvent{
		event(1, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED),
		event(2, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED),
		event(3, enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED),
		event(4, enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED),
		event(5, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED),
	}
	events[0].Attributes = &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
		WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
			WorkflowType: &commonpb.WorkflowType{Name: vaneworkflow.AgentFirstRetentionClockWorkflowNameV1},
			TaskQueue:    &taskqueuepb.TaskQueue{Name: expect.TaskQueue}, Input: requestPayload,
		},
	}
	events[1].Attributes = &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{
		WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{},
	}
	events[2].Attributes = &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{
		WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
			ScheduledEventId: 2,
		},
	}
	events[3].Attributes = &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{
		WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
			ScheduledEventId: 2, StartedEventId: 3,
			WorkerVersion: &commonpb.WorkerVersionStamp{BuildId: expect.WorkerBuildID},
		},
	}
	events[4].Attributes = &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{
		WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{
			Result: resultPayload,
		},
	}
	return expect, events
}
