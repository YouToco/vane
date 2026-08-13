// Package agentfirstaudit validates the external evidence consumed by the
// Agent-first legacy-retention Gate. It is deliberately independent from the
// migration ledger: Temporal observations are proved here, then PostgreSQL
// persists only their canonical digests.
package agentfirstaudit

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	vaneworkflow "github.com/YouToco/vane/workflow"
)

const (
	maxWitnessHistoryPages  = 16
	maxWitnessHistoryEvents = 64
)

type HistoryReader interface {
	GetWorkflowExecutionHistory(context.Context,
		*workflowservice.GetWorkflowExecutionHistoryRequest,
		...grpc.CallOption,
	) (*workflowservice.GetWorkflowExecutionHistoryResponse, error)
}

type RetentionClockExpectation struct {
	Namespace      string
	WorkflowID     string
	RunID          string
	TaskQueue      string
	Nonce          string
	SourceRevision string
	WorkerBuildID  string
}

type RetentionClockEvidence struct {
	Namespace     string
	WorkflowID    string
	RunID         string
	TaskQueue     string
	ObservedAtUTC time.Time
	HistoryDigest string
	EventCount    int
	WorkerBuildID string
}

// ReadRetentionClockEvidence reads every history page and accepts only the
// five-event, no-side-effect Workflow shape. The server event timestamp and
// worker BuildID are authority; the Workflow result is checked only as a
// redundant deterministic projection.
func ReadRetentionClockEvidence(
	ctx context.Context,
	reader HistoryReader,
	expect RetentionClockExpectation,
) (RetentionClockEvidence, error) {
	if reader == nil || expect.Namespace == "" || expect.WorkflowID == "" ||
		expect.RunID == "" || expect.TaskQueue == "" || expect.Nonce == "" ||
		!validSourceRevision(expect.SourceRevision) ||
		expect.WorkerBuildID != "vane/"+expect.SourceRevision {
		return RetentionClockEvidence{}, fmt.Errorf("retention clock expectation is invalid")
	}
	var events []*historypb.HistoryEvent
	var token []byte
	seenTokens := map[string]struct{}{}
	archived := false
	for page := 0; ; page++ {
		if page >= maxWitnessHistoryPages {
			return RetentionClockEvidence{}, fmt.Errorf("retention clock history exceeds page limit")
		}
		response, err := reader.GetWorkflowExecutionHistory(ctx,
			&workflowservice.GetWorkflowExecutionHistoryRequest{
				Namespace: expect.Namespace,
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: expect.WorkflowID, RunId: expect.RunID,
				},
				NextPageToken:          token,
				WaitNewEvent:           false,
				HistoryEventFilterType: enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT,
				SkipArchival:           true,
			})
		if err != nil {
			return RetentionClockEvidence{}, fmt.Errorf("read retention clock history: %w", err)
		}
		if response == nil || response.History == nil {
			return RetentionClockEvidence{}, fmt.Errorf("retention clock history page is absent")
		}
		archived = archived || response.Archived
		events = append(events, response.History.Events...)
		if len(events) > maxWitnessHistoryEvents {
			return RetentionClockEvidence{}, fmt.Errorf("retention clock history exceeds event limit")
		}
		next := response.NextPageToken
		if len(next) == 0 {
			break
		}
		key := string(next)
		if _, exists := seenTokens[key]; exists || bytes.Equal(next, token) {
			return RetentionClockEvidence{}, fmt.Errorf("retention clock history token cycles")
		}
		seenTokens[key] = struct{}{}
		token = bytes.Clone(next)
	}
	if archived {
		return RetentionClockEvidence{}, fmt.Errorf("fresh retention clock history is archived")
	}
	evidence, err := validateRetentionClockEvents(events, expect)
	if err != nil {
		return RetentionClockEvidence{}, err
	}
	evidence.Namespace = expect.Namespace
	evidence.WorkflowID = expect.WorkflowID
	evidence.RunID = expect.RunID
	evidence.TaskQueue = expect.TaskQueue
	return evidence, nil
}

func validSourceRevision(value string) bool {
	if len(value) != 40 {
		return false
	}
	for _, current := range value {
		if !('0' <= current && current <= '9') &&
			!('a' <= current && current <= 'f') {
			return false
		}
	}
	return true
}

func validateRetentionClockEvents(
	events []*historypb.HistoryEvent,
	expect RetentionClockExpectation,
) (RetentionClockEvidence, error) {
	wantTypes := []enumspb.EventType{
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
	}
	if len(events) != len(wantTypes) {
		return RetentionClockEvidence{}, fmt.Errorf("retention clock history has %d events", len(events))
	}
	for index, event := range events {
		if event == nil || event.EventId != int64(index+1) ||
			event.EventType != wantTypes[index] || event.EventTime == nil ||
			event.EventTime.CheckValid() != nil {
			return RetentionClockEvidence{}, fmt.Errorf("retention clock event %d differs", index+1)
		}
	}
	started := events[0].GetWorkflowExecutionStartedEventAttributes()
	if started == nil || started.WorkflowType.GetName() !=
		vaneworkflow.AgentFirstRetentionClockWorkflowNameV1 ||
		started.TaskQueue.GetName() != expect.TaskQueue {
		return RetentionClockEvidence{}, fmt.Errorf("retention clock start authority differs")
	}
	expectedRequest, err := converter.GetDefaultDataConverter().ToPayloads(
		vaneworkflow.AgentFirstRetentionClockRequestV1{
			Nonce: expect.Nonce, SourceRevision: expect.SourceRevision,
		})
	if err != nil || !proto.Equal(started.Input, expectedRequest) {
		return RetentionClockEvidence{}, fmt.Errorf("retention clock input bytes differ")
	}
	var request vaneworkflow.AgentFirstRetentionClockRequestV1
	if err := converter.GetDefaultDataConverter().FromPayloads(started.Input, &request); err != nil ||
		request.Nonce != expect.Nonce || request.SourceRevision != expect.SourceRevision {
		return RetentionClockEvidence{}, fmt.Errorf("retention clock input differs")
	}
	taskStarted := events[2].GetWorkflowTaskStartedEventAttributes()
	taskCompleted := events[3].GetWorkflowTaskCompletedEventAttributes()
	if taskStarted == nil || taskStarted.ScheduledEventId != 2 ||
		taskCompleted == nil || taskCompleted.ScheduledEventId != 2 ||
		taskCompleted.StartedEventId != 3 ||
		taskCompleted.GetWorkerVersion().GetBuildId() != expect.WorkerBuildID ||
		taskCompleted.GetWorkerVersion().GetUseVersioning() {
		return RetentionClockEvidence{}, fmt.Errorf("retention clock worker authority differs")
	}
	completed := events[4].GetWorkflowExecutionCompletedEventAttributes()
	var result vaneworkflow.AgentFirstRetentionClockResultV1
	if completed == nil || converter.GetDefaultDataConverter().FromPayloads(
		completed.Result, &result) != nil {
		return RetentionClockEvidence{}, fmt.Errorf("retention clock result is invalid")
	}
	observed := events[2].EventTime.AsTime().UTC()
	parsed, err := time.Parse(time.RFC3339Nano, result.ObservedAtUTC)
	if err != nil || !parsed.Equal(observed) || result.Nonce != expect.Nonce ||
		result.SourceRevision != expect.SourceRevision ||
		result.Namespace != expect.Namespace || result.WorkflowID != expect.WorkflowID ||
		result.RunID != expect.RunID {
		return RetentionClockEvidence{}, fmt.Errorf("retention clock result differs from server history")
	}
	expectedResult, err := converter.GetDefaultDataConverter().ToPayloads(result)
	if err != nil || !proto.Equal(completed.Result, expectedResult) {
		return RetentionClockEvidence{}, fmt.Errorf("retention clock result bytes differ")
	}
	history := &historypb.History{Events: events}
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(history)
	if err != nil {
		return RetentionClockEvidence{}, fmt.Errorf("marshal retention clock history: %w", err)
	}
	digest := sha256.Sum256(raw)
	return RetentionClockEvidence{
		ObservedAtUTC: observed,
		HistoryDigest: hex.EncodeToString(digest[:]),
		EventCount:    len(events),
		WorkerBuildID: expect.WorkerBuildID,
	}, nil
}
