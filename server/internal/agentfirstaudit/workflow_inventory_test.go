package agentfirstaudit

import (
	"context"
	"errors"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	sdkworkflow "go.temporal.io/sdk/workflow"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestReadLegacyWorkflowInventoryReadsEveryPageDescribesAndReplays(t *testing.T) {
	reader := validLegacyWorkflowReader()
	replayed := 0
	seenExecutions := map[string]bool{}
	inventory, err := readLegacyWorkflowInventoryWithReplayer(t.Context(), reader, "vane", false,
		func(history *historypb.History, execution sdkworkflow.Execution) error {
			replayed++
			want := reader.infos[execution.ID]
			if len(history.GetEvents()) != 2 || want == nil ||
				want.GetExecution().GetRunId() != execution.RunID {
				t.Fatalf("events=%d execution=%+v", len(history.GetEvents()), execution)
			}
			seenExecutions[execution.ID] = true
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if inventory.Archived || inventory.Count != 2 || replayed != 2 || len(inventory.Digest) != 64 ||
		inventory.Runs[0].WorkflowID != "legacy-a" || inventory.Runs[1].WorkflowID != "legacy-b" ||
		reader.standardCalls != 2 || reader.describeCalls != 2 || reader.historyCalls != 2 ||
		!seenExecutions["legacy-a"] || !seenExecutions["legacy-b"] {
		t.Fatalf("inventory=%+v reader=%+v replayed=%d", inventory, reader, replayed)
	}
}

func TestReadLegacyWorkflowInventoryUsesArchivedAuthority(t *testing.T) {
	reader := validLegacyWorkflowReader()
	reader.standardPages = map[string]*workflowservice.ListWorkflowExecutionsResponse{"": {}}
	reader.archivedPages = map[string]*workflowservice.ListArchivedWorkflowExecutionsResponse{
		"": {Executions: []*workflowpb.WorkflowExecutionInfo{reader.infos["legacy-a"]}},
	}
	reader.archivedHistory = true
	reader.describeNotFound = true
	inventory, err := readLegacyWorkflowInventoryWithReplayer(t.Context(), reader, "vane", true,
		func(*historypb.History, sdkworkflow.Execution) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if !inventory.Archived || inventory.Count != 1 || reader.archivedCalls != 1 ||
		reader.standardCalls != 0 || reader.describeCalls != 0 || !reader.lastHistoryRequestArchived {
		t.Fatalf("inventory=%+v reader=%+v", inventory, reader)
	}
}

func TestReadStableLegacyWorkflowInventoryRejectsSecondScanDrift(t *testing.T) {
	reader := validLegacyWorkflowReader()
	reader.driftSecondScan = true
	if _, err := readStableLegacyWorkflowInventoryWithReplayer(t.Context(), reader, "vane", false,
		func(*historypb.History, sdkworkflow.Execution) error { return nil }); err == nil {
		t.Fatal("visibility drift accepted")
	}
}

func TestRequireWorkflowVisibleUsesUnfilteredCompleteVisibility(t *testing.T) {
	reader := validLegacyWorkflowReader()
	if err := RequireWorkflowVisible(t.Context(), reader, "vane", "current-v3",
		"123e4567-e89b-42d3-a456-426614174003", "ResearchScheduledWorkflowV3"); err != nil {
		t.Fatal(err)
	}
	if reader.standardCalls != 2 {
		t.Fatalf("standard calls=%d", reader.standardCalls)
	}
	if err := RequireWorkflowVisible(t.Context(), reader, "vane", "missing",
		"123e4567-e89b-42d3-a456-426614174099", "ResearchScheduledWorkflowV3"); err == nil {
		t.Fatal("missing visibility calibration execution accepted")
	}
}

func TestVerifyLegacyExecutionsPhysicallyAbsentRequiresTwoNegativeProbes(t *testing.T) {
	runs := []LegacyWorkflowRun{{
		WorkflowID: "legacy-a", RunID: "123e4567-e89b-42d3-a456-426614174001",
	}}
	reader := validLegacyWorkflowReader()
	reader.describeNotFound = true
	reader.historyNotFound = true
	if err := VerifyLegacyExecutionsPhysicallyAbsent(t.Context(), reader, "vane", runs); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name     string
		describe bool
		history  bool
	}{
		{"description remains", false, true},
		{"history remains", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := validLegacyWorkflowReader()
			fixture.describeNotFound, fixture.historyNotFound = tc.describe, tc.history
			if err := VerifyLegacyExecutionsPhysicallyAbsent(
				t.Context(), fixture, "vane", runs); err == nil {
				t.Fatal("retained workflow was declared physically absent")
			}
		})
	}
}

func TestReadLegacyWorkflowInventoryRejectsIncompleteOrDriftingEvidence(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*legacyWorkflowReaderFake)
	}{
		{"visibility token cycle", func(f *legacyWorkflowReaderFake) {
			f.standardPages["next"].NextPageToken = []byte("next")
		}},
		{"duplicate run", func(f *legacyWorkflowReaderFake) {
			f.standardPages["next"].Executions = []*workflowpb.WorkflowExecutionInfo{f.infos["legacy-a"]}
		}},
		{"running run", func(f *legacyWorkflowReaderFake) {
			f.infos["legacy-a"].Status = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
		}},
		{"continued chain", func(f *legacyWorkflowReaderFake) {
			f.infos["legacy-a"].Status = enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW
		}},
		{"reset chain", func(f *legacyWorkflowReaderFake) {
			f.infos["legacy-a"].FirstRunId = "123e4567-e89b-42d3-a456-426614174099"
		}},
		{"description drift", func(f *legacyWorkflowReaderFake) {
			f.describeMutate = func(response *workflowservice.DescribeWorkflowExecutionResponse) {
				response.WorkflowExecutionInfo.HistoryLength++
			}
		}},
		{"pending activity", func(f *legacyWorkflowReaderFake) {
			f.pendingDescription = true
		}},
		{"history archive mismatch", func(f *legacyWorkflowReaderFake) {
			f.archivedHistory = true
		}},
		{"history token cycle", func(f *legacyWorkflowReaderFake) {
			f.historyTokenCycle = true
		}},
		{"history event drift", func(f *legacyWorkflowReaderFake) {
			f.historyEventMutate = func(events []*historypb.HistoryEvent) { events[1].EventId = 7 }
		}},
		{"history length drift", func(f *legacyWorkflowReaderFake) {
			f.infos["legacy-a"].HistoryLength++
		}},
		{"history closure drift", func(f *legacyWorkflowReaderFake) {
			f.historyEventMutate = func(events []*historypb.HistoryEvent) {
				events[1].EventType = enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED
			}
		}},
		{"replay failure", func(f *legacyWorkflowReaderFake) { f.replayFails = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := validLegacyWorkflowReader()
			tc.mutate(reader)
			_, err := readLegacyWorkflowInventoryWithReplayer(t.Context(), reader, "vane", false,
				func(*historypb.History, sdkworkflow.Execution) error {
					if reader.replayFails {
						return errors.New("nondeterministic")
					}
					return nil
				})
			if err == nil {
				t.Fatal("incomplete legacy evidence accepted")
			}
		})
	}
}

func TestReadLegacyWorkflowInventoryCannotBypassProductionReplayer(t *testing.T) {
	reader := validLegacyWorkflowReader()
	if _, err := ReadLegacyWorkflowInventory(t.Context(), reader, "vane", false); err == nil {
		t.Fatal("non-production-shaped history bypassed the fixed production replayer")
	}
}

type legacyWorkflowReaderFake struct {
	standardPages              map[string]*workflowservice.ListWorkflowExecutionsResponse
	archivedPages              map[string]*workflowservice.ListArchivedWorkflowExecutionsResponse
	infos                      map[string]*workflowpb.WorkflowExecutionInfo
	standardCalls              int
	archivedCalls              int
	describeCalls              int
	historyCalls               int
	archivedHistory            bool
	lastHistoryRequestArchived bool
	pendingDescription         bool
	historyTokenCycle          bool
	replayFails                bool
	driftSecondScan            bool
	describeNotFound           bool
	historyNotFound            bool
	describeMutate             func(*workflowservice.DescribeWorkflowExecutionResponse)
	historyEventMutate         func([]*historypb.HistoryEvent)
}

func validLegacyWorkflowReader() *legacyWorkflowReaderFake {
	first := legacyWorkflowInfoFixture("legacy-a", "123e4567-e89b-42d3-a456-426614174001")
	second := legacyWorkflowInfoFixture("legacy-b", "123e4567-e89b-42d3-a456-426614174002")
	current := legacyWorkflowInfoFixture("current-v3", "123e4567-e89b-42d3-a456-426614174003")
	current.Type.Name = "ResearchScheduledWorkflowV3"
	return &legacyWorkflowReaderFake{
		infos: map[string]*workflowpb.WorkflowExecutionInfo{"legacy-a": first, "legacy-b": second},
		standardPages: map[string]*workflowservice.ListWorkflowExecutionsResponse{
			"":     {Executions: []*workflowpb.WorkflowExecutionInfo{current, first}, NextPageToken: []byte("next")},
			"next": {Executions: []*workflowpb.WorkflowExecutionInfo{second}},
		},
		archivedPages: map[string]*workflowservice.ListArchivedWorkflowExecutionsResponse{"": {}},
	}
}

func legacyWorkflowInfoFixture(workflowID, runID string) *workflowpb.WorkflowExecutionInfo {
	started := time.Date(2026, 8, 1, 1, 2, 3, 0, time.UTC)
	return &workflowpb.WorkflowExecutionInfo{
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
		Type:      &commonpb.WorkflowType{Name: legacyPushWorkflowType},
		StartTime: timestamppb.New(started), ExecutionTime: timestamppb.New(started),
		CloseTime:     timestamppb.New(started.Add(time.Second)),
		Status:        enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		HistoryLength: 2, HistorySizeBytes: 200, TaskQueue: "vane-push",
		StateTransitionCount: 2, FirstRunId: runID,
	}
}

func (f *legacyWorkflowReaderFake) ListWorkflowExecutions(
	_ context.Context, request *workflowservice.ListWorkflowExecutionsRequest, _ ...grpc.CallOption,
) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	f.standardCalls++
	if request.Namespace != "vane" || request.Query != "" ||
		request.PageSize != legacyVisibilityPageSize {
		return nil, errors.New("request differs")
	}
	response := f.standardPages[string(request.NextPageToken)]
	if response == nil {
		return nil, errors.New("unknown page")
	}
	cloned := proto.Clone(response).(*workflowservice.ListWorkflowExecutionsResponse)
	if f.driftSecondScan && f.standardCalls >= 3 && len(cloned.Executions) > 0 {
		cloned.Executions[len(cloned.Executions)-1].HistoryLength++
	}
	return cloned, nil
}

func (f *legacyWorkflowReaderFake) ListArchivedWorkflowExecutions(
	_ context.Context, request *workflowservice.ListArchivedWorkflowExecutionsRequest,
	_ ...grpc.CallOption,
) (*workflowservice.ListArchivedWorkflowExecutionsResponse, error) {
	f.archivedCalls++
	if request.Namespace != "vane" || request.Query != "" ||
		request.PageSize != legacyVisibilityPageSize {
		return nil, errors.New("request differs")
	}
	response := f.archivedPages[string(request.NextPageToken)]
	if response == nil {
		return nil, errors.New("unknown page")
	}
	return proto.Clone(response).(*workflowservice.ListArchivedWorkflowExecutionsResponse), nil
}

func (f *legacyWorkflowReaderFake) DescribeWorkflowExecution(
	_ context.Context, request *workflowservice.DescribeWorkflowExecutionRequest,
	_ ...grpc.CallOption,
) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	f.describeCalls++
	if f.describeNotFound {
		return nil, serviceerror.NewNotFound("absent")
	}
	info := f.infos[request.GetExecution().GetWorkflowId()]
	if info == nil || info.GetExecution().GetRunId() != request.GetExecution().GetRunId() {
		return nil, errors.New("unknown execution")
	}
	response := &workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: proto.Clone(info).(*workflowpb.WorkflowExecutionInfo),
	}
	if f.pendingDescription {
		response.PendingActivities = []*workflowpb.PendingActivityInfo{{}}
	}
	if f.describeMutate != nil {
		f.describeMutate(response)
	}
	return response, nil
}

func (f *legacyWorkflowReaderFake) GetWorkflowExecutionHistory(
	_ context.Context, request *workflowservice.GetWorkflowExecutionHistoryRequest,
	_ ...grpc.CallOption,
) (*workflowservice.GetWorkflowExecutionHistoryResponse, error) {
	f.historyCalls++
	if f.historyNotFound {
		return nil, serviceerror.NewNotFound("absent")
	}
	f.lastHistoryRequestArchived = !request.GetSkipArchival()
	if f.historyTokenCycle {
		return &workflowservice.GetWorkflowExecutionHistoryResponse{
			History: &historypb.History{}, NextPageToken: []byte("cycle"), Archived: f.archivedHistory,
		}, nil
	}
	started := time.Date(2026, 8, 1, 1, 2, 3, 0, time.UTC)
	events := []*historypb.HistoryEvent{
		{EventId: 1, EventTime: timestamppb.New(started),
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
				WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
					WorkflowType: &commonpb.WorkflowType{Name: legacyPushWorkflowType},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: "vane-push"},
				},
			}},
		{EventId: 2, EventTime: timestamppb.New(started.Add(time.Second)),
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{
				WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{},
			}},
	}
	if f.historyEventMutate != nil {
		f.historyEventMutate(events)
	}
	return &workflowservice.GetWorkflowExecutionHistoryResponse{
		History: &historypb.History{Events: events}, Archived: f.archivedHistory,
	}, nil
}
