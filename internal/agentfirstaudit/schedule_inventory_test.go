package agentfirstaudit

import (
	"context"
	"errors"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	vaneworkflow "github.com/YouToco/vane/workflow"
)

func TestReadStableScheduleInventoryScansEveryPageAndDescribesEverySchedule(t *testing.T) {
	reader := validScheduleInventoryReader()
	inventory, err := ReadStableScheduleInventory(t.Context(), reader, "vane")
	if err != nil {
		t.Fatal(err)
	}
	if inventory.Count != 2 || len(inventory.Digest) != 64 || len(inventory.Items) != 2 ||
		len(inventory.Items[0].ActionDigest) != 64 ||
		reader.listCalls != 4 || reader.describeCalls != 4 {
		t.Fatalf("inventory=%+v reader=%+v", inventory, reader)
	}
}

func TestReadStableScheduleInventoryRejectsLegacyUnknownOrDriftingAuthority(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*scheduleInventoryReaderFake)
	}{
		{"token cycle", func(f *scheduleInventoryReaderFake) {
			f.pages["next"].NextPageToken = []byte("next")
		}},
		{"duplicate id", func(f *scheduleInventoryReaderFake) {
			f.pages["next"].Schedules[0].ScheduleId = "schedule-a"
		}},
		{"legacy list action", func(f *scheduleInventoryReaderFake) {
			f.pages[""].Schedules[0].Info.WorkflowType.Name = legacyPushWorkflowType
		}},
		{"legacy described action", func(f *scheduleInventoryReaderFake) {
			f.descriptions["schedule-a"].Schedule.Action.GetStartWorkflow().WorkflowType.Name =
				legacyPushWorkflowType
		}},
		{"unknown action", func(f *scheduleInventoryReaderFake) {
			f.descriptions["schedule-a"].Schedule.Action = &schedulepb.ScheduleAction{}
		}},
		{"missing conflict token", func(f *scheduleInventoryReaderFake) {
			f.descriptions["schedule-a"].ConflictToken = nil
		}},
		{"second scan drift", func(f *scheduleInventoryReaderFake) { f.driftSecondScan = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := validScheduleInventoryReader()
			tc.mutate(reader)
			if _, err := ReadStableScheduleInventory(t.Context(), reader, "vane"); err == nil {
				t.Fatal("unsafe schedule authority accepted")
			}
		})
	}
}

func TestValidateScheduleInventoryParityBindsActionAndPausedState(t *testing.T) {
	reader := validScheduleInventoryReader()
	inventory, err := ReadStableScheduleInventory(t.Context(), reader, "vane")
	if err != nil {
		t.Fatal(err)
	}
	expected := make([]ExpectedScheduleAuthority, 0, len(inventory.Items))
	for _, item := range inventory.Items {
		expected = append(expected, ExpectedScheduleAuthority{
			ScheduleID: item.ScheduleID, TargetActionDigest: item.ActionDigest, Paused: item.Paused,
		})
	}
	if err := ValidateScheduleInventoryParity(inventory, expected); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func([]ExpectedScheduleAuthority){
		func(items []ExpectedScheduleAuthority) { items[0].TargetActionDigest = emptySHA256 },
		func(items []ExpectedScheduleAuthority) { items[0].Paused = !items[0].Paused },
		func(items []ExpectedScheduleAuthority) { items[0].ScheduleID = "orphan" },
	} {
		copyItems := append([]ExpectedScheduleAuthority(nil), expected...)
		mutate(copyItems)
		if err := ValidateScheduleInventoryParity(inventory, copyItems); err == nil {
			t.Fatal("schedule parity drift accepted")
		}
	}
}

type scheduleInventoryReaderFake struct {
	pages           map[string]*workflowservice.ListSchedulesResponse
	descriptions    map[string]*workflowservice.DescribeScheduleResponse
	listCalls       int
	describeCalls   int
	driftSecondScan bool
}

func validScheduleInventoryReader() *scheduleInventoryReaderFake {
	return &scheduleInventoryReaderFake{
		pages: map[string]*workflowservice.ListSchedulesResponse{
			"": {Schedules: []*schedulepb.ScheduleListEntry{scheduleListEntryFixture("schedule-a")},
				NextPageToken: []byte("next")},
			"next": {Schedules: []*schedulepb.ScheduleListEntry{scheduleListEntryFixture("schedule-b")}},
		},
		descriptions: map[string]*workflowservice.DescribeScheduleResponse{
			"schedule-a": scheduleDescriptionFixture("schedule-a"),
			"schedule-b": scheduleDescriptionFixture("schedule-b"),
		},
	}
}

func scheduleListEntryFixture(id string) *schedulepb.ScheduleListEntry {
	return &schedulepb.ScheduleListEntry{
		ScheduleId: id,
		Info: &schedulepb.ScheduleListInfo{
			WorkflowType: &commonpb.WorkflowType{Name: vaneworkflow.ResearchScheduledWorkflowV3Name},
		},
	}
}

func scheduleDescriptionFixture(id string) *workflowservice.DescribeScheduleResponse {
	return &workflowservice.DescribeScheduleResponse{
		Schedule: &schedulepb.Schedule{
			Spec: &schedulepb.ScheduleSpec{}, Policies: &schedulepb.SchedulePolicies{},
			State: &schedulepb.ScheduleState{},
			Action: &schedulepb.ScheduleAction{Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   "workflow-" + id,
					WorkflowType: &commonpb.WorkflowType{Name: vaneworkflow.ResearchScheduledWorkflowV3Name},
				},
			}},
		},
		Info: &schedulepb.ScheduleInfo{}, ConflictToken: []byte("token-" + id),
	}
}

func (f *scheduleInventoryReaderFake) ListSchedules(
	_ context.Context, request *workflowservice.ListSchedulesRequest, _ ...grpc.CallOption,
) (*workflowservice.ListSchedulesResponse, error) {
	f.listCalls++
	if request.Namespace != "vane" || request.Query != "" ||
		request.MaximumPageSize != scheduleInventoryPageSize {
		return nil, errors.New("request differs")
	}
	response := f.pages[string(request.NextPageToken)]
	if response == nil {
		return nil, errors.New("unknown page")
	}
	cloned := proto.Clone(response).(*workflowservice.ListSchedulesResponse)
	if f.driftSecondScan && f.listCalls >= 3 && len(cloned.Schedules) > 0 {
		cloned.Schedules[0].ScheduleId += "-drift"
	}
	return cloned, nil
}

func (f *scheduleInventoryReaderFake) DescribeSchedule(
	_ context.Context, request *workflowservice.DescribeScheduleRequest, _ ...grpc.CallOption,
) (*workflowservice.DescribeScheduleResponse, error) {
	f.describeCalls++
	response := f.descriptions[request.ScheduleId]
	if response == nil {
		return nil, errors.New("unknown schedule")
	}
	return proto.Clone(response).(*workflowservice.DescribeScheduleResponse), nil
}
