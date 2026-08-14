package agentfirstaudit

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	sdkmocks "go.temporal.io/sdk/mocks"

	vaneworkflow "github.com/YouToco/vane/server/workflow"
)

func TestProductionClockRunnerBindsStartContract(t *testing.T) {
	request := clockRunnerRequest()
	workflowID := "agent-first-retention-clock-" + request.OperationID
	stop := errors.New("stop after exact workflow result read")
	run := sdkmocks.NewWorkflowRun(t)
	run.On("Get", mock.Anything, mock.Anything).Return(stop).Once()
	clientMock := sdkmocks.NewClient(t)
	clientMock.On("ExecuteWorkflow", mock.Anything,
		mock.MatchedBy(func(options client.StartWorkflowOptions) bool {
			return options.ID == workflowID && options.TaskQueue == request.TaskQueue &&
				options.WorkflowExecutionTimeout == 5*time.Minute &&
				options.WorkflowTaskTimeout == time.Minute &&
				options.WorkflowIDReusePolicy == enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE
		}),
		vaneworkflow.AgentFirstRetentionClockWorkflowNameV1,
		vaneworkflow.AgentFirstRetentionClockRequestV1{
			Nonce: request.OperationID, SourceRevision: request.SourceRevision,
		}).Return(run, nil).Once()

	_, err := productionClockRunner(clientMock, request)(t.Context(), TemporalAuthority{})
	if !errors.Is(err, stop) {
		t.Fatalf("error=%v", err)
	}
}

func TestProductionClockRunnerAdoptsExactAlreadyStartedRun(t *testing.T) {
	request := clockRunnerRequest()
	workflowID := "agent-first-retention-clock-" + request.OperationID
	runID := "123e4567-e89b-42d3-a456-426614174099"
	stop := errors.New("stop after adopted workflow result read")
	run := sdkmocks.NewWorkflowRun(t)
	run.On("Get", mock.Anything, mock.Anything).Return(stop).Once()
	clientMock := sdkmocks.NewClient(t)
	clientMock.On("ExecuteWorkflow", mock.Anything, mock.Anything,
		vaneworkflow.AgentFirstRetentionClockWorkflowNameV1, mock.Anything).
		Return(nil, serviceerror.NewWorkflowExecutionAlreadyStarted("exists", "request", runID)).Once()
	clientMock.On("GetWorkflow", mock.Anything, workflowID, runID).Return(run).Once()

	_, err := productionClockRunner(clientMock, request)(context.Background(), TemporalAuthority{})
	if !errors.Is(err, stop) {
		t.Fatalf("error=%v", err)
	}
}

func TestPrimeRetentionClockCannotBypassProductionStartContract(t *testing.T) {
	request := newBaselineCollectorFixture(t).request
	workflowID := "agent-first-retention-clock-" + request.OperationID
	stop := errors.New("stop after primed workflow result read")
	run := sdkmocks.NewWorkflowRun(t)
	run.On("Get", mock.Anything, mock.Anything).Return(stop).Once()
	clientMock := sdkmocks.NewClient(t)
	clientMock.On("ExecuteWorkflow", mock.Anything,
		mock.MatchedBy(func(options client.StartWorkflowOptions) bool {
			return options.ID == workflowID && options.TaskQueue == request.TaskQueue &&
				options.WorkflowExecutionTimeout == 5*time.Minute &&
				options.WorkflowTaskTimeout == time.Minute &&
				options.WorkflowIDReusePolicy ==
					enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE
		}),
		vaneworkflow.AgentFirstRetentionClockWorkflowNameV1,
		vaneworkflow.AgentFirstRetentionClockRequestV1{
			Nonce: request.OperationID, SourceRevision: request.SourceRevision,
		}).Return(run, nil).Once()

	_, err := PrimeRetentionClock(t.Context(), clientMock, request)
	if !errors.Is(err, stop) {
		t.Fatalf("error=%v", err)
	}
}

func clockRunnerRequest() BaselineCollectorRequest {
	return BaselineCollectorRequest{
		Namespace: "vane", TaskQueue: "vane-push",
		OperationID:    "123e4567-e89b-42d3-a456-426614174010",
		SourceRevision: strings.Repeat("a", 40),
	}
}
