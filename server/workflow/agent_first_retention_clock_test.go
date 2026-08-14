package workflow

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
)

func TestAgentFirstRetentionClockWorkflowV1UsesWorkflowHistoryTime(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestWorkflowEnvironment()
	started := time.Date(2026, time.August, 13, 12, 34, 56, 0, time.UTC)
	environment.SetStartTime(started)
	environment.ExecuteWorkflow(AgentFirstRetentionClockWorkflowV1,
		AgentFirstRetentionClockRequestV1{
			Nonce: "audit-019", SourceRevision: "a6bb47e",
		})
	require.NoError(t, environment.GetWorkflowError())
	var result AgentFirstRetentionClockResultV1
	require.NoError(t, environment.GetWorkflowResult(&result))
	require.Equal(t, "audit-019", result.Nonce)
	require.Equal(t, "a6bb47e", result.SourceRevision)
	require.Equal(t, started.Format(time.RFC3339Nano), result.ObservedAtUTC)
	require.NotEmpty(t, result.WorkflowID)
	require.NotEmpty(t, result.RunID)
}

func TestAgentFirstRetentionClockWorkflowV1RejectsUnboundInput(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestWorkflowEnvironment()
	environment.ExecuteWorkflow(AgentFirstRetentionClockWorkflowV1,
		AgentFirstRetentionClockRequestV1{Nonce: " bad", SourceRevision: "a6bb47e"})
	require.Error(t, environment.GetWorkflowError())
}
