package agentfirstaudit

import (
	"io"
	"log/slog"

	historypb "go.temporal.io/api/history/v1"
	sdklog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/worker"
	sdkworkflow "go.temporal.io/sdk/workflow"

	vaneworkflow "github.com/YouToco/vane/workflow"
)

func ReplayLegacyPushPipeline(
	history *historypb.History,
	execution sdkworkflow.Execution,
) error {
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflow(vaneworkflow.PushPipelineWorkflow)
	logger := sdklog.NewStructuredLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))
	return replayer.ReplayWorkflowHistoryWithOptions(logger, history,
		worker.ReplayWorkflowHistoryOptions{OriginalExecution: execution})
}
