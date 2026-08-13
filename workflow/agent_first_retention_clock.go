package workflow

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"go.temporal.io/sdk/workflow"
)

const AgentFirstRetentionClockWorkflowNameV1 = "AgentFirstRetentionClockWorkflowV1"

// AgentFirstRetentionClockRequestV1 binds the server-time witness to one
// operator-generated nonce and one exact source revision. Neither value grants
// authority; the retention audit persists and independently digests the
// completed workflow history before it may become an attestation baseline.
type AgentFirstRetentionClockRequestV1 struct {
	Nonce          string `json:"nonce"`
	SourceRevision string `json:"source_revision"`
}

// AgentFirstRetentionClockResultV1 is deliberately small and contains only
// values recorded in immutable Temporal history. ObservedAtUTC comes from
// workflow.Now, not the operator workstation clock.
type AgentFirstRetentionClockResultV1 struct {
	Nonce          string `json:"nonce"`
	SourceRevision string `json:"source_revision"`
	Namespace      string `json:"namespace"`
	WorkflowID     string `json:"workflow_id"`
	RunID          string `json:"run_id"`
	ObservedAtUTC  string `json:"observed_at_utc"`
}

// AgentFirstRetentionClockWorkflowV1 provides a Temporal-server-clock witness
// for the deployment-bound retention audit. It performs no Activity and has no
// application side effect.
func AgentFirstRetentionClockWorkflowV1(
	ctx workflow.Context,
	request AgentFirstRetentionClockRequestV1,
) (AgentFirstRetentionClockResultV1, error) {
	if !validRetentionClockToken(request.Nonce, 128) ||
		!validRetentionClockToken(request.SourceRevision, 64) {
		return AgentFirstRetentionClockResultV1{},
			fmt.Errorf("agent-first retention clock request is invalid")
	}
	info := workflow.GetInfo(ctx)
	return AgentFirstRetentionClockResultV1{
		Nonce:          request.Nonce,
		SourceRevision: request.SourceRevision,
		Namespace:      info.Namespace,
		WorkflowID:     info.WorkflowExecution.ID,
		RunID:          info.WorkflowExecution.RunID,
		ObservedAtUTC:  workflow.Now(ctx).UTC().Format("2006-01-02T15:04:05.999999999Z"),
	}, nil
}

func validRetentionClockToken(value string, maximum int) bool {
	if value == "" || len(value) > maximum || !utf8.ValidString(value) ||
		strings.TrimSpace(value) != value {
		return false
	}
	for _, current := range value {
		if current < 0x21 || current > 0x7e {
			return false
		}
	}
	return true
}
