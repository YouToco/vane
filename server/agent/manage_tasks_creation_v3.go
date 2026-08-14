package agent

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/YouToco/vane/task"
	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

// ResearchTaskCreationV3Controller is the narrow native lifecycle consumed by
// manage_tasks. It intentionally excludes every V1 create_schedule method and
// all low-level lease/checkpoint operations.
type ResearchTaskCreationV3Controller interface {
	PrepareResearchV3(
		context.Context,
		task.ResearchV3CreationProposalInput,
	) (task.CreationProposal, error)
	ExecuteResearchV3(
		context.Context,
		int64,
		string,
		task.CreationReceiptTarget,
	) (task.CreationResult, error)
}

type researchTaskCreationV3Executor struct {
	controller ResearchTaskCreationV3Controller
}

func NewResearchTaskCreationV3Executor(
	controller ResearchTaskCreationV3Controller,
) ResearchTaskCreationV3Executor {
	if controller == nil {
		return nil
	}
	return &researchTaskCreationV3Executor{controller: controller}
}

func (e *researchTaskCreationV3Executor) ExecuteResearchTaskCreationV3(
	ctx context.Context,
	in ResearchTaskCreationV3Input,
) (ResearchTaskCreationV3Outcome, error) {
	if e == nil || e.controller == nil {
		return ResearchTaskCreationV3Outcome{},
			errors.New("agent: native V3 creation controller is unavailable")
	}
	proposal, err := e.controller.PrepareResearchV3(ctx,
		task.ResearchV3CreationProposalInput{
			ActionID: in.ActionID, UserID: in.UserID, SessionID: in.SessionID,
			TaskName: in.Name, TaskManual: in.Manual, SpecJSON: in.Schedule,
			Notification: taskstate.NotificationPolicyV3{
				MinimumSignificance: taskstate.NotificationThresholdV3(
					in.Notification.MinimumSignificance),
				SuppressEmpty: in.Notification.SuppressEmpty,
			},
			Output: taskstate.OutputPreferenceV3{
				Language:             taskstate.OutputLanguageV3(in.Output.Language),
				Format:               taskstate.OutputFormatV3(in.Output.Format),
				Instructions:         in.Output.Instructions,
				IncludeEvidenceLinks: in.Output.IncludeEvidenceLinks,
			},
			ExpiresAt: time.Now().Add(durableOperationTTL),
		})
	if err != nil {
		return ResearchTaskCreationV3Outcome{},
			fmt.Errorf("prepare native V3 task creation: %w", err)
	}
	if proposal.ID != in.ActionID || strings.TrimSpace(proposal.Summary) != in.Name {
		return ResearchTaskCreationV3Outcome{},
			errors.New("agent: native V3 creation proposal identity is invalid")
	}
	result, err := e.controller.ExecuteResearchV3(
		ctx, in.UserID, proposal.ID, task.AgentAutoReceiptTarget(proposal.ID))
	if err != nil {
		return ResearchTaskCreationV3Outcome{},
			fmt.Errorf("execute native V3 task creation: %w", err)
	}
	if result.OperationID != proposal.ID || strings.TrimSpace(result.Summary) != in.Name {
		return ResearchTaskCreationV3Outcome{},
			errors.New("agent: native V3 creation result identity is invalid")
	}
	if !result.ReceiptBound || (result.SessionID == nil) != (in.SessionID == nil) ||
		(result.SessionID != nil && *result.SessionID != *in.SessionID) {
		return ResearchTaskCreationV3Outcome{},
			errors.New("agent: native V3 creation receipt binding is invalid")
	}
	status := ""
	switch result.Status {
	case types.TaskOperationStatusExecuted:
		status = "completed"
	case types.TaskOperationStatusExecuting:
		status = "executing"
	case types.TaskOperationStatusBlocked,
		types.TaskOperationStatusFailed,
		types.TaskOperationStatusCancelled,
		types.TaskOperationStatusExpired:
		status = "blocked"
	default:
		return ResearchTaskCreationV3Outcome{},
			errors.New("agent: native V3 creation result status is invalid")
	}
	taskRef := strings.TrimSpace(result.TaskID)
	if status == "blocked" {
		taskRef = ""
	}
	return ResearchTaskCreationV3Outcome{
		TaskRef:  taskRef,
		TaskName: in.Name,
		Status:   status,
	}, nil
}
