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

var errResearchTaskDefinitionEditNotExecuted = errors.New(
	"agent: native V3 task edit was not executed")

// ResearchTaskDefinitionEditV3Controller is the narrow native V3 lifecycle
// consumed by manage_tasks. It deliberately excludes global recovery claims,
// phase checkpoints and every retained V1/V2 edit method.
type ResearchTaskDefinitionEditV3Controller interface {
	PrepareChanges(
		context.Context,
		task.ResearchTaskDefinitionEditChangesInputV3,
	) (*types.TaskDefinitionEditOperation, error)
	Execute(
		context.Context,
		types.TaskDefinitionEditScope,
		task.TaskDefinitionEditReceiptTarget,
	) (task.TaskDefinitionEditOutcome, error)
}

type researchTaskDefinitionEditV3Executor struct {
	controller ResearchTaskDefinitionEditV3Controller
}

func NewResearchTaskDefinitionEditV3Executor(
	controller ResearchTaskDefinitionEditV3Controller,
) ResearchTaskDefinitionEditV3Executor {
	if controller == nil {
		return nil
	}
	return &researchTaskDefinitionEditV3Executor{controller: controller}
}

func (e *researchTaskDefinitionEditV3Executor) ExecuteResearchTaskDefinitionEditV3(
	ctx context.Context, in ResearchTaskDefinitionEditV3Input,
) (ResearchTaskDefinitionEditV3Outcome, error) {
	if e == nil || e.controller == nil {
		return ResearchTaskDefinitionEditV3Outcome{},
			errors.New("agent: native V3 edit controller is unavailable")
	}
	changes := task.ResearchV3DefinitionChanges{
		TaskName: in.Changes.Name, TaskManual: in.Changes.Manual,
		SpecJSON: in.Changes.Schedule,
	}
	if in.Changes.Notification != nil {
		changes.Notification = &taskstate.NotificationPolicyV3{
			MinimumSignificance: taskstate.NotificationThresholdV3(
				in.Changes.Notification.MinimumSignificance),
			SuppressEmpty: in.Changes.Notification.SuppressEmpty,
		}
	}
	if in.Changes.Output != nil {
		changes.Output = &taskstate.OutputPreferenceV3{
			Language:             taskstate.OutputLanguageV3(in.Changes.Output.Language),
			Format:               taskstate.OutputFormatV3(in.Changes.Output.Format),
			Instructions:         in.Changes.Output.Instructions,
			IncludeEvidenceLinks: in.Changes.Output.IncludeEvidenceLinks,
		}
	}
	op, err := e.controller.PrepareChanges(ctx,
		task.ResearchTaskDefinitionEditChangesInputV3{
			ActionID: in.ActionID, TenantID: in.TenantID, UserID: in.UserID,
			TaskID: in.TaskRef, Changes: changes, SessionID: in.SessionID,
			ExpiresAt: time.Now().Add(durableOperationTTL),
		})
	if err != nil {
		if errors.Is(err, task.ErrResearchTaskDefinitionEditNotExecuted) {
			return ResearchTaskDefinitionEditV3Outcome{}, fmt.Errorf(
				"%w: %v", errResearchTaskDefinitionEditNotExecuted, err)
		}
		return ResearchTaskDefinitionEditV3Outcome{},
			fmt.Errorf("prepare native V3 task edit: %w", err)
	}
	if op == nil || op.Protocol != types.TaskDefinitionEditProtocolResearchV3 ||
		op.ID != in.ActionID || op.TenantID != in.TenantID ||
		op.UserID != in.UserID || op.TargetTenantID != in.TenantID ||
		op.TargetUserID != in.UserID || op.TaskID != in.TaskRef ||
		op.SessionID != in.SessionID {
		return ResearchTaskDefinitionEditV3Outcome{},
			errors.New("agent: native V3 edit proposal identity is invalid")
	}
	target, err := taskstate.DecodeApprovedDefinitionV3(op.TargetDefinition)
	if err != nil || target.TenantID != in.TenantID || target.UserID != in.UserID ||
		target.TaskID != in.TaskRef || strings.TrimSpace(target.TaskName) == "" {
		return ResearchTaskDefinitionEditV3Outcome{},
			errors.New("agent: native V3 edit target definition is invalid")
	}
	outcome, err := e.controller.Execute(ctx, op.Scope(),
		task.TaskDefinitionEditReceiptTarget{
			Provider: task.AgentAutoReceiptProvider,
			Target:   op.ID,
		})
	if err != nil {
		return ResearchTaskDefinitionEditV3Outcome{},
			fmt.Errorf("execute native V3 task edit: %w", err)
	}
	if outcome.OperationID != op.ID || outcome.TaskID != op.TaskID ||
		outcome.SessionID != op.SessionID || !outcome.ReceiptBound {
		return ResearchTaskDefinitionEditV3Outcome{},
			errors.New("agent: native V3 edit result binding is invalid")
	}
	status := ""
	switch outcome.Status {
	case types.TaskDefinitionEditOperationStatusCompleted:
		status = "completed"
	case types.TaskDefinitionEditOperationStatusExecuting:
		status = "executing"
	case types.TaskDefinitionEditOperationStatusBlocked:
		status = "blocked"
	case types.TaskDefinitionEditOperationStatusSuperseded:
		status = "superseded"
	default:
		return ResearchTaskDefinitionEditV3Outcome{},
			errors.New("agent: native V3 edit result status is invalid")
	}
	return ResearchTaskDefinitionEditV3Outcome{
		OperationID: op.ID, TaskRef: op.TaskID,
		TaskName: target.TaskName, Status: status,
	}, nil
}
