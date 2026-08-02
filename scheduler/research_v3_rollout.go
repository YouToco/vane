package scheduler

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"

	"github.com/YouToco/vane/types"
	"github.com/YouToco/vane/workflow"
)

// researchV3Rollout holds two deliberately independent exact-task controls.
// Shadow never mutates the durable Schedule Action; authority alone selects
// that Action for V3 authority. The current coordinator remains delivery-dark;
// enabling user-visible delivery is a separate reviewed Gate. Neither control
// has an allow-all state.
type researchV3Rollout struct {
	shadowID    string
	authorityID string
}

// WithResearchRuntimeV3ShadowCanary enables an independent, no-delivery V3
// execution for one exact task. It cannot affect a Schedule Action.
func WithResearchRuntimeV3ShadowCanary(scheduleID string) SchedulerOption {
	return func(s *Scheduler) {
		trimmed := strings.TrimSpace(scheduleID)
		if scheduleID != "" && trimmed == "" {
			s.researchV3.shadowID = ""
			return
		}
		s.researchV3.shadowID = trimmed
	}
}

// WithResearchRuntimeV3AuthorityCanary reserves the future delivery-capable
// durable V3 selector. It is hard-disabled until receipt-backed delivery and
// its canonical-definition preflight are implemented.
func WithResearchRuntimeV3AuthorityCanary(scheduleID string) SchedulerOption {
	return func(s *Scheduler) {
		// Delivery is not receipt-backed yet. Keep the future API surface but
		// hard-disable every direct option so tests or alternate composition roots
		// cannot silently rewrite a production Action into a no-delivery runtime.
		_ = scheduleID
		s.researchV3.authorityID = ""
	}
}

func (r researchV3Rollout) authoritySelected(taskID string) bool {
	return r.authorityID != "" && taskID == r.authorityID
}

// actionParamsFor applies all runtime selectors to one newly constructed
// durable Action. V3 replaces the complete legacy execution envelope rather
// than decorating it, so no retained Source scope, display prompt, or run
// snapshot can leak into the discover-at-run protocol.
func (s *Scheduler) actionParamsFor(params workflow.PushParams) workflow.PushParams {
	if s.researchV3.authoritySelected(params.ScheduleID) {
		return workflow.PushParams{
			TenantID:       params.TenantID,
			UserID:         params.UserID,
			RunKind:        workflow.PushRunKindScheduled,
			ExecutionMode:  types.ExecutionModeDiscoverAtRun,
			RuntimeVersion: workflow.ResearchRuntimeV3,
			ScheduleID:     params.ScheduleID,
		}
	}
	params.RuntimeVersion = s.runtimeVersionFor(
		params.ScheduleID, params.ExecutionMode)
	return params
}

type researchV3DefinitionStore interface {
	HasCurrentResearchApprovedDefinitionV3(
		context.Context, int64, int64, string,
	) (bool, error)
}

// TriggerResearchShadowNow starts a no-delivery V3 run beside the existing
// schedule. It never calls ScheduleClient, so the authoritative Action, spec,
// next fire time, and Monday 09:00 production path remain untouched.
func (s *Scheduler) TriggerResearchShadowNow(
	ctx context.Context,
	taskID string,
	userID int64,
	idempotencyKey string,
) error {
	if s == nil || s.c == nil || s.st == nil {
		return types.NewAppError(types.CodeInternal,
			"research V3 shadow control plane is unavailable", nil)
	}
	if !s.researchV3.shadowIDMatch(taskID) {
		return types.NewAppError(types.CodeNotFound,
			"research V3 shadow task is not configured", types.ErrNotFound)
	}
	if taskID == "" || strings.TrimSpace(taskID) != taskID || userID <= 0 ||
		idempotencyKey == "" || strings.TrimSpace(idempotencyKey) != idempotencyKey ||
		len(idempotencyKey) > 512 {
		return types.NewAppError(types.CodeValidation,
			"research V3 shadow request is invalid", types.ErrValidation)
	}
	schedule, err := s.st.GetSchedule(ctx, taskID, userID)
	if err != nil {
		return err
	}
	if schedule == nil || schedule.ID != taskID || schedule.UserID != userID ||
		schedule.TenantID <= 0 || schedule.Status != types.ScheduleStatusActive {
		return types.NewAppError(types.CodeNotFound,
			"research V3 shadow task is not active", types.ErrNotFound)
	}
	definitions, ok := s.st.(researchV3DefinitionStore)
	if !ok {
		return types.NewAppError(types.CodeConflict,
			"research V3 definition preflight is unavailable", types.ErrConflict)
	}
	available, err := definitions.HasCurrentResearchApprovedDefinitionV3(
		ctx, schedule.TenantID, userID, taskID)
	if err != nil {
		return err
	}
	if !available {
		return types.NewAppError(types.CodeConflict,
			"task has no current Research V3 approved definition", types.ErrConflict)
	}
	_, err = s.c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                                       researchShadowWorkflowID(taskID, idempotencyKey),
		TaskQueue:                                s.tq,
		WorkflowIDReusePolicy:                    enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		WorkflowExecutionErrorWhenAlreadyStarted: true,
	}, workflow.ResearchShadowWorkflowV3, workflow.ResearchShadowInputV3{
		TenantID: schedule.TenantID, UserID: userID, TaskID: taskID,
	})
	var alreadyStarted *serviceerror.WorkflowExecutionAlreadyStarted
	if errors.As(err, &alreadyStarted) {
		return nil
	}
	if err != nil {
		return types.NewAppError(types.CodeInternal,
			"start research V3 shadow workflow", err)
	}
	return nil
}

func (r researchV3Rollout) shadowIDMatch(taskID string) bool {
	return r.shadowID != "" && taskID == r.shadowID
}

func researchShadowWorkflowID(taskID, idempotencyKey string) string {
	digest := sha256.Sum256([]byte(taskID + "\x00" + idempotencyKey))
	return fmt.Sprintf("research-v3-shadow-%s", hex.EncodeToString(digest[:]))
}
