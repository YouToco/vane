package scheduler

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"unicode"
	"unicode/utf8"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"

	"github.com/YouToco/vane/types"
	"github.com/YouToco/vane/workflow"
)

// researchV3Rollout holds two exact-task controls. Shadow never mutates the
// durable Schedule Action. Authority only admits the explicit cutover saga;
// normal create/edit/reconcile paths never interpret it as an Action selector.
// Neither control has an allow-all state.
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

// WithResearchRuntimeV3AuthorityCanary admits only the explicit receipt-backed
// cutover control plane for one exact task. It does not rewrite normal Actions.
func WithResearchRuntimeV3AuthorityCanary(scheduleID string) SchedulerOption {
	return func(s *Scheduler) {
		trimmed := strings.TrimSpace(scheduleID)
		if !validResearchV3RolloutID(trimmed) {
			s.researchV3.authorityID = ""
			return
		}
		s.researchV3.authorityID = trimmed
	}
}

func validResearchV3RolloutID(value string) bool {
	if value == "" || len(value) > 255 || !utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) || unicode.In(r, unicode.Cf) {
			return false
		}
	}
	return true
}

func decodeResearchScheduledActionV3(
	action client.ScheduleAction,
) (workflow.ResearchScheduledInputV3, bool, error) {
	wf, ok := action.(*client.ScheduleWorkflowAction)
	if !ok || scheduleActionWorkflowNameV3(wf) != workflow.ResearchScheduledWorkflowV3Name {
		return workflow.ResearchScheduledInputV3{}, false, nil
	}
	if len(wf.Args) != 1 {
		return workflow.ResearchScheduledInputV3{}, true, types.NewAppError(
			types.CodeConflict, "research V3 Schedule Action envelope is malformed", types.ErrConflict)
	}
	var input workflow.ResearchScheduledInputV3
	switch first := wf.Args[0].(type) {
	case workflow.ResearchScheduledInputV3:
		input = first
	case *commonpb.Payload:
		if err := converter.GetDefaultDataConverter().FromPayload(first, &input); err != nil {
			return workflow.ResearchScheduledInputV3{}, true, types.NewAppError(
				types.CodeConflict, "decode research V3 Schedule Action envelope", err)
		}
	default:
		return workflow.ResearchScheduledInputV3{}, true, types.NewAppError(
			types.CodeConflict, "research V3 Schedule Action envelope type is invalid", types.ErrConflict)
	}
	if err := validateResearchScheduledInputV3(input); err != nil {
		return workflow.ResearchScheduledInputV3{}, true, types.NewAppError(
			types.CodeConflict, "research V3 Schedule Action envelope is invalid", err)
	}
	return input, true, nil
}

func validateResearchScheduledInputV3(input workflow.ResearchScheduledInputV3) error {
	if input.TenantID <= 0 || input.UserID <= 0 || !validResearchV3RolloutID(input.TaskID) ||
		len(input.ActionAuthorizationToken) != 64 ||
		input.ActionAuthorizationToken != strings.ToLower(input.ActionAuthorizationToken) {
		return types.ErrConflict
	}
	if _, err := hex.DecodeString(input.ActionAuthorizationToken); err != nil {
		return errors.Join(types.ErrConflict, err)
	}
	return nil
}

func scheduleActionWorkflowNameV3(action *client.ScheduleWorkflowAction) string {
	if action == nil {
		return ""
	}
	if name, ok := action.Workflow.(string); ok {
		return name
	}
	value := reflect.ValueOf(action.Workflow)
	if !value.IsValid() || value.Kind() != reflect.Func || value.IsNil() {
		return ""
	}
	function := runtime.FuncForPC(value.Pointer())
	if function == nil {
		return ""
	}
	name := function.Name()
	if index := strings.LastIndexByte(name, '.'); index >= 0 {
		name = name[index+1:]
	}
	return name
}

// actionParamsFor applies all runtime selectors to one newly constructed
// durable Action. V3 replaces the complete legacy execution envelope rather
// than decorating it, so no retained Source scope, display prompt, or run
// snapshot can leak into the discover-at-run protocol.
func (s *Scheduler) actionParamsFor(params workflow.PushParams) workflow.PushParams {
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
