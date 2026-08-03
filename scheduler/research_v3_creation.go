package scheduler

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/proto"

	"github.com/YouToco/vane/types"
	"github.com/YouToco/vane/workflow"
)

const preparedResearchTaskScheduleV3WireVersion = "vane.prepared-research-task-schedule/v3"

// PreparedResearchTaskScheduleV3 is a distinct durable wire. Schedule keeps
// the retained task-schedule/v1 lifecycle identity, while Input and the exact
// protobuf Action freeze the formal ResearchScheduledWorkflowV3 capability.
// No field is added to any V1 definition-edit or creation checkpoint layout.
type PreparedResearchTaskScheduleV3 struct {
	WireVersion               string                            `json:"wire_version"`
	Schedule                  PreparedTaskSchedule              `json:"schedule"`
	Input                     workflow.ResearchScheduledInputV3 `json:"input"`
	TargetAction              []byte                            `json:"target_action"`
	TargetActionDigest        string                            `json:"target_action_digest"`
	ActionAuthorizationDigest string                            `json:"action_authorization_digest"`
}

type researchTaskScheduleExpectedV3 struct {
	prepared PreparedResearchTaskScheduleV3
	base     taskScheduleExpected
}

// PrepareResearchTaskScheduleV3 always prepares the formal native V3 action.
// The result does not depend on a legacy runtime rollout selector.
func (s *Scheduler) PrepareResearchTaskScheduleV3(
	ctx context.Context,
	req TaskScheduleRequest,
) (PreparedResearchTaskScheduleV3, error) {
	if err := taskScheduleContextError(ctx, "prepare_research_v3", ""); err != nil {
		return PreparedResearchTaskScheduleV3{}, err
	}
	prepared, err := s.buildPreparedTaskSchedule(req)
	if err != nil {
		return PreparedResearchTaskScheduleV3{}, err
	}
	// Erase every rollout-selected action field and replace it with the stable
	// base envelope used only to reuse the proven schedule spec/memo verifier.
	prepared.FingerprintVersion = taskScheduleFingerprintVersionV2
	prepared.Action.Params = makePushParams(
		req.TenantID, req.UserID, prepared.TaskID, workflow.PushScope{}, req.NLDescription)
	prepared.Action.Params.TenantID = req.TenantID
	prepared.Action.Params.ExecutionMode = types.ExecutionModeDiscoverAtRun
	prepared.Action.Params.RuntimeVersion = workflow.ResearchRuntimeV3
	prepared.Action.Params.NLDesc = ""
	prepared.Action.Params.Scope = workflow.PushScope{}
	prepared.RequestDigest = ""

	if _, err := taskScheduleActionDataConverter(
		prepared, s.taskScheduleDecoder(prepared.ConverterID)); err != nil {
		return PreparedResearchTaskScheduleV3{}, newTaskScheduleError(
			TaskScheduleErrorInvalid, "prepare_research_v3", prepared.TaskID, err)
	}
	namespaceID, err := s.resolveTaskScheduleNamespaceID(ctx, prepared.TaskID)
	if err != nil {
		return PreparedResearchTaskScheduleV3{}, err
	}
	prepared.NamespaceID = namespaceID
	prepared.RequestDigest, err = digestPreparedTaskSchedule(prepared)
	if err != nil {
		return PreparedResearchTaskScheduleV3{}, newTaskScheduleError(
			TaskScheduleErrorInvalid, "prepare_research_v3", prepared.TaskID, err)
	}
	if err := validatePreparedTaskSchedule(prepared); err != nil {
		return PreparedResearchTaskScheduleV3{}, newTaskScheduleError(
			TaskScheduleErrorInvalid, "prepare_research_v3", prepared.TaskID, err)
	}
	token, authorizationDigest, err := newResearchV3ActionAuthorization()
	if err != nil {
		return PreparedResearchTaskScheduleV3{}, err
	}
	checkpoint := PreparedResearchTaskScheduleV3{
		WireVersion: preparedResearchTaskScheduleV3WireVersion,
		Schedule:    prepared,
		Input: workflow.ResearchScheduledInputV3{
			TenantID: req.TenantID, UserID: req.UserID, TaskID: prepared.TaskID,
			ActionAuthorizationToken: token,
		},
		ActionAuthorizationDigest: authorizationDigest,
	}
	return s.recoverPreparedResearchTaskScheduleV3(
		ctx, checkpoint, "prepare_research_v3", false)
}

// RecoverPreparedResearchTaskScheduleV3 validates and reconstructs a complete
// durable V3 checkpoint. It never mints a replacement authorization token.
func (s *Scheduler) RecoverPreparedResearchTaskScheduleV3(
	ctx context.Context,
	prepared PreparedResearchTaskScheduleV3,
) (PreparedResearchTaskScheduleV3, error) {
	return s.recoverPreparedResearchTaskScheduleV3(
		ctx, prepared, "recover_research_v3", true)
}

func (s *Scheduler) recoverPreparedResearchTaskScheduleV3(
	ctx context.Context,
	prepared PreparedResearchTaskScheduleV3,
	operation string,
	requireEvidence bool,
) (PreparedResearchTaskScheduleV3, error) {
	expected, err := s.buildResearchTaskScheduleExpectedV3(
		ctx, prepared, operation, true, requireEvidence)
	if err != nil {
		return PreparedResearchTaskScheduleV3{}, err
	}
	targetAction, wantTargetDigest, wantAuthorizationDigest, err := expected.actionEvidence()
	if err != nil {
		return PreparedResearchTaskScheduleV3{}, newTaskScheduleError(
			TaskScheduleErrorInvalid, operation, prepared.Schedule.TaskID, err)
	}
	if len(prepared.TargetAction) != 0 &&
		(!bytes.Equal(prepared.TargetAction, targetAction) ||
			prepared.TargetActionDigest != wantTargetDigest ||
			prepared.ActionAuthorizationDigest != wantAuthorizationDigest) {
		return PreparedResearchTaskScheduleV3{}, newTaskScheduleError(
			TaskScheduleErrorConflict, operation, prepared.Schedule.TaskID,
			errors.New("durable research V3 Action evidence differs"))
	}
	prepared.TargetAction = targetAction
	prepared.TargetActionDigest = wantTargetDigest
	prepared.ActionAuthorizationDigest = wantAuthorizationDigest
	return prepared, nil
}

func (s *Scheduler) buildResearchTaskScheduleExpectedV3(
	ctx context.Context,
	prepared PreparedResearchTaskScheduleV3,
	operation string,
	forMutation bool,
	requireEvidence bool,
) (researchTaskScheduleExpectedV3, error) {
	if prepared.WireVersion != preparedResearchTaskScheduleV3WireVersion {
		return researchTaskScheduleExpectedV3{}, newTaskScheduleError(
			TaskScheduleErrorInvalid, operation, prepared.Schedule.TaskID,
			fmt.Errorf("unsupported research V3 prepared wire %q", prepared.WireVersion))
	}
	if err := validatePreparedTaskSchedule(prepared.Schedule); err != nil {
		return researchTaskScheduleExpectedV3{}, newTaskScheduleError(
			TaskScheduleErrorInvalid, operation, prepared.Schedule.TaskID, err)
	}
	params := prepared.Schedule.Action.Params
	input := prepared.Input
	if prepared.Schedule.FingerprintVersion != taskScheduleFingerprintVersionV2 ||
		!workflow.IsResearchRuntimeV3(params.RuntimeVersion) ||
		params.ExecutionMode != types.ExecutionModeDiscoverAtRun ||
		params.TenantID != prepared.Schedule.TenantID || params.UserID != prepared.Schedule.UserID ||
		params.ScheduleID != prepared.Schedule.TaskID || params.NLDesc != "" ||
		params.Scope.TopN != 0 || len(params.Scope.SourceIDs) != 0 ||
		input.TenantID != prepared.Schedule.TenantID || input.UserID != prepared.Schedule.UserID ||
		input.TaskID != prepared.Schedule.TaskID ||
		len(input.ActionAuthorizationToken) != sha256.Size*2 ||
		input.ActionAuthorizationToken != bytesToLowerASCII(input.ActionAuthorizationToken) {
		return researchTaskScheduleExpectedV3{}, newTaskScheduleError(
			TaskScheduleErrorInvalid, operation, prepared.Schedule.TaskID,
			errors.New("research V3 prepared identity differs"))
	}
	if _, err := hex.DecodeString(input.ActionAuthorizationToken); err != nil {
		return researchTaskScheduleExpectedV3{}, newTaskScheduleError(
			TaskScheduleErrorInvalid, operation, prepared.Schedule.TaskID,
			errors.New("research V3 authorization token is invalid"))
	}
	if requireEvidence {
		if len(prepared.TargetAction) == 0 ||
			validateTaskScheduleDigest("target_action_digest", prepared.TargetActionDigest) != nil ||
			validateTaskScheduleDigest("action_authorization_digest", prepared.ActionAuthorizationDigest) != nil {
			return researchTaskScheduleExpectedV3{}, newTaskScheduleError(
				TaskScheduleErrorInvalid, operation, prepared.Schedule.TaskID,
				errors.New("research V3 Action evidence is incomplete"))
		}
	}
	base, err := s.buildTaskScheduleExpected(ctx, prepared.Schedule, operation, forMutation)
	if err != nil {
		return researchTaskScheduleExpectedV3{}, err
	}
	expected := researchTaskScheduleExpectedV3{prepared: prepared, base: base}
	if requireEvidence {
		targetAction, targetDigest, authorizationDigest, err := expected.actionEvidence()
		if err != nil {
			return researchTaskScheduleExpectedV3{}, newTaskScheduleError(
				TaskScheduleErrorInvalid, operation, prepared.Schedule.TaskID, err)
		}
		if !bytes.Equal(prepared.TargetAction, targetAction) ||
			prepared.TargetActionDigest != targetDigest ||
			prepared.ActionAuthorizationDigest != authorizationDigest {
			return researchTaskScheduleExpectedV3{}, newTaskScheduleError(
				TaskScheduleErrorConflict, operation, prepared.Schedule.TaskID,
				errors.New("durable research V3 Action evidence differs"))
		}
	}
	return expected, nil
}

func (expected researchTaskScheduleExpectedV3) actionEvidence() ([]byte, string, string, error) {
	activeFingerprint := expected.base.fingerprint
	activeFingerprint.LifecyclePhase = taskScheduleV1PhaseActive
	activeSchedule, err := expected.base.protoSchedule(
		activeFingerprint, false, expected.prepared.Schedule.Action.ActivationNote)
	if err != nil {
		return nil, "", "", err
	}
	if err := formalizeResearchScheduleV3(activeSchedule, expected); err != nil {
		return nil, "", "", err
	}
	targetAction, err := proto.MarshalOptions{Deterministic: true}.Marshal(activeSchedule.GetAction())
	if err != nil || len(targetAction) == 0 {
		return nil, "", "", fmt.Errorf("encode formal research V3 Action: %w", err)
	}
	targetSum := sha256.Sum256(targetAction)
	authorizationSum := sha256.Sum256([]byte(expected.prepared.Input.ActionAuthorizationToken))
	return targetAction, hex.EncodeToString(targetSum[:]), hex.EncodeToString(authorizationSum[:]), nil
}

func bytesToLowerASCII(value string) string {
	copy := []byte(value)
	for index, current := range copy {
		if current >= 'A' && current <= 'Z' {
			copy[index] = current + ('a' - 'A')
		}
	}
	return string(copy)
}

func formalizeResearchScheduleV3(
	schedule *schedulepb.Schedule,
	expected researchTaskScheduleExpectedV3,
) error {
	action := schedule.GetAction().GetStartWorkflow()
	if action == nil {
		return errors.New("base schedule action is unavailable")
	}
	dc, err := taskScheduleActionDataConverter(expected.prepared.Schedule, expected.base.dc)
	if err != nil {
		return err
	}
	payload, err := dc.ToPayload(expected.prepared.Input)
	if err != nil || payload == nil {
		return fmt.Errorf("encode research V3 schedule input: %w", err)
	}
	action.WorkflowType = &commonpb.WorkflowType{Name: workflow.ResearchScheduledWorkflowV3Name}
	action.Input = &commonpb.Payloads{Payloads: []*commonpb.Payload{payload}}
	return nil
}

func verifyResearchTaskScheduleDescriptionV3(
	expected researchTaskScheduleExpectedV3,
	desc *workflowservice.DescribeScheduleResponse,
	operation string,
) (TaskScheduleSnapshot, error) {
	cloned, ok := proto.Clone(desc).(*workflowservice.DescribeScheduleResponse)
	if !ok || cloned == nil || cloned.GetSchedule() == nil {
		return TaskScheduleSnapshot{}, newTaskScheduleError(
			TaskScheduleErrorConflict, operation, expected.base.taskID,
			errors.New("Describe returned no schedule"))
	}
	action := cloned.GetSchedule().GetAction().GetStartWorkflow()
	if action == nil || action.GetWorkflowType().GetName() != workflow.ResearchScheduledWorkflowV3Name {
		return TaskScheduleSnapshot{}, newTaskScheduleError(
			TaskScheduleErrorConflict, operation, expected.base.taskID,
			errors.New("formal research V3 workflow type differs"))
	}
	dc, err := taskScheduleActionDataConverter(expected.prepared.Schedule, expected.base.dc)
	if err != nil {
		return TaskScheduleSnapshot{}, err
	}
	wantInput, err := dc.ToPayload(expected.prepared.Input)
	if err != nil || wantInput == nil {
		return TaskScheduleSnapshot{}, newTaskScheduleError(
			TaskScheduleErrorBlocked, operation, expected.base.taskID, err)
	}
	inputs := action.GetInput().GetPayloads()
	if len(inputs) != 1 || !proto.Equal(inputs[0], wantInput) {
		return TaskScheduleSnapshot{}, newTaskScheduleError(
			TaskScheduleErrorConflict, operation, expected.base.taskID,
			errors.New("formal research V3 workflow input differs"))
	}
	baseInput, err := dc.ToPayload(expected.base.params)
	if err != nil || baseInput == nil {
		return TaskScheduleSnapshot{}, newTaskScheduleError(
			TaskScheduleErrorBlocked, operation, expected.base.taskID, err)
	}
	action.WorkflowType = &commonpb.WorkflowType{Name: taskScheduleV1WorkflowType}
	action.Input = &commonpb.Payloads{Payloads: []*commonpb.Payload{baseInput}}
	return verifyTaskScheduleDescription(expected.base, cloned, operation)
}

func (e researchTaskScheduleExpectedV3) createRequest() (*workflowservice.CreateScheduleRequest, error) {
	request, err := e.base.createRequest()
	if err != nil {
		return nil, err
	}
	if err := formalizeResearchScheduleV3(request.Schedule, e); err != nil {
		return nil, err
	}
	return request, nil
}

func (e researchTaskScheduleExpectedV3) activationRequest(
	conflictToken []byte,
) (*workflowservice.UpdateScheduleRequest, error) {
	request, err := e.base.activationRequest(conflictToken)
	if err != nil {
		return nil, err
	}
	if err := formalizeResearchScheduleV3(request.Schedule, e); err != nil {
		return nil, err
	}
	return request, nil
}

// EnsurePausedResearchTaskV3 creates or exactly adopts the formal V3 schedule.
func (s *Scheduler) EnsurePausedResearchTaskV3(
	ctx context.Context,
	prepared PreparedResearchTaskScheduleV3,
) (EnsurePausedTaskResult, error) {
	const operation = "ensure_paused_research_v3"
	if err := taskScheduleContextError(ctx, operation, ""); err != nil {
		return EnsurePausedTaskResult{}, err
	}
	expected, err := s.buildResearchTaskScheduleExpectedV3(ctx, prepared, operation, true, true)
	if err != nil {
		return EnsurePausedTaskResult{}, err
	}
	release, err := s.acquireTaskScheduleGate(ctx, operation, expected.base.taskID)
	if err != nil {
		return EnsurePausedTaskResult{}, err
	}
	defer release()

	existing, existingErr := s.describeTaskSchedule(ctx, expected.base)
	if existingErr == nil {
		snapshot, verifyErr := verifyResearchTaskScheduleDescriptionV3(expected, existing, operation)
		if verifyErr != nil {
			return EnsurePausedTaskResult{}, verifyErr
		}
		if snapshot.State != TaskSchedulePausedProvisioningExact {
			return EnsurePausedTaskResult{}, newTaskScheduleError(
				TaskScheduleErrorUnsafeState, operation, expected.base.taskID,
				fmt.Errorf("expected %s, got %s", TaskSchedulePausedProvisioningExact, snapshot.State))
		}
	} else if !isTaskScheduleNotFound(existingErr) {
		return EnsurePausedTaskResult{}, classifyTaskScheduleReadError(
			operation, expected.base.taskID, existingErr)
	}

	createRequest, err := expected.createRequest()
	if err != nil {
		return EnsurePausedTaskResult{}, newTaskScheduleError(
			TaskScheduleErrorBlocked, operation, expected.base.taskID, err)
	}
	createResponse, createErr := s.c.WorkflowService().CreateSchedule(ctx, createRequest)
	if createErr != nil && !taskScheduleMutationDefinitelyRejected(createErr) &&
		!isTaskScheduleAlreadyExistsError(createErr) {
		replayed, replayErr := s.createTaskScheduleForRecovery(ctx, createRequest)
		if replayErr == nil {
			createResponse, createErr = replayed, nil
		} else {
			createErr = errors.Join(createErr, replayErr)
		}
	}
	desc, describeErr := s.describeTaskScheduleForRecovery(ctx, expected.base)
	if describeErr != nil {
		if isTaskScheduleNotFound(describeErr) {
			if isTaskScheduleAlreadyExistsError(createErr) {
				return EnsurePausedTaskResult{}, newTaskScheduleError(
					TaskScheduleErrorTransient, operation, expected.base.taskID,
					errors.Join(createErr, describeErr))
			}
			cause := createErr
			if cause == nil {
				cause = describeErr
			}
			return EnsurePausedTaskResult{}, classifyTaskScheduleMutationError(
				operation, expected.base.taskID, cause)
		}
		if isTaskScheduleAlreadyExistsError(createErr) {
			return EnsurePausedTaskResult{}, newTaskScheduleError(
				TaskScheduleErrorOutcomeUnknown, operation, expected.base.taskID,
				errors.Join(createErr, describeErr))
		}
		if taskScheduleMutationDefinitelyRejected(createErr) {
			return EnsurePausedTaskResult{}, classifyTaskScheduleMutationError(
				operation, expected.base.taskID, createErr)
		}
		return EnsurePausedTaskResult{}, newTaskScheduleError(
			TaskScheduleErrorOutcomeUnknown, operation, expected.base.taskID,
			errors.Join(createErr, describeErr))
	}
	snapshot, err := verifyResearchTaskScheduleDescriptionV3(expected, desc, operation)
	if err != nil {
		return EnsurePausedTaskResult{}, err
	}
	if snapshot.State != TaskSchedulePausedProvisioningExact {
		return EnsurePausedTaskResult{}, newTaskScheduleError(
			TaskScheduleErrorUnsafeState, operation, expected.base.taskID,
			fmt.Errorf("expected %s, got %s", TaskSchedulePausedProvisioningExact, snapshot.State))
	}
	if createErr != nil {
		if !taskScheduleMutationDefinitelyRejected(createErr) &&
			!isTaskScheduleAlreadyExistsError(createErr) {
			return EnsurePausedTaskResult{}, newTaskScheduleError(
				TaskScheduleErrorOutcomeUnknown, operation, expected.base.taskID, createErr)
		}
		return EnsurePausedTaskResult{}, classifyTaskScheduleMutationError(
			operation, expected.base.taskID, createErr)
	}
	if createResponse == nil || len(createResponse.GetConflictToken()) == 0 {
		return EnsurePausedTaskResult{}, newTaskScheduleError(
			TaskScheduleErrorOutcomeUnknown, operation, expected.base.taskID,
			errors.New("CreateSchedule returned no creation conflict token"))
	}
	if snapshot.Revision != taskScheduleRevision(createResponse.GetConflictToken()) {
		return EnsurePausedTaskResult{}, newTaskScheduleError(
			TaskScheduleErrorUnsafeState, operation, expected.base.taskID,
			errors.New("schedule changed after its original CreateSchedule request"))
	}
	snapshot.State = TaskSchedulePausedVirginExact
	return EnsurePausedTaskResult{Disposition: TaskScheduleEnsured, Snapshot: snapshot}, nil
}

func (s *Scheduler) DescribeResearchTaskV3(
	ctx context.Context,
	prepared PreparedResearchTaskScheduleV3,
) (TaskScheduleSnapshot, error) {
	const operation = "describe_research_v3"
	expected, err := s.buildResearchTaskScheduleExpectedV3(ctx, prepared, operation, false, true)
	if err != nil {
		return TaskScheduleSnapshot{}, err
	}
	desc, err := s.describeTaskSchedule(ctx, expected.base)
	if err != nil {
		return TaskScheduleSnapshot{}, classifyTaskScheduleReadError(operation, expected.base.taskID, err)
	}
	return verifyResearchTaskScheduleDescriptionV3(expected, desc, operation)
}

func (s *Scheduler) ActivateResearchTaskV3(
	ctx context.Context,
	prepared PreparedResearchTaskScheduleV3,
	ensured TaskScheduleSnapshot,
) (TaskScheduleSnapshot, error) {
	const operation = "activate_research_v3"
	expected, err := s.buildResearchTaskScheduleExpectedV3(ctx, prepared, operation, true, true)
	if err != nil {
		return TaskScheduleSnapshot{}, err
	}
	if err := validateTaskScheduleActivationReceipt(expected.base, ensured); err != nil {
		return TaskScheduleSnapshot{}, newTaskScheduleError(
			TaskScheduleErrorInvalid, operation, expected.base.taskID, err)
	}
	release, err := s.acquireTaskScheduleGate(ctx, operation, expected.base.taskID)
	if err != nil {
		return TaskScheduleSnapshot{}, err
	}
	defer release()
	desc, err := s.describeTaskSchedule(ctx, expected.base)
	if err != nil {
		return TaskScheduleSnapshot{}, classifyTaskScheduleReadError(operation, expected.base.taskID, err)
	}
	snapshot, err := verifyResearchTaskScheduleDescriptionV3(expected, desc, operation)
	if err != nil {
		return TaskScheduleSnapshot{}, err
	}
	switch snapshot.State {
	case TaskScheduleActiveVirginExact, TaskScheduleActiveUsedExact:
		return snapshot, nil
	case TaskSchedulePausedUsedExact:
		return TaskScheduleSnapshot{}, newTaskScheduleError(
			TaskScheduleErrorUnsafeState, operation, expected.base.taskID,
			errors.New("used schedule cannot be activated from provisioning state"))
	case TaskSchedulePausedProvisioningExact:
		if snapshot.Revision != ensured.Revision {
			return TaskScheduleSnapshot{}, newTaskScheduleError(
				TaskScheduleErrorUnsafeState, operation, expected.base.taskID,
				errors.New("schedule changed after the paused definition was checkpointed"))
		}
	default:
		return TaskScheduleSnapshot{}, newTaskScheduleError(
			TaskScheduleErrorUnsafeState, operation, expected.base.taskID,
			fmt.Errorf("unexpected state %s", snapshot.State))
	}
	request, err := expected.activationRequest(desc.GetConflictToken())
	if err != nil {
		return TaskScheduleSnapshot{}, newTaskScheduleError(
			TaskScheduleErrorBlocked, operation, expected.base.taskID, err)
	}
	updateErr := s.compareAndSwapResearchV3CutoverSchedule(
		ctx, expected.base.taskID, request.Schedule,
		request.ConflictToken, request.RequestId)
	post, describeErr := s.describeTaskScheduleForRecovery(ctx, expected.base)
	if describeErr != nil {
		if isTaskScheduleNotFound(describeErr) {
			return TaskScheduleSnapshot{}, newTaskScheduleError(
				TaskScheduleErrorNotFound, operation, expected.base.taskID, describeErr)
		}
		if taskScheduleMutationDefinitelyRejected(updateErr) {
			return TaskScheduleSnapshot{}, classifyTaskScheduleMutationError(
				operation, expected.base.taskID, updateErr)
		}
		return TaskScheduleSnapshot{}, newTaskScheduleError(
			TaskScheduleErrorOutcomeUnknown, operation, expected.base.taskID,
			errors.Join(updateErr, describeErr))
	}
	postSnapshot, err := verifyResearchTaskScheduleDescriptionV3(expected, post, operation)
	if err != nil {
		return TaskScheduleSnapshot{}, err
	}
	if postSnapshot.State == TaskScheduleActiveVirginExact ||
		postSnapshot.State == TaskScheduleActiveUsedExact {
		return postSnapshot, nil
	}
	if updateErr == nil {
		updateErr = errors.New("activation returned success but schedule remained paused")
	}
	return TaskScheduleSnapshot{}, classifyTaskScheduleMutationError(
		operation, expected.base.taskID, updateErr)
}

func (s *Scheduler) DeleteResearchTaskV3(
	ctx context.Context,
	prepared PreparedResearchTaskScheduleV3,
) error {
	const operation = "delete_research_v3"
	expected, err := s.buildResearchTaskScheduleExpectedV3(ctx, prepared, operation, false, true)
	if err != nil {
		return err
	}
	release, err := s.acquireTaskScheduleGate(ctx, operation, expected.base.taskID)
	if err != nil {
		return err
	}
	defer release()
	desc, err := s.describeTaskSchedule(ctx, expected.base)
	if err != nil {
		if isTaskScheduleNotFound(err) {
			return nil
		}
		return classifyTaskScheduleReadError(operation, expected.base.taskID, err)
	}
	if _, err := verifyResearchTaskScheduleDescriptionV3(expected, desc, operation); err != nil {
		return err
	}
	deleteErr := s.applyScheduleCommandRemote(ctx, &types.ScheduleCommand{
		TaskID: expected.base.taskID, Kind: types.ScheduleCommandDelete,
	})
	if deleteErr == nil {
		return nil
	}
	post, describeErr := s.describeTaskScheduleForRecovery(ctx, expected.base)
	if isTaskScheduleNotFound(describeErr) {
		return nil
	}
	if describeErr != nil {
		if taskScheduleMutationDefinitelyRejected(deleteErr) {
			return classifyTaskScheduleMutationError(operation, expected.base.taskID, deleteErr)
		}
		return newTaskScheduleError(
			TaskScheduleErrorOutcomeUnknown, operation, expected.base.taskID,
			errors.Join(deleteErr, describeErr))
	}
	if _, err := verifyResearchTaskScheduleDescriptionV3(expected, post, operation); err != nil {
		return err
	}
	if deleteErr == nil {
		deleteErr = errors.New("Delete returned success but schedule still exists")
	}
	return classifyTaskScheduleMutationError(operation, expected.base.taskID, deleteErr)
}
