package scheduler

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	commonpb "go.temporal.io/api/common/v1"
	enums "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/YouToco/vane/types"
	"github.com/YouToco/vane/workflow"
)

const (
	taskDefinitionEditWireVersionV1 = "v1"
	taskDefinitionEditWireVersionV2 = "v2"
	// Literals by design: advancing either current task-schedule writer alias
	// must not reinterpret or strand a frozen definition-edit/v1 operation.
	taskDefinitionEditOwnershipIDSchemeVersion = "v1"
	// Literal by design: advancing the current task-schedule writer must not
	// reinterpret or strand an already frozen definition-edit/v1 checkpoint.
	taskDefinitionEditOwnershipFingerprintVersion = "v2"
	taskDefinitionEditNotePrefix                  = "vane/task-definition-edit/v1"
	taskDefinitionEditV1MinIntervalSeconds        = 3600
	taskDefinitionEditV1DefaultTimeZone           = "Asia/Shanghai"
	taskDefinitionEditV1MaxOperationIDBytes       = 512
)

// TaskDefinitionEditOriginalState is the database schedule status that the
// future durable coordinator has already checked before preparing an edit.
// Prepare also requires Temporal's paused flag to agree; it never guesses which
// system is authoritative when the two disagree.
type TaskDefinitionEditOriginalState string

const (
	TaskDefinitionEditOriginalStateUnknown TaskDefinitionEditOriginalState = ""
	TaskDefinitionEditOriginalStateActive  TaskDefinitionEditOriginalState = "active"
	TaskDefinitionEditOriginalStatePaused  TaskDefinitionEditOriginalState = "paused"
)

// TaskDefinitionEditPhase names one exact, operation-specific Temporal
// representation. The marker is part of the scheduled workflow Action memo,
// so a late restore cannot mistake a newer edit's pause for its own target.
type TaskDefinitionEditPhase string

const (
	TaskDefinitionEditPhaseUnknown      TaskDefinitionEditPhase = ""
	TaskDefinitionEditPhaseBaseOriginal TaskDefinitionEditPhase = "base_original"
	TaskDefinitionEditPhaseBasePaused   TaskDefinitionEditPhase = "base_paused"
	TaskDefinitionEditPhaseTargetPaused TaskDefinitionEditPhase = "target_paused"
	TaskDefinitionEditPhaseTargetFinal  TaskDefinitionEditPhase = "target_final"
)

// TaskDefinitionEditHead is an immutable Approved Definition identity.
type TaskDefinitionEditHead struct {
	Version int64  `json:"version"`
	Digest  string `json:"digest"`
}

// TaskDefinitionEditDefinition is the scheduler projection of one Approved
// Definition. Execution mode, runtime version, workflow identity, policies,
// and converter are deliberately preserved from the exact base schedule.
type TaskDefinitionEditDefinition struct {
	Spec          ScheduleSpec       `json:"spec"`
	Scope         workflow.PushScope `json:"scope"`
	NLDescription string             `json:"nl_description"`
}

// TaskDefinitionEditRequest is read-only input to PrepareTaskDefinitionEdit.
// Creation is used only as the immutable ownership generation; Base and Target
// are independent so the same task can be edited repeatedly after creation.
type TaskDefinitionEditRequest struct {
	OperationID   string                          `json:"operation_id"`
	Creation      PreparedTaskSchedule            `json:"creation"`
	BaseHead      TaskDefinitionEditHead          `json:"base_head"`
	TargetHead    TaskDefinitionEditHead          `json:"target_head"`
	OriginalState TaskDefinitionEditOriginalState `json:"original_state"`
	Base          TaskDefinitionEditDefinition    `json:"base"`
	Target        TaskDefinitionEditDefinition    `json:"target"`
}

// TaskDefinitionEditFingerprint extends the existing ownership fingerprint in
// place under the same Action memo key. Older readers retain ownership fields;
// edit-aware readers additionally require the Approved head, operation digest,
// and exact phase.
type TaskDefinitionEditFingerprint struct {
	IDSchemeVersion        string `json:"id_scheme_version"`
	FingerprintVersion     string `json:"fingerprint_version"`
	Namespace              string `json:"namespace"`
	NamespaceID            string `json:"namespace_id"`
	ConverterID            string `json:"converter_id"`
	TenantID               int64  `json:"tenant_id"`
	UserID                 int64  `json:"user_id"`
	TaskID                 string `json:"task_id"`
	CreationOperationID    string `json:"operation_id"`
	CreationPreparedDigest string `json:"prepared_digest"`
	CreationRequestDigest  string `json:"request_digest"`
	LifecyclePhase         string `json:"lifecycle_phase"`
	DefinitionVersion      int64  `json:"definition_version,omitempty"`
	DefinitionDigest       string `json:"definition_digest,omitempty"`
	EditOperationDigest    string `json:"edit_operation_digest,omitempty"`
	EditPhase              string `json:"edit_phase,omitempty"`
}

// TaskDefinitionEditScheduleState freezes every mutable ScheduleState field
// supported by this first edit wire.
type TaskDefinitionEditScheduleState struct {
	Paused           bool   `json:"paused"`
	Note             string `json:"note"`
	LimitedActions   bool   `json:"limited_actions"`
	RemainingActions int64  `json:"remaining_actions"`
}

// PreparedTaskDefinitionEditSchedule is a complete semantic representation of
// the supported Temporal Schedule. Fields that are not represented are
// required to be empty by the strict verifier before a full replacement write.
type PreparedTaskDefinitionEditSchedule struct {
	Digest                string                          `json:"digest"`
	Timing                PreparedTaskScheduleTiming      `json:"timing"`
	Action                PreparedTaskScheduleAction      `json:"action"`
	Policy                PreparedTaskSchedulePolicy      `json:"policy"`
	WorkflowIDReusePolicy int32                           `json:"workflow_id_reuse_policy"`
	State                 TaskDefinitionEditScheduleState `json:"state"`
	Fingerprint           TaskDefinitionEditFingerprint   `json:"fingerprint"`
}

// PreparedTaskDefinitionEdit is the immutable wire that the future durable
// coordinator must checkpoint before any Temporal mutation. BaseRevision is an
// opaque conflict token observed before execution; every phase receipt
// supplies the only revision accepted by the next phase.
type PreparedTaskDefinitionEdit struct {
	WireVersion            string                             `json:"wire_version"`
	OperationID            string                             `json:"operation_id"`
	OperationDigest        string                             `json:"operation_digest"`
	RequestDigest          string                             `json:"request_digest"`
	BaseProjectionDigest   string                             `json:"base_projection_digest"`
	TargetProjectionDigest string                             `json:"target_projection_digest"`
	Creation               PreparedTaskSchedule               `json:"creation"`
	BaseHead               TaskDefinitionEditHead             `json:"base_head"`
	TargetHead             TaskDefinitionEditHead             `json:"target_head"`
	OriginalState          TaskDefinitionEditOriginalState    `json:"original_state"`
	BaseRevision           string                             `json:"base_revision"`
	BaseOriginal           PreparedTaskDefinitionEditSchedule `json:"base_original"`
	BasePaused             PreparedTaskDefinitionEditSchedule `json:"base_paused"`
	TargetPaused           PreparedTaskDefinitionEditSchedule `json:"target_paused"`
	TargetFinal            PreparedTaskDefinitionEditSchedule `json:"target_final"`
}

// TaskDefinitionEditSnapshot is a durable proof of one exact remote phase.
type TaskDefinitionEditSnapshot struct {
	TaskID               string                  `json:"task_id"`
	RequestDigest        string                  `json:"request_digest"`
	Phase                TaskDefinitionEditPhase `json:"phase"`
	RepresentationDigest string                  `json:"representation_digest"`
	Revision             string                  `json:"revision"`
}

// PrepareTaskDefinitionEdit freezes the exact base and target Temporal
// representations without mutating Temporal. The authenticated owner command
// and PostgreSQL lease/fence are supplied by the coordinator.
func (s *Scheduler) PrepareTaskDefinitionEdit(
	ctx context.Context,
	req TaskDefinitionEditRequest,
) (PreparedTaskDefinitionEdit, TaskDefinitionEditSnapshot, error) {
	if err := taskScheduleContextError(ctx, "prepare_definition_edit", req.Creation.TaskID); err != nil {
		return PreparedTaskDefinitionEdit{}, TaskDefinitionEditSnapshot{}, err
	}
	req.Creation = clonePreparedTaskSchedule(req.Creation)
	baseTiming, targetTiming, err := validateTaskDefinitionEditRequest(req)
	if err != nil {
		return PreparedTaskDefinitionEdit{}, TaskDefinitionEditSnapshot{}, newTaskScheduleError(
			TaskScheduleErrorInvalid, "prepare_definition_edit", req.Creation.TaskID, err,
		)
	}
	wireVersion := taskDefinitionEditWireVersionV1
	if req.Creation.FingerprintVersion == taskScheduleFingerprintVersionV1 {
		// Wire v1 is retained byte-for-byte for v2 creation provenance. A
		// distinct wire version admits historical v1 creation bytes without
		// widening or reinterpreting the v1 recovery contract.
		wireVersion = taskDefinitionEditWireVersionV2
	}
	dc, err := s.taskDefinitionEditEnvironment(ctx, req.Creation, "prepare_definition_edit")
	if err != nil {
		return PreparedTaskDefinitionEdit{}, TaskDefinitionEditSnapshot{}, err
	}
	desc, err := s.describeTaskDefinitionEdit(ctx, req.Creation.Namespace, req.Creation.TaskID)
	if err != nil {
		return PreparedTaskDefinitionEdit{}, TaskDefinitionEditSnapshot{}, classifyTaskDefinitionEditReadError(
			"prepare_definition_edit", req.Creation.TaskID, err,
		)
	}
	baseOriginal, err := freezeTaskDefinitionEditBase(req, baseTiming, desc, dc)
	if err != nil {
		return PreparedTaskDefinitionEdit{}, TaskDefinitionEditSnapshot{}, newTaskScheduleError(
			TaskScheduleErrorUnsafeState, "prepare_definition_edit", req.Creation.TaskID, err,
		)
	}

	baseAction := cloneTaskDefinitionEditAction(baseOriginal.Action)
	targetAction := cloneTaskDefinitionEditAction(baseOriginal.Action)
	targetAction.Params.Scope = cloneTaskDefinitionEditScope(req.Target.Scope)
	targetAction.Params.NLDesc = req.Target.NLDescription
	baseProjectionDigest, err := digestTaskDefinitionEditProjectionV1(req.Base)
	if err != nil {
		return PreparedTaskDefinitionEdit{}, TaskDefinitionEditSnapshot{}, newTaskScheduleError(
			TaskScheduleErrorInvalid, "prepare_definition_edit", req.Creation.TaskID, err,
		)
	}
	targetProjectionDigest, err := digestTaskDefinitionEditProjectionV1(req.Target)
	if err != nil {
		return PreparedTaskDefinitionEdit{}, TaskDefinitionEditSnapshot{}, newTaskScheduleError(
			TaskScheduleErrorInvalid, "prepare_definition_edit", req.Creation.TaskID, err,
		)
	}
	operationDigest, err := digestTaskDefinitionEditOperationSeed(taskDefinitionEditOperationSeed{
		WireVersion:            wireVersion,
		OperationID:            req.OperationID,
		CreationRequestDigest:  req.Creation.RequestDigest,
		TenantID:               req.Creation.TenantID,
		UserID:                 req.Creation.UserID,
		TaskID:                 req.Creation.TaskID,
		BaseHead:               req.BaseHead,
		TargetHead:             req.TargetHead,
		OriginalState:          req.OriginalState,
		BaseProjectionDigest:   baseProjectionDigest,
		TargetProjectionDigest: targetProjectionDigest,
		BaseTiming:             baseTiming,
		BaseAction:             baseAction,
		BasePolicy:             baseOriginal.Policy,
		BaseReusePolicy:        baseOriginal.WorkflowIDReusePolicy,
		BaseState:              baseOriginal.State,
		TargetTiming:           targetTiming,
		TargetAction:           targetAction,
		TargetPolicy:           baseOriginal.Policy,
		TargetReusePolicy:      baseOriginal.WorkflowIDReusePolicy,
	})
	if err != nil {
		return PreparedTaskDefinitionEdit{}, TaskDefinitionEditSnapshot{}, newTaskScheduleError(
			TaskScheduleErrorInvalid, "prepare_definition_edit", req.Creation.TaskID, err,
		)
	}
	prepared := PreparedTaskDefinitionEdit{
		WireVersion:            wireVersion,
		OperationID:            req.OperationID,
		OperationDigest:        operationDigest,
		BaseProjectionDigest:   baseProjectionDigest,
		TargetProjectionDigest: targetProjectionDigest,
		Creation:               req.Creation,
		BaseHead:               req.BaseHead,
		TargetHead:             req.TargetHead,
		OriginalState:          req.OriginalState,
		BaseRevision:           taskScheduleRevision(desc.GetConflictToken()),
		BaseOriginal:           baseOriginal,
	}
	prepared.BasePaused = clonePreparedTaskDefinitionEditSchedule(baseOriginal)
	prepared.TargetPaused = clonePreparedTaskDefinitionEditSchedule(baseOriginal)
	prepared.TargetFinal = clonePreparedTaskDefinitionEditSchedule(baseOriginal)

	if req.OriginalState == TaskDefinitionEditOriginalStateActive {
		prepared.BasePaused.Fingerprint = taskDefinitionEditFingerprintFor(
			baseOriginal.Fingerprint, req.BaseHead, operationDigest, "base_paused",
		)
		prepared.BasePaused.State.Paused = true
		prepared.BasePaused.State.Note = taskDefinitionEditNote("base_paused", operationDigest)
	}

	for _, target := range []*PreparedTaskDefinitionEditSchedule{
		&prepared.TargetPaused,
		&prepared.TargetFinal,
	} {
		target.Timing = clonePreparedTaskScheduleTiming(targetTiming)
		target.Action.Params.Scope = cloneTaskDefinitionEditScope(req.Target.Scope)
		target.Action.Params.NLDesc = req.Target.NLDescription
	}
	if req.OriginalState == TaskDefinitionEditOriginalStateActive {
		prepared.TargetPaused.Fingerprint = taskDefinitionEditFingerprintFor(
			baseOriginal.Fingerprint, req.TargetHead, operationDigest, "target_paused",
		)
		prepared.TargetPaused.State.Paused = true
		prepared.TargetPaused.State.Note = taskDefinitionEditNote("target_paused", operationDigest)
		prepared.TargetFinal.Fingerprint = taskDefinitionEditFingerprintFor(
			baseOriginal.Fingerprint, req.TargetHead, operationDigest, "final_active",
		)
		prepared.TargetFinal.State.Paused = false
		prepared.TargetFinal.State.Note = taskDefinitionEditNote("final_active", operationDigest)
	} else {
		prepared.TargetPaused.Fingerprint = taskDefinitionEditFingerprintFor(
			baseOriginal.Fingerprint, req.TargetHead, operationDigest, "final_paused",
		)
		prepared.TargetPaused.State = baseOriginal.State
		prepared.TargetFinal = clonePreparedTaskDefinitionEditSchedule(prepared.TargetPaused)
	}

	for _, representation := range []*PreparedTaskDefinitionEditSchedule{
		&prepared.BaseOriginal,
		&prepared.BasePaused,
		&prepared.TargetPaused,
		&prepared.TargetFinal,
	} {
		representation.Digest, err = digestPreparedTaskDefinitionEditSchedule(*representation)
		if err != nil {
			return PreparedTaskDefinitionEdit{}, TaskDefinitionEditSnapshot{}, newTaskScheduleError(
				TaskScheduleErrorInvalid, "prepare_definition_edit", req.Creation.TaskID, err,
			)
		}
	}
	prepared.RequestDigest, err = digestPreparedTaskDefinitionEdit(prepared)
	if err != nil {
		return PreparedTaskDefinitionEdit{}, TaskDefinitionEditSnapshot{}, newTaskScheduleError(
			TaskScheduleErrorInvalid, "prepare_definition_edit", req.Creation.TaskID, err,
		)
	}
	if err := validatePreparedTaskDefinitionEdit(prepared); err != nil {
		return PreparedTaskDefinitionEdit{}, TaskDefinitionEditSnapshot{}, newTaskScheduleError(
			TaskScheduleErrorInvalid, "prepare_definition_edit", req.Creation.TaskID, err,
		)
	}
	snapshot := taskDefinitionEditSnapshot(
		prepared, TaskDefinitionEditPhaseBaseOriginal, prepared.BaseOriginal, desc.GetConflictToken(),
	)
	return clonePreparedTaskDefinitionEdit(prepared), snapshot, nil
}

// DescribeTaskDefinitionEdit strictly classifies the current remote
// representation. Any foreign phase, hidden field, or unknown protobuf field
// fails closed instead of being adopted.
func (s *Scheduler) DescribeTaskDefinitionEdit(
	ctx context.Context,
	prepared PreparedTaskDefinitionEdit,
) (TaskDefinitionEditSnapshot, error) {
	if err := taskScheduleContextError(ctx, "describe_definition_edit", prepared.Creation.TaskID); err != nil {
		return TaskDefinitionEditSnapshot{}, err
	}
	prepared, dc, err := s.buildTaskDefinitionEditRuntime(ctx, prepared, "describe_definition_edit")
	if err != nil {
		return TaskDefinitionEditSnapshot{}, err
	}
	desc, err := s.describeTaskDefinitionEdit(ctx, prepared.Creation.Namespace, prepared.Creation.TaskID)
	if err != nil {
		return TaskDefinitionEditSnapshot{}, classifyTaskDefinitionEditReadError(
			"describe_definition_edit", prepared.Creation.TaskID, err,
		)
	}
	return classifyTaskDefinitionEditDescription(prepared, desc, dc, "describe_definition_edit")
}

// PauseTaskDefinitionEdit moves an originally active exact base schedule to an
// operation-specific paused base using raw conflict-token CAS. An originally
// paused task is observed only and its note is preserved byte-for-byte.
func (s *Scheduler) PauseTaskDefinitionEdit(
	ctx context.Context,
	prepared PreparedTaskDefinitionEdit,
) (TaskDefinitionEditSnapshot, error) {
	prepared, dc, err := s.buildTaskDefinitionEditRuntime(ctx, prepared, "pause_definition_edit")
	if err != nil {
		return TaskDefinitionEditSnapshot{}, err
	}
	base := taskDefinitionEditSnapshotFromRevision(
		prepared, TaskDefinitionEditPhaseBaseOriginal, prepared.BaseOriginal, prepared.BaseRevision,
	)
	if prepared.OriginalState == TaskDefinitionEditOriginalStatePaused {
		return s.observeTaskDefinitionEditSource(ctx, prepared, dc, base, "pause_definition_edit")
	}
	return s.transitionTaskDefinitionEdit(
		ctx, prepared, dc, base,
		TaskDefinitionEditPhaseBaseOriginal, prepared.BaseOriginal,
		TaskDefinitionEditPhaseBasePaused, prepared.BasePaused,
		"pause_definition_edit", "base_paused",
	)
}

// ApplyTaskDefinitionEdit replaces the exact paused base with the frozen
// target. The future coordinator must separately prove that PostgreSQL's
// current Approved head still equals TargetHead immediately before calling.
func (s *Scheduler) ApplyTaskDefinitionEdit(
	ctx context.Context,
	prepared PreparedTaskDefinitionEdit,
	source TaskDefinitionEditSnapshot,
) (TaskDefinitionEditSnapshot, error) {
	prepared, dc, err := s.buildTaskDefinitionEditRuntime(ctx, prepared, "apply_definition_edit")
	if err != nil {
		return TaskDefinitionEditSnapshot{}, err
	}
	sourcePhase := TaskDefinitionEditPhaseBasePaused
	sourceRepresentation := prepared.BasePaused
	destinationPhase := TaskDefinitionEditPhaseTargetPaused
	if prepared.OriginalState == TaskDefinitionEditOriginalStatePaused {
		sourcePhase = TaskDefinitionEditPhaseBaseOriginal
		sourceRepresentation = prepared.BaseOriginal
		destinationPhase = TaskDefinitionEditPhaseTargetFinal
	}
	return s.transitionTaskDefinitionEdit(
		ctx, prepared, dc, source,
		sourcePhase, sourceRepresentation,
		destinationPhase, prepared.TargetPaused,
		"apply_definition_edit", "target_applied",
	)
}

// RestoreTaskDefinitionEdit restores only a task that was originally active.
// Originally paused tasks remain paused and are merely re-observed.
func (s *Scheduler) RestoreTaskDefinitionEdit(
	ctx context.Context,
	prepared PreparedTaskDefinitionEdit,
	source TaskDefinitionEditSnapshot,
) (TaskDefinitionEditSnapshot, error) {
	prepared, dc, err := s.buildTaskDefinitionEditRuntime(ctx, prepared, "restore_definition_edit")
	if err != nil {
		return TaskDefinitionEditSnapshot{}, err
	}
	if prepared.OriginalState == TaskDefinitionEditOriginalStatePaused {
		if err := validateTaskDefinitionEditSnapshot(
			prepared, source, TaskDefinitionEditPhaseTargetFinal, prepared.TargetFinal,
		); err != nil {
			return TaskDefinitionEditSnapshot{}, newTaskScheduleError(
				TaskScheduleErrorInvalid, "restore_definition_edit", prepared.Creation.TaskID, err,
			)
		}
		return s.observeTaskDefinitionEditSource(ctx, prepared, dc, source, "restore_definition_edit")
	}
	return s.transitionTaskDefinitionEdit(
		ctx, prepared, dc, source,
		TaskDefinitionEditPhaseTargetPaused, prepared.TargetPaused,
		TaskDefinitionEditPhaseTargetFinal, prepared.TargetFinal,
		"restore_definition_edit", "target_restored",
	)
}

func (s *Scheduler) transitionTaskDefinitionEdit(
	ctx context.Context,
	prepared PreparedTaskDefinitionEdit,
	dc converter.DataConverter,
	source TaskDefinitionEditSnapshot,
	sourcePhase TaskDefinitionEditPhase,
	sourceRepresentation PreparedTaskDefinitionEditSchedule,
	destinationPhase TaskDefinitionEditPhase,
	destinationRepresentation PreparedTaskDefinitionEditSchedule,
	operation string,
	requestPhase string,
) (TaskDefinitionEditSnapshot, error) {
	if err := validateTaskDefinitionEditSnapshot(prepared, source, sourcePhase, sourceRepresentation); err != nil {
		return TaskDefinitionEditSnapshot{}, newTaskScheduleError(
			TaskScheduleErrorInvalid, operation, prepared.Creation.TaskID, err,
		)
	}
	release, err := s.acquireTaskScheduleGate(ctx, operation, prepared.Creation.TaskID)
	if err != nil {
		return TaskDefinitionEditSnapshot{}, err
	}
	defer release()
	if err := taskScheduleContextError(ctx, operation, prepared.Creation.TaskID); err != nil {
		return TaskDefinitionEditSnapshot{}, err
	}

	desc, err := s.describeTaskDefinitionEdit(ctx, prepared.Creation.Namespace, prepared.Creation.TaskID)
	if err != nil {
		return TaskDefinitionEditSnapshot{}, classifyTaskDefinitionEditReadError(operation, prepared.Creation.TaskID, err)
	}
	current, err := classifyTaskDefinitionEditDescription(prepared, desc, dc, operation)
	if err != nil {
		return TaskDefinitionEditSnapshot{}, err
	}
	if current.Phase == destinationPhase &&
		current.RepresentationDigest == destinationRepresentation.Digest {
		return current, nil
	}
	if current.Phase != sourcePhase || current.RepresentationDigest != sourceRepresentation.Digest {
		return TaskDefinitionEditSnapshot{}, newTaskScheduleError(
			TaskScheduleErrorUnsafeState, operation, prepared.Creation.TaskID,
			fmt.Errorf("remote phase %q is not the authorized source %q", current.Phase, sourcePhase),
		)
	}
	if current.Revision != source.Revision {
		return TaskDefinitionEditSnapshot{}, newTaskScheduleError(
			TaskScheduleErrorUnsafeState, operation, prepared.Creation.TaskID,
			errors.New("schedule revision changed after the source phase was checkpointed"),
		)
	}

	request, err := buildTaskDefinitionEditUpdateRequest(
		prepared, destinationRepresentation, desc.GetConflictToken(), requestPhase, dc,
	)
	if err != nil {
		return TaskDefinitionEditSnapshot{}, newTaskScheduleError(
			TaskScheduleErrorBlocked, operation, prepared.Creation.TaskID, err,
		)
	}
	_, updateErr := s.c.WorkflowService().UpdateSchedule(ctx, request)
	post, describeErr := s.describeTaskDefinitionEditForRecovery(
		ctx, prepared.Creation.Namespace, prepared.Creation.TaskID,
	)
	if describeErr != nil {
		if isTaskDefinitionEditScheduleNotFound(describeErr) {
			return TaskDefinitionEditSnapshot{}, newTaskScheduleError(
				TaskScheduleErrorNotFound, operation, prepared.Creation.TaskID, describeErr,
			)
		}
		if isTaskDefinitionEditScheduleNotFound(updateErr) {
			return TaskDefinitionEditSnapshot{}, newTaskScheduleError(
				TaskScheduleErrorNotFound, operation, prepared.Creation.TaskID, updateErr,
			)
		}
		if taskScheduleMutationDefinitelyRejected(updateErr) {
			return TaskDefinitionEditSnapshot{}, classifyTaskScheduleMutationError(
				operation, prepared.Creation.TaskID, updateErr,
			)
		}
		return TaskDefinitionEditSnapshot{}, newTaskScheduleError(
			TaskScheduleErrorOutcomeUnknown, operation, prepared.Creation.TaskID,
			errors.Join(updateErr, describeErr),
		)
	}
	postSnapshot, err := classifyTaskDefinitionEditDescription(prepared, post, dc, operation)
	if err != nil {
		return TaskDefinitionEditSnapshot{}, err
	}
	if postSnapshot.Phase == destinationPhase &&
		postSnapshot.RepresentationDigest == destinationRepresentation.Digest {
		return postSnapshot, nil
	}
	if postSnapshot.Phase == sourcePhase &&
		postSnapshot.RepresentationDigest == sourceRepresentation.Digest {
		if postSnapshot.Revision != source.Revision {
			return TaskDefinitionEditSnapshot{}, newTaskScheduleError(
				TaskScheduleErrorUnsafeState, operation, prepared.Creation.TaskID,
				errors.New("source representation was replayed with a different revision"),
			)
		}
		if taskScheduleMutationDefinitelyRejected(updateErr) {
			return TaskDefinitionEditSnapshot{}, classifyTaskScheduleMutationError(
				operation, prepared.Creation.TaskID, updateErr,
			)
		}
		cause := updateErr
		if cause == nil {
			cause = errors.New("update returned success without changing the schedule")
		}
		return TaskDefinitionEditSnapshot{}, newTaskScheduleError(
			TaskScheduleErrorOutcomeUnknown, operation, prepared.Creation.TaskID, cause,
		)
	}
	return TaskDefinitionEditSnapshot{}, newTaskScheduleError(
		TaskScheduleErrorUnsafeState, operation, prepared.Creation.TaskID,
		fmt.Errorf("post-update phase %q is neither source nor destination", postSnapshot.Phase),
	)
}

func (s *Scheduler) observeTaskDefinitionEditSource(
	ctx context.Context,
	prepared PreparedTaskDefinitionEdit,
	dc converter.DataConverter,
	expected TaskDefinitionEditSnapshot,
	operation string,
) (TaskDefinitionEditSnapshot, error) {
	if err := validateTaskDefinitionEditSnapshot(
		prepared, expected, expected.Phase, taskDefinitionEditRepresentation(prepared, expected.Phase),
	); err != nil {
		return TaskDefinitionEditSnapshot{}, newTaskScheduleError(
			TaskScheduleErrorInvalid, operation, prepared.Creation.TaskID, err,
		)
	}
	desc, err := s.describeTaskDefinitionEdit(ctx, prepared.Creation.Namespace, prepared.Creation.TaskID)
	if err != nil {
		return TaskDefinitionEditSnapshot{}, classifyTaskDefinitionEditReadError(operation, prepared.Creation.TaskID, err)
	}
	current, err := classifyTaskDefinitionEditDescription(prepared, desc, dc, operation)
	if err != nil {
		return TaskDefinitionEditSnapshot{}, err
	}
	if current.Phase != expected.Phase ||
		current.RepresentationDigest != expected.RepresentationDigest ||
		current.Revision != expected.Revision {
		return TaskDefinitionEditSnapshot{}, newTaskScheduleError(
			TaskScheduleErrorUnsafeState, operation, prepared.Creation.TaskID,
			errors.New("observed schedule is not the checkpointed exact phase and revision"),
		)
	}
	return current, nil
}

func (s *Scheduler) buildTaskDefinitionEditRuntime(
	ctx context.Context,
	prepared PreparedTaskDefinitionEdit,
	operation string,
) (PreparedTaskDefinitionEdit, converter.DataConverter, error) {
	if err := taskScheduleContextError(ctx, operation, prepared.Creation.TaskID); err != nil {
		return PreparedTaskDefinitionEdit{}, nil, err
	}
	prepared = clonePreparedTaskDefinitionEdit(prepared)
	if err := validatePreparedTaskDefinitionEdit(prepared); err != nil {
		return PreparedTaskDefinitionEdit{}, nil, newTaskScheduleError(
			TaskScheduleErrorInvalid, operation, prepared.Creation.TaskID, err,
		)
	}
	dc, err := s.taskDefinitionEditEnvironment(ctx, prepared.Creation, operation)
	if err != nil {
		return PreparedTaskDefinitionEdit{}, nil, err
	}
	return prepared, dc, nil
}

// ValidateTaskDefinitionEditEnvironment is the read-only startup preflight for
// a durable nonterminal operation. It reuses the exact retained decoder and
// live namespace identity checks used before every remote phase, but performs
// no Schedule Describe or mutation.
func (s *Scheduler) ValidateTaskDefinitionEditEnvironment(
	ctx context.Context,
	prepared PreparedTaskDefinitionEdit,
) error {
	_, _, err := s.buildTaskDefinitionEditRuntime(
		ctx, prepared, "validate_definition_edit_environment",
	)
	return err
}

func (s *Scheduler) taskDefinitionEditEnvironment(
	ctx context.Context,
	creation PreparedTaskSchedule,
	operation string,
) (converter.DataConverter, error) {
	if s == nil || s.c == nil {
		return nil, newTaskScheduleError(
			TaskScheduleErrorInvalid, operation, creation.TaskID, errors.New("temporal client is required"),
		)
	}
	namespace, _, _ := s.taskScheduleEnvironment()
	if namespace != creation.Namespace {
		return nil, newTaskScheduleError(
			TaskScheduleErrorConflict, operation, creation.TaskID,
			fmt.Errorf("prepared namespace %q does not match current namespace %q", creation.Namespace, namespace),
		)
	}
	dc := s.taskScheduleDecoder(creation.ConverterID)
	if dc == nil {
		return nil, newTaskScheduleError(
			TaskScheduleErrorBlocked, operation, creation.TaskID,
			fmt.Errorf("prepared converter %q is unavailable", creation.ConverterID),
		)
	}
	if _, requestAware := dc.(taskScheduleRequestContextAwareConverter); requestAware {
		return nil, newTaskScheduleError(
			TaskScheduleErrorBlocked, operation, creation.TaskID,
			errors.New("prepared converter is request-context-aware and cannot be recovered durably"),
		)
	}
	namespaceID, err := s.resolveTaskScheduleNamespaceID(ctx, creation.TaskID)
	if err != nil {
		return nil, err
	}
	if namespaceID != creation.NamespaceID {
		return nil, newTaskScheduleError(
			TaskScheduleErrorConflict, operation, creation.TaskID,
			fmt.Errorf("prepared namespace id %q does not match current namespace id %q", creation.NamespaceID, namespaceID),
		)
	}
	return dc, nil
}

func validateTaskDefinitionEditRequest(
	req TaskDefinitionEditRequest,
) (PreparedTaskScheduleTiming, PreparedTaskScheduleTiming, error) {
	if err := validateTaskDefinitionEditRequestIdentityV1(req); err != nil {
		return PreparedTaskScheduleTiming{}, PreparedTaskScheduleTiming{}, err
	}
	_, baseTiming, err := buildTaskDefinitionEditScheduleSpecV1(req.Base.Spec)
	if err != nil {
		return PreparedTaskScheduleTiming{}, PreparedTaskScheduleTiming{}, fmt.Errorf("base schedule spec: %w", err)
	}
	// Only the target is a new write. The base may have been valid under an
	// older retained policy and must remain editable. Durable recovery never
	// reaches either compiler; it trusts the exact sealed heads/checkpoints.
	_, targetTiming, err := buildTaskScheduleSpec(req.Target.Spec)
	if err != nil {
		return PreparedTaskScheduleTiming{}, PreparedTaskScheduleTiming{}, fmt.Errorf("target schedule current writer: %w", err)
	}
	return baseTiming, targetTiming, nil
}

// validateTaskDefinitionEditRequestIdentityV1 validates only the frozen
// ownership, heads, and non-compiled projections. Recovery deliberately does
// not re-run spec compilation: proposal sealing already bound exact Approved
// head bytes to exact prepared representations, and compiler policy evolves.
func validateTaskDefinitionEditRequestIdentityV1(req TaskDefinitionEditRequest) error {
	if err := validatePreparedTaskSchedule(req.Creation); err != nil {
		return fmt.Errorf("validate creation ownership: %w", err)
	}
	if req.Creation.IDSchemeVersion != taskDefinitionEditOwnershipIDSchemeVersion {
		return errors.New("definition edit requires the retained v1 task ID scheme")
	}
	if req.Creation.FingerprintVersion != taskScheduleFingerprintVersionV1 &&
		req.Creation.FingerprintVersion != taskDefinitionEditOwnershipFingerprintVersion {
		return errors.New("definition edit requires a retained v1 or v2 task ownership fingerprint")
	}
	if err := validateTaskScheduleString("operation_id", req.OperationID, true); err != nil {
		return err
	}
	if len(req.OperationID) > taskDefinitionEditV1MaxOperationIDBytes {
		return errors.New("operation_id is too long")
	}
	if err := validateTaskDefinitionEditHead("base_head", req.BaseHead); err != nil {
		return err
	}
	if err := validateTaskDefinitionEditHead("target_head", req.TargetHead); err != nil {
		return err
	}
	if req.BaseHead.Version == int64(^uint64(0)>>1) || req.TargetHead.Version != req.BaseHead.Version+1 {
		return errors.New("target head version must immediately follow base head version")
	}
	if req.OriginalState != TaskDefinitionEditOriginalStateActive &&
		req.OriginalState != TaskDefinitionEditOriginalStatePaused {
		return errors.New("original state must be active or paused")
	}
	if err := validateTaskDefinitionEditDefinition("base", req.Base); err != nil {
		return err
	}
	if err := validateTaskDefinitionEditDefinition("target", req.Target); err != nil {
		return err
	}
	return nil
}

// validateTaskDefinitionEditScheduleSpecV1 freezes the timing policy that was
// valid when definition-edit/v1 bytes were introduced. It is used only while
// preparing/sealing a new edit against an old base; durable Decode never
// re-runs this compiler.
func validateTaskDefinitionEditScheduleSpecV1(spec ScheduleSpec) error {
	if spec.EverySeconds < 0 {
		return errors.New("every_seconds must not be negative")
	}
	hasCron := strings.TrimSpace(spec.Cron) != ""
	hasEvery := spec.EverySeconds > 0
	if hasCron == hasEvery {
		return errors.New("definition edit v1 spec must contain exactly one of cron or every_seconds")
	}
	if hasEvery {
		if spec.EverySeconds < taskDefinitionEditV1MinIntervalSeconds {
			return fmt.Errorf(
				"every_seconds %d is below the definition edit v1 floor %d",
				spec.EverySeconds, taskDefinitionEditV1MinIntervalSeconds,
			)
		}
		_, err := parseTaskDefinitionEditAnchorV1(spec.AnchorAt)
		return err
	}
	if strings.TrimSpace(spec.AnchorAt) != "" {
		return errors.New("definition edit v1 anchor_at is only valid with every_seconds")
	}
	fields := strings.Fields(spec.Cron)
	if len(fields) != 5 {
		return errors.New("definition edit v1 cron must contain five fields")
	}
	minute := fields[0]
	if strings.ContainsAny(minute, "*/,-") {
		return errors.New("definition edit v1 cron minute field exceeds the hourly floor")
	}
	value, err := strconv.Atoi(minute)
	if err != nil || value < 0 || value > 59 {
		return errors.New("definition edit v1 cron minute field must be an integer from 0 through 59")
	}
	return nil
}

func parseTaskDefinitionEditAnchorV1(raw string) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, nil
	}
	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("definition edit v1 anchor_at must be RFC3339: %w", err)
	}
	return parsed, nil
}

func validateTaskDefinitionEditDefinition(name string, definition TaskDefinitionEditDefinition) error {
	if err := validateTaskScheduleString(name+".nl_description", definition.NLDescription, true); err != nil {
		return err
	}
	if definition.Scope.TopN < 0 {
		return fmt.Errorf("%s.scope.top_n must not be negative", name)
	}
	seen := make(map[int64]struct{}, len(definition.Scope.SourceIDs))
	for _, sourceID := range definition.Scope.SourceIDs {
		if sourceID <= 0 {
			return fmt.Errorf("%s.scope source ids must be positive", name)
		}
		if _, exists := seen[sourceID]; exists {
			return fmt.Errorf("%s.scope contains duplicate source id %d", name, sourceID)
		}
		seen[sourceID] = struct{}{}
	}
	return nil
}

func validateTaskDefinitionEditHead(name string, head TaskDefinitionEditHead) error {
	if head.Version <= 0 {
		return fmt.Errorf("%s version must be positive", name)
	}
	if err := validateTaskScheduleDigest(name+".digest", head.Digest); err != nil {
		return err
	}
	return nil
}

func freezeTaskDefinitionEditBase(
	req TaskDefinitionEditRequest,
	timing PreparedTaskScheduleTiming,
	desc *workflowservice.DescribeScheduleResponse,
	dc converter.DataConverter,
) (PreparedTaskDefinitionEditSchedule, error) {
	if desc == nil || len(desc.GetConflictToken()) == 0 || desc.GetSchedule() == nil {
		return PreparedTaskDefinitionEditSchedule{}, errors.New("describe returned an incomplete schedule")
	}
	if err := rejectTaskDefinitionEditUnknownFields(desc.GetSchedule().ProtoReflect(), "schedule"); err != nil {
		return PreparedTaskDefinitionEditSchedule{}, err
	}
	if err := rejectTaskDefinitionEditUnknownFields(desc.GetMemo().ProtoReflect(), "memo"); err != nil {
		return PreparedTaskDefinitionEditSchedule{}, err
	}
	if err := rejectTaskDefinitionEditUnknownFields(desc.GetSearchAttributes().ProtoReflect(), "search_attributes"); err != nil {
		return PreparedTaskDefinitionEditSchedule{}, err
	}
	if len(desc.GetMemo().GetFields()) != 0 || len(desc.GetSearchAttributes().GetIndexedFields()) != 0 {
		return PreparedTaskDefinitionEditSchedule{}, errors.New("top-level schedule metadata is unsupported")
	}
	schedule := desc.GetSchedule()
	if !taskScheduleProtoSpecMatches(schedule.GetSpec(), timing) {
		return PreparedTaskDefinitionEditSchedule{}, errors.New("temporal spec does not match the approved base")
	}
	if err := validateTaskDefinitionEditPolicies(schedule.GetPolicies(), req.Creation.Policy); err != nil {
		return PreparedTaskDefinitionEditSchedule{}, err
	}
	state := schedule.GetState()
	if state == nil || state.GetLimitedActions() || state.GetRemainingActions() != 0 {
		return PreparedTaskDefinitionEditSchedule{}, errors.New("schedule action limits are unsupported")
	}
	if state.GetPaused() != (req.OriginalState == TaskDefinitionEditOriginalStatePaused) {
		return PreparedTaskDefinitionEditSchedule{}, errors.New("postgresql status and temporal paused state disagree")
	}
	if !utf8.ValidString(state.GetNotes()) {
		return PreparedTaskDefinitionEditSchedule{}, errors.New("schedule note is not valid utf-8")
	}

	fingerprint, params, reusePolicy, err := decodeTaskDefinitionEditAction(schedule.GetAction(), req.Creation, dc)
	if err != nil {
		return PreparedTaskDefinitionEditSchedule{}, err
	}
	if params.TenantID != req.Creation.TenantID || params.UserID != req.Creation.UserID ||
		params.RunKind != workflow.PushRunKindScheduled || params.ScheduleID != req.Creation.TaskID ||
		params.ExecutionMode != types.ExecutionModeCompiled || params.Snapshot != nil ||
		params.NLDesc != req.Base.NLDescription || params.Scope.TopN != req.Base.Scope.TopN ||
		!slices.Equal(params.Scope.SourceIDs, req.Base.Scope.SourceIDs) {
		return PreparedTaskDefinitionEditSchedule{}, errors.New("temporal action does not match the approved base")
	}
	if params.RuntimeVersion != "" && !workflow.IsCompiledRuntimeV1(params.RuntimeVersion) {
		return PreparedTaskDefinitionEditSchedule{}, errors.New("temporal action runtime version is unsupported")
	}
	if err := validateTaskDefinitionEditBaseFingerprint(fingerprint, req.BaseHead, state.GetPaused(), state.GetNotes()); err != nil {
		return PreparedTaskDefinitionEditSchedule{}, err
	}
	info := desc.GetInfo()
	// Temporal exposes no replacement validity signal yet; fail closed while
	// the deprecated field remains part of the server wire.
	//nolint:staticcheck
	if info == nil || info.GetInvalidScheduleError() != "" {
		return PreparedTaskDefinitionEditSchedule{}, errors.New("schedule info is missing or invalid")
	}
	action := req.Creation.Action
	action.Params = params
	action.Params.Scope.SourceIDs = slices.Clone(params.Scope.SourceIDs)
	return PreparedTaskDefinitionEditSchedule{
		Timing:                clonePreparedTaskScheduleTiming(timing),
		Action:                action,
		Policy:                req.Creation.Policy,
		WorkflowIDReusePolicy: int32(reusePolicy),
		State: TaskDefinitionEditScheduleState{
			Paused: state.GetPaused(), Note: state.GetNotes(),
			LimitedActions: state.GetLimitedActions(), RemainingActions: state.GetRemainingActions(),
		},
		Fingerprint: fingerprint,
	}, nil
}

func decodeTaskDefinitionEditAction(
	action *schedulepb.ScheduleAction,
	creation PreparedTaskSchedule,
	dc converter.DataConverter,
) (TaskDefinitionEditFingerprint, workflow.PushParams, enums.WorkflowIdReusePolicy, error) {
	workflowAction := action.GetStartWorkflow()
	if workflowAction == nil {
		return TaskDefinitionEditFingerprint{}, workflow.PushParams{}, 0, errors.New("schedule action is not a workflow action")
	}
	actionDC, err := taskScheduleActionDataConverter(creation, dc)
	if err != nil {
		return TaskDefinitionEditFingerprint{}, workflow.PushParams{}, 0, err
	}
	inputs := workflowAction.GetInput().GetPayloads()
	if len(inputs) != 1 || inputs[0] == nil {
		return TaskDefinitionEditFingerprint{}, workflow.PushParams{}, 0, errors.New("workflow action must contain one non-nil input")
	}
	var params workflow.PushParams
	if err := actionDC.FromPayload(inputs[0], &params); err != nil {
		return TaskDefinitionEditFingerprint{}, workflow.PushParams{}, 0, fmt.Errorf("decode workflow params: %w", err)
	}
	fingerprintPayload, ok := workflowAction.GetMemo().GetFields()[taskScheduleMemoKey]
	if !ok || fingerprintPayload == nil || len(workflowAction.GetMemo().GetFields()) != 1 {
		return TaskDefinitionEditFingerprint{}, workflow.PushParams{}, 0, errors.New("workflow action memo is not the single vane fingerprint")
	}
	var fingerprint TaskDefinitionEditFingerprint
	if err := actionDC.FromPayload(fingerprintPayload, &fingerprint); err != nil {
		return TaskDefinitionEditFingerprint{}, workflow.PushParams{}, 0, fmt.Errorf("decode definition edit fingerprint: %w", err)
	}
	if err := verifyTaskDefinitionEditPayloadRoundTrip(actionDC, fingerprintPayload, fingerprint); err != nil {
		return TaskDefinitionEditFingerprint{}, workflow.PushParams{}, 0, err
	}
	if err := verifyTaskDefinitionEditPayloadRoundTrip(actionDC, inputs[0], params); err != nil {
		return TaskDefinitionEditFingerprint{}, workflow.PushParams{}, 0, err
	}
	expected := taskScheduleExpected{
		taskID:      creation.TaskID,
		fingerprint: taskDefinitionEditCreationFingerprint(creation),
		params:      params,
		taskQueue:   creation.Action.TaskQueue,
		prepared:    creation,
		dc:          dc,
	}
	expected.prepared.Action.Params = params
	gotCore, err := verifyTaskScheduleProtoAction(action, expected)
	if err != nil {
		return TaskDefinitionEditFingerprint{}, workflow.PushParams{}, 0, err
	}
	if gotCore != expected.fingerprint || gotCore.LifecyclePhase != taskScheduleV1PhaseActive {
		return TaskDefinitionEditFingerprint{}, workflow.PushParams{}, 0, errors.New("workflow action ownership fingerprint is not active and exact")
	}
	reusePolicy := workflowAction.GetWorkflowIdReusePolicy()
	if reusePolicy != enums.WORKFLOW_ID_REUSE_POLICY_UNSPECIFIED &&
		reusePolicy != enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE {
		return TaskDefinitionEditFingerprint{}, workflow.PushParams{}, 0, errors.New("workflow id reuse policy is unsupported")
	}
	return fingerprint, params, reusePolicy, nil
}

func verifyTaskDefinitionEditPayloadRoundTrip[T any](
	dc converter.DataConverter,
	got *commonpb.Payload,
	value T,
) error {
	reencoded, err := dc.ToPayload(value)
	if err != nil {
		return fmt.Errorf("re-encode temporal payload: %w", err)
	}
	if reencoded == nil || !proto.Equal(got, reencoded) {
		return errors.New("temporal payload is not the supported canonical encoding")
	}
	return nil
}

func validateTaskDefinitionEditBaseFingerprint(
	fingerprint TaskDefinitionEditFingerprint,
	head TaskDefinitionEditHead,
	paused bool,
	note string,
) error {
	markerAbsent := fingerprint.DefinitionVersion == 0 && fingerprint.DefinitionDigest == "" &&
		fingerprint.EditOperationDigest == "" && fingerprint.EditPhase == ""
	if head.Version == 1 && markerAbsent {
		if !paused && note != taskScheduleV1ActivationNote {
			return errors.New("unmarked active base has an unexpected note")
		}
		return nil
	}
	if markerAbsent || fingerprint.DefinitionVersion != head.Version ||
		fingerprint.DefinitionDigest != head.Digest {
		return errors.New("definition edit marker does not match the approved base head")
	}
	if err := validateTaskScheduleDigest("base edit operation digest", fingerprint.EditOperationDigest); err != nil {
		return err
	}
	// The final phase records the lifecycle state at commit time, while later
	// pause, resume, and runtime-cutover operations deliberately preserve the
	// committed definition marker. Either final variant therefore proves the
	// same Approved head. The current paused flag is checked against
	// PostgreSQL above and its note is frozen into the new edit proposal.
	if fingerprint.EditPhase != "final_active" &&
		fingerprint.EditPhase != "final_paused" {
		return errors.New("base definition edit marker is not a final phase")
	}
	return nil
}

func validateTaskDefinitionEditPolicies(
	policies *schedulepb.SchedulePolicies,
	expected PreparedTaskSchedulePolicy,
) error {
	if policies == nil ||
		policies.GetOverlapPolicy() != enums.ScheduleOverlapPolicy(expected.Overlap) ||
		!protoDurationMatches(policies.GetCatchupWindow(), time.Duration(expected.CatchupNanos)) ||
		policies.GetPauseOnFailure() != expected.PauseOnFailure ||
		policies.GetKeepOriginalWorkflowId() {
		return errors.New("schedule policies are not the frozen supported policies")
	}
	return nil
}

func classifyTaskDefinitionEditDescription(
	prepared PreparedTaskDefinitionEdit,
	desc *workflowservice.DescribeScheduleResponse,
	dc converter.DataConverter,
	operation string,
) (TaskDefinitionEditSnapshot, error) {
	type candidate struct {
		phase          TaskDefinitionEditPhase
		representation PreparedTaskDefinitionEditSchedule
	}
	candidates := []candidate{
		{TaskDefinitionEditPhaseTargetFinal, prepared.TargetFinal},
		{TaskDefinitionEditPhaseTargetPaused, prepared.TargetPaused},
		{TaskDefinitionEditPhaseBasePaused, prepared.BasePaused},
		{TaskDefinitionEditPhaseBaseOriginal, prepared.BaseOriginal},
	}
	if prepared.OriginalState == TaskDefinitionEditOriginalStatePaused {
		// These pairs are intentionally byte-identical for an originally
		// paused task: there is no pause or restore mutation. Classify them as
		// the phases the coordinator is authorized to checkpoint.
		candidates = []candidate{
			{TaskDefinitionEditPhaseTargetFinal, prepared.TargetFinal},
			{TaskDefinitionEditPhaseBaseOriginal, prepared.BaseOriginal},
		}
	}
	for _, candidate := range candidates {
		matches, err := taskDefinitionEditDescriptionMatches(desc, candidate.representation, dc)
		if err != nil {
			return TaskDefinitionEditSnapshot{}, newTaskScheduleError(
				TaskScheduleErrorUnsafeState, operation, prepared.Creation.TaskID, err,
			)
		}
		if matches {
			return taskDefinitionEditSnapshot(
				prepared, candidate.phase, candidate.representation, desc.GetConflictToken(),
			), nil
		}
	}
	return TaskDefinitionEditSnapshot{}, newTaskScheduleError(
		TaskScheduleErrorUnsafeState, operation, prepared.Creation.TaskID,
		errors.New("remote schedule is not any frozen definition edit representation"),
	)
}

func taskDefinitionEditDescriptionMatches(
	desc *workflowservice.DescribeScheduleResponse,
	expected PreparedTaskDefinitionEditSchedule,
	dc converter.DataConverter,
) (bool, error) {
	if desc == nil || len(desc.GetConflictToken()) == 0 || desc.GetSchedule() == nil {
		return false, errors.New("describe returned an incomplete schedule")
	}
	if err := rejectTaskDefinitionEditUnknownFields(desc.GetSchedule().ProtoReflect(), "schedule"); err != nil {
		return false, err
	}
	if err := rejectTaskDefinitionEditUnknownFields(desc.GetMemo().ProtoReflect(), "memo"); err != nil {
		return false, err
	}
	if err := rejectTaskDefinitionEditUnknownFields(desc.GetSearchAttributes().ProtoReflect(), "search_attributes"); err != nil {
		return false, err
	}
	if len(desc.GetMemo().GetFields()) != 0 || len(desc.GetSearchAttributes().GetIndexedFields()) != 0 {
		return false, errors.New("top-level schedule metadata is unsupported")
	}
	schedule := desc.GetSchedule()
	if !taskScheduleProtoSpecMatches(schedule.GetSpec(), expected.Timing) {
		return false, nil
	}
	if err := validateTaskDefinitionEditPolicies(schedule.GetPolicies(), expected.Policy); err != nil {
		return false, nil
	}
	state := schedule.GetState()
	if state == nil || state.GetPaused() != expected.State.Paused ||
		state.GetNotes() != expected.State.Note ||
		state.GetLimitedActions() != expected.State.LimitedActions ||
		state.GetRemainingActions() != expected.State.RemainingActions {
		return false, nil
	}
	creation := PreparedTaskSchedule{
		IDSchemeVersion:    expected.Fingerprint.IDSchemeVersion,
		FingerprintVersion: expected.Fingerprint.FingerprintVersion,
		Namespace:          expected.Fingerprint.Namespace,
		NamespaceID:        expected.Fingerprint.NamespaceID,
		ConverterID:        expected.Fingerprint.ConverterID,
		TaskID:             expected.Fingerprint.TaskID,
		TenantID:           expected.Fingerprint.TenantID,
		UserID:             expected.Fingerprint.UserID,
		OperationID:        expected.Fingerprint.CreationOperationID,
		PreparedDigest:     expected.Fingerprint.CreationPreparedDigest,
		RequestDigest:      expected.Fingerprint.CreationRequestDigest,
		Action:             expected.Action,
	}
	fingerprint, params, reusePolicy, err := decodeTaskDefinitionEditAction(schedule.GetAction(), creation, dc)
	if err != nil {
		return false, err
	}
	if fingerprint != expected.Fingerprint || !taskScheduleParamsEqual(params, expected.Action.Params) ||
		int32(reusePolicy) != expected.WorkflowIDReusePolicy {
		return false, nil
	}
	info := desc.GetInfo()
	// Temporal exposes no replacement validity signal yet; fail closed while
	// the deprecated field remains part of the server wire.
	//nolint:staticcheck
	if info == nil || info.GetInvalidScheduleError() != "" {
		return false, errors.New("schedule info is missing or invalid")
	}
	return true, nil
}

func buildTaskDefinitionEditUpdateRequest(
	prepared PreparedTaskDefinitionEdit,
	representation PreparedTaskDefinitionEditSchedule,
	conflictToken []byte,
	phase string,
	dc converter.DataConverter,
) (*workflowservice.UpdateScheduleRequest, error) {
	if len(conflictToken) == 0 {
		return nil, errors.New("describe returned no schedule conflict token")
	}
	schedule, err := taskDefinitionEditProtoSchedule(representation, dc)
	if err != nil {
		return nil, err
	}
	return &workflowservice.UpdateScheduleRequest{
		Namespace:     prepared.Creation.Namespace,
		ScheduleId:    prepared.Creation.TaskID,
		Schedule:      schedule,
		ConflictToken: slices.Clone(conflictToken),
		Identity:      "vane-task-definition-edit/" + prepared.WireVersion,
		RequestId: taskScheduleRequestID(
			"definition_edit/"+phase+"/"+prepared.OperationDigest,
			prepared.RequestDigest,
		),
	}, nil
}

func taskDefinitionEditProtoSchedule(
	representation PreparedTaskDefinitionEditSchedule,
	dc converter.DataConverter,
) (*schedulepb.Schedule, error) {
	creation := PreparedTaskSchedule{
		Namespace: representation.Fingerprint.Namespace,
		Action:    representation.Action,
	}
	actionDC, err := taskScheduleActionDataConverter(creation, dc)
	if err != nil {
		return nil, err
	}
	fingerprintPayload, err := actionDC.ToPayload(representation.Fingerprint)
	if err != nil {
		return nil, fmt.Errorf("encode definition edit fingerprint: %w", err)
	}
	paramsPayload, err := actionDC.ToPayload(representation.Action.Params)
	if err != nil {
		return nil, fmt.Errorf("encode definition edit workflow params: %w", err)
	}
	if fingerprintPayload == nil || paramsPayload == nil {
		return nil, errors.New("definition edit converter returned a nil payload")
	}
	return &schedulepb.Schedule{
		Spec: taskScheduleProtoSpec(representation.Timing),
		Action: &schedulepb.ScheduleAction{Action: &schedulepb.ScheduleAction_StartWorkflow{
			StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
				WorkflowId:   representation.Action.ActionID,
				WorkflowType: &commonpb.WorkflowType{Name: representation.Action.WorkflowType},
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: representation.Action.TaskQueue,
					Kind: enums.TASK_QUEUE_KIND_NORMAL,
				},
				Input:                    &commonpb.Payloads{Payloads: []*commonpb.Payload{paramsPayload}},
				WorkflowExecutionTimeout: durationpb.New(time.Duration(representation.Action.WorkflowExecutionTimeoutNanos)),
				WorkflowRunTimeout:       durationpb.New(time.Duration(representation.Action.WorkflowRunTimeoutNanos)),
				WorkflowTaskTimeout:      durationpb.New(time.Duration(representation.Action.WorkflowTaskTimeoutNanos)),
				WorkflowIdReusePolicy:    enums.WorkflowIdReusePolicy(representation.WorkflowIDReusePolicy),
				Memo: &commonpb.Memo{Fields: map[string]*commonpb.Payload{
					taskScheduleMemoKey: fingerprintPayload,
				}},
			},
		}},
		Policies: &schedulepb.SchedulePolicies{
			OverlapPolicy:  enums.ScheduleOverlapPolicy(representation.Policy.Overlap),
			CatchupWindow:  durationpb.New(time.Duration(representation.Policy.CatchupNanos)),
			PauseOnFailure: representation.Policy.PauseOnFailure,
		},
		State: &schedulepb.ScheduleState{
			Notes:            representation.State.Note,
			Paused:           representation.State.Paused,
			LimitedActions:   representation.State.LimitedActions,
			RemainingActions: representation.State.RemainingActions,
		},
	}, nil
}

func validatePreparedTaskDefinitionEdit(prepared PreparedTaskDefinitionEdit) error {
	if err := validatePreparedTaskSchedule(prepared.Creation); err != nil {
		return fmt.Errorf("validate creation ownership: %w", err)
	}
	if prepared.Creation.IDSchemeVersion != taskDefinitionEditOwnershipIDSchemeVersion {
		return errors.New("definition edit ownership ID scheme is not retained v1")
	}
	switch prepared.WireVersion {
	case taskDefinitionEditWireVersionV1:
		if prepared.Creation.FingerprintVersion != taskDefinitionEditOwnershipFingerprintVersion {
			return errors.New("definition edit v1 ownership fingerprint is not retained v2")
		}
	case taskDefinitionEditWireVersionV2:
		if prepared.Creation.FingerprintVersion != taskScheduleFingerprintVersionV1 {
			return errors.New("definition edit v2 ownership fingerprint is not retained v1")
		}
	default:
		return fmt.Errorf("unsupported definition edit wire version %q", prepared.WireVersion)
	}
	if err := validateTaskScheduleString("operation_id", prepared.OperationID, true); err != nil {
		return err
	}
	if len(prepared.OperationID) > taskDefinitionEditV1MaxOperationIDBytes {
		return errors.New("operation_id is too long")
	}
	if err := validateTaskScheduleDigest("operation_digest", prepared.OperationDigest); err != nil {
		return err
	}
	if err := validateTaskScheduleDigest("request_digest", prepared.RequestDigest); err != nil {
		return err
	}
	if err := validateTaskScheduleDigest("base_projection_digest", prepared.BaseProjectionDigest); err != nil {
		return err
	}
	if err := validateTaskScheduleDigest("target_projection_digest", prepared.TargetProjectionDigest); err != nil {
		return err
	}
	if err := validateTaskDefinitionEditHead("base_head", prepared.BaseHead); err != nil {
		return err
	}
	if err := validateTaskDefinitionEditHead("target_head", prepared.TargetHead); err != nil {
		return err
	}
	if prepared.TargetHead.Version != prepared.BaseHead.Version+1 {
		return errors.New("target head version must immediately follow base head version")
	}
	if prepared.OriginalState != TaskDefinitionEditOriginalStateActive &&
		prepared.OriginalState != TaskDefinitionEditOriginalStatePaused {
		return errors.New("original state must be active or paused")
	}
	if prepared.BaseRevision == "" {
		return errors.New("base revision is empty")
	}
	if _, err := base64.RawURLEncoding.DecodeString(prepared.BaseRevision); err != nil {
		return fmt.Errorf("base revision is invalid: %w", err)
	}
	for name, representation := range map[string]PreparedTaskDefinitionEditSchedule{
		"base_original": prepared.BaseOriginal,
		"base_paused":   prepared.BasePaused,
		"target_paused": prepared.TargetPaused,
		"target_final":  prepared.TargetFinal,
	} {
		if err := validatePreparedTaskDefinitionEditSchedule(name, representation, prepared.Creation); err != nil {
			return err
		}
	}
	operationDigest, err := digestTaskDefinitionEditOperationSeed(
		taskDefinitionEditOperationSeedFromPrepared(prepared),
	)
	if err != nil {
		return err
	}
	if operationDigest != prepared.OperationDigest {
		return errors.New("operation_digest does not match the frozen definition edit operation")
	}
	if err := validateTaskDefinitionEditPreparedPhases(prepared); err != nil {
		return err
	}
	digest, err := digestPreparedTaskDefinitionEdit(prepared)
	if err != nil {
		return err
	}
	if digest != prepared.RequestDigest {
		return errors.New("request_digest does not match the immutable prepared definition edit")
	}
	return nil
}

func validatePreparedTaskDefinitionEditSchedule(
	name string,
	representation PreparedTaskDefinitionEditSchedule,
	creation PreparedTaskSchedule,
) error {
	if err := validateTaskScheduleDigest(name+".digest", representation.Digest); err != nil {
		return err
	}
	digest, err := digestPreparedTaskDefinitionEditSchedule(representation)
	if err != nil {
		return err
	}
	if digest != representation.Digest {
		return fmt.Errorf("%s digest does not match its representation", name)
	}
	if _, err := scheduleSpecFromPreparedTimingV1(representation.Timing); err != nil {
		return fmt.Errorf("%s timing: %w", name, err)
	}
	if representation.Action.TaskQueue != creation.Action.TaskQueue ||
		representation.Action.WorkflowType != creation.Action.WorkflowType ||
		representation.Action.ActionID != creation.Action.ActionID ||
		representation.Action.WorkflowExecutionTimeoutNanos != creation.Action.WorkflowExecutionTimeoutNanos ||
		representation.Action.WorkflowRunTimeoutNanos != creation.Action.WorkflowRunTimeoutNanos ||
		representation.Action.WorkflowTaskTimeoutNanos != creation.Action.WorkflowTaskTimeoutNanos ||
		representation.Action.HasRetryPolicy != creation.Action.HasRetryPolicy ||
		representation.Action.ActivationNote != creation.Action.ActivationNote {
		return fmt.Errorf("%s changes immutable workflow execution settings", name)
	}
	params := representation.Action.Params
	if params.TenantID != creation.TenantID || params.UserID != creation.UserID ||
		params.RunKind != workflow.PushRunKindScheduled || params.ScheduleID != creation.TaskID ||
		params.ExecutionMode != types.ExecutionModeCompiled || params.Snapshot != nil {
		return fmt.Errorf("%s workflow params do not match the task owner", name)
	}
	if params.RuntimeVersion != "" && !workflow.IsCompiledRuntimeV1(params.RuntimeVersion) {
		return fmt.Errorf("%s workflow runtime version is unsupported", name)
	}
	if err := validateTaskDefinitionEditActionParams(name, params); err != nil {
		return err
	}
	if representation.Policy != creation.Policy || representation.State.LimitedActions ||
		representation.State.RemainingActions != 0 {
		return fmt.Errorf("%s changes unsupported schedule policy or limits", name)
	}
	if representation.WorkflowIDReusePolicy != int32(enums.WORKFLOW_ID_REUSE_POLICY_UNSPECIFIED) &&
		representation.WorkflowIDReusePolicy != int32(enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE) {
		return fmt.Errorf("%s workflow id reuse policy is unsupported", name)
	}
	if !utf8.ValidString(representation.State.Note) {
		return fmt.Errorf("%s state note is not valid utf-8", name)
	}
	wantFingerprint := taskDefinitionEditCreationFingerprint(creation)
	if taskDefinitionEditCoreFingerprint(representation.Fingerprint) != wantFingerprint {
		return fmt.Errorf("%s ownership fingerprint is not exact", name)
	}
	return nil
}

func validateTaskDefinitionEditPreparedPhases(prepared PreparedTaskDefinitionEdit) error {
	baseOriginal := prepared.BaseOriginal
	if err := validateTaskDefinitionEditBaseFingerprint(
		baseOriginal.Fingerprint, prepared.BaseHead, baseOriginal.State.Paused, baseOriginal.State.Note,
	); err != nil {
		return fmt.Errorf("base_original fingerprint: %w", err)
	}
	if !taskDefinitionEditSchedulesShareExecutionEnvelope(baseOriginal, prepared.TargetFinal) {
		return errors.New("definition edit changes fields outside timing, scope, or natural-language description")
	}
	if prepared.OriginalState == TaskDefinitionEditOriginalStateActive {
		if !taskDefinitionEditSchedulesShareDefinition(baseOriginal, prepared.BasePaused) ||
			!taskDefinitionEditSchedulesShareDefinition(prepared.TargetPaused, prepared.TargetFinal) {
			return errors.New("active edit changes definition within a pause or restore transition")
		}
		if baseOriginal.State.Paused || !prepared.BasePaused.State.Paused ||
			!prepared.TargetPaused.State.Paused || prepared.TargetFinal.State.Paused {
			return errors.New("active edit phases have invalid paused flags")
		}
		if err := validateTaskDefinitionEditPhaseFingerprint(
			prepared.BasePaused.Fingerprint, prepared.BaseHead, prepared.OperationDigest, "base_paused",
		); err != nil {
			return err
		}
		if err := validateTaskDefinitionEditPhaseFingerprint(
			prepared.TargetPaused.Fingerprint, prepared.TargetHead, prepared.OperationDigest, "target_paused",
		); err != nil {
			return err
		}
		if err := validateTaskDefinitionEditPhaseFingerprint(
			prepared.TargetFinal.Fingerprint, prepared.TargetHead, prepared.OperationDigest, "final_active",
		); err != nil {
			return err
		}
		if prepared.BasePaused.State.Note != taskDefinitionEditNote("base_paused", prepared.OperationDigest) ||
			prepared.TargetPaused.State.Note != taskDefinitionEditNote("target_paused", prepared.OperationDigest) ||
			prepared.TargetFinal.State.Note != taskDefinitionEditNote("final_active", prepared.OperationDigest) {
			return errors.New("active edit phase notes do not match their operation markers")
		}
	} else {
		if !baseOriginal.State.Paused || !prepared.BasePaused.State.Paused ||
			!prepared.TargetPaused.State.Paused || !prepared.TargetFinal.State.Paused {
			return errors.New("paused edit phases must remain paused")
		}
		if prepared.BasePaused.Digest != prepared.BaseOriginal.Digest ||
			prepared.TargetPaused.Digest != prepared.TargetFinal.Digest {
			return errors.New("paused edit contains an unexpected pause or restore transition")
		}
		if prepared.TargetFinal.State.Note != prepared.BaseOriginal.State.Note {
			return errors.New("paused edit did not preserve the original note")
		}
		if err := validateTaskDefinitionEditPhaseFingerprint(
			prepared.TargetFinal.Fingerprint, prepared.TargetHead, prepared.OperationDigest, "final_paused",
		); err != nil {
			return err
		}
	}
	return nil
}

func validateTaskDefinitionEditActionParams(name string, params workflow.PushParams) error {
	if err := validateTaskScheduleString(name+".action.params.nl_desc", params.NLDesc, true); err != nil {
		return err
	}
	if params.Scope.TopN < 0 {
		return fmt.Errorf("%s action scope top_n must not be negative", name)
	}
	seen := make(map[int64]struct{}, len(params.Scope.SourceIDs))
	for _, sourceID := range params.Scope.SourceIDs {
		if sourceID <= 0 {
			return fmt.Errorf("%s action scope source ids must be positive", name)
		}
		if _, exists := seen[sourceID]; exists {
			return fmt.Errorf("%s action scope contains duplicate source id %d", name, sourceID)
		}
		seen[sourceID] = struct{}{}
	}
	return nil
}

func taskDefinitionEditSchedulesShareDefinition(
	a PreparedTaskDefinitionEditSchedule,
	b PreparedTaskDefinitionEditSchedule,
) bool {
	return preparedTaskScheduleTimingEqual(a.Timing, b.Timing) &&
		a.Action.TaskQueue == b.Action.TaskQueue &&
		a.Action.WorkflowType == b.Action.WorkflowType &&
		a.Action.ActionID == b.Action.ActionID &&
		a.Action.WorkflowExecutionTimeoutNanos == b.Action.WorkflowExecutionTimeoutNanos &&
		a.Action.WorkflowRunTimeoutNanos == b.Action.WorkflowRunTimeoutNanos &&
		a.Action.WorkflowTaskTimeoutNanos == b.Action.WorkflowTaskTimeoutNanos &&
		a.Action.HasRetryPolicy == b.Action.HasRetryPolicy &&
		a.Action.ActivationNote == b.Action.ActivationNote &&
		taskScheduleParamsEqual(a.Action.Params, b.Action.Params) &&
		a.Policy == b.Policy &&
		a.WorkflowIDReusePolicy == b.WorkflowIDReusePolicy
}

func taskDefinitionEditSchedulesShareExecutionEnvelope(
	a PreparedTaskDefinitionEditSchedule,
	b PreparedTaskDefinitionEditSchedule,
) bool {
	aParams := a.Action.Params
	bParams := b.Action.Params
	return a.Action.TaskQueue == b.Action.TaskQueue &&
		a.Action.WorkflowType == b.Action.WorkflowType &&
		a.Action.ActionID == b.Action.ActionID &&
		a.Action.WorkflowExecutionTimeoutNanos == b.Action.WorkflowExecutionTimeoutNanos &&
		a.Action.WorkflowRunTimeoutNanos == b.Action.WorkflowRunTimeoutNanos &&
		a.Action.WorkflowTaskTimeoutNanos == b.Action.WorkflowTaskTimeoutNanos &&
		a.Action.HasRetryPolicy == b.Action.HasRetryPolicy &&
		a.Action.ActivationNote == b.Action.ActivationNote &&
		aParams.TenantID == bParams.TenantID && aParams.UserID == bParams.UserID &&
		aParams.RunKind == bParams.RunKind && aParams.ExecutionMode == bParams.ExecutionMode &&
		aParams.RuntimeVersion == bParams.RuntimeVersion && aParams.ScheduleID == bParams.ScheduleID &&
		aParams.Snapshot == nil && bParams.Snapshot == nil &&
		a.Policy == b.Policy && a.WorkflowIDReusePolicy == b.WorkflowIDReusePolicy
}

func validateTaskDefinitionEditPhaseFingerprint(
	fingerprint TaskDefinitionEditFingerprint,
	head TaskDefinitionEditHead,
	operationDigest string,
	phase string,
) error {
	if fingerprint.DefinitionVersion != head.Version || fingerprint.DefinitionDigest != head.Digest ||
		fingerprint.EditOperationDigest != operationDigest || fingerprint.EditPhase != phase {
		return fmt.Errorf("definition edit fingerprint does not match %s", phase)
	}
	return nil
}

func validateTaskDefinitionEditSnapshot(
	prepared PreparedTaskDefinitionEdit,
	snapshot TaskDefinitionEditSnapshot,
	expectedPhase TaskDefinitionEditPhase,
	expectedRepresentation PreparedTaskDefinitionEditSchedule,
) error {
	if snapshot.TaskID != prepared.Creation.TaskID || snapshot.RequestDigest != prepared.RequestDigest ||
		snapshot.Phase != expectedPhase || snapshot.RepresentationDigest != expectedRepresentation.Digest {
		return errors.New("definition edit snapshot does not belong to the expected prepared phase")
	}
	if snapshot.Revision == "" {
		return errors.New("definition edit snapshot revision is empty")
	}
	if _, err := base64.RawURLEncoding.DecodeString(snapshot.Revision); err != nil {
		return fmt.Errorf("definition edit snapshot revision is invalid: %w", err)
	}
	return nil
}

func rejectTaskDefinitionEditUnknownFields(message protoreflect.Message, path string) error {
	if !message.IsValid() {
		return nil
	}
	if len(message.GetUnknown()) != 0 {
		return fmt.Errorf("%s contains unknown protobuf fields", path)
	}
	var nestedErr error
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		if nestedErr != nil {
			return false
		}
		fieldPath := path + "." + string(field.Name())
		if field.IsMap() {
			if field.MapValue().Kind() != protoreflect.MessageKind &&
				field.MapValue().Kind() != protoreflect.GroupKind {
				return true
			}
			value.Map().Range(func(key protoreflect.MapKey, mapValue protoreflect.Value) bool {
				nestedErr = rejectTaskDefinitionEditUnknownFields(
					mapValue.Message(), fmt.Sprintf("%s[%v]", fieldPath, key.Interface()),
				)
				return nestedErr == nil
			})
			return nestedErr == nil
		}
		if field.IsList() {
			if field.Kind() != protoreflect.MessageKind && field.Kind() != protoreflect.GroupKind {
				return true
			}
			list := value.List()
			for i := 0; i < list.Len(); i++ {
				nestedErr = rejectTaskDefinitionEditUnknownFields(
					list.Get(i).Message(), fmt.Sprintf("%s[%d]", fieldPath, i),
				)
				if nestedErr != nil {
					return false
				}
			}
			return true
		}
		if field.Kind() == protoreflect.MessageKind || field.Kind() == protoreflect.GroupKind {
			nestedErr = rejectTaskDefinitionEditUnknownFields(value.Message(), fieldPath)
			return nestedErr == nil
		}
		return true
	})
	return nestedErr
}

func taskDefinitionEditCoreFingerprint(fingerprint TaskDefinitionEditFingerprint) taskScheduleFingerprint {
	return taskScheduleFingerprint{
		IDSchemeVersion:    fingerprint.IDSchemeVersion,
		FingerprintVersion: fingerprint.FingerprintVersion,
		Namespace:          fingerprint.Namespace,
		NamespaceID:        fingerprint.NamespaceID,
		ConverterID:        fingerprint.ConverterID,
		TenantID:           fingerprint.TenantID,
		UserID:             fingerprint.UserID,
		TaskID:             fingerprint.TaskID,
		OperationID:        fingerprint.CreationOperationID,
		PreparedDigest:     fingerprint.CreationPreparedDigest,
		RequestDigest:      fingerprint.CreationRequestDigest,
		LifecyclePhase:     fingerprint.LifecyclePhase,
	}
}

func taskDefinitionEditCreationFingerprint(creation PreparedTaskSchedule) taskScheduleFingerprint {
	return taskScheduleFingerprint{
		IDSchemeVersion:    creation.IDSchemeVersion,
		FingerprintVersion: creation.FingerprintVersion,
		Namespace:          creation.Namespace,
		NamespaceID:        creation.NamespaceID,
		ConverterID:        creation.ConverterID,
		TenantID:           creation.TenantID,
		UserID:             creation.UserID,
		TaskID:             creation.TaskID,
		OperationID:        creation.OperationID,
		PreparedDigest:     creation.PreparedDigest,
		RequestDigest:      creation.RequestDigest,
		LifecyclePhase:     taskScheduleV1PhaseActive,
	}
}

func taskDefinitionEditFingerprintFor(
	base TaskDefinitionEditFingerprint,
	head TaskDefinitionEditHead,
	operationDigest string,
	phase string,
) TaskDefinitionEditFingerprint {
	base.DefinitionVersion = head.Version
	base.DefinitionDigest = head.Digest
	base.EditOperationDigest = operationDigest
	base.EditPhase = phase
	return base
}

func taskDefinitionEditNote(phase, operationDigest string) string {
	const shortDigestLength = 16
	if len(operationDigest) > shortDigestLength {
		operationDigest = operationDigest[:shortDigestLength]
	}
	return taskDefinitionEditNotePrefix + ":" + phase + ":" + operationDigest
}

type taskDefinitionEditOperationSeed struct {
	WireVersion            string                          `json:"wire_version"`
	OperationID            string                          `json:"operation_id"`
	CreationRequestDigest  string                          `json:"creation_request_digest"`
	TenantID               int64                           `json:"tenant_id"`
	UserID                 int64                           `json:"user_id"`
	TaskID                 string                          `json:"task_id"`
	BaseHead               TaskDefinitionEditHead          `json:"base_head"`
	TargetHead             TaskDefinitionEditHead          `json:"target_head"`
	OriginalState          TaskDefinitionEditOriginalState `json:"original_state"`
	BaseProjectionDigest   string                          `json:"base_projection_digest"`
	TargetProjectionDigest string                          `json:"target_projection_digest"`
	BaseTiming             PreparedTaskScheduleTiming      `json:"base_timing"`
	BaseAction             PreparedTaskScheduleAction      `json:"base_action"`
	BasePolicy             PreparedTaskSchedulePolicy      `json:"base_policy"`
	BaseReusePolicy        int32                           `json:"base_reuse_policy"`
	BaseState              TaskDefinitionEditScheduleState `json:"base_state"`
	TargetTiming           PreparedTaskScheduleTiming      `json:"target_timing"`
	TargetAction           PreparedTaskScheduleAction      `json:"target_action"`
	TargetPolicy           PreparedTaskSchedulePolicy      `json:"target_policy"`
	TargetReusePolicy      int32                           `json:"target_reuse_policy"`
}

func taskDefinitionEditOperationSeedFromPrepared(
	prepared PreparedTaskDefinitionEdit,
) taskDefinitionEditOperationSeed {
	return taskDefinitionEditOperationSeed{
		WireVersion:            prepared.WireVersion,
		OperationID:            prepared.OperationID,
		CreationRequestDigest:  prepared.Creation.RequestDigest,
		TenantID:               prepared.Creation.TenantID,
		UserID:                 prepared.Creation.UserID,
		TaskID:                 prepared.Creation.TaskID,
		BaseHead:               prepared.BaseHead,
		TargetHead:             prepared.TargetHead,
		OriginalState:          prepared.OriginalState,
		BaseProjectionDigest:   prepared.BaseProjectionDigest,
		TargetProjectionDigest: prepared.TargetProjectionDigest,
		BaseTiming:             clonePreparedTaskScheduleTiming(prepared.BaseOriginal.Timing),
		BaseAction:             cloneTaskDefinitionEditAction(prepared.BaseOriginal.Action),
		BasePolicy:             prepared.BaseOriginal.Policy,
		BaseReusePolicy:        prepared.BaseOriginal.WorkflowIDReusePolicy,
		BaseState:              prepared.BaseOriginal.State,
		TargetTiming:           clonePreparedTaskScheduleTiming(prepared.TargetFinal.Timing),
		TargetAction:           cloneTaskDefinitionEditAction(prepared.TargetFinal.Action),
		TargetPolicy:           prepared.TargetFinal.Policy,
		TargetReusePolicy:      prepared.TargetFinal.WorkflowIDReusePolicy,
	}
}

func digestTaskDefinitionEditOperationSeed(seed taskDefinitionEditOperationSeed) (string, error) {
	seed.BaseTiming = clonePreparedTaskScheduleTiming(seed.BaseTiming)
	seed.BaseAction = cloneTaskDefinitionEditAction(seed.BaseAction)
	seed.TargetTiming = clonePreparedTaskScheduleTiming(seed.TargetTiming)
	seed.TargetAction = cloneTaskDefinitionEditAction(seed.TargetAction)
	return digestTaskDefinitionEditJSON(seed)
}

type taskDefinitionEditProjectionV1 struct {
	Spec          ScheduleSpec       `json:"spec"`
	Scope         workflow.PushScope `json:"scope"`
	NLDescription string             `json:"nl_description"`
}

func digestTaskDefinitionEditProjectionV1(definition TaskDefinitionEditDefinition) (string, error) {
	if err := validateTaskDefinitionEditDefinition("projection", definition); err != nil {
		return "", err
	}
	return digestTaskDefinitionEditJSON(taskDefinitionEditProjectionV1{
		Spec:          definition.Spec,
		Scope:         cloneTaskDefinitionEditScope(definition.Scope),
		NLDescription: definition.NLDescription,
	})
}

func digestPreparedTaskDefinitionEditSchedule(
	representation PreparedTaskDefinitionEditSchedule,
) (string, error) {
	representation = clonePreparedTaskDefinitionEditSchedule(representation)
	representation.Digest = ""
	return digestTaskDefinitionEditJSON(representation)
}

func digestPreparedTaskDefinitionEdit(prepared PreparedTaskDefinitionEdit) (string, error) {
	prepared = clonePreparedTaskDefinitionEdit(prepared)
	prepared.RequestDigest = ""
	return digestTaskDefinitionEditJSON(prepared)
}

func digestTaskDefinitionEditJSON(value any) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("marshal task definition edit: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

func clonePreparedTaskDefinitionEdit(prepared PreparedTaskDefinitionEdit) PreparedTaskDefinitionEdit {
	prepared.Creation = clonePreparedTaskSchedule(prepared.Creation)
	prepared.BaseOriginal = clonePreparedTaskDefinitionEditSchedule(prepared.BaseOriginal)
	prepared.BasePaused = clonePreparedTaskDefinitionEditSchedule(prepared.BasePaused)
	prepared.TargetPaused = clonePreparedTaskDefinitionEditSchedule(prepared.TargetPaused)
	prepared.TargetFinal = clonePreparedTaskDefinitionEditSchedule(prepared.TargetFinal)
	return prepared
}

func clonePreparedTaskDefinitionEditSchedule(
	representation PreparedTaskDefinitionEditSchedule,
) PreparedTaskDefinitionEditSchedule {
	representation.Timing = clonePreparedTaskScheduleTiming(representation.Timing)
	representation.Action.Params.Scope.SourceIDs = slices.Clone(representation.Action.Params.Scope.SourceIDs)
	return representation
}

func cloneTaskDefinitionEditAction(action PreparedTaskScheduleAction) PreparedTaskScheduleAction {
	action.Params.Scope = cloneTaskDefinitionEditScope(action.Params.Scope)
	return action
}

func clonePreparedTaskScheduleTiming(timing PreparedTaskScheduleTiming) PreparedTaskScheduleTiming {
	if timing.Calendar != nil {
		calendar := *timing.Calendar
		timing.Calendar = &calendar
	}
	return timing
}

func cloneTaskDefinitionEditDefinition(definition TaskDefinitionEditDefinition) TaskDefinitionEditDefinition {
	definition.Scope = cloneTaskDefinitionEditScope(definition.Scope)
	return definition
}

func cloneTaskDefinitionEditScope(scope workflow.PushScope) workflow.PushScope {
	scope.SourceIDs = slices.Clone(scope.SourceIDs)
	return scope
}

func taskDefinitionEditSnapshot(
	prepared PreparedTaskDefinitionEdit,
	phase TaskDefinitionEditPhase,
	representation PreparedTaskDefinitionEditSchedule,
	conflictToken []byte,
) TaskDefinitionEditSnapshot {
	return taskDefinitionEditSnapshotFromRevision(
		prepared, phase, representation, taskScheduleRevision(conflictToken),
	)
}

func taskDefinitionEditSnapshotFromRevision(
	prepared PreparedTaskDefinitionEdit,
	phase TaskDefinitionEditPhase,
	representation PreparedTaskDefinitionEditSchedule,
	revision string,
) TaskDefinitionEditSnapshot {
	return TaskDefinitionEditSnapshot{
		TaskID:               prepared.Creation.TaskID,
		RequestDigest:        prepared.RequestDigest,
		Phase:                phase,
		RepresentationDigest: representation.Digest,
		Revision:             revision,
	}
}

func taskDefinitionEditRepresentation(
	prepared PreparedTaskDefinitionEdit,
	phase TaskDefinitionEditPhase,
) PreparedTaskDefinitionEditSchedule {
	switch phase {
	case TaskDefinitionEditPhaseBaseOriginal:
		return prepared.BaseOriginal
	case TaskDefinitionEditPhaseBasePaused:
		return prepared.BasePaused
	case TaskDefinitionEditPhaseTargetPaused:
		return prepared.TargetPaused
	case TaskDefinitionEditPhaseTargetFinal:
		return prepared.TargetFinal
	default:
		return PreparedTaskDefinitionEditSchedule{}
	}
}

func (s *Scheduler) describeTaskDefinitionEdit(
	ctx context.Context,
	namespace string,
	taskID string,
) (*workflowservice.DescribeScheduleResponse, error) {
	return s.c.WorkflowService().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
		Namespace: namespace, ScheduleId: taskID,
	})
}

func classifyTaskDefinitionEditReadError(operation, taskID string, err error) error {
	if isTaskDefinitionEditScheduleNotFound(err) {
		return newTaskScheduleError(TaskScheduleErrorNotFound, operation, taskID, err)
	}
	return classifyTaskScheduleOperationError(operation, taskID, err)
}

func isTaskDefinitionEditScheduleNotFound(err error) bool {
	if err == nil {
		return false
	}
	if _, namespaceMissing := errors.AsType[*serviceerror.NamespaceNotFound](err); namespaceMissing {
		return false
	}
	_, scheduleMissing := errors.AsType[*serviceerror.NotFound](err)
	return scheduleMissing
}

func (s *Scheduler) describeTaskDefinitionEditForRecovery(
	ctx context.Context,
	namespace string,
	taskID string,
) (*workflowservice.DescribeScheduleResponse, error) {
	recoveryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), taskScheduleRecoveryTimeout)
	defer cancel()
	return s.describeTaskDefinitionEdit(recoveryCtx, namespace, taskID)
}
