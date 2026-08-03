package scheduler

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"

	schedulepb "go.temporal.io/api/schedule/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"

	"github.com/YouToco/vane/internal/strictjson"
	"github.com/YouToco/vane/taskstate"
)

const (
	preparedResearchTaskDefinitionEditV3WireVersion = "vane.prepared-research-task-definition-edit/v3"
	researchTaskDefinitionEditSnapshotV3WireVersion = "vane.research-task-definition-edit-snapshot/v3"
	researchTaskDefinitionEditPausedNoteV3          = "vane/research-task-definition-edit/v3:paused"
)

// ResearchTaskDefinitionEditOriginalStateV3 is the owner-visible state which
// must be restored after the new definition and Temporal representation have
// both committed. It is deliberately independent from retained V1/V2 edit
// enums so their durable readers cannot reinterpret this wire.
type ResearchTaskDefinitionEditOriginalStateV3 string

const (
	ResearchTaskDefinitionEditOriginalActiveV3 ResearchTaskDefinitionEditOriginalStateV3 = "active"
	ResearchTaskDefinitionEditOriginalPausedV3 ResearchTaskDefinitionEditOriginalStateV3 = "paused"
)

// PreparedResearchTaskDefinitionEditV3 freezes both sides of a native V3
// edit. Base and Target retain the creation operation identity and exact V3
// authorization token. Target changes only definition/timing fingerprints;
// no Source, fetch target, or long-lived Tool call can enter this protocol.
type PreparedResearchTaskDefinitionEditV3 struct {
	WireVersion             string                                    `json:"wire_version"`
	OperationID             string                                    `json:"operation_id"`
	OriginalState           ResearchTaskDefinitionEditOriginalStateV3 `json:"original_state"`
	BaseDefinitionVersion   int64                                     `json:"base_definition_version"`
	BaseDefinitionDigest    string                                    `json:"base_definition_digest"`
	TargetDefinitionVersion int64                                     `json:"target_definition_version"`
	TargetDefinitionDigest  string                                    `json:"target_definition_digest"`
	Base                    PreparedResearchTaskScheduleV3            `json:"base"`
	Target                  PreparedResearchTaskScheduleV3            `json:"target"`
	RequestDigest           string                                    `json:"request_digest"`
}

// ResearchTaskDefinitionEditSnapshotV3 is the bounded receipt checkpointed
// after each exact Temporal representation is observed. It contains no token
// or protobuf bytes.
type ResearchTaskDefinitionEditSnapshotV3 struct {
	WireVersion      string `json:"wire_version"`
	Phase            string `json:"phase"`
	TaskID           string `json:"task_id"`
	DefinitionDigest string `json:"definition_digest"`
	Revision         string `json:"revision"`
	Paused           bool   `json:"paused"`
}

// PrepareResearchTaskDefinitionEditV3 freezes the target timing against an
// exact native-creation checkpoint and the current formal V3 Schedule. The
// caller supplies the next immutable definition identity; this method never
// reads or compiles task semantics.
func (s *Scheduler) PrepareResearchTaskDefinitionEditV3(
	ctx context.Context,
	operationID string,
	base PreparedResearchTaskScheduleV3,
	baseDefinitionVersion int64,
	baseDefinitionDigest string,
	targetDefinitionVersion int64,
	targetDefinition taskstate.ApprovedDefinitionV3,
) (PreparedResearchTaskDefinitionEditV3, ResearchTaskDefinitionEditSnapshotV3, error) {
	const operation = "prepare_research_definition_edit_v3"
	if err := taskScheduleContextError(ctx, operation, base.Schedule.TaskID); err != nil {
		return PreparedResearchTaskDefinitionEditV3{}, ResearchTaskDefinitionEditSnapshotV3{}, err
	}
	targetDefinitionDigest, digestErr := taskstate.DigestApprovedDefinitionV3(targetDefinition)
	if !validResearchTaskDefinitionEditOperationIDV3(operationID) || digestErr != nil ||
		baseDefinitionVersion <= 0 || targetDefinitionVersion != baseDefinitionVersion+1 ||
		validateTaskScheduleDigest("base_definition_digest", baseDefinitionDigest) != nil ||
		baseDefinitionDigest == targetDefinitionDigest {
		return PreparedResearchTaskDefinitionEditV3{}, ResearchTaskDefinitionEditSnapshotV3{},
			newTaskScheduleError(TaskScheduleErrorInvalid, operation,
				base.Schedule.TaskID, errors.New("research V3 edit identity is invalid"))
	}
	recoveredBase, err := s.recoverPreparedResearchTaskScheduleV3(
		ctx, base, operation, true)
	if err != nil {
		return PreparedResearchTaskDefinitionEditV3{}, ResearchTaskDefinitionEditSnapshotV3{}, err
	}
	if recoveredBase.Schedule.PreparedDigest != baseDefinitionDigest {
		return PreparedResearchTaskDefinitionEditV3{}, ResearchTaskDefinitionEditSnapshotV3{},
			newTaskScheduleError(TaskScheduleErrorConflict, operation,
				recoveredBase.Schedule.TaskID,
				errors.New("base Schedule definition digest differs"))
	}
	if targetDefinition.TenantID != recoveredBase.Schedule.TenantID ||
		targetDefinition.UserID != recoveredBase.Schedule.UserID ||
		targetDefinition.TaskID != recoveredBase.Schedule.TaskID {
		return PreparedResearchTaskDefinitionEditV3{}, ResearchTaskDefinitionEditSnapshotV3{},
			newTaskScheduleError(TaskScheduleErrorInvalid, operation,
				recoveredBase.Schedule.TaskID,
				errors.New("target definition scope differs from the formal Schedule"))
	}
	var targetSpec ScheduleSpec
	if err := json.Unmarshal(targetDefinition.SpecJSON, &targetSpec); err != nil {
		return PreparedResearchTaskDefinitionEditV3{}, ResearchTaskDefinitionEditSnapshotV3{},
			newTaskScheduleError(TaskScheduleErrorInvalid, operation,
				recoveredBase.Schedule.TaskID,
				errors.New("target definition schedule is invalid"))
	}
	_, targetTiming, err := buildTaskScheduleSpec(targetSpec)
	if err != nil {
		return PreparedResearchTaskDefinitionEditV3{}, ResearchTaskDefinitionEditSnapshotV3{},
			newTaskScheduleError(TaskScheduleErrorInvalid, operation,
				recoveredBase.Schedule.TaskID, err)
	}
	target := clonePreparedResearchTaskScheduleV3(recoveredBase)
	target.Schedule.PreparedDigest = targetDefinitionDigest
	target.Schedule.Timing = targetTiming
	target.Schedule.RequestDigest = ""
	target.Schedule.RequestDigest, err = digestPreparedTaskSchedule(target.Schedule)
	if err != nil {
		return PreparedResearchTaskDefinitionEditV3{}, ResearchTaskDefinitionEditSnapshotV3{}, err
	}
	// The formal Action includes the fingerprint memo, so its digest changes
	// with the definition even though the Workflow input/token do not.
	target.TargetAction = nil
	target.TargetActionDigest = ""
	target.ActionAuthorizationDigest = recoveredBase.ActionAuthorizationDigest
	target, err = s.recoverPreparedResearchTaskScheduleV3(
		ctx, target, operation, false)
	if err != nil {
		return PreparedResearchTaskDefinitionEditV3{}, ResearchTaskDefinitionEditSnapshotV3{}, err
	}
	if target.Input.ActionAuthorizationToken != recoveredBase.Input.ActionAuthorizationToken ||
		target.ActionAuthorizationDigest != recoveredBase.ActionAuthorizationDigest {
		return PreparedResearchTaskDefinitionEditV3{}, ResearchTaskDefinitionEditSnapshotV3{},
			newTaskScheduleError(TaskScheduleErrorConflict, operation,
				target.Schedule.TaskID,
				errors.New("research V3 edit changed Action authorization"))
	}

	baseExpected, err := s.buildResearchTaskScheduleExpectedV3(
		ctx, recoveredBase, operation, false, true)
	if err != nil {
		return PreparedResearchTaskDefinitionEditV3{}, ResearchTaskDefinitionEditSnapshotV3{}, err
	}
	desc, err := s.describeTaskSchedule(ctx, baseExpected.base)
	if err != nil {
		return PreparedResearchTaskDefinitionEditV3{}, ResearchTaskDefinitionEditSnapshotV3{},
			classifyTaskScheduleReadError(operation, recoveredBase.Schedule.TaskID, err)
	}
	baseSnapshot, err := verifyResearchTaskScheduleDescriptionV3(baseExpected, desc, operation)
	if err != nil {
		return PreparedResearchTaskDefinitionEditV3{}, ResearchTaskDefinitionEditSnapshotV3{}, err
	}
	originalState := ResearchTaskDefinitionEditOriginalPausedV3
	switch baseSnapshot.State {
	case TaskScheduleActiveVirginExact, TaskScheduleActiveUsedExact:
		originalState = ResearchTaskDefinitionEditOriginalActiveV3
	case TaskSchedulePausedUsedExact:
	default:
		return PreparedResearchTaskDefinitionEditV3{}, ResearchTaskDefinitionEditSnapshotV3{},
			newTaskScheduleError(TaskScheduleErrorUnsafeState, operation,
				recoveredBase.Schedule.TaskID,
				fmt.Errorf("unsupported native V3 edit base state %s", baseSnapshot.State))
	}
	prepared := PreparedResearchTaskDefinitionEditV3{
		WireVersion: preparedResearchTaskDefinitionEditV3WireVersion,
		OperationID: operationID, OriginalState: originalState,
		BaseDefinitionVersion:   baseDefinitionVersion,
		BaseDefinitionDigest:    baseDefinitionDigest,
		TargetDefinitionVersion: targetDefinitionVersion,
		TargetDefinitionDigest:  targetDefinitionDigest,
		Base:                    recoveredBase, Target: target,
	}
	prepared.RequestDigest, err = digestPreparedResearchTaskDefinitionEditV3(prepared)
	if err != nil {
		return PreparedResearchTaskDefinitionEditV3{}, ResearchTaskDefinitionEditSnapshotV3{}, err
	}
	if err := validatePreparedResearchTaskDefinitionEditV3(prepared); err != nil {
		return PreparedResearchTaskDefinitionEditV3{}, ResearchTaskDefinitionEditSnapshotV3{},
			newTaskScheduleError(TaskScheduleErrorInvalid, operation,
				prepared.Base.Schedule.TaskID, err)
	}
	return prepared, researchTaskDefinitionEditSnapshotV3(
		prepared, "base_original", baseSnapshot, originalState == ResearchTaskDefinitionEditOriginalPausedV3), nil
}

func clonePreparedResearchTaskScheduleV3(
	prepared PreparedResearchTaskScheduleV3,
) PreparedResearchTaskScheduleV3 {
	prepared.Schedule = clonePreparedTaskSchedule(prepared.Schedule)
	prepared.TargetAction = bytes.Clone(prepared.TargetAction)
	return prepared
}

func digestPreparedResearchTaskDefinitionEditV3(
	prepared PreparedResearchTaskDefinitionEditV3,
) (string, error) {
	prepared.RequestDigest = ""
	raw, err := json.Marshal(prepared)
	if err != nil {
		return "", fmt.Errorf("marshal prepared research V3 edit: %w", err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func validatePreparedResearchTaskDefinitionEditV3(
	prepared PreparedResearchTaskDefinitionEditV3,
) error {
	if prepared.WireVersion != preparedResearchTaskDefinitionEditV3WireVersion ||
		!validResearchTaskDefinitionEditOperationIDV3(prepared.OperationID) ||
		prepared.BaseDefinitionVersion <= 0 ||
		prepared.TargetDefinitionVersion != prepared.BaseDefinitionVersion+1 ||
		prepared.BaseDefinitionDigest == prepared.TargetDefinitionDigest ||
		validateTaskScheduleDigest("base_definition_digest", prepared.BaseDefinitionDigest) != nil ||
		validateTaskScheduleDigest("target_definition_digest", prepared.TargetDefinitionDigest) != nil ||
		validateTaskScheduleDigest("request_digest", prepared.RequestDigest) != nil {
		return errors.New("research V3 edit envelope is invalid")
	}
	if prepared.OriginalState != ResearchTaskDefinitionEditOriginalActiveV3 &&
		prepared.OriginalState != ResearchTaskDefinitionEditOriginalPausedV3 {
		return errors.New("research V3 edit original state is invalid")
	}
	base, target := prepared.Base, prepared.Target
	if validatePreparedResearchTaskScheduleV3Pure(base) != nil ||
		validatePreparedResearchTaskScheduleV3Pure(target) != nil ||
		base.Schedule.TaskID == "" || base.Schedule.TaskID != target.Schedule.TaskID ||
		base.Schedule.TenantID != target.Schedule.TenantID ||
		base.Schedule.UserID != target.Schedule.UserID ||
		base.Schedule.OperationID != target.Schedule.OperationID ||
		base.Schedule.PreparedDigest != prepared.BaseDefinitionDigest ||
		target.Schedule.PreparedDigest != prepared.TargetDefinitionDigest ||
		base.Input != target.Input ||
		base.ActionAuthorizationDigest != target.ActionAuthorizationDigest {
		return errors.New("research V3 edit base/target identity differs")
	}
	wantTarget := clonePreparedResearchTaskScheduleV3(base)
	wantTarget.Schedule.PreparedDigest = target.Schedule.PreparedDigest
	wantTarget.Schedule.Timing = target.Schedule.Timing
	wantTarget.Schedule.RequestDigest = target.Schedule.RequestDigest
	wantTarget.TargetAction = bytes.Clone(target.TargetAction)
	wantTarget.TargetActionDigest = target.TargetActionDigest
	if !reflect.DeepEqual(wantTarget, target) {
		return errors.New("research V3 edit changed the frozen execution envelope")
	}
	want, err := digestPreparedResearchTaskDefinitionEditV3(prepared)
	if err != nil || want != prepared.RequestDigest {
		return errors.New("research V3 edit request digest differs")
	}
	return nil
}

func validResearchTaskDefinitionEditOperationIDV3(value string) bool {
	if len(value) == 0 || len(value) > 255 || strings.TrimSpace(value) != value {
		return false
	}
	for _, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') || strings.ContainsRune("-_.:", char) {
			continue
		}
		return false
	}
	return true
}

func validatePreparedResearchTaskScheduleV3Pure(
	prepared PreparedResearchTaskScheduleV3,
) error {
	if prepared.WireVersion != preparedResearchTaskScheduleV3WireVersion ||
		validatePreparedTaskSchedule(prepared.Schedule) != nil ||
		prepared.Input.TenantID != prepared.Schedule.TenantID ||
		prepared.Input.UserID != prepared.Schedule.UserID ||
		prepared.Input.TaskID != prepared.Schedule.TaskID ||
		len(prepared.Input.ActionAuthorizationToken) != sha256.Size*2 ||
		validateTaskScheduleDigest("target_action_digest", prepared.TargetActionDigest) != nil ||
		validateTaskScheduleDigest("action_authorization_digest", prepared.ActionAuthorizationDigest) != nil ||
		len(prepared.TargetAction) == 0 {
		return errors.New("research V3 schedule checkpoint is invalid")
	}
	actionSum := sha256.Sum256(prepared.TargetAction)
	authorizationSum := sha256.Sum256([]byte(prepared.Input.ActionAuthorizationToken))
	if hex.EncodeToString(actionSum[:]) != prepared.TargetActionDigest ||
		hex.EncodeToString(authorizationSum[:]) != prepared.ActionAuthorizationDigest {
		return errors.New("research V3 schedule evidence digest differs")
	}
	return nil
}

// EncodePreparedResearchTaskDefinitionEditV3 produces the immutable Store
// checkpoint. Strict recovery rejects non-canonical bytes and any token drift.
func EncodePreparedResearchTaskDefinitionEditV3(
	prepared PreparedResearchTaskDefinitionEditV3,
) ([]byte, error) {
	if err := validatePreparedResearchTaskDefinitionEditV3(prepared); err != nil {
		return nil, err
	}
	return json.Marshal(prepared)
}

func DecodePreparedResearchTaskDefinitionEditV3(
	raw []byte,
) (PreparedResearchTaskDefinitionEditV3, error) {
	var prepared PreparedResearchTaskDefinitionEditV3
	if len(raw) == 0 || len(raw) > 4<<20 || strictjson.DecodeExact(raw, &prepared) != nil {
		return PreparedResearchTaskDefinitionEditV3{}, errors.New("research V3 edit checkpoint is invalid")
	}
	canonical, err := json.Marshal(prepared)
	if err != nil || !bytes.Equal(canonical, raw) ||
		validatePreparedResearchTaskDefinitionEditV3(prepared) != nil {
		return PreparedResearchTaskDefinitionEditV3{}, errors.New("research V3 edit checkpoint is not canonical")
	}
	return prepared, nil
}

func researchTaskDefinitionEditSnapshotV3(
	prepared PreparedResearchTaskDefinitionEditV3,
	phase string,
	snapshot TaskScheduleSnapshot,
	paused bool,
) ResearchTaskDefinitionEditSnapshotV3 {
	digest := prepared.BaseDefinitionDigest
	if phase == "target_paused" || phase == "target_final" {
		digest = prepared.TargetDefinitionDigest
	}
	return ResearchTaskDefinitionEditSnapshotV3{
		WireVersion: researchTaskDefinitionEditSnapshotV3WireVersion,
		Phase:       phase, TaskID: prepared.Base.Schedule.TaskID,
		DefinitionDigest: digest, Revision: snapshot.Revision, Paused: paused,
	}
}

// PauseResearchTaskDefinitionEditV3 moves an originally-active schedule to
// the exact base-paused representation. An originally-paused schedule is only
// re-described; no RPC is needed.
func (s *Scheduler) PauseResearchTaskDefinitionEditV3(
	ctx context.Context,
	prepared PreparedResearchTaskDefinitionEditV3,
) (ResearchTaskDefinitionEditSnapshotV3, error) {
	return s.mutateResearchTaskDefinitionEditV3(ctx, prepared, "base_paused")
}

// ApplyResearchTaskDefinitionEditV3 swaps base-paused to target-paused. It is
// safe to retry after UpdateSchedule response loss.
func (s *Scheduler) ApplyResearchTaskDefinitionEditV3(
	ctx context.Context,
	prepared PreparedResearchTaskDefinitionEditV3,
) (ResearchTaskDefinitionEditSnapshotV3, error) {
	return s.mutateResearchTaskDefinitionEditV3(ctx, prepared, "target_paused")
}

// RestoreResearchTaskDefinitionEditV3 restores the original active/paused
// state using the exact target definition representation.
func (s *Scheduler) RestoreResearchTaskDefinitionEditV3(
	ctx context.Context,
	prepared PreparedResearchTaskDefinitionEditV3,
) (ResearchTaskDefinitionEditSnapshotV3, error) {
	return s.mutateResearchTaskDefinitionEditV3(ctx, prepared, "target_final")
}

func (s *Scheduler) mutateResearchTaskDefinitionEditV3(
	ctx context.Context,
	prepared PreparedResearchTaskDefinitionEditV3,
	phase string,
) (ResearchTaskDefinitionEditSnapshotV3, error) {
	operation := "research_definition_edit_v3_" + phase
	if err := validatePreparedResearchTaskDefinitionEditV3(prepared); err != nil {
		return ResearchTaskDefinitionEditSnapshotV3{},
			newTaskScheduleError(TaskScheduleErrorInvalid, operation,
				prepared.Base.Schedule.TaskID, err)
	}
	baseExpected, err := s.buildResearchTaskScheduleExpectedV3(
		ctx, prepared.Base, operation, true, true)
	if err != nil {
		return ResearchTaskDefinitionEditSnapshotV3{}, err
	}
	targetExpected, err := s.buildResearchTaskScheduleExpectedV3(
		ctx, prepared.Target, operation, true, true)
	if err != nil {
		return ResearchTaskDefinitionEditSnapshotV3{}, err
	}
	release, err := s.acquireTaskScheduleGate(ctx, operation, baseExpected.base.taskID)
	if err != nil {
		return ResearchTaskDefinitionEditSnapshotV3{}, err
	}
	defer release()
	desc, err := s.describeTaskSchedule(ctx, baseExpected.base)
	if err != nil {
		return ResearchTaskDefinitionEditSnapshotV3{},
			classifyTaskScheduleReadError(operation, baseExpected.base.taskID, err)
	}
	if snapshot, ok := researchTaskDefinitionEditDesiredSnapshotV3(
		prepared, phase, baseExpected, targetExpected, desc); ok {
		return snapshot, nil
	}
	if !researchTaskDefinitionEditSourceExactV3(
		prepared, phase, baseExpected, targetExpected, desc) {
		return ResearchTaskDefinitionEditSnapshotV3{},
			newTaskScheduleError(TaskScheduleErrorUnsafeState, operation,
				baseExpected.base.taskID,
				errors.New("research V3 edit observed a foreign Schedule representation"))
	}
	desired, err := researchTaskDefinitionEditDesiredScheduleV3(
		prepared, phase, baseExpected, targetExpected)
	if err != nil {
		return ResearchTaskDefinitionEditSnapshotV3{},
			newTaskScheduleError(TaskScheduleErrorBlocked, operation,
				baseExpected.base.taskID, err)
	}
	updateErr := s.compareAndSwapResearchV3CutoverSchedule(
		ctx, baseExpected.base.taskID, desired, desc.GetConflictToken(),
		taskScheduleRequestID(operation, prepared.RequestDigest))
	post, describeErr := s.describeTaskScheduleForRecovery(ctx, baseExpected.base)
	if describeErr == nil {
		if snapshot, ok := researchTaskDefinitionEditDesiredSnapshotV3(
			prepared, phase, baseExpected, targetExpected, post); ok {
			return snapshot, nil
		}
	}
	if isTaskScheduleNotFound(describeErr) {
		return ResearchTaskDefinitionEditSnapshotV3{},
			newTaskScheduleError(TaskScheduleErrorNotFound, operation,
				baseExpected.base.taskID, describeErr)
	}
	if describeErr != nil && !taskScheduleMutationDefinitelyRejected(updateErr) {
		return ResearchTaskDefinitionEditSnapshotV3{},
			newTaskScheduleError(TaskScheduleErrorOutcomeUnknown, operation,
				baseExpected.base.taskID, errors.Join(updateErr, describeErr))
	}
	if updateErr == nil {
		updateErr = errors.New("UpdateSchedule returned success but target representation differs")
	}
	return ResearchTaskDefinitionEditSnapshotV3{},
		classifyTaskScheduleMutationError(operation, baseExpected.base.taskID, updateErr)
}

func researchTaskDefinitionEditDesiredScheduleV3(
	prepared PreparedResearchTaskDefinitionEditV3,
	phase string,
	base, target researchTaskScheduleExpectedV3,
) (*schedulepb.Schedule, error) {
	var expected researchTaskScheduleExpectedV3
	var paused bool
	var note string
	switch phase {
	case "base_paused":
		expected, paused, note = base, true, researchTaskDefinitionEditPausedNoteV3
	case "target_paused":
		expected, paused, note = target, true, researchTaskDefinitionEditPausedNoteV3
	case "target_final":
		expected = target
		paused = prepared.OriginalState == ResearchTaskDefinitionEditOriginalPausedV3
		if paused {
			note = researchTaskDefinitionEditPausedNoteV3
		} else {
			note = target.prepared.Schedule.Action.ActivationNote
		}
	default:
		return nil, errors.New("unsupported research V3 edit phase")
	}
	fingerprint := expected.base.fingerprint
	fingerprint.LifecyclePhase = taskScheduleV1PhaseActive
	schedule, err := expected.base.protoSchedule(fingerprint, paused, note)
	if err != nil {
		return nil, err
	}
	if err := formalizeResearchScheduleV3(schedule, expected); err != nil {
		return nil, err
	}
	return schedule, nil
}

func researchTaskDefinitionEditSourceExactV3(
	prepared PreparedResearchTaskDefinitionEditV3,
	phase string,
	base, target researchTaskScheduleExpectedV3,
	desc *workflowservice.DescribeScheduleResponse,
) bool {
	switch phase {
	case "base_paused":
		snapshot, err := verifyResearchTaskScheduleDescriptionV3(base, desc, phase)
		if err != nil {
			return false
		}
		if prepared.OriginalState == ResearchTaskDefinitionEditOriginalActiveV3 {
			return snapshot.State == TaskScheduleActiveVirginExact ||
				snapshot.State == TaskScheduleActiveUsedExact
		}
		return snapshot.State == TaskSchedulePausedUsedExact
	case "target_paused":
		if prepared.OriginalState == ResearchTaskDefinitionEditOriginalPausedV3 {
			snapshot, err := verifyResearchTaskScheduleDescriptionV3(base, desc, phase)
			return err == nil && snapshot.State == TaskSchedulePausedUsedExact
		}
		return researchTaskDefinitionEditScheduleMatchesV3(
			base, desc, true, researchTaskDefinitionEditPausedNoteV3)
	case "target_final":
		return researchTaskDefinitionEditScheduleMatchesV3(target, desc, true,
			researchTaskDefinitionEditPausedNoteV3)
	default:
		return false
	}
}

func researchTaskDefinitionEditDesiredSnapshotV3(
	prepared PreparedResearchTaskDefinitionEditV3,
	phase string,
	base, target researchTaskScheduleExpectedV3,
	desc *workflowservice.DescribeScheduleResponse,
) (ResearchTaskDefinitionEditSnapshotV3, bool) {
	var expected researchTaskScheduleExpectedV3
	var paused bool
	var note string
	switch phase {
	case "base_paused":
		if prepared.OriginalState == ResearchTaskDefinitionEditOriginalPausedV3 {
			snapshot, err := verifyResearchTaskScheduleDescriptionV3(base, desc, phase)
			if err == nil && snapshot.State == TaskSchedulePausedUsedExact {
				return researchTaskDefinitionEditSnapshotV3(
					prepared, phase, snapshot, true), true
			}
			return ResearchTaskDefinitionEditSnapshotV3{}, false
		}
		expected, paused, note = base, true, researchTaskDefinitionEditPausedNoteV3
	case "target_paused":
		expected, paused, note = target, true, researchTaskDefinitionEditPausedNoteV3
	case "target_final":
		expected = target
		paused = prepared.OriginalState == ResearchTaskDefinitionEditOriginalPausedV3
		if paused {
			note = researchTaskDefinitionEditPausedNoteV3
		} else {
			note = target.prepared.Schedule.Action.ActivationNote
		}
	default:
		return ResearchTaskDefinitionEditSnapshotV3{}, false
	}
	if !researchTaskDefinitionEditScheduleMatchesV3(expected, desc, paused, note) {
		return ResearchTaskDefinitionEditSnapshotV3{}, false
	}
	snapshot, err := verifyResearchTaskScheduleDescriptionV3(expected, desc, phase)
	if err != nil {
		return ResearchTaskDefinitionEditSnapshotV3{}, false
	}
	return researchTaskDefinitionEditSnapshotV3(prepared, phase, snapshot, paused), true
}

func researchTaskDefinitionEditScheduleMatchesV3(
	expected researchTaskScheduleExpectedV3,
	desc *workflowservice.DescribeScheduleResponse,
	paused bool,
	note string,
) bool {
	if desc == nil || desc.GetSchedule() == nil ||
		desc.GetSchedule().GetState().GetPaused() != paused ||
		desc.GetSchedule().GetState().GetNotes() != note {
		return false
	}
	_, err := verifyResearchTaskScheduleDescriptionV3(expected, desc, "research_edit_compare")
	return err == nil
}
