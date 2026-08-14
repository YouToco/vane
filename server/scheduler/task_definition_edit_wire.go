package scheduler

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/YouToco/vane/server/internal/strictjson"
)

const (
	maxPreparedTaskDefinitionEditWireBytes = 4 << 20
	maxTaskDefinitionEditSnapshotWireBytes = 16 << 10
)

// EncodePreparedTaskDefinitionEdit returns the canonical, fully validated
// C2b3-1 wire. It is a pure serialization boundary: it performs no Temporal
// I/O and grants no authority to mutate a Schedule.
func EncodePreparedTaskDefinitionEdit(prepared PreparedTaskDefinitionEdit) ([]byte, error) {
	prepared = clonePreparedTaskDefinitionEdit(prepared)
	if err := validatePreparedTaskDefinitionEdit(prepared); err != nil {
		return nil, invalidTaskDefinitionEditWire("encode prepared definition edit", err)
	}
	encoded, err := json.Marshal(prepared)
	if err != nil {
		return nil, invalidTaskDefinitionEditWire("encode prepared definition edit", err)
	}
	if len(encoded) == 0 || len(encoded) > maxPreparedTaskDefinitionEditWireBytes {
		return nil, invalidTaskDefinitionEditWire(
			"encode prepared definition edit",
			fmt.Errorf("wire size %d is outside the supported bound", len(encoded)),
		)
	}
	return encoded, nil
}

// DecodePreparedTaskDefinitionEdit accepts only the exact canonical C2b3-1
// wire. Unknown, duplicate, case-folded, escaped, missing, null, and
// non-canonical fields fail closed before a recovered operation can use them.
func DecodePreparedTaskDefinitionEdit(raw []byte) (PreparedTaskDefinitionEdit, error) {
	if len(raw) == 0 || len(raw) > maxPreparedTaskDefinitionEditWireBytes {
		return PreparedTaskDefinitionEdit{}, invalidTaskDefinitionEditWire(
			"decode prepared definition edit",
			fmt.Errorf("wire size %d is outside the supported bound", len(raw)),
		)
	}
	var prepared PreparedTaskDefinitionEdit
	if err := strictjson.DecodeExact(raw, &prepared); err != nil {
		return PreparedTaskDefinitionEdit{}, invalidTaskDefinitionEditWire(
			"decode prepared definition edit", err,
		)
	}
	canonical, err := EncodePreparedTaskDefinitionEdit(prepared)
	if err != nil {
		return PreparedTaskDefinitionEdit{}, err
	}
	if !bytes.Equal(canonical, raw) {
		return PreparedTaskDefinitionEdit{}, invalidTaskDefinitionEditWire(
			"decode prepared definition edit", errors.New("wire is not canonical"),
		)
	}
	return clonePreparedTaskDefinitionEdit(prepared), nil
}

// ValidatePreparedTaskDefinitionEditRequest binds both decoded Approved
// projections and every request identity to the exact prepared Temporal wire.
// This closes the gap between immutable definition digests and the separately
// frozen Schedule representations; it performs no remote read or write.
func ValidatePreparedTaskDefinitionEditRequest(
	prepared PreparedTaskDefinitionEdit,
	req TaskDefinitionEditRequest,
) error {
	prepared = clonePreparedTaskDefinitionEdit(prepared)
	req.Creation = clonePreparedTaskSchedule(req.Creation)
	req.Base = cloneTaskDefinitionEditDefinition(req.Base)
	req.Target = cloneTaskDefinitionEditDefinition(req.Target)
	if err := validatePreparedTaskDefinitionEdit(prepared); err != nil {
		return invalidTaskDefinitionEditWire("validate definition edit request", err)
	}
	if err := validateTaskDefinitionEditRequestIdentityV1(req); err != nil {
		return invalidTaskDefinitionEditWire("validate definition edit request", err)
	}
	baseProjectionDigest, err := digestTaskDefinitionEditProjectionV1(req.Base)
	if err != nil {
		return invalidTaskDefinitionEditWire("digest base definition projection", err)
	}
	targetProjectionDigest, err := digestTaskDefinitionEditProjectionV1(req.Target)
	if err != nil {
		return invalidTaskDefinitionEditWire("digest target definition projection", err)
	}
	preparedCreation, err := json.Marshal(prepared.Creation)
	if err != nil {
		return invalidTaskDefinitionEditWire(
			"validate definition edit request", fmt.Errorf("encode prepared creation: %w", err),
		)
	}
	requestCreation, err := json.Marshal(req.Creation)
	if err != nil {
		return invalidTaskDefinitionEditWire(
			"validate definition edit request", fmt.Errorf("encode requested creation: %w", err),
		)
	}
	if prepared.OperationID != req.OperationID || prepared.BaseHead != req.BaseHead ||
		prepared.TargetHead != req.TargetHead || prepared.OriginalState != req.OriginalState ||
		prepared.BaseProjectionDigest != baseProjectionDigest ||
		prepared.TargetProjectionDigest != targetProjectionDigest ||
		!bytes.Equal(preparedCreation, requestCreation) ||
		!taskDefinitionEditDefinitionMatchesRepresentation(
			prepared.BaseOriginal, req.Base,
		) ||
		!taskDefinitionEditDefinitionMatchesRepresentation(
			prepared.TargetFinal, req.Target,
		) {
		return invalidTaskDefinitionEditWire(
			"validate definition edit request",
			errors.New("request differs from the frozen definition edit"),
		)
	}
	return nil
}

// ValidatePreparedTaskDefinitionEditRequestForWrite is the proposal-sealing
// gate. It first proves the frozen identities/projections, then re-applies the
// current target writer policy and verifies that both prepared timings are the
// exact compiler outputs. Recovery must use the frozen-only validator above.
func ValidatePreparedTaskDefinitionEditRequestForWrite(
	prepared PreparedTaskDefinitionEdit,
	req TaskDefinitionEditRequest,
) error {
	return validatePreparedTaskDefinitionEditRequestForWrite(
		prepared, req, ValidateTaskScheduleSpec,
	)
}

func validatePreparedTaskDefinitionEditRequestForWrite(
	prepared PreparedTaskDefinitionEdit,
	req TaskDefinitionEditRequest,
	validateCurrentTarget func(ScheduleSpec) error,
) error {
	if err := ValidatePreparedTaskDefinitionEditRequest(prepared, req); err != nil {
		return err
	}
	if validateCurrentTarget == nil {
		return invalidTaskDefinitionEditWire(
			"validate definition edit current writer", errors.New("current target validator is unavailable"),
		)
	}
	if err := validateCurrentTarget(req.Target.Spec); err != nil {
		return invalidTaskDefinitionEditWire("validate definition edit current writer", err)
	}
	_, baseTiming, err := buildTaskDefinitionEditScheduleSpecV1(req.Base.Spec)
	if err != nil {
		return invalidTaskDefinitionEditWire("compile definition edit v1 base", err)
	}
	_, targetTiming, err := buildTaskScheduleSpec(req.Target.Spec)
	if err != nil {
		return invalidTaskDefinitionEditWire("compile definition edit current target", err)
	}
	if !preparedTaskScheduleTimingEqual(prepared.BaseOriginal.Timing, baseTiming) ||
		!preparedTaskScheduleTimingEqual(prepared.TargetFinal.Timing, targetTiming) {
		return invalidTaskDefinitionEditWire(
			"validate definition edit compiled timings",
			errors.New("prepared timing differs from the proposal-sealing compilers"),
		)
	}
	return nil
}

// EncodeTaskDefinitionEditBaseSnapshot returns the canonical proof observed by
// PrepareTaskDefinitionEdit. Only base_original is accepted here: later phase
// receipts belong to the fenced coordinator checkpoints in C2b3-2b.
func EncodeTaskDefinitionEditBaseSnapshot(
	prepared PreparedTaskDefinitionEdit,
	snapshot TaskDefinitionEditSnapshot,
) ([]byte, error) {
	prepared = clonePreparedTaskDefinitionEdit(prepared)
	if err := validatePreparedTaskDefinitionEdit(prepared); err != nil {
		return nil, invalidTaskDefinitionEditWire("encode definition edit base snapshot", err)
	}
	if err := validateFrozenTaskDefinitionEditBaseSnapshot(prepared, snapshot); err != nil {
		return nil, invalidTaskDefinitionEditWire("encode definition edit base snapshot", err)
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		return nil, invalidTaskDefinitionEditWire("encode definition edit base snapshot", err)
	}
	if len(encoded) == 0 || len(encoded) > maxTaskDefinitionEditSnapshotWireBytes {
		return nil, invalidTaskDefinitionEditWire(
			"encode definition edit base snapshot",
			fmt.Errorf("wire size %d is outside the supported bound", len(encoded)),
		)
	}
	return encoded, nil
}

// DecodeTaskDefinitionEditBaseSnapshot strictly recovers and rebinds a frozen
// base snapshot to its prepared edit.
func DecodeTaskDefinitionEditBaseSnapshot(
	prepared PreparedTaskDefinitionEdit,
	raw []byte,
) (TaskDefinitionEditSnapshot, error) {
	if len(raw) == 0 || len(raw) > maxTaskDefinitionEditSnapshotWireBytes {
		return TaskDefinitionEditSnapshot{}, invalidTaskDefinitionEditWire(
			"decode definition edit base snapshot",
			fmt.Errorf("wire size %d is outside the supported bound", len(raw)),
		)
	}
	var snapshot TaskDefinitionEditSnapshot
	if err := strictjson.DecodeExact(raw, &snapshot); err != nil {
		return TaskDefinitionEditSnapshot{}, invalidTaskDefinitionEditWire(
			"decode definition edit base snapshot", err,
		)
	}
	canonical, err := EncodeTaskDefinitionEditBaseSnapshot(prepared, snapshot)
	if err != nil {
		return TaskDefinitionEditSnapshot{}, err
	}
	if !bytes.Equal(canonical, raw) {
		return TaskDefinitionEditSnapshot{}, invalidTaskDefinitionEditWire(
			"decode definition edit base snapshot", errors.New("wire is not canonical"),
		)
	}
	return snapshot, nil
}

// EncodeTaskDefinitionEditPhaseSnapshot canonicalizes a post-seal Temporal
// observation and binds it to exactly one representation in the frozen edit.
// Unlike EncodeTaskDefinitionEditBaseSnapshot, this codec intentionally does
// not require BaseRevision: later observations carry the conflict token for
// the next fenced remote transition.
func EncodeTaskDefinitionEditPhaseSnapshot(
	prepared PreparedTaskDefinitionEdit,
	snapshot TaskDefinitionEditSnapshot,
) ([]byte, error) {
	prepared = clonePreparedTaskDefinitionEdit(prepared)
	if err := validatePreparedTaskDefinitionEdit(prepared); err != nil {
		return nil, invalidTaskDefinitionEditWire(
			"encode definition edit phase snapshot", err,
		)
	}
	representation, err := taskDefinitionEditPhaseRepresentation(
		prepared, snapshot.Phase,
	)
	if err != nil {
		return nil, invalidTaskDefinitionEditWire(
			"encode definition edit phase snapshot", err,
		)
	}
	if err := validateTaskDefinitionEditSnapshot(
		prepared, snapshot, snapshot.Phase, representation,
	); err != nil {
		return nil, invalidTaskDefinitionEditWire(
			"encode definition edit phase snapshot", err,
		)
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		return nil, invalidTaskDefinitionEditWire(
			"encode definition edit phase snapshot", err,
		)
	}
	if len(encoded) == 0 || len(encoded) > maxTaskDefinitionEditSnapshotWireBytes {
		return nil, invalidTaskDefinitionEditWire(
			"encode definition edit phase snapshot",
			fmt.Errorf("wire size %d is outside the supported bound", len(encoded)),
		)
	}
	return encoded, nil
}

// DecodeTaskDefinitionEditPhaseSnapshot is the retained reader used by the
// durable Store. It rejects non-canonical JSON and observations which do not
// match the exact TaskID, request digest, phase representation, or revision
// shape frozen by PreparedTaskDefinitionEdit.
func DecodeTaskDefinitionEditPhaseSnapshot(
	prepared PreparedTaskDefinitionEdit,
	raw []byte,
) (TaskDefinitionEditSnapshot, error) {
	if len(raw) == 0 || len(raw) > maxTaskDefinitionEditSnapshotWireBytes {
		return TaskDefinitionEditSnapshot{}, invalidTaskDefinitionEditWire(
			"decode definition edit phase snapshot",
			fmt.Errorf("wire size %d is outside the supported bound", len(raw)),
		)
	}
	var snapshot TaskDefinitionEditSnapshot
	if err := strictjson.DecodeExact(raw, &snapshot); err != nil {
		return TaskDefinitionEditSnapshot{}, invalidTaskDefinitionEditWire(
			"decode definition edit phase snapshot", err,
		)
	}
	canonical, err := EncodeTaskDefinitionEditPhaseSnapshot(prepared, snapshot)
	if err != nil {
		return TaskDefinitionEditSnapshot{}, err
	}
	if !bytes.Equal(canonical, raw) {
		return TaskDefinitionEditSnapshot{}, invalidTaskDefinitionEditWire(
			"decode definition edit phase snapshot",
			errors.New("wire is not canonical"),
		)
	}
	return snapshot, nil
}

func taskDefinitionEditPhaseRepresentation(
	prepared PreparedTaskDefinitionEdit,
	phase TaskDefinitionEditPhase,
) (PreparedTaskDefinitionEditSchedule, error) {
	switch phase {
	case TaskDefinitionEditPhaseBaseOriginal:
		return prepared.BaseOriginal, nil
	case TaskDefinitionEditPhaseBasePaused:
		return prepared.BasePaused, nil
	case TaskDefinitionEditPhaseTargetPaused:
		return prepared.TargetPaused, nil
	case TaskDefinitionEditPhaseTargetFinal:
		return prepared.TargetFinal, nil
	default:
		return PreparedTaskDefinitionEditSchedule{}, errors.New(
			"definition edit phase snapshot has an unsupported phase",
		)
	}
}

func validateFrozenTaskDefinitionEditBaseSnapshot(
	prepared PreparedTaskDefinitionEdit,
	snapshot TaskDefinitionEditSnapshot,
) error {
	if err := validateTaskDefinitionEditSnapshot(
		prepared,
		snapshot,
		TaskDefinitionEditPhaseBaseOriginal,
		prepared.BaseOriginal,
	); err != nil {
		return err
	}
	if snapshot.Revision != prepared.BaseRevision {
		return errors.New("base snapshot revision differs from the prepared base revision")
	}
	return nil
}

func taskDefinitionEditDefinitionMatchesRepresentation(
	representation PreparedTaskDefinitionEditSchedule,
	definition TaskDefinitionEditDefinition,
) bool {
	params := representation.Action.Params
	return params.NLDesc == definition.NLDescription &&
		params.Scope.TopN == definition.Scope.TopN &&
		slices.Equal(params.Scope.SourceIDs, definition.Scope.SourceIDs)
}

func invalidTaskDefinitionEditWire(operation string, cause error) error {
	return newTaskScheduleError(TaskScheduleErrorInvalid, operation, "", cause)
}
