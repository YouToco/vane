package store

import (
	"bytes"
	"encoding/json"
	"errors"

	"github.com/YouToco/vane/server/internal/strictjson"
	"github.com/YouToco/vane/server/taskstate"
)

const (
	taskRunSnapshotShadowPayloadSchemaV2 = "vane.task-run-snapshot-shadow/v2"
	maxTaskRunSnapshotShadowPayloadBytes = 5 << 20
)

type taskRunSnapshotShadowIdentityV2 struct {
	TenantID           int64  `json:"tenant_id"`
	UserID             int64  `json:"user_id"`
	TaskID             string `json:"task_id"`
	TemporalWorkflowID string `json:"temporal_workflow_id"`
	TemporalRunID      string `json:"temporal_run_id"`
}

type taskRunSnapshotShadowLegacyV2 struct {
	SnapshotID    int64           `json:"snapshot_id"`
	PayloadDigest string          `json:"payload_digest"`
	Payload       json.RawMessage `json:"payload"`
}

type taskRunSnapshotShadowApprovedV2 struct {
	Version       int64           `json:"version"`
	Digest        string          `json:"digest"`
	SchemaVersion string          `json:"schema_version"`
	Payload       json.RawMessage `json:"payload"`
}

type taskRunSnapshotShadowAdaptiveV2 struct {
	Version                        int64           `json:"version"`
	Digest                         string          `json:"digest"`
	SchemaVersion                  string          `json:"schema_version"`
	Payload                        json.RawMessage `json:"payload"`
	BasisDefinitionVersion         int64           `json:"basis_definition_version"`
	BasisDefinitionDigest          string          `json:"basis_definition_digest"`
	LastKnownGoodDefinitionVersion *int64          `json:"last_known_good_definition_version"`
}

type taskRunSnapshotShadowPayloadV2 struct {
	SchemaVersion string                           `json:"schema_version"`
	Status        TaskRunSnapshotShadowStatus      `json:"status"`
	Identity      taskRunSnapshotShadowIdentityV2  `json:"identity"`
	Legacy        taskRunSnapshotShadowLegacyV2    `json:"legacy"`
	Approved      *taskRunSnapshotShadowApprovedV2 `json:"approved"`
	Adaptive      *taskRunSnapshotShadowAdaptiveV2 `json:"adaptive"`
}

func encodeTaskRunSnapshotShadowPayloadV2(
	payload taskRunSnapshotShadowPayloadV2,
) ([]byte, string, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, "", err
	}
	decoded, canonical, err := readTaskRunSnapshotShadowPayloadV2(raw)
	if err != nil || decoded.Status != payload.Status {
		return nil, "", errors.New("task run snapshot v2 shadow payload is invalid")
	}
	return canonical, sha256Hex(canonical), nil
}

func readTaskRunSnapshotShadowPayloadV2(
	raw []byte,
) (taskRunSnapshotShadowPayloadV2, []byte, error) {
	if len(raw) == 0 || len(raw) > maxTaskRunSnapshotShadowPayloadBytes ||
		strictjson.Validate(raw) != nil {
		return taskRunSnapshotShadowPayloadV2{}, nil,
			errors.New("task run snapshot v2 shadow payload is invalid")
	}
	var payload taskRunSnapshotShadowPayloadV2
	if err := strictjson.DecodeExact(raw, &payload); err != nil {
		return taskRunSnapshotShadowPayloadV2{}, nil,
			errors.New("task run snapshot v2 shadow payload is invalid")
	}
	if payload.SchemaVersion != taskRunSnapshotShadowPayloadSchemaV2 ||
		!payload.Status.valid() ||
		payload.Identity.TenantID <= 0 || payload.Identity.UserID <= 0 ||
		!validTaskRunTaskID(payload.Identity.TaskID) ||
		!validTaskRunReference(payload.Identity.TemporalWorkflowID) ||
		!validTaskRunReference(payload.Identity.TemporalRunID) ||
		payload.Legacy.SnapshotID <= 0 ||
		!validTaskStateDigest(payload.Legacy.PayloadDigest) {
		return taskRunSnapshotShadowPayloadV2{}, nil,
			errors.New("task run snapshot v2 shadow envelope is invalid")
	}
	legacy, err := readTaskRunSnapshotPayload(payload.Legacy.Payload)
	if err != nil || !bytes.Equal(legacy.Canonical, payload.Legacy.Payload) ||
		!constantTimeDigestEqual(sha256Hex(payload.Legacy.Payload),
			payload.Legacy.PayloadDigest) ||
		legacy.Payload == nil ||
		legacy.Payload.TenantID != payload.Identity.TenantID ||
		legacy.Payload.UserID != payload.Identity.UserID ||
		legacy.Payload.TaskID != payload.Identity.TaskID {
		return taskRunSnapshotShadowPayloadV2{}, nil,
			errors.New("task run snapshot v2 legacy payload is invalid")
	}
	if payload.Status == TaskRunSnapshotShadowHeadless {
		if payload.Approved != nil || payload.Adaptive != nil {
			return taskRunSnapshotShadowPayloadV2{}, nil,
				errors.New("headless task run shadow carries state")
		}
	} else if err := validateTaskRunSnapshotShadowApprovedV2(
		payload.Approved, payload.Identity); err != nil {
		return taskRunSnapshotShadowPayloadV2{}, nil, err
	}
	if payload.Adaptive != nil {
		if payload.Approved == nil || payload.Adaptive.Version <= 0 ||
			!validTaskStateDigest(payload.Adaptive.Digest) ||
			payload.Adaptive.SchemaVersion == "" ||
			payload.Adaptive.BasisDefinitionVersion <= 0 ||
			!validTaskStateDigest(payload.Adaptive.BasisDefinitionDigest) ||
			!constantTimeDigestEqual(sha256Hex(payload.Adaptive.Payload),
				payload.Adaptive.Digest) {
			return taskRunSnapshotShadowPayloadV2{}, nil,
				errors.New("task run snapshot v2 adaptive state is invalid")
		}
		state, err := taskstate.DecodeAdaptiveStateV1(payload.Adaptive.Payload)
		if err != nil {
			return taskRunSnapshotShadowPayloadV2{}, nil,
				errors.New("task run snapshot v2 adaptive state is invalid")
		}
		reencoded, err := taskstate.EncodeAdaptiveStateV1(state)
		if err != nil || !bytes.Equal(reencoded, payload.Adaptive.Payload) ||
			state.SchemaVersion != payload.Adaptive.SchemaVersion ||
			state.TenantID != payload.Identity.TenantID ||
			state.UserID != payload.Identity.UserID ||
			state.TaskID != payload.Identity.TaskID {
			return taskRunSnapshotShadowPayloadV2{}, nil,
				errors.New("task run snapshot v2 adaptive state is not canonical")
		}
	}
	if err := validateTaskRunSnapshotShadowStatusV2(
		payload, legacy.Payload); err != nil {
		return taskRunSnapshotShadowPayloadV2{}, nil, err
	}
	canonical, err := json.Marshal(payload)
	if err != nil || !bytes.Equal(canonical, raw) {
		return taskRunSnapshotShadowPayloadV2{}, nil,
			errors.New("task run snapshot v2 shadow payload is not canonical")
	}
	return payload, canonical, nil
}

func validateTaskRunSnapshotShadowStatusV2(
	payload taskRunSnapshotShadowPayloadV2,
	legacy *taskRunSnapshotPayloadV1,
) error {
	var definition *taskstate.ApprovedDefinitionV1
	if payload.Approved != nil {
		decoded, err := taskstate.DecodeApprovedDefinitionV1(payload.Approved.Payload)
		if err != nil {
			return errors.New("task run shadow approved definition is invalid")
		}
		definition = &decoded
	}
	expected := classifyTaskRunSnapshotShadowV2(
		legacy, definition, payload.Approved, payload.Adaptive)
	if payload.Status != expected {
		return errors.New("task run shadow status differs from frozen inputs")
	}
	return nil
}

func validateTaskRunSnapshotShadowApprovedV2(
	approved *taskRunSnapshotShadowApprovedV2,
	identity taskRunSnapshotShadowIdentityV2,
) error {
	if approved == nil || approved.Version <= 0 ||
		!validTaskStateDigest(approved.Digest) ||
		approved.SchemaVersion == "" ||
		!constantTimeDigestEqual(sha256Hex(approved.Payload), approved.Digest) {
		return errors.New("task run snapshot v2 approved definition is invalid")
	}
	definition, err := taskstate.DecodeApprovedDefinitionV1(approved.Payload)
	if err != nil {
		return errors.New("task run snapshot v2 approved definition is invalid")
	}
	reencoded, err := taskstate.EncodeApprovedDefinitionV1(definition)
	if err != nil || !bytes.Equal(reencoded, approved.Payload) ||
		definition.SchemaVersion != approved.SchemaVersion ||
		definition.TenantID != identity.TenantID ||
		definition.UserID != identity.UserID ||
		definition.TaskID != identity.TaskID {
		return errors.New("task run snapshot v2 approved definition is not canonical")
	}
	return nil
}
