package task

import (
	"bytes"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/YouToco/vane/internal/strictjson"
	"github.com/YouToco/vane/observation"
	"github.com/YouToco/vane/scheduler"
	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
	"github.com/YouToco/vane/workflow"
)

const (
	taskDefinitionEditProposalVersion     = "vane.task-definition-edit-proposal/v2"
	maxTaskDefinitionEditProposalBytes    = 64 << 10
	maxTaskDefinitionEditReferenceBytes   = 1024
	maxTaskDefinitionEditOperationIDBytes = 512
	maxTaskDefinitionEditTaskIDBytes      = 255
	maxTaskDefinitionEditDefinitionBytes  = 2 << 20
	maxTaskDefinitionEditPreparedBytes    = 4 << 20
	maxTaskDefinitionEditBaseSnapshotSize = 16 << 10
)

var ErrDefinitionEditProposalInvalid = errors.New(
	"task: frozen definition edit proposal is invalid",
)

// TaskDefinitionEditProposalActorV2 is the authenticated internal principal.
// Provider callback identifiers never substitute for this scope.
type TaskDefinitionEditProposalActorV2 struct {
	TenantID int64 `json:"tenant_id"`
	UserID   int64 `json:"user_id"`
}

// TaskDefinitionEditProposalTargetV2 is the immutable resource scope. V2 is
// owner-only, but actor and target remain distinct fields so a later team
// protocol cannot reinterpret old bytes.
type TaskDefinitionEditProposalTargetV2 struct {
	TenantID int64  `json:"tenant_id"`
	UserID   int64  `json:"user_id"`
	TaskID   string `json:"task_id"`
}

// TaskDefinitionEditOriginalStatusV2 is the frozen database status spelling
// carried by proposal/v2. Current ScheduleStatus names are translated only at
// seal time so a future current enum rename cannot strand historical bytes.
type TaskDefinitionEditOriginalStatusV2 string

const (
	TaskDefinitionEditOriginalStatusV2Active TaskDefinitionEditOriginalStatusV2 = "active"
	TaskDefinitionEditOriginalStatusV2Paused TaskDefinitionEditOriginalStatusV2 = "paused"
)

// TaskDefinitionEditProposalV2 is the small canonical operation envelope. The
// exact target definition, prepared Temporal edit, and base snapshot remain
// separate BYTEA checkpoints; their digests make this envelope bind all three
// without duplicating multi-megabyte payloads.
type TaskDefinitionEditProposalV2 struct {
	WireVersion            string                             `json:"wire_version"`
	OperationID            string                             `json:"operation_id"`
	OperationRef           string                             `json:"operation_ref"`
	Actor                  TaskDefinitionEditProposalActorV2  `json:"actor"`
	Target                 TaskDefinitionEditProposalTargetV2 `json:"target"`
	SessionID              int64                              `json:"session_id"`
	ExpiresAtUnixMicros    int64                              `json:"expires_at_unix_micros"`
	OriginalStatus         TaskDefinitionEditOriginalStatusV2 `json:"original_status"`
	BaseHead               scheduler.TaskDefinitionEditHead   `json:"base_head"`
	TargetHead             scheduler.TaskDefinitionEditHead   `json:"target_head"`
	TargetDefinitionDigest string                             `json:"target_definition_digest"`
	PreparedEditDigest     string                             `json:"prepared_edit_digest"`
	BaseSnapshotDigest     string                             `json:"base_snapshot_digest"`
}

// BuildTaskDefinitionEditProposalInput contains every value that must be
// cross-checked before an edit operation may be inserted. ExpiresAt is
// converted to integer microseconds; no process-local clock participates in
// the durable bytes.
type BuildTaskDefinitionEditProposalInput struct {
	OperationID      string
	OperationRef     string
	ActorTenantID    int64
	ActorUserID      int64
	TargetTenantID   int64
	TargetUserID     int64
	TaskID           string
	SessionID        int64
	ExpiresAt        time.Time
	OriginalStatus   types.ScheduleStatus
	BaseHead         scheduler.TaskDefinitionEditHead
	TargetHead       scheduler.TaskDefinitionEditHead
	BaseDefinition   taskstate.ApprovedDefinitionV1
	TargetDefinition taskstate.ApprovedDefinitionV1
	PreparedEdit     scheduler.PreparedTaskDefinitionEdit
	BaseSnapshot     scheduler.TaskDefinitionEditSnapshot
}

// FrozenTaskDefinitionEditProposal is the validated operation checkpoint set.
// Byte slices are exact canonical database payloads; typed values are decoded
// from those same bytes rather than retained aliases of caller-owned memory.
type FrozenTaskDefinitionEditProposal struct {
	Proposal              TaskDefinitionEditProposalV2
	CanonicalProposal     []byte
	ProposalDigest        string
	BaseDefinition        taskstate.ApprovedDefinitionV1
	BaseDefinitionBytes   []byte
	TargetDefinition      taskstate.ApprovedDefinitionV1
	TargetDefinitionBytes []byte
	PreparedEdit          scheduler.PreparedTaskDefinitionEdit
	PreparedEditBytes     []byte
	BaseSnapshot          scheduler.TaskDefinitionEditSnapshot
	BaseSnapshotBytes     []byte
}

// BuildFrozenTaskDefinitionEditProposal validates both immutable Approved
// definitions, the complete prepared Temporal wire, and the observed base
// snapshot before producing any bytes that can be persisted or summarized.
func BuildFrozenTaskDefinitionEditProposal(
	in BuildTaskDefinitionEditProposalInput,
) (FrozenTaskDefinitionEditProposal, error) {
	baseBytes, err := taskstate.EncodeApprovedDefinitionV1(in.BaseDefinition)
	if err != nil {
		return FrozenTaskDefinitionEditProposal{}, invalidDefinitionEditProposal(
			"encode base definition", err,
		)
	}
	baseDefinition, err := taskstate.DecodeApprovedDefinitionV1(baseBytes)
	if err != nil {
		return FrozenTaskDefinitionEditProposal{}, invalidDefinitionEditProposal(
			"decode canonical base definition", err,
		)
	}
	targetBytes, err := taskstate.EncodeApprovedDefinitionV1(in.TargetDefinition)
	if err != nil {
		return FrozenTaskDefinitionEditProposal{}, invalidDefinitionEditProposal(
			"encode target definition", err,
		)
	}
	targetDefinition, err := taskstate.DecodeApprovedDefinitionV1(targetBytes)
	if err != nil {
		return FrozenTaskDefinitionEditProposal{}, invalidDefinitionEditProposal(
			"decode canonical target definition", err,
		)
	}
	// Only proposal sealing consults the current source materialization
	// registry. Recovery below uses the frozen V1 reader so a future registry
	// change cannot strand an already authenticated operation.
	if err := validateDefinitionEditTargetPolicy(targetDefinition, true); err != nil {
		return FrozenTaskDefinitionEditProposal{}, err
	}
	preparedBytes, err := scheduler.EncodePreparedTaskDefinitionEdit(in.PreparedEdit)
	if err != nil {
		return FrozenTaskDefinitionEditProposal{}, invalidDefinitionEditProposal(
			"encode prepared edit", err,
		)
	}
	prepared, err := scheduler.DecodePreparedTaskDefinitionEdit(preparedBytes)
	if err != nil {
		return FrozenTaskDefinitionEditProposal{}, invalidDefinitionEditProposal(
			"decode canonical prepared edit", err,
		)
	}
	baseProjection, err := definitionEditSchedulerProjection(baseDefinition)
	if err != nil {
		return FrozenTaskDefinitionEditProposal{}, invalidDefinitionEditProposal(
			"encode base projection", err,
		)
	}
	targetProjection, err := definitionEditSchedulerProjection(targetDefinition)
	if err != nil {
		return FrozenTaskDefinitionEditProposal{}, invalidDefinitionEditProposal(
			"encode target projection", err,
		)
	}
	originalStatus, originalState, err := definitionEditOriginalStatusForWrite(in.OriginalStatus)
	if err != nil {
		return FrozenTaskDefinitionEditProposal{}, invalidDefinitionEditProposal(
			"encode original status", err,
		)
	}
	if err := scheduler.ValidatePreparedTaskDefinitionEditRequestForWrite(
		prepared,
		scheduler.TaskDefinitionEditRequest{
			OperationID: in.OperationID, Creation: prepared.Creation,
			BaseHead: in.BaseHead, TargetHead: in.TargetHead,
			OriginalState: originalState, Base: baseProjection, Target: targetProjection,
		},
	); err != nil {
		return FrozenTaskDefinitionEditProposal{}, invalidDefinitionEditProposal(
			"validate prepared edit current writer", err,
		)
	}
	baseSnapshotBytes, err := scheduler.EncodeTaskDefinitionEditBaseSnapshot(
		prepared, in.BaseSnapshot,
	)
	if err != nil {
		return FrozenTaskDefinitionEditProposal{}, invalidDefinitionEditProposal(
			"encode base snapshot", err,
		)
	}
	proposal := TaskDefinitionEditProposalV2{
		WireVersion:  taskDefinitionEditProposalVersion,
		OperationID:  in.OperationID,
		OperationRef: in.OperationRef,
		Actor: TaskDefinitionEditProposalActorV2{
			TenantID: in.ActorTenantID,
			UserID:   in.ActorUserID,
		},
		Target: TaskDefinitionEditProposalTargetV2{
			TenantID: in.TargetTenantID,
			UserID:   in.TargetUserID,
			TaskID:   in.TaskID,
		},
		SessionID:              in.SessionID,
		ExpiresAtUnixMicros:    in.ExpiresAt.UnixMicro(),
		OriginalStatus:         originalStatus,
		BaseHead:               in.BaseHead,
		TargetHead:             in.TargetHead,
		TargetDefinitionDigest: sha256Hex(targetBytes),
		PreparedEditDigest:     sha256Hex(preparedBytes),
		BaseSnapshotDigest:     sha256Hex(baseSnapshotBytes),
	}
	canonical, err := json.Marshal(proposal)
	if err != nil {
		return FrozenTaskDefinitionEditProposal{}, invalidDefinitionEditProposal(
			"encode proposal", err,
		)
	}
	return DecodeFrozenTaskDefinitionEditProposal(
		canonical, baseBytes, targetBytes, preparedBytes, baseSnapshotBytes,
	)
}

// DecodeFrozenTaskDefinitionEditProposal strictly decodes the five exact
// operation checkpoints. The base definition bytes are stored with the
// operation itself so deleting the mutable task and its Approved history cannot
// strand recovery; callers may separately corroborate its original provenance.
func DecodeFrozenTaskDefinitionEditProposal(
	canonicalProposal []byte,
	baseDefinitionBytes []byte,
	targetDefinitionBytes []byte,
	preparedEditBytes []byte,
	baseSnapshotBytes []byte,
) (FrozenTaskDefinitionEditProposal, error) {
	if err := validDefinitionEditProposalSize(
		"proposal", canonicalProposal, maxTaskDefinitionEditProposalBytes,
	); err != nil {
		return FrozenTaskDefinitionEditProposal{}, err
	}
	if err := validDefinitionEditProposalSize(
		"base definition", baseDefinitionBytes, maxTaskDefinitionEditDefinitionBytes,
	); err != nil {
		return FrozenTaskDefinitionEditProposal{}, err
	}
	if err := validDefinitionEditProposalSize(
		"target definition", targetDefinitionBytes, maxTaskDefinitionEditDefinitionBytes,
	); err != nil {
		return FrozenTaskDefinitionEditProposal{}, err
	}
	if err := validDefinitionEditProposalSize(
		"prepared edit", preparedEditBytes, maxTaskDefinitionEditPreparedBytes,
	); err != nil {
		return FrozenTaskDefinitionEditProposal{}, err
	}
	if err := validDefinitionEditProposalSize(
		"base snapshot", baseSnapshotBytes, maxTaskDefinitionEditBaseSnapshotSize,
	); err != nil {
		return FrozenTaskDefinitionEditProposal{}, err
	}

	var proposal TaskDefinitionEditProposalV2
	if err := strictjson.DecodeExact(canonicalProposal, &proposal); err != nil {
		return FrozenTaskDefinitionEditProposal{}, invalidDefinitionEditProposal(
			"decode proposal", err,
		)
	}
	canonical, err := json.Marshal(proposal)
	if err != nil || !bytes.Equal(canonical, canonicalProposal) {
		return FrozenTaskDefinitionEditProposal{}, invalidDefinitionEditProposal(
			"decode proposal", errors.New("proposal bytes are not canonical"),
		)
	}

	targetDefinition, err := taskstate.DecodeApprovedDefinitionV1(targetDefinitionBytes)
	if err != nil {
		return FrozenTaskDefinitionEditProposal{}, invalidDefinitionEditProposal(
			"decode target definition", err,
		)
	}
	canonicalTarget, err := taskstate.EncodeApprovedDefinitionV1(targetDefinition)
	if err != nil || !bytes.Equal(canonicalTarget, targetDefinitionBytes) {
		return FrozenTaskDefinitionEditProposal{}, invalidDefinitionEditProposal(
			"decode target definition", errors.New("target definition bytes are not canonical"),
		)
	}
	baseDefinition, err := taskstate.DecodeApprovedDefinitionV1(baseDefinitionBytes)
	if err != nil {
		return FrozenTaskDefinitionEditProposal{}, invalidDefinitionEditProposal(
			"decode base definition", err,
		)
	}
	canonicalBase, err := taskstate.EncodeApprovedDefinitionV1(baseDefinition)
	if err != nil || !bytes.Equal(canonicalBase, baseDefinitionBytes) {
		return FrozenTaskDefinitionEditProposal{}, invalidDefinitionEditProposal(
			"decode base definition", errors.New("base definition bytes are not canonical"),
		)
	}
	prepared, err := scheduler.DecodePreparedTaskDefinitionEdit(preparedEditBytes)
	if err != nil {
		return FrozenTaskDefinitionEditProposal{}, invalidDefinitionEditProposal(
			"decode prepared edit", err,
		)
	}
	baseSnapshot, err := scheduler.DecodeTaskDefinitionEditBaseSnapshot(
		prepared, baseSnapshotBytes,
	)
	if err != nil {
		return FrozenTaskDefinitionEditProposal{}, invalidDefinitionEditProposal(
			"decode base snapshot", err,
		)
	}

	frozen := FrozenTaskDefinitionEditProposal{
		Proposal:              proposal,
		CanonicalProposal:     bytes.Clone(canonicalProposal),
		ProposalDigest:        sha256Hex(canonicalProposal),
		BaseDefinition:        baseDefinition,
		BaseDefinitionBytes:   bytes.Clone(baseDefinitionBytes),
		TargetDefinition:      targetDefinition,
		TargetDefinitionBytes: bytes.Clone(targetDefinitionBytes),
		PreparedEdit:          prepared,
		PreparedEditBytes:     bytes.Clone(preparedEditBytes),
		BaseSnapshot:          baseSnapshot,
		BaseSnapshotBytes:     bytes.Clone(baseSnapshotBytes),
	}
	if err := validateFrozenTaskDefinitionEditProposal(frozen); err != nil {
		return FrozenTaskDefinitionEditProposal{}, err
	}
	return frozen, nil
}

func validateFrozenTaskDefinitionEditProposal(
	frozen FrozenTaskDefinitionEditProposal,
) error {
	proposal := frozen.Proposal
	if proposal.WireVersion != taskDefinitionEditProposalVersion ||
		!validTaskDefinitionEditIdentifier(proposal.OperationID, maxTaskDefinitionEditOperationIDBytes) ||
		!validTaskDefinitionEditIdentifier(proposal.OperationRef, maxTaskDefinitionEditReferenceBytes) ||
		proposal.Actor.TenantID <= 0 || proposal.Actor.UserID <= 0 ||
		proposal.Target.TenantID <= 0 || proposal.Target.UserID <= 0 ||
		!validTaskDefinitionEditIdentifier(proposal.Target.TaskID, maxTaskDefinitionEditTaskIDBytes) ||
		proposal.SessionID <= 0 || proposal.ExpiresAtUnixMicros <= 0 {
		return invalidDefinitionEditProposal("validate proposal identity", errors.New("identity or expiry is invalid"))
	}
	if proposal.Actor.TenantID != proposal.Target.TenantID ||
		proposal.Actor.UserID != proposal.Target.UserID {
		return invalidDefinitionEditProposal("validate proposal actor", errors.New("v1 actor is not the target owner"))
	}
	originalState, err := definitionEditOriginalState(proposal.OriginalStatus)
	if err != nil {
		return invalidDefinitionEditProposal("validate original status", err)
	}
	if proposal.BaseHead.Version <= 0 || proposal.BaseHead.Version == math.MaxInt64 ||
		proposal.TargetHead.Version != proposal.BaseHead.Version+1 ||
		!validLowerSHA256(proposal.BaseHead.Digest) ||
		!validLowerSHA256(proposal.TargetHead.Digest) {
		return invalidDefinitionEditProposal("validate proposal heads", errors.New("definition heads are invalid"))
	}
	if !validLowerSHA256(proposal.TargetDefinitionDigest) ||
		!validLowerSHA256(proposal.PreparedEditDigest) ||
		!validLowerSHA256(proposal.BaseSnapshotDigest) ||
		!definitionEditDigestEqual(proposal.TargetDefinitionDigest, sha256Hex(frozen.TargetDefinitionBytes)) ||
		!definitionEditDigestEqual(proposal.PreparedEditDigest, sha256Hex(frozen.PreparedEditBytes)) ||
		!definitionEditDigestEqual(proposal.BaseSnapshotDigest, sha256Hex(frozen.BaseSnapshotBytes)) ||
		!definitionEditDigestEqual(proposal.TargetHead.Digest, proposal.TargetDefinitionDigest) {
		return invalidDefinitionEditProposal("validate proposal digests", errors.New("checkpoint digest differs"))
	}

	baseBytes, err := taskstate.EncodeApprovedDefinitionV1(frozen.BaseDefinition)
	if err != nil {
		return invalidDefinitionEditProposal("validate base definition", err)
	}
	baseDefinition, err := taskstate.DecodeApprovedDefinitionV1(baseBytes)
	if err != nil {
		return invalidDefinitionEditProposal("decode canonical base definition", err)
	}
	if !bytes.Equal(baseBytes, frozen.BaseDefinitionBytes) ||
		!definitionEditDigestEqual(proposal.BaseHead.Digest, sha256Hex(baseBytes)) {
		return invalidDefinitionEditProposal("validate base definition", errors.New("base head digest differs"))
	}
	if baseDefinition.ExecutionMode != types.ExecutionModeCompiled {
		return invalidDefinitionEditProposal("validate base definition", errors.New("base definition is not compiled"))
	}
	if err := validateDefinitionEditTargetPolicy(frozen.TargetDefinition, false); err != nil {
		return err
	}
	for _, scoped := range []struct {
		name       string
		definition taskstate.ApprovedDefinitionV1
	}{
		{name: "base", definition: baseDefinition},
		{name: "target", definition: frozen.TargetDefinition},
	} {
		name, definition := scoped.name, scoped.definition
		if definition.TenantID != proposal.Target.TenantID ||
			definition.UserID != proposal.Target.UserID || definition.TaskID != proposal.Target.TaskID {
			return invalidDefinitionEditProposal(
				"validate "+name+" definition scope", errors.New("definition scope differs"),
			)
		}
	}
	if frozen.PreparedEdit.OperationID != proposal.OperationID ||
		frozen.PreparedEdit.Creation.TenantID != proposal.Target.TenantID ||
		frozen.PreparedEdit.Creation.UserID != proposal.Target.UserID ||
		frozen.PreparedEdit.Creation.TaskID != proposal.Target.TaskID ||
		frozen.PreparedEdit.BaseHead != proposal.BaseHead ||
		frozen.PreparedEdit.TargetHead != proposal.TargetHead ||
		frozen.PreparedEdit.OriginalState != originalState {
		return invalidDefinitionEditProposal("validate prepared edit scope", errors.New("prepared edit differs"))
	}
	baseProjection, err := definitionEditSchedulerProjection(baseDefinition)
	if err != nil {
		return invalidDefinitionEditProposal("decode base projection", err)
	}
	targetProjection, err := definitionEditSchedulerProjection(frozen.TargetDefinition)
	if err != nil {
		return invalidDefinitionEditProposal("decode target projection", err)
	}
	if err := scheduler.ValidatePreparedTaskDefinitionEditRequest(
		frozen.PreparedEdit,
		scheduler.TaskDefinitionEditRequest{
			OperationID:   proposal.OperationID,
			Creation:      frozen.PreparedEdit.Creation,
			BaseHead:      proposal.BaseHead,
			TargetHead:    proposal.TargetHead,
			OriginalState: originalState,
			Base:          baseProjection,
			Target:        targetProjection,
		},
	); err != nil {
		return invalidDefinitionEditProposal("bind approved and Temporal projections", err)
	}
	return nil
}

func validateDefinitionEditTargetPolicy(
	definition taskstate.ApprovedDefinitionV1,
	requireCurrentWriter bool,
) error {
	if requireCurrentWriter {
		if err := taskstate.ValidateApprovedDefinitionV1ForWrite(definition); err != nil {
			return invalidDefinitionEditProposal("validate target writer gate", err)
		}
	}
	if definition.ExecutionMode != types.ExecutionModeCompiled ||
		definition.SourceScope != taskstate.SourceScopeApprovedPlan ||
		definition.Intent != definition.PlaybookContent {
		return invalidDefinitionEditProposal(
			"validate frozen target policy",
			errors.New("target is not a compiled exact approved-plan legacy projection"),
		)
	}
	return nil
}

func definitionEditSchedulerProjection(
	definition taskstate.ApprovedDefinitionV1,
) (scheduler.TaskDefinitionEditDefinition, error) {
	var spec scheduler.ScheduleSpec
	if err := strictjson.DecodeExact(definition.SpecJSON, &spec); err != nil {
		return scheduler.TaskDefinitionEditDefinition{}, fmt.Errorf("decode exact schedule spec: %w", err)
	}
	approvedScope, err := decodeDefinitionEditApprovedScope(definition.ScopeJSON)
	if err != nil {
		return scheduler.TaskDefinitionEditDefinition{}, fmt.Errorf("decode exact push scope: %w", err)
	}
	scope := workflow.PushScope{
		SourceIDs: approvedScope.SourceIDs,
		TopN:      approvedScope.TopN,
	}
	return scheduler.TaskDefinitionEditDefinition{
		Spec: spec, Scope: scope, NLDescription: definition.NLDescription,
	}, nil
}

type definitionEditApprovedScope struct {
	SourceIDs   []int64               `json:"source_ids,omitempty"`
	TopN        int                   `json:"top_n,omitempty"`
	Observation *observation.PolicyV1 `json:"observation,omitempty"`
}

func decodeDefinitionEditApprovedScope(
	raw json.RawMessage,
) (definitionEditApprovedScope, error) {
	var wire struct {
		SourceIDs   []int64         `json:"source_ids,omitempty"`
		TopN        int             `json:"top_n,omitempty"`
		Observation json.RawMessage `json:"observation,omitempty"`
	}
	if err := strictjson.DecodeExact(raw, &wire); err != nil {
		return definitionEditApprovedScope{}, err
	}
	scope := definitionEditApprovedScope{
		SourceIDs: wire.SourceIDs,
		TopN:      wire.TopN,
	}
	if len(wire.Observation) == 0 ||
		bytes.Equal(bytes.TrimSpace(wire.Observation), []byte("null")) {
		return scope, nil
	}
	policy, err := observation.DecodePolicyV1Exact(wire.Observation)
	if err != nil {
		return definitionEditApprovedScope{}, err
	}
	scope.Observation = &policy
	return scope, nil
}

func definitionEditOriginalStatusForWrite(
	status types.ScheduleStatus,
) (TaskDefinitionEditOriginalStatusV2, scheduler.TaskDefinitionEditOriginalState, error) {
	switch status {
	case types.ScheduleStatusActive:
		return TaskDefinitionEditOriginalStatusV2Active, scheduler.TaskDefinitionEditOriginalStateActive, nil
	case types.ScheduleStatusPaused:
		return TaskDefinitionEditOriginalStatusV2Paused, scheduler.TaskDefinitionEditOriginalStatePaused, nil
	default:
		return "", scheduler.TaskDefinitionEditOriginalStateUnknown, errors.New("status must be active or paused")
	}
}

func definitionEditOriginalState(
	status TaskDefinitionEditOriginalStatusV2,
) (scheduler.TaskDefinitionEditOriginalState, error) {
	switch status {
	case TaskDefinitionEditOriginalStatusV2Active:
		return scheduler.TaskDefinitionEditOriginalStateActive, nil
	case TaskDefinitionEditOriginalStatusV2Paused:
		return scheduler.TaskDefinitionEditOriginalStatePaused, nil
	default:
		return scheduler.TaskDefinitionEditOriginalStateUnknown, errors.New("frozen status must be active or paused")
	}
}

func validTaskDefinitionEditIdentifier(value string, maximum int) bool {
	if value == "" || len(value) > maximum || !utf8.ValidString(value) ||
		strings.TrimSpace(value) != value {
		return false
	}
	for _, char := range value {
		if unicode.IsControl(char) || unicode.In(char, unicode.Cf) {
			return false
		}
	}
	return true
}

func definitionEditDigestEqual(left, right string) bool {
	return len(left) == len(right) &&
		subtle.ConstantTimeCompare([]byte(left), []byte(right)) == 1
}

func validDefinitionEditProposalSize(name string, value []byte, maximum int) error {
	if len(value) == 0 || len(value) > maximum {
		return invalidDefinitionEditProposal(
			"validate "+name+" size",
			fmt.Errorf("size %d is outside the supported bound", len(value)),
		)
	}
	return nil
}

func invalidDefinitionEditProposal(operation string, cause error) error {
	return fmt.Errorf("%w: %s: %w", ErrDefinitionEditProposalInvalid, operation, cause)
}
