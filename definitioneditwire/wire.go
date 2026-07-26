// Package definitioneditwire is the dependency-light retained reader for
// durable Approved Definition edit checkpoints. It intentionally imports no
// task, scheduler, workflow, or store package, so recovery can validate frozen
// bytes without creating a control-plane import cycle.
package definitioneditwire

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/YouToco/vane/internal/strictjson"
	"github.com/YouToco/vane/observation"
)

const (
	proposalWireVersion   = "vane.task-definition-edit-proposal/v1"
	preparedWireVersionV1 = "v1"
	preparedWireVersionV2 = "v2"

	maxProposalBytes       = 64 << 10
	maxDefinitionBytes     = 2 << 20
	maxPreparedBytes       = 4 << 20
	maxSnapshotBytes       = 16 << 10
	maxOperationIDBytes    = 512
	maxReferenceBytes      = 1024
	maxTaskIDBytes         = 255
	retainedIDScheme       = "v1"
	retainedFingerprintV1  = "v1"
	retainedFingerprintV2  = "v2"
	retainedRuntime        = "compiled-snapshot/v1"
	retainedEditNoteRoot   = "vane/task-definition-edit/v1"
	retainedActivationNote = "vane/task-schedule/v1:definition-committed"
)

// ErrInvalid identifies a malformed, non-canonical, or cross-spliced frozen
// edit checkpoint. Callers should not expose the detailed cause to end users.
var ErrInvalid = errors.New("definition edit wire is invalid")

type OriginalStatusV1 string

const (
	OriginalStatusActive OriginalStatusV1 = "active"
	OriginalStatusPaused OriginalStatusV1 = "paused"
)

type SnapshotPhaseV1 string

const (
	SnapshotPhaseBaseOriginal SnapshotPhaseV1 = "base_original"
	SnapshotPhaseBasePaused   SnapshotPhaseV1 = "base_paused"
	SnapshotPhaseTargetPaused SnapshotPhaseV1 = "target_paused"
	SnapshotPhaseTargetFinal  SnapshotPhaseV1 = "target_final"
)

type HeadV1 struct {
	Version int64  `json:"version"`
	Digest  string `json:"digest"`
}

type ProposalActorV1 struct {
	TenantID int64 `json:"tenant_id"`
	UserID   int64 `json:"user_id"`
}

type ProposalTargetV1 struct {
	TenantID int64  `json:"tenant_id"`
	UserID   int64  `json:"user_id"`
	TaskID   string `json:"task_id"`
}

// ProposalV1 is the small authenticated envelope emitted by the task layer.
type ProposalV1 struct {
	WireVersion            string           `json:"wire_version"`
	OperationID            string           `json:"operation_id"`
	ApprovalRef            string           `json:"approval_ref"`
	Actor                  ProposalActorV1  `json:"actor"`
	Target                 ProposalTargetV1 `json:"target"`
	SessionID              int64            `json:"session_id"`
	ExpiresAtUnixMicros    int64            `json:"expires_at_unix_micros"`
	OriginalStatus         OriginalStatusV1 `json:"original_status"`
	BaseHead               HeadV1           `json:"base_head"`
	TargetHead             HeadV1           `json:"target_head"`
	TargetDefinitionDigest string           `json:"target_definition_digest"`
	PreparedEditDigest     string           `json:"prepared_edit_digest"`
	BaseSnapshotDigest     string           `json:"base_snapshot_digest"`
}

type PushScopeV1 struct {
	SourceIDs []int64 `json:"source_ids,omitempty"`
	TopN      int     `json:"top_n,omitempty"`
}

type approvedScopeWireV1 struct {
	Observation json.RawMessage `json:"observation,omitempty"`
	SourceIDs   []int64         `json:"source_ids,omitempty"`
	TopN        int             `json:"top_n,omitempty"`
}

// ScheduleSpecV1 mirrors the frozen scheduler V1 projection layout. Approved
// Definition stores canonical JSON objects with lexicographically ordered map
// keys, while the scheduler projection digest was written from this typed
// field order. Retained readers must reconstruct that writer order instead of
// hashing the raw nested JSON bytes.
type ScheduleSpecV1 struct {
	Cron         string `json:"cron,omitempty"`
	EverySeconds int    `json:"every_seconds,omitempty"`
	AnchorAt     string `json:"anchor_at,omitempty"`
	TZ           string `json:"tz,omitempty"`
}

type PushParamsV1 struct {
	TenantID       int64           `json:"tenant_id,omitempty"`
	UserID         int64           `json:"user_id"`
	RunKind        string          `json:"run_kind,omitempty"`
	ExecutionMode  string          `json:"execution_mode,omitempty"`
	RuntimeVersion string          `json:"runtime_version,omitempty"`
	ScheduleID     string          `json:"schedule_id,omitempty"`
	Scope          PushScopeV1     `json:"scope"`
	NLDesc         string          `json:"nl_desc,omitempty"`
	RunSnapshot    json.RawMessage `json:"run_snapshot,omitempty"`
}

type PreparedTimingV1 struct {
	Calendar     *PreparedCalendarV1 `json:"calendar,omitempty"`
	EveryNanos   int64               `json:"every_nanos,omitempty"`
	OffsetNanos  int64               `json:"offset_nanos,omitempty"`
	TimeZoneName string              `json:"time_zone_name"`
}

type PreparedCalendarV1 struct {
	Second     uint64 `json:"second"`
	Minute     uint64 `json:"minute"`
	Hour       uint64 `json:"hour"`
	DayOfMonth uint64 `json:"day_of_month"`
	Month      uint64 `json:"month"`
	DayOfWeek  uint64 `json:"day_of_week"`
}

type PreparedActionV1 struct {
	Params                        PushParamsV1 `json:"params"`
	TaskQueue                     string       `json:"task_queue"`
	WorkflowType                  string       `json:"workflow_type"`
	ActionID                      string       `json:"action_id"`
	WorkflowExecutionTimeoutNanos int64        `json:"workflow_execution_timeout_nanos"`
	WorkflowRunTimeoutNanos       int64        `json:"workflow_run_timeout_nanos"`
	WorkflowTaskTimeoutNanos      int64        `json:"workflow_task_timeout_nanos"`
	HasRetryPolicy                bool         `json:"has_retry_policy"`
	ActivationNote                string       `json:"activation_note"`
}

type PreparedPolicyV1 struct {
	Overlap        int32 `json:"overlap"`
	CatchupNanos   int64 `json:"catchup_nanos"`
	PauseOnFailure bool  `json:"pause_on_failure"`
}

type PreparedCreationStateV1 struct {
	Paused             bool   `json:"paused"`
	RemainingActions   int    `json:"remaining_actions"`
	TriggerImmediately bool   `json:"trigger_immediately"`
	BackfillCount      int    `json:"backfill_count"`
	Note               string `json:"note"`
}

// PreparedCreationV1 is the exact successful create_schedule provenance.
type PreparedCreationV1 struct {
	IDSchemeVersion    string                  `json:"id_scheme_version"`
	FingerprintVersion string                  `json:"fingerprint_version"`
	Namespace          string                  `json:"namespace"`
	NamespaceID        string                  `json:"namespace_id"`
	ConverterID        string                  `json:"converter_id"`
	TaskID             string                  `json:"task_id"`
	TenantID           int64                   `json:"tenant_id"`
	UserID             int64                   `json:"user_id"`
	OperationID        string                  `json:"operation_id"`
	PreparedDigest     string                  `json:"prepared_digest"`
	RequestDigest      string                  `json:"request_digest"`
	Timing             PreparedTimingV1        `json:"timing"`
	Action             PreparedActionV1        `json:"action"`
	Policy             PreparedPolicyV1        `json:"policy"`
	Creation           PreparedCreationStateV1 `json:"creation"`
}

type ScheduleStateV1 struct {
	Paused           bool   `json:"paused"`
	Note             string `json:"note"`
	LimitedActions   bool   `json:"limited_actions"`
	RemainingActions int64  `json:"remaining_actions"`
}

type FingerprintV1 struct {
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

type RepresentationV1 struct {
	Digest                string           `json:"digest"`
	Timing                PreparedTimingV1 `json:"timing"`
	Action                PreparedActionV1 `json:"action"`
	Policy                PreparedPolicyV1 `json:"policy"`
	WorkflowIDReusePolicy int32            `json:"workflow_id_reuse_policy"`
	State                 ScheduleStateV1  `json:"state"`
	Fingerprint           FingerprintV1    `json:"fingerprint"`
}

// PreparedEditV1 mirrors the frozen scheduler wire without depending on its
// runtime package. It is a retained reader, not a second current writer.
type PreparedEditV1 struct {
	WireVersion            string             `json:"wire_version"`
	OperationID            string             `json:"operation_id"`
	OperationDigest        string             `json:"operation_digest"`
	RequestDigest          string             `json:"request_digest"`
	BaseProjectionDigest   string             `json:"base_projection_digest"`
	TargetProjectionDigest string             `json:"target_projection_digest"`
	Creation               PreparedCreationV1 `json:"creation"`
	BaseHead               HeadV1             `json:"base_head"`
	TargetHead             HeadV1             `json:"target_head"`
	OriginalState          OriginalStatusV1   `json:"original_state"`
	BaseRevision           string             `json:"base_revision"`
	BaseOriginal           RepresentationV1   `json:"base_original"`
	BasePaused             RepresentationV1   `json:"base_paused"`
	TargetPaused           RepresentationV1   `json:"target_paused"`
	TargetFinal            RepresentationV1   `json:"target_final"`
}

type SnapshotV1 struct {
	TaskID               string          `json:"task_id"`
	RequestDigest        string          `json:"request_digest"`
	Phase                SnapshotPhaseV1 `json:"phase"`
	RepresentationDigest string          `json:"representation_digest"`
	Revision             string          `json:"revision"`
}

type operationSeedV1 struct {
	WireVersion            string           `json:"wire_version"`
	OperationID            string           `json:"operation_id"`
	CreationRequestDigest  string           `json:"creation_request_digest"`
	TenantID               int64            `json:"tenant_id"`
	UserID                 int64            `json:"user_id"`
	TaskID                 string           `json:"task_id"`
	BaseHead               HeadV1           `json:"base_head"`
	TargetHead             HeadV1           `json:"target_head"`
	OriginalState          OriginalStatusV1 `json:"original_state"`
	BaseProjectionDigest   string           `json:"base_projection_digest"`
	TargetProjectionDigest string           `json:"target_projection_digest"`
	BaseTiming             PreparedTimingV1 `json:"base_timing"`
	BaseAction             PreparedActionV1 `json:"base_action"`
	BasePolicy             PreparedPolicyV1 `json:"base_policy"`
	BaseReusePolicy        int32            `json:"base_reuse_policy"`
	BaseState              ScheduleStateV1  `json:"base_state"`
	TargetTiming           PreparedTimingV1 `json:"target_timing"`
	TargetAction           PreparedActionV1 `json:"target_action"`
	TargetPolicy           PreparedPolicyV1 `json:"target_policy"`
	TargetReusePolicy      int32            `json:"target_reuse_policy"`
}

type approvedProjectionV1 struct {
	Spec          ScheduleSpecV1 `json:"spec"`
	Scope         PushScopeV1    `json:"scope"`
	NLDescription string         `json:"nl_description"`
}

// FrozenProposal contains typed values decoded from, and exact copies of, all
// five caller checkpoints. Approved Definition semantics remain owned by
// taskstate and the Store's immutable-history corroboration.
type FrozenProposal struct {
	Proposal              ProposalV1
	CanonicalProposal     []byte
	ProposalDigest        string
	BaseDefinitionBytes   []byte
	TargetDefinitionBytes []byte
	Prepared              PreparedEditV1
	PreparedEditBytes     []byte
	BaseSnapshot          SnapshotV1
	BaseSnapshotBytes     []byte
}

// DecodeFrozenProposal validates canonical shape, frozen identities, every
// raw-byte digest, prepared phase binding, and the observed base snapshot.
func DecodeFrozenProposal(
	canonicalProposal, baseDefinition, targetDefinition, preparedEdit, baseSnapshot []byte,
) (FrozenProposal, error) {
	if err := bounded("proposal", canonicalProposal, maxProposalBytes); err != nil {
		return FrozenProposal{}, err
	}
	if err := bounded("base definition", baseDefinition, maxDefinitionBytes); err != nil {
		return FrozenProposal{}, err
	}
	if err := bounded("target definition", targetDefinition, maxDefinitionBytes); err != nil {
		return FrozenProposal{}, err
	}
	if err := bounded("prepared edit", preparedEdit, maxPreparedBytes); err != nil {
		return FrozenProposal{}, err
	}
	if err := bounded("base snapshot", baseSnapshot, maxSnapshotBytes); err != nil {
		return FrozenProposal{}, err
	}

	var proposal ProposalV1
	if err := decodeCanonical("proposal", canonicalProposal, &proposal); err != nil {
		return FrozenProposal{}, err
	}
	var prepared PreparedEditV1
	if err := decodeCanonical("prepared edit", preparedEdit, &prepared); err != nil {
		return FrozenProposal{}, err
	}
	if err := validateProposal(proposal, baseDefinition, targetDefinition, preparedEdit, baseSnapshot); err != nil {
		return FrozenProposal{}, err
	}
	if err := validatePrepared(proposal, prepared); err != nil {
		return FrozenProposal{}, err
	}
	var snapshot SnapshotV1
	if err := decodeCanonical("base snapshot", baseSnapshot, &snapshot); err != nil {
		return FrozenProposal{}, err
	}
	if err := validateSnapshot(prepared, snapshot, SnapshotPhaseBaseOriginal); err != nil {
		return FrozenProposal{}, invalid("validate base snapshot", err)
	}
	if snapshot.Revision != prepared.BaseRevision {
		return FrozenProposal{}, invalid("validate base snapshot", errors.New("revision differs from prepared base"))
	}

	return FrozenProposal{
		Proposal:              proposal,
		CanonicalProposal:     bytes.Clone(canonicalProposal),
		ProposalDigest:        digest(canonicalProposal),
		BaseDefinitionBytes:   bytes.Clone(baseDefinition),
		TargetDefinitionBytes: bytes.Clone(targetDefinition),
		Prepared:              prepared,
		PreparedEditBytes:     bytes.Clone(preparedEdit),
		BaseSnapshot:          snapshot,
		BaseSnapshotBytes:     bytes.Clone(baseSnapshot),
	}, nil
}

// DecodePhaseSnapshot validates one later remote observation against the exact
// representation frozen in prepared. It accepts all four protocol phases; the
// Store method chooses which phase is legal for a particular checkpoint CAS.
func DecodePhaseSnapshot(prepared PreparedEditV1, raw []byte) (SnapshotV1, error) {
	if err := bounded("phase snapshot", raw, maxSnapshotBytes); err != nil {
		return SnapshotV1{}, err
	}
	var snapshot SnapshotV1
	if err := decodeCanonical("phase snapshot", raw, &snapshot); err != nil {
		return SnapshotV1{}, err
	}
	if err := validateSnapshot(prepared, snapshot, snapshot.Phase); err != nil {
		return SnapshotV1{}, invalid("validate phase snapshot", err)
	}
	return snapshot, nil
}

// DecodePhaseSnapshotBytes is the recovery convenience for a Store row. It
// re-decodes the exact persisted prepared wire before binding the checkpoint;
// no caller-retained typed value is trusted across transactions.
func DecodePhaseSnapshotBytes(preparedRaw, snapshotRaw []byte) (SnapshotV1, error) {
	if err := bounded("prepared edit", preparedRaw, maxPreparedBytes); err != nil {
		return SnapshotV1{}, err
	}
	var prepared PreparedEditV1
	if err := decodeCanonical("prepared edit", preparedRaw, &prepared); err != nil {
		return SnapshotV1{}, err
	}
	// Reconstruct only the envelope fields consumed by validatePrepared. Raw
	// definition/proposal digests were already corroborated when the operation
	// row was sealed; the prepared wire's own identities and phases are checked
	// again here before a later remote checkpoint can be accepted.
	proposal := ProposalV1{
		OperationID: prepared.OperationID,
		Target: ProposalTargetV1{
			TenantID: prepared.Creation.TenantID,
			UserID:   prepared.Creation.UserID,
			TaskID:   prepared.Creation.TaskID,
		},
		OriginalStatus: prepared.OriginalState,
		BaseHead:       prepared.BaseHead,
		TargetHead:     prepared.TargetHead,
	}
	if err := validatePrepared(proposal, prepared); err != nil {
		return SnapshotV1{}, err
	}
	return DecodePhaseSnapshot(prepared, snapshotRaw)
}

// CanonicalCreation returns the exact creation ownership record that must be
// corroborated against the successful create_schedule pending-action tombstone.
func CanonicalCreation(prepared PreparedEditV1) ([]byte, error) {
	encoded, err := json.Marshal(prepared.Creation)
	if err != nil {
		return nil, invalid("encode creation provenance", err)
	}
	return encoded, nil
}

// ValidateApprovedProjectionBindings proves that the exact Approved
// Definition projection bytes sealed in PostgreSQL are the base/target
// projections named by prepared, and that the corresponding Temporal Actions
// carry the same natural-language description and source scope. Timing policy
// remains the proposal-sealing current-writer gate; this retained reader never
// re-runs an evolving compiler.
func ValidateApprovedProjectionBindings(
	prepared PreparedEditV1,
	baseSpec, baseScope []byte,
	baseNLDescription string,
	targetSpec, targetScope []byte,
	targetNLDescription string,
) error {
	baseProjection, baseDigest, err := approvedProjection(
		baseSpec, baseScope, baseNLDescription)
	if err != nil {
		return err
	}
	targetProjection, targetDigest, err := approvedProjection(
		targetSpec, targetScope, targetNLDescription)
	if err != nil {
		return err
	}
	if !digestEqual(baseDigest, prepared.BaseProjectionDigest) ||
		!digestEqual(targetDigest, prepared.TargetProjectionDigest) {
		return invalid("validate approved projections",
			errors.New("projection digest differs from Approved Definition"))
	}
	if !projectionMatchesAction(baseProjection, prepared.BaseOriginal.Action) ||
		!projectionMatchesAction(targetProjection, prepared.TargetFinal.Action) {
		return invalid("validate approved projections",
			errors.New("projection differs from frozen Temporal Action"))
	}
	return nil
}

func approvedProjection(
	specRaw, scopeRaw []byte,
	nlDescription string,
) (approvedProjectionV1, string, error) {
	if len(specRaw) == 0 || len(scopeRaw) == 0 ||
		!utf8.ValidString(nlDescription) || nlDescription == "" ||
		strings.TrimSpace(nlDescription) != nlDescription {
		return approvedProjectionV1{}, "", invalid(
			"validate approved projection", errors.New("projection is invalid"))
	}
	if err := strictjson.Validate(specRaw); err != nil {
		return approvedProjectionV1{}, "", invalid("decode approved spec", err)
	}
	var specObject map[string]json.RawMessage
	if err := strictjson.DecodeExact(specRaw, &specObject); err != nil || specObject == nil {
		return approvedProjectionV1{}, "", invalid(
			"decode approved spec", errors.New("approved spec is not an object"))
	}
	var spec ScheduleSpecV1
	if err := strictjson.DecodeExact(specRaw, &spec); err != nil {
		return approvedProjectionV1{}, "", invalid("decode approved spec", err)
	}
	scope, err := decodeApprovedScope(scopeRaw)
	if err != nil {
		return approvedProjectionV1{}, "", err
	}
	if scope.TopN < 0 {
		return approvedProjectionV1{}, "", invalid(
			"validate approved scope", errors.New("top_n is negative"))
	}
	seen := make(map[int64]struct{}, len(scope.SourceIDs))
	for _, sourceID := range scope.SourceIDs {
		if sourceID <= 0 {
			return approvedProjectionV1{}, "", invalid(
				"validate approved scope", errors.New("source id is invalid"))
		}
		if _, duplicate := seen[sourceID]; duplicate {
			return approvedProjectionV1{}, "", invalid(
				"validate approved scope", errors.New("source ids contain a duplicate"))
		}
		seen[sourceID] = struct{}{}
	}
	projection := approvedProjectionV1{
		Spec: spec, Scope: scope,
		NLDescription: nlDescription,
	}
	projectionDigest, err := digestJSON(projection)
	if err != nil {
		return approvedProjectionV1{}, "", invalid(
			"digest approved projection", err)
	}
	return projection, projectionDigest, nil
}

func decodeApprovedScope(raw []byte) (PushScopeV1, error) {
	var wire approvedScopeWireV1
	if err := decodeCanonical("approved scope", raw, &wire); err != nil {
		return PushScopeV1{}, err
	}
	if len(wire.Observation) != 0 &&
		!bytes.Equal(bytes.TrimSpace(wire.Observation), []byte("null")) {
		if _, err := observation.DecodePolicyV1Exact(wire.Observation); err != nil {
			return PushScopeV1{}, invalid("decode approved observation", err)
		}
	}
	return PushScopeV1{
		SourceIDs: wire.SourceIDs,
		TopN:      wire.TopN,
	}, nil
}

func projectionMatchesAction(
	projection approvedProjectionV1,
	action PreparedActionV1,
) bool {
	return projection.NLDescription == action.Params.NLDesc &&
		projection.Scope.TopN == action.Params.Scope.TopN &&
		slices.Equal(projection.Scope.SourceIDs, action.Params.Scope.SourceIDs)
}

func validateProposal(
	proposal ProposalV1,
	baseDefinition, targetDefinition, preparedEdit, baseSnapshot []byte,
) error {
	if proposal.WireVersion != proposalWireVersion ||
		!validIdentifier(proposal.OperationID, maxOperationIDBytes) ||
		!validIdentifier(proposal.ApprovalRef, maxReferenceBytes) ||
		proposal.Actor.TenantID <= 0 || proposal.Actor.UserID <= 0 ||
		proposal.Target.TenantID <= 0 || proposal.Target.UserID <= 0 ||
		!validIdentifier(proposal.Target.TaskID, maxTaskIDBytes) ||
		proposal.SessionID <= 0 || proposal.ExpiresAtUnixMicros <= 0 {
		return invalid("validate proposal identity", errors.New("identity or expiry is invalid"))
	}
	if proposal.Actor.TenantID != proposal.Target.TenantID ||
		proposal.Actor.UserID != proposal.Target.UserID {
		return invalid("validate proposal actor", errors.New("v1 actor is not the target owner"))
	}
	if proposal.OriginalStatus != OriginalStatusActive && proposal.OriginalStatus != OriginalStatusPaused {
		return invalid("validate original status", errors.New("status is unsupported"))
	}
	if err := validateHeads(proposal.BaseHead, proposal.TargetHead); err != nil {
		return invalid("validate proposal heads", err)
	}
	checks := []struct{ got, want string }{
		{proposal.BaseHead.Digest, digest(baseDefinition)},
		{proposal.TargetHead.Digest, digest(targetDefinition)},
		{proposal.TargetDefinitionDigest, digest(targetDefinition)},
		{proposal.PreparedEditDigest, digest(preparedEdit)},
		{proposal.BaseSnapshotDigest, digest(baseSnapshot)},
	}
	for _, check := range checks {
		if !validDigest(check.got) || !digestEqual(check.got, check.want) {
			return invalid("validate proposal digests", errors.New("checkpoint digest differs"))
		}
	}
	return nil
}

func validatePrepared(proposal ProposalV1, prepared PreparedEditV1) error {
	if prepared.OperationID != proposal.OperationID ||
		!validIdentifier(prepared.OperationID, maxOperationIDBytes) {
		return invalid("validate prepared identity", errors.New("operation identity differs"))
	}
	for _, value := range []string{
		prepared.OperationDigest, prepared.RequestDigest,
		prepared.BaseProjectionDigest, prepared.TargetProjectionDigest,
	} {
		if !validDigest(value) {
			return invalid("validate prepared digests", errors.New("digest is invalid"))
		}
	}
	if prepared.BaseHead != proposal.BaseHead || prepared.TargetHead != proposal.TargetHead ||
		prepared.OriginalState != proposal.OriginalStatus {
		return invalid("validate prepared heads", errors.New("proposal and prepared edit differ"))
	}
	if err := validateHeads(prepared.BaseHead, prepared.TargetHead); err != nil {
		return invalid("validate prepared heads", err)
	}
	if !validRevision(prepared.BaseRevision) {
		return invalid("validate prepared revision", errors.New("base revision is invalid"))
	}
	creation := prepared.Creation
	if creation.IDSchemeVersion != retainedIDScheme ||
		creation.TenantID != proposal.Target.TenantID ||
		creation.UserID != proposal.Target.UserID || creation.TaskID != proposal.Target.TaskID ||
		!validIdentifier(creation.OperationID, maxOperationIDBytes) ||
		!validDigest(creation.PreparedDigest) || !validDigest(creation.RequestDigest) ||
		!validIdentifier(creation.Namespace, maxReferenceBytes) ||
		!validIdentifier(creation.NamespaceID, maxReferenceBytes) ||
		!validIdentifier(creation.ConverterID, maxReferenceBytes) {
		return invalid("validate creation ownership", errors.New("creation provenance is invalid"))
	}
	switch prepared.WireVersion {
	case preparedWireVersionV1:
		if creation.FingerprintVersion != retainedFingerprintV2 {
			return invalid(
				"validate creation ownership",
				errors.New("definition edit v1 requires retained v2 creation provenance"),
			)
		}
	case preparedWireVersionV2:
		if creation.FingerprintVersion != retainedFingerprintV1 {
			return invalid(
				"validate creation ownership",
				errors.New("definition edit v2 requires retained v1 creation provenance"),
			)
		}
	default:
		return invalid(
			"validate prepared identity",
			errors.New("prepared wire version is unsupported"),
		)
	}
	if err := validateCreationAction(creation.Action, creation); err != nil {
		return invalid("validate creation action", err)
	}
	if err := validateTiming(creation.Timing); err != nil {
		return invalid("validate creation timing", err)
	}

	representations := []struct {
		name  string
		value RepresentationV1
	}{
		{"base_original", prepared.BaseOriginal},
		{"base_paused", prepared.BasePaused},
		{"target_paused", prepared.TargetPaused},
		{"target_final", prepared.TargetFinal},
	}
	for _, representation := range representations {
		if err := validateRepresentation(representation.name, representation.value, creation); err != nil {
			return invalid("validate "+representation.name, err)
		}
	}
	seed := operationSeedV1{
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
		BaseTiming:             prepared.BaseOriginal.Timing,
		BaseAction:             prepared.BaseOriginal.Action,
		BasePolicy:             prepared.BaseOriginal.Policy,
		BaseReusePolicy:        prepared.BaseOriginal.WorkflowIDReusePolicy,
		BaseState:              prepared.BaseOriginal.State,
		TargetTiming:           prepared.TargetFinal.Timing,
		TargetAction:           prepared.TargetFinal.Action,
		TargetPolicy:           prepared.TargetFinal.Policy,
		TargetReusePolicy:      prepared.TargetFinal.WorkflowIDReusePolicy,
	}
	operationDigest, err := digestJSON(seed)
	if err != nil || !digestEqual(operationDigest, prepared.OperationDigest) {
		return invalid("validate operation digest",
			errors.New("operation digest differs from frozen operation"))
	}
	requestSeed := prepared
	requestSeed.RequestDigest = ""
	requestDigest, err := digestJSON(requestSeed)
	if err != nil || !digestEqual(requestDigest, prepared.RequestDigest) {
		return invalid("validate request digest",
			errors.New("request digest differs from prepared edit"))
	}
	if err := validatePreparedPhases(prepared); err != nil {
		return invalid("validate prepared phases", err)
	}
	return nil
}

func validateRepresentation(name string, value RepresentationV1, creation PreparedCreationV1) error {
	if !validDigest(value.Digest) || value.State.LimitedActions || value.State.RemainingActions != 0 {
		return errors.New("digest or action limits are invalid")
	}
	if value.WorkflowIDReusePolicy != 0 && value.WorkflowIDReusePolicy != 1 {
		return errors.New("workflow reuse policy is unsupported")
	}
	representationSeed := value
	representationSeed.Digest = ""
	representationDigest, err := digestJSON(representationSeed)
	if err != nil || !digestEqual(representationDigest, value.Digest) {
		return errors.New("representation digest differs")
	}
	if value.Policy != creation.Policy || !sameActionEnvelope(value.Action, creation.Action) {
		return errors.New("immutable execution envelope differs")
	}
	if err := validateActionOwner(value.Action, creation); err != nil {
		return err
	}
	if err := validateTiming(value.Timing); err != nil {
		return err
	}
	if !utf8.ValidString(value.State.Note) || !fingerprintCoreMatches(value.Fingerprint, creation) {
		return errors.New("state or ownership fingerprint differs")
	}
	return nil
}

func validatePreparedPhases(prepared PreparedEditV1) error {
	if !baseFingerprintMatches(prepared.BaseOriginal, prepared.BaseHead) {
		return errors.New("base original fingerprint differs")
	}
	if !sameExecutionEnvelope(prepared.BaseOriginal, prepared.TargetFinal) {
		return errors.New("target changes immutable execution settings")
	}
	if prepared.OriginalState == OriginalStatusActive {
		if !sameDefinition(prepared.BaseOriginal, prepared.BasePaused) ||
			!sameDefinition(prepared.TargetPaused, prepared.TargetFinal) ||
			prepared.BaseOriginal.State.Paused || !prepared.BasePaused.State.Paused ||
			!prepared.TargetPaused.State.Paused || prepared.TargetFinal.State.Paused {
			return errors.New("active edit phase transition differs")
		}
		checks := []struct {
			fingerprint FingerprintV1
			head        HeadV1
			phase       string
		}{
			{prepared.BasePaused.Fingerprint, prepared.BaseHead, "base_paused"},
			{prepared.TargetPaused.Fingerprint, prepared.TargetHead, "target_paused"},
			{prepared.TargetFinal.Fingerprint, prepared.TargetHead, "final_active"},
		}
		for _, check := range checks {
			if !phaseFingerprintMatches(check.fingerprint, check.head, prepared.OperationDigest, check.phase) {
				return errors.New("active edit fingerprint differs")
			}
			if noteFor(check.phase, prepared.OperationDigest) != phaseNote(prepared, check.phase) {
				return errors.New("active edit note differs")
			}
		}
		return nil
	}
	if !prepared.BaseOriginal.State.Paused || !prepared.BasePaused.State.Paused ||
		!prepared.TargetPaused.State.Paused || !prepared.TargetFinal.State.Paused ||
		prepared.BasePaused.Digest != prepared.BaseOriginal.Digest ||
		prepared.TargetPaused.Digest != prepared.TargetFinal.Digest ||
		prepared.TargetFinal.State.Note != prepared.BaseOriginal.State.Note ||
		!phaseFingerprintMatches(prepared.TargetFinal.Fingerprint, prepared.TargetHead,
			prepared.OperationDigest, "final_paused") {
		return errors.New("paused edit phase transition differs")
	}
	return nil
}

func phaseNote(prepared PreparedEditV1, phase string) string {
	switch phase {
	case "base_paused":
		return prepared.BasePaused.State.Note
	case "target_paused":
		return prepared.TargetPaused.State.Note
	case "final_active":
		return prepared.TargetFinal.State.Note
	default:
		return ""
	}
}

func validateSnapshot(prepared PreparedEditV1, snapshot SnapshotV1, expected SnapshotPhaseV1) error {
	representation, ok := representationForPhase(prepared, expected)
	if !ok || snapshot.Phase != expected || snapshot.TaskID != prepared.Creation.TaskID ||
		snapshot.RequestDigest != prepared.RequestDigest ||
		snapshot.RepresentationDigest != representation.Digest || !validRevision(snapshot.Revision) {
		return errors.New("snapshot does not belong to the prepared phase")
	}
	return nil
}

func representationForPhase(prepared PreparedEditV1, phase SnapshotPhaseV1) (RepresentationV1, bool) {
	switch phase {
	case SnapshotPhaseBaseOriginal:
		return prepared.BaseOriginal, true
	case SnapshotPhaseBasePaused:
		return prepared.BasePaused, true
	case SnapshotPhaseTargetPaused:
		return prepared.TargetPaused, true
	case SnapshotPhaseTargetFinal:
		return prepared.TargetFinal, true
	default:
		return RepresentationV1{}, false
	}
}

func validateCreationAction(action PreparedActionV1, creation PreparedCreationV1) error {
	if creation.FingerprintVersion != retainedFingerprintV1 {
		return validateActionOwner(action, creation)
	}
	if action.Params.TenantID != 0 || action.Params.ExecutionMode != "" ||
		action.Params.RuntimeVersion != "" {
		return errors.New("retained v1 creation action is not in its legacy shape")
	}
	action.Params.TenantID = creation.TenantID
	action.Params.ExecutionMode = "compiled"
	return validateActionOwner(action, creation)
}

func validateActionOwner(action PreparedActionV1, creation PreparedCreationV1) error {
	params := action.Params
	if params.TenantID != creation.TenantID || params.UserID != creation.UserID ||
		params.RunKind != "scheduled" || params.ExecutionMode != "compiled" ||
		params.ScheduleID != creation.TaskID || len(params.RunSnapshot) != 0 ||
		(params.RuntimeVersion != "" && params.RuntimeVersion != retainedRuntime) {
		return errors.New("workflow params do not match the task owner")
	}
	if params.Scope.TopN < 0 || !validIdentifier(action.TaskQueue, maxReferenceBytes) ||
		!validIdentifier(action.WorkflowType, maxReferenceBytes) ||
		!validIdentifier(action.ActionID, maxReferenceBytes) {
		return errors.New("workflow action is invalid")
	}
	seen := make(map[int64]struct{}, len(params.Scope.SourceIDs))
	for _, sourceID := range params.Scope.SourceIDs {
		if sourceID <= 0 {
			return errors.New("source id is invalid")
		}
		if _, duplicate := seen[sourceID]; duplicate {
			return errors.New("source ids contain a duplicate")
		}
		seen[sourceID] = struct{}{}
	}
	return nil
}

func validateTiming(timing PreparedTimingV1) error {
	hasCalendar := timing.Calendar != nil
	hasEvery := timing.EveryNanos > 0
	if hasCalendar == hasEvery || timing.EveryNanos < 0 || timing.OffsetNanos < 0 ||
		!validIdentifier(timing.TimeZoneName, maxReferenceBytes) {
		return errors.New("timing is invalid")
	}
	if _, err := time.LoadLocation(timing.TimeZoneName); err != nil {
		return errors.New("timing time zone is invalid")
	}
	if hasCalendar {
		if timing.EveryNanos != 0 || timing.OffsetNanos != 0 {
			return errors.New("calendar timing has an interval or offset")
		}
		calendar := timing.Calendar
		fields := []struct {
			bits     uint64
			min, max int
		}{
			{calendar.Second, 0, 59},
			{calendar.Minute, 0, 59},
			{calendar.Hour, 0, 23},
			{calendar.DayOfMonth, 1, 31},
			{calendar.Month, 1, 12},
			{calendar.DayOfWeek, 0, 6},
		}
		for _, field := range fields {
			allowed := bitMask(field.min, field.max)
			if field.bits == 0 || field.bits&^allowed != 0 {
				return errors.New("calendar timing bits are invalid")
			}
		}
		return nil
	}
	if timing.OffsetNanos >= timing.EveryNanos {
		return errors.New("interval offset is not smaller than every")
	}
	return nil
}

func bitMask(minimum, maximum int) uint64 {
	var mask uint64
	for value := minimum; value <= maximum; value++ {
		mask |= uint64(1) << value
	}
	return mask
}

func sameActionEnvelope(left, right PreparedActionV1) bool {
	return left.TaskQueue == right.TaskQueue && left.WorkflowType == right.WorkflowType &&
		left.ActionID == right.ActionID &&
		left.WorkflowExecutionTimeoutNanos == right.WorkflowExecutionTimeoutNanos &&
		left.WorkflowRunTimeoutNanos == right.WorkflowRunTimeoutNanos &&
		left.WorkflowTaskTimeoutNanos == right.WorkflowTaskTimeoutNanos &&
		left.HasRetryPolicy == right.HasRetryPolicy && left.ActivationNote == right.ActivationNote
}

func sameExecutionEnvelope(left, right RepresentationV1) bool {
	return sameActionEnvelope(left.Action, right.Action) &&
		left.Action.Params.TenantID == right.Action.Params.TenantID &&
		left.Action.Params.UserID == right.Action.Params.UserID &&
		left.Action.Params.RunKind == right.Action.Params.RunKind &&
		left.Action.Params.ExecutionMode == right.Action.Params.ExecutionMode &&
		left.Action.Params.RuntimeVersion == right.Action.Params.RuntimeVersion &&
		left.Action.Params.ScheduleID == right.Action.Params.ScheduleID &&
		len(left.Action.Params.RunSnapshot) == 0 && len(right.Action.Params.RunSnapshot) == 0 &&
		left.Policy == right.Policy && left.WorkflowIDReusePolicy == right.WorkflowIDReusePolicy
}

func sameDefinition(left, right RepresentationV1) bool {
	return reflect.DeepEqual(left.Timing, right.Timing) &&
		reflect.DeepEqual(left.Action, right.Action) && left.Policy == right.Policy &&
		left.WorkflowIDReusePolicy == right.WorkflowIDReusePolicy
}

func fingerprintCoreMatches(value FingerprintV1, creation PreparedCreationV1) bool {
	return value.IDSchemeVersion == creation.IDSchemeVersion &&
		value.FingerprintVersion == creation.FingerprintVersion &&
		value.Namespace == creation.Namespace && value.NamespaceID == creation.NamespaceID &&
		value.ConverterID == creation.ConverterID && value.TenantID == creation.TenantID &&
		value.UserID == creation.UserID && value.TaskID == creation.TaskID &&
		value.CreationOperationID == creation.OperationID &&
		value.CreationPreparedDigest == creation.PreparedDigest &&
		value.CreationRequestDigest == creation.RequestDigest && value.LifecyclePhase == "active"
}

func baseFingerprintMatches(representation RepresentationV1, head HeadV1) bool {
	value := representation.Fingerprint
	if value.DefinitionVersion == 0 && value.DefinitionDigest == "" &&
		value.EditOperationDigest == "" && value.EditPhase == "" {
		return head.Version == 1 &&
			(representation.State.Paused || representation.State.Note == retainedActivationNote)
	}
	if value.DefinitionVersion != head.Version || value.DefinitionDigest != head.Digest ||
		!validDigest(value.EditOperationDigest) {
		return false
	}
	if representation.State.Paused {
		return value.EditPhase == "final_active" || value.EditPhase == "final_paused"
	}
	return value.EditPhase == "final_active" &&
		representation.State.Note == noteFor("final_active", value.EditOperationDigest)
}

func phaseFingerprintMatches(value FingerprintV1, head HeadV1, operationDigest, phase string) bool {
	return value.DefinitionVersion == head.Version && value.DefinitionDigest == head.Digest &&
		value.EditOperationDigest == operationDigest && value.EditPhase == phase
}

func validateHeads(base, target HeadV1) error {
	if base.Version <= 0 || base.Version == math.MaxInt64 || target.Version != base.Version+1 ||
		!validDigest(base.Digest) || !validDigest(target.Digest) {
		return errors.New("definition heads are invalid")
	}
	return nil
}

func noteFor(phase, operationDigest string) string {
	if len(operationDigest) > 16 {
		operationDigest = operationDigest[:16]
	}
	return retainedEditNoteRoot + ":" + phase + ":" + operationDigest
}

func validRevision(value string) bool {
	if value == "" {
		return false
	}
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	return err == nil && len(decoded) != 0 && base64.RawURLEncoding.EncodeToString(decoded) == value
}

func validIdentifier(value string, maximum int) bool {
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

func validDigest(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && hex.EncodeToString(decoded) == value
}

func digestEqual(left, right string) bool {
	return len(left) == len(right) && subtle.ConstantTimeCompare([]byte(left), []byte(right)) == 1
}

func digest(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

func digestJSON(value any) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return digest(encoded), nil
}

func bounded(name string, value []byte, maximum int) error {
	if len(value) == 0 || len(value) > maximum {
		return invalid("validate "+name+" size", fmt.Errorf("size %d is outside the supported bound", len(value)))
	}
	return nil
}

func decodeCanonical(name string, raw []byte, destination any) error {
	if err := strictjson.DecodeExact(raw, destination); err != nil {
		return invalid("decode "+name, err)
	}
	canonical, err := json.Marshal(destination)
	if err != nil || !bytes.Equal(canonical, raw) {
		if err == nil {
			err = errors.New("bytes are not canonical")
		}
		return invalid("decode "+name, err)
	}
	return nil
}

func invalid(operation string, cause error) error {
	return fmt.Errorf("%w: %s: %v", ErrInvalid, operation, cause)
}
