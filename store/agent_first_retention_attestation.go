package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/internal/strictjson"
)

type AgentFirstRetentionPhase string

const (
	AgentFirstRetentionPhaseBaseline AgentFirstRetentionPhase = "baseline"
	AgentFirstRetentionPhasePrepared AgentFirstRetentionPhase = "prepared"
)

type AgentFirstArchivalState string

const (
	AgentFirstArchivalDisabled AgentFirstArchivalState = "disabled"
	AgentFirstArchivalEnabled  AgentFirstArchivalState = "enabled"
)

const (
	maxAgentFirstRetentionSeconds = int64(315360000)
	maxAgentFirstClusterIDBytes   = 512
	maxAgentFirstNamespaceBytes   = 255
	agentFirstEmptyDigest         = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
)

var agentFirstSourceRevisionPattern = regexp.MustCompile(`^[0-9a-f]{40}$`)
var agentFirstDigestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
var agentFirstNamespaceIDPattern = regexp.MustCompile(
	`^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

// AgentFirstRetentionAttestationInput contains only evidence observed outside
// PostgreSQL. The append function supplies database identity, the semantic DB
// snapshot, canonical bytes, both digests, issued_at and expires_at.
type AgentFirstRetentionAttestationInput struct {
	Phase                      AgentFirstRetentionPhase
	ParentDigest               string
	TemporalClusterID          string
	TemporalNamespace          string
	TemporalNamespaceID        string
	RetentionSeconds           int64
	HistoryArchivalState       AgentFirstArchivalState
	HistoryArchiveURIDigest    string
	VisibilityArchivalState    AgentFirstArchivalState
	VisibilityArchiveURIDigest string
	TemporalServerWitness      time.Time
	WorkflowInventoryDigest    string
	ScheduleInventoryDigest    string
	ArchiveInventoryDigest     string
	TemporalEvidenceDigest     string
	SourceRevision             string
	DeployDigest               string
}

type AgentFirstRetentionAttestationEvent struct {
	ID                         int64
	Phase                      AgentFirstRetentionPhase
	ParentDigest               *string
	TemporalClusterID          string
	TemporalNamespace          string
	TemporalNamespaceID        string
	RetentionSeconds           int64
	HistoryArchivalState       AgentFirstArchivalState
	HistoryArchiveURIDigest    string
	VisibilityArchivalState    AgentFirstArchivalState
	VisibilityArchiveURIDigest string
	TemporalServerWitness      time.Time
	WorkflowInventoryDigest    string
	ScheduleInventoryDigest    string
	ArchiveInventoryDigest     string
	TemporalEvidenceDigest     string
	SourceRevision             string
	DeployDigest               string
	DatabaseIdentity           []byte
	LegacyDBSnapshot           []byte
	LegacyDBSnapshotDigest     string
	CanonicalPayload           []byte
	PayloadDigest              string
	IssuedAt                   time.Time
	ExpiresAt                  time.Time
}

type agentFirstRetentionPayloadV130 struct {
	ArchiveInventoryDigest     string                         `json:"archive_inventory_digest"`
	DatabaseIdentity           agentFirstDatabaseIdentityV130 `json:"database_identity"`
	DeployDigest               string                         `json:"deploy_digest"`
	ExpiresAt                  string                         `json:"expires_at"`
	HistoryArchiveURIDigest    string                         `json:"history_archive_uri_digest"`
	HistoryArchivalState       AgentFirstArchivalState        `json:"history_archival_state"`
	IssuedAt                   string                         `json:"issued_at"`
	LegacyDBSnapshot           agentFirstLegacyDBSnapshotV130 `json:"legacy_db_snapshot"`
	LegacyDBSnapshotDigest     string                         `json:"legacy_db_snapshot_digest"`
	ParentDigest               *string                        `json:"parent_digest"`
	Phase                      AgentFirstRetentionPhase       `json:"phase"`
	RetentionSeconds           int64                          `json:"retention_seconds"`
	ScheduleInventoryDigest    string                         `json:"schedule_inventory_digest"`
	SchemaVersion              string                         `json:"schema_version"`
	SourceRevision             string                         `json:"source_revision"`
	TemporalClusterID          string                         `json:"temporal_cluster_id"`
	TemporalEvidenceDigest     string                         `json:"temporal_evidence_digest"`
	TemporalNamespace          string                         `json:"temporal_namespace"`
	TemporalNamespaceID        string                         `json:"temporal_namespace_id"`
	TemporalServerWitness      string                         `json:"temporal_server_witness"`
	VisibilityArchiveURIDigest string                         `json:"visibility_archive_uri_digest"`
	VisibilityArchivalState    AgentFirstArchivalState        `json:"visibility_archival_state"`
	WorkflowInventoryDigest    string                         `json:"workflow_inventory_digest"`
}

type agentFirstDatabaseIdentityV130 struct {
	DatabaseName       string `json:"database_name"`
	DatabaseOID        int64  `json:"database_oid"`
	PGSystemIdentifier string `json:"pg_system_identifier"`
	SchemaVersion      string `json:"schema_version"`
	ServerVersionNum   int64  `json:"server_version_num"`
}

type agentFirstLegacyDBSnapshotV130 struct {
	AuthorityInventory      agentFirstInventorySnapshotV130  `json:"authority_inventory"`
	LegacyCreation          agentFirstLegacyLaneSnapshotV130 `json:"legacy_creation"`
	Protocol1DefinitionEdit agentFirstLegacyLaneSnapshotV130 `json:"protocol1_definition_edit"`
	ScheduleInventory       agentFirstInventorySnapshotV130  `json:"schedule_inventory"`
	SchemaVersion           string                           `json:"schema_version"`
}

type agentFirstLegacyLaneSnapshotV130 struct {
	ActiveCount         int64  `json:"active_count"`
	LeaseCount          int64  `json:"lease_count"`
	OperationCount      int64  `json:"operation_count"`
	OperationDigest     string `json:"operation_digest"`
	PendingReceiptCount int64  `json:"pending_receipt_count"`
	ReceiptCount        int64  `json:"receipt_count"`
	ReceiptDigest       string `json:"receipt_digest"`
	ReceiptLeaseCount   int64  `json:"receipt_lease_count"`
	ReceiptGapCount     int64  `json:"receipt_gap_count"`
}

type agentFirstInventorySnapshotV130 struct {
	Digest    string `json:"digest"`
	ItemCount int64  `json:"item_count"`
}

const agentFirstRetentionEventColumnsV130 = `
	id,phase,parent_digest,temporal_cluster_id,temporal_namespace,temporal_namespace_id,
	retention_seconds,history_archival_state,history_archive_uri_digest,
	visibility_archival_state,visibility_archive_uri_digest,temporal_server_witness,
	workflow_inventory_digest,schedule_inventory_digest,archive_inventory_digest,
	temporal_evidence_digest,source_revision,deploy_digest,database_identity,
	legacy_db_snapshot,legacy_db_snapshot_digest,canonical_payload,payload_digest,
	issued_at,expires_at`

// AppendAgentFirstRetentionAttestation appends one owner-issued phase event.
// It intentionally has no idempotent adoption rule: the parent has at most one
// child, and a lost response is recovered by reading the immutable ledger
// rather than authorizing a second external observation under the same phase.
func (s *Store) AppendAgentFirstRetentionAttestation(
	ctx context.Context,
	in AgentFirstRetentionAttestationInput,
) (*AgentFirstRetentionAttestationEvent, error) {
	if err := validateAgentFirstRetentionInput(in); err != nil {
		return nil, err
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("store: begin Agent-first retention attestation: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	var parent any
	if in.ParentDigest != "" {
		parent = in.ParentDigest
	}
	row := tx.QueryRow(ctx, `SELECT `+agentFirstRetentionEventColumnsV130+`
		FROM append_agent_first_retention_attestation_v130(
		 $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)`,
		string(in.Phase), parent, in.TemporalClusterID,
		in.TemporalNamespace, in.TemporalNamespaceID, in.RetentionSeconds,
		string(in.HistoryArchivalState), in.HistoryArchiveURIDigest,
		string(in.VisibilityArchivalState), in.VisibilityArchiveURIDigest,
		in.TemporalServerWitness, in.WorkflowInventoryDigest,
		in.ScheduleInventoryDigest, in.ArchiveInventoryDigest,
		in.TemporalEvidenceDigest, in.SourceRevision, in.DeployDigest)
	event, err := scanAgentFirstRetentionEvent(row)
	if err != nil {
		return nil, fmt.Errorf("store: append Agent-first retention attestation: %w", err)
	}
	if err := validateAgentFirstRetentionEvent(event); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("store: commit Agent-first retention attestation: %w", err)
	}
	return event, nil
}

func scanAgentFirstRetentionEvent(row pgx.Row) (*AgentFirstRetentionAttestationEvent, error) {
	var event AgentFirstRetentionAttestationEvent
	err := row.Scan(&event.ID, &event.Phase, &event.ParentDigest,
		&event.TemporalClusterID, &event.TemporalNamespace, &event.TemporalNamespaceID,
		&event.RetentionSeconds, &event.HistoryArchivalState,
		&event.HistoryArchiveURIDigest, &event.VisibilityArchivalState,
		&event.VisibilityArchiveURIDigest, &event.TemporalServerWitness,
		&event.WorkflowInventoryDigest, &event.ScheduleInventoryDigest,
		&event.ArchiveInventoryDigest, &event.TemporalEvidenceDigest,
		&event.SourceRevision, &event.DeployDigest, &event.DatabaseIdentity,
		&event.LegacyDBSnapshot, &event.LegacyDBSnapshotDigest,
		&event.CanonicalPayload, &event.PayloadDigest, &event.IssuedAt,
		&event.ExpiresAt)
	if err != nil {
		return nil, err
	}
	event.LegacyDBSnapshot = bytes.Clone(event.LegacyDBSnapshot)
	event.DatabaseIdentity = bytes.Clone(event.DatabaseIdentity)
	event.CanonicalPayload = bytes.Clone(event.CanonicalPayload)
	return &event, nil
}

func validateAgentFirstRetentionInput(in AgentFirstRetentionAttestationInput) error {
	switch in.Phase {
	case AgentFirstRetentionPhaseBaseline:
		if in.ParentDigest != "" {
			return fmt.Errorf("store: Agent-first baseline parent must be empty")
		}
	case AgentFirstRetentionPhasePrepared:
		if !agentFirstDigestPattern.MatchString(in.ParentDigest) {
			return fmt.Errorf("store: Agent-first retention parent digest is invalid")
		}
	default:
		return fmt.Errorf("store: Agent-first retention phase is invalid")
	}
	if !validBoundedAgentFirstText(in.TemporalClusterID, maxAgentFirstClusterIDBytes) ||
		!validBoundedAgentFirstText(in.TemporalNamespace, maxAgentFirstNamespaceBytes) ||
		!agentFirstNamespaceIDPattern.MatchString(in.TemporalNamespaceID) ||
		in.RetentionSeconds < 1 || in.RetentionSeconds > maxAgentFirstRetentionSeconds ||
		in.TemporalServerWitness.IsZero() ||
		in.TemporalServerWitness.Year() < 1 || in.TemporalServerWitness.Year() > 9999 {
		return fmt.Errorf("store: Agent-first Temporal identity is invalid")
	}
	if in.HistoryArchivalState != in.VisibilityArchivalState ||
		!validAgentFirstArchive(in.HistoryArchivalState, in.HistoryArchiveURIDigest) ||
		!validAgentFirstArchive(in.VisibilityArchivalState, in.VisibilityArchiveURIDigest) {
		return fmt.Errorf("store: Agent-first archival evidence is invalid")
	}
	for _, digest := range []string{in.WorkflowInventoryDigest,
		in.ScheduleInventoryDigest, in.ArchiveInventoryDigest,
		in.TemporalEvidenceDigest, in.DeployDigest} {
		if !agentFirstDigestPattern.MatchString(digest) {
			return fmt.Errorf("store: Agent-first evidence digest is invalid")
		}
	}
	if !agentFirstSourceRevisionPattern.MatchString(in.SourceRevision) {
		return fmt.Errorf("store: Agent-first source revision is invalid")
	}
	return nil
}

func validBoundedAgentFirstText(value string, maximum int) bool {
	return value != "" && strings.TrimSpace(value) == value && utf8.ValidString(value) &&
		len(value) <= maximum
}

func validAgentFirstArchive(state AgentFirstArchivalState, digest string) bool {
	switch state {
	case AgentFirstArchivalDisabled:
		return digest == agentFirstEmptyDigest
	case AgentFirstArchivalEnabled:
		return agentFirstDigestPattern.MatchString(digest) && digest != agentFirstEmptyDigest
	default:
		return false
	}
}

func validateAgentFirstRetentionEvent(event *AgentFirstRetentionAttestationEvent) error {
	if event == nil || event.ID <= 0 {
		return fmt.Errorf("store: Agent-first retention event is invalid")
	}
	if got := sha256.Sum256(event.LegacyDBSnapshot); hex.EncodeToString(got[:]) != event.LegacyDBSnapshotDigest {
		return fmt.Errorf("store: Agent-first legacy DB snapshot digest differs")
	}
	if got := sha256.Sum256(event.CanonicalPayload); hex.EncodeToString(got[:]) != event.PayloadDigest {
		return fmt.Errorf("store: Agent-first retention payload digest differs")
	}
	var databaseIdentity agentFirstDatabaseIdentityV130
	if err := strictjson.DecodeExact(event.DatabaseIdentity, &databaseIdentity); err != nil {
		return fmt.Errorf("store: Agent-first database identity is not exact: %w", err)
	}
	if databaseIdentity.SchemaVersion != "vane.agent-first-database-identity/v130" ||
		databaseIdentity.DatabaseName == "" || databaseIdentity.DatabaseOID <= 0 ||
		databaseIdentity.PGSystemIdentifier == "" || databaseIdentity.ServerVersionNum < 180000 {
		return fmt.Errorf("store: Agent-first database identity fields are invalid")
	}
	var snapshot agentFirstLegacyDBSnapshotV130
	if err := strictjson.DecodeExact(event.LegacyDBSnapshot, &snapshot); err != nil {
		return fmt.Errorf("store: Agent-first legacy DB snapshot is not exact: %w", err)
	}
	if snapshot.SchemaVersion != "vane.agent-first-legacy-db-snapshot/v130" ||
		!agentFirstDigestPattern.MatchString(snapshot.ScheduleInventory.Digest) ||
		!agentFirstDigestPattern.MatchString(snapshot.AuthorityInventory.Digest) ||
		snapshot.LegacyCreation.ActiveCount < 0 || snapshot.LegacyCreation.LeaseCount < 0 ||
		snapshot.LegacyCreation.OperationCount < 0 || snapshot.LegacyCreation.PendingReceiptCount < 0 ||
		snapshot.LegacyCreation.ReceiptCount < 0 || snapshot.LegacyCreation.ReceiptLeaseCount < 0 ||
		snapshot.LegacyCreation.ReceiptGapCount < 0 ||
		!agentFirstDigestPattern.MatchString(snapshot.LegacyCreation.OperationDigest) ||
		!agentFirstDigestPattern.MatchString(snapshot.LegacyCreation.ReceiptDigest) ||
		snapshot.Protocol1DefinitionEdit.ActiveCount < 0 ||
		snapshot.Protocol1DefinitionEdit.LeaseCount < 0 ||
		snapshot.Protocol1DefinitionEdit.OperationCount < 0 ||
		snapshot.Protocol1DefinitionEdit.PendingReceiptCount < 0 ||
		snapshot.Protocol1DefinitionEdit.ReceiptCount < 0 ||
		snapshot.Protocol1DefinitionEdit.ReceiptLeaseCount < 0 ||
		snapshot.Protocol1DefinitionEdit.ReceiptGapCount < 0 ||
		!agentFirstDigestPattern.MatchString(snapshot.Protocol1DefinitionEdit.OperationDigest) ||
		!agentFirstDigestPattern.MatchString(snapshot.Protocol1DefinitionEdit.ReceiptDigest) {
		return fmt.Errorf("store: Agent-first legacy DB snapshot fields are invalid")
	}
	if !agentFirstLegacyLaneQuiescent(snapshot.LegacyCreation) ||
		!agentFirstLegacyLaneQuiescent(snapshot.Protocol1DefinitionEdit) {
		return fmt.Errorf("store: Agent-first legacy DB snapshot is not quiescent")
	}
	var payload agentFirstRetentionPayloadV130
	if err := strictjson.DecodeExact(event.CanonicalPayload, &payload); err != nil {
		return fmt.Errorf("store: Agent-first retention payload is not exact: %w", err)
	}
	issuedAt, err := time.Parse(time.RFC3339Nano, payload.IssuedAt)
	if err != nil {
		return fmt.Errorf("store: Agent-first retention issued_at is invalid: %w", err)
	}
	expiresAt, err := time.Parse(time.RFC3339Nano, payload.ExpiresAt)
	if err != nil {
		return fmt.Errorf("store: Agent-first retention expires_at is invalid: %w", err)
	}
	witness, err := time.Parse(time.RFC3339Nano, payload.TemporalServerWitness)
	if err != nil {
		return fmt.Errorf("store: Agent-first Temporal witness is invalid: %w", err)
	}
	if payload.SchemaVersion != "vane.agent-first-retention-attestation/v130" ||
		payload.Phase != event.Phase || !sameAgentFirstParent(payload.ParentDigest, event.ParentDigest) ||
		payload.TemporalClusterID != event.TemporalClusterID ||
		payload.TemporalNamespace != event.TemporalNamespace ||
		payload.TemporalNamespaceID != event.TemporalNamespaceID ||
		payload.RetentionSeconds != event.RetentionSeconds ||
		payload.HistoryArchivalState != event.HistoryArchivalState ||
		payload.HistoryArchiveURIDigest != event.HistoryArchiveURIDigest ||
		payload.VisibilityArchivalState != event.VisibilityArchivalState ||
		payload.VisibilityArchiveURIDigest != event.VisibilityArchiveURIDigest ||
		payload.WorkflowInventoryDigest != event.WorkflowInventoryDigest ||
		payload.ScheduleInventoryDigest != event.ScheduleInventoryDigest ||
		payload.ArchiveInventoryDigest != event.ArchiveInventoryDigest ||
		payload.TemporalEvidenceDigest != event.TemporalEvidenceDigest ||
		payload.SourceRevision != event.SourceRevision ||
		payload.DeployDigest != event.DeployDigest ||
		payload.DatabaseIdentity != databaseIdentity ||
		payload.LegacyDBSnapshotDigest != event.LegacyDBSnapshotDigest ||
		payload.LegacyDBSnapshot != snapshot ||
		!issuedAt.Equal(event.IssuedAt) || !expiresAt.Equal(event.ExpiresAt) ||
		!witness.Equal(event.TemporalServerWitness) {
		return fmt.Errorf("store: Agent-first retention payload differs from row evidence")
	}
	return nil
}

func sameAgentFirstParent(left, right *string) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func agentFirstLegacyLaneQuiescent(lane agentFirstLegacyLaneSnapshotV130) bool {
	return lane.ActiveCount == 0 && lane.LeaseCount == 0 &&
		lane.ReceiptLeaseCount == 0 && lane.PendingReceiptCount == 0 &&
		lane.ReceiptGapCount == 0
}
