package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
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
	maxAgentFirstAuditSchedules   = 100000
	agentFirstEmptyDigest         = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	// These hashes bootstrap trust in the two catalog verifiers themselves.
	// All other v130/v132 functions, triggers, policies and ACLs are covered by
	// the descriptor. Keeping these two expected definitions in the binary
	// prevents a replaced database verifier from approving its own drift.
	agentFirstFenceDescriptorDefinitionSHA256 = "80c91011df44a29d84f4ed2760153921eb8addd7592dedb0f08e145eed403f4d"
	agentFirstFenceAssertionDefinitionSHA256  = "d9c29f2f930beacfb11280165bde7c01787d893b57e6509e21640151bed1157f"
)

var agentFirstSourceRevisionPattern = regexp.MustCompile(`^[0-9a-f]{40}$`)
var agentFirstDigestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
var agentFirstNamespaceIDPattern = regexp.MustCompile(
	`^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

// ErrAgentFirstRetentionAttestationNotFound lets the offline collector
// distinguish a first append from an unavailable or ambiguous adoption read.
// It is deliberately returned only after an exact external-evidence or
// canonical payload-digest lookup.
var ErrAgentFirstRetentionAttestationNotFound = errors.New(
	"Agent-first retention attestation not found")

// ErrAgentFirstRetentionAttestationStale means an exact immutable event
// exists but is expired or no longer belongs to the latest baseline chain. A
// caller must not turn this into a new append or report it as consumable.
var ErrAgentFirstRetentionAttestationStale = errors.New(
	"Agent-first retention attestation is stale")

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

type AgentFirstLegacyWriteFence struct {
	InstalledAt                 time.Time
	PreexistingAttestationMaxID int64
	DescriptorDigest            string
}

type AgentFirstRetentionAuditSnapshot struct {
	LegacyDBSnapshot       []byte
	LegacyDBSnapshotDigest string
	ScheduleDigest         string
	Schedules              []AgentFirstRetentionSchedule
}

type AgentFirstRetentionSchedule struct {
	ID                         string  `json:"id"`
	TenantID                   int64   `json:"tenant_id"`
	UserID                     int64   `json:"user_id"`
	Status                     string  `json:"status"`
	ExecutionMode              string  `json:"execution_mode"`
	ApprovedDefinitionVersion  *int64  `json:"approved_definition_version"`
	ApprovedDefinitionDigest   *string `json:"approved_definition_digest"`
	DefinitionEditOperationID  *string `json:"definition_edit_operation_id"`
	DefinitionEditFence        *int64  `json:"definition_edit_fence"`
	AuthorityGeneration        *int64  `json:"authority_generation"`
	AuthorityDefinitionVersion *int64  `json:"authority_definition_version"`
	AuthorityDefinitionDigest  *string `json:"authority_definition_digest"`
	TargetActionDigest         *string `json:"target_action_digest"`
	ActionAuthorizationDigest  *string `json:"action_authorization_digest"`
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
	InvalidStateCount   int64  `json:"invalid_state_count"`
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

type agentFirstFenceCatalogQueryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func verifyAgentFirstFenceVerifierCatalog(
	ctx context.Context,
	queryer agentFirstFenceCatalogQueryer,
) error {
	var descriptorDefinition, assertionDefinition string
	if err := queryer.QueryRow(ctx, `SELECT
		encode(sha256(convert_to(pg_get_functiondef(
		 'public.agent_first_legacy_write_fence_descriptor_v132()'::regprocedure),
		 'UTF8')),'hex'),
		encode(sha256(convert_to(pg_get_functiondef(
		 'public.assert_agent_first_legacy_write_fence_v132()'::regprocedure),
		 'UTF8')),'hex')`).Scan(&descriptorDefinition, &assertionDefinition); err != nil {
		return fmt.Errorf("read Agent-first fence verifier catalog: %w", err)
	}
	if descriptorDefinition != agentFirstFenceDescriptorDefinitionSHA256 ||
		assertionDefinition != agentFirstFenceAssertionDefinitionSHA256 {
		return fmt.Errorf("Agent-first fence verifier catalog drifted")
	}
	return nil
}

// AssertAgentFirstLegacyWriteFence proves that migration 132's four physical
// protocol fences are installed and ENABLE ALWAYS. The retention collector is
// owner-operated, but it still refuses to attest an interval after catalog
// drift or a partially applied migration.
func (s *Store) AssertAgentFirstLegacyWriteFence(
	ctx context.Context,
) (*AgentFirstLegacyWriteFence, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store: Agent-first legacy write fence is unavailable")
	}
	if err := verifyAgentFirstFenceVerifierCatalog(ctx, s.pool); err != nil {
		return nil, fmt.Errorf("store: %w", err)
	}
	var fence AgentFirstLegacyWriteFence
	if err := s.pool.QueryRow(ctx,
		`SELECT installed_at,preexisting_attestation_max_id,descriptor_digest
		   FROM public.assert_agent_first_legacy_write_fence_v132()`).Scan(
		&fence.InstalledAt, &fence.PreexistingAttestationMaxID,
		&fence.DescriptorDigest); err != nil {
		return nil, fmt.Errorf("store: assert Agent-first legacy write fence: %w", err)
	}
	if fence.InstalledAt.IsZero() || fence.PreexistingAttestationMaxID < 0 ||
		!agentFirstDigestPattern.MatchString(fence.DescriptorDigest) {
		return nil, fmt.Errorf("store: Agent-first legacy write fence is invalid")
	}
	return &fence, nil
}

// ReadAgentFirstRetentionAuditSnapshot reads the exact v130 semantic snapshot
// and the per-schedule V3 Action authority from one owner-only, repeatable-read
// transaction. It is for the offline deployment collector, never server or
// Agent runtime paths.
func (s *Store) ReadAgentFirstRetentionAuditSnapshot(
	ctx context.Context,
) (*AgentFirstRetentionAuditSnapshot, error) {
	tx, err := s.beginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return nil, fmt.Errorf("store: begin Agent-first audit snapshot: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	var snapshot AgentFirstRetentionAuditSnapshot
	if err := tx.QueryRow(ctx,
		`SELECT public.agent_first_legacy_db_snapshot_v130()`).Scan(
		&snapshot.LegacyDBSnapshot); err != nil {
		return nil, fmt.Errorf("store: read Agent-first semantic snapshot: %w", err)
	}
	var scheduleCount int64
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM public.schedules`).Scan(
		&scheduleCount); err != nil {
		return nil, fmt.Errorf("store: count Agent-first schedules: %w", err)
	}
	if scheduleCount < 0 || scheduleCount > maxAgentFirstAuditSchedules {
		return nil, fmt.Errorf("store: Agent-first schedule inventory exceeds limit")
	}
	rows, err := tx.Query(ctx, `
		SELECT schedule.id,schedule.tenant_id,schedule.user_id,schedule.status,
		       schedule.execution_mode,schedule.approved_definition_version,
		       schedule.approved_definition_digest,schedule.definition_edit_operation_id,
		       schedule.definition_edit_fence,authority.generation,
		       authority.definition_version,authority.definition_digest,
		       authority.target_action_digest,authority.action_authorization_digest
		  FROM public.schedules schedule
		  LEFT JOIN public.research_v3_delivery_authorities authority
		    ON authority.tenant_id=schedule.tenant_id
		   AND authority.user_id=schedule.user_id
		   AND authority.task_id=schedule.id
		   AND authority.status='enabled'
		 ORDER BY schedule.tenant_id,schedule.user_id,schedule.id,authority.generation`)
	if err != nil {
		return nil, fmt.Errorf("store: read Agent-first schedule authority: %w", err)
	}
	defer rows.Close()
	seen := make(map[string]struct{})
	for rows.Next() {
		var schedule AgentFirstRetentionSchedule
		if err := rows.Scan(&schedule.ID, &schedule.TenantID, &schedule.UserID,
			&schedule.Status, &schedule.ExecutionMode,
			&schedule.ApprovedDefinitionVersion, &schedule.ApprovedDefinitionDigest,
			&schedule.DefinitionEditOperationID, &schedule.DefinitionEditFence,
			&schedule.AuthorityGeneration, &schedule.AuthorityDefinitionVersion,
			&schedule.AuthorityDefinitionDigest, &schedule.TargetActionDigest,
			&schedule.ActionAuthorizationDigest); err != nil {
			return nil, fmt.Errorf("store: scan Agent-first schedule authority: %w", err)
		}
		if _, duplicate := seen[schedule.ID]; duplicate {
			return nil, fmt.Errorf("store: multiple enabled authorities for schedule %q", schedule.ID)
		}
		if err := validateAgentFirstRetentionSchedule(schedule); err != nil {
			return nil, err
		}
		seen[schedule.ID] = struct{}{}
		snapshot.Schedules = append(snapshot.Schedules, schedule)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterate Agent-first schedule authority: %w", err)
	}
	if int64(len(snapshot.Schedules)) != scheduleCount {
		return nil, fmt.Errorf("store: Agent-first schedule inventory count differs")
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("store: commit Agent-first audit snapshot: %w", err)
	}
	legacySum := sha256.Sum256(snapshot.LegacyDBSnapshot)
	snapshot.LegacyDBSnapshot = bytes.Clone(snapshot.LegacyDBSnapshot)
	snapshot.LegacyDBSnapshotDigest = hex.EncodeToString(legacySum[:])
	canonicalSchedules, err := json.Marshal(snapshot.Schedules)
	if err != nil {
		return nil, fmt.Errorf("store: marshal Agent-first schedule snapshot: %w", err)
	}
	scheduleSum := sha256.Sum256(canonicalSchedules)
	snapshot.ScheduleDigest = hex.EncodeToString(scheduleSum[:])
	return &snapshot, nil
}

func validateAgentFirstRetentionSchedule(schedule AgentFirstRetentionSchedule) error {
	if !validBoundedAgentFirstText(schedule.ID, maxAgentFirstNamespaceBytes) ||
		schedule.TenantID <= 0 || schedule.UserID <= 0 ||
		(schedule.Status != "active" && schedule.Status != "paused") ||
		schedule.ExecutionMode != "discover_at_run" ||
		schedule.ApprovedDefinitionVersion == nil || *schedule.ApprovedDefinitionVersion <= 0 ||
		schedule.ApprovedDefinitionDigest == nil ||
		!agentFirstDigestPattern.MatchString(*schedule.ApprovedDefinitionDigest) ||
		schedule.DefinitionEditOperationID != nil || schedule.DefinitionEditFence != nil ||
		schedule.AuthorityGeneration == nil || *schedule.AuthorityGeneration <= 0 ||
		schedule.AuthorityDefinitionVersion == nil ||
		*schedule.AuthorityDefinitionVersion != *schedule.ApprovedDefinitionVersion ||
		schedule.AuthorityDefinitionDigest == nil ||
		*schedule.AuthorityDefinitionDigest != *schedule.ApprovedDefinitionDigest ||
		schedule.TargetActionDigest == nil ||
		!agentFirstDigestPattern.MatchString(*schedule.TargetActionDigest) ||
		schedule.ActionAuthorizationDigest == nil ||
		!agentFirstDigestPattern.MatchString(*schedule.ActionAuthorizationDigest) {
		return fmt.Errorf("store: schedule %q lacks exact enabled V3 authority", schedule.ID)
	}
	return nil
}

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

// AppendAgentFirstRetentionAttestationV132 appends through the physical
// legacy-protocol fence and CASes the semantic database snapshot observed by
// the offline collector. It preserves the canonical v130 event bytes.
func (s *Store) AppendAgentFirstRetentionAttestationV132(
	ctx context.Context,
	in AgentFirstRetentionAttestationInput,
	expectedLegacyDBSnapshotDigest string,
) (*AgentFirstRetentionAttestationEvent, error) {
	if err := validateAgentFirstRetentionInput(in); err != nil {
		return nil, err
	}
	if !agentFirstDigestPattern.MatchString(expectedLegacyDBSnapshotDigest) {
		return nil, fmt.Errorf("store: expected Agent-first database snapshot digest is invalid")
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("store: begin fenced Agent-first retention attestation: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := verifyAgentFirstFenceVerifierCatalog(ctx, tx); err != nil {
		return nil, fmt.Errorf("store: %w", err)
	}
	var parent any
	if in.ParentDigest != "" {
		parent = in.ParentDigest
	}
	row := tx.QueryRow(ctx, `SELECT `+agentFirstRetentionEventColumnsV130+`
		FROM append_agent_first_retention_attestation_v132(
		 $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)`,
		string(in.Phase), parent, in.TemporalClusterID,
		in.TemporalNamespace, in.TemporalNamespaceID, in.RetentionSeconds,
		string(in.HistoryArchivalState), in.HistoryArchiveURIDigest,
		string(in.VisibilityArchivalState), in.VisibilityArchiveURIDigest,
		in.TemporalServerWitness, in.WorkflowInventoryDigest,
		in.ScheduleInventoryDigest, in.ArchiveInventoryDigest,
		in.TemporalEvidenceDigest, in.SourceRevision, in.DeployDigest,
		expectedLegacyDBSnapshotDigest)
	event, err := scanAgentFirstRetentionEvent(row)
	if err != nil {
		return nil, fmt.Errorf("store: append fenced Agent-first retention attestation: %w", err)
	}
	if err := validateAgentFirstRetentionEvent(event); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("store: commit fenced Agent-first retention attestation: %w", err)
	}
	return event, nil
}

// LoadAgentFirstRetentionAttestation adopts a committed event after a lost
// append response. Every caller-supplied external evidence field participates
// in the lookup; zero or multiple matches fail closed.
func (s *Store) LoadAgentFirstRetentionAttestation(
	ctx context.Context,
	in AgentFirstRetentionAttestationInput,
) (*AgentFirstRetentionAttestationEvent, error) {
	if err := validateAgentFirstRetentionInput(in); err != nil {
		return nil, err
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("store: begin Agent-first retention adoption: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	// Serialize the chain-head decision with the append function. This is not a
	// replacement for migration132's live cross-system re-audit; it only keeps
	// response-loss adoption from reviving an expired or superseded event.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(6215335020355474130)`); err != nil {
		return nil, fmt.Errorf("store: lock Agent-first retention adoption: %w", err)
	}
	event, err := loadAgentFirstRetentionAttestationTx(ctx, tx, in)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("store: commit Agent-first retention adoption: %w", err)
	}
	return event, nil
}

// LoadAgentFirstRetentionAttestationV132 adopts an exact committed event only
// while the physical legacy write fence and the collector's final semantic DB
// snapshot remain true in the same transaction. This is the response-loss
// counterpart of AppendAgentFirstRetentionAttestationV132.
func (s *Store) LoadAgentFirstRetentionAttestationV132(
	ctx context.Context,
	in AgentFirstRetentionAttestationInput,
	expectedLegacyDBSnapshotDigest string,
) (*AgentFirstRetentionAttestationEvent, error) {
	if err := validateAgentFirstRetentionInput(in); err != nil {
		return nil, err
	}
	if !agentFirstDigestPattern.MatchString(expectedLegacyDBSnapshotDigest) {
		return nil, fmt.Errorf("store: expected Agent-first snapshot digest is invalid")
	}
	// Read Committed is intentional: the advisory lock can wait behind an
	// append whose commit is the response we are adopting. A Repeatable Read
	// snapshot would be frozen before that wait and could never see the row.
	tx, err := s.beginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("store: begin fenced Agent-first retention adoption: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(6215335020355474130)`); err != nil {
		return nil, fmt.Errorf("store: lock fenced Agent-first retention adoption: %w", err)
	}
	if _, err := tx.Exec(ctx, `LOCK TABLE public.schedules,
		public.research_v3_delivery_authorities IN SHARE MODE NOWAIT`); err != nil {
		return nil, fmt.Errorf("store: lock fenced Agent-first live authority: %w", err)
	}
	if _, err := tx.Exec(ctx, `LOCK TABLE
		public.agent_first_retention_attestation_events,
		public.task_creation_operations,public.task_creation_receipts,
		public.task_definition_edit_operations,public.task_definition_edit_receipts
		IN ACCESS SHARE MODE`); err != nil {
		return nil, fmt.Errorf("store: lock fenced Agent-first adoption authority: %w", err)
	}
	if err := verifyAgentFirstFenceVerifierCatalog(ctx, tx); err != nil {
		return nil, fmt.Errorf("store: %w", err)
	}
	var fence AgentFirstLegacyWriteFence
	if err := tx.QueryRow(ctx, `SELECT installed_at,preexisting_attestation_max_id,
		descriptor_digest FROM public.assert_agent_first_legacy_write_fence_v132()`).Scan(
		&fence.InstalledAt, &fence.PreexistingAttestationMaxID,
		&fence.DescriptorDigest); err != nil {
		return nil, fmt.Errorf("store: assert fenced Agent-first adoption: %w", err)
	}
	var actualSnapshotDigest string
	if err := tx.QueryRow(ctx, `SELECT encode(sha256(
		public.agent_first_legacy_db_snapshot_v130()),'hex')`).Scan(
		&actualSnapshotDigest); err != nil {
		return nil, fmt.Errorf("store: recompute fenced Agent-first adoption snapshot: %w", err)
	}
	if actualSnapshotDigest != expectedLegacyDBSnapshotDigest {
		return nil, ErrAgentFirstRetentionAttestationStale
	}
	event, err := loadAgentFirstRetentionAttestationTx(ctx, tx, in)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("store: commit fenced Agent-first retention adoption: %w", err)
	}
	return event, nil
}

func loadAgentFirstRetentionAttestationTx(
	ctx context.Context,
	tx pgx.Tx,
	in AgentFirstRetentionAttestationInput,
) (*AgentFirstRetentionAttestationEvent, error) {
	var parent any
	if in.ParentDigest != "" {
		parent = in.ParentDigest
	}
	rows, err := tx.Query(ctx, `SELECT `+agentFirstRetentionEventColumnsV130+`
		  FROM public.agent_first_retention_attestation_events
		 WHERE phase=$1 AND parent_digest IS NOT DISTINCT FROM $2
		   AND temporal_cluster_id=$3 AND temporal_namespace=$4
		   AND temporal_namespace_id=$5 AND retention_seconds=$6
		   AND history_archival_state=$7 AND history_archive_uri_digest=$8
		   AND visibility_archival_state=$9 AND visibility_archive_uri_digest=$10
		   AND temporal_server_witness=$11 AND workflow_inventory_digest=$12
		   AND schedule_inventory_digest=$13 AND archive_inventory_digest=$14
		   AND temporal_evidence_digest=$15 AND source_revision=$16 AND deploy_digest=$17
		 ORDER BY id DESC LIMIT 2`,
		string(in.Phase), parent, in.TemporalClusterID, in.TemporalNamespace,
		in.TemporalNamespaceID, in.RetentionSeconds, string(in.HistoryArchivalState),
		in.HistoryArchiveURIDigest, string(in.VisibilityArchivalState),
		in.VisibilityArchiveURIDigest, in.TemporalServerWitness,
		in.WorkflowInventoryDigest, in.ScheduleInventoryDigest,
		in.ArchiveInventoryDigest, in.TemporalEvidenceDigest,
		in.SourceRevision, in.DeployDigest)
	if err != nil {
		return nil, fmt.Errorf("store: load Agent-first retention attestation: %w", err)
	}
	defer rows.Close()
	var found []*AgentFirstRetentionAttestationEvent
	for rows.Next() {
		event, err := scanAgentFirstRetentionEvent(rows)
		if err != nil {
			return nil, fmt.Errorf("store: scan Agent-first retention attestation: %w", err)
		}
		if err := validateAgentFirstRetentionEvent(event); err != nil {
			return nil, err
		}
		found = append(found, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterate Agent-first retention attestation: %w", err)
	}
	if len(found) == 0 {
		return nil, ErrAgentFirstRetentionAttestationNotFound
	}
	if len(found) != 1 {
		return nil, fmt.Errorf("store: exact Agent-first retention attestation count is %d", len(found))
	}
	event := found[0]
	var databaseNow time.Time
	var latestBaseline *string
	var parentID *int64
	if err := tx.QueryRow(ctx, `
		SELECT clock_timestamp(),(
			SELECT payload_digest
			  FROM public.agent_first_retention_attestation_events
			 WHERE temporal_cluster_id=$1 AND temporal_namespace=$2
			   AND temporal_namespace_id=$3 AND phase='baseline'
			 ORDER BY id DESC LIMIT 1),(
			SELECT id FROM public.agent_first_retention_attestation_events
			 WHERE payload_digest=$4)`, event.TemporalClusterID,
		event.TemporalNamespace, event.TemporalNamespaceID, event.ParentDigest).Scan(
		&databaseNow, &latestBaseline, &parentID); err != nil {
		return nil, fmt.Errorf("store: read Agent-first retention chain head: %w", err)
	}
	var fenceCutoff int64
	var fenceAvailable bool
	if err := tx.QueryRow(ctx, `SELECT to_regclass(
		'public.agent_first_legacy_protocol_write_fence_v132') IS NOT NULL`).Scan(
		&fenceAvailable); err != nil {
		return nil, fmt.Errorf("store: inspect Agent-first retention fence epoch: %w", err)
	}
	if fenceAvailable {
		if err := tx.QueryRow(ctx, `SELECT preexisting_attestation_max_id
			FROM public.agent_first_legacy_protocol_write_fence_v132
			WHERE singleton`).Scan(&fenceCutoff); err != nil {
			return nil, fmt.Errorf("store: read Agent-first retention fence epoch: %w", err)
		}
	}
	if err := validateAgentFirstRetentionAdoption(
		event, databaseNow, latestBaseline, fenceCutoff, parentID); err != nil {
		return nil, err
	}
	return event, nil
}

func validateAgentFirstRetentionAdoption(
	event *AgentFirstRetentionAttestationEvent,
	databaseNow time.Time,
	latestBaseline *string,
	fenceCutoff int64,
	parentID *int64,
) error {
	if event == nil || databaseNow.IsZero() || latestBaseline == nil ||
		!event.ExpiresAt.After(databaseNow) {
		return ErrAgentFirstRetentionAttestationStale
	}
	if event.Phase == AgentFirstRetentionPhaseBaseline {
		if *latestBaseline != event.PayloadDigest || event.ID <= fenceCutoff {
			return ErrAgentFirstRetentionAttestationStale
		}
		return nil
	}
	if event.Phase != AgentFirstRetentionPhasePrepared || event.ParentDigest == nil ||
		*latestBaseline != *event.ParentDigest || parentID == nil || *parentID <= fenceCutoff {
		return ErrAgentFirstRetentionAttestationStale
	}
	return nil
}

// LoadAgentFirstRetentionAttestationByDigest loads one immutable ledger event
// by its database-generated canonical payload digest. The offline prepared
// collector uses this to bind a content-addressed evidence file to the exact
// baseline row; it does not grant any server or Agent runtime authority.
func (s *Store) LoadAgentFirstRetentionAttestationByDigest(
	ctx context.Context,
	payloadDigest string,
) (*AgentFirstRetentionAttestationEvent, error) {
	if !agentFirstDigestPattern.MatchString(payloadDigest) {
		return nil, fmt.Errorf("store: Agent-first retention payload digest is invalid")
	}
	rows, err := s.pool.Query(ctx, `SELECT `+agentFirstRetentionEventColumnsV130+`
		  FROM public.agent_first_retention_attestation_events
		 WHERE payload_digest=$1 ORDER BY id DESC LIMIT 2`, payloadDigest)
	if err != nil {
		return nil, fmt.Errorf("store: load Agent-first retention attestation by digest: %w", err)
	}
	defer rows.Close()
	var found []*AgentFirstRetentionAttestationEvent
	for rows.Next() {
		event, err := scanAgentFirstRetentionEvent(rows)
		if err != nil {
			return nil, fmt.Errorf("store: scan Agent-first retention attestation by digest: %w", err)
		}
		if err := validateAgentFirstRetentionEvent(event); err != nil {
			return nil, err
		}
		found = append(found, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterate Agent-first retention attestation by digest: %w", err)
	}
	if len(found) == 0 {
		return nil, ErrAgentFirstRetentionAttestationNotFound
	}
	if len(found) != 1 {
		return nil, fmt.Errorf("store: Agent-first retention payload digest count is %d", len(found))
	}
	return found[0], nil
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
		snapshot.ScheduleInventory.ItemCount < 0 ||
		snapshot.AuthorityInventory.ItemCount < 0 ||
		snapshot.LegacyCreation.ActiveCount < 0 || snapshot.LegacyCreation.LeaseCount < 0 ||
		snapshot.LegacyCreation.InvalidStateCount < 0 ||
		snapshot.LegacyCreation.OperationCount < 0 || snapshot.LegacyCreation.PendingReceiptCount < 0 ||
		snapshot.LegacyCreation.ReceiptCount < 0 || snapshot.LegacyCreation.ReceiptLeaseCount < 0 ||
		snapshot.LegacyCreation.ReceiptGapCount < 0 ||
		!agentFirstDigestPattern.MatchString(snapshot.LegacyCreation.OperationDigest) ||
		!agentFirstDigestPattern.MatchString(snapshot.LegacyCreation.ReceiptDigest) ||
		snapshot.Protocol1DefinitionEdit.ActiveCount < 0 ||
		snapshot.Protocol1DefinitionEdit.InvalidStateCount < 0 ||
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
	parentDigest := ""
	if event.ParentDigest != nil {
		parentDigest = *event.ParentDigest
	}
	if err := validateAgentFirstRetentionInput(AgentFirstRetentionAttestationInput{
		Phase: event.Phase, ParentDigest: parentDigest,
		TemporalClusterID:          event.TemporalClusterID,
		TemporalNamespace:          event.TemporalNamespace,
		TemporalNamespaceID:        event.TemporalNamespaceID,
		RetentionSeconds:           event.RetentionSeconds,
		HistoryArchivalState:       event.HistoryArchivalState,
		HistoryArchiveURIDigest:    event.HistoryArchiveURIDigest,
		VisibilityArchivalState:    event.VisibilityArchivalState,
		VisibilityArchiveURIDigest: event.VisibilityArchiveURIDigest,
		TemporalServerWitness:      event.TemporalServerWitness,
		WorkflowInventoryDigest:    event.WorkflowInventoryDigest,
		ScheduleInventoryDigest:    event.ScheduleInventoryDigest,
		ArchiveInventoryDigest:     event.ArchiveInventoryDigest,
		TemporalEvidenceDigest:     event.TemporalEvidenceDigest,
		SourceRevision:             event.SourceRevision, DeployDigest: event.DeployDigest,
	}); err != nil {
		return fmt.Errorf("store: Agent-first retention row fields are invalid: %w", err)
	}
	if event.IssuedAt.IsZero() || event.ExpiresAt.IsZero() ||
		event.TemporalServerWitness.Before(event.IssuedAt.Add(-10*time.Minute)) ||
		event.TemporalServerWitness.After(event.IssuedAt.Add(5*time.Second)) {
		return fmt.Errorf("store: Agent-first retention row time binding is invalid")
	}
	wantLifetime := 10 * time.Minute
	if event.Phase == AgentFirstRetentionPhaseBaseline {
		wantLifetime += time.Duration(event.RetentionSeconds) * time.Second
	}
	if event.ExpiresAt.Sub(event.IssuedAt) != wantLifetime {
		return fmt.Errorf("store: Agent-first retention row expiry is invalid")
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
		lane.InvalidStateCount == 0 &&
		lane.ReceiptLeaseCount == 0 && lane.PendingReceiptCount == 0 &&
		lane.ReceiptGapCount == 0
}
