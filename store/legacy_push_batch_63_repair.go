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
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/internal/strictjson"
	"github.com/YouToco/vane/pusheffect"
	"github.com/YouToco/vane/types"
)

const (
	legacyBatch63ID            int64 = 63
	legacyBatch63EvidenceClass       = "journald_nil_client_before_provider_call/v1"
	legacyBatch63Revision            = "5a82b1350aba467189ba36a90105f6de3d4d65e4"
	legacyBatch63TaskID              = "task-v1-c989c72382e52a2f1f6a8d0deea24bf9b072026ae5c16ce597e8785fa5ac0063"
	legacyBatch63RunID               = "019f95d0-bc42-7ce4-be0d-067c2ed6bdc2"
	legacyBatch63WorkflowID          = "wf-" + legacyBatch63TaskID +
		"-2026-07-24T20:28:32Z"
	legacyBatch63EffectID       = "05daa6d9-8044-59f7-9935-c595533ecb4c"
	legacyBatch63EvidenceDigest = "80bcb17806bf55d8a7d9628663a6fa16d35d9264b6be055a353cf7410774b4c3"
	legacyBatch63CodeDigest     = "0257f221ba710fc4647c2aaddfc12315e3cdb33628abf8f63e2919d556726ba4"
	legacyBatch63JournalDigest  = "eb7a62d278661fbc47622bb3bde62de3a5dcd1e8056f4ce0094657ca7d17bf76"
)

var legacyBatch63DigestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

// LegacyBatch63RepairEvidence is the content-addressed operator adjudication.
// EvidenceBytes are the canonical envelope; the Store hashes the bytes itself
// and never accepts a caller-supplied digest as proof.
type LegacyBatch63RepairEvidence struct {
	CanonicalBytes []byte
}

type legacyBatch63EvidenceWire struct {
	SchemaVersion              string   `json:"schema_version"`
	BatchID                    int64    `json:"batch_id"`
	TaskID                     string   `json:"task_id"`
	TemporalWorkflowID         string   `json:"temporal_workflow_id"`
	TemporalRunID              string   `json:"temporal_run_id"`
	TemporalHistoryDisposition string   `json:"temporal_history_disposition"`
	ServiceRevision            string   `json:"service_revision"`
	ActivityID                 string   `json:"activity_id"`
	Attempt                    int      `json:"attempt"`
	ItemCount                  int      `json:"item_count"`
	ErrorCode                  string   `json:"error_code"`
	ErrorMessage               string   `json:"error_message"`
	Retryable                  bool     `json:"retryable"`
	CodePath                   string   `json:"code_path"`
	CodeExcerpt                string   `json:"code_excerpt"`
	CodeExcerptSHA256          string   `json:"code_excerpt_sha256"`
	JournalLines               []string `json:"journal_lines"`
	JournalSHA256              string   `json:"journal_sha256"`
}

type legacyBatch63DeliveryMaterial struct {
	ID            int64           `json:"id"`
	ContentItemID *int64          `json:"content_item_id"`
	Score         string          `json:"score"`
	BodyMD        string          `json:"body_md"`
	CardJSON      json.RawMessage `json:"card_json"`
	Status        string          `json:"status"`
	MessageID     string          `json:"message_id"`
	SentAt        *time.Time      `json:"sent_at"`
	Title         string          `json:"title"`
	URL           string          `json:"url"`
	PublishedAt   *time.Time      `json:"published_at"`
	CreatedAt     time.Time       `json:"created_at"`
	SourceTitle   string          `json:"source_title"`
	Platform      string          `json:"platform"`
}

type legacyBatch63Material struct {
	BatchID         int64                           `json:"batch_id"`
	TenantID        int64                           `json:"tenant_id"`
	UserID          int64                           `json:"user_id"`
	TaskID          string                          `json:"task_id"`
	SnapshotID      int64                           `json:"snapshot_id"`
	WorkflowID      string                          `json:"workflow_id"`
	RunID           string                          `json:"run_id"`
	BatchStatus     string                          `json:"batch_status"`
	Authority       *string                         `json:"authority"`
	IdempotencyKey  string                          `json:"idempotency_key"`
	Deliveries      []legacyBatch63DeliveryMaterial `json:"deliveries"`
	EffectCount     int64                           `json:"effect_count"`
	EventCount      int64                           `json:"event_count"`
	FeishuAppID     string                          `json:"feishu_app_id"`
	FeishuEnabled   bool                            `json:"feishu_enabled"`
	OwnerOpenID     string                          `json:"owner_open_id"`
	OwnerChatID     string                          `json:"owner_chat_id"`
	OwnerAppID      string                          `json:"owner_app_id"`
	SnapshotPayload []byte                          `json:"snapshot_payload"`
	TaskTitle       string                          `json:"task_title"`
}

type legacyBatch63PlanWire struct {
	SchemaVersion       string `json:"schema_version"`
	MaterialDigest      string `json:"material_digest"`
	EvidenceDigest      string `json:"evidence_digest"`
	EvidenceClass       string `json:"evidence_class"`
	ServiceRevision     string `json:"service_revision"`
	EffectPayloadDigest string `json:"effect_payload_digest"`
	CardDigest          string `json:"card_digest"`
	ExpiresAt           string `json:"expires_at"`
}

type LegacyBatch63RepairPlan struct {
	PlanDigest      string              `json:"plan_digest"`
	MaterialDigest  string              `json:"material_digest"`
	EvidenceDigest  string              `json:"evidence_digest"`
	EvidenceClass   string              `json:"evidence_class"`
	ServiceRevision string              `json:"service_revision"`
	Prepared        pusheffect.Prepared `json:"prepared"`
	PayloadDigest   string              `json:"payload_digest"`
	CardDigest      string              `json:"card_digest"`
	DatabaseNow     time.Time           `json:"database_now"`
	EnableBy        time.Time           `json:"enable_by"`
	ExpiresAt       time.Time           `json:"expires_at"`
}

type LegacyBatch63CardItem struct {
	BodyMD      string
	DeliveryID  int64
	Title       string
	Score       int
	URL         string
	SourceTitle string
	Platform    string
	PublishedAt *time.Time
}

type LegacyBatch63CardInput struct {
	EffectID  string
	TaskTitle string
	Items     []LegacyBatch63CardItem
}

type LegacyBatch63CardBuilder func(LegacyBatch63CardInput) string

type LegacyBatch63RepairStatus struct {
	Phase          string     `json:"phase"`
	PlanDigest     string     `json:"plan_digest"`
	EffectID       string     `json:"effect_id"`
	EffectStatus   string     `json:"effect_status"`
	BatchStatus    string     `json:"batch_status"`
	Authority      string     `json:"authority"`
	EnableBy       *time.Time `json:"enable_by,omitempty"`
	ExpiresAt      *time.Time `json:"expires_at,omitempty"`
	MaterialDigest string     `json:"material_digest,omitempty"`
	EvidenceDigest string     `json:"evidence_digest,omitempty"`
	DatabaseNow    time.Time  `json:"-"`
}

// PreviewLegacyBatch63Repair binds the full physical aggregate, canonical
// evidence bytes and exact immutable effect payload without writing anything.
func (s *Store) PreviewLegacyBatch63Repair(
	ctx context.Context,
	evidence LegacyBatch63RepairEvidence,
	expiresAt time.Time,
	buildCard LegacyBatch63CardBuilder,
) (LegacyBatch63RepairPlan, error) {
	evidenceWire, err := validateLegacyBatch63Evidence(evidence)
	if err != nil {
		return LegacyBatch63RepairPlan{}, err
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return LegacyBatch63RepairPlan{}, legacyBatch63Database("begin preview", err)
	}
	defer rollbackPushEffectTx(ctx, tx)
	material, databaseNow, err := loadLegacyBatch63Material(ctx, tx, false)
	if err != nil {
		return LegacyBatch63RepairPlan{}, err
	}
	plan, err := buildLegacyBatch63Plan(
		material, databaseNow, evidenceWire, evidence, expiresAt, buildCard)
	if err != nil {
		return LegacyBatch63RepairPlan{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return LegacyBatch63RepairPlan{}, legacyBatch63Database("commit preview", err)
	}
	return plan, nil
}

func buildLegacyBatch63Plan(
	material legacyBatch63Material,
	databaseNow time.Time,
	evidenceWire *legacyBatch63EvidenceWire,
	evidence LegacyBatch63RepairEvidence,
	expiresAt time.Time,
	buildCard LegacyBatch63CardBuilder,
) (LegacyBatch63RepairPlan, error) {
	prepared, err := buildLegacyBatch63Prepared(
		material, databaseNow, expiresAt, buildCard)
	if err != nil {
		return LegacyBatch63RepairPlan{}, err
	}
	canonical, err := pusheffect.Canonicalize(prepared)
	if err != nil {
		return LegacyBatch63RepairPlan{}, legacyBatch63Validation(
			"repair effect payload is invalid")
	}
	materialBytes, err := json.Marshal(material)
	if err != nil {
		return LegacyBatch63RepairPlan{}, legacyBatch63Database(
			"encode material", err)
	}
	materialDigest := legacyBatch63Digest(materialBytes)
	canonicalEvidence, err := json.Marshal(evidenceWire)
	if err != nil {
		return LegacyBatch63RepairPlan{}, legacyBatch63Database(
			"encode canonical evidence", err)
	}
	evidenceDigest := legacyBatch63Digest(canonicalEvidence)
	wire := legacyBatch63PlanWire{
		SchemaVersion:       "vane.legacy-batch63-repair-plan/v1",
		MaterialDigest:      materialDigest,
		EvidenceDigest:      evidenceDigest,
		EvidenceClass:       legacyBatch63EvidenceClass,
		ServiceRevision:     evidenceWire.ServiceRevision,
		EffectPayloadDigest: canonical.Digest(),
		CardDigest:          canonical.CardDigest(),
		ExpiresAt:           prepared.IdempotencyExpiresAt.Format(time.RFC3339Nano),
	}
	planBytes, err := json.Marshal(wire)
	if err != nil {
		return LegacyBatch63RepairPlan{}, legacyBatch63Database(
			"encode plan", err)
	}
	return LegacyBatch63RepairPlan{
		PlanDigest:      legacyBatch63Digest(planBytes),
		MaterialDigest:  materialDigest,
		EvidenceDigest:  evidenceDigest,
		EvidenceClass:   legacyBatch63EvidenceClass,
		ServiceRevision: evidenceWire.ServiceRevision,
		Prepared:        cloneLegacyBatch63Prepared(prepared),
		PayloadDigest:   canonical.Digest(),
		CardDigest:      canonical.CardDigest(),
		DatabaseNow:     databaseNow,
		EnableBy:        legacyBatch63EnableBy(databaseNow, expiresAt),
		ExpiresAt:       expiresAt,
	}, nil
}

// FinalizeLegacyBatch63Repair re-previews under the current database state,
// rejects any drift, then invokes the physically pinned SECURITY DEFINER
// transition. Effect creation, batch activation and finalized audit are one
// database transaction.
func (s *Store) FinalizeLegacyBatch63Repair(
	ctx context.Context,
	expectedPlanDigest string,
	evidence LegacyBatch63RepairEvidence,
	expiresAt time.Time,
	buildCard LegacyBatch63CardBuilder,
) (LegacyBatch63RepairStatus, error) {
	evidenceWire, err := validateLegacyBatch63Evidence(evidence)
	if err != nil {
		return LegacyBatch63RepairStatus{}, err
	}
	if status, replay := s.verifyLegacyBatch63FinalizeReplay(
		ctx, expectedPlanDigest, evidence, expiresAt); replay {
		return status, nil
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return LegacyBatch63RepairStatus{}, legacyBatch63Database(
			"begin finalize transaction", err)
	}
	defer rollbackPushEffectTx(ctx, tx)
	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(6215335020355474248)`); err != nil {
		return LegacyBatch63RepairStatus{}, legacyBatch63Database(
			"lock finalize repair", err)
	}
	var alreadyFinalized bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM legacy_batch63_repair_events
			 WHERE batch_id=63 AND phase='finalized'
		)`,
	).Scan(&alreadyFinalized); err != nil {
		return LegacyBatch63RepairStatus{}, legacyBatch63Database(
			"check finalized replay", err)
	}
	if alreadyFinalized {
		if err := tx.Rollback(ctx); err != nil {
			return LegacyBatch63RepairStatus{}, legacyBatch63Database(
				"release finalized replay lock", err)
		}
		if status, replay := s.verifyLegacyBatch63FinalizeReplay(
			ctx, expectedPlanDigest, evidence, expiresAt); replay {
			return status, nil
		}
		return LegacyBatch63RepairStatus{}, legacyBatch63Conflict(
			"finalized repair replay drifted")
	}
	material, databaseNow, err := loadLegacyBatch63Material(ctx, tx, true)
	if err != nil {
		return LegacyBatch63RepairStatus{}, err
	}
	preview, err := buildLegacyBatch63Plan(
		material, databaseNow, evidenceWire, evidence, expiresAt, buildCard)
	if err != nil {
		return LegacyBatch63RepairStatus{}, err
	}
	if preview.PlanDigest != expectedPlanDigest {
		return LegacyBatch63RepairStatus{}, legacyBatch63Conflict(
			"repair plan digest drifted")
	}
	prepared := preview.Prepared
	canonical, err := pusheffect.Canonicalize(prepared)
	if err != nil {
		return LegacyBatch63RepairStatus{}, legacyBatch63Validation(
			"repair effect payload is invalid")
	}
	if _, err := tx.Exec(ctx,
		`SET LOCAL ROLE vane_legacy_batch63_repair`); err != nil {
		return LegacyBatch63RepairStatus{}, legacyBatch63Database(
			"activate repair role", err)
	}
	var phase string
	var enableBy time.Time
	err = tx.QueryRow(ctx, `
		SELECT phase,enable_by
		  FROM finalize_legacy_push_batch_63_v1(
		    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,
		    $17,$18,$19,$20,$21::uuid,$22
		  )`,
		expectedPlanDigest, preview.MaterialDigest, preview.EvidenceDigest,
		legacyBatch63EvidenceClass, legacyBatch63Revision,
		prepared.TenantID, prepared.UserID, prepared.TaskID,
		prepared.RunSnapshotID, prepared.RunID, prepared.ID,
		prepared.DeliveryIDs, canonical.Payload(), canonical.Digest(),
		prepared.Card, canonical.CardDigest(), prepared.Provider,
		prepared.AppIdentity, prepared.ProviderChatID, prepared.Target,
		prepared.ProviderUUID, prepared.IdempotencyExpiresAt,
	).Scan(&phase, &enableBy)
	if err != nil {
		return LegacyBatch63RepairStatus{}, legacyBatch63Database("finalize repair", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return LegacyBatch63RepairStatus{}, legacyBatch63Database(
			"commit finalize repair", err)
	}
	status, err := s.VerifyLegacyBatch63Repair(ctx)
	if err != nil {
		return LegacyBatch63RepairStatus{}, err
	}
	status.EnableBy = &enableBy
	return status, nil
}

func (s *Store) verifyLegacyBatch63FinalizeReplay(
	ctx context.Context,
	expectedPlanDigest string,
	evidence LegacyBatch63RepairEvidence,
	expiresAt time.Time,
) (LegacyBatch63RepairStatus, bool) {
	status, err := s.VerifyLegacyBatch63Repair(ctx)
	if err != nil || status.Phase != "finalized" ||
		status.PlanDigest != expectedPlanDigest ||
		status.EvidenceDigest != legacyBatch63EvidenceDigest ||
		status.ExpiresAt == nil || !status.ExpiresAt.Equal(expiresAt) {
		return LegacyBatch63RepairStatus{}, false
	}
	return status, true
}

func (s *Store) AbortLegacyBatch63Repair(
	ctx context.Context,
	expectedPlanDigest string,
) (LegacyBatch63RepairStatus, error) {
	if !legacyBatch63ValidDigest(expectedPlanDigest) {
		return LegacyBatch63RepairStatus{}, legacyBatch63Validation(
			"repair plan digest is invalid")
	}
	tx, err := s.beginLegacyBatch63RepairTx(ctx)
	if err != nil {
		return LegacyBatch63RepairStatus{}, err
	}
	defer rollbackPushEffectTx(ctx, tx)
	var phase string
	if err := tx.QueryRow(ctx,
		`SELECT abort_legacy_push_batch_63_v1($1)`,
		expectedPlanDigest).Scan(&phase); err != nil {
		return LegacyBatch63RepairStatus{}, legacyBatch63Database(
			"abort repair", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return LegacyBatch63RepairStatus{}, legacyBatch63Database(
			"commit abort repair", err)
	}
	return s.VerifyLegacyBatch63Repair(ctx)
}

func (s *Store) VerifyLegacyBatch63Repair(
	ctx context.Context,
) (LegacyBatch63RepairStatus, error) {
	var result LegacyBatch63RepairStatus
	var enableBy, expiresAt *time.Time
	var integrity bool
	err := s.pool.QueryRow(ctx, `
		SELECT COALESCE(e.phase,'absent'),
		       COALESCE(e.plan_digest,''),
		       COALESCE(e.effect_id,''),COALESCE(e.material_digest,''),
		       COALESCE(e.evidence_digest,''),
		       COALESCE(pe.status,''),
		       COALESCE(b.status,''),
		       COALESCE(b.delivery_authority,''),
		       e.enable_by,e.idempotency_expires_at,
		       CASE
		         WHEN e.phase IS NULL THEN
		           b.status='failed' AND b.delivery_authority IS NULL AND
		           NOT EXISTS (
		             SELECT 1 FROM push_effects existing
		              WHERE existing.batch_id=63
		           )
		         WHEN e.phase='finalized' THEN
		           b.delivery_authority='effect' AND
		           pe.id=e.effect_id AND
		           pe.id='05daa6d9-8044-59f7-9935-c595533ecb4c' AND
		           pe.batch_id=63 AND pe.tenant_id=1 AND pe.user_id=1 AND
		           pe.task_id=$1 AND pe.run_snapshot_id=3 AND pe.run_id=$2 AND
		           pe.step_id='push-legacy-batch63-repair/v1' AND
		           pe.chunk_index=0 AND pe.chunk_count=1 AND
		           pe.delivery_ids=ARRAY[202,203,204,205,206]::bigint[] AND
		           pe.provider='feishu' AND
		           (
		             pe.status IN (
		               'prepared','sending','definite_failed','ambiguous','sent'
		             ) OR
		             (
		               pe.status='blocked' AND pe.attempt>=8 AND
		               pe.fence>0 AND
		               pe.failure_class='attempt_budget_exhausted'
		             ) OR
		             (
		               pe.status='blocked' AND pe.attempt>0 AND
		               pe.fence>0 AND
		               pe.failure_class='provider_window_expired_no_send'
		             )
		           ) AND
		           pe.canonical_payload=e.canonical_payload AND
		           pe.payload_digest=e.payload_digest AND
		           pe.card_digest=e.card_digest AND
		           pe.idempotency_expires_at=e.idempotency_expires_at AND
		           e.effect_id='05daa6d9-8044-59f7-9935-c595533ecb4c' AND
		           e.evidence_digest=$3 AND
		           NOT EXISTS (
		             SELECT 1 FROM push_effects other
		              WHERE other.batch_id=63 AND other.id<>pe.id
		           )
		         WHEN e.phase='blocked' THEN
		           b.status='failed' AND b.delivery_authority='effect' AND
		           pe.status='blocked' AND pe.fence=1 AND pe.attempt=1 AND
		           pe.failure_class='operator_enable_deadline_missed' AND
		           pe.id=e.effect_id AND
		           pe.id='05daa6d9-8044-59f7-9935-c595533ecb4c' AND
		           pe.batch_id=63 AND pe.tenant_id=1 AND pe.user_id=1 AND
		           pe.task_id=$1 AND pe.run_snapshot_id=3 AND pe.run_id=$2 AND
		           pe.step_id='push-legacy-batch63-repair/v1' AND
		           pe.delivery_ids=ARRAY[202,203,204,205,206]::bigint[] AND
		           pe.canonical_payload=e.canonical_payload AND
		           pe.payload_digest=e.payload_digest AND
		           pe.card_digest=e.card_digest AND
		           e.evidence_digest=$3
		         ELSE FALSE
		       END,
		       clock_timestamp()
		  FROM push_batches b
		  LEFT JOIN LATERAL (
		    SELECT phase,plan_digest,effect_id,material_digest,evidence_digest,
		           canonical_payload,payload_digest,card_digest,
		           enable_by,idempotency_expires_at
		      FROM legacy_batch63_repair_events
		     WHERE batch_id=63
		     ORDER BY id DESC LIMIT 1
		  ) e ON true
		  LEFT JOIN push_effects pe
		    ON pe.batch_id=63 AND pe.id=e.effect_id
		 WHERE b.id=63`,
		legacyBatch63TaskID, legacyBatch63RunID, legacyBatch63EvidenceDigest,
	).Scan(&result.Phase, &result.PlanDigest, &result.EffectID,
		&result.MaterialDigest, &result.EvidenceDigest,
		&result.EffectStatus, &result.BatchStatus, &result.Authority,
		&enableBy, &expiresAt, &integrity, &result.DatabaseNow)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return LegacyBatch63RepairStatus{}, legacyBatch63Conflict(
				"physical batch 63 is absent")
		}
		return LegacyBatch63RepairStatus{}, legacyBatch63Database(
			"verify repair", err)
	}
	if !integrity {
		return LegacyBatch63RepairStatus{}, legacyBatch63Conflict(
			"repair aggregate integrity drifted")
	}
	result.EnableBy, result.ExpiresAt = enableBy, expiresAt
	return result, nil
}

func (s *Store) beginLegacyBatch63RepairTx(
	ctx context.Context,
) (pgx.Tx, error) {
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, legacyBatch63Database("begin repair transaction", err)
	}
	if _, err := tx.Exec(ctx,
		`SET LOCAL ROLE vane_legacy_batch63_repair`); err != nil {
		_ = tx.Rollback(ctx)
		return nil, legacyBatch63Database("activate repair role", err)
	}
	return tx, nil
}

func loadLegacyBatch63Material(
	ctx context.Context,
	tx pgx.Tx,
	lock bool,
) (legacyBatch63Material, time.Time, error) {
	var material legacyBatch63Material
	var databaseNow time.Time
	var feishuRaw, ownerRaw []byte
	settingSuffix := ""
	batchSuffix := ""
	deliverySuffix := ""
	sourceSuffix := ""
	if lock {
		if _, err := tx.Exec(ctx,
			`LOCK TABLE content_sources IN SHARE MODE`); err != nil {
			return material, time.Time{}, legacyBatch63Database(
				"lock repair source attribution", err)
		}
		settingSuffix = " FOR SHARE"
		batchSuffix = " FOR UPDATE OF b FOR SHARE OF s"
		deliverySuffix = " FOR UPDATE OF d FOR SHARE OF c"
		sourceSuffix = " FOR SHARE"
	}
	if err := tx.QueryRow(ctx,
		`SELECT value FROM settings WHERE key='feishu'`+settingSuffix,
	).Scan(&feishuRaw); err != nil {
		return material, time.Time{}, legacyBatch63Conflict(
			"provider setting is unavailable")
	}
	if err := tx.QueryRow(ctx,
		`SELECT value FROM settings WHERE key='feishu_owner'`+settingSuffix,
	).Scan(&ownerRaw); err != nil {
		return material, time.Time{}, legacyBatch63Conflict(
			"owner setting is unavailable")
	}
	var (
		snapshot taskRunSnapshot
		rawMode  string
	)
	err := tx.QueryRow(ctx, `
		SELECT b.id,b.tenant_id,b.user_id,s.task_id,s.id,
		       s.temporal_workflow_id,s.temporal_run_id,b.status,
		       b.delivery_authority,b.idempotency_key,
		       s.run_kind,s.execution_mode,s.adaptive_version,
		       s.capability_catalog_digest,s.tool_policy_digest,
		       s.prompt_policy_digest,s.model_policy_digest,
		       s.quota_policy_digest,s.definition_digest,s.plan_digest,
		       s.payload_digest,s.reference_digest,s.reference_schema_version,
		       s.payload,s.budget,
		       clock_timestamp(),
		       (SELECT count(*) FROM push_effects WHERE batch_id=63),
		       (SELECT count(*)
		          FROM task_observed_events oe
		          JOIN deliveries od ON od.id=oe.delivery_id
		         WHERE od.batch_id=63)
		  FROM push_batches b
		  JOIN task_run_snapshots s ON s.id=b.run_snapshot_id
		 WHERE b.id=63`+batchSuffix,
	).Scan(&material.BatchID, &material.TenantID, &material.UserID,
		&material.TaskID, &material.SnapshotID, &material.WorkflowID,
		&material.RunID, &material.BatchStatus, &material.Authority,
		&material.IdempotencyKey, &snapshot.RunKind, &rawMode,
		&snapshot.AdaptiveVersion, &snapshot.CapabilityCatalogDigest,
		&snapshot.ToolPolicyDigest, &snapshot.PromptPolicyDigest,
		&snapshot.ModelPolicyDigest, &snapshot.QuotaPolicyDigest,
		&snapshot.DefinitionDigest, &snapshot.PlanDigest,
		&snapshot.PayloadDigest, &snapshot.ReferenceDigest,
		&snapshot.ReferenceSchemaVersion, &material.SnapshotPayload,
		&snapshot.BudgetJSON,
		&databaseNow, &material.EffectCount, &material.EventCount)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return material, time.Time{}, legacyBatch63Conflict(
				"physical batch 63 is absent")
		}
		return material, time.Time{}, legacyBatch63Database(
			"load repair material", err)
	}
	var feishu struct {
		AppID   string `json:"app_id"`
		Enabled bool   `json:"enabled"`
	}
	var owner struct {
		OpenID      string `json:"open_id"`
		ChatID      string `json:"chat_id"`
		AppIdentity string `json:"app_identity"`
	}
	if json.Unmarshal(feishuRaw, &feishu) != nil ||
		json.Unmarshal(ownerRaw, &owner) != nil {
		return material, time.Time{}, legacyBatch63Conflict(
			"provider settings are invalid")
	}
	material.FeishuAppID, material.FeishuEnabled = feishu.AppID, feishu.Enabled
	material.OwnerOpenID, material.OwnerChatID = owner.OpenID, owner.ChatID
	material.OwnerAppID = owner.AppIdentity
	mode, err := types.ParseExecutionMode(rawMode)
	if err != nil {
		return material, time.Time{}, legacyBatch63Conflict(
			"frozen run snapshot mode is invalid")
	}
	snapshot.ID, snapshot.TenantID, snapshot.UserID = material.SnapshotID,
		material.TenantID, material.UserID
	snapshot.TaskID, snapshot.TemporalWorkflowID, snapshot.TemporalRunID =
		material.TaskID, material.WorkflowID, material.RunID
	snapshot.Mode, snapshot.Payload = mode, material.SnapshotPayload
	decoded, err := readTaskRunSnapshotPayload(material.SnapshotPayload)
	if err != nil || decoded.Payload == nil ||
		decoded.Payload.Definition == nil ||
		decoded.Payload.TaskID != legacyBatch63TaskID ||
		decoded.Payload.TenantID != material.TenantID ||
		decoded.Payload.UserID != material.UserID ||
		decoded.DefinitionDigest != snapshot.DefinitionDigest ||
		decoded.PlanDigest != snapshot.PlanDigest ||
		decoded.PolicyDigests.CapabilityCatalog !=
			snapshot.CapabilityCatalogDigest ||
		decoded.PolicyDigests.ToolPolicy != snapshot.ToolPolicyDigest ||
		decoded.PolicyDigests.PromptPolicy != snapshot.PromptPolicyDigest ||
		decoded.PolicyDigests.ModelPolicy != snapshot.ModelPolicyDigest ||
		decoded.PolicyDigests.QuotaPolicy != snapshot.QuotaPolicyDigest ||
		legacyBatch63Digest(decoded.Canonical) != snapshot.PayloadDigest {
		return material, time.Time{}, legacyBatch63Conflict(
			"frozen run snapshot payload is invalid")
	}
	if _, err := snapshot.safeRef(); err != nil {
		return material, time.Time{}, legacyBatch63Conflict(
			fmt.Sprintf("frozen run snapshot reference is invalid: %v", err))
	}
	material.TaskTitle = decoded.Payload.Definition.NLDescription
	frozenSources := make(map[int64]taskRunSourceIdentityV1,
		len(decoded.Payload.Definition.Sources))
	frozenSourceIDs := make([]int64, 0,
		len(decoded.Payload.Definition.Sources))
	for _, source := range decoded.Payload.Definition.Sources {
		frozenSources[source.SourceID] = source
		frozenSourceIDs = append(frozenSourceIDs, source.SourceID)
	}
	if len(frozenSourceIDs) == 0 {
		return material, time.Time{}, legacyBatch63Conflict(
			"frozen source set is empty")
	}
	rows, err := tx.Query(ctx, `
		SELECT d.id,d.content_item_id,d.score::text,d.body_md,d.card_json,
		       d.status,d.feishu_message_id,d.sent_at,
		       COALESCE(c.title,''),COALESCE(c.url,''),c.published_at,
		       d.created_at
		  FROM deliveries d
		  JOIN content_items c ON c.id=d.content_item_id
		 WHERE d.batch_id=63 ORDER BY d.id`+deliverySuffix)
	if err != nil {
		return material, time.Time{}, legacyBatch63Database(
			"load repair deliveries", err)
	}
	defer rows.Close()
	for rows.Next() {
		var item legacyBatch63DeliveryMaterial
		if err := rows.Scan(&item.ID, &item.ContentItemID, &item.Score,
			&item.BodyMD, &item.CardJSON, &item.Status, &item.MessageID,
			&item.SentAt, &item.Title, &item.URL, &item.PublishedAt,
			&item.CreatedAt); err != nil {
			return material, time.Time{}, legacyBatch63Database(
				"scan repair delivery", err)
		}
		item.CardJSON = slices.Clone(item.CardJSON)
		material.Deliveries = append(material.Deliveries, item)
	}
	if err := rows.Err(); err != nil {
		return material, time.Time{}, legacyBatch63Database(
			"iterate repair deliveries", err)
	}
	rows.Close()
	for index := range material.Deliveries {
		item := &material.Deliveries[index]
		if item.ContentItemID == nil {
			return material, time.Time{}, legacyBatch63Conflict(
				"repair content identity is unavailable")
		}
		var sourceID int64
		if err := tx.QueryRow(ctx, `
			SELECT source_id
			  FROM content_sources
			 WHERE content_item_id=$1 AND source_id=ANY($2)
			   AND first_seen_at<=$3
			 ORDER BY source_id LIMIT 1`+sourceSuffix,
			*item.ContentItemID, frozenSourceIDs, item.CreatedAt,
		).Scan(&sourceID); err != nil {
			return material, time.Time{}, legacyBatch63Conflict(
				"repair content has no frozen source attribution")
		}
		source := frozenSources[sourceID]
		item.SourceTitle = source.Title
		item.Platform = source.Platform
	}
	return material, databaseNow.UTC(), nil
}

func buildLegacyBatch63Prepared(
	material legacyBatch63Material,
	databaseNow time.Time,
	expiresAt time.Time,
	buildCard LegacyBatch63CardBuilder,
) (pusheffect.Prepared, error) {
	if material.BatchID != legacyBatch63ID ||
		material.BatchStatus != string(types.BatchStatusFailed) ||
		material.Authority != nil || material.EffectCount != 0 ||
		material.EventCount != 0 || len(material.Deliveries) != 5 {
		return pusheffect.Prepared{},
			legacyBatch63Conflict("repair aggregate is not admissible")
	}
	deliveryIDs := make([]int64, len(material.Deliveries))
	items := make([]LegacyBatch63CardItem, len(material.Deliveries))
	for i, delivery := range material.Deliveries {
		if delivery.Status != string(types.DeliveryStatusPending) ||
			delivery.MessageID != "" || delivery.SentAt != nil ||
			!bytes.Equal(bytes.TrimSpace(delivery.CardJSON), []byte(`{}`)) {
			return pusheffect.Prepared{},
				legacyBatch63Conflict("repair delivery state drifted")
		}
		deliveryIDs[i] = delivery.ID
		score, err := strconv.ParseFloat(delivery.Score, 64)
		if err != nil {
			return pusheffect.Prepared{},
				legacyBatch63Conflict("repair delivery score is invalid")
		}
		items[i] = LegacyBatch63CardItem{
			BodyMD: delivery.BodyMD, DeliveryID: delivery.ID,
			Title: delivery.Title, Score: int(score),
			URL: delivery.URL, SourceTitle: delivery.SourceTitle,
			Platform:    delivery.Platform,
			PublishedAt: delivery.PublishedAt,
		}
	}
	if material.TenantID != 1 || material.UserID != 1 ||
		material.SnapshotID != 3 ||
		!slices.Equal(deliveryIDs, []int64{202, 203, 204, 205, 206}) ||
		material.TaskID != legacyBatch63TaskID ||
		material.RunID != legacyBatch63RunID ||
		material.WorkflowID != legacyBatch63WorkflowID ||
		material.OwnerAppID != material.FeishuAppID ||
		!material.FeishuEnabled ||
		expiresAt.Location() != time.UTC ||
		!expiresAt.Equal(expiresAt.Truncate(time.Microsecond)) ||
		expiresAt.Before(databaseNow.Add(45*time.Minute)) ||
		expiresAt.After(databaseNow.Add(time.Hour)) ||
		buildCard == nil {
		return pusheffect.Prepared{}, legacyBatch63Conflict(
			"repair immutable plan does not match live material")
	}
	card := []byte(buildCard(LegacyBatch63CardInput{
		EffectID:  legacyBatch63EffectID,
		TaskTitle: material.TaskTitle,
		Items:     items,
	}))
	prepared := pusheffect.Prepared{
		ID: legacyBatch63EffectID, TenantID: material.TenantID,
		UserID: material.UserID, TaskID: material.TaskID,
		RunSnapshotID: material.SnapshotID, RunID: material.RunID,
		StepID:     "push-legacy-batch63-repair/v1",
		ChunkIndex: 0, ChunkCount: 1, BatchID: legacyBatch63ID,
		DeliveryIDs: deliveryIDs, Provider: "feishu",
		AppIdentity:    material.FeishuAppID,
		ProviderChatID: material.OwnerChatID, Target: material.OwnerOpenID,
		Card: card, ProviderUUID: legacyBatch63EffectID,
		IdempotencyExpiresAt: expiresAt,
	}
	if err := validateLegacyBatch63Card(
		prepared.Card, prepared.ID, deliveryIDs); err != nil {
		return pusheffect.Prepared{}, err
	}
	return prepared, nil
}

func validateLegacyBatch63Card(
	card []byte,
	effectID string,
	deliveryIDs []int64,
) error {
	var decoded any
	if err := strictjson.Decode(card, &decoded); err != nil {
		return legacyBatch63Validation("repair card is not strict JSON")
	}
	effects := make(map[string]struct{})
	deliveries := make(map[int64]struct{})
	var walk func(any) error
	walk = func(value any) error {
		switch typed := value.(type) {
		case map[string]any:
			for key, child := range typed {
				switch key {
				case "effect_id":
					text, ok := child.(string)
					if !ok || text == "" {
						return errors.New("invalid effect marker")
					}
					effects[text] = struct{}{}
				case "delivery_id":
					var (
						id  int64
						err error
					)
					switch scalar := child.(type) {
					case string:
						id, err = strconv.ParseInt(scalar, 10, 64)
					case json.Number:
						id, err = scalar.Int64()
					case float64:
						id = int64(scalar)
						if float64(id) != scalar {
							err = errors.New("non-integer delivery")
						}
					default:
						err = errors.New("invalid delivery marker")
					}
					if err != nil || id <= 0 {
						return errors.New("invalid delivery marker")
					}
					deliveries[id] = struct{}{}
				}
				if err := walk(child); err != nil {
					return err
				}
			}
		case []any:
			for _, child := range typed {
				if err := walk(child); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := walk(decoded); err != nil {
		return legacyBatch63Validation("repair card marker is invalid")
	}
	if len(effects) != 1 {
		return legacyBatch63Conflict("repair card effect marker set drifted")
	}
	if _, ok := effects[effectID]; !ok || len(deliveries) != len(deliveryIDs) {
		return legacyBatch63Conflict("repair card aggregate marker set drifted")
	}
	for _, id := range deliveryIDs {
		if _, ok := deliveries[id]; !ok {
			return legacyBatch63Conflict("repair card delivery marker set drifted")
		}
	}
	return nil
}

func validateLegacyBatch63Evidence(
	e LegacyBatch63RepairEvidence,
) (*legacyBatch63EvidenceWire, error) {
	if len(e.CanonicalBytes) == 0 || len(e.CanonicalBytes) > 1<<20 {
		return nil, legacyBatch63Validation("repair evidence is invalid")
	}
	var wire *legacyBatch63EvidenceWire
	if err := strictjson.Decode(e.CanonicalBytes, &wire); err != nil ||
		wire == nil {
		return nil, legacyBatch63Validation(
			"repair evidence is not strict canonical JSON")
	}
	canonical, err := json.Marshal(wire)
	if err != nil || !bytes.Equal(bytes.TrimSpace(e.CanonicalBytes), canonical) {
		return nil, legacyBatch63Validation("repair evidence bytes are not canonical")
	}
	if wire.SchemaVersion != "vane.legacy-batch63-repair-evidence/v1" ||
		wire.BatchID != legacyBatch63ID ||
		wire.TaskID != legacyBatch63TaskID ||
		wire.TemporalRunID != legacyBatch63RunID ||
		wire.TemporalWorkflowID != legacyBatch63WorkflowID ||
		wire.TemporalHistoryDisposition != "expired_not_found" ||
		wire.ServiceRevision != legacyBatch63Revision ||
		wire.ActivityID != "52" || wire.Attempt != 1 ||
		wire.ItemCount != 5 || wire.ErrorCode != "CONFLICT" ||
		wire.ErrorMessage != "飞书通道未连接，无法主动推送" ||
		wire.Retryable || wire.CodePath != "feishu/push.go" ||
		len(wire.JournalLines) < 2 ||
		!legacyBatch63DigestPattern.MatchString(wire.CodeExcerptSHA256) ||
		!legacyBatch63DigestPattern.MatchString(wire.JournalSHA256) {
		return nil, legacyBatch63Validation("repair evidence identity is incomplete")
	}
	if legacyBatch63Digest(canonical) != legacyBatch63EvidenceDigest ||
		wire.CodeExcerptSHA256 != legacyBatch63CodeDigest ||
		wire.JournalSHA256 != legacyBatch63JournalDigest {
		return nil, legacyBatch63Validation(
			"repair evidence is not the adjudicated artifact")
	}
	if legacyBatch63Digest([]byte(wire.CodeExcerpt)) !=
		wire.CodeExcerptSHA256 ||
		legacyBatch63Digest([]byte(strings.Join(wire.JournalLines, "\n"))) !=
			wire.JournalSHA256 {
		return nil, legacyBatch63Validation("repair evidence content digest drifted")
	}
	nilGuard := strings.Index(wire.CodeExcerpt, "if client == nil")
	conflictReturn := strings.Index(wire.CodeExcerpt,
		"飞书通道未连接，无法主动推送")
	providerCall := strings.Index(wire.CodeExcerpt,
		"client.Im.Message.Create")
	if nilGuard < 0 || conflictReturn <= nilGuard ||
		providerCall <= conflictReturn {
		return nil, legacyBatch63Validation(
			"repair code proof does not precede the provider call")
	}
	journal := strings.Join(wire.JournalLines, "\n")
	for _, exact := range []string{
		legacyBatch63RunID, "ActivityID 52", "Attempt 1",
		"CONFLICT: 飞书通道未连接，无法主动推送",
		`"items":5`, "retryable: false",
	} {
		if !strings.Contains(journal, exact) {
			return nil, legacyBatch63Validation(
				"repair journal evidence is incomplete")
		}
	}
	return wire, nil
}

func cloneLegacyBatch63Prepared(p pusheffect.Prepared) pusheffect.Prepared {
	p.DeliveryIDs = slices.Clone(p.DeliveryIDs)
	p.ObservationEventKeys = slices.Clone(p.ObservationEventKeys)
	p.Card = slices.Clone(p.Card)
	return p
}

func legacyBatch63Digest(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

func legacyBatch63EnableBy(databaseNow, expiresAt time.Time) time.Time {
	enableBy := databaseNow.Add(5 * time.Minute)
	if expiryLimit := expiresAt.Add(-40 * time.Minute); expiryLimit.Before(enableBy) {
		enableBy = expiryLimit
	}
	return enableBy
}

func legacyBatch63ValidDigest(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size &&
		hex.EncodeToString(decoded) == value
}

func legacyBatch63Validation(message string) error {
	return types.NewAppError(types.CodeValidation, message, nil)
}

func legacyBatch63Conflict(message string) error {
	return types.NewAppError(types.CodeConflict, message, nil)
}

func legacyBatch63Database(operation string, err error) error {
	return types.NewAppError(types.CodeDatabase,
		fmt.Sprintf("legacy batch 63 repair: %s", operation), err)
}
