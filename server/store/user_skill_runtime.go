package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/server/capabilityruntime"
	"github.com/YouToco/vane/server/internal/credentialguard"
	"github.com/YouToco/vane/server/internal/strictjson"
	"github.com/YouToco/vane/server/skillruntime"
	"github.com/YouToco/vane/server/types"
)

// AddSkillCapabilityVersionInput adds an immutable candidate version. It does
// not move the installation head or activate the candidate; upgrade remains an
// explicit, exact-version operation.
type AddSkillCapabilityVersionInput struct {
	TenantID      int64
	ActorUserID   int64
	CapabilityID  uuid.UUID
	Source        types.UserCapabilitySource
	SourceRef     string
	PayloadDigest string
	Manifest      json.RawMessage
	Compatible    bool
	Skill         types.SkillCapabilityVersion
}

type SkillResourceMetadata struct {
	Path   string `json:"path"`
	Kind   string `json:"kind"`
	Digest string `json:"digest"`
	Size   int64  `json:"size"`
}

// SkillCapabilityDetail contains exact immutable metadata only. Resource
// bytes require the separate scoped handle/chunk reader.
type SkillCapabilityDetail struct {
	Capability   types.UserCapability         `json:"capability"`
	Version      types.UserCapabilityVersion  `json:"version"`
	Skill        types.SkillCapabilityVersion `json:"skill"`
	Ref          skillruntime.SkillRefV1      `json:"ref"`
	Resources    []SkillResourceMetadata      `json:"resources"`
	HeadRevision string                       `json:"head_revision"`
}

type SkillResourceChange struct {
	Path   string                 `json:"path"`
	Before *SkillResourceMetadata `json:"before,omitempty"`
	After  *SkillResourceMetadata `json:"after,omitempty"`
}

type SkillCapabilityDiff struct {
	CapabilityID    uuid.UUID             `json:"capability_id"`
	FromVersion     uuid.UUID             `json:"from_version"`
	ToVersion       uuid.UUID             `json:"to_version"`
	ManifestChanged bool                  `json:"manifest_changed"`
	SkillMDChanged  bool                  `json:"skill_md_changed"`
	Changes         []SkillResourceChange `json:"changes"`
}

type SkillCapabilityMutationInput struct {
	TenantID                 int64
	ActorUserID              int64
	CapabilityID             uuid.UUID
	VersionID                uuid.UUID
	ExpectedStatus           types.UserCapabilityStatus
	ExpectedCurrentVersionID uuid.UUID
	ExpectedHeadRevision     string
	OperationID              string
}

type SkillCapabilityMutationReceipt struct {
	OperationID          string                     `json:"operation_id"`
	Action               string                     `json:"action"`
	ActorUserID          int64                      `json:"actor_user_id"`
	CapabilityID         uuid.UUID                  `json:"capability_id"`
	VersionID            uuid.UUID                  `json:"version_id"`
	BaseStatus           types.UserCapabilityStatus `json:"base_status"`
	BaseCurrentVersionID uuid.UUID                  `json:"base_current_version_id"`
	ResultStatus         types.UserCapabilityStatus `json:"result_status"`
	BaseHeadRevision     string                     `json:"base_head_revision"`
	ResultHeadRevision   string                     `json:"result_head_revision"`
	Replayed             bool                       `json:"replayed"`
}

func (s *Store) AddSkillCapabilityVersion(ctx context.Context, input AddSkillCapabilityVersionInput) (*types.UserCapabilityVersion, error) {
	if input.TenantID <= 0 || input.ActorUserID <= 0 || input.CapabilityID == uuid.Nil {
		return nil, capabilityValidation("Skill version principal is invalid")
	}
	if err := validateSkillVersionPayload(input.Source, input.SourceRef, input.PayloadDigest,
		input.Manifest, input.Compatible, input.Skill); err != nil {
		return nil, err
	}
	if err := validateSkillVersionCredentialSafety(input.SourceRef, input.Manifest, input.Skill); err != nil {
		return nil, err
	}

	tx, role, err := s.beginCapabilityTx(ctx, input.TenantID, input.ActorUserID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	head, err := lockSkillCapabilityForMutation(ctx, tx, input.TenantID, input.ActorUserID,
		role, input.CapabilityID)
	if err != nil {
		return nil, err
	}
	existing, err := findMatchingSkillVersionTx(ctx, tx, *head, input)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		if err := tx.Commit(ctx); err != nil {
			return nil, capabilityDatabase("commit idempotent Skill version read", err)
		}
		return existing, nil
	}
	var nextVersion int
	if err := tx.QueryRow(ctx, `
		SELECT COALESCE(MAX(version),0)+1 FROM user_capability_versions
		WHERE tenant_id=$1 AND capability_id=$2`, input.TenantID, input.CapabilityID).Scan(&nextVersion); err != nil {
		return nil, capabilityDatabase("allocate Skill capability version", err)
	}
	versionID := uuid.New()
	var version types.UserCapabilityVersion
	err = tx.QueryRow(ctx, `
		INSERT INTO user_capability_versions(
		 id,capability_id,tenant_id,owner_user_id,version,visibility,source_kind,
		 source_ref,payload_digest,manifest_payload,compatible,created_by)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
		RETURNING id,capability_id,tenant_id,owner_user_id,version,source_kind,
		          source_ref,payload_digest,manifest_payload,compatible,created_by,created_at`,
		versionID, head.ID, head.TenantID, head.OwnerUserID, nextVersion, head.Visibility,
		input.Source, input.SourceRef, input.PayloadDigest, []byte(input.Manifest),
		input.Compatible, input.ActorUserID).Scan(
		&version.ID, &version.CapabilityID, &version.TenantID, &version.OwnerUserID,
		&version.Version, &version.Source, &version.SourceRef, &version.PayloadDigest,
		&version.Manifest, &version.Compatible, &version.CreatedBy, &version.CreatedAt)
	if err != nil {
		return nil, capabilityDatabase("add Skill capability version", err)
	}
	if err := insertSkillVersionPayload(ctx, tx, head, versionID, input.Skill); err != nil {
		return nil, err
	}
	if err := appendSkillCapabilityEvent(ctx, tx, *head, input.ActorUserID,
		"version_added", &versionID, map[string]any{"version": nextVersion}); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, capabilityDatabase("commit Skill capability version", err)
	}
	return &version, nil
}

// ActivateSkillCapability selects one exact compatible declarative version.
// The expected head and stable operation ID make response-loss retry an exact
// receipt replay rather than a stale state transition.
func (s *Store) ActivateSkillCapability(ctx context.Context,
	input SkillCapabilityMutationInput,
) (*SkillCapabilityMutationReceipt, error) {
	if err := validateSkillMutationInput(input); err != nil {
		return nil, err
	}
	tx, role, err := s.beginCapabilityTx(ctx, input.TenantID, input.ActorUserID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	head, err := lockSkillCapabilityForMutation(ctx, tx, input.TenantID, input.ActorUserID,
		role, input.CapabilityID)
	if err != nil {
		return nil, err
	}
	if receipt, err := replaySkillMutationReceiptTx(ctx, tx, *head, input, "activate"); err != nil || receipt != nil {
		if err != nil {
			return nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, capabilityDatabase("commit Skill activation replay", err)
		}
		return receipt, nil
	}
	if err := matchExpectedSkillHead(*head, input); err != nil {
		return nil, err
	}
	detail, err := querySkillCapabilityDetailTx(ctx, tx, *head, input.VersionID)
	if err != nil {
		return nil, err
	}
	if !detail.Version.Compatible || detail.Skill.ContainsScripts ||
		validateResolvedSkillDetail(detail) != nil {
		return nil, capabilityValidation("Skill version is not declarative-runtime compatible")
	}
	baseRevision := skillCapabilityHeadRevision(*head)
	var resultUpdatedAt = head.UpdatedAt
	if err := tx.QueryRow(ctx, `
		UPDATE user_capabilities
		SET current_version_id=$2,status='active',updated_at=clock_timestamp()
		WHERE tenant_id=$1 AND id=$3
		RETURNING updated_at`, input.TenantID, input.VersionID, input.CapabilityID).Scan(&resultUpdatedAt); err != nil {
		return nil, capabilityDatabase("activate Skill capability", err)
	}
	resultHead := *head
	resultHead.Status = types.UserCapabilityActive
	resultHead.CurrentVersionID = &input.VersionID
	resultHead.UpdatedAt = resultUpdatedAt
	receipt := SkillCapabilityMutationReceipt{
		OperationID: input.OperationID, Action: "activate", ActorUserID: input.ActorUserID,
		CapabilityID: input.CapabilityID, VersionID: input.VersionID, BaseStatus: head.Status,
		BaseCurrentVersionID: input.ExpectedCurrentVersionID, ResultStatus: types.UserCapabilityActive,
		BaseHeadRevision: baseRevision, ResultHeadRevision: skillCapabilityHeadRevision(resultHead),
	}
	if err := appendSkillMutationReceiptEvent(ctx, tx, *head, input.ActorUserID,
		"activated", receipt); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, capabilityDatabase("commit Skill activation", err)
	}
	return &receipt, nil
}

func (s *Store) PauseSkillCapability(ctx context.Context,
	input SkillCapabilityMutationInput,
) (*SkillCapabilityMutationReceipt, error) {
	if err := validateSkillMutationInput(input); err != nil {
		return nil, err
	}
	tx, role, err := s.beginCapabilityTx(ctx, input.TenantID, input.ActorUserID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	head, err := lockSkillCapabilityForMutation(ctx, tx, input.TenantID, input.ActorUserID,
		role, input.CapabilityID)
	if err != nil {
		return nil, err
	}
	if receipt, err := replaySkillMutationReceiptTx(ctx, tx, *head, input, "pause"); err != nil || receipt != nil {
		if err != nil {
			return nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, capabilityDatabase("commit Skill pause replay", err)
		}
		return receipt, nil
	}
	if err := matchExpectedSkillHead(*head, input); err != nil {
		return nil, err
	}
	if head.Status != types.UserCapabilityActive || head.CurrentVersionID == nil ||
		*head.CurrentVersionID != input.VersionID {
		return nil, capabilityValidation("only an active Skill capability can be paused")
	}
	baseRevision := skillCapabilityHeadRevision(*head)
	var resultUpdatedAt = head.UpdatedAt
	if err := tx.QueryRow(ctx, `
		UPDATE user_capabilities SET status='paused',updated_at=clock_timestamp()
		WHERE tenant_id=$1 AND id=$2 RETURNING updated_at`,
		input.TenantID, input.CapabilityID).Scan(&resultUpdatedAt); err != nil {
		return nil, capabilityDatabase("pause Skill capability", err)
	}
	resultHead := *head
	resultHead.Status = types.UserCapabilityPaused
	resultHead.UpdatedAt = resultUpdatedAt
	receipt := SkillCapabilityMutationReceipt{
		OperationID: input.OperationID, Action: "pause", ActorUserID: input.ActorUserID,
		CapabilityID: input.CapabilityID, VersionID: input.VersionID, BaseStatus: head.Status,
		BaseCurrentVersionID: input.ExpectedCurrentVersionID, ResultStatus: types.UserCapabilityPaused,
		BaseHeadRevision: baseRevision, ResultHeadRevision: skillCapabilityHeadRevision(resultHead),
	}
	if err := appendSkillMutationReceiptEvent(ctx, tx, *head, input.ActorUserID,
		"paused", receipt); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, capabilityDatabase("commit Skill pause", err)
	}
	return &receipt, nil
}

func (s *Store) GetSkillCapabilityDetail(ctx context.Context, tenantID, actorUserID int64,
	capabilityID, versionID uuid.UUID,
) (*SkillCapabilityDetail, error) {
	if tenantID <= 0 || actorUserID <= 0 || capabilityID == uuid.Nil || versionID == uuid.Nil {
		return nil, capabilityValidation("exact Skill version is required")
	}
	tx, _, err := s.beginCapabilityTx(ctx, tenantID, actorUserID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	head, err := queryVisibleSkillHeadTx(ctx, tx, tenantID, capabilityID, false)
	if err != nil {
		return nil, err
	}
	detail, err := querySkillCapabilityDetailTx(ctx, tx, *head, versionID)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, capabilityDatabase("commit Skill detail read", err)
	}
	return detail, nil
}

func (s *Store) DiffSkillCapabilityVersions(ctx context.Context, tenantID, actorUserID int64,
	capabilityID, fromVersionID, toVersionID uuid.UUID,
) (*SkillCapabilityDiff, error) {
	if fromVersionID == toVersionID {
		return nil, capabilityValidation("Skill diff requires two distinct exact versions")
	}
	if tenantID <= 0 || actorUserID <= 0 || capabilityID == uuid.Nil ||
		fromVersionID == uuid.Nil || toVersionID == uuid.Nil {
		return nil, capabilityValidation("Skill diff identity is invalid")
	}
	tx, _, err := s.beginCapabilityTx(ctx, tenantID, actorUserID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	head, err := queryVisibleSkillHeadTx(ctx, tx, tenantID, capabilityID, false)
	if err != nil {
		return nil, err
	}
	from, err := querySkillCapabilityDetailTx(ctx, tx, *head, fromVersionID)
	if err != nil {
		return nil, err
	}
	to, err := querySkillCapabilityDetailTx(ctx, tx, *head, toVersionID)
	if err != nil {
		return nil, err
	}
	diff := &SkillCapabilityDiff{
		CapabilityID: capabilityID, FromVersion: fromVersionID, ToVersion: toVersionID,
		ManifestChanged: from.Version.PayloadDigest != to.Version.PayloadDigest ||
			!bytes.Equal(from.Version.Manifest, to.Version.Manifest),
		SkillMDChanged: from.Skill.SkillMDDigest != to.Skill.SkillMDDigest,
		Changes:        diffSkillResources(from.Resources, to.Resources),
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, capabilityDatabase("commit Skill diff read", err)
	}
	return diff, nil
}

// ResolveSkillRef returns the exact compatible no-script identity currently
// selected by an active capability head. It does not make resource bytes
// model-visible. Frozen historical runs retain the resolved ref and replay it
// through the exact-version resource reader.
func (s *Store) ResolveSkillRef(ctx context.Context, principal capabilityruntime.PrincipalV1,
	capabilityID, versionID uuid.UUID,
) (skillruntime.SkillRefV1, error) {
	tx, err := s.beginSkillRuntimeTx(ctx, principal)
	if err != nil {
		return skillruntime.SkillRefV1{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	head, err := queryVisibleSkillHeadTx(ctx, tx, int64(principal.TenantID), capabilityID, false)
	if err != nil {
		return skillruntime.SkillRefV1{}, err
	}
	detail, err := querySkillCapabilityDetailTx(ctx, tx, *head, versionID)
	if err != nil {
		return skillruntime.SkillRefV1{}, err
	}
	if detail.Capability.Status != types.UserCapabilityActive ||
		detail.Capability.CurrentVersionID == nil || *detail.Capability.CurrentVersionID != versionID ||
		detail.Ref.Validate() != nil || validateResolvedSkillDetail(detail) != nil {
		return skillruntime.SkillRefV1{}, capabilityValidation("Skill version is not runtime compatible")
	}
	if err := tx.Commit(ctx); err != nil {
		return skillruntime.SkillRefV1{}, capabilityDatabase("commit Skill ref resolution", err)
	}
	return detail.Ref, nil
}

// OpenSkillResource validates the complete exact Skill reference again before
// returning a byte-free resource handle.
func (s *Store) OpenSkillResource(ctx context.Context, principal capabilityruntime.PrincipalV1,
	ref skillruntime.SkillRefV1, resourcePath string,
) (skillruntime.SkillResourceHandleV1, error) {
	if ref.Validate() != nil || !utf8.ValidString(resourcePath) ||
		int64(principal.TenantID) != ref.TenantID {
		return skillruntime.SkillResourceHandleV1{}, capabilityValidation("Skill resource request is invalid")
	}
	tx, err := s.beginSkillRuntimeTx(ctx, principal)
	if err != nil {
		return skillruntime.SkillResourceHandleV1{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	head, err := queryVisibleSkillHeadTx(ctx, tx, ref.TenantID, ref.CapabilityID, false)
	if err != nil {
		return skillruntime.SkillResourceHandleV1{}, err
	}
	detail, err := querySkillCapabilityDetailTx(ctx, tx, *head, ref.CapabilityVersionID)
	activated, activationErr := skillVersionWasActivatedTx(ctx, tx, ref.TenantID,
		ref.CapabilityID, ref.CapabilityVersionID)
	if err != nil || activationErr != nil || !activated || detail.Ref != ref ||
		validateResolvedSkillDetail(detail) != nil {
		if err != nil {
			return skillruntime.SkillResourceHandleV1{}, err
		}
		if activationErr != nil {
			return skillruntime.SkillResourceHandleV1{}, activationErr
		}
		return skillruntime.SkillResourceHandleV1{}, capabilityNotFound("exact Skill reference is unavailable")
	}
	var metadata SkillResourceMetadata
	err = tx.QueryRow(ctx, `
		SELECT file_path,file_kind,content_digest,octet_length(content_payload)
		FROM skill_capability_files
		WHERE tenant_id=$1 AND capability_id=$2 AND capability_version_id=$3 AND file_path=$4`,
		ref.TenantID, ref.CapabilityID, ref.CapabilityVersionID, resourcePath).Scan(
		&metadata.Path, &metadata.Kind, &metadata.Digest, &metadata.Size)
	if errors.Is(err, pgx.ErrNoRows) {
		return skillruntime.SkillResourceHandleV1{}, capabilityNotFound("Skill resource is unavailable")
	}
	if err != nil {
		return skillruntime.SkillResourceHandleV1{}, capabilityDatabase("open Skill resource", err)
	}
	handle := skillruntime.SkillResourceHandleV1{
		SchemaVersion: skillruntime.SkillResourceHandleSchemaVersionV1,
		Skill:         ref, Path: metadata.Path, Kind: metadata.Kind,
		ContentDigest: metadata.Digest, ContentSize: metadata.Size,
	}
	if err := handle.Validate(); err != nil {
		return skillruntime.SkillResourceHandleV1{}, capabilityDatabase("stored Skill resource metadata is invalid", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return skillruntime.SkillResourceHandleV1{}, capabilityDatabase("commit Skill resource open", err)
	}
	return handle, nil
}

// ReadSkillResourceChunk returns at most 64 KiB and re-proves the exact Skill
// ref plus resource metadata on every call. No production caller is wired in
// the dark slice.
func (s *Store) ReadSkillResourceChunk(ctx context.Context, principal capabilityruntime.PrincipalV1,
	handle skillruntime.SkillResourceHandleV1, offset int64, limit int,
) (skillruntime.SkillResourceChunkV1, error) {
	if handle.Validate() != nil || int64(principal.TenantID) != handle.Skill.TenantID || offset < 0 ||
		offset > handle.ContentSize || limit <= 0 || limit > skillruntime.MaxResourceChunkBytesV1 {
		return skillruntime.SkillResourceChunkV1{}, capabilityValidation("Skill resource chunk request is invalid")
	}
	tx, err := s.beginSkillRuntimeTx(ctx, principal)
	if err != nil {
		return skillruntime.SkillResourceChunkV1{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	head, err := queryVisibleSkillHeadTx(ctx, tx, handle.Skill.TenantID, handle.Skill.CapabilityID, false)
	if err != nil {
		return skillruntime.SkillResourceChunkV1{}, err
	}
	detail, err := querySkillCapabilityDetailTx(ctx, tx, *head, handle.Skill.CapabilityVersionID)
	activated, activationErr := skillVersionWasActivatedTx(ctx, tx, handle.Skill.TenantID,
		handle.Skill.CapabilityID, handle.Skill.CapabilityVersionID)
	if err != nil || activationErr != nil || !activated || detail.Ref != handle.Skill ||
		validateResolvedSkillDetail(detail) != nil {
		if err != nil {
			return skillruntime.SkillResourceChunkV1{}, err
		}
		if activationErr != nil {
			return skillruntime.SkillResourceChunkV1{}, activationErr
		}
		return skillruntime.SkillResourceChunkV1{}, capabilityNotFound("exact Skill reference is unavailable")
	}
	var pathValue, kind, digest string
	var size int64
	var data []byte
	err = tx.QueryRow(ctx, `
		SELECT file_path,file_kind,content_digest,octet_length(content_payload),
		       substring(content_payload FROM $5 FOR $6)
		FROM skill_capability_files
		WHERE tenant_id=$1 AND capability_id=$2 AND capability_version_id=$3 AND file_path=$4`,
		handle.Skill.TenantID, handle.Skill.CapabilityID, handle.Skill.CapabilityVersionID,
		handle.Path, offset+1, limit).Scan(&pathValue, &kind, &digest, &size, &data)
	if errors.Is(err, pgx.ErrNoRows) {
		return skillruntime.SkillResourceChunkV1{}, capabilityNotFound("Skill resource is unavailable")
	}
	if err != nil {
		return skillruntime.SkillResourceChunkV1{}, capabilityDatabase("read Skill resource", err)
	}
	if pathValue != handle.Path || kind != handle.Kind || digest != handle.ContentDigest || size != handle.ContentSize {
		return skillruntime.SkillResourceChunkV1{}, capabilityNotFound("Skill resource handle drifted")
	}
	chunkData := make([]byte, len(data))
	copy(chunkData, data)
	chunk := skillruntime.SkillResourceChunkV1{
		SchemaVersion: skillruntime.SkillResourceChunkSchemaVersionV1,
		HandleDigest:  skillruntime.DigestHandleV1(handle), Offset: offset,
		Data: chunkData, EOF: offset+int64(len(data)) == size,
	}
	if err := chunk.Validate(handle); err != nil {
		return skillruntime.SkillResourceChunkV1{}, capabilityDatabase("stored Skill resource chunk is invalid", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return skillruntime.SkillResourceChunkV1{}, capabilityDatabase("commit Skill resource read", err)
	}
	return chunk, nil
}

func validateSkillVersionPayload(source types.UserCapabilitySource, sourceRef, payloadDigest string,
	manifest json.RawMessage, compatible bool, skill types.SkillCapabilityVersion,
) error {
	if source != types.UserCapabilityUpload && source != types.UserCapabilityPublicCatalog {
		return capabilityValidation("Skill source must be upload or public_catalog")
	}
	if len(sourceRef) > 2048 || !utf8.ValidString(sourceRef) ||
		strings.ContainsAny(sourceRef, "\x00\r\n?#") ||
		(source == types.UserCapabilityPublicCatalog && sourceRef == "") ||
		!validSHA256(payloadDigest) || !validJSONObject(manifest) || digestBytes(manifest) != payloadDigest ||
		!bytes.Equal(manifest, skill.FileManifest) ||
		skill.Name == "" || !capabilitySlugPattern.MatchString(skill.Name) ||
		len(skill.Description) > 4096 || !utf8.ValidString(skill.Description) ||
		!validSHA256(skill.SkillMDDigest) || !validSHA256(skill.ArchiveDigest) ||
		!validJSONObject(skill.FileManifest) || compatible == skill.ContainsScripts {
		return capabilityValidation("Skill version metadata is invalid")
	}
	if err := validateSkillFiles(skill.Files, skill.SkillMDDigest); err != nil {
		return err
	}
	return validateSkillFileManifest(skill)
}

func validateSkillVersionCredentialSafety(sourceRef string, manifest json.RawMessage,
	skill types.SkillCapabilityVersion,
) error {
	for _, value := range []string{sourceRef, string(manifest), skill.Name,
		skill.Description, string(skill.FileManifest)} {
		if credentialguard.ContainsCredential(value) {
			return capabilityValidation("Skill capability contains credential material")
		}
	}
	for _, file := range skill.Files {
		if credentialguard.ContainsCredential(file.Path) || credentialguard.ContainsCredential(string(file.Content)) {
			return capabilityValidation("Skill capability contains credential material")
		}
	}
	return nil
}

func insertSkillVersionPayload(ctx context.Context, tx pgx.Tx, head *types.UserCapability,
	versionID uuid.UUID, skill types.SkillCapabilityVersion,
) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO skill_capability_versions(
		 capability_version_id,capability_id,tenant_id,owner_user_id,visibility,
		 name,description,skill_md_digest,archive_digest,file_manifest_payload,
		 file_manifest_digest,contains_scripts)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
		versionID, head.ID, head.TenantID, head.OwnerUserID, head.Visibility,
		skill.Name, skill.Description, skill.SkillMDDigest, skill.ArchiveDigest,
		[]byte(skill.FileManifest), digestBytes(skill.FileManifest), skill.ContainsScripts)
	if err != nil {
		return capabilityDatabase("store Skill version metadata", err)
	}
	for _, file := range skill.Files {
		if _, err := tx.Exec(ctx, `
			INSERT INTO skill_capability_files(
			 capability_version_id,capability_id,tenant_id,owner_user_id,visibility,
			 file_path,file_kind,content_digest,content_payload)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
			versionID, head.ID, head.TenantID, head.OwnerUserID, head.Visibility,
			file.Path, file.Kind, file.Digest, file.Content); err != nil {
			return capabilityDatabase("store Skill version resource", err)
		}
	}
	return nil
}

func findMatchingSkillVersionTx(ctx context.Context, tx pgx.Tx, head types.UserCapability,
	input AddSkillCapabilityVersionInput,
) (*types.UserCapabilityVersion, error) {
	rows, err := tx.Query(ctx, `
		SELECT id FROM user_capability_versions
		WHERE tenant_id=$1 AND capability_id=$2 AND payload_digest=$3
		ORDER BY version`, head.TenantID, head.ID, input.PayloadDigest)
	if err != nil {
		return nil, capabilityDatabase("find existing Skill capability version", err)
	}
	var versionIDs []uuid.UUID
	for rows.Next() {
		var versionID uuid.UUID
		if err := rows.Scan(&versionID); err != nil {
			rows.Close()
			return nil, capabilityDatabase("scan existing Skill capability version", err)
		}
		versionIDs = append(versionIDs, versionID)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, capabilityDatabase("iterate existing Skill capability versions", err)
	}
	rows.Close()
	for _, versionID := range versionIDs {
		detail, err := querySkillCapabilityDetailTx(ctx, tx, head, versionID)
		if err != nil {
			return nil, err
		}
		if skillDetailMatchesAddInput(detail, input) {
			version := detail.Version
			return &version, nil
		}
	}
	return nil, nil
}

func skillDetailMatchesAddInput(detail *SkillCapabilityDetail, input AddSkillCapabilityVersionInput) bool {
	if detail == nil || detail.Version.Source != input.Source ||
		detail.Version.SourceRef != input.SourceRef || detail.Version.PayloadDigest != input.PayloadDigest ||
		!bytes.Equal(detail.Version.Manifest, input.Manifest) || detail.Version.Compatible != input.Compatible ||
		detail.Skill.Name != input.Skill.Name || detail.Skill.Description != input.Skill.Description ||
		detail.Skill.SkillMDDigest != input.Skill.SkillMDDigest ||
		detail.Skill.ArchiveDigest != input.Skill.ArchiveDigest ||
		!bytes.Equal(detail.Skill.FileManifest, input.Skill.FileManifest) ||
		detail.Skill.ContainsScripts != input.Skill.ContainsScripts ||
		len(detail.Resources) != len(input.Skill.Files) {
		return false
	}
	resources := make(map[string]SkillResourceMetadata, len(detail.Resources))
	for _, resource := range detail.Resources {
		resources[resource.Path] = resource
	}
	for _, file := range input.Skill.Files {
		resource, ok := resources[file.Path]
		if !ok || resource.Kind != file.Kind || resource.Digest != file.Digest ||
			resource.Size != int64(len(file.Content)) {
			return false
		}
	}
	return true
}

func lockSkillCapabilityForMutation(ctx context.Context, tx pgx.Tx, tenantID, actorUserID int64,
	role types.MembershipRole, capabilityID uuid.UUID,
) (*types.UserCapability, error) {
	visible, err := queryVisibleSkillHeadTx(ctx, tx, tenantID, capabilityID, false)
	if err != nil {
		return nil, err
	}
	if (visible.Visibility == types.UserCapabilityPersonal && visible.OwnerUserID != actorUserID) ||
		(visible.Visibility == types.UserCapabilityWorkspace &&
			role != types.MembershipRoleOwner && role != types.MembershipRoleAdmin) {
		return nil, capabilityForbidden("principal cannot manage this Skill capability")
	}
	return queryVisibleSkillHeadTx(ctx, tx, tenantID, capabilityID, true)
}

func queryVisibleSkillHeadTx(ctx context.Context, tx pgx.Tx, tenantID int64,
	capabilityID uuid.UUID, forUpdate bool,
) (*types.UserCapability, error) {
	query := `
		SELECT id,tenant_id,owner_user_id,kind,visibility,slug,display_name,status,
		       current_version_id,created_at,updated_at
		FROM user_capabilities WHERE tenant_id=$1 AND id=$2 AND kind='skill'`
	if forUpdate {
		query += " FOR UPDATE"
	}
	var head types.UserCapability
	err := tx.QueryRow(ctx, query, tenantID, capabilityID).Scan(
		&head.ID, &head.TenantID, &head.OwnerUserID, &head.Kind, &head.Visibility,
		&head.Slug, &head.DisplayName, &head.Status, &head.CurrentVersionID,
		&head.CreatedAt, &head.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, capabilityNotFound("Skill capability is unavailable")
	}
	if err != nil {
		return nil, capabilityDatabase("load Skill capability", err)
	}
	return &head, nil
}

func querySkillCapabilityDetailTx(ctx context.Context, tx pgx.Tx, head types.UserCapability,
	versionID uuid.UUID,
) (*SkillCapabilityDetail, error) {
	detail := &SkillCapabilityDetail{Capability: head}
	var fileManifestDigest string
	err := tx.QueryRow(ctx, `
		SELECT v.id,v.capability_id,v.tenant_id,v.owner_user_id,v.version,v.source_kind,
		       v.source_ref,v.payload_digest,v.manifest_payload,v.compatible,v.created_by,v.created_at,
		       s.capability_version_id,s.name,s.description,s.skill_md_digest,s.archive_digest,
		       s.file_manifest_payload,s.file_manifest_digest,s.contains_scripts,s.created_at
		FROM user_capability_versions v
		JOIN skill_capability_versions s ON s.capability_version_id=v.id
		 AND s.tenant_id=v.tenant_id AND s.capability_id=v.capability_id
		WHERE v.tenant_id=$1 AND v.capability_id=$2 AND v.id=$3`,
		head.TenantID, head.ID, versionID).Scan(
		&detail.Version.ID, &detail.Version.CapabilityID, &detail.Version.TenantID,
		&detail.Version.OwnerUserID, &detail.Version.Version, &detail.Version.Source,
		&detail.Version.SourceRef, &detail.Version.PayloadDigest, &detail.Version.Manifest,
		&detail.Version.Compatible, &detail.Version.CreatedBy, &detail.Version.CreatedAt,
		&detail.Skill.CapabilityVersionID, &detail.Skill.Name, &detail.Skill.Description,
		&detail.Skill.SkillMDDigest, &detail.Skill.ArchiveDigest, &detail.Skill.FileManifest,
		&fileManifestDigest, &detail.Skill.ContainsScripts, &detail.Skill.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, capabilityNotFound("exact Skill version is unavailable")
	}
	if err != nil {
		return nil, capabilityDatabase("load exact Skill version", err)
	}
	rows, err := tx.Query(ctx, `
		SELECT file_path,file_kind,content_digest,octet_length(content_payload)
		FROM skill_capability_files
		WHERE tenant_id=$1 AND capability_id=$2 AND capability_version_id=$3
		ORDER BY file_path`, head.TenantID, head.ID, versionID)
	if err != nil {
		return nil, capabilityDatabase("list Skill resources", err)
	}
	defer rows.Close()
	detail.Resources = make([]SkillResourceMetadata, 0)
	for rows.Next() {
		var metadata SkillResourceMetadata
		if err := rows.Scan(&metadata.Path, &metadata.Kind, &metadata.Digest, &metadata.Size); err != nil {
			return nil, capabilityDatabase("scan Skill resource metadata", err)
		}
		detail.Resources = append(detail.Resources, metadata)
	}
	if err := rows.Err(); err != nil {
		return nil, capabilityDatabase("iterate Skill resource metadata", err)
	}
	detail.Ref = skillruntime.SkillRefV1{
		SchemaVersion: skillruntime.SkillRefSchemaVersionV1,
		TenantID:      head.TenantID, OwnerUserID: head.OwnerUserID,
		CapabilityID: head.ID, CapabilityVersionID: versionID,
		Version: detail.Version.Version, Visibility: string(head.Visibility),
		PayloadDigest: detail.Version.PayloadDigest, SkillMDDigest: detail.Skill.SkillMDDigest,
		FileManifestDigest: fileManifestDigest, Compatible: detail.Version.Compatible,
		ContainsScripts: detail.Skill.ContainsScripts,
	}
	detail.HeadRevision = skillCapabilityHeadRevision(head)
	return detail, nil
}

func skillVersionWasActivatedTx(ctx context.Context, tx pgx.Tx, tenantID int64,
	capabilityID, versionID uuid.UUID,
) (bool, error) {
	var activated bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS(
		 SELECT 1 FROM user_capability_events
		 WHERE tenant_id=$1 AND capability_id=$2 AND version_id=$3 AND event_kind='activated')`,
		tenantID, capabilityID, versionID).Scan(&activated); err != nil {
		return false, capabilityDatabase("verify Skill activation history", err)
	}
	return activated, nil
}

func diffSkillResources(from, to []SkillResourceMetadata) []SkillResourceChange {
	left := make(map[string]SkillResourceMetadata, len(from))
	right := make(map[string]SkillResourceMetadata, len(to))
	paths := make(map[string]struct{}, len(from)+len(to))
	for _, item := range from {
		left[item.Path], paths[item.Path] = item, struct{}{}
	}
	for _, item := range to {
		right[item.Path], paths[item.Path] = item, struct{}{}
	}
	ordered := make([]string, 0, len(paths))
	for itemPath := range paths {
		ordered = append(ordered, itemPath)
	}
	slices.Sort(ordered)
	changes := make([]SkillResourceChange, 0)
	for _, itemPath := range ordered {
		before, hadBefore := left[itemPath]
		after, hasAfter := right[itemPath]
		if hadBefore && hasAfter && before == after {
			continue
		}
		change := SkillResourceChange{Path: itemPath}
		if hadBefore {
			value := before
			change.Before = &value
		}
		if hasAfter {
			value := after
			change.After = &value
		}
		changes = append(changes, change)
	}
	return changes
}

func appendSkillCapabilityEvent(ctx context.Context, tx pgx.Tx, head types.UserCapability,
	actorUserID int64, kind string, versionID *uuid.UUID, details map[string]any,
) error {
	if details == nil {
		details = map[string]any{}
	}
	payload, err := json.Marshal(details)
	if err != nil {
		return capabilityValidation("Skill capability event details are invalid")
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO user_capability_events(
		 tenant_id,capability_id,owner_user_id,visibility,actor_user_id,event_kind,version_id,details)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8)`,
		head.TenantID, head.ID, head.OwnerUserID, head.Visibility, actorUserID, kind, versionID, payload)
	if err != nil {
		return capabilityDatabase("append Skill capability event", err)
	}
	return nil
}

func appendSkillMutationReceiptEvent(ctx context.Context, tx pgx.Tx, head types.UserCapability,
	actorUserID int64, eventKind string, receipt SkillCapabilityMutationReceipt,
) error {
	payload, err := json.Marshal(receipt)
	if err != nil {
		return capabilityValidation("Skill mutation receipt is invalid")
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO user_capability_events(
		 tenant_id,capability_id,owner_user_id,visibility,actor_user_id,event_kind,version_id,details)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8)`,
		head.TenantID, head.ID, head.OwnerUserID, head.Visibility, actorUserID,
		eventKind, receipt.VersionID, payload)
	if err != nil {
		return capabilityDatabase("append Skill mutation receipt", err)
	}
	return nil
}

func replaySkillMutationReceiptTx(ctx context.Context, tx pgx.Tx, head types.UserCapability,
	input SkillCapabilityMutationInput, action string,
) (*SkillCapabilityMutationReceipt, error) {
	rows, err := tx.Query(ctx, `
		SELECT details FROM user_capability_events
		WHERE tenant_id=$1 AND capability_id=$2 AND details->>'operation_id'=$3
		ORDER BY id`, head.TenantID, head.ID, input.OperationID)
	if err != nil {
		return nil, capabilityDatabase("load Skill mutation receipt", err)
	}
	defer rows.Close()
	var receipts []SkillCapabilityMutationReceipt
	for rows.Next() {
		var payload []byte
		if err := rows.Scan(&payload); err != nil {
			return nil, capabilityDatabase("scan Skill mutation receipt", err)
		}
		var receipt SkillCapabilityMutationReceipt
		if err := json.Unmarshal(payload, &receipt); err != nil {
			return nil, capabilityDatabase("decode Skill mutation receipt", err)
		}
		receipts = append(receipts, receipt)
	}
	if err := rows.Err(); err != nil {
		return nil, capabilityDatabase("iterate Skill mutation receipts", err)
	}
	if len(receipts) == 0 {
		return nil, nil
	}
	if len(receipts) != 1 {
		return nil, types.NewAppError(types.CodeConflict,
			"duplicate Skill mutation receipt", types.ErrConflict)
	}
	receipt := receipts[0]
	expectedResultStatus := types.UserCapabilityActive
	if action == "pause" {
		expectedResultStatus = types.UserCapabilityPaused
	}
	if receipt.OperationID != input.OperationID || receipt.Action != action ||
		receipt.ActorUserID != input.ActorUserID || receipt.CapabilityID != input.CapabilityID ||
		receipt.VersionID != input.VersionID ||
		receipt.BaseStatus != input.ExpectedStatus ||
		receipt.BaseCurrentVersionID != input.ExpectedCurrentVersionID ||
		receipt.BaseHeadRevision != input.ExpectedHeadRevision ||
		receipt.ResultStatus != expectedResultStatus ||
		!validSHA256(receipt.ResultHeadRevision) {
		return nil, types.NewAppError(types.CodeConflict,
			"Skill operation ID belongs to another mutation", types.ErrConflict)
	}
	receipt.Replayed = true
	return &receipt, nil
}

func validateSkillMutationInput(input SkillCapabilityMutationInput) error {
	if input.TenantID <= 0 || input.ActorUserID <= 0 || input.CapabilityID == uuid.Nil ||
		input.VersionID == uuid.Nil || input.ExpectedCurrentVersionID == uuid.Nil ||
		!validSkillCapabilityStatus(input.ExpectedStatus) || !validSHA256(input.ExpectedHeadRevision) ||
		!validSkillOperationID(input.OperationID) {
		return capabilityValidation("Skill lifecycle mutation is invalid")
	}
	return nil
}

func validSkillCapabilityStatus(status types.UserCapabilityStatus) bool {
	switch status {
	case types.UserCapabilityDraft, types.UserCapabilityActive,
		types.UserCapabilityPaused, types.UserCapabilityIncompatible:
		return true
	default:
		return false
	}
}

func matchExpectedSkillHead(head types.UserCapability, input SkillCapabilityMutationInput) error {
	if head.Status != input.ExpectedStatus || head.CurrentVersionID == nil ||
		*head.CurrentVersionID != input.ExpectedCurrentVersionID ||
		skillCapabilityHeadRevision(head) != input.ExpectedHeadRevision {
		return types.NewAppError(types.CodeConflict,
			"Skill capability head changed after the request was prepared", types.ErrConflict)
	}
	return nil
}

func validSkillOperationID(value string) bool {
	if len(value) == 0 || len(value) > 128 {
		return false
	}
	for i := range len(value) {
		b := value[i]
		if (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') ||
			(b >= '0' && b <= '9') || b == '-' || b == '_' || b == '.' || b == ':' {
			continue
		}
		return false
	}
	return true
}

func skillCapabilityHeadRevision(head types.UserCapability) string {
	type headRevisionV1 struct {
		SchemaVersion    string                         `json:"schema_version"`
		TenantID         int64                          `json:"tenant_id"`
		CapabilityID     uuid.UUID                      `json:"capability_id"`
		OwnerUserID      int64                          `json:"owner_user_id"`
		Visibility       types.UserCapabilityVisibility `json:"visibility"`
		Status           types.UserCapabilityStatus     `json:"status"`
		CurrentVersionID *uuid.UUID                     `json:"current_version_id"`
		UpdatedAt        string                         `json:"updated_at"`
	}
	payload, _ := json.Marshal(headRevisionV1{
		SchemaVersion: "vane.skill-capability-head/v1", TenantID: head.TenantID,
		CapabilityID: head.ID, OwnerUserID: head.OwnerUserID, Visibility: head.Visibility,
		Status: head.Status, CurrentVersionID: head.CurrentVersionID,
		UpdatedAt: head.UpdatedAt.UTC().Format("2006-01-02T15:04:05.999999999Z07:00"),
	})
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func (s *Store) beginSkillRuntimeTx(ctx context.Context,
	principal capabilityruntime.PrincipalV1,
) (pgx.Tx, error) {
	if principal.ActorType != types.ActorTypeUser || principal.TenantID <= 0 ||
		principal.UserID <= 0 || !principal.Role.Valid() ||
		principal.MembershipAuthorizationGeneration <= 0 ||
		principal.A2ATokenAuthorityID != "" || principal.RequiredA2AScope != "" {
		return nil, capabilityForbidden("human user Principal is required for declarative Skill runtime")
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, capabilityDatabase("begin Skill runtime transaction", err)
	}
	var liveRole types.MembershipRole
	var liveGeneration int64
	err = tx.QueryRow(ctx, `
		SELECT m.role,m.authorization_generation
		FROM memberships m JOIN tenants t ON t.id=m.tenant_id
		WHERE m.tenant_id=$1 AND m.user_id=$2 AND t.status='active' AND t.deleted_at IS NULL
		FOR SHARE OF m,t`, principal.TenantID, principal.UserID).Scan(&liveRole, &liveGeneration)
	if err != nil {
		_ = tx.Rollback(ctx)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, capabilityForbidden("live Skill runtime membership is required")
		}
		return nil, capabilityDatabase("reprove Skill runtime Principal", err)
	}
	if liveRole != principal.Role || liveGeneration != principal.MembershipAuthorizationGeneration {
		_ = tx.Rollback(ctx)
		return nil, capabilityForbidden("Skill runtime Principal authority changed")
	}
	if _, err := tx.Exec(ctx, `
		SELECT set_config('app.tenant_id',$1,true),set_config('app.user_id',$2,true),
		       set_config('app.membership_role',$3,true)`,
		fmt.Sprint(principal.TenantID), fmt.Sprint(principal.UserID), string(liveRole)); err != nil {
		_ = tx.Rollback(ctx)
		return nil, capabilityDatabase("set Skill runtime scope", err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_app`); err != nil {
		_ = tx.Rollback(ctx)
		return nil, capabilityDatabase("enter Skill runtime role", err)
	}
	return tx, nil
}

func capabilityNotFound(message string) error {
	return types.NewAppError(types.CodeNotFound, message, types.ErrNotFound)
}

type storedSkillFileManifestV1 struct {
	SchemaVersion  string                      `json:"schema_version"`
	Name           string                      `json:"name"`
	Description    string                      `json:"description,omitempty"`
	Compatible     bool                        `json:"compatible"`
	ContainsScript bool                        `json:"contains_scripts"`
	Files          []storedSkillManifestFileV1 `json:"files"`
}

type storedSkillManifestFileV1 struct {
	Path   string `json:"path"`
	Kind   string `json:"kind"`
	Size   int64  `json:"size"`
	Digest string `json:"digest"`
}

func validateSkillFileManifest(skill types.SkillCapabilityVersion) error {
	manifest, err := decodeSkillFileManifest(skill)
	if err != nil {
		return err
	}
	stored := make(map[string]types.SkillCapabilityFile, len(skill.Files))
	for _, file := range skill.Files {
		stored[file.Path] = file
	}
	foundSkillMD := false
	for _, file := range manifest.Files {
		if file.Kind == "script" {
			continue
		}
		storedFile, ok := stored[file.Path]
		if !ok || storedFile.Kind != file.Kind || storedFile.Digest != file.Digest ||
			int64(len(storedFile.Content)) != file.Size {
			return capabilityValidation("Skill file manifest differs from stored resources")
		}
		delete(stored, file.Path)
		foundSkillMD = foundSkillMD || (file.Path == "SKILL.md" && file.Kind == "skill_md" &&
			file.Digest == skill.SkillMDDigest)
	}
	if len(stored) != 0 || !foundSkillMD {
		return capabilityValidation("Skill file manifest is incomplete")
	}
	return nil
}

func decodeSkillFileManifest(skill types.SkillCapabilityVersion) (storedSkillFileManifestV1, error) {
	var manifest storedSkillFileManifestV1
	if err := strictjson.DecodeExact(skill.FileManifest, &manifest); err != nil {
		return storedSkillFileManifestV1{}, capabilityValidation("Skill file manifest is invalid")
	}
	canonical, err := json.Marshal(manifest)
	if err != nil || !bytes.Equal(canonical, skill.FileManifest) ||
		manifest.SchemaVersion != "vane.skill-package/v1" || manifest.Name != skill.Name ||
		manifest.Description != skill.Description || manifest.Compatible == skill.ContainsScripts ||
		manifest.ContainsScript != skill.ContainsScripts || len(manifest.Files) == 0 ||
		len(manifest.Files) > 128 {
		return storedSkillFileManifestV1{}, capabilityValidation("Skill file manifest is not canonical")
	}
	seen := make(map[string]struct{}, len(manifest.Files))
	previous := ""
	var totalSize int64
	for _, file := range manifest.Files {
		if file.Path <= previous || !validSHA256(file.Digest) || file.Size < 0 || file.Size > 4<<20 {
			return storedSkillFileManifestV1{}, capabilityValidation("Skill file manifest ordering or digest is invalid")
		}
		previous = file.Path
		totalSize += file.Size
		if totalSize > 16<<20 {
			return storedSkillFileManifestV1{}, capabilityValidation("Skill file manifest exceeds size limit")
		}
		if _, duplicate := seen[file.Path]; duplicate {
			return storedSkillFileManifestV1{}, capabilityValidation("Skill file manifest path is duplicated")
		}
		seen[file.Path] = struct{}{}
		if file.Kind == "script" {
			if !skill.ContainsScripts || !strings.HasPrefix(file.Path, "scripts/") {
				return storedSkillFileManifestV1{}, capabilityValidation("Skill script manifest entry is invalid")
			}
			continue
		}
		if !validStoredSkillPath(file.Path, file.Kind) {
			return storedSkillFileManifestV1{}, capabilityValidation("Skill resource manifest path is invalid")
		}
	}
	return manifest, nil
}

func validateResolvedSkillDetail(detail *SkillCapabilityDetail) error {
	if detail == nil || !detail.Version.Compatible || detail.Skill.ContainsScripts ||
		detail.Ref.Validate() != nil {
		return capabilityValidation("Skill version is not declarative-runtime compatible")
	}
	manifest, err := decodeSkillFileManifest(detail.Skill)
	if err != nil {
		return err
	}
	resources := make(map[string]SkillResourceMetadata, len(detail.Resources))
	for _, resource := range detail.Resources {
		resources[resource.Path] = resource
	}
	foundSkillMD := false
	for _, file := range manifest.Files {
		resource, ok := resources[file.Path]
		if !ok || resource.Kind != file.Kind || resource.Digest != file.Digest ||
			resource.Size != file.Size {
			return capabilityValidation("Skill runtime manifest differs from stored resources")
		}
		delete(resources, file.Path)
		foundSkillMD = foundSkillMD || (file.Path == "SKILL.md" && file.Kind == "skill_md" &&
			file.Digest == detail.Skill.SkillMDDigest)
	}
	if len(resources) != 0 || !foundSkillMD {
		return capabilityValidation("Skill runtime manifest is incomplete")
	}
	return nil
}
