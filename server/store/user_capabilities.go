package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/YouToco/vane/server/internal/credentialguard"
	"github.com/YouToco/vane/server/mcpclient"
	"github.com/YouToco/vane/server/types"
)

var (
	capabilitySlugPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,127}$`)
	vaultReferencePattern = regexp.MustCompile(`^vault:[A-Za-z0-9][A-Za-z0-9._-]{0,239}$`)
)

// CreateSkillCapability atomically creates an installation, immutable v1,
// parser-approved files, and its audit event. It never executes package data.
func (s *Store) CreateSkillCapability(ctx context.Context, input types.CreateSkillCapability) (*types.UserCapability, *types.UserCapabilityVersion, error) {
	if err := validateCapabilityCreate(input.TenantID, input.ActorUserID, input.Visibility,
		input.Slug, input.DisplayName, input.PayloadDigest, input.Manifest); err != nil {
		return nil, nil, err
	}
	if input.Source != types.UserCapabilityUpload && input.Source != types.UserCapabilityPublicCatalog {
		return nil, nil, capabilityValidation("Skill source must be upload or public_catalog")
	}
	if len(input.SourceRef) > 2048 || !utf8.ValidString(input.SourceRef) ||
		strings.ContainsAny(input.SourceRef, "\x00\r\n?#") ||
		(input.Source == types.UserCapabilityPublicCatalog && input.SourceRef == "") {
		return nil, nil, capabilityValidation("Skill source reference is invalid")
	}
	if input.Skill.Name == "" || !capabilitySlugPattern.MatchString(input.Skill.Name) ||
		len(input.Skill.Description) > 4096 || !utf8.ValidString(input.Skill.Description) ||
		!validSHA256(input.Skill.SkillMDDigest) || !validSHA256(input.Skill.ArchiveDigest) ||
		!validJSONObject(input.Skill.FileManifest) {
		return nil, nil, capabilityValidation("Skill package metadata is invalid")
	}
	if input.Skill.ContainsScripts && input.Compatible {
		return nil, nil, capabilityValidation("Skill packages containing scripts must be incompatible")
	}
	if err := validateSkillFiles(input.Skill.Files, input.Skill.SkillMDDigest); err != nil {
		return nil, nil, err
	}
	if err := validateSkillCapabilityCredentialSafety(input); err != nil {
		return nil, nil, err
	}

	tx, role, err := s.beginCapabilityTx(ctx, input.TenantID, input.ActorUserID)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if input.Visibility == types.UserCapabilityWorkspace &&
		role != types.MembershipRoleOwner && role != types.MembershipRoleAdmin {
		return nil, nil, capabilityForbidden("only workspace owners or admins can publish shared capabilities")
	}

	capabilityID, versionID := uuid.New(), uuid.New()
	status := types.UserCapabilityDraft
	if !input.Compatible {
		status = types.UserCapabilityIncompatible
	}
	capability, version, err := insertCapabilityHeadAndVersion(ctx, tx, capabilityID, versionID,
		types.UserCapabilitySkill, status, input.TenantID, input.ActorUserID, input.Visibility,
		input.Slug, input.DisplayName, input.Source, input.SourceRef,
		input.PayloadDigest, input.Manifest, input.Compatible)
	if err != nil {
		return nil, nil, err
	}
	fileManifestDigest := digestBytes(input.Skill.FileManifest)
	_, err = tx.Exec(ctx, `
		INSERT INTO skill_capability_versions(
		 capability_version_id,capability_id,tenant_id,owner_user_id,visibility,
		 name,description,skill_md_digest,archive_digest,file_manifest_payload,
		 file_manifest_digest,contains_scripts)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
		versionID, capabilityID, input.TenantID, input.ActorUserID, input.Visibility,
		input.Skill.Name, input.Skill.Description, input.Skill.SkillMDDigest,
		input.Skill.ArchiveDigest, []byte(input.Skill.FileManifest), fileManifestDigest,
		input.Skill.ContainsScripts)
	if err != nil {
		return nil, nil, capabilityDatabase("store Skill version", err)
	}
	for _, file := range input.Skill.Files {
		if _, err := tx.Exec(ctx, `
			INSERT INTO skill_capability_files(
			 capability_version_id,capability_id,tenant_id,owner_user_id,visibility,
			 file_path,file_kind,content_digest,content_payload)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
			versionID, capabilityID, input.TenantID, input.ActorUserID, input.Visibility,
			file.Path, file.Kind, file.Digest, file.Content); err != nil {
			return nil, nil, capabilityDatabase("store Skill file", err)
		}
	}
	if err := appendCapabilityInstallEvent(ctx, tx, input.TenantID, input.ActorUserID,
		input.Visibility, capabilityID, versionID, input.Source); err != nil {
		return nil, nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, nil, capabilityDatabase("commit Skill capability", err)
	}
	return capability, version, nil
}

// CreateMCPCapability stores only a validated non-secret remote connection and
// frozen local read-only tool catalog. It performs no network request.
func (s *Store) CreateMCPCapability(ctx context.Context, input types.CreateMCPCapability) (*types.UserCapability, *types.UserCapabilityVersion, error) {
	if err := validateCapabilityCreate(input.TenantID, input.ActorUserID, input.Visibility,
		input.Slug, input.DisplayName, input.PayloadDigest, input.Manifest); err != nil {
		return nil, nil, err
	}
	if err := mcpclient.ValidateTransport(mcpclient.TransportStreamableHTTP, input.Connection.ProtocolVersion); err != nil {
		return nil, nil, capabilityValidation(err.Error())
	}
	if err := mcpclient.ValidateEndpointSyntax(input.Connection.EndpointURL); err != nil {
		return nil, nil, capabilityValidation("MCP endpoint must be a credential-free HTTPS URL")
	}
	if !validMCPAuthentication(input.Connection.Authentication, input.Connection.CredentialRef) ||
		!validSHA256(input.Connection.ToolSchemaDigest) || !validJSONObject(input.Connection.ToolSchema) ||
		digestBytes(input.Connection.ToolSchema) != input.Connection.ToolSchemaDigest {
		return nil, nil, capabilityValidation("MCP connection metadata is invalid")
	}
	if err := validateMCPCapabilityCredentialSafety(input); err != nil {
		return nil, nil, err
	}

	tx, role, err := s.beginCapabilityTx(ctx, input.TenantID, input.ActorUserID)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if input.Visibility == types.UserCapabilityWorkspace &&
		role != types.MembershipRoleOwner && role != types.MembershipRoleAdmin {
		return nil, nil, capabilityForbidden("only workspace owners or admins can publish shared capabilities")
	}
	capabilityID, versionID := uuid.New(), uuid.New()
	capability, version, err := insertCapabilityHeadAndVersion(ctx, tx, capabilityID, versionID,
		types.UserCapabilityMCP, types.UserCapabilityDraft, input.TenantID, input.ActorUserID,
		input.Visibility, input.Slug, input.DisplayName, types.UserCapabilityRemoteMCP, "",
		input.PayloadDigest, input.Manifest, true)
	if err != nil {
		return nil, nil, err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO mcp_connection_versions(
		 capability_version_id,capability_id,tenant_id,owner_user_id,visibility,
		 endpoint_url,protocol_version,authentication_kind,credential_ref,
		 tool_schema_payload,tool_schema_digest)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
		versionID, capabilityID, input.TenantID, input.ActorUserID, input.Visibility,
		input.Connection.EndpointURL, input.Connection.ProtocolVersion,
		input.Connection.Authentication, input.Connection.CredentialRef,
		[]byte(input.Connection.ToolSchema), input.Connection.ToolSchemaDigest)
	if err != nil {
		return nil, nil, capabilityDatabase("store MCP version", err)
	}
	if err := appendCapabilityInstallEvent(ctx, tx, input.TenantID, input.ActorUserID,
		input.Visibility, capabilityID, versionID, types.UserCapabilityRemoteMCP); err != nil {
		return nil, nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, nil, capabilityDatabase("commit MCP capability", err)
	}
	return capability, version, nil
}

// ListVisibleUserCapabilities relies on FORCE RLS in addition to the explicit
// tenant predicate, so a forgotten predicate cannot disclose another tenant.
func (s *Store) ListVisibleUserCapabilities(ctx context.Context, tenantID, actorUserID int64) ([]types.UserCapability, error) {
	tx, _, err := s.beginCapabilityTx(ctx, tenantID, actorUserID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, `
		SELECT id,tenant_id,owner_user_id,kind,visibility,slug,display_name,status,
		       current_version_id,created_at,updated_at
		FROM user_capabilities WHERE tenant_id=$1
		ORDER BY visibility,display_name,id`, tenantID)
	if err != nil {
		return nil, capabilityDatabase("list capabilities", err)
	}
	defer rows.Close()
	result := make([]types.UserCapability, 0)
	for rows.Next() {
		var item types.UserCapability
		if err := rows.Scan(&item.ID, &item.TenantID, &item.OwnerUserID, &item.Kind,
			&item.Visibility, &item.Slug, &item.DisplayName, &item.Status,
			&item.CurrentVersionID, &item.CreatedAt, &item.UpdatedAt); err != nil {
			return nil, capabilityDatabase("scan capability", err)
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, capabilityDatabase("iterate capabilities", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, capabilityDatabase("commit capability list", err)
	}
	return result, nil
}

func (s *Store) beginCapabilityTx(ctx context.Context, tenantID, actorUserID int64) (pgx.Tx, types.MembershipRole, error) {
	if tenantID <= 0 || actorUserID <= 0 {
		return nil, "", capabilityValidation("capability principal is invalid")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, "", capabilityDatabase("begin capability transaction", err)
	}
	var role types.MembershipRole
	err = tx.QueryRow(ctx, `
		SELECT m.role FROM memberships m JOIN tenants t ON t.id=m.tenant_id
		WHERE m.tenant_id=$1 AND m.user_id=$2 AND t.status='active' AND t.deleted_at IS NULL
		FOR SHARE OF m,t`, tenantID, actorUserID).Scan(&role)
	if err != nil {
		_ = tx.Rollback(ctx)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, "", capabilityForbidden("active workspace membership is required")
		}
		return nil, "", capabilityDatabase("resolve capability membership", err)
	}
	if _, err := tx.Exec(ctx, `
		SELECT set_config('app.tenant_id',$1,true),
		       set_config('app.user_id',$2,true),
		       set_config('app.membership_role',$3,true)`,
		fmt.Sprint(tenantID), fmt.Sprint(actorUserID), string(role)); err != nil {
		_ = tx.Rollback(ctx)
		return nil, "", capabilityDatabase("set capability scope", err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_app`); err != nil {
		_ = tx.Rollback(ctx)
		return nil, "", capabilityDatabase("enter capability role", err)
	}
	return tx, role, nil
}

func insertCapabilityHeadAndVersion(ctx context.Context, tx pgx.Tx, capabilityID, versionID uuid.UUID,
	kind types.UserCapabilityKind, status types.UserCapabilityStatus, tenantID, actorUserID int64,
	visibility types.UserCapabilityVisibility, slug, displayName string, source types.UserCapabilitySource,
	sourceRef, payloadDigest string, manifest json.RawMessage, compatible bool,
) (*types.UserCapability, *types.UserCapabilityVersion, error) {
	var capability types.UserCapability
	err := tx.QueryRow(ctx, `
		INSERT INTO user_capabilities(
		 id,tenant_id,owner_user_id,kind,visibility,slug,display_name,status)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8)
		RETURNING id,tenant_id,owner_user_id,kind,visibility,slug,display_name,status,
		          created_at,updated_at`, capabilityID, tenantID, actorUserID, kind,
		visibility, slug, displayName, status).Scan(
		&capability.ID, &capability.TenantID, &capability.OwnerUserID, &capability.Kind,
		&capability.Visibility, &capability.Slug, &capability.DisplayName, &capability.Status,
		&capability.CreatedAt, &capability.UpdatedAt)
	if err != nil {
		return nil, nil, capabilityDatabase("create capability", err)
	}
	var version types.UserCapabilityVersion
	err = tx.QueryRow(ctx, `
		INSERT INTO user_capability_versions(
		 id,capability_id,tenant_id,owner_user_id,version,visibility,source_kind,
		 source_ref,payload_digest,manifest_payload,compatible,created_by)
		VALUES($1,$2,$3,$4,1,$5,$6,$7,$8,$9,$10,$4)
		RETURNING id,capability_id,tenant_id,owner_user_id,version,source_kind,
		          source_ref,payload_digest,manifest_payload,compatible,created_by,created_at`,
		versionID, capabilityID, tenantID, actorUserID, visibility, source, sourceRef,
		payloadDigest, []byte(manifest), compatible).Scan(
		&version.ID, &version.CapabilityID, &version.TenantID, &version.OwnerUserID,
		&version.Version, &version.Source, &version.SourceRef, &version.PayloadDigest,
		&version.Manifest, &version.Compatible, &version.CreatedBy, &version.CreatedAt)
	if err != nil {
		return nil, nil, capabilityDatabase("create capability version", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE user_capabilities SET current_version_id=$2,updated_at=clock_timestamp()
		WHERE id=$1`, capabilityID, versionID); err != nil {
		return nil, nil, capabilityDatabase("advance capability head", err)
	}
	capability.CurrentVersionID = &versionID
	return &capability, &version, nil
}

func appendCapabilityInstallEvent(ctx context.Context, tx pgx.Tx, tenantID, actorUserID int64,
	visibility types.UserCapabilityVisibility, capabilityID, versionID uuid.UUID,
	source types.UserCapabilitySource,
) error {
	details, _ := json.Marshal(map[string]string{"source": string(source)})
	_, err := tx.Exec(ctx, `
		INSERT INTO user_capability_events(
		 tenant_id,capability_id,owner_user_id,visibility,actor_user_id,event_kind,version_id,details)
		VALUES($1,$2,$3,$4,$3,'installed',$5,$6)`,
		tenantID, capabilityID, actorUserID, visibility, versionID, details)
	if err != nil {
		return capabilityDatabase("append capability event", err)
	}
	return nil
}

func validateCapabilityCreate(tenantID, actorUserID int64, visibility types.UserCapabilityVisibility,
	slug, displayName, payloadDigest string, manifest json.RawMessage,
) error {
	if tenantID <= 0 || actorUserID <= 0 ||
		(visibility != types.UserCapabilityPersonal && visibility != types.UserCapabilityWorkspace) ||
		!capabilitySlugPattern.MatchString(slug) || strings.TrimSpace(displayName) != displayName ||
		len(displayName) == 0 || len(displayName) > 256 || !utf8.ValidString(displayName) ||
		!validSHA256(payloadDigest) || !validJSONObject(manifest) ||
		digestBytes(manifest) != payloadDigest {
		return capabilityValidation("capability metadata is invalid")
	}
	return nil
}

func validJSONObject(payload []byte) bool {
	if len(payload) < 2 || !json.Valid(payload) {
		return false
	}
	var object map[string]json.RawMessage
	return json.Unmarshal(payload, &object) == nil && object != nil
}

func validateSkillFiles(files []types.SkillCapabilityFile, skillMDDigest string) error {
	if len(files) == 0 || len(files) > 128 {
		return capabilityValidation("Skill file set is invalid")
	}
	seen := make(map[string]struct{}, len(files))
	foundSkillMD := false
	var total int
	for _, file := range files {
		if file.Path == "" || len(file.Path) > 1024 || !validStoredSkillPath(file.Path, file.Kind) ||
			(file.Kind != "skill_md" && file.Kind != "reference" && file.Kind != "asset") ||
			!validSHA256(file.Digest) || digestBytes(file.Content) != file.Digest || len(file.Content) > 4<<20 {
			return capabilityValidation("Skill file is invalid")
		}
		if _, duplicate := seen[file.Path]; duplicate {
			return capabilityValidation("Skill file path is duplicated")
		}
		seen[file.Path] = struct{}{}
		total += len(file.Content)
		if total > 16<<20 {
			return capabilityValidation("Skill file set exceeds size limit")
		}
		if file.Path == "SKILL.md" && file.Kind == "skill_md" && file.Digest == skillMDDigest {
			foundSkillMD = true
		}
	}
	if !foundSkillMD {
		return capabilityValidation("Skill file set has no matching SKILL.md")
	}
	return nil
}

func validateSkillCapabilityCredentialSafety(input types.CreateSkillCapability) error {
	values := []string{
		input.Slug, input.DisplayName, input.SourceRef, string(input.Manifest),
		input.Skill.Name, input.Skill.Description, string(input.Skill.FileManifest),
	}
	for _, value := range values {
		if credentialguard.ContainsCredential(value) {
			return capabilityValidation("Skill capability contains credential material")
		}
	}
	for _, file := range input.Skill.Files {
		if credentialguard.ContainsCredential(file.Path) ||
			credentialguard.ContainsCredential(string(file.Content)) {
			return capabilityValidation("Skill capability contains credential material")
		}
	}
	return nil
}

func validateMCPCapabilityCredentialSafety(input types.CreateMCPCapability) error {
	for _, value := range []string{
		input.Slug, input.DisplayName, string(input.Manifest),
		input.Connection.EndpointURL, string(input.Connection.ToolSchema),
	} {
		if credentialguard.ContainsCredential(value) {
			return capabilityValidation("MCP capability contains credential material")
		}
	}
	return nil
}

func validStoredSkillPath(filePath, kind string) bool {
	if strings.Contains(filePath, "\\") || strings.HasPrefix(filePath, "/") || path.Clean(filePath) != filePath {
		return false
	}
	for _, component := range strings.Split(filePath, "/") {
		if component == "" || strings.HasPrefix(component, ".") {
			return false
		}
	}
	switch kind {
	case "skill_md":
		return filePath == "SKILL.md"
	case "reference":
		return strings.HasPrefix(filePath, "references/")
	case "asset":
		return strings.HasPrefix(filePath, "assets/")
	default:
		return false
	}
}

func validMCPAuthentication(kind types.MCPAuthenticationKind, credentialRef string) bool {
	if len(credentialRef) > 256 || strings.ContainsAny(credentialRef, "\r\n\x00") {
		return false
	}
	switch kind {
	case types.MCPAuthenticationNone:
		return credentialRef == ""
	case types.MCPAuthenticationAPIKey, types.MCPAuthenticationBearer, types.MCPAuthenticationOAuth2:
		return vaultReferencePattern.MatchString(credentialRef)
	default:
		return false
	}
}

func validSHA256(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && value == strings.ToLower(value)
}

func digestBytes(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

func capabilityValidation(message string) error {
	return types.NewAppError(types.CodeValidation, message, nil)
}

func capabilityForbidden(message string) error {
	return types.NewAppError(types.CodeForbidden, message, nil)
}

func capabilityDatabase(message string, err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation {
		return types.NewAppError(types.CodeConflict, message, err)
	}
	return types.NewAppError(types.CodeDatabase, message, err)
}
