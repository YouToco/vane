package types

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// UserCapabilityKind distinguishes the two lower-trust extension formats that
// the Vane harness can compile. A capability never becomes executable merely
// because one of these records exists.
type UserCapabilityKind string

const (
	UserCapabilitySkill UserCapabilityKind = "skill"
	UserCapabilityMCP   UserCapabilityKind = "mcp"
)

// UserCapabilityVisibility controls discovery inside one tenant. Personal
// capabilities remain visible only to their owner; workspace capabilities are
// visible to tenant members but can only be published by an owner or admin.
type UserCapabilityVisibility string

const (
	UserCapabilityPersonal  UserCapabilityVisibility = "personal"
	UserCapabilityWorkspace UserCapabilityVisibility = "workspace"
)

type UserCapabilityStatus string

const (
	UserCapabilityDraft        UserCapabilityStatus = "draft"
	UserCapabilityActive       UserCapabilityStatus = "active"
	UserCapabilityPaused       UserCapabilityStatus = "paused"
	UserCapabilityIncompatible UserCapabilityStatus = "incompatible"
)

type UserCapabilitySource string

const (
	UserCapabilityUpload        UserCapabilitySource = "upload"
	UserCapabilityPublicCatalog UserCapabilitySource = "public_catalog"
	UserCapabilityRemoteMCP     UserCapabilitySource = "remote_mcp"
)

// UserCapability is the mutable installation head. CurrentVersionID may move
// only to another immutable version belonging to the same installation.
type UserCapability struct {
	ID               uuid.UUID                `json:"id"`
	TenantID         int64                    `json:"tenant_id"`
	OwnerUserID      int64                    `json:"owner_user_id"`
	Kind             UserCapabilityKind       `json:"kind"`
	Visibility       UserCapabilityVisibility `json:"visibility"`
	Slug             string                   `json:"slug"`
	DisplayName      string                   `json:"display_name"`
	Status           UserCapabilityStatus     `json:"status"`
	CurrentVersionID *uuid.UUID               `json:"current_version_id,omitempty"`
	CreatedAt        time.Time                `json:"created_at"`
	UpdatedAt        time.Time                `json:"updated_at"`
}

// UserCapabilityVersion is immutable content-addressed configuration. Payload
// contains declarative non-secret metadata only; credentials live in the
// separate encrypted credential system and are referenced by opaque ID.
type UserCapabilityVersion struct {
	ID            uuid.UUID            `json:"id"`
	CapabilityID  uuid.UUID            `json:"capability_id"`
	TenantID      int64                `json:"tenant_id"`
	OwnerUserID   int64                `json:"owner_user_id"`
	Version       int                  `json:"version"`
	Source        UserCapabilitySource `json:"source"`
	SourceRef     string               `json:"source_ref,omitempty"`
	PayloadDigest string               `json:"payload_digest"`
	Manifest      json.RawMessage      `json:"manifest"`
	Compatible    bool                 `json:"compatible"`
	CreatedBy     int64                `json:"created_by"`
	CreatedAt     time.Time            `json:"created_at"`
}

type SkillCapabilityVersion struct {
	CapabilityVersionID uuid.UUID             `json:"capability_version_id"`
	Name                string                `json:"name"`
	Description         string                `json:"description,omitempty"`
	SkillMDDigest       string                `json:"skill_md_digest"`
	ArchiveDigest       string                `json:"archive_digest"`
	FileManifest        json.RawMessage       `json:"file_manifest"`
	ContainsScripts     bool                  `json:"contains_scripts"`
	Files               []SkillCapabilityFile `json:"files,omitempty"`
	CreatedAt           time.Time             `json:"created_at"`
}

type SkillCapabilityFile struct {
	Path    string `json:"path"`
	Kind    string `json:"kind"`
	Digest  string `json:"digest"`
	Content []byte `json:"-"`
}

type MCPAuthenticationKind string

const (
	MCPAuthenticationNone   MCPAuthenticationKind = "none"
	MCPAuthenticationAPIKey MCPAuthenticationKind = "api_key"
	MCPAuthenticationBearer MCPAuthenticationKind = "bearer"
	MCPAuthenticationOAuth2 MCPAuthenticationKind = "oauth2"
)

// MCPConnectionVersion freezes the non-secret connection and tool catalog.
// CredentialRef is opaque and may be resolved only by the trusted harness.
type MCPConnectionVersion struct {
	CapabilityVersionID uuid.UUID             `json:"capability_version_id"`
	EndpointURL         string                `json:"endpoint_url"`
	ProtocolVersion     string                `json:"protocol_version"`
	Authentication      MCPAuthenticationKind `json:"authentication"`
	CredentialRef       string                `json:"credential_ref,omitempty"`
	ToolSchemaDigest    string                `json:"tool_schema_digest"`
	ToolSchema          json.RawMessage       `json:"tool_schema"`
	CreatedAt           time.Time             `json:"created_at"`
}

type UserCapabilityEvent struct {
	ID           int64           `json:"id"`
	TenantID     int64           `json:"tenant_id"`
	CapabilityID uuid.UUID       `json:"capability_id"`
	ActorUserID  int64           `json:"actor_user_id"`
	EventKind    string          `json:"event_kind"`
	VersionID    *uuid.UUID      `json:"version_id,omitempty"`
	Details      json.RawMessage `json:"details"`
	CreatedAt    time.Time       `json:"created_at"`
}

type CreateSkillCapability struct {
	TenantID      int64
	ActorUserID   int64
	Visibility    UserCapabilityVisibility
	Slug          string
	DisplayName   string
	Source        UserCapabilitySource
	SourceRef     string
	PayloadDigest string
	Manifest      json.RawMessage
	Compatible    bool
	Skill         SkillCapabilityVersion
}

type CreateMCPCapability struct {
	TenantID      int64
	ActorUserID   int64
	Visibility    UserCapabilityVisibility
	Slug          string
	DisplayName   string
	PayloadDigest string
	Manifest      json.RawMessage
	Connection    MCPConnectionVersion
}
