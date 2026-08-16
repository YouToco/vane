// Package skillruntime defines the data-only, exact-version contract for
// declarative Skill resources. It cannot execute scripts, call the network, or
// decide whether a Skill is visible to a principal; those authorities remain
// in the Store and the Vane harness.
package skillruntime

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"
)

const (
	SkillRefSchemaVersionV1            = "vane.skill-ref/v1"
	SkillResourceHandleSchemaVersionV1 = "vane.skill-resource-handle/v1"
	SkillResourceChunkSchemaVersionV1  = "vane.skill-resource-chunk/v1"

	VisibilityPersonalV1  = "personal"
	VisibilityWorkspaceV1 = "workspace"

	ResourceKindSkillMDV1   = "skill_md"
	ResourceKindReferenceV1 = "reference"
	ResourceKindAssetV1     = "asset"

	MaxResourceChunkBytesV1 = 64 << 10
)

var ErrInvalidSkillContract = errors.New("skillruntime: invalid contract")

// SkillRefV1 freezes one visible immutable Skill version. It intentionally
// carries identities and digests only: Skill instructions and resources never
// enter a policy manifest, model request, tool description, or history through
// this type.
type SkillRefV1 struct {
	SchemaVersion       string    `json:"schema_version"`
	TenantID            int64     `json:"tenant_id"`
	OwnerUserID         int64     `json:"owner_user_id"`
	CapabilityID        uuid.UUID `json:"capability_id"`
	CapabilityVersionID uuid.UUID `json:"capability_version_id"`
	Version             int       `json:"version"`
	Visibility          string    `json:"visibility"`
	PayloadDigest       string    `json:"payload_digest"`
	SkillMDDigest       string    `json:"skill_md_digest"`
	FileManifestDigest  string    `json:"file_manifest_digest"`
	Compatible          bool      `json:"compatible"`
	ContainsScripts     bool      `json:"contains_scripts"`
}

// SkillResourceHandleV1 binds a resource path and digest to an exact Skill
// reference. It contains no resource bytes and is safe to persist internally.
type SkillResourceHandleV1 struct {
	SchemaVersion string     `json:"schema_version"`
	Skill         SkillRefV1 `json:"skill"`
	Path          string     `json:"path"`
	Kind          string     `json:"kind"`
	ContentDigest string     `json:"content_digest"`
	ContentSize   int64      `json:"content_size"`
}

// SkillResourceChunkV1 is an internal bounded read result. Data is deliberately
// absent from SkillRef and Handle and may only be obtained through the scoped
// Store reader. It is never model-visible by contract.
type SkillResourceChunkV1 struct {
	SchemaVersion string `json:"schema_version"`
	HandleDigest  string `json:"handle_digest"`
	Offset        int64  `json:"offset"`
	Data          []byte `json:"data"`
	EOF           bool   `json:"eof"`
}

func (r SkillRefV1) Validate() error {
	if r.SchemaVersion != SkillRefSchemaVersionV1 || r.TenantID <= 0 ||
		r.OwnerUserID <= 0 || r.CapabilityID == uuid.Nil ||
		r.CapabilityVersionID == uuid.Nil || r.Version <= 0 ||
		(r.Visibility != VisibilityPersonalV1 && r.Visibility != VisibilityWorkspaceV1) ||
		!validDigest(r.PayloadDigest) || !validDigest(r.SkillMDDigest) ||
		!validDigest(r.FileManifestDigest) || !r.Compatible || r.ContainsScripts {
		return invalid("Skill ref")
	}
	return nil
}

func (h SkillResourceHandleV1) Validate() error {
	if h.SchemaVersion != SkillResourceHandleSchemaVersionV1 || h.Skill.Validate() != nil ||
		!validResourcePath(h.Path, h.Kind) || !validDigest(h.ContentDigest) ||
		h.ContentSize < 0 || h.ContentSize > 4<<20 {
		return invalid("Skill resource handle")
	}
	return nil
}

func (c SkillResourceChunkV1) Validate(handle SkillResourceHandleV1) error {
	if handle.Validate() != nil || c.SchemaVersion != SkillResourceChunkSchemaVersionV1 ||
		!validDigest(c.HandleDigest) || c.HandleDigest != DigestHandleV1(handle) ||
		c.Data == nil || c.Offset < 0 || c.Offset > handle.ContentSize ||
		len(c.Data) > MaxResourceChunkBytesV1 ||
		int64(len(c.Data)) > handle.ContentSize-c.Offset ||
		c.EOF != (c.Offset+int64(len(c.Data)) == handle.ContentSize) ||
		(!c.EOF && len(c.Data) == 0) {
		return invalid("Skill resource chunk")
	}
	return nil
}

func DigestRefV1(ref SkillRefV1) string {
	payload, err := EncodeSkillRefV1(ref)
	if err != nil {
		return ""
	}
	return digest(payload)
}

func DigestHandleV1(handle SkillResourceHandleV1) string {
	payload, err := EncodeSkillResourceHandleV1(handle)
	if err != nil {
		return ""
	}
	return digest(payload)
}

func validResourcePath(value, kind string) bool {
	if value == "" || len(value) > 1024 || !utf8.ValidString(value) ||
		strings.Contains(value, "\\") || strings.HasPrefix(value, "/") || path.Clean(value) != value {
		return false
	}
	for _, component := range strings.Split(value, "/") {
		if component == "" || strings.HasPrefix(component, ".") {
			return false
		}
	}
	switch kind {
	case ResourceKindSkillMDV1:
		return value == "SKILL.md"
	case ResourceKindReferenceV1:
		return strings.HasPrefix(value, "references/")
	case ResourceKindAssetV1:
		return strings.HasPrefix(value, "assets/")
	default:
		return false
	}
}

func validDigest(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && value == strings.ToLower(value)
}

func digest(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

func invalid(subject string) error {
	return fmt.Errorf("%w: %s is invalid", ErrInvalidSkillContract, subject)
}
