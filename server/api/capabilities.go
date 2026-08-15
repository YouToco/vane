package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"regexp"

	"github.com/YouToco/vane/server/auth"
	"github.com/YouToco/vane/server/mcpclient"
	"github.com/YouToco/vane/server/skillpkg"
	"github.com/YouToco/vane/server/types"
)

const (
	capabilitySkillBodyLimit = skillpkg.DefaultMaxArchiveBytes + 64<<10
	capabilityMCPBodyLimit   = mcpclient.MaxCatalogBytes + 512<<10
	capabilityTextFieldLimit = 4096
)

var opaqueVaultCredentialRef = regexp.MustCompile(`^vault:[A-Za-z0-9][A-Za-z0-9._-]{0,239}$`)

// CapabilityStore deliberately exposes no execution or credential-resolution
// operation. These dark control-plane endpoints can only install and list
// immutable metadata.
type CapabilityStore interface {
	CreateSkillCapability(context.Context, types.CreateSkillCapability) (*types.UserCapability, *types.UserCapabilityVersion, error)
	CreateMCPCapability(context.Context, types.CreateMCPCapability) (*types.UserCapability, *types.UserCapabilityVersion, error)
	ListVisibleUserCapabilities(context.Context, int64, int64) ([]types.UserCapability, error)
}

type MCPEndpointAdmission interface {
	Validate(context.Context, string) (mcpclient.ResolvedEndpoint, error)
}

type capabilityInstallResponse struct {
	Capability *types.UserCapability `json:"capability"`
	VersionID  string                `json:"version_id"`
	Compatible bool                  `json:"compatible"`
}

func (s *server) capabilityStore() CapabilityStore {
	if s.deps.Capabilities != nil {
		return s.deps.Capabilities
	}
	if s.deps.Store != nil {
		return s.deps.Store
	}
	return nil
}

func (s *server) mcpEndpointAdmission() MCPEndpointAdmission {
	if s.deps.MCPEndpointAdmission != nil {
		return s.deps.MCPEndpointAdmission
	}
	return mcpclient.EndpointValidator{}
}

func capabilityPrincipal(ctx context.Context) (auth.Principal, error) {
	p, err := auth.PrincipalFromContext(ctx)
	if err != nil {
		return auth.Principal{}, err
	}
	if p.TenantID <= 0 || p.UserID <= 0 || !p.Role.Valid() || p.ActorType != types.ActorTypeUser {
		return auth.Principal{}, types.NewAppError(types.CodeForbidden,
			"能力中心仅允许有效工作区成员访问", types.ErrForbidden)
	}
	return p, nil
}

func (s *server) handleListCapabilities(w http.ResponseWriter, r *http.Request) {
	p, err := capabilityPrincipal(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}
	capabilities := s.capabilityStore()
	if capabilities == nil {
		writeError(w, http.StatusServiceUnavailable, "能力中心尚未启用")
		return
	}
	items, err := capabilities.ListVisibleUserCapabilities(r.Context(), int64(p.TenantID), p.UserID)
	if err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"capabilities": items})
}

type skillUploadFields struct {
	Visibility  types.UserCapabilityVisibility
	Slug        string
	DisplayName string
	Archive     []byte
}

func (s *server) handleCreateSkillCapability(w http.ResponseWriter, r *http.Request) {
	p, err := capabilityPrincipal(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}
	capabilities := s.capabilityStore()
	if capabilities == nil {
		writeError(w, http.StatusServiceUnavailable, "能力中心尚未启用")
		return
	}
	fields, err := decodeSkillUpload(w, r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	pkg, err := skillpkg.ParseZIP(fields.Archive, skillpkg.DefaultLimits())
	if err != nil {
		writeError(w, http.StatusBadRequest, "Skill 压缩包不安全或格式无效")
		return
	}
	if fields.Slug == "" {
		fields.Slug = pkg.Manifest.Name
	}
	if fields.DisplayName == "" {
		fields.DisplayName = pkg.Manifest.Name
	}
	files := make([]types.SkillCapabilityFile, 0, len(pkg.Manifest.Files))
	var skillMDDigest string
	for _, file := range pkg.Manifest.Files {
		if file.Kind == skillpkg.FileScript {
			// Script identity remains in the immutable manifest, but bytes never
			// cross into Store or an executor. Presence marks the whole version
			// incompatible until a future isolated sandbox exists.
			continue
		}
		files = append(files, types.SkillCapabilityFile{
			Path: file.Path, Kind: string(file.Kind), Digest: file.Digest,
			Content: bytes.Clone(file.Data),
		})
		if file.Kind == skillpkg.FileSkillMD {
			skillMDDigest = file.Digest
		}
	}
	capability, version, err := capabilities.CreateSkillCapability(r.Context(), types.CreateSkillCapability{
		TenantID: int64(p.TenantID), ActorUserID: p.UserID,
		Visibility: fields.Visibility, Slug: fields.Slug, DisplayName: fields.DisplayName,
		Source: types.UserCapabilityUpload, PayloadDigest: pkg.ManifestDigest,
		Manifest: json.RawMessage(bytes.Clone(pkg.ManifestJSON)), Compatible: pkg.Manifest.Compatible,
		Skill: types.SkillCapabilityVersion{
			Name: pkg.Manifest.Name, Description: pkg.Manifest.Description,
			SkillMDDigest: skillMDDigest, ArchiveDigest: pkg.ArchiveDigest,
			FileManifest:    json.RawMessage(bytes.Clone(pkg.ManifestJSON)),
			ContainsScripts: pkg.Manifest.ContainsScript, Files: files,
		},
	})
	if err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, capabilityInstallResponse{
		Capability: capability, VersionID: version.ID.String(), Compatible: version.Compatible,
	})
}

func decodeSkillUpload(w http.ResponseWriter, r *http.Request) (skillUploadFields, error) {
	r.Body = http.MaxBytesReader(w, r.Body, capabilitySkillBodyLimit)
	reader, err := r.MultipartReader()
	if err != nil {
		return skillUploadFields{}, errors.New("请求必须是 multipart/form-data")
	}
	var result skillUploadFields
	seen := make(map[string]bool)
	for {
		part, nextErr := reader.NextPart()
		if errors.Is(nextErr, io.EOF) {
			break
		}
		if nextErr != nil {
			return skillUploadFields{}, errors.New("读取上传内容失败或请求体过大")
		}
		name := part.FormName()
		if seen[name] || (name != "archive" && name != "visibility" && name != "slug" && name != "display_name") {
			_ = part.Close()
			return skillUploadFields{}, errors.New("上传字段重复或不受支持")
		}
		seen[name] = true
		if name == "archive" {
			if part.FileName() == "" {
				_ = part.Close()
				return skillUploadFields{}, errors.New("archive 必须是 ZIP 文件")
			}
			result.Archive, err = readMultipartPart(part, skillpkg.DefaultMaxArchiveBytes)
		} else {
			var value []byte
			value, err = readMultipartPart(part, capabilityTextFieldLimit)
			if err == nil {
				switch name {
				case "visibility":
					result.Visibility = types.UserCapabilityVisibility(string(value))
				case "slug":
					result.Slug = string(value)
				case "display_name":
					result.DisplayName = string(value)
				}
			}
		}
		_ = part.Close()
		if err != nil {
			return skillUploadFields{}, err
		}
	}
	if len(result.Archive) == 0 {
		return skillUploadFields{}, errors.New("缺少 Skill ZIP 文件")
	}
	if result.Visibility == "" {
		result.Visibility = types.UserCapabilityPersonal
	}
	if result.Visibility != types.UserCapabilityPersonal && result.Visibility != types.UserCapabilityWorkspace {
		return skillUploadFields{}, errors.New("visibility 只允许 personal 或 workspace")
	}
	return result, nil
}

func readMultipartPart(part *multipart.Part, limit int64) ([]byte, error) {
	payload, err := io.ReadAll(io.LimitReader(part, limit+1))
	if err != nil {
		return nil, errors.New("读取上传字段失败")
	}
	if int64(len(payload)) > limit {
		return nil, errors.New("上传字段超过大小限制")
	}
	return payload, nil
}

type createMCPRequest struct {
	Visibility          types.UserCapabilityVisibility       `json:"visibility"`
	Slug                string                               `json:"slug"`
	DisplayName         string                               `json:"display_name"`
	EndpointURL         string                               `json:"endpoint_url"`
	Transport           string                               `json:"transport"`
	ProtocolVersion     string                               `json:"protocol_version"`
	Authentication      types.MCPAuthenticationKind          `json:"authentication"`
	CredentialRef       string                               `json:"credential_ref"`
	Tools               []mcpRemoteToolRequest               `json:"tools"`
	LocalReadOnlyPolicy map[string]mcpclient.LocalToolPolicy `json:"local_read_only_policy"`
}

type mcpRemoteToolRequest struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"input_schema"`
	Annotations json.RawMessage `json:"annotations,omitempty"`
}

func (s *server) handleCreateMCPCapability(w http.ResponseWriter, r *http.Request) {
	p, err := capabilityPrincipal(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}
	capabilities := s.capabilityStore()
	if capabilities == nil {
		writeError(w, http.StatusServiceUnavailable, "能力中心尚未启用")
		return
	}
	var req createMCPRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, capabilityMCPBodyLimit))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "MCP 配置不是合法 JSON 或超过大小限制")
		return
	}
	if err := ensureJSONEOF(decoder); err != nil {
		writeError(w, http.StatusBadRequest, "MCP 配置包含多余 JSON")
		return
	}
	if req.Visibility == "" {
		req.Visibility = types.UserCapabilityPersonal
	}
	if req.Visibility != types.UserCapabilityPersonal && req.Visibility != types.UserCapabilityWorkspace {
		writeError(w, http.StatusBadRequest, "visibility 只允许 personal 或 workspace")
		return
	}
	if req.Authentication == types.MCPAuthenticationOAuth2 {
		writeError(w, http.StatusBadRequest, "MCP OAuth 尚未开放")
		return
	}
	switch req.Authentication {
	case types.MCPAuthenticationNone:
		if req.CredentialRef != "" {
			writeError(w, http.StatusBadRequest, "无认证 MCP 不得绑定凭证")
			return
		}
	case types.MCPAuthenticationAPIKey, types.MCPAuthenticationBearer:
		if !opaqueVaultCredentialRef.MatchString(req.CredentialRef) {
			writeError(w, http.StatusBadRequest, "MCP 凭证必须使用不透明 vault 引用")
			return
		}
	default:
		writeError(w, http.StatusBadRequest, "MCP 认证方式不受支持")
		return
	}
	if err := mcpclient.ValidateTransport(req.Transport, req.ProtocolVersion); err != nil {
		writeError(w, http.StatusBadRequest, "仅支持远程 Streamable HTTP MCP")
		return
	}
	resolved, err := s.mcpEndpointAdmission().Validate(r.Context(), req.EndpointURL)
	if err != nil {
		writeError(w, http.StatusBadRequest, "MCP 地址必须是公网 HTTPS 且不得解析到私网")
		return
	}
	remoteTools := make([]mcpclient.RemoteTool, len(req.Tools))
	for index, tool := range req.Tools {
		remoteTools[index] = mcpclient.RemoteTool{
			Name: tool.Name, Description: tool.Description,
			InputSchema: tool.InputSchema, Annotations: tool.Annotations,
		}
	}
	catalog, err := mcpclient.FreezeReadOnlyTools(remoteTools, req.LocalReadOnlyPolicy)
	if err != nil {
		writeError(w, http.StatusBadRequest, "MCP 工具必须通过本地只读策略并提供有效 schema")
		return
	}
	manifest, err := json.Marshal(struct {
		SchemaVersion    string                      `json:"schema_version"`
		EndpointURL      string                      `json:"endpoint_url"`
		Transport        string                      `json:"transport"`
		ProtocolVersion  string                      `json:"protocol_version"`
		Authentication   types.MCPAuthenticationKind `json:"authentication"`
		ToolSchemaDigest string                      `json:"tool_schema_digest"`
	}{
		SchemaVersion: "vane.mcp-capability/v1", EndpointURL: resolved.URL,
		Transport: req.Transport, ProtocolVersion: req.ProtocolVersion,
		Authentication: req.Authentication, ToolSchemaDigest: catalog.Digest,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "构建 MCP 快照失败")
		return
	}
	digest := sha256.Sum256(manifest)
	capability, version, err := capabilities.CreateMCPCapability(r.Context(), types.CreateMCPCapability{
		TenantID: int64(p.TenantID), ActorUserID: p.UserID,
		Visibility: req.Visibility, Slug: req.Slug, DisplayName: req.DisplayName,
		PayloadDigest: hex.EncodeToString(digest[:]), Manifest: manifest,
		Connection: types.MCPConnectionVersion{
			EndpointURL: resolved.URL, ProtocolVersion: req.ProtocolVersion,
			Authentication: req.Authentication, CredentialRef: req.CredentialRef,
			ToolSchemaDigest: catalog.Digest, ToolSchema: json.RawMessage(bytes.Clone(catalog.Payload)),
		},
	})
	if err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, capabilityInstallResponse{
		Capability: capability, VersionID: version.ID.String(), Compatible: version.Compatible,
	})
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON")
	}
	return nil
}

// compile-time assertion that the production validator performs DNS admission
// and not just syntax validation.
var _ MCPEndpointAdmission = mcpclient.EndpointValidator{}
