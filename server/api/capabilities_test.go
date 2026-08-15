package api

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/server/auth"
	"github.com/YouToco/vane/server/mcpclient"
	"github.com/YouToco/vane/server/skillpkg"
	"github.com/YouToco/vane/server/types"
)

type fakeCapabilityStore struct {
	listTenant int64
	listUser   int64
	listed     []types.UserCapability
	skillInput *types.CreateSkillCapability
	mcpInput   *types.CreateMCPCapability
	createErr  error
}

func fakeCapabilityResult(inputTenant, inputUser int64, kind types.UserCapabilityKind,
	visibility types.UserCapabilityVisibility, compatible bool,
) (*types.UserCapability, *types.UserCapabilityVersion) {
	capabilityID, versionID := uuid.New(), uuid.New()
	return &types.UserCapability{
			ID: capabilityID, TenantID: inputTenant, OwnerUserID: inputUser,
			Kind: kind, Visibility: visibility, Slug: "installed", DisplayName: "Installed",
			Status: types.UserCapabilityDraft, CurrentVersionID: &versionID,
		}, &types.UserCapabilityVersion{
			ID: versionID, CapabilityID: capabilityID, TenantID: inputTenant,
			OwnerUserID: inputUser, Compatible: compatible,
		}
}

func (f *fakeCapabilityStore) CreateSkillCapability(_ context.Context, input types.CreateSkillCapability) (*types.UserCapability, *types.UserCapabilityVersion, error) {
	f.skillInput = &input
	if f.createErr != nil {
		return nil, nil, f.createErr
	}
	capability, version := fakeCapabilityResult(input.TenantID, input.ActorUserID,
		types.UserCapabilitySkill, input.Visibility, input.Compatible)
	return capability, version, nil
}

func (f *fakeCapabilityStore) CreateMCPCapability(_ context.Context, input types.CreateMCPCapability) (*types.UserCapability, *types.UserCapabilityVersion, error) {
	f.mcpInput = &input
	if f.createErr != nil {
		return nil, nil, f.createErr
	}
	capability, version := fakeCapabilityResult(input.TenantID, input.ActorUserID,
		types.UserCapabilityMCP, input.Visibility, true)
	return capability, version, nil
}

func (f *fakeCapabilityStore) ListVisibleUserCapabilities(_ context.Context, tenantID, userID int64) ([]types.UserCapability, error) {
	f.listTenant, f.listUser = tenantID, userID
	return f.listed, nil
}

type fakeMCPAdmission struct {
	validated string
	err       error
}

type fakeMCPReadOnlyPolicy struct {
	resolvedURL string
	toolNames   []string
	policy      map[string]mcpclient.LocalToolPolicy
	err         error
}

func (f *fakeMCPReadOnlyPolicy) Resolve(_ context.Context, _ auth.Principal,
	endpoint mcpclient.ResolvedEndpoint, tools []mcpclient.RemoteTool,
) (map[string]mcpclient.LocalToolPolicy, error) {
	f.resolvedURL = endpoint.URL
	for _, tool := range tools {
		f.toolNames = append(f.toolNames, tool.Name)
	}
	return f.policy, f.err
}

func (f *fakeMCPAdmission) Validate(_ context.Context, raw string) (mcpclient.ResolvedEndpoint, error) {
	f.validated = raw
	if f.err != nil {
		return mcpclient.ResolvedEndpoint{}, f.err
	}
	return mcpclient.ResolvedEndpoint{
		URL: raw, Host: "mcp.example.com", Addresses: []netip.Addr{netip.MustParseAddr("8.8.8.8")},
	}, nil
}

func capabilityTestMux(t *testing.T, tenantID, userID int64, role types.MembershipRole,
	capabilities CapabilityStore, admission MCPEndpointAdmission,
) (*http.ServeMux, *http.Cookie) {
	return capabilityTestMuxWithPolicy(t, tenantID, userID, role, capabilities, admission,
		&fakeMCPReadOnlyPolicy{policy: map[string]mcpclient.LocalToolPolicy{
			"read_market": {ReadOnly: true, Budget: 3},
		}})
}

func capabilityTestMuxWithPolicy(t *testing.T, tenantID, userID int64, role types.MembershipRole,
	capabilities CapabilityStore, admission MCPEndpointAdmission, policy MCPReadOnlyPolicyResolver,
) (*http.ServeMux, *http.Cookie) {
	t.Helper()
	authStore := newFakeAuthStore()
	token, hash, err := auth.NewSessionToken()
	if err != nil {
		t.Fatal(err)
	}
	authStore.sessions[string(hash)] = &types.Session{
		TokenHash: hash, TenantID: tenantID, UserID: userID, Role: role,
		ActorType: types.ActorTypeUser, ExpiresAt: time.Now().Add(time.Hour),
	}
	authStore.members[userID] = []types.Membership{{
		TenantID: tenantID, UserID: userID, Role: role,
	}}
	mux := http.NewServeMux()
	Mount(mux, Deps{
		Auth: authStore, Principal: auth.NewContextResolver(), Capabilities: capabilities,
		MCPEndpointAdmission: admission, MCPReadOnlyPolicy: policy,
	})
	return mux, &http.Cookie{Name: sessionCookieName, Value: token}
}

func serveCapabilityRequest(mux *http.ServeMux, request *http.Request, cookie *http.Cookie) *httptest.ResponseRecorder {
	if cookie != nil {
		request.AddCookie(cookie)
	}
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, request)
	return recorder
}

func TestCapabilityListUsesOnlyPrincipalScope(t *testing.T) {
	store := &fakeCapabilityStore{listed: []types.UserCapability{{
		ID: uuid.New(), TenantID: 11, OwnerUserID: 22,
		Kind: types.UserCapabilitySkill, Visibility: types.UserCapabilityPersonal,
	}}}
	mux, cookie := capabilityTestMux(t, 11, 22, types.MembershipRoleMember, store, nil)
	recorder := serveCapabilityRequest(mux,
		httptest.NewRequest(http.MethodGet, "/api/capabilities?tenant_id=999", nil), cookie)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if store.listTenant != 11 || store.listUser != 22 {
		t.Fatalf("query parameter escaped principal scope: tenant=%d user=%d", store.listTenant, store.listUser)
	}
}

func skillZIP(t *testing.T, withScript bool) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer := zip.NewWriter(&buffer)
	skill, err := writer.Create("SKILL.md")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = skill.Write([]byte("---\nname: market-watch\ndescription: Safe watcher\n---\nInstructions"))
	if withScript {
		script, createErr := writer.Create("scripts/run.sh")
		if createErr != nil {
			t.Fatal(createErr)
		}
		_, _ = script.Write([]byte("echo never-executed"))
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

func skillZIPWithFiles(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer := zip.NewWriter(&buffer)
	for name, content := range files {
		part, err := writer.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := part.Write([]byte(content)); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

func skillUploadRequest(t *testing.T, visibility string, archive []byte, extraField bool) *http.Request {
	t.Helper()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	_ = writer.WriteField("visibility", visibility)
	_ = writer.WriteField("display_name", "Market Watch")
	if extraField {
		_ = writer.WriteField("tenant_id", "999")
	}
	part, err := writer.CreateFormFile("archive", "skill.zip")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = part.Write(archive)
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/api/capabilities/skills", &body)
	request.Header.Set("Content-Type", writer.FormDataContentType())
	return request
}

func TestSkillUploadParsesDeclarativePackageAndNeverStoresScripts(t *testing.T) {
	store := &fakeCapabilityStore{}
	mux, cookie := capabilityTestMux(t, 31, 41, types.MembershipRoleMember, store, nil)
	recorder := serveCapabilityRequest(mux, skillUploadRequest(t, "personal", skillZIP(t, true), false), cookie)
	if recorder.Code != http.StatusCreated {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if store.skillInput == nil || store.skillInput.TenantID != 31 || store.skillInput.ActorUserID != 41 {
		t.Fatalf("principal not propagated: %+v", store.skillInput)
	}
	if store.skillInput.Compatible || !store.skillInput.Skill.ContainsScripts {
		t.Fatalf("script package must stay incompatible: %+v", store.skillInput.Skill)
	}
	for _, file := range store.skillInput.Skill.Files {
		if strings.HasPrefix(file.Path, "scripts/") || file.Kind == "script" {
			t.Fatalf("script crossed persistence boundary: %+v", file)
		}
	}
	if !strings.Contains(string(store.skillInput.Manifest), `"contains_scripts":true`) {
		t.Fatal("immutable manifest lost script incompatibility evidence")
	}
}

func TestSkillWorkspacePublishForbiddenIsHTTP403(t *testing.T) {
	for _, sessionRole := range []types.MembershipRole{
		types.MembershipRoleMember,
		// Even an owner role cached in the request principal cannot override a
		// live Store denial after membership was concurrently downgraded.
		types.MembershipRoleOwner,
	} {
		store := &fakeCapabilityStore{createErr: types.NewAppError(
			types.CodeForbidden, "only workspace owners or admins can publish", types.ErrForbidden)}
		mux, cookie := capabilityTestMux(t, 51, 61, sessionRole, store, nil)
		recorder := serveCapabilityRequest(mux, skillUploadRequest(t, "workspace", skillZIP(t, false), false), cookie)
		if recorder.Code != http.StatusForbidden {
			t.Fatalf("session_role=%s status=%d body=%s", sessionRole, recorder.Code, recorder.Body.String())
		}
	}
}

func TestSkillUploadRejectsTenantOverrideAndOversizeBeforeStore(t *testing.T) {
	store := &fakeCapabilityStore{}
	mux, cookie := capabilityTestMux(t, 71, 81, types.MembershipRoleOwner, store, nil)
	recorder := serveCapabilityRequest(mux, skillUploadRequest(t, "personal", skillZIP(t, false), true), cookie)
	if recorder.Code != http.StatusBadRequest || store.skillInput != nil {
		t.Fatalf("tenant override status=%d stored=%v", recorder.Code, store.skillInput != nil)
	}

	oversize := bytes.Repeat([]byte("x"), skillpkg.DefaultMaxArchiveBytes+1)
	recorder = serveCapabilityRequest(mux, skillUploadRequest(t, "personal", oversize, false), cookie)
	if recorder.Code != http.StatusBadRequest || store.skillInput != nil {
		t.Fatalf("oversize status=%d stored=%v", recorder.Code, store.skillInput != nil)
	}
}

func TestSkillUploadRejectsCredentialMaterialBeforeStore(t *testing.T) {
	for name, files := range map[string]map[string]string{
		"github_pat": {
			"SKILL.md": "---\nname: market-watch\ndescription: Safe watcher\n---\nghp_123456789012345678901234567890",
		},
		"jwt_manifest_field": {
			"SKILL.md": "---\nname: market-watch\ndescription: eyJabcdefghijk.abcdefghijk.abcdefghijk\n---\nInstructions",
		},
		"credential_dsn_reference": {
			"SKILL.md":             "---\nname: market-watch\ndescription: Safe watcher\n---\nInstructions",
			"references/source.md": "postgres://user:password@db.example/vane",
		},
		"general_token": {
			"SKILL.md": "---\nname: market-watch\ndescription: Safe watcher\n---\ntoken: abcdefghijklmnop",
		},
	} {
		t.Run(name, func(t *testing.T) {
			store := &fakeCapabilityStore{}
			mux, cookie := capabilityTestMux(t, 71, 81, types.MembershipRoleOwner, store, nil)
			recorder := serveCapabilityRequest(mux,
				skillUploadRequest(t, "personal", skillZIPWithFiles(t, files), false), cookie)
			if recorder.Code != http.StatusBadRequest || store.skillInput != nil {
				t.Fatalf("status=%d stored=%v body=%s", recorder.Code, store.skillInput != nil, recorder.Body.String())
			}
		})
	}
}

func validMCPRequest() map[string]any {
	return map[string]any{
		"visibility": "workspace", "slug": "market-mcp", "display_name": "Market MCP",
		"endpoint_url": "https://mcp.example.com/v1", "transport": "streamable_http",
		"protocol_version": "2025-11-25", "authentication": "bearer",
		"credential_ref": "vault:credential-123",
		"tools": []map[string]any{{
			"name": "read_market", "description": "Read market records",
			"input_schema": map[string]any{"type": "object", "properties": map[string]any{}},
			"annotations":  map[string]any{"readOnlyHint": false},
		}},
	}
}

func TestMCPCreateValidatesEndpointFreezesToolsAndNeverEchoesCredential(t *testing.T) {
	store := &fakeCapabilityStore{}
	admission := &fakeMCPAdmission{}
	trustedPolicy := &fakeMCPReadOnlyPolicy{policy: map[string]mcpclient.LocalToolPolicy{
		"read_market": {ReadOnly: true, Budget: 3},
	}}
	mux, cookie := capabilityTestMuxWithPolicy(t, 91, 101, types.MembershipRoleAdmin,
		store, admission, trustedPolicy)
	body, _ := json.Marshal(validMCPRequest())
	recorder := serveCapabilityRequest(mux,
		httptest.NewRequest(http.MethodPost, "/api/capabilities/mcp", bytes.NewReader(body)), cookie)
	if recorder.Code != http.StatusCreated {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if admission.validated != "https://mcp.example.com/v1" || store.mcpInput == nil {
		t.Fatalf("MCP admission/store not called: validated=%q input=%+v", admission.validated, store.mcpInput)
	}
	if trustedPolicy.resolvedURL != "https://mcp.example.com/v1" ||
		len(trustedPolicy.toolNames) != 1 || trustedPolicy.toolNames[0] != "read_market" {
		t.Fatalf("trusted policy did not receive frozen endpoint/tool identity: %+v", trustedPolicy)
	}
	if store.mcpInput.Connection.CredentialRef != "vault:credential-123" {
		t.Fatalf("opaque credential ref not preserved internally: %+v", store.mcpInput.Connection)
	}
	if bytes.Contains(recorder.Body.Bytes(), []byte("credential-123")) ||
		bytes.Contains(store.mcpInput.Manifest, []byte("credential-123")) {
		t.Fatal("credential reference leaked into response or public manifest")
	}
	var catalog mcpclient.FrozenToolCatalog
	if err := json.Unmarshal(store.mcpInput.Connection.ToolSchema, &catalog); err != nil {
		t.Fatal(err)
	}
	if len(catalog.Tools) != 1 || !catalog.Tools[0].Policy.ReadOnly || catalog.Tools[0].Policy.Budget != 3 {
		t.Fatalf("frozen local policy mismatch: %+v", catalog.Tools)
	}
}

func TestMCPCreateRejectsCallerPolicyMissingTrustedPolicyAndSecretPath(t *testing.T) {
	store := &fakeCapabilityStore{}
	admission := &fakeMCPAdmission{}

	callerPolicy := validMCPRequest()
	callerPolicy["local_read_only_policy"] = map[string]any{
		"read_market": map[string]any{"read_only": true, "budget": 99},
	}
	body, _ := json.Marshal(callerPolicy)
	mux, cookie := capabilityTestMux(t, 91, 101, types.MembershipRoleAdmin, store, admission)
	recorder := serveCapabilityRequest(mux,
		httptest.NewRequest(http.MethodPost, "/api/capabilities/mcp", bytes.NewReader(body)), cookie)
	if recorder.Code != http.StatusBadRequest || store.mcpInput != nil {
		t.Fatalf("caller policy status=%d stored=%v", recorder.Code, store.mcpInput != nil)
	}

	store = &fakeCapabilityStore{}
	body, _ = json.Marshal(validMCPRequest())
	mux, cookie = capabilityTestMuxWithPolicy(t, 91, 101, types.MembershipRoleAdmin,
		store, admission, nil)
	recorder = serveCapabilityRequest(mux,
		httptest.NewRequest(http.MethodPost, "/api/capabilities/mcp", bytes.NewReader(body)), cookie)
	if recorder.Code != http.StatusBadRequest || store.mcpInput != nil {
		t.Fatalf("missing trusted policy status=%d stored=%v", recorder.Code, store.mcpInput != nil)
	}

	store = &fakeCapabilityStore{}
	secretPath := validMCPRequest()
	secretPath["endpoint_url"] = "https://mcp.example.com/sk-1234567890abcdef1234567890abcdef"
	body, _ = json.Marshal(secretPath)
	mux, cookie = capabilityTestMux(t, 91, 101, types.MembershipRoleAdmin, store, admission)
	recorder = serveCapabilityRequest(mux,
		httptest.NewRequest(http.MethodPost, "/api/capabilities/mcp", bytes.NewReader(body)), cookie)
	if recorder.Code != http.StatusBadRequest || store.mcpInput != nil {
		t.Fatalf("secret path status=%d stored=%v", recorder.Code, store.mcpInput != nil)
	}
}

func TestMCPCreateRejectsOAuthTenantOverrideAndUnsafeEndpoint(t *testing.T) {
	store := &fakeCapabilityStore{}
	admission := &fakeMCPAdmission{}
	mux, cookie := capabilityTestMux(t, 111, 121, types.MembershipRoleOwner, store, admission)

	oauth := validMCPRequest()
	oauth["authentication"] = "oauth2"
	body, _ := json.Marshal(oauth)
	recorder := serveCapabilityRequest(mux,
		httptest.NewRequest(http.MethodPost, "/api/capabilities/mcp", bytes.NewReader(body)), cookie)
	if recorder.Code != http.StatusBadRequest || store.mcpInput != nil {
		t.Fatalf("OAuth status=%d stored=%v", recorder.Code, store.mcpInput != nil)
	}
	inlineSecret := validMCPRequest()
	inlineSecret["credential_ref"] = "raw-api-secret"
	body, _ = json.Marshal(inlineSecret)
	recorder = serveCapabilityRequest(mux,
		httptest.NewRequest(http.MethodPost, "/api/capabilities/mcp", bytes.NewReader(body)), cookie)
	if recorder.Code != http.StatusBadRequest || store.mcpInput != nil {
		t.Fatalf("inline secret status=%d stored=%v", recorder.Code, store.mcpInput != nil)
	}

	override := validMCPRequest()
	override["tenant_id"] = 999
	body, _ = json.Marshal(override)
	recorder = serveCapabilityRequest(mux,
		httptest.NewRequest(http.MethodPost, "/api/capabilities/mcp", bytes.NewReader(body)), cookie)
	if recorder.Code != http.StatusBadRequest || store.mcpInput != nil {
		t.Fatalf("tenant override status=%d stored=%v", recorder.Code, store.mcpInput != nil)
	}

	unsafeStore := &fakeCapabilityStore{}
	unsafeAdmission := &fakeMCPAdmission{err: mcpclient.ErrUnsafeEndpoint}
	mux, cookie = capabilityTestMux(t, 111, 121, types.MembershipRoleOwner, unsafeStore, unsafeAdmission)
	body, _ = json.Marshal(validMCPRequest())
	recorder = serveCapabilityRequest(mux,
		httptest.NewRequest(http.MethodPost, "/api/capabilities/mcp", bytes.NewReader(body)), cookie)
	if recorder.Code != http.StatusBadRequest || unsafeStore.mcpInput != nil {
		t.Fatalf("unsafe endpoint status=%d stored=%v", recorder.Code, unsafeStore.mcpInput != nil)
	}
}

func TestCapabilityRequiresCompleteUserPrincipal(t *testing.T) {
	store := &fakeCapabilityStore{}
	// Replace the session actor with a service account: the UI capability center
	// must not silently broaden an automation principal into a human publisher.
	authStore := newFakeAuthStore()
	token, hash, _ := auth.NewSessionToken()
	authStore.sessions[string(hash)] = &types.Session{
		TokenHash: hash, TenantID: 131, UserID: 141, Role: types.MembershipRoleMember,
		ActorType: types.ActorTypeServiceAccount, ExpiresAt: time.Now().Add(time.Hour),
	}
	authStore.members[141] = []types.Membership{{TenantID: 131, UserID: 141, Role: types.MembershipRoleMember}}
	mux := http.NewServeMux()
	Mount(mux, Deps{Auth: authStore, Principal: auth.NewContextResolver(), Capabilities: store})
	recorder := serveCapabilityRequest(mux,
		httptest.NewRequest(http.MethodGet, "/api/capabilities", nil),
		&http.Cookie{Name: sessionCookieName, Value: token})
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("service account status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}
