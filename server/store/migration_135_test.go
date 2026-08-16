package store

import (
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/YouToco/vane/server/internal/testgate"
	"github.com/YouToco/vane/server/mcpclient"
	"github.com/YouToco/vane/server/types"
)

func TestMigration135CapabilityIsolationContract(t *testing.T) {
	payload, err := migrationsFS.ReadFile("migrations/135_user_capability_foundation.sql")
	if err != nil {
		t.Fatal(err)
	}
	sqlText := string(payload)
	for _, fragment := range []string{
		"CREATE TABLE user_capabilities", "CREATE TABLE user_capability_versions",
		"CREATE TABLE skill_capability_versions", "CREATE TABLE skill_capability_files",
		"CREATE TABLE mcp_connection_versions", "CREATE TABLE user_capability_events",
		"payload_digest=encode(sha256(manifest_payload),'hex')",
		"file_kind IN ('skill_md','reference','asset')",
		"protocol_version IN ('2025-06-18','2025-11-25')",
		"ALTER TABLE user_capabilities FORCE ROW LEVEL SECURITY",
		"ALTER TABLE user_capability_versions FORCE ROW LEVEL SECURITY",
		"ALTER TABLE skill_capability_versions FORCE ROW LEVEL SECURITY",
		"ALTER TABLE skill_capability_files FORCE ROW LEVEL SECURITY",
		"ALTER TABLE mcp_connection_versions FORCE ROW LEVEL SECURITY",
		"ALTER TABLE user_capability_events FORCE ROW LEVEL SECURITY",
		"refusing downgrade while retained user capabilities exist",
	} {
		if !strings.Contains(sqlText, fragment) {
			t.Errorf("migration 135 missing %q", fragment)
		}
	}
	for _, forbidden := range []string{
		"file_kind IN ('skill_md','reference','asset','script')",
		"GRANT SELECT,INSERT,UPDATE ON user_capability_versions",
		"GRANT SELECT,INSERT,UPDATE ON skill_capability_versions",
		"GRANT SELECT,INSERT,UPDATE ON mcp_connection_versions",
		"GRANT DELETE", "stdio", "'sse'",
	} {
		if strings.Contains(sqlText, forbidden) {
			t.Errorf("migration 135 contains forbidden authority %q", forbidden)
		}
	}
}

func TestMigration135RLSAndImmutableVersionsPostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		testgate.Database(t)
	}
	database, provider, scratchURL, drop := migration128Scratch(t, databaseURL)
	t.Cleanup(drop)
	if _, err := provider.UpTo(t.Context(), 135); err != nil {
		t.Fatal(err)
	}

	userA := migration135User(t, database, "a")
	userB := migration135User(t, database, "b")
	userC := migration135User(t, database, "c")
	tenantA := migration135Team(t, database, "A")
	tenantB := migration135Team(t, database, "B")
	for _, membership := range []struct {
		tenant, user int64
		role         string
	}{
		{tenantA, userA, "owner"}, {tenantA, userB, "member"}, {tenantB, userC, "owner"},
	} {
		if _, err := database.ExecContext(t.Context(), `
			INSERT INTO memberships(tenant_id,user_id,role) VALUES($1,$2,$3)`,
			membership.tenant, membership.user, membership.role); err != nil {
			t.Fatal(err)
		}
	}
	personalID, _ := migration135Capability(t, database, tenantA, userA, "personal", "private")
	workspaceID, workspaceVersionID := migration135Capability(t, database, tenantA, userA, "workspace", "shared")
	_ = personalID

	assertVisible := func(tenantID, userID int64, role string, want []string) {
		t.Helper()
		tx, err := database.BeginTx(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		if _, err := tx.ExecContext(t.Context(), `
			SELECT set_config('app.tenant_id',$1,true),set_config('app.user_id',$2,true),
			       set_config('app.membership_role',$3,true)`,
			fmt.Sprint(tenantID), fmt.Sprint(userID), role); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(t.Context(), `SET LOCAL ROLE vane_app`); err != nil {
			t.Fatal(err)
		}
		rows, err := tx.QueryContext(t.Context(), `SELECT slug FROM user_capabilities ORDER BY slug`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		got := make([]string, 0)
		for rows.Next() {
			var slug string
			if err := rows.Scan(&slug); err != nil {
				t.Fatal(err)
			}
			got = append(got, slug)
		}
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("visible=%v want=%v", got, want)
		}
	}
	assertVisible(tenantA, userA, "owner", []string{"private", "shared"})
	assertVisible(tenantA, userB, "member", []string{"shared"})
	assertVisible(tenantB, userC, "owner", []string{})

	st, err := New(t.Context(), scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	skillManifest := json.RawMessage(`{"schema_version":"vane.skill-package/v1","name":"member-skill"}`)
	skillMD := []byte("# Member Skill\n\nDeclarative instructions only.")
	skillFileManifest := json.RawMessage(`{"files":[{"path":"SKILL.md","kind":"skill_md"}]}`)
	skillInput := types.CreateSkillCapability{
		TenantID: tenantA, ActorUserID: userB, Visibility: types.UserCapabilityPersonal,
		Slug: "member-skill", DisplayName: "Member Skill", Source: types.UserCapabilityUpload,
		PayloadDigest: digestBytes(skillManifest), Manifest: skillManifest, Compatible: true,
		Skill: types.SkillCapabilityVersion{
			Name: "member-skill", Description: "Safe declarative team research instructions",
			SkillMDDigest: digestBytes(skillMD), ArchiveDigest: strings.Repeat("a", 64),
			FileManifest: skillFileManifest,
			Files: []types.SkillCapabilityFile{{
				Path: "SKILL.md", Kind: "skill_md", Digest: digestBytes(skillMD), Content: skillMD,
			}},
		},
	}
	capability, version, err := st.CreateSkillCapability(t.Context(), skillInput)
	if err != nil {
		t.Fatalf("create personal Skill capability: %v", err)
	}
	if capability.CurrentVersionID == nil || *capability.CurrentVersionID != version.ID ||
		capability.OwnerUserID != userB || version.PayloadDigest != skillInput.PayloadDigest {
		t.Fatalf("created Skill capability=%+v version=%+v", capability, version)
	}
	sharedByMember := skillInput
	sharedByMember.Visibility = types.UserCapabilityWorkspace
	sharedByMember.Slug = "member-shared-skill"
	sharedByMember.DisplayName = "Member Shared Skill"
	if _, _, err := st.CreateSkillCapability(
		t.Context(), sharedByMember); types.CodeOf(err) != types.CodeForbidden {
		t.Fatalf("member shared Skill code=%s err=%v", types.CodeOf(err), err)
	}

	mcpManifest := json.RawMessage(`{"schema_version":"vane.mcp-capability/v1","name":"workspace-mcp"}`)
	toolSchema := json.RawMessage(`{"tools":[]}`)
	mcpCapability, mcpVersion, err := st.CreateMCPCapability(t.Context(), types.CreateMCPCapability{
		TenantID: tenantA, ActorUserID: userA, Visibility: types.UserCapabilityWorkspace,
		Slug: "workspace-mcp", DisplayName: "Workspace MCP",
		PayloadDigest: digestBytes(mcpManifest), Manifest: mcpManifest,
		Connection: types.MCPConnectionVersion{
			EndpointURL:      "https://mcp.example.com/v1",
			ProtocolVersion:  mcpclient.ProtocolVersion20251125,
			Authentication:   types.MCPAuthenticationNone,
			ToolSchemaDigest: digestBytes(toolSchema), ToolSchema: toolSchema,
		},
	})
	if err != nil {
		t.Fatalf("create workspace MCP capability: %v", err)
	}
	if mcpCapability.CurrentVersionID == nil ||
		*mcpCapability.CurrentVersionID != mcpVersion.ID ||
		mcpCapability.Kind != types.UserCapabilityMCP {
		t.Fatalf("created MCP capability=%+v version=%+v", mcpCapability, mcpVersion)
	}

	memberVisible, err := st.ListVisibleUserCapabilities(t.Context(), tenantA, userB)
	if err != nil {
		t.Fatalf("list member capabilities: %v", err)
	}
	visibleSlugs := make(map[string]bool, len(memberVisible))
	for _, item := range memberVisible {
		visibleSlugs[item.Slug] = true
	}
	for _, slug := range []string{"shared", "member-skill", "workspace-mcp"} {
		if !visibleSlugs[slug] {
			t.Fatalf("member capabilities missing %q: %+v", slug, memberVisible)
		}
	}
	if visibleSlugs["private"] {
		t.Fatalf("member discovered owner personal capability: %+v", memberVisible)
	}
	if _, err := st.ListVisibleUserCapabilities(
		t.Context(), tenantA, userC); types.CodeOf(err) != types.CodeForbidden {
		t.Fatalf("non-member capability list code=%s err=%v", types.CodeOf(err), err)
	}
	if _, err := st.ListVisibleUserCapabilities(
		t.Context(), 0, userB); types.CodeOf(err) != types.CodeValidation {
		t.Fatalf("invalid capability principal code=%s err=%v", types.CodeOf(err), err)
	}

	var canUpdateVersion, canDeleteVersion bool
	if err := database.QueryRowContext(t.Context(), `
		SELECT has_table_privilege('vane_app','user_capability_versions','UPDATE'),
		       has_table_privilege('vane_app','user_capability_versions','DELETE')`).Scan(
		&canUpdateVersion, &canDeleteVersion); err != nil {
		t.Fatal(err)
	}
	if canUpdateVersion || canDeleteVersion {
		t.Fatalf("immutable version update=%t delete=%t", canUpdateVersion, canDeleteVersion)
	}
	invalidFileDigest := sha256.Sum256([]byte("x"))
	if _, err := database.ExecContext(t.Context(), `
		INSERT INTO skill_capability_files(
		 capability_version_id,capability_id,tenant_id,owner_user_id,visibility,
		 file_path,file_kind,content_digest,content_payload)
		VALUES($1,$2,$3,$4,'workspace','scripts/run.sh','script',$5,'x')`,
		workspaceVersionID, workspaceID, tenantA, userA,
		fmt.Sprintf("%x", invalidFileDigest[:])); err == nil {
		t.Fatal("database accepted executable Skill file kind")
	}
	if _, err := provider.DownTo(t.Context(), 134); err == nil ||
		!strings.Contains(err.Error(), "retained user capabilities") {
		t.Fatalf("migration 135 destroyed retained capabilities: %v", err)
	}
}

func migration135User(t *testing.T, database *sql.DB, suffix string) int64 {
	t.Helper()
	var id int64
	if err := database.QueryRowContext(t.Context(), `
		INSERT INTO users(feishu_open_id,name) VALUES($1,$1) RETURNING id`,
		"migration-135-"+suffix+"-"+uuid.NewString()).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

func migration135Team(t *testing.T, database *sql.DB, name string) int64 {
	t.Helper()
	var id int64
	if err := database.QueryRowContext(t.Context(), `
		INSERT INTO tenants(status,plan,display_name,workspace_kind,seat_limit)
		VALUES('active','free',$1,'team',5) RETURNING id`, name).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

func migration135Capability(t *testing.T, database *sql.DB, tenantID, userID int64, visibility, slug string) (uuid.UUID, uuid.UUID) {
	t.Helper()
	capabilityID, versionID := uuid.New(), uuid.New()
	manifest := []byte(`{"schema_version":"vane.skill-package/v1"}`)
	digest := sha256.Sum256(manifest)
	if _, err := database.ExecContext(t.Context(), `
		INSERT INTO user_capabilities(
		 id,tenant_id,owner_user_id,kind,visibility,slug,display_name,status)
		VALUES($1,$2,$3,'skill',$4,$5,$5,'draft')`,
		capabilityID, tenantID, userID, visibility, slug); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), `
		INSERT INTO user_capability_versions(
		 id,capability_id,tenant_id,owner_user_id,version,visibility,source_kind,
		 payload_digest,manifest_payload,compatible,created_by)
		VALUES($1,$2,$3,$4,1,$5,'upload',$6,$7,true,$4)`,
		versionID, capabilityID, tenantID, userID, visibility,
		fmt.Sprintf("%x", digest[:]), manifest); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), `
		INSERT INTO skill_capability_versions(
		 capability_version_id,capability_id,tenant_id,owner_user_id,visibility,
		 name,skill_md_digest,archive_digest,file_manifest_payload,file_manifest_digest)
		VALUES($1,$2,$3,$4,$5,$6,repeat('1',64),repeat('2',64),$7,$8)`,
		versionID, capabilityID, tenantID, userID, visibility, slug, manifest,
		fmt.Sprintf("%x", digest[:])); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), `
		UPDATE user_capabilities SET current_version_id=$2 WHERE id=$1`,
		capabilityID, versionID); err != nil {
		t.Fatal(err)
	}
	return capabilityID, versionID
}
