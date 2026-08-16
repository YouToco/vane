package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/server/types"
)

func TestValidateCapabilityCreateRequiresExactContentDigest(t *testing.T) {
	manifest := json.RawMessage(`{"schema_version":"vane.skill-package/v1"}`)
	digest := sha256.Sum256(manifest)
	if err := validateCapabilityCreate(1, 2, types.UserCapabilityPersonal,
		"market-watch", "Market Watch", hex.EncodeToString(digest[:]), manifest); err != nil {
		t.Fatal(err)
	}
	if err := validateCapabilityCreate(1, 2, types.UserCapabilityPersonal,
		"market-watch", "Market Watch", string(make([]byte, 64)), manifest); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("digest mismatch error=%v", err)
	}
	if err := validateCapabilityCreate(1, 2, types.UserCapabilityVisibility("public"),
		"market-watch", "Market Watch", hex.EncodeToString(digest[:]), manifest); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("visibility error=%v", err)
	}
}

func TestValidateSkillFilesRejectsAnythingOutsideDeclarativePackage(t *testing.T) {
	skillMD := []byte("safe")
	digest := digestBytes(skillMD)
	valid := []types.SkillCapabilityFile{{Path: "SKILL.md", Kind: "skill_md", Digest: digest, Content: skillMD}}
	if err := validateSkillFiles(valid, digest); err != nil {
		t.Fatal(err)
	}
	for _, file := range []types.SkillCapabilityFile{
		{Path: "scripts/run.sh", Kind: "asset", Digest: digest, Content: skillMD},
		{Path: "assets/.env", Kind: "asset", Digest: digest, Content: skillMD},
		{Path: "../SKILL.md", Kind: "skill_md", Digest: digest, Content: skillMD},
		{Path: "SKILL.md", Kind: "script", Digest: digest, Content: skillMD},
	} {
		files := append([]types.SkillCapabilityFile(nil), valid...)
		files = append(files, file)
		if err := validateSkillFiles(files, digest); !errors.Is(err, types.ErrValidation) {
			t.Fatalf("file=%+v error=%v", file, err)
		}
	}
}

func TestValidMCPAuthenticationNeverAcceptsInlineSecretsForNoAuth(t *testing.T) {
	if !validMCPAuthentication(types.MCPAuthenticationNone, "") {
		t.Fatal("none auth rejected")
	}
	if validMCPAuthentication(types.MCPAuthenticationNone, "secret") {
		t.Fatal("none auth accepted a credential")
	}
	if !validMCPAuthentication(types.MCPAuthenticationOAuth2, "vault:credential-id") {
		t.Fatal("opaque OAuth credential reference rejected")
	}
	if validMCPAuthentication(types.MCPAuthenticationBearer, "") {
		t.Fatal("bearer accepted no credential reference")
	}
}

func TestCapabilityStoreCredentialSafetyIsLastLineOfDefense(t *testing.T) {
	manifest := json.RawMessage(`{"schema_version":"vane.skill-package/v1"}`)
	cleanSkill := types.CreateSkillCapability{
		Slug: "market-watch", DisplayName: "Market Watch", Manifest: manifest,
		Skill: types.SkillCapabilityVersion{
			Name: "market-watch", Description: "Safe watcher", FileManifest: manifest,
			Files: []types.SkillCapabilityFile{{Path: "SKILL.md", Content: []byte("safe")}},
		},
	}
	for name, mutate := range map[string]func(*types.CreateSkillCapability){
		"github_pat_content": func(input *types.CreateSkillCapability) {
			input.Skill.Files[0].Content = []byte("ghp_123456789012345678901234567890")
		},
		"jwt_manifest": func(input *types.CreateSkillCapability) {
			input.Manifest = json.RawMessage(`{"description":"eyJabcdefghijk.abcdefghijk.abcdefghijk"}`)
		},
		"credential_dsn": func(input *types.CreateSkillCapability) {
			input.Skill.Files[0].Content = []byte("postgres://user:password@db.example/vane")
		},
		"general_token": func(input *types.CreateSkillCapability) {
			input.Skill.Description = "token: abcdefghijklmnop"
		},
	} {
		t.Run(name, func(t *testing.T) {
			input := cleanSkill
			input.Skill.Files = append([]types.SkillCapabilityFile(nil), cleanSkill.Skill.Files...)
			mutate(&input)
			if err := validateSkillCapabilityCredentialSafety(input); !errors.Is(err, types.ErrValidation) {
				t.Fatalf("error=%v", err)
			}
		})
	}

	cleanMCP := types.CreateMCPCapability{
		Slug: "market-mcp", DisplayName: "Market MCP", Manifest: json.RawMessage(`{"schema_version":"vane.mcp-capability/v1"}`),
		Connection: types.MCPConnectionVersion{
			EndpointURL: "https://mcp.example.com/v1", ToolSchema: json.RawMessage(`{"tools":[]}`),
		},
	}
	cleanMCP.Connection.EndpointURL = "https://mcp.example.com/sk-1234567890abcdef1234567890abcdef"
	if err := validateMCPCapabilityCredentialSafety(cleanMCP); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("secret endpoint error=%v", err)
	}
}

func TestCreateSkillCapabilityCallsCredentialGateBeforeDatabase(t *testing.T) {
	manifest := json.RawMessage(`{"schema_version":"vane.skill-package/v1","description":"ghp_123456789012345678901234567890"}`)
	payloadDigest := digestBytes(manifest)
	skillMD := []byte("safe declarative skill")
	skillDigest := digestBytes(skillMD)
	fileManifest := json.RawMessage(`{"files":["SKILL.md"]}`)

	databaseTouched := false
	st := &Store{beginTx: func(context.Context, pgx.TxOptions) (pgx.Tx, error) {
		databaseTouched = true
		return nil, errors.New("database sentinel")
	}}
	_, _, err := st.CreateSkillCapability(t.Context(), types.CreateSkillCapability{
		TenantID: 1, ActorUserID: 2, Visibility: types.UserCapabilityPersonal,
		Slug: "market-watch", DisplayName: "Market Watch", Source: types.UserCapabilityUpload,
		PayloadDigest: payloadDigest, Manifest: manifest, Compatible: true,
		Skill: types.SkillCapabilityVersion{
			Name: "market-watch", Description: "Safe watcher",
			SkillMDDigest: skillDigest, ArchiveDigest: strings.Repeat("a", 64),
			FileManifest: fileManifest,
			Files: []types.SkillCapabilityFile{{
				Path: "SKILL.md", Kind: "skill_md", Digest: skillDigest, Content: skillMD,
			}},
		},
	})
	if !errors.Is(err, types.ErrValidation) {
		t.Fatalf("credential-bearing public create error=%v", err)
	}
	if databaseTouched {
		t.Fatal("credential-bearing capability reached the database boundary")
	}
}
