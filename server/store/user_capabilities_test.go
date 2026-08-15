package store

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"testing"

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
