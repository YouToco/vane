package agentpolicy

import (
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// This is an intentional dark launch. The first runtime slice may define and
// persist exact Skill identities, but it may not alter any production Agent,
// HTTP, A2A, scheduler, workflow, or model call path.
func TestSkillRuntimeDarkSliceHasNoProductionCallPoint(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve guard location")
	}
	serverRoot := filepath.Dir(filepath.Dir(thisFile))
	allowed := map[string]bool{
		filepath.Join(serverRoot, "agentpolicy", "manifest_v2.go"):  true,
		filepath.Join(serverRoot, "skillruntime", "codec.go"):       true,
		filepath.Join(serverRoot, "skillruntime", "contract.go"):    true,
		filepath.Join(serverRoot, "store", "user_skill_runtime.go"): true,
	}
	identifiers := []string{
		"BuildManifestV2(", "EncodeManifestV2(", "DecodeManifestV2(", "LowerTrustSkillRefV2(",
		"AddSkillCapabilityVersion(", "ActivateSkillCapability(", "PauseSkillCapability(",
		"GetSkillCapabilityDetail(", "DiffSkillCapabilityVersions(", "ResolveSkillRef(",
		"OpenSkillResource(", "ReadSkillResourceChunk(",
		"EncodeSkillRefV1(", "DecodeSkillRefV1(",
		"EncodeSkillResourceHandleV1(", "DecodeSkillResourceHandleV1(",
		"EncodeSkillResourceChunkV1(", "DecodeSkillResourceChunkV1(",
	}
	err := filepath.WalkDir(serverRoot, func(filePath string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(filePath, ".go") ||
			strings.HasSuffix(filePath, "_test.go") || allowed[filePath] {
			return nil
		}
		payload, readErr := os.ReadFile(filePath)
		if readErr != nil {
			return readErr
		}
		for _, identifier := range identifiers {
			if strings.Contains(string(payload), identifier) {
				t.Errorf("dark Skill runtime gained production call point %s in %s", identifier, filePath)
			}
		}
		if strings.Contains(string(payload), `github.com/YouToco/vane/server/skillruntime`) {
			t.Errorf("dark Skill contract imported by production file %s", filePath)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSkillRuntimeGateOffPreservesCurrentV1AgentBytes(t *testing.T) {
	catalog := ToolCatalogV1{SchemaVersion: ToolCatalogSchemaVersionV1}
	owner, err := CompileV1(CurrentOwnerV1("deepseek", "deepseek-v4-flash"), catalog)
	if err != nil {
		t.Fatal(err)
	}
	a2a, err := CompileV1(CurrentA2AV1("deepseek", "deepseek-v4-flash"), catalog)
	if err != nil {
		t.Fatal(err)
	}
	ownerPayload, ownerManifestDigest, err := EncodeManifestV1(owner.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	a2aPayload, a2aManifestDigest, err := EncodeManifestV1(a2a.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	got := []string{
		digest([]byte(owner.SystemPrompt)), digest(ownerPayload), ownerManifestDigest,
		digest([]byte(a2a.SystemPrompt)), digest(a2aPayload), a2aManifestDigest,
		owner.Manifest.ToolCatalogDigest, a2a.Manifest.ToolCatalogDigest,
	}
	want := []string{
		"3c3932de9dfd8d40c235f5406a2fd29da46a87d7939a4d25030188350317d140",
		"d1d39078ca002fb2aa7a607a531067e0571739b36a971ead4bed04a8cb479537",
		"d1d39078ca002fb2aa7a607a531067e0571739b36a971ead4bed04a8cb479537",
		"1bdedbbcb1d79d2977d319b93b45ed7e3a6210a7e4f3604de6e72c0a112774d5",
		"8b6b01651027cb6e3f29ceeb03d8323d71352ab4cd0566d1910f318026b96c30",
		"8b6b01651027cb6e3f29ceeb03d8323d71352ab4cd0566d1910f318026b96c30",
		"6012028f10b9fd068f880a45355838352ee33738f308765c2cc12a892af0282e",
		"6012028f10b9fd068f880a45355838352ee33738f308765c2cc12a892af0282e",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("record current V1 byte identities:\n%q", got)
	}
}
