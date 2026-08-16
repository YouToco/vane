package agentpolicy

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
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
	definingManifestFile := filepath.Join(serverRoot, "agentpolicy", "manifest_v2.go")
	identifiers := map[string]bool{
		"BuildManifestV2": true, "EncodeManifestV2": true, "DecodeManifestV2": true,
		"LowerTrustSkillRefV2": true, "AddSkillCapabilityVersion": true,
		"ActivateSkillCapability": true, "PauseSkillCapability": true,
		"GetSkillCapabilityDetail": true, "DiffSkillCapabilityVersions": true,
		"ResolveSkillRef": true, "OpenSkillResource": true, "ReadSkillResourceChunk": true,
	}
	err := filepath.WalkDir(serverRoot, func(filePath string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(filePath, ".go") ||
			strings.HasSuffix(filePath, "_test.go") {
			return nil
		}
		payload, readErr := os.ReadFile(filePath)
		if readErr != nil {
			return readErr
		}
		findings, parseErr := darkSkillProductionReferences(filePath, payload, identifiers,
			filePath == definingManifestFile)
		if parseErr != nil {
			return parseErr
		}
		for _, finding := range findings {
			t.Errorf("dark Skill runtime gained production reference %s in %s", finding, filePath)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSkillRuntimeDarkGuardCatchesMethodValuesAndForwarders(t *testing.T) {
	blocked := map[string]bool{"ResolveSkillRef": true, "ReadSkillResourceChunk": true}
	for name, mutation := range map[string]struct {
		filePath string
		source   string
	}{
		"method value": {
			filePath: "mutation.go",
			source:   `package x; func f(s interface{ ResolveSkillRef() }) { x := s.ResolveSkillRef; x() }`,
		},
		"interface forwarder": {
			filePath: "mutation.go",
			source:   `package x; type S struct{}; func (s S) ReadSkillResourceChunk(){}; func f(s S){ s.ReadSkillResourceChunk() }`,
		},
		"wrapper in defining store file": {
			filePath: filepath.Join("store", "user_skill_runtime.go"),
			source:   `package store; type Store struct{}; func (s *Store) ReadSkillResourceChunk(){}; func (s *Store) RuntimeRead(){ s.ReadSkillResourceChunk() }`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			findings, err := darkSkillProductionReferences(mutation.filePath,
				[]byte(mutation.source), blocked, false)
			if err != nil || len(findings) == 0 {
				t.Fatalf("findings=%v err=%v", findings, err)
			}
		})
	}
}

func darkSkillProductionReferences(filePath string, payload []byte, blocked map[string]bool,
	allowManifestInternals bool,
) ([]string, error) {
	parsed, err := parser.ParseFile(token.NewFileSet(), filePath, payload, 0)
	if err != nil {
		return nil, err
	}
	findings := make([]string, 0)
	ast.Inspect(parsed, func(node ast.Node) bool {
		switch value := node.(type) {
		case *ast.SelectorExpr:
			if blocked[value.Sel.Name] {
				findings = append(findings, value.Sel.Name)
			}
		case *ast.CallExpr:
			identifier, ok := value.Fun.(*ast.Ident)
			if ok && blocked[identifier.Name] && !allowManifestInternals {
				findings = append(findings, identifier.Name)
			}
		case *ast.ImportSpec:
			importPath, unquoteErr := strconv.Unquote(value.Path.Value)
			if unquoteErr == nil && importPath == "github.com/YouToco/vane/server/skillruntime" &&
				!strings.HasSuffix(filePath, filepath.Join("store", "user_skill_runtime.go")) &&
				!allowManifestInternals {
				findings = append(findings, "skillruntime import")
			}
		}
		return true
	})
	return findings, nil
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
