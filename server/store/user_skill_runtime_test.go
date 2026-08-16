package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/server/internal/testgate"
	"github.com/YouToco/vane/server/skillruntime"
	"github.com/YouToco/vane/server/types"
)

func TestAddSkillVersionRejectsManifestResourceDriftBeforeDatabase(t *testing.T) {
	input := skillAddInputForRuntime(1, 2,
		uuid.MustParse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
		"safe instructions", "references/guide.md", []byte("guide"))
	manifest := storedSkillFileManifestV1{}
	if err := json.Unmarshal(input.Skill.FileManifest, &manifest); err != nil {
		t.Fatal(err)
	}
	manifest.Files[1].Digest = strings.Repeat("f", 64)
	input.Skill.FileManifest, _ = json.Marshal(manifest)
	input.Manifest = json.RawMessage(bytes.Clone(input.Skill.FileManifest))
	input.PayloadDigest = digestBytes(input.Manifest)
	databaseTouched := false
	st := &Store{beginTx: func(context.Context, pgx.TxOptions) (pgx.Tx, error) {
		databaseTouched = true
		return nil, errors.New("database sentinel")
	}}
	if _, err := st.AddSkillCapabilityVersion(t.Context(), input); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("manifest drift err=%v", err)
	}
	if databaseTouched {
		t.Fatal("manifest/resource drift reached database boundary")
	}
}

func TestSkillRuntimeLifecycleIsolationAndExactResourcePostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		testgate.Database(t)
	}
	database, provider, scratchURL, drop := migration128Scratch(t, databaseURL)
	t.Cleanup(drop)
	if _, err := provider.UpTo(t.Context(), 135); err != nil {
		t.Fatal(err)
	}

	owner := migration135User(t, database, "skill-owner")
	admin := migration135User(t, database, "skill-admin")
	member := migration135User(t, database, "skill-member")
	other := migration135User(t, database, "skill-other")
	teamA := migration135Team(t, database, "Skill A")
	teamB := migration135Team(t, database, "Skill B")
	for _, membership := range []struct {
		tenant, user int64
		role         string
	}{
		{teamA, owner, "owner"}, {teamA, admin, "admin"}, {teamA, member, "member"},
		{teamB, owner, "owner"}, {teamB, other, "owner"},
	} {
		if _, err := database.ExecContext(t.Context(), `
			INSERT INTO memberships(tenant_id,user_id,role) VALUES($1,$2,$3)`,
			membership.tenant, membership.user, membership.role); err != nil {
			t.Fatal(err)
		}
	}
	st, err := New(t.Context(), scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)

	created, v1, err := st.CreateSkillCapability(t.Context(), skillCreateInputForRuntime(
		teamA, owner, types.UserCapabilityPersonal, "personal-watch", "v1 instructions"))
	if err != nil {
		t.Fatal(err)
	}
	v2Input := skillAddInputForRuntime(teamA, owner, created.ID, "v2 instructions", "references/guide.md", []byte("guide-v2"))
	v2, err := st.AddSkillCapabilityVersion(t.Context(), v2Input)
	if err != nil {
		t.Fatal(err)
	}
	if v2.Version != 2 || v2.CreatedBy != owner {
		t.Fatalf("v2=%+v", v2)
	}
	replayedV2, err := st.AddSkillCapabilityVersion(t.Context(), v2Input)
	if err != nil || replayedV2.ID != v2.ID || replayedV2.Version != 2 {
		t.Fatalf("idempotent v2=%+v err=%v", replayedV2, err)
	}
	var versionTwoCount, versionAddedEvents int
	if err := database.QueryRowContext(t.Context(), `
		SELECT count(*) FROM user_capability_versions WHERE capability_id=$1 AND version=2`,
		created.ID).Scan(&versionTwoCount); err != nil {
		t.Fatal(err)
	}
	if err := database.QueryRowContext(t.Context(), `
		SELECT count(*) FROM user_capability_events WHERE capability_id=$1 AND event_kind='version_added'`,
		created.ID).Scan(&versionAddedEvents); err != nil || versionTwoCount != 1 || versionAddedEvents != 1 {
		t.Fatalf("versions=%d add events=%d err=%v", versionTwoCount, versionAddedEvents, err)
	}
	v1Detail, err := st.GetSkillCapabilityDetail(t.Context(), teamA, owner, created.ID, v1.ID)
	if err != nil {
		t.Fatal(err)
	}
	if v1Detail.Capability.CurrentVersionID == nil || *v1Detail.Capability.CurrentVersionID != v1.ID {
		t.Fatalf("add-version moved head: %+v", v1Detail.Capability)
	}
	if len(v1Detail.Skill.Files) != 0 {
		t.Fatalf("detail leaked Skill bytes: %+v", v1Detail.Skill.Files)
	}
	if _, err := st.GetSkillCapabilityDetail(t.Context(), teamA, member, created.ID, v1.ID); types.CodeOf(err) != types.CodeNotFound {
		t.Fatalf("member read personal Skill code=%s err=%v", types.CodeOf(err), err)
	}
	if _, err := st.GetSkillCapabilityDetail(t.Context(), teamB, owner, created.ID, v1.ID); types.CodeOf(err) != types.CodeNotFound {
		t.Fatalf("cross-tenant Skill code=%s err=%v", types.CodeOf(err), err)
	}

	diff, err := st.DiffSkillCapabilityVersions(t.Context(), teamA, owner, created.ID, v1.ID, v2.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !diff.ManifestChanged || !diff.SkillMDChanged || len(diff.Changes) != 2 ||
		diff.Changes[0].Path != "SKILL.md" || diff.Changes[1].Path != "references/guide.md" {
		t.Fatalf("diff=%+v", diff)
	}
	active, err := st.ActivateSkillCapability(t.Context(), teamA, owner, created.ID, v2.ID)
	if err != nil {
		t.Fatal(err)
	}
	if active.Capability.Status != types.UserCapabilityActive ||
		active.Capability.CurrentVersionID == nil || *active.Capability.CurrentVersionID != v2.ID ||
		active.Ref.Validate() != nil {
		t.Fatalf("active=%+v", active)
	}
	if _, err := st.ActivateSkillCapability(t.Context(), teamA, owner, created.ID, v2.ID); err != nil {
		t.Fatalf("idempotent activation: %v", err)
	}
	var activationEvents int
	if err := database.QueryRowContext(t.Context(), `
		SELECT count(*) FROM user_capability_events WHERE capability_id=$1 AND event_kind='activated'`,
		created.ID).Scan(&activationEvents); err != nil || activationEvents != 1 {
		t.Fatalf("activation events=%d err=%v", activationEvents, err)
	}

	// Exact historical v1 remains readable after v2 becomes current. A query
	// that drops capability_version_id would return v2 bytes and fail here.
	v1Ref, err := st.ResolveSkillRef(t.Context(), teamA, owner, created.ID, v1.ID)
	if err != nil {
		t.Fatal(err)
	}
	v1Handle, err := st.OpenSkillResource(t.Context(), owner, v1Ref, "SKILL.md")
	if err != nil {
		t.Fatal(err)
	}
	v1Chunk, err := st.ReadSkillResourceChunk(t.Context(), owner, v1Handle, 0, 4)
	if err != nil {
		t.Fatal(err)
	}
	var reconstructed []byte
	reconstructed = append(reconstructed, v1Chunk.Data...)
	for !v1Chunk.EOF {
		v1Chunk, err = st.ReadSkillResourceChunk(t.Context(), owner, v1Handle,
			int64(len(reconstructed)), 4)
		if err != nil {
			t.Fatal(err)
		}
		reconstructed = append(reconstructed, v1Chunk.Data...)
	}
	if !bytes.Equal(reconstructed, []byte("v1 instructions")) || digestBytes(reconstructed) != v1Handle.ContentDigest {
		t.Fatalf("historical resource=%q handle=%+v", reconstructed, v1Handle)
	}
	forged := v1Handle
	forged.ContentDigest = strings.Repeat("f", 64)
	if _, err := st.ReadSkillResourceChunk(t.Context(), owner, forged, 0, 4); types.CodeOf(err) != types.CodeNotFound {
		t.Fatalf("forged handle code=%s err=%v", types.CodeOf(err), err)
	}
	if _, err := st.ReadSkillResourceChunk(t.Context(), member, v1Handle, 0, 4); types.CodeOf(err) != types.CodeNotFound {
		t.Fatalf("member read personal bytes code=%s err=%v", types.CodeOf(err), err)
	}

	paused, err := st.PauseSkillCapability(t.Context(), teamA, owner, created.ID)
	if err != nil || paused.Status != types.UserCapabilityPaused {
		t.Fatalf("paused=%+v err=%v", paused, err)
	}
	if _, err := st.PauseSkillCapability(t.Context(), teamA, owner, created.ID); err != nil {
		t.Fatalf("idempotent pause: %v", err)
	}
	var pauseEvents int
	if err := database.QueryRowContext(t.Context(), `
		SELECT count(*) FROM user_capability_events WHERE capability_id=$1 AND event_kind='paused'`,
		created.ID).Scan(&pauseEvents); err != nil || pauseEvents != 1 {
		t.Fatalf("pause events=%d err=%v", pauseEvents, err)
	}

	scriptInput := skillAddInputForRuntime(teamA, owner, created.ID, "script metadata", "", nil)
	scriptInput.Compatible = false
	scriptInput.Skill.ContainsScripts = true
	scriptInput.Skill.FileManifest = runtimeSkillManifest("runtime-version", "", false, true,
		[]storedSkillManifestFileV1{
			{Path: "SKILL.md", Kind: "skill_md", Size: int64(len("script metadata")), Digest: digestBytes([]byte("script metadata"))},
			{Path: "scripts/run.sh", Kind: "script", Size: int64(len("not-stored")), Digest: digestBytes([]byte("not-stored"))},
		})
	scriptInput.Manifest = json.RawMessage(bytes.Clone(scriptInput.Skill.FileManifest))
	scriptInput.PayloadDigest = digestBytes(scriptInput.Manifest)
	scriptVersion, err := st.AddSkillCapabilityVersion(t.Context(), scriptInput)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.ActivateSkillCapability(t.Context(), teamA, owner, created.ID, scriptVersion.ID); types.CodeOf(err) != types.CodeValidation {
		t.Fatalf("script activation code=%s err=%v", types.CodeOf(err), err)
	}

	shared, sharedV1, err := st.CreateSkillCapability(t.Context(), skillCreateInputForRuntime(
		teamA, owner, types.UserCapabilityWorkspace, "shared-watch", "shared-v1"))
	if err != nil {
		t.Fatal(err)
	}
	memberInput := skillAddInputForRuntime(teamA, member, shared.ID, "member-v2", "", nil)
	if _, err := st.AddSkillCapabilityVersion(t.Context(), memberInput); types.CodeOf(err) != types.CodeForbidden {
		t.Fatalf("member add shared version code=%s err=%v", types.CodeOf(err), err)
	}
	if _, err := st.ActivateSkillCapability(t.Context(), teamA, member, shared.ID, sharedV1.ID); types.CodeOf(err) != types.CodeForbidden {
		t.Fatalf("member activate shared code=%s err=%v", types.CodeOf(err), err)
	}
	adminInput := skillAddInputForRuntime(teamA, admin, shared.ID, "admin-v2", "", nil)
	sharedV2, err := st.AddSkillCapabilityVersion(t.Context(), adminInput)
	if err != nil || sharedV2.CreatedBy != admin {
		t.Fatalf("admin v2=%+v err=%v", sharedV2, err)
	}
	if _, err := st.ActivateSkillCapability(t.Context(), teamA, admin, shared.ID, sharedV2.ID); err != nil {
		t.Fatal(err)
	}
	sharedRef, err := st.ResolveSkillRef(t.Context(), teamA, member, shared.ID, sharedV2.ID)
	if err != nil || sharedRef.Visibility != skillruntime.VisibilityWorkspaceV1 {
		t.Fatalf("member shared ref=%+v err=%v", sharedRef, err)
	}
}

func skillCreateInputForRuntime(tenantID, actorUserID int64,
	visibility types.UserCapabilityVisibility, slug, skillText string,
) types.CreateSkillCapability {
	skillMD := []byte(skillText)
	fileManifest := runtimeSkillManifest(slug, "", true, false, []storedSkillManifestFileV1{{
		Path: "SKILL.md", Kind: "skill_md", Size: int64(len(skillMD)), Digest: digestBytes(skillMD),
	}})
	return types.CreateSkillCapability{
		TenantID: tenantID, ActorUserID: actorUserID, Visibility: visibility,
		Slug: slug, DisplayName: slug, Source: types.UserCapabilityUpload,
		PayloadDigest: digestBytes(fileManifest), Manifest: fileManifest, Compatible: true,
		Skill: types.SkillCapabilityVersion{
			Name: slug, SkillMDDigest: digestBytes(skillMD), ArchiveDigest: strings.Repeat("a", 64),
			FileManifest: fileManifest,
			Files: []types.SkillCapabilityFile{{
				Path: "SKILL.md", Kind: "skill_md", Digest: digestBytes(skillMD), Content: skillMD,
			}},
		},
	}
}

func skillAddInputForRuntime(tenantID, actorUserID int64, capabilityID uuid.UUID,
	skillText, extraPath string, extraContent []byte,
) AddSkillCapabilityVersionInput {
	skillMD := []byte(skillText)
	files := []types.SkillCapabilityFile{{
		Path: "SKILL.md", Kind: "skill_md", Digest: digestBytes(skillMD), Content: skillMD,
	}}
	manifestFiles := []storedSkillManifestFileV1{{
		Path: "SKILL.md", Kind: "skill_md", Size: int64(len(skillMD)), Digest: digestBytes(skillMD),
	}}
	if extraPath != "" {
		files = append(files, types.SkillCapabilityFile{
			Path: extraPath, Kind: "reference", Digest: digestBytes(extraContent), Content: extraContent,
		})
		manifestFiles = append(manifestFiles, storedSkillManifestFileV1{
			Path: extraPath, Kind: "reference", Size: int64(len(extraContent)), Digest: digestBytes(extraContent),
		})
	}
	fileManifest := runtimeSkillManifest("runtime-version", "", true, false, manifestFiles)
	return AddSkillCapabilityVersionInput{
		TenantID: tenantID, ActorUserID: actorUserID, CapabilityID: capabilityID,
		Source: types.UserCapabilityUpload, PayloadDigest: digestBytes(fileManifest), Manifest: fileManifest,
		Compatible: true,
		Skill: types.SkillCapabilityVersion{
			Name: "runtime-version", SkillMDDigest: digestBytes(skillMD), ArchiveDigest: strings.Repeat("b", 64),
			FileManifest: fileManifest, Files: files,
		},
	}
}

func runtimeSkillManifest(name, description string, compatible, containsScripts bool,
	files []storedSkillManifestFileV1,
) json.RawMessage {
	payload, _ := json.Marshal(storedSkillFileManifestV1{
		SchemaVersion: "vane.skill-package/v1", Name: name, Description: description,
		Compatible: compatible, ContainsScript: containsScripts, Files: files,
	})
	return payload
}
