package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/server/capabilityruntime"
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

func TestSkillMutationValidationAndOperationIdentifiers(t *testing.T) {
	valid := SkillCapabilityMutationInput{
		TenantID: 1, ActorUserID: 2,
		CapabilityID:             uuid.MustParse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
		VersionID:                uuid.MustParse("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"),
		ExpectedCurrentVersionID: uuid.MustParse("cccccccc-cccc-4ccc-8ccc-cccccccccccc"),
		ExpectedStatus:           types.UserCapabilityActive,
		ExpectedHeadRevision:     strings.Repeat("a", 64), OperationID: "op:A-z_0.9",
	}
	if err := validateSkillMutationInput(valid); err != nil {
		t.Fatal(err)
	}
	for _, status := range []types.UserCapabilityStatus{
		types.UserCapabilityDraft, types.UserCapabilityActive,
		types.UserCapabilityPaused, types.UserCapabilityIncompatible,
	} {
		if !validSkillCapabilityStatus(status) {
			t.Fatalf("valid status rejected: %q", status)
		}
	}
	if validSkillCapabilityStatus("deleted") {
		t.Fatal("unknown status accepted")
	}
	for _, operationID := range []string{"", strings.Repeat("a", 129), "bad/id"} {
		candidate := valid
		candidate.OperationID = operationID
		if err := validateSkillMutationInput(candidate); types.CodeOf(err) != types.CodeValidation {
			t.Fatalf("operation_id=%q code=%s err=%v", operationID, types.CodeOf(err), err)
		}
	}
	st := &Store{beginTx: func(context.Context, pgx.TxOptions) (pgx.Tx, error) {
		return nil, errors.New("database sentinel")
	}}
	invalid := valid
	invalid.TenantID = 0
	if _, err := st.ActivateSkillCapability(t.Context(), invalid); types.CodeOf(err) != types.CodeValidation {
		t.Fatalf("invalid activation code=%s err=%v", types.CodeOf(err), err)
	}
	if _, err := st.PauseSkillCapability(t.Context(), invalid); types.CodeOf(err) != types.CodeValidation {
		t.Fatalf("invalid pause code=%s err=%v", types.CodeOf(err), err)
	}
	principal := capabilityruntime.PrincipalV1{
		TenantID: 1, UserID: 2, Role: types.MembershipRoleOwner,
		ActorType: types.ActorTypeUser, MembershipAuthorizationGeneration: 1,
	}
	if _, err := st.beginSkillRuntimeTx(t.Context(), principal); types.CodeOf(err) != types.CodeDatabase {
		t.Fatalf("runtime begin failure code=%s err=%v", types.CodeOf(err), err)
	}
}

func TestSkillRuntimeLifecycleIsolationAndExactResourcePostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		testgate.Database(t)
	}
	database, provider, scratchURL, drop := migration128Scratch(t, databaseURL)
	t.Cleanup(drop)
	if _, err := provider.UpTo(t.Context(), 139); err != nil {
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
	ownerPrincipal := skillRuntimePrincipal(t, database, teamA, owner, types.MembershipRoleOwner)
	memberPrincipal := skillRuntimePrincipal(t, database, teamA, member, types.MembershipRoleMember)

	created, v1, err := st.CreateSkillCapability(t.Context(), skillCreateInputForRuntime(
		teamA, owner, types.UserCapabilityPersonal, "personal-watch", "v1 instructions"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.ResolveSkillRef(t.Context(), ownerPrincipal, created.ID, v1.ID); types.CodeOf(err) != types.CodeValidation {
		t.Fatalf("draft Skill resolved for runtime code=%s err=%v", types.CodeOf(err), err)
	}
	draftDetail, err := st.GetSkillCapabilityDetail(t.Context(), teamA, owner, created.ID, v1.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.OpenSkillResource(t.Context(), ownerPrincipal, draftDetail.Ref, "SKILL.md"); types.CodeOf(err) != types.CodeNotFound {
		t.Fatalf("never-activated Skill opened code=%s err=%v", types.CodeOf(err), err)
	}
	pauseDraft := skillMutationInput(draftDetail, owner, v1.ID, "pause-draft")
	if _, err := st.PauseSkillCapability(t.Context(), pauseDraft); types.CodeOf(err) != types.CodeValidation {
		t.Fatalf("draft pause code=%s err=%v", types.CodeOf(err), err)
	}
	servicePrincipal := ownerPrincipal
	servicePrincipal.ActorType = types.ActorTypeServiceAccount
	servicePrincipal.A2ATokenAuthorityID = uuid.NewString()
	servicePrincipal.RequiredA2AScope = types.A2AScopeAssistantChat
	if _, err := st.ResolveSkillRef(t.Context(), servicePrincipal, created.ID, v1.ID); types.CodeOf(err) != types.CodeForbidden {
		t.Fatalf("service principal runtime code=%s err=%v", types.CodeOf(err), err)
	}
	stalePrincipal := ownerPrincipal
	stalePrincipal.MembershipAuthorizationGeneration++
	if _, err := st.ResolveSkillRef(t.Context(), stalePrincipal, created.ID, v1.ID); types.CodeOf(err) != types.CodeForbidden {
		t.Fatalf("stale principal runtime code=%s err=%v", types.CodeOf(err), err)
	}
	missingPrincipal := ownerPrincipal
	missingPrincipal.UserID = 999999999
	if _, err := st.ResolveSkillRef(t.Context(), missingPrincipal, created.ID, v1.ID); types.CodeOf(err) != types.CodeForbidden {
		t.Fatalf("missing principal runtime code=%s err=%v", types.CodeOf(err), err)
	}
	activateV1 := skillMutationInput(draftDetail, owner, v1.ID, "activate-v1")
	if _, err := st.ActivateSkillCapability(t.Context(), activateV1); err != nil {
		t.Fatal(err)
	}
	v1Ref, err := st.ResolveSkillRef(t.Context(), ownerPrincipal, created.ID, v1.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.ResolveSkillRef(t.Context(), ownerPrincipal, created.ID, uuid.New()); types.CodeOf(err) != types.CodeNotFound {
		t.Fatalf("missing runtime version code=%s err=%v", types.CodeOf(err), err)
	}
	missingVersionRef := v1Ref
	missingVersionRef.CapabilityVersionID = uuid.New()
	if _, err := st.OpenSkillResource(t.Context(), ownerPrincipal, missingVersionRef, "SKILL.md"); types.CodeOf(err) != types.CodeNotFound {
		t.Fatalf("missing ref version code=%s err=%v", types.CodeOf(err), err)
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
	activateV2 := skillMutationInput(v1Detail, owner, v2.ID, "activate-v2")
	active, err := st.ActivateSkillCapability(t.Context(), activateV2)
	if err != nil {
		t.Fatal(err)
	}
	if active.ResultStatus != types.UserCapabilityActive || active.VersionID != v2.ID || active.Replayed {
		t.Fatalf("active=%+v", active)
	}
	replayedActivation, err := st.ActivateSkillCapability(t.Context(), activateV2)
	if err != nil || !replayedActivation.Replayed || replayedActivation.ResultHeadRevision != active.ResultHeadRevision {
		t.Fatalf("idempotent activation: %v", err)
	}
	var activationEvents int
	if err := database.QueryRowContext(t.Context(), `
		SELECT count(*) FROM user_capability_events
		WHERE capability_id=$1 AND version_id=$2 AND event_kind='activated'`,
		created.ID, v2.ID).Scan(&activationEvents); err != nil || activationEvents != 1 {
		t.Fatalf("activation events=%d err=%v", activationEvents, err)
	}

	v3Input := skillAddInputForRuntime(teamA, owner, created.ID, "v3 instructions", "", nil)
	v3, err := st.AddSkillCapabilityVersion(t.Context(), v3Input)
	if err != nil {
		t.Fatal(err)
	}
	v2Detail, err := st.GetSkillCapabilityDetail(t.Context(), teamA, owner, created.ID, v2.ID)
	if err != nil {
		t.Fatal(err)
	}
	activateV3 := skillMutationInput(v2Detail, owner, v3.ID, "activate-v3")
	if _, err := st.ActivateSkillCapability(t.Context(), activateV3); err != nil {
		t.Fatal(err)
	}
	staleReplay, err := st.ActivateSkillCapability(t.Context(), activateV2)
	if err != nil || !staleReplay.Replayed {
		t.Fatalf("lost-response stale activation replay=%+v err=%v", staleReplay, err)
	}
	v3Detail, err := st.GetSkillCapabilityDetail(t.Context(), teamA, owner, created.ID, v3.ID)
	if err != nil || v3Detail.Capability.CurrentVersionID == nil || *v3Detail.Capability.CurrentVersionID != v3.ID {
		t.Fatalf("stale activation moved head detail=%+v err=%v", v3Detail, err)
	}
	conflictingActivate := activateV2
	conflictingActivate.OperationID = "activate-v2-stale-new-operation"
	if _, err := st.ActivateSkillCapability(t.Context(), conflictingActivate); types.CodeOf(err) != types.CodeConflict {
		t.Fatalf("stale new activation code=%s err=%v", types.CodeOf(err), err)
	}
	operationReuse := activateV2
	operationReuse.VersionID = v3.ID
	if _, err := st.ActivateSkillCapability(t.Context(), operationReuse); types.CodeOf(err) != types.CodeConflict {
		t.Fatalf("operation reuse code=%s err=%v", types.CodeOf(err), err)
	}

	// Exact historical v1 remains readable after v3 becomes current. A query
	// that drops capability_version_id would return v2 bytes and fail here.
	v1Handle, err := st.OpenSkillResource(t.Context(), ownerPrincipal, v1Ref, "SKILL.md")
	if err != nil {
		t.Fatal(err)
	}
	v1Chunk, err := st.ReadSkillResourceChunk(t.Context(), ownerPrincipal, v1Handle, 0, 4)
	if err != nil {
		t.Fatal(err)
	}
	var reconstructed []byte
	reconstructed = append(reconstructed, v1Chunk.Data...)
	for !v1Chunk.EOF {
		v1Chunk, err = st.ReadSkillResourceChunk(t.Context(), ownerPrincipal, v1Handle,
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
	if _, err := st.ReadSkillResourceChunk(t.Context(), ownerPrincipal, forged, 0, 4); types.CodeOf(err) != types.CodeNotFound {
		t.Fatalf("forged handle code=%s err=%v", types.CodeOf(err), err)
	}
	if _, err := st.ReadSkillResourceChunk(t.Context(), memberPrincipal, v1Handle, 0, 4); types.CodeOf(err) != types.CodeNotFound {
		t.Fatalf("member read personal bytes code=%s err=%v", types.CodeOf(err), err)
	}

	pauseV3 := skillMutationInput(v3Detail, owner, v3.ID, "pause-v3-first")
	paused, err := st.PauseSkillCapability(t.Context(), pauseV3)
	if err != nil || paused.ResultStatus != types.UserCapabilityPaused || paused.Replayed {
		t.Fatalf("paused=%+v err=%v", paused, err)
	}
	pauseReplay, err := st.PauseSkillCapability(t.Context(), pauseV3)
	if err != nil || !pauseReplay.Replayed {
		t.Fatalf("idempotent pause=%+v err=%v", pauseReplay, err)
	}
	pauseOperationReuse := pauseV3
	pauseOperationReuse.VersionID = v2.ID
	if _, err := st.PauseSkillCapability(t.Context(), pauseOperationReuse); types.CodeOf(err) != types.CodeConflict {
		t.Fatalf("pause operation reuse code=%s err=%v", types.CodeOf(err), err)
	}
	pausedDetail, err := st.GetSkillCapabilityDetail(t.Context(), teamA, owner, created.ID, v3.ID)
	if err != nil {
		t.Fatal(err)
	}
	reactivateV3 := skillMutationInput(pausedDetail, owner, v3.ID, "reactivate-v3")
	if _, err := st.ActivateSkillCapability(t.Context(), reactivateV3); err != nil {
		t.Fatal(err)
	}
	stalePauseReplay, err := st.PauseSkillCapability(t.Context(), pauseV3)
	if err != nil || !stalePauseReplay.Replayed {
		t.Fatalf("lost-response stale pause replay=%+v err=%v", stalePauseReplay, err)
	}
	stalePauseNewOperation := pauseV3
	stalePauseNewOperation.OperationID = "pause-v3-stale-new-operation"
	if _, err := st.PauseSkillCapability(t.Context(), stalePauseNewOperation); types.CodeOf(err) != types.CodeConflict {
		t.Fatalf("stale new pause code=%s err=%v", types.CodeOf(err), err)
	}
	reactivatedDetail, err := st.GetSkillCapabilityDetail(t.Context(), teamA, owner, created.ID, v3.ID)
	if err != nil || reactivatedDetail.Capability.Status != types.UserCapabilityActive {
		t.Fatalf("stale pause changed reactivated head=%+v err=%v", reactivatedDetail, err)
	}
	pauseV3Final := skillMutationInput(reactivatedDetail, owner, v3.ID, "pause-v3-final")
	if _, err := st.PauseSkillCapability(t.Context(), pauseV3Final); err != nil {
		t.Fatal(err)
	}
	var pauseFirstEvents, pauseFinalEvents int
	if err := database.QueryRowContext(t.Context(), `
		SELECT count(*) FILTER (WHERE details->>'operation_id'='pause-v3-first'),
		       count(*) FILTER (WHERE details->>'operation_id'='pause-v3-final')
		FROM user_capability_events WHERE capability_id=$1 AND event_kind='paused'`,
		created.ID).Scan(&pauseFirstEvents, &pauseFinalEvents); err != nil || pauseFirstEvents != 1 || pauseFinalEvents != 1 {
		t.Fatalf("pause events first=%d final=%d err=%v", pauseFirstEvents, pauseFinalEvents, err)
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
	pausedFinalDetail, err := st.GetSkillCapabilityDetail(t.Context(), teamA, owner, created.ID, v3.ID)
	if err != nil {
		t.Fatal(err)
	}
	activateScript := skillMutationInput(pausedFinalDetail, owner, scriptVersion.ID, "activate-script")
	if _, err := st.ActivateSkillCapability(t.Context(), activateScript); types.CodeOf(err) != types.CodeValidation {
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
	sharedDetail, err := st.GetSkillCapabilityDetail(t.Context(), teamA, owner, shared.ID, sharedV1.ID)
	if err != nil {
		t.Fatal(err)
	}
	memberActivate := skillMutationInput(sharedDetail, member, sharedV1.ID, "member-activate-shared")
	if _, err := st.ActivateSkillCapability(t.Context(), memberActivate); types.CodeOf(err) != types.CodeForbidden {
		t.Fatalf("member activate shared code=%s err=%v", types.CodeOf(err), err)
	}
	adminInput := skillAddInputForRuntime(teamA, admin, shared.ID, "admin-v2", "", nil)
	sharedV2, err := st.AddSkillCapabilityVersion(t.Context(), adminInput)
	if err != nil || sharedV2.CreatedBy != admin {
		t.Fatalf("admin v2=%+v err=%v", sharedV2, err)
	}
	sharedV1Detail, err := st.GetSkillCapabilityDetail(t.Context(), teamA, owner, shared.ID, sharedV1.ID)
	if err != nil {
		t.Fatal(err)
	}
	adminActivate := skillMutationInput(sharedV1Detail, admin, sharedV2.ID, "admin-activate-shared-v2")
	if _, err := st.ActivateSkillCapability(t.Context(), adminActivate); err != nil {
		t.Fatal(err)
	}
	sharedRef, err := st.ResolveSkillRef(t.Context(), memberPrincipal, shared.ID, sharedV2.ID)
	if err != nil || sharedRef.Visibility != skillruntime.VisibilityWorkspaceV1 {
		t.Fatalf("member shared ref=%+v err=%v", sharedRef, err)
	}

	faultCapability, faultV1, err := st.CreateSkillCapability(t.Context(), skillCreateInputForRuntime(
		teamA, owner, types.UserCapabilityPersonal, "event-fault-watch", "fault-v1"))
	if err != nil {
		t.Fatal(err)
	}
	faultDetail, err := st.GetSkillCapabilityDetail(t.Context(), teamA, owner, faultCapability.ID, faultV1.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), `REVOKE INSERT ON user_capability_events FROM vane_app`); err != nil {
		t.Fatal(err)
	}
	faultActivation := skillMutationInput(faultDetail, owner, faultV1.ID, "activation-event-fault")
	if _, err := st.ActivateSkillCapability(t.Context(), faultActivation); types.CodeOf(err) != types.CodeDatabase {
		t.Fatalf("missing event authority code=%s err=%v", types.CodeOf(err), err)
	}
	if _, err := database.ExecContext(t.Context(), `GRANT INSERT ON user_capability_events TO vane_app`); err != nil {
		t.Fatal(err)
	}
	faultAfter, err := st.GetSkillCapabilityDetail(t.Context(), teamA, owner, faultCapability.ID, faultV1.ID)
	if err != nil || faultAfter.Capability.Status != types.UserCapabilityDraft {
		t.Fatalf("failed activation committed without event detail=%+v err=%v", faultAfter, err)
	}
}

func skillRuntimePrincipal(t *testing.T, database *sql.DB, tenantID, userID int64,
	role types.MembershipRole,
) capabilityruntime.PrincipalV1 {
	t.Helper()
	var generation int64
	if err := database.QueryRowContext(t.Context(), `
		SELECT authorization_generation FROM memberships WHERE tenant_id=$1 AND user_id=$2`,
		tenantID, userID).Scan(&generation); err != nil {
		t.Fatal(err)
	}
	return capabilityruntime.PrincipalV1{
		TenantID: types.TenantID(tenantID), UserID: userID, Role: role,
		ActorType: types.ActorTypeUser, MembershipAuthorizationGeneration: generation,
	}
}

func skillMutationInput(detail *SkillCapabilityDetail, actorUserID int64, versionID uuid.UUID,
	operationID string,
) SkillCapabilityMutationInput {
	return SkillCapabilityMutationInput{
		TenantID: detail.Capability.TenantID, ActorUserID: actorUserID,
		CapabilityID: detail.Capability.ID, VersionID: versionID,
		ExpectedStatus: detail.Capability.Status, ExpectedHeadRevision: detail.HeadRevision,
		ExpectedCurrentVersionID: *detail.Capability.CurrentVersionID, OperationID: operationID,
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
