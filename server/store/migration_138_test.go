package store

import (
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/server/internal/testgate"
	"github.com/YouToco/vane/server/types"
)

func TestMigration138WorkspaceMemoryContract(t *testing.T) {
	payload, err := migrationsFS.ReadFile("migrations/138_workspace_long_term_memory.sql")
	if err != nil {
		t.Fatal(err)
	}
	sqlText := string(payload)
	for _, want := range []string{
		"CREATE TABLE workspace_memory_authorizations",
		"CREATE TABLE workspace_memory_records",
		"CREATE TABLE workspace_memory_events",
		"CREATE TABLE workspace_memory_receipts",
		"FORCE ROW LEVEL SECURITY", "vane_workspace_memory_editor",
		"uq_workspace_memory_record_authorization",
		"uq_workspace_memory_event_authorization",
		"uq_workspace_memory_event_consumption_binding",
		"owner_explicit_agent_turn", "refusing downgrade while retained workspace memory exists",
	} {
		if !strings.Contains(sqlText, want) {
			t.Errorf("migration 138 missing %q", want)
		}
	}
	for _, forbidden := range []string{
		"FROM memory_records", "JOIN memory_records", "UPDATE workspace_memory_records",
		"DELETE ON workspace_memory", "GRANT UPDATE ON workspace_memory_records",
	} {
		if strings.Contains(sqlText, forbidden) {
			t.Errorf("migration 138 violates independent append-only ledger: %q", forbidden)
		}
	}
	workspaceStore, err := os.ReadFile("workspace_memory.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, lock := range []string{"FOR KEY SHARE OF m,t", "FOR UPDATE OF m,t",
		"lockTenantAdmissionRootShared", "pg_catalog.pg_advisory_xact_lock_shared",
		"pg_catalog.pg_advisory_xact_lock"} {
		if !strings.Contains(string(workspaceStore), lock) {
			t.Errorf("workspace memory admission missing %q", lock)
		}
	}
}

func TestMigration138WorkspaceMemoryIsolationAndRolesPostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		testgate.Database(t)
	}
	database, provider, scratchURL, drop := migration128Scratch(t, databaseURL)
	t.Cleanup(drop)
	if _, err := provider.UpTo(t.Context(), 138); err != nil {
		t.Fatal(err)
	}

	creator := migration138User(t, database, "creator")
	member := migration138User(t, database, "member")
	admin := migration138User(t, database, "admin")
	outsider := migration138User(t, database, "outsider")
	team := migration138Workspace(t, database, "team", "team", nil)
	otherTeam := migration138Workspace(t, database, "other", "team", nil)
	personal := migration138Workspace(t, database, "personal", "personal", &creator)
	for _, membership := range []struct {
		tenant, user int64
		role         string
	}{
		{team, creator, "member"}, {team, member, "member"},
		{team, admin, "admin"}, {otherTeam, outsider, "owner"},
		{personal, creator, "owner"},
	} {
		if _, err := database.ExecContext(t.Context(), `INSERT INTO memberships(
			tenant_id,user_id,role) VALUES($1,$2,$3)`, membership.tenant,
			membership.user, membership.role); err != nil {
			t.Fatal(err)
		}
	}
	sessions := map[int64]int64{}
	for _, pair := range []struct{ tenant, user int64 }{
		{team, creator}, {team, member}, {team, admin},
		{otherTeam, outsider}, {personal, creator},
	} {
		var sessionID int64
		if err := database.QueryRowContext(t.Context(), `INSERT INTO agent_sessions(
			tenant_id,user_id) VALUES($1,$2) RETURNING id`, pair.tenant, pair.user).
			Scan(&sessionID); err != nil {
			t.Fatal(err)
		}
		sessions[pair.tenant<<32|pair.user] = sessionID
	}

	workspaceStoreURL := migration138ApplicationURL(t, scratchURL,
		"migration138-workspace-memory")
	st, err := New(t.Context(), workspaceStoreURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	canonicalReplay := migration138Action(types.MemoryActionRemember, 0, "canonical replay")
	canonicalReplay.Evidence.OwnerRequest = "  明确写入团队记忆  \n"
	canonicalAuth, err := st.PrepareWorkspaceMemoryAuthorization(t.Context(), team,
		creator, sessions[team<<32|creator], canonicalReplay)
	if err != nil {
		t.Fatal(err)
	}
	var canonicalOwnerRequest string
	if err := database.QueryRowContext(t.Context(), `SELECT owner_request
		FROM workspace_memory_authorizations WHERE tenant_id=$1 AND id=$2`,
		team, canonicalAuth).Scan(&canonicalOwnerRequest); err != nil {
		t.Fatal(err)
	}
	if canonicalOwnerRequest != "明确写入团队记忆" {
		t.Fatalf("authorization persisted non-canonical request %q", canonicalOwnerRequest)
	}
	var alternateSession int64
	if err := database.QueryRowContext(t.Context(), `INSERT INTO agent_sessions(
		tenant_id,user_id) VALUES($1,$2) RETURNING id`, team, creator).Scan(&alternateSession); err != nil {
		t.Fatal(err)
	}
	for _, mutation := range []struct {
		name    string
		query   string
		value   any
		restore any
	}{
		{"owner", "owner_request=$3", "owner-drift", canonicalOwnerRequest},
		{"session", "session_id=$3", alternateSession, sessions[team<<32|creator]},
		{"trace", "trace_id=$3", uuid.NewString(), canonicalReplay.Evidence.SourceID},
		{"authorization digest", "authorization_digest=$3", strings.Repeat("0", 64),
			canonicalReplay.Evidence.AuthorizationDigest},
	} {
		if _, err := database.ExecContext(t.Context(), `UPDATE workspace_memory_authorizations SET `+
			mutation.query+` WHERE tenant_id=$1 AND id=$2`, team, canonicalAuth, mutation.value); err != nil {
			t.Fatal(err)
		}
		if _, err := st.PrepareWorkspaceMemoryAuthorization(t.Context(), team, creator,
			sessions[team<<32|creator], canonicalReplay); types.CodeOf(err) != types.CodeConflict {
			t.Fatalf("same-id %s drift code=%s err=%v", mutation.name, types.CodeOf(err), err)
		}
		if _, err := database.ExecContext(t.Context(), `UPDATE workspace_memory_authorizations SET `+
			mutation.query+` WHERE tenant_id=$1 AND id=$2`, team, canonicalAuth, mutation.restore); err != nil {
			t.Fatal(err)
		}
	}
	teamPersonalAction := migration138Action(
		types.MemoryActionRemember, 0, "must-not-enter-personal-ledger")
	if _, err := st.PrepareMemoryAuthorization(t.Context(), team, creator,
		sessions[team<<32|creator], teamPersonalAction); types.CodeOf(err) != types.CodeForbidden {
		t.Fatalf("personal ledger accepted team workspace code=%s err=%v",
			types.CodeOf(err), err)
	}

	// Personal v129 data exists for the same account but must never enter the
	// team's corpus, even when the query exactly matches it.
	personalAction := migration138Action(types.MemoryActionRemember, 0, "personal-only-zebra")
	personalAuth, err := st.PrepareMemoryAuthorization(
		t.Context(), personal, creator, sessions[personal<<32|creator], personalAction)
	if err != nil {
		t.Fatal(err)
	}
	personalAction.Evidence.AuthorizationID = personalAuth
	if _, err := st.ApplyMemoryAction(t.Context(), personal, creator,
		strings.Repeat("1", 64), personalAction); err != nil {
		t.Fatal(err)
	}

	remember := migration138Action(types.MemoryActionRemember, 0, "team-shared-orbit")
	remembered := migration138Apply(t, st, team, creator,
		sessions[team<<32|creator], strings.Repeat("2", 64), remember)
	if remembered.Memory.CreatorUserID != creator {
		t.Fatalf("creator=%d want=%d", remembered.Memory.CreatorUserID, creator)
	}
	remember.Evidence.AuthorizationID = remembered.Memory.Evidence.AuthorizationID
	replayed, err := st.ApplyWorkspaceMemoryAction(t.Context(), team, creator,
		strings.Repeat("2", 64), remember)
	if err != nil || replayed.Event.ID != remembered.Event.ID {
		t.Fatalf("same-key receipt replay result=%+v err=%v", replayed, err)
	}
	if _, err := st.ApplyWorkspaceMemoryAction(t.Context(), team, creator,
		strings.Repeat("c", 64), remember); types.CodeOf(err) != types.CodeConflict {
		t.Fatalf("second authorization consumption code=%s err=%v", types.CodeOf(err), err)
	}
	// Defense-in-depth exact-once constraints are independent mutation guards.
	// Even the restricted role cannot reuse a consumed remember authorization.
	duplicateRecordTx, err := database.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	migration138EnterWorkspaceRole(t, duplicateRecordTx, team, creator, "member")
	if _, err := duplicateRecordTx.ExecContext(t.Context(), `INSERT INTO workspace_memory_records(
		tenant_id,creator_user_id,created_by_user_id,memory_text,evidence_source_type,
		evidence_source_id,authorization_id,owner_request,authorization_digest,supersedes_memory_id)
		VALUES($1,$2,$2,$3,$4,$5,$6,$7,$8,NULL)`, team, creator,
		remembered.Memory.Text, remembered.Memory.Evidence.SourceType,
		remembered.Memory.Evidence.SourceID, remembered.Memory.Evidence.AuthorizationID,
		remembered.Memory.Evidence.OwnerRequest,
		remembered.Memory.Evidence.AuthorizationDigest); err == nil {
		t.Fatal("record exact-once constraint accepted consumed authorization")
	}
	if err := duplicateRecordTx.Rollback(); err != nil {
		t.Fatal(err)
	}

	// Drop only the record guard inside a rollback-only owner transaction to
	// prove the event guard independently rejects the same authorization.
	duplicateEventTx, err := database.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := duplicateEventTx.ExecContext(t.Context(), `ALTER TABLE workspace_memory_records
		DROP CONSTRAINT uq_workspace_memory_record_authorization`); err != nil {
		t.Fatal(err)
	}
	migration138EnterWorkspaceRole(t, duplicateEventTx, team, creator, "member")
	var duplicateRecordID int64
	if err := duplicateEventTx.QueryRowContext(t.Context(), `INSERT INTO workspace_memory_records(
		tenant_id,creator_user_id,created_by_user_id,memory_text,evidence_source_type,
		evidence_source_id,authorization_id,owner_request,authorization_digest,supersedes_memory_id)
		VALUES($1,$2,$2,$3,$4,$5,$6,$7,$8,NULL) RETURNING id`, team, creator,
		remembered.Memory.Text, remembered.Memory.Evidence.SourceType,
		remembered.Memory.Evidence.SourceID, remembered.Memory.Evidence.AuthorizationID,
		remembered.Memory.Evidence.OwnerRequest,
		remembered.Memory.Evidence.AuthorizationDigest).Scan(&duplicateRecordID); err != nil {
		t.Fatal(err)
	}
	if _, err := duplicateEventTx.ExecContext(t.Context(), `INSERT INTO workspace_memory_events(
		tenant_id,actor_user_id,actor_role,event_kind,target_memory_id,result_memory_id,
		evidence_source_type,evidence_source_id,authorization_id,owner_request,authorization_digest)
		VALUES($1,$2,'member','remember',NULL,$3,$4,$5,$6,$7,$8)`, team, creator,
		duplicateRecordID, remembered.Memory.Evidence.SourceType,
		remembered.Memory.Evidence.SourceID, remembered.Memory.Evidence.AuthorizationID,
		remembered.Memory.Evidence.OwnerRequest,
		remembered.Memory.Evidence.AuthorizationDigest); err == nil {
		t.Fatal("event exact-once constraint accepted consumed authorization")
	}
	if err := duplicateEventTx.Rollback(); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"source", "owner", "digest"} {
		migration138AssertRecordFieldMutationRejected(t, database, team, creator,
			remembered, field)
	}
	for _, field := range []string{"source", "owner", "digest"} {
		migration138AssertEventFieldMutationRejected(t, database, team, creator,
			remembered, field)
	}
	recall, err := st.RecallWorkspaceMemories(t.Context(), team, member,
		types.MemoryRecallQuery{Query: "team shared orbit", Limit: 8})
	if err != nil {
		t.Fatal(err)
	}
	if len(recall.Memories) != 1 || recall.Memories[0].Memory.ID != remembered.Memory.ID {
		t.Fatalf("member recall=%+v", recall)
	}
	personalLeak, err := st.RecallWorkspaceMemories(t.Context(), team, member,
		types.MemoryRecallQuery{Query: "personal only zebra", Limit: 8})
	if err != nil {
		t.Fatal(err)
	}
	if len(personalLeak.Memories) != 0 {
		t.Fatalf("team recall leaked personal memory: %+v", personalLeak)
	}
	other, err := st.RecallWorkspaceMemories(t.Context(), otherTeam, outsider,
		types.MemoryRecallQuery{Query: "team shared orbit", Limit: 8})
	if err != nil {
		t.Fatal(err)
	}
	if len(other.Memories) != 0 {
		t.Fatalf("cross-workspace recall leaked: %+v", other)
	}

	// A non-admin member cannot forget another member's memory. Failed actions
	// leave their authorization unconsumed and append no event.
	forget := migration138Action(types.MemoryActionForget, remembered.Memory.ID, "")
	if _, err := st.PrepareWorkspaceMemoryAuthorization(
		t.Context(), team, member, sessions[team<<32|member], forget); types.CodeOf(err) != types.CodeForbidden {
		t.Fatalf("member forget code=%s err=%v", types.CodeOf(err), err)
	}

	// Creator correction is allowed. An Admin may correct it again, but the
	// replacement still belongs to the original creator rather than the actor.
	correct := migration138Action(types.MemoryActionCorrect, remembered.Memory.ID,
		"team-shared-orbit-corrected")
	corrected := migration138Apply(t, st, team, creator,
		sessions[team<<32|creator], strings.Repeat("4", 64), correct)
	adminCorrect := migration138Action(types.MemoryActionCorrect, corrected.Memory.ID,
		"team-shared-orbit-admin-corrected")
	adminCorrected := migration138Apply(t, st, team, admin,
		sessions[team<<32|admin], strings.Repeat("5", 64), adminCorrect)
	if adminCorrected.Memory.CreatorUserID != creator {
		t.Fatalf("admin correction changed creator=%d want=%d",
			adminCorrected.Memory.CreatorUserID, creator)
	}
	adminForget := migration138Action(types.MemoryActionForget, adminCorrected.Memory.ID, "")
	migration138Apply(t, st, team, admin, sessions[team<<32|admin],
		strings.Repeat("6", 64), adminForget)
	afterForget, err := st.RecallWorkspaceMemories(t.Context(), team, creator,
		types.MemoryRecallQuery{Query: "orbit corrected", Limit: 8})
	if err != nil {
		t.Fatal(err)
	}
	if len(afterForget.Memories) != 0 {
		t.Fatalf("forgotten record remained in BM25 corpus: %+v", afterForget)
	}

	// A durable authorization freezes the membership role. Changing the role
	// between Prepare and Apply makes the old authorization invisible/invalid;
	// it is not silently upgraded to the actor's new authority.
	driftBase := migration138Apply(t, st, team, creator,
		sessions[team<<32|creator], strings.Repeat("7", 64),
		migration138Action(types.MemoryActionRemember, 0, "role-drift-boundary"))
	for _, mutation := range []struct {
		name  string
		value any
	}{{"clear", nil}, {"change", driftBase.Event.ID}} {
		mutationTx, err := database.BeginTx(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}
		migration138EnterWorkspaceRole(t, mutationTx, team, creator, "member")
		tag, err := mutationTx.ExecContext(t.Context(), `UPDATE workspace_memory_authorizations
			SET consumed_event_id=$3 WHERE tenant_id=$1 AND actor_user_id=$2 AND id=$4`,
			team, creator, mutation.value, remembered.Memory.Evidence.AuthorizationID)
		if err != nil {
			t.Fatal(err)
		}
		rowsAffected, err := tag.RowsAffected()
		if err != nil {
			t.Fatal(err)
		}
		if rowsAffected != 0 {
			t.Fatalf("restricted role could %s consumed authorization", mutation.name)
		}
		if err := mutationTx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	var retainedConsumed int64
	if err := database.QueryRowContext(t.Context(), `SELECT consumed_event_id
		FROM workspace_memory_authorizations WHERE tenant_id=$1 AND id=$2`, team,
		remembered.Memory.Evidence.AuthorizationID).Scan(&retainedConsumed); err != nil {
		t.Fatal(err)
	}
	if retainedConsumed != remembered.Event.ID {
		t.Fatalf("consumed binding changed=%d want=%d", retainedConsumed, remembered.Event.ID)
	}
	// Independently mutate below RLS as the migration owner: the composite
	// deferred FK, not the policy, must reject swapping A/B consumed events.
	swapTx, err := database.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := swapTx.ExecContext(t.Context(), `UPDATE workspace_memory_authorizations
		SET consumed_event_id=NULL WHERE tenant_id=$1 AND actor_user_id=$2
		AND id IN($3::uuid,$4::uuid)`, team, creator,
		remembered.Memory.Evidence.AuthorizationID,
		driftBase.Memory.Evidence.AuthorizationID); err != nil {
		t.Fatal(err)
	}
	if _, err := swapTx.ExecContext(t.Context(), `UPDATE workspace_memory_authorizations
		SET consumed_event_id=CASE id WHEN $3::uuid THEN $4::bigint ELSE $5::bigint END
		WHERE tenant_id=$1 AND actor_user_id=$2 AND id IN($3::uuid,$6::uuid)`,
		team, creator, remembered.Memory.Evidence.AuthorizationID, driftBase.Event.ID,
		remembered.Event.ID, driftBase.Memory.Evidence.AuthorizationID); err != nil {
		t.Fatal(err)
	}
	if _, err := swapTx.ExecContext(t.Context(),
		`SET CONSTRAINTS fk_workspace_memory_authorization_consumed IMMEDIATE`); err == nil {
		t.Fatal("composite consumed FK accepted swapped authorization events")
	}
	if err := swapTx.Rollback(); err != nil {
		t.Fatal(err)
	}
	drift := migration138Action(types.MemoryActionCorrect, driftBase.Memory.ID,
		"role-drift-must-not-apply")
	driftAuth, err := st.PrepareWorkspaceMemoryAuthorization(t.Context(), team,
		creator, sessions[team<<32|creator], drift)
	if err != nil {
		t.Fatal(err)
	}
	drift.Evidence.AuthorizationID = driftAuth
	for _, field := range []string{"action", "target"} {
		migration138AssertCorrectRecordBindingRejected(t, database, team, creator,
			driftBase, remembered, drift, field)
		migration138AssertCorrectEventBindingRejected(t, database, team, creator,
			driftBase, remembered, drift, field)
	}
	if _, err := st.ApplyWorkspaceMemoryAction(t.Context(), team, member,
		strings.Repeat("8", 64), drift); types.CodeOf(err) != types.CodeConflict {
		t.Fatalf("cross-actor authorization replay code=%s err=%v", types.CodeOf(err), err)
	}
	wrongTarget := drift
	wrongTarget.MemoryID = remembered.Memory.ID
	if _, err := st.ApplyWorkspaceMemoryAction(t.Context(), team, creator,
		strings.Repeat("9", 64), wrongTarget); types.CodeOf(err) != types.CodeConflict {
		t.Fatalf("target-bound authorization replay code=%s err=%v", types.CodeOf(err), err)
	}
	wrongAction := drift
	wrongAction.Action = types.MemoryActionForget
	wrongAction.Text = ""
	if _, err := st.ApplyWorkspaceMemoryAction(t.Context(), team, creator,
		strings.Repeat("a", 64), wrongAction); types.CodeOf(err) != types.CodeConflict {
		t.Fatalf("action-bound authorization replay code=%s err=%v", types.CodeOf(err), err)
	}
	if _, err := database.ExecContext(t.Context(), `UPDATE memberships SET role='admin'
		WHERE tenant_id=$1 AND user_id=$2`, team, creator); err != nil {
		t.Fatal(err)
	}
	if _, err := st.ApplyWorkspaceMemoryAction(t.Context(), team, creator,
		strings.Repeat("b", 64), drift); types.CodeOf(err) != types.CodeConflict {
		t.Fatalf("role-drift apply code=%s err=%v", types.CodeOf(err), err)
	}
	if _, err := database.ExecContext(t.Context(), `UPDATE memberships SET role='member'
		WHERE tenant_id=$1 AND user_id=$2`, team, creator); err != nil {
		t.Fatal(err)
	}
	var consumed *int64
	if err := database.QueryRowContext(t.Context(), `SELECT consumed_event_id
		FROM workspace_memory_authorizations WHERE tenant_id=$1 AND id=$2`,
		team, driftAuth).Scan(&consumed); err != nil {
		t.Fatal(err)
	}
	if consumed != nil {
		t.Fatalf("role-drift authorization consumed event=%d", *consumed)
	}

	// A correct authorization cannot be repurposed through the remember-shaped
	// (NULL supersedes) record branch even by a direct holder of the capability
	// role. The database binds both branches to the frozen action and target.
	mutationTx, err := database.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mutationTx.ExecContext(t.Context(), `SELECT
		set_config('app.tenant_id',$1,true),set_config('app.user_id',$2,true),
		set_config('app.membership_role','member',true),
		set_config('app.workspace_kind','team',true)`, fmt.Sprint(team), fmt.Sprint(creator)); err != nil {
		t.Fatal(err)
	}
	if _, err := mutationTx.ExecContext(t.Context(), `SET LOCAL ROLE vane_workspace_memory_editor`); err != nil {
		t.Fatal(err)
	}
	if _, err := mutationTx.ExecContext(t.Context(), `INSERT INTO workspace_memory_records(
		tenant_id,creator_user_id,created_by_user_id,memory_text,evidence_source_type,
		evidence_source_id,authorization_id,owner_request,authorization_digest,supersedes_memory_id)
		VALUES($1,$2,$2,$3,$4,$5,$6,$7,$8,NULL)`, team, creator, drift.Text,
		drift.Evidence.SourceType, drift.Evidence.SourceID, driftAuth,
		drift.Evidence.OwnerRequest, drift.Evidence.AuthorizationDigest); err == nil {
		t.Fatal("correct authorization entered remember-shaped record branch")
	}
	if err := mutationTx.Rollback(); err != nil {
		t.Fatal(err)
	}

	// Purge owns the exclusive tenant-admission root. A team recall joins it as
	// shared before touching its workspace lock, then resumes without deadlock.
	rootTx, err := database.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rootTx.ExecContext(t.Context(), `SELECT pg_advisory_xact_lock(
		hashtextextended('vane/tenant-admission/v1/'||($1::bigint)::text,$2))`,
		team, tenantAdmissionRootLockSeed); err != nil {
		t.Fatal(err)
	}
	rootRecallDone := make(chan error, 1)
	go func() {
		_, recallErr := st.RecallWorkspaceMemories(t.Context(), team, member,
			types.MemoryRecallQuery{Query: "role drift boundary", Limit: 8})
		rootRecallDone <- recallErr
	}()
	migration138AwaitAdvisoryWaiters(t, database, "migration138-workspace-memory", 1)
	if err := rootTx.Rollback(); err != nil {
		t.Fatal(err)
	}
	select {
	case recallErr := <-rootRecallDone:
		if recallErr != nil {
			t.Fatal(recallErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("workspace recall deadlocked after tenant admission release")
	}

	// The admission key is workspace-wide rather than actor-wide: a member's
	// recall must wait behind an exclusive writer lock acquired by another actor.
	lockTx, err := database.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lockTx.ExecContext(t.Context(), `SELECT pg_advisory_xact_lock(
		hashtextextended('vane.workspace_memory:' || ($1::bigint)::text,0))`, team); err != nil {
		t.Fatal(err)
	}
	recallDone := make(chan error, 1)
	go func() {
		_, recallErr := st.RecallWorkspaceMemories(t.Context(), team, member,
			types.MemoryRecallQuery{Query: "role drift boundary", Limit: 8})
		recallDone <- recallErr
	}()
	migration138AwaitAdvisoryWaiters(t, database, "migration138-workspace-memory", 1)
	if err := lockTx.Rollback(); err != nil {
		t.Fatal(err)
	}
	select {
	case recallErr := <-recallDone:
		if recallErr != nil {
			t.Fatal(recallErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cross-actor recall did not resume after admission release")
	}

	// Mutation proof: a SELECT with its application tenant predicate removed is
	// still exact-tenant under FORCE RLS; append-only tables expose no mutation.
	tx, err := database.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(t.Context(), `SELECT set_config('app.tenant_id',$1,true),
		set_config('app.user_id',$2,true),set_config('app.membership_role','member',true),
		set_config('app.workspace_kind','team',true)`, fmt.Sprint(team), fmt.Sprint(member)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `SET LOCAL ROLE vane_workspace_memory_editor`); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := tx.QueryRowContext(t.Context(), `SELECT count(*) FROM workspace_memory_records`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	var retained int
	if err := database.QueryRowContext(t.Context(), `SELECT count(*)
		FROM workspace_memory_records WHERE tenant_id=$1`, team).Scan(&retained); err != nil {
		t.Fatal(err)
	}
	if count != retained {
		t.Fatalf("tenant-scoped retained records=%d want=%d", count, retained)
	}
	var canUpdate, canDelete bool
	if err := tx.QueryRowContext(t.Context(), `SELECT
		has_table_privilege(current_user,'workspace_memory_records','UPDATE'),
		has_table_privilege(current_user,'workspace_memory_events','DELETE')`).Scan(
		&canUpdate, &canDelete); err != nil {
		t.Fatal(err)
	}
	if canUpdate || canDelete {
		t.Fatalf("append-only authority update=%t delete=%t", canUpdate, canDelete)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	var workspaceRole bool
	if err := database.QueryRowContext(t.Context(), `SELECT EXISTS(
		SELECT 1 FROM pg_auth_members edge
		JOIN pg_roles role ON role.oid=edge.roleid
		JOIN pg_roles member ON member.oid=edge.member
		WHERE role.rolname='vane_workspace_memory_editor'
		  AND member.rolname='vane_server_runtime')`).Scan(
		&workspaceRole); err != nil {
		t.Fatal(err)
	}
	if workspaceRole {
		t.Fatal("plain migration Up granted workspace capability to runtime")
	}
	var policyContract string
	if err := database.QueryRowContext(t.Context(), `SELECT md5(string_agg(
		relation.relname||'|'||policy.polname||'|'||policy.polpermissive::text||'|'||
		policy.polcmd::text||'|'||
		COALESCE(pg_get_expr(policy.polqual,policy.polrelid),'<null>')||'|'||
		COALESCE(pg_get_expr(policy.polwithcheck,policy.polrelid),'<null>'),E'\n'
		ORDER BY relation.relname,policy.polname))
		FROM pg_policy policy JOIN pg_class relation ON relation.oid=policy.polrelid
		WHERE relation.relname LIKE 'workspace_memory_%'`).Scan(&policyContract); err != nil {
		t.Fatal(err)
	}
	if policyContract != "6917d270023b8fb464af8bc03d56ba2f" {
		t.Fatalf("workspace memory policy contract=%s", policyContract)
	}
	if err := ProvisionServerRuntime(t.Context(), scratchURL); err != nil {
		t.Fatalf("provision v138 runtime: %v", err)
	}
	if err := ProvisionServerRuntime(t.Context(), scratchURL); err != nil {
		t.Fatalf("replay provision v138 runtime: %v", err)
	}
	if err := database.QueryRowContext(t.Context(), `SELECT pg_has_role(
		'vane_server_runtime','vane_workspace_memory_editor','MEMBER')`).Scan(
		&workspaceRole); err != nil {
		t.Fatal(err)
	}
	if !workspaceRole {
		t.Fatal("current provisioner omitted workspace memory capability")
	}
	var edgeCount int
	if err := database.QueryRowContext(t.Context(), `SELECT count(*) FROM pg_auth_members edge
		JOIN pg_roles role ON role.oid=edge.roleid
		JOIN pg_roles member ON member.oid=edge.member
		WHERE role.rolname='vane_workspace_memory_editor'
		  AND member.rolname='vane_server_runtime'
		  AND NOT edge.admin_option AND NOT edge.inherit_option AND edge.set_option`).Scan(
		&edgeCount); err != nil {
		t.Fatal(err)
	}
	if edgeCount != 1 {
		t.Fatalf("workspace runtime edge count=%d want=1", edgeCount)
	}

	// Startup validation rejects shared-object authority and any policy
	// replacement, including a superficially valid policy name/role with true
	// predicates. These are mutation proofs for the exact verifier.
	runtimePassword := "migration-138-runtime-password"
	if _, err := database.ExecContext(t.Context(), `ALTER ROLE vane_server_runtime
		LOGIN PASSWORD '`+runtimePassword+`'`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = database.ExecContext(t.Context(),
			`ALTER ROLE vane_server_runtime NOLOGIN PASSWORD NULL`)
	})
	runtimeURL := serverRuntimeTestURL(t, scratchURL, runtimePassword)
	baseline, err := NewServerRuntime(t.Context(), runtimeURL)
	if err != nil {
		t.Fatalf("exact v138 runtime rejected: %v", err)
	}
	baseline.Close()
	if _, err := database.ExecContext(t.Context(), `REVOKE vane_workspace_memory_editor
		FROM vane_server_runtime`); err != nil {
		t.Fatal(err)
	}
	if missing, err := NewServerRuntime(t.Context(), runtimeURL); err == nil {
		missing.Close()
		t.Fatal("runtime accepted missing workspace memory capability")
	} else if !strings.Contains(err.Error(), "memberships differ") {
		t.Fatalf("unexpected missing workspace capability error: %v", err)
	}
	if _, err := database.ExecContext(t.Context(), `GRANT vane_workspace_memory_editor
		TO vane_server_runtime WITH ADMIN FALSE,SET TRUE,INHERIT FALSE`); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), `DO $$ BEGIN EXECUTE format(
		'GRANT CONNECT ON DATABASE %I TO vane_workspace_memory_editor',current_database()); END $$`); err != nil {
		t.Fatal(err)
	}
	if mutated, err := NewServerRuntime(t.Context(), runtimeURL); err == nil {
		mutated.Close()
		t.Fatal("runtime accepted unexpected database CONNECT authority")
	}
	if _, err := database.ExecContext(t.Context(), `DO $$ BEGIN EXECUTE format(
		'REVOKE CONNECT ON DATABASE %I FROM vane_workspace_memory_editor',current_database()); END $$`); err != nil {
		t.Fatal(err)
	}
	if repaired, err := NewServerRuntime(t.Context(), runtimeURL); err != nil {
		t.Fatalf("runtime stayed rejected after CONNECT repair: %v", err)
	} else {
		repaired.Close()
	}
	if _, err := database.ExecContext(t.Context(), `DROP POLICY workspace_memory_record_tenant
		ON workspace_memory_records;
		CREATE POLICY workspace_memory_record_tenant ON workspace_memory_records
		TO vane_workspace_memory_editor USING(true) WITH CHECK(true)`); err != nil {
		t.Fatal(err)
	}
	if mutated, err := NewServerRuntime(t.Context(), runtimeURL); err == nil {
		mutated.Close()
		t.Fatal("runtime accepted USING(true)/WITH CHECK(true) policy mutation")
	}
	if _, err := database.ExecContext(t.Context(),
		`ALTER ROLE vane_server_runtime NOLOGIN PASSWORD NULL`); err != nil {
		t.Fatal(err)
	}
	if err := DeprovisionServerRuntime(t.Context(), scratchURL); err != nil {
		t.Fatalf("deprovision v138 runtime: %v", err)
	}
	dry, err := st.PurgeTenant(t.Context(), team, true)
	if err != nil {
		t.Fatalf("dry purge consumed workspace memory ledger: %v", err)
	}
	for _, table := range []string{"workspace_memory_receipts", "workspace_memory_events",
		"workspace_memory_records", "workspace_memory_authorizations"} {
		if dry.Rows[table] == 0 {
			t.Errorf("dry purge did not exercise %s", table)
		}
	}
	var retainedAfterDry int
	if err := database.QueryRowContext(t.Context(), `SELECT count(*) FROM workspace_memory_events
		WHERE tenant_id=$1`, team).Scan(&retainedAfterDry); err != nil {
		t.Fatal(err)
	}
	if retainedAfterDry == 0 {
		t.Fatal("dry purge committed workspace memory deletion")
	}
	// Deterministic no-deadlock barrier: recall first holds the shared tenant
	// root while waiting for the workspace child lock; purge then waits for the
	// exclusive root. Releasing the child lets recall drain before purge.
	workspaceBarrier, err := database.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := workspaceBarrier.ExecContext(t.Context(), `SELECT pg_advisory_xact_lock(
		hashtextextended('vane.workspace_memory:'||($1::bigint)::text,0))`, team); err != nil {
		t.Fatal(err)
	}
	concurrentRecall := make(chan error, 1)
	go func() {
		_, recallErr := st.RecallWorkspaceMemories(t.Context(), team, member,
			types.MemoryRecallQuery{Query: "role drift boundary", Limit: 8})
		concurrentRecall <- recallErr
	}()
	migration138AwaitAdvisoryWaiters(t, database, "migration138-workspace-memory", 1)
	purgeDone := make(chan struct {
		report *PurgeReport
		err    error
	}, 1)
	go func() {
		report, purgeErr := st.PurgeTenant(t.Context(), team, false)
		purgeDone <- struct {
			report *PurgeReport
			err    error
		}{report: report, err: purgeErr}
	}()
	migration138AwaitAdvisoryWaiters(t, database, "migration138-workspace-memory", 2)
	if err := workspaceBarrier.Rollback(); err != nil {
		t.Fatal(err)
	}
	select {
	case recallErr := <-concurrentRecall:
		if recallErr != nil {
			t.Fatalf("concurrent recall failed: %v", recallErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent recall deadlocked with purge")
	}
	var realPurge *PurgeReport
	select {
	case outcome := <-purgeDone:
		if outcome.err != nil {
			t.Fatalf("real purge consumed workspace memory ledger: %v", outcome.err)
		}
		realPurge = outcome.report
	case <-time.After(5 * time.Second):
		t.Fatal("workspace memory purge deadlocked")
	}
	if realPurge.Rows["workspace_memory_authorizations"] == 0 ||
		realPurge.Rows["workspace_memory_events"] == 0 {
		t.Fatalf("real purge missed consumed authorization cycle: %+v", realPurge.Rows)
	}
}

func migration138ApplicationURL(t *testing.T, databaseURL, application string) string {
	t.Helper()
	parsed, err := url.Parse(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	query := parsed.Query()
	query.Set("application_name", application)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func migration138EnterWorkspaceRole(
	t *testing.T, tx *sql.Tx, tenantID, userID int64, role string,
) {
	t.Helper()
	if _, err := tx.ExecContext(t.Context(), `SELECT
		set_config('app.tenant_id',$1,true),set_config('app.user_id',$2,true),
		set_config('app.membership_role',$3,true),set_config('app.workspace_kind','team',true)`,
		fmt.Sprint(tenantID), fmt.Sprint(userID), role); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `SET LOCAL ROLE vane_workspace_memory_editor`); err != nil {
		t.Fatal(err)
	}
}

func migration138AssertRecordFieldMutationRejected(
	t *testing.T, database *sql.DB, tenantID, actorID int64,
	remembered *types.MemoryActionResult, field string,
) {
	t.Helper()
	tx, err := database.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(t.Context(), `ALTER TABLE workspace_memory_records
		DROP CONSTRAINT uq_workspace_memory_record_authorization`); err != nil {
		t.Fatal(err)
	}
	migration138EnterWorkspaceRole(t, tx, tenantID, actorID, "member")
	sourceID, ownerRequest := remembered.Memory.Evidence.SourceID,
		remembered.Memory.Evidence.OwnerRequest
	authorizationDigest := remembered.Memory.Evidence.AuthorizationDigest
	var supersedes any
	switch field {
	case "source":
		sourceID = uuid.NewString()
	case "owner":
		ownerRequest += " drift"
	case "digest":
		authorizationDigest = strings.Repeat("0", 64)
	default:
		t.Fatalf("unknown record mutation %s", field)
	}
	if _, err := tx.ExecContext(t.Context(), `INSERT INTO workspace_memory_records(
		tenant_id,creator_user_id,created_by_user_id,memory_text,evidence_source_type,
		evidence_source_id,authorization_id,owner_request,authorization_digest,supersedes_memory_id)
		VALUES($1,$2,$2,$3,$4,$5,$6,$7,$8,$9)`, tenantID, actorID,
		remembered.Memory.Text, remembered.Memory.Evidence.SourceType, sourceID,
		remembered.Memory.Evidence.AuthorizationID, ownerRequest, authorizationDigest,
		supersedes); err == nil {
		t.Fatalf("record RLS accepted single-field %s mutation", field)
	}
}

func migration138AssertEventFieldMutationRejected(
	t *testing.T, database *sql.DB, tenantID, actorID int64,
	remembered *types.MemoryActionResult, field string,
) {
	t.Helper()
	tx, err := database.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for _, statement := range []string{
		`ALTER TABLE workspace_memory_authorizations
		 DROP CONSTRAINT fk_workspace_memory_authorization_consumed`,
		`ALTER TABLE workspace_memory_records
		 DROP CONSTRAINT uq_workspace_memory_record_authorization`,
		`ALTER TABLE workspace_memory_events
		 DROP CONSTRAINT uq_workspace_memory_event_authorization`,
		`ALTER TABLE workspace_memory_events
		 DROP CONSTRAINT uq_workspace_memory_event_consumption_binding`,
	} {
		if _, err := tx.ExecContext(t.Context(), statement); err != nil {
			t.Fatal(err)
		}
	}
	migration138EnterWorkspaceRole(t, tx, tenantID, actorID, "member")
	var resultID int64
	if err := tx.QueryRowContext(t.Context(), `INSERT INTO workspace_memory_records(
		tenant_id,creator_user_id,created_by_user_id,memory_text,evidence_source_type,
		evidence_source_id,authorization_id,owner_request,authorization_digest,supersedes_memory_id)
		VALUES($1,$2,$2,$3,$4,$5,$6,$7,$8,NULL) RETURNING id`, tenantID, actorID,
		remembered.Memory.Text, remembered.Memory.Evidence.SourceType,
		remembered.Memory.Evidence.SourceID, remembered.Memory.Evidence.AuthorizationID,
		remembered.Memory.Evidence.OwnerRequest,
		remembered.Memory.Evidence.AuthorizationDigest).Scan(&resultID); err != nil {
		t.Fatal(err)
	}
	action := types.MemoryActionRemember
	var target any
	sourceID, ownerRequest := remembered.Memory.Evidence.SourceID,
		remembered.Memory.Evidence.OwnerRequest
	authorizationDigest := remembered.Memory.Evidence.AuthorizationDigest
	switch field {
	case "source":
		sourceID = uuid.NewString()
	case "owner":
		ownerRequest += " drift"
	case "digest":
		authorizationDigest = strings.Repeat("0", 64)
	default:
		t.Fatalf("unknown event mutation %s", field)
	}
	if _, err := tx.ExecContext(t.Context(), `INSERT INTO workspace_memory_events(
		tenant_id,actor_user_id,actor_role,event_kind,target_memory_id,result_memory_id,
		evidence_source_type,evidence_source_id,authorization_id,owner_request,authorization_digest)
		VALUES($1,$2,'member',$3,$4,$5,$6,$7,$8,$9,$10)`, tenantID, actorID,
		action, target, resultID, remembered.Memory.Evidence.SourceType, sourceID,
		remembered.Memory.Evidence.AuthorizationID, ownerRequest, authorizationDigest); err == nil {
		t.Fatalf("event RLS accepted single-field %s mutation", field)
	}
}

func migration138AssertCorrectRecordBindingRejected(
	t *testing.T, database *sql.DB, tenantID, actorID int64,
	target, alternate *types.MemoryActionResult, action types.MemoryAction, field string,
) {
	t.Helper()
	tx, err := database.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	switch field {
	case "action":
		if _, err := tx.ExecContext(t.Context(), `ALTER TABLE workspace_memory_authorizations
			DROP CONSTRAINT ck_workspace_memory_authorization_action_shape`); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(t.Context(), `UPDATE workspace_memory_authorizations
			SET action_kind='remember' WHERE tenant_id=$1 AND id=$2`, tenantID,
			action.Evidence.AuthorizationID); err != nil {
			t.Fatal(err)
		}
	case "target":
		if _, err := tx.ExecContext(t.Context(), `UPDATE workspace_memory_authorizations
			SET target_memory_id=$3,target_creator_user_id=$2
			WHERE tenant_id=$1 AND id=$4`, tenantID, actorID, alternate.Memory.ID,
			action.Evidence.AuthorizationID); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("unknown correct record binding %s", field)
	}
	migration138EnterWorkspaceRole(t, tx, tenantID, actorID, "member")
	if _, err := tx.ExecContext(t.Context(), `INSERT INTO workspace_memory_records(
		tenant_id,creator_user_id,created_by_user_id,memory_text,evidence_source_type,
		evidence_source_id,authorization_id,owner_request,authorization_digest,supersedes_memory_id)
		VALUES($1,$2,$2,$3,$4,$5,$6,$7,$8,$9)`, tenantID, actorID, action.Text,
		action.Evidence.SourceType, action.Evidence.SourceID, action.Evidence.AuthorizationID,
		action.Evidence.OwnerRequest, action.Evidence.AuthorizationDigest, target.Memory.ID); err == nil {
		t.Fatalf("record RLS accepted isolated correct %s mutation", field)
	}
}

func migration138AssertCorrectEventBindingRejected(
	t *testing.T, database *sql.DB, tenantID, actorID int64,
	target, alternate *types.MemoryActionResult, action types.MemoryAction, field string,
) {
	t.Helper()
	tx, err := database.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var supersedes any
	eventKind := types.MemoryActionCorrect
	eventTarget := target.Memory.ID
	switch field {
	case "action":
		if _, err := tx.ExecContext(t.Context(), `ALTER TABLE workspace_memory_events
			DROP CONSTRAINT ck_workspace_memory_event_shape`); err != nil {
			t.Fatal(err)
		}
		eventKind = types.MemoryActionRemember
	case "target":
		supersedes = alternate.Memory.ID
		eventTarget = alternate.Memory.ID
	default:
		t.Fatalf("unknown correct event binding %s", field)
	}
	var resultID int64
	if err := tx.QueryRowContext(t.Context(), `INSERT INTO workspace_memory_records(
		tenant_id,creator_user_id,created_by_user_id,memory_text,evidence_source_type,
		evidence_source_id,authorization_id,owner_request,authorization_digest,supersedes_memory_id)
		VALUES($1,$2,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING id`, tenantID, actorID,
		action.Text, action.Evidence.SourceType, action.Evidence.SourceID,
		action.Evidence.AuthorizationID, action.Evidence.OwnerRequest,
		action.Evidence.AuthorizationDigest, supersedes).Scan(&resultID); err != nil {
		t.Fatal(err)
	}
	migration138EnterWorkspaceRole(t, tx, tenantID, actorID, "member")
	if _, err := tx.ExecContext(t.Context(), `INSERT INTO workspace_memory_events(
		tenant_id,actor_user_id,actor_role,event_kind,target_memory_id,result_memory_id,
		evidence_source_type,evidence_source_id,authorization_id,owner_request,authorization_digest)
		VALUES($1,$2,'member',$3,$4,$5,$6,$7,$8,$9,$10)`, tenantID, actorID,
		eventKind, eventTarget, resultID, action.Evidence.SourceType,
		action.Evidence.SourceID, action.Evidence.AuthorizationID,
		action.Evidence.OwnerRequest, action.Evidence.AuthorizationDigest); err == nil {
		t.Fatalf("event RLS accepted isolated correct %s mutation", field)
	}
}

func migration138AwaitAdvisoryWaiters(
	t *testing.T, database *sql.DB, application string, want int,
) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var waiters int
		err := database.QueryRowContext(t.Context(), `SELECT count(*) FROM pg_locks lock
			JOIN pg_stat_activity activity ON activity.pid=lock.pid
			WHERE lock.locktype='advisory' AND NOT lock.granted
			  AND activity.datname=current_database() AND activity.application_name=$1`,
			application).Scan(&waiters)
		if err != nil {
			t.Fatal(err)
		}
		if waiters >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("did not observe %d ungranted advisory locks for %s", want, application)
}

func migration138Apply(t *testing.T, st *Store, tenantID, actorID, sessionID int64,
	key string, action types.MemoryAction) *types.MemoryActionResult {
	t.Helper()
	authorizationID, err := st.PrepareWorkspaceMemoryAuthorization(
		t.Context(), tenantID, actorID, sessionID, action)
	if err != nil {
		t.Fatal(err)
	}
	action.Evidence.AuthorizationID = authorizationID
	result, err := st.ApplyWorkspaceMemoryAction(
		t.Context(), tenantID, actorID, key, action)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func migration138Action(kind string, memoryID int64, text string) types.MemoryAction {
	return types.MemoryAction{Action: kind, MemoryID: memoryID, Text: text,
		Evidence: types.MemoryEvidence{
			SourceType: types.MemoryEvidenceOwnerExplicitAgentTurn,
			SourceID:   uuid.NewString(), OwnerRequest: "明确写入团队记忆",
			AuthorizationDigest: strings.Repeat("a", 64),
		}}
}

func migration138User(t *testing.T, database *sql.DB, suffix string) int64 {
	t.Helper()
	var id int64
	if err := database.QueryRowContext(t.Context(), `INSERT INTO users(
		feishu_open_id,name) VALUES($1,$1) RETURNING id`,
		"migration-138-"+suffix+"-"+uuid.NewString()).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

func migration138Workspace(t *testing.T, database *sql.DB, name, kind string,
	personalOwner *int64) int64 {
	t.Helper()
	var id int64
	seatLimit := 5
	if kind == "personal" {
		seatLimit = 1
	}
	if err := database.QueryRowContext(t.Context(), `INSERT INTO tenants(
		status,plan,display_name,workspace_kind,personal_owner_user_id,seat_limit)
		VALUES('active','free',$1,$2,$3,$4) RETURNING id`, name, kind,
		personalOwner, seatLimit).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}
