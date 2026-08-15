package store

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/server/internal/testgate"
	"github.com/YouToco/vane/server/types"
)

func TestMigration140ScopedA2ATaskContract(t *testing.T) {
	payload, err := migrationsFS.ReadFile("migrations/140_scoped_a2a_principal_tasks.sql")
	if err != nil {
		t.Fatal(err)
	}
	sqlText := string(payload)
	for _, want := range []string{"CREATE TABLE a2a_principal_tasks", "FORCE ROW LEVEL SECURITY",
		"tenant_id,principal_user_id,id", "app.a2a_token_id", "created_by_token_id",
		"task#>>'{status,state}'=status", "task->>'contextId'=context_id",
		"refusing to drop retained principal-scoped A2A tasks"} {
		if !strings.Contains(sqlText, want) {
			t.Errorf("migration 140 missing %q", want)
		}
	}
	for _, forbidden := range []string{"INSERT INTO a2a_tasks", "UPDATE a2a_tasks", "DELETE FROM a2a_tasks"} {
		if strings.Contains(sqlText, forbidden) {
			t.Errorf("migration 140 must not invent authority for legacy global tasks: %q", forbidden)
		}
	}
}

func TestMigration140ScopedA2ATaskIsolationPostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		testgate.Database(t)
	}
	database, provider, scratchURL, drop := migration128Scratch(t, databaseURL)
	t.Cleanup(drop)
	if _, err := provider.UpTo(t.Context(), 140); err != nil {
		t.Fatal(err)
	}

	userA := migration138User(t, database, "a2a140-a")
	userB := migration138User(t, database, "a2a140-b")
	teamA := migration138Workspace(t, database, "a2a140-team-a", "team", nil)
	teamB := migration138Workspace(t, database, "a2a140-team-b", "team", nil)
	for _, row := range []struct{ tenant, user int64 }{
		{teamA, userA}, {teamA, userB}, {teamB, userA},
	} {
		if _, err := database.ExecContext(t.Context(), `INSERT INTO memberships(tenant_id,user_id,role)
			VALUES($1,$2,'member')`, row.tenant, row.user); err != nil {
			t.Fatal(err)
		}
	}
	st, err := New(t.Context(), migration138ApplicationURL(t, scratchURL, "migration140-a2a"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	issue := func(tenant, user int64, marker byte, identity string) types.A2AExecutionScope {
		token := migration139Issue(t, st, types.IssueA2AAccessToken{
			TenantID: tenant, ActorUserID: user, PrincipalUserID: user,
			ActorType: types.ActorTypeUser,
			Scopes:    []types.A2AScope{types.A2AScopeAssistantChat, types.A2AScopeContentQuery},
			TokenHash: bytes.Repeat([]byte{marker}, 32), ExpiresAt: time.Now().Add(time.Hour),
		}, identity)
		return types.A2AExecutionScope{TokenID: token.ID, TenantID: tenant, UserID: user,
			Role: types.MembershipRoleMember, ActorType: types.ActorTypeUser,
			Scopes: append([]types.A2AScope(nil), token.Scopes...)}
	}
	scopeA := issue(teamA, userA, 0x41, "scope-a")
	scopeB := issue(teamA, userB, 0x42, "scope-b")
	scopeOther := issue(teamB, userA, 0x43, "scope-other")

	taskPayload := func(id, contextID, status string) json.RawMessage {
		payload, err := json.Marshal(map[string]any{
			"id": id, "contextId": contextID,
			"status": map[string]any{"state": status},
		})
		if err != nil {
			t.Fatal(err)
		}
		return payload
	}
	for _, scope := range []types.A2AExecutionScope{scopeA, scopeB, scopeOther} {
		task := &types.A2ATask{ID: "same", ContextID: "same-context",
			Status: "TASK_STATE_SUBMITTED",
			Task:   taskPayload("same", "same-context", "TASK_STATE_SUBMITTED")}
		if err := st.CreateA2APrincipalTask(t.Context(), scope, task); err != nil {
			t.Fatal(err)
		}
	}
	if count, err := st.CountA2ATasks(t.Context()); err != nil || count != 3 {
		t.Fatalf("principal task readiness count=%d err=%v", count, err)
	}
	if _, err := st.GetA2APrincipalTask(t.Context(), scopeA, "same"); err != nil {
		t.Fatal(err)
	}
	privateTask := &types.A2ATask{ID: "a-only", ContextID: "private-context",
		Status: "TASK_STATE_SUBMITTED",
		Task:   taskPayload("a-only", "private-context", "TASK_STATE_SUBMITTED")}
	if err := st.CreateA2APrincipalTask(t.Context(), scopeA, privateTask); err != nil {
		t.Fatal(err)
	}
	for _, scope := range []types.A2AExecutionScope{scopeB, scopeOther} {
		if _, err := st.GetA2APrincipalTask(t.Context(), scope, "a-only"); types.CodeOf(err) != types.CodeNotFound {
			t.Fatalf("cross-principal task read code=%s err=%v", types.CodeOf(err), err)
		}
		items, _, _, err := st.ListA2APrincipalTasks(t.Context(), scope,
			types.A2ATaskQuery{ContextID: "private-context"})
		if err != nil || len(items) != 0 {
			t.Fatalf("cross-principal history leaked items=%d err=%v", len(items), err)
		}
	}
	working := &types.A2ATask{ID: "stale-working", ContextID: "stale-context",
		Status: "TASK_STATE_WORKING",
		Task:   taskPayload("stale-working", "stale-context", "TASK_STATE_WORKING")}
	if err := st.CreateA2APrincipalTask(t.Context(), scopeA, working); err != nil {
		t.Fatal(err)
	}
	ancient := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	if _, err := database.ExecContext(t.Context(), `UPDATE a2a_principal_tasks
		SET updated_at=$1 WHERE tenant_id=$2 AND principal_user_id=$3 AND id=$4`,
		ancient, teamA, userA, working.ID); err != nil {
		t.Fatal(err)
	}
	if count, err := st.FailStaleA2APrincipalTasks(t.Context(), ancient.Add(time.Hour)); err != nil || count != 1 {
		t.Fatalf("stale scoped task cleanup count=%d err=%v", count, err)
	}
	sealed, err := st.GetA2APrincipalTask(t.Context(), scopeA, working.ID)
	if err != nil {
		t.Fatal(err)
	}
	var sealedPayload struct {
		Status struct {
			State     string     `json:"state"`
			Timestamp *time.Time `json:"timestamp"`
		} `json:"status"`
	}
	if err := json.Unmarshal(sealed.Task, &sealedPayload); err != nil ||
		sealed.Status != "TASK_STATE_FAILED" || sealedPayload.Status.State != "TASK_STATE_FAILED" ||
		sealedPayload.Status.Timestamp == nil {
		t.Fatalf("stale task did not seal both projections row=%+v payload=%+v err=%v",
			sealed, sealedPayload, err)
	}

	// Content facts stay global, but only task-bound fetch targets in the exact
	// workspace can authorize discovery.
	makeContent := func(tenant, user int64, suffix string) int64 {
		var targetID, contentID int64
		url := "https://example.invalid/" + suffix + "/" + uuid.NewString()
		if err := database.QueryRowContext(t.Context(), `INSERT INTO fetch_targets(
			platform,capability,url,title,config,status)
			VALUES('web','search',$1,$2,'{}','active') RETURNING id`, url, suffix).Scan(&targetID); err != nil {
			t.Fatal(err)
		}
		taskID := "a2a140-task-" + suffix + "-" + uuid.NewString()
		if _, err := database.ExecContext(t.Context(), `INSERT INTO schedules(
			id,tenant_id,user_id,nl_description,status) VALUES($1,$2,$3,$4,'paused')`,
			taskID, tenant, user, suffix); err != nil {
			t.Fatal(err)
		}
		if _, err := database.ExecContext(t.Context(), `INSERT INTO task_fetch_targets(
			schedule_id,fetch_target_id) VALUES($1,$2)`, taskID, targetID); err != nil {
			t.Fatal(err)
		}
		if err := database.QueryRowContext(t.Context(), `INSERT INTO content_items(
			source_id,external_id,canonical_key,url,title,content,content_hash,kind)
			VALUES($1,$2,$3,$4,$5,$5,$3,'article') RETURNING id`, targetID,
			uuid.NewString(), "canonical-"+uuid.NewString(), url, suffix).Scan(&contentID); err != nil {
			t.Fatal(err)
		}
		if _, err := database.ExecContext(t.Context(), `INSERT INTO content_sources(
			content_item_id,source_id,external_id,url) VALUES($1,$2,$3,$4)`,
			contentID, targetID, uuid.NewString(), url); err != nil {
			t.Fatal(err)
		}
		return contentID
	}
	wantA := makeContent(teamA, userA, "visible-a")
	_ = makeContent(teamB, userA, "hidden-b")
	items, err := st.SearchContentItemsForA2A(t.Context(), scopeA, "visible", time.Now().Add(-time.Hour), 20)
	if err != nil || len(items) != 1 || items[0].ID != wantA {
		t.Fatalf("workspace content projection=%+v err=%v", items, err)
	}
	items, err = st.SearchContentItemsForA2A(t.Context(), scopeOther, "visible-a", time.Now().Add(-time.Hour), 20)
	if err != nil || len(items) != 0 {
		t.Fatalf("cross-workspace content leaked=%+v err=%v", items, err)
	}
	forgedScope := scopeA
	forgedScope.Scopes = []types.A2AScope{types.A2AScopeContentQuery}
	if _, err := st.GetA2APrincipalTask(t.Context(), forgedScope, "same"); types.CodeOf(err) != types.CodeForbidden {
		t.Fatalf("forged scope drift code=%s err=%v", types.CodeOf(err), err)
	}
	forgedRole := scopeA
	forgedRole.Role = types.MembershipRoleOwner
	if _, err := st.GetA2APrincipalTask(t.Context(), forgedRole, "same"); types.CodeOf(err) != types.CodeForbidden {
		t.Fatalf("forged role drift code=%s err=%v", types.CodeOf(err), err)
	}
	if _, err := database.ExecContext(t.Context(), `INSERT INTO a2a_principal_tasks(
		tenant_id,principal_user_id,actor_type,created_by_token_id,id,context_id,status,task)
		VALUES($1,$2,$3,$4,'torn-projection','torn-context','TASK_STATE_COMPLETED',$5)`,
		scopeA.TenantID, scopeA.UserID, scopeA.ActorType, scopeA.TokenID,
		taskPayload("torn-projection", "torn-context", "TASK_STATE_WORKING")); err == nil || !postgresCodeIs(err, "23514") {
		t.Fatalf("torn task projection must fail ck_a2a_principal_task_projection_v140: %v", err)
	}

	if err := st.RevokeA2AAccessToken(t.Context(), teamA, userA, scopeA.TokenID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.GetA2APrincipalTask(t.Context(), scopeA, "same"); types.CodeOf(err) != types.CodeForbidden {
		t.Fatalf("revoked credential read code=%s err=%v", types.CodeOf(err), err)
	}
	if _, err := database.ExecContext(t.Context(), `DELETE FROM memberships WHERE tenant_id=$1 AND user_id=$2`,
		teamA, userB); err != nil {
		t.Fatal(err)
	}
	if _, err := st.GetA2APrincipalTask(t.Context(), scopeB, "same"); types.CodeOf(err) != types.CodeForbidden {
		t.Fatalf("removed member read code=%s err=%v", types.CodeOf(err), err)
	}

	// Explicit workspace erasure must remove the child task before its durable
	// credential. The dry run executes the real DELETE order and rolls back.
	dryReport, err := st.PurgeTenant(t.Context(), teamB, true)
	if err != nil {
		t.Fatalf("dry-run purge scoped A2A workspace: %v", err)
	}
	if dryReport.Rows["a2a_principal_tasks"] != 1 || dryReport.Rows["a2a_access_tokens"] != 1 {
		t.Fatalf("dry-run purge omitted scoped A2A rows: %+v", dryReport.Rows)
	}
	report, err := st.PurgeTenant(t.Context(), teamB, false)
	if err != nil {
		t.Fatalf("purge scoped A2A workspace: %v", err)
	}
	if report.Rows["a2a_principal_tasks"] != 1 || report.Rows["a2a_access_tokens"] != 1 {
		t.Fatalf("purge omitted scoped A2A rows: %+v", report.Rows)
	}
	var retainedTasks, retainedTokens, retainedTenant int64
	if err := database.QueryRowContext(t.Context(), `SELECT
		(SELECT count(*) FROM a2a_principal_tasks WHERE tenant_id=$1),
		(SELECT count(*) FROM a2a_access_tokens WHERE tenant_id=$1),
		(SELECT count(*) FROM tenants WHERE id=$1)`, teamB).Scan(
		&retainedTasks, &retainedTokens, &retainedTenant); err != nil {
		t.Fatal(err)
	}
	if retainedTasks != 0 || retainedTokens != 0 || retainedTenant != 0 {
		t.Fatalf("workspace purge retained tasks=%d tokens=%d tenant=%d",
			retainedTasks, retainedTokens, retainedTenant)
	}
	if _, err := provider.DownTo(t.Context(), 139); err == nil || !postgresCodeIs(err, "55000") {
		t.Fatalf("migration140 Down must refuse retained principal task history: %v", err)
	}
}
