package store

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/YouToco/vane/server/internal/testgate"
	"github.com/YouToco/vane/server/types"
)

func TestMigration136TeamTaskIsolationContract(t *testing.T) {
	payload, err := migrationsFS.ReadFile("migrations/136_team_task_isolation.sql")
	if err != nil {
		t.Fatal(err)
	}
	sqlText := string(payload)
	for _, fragment := range []string{
		"CREATE TABLE task_workspace_access", "REFERENCES schedules(tenant_id,user_id,id)",
		"ALTER TABLE task_workspace_access FORCE ROW LEVEL SECURITY",
		"CREATE TABLE task_access_audit_events",
		"ALTER TABLE task_access_audit_events FORCE ROW LEVEL SECURITY",
		"execution_user_id", "task.assignee_changed",
		"VALUES(NEW.tenant_id,NEW.user_id,NEW.id,NEW.user_id,NEW.user_id",
		"REVOKE ALL ON FUNCTION schedule_workspace_access_defaults_v1() FROM PUBLIC",
		"GRANT UPDATE (assignee_user_id,updated_at) ON task_workspace_access TO vane_app",
	} {
		if !strings.Contains(sqlText, fragment) {
			t.Errorf("migration 136 missing %q", fragment)
		}
	}
	for _, forbidden := range []string{
		"SET user_id=$3", "UPDATE schedules SET user_id",
		"ALTER TABLE schedules", "ON UPDATE CASCADE", "GRANT UPDATE (user_id",
	} {
		if strings.Contains(sqlText, forbidden) {
			t.Errorf("migration 136 can rewrite frozen execution identity: %q", forbidden)
		}
	}
}

func TestMigration136TeamTaskAuthorizationAndFrozenExecutionIdentityPostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		testgate.Database(t)
	}
	database, provider, scratchURL, drop := migration128Scratch(t, databaseURL)
	t.Cleanup(drop)
	if _, err := provider.UpTo(t.Context(), 135); err != nil {
		t.Fatal(err)
	}

	creator := migration136User(t, database, "creator")
	admin := migration136User(t, database, "admin")
	member := migration136User(t, database, "member")
	outsider := migration136User(t, database, "outsider")
	tenant := migration136Team(t, database, "team")
	otherTenant := migration136Team(t, database, "other")
	for _, membership := range []struct {
		tenant, user int64
		role         string
	}{
		{tenant, creator, "member"}, {tenant, admin, "admin"},
		{tenant, member, "member"}, {otherTenant, outsider, "owner"},
	} {
		if _, err := database.ExecContext(t.Context(), `
			INSERT INTO memberships(tenant_id,user_id,role) VALUES($1,$2,$3)`,
			membership.tenant, membership.user, membership.role); err != nil {
			t.Fatal(err)
		}
	}

	// Insert before 136 proves the compatibility backfill is deterministic.
	legacyTask := "migration-136-legacy-" + uuid.NewString()
	if _, err := database.ExecContext(t.Context(), `
		INSERT INTO schedules(id,tenant_id,user_id,nl_description,status)
		VALUES($1,$2,$3,'legacy','active')`, legacyTask, tenant, creator); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.UpTo(t.Context(), 136); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(),
		`SELECT * FROM assert_agent_first_legacy_write_fence_v132()`); err != nil {
		t.Fatalf("migration 136 drifted the frozen schedules descriptor: %v", err)
	}
	var publicCanExecute bool
	if err := database.QueryRowContext(t.Context(), `
		SELECT EXISTS (
			SELECT 1
			  FROM pg_proc p,
			       LATERAL aclexplode(
			         COALESCE(p.proacl,acldefault('f',p.proowner))
			       ) acl
			 WHERE p.oid='schedule_workspace_access_defaults_v1()'::regprocedure
			   AND acl.grantee=0 AND acl.privilege_type='EXECUTE'
		)`).Scan(&publicCanExecute); err != nil {
		t.Fatal(err)
	}
	if publicCanExecute {
		t.Fatal("schedule identity SECURITY DEFINER function is executable by PUBLIC")
	}
	var executionID, creatorID, assigneeID int64
	var visibility string
	if err := database.QueryRowContext(t.Context(), `
		SELECT s.user_id,a.creator_user_id,a.assignee_user_id,a.task_visibility
		FROM schedules s JOIN task_workspace_access a
		  ON a.tenant_id=s.tenant_id AND a.execution_user_id=s.user_id
		 AND a.schedule_id=s.id WHERE s.id=$1`, legacyTask).Scan(
		&executionID, &creatorID, &assigneeID, &visibility); err != nil {
		t.Fatal(err)
	}
	if executionID != creator || creatorID != creator || assigneeID != creator ||
		visibility != "workspace" {
		t.Fatalf("backfill execution=%d creator=%d assignee=%d visibility=%q",
			executionID, creatorID, assigneeID, visibility)
	}

	st, err := New(t.Context(), scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	if _, err := st.PurgeTenant(t.Context(), 0, true); types.CodeOf(err) != types.CodeValidation {
		t.Fatalf("invalid tenant purge code=%s err=%v", types.CodeOf(err), err)
	}

	visible, err := st.ListSchedulesForMember(t.Context(), tenant, member)
	if err != nil {
		t.Fatal(err)
	}
	if len(visible) != 1 || visible[0].ID != legacyTask ||
		visible[0].UserID != creator || visible[0].AssigneeUserID != creator {
		t.Fatalf("member visibility/frozen projection=%+v", visible)
	}
	if _, err := st.GetScheduleForMember(
		t.Context(), otherTenant, outsider, legacyTask); types.CodeOf(err) != types.CodeNotFound {
		t.Fatalf("cross-tenant task read code=%s err=%v", types.CodeOf(err), err)
	}
	readBack, err := st.GetScheduleForMember(t.Context(), tenant, member, legacyTask)
	if err != nil {
		t.Fatalf("member task read: %v", err)
	}
	if readBack.ID != legacyTask || readBack.UserID != creator {
		t.Fatalf("member task read projection=%+v", readBack)
	}
	if _, err := st.GetScheduleForMember(
		t.Context(), tenant, member, " invalid "); types.CodeOf(err) != types.CodeValidation {
		t.Fatalf("invalid task read code=%s err=%v", types.CodeOf(err), err)
	}
	if _, err := st.ListSchedulesForMember(
		t.Context(), 0, member); types.CodeOf(err) != types.CodeValidation {
		t.Fatalf("invalid workspace list code=%s err=%v", types.CodeOf(err), err)
	}

	// Ordinary members can view shared results but cannot mutate a task they
	// did not create. Current DB role, not a cached session role, decides.
	if _, err := st.AuthorizeScheduleMutation(t.Context(), tenant, member,
		legacyTask, TaskMutationRun); types.CodeOf(err) != types.CodeForbidden {
		t.Fatalf("non-creator member mutation code=%s err=%v", types.CodeOf(err), err)
	}
	if _, err := st.AuthorizeScheduleMutation(t.Context(), tenant, creator,
		legacyTask, TaskMutation("invalid")); types.CodeOf(err) != types.CodeValidation {
		t.Fatalf("invalid task mutation code=%s err=%v", types.CodeOf(err), err)
	}
	if _, err := st.AuthorizeScheduleMutation(t.Context(), tenant, admin,
		"missing-"+uuid.NewString(), TaskMutationRun); types.CodeOf(err) != types.CodeNotFound {
		t.Fatalf("missing task mutation code=%s err=%v", types.CodeOf(err), err)
	}
	if _, err := st.AuthorizeScheduleMutation(t.Context(), tenant, creator,
		legacyTask, TaskMutationRun); err != nil {
		t.Fatalf("creator mutation: %v", err)
	}
	if _, err := st.AuthorizeScheduleMutation(t.Context(), tenant, admin,
		legacyTask, TaskMutationPause); err != nil {
		t.Fatalf("admin mutation: %v", err)
	}
	transferred, err := st.TransferScheduleAssignee(
		t.Context(), tenant, admin, legacyTask, member)
	if err != nil {
		t.Fatal(err)
	}
	if transferred.UserID != creator || transferred.CreatorUserID != creator ||
		transferred.AssigneeUserID != member {
		t.Fatalf("transfer rewrote frozen identity: %+v", transferred)
	}
	if _, err := st.TransferScheduleAssignee(
		t.Context(), tenant, member, legacyTask, creator); types.CodeOf(err) != types.CodeForbidden {
		t.Fatalf("member transfer code=%s err=%v", types.CodeOf(err), err)
	}
	if _, err := st.TransferScheduleAssignee(
		t.Context(), tenant, admin, legacyTask, outsider); types.CodeOf(err) != types.CodeNotFound {
		t.Fatalf("foreign assignee code=%s err=%v", types.CodeOf(err), err)
	}
	if _, err := st.TransferScheduleAssignee(
		t.Context(), tenant, admin, legacyTask, member); types.CodeOf(err) != types.CodeConflict {
		t.Fatalf("same assignee code=%s err=%v", types.CodeOf(err), err)
	}
	if _, err := st.TransferScheduleAssignee(
		t.Context(), tenant, admin, "missing-"+uuid.NewString(), member); types.CodeOf(err) != types.CodeNotFound {
		t.Fatalf("missing transfer task code=%s err=%v", types.CodeOf(err), err)
	}
	if _, err := st.TransferScheduleAssignee(
		t.Context(), tenant, admin, legacyTask, 0); types.CodeOf(err) != types.CodeValidation {
		t.Fatalf("invalid transfer target code=%s err=%v", types.CodeOf(err), err)
	}
	if err := database.QueryRowContext(t.Context(), `
		SELECT s.user_id,a.creator_user_id,a.assignee_user_id
		FROM schedules s JOIN task_workspace_access a
		  ON a.tenant_id=s.tenant_id AND a.execution_user_id=s.user_id
		 AND a.schedule_id=s.id WHERE s.id=$1`, legacyTask).Scan(
		&executionID, &creatorID, &assigneeID); err != nil {
		t.Fatal(err)
	}
	if executionID != creator || creatorID != creator || assigneeID != member {
		t.Fatalf("persisted identity execution=%d creator=%d assignee=%d",
			executionID, creatorID, assigneeID)
	}

	// Mutation proof: even a query with its tenant predicate deleted remains
	// tenant-scoped by RLS, and the append-only table exposes no UPDATE/DELETE.
	tx, err := database.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(t.Context(), `
		SELECT set_config('app.tenant_id',$1,true),set_config('app.user_id',$2,true)`,
		fmt.Sprint(tenant), fmt.Sprint(admin)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `SET LOCAL ROLE vane_app`); err != nil {
		t.Fatal(err)
	}
	var auditCount int
	if err := tx.QueryRowContext(t.Context(), `
		SELECT count(*) FROM task_access_audit_events`).Scan(&auditCount); err != nil {
		t.Fatal(err)
	}
	if auditCount != 3 {
		t.Fatalf("tenant-scoped audit count=%d want=3", auditCount)
	}
	var canUpdate, canDelete bool
	if err := tx.QueryRowContext(t.Context(), `
		SELECT has_table_privilege(current_user,'task_access_audit_events','UPDATE'),
		       has_table_privilege(current_user,'task_access_audit_events','DELETE')`).Scan(
		&canUpdate, &canDelete); err != nil {
		t.Fatal(err)
	}
	if canUpdate || canDelete {
		t.Fatalf("append-only audit update=%t delete=%t", canUpdate, canDelete)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	dryRun, err := st.PurgeTenant(t.Context(), tenant, true)
	if err != nil {
		t.Fatalf("dry-run team task purge: %v", err)
	}
	if dryRun.Rows["task_workspace_access"] != 1 ||
		dryRun.Rows["task_access_audit_events"] != 3 {
		t.Fatalf("team task purge report=%+v", dryRun.Rows)
	}
	if _, err := st.PurgeTenant(t.Context(), tenant, false); err != nil {
		t.Fatalf("team task purge: %v", err)
	}
	var retained int
	if err := database.QueryRowContext(t.Context(), `
		SELECT count(*) FROM task_workspace_access WHERE tenant_id=$1`, tenant).Scan(&retained); err != nil {
		t.Fatal(err)
	}
	if retained != 0 {
		t.Fatalf("task workspace access retained after purge: %d", retained)
	}
}

func migration136User(t *testing.T, database *sql.DB, suffix string) int64 {
	t.Helper()
	var id int64
	if err := database.QueryRowContext(t.Context(), `
		INSERT INTO users(feishu_open_id,name) VALUES($1,$1) RETURNING id`,
		"migration-136-"+suffix+"-"+uuid.NewString()).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

func migration136Team(t *testing.T, database *sql.DB, name string) int64 {
	t.Helper()
	var id int64
	if err := database.QueryRowContext(t.Context(), `
		INSERT INTO tenants(status,plan,display_name,workspace_kind,seat_limit)
		VALUES('active','free',$1,'team',5) RETURNING id`, name).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}
