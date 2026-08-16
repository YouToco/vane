package store

import (
	"database/sql"
	"os"
	"strings"
	"testing"

	"github.com/YouToco/vane/server/internal/testgate"
	"github.com/YouToco/vane/server/types"
)

func TestMigration151WorkspaceMemoryRuntimeActivationContract(t *testing.T) {
	migration, err := os.ReadFile("migrations/151_workspace_memory_runtime_activation.sql")
	if err != nil {
		t.Fatal(err)
	}
	source := string(migration)
	for _, required := range []string{
		"assert_vane_workspace_memory_editor_v151",
		"provision_vane_server_runtime_v151",
		"deprovision_vane_server_runtime_v151",
		"CREATE OR REPLACE FUNCTION provision_vane_server_runtime_v129",
		"CREATE OR REPLACE FUNCTION deprovision_vane_server_runtime_v129",
		"relforcerowsecurity", "6917d270023b8fb464af8bc03d56ba2f",
		"required_acl_count<>14", "unexpected_authority_count<>0",
		"workspace_was_member", "provision_vane_server_runtime_v128",
	} {
		if !strings.Contains(source, required) {
			t.Errorf("migration 151 omitted %q", required)
		}
	}
	if !strings.Contains(source, "deprovision vane_server_runtime before schema downgrade") {
		t.Error("migration 151 Down can destroy active runtime wrappers")
	}
}

func TestMigration151WorkspaceMemoryRuntimeActivationPostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		testgate.Database(t)
	}
	database, provider, scratchURL, drop := migration128Scratch(t, databaseURL)
	t.Cleanup(drop)
	if _, err := provider.UpTo(t.Context(), 150); err != nil {
		t.Fatal(err)
	}
	loadBridgeContract := func() string {
		t.Helper()
		var contract string
		if err := database.QueryRowContext(t.Context(), `SELECT pg_catalog.md5(
			pg_catalog.string_agg(p.proname||'|'||pg_catalog.pg_get_functiondef(p.oid)||'|'||
			p.proowner::text||'|'||COALESCE(p.proconfig::text,'<null>')||'|'||
			COALESCE(p.proacl::text,'<null>'),E'\n' ORDER BY p.proname))
			FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace
			WHERE n.nspname='public' AND p.proname IN(
			'provision_vane_server_runtime_v129','deprovision_vane_server_runtime_v129',
			'provision_vane_server_runtime_v138','deprovision_vane_server_runtime_v138')`).Scan(
			&contract); err != nil {
			t.Fatal(err)
		}
		return contract
	}
	bridgeAt150 := loadBridgeContract()
	if _, err := provider.UpTo(t.Context(), 151); err != nil {
		t.Fatal(err)
	}

	assertWorkspaceEdge := func(want bool) {
		t.Helper()
		var direct, exact int
		if err := database.QueryRowContext(t.Context(), `SELECT count(*)::int,
			count(*) FILTER(WHERE NOT edge.admin_option AND NOT edge.inherit_option
			  AND edge.set_option)::int FROM pg_catalog.pg_auth_members edge
			JOIN pg_catalog.pg_roles granted ON granted.oid=edge.roleid
			JOIN pg_catalog.pg_roles member ON member.oid=edge.member
			WHERE granted.rolname='vane_workspace_memory_editor'
			  AND member.rolname='vane_server_runtime'`).Scan(&direct, &exact); err != nil {
			t.Fatal(err)
		}
		wantCount := 0
		if want {
			wantCount = 1
		}
		if direct != wantCount || exact != wantCount {
			t.Fatalf("workspace runtime edge direct=%d exact=%d want=%d",
				direct, exact, wantCount)
		}
	}
	assertWorkspaceEdge(false)

	// Each mutation runs inside the same owner transaction as the provision
	// call. A rejection aborts and rolls back both the unsafe catalog change and
	// every role edge the wrapper may have touched.
	for _, mutation := range []struct {
		name string
		sql  string
	}{
		{"permissive policy", `ALTER POLICY workspace_memory_record_tenant
			ON public.workspace_memory_records USING(true) WITH CHECK(true)`},
		{"no force RLS", `ALTER TABLE public.workspace_memory_records NO FORCE ROW LEVEL SECURITY`},
		{"missing required ACL", `REVOKE SELECT ON public.workspace_memory_records
			FROM vane_workspace_memory_editor`},
		{"unexpected database authority", `DO $$ BEGIN EXECUTE format(
			'GRANT CONNECT ON DATABASE %I TO vane_workspace_memory_editor',
			pg_catalog.current_database()); END $$`},
		{"unexpected role grantee", `GRANT vane_workspace_memory_editor TO vane_app
			WITH ADMIN FALSE,SET TRUE,INHERIT FALSE`},
	} {
		t.Run("provision rejects "+mutation.name, func(t *testing.T) {
			tx, err := database.BeginTx(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback() }()
			if _, err := tx.ExecContext(t.Context(), mutation.sql); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.ExecContext(t.Context(),
				`SELECT public.provision_vane_server_runtime_v151()`); err == nil {
				t.Fatal("unsafe catalog provisioned workspace memory runtime")
			}
			if err := tx.Rollback(); err != nil && err != sql.ErrTxDone {
				t.Fatal(err)
			}
			assertWorkspaceEdge(false)
		})
	}

	if err := ProvisionServerRuntime(t.Context(), scratchURL); err != nil {
		t.Fatalf("provision v151 runtime: %v", err)
	}
	runtimeProvisioned := true
	t.Cleanup(func() {
		if !runtimeProvisioned {
			return
		}
		_, _ = database.ExecContext(t.Context(),
			`ALTER ROLE vane_server_runtime NOLOGIN PASSWORD NULL`)
		_ = DeprovisionServerRuntime(t.Context(), scratchURL)
	})
	assertWorkspaceEdge(true)
	// Once the edge is active, a later failure inside v129 must roll back the
	// v151 wrapper's initial REVOKE. This is the deployment retry/rollback
	// boundary, not just a pre-grant catalog rejection.
	activeFailure, err := database.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := activeFailure.ExecContext(t.Context(), `REVOKE SELECT
		ON public.memory_records FROM vane_memory_editor`); err != nil {
		t.Fatal(err)
	}
	if _, err := activeFailure.ExecContext(t.Context(),
		`SELECT public.provision_vane_server_runtime_v151()`); err == nil {
		t.Fatal("active runtime provision accepted broken personal-memory ACL")
	}
	if err := activeFailure.Rollback(); err != nil && err != sql.ErrTxDone {
		t.Fatal(err)
	}
	assertWorkspaceEdge(true)
	// This is the exact rollback bridge invoked by the previous production
	// binary. It must preserve both post-v129 capability edges.
	if _, err := database.ExecContext(t.Context(),
		`SELECT public.provision_vane_server_runtime_v129()`); err != nil {
		t.Fatalf("old v129 provision replay after v151 activation: %v", err)
	}
	assertWorkspaceEdge(true)
	if _, err := database.ExecContext(t.Context(),
		`SELECT public.deprovision_vane_server_runtime_v129()`); err != nil {
		t.Fatalf("old v129 deprovision after v151 activation: %v", err)
	}
	assertWorkspaceEdge(false)
	if err := ProvisionServerRuntime(t.Context(), scratchURL); err != nil {
		t.Fatalf("reprovision v151 after old bridge teardown: %v", err)
	}
	assertWorkspaceEdge(true)
	if _, err := database.ExecContext(t.Context(),
		`SELECT public.provision_vane_server_runtime_v138()`); err != nil {
		t.Fatalf("old v138 provision replay after v151 activation: %v", err)
	}
	assertWorkspaceEdge(true)
	if _, err := database.ExecContext(t.Context(),
		`SELECT public.deprovision_vane_server_runtime_v138()`); err != nil {
		t.Fatalf("old v138 deprovision after v151 activation: %v", err)
	}
	assertWorkspaceEdge(false)
	if err := ProvisionServerRuntime(t.Context(), scratchURL); err != nil {
		t.Fatalf("reprovision v151 after old v138 teardown: %v", err)
	}
	assertWorkspaceEdge(true)
	if _, err := database.ExecContext(t.Context(), `CREATE SCHEMA shadow;
		CREATE TABLE shadow.workspace_memory_records(id bigint);
		ALTER TABLE shadow.workspace_memory_records ENABLE ROW LEVEL SECURITY;
		CREATE POLICY unrelated_shadow_policy ON shadow.workspace_memory_records USING(true)`); err != nil {
		t.Fatal(err)
	}

	creator := migration138User(t, database, "v151-creator")
	member := migration138User(t, database, "v151-member")
	team := migration138Workspace(t, database, "v151-team", "team", nil)
	personal := migration138Workspace(t, database, "v151-personal", "personal", &creator)
	for _, membership := range []struct {
		tenant, user int64
		role         string
	}{
		{team, creator, "member"}, {team, member, "member"},
		{personal, creator, "owner"},
	} {
		if _, err := database.ExecContext(t.Context(), `INSERT INTO memberships(
			tenant_id,user_id,role) VALUES($1,$2,$3)`, membership.tenant,
			membership.user, membership.role); err != nil {
			t.Fatal(err)
		}
	}
	var teamSession, personalSession int64
	if err := database.QueryRowContext(t.Context(), `INSERT INTO agent_sessions(
		tenant_id,user_id) VALUES($1,$2) RETURNING id`, team, creator).Scan(&teamSession); err != nil {
		t.Fatal(err)
	}
	if err := database.QueryRowContext(t.Context(), `INSERT INTO agent_sessions(
		tenant_id,user_id) VALUES($1,$2) RETURNING id`, personal, creator).Scan(&personalSession); err != nil {
		t.Fatal(err)
	}

	// Seed a personal record through the compatibility owner Store. The true
	// runtime Store below must never union it into the team's corpus.
	ownerStore, err := New(t.Context(), migration138ApplicationURL(t, scratchURL,
		"migration151-personal-seed"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ownerStore.Close)
	personalAction := migration138Action(types.MemoryActionRemember, 0,
		"v151-personal-secret-token")
	personalAuthorization, err := ownerStore.PrepareMemoryAuthorization(
		t.Context(), personal, creator, personalSession, personalAction)
	if err != nil {
		t.Fatal(err)
	}
	personalAction.Evidence.AuthorizationID = personalAuthorization
	if _, err := ownerStore.ApplyMemoryAction(t.Context(), personal, creator,
		strings.Repeat("f", 64), personalAction); err != nil {
		t.Fatal(err)
	}

	runtimePassword := "migration-151-runtime-password"
	if _, err := database.ExecContext(t.Context(), `ALTER ROLE vane_server_runtime
		LOGIN PASSWORD '`+runtimePassword+`'`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = database.ExecContext(t.Context(),
			`ALTER ROLE vane_server_runtime NOLOGIN PASSWORD NULL`)
	})
	runtimeStore, err := NewServerRuntime(t.Context(),
		serverRuntimeTestURL(t, scratchURL, runtimePassword))
	if err != nil {
		t.Fatalf("open exact runtime Store: %v", err)
	}
	runtimeStore.Close()
	if _, err := database.ExecContext(t.Context(), `SELECT
		public.assert_vane_workspace_memory_editor_v151(true)`); err != nil {
		t.Fatalf("strong SQL assertion counted a decoy schema: %v", err)
	}
	// Re-open because Close above deliberately proves constructor validation;
	// all business calls below now use a fresh, validated production pool.
	runtimeStore, err = NewServerRuntime(t.Context(),
		serverRuntimeTestURL(t, scratchURL, runtimePassword))
	if err != nil {
		t.Fatal(err)
	}
	defer runtimeStore.Close()
	teamAction := migration138Action(types.MemoryActionRemember, 0,
		"v151-team-runtime-token")
	migration138Apply(t, runtimeStore, team, creator, teamSession,
		strings.Repeat("e", 64), teamAction)
	recalled, err := runtimeStore.RecallWorkspaceMemories(t.Context(), team, member,
		types.MemoryRecallQuery{Query: "v151-team-runtime-token", Limit: 5})
	if err != nil {
		t.Fatalf("member recall through exact runtime Store: %v", err)
	}
	if len(recalled.Memories) != 1 ||
		recalled.Memories[0].Memory.Text != "v151-team-runtime-token" {
		t.Fatalf("runtime team recall=%+v", recalled.Memories)
	}
	notLeaked, err := runtimeStore.RecallWorkspaceMemories(t.Context(), team, member,
		types.MemoryRecallQuery{Query: "v151-personal-secret-token", Limit: 5})
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range notLeaked.Memories {
		if item.Memory.Text == "v151-personal-secret-token" {
			t.Fatalf("personal record leaked into team runtime corpus: %+v", notLeaked.Memories)
		}
	}

	if _, err := database.ExecContext(t.Context(),
		`ALTER ROLE vane_server_runtime NOLOGIN PASSWORD NULL`); err != nil {
		t.Fatal(err)
	}
	if err := DeprovisionServerRuntime(t.Context(), scratchURL); err != nil {
		t.Fatalf("deprovision v151 runtime: %v", err)
	}
	runtimeProvisioned = false
	assertWorkspaceEdge(false)
	if _, err := provider.DownTo(t.Context(), 150); err != nil {
		t.Fatal(err)
	}
	var version int64
	if err := database.QueryRowContext(t.Context(), `SELECT max(version_id)
		FROM goose_db_version WHERE is_applied`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 150 {
		t.Fatalf("schema version=%d want=150", version)
	}
	if bridgeAfterDown := loadBridgeContract(); bridgeAfterDown != bridgeAt150 {
		t.Fatalf("migration 151 Down bridge contract=%s want schema150=%s",
			bridgeAfterDown, bridgeAt150)
	}
}
