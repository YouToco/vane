package store

import (
	"context"
	"fmt"
	"io/fs"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestTaskSourceIsolationFoundationIsExplicit(t *testing.T) {
	raw, err := fs.ReadFile(
		migrationsFS,
		"migrations/075_task_source_isolation_foundation.sql",
	)
	if err != nil {
		t.Fatal(err)
	}
	sqlText := string(raw)
	for _, required := range []string{
		"CREATE TABLE task_sources",
		"CREATE TABLE task_source_states",
		"CREATE TABLE task_run_sources",
		"CREATE TABLE task_content_records",
		"CREATE TABLE task_content_appearances",
		"CREATE TABLE task_content_evidence",
		"FOREIGN KEY (tenant_id, user_id, task_id)",
		"current_setting(''app.tenant_id''",
		"current_setting(''app.user_id''",
		"ON DELETE SET NULL (task_source_id)",
		"GRANT UPDATE (retired_at) ON task_sources",
		"075: refusing downgrade while task Source/content evidence exists",
	} {
		if !strings.Contains(sqlText, required) {
			t.Errorf("migration 075 missing %q", required)
		}
	}
	for _, forbidden := range []string{
		"INSERT INTO fetch_targets",
		"UPDATE fetch_targets",
		"INSERT INTO content_items",
		"UPDATE content_items",
	} {
		if strings.Contains(sqlText, forbidden) {
			t.Errorf("zero-call-point migration 075 contains legacy write %q", forbidden)
		}
	}
}

func asTaskSourceOwner(
	t *testing.T,
	st *Store,
	tenantID, userID int64,
	fn func(pgx.Tx),
) {
	t.Helper()
	ctx := t.Context()
	tx, err := st.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.tenant_id',$1,true),
		        set_config('app.user_id',$2,true)`,
		fmt.Sprint(tenantID), fmt.Sprint(userID)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_app`); err != nil {
		t.Fatal(err)
	}
	fn(tx)
}

func TestTaskSourceIsolationFoundationRLSAndCompositeScope(t *testing.T) {
	st := tenantTestStore(t)
	ctx := t.Context()

	type fixture struct {
		tenantID int64
		userID   int64
		taskID   string
		sourceID int64
	}
	makeFixture := func(label string) fixture {
		var f fixture
		if err := st.pool.QueryRow(ctx,
			`INSERT INTO tenants DEFAULT VALUES RETURNING id`,
		).Scan(&f.tenantID); err != nil {
			t.Fatal(err)
		}
		user, err := st.UpsertUserByOpenID(
			ctx,
			fmt.Sprintf("ou_source_075_%s_%d", label, time.Now().UnixNano()),
			"source-075-"+label,
		)
		if err != nil {
			t.Fatal(err)
		}
		f.userID = user.ID
		f.taskID = fmt.Sprintf("source-075-%s-%d", label, time.Now().UnixNano())
		if _, err := st.pool.Exec(ctx,
			`INSERT INTO memberships (tenant_id,user_id,role)
			 VALUES ($1,$2,'owner')`,
			f.tenantID, f.userID); err != nil {
			t.Fatal(err)
		}
		if _, err := st.pool.Exec(ctx,
			`INSERT INTO schedules (
			    id,tenant_id,user_id,nl_description,spec_json,scope_json,
			    status,execution_mode
			 ) VALUES ($1,$2,$3,'source 075','{}','{}','paused','compiled')`,
			f.taskID, f.tenantID, f.userID); err != nil {
			t.Fatal(err)
		}
		if err := st.pool.QueryRow(ctx,
			`INSERT INTO task_sources (
			    tenant_id,user_id,task_id,revision,schema_version,
			    tool_name,tool_version,tool_arguments,platform,capability,
			    title,endpoint_url,runtime_config,identity_digest
			 ) VALUES (
			    $1,$2,$3,1,'vane.task-source/v1',
			    'weibo_user_posts','v1','{"uid":"2803301701"}',
			    'weibo','user_posts','same public blogger',
			    'vane://weibo/user_posts?uid=2803301701',
			    '{"uid":"2803301701"}',$4
			 ) RETURNING id`,
			f.tenantID, f.userID, f.taskID, strings.Repeat("a", 64),
		).Scan(&f.sourceID); err != nil {
			t.Fatal(err)
		}
		if _, err := st.pool.Exec(ctx,
			`INSERT INTO task_source_states (
			    tenant_id,user_id,task_id,source_id
			 ) VALUES ($1,$2,$3,$4)`,
			f.tenantID, f.userID, f.taskID, f.sourceID); err != nil {
			t.Fatal(err)
		}
		return f
	}

	a := makeFixture("a")
	b := makeFixture("b")
	t.Cleanup(func() {
		cleanupCtx := context.WithoutCancel(ctx)
		for _, f := range []fixture{a, b} {
			_, _ = st.pool.Exec(cleanupCtx, `DELETE FROM schedules WHERE id=$1`, f.taskID)
			_, _ = st.pool.Exec(cleanupCtx,
				`DELETE FROM memberships WHERE tenant_id=$1 AND user_id=$2`,
				f.tenantID, f.userID)
			_, _ = st.pool.Exec(cleanupCtx, `DELETE FROM users WHERE id=$1`, f.userID)
			_, _ = st.pool.Exec(cleanupCtx, `DELETE FROM tenants WHERE id=$1`, f.tenantID)
		}
	})

	asTaskSourceOwner(t, st, a.tenantID, a.userID, func(tx pgx.Tx) {
		var visible int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM task_sources`).Scan(&visible); err != nil {
			t.Fatal(err)
		}
		if visible != 1 {
			t.Fatalf("owner A sees %d task Sources, want exactly its own 1", visible)
		}

		tag, err := tx.Exec(ctx,
			`UPDATE task_source_states
			    SET status='disabled',state_version=state_version+1
			  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3 AND source_id=$4`,
			b.tenantID, b.userID, b.taskID, b.sourceID)
		if err != nil {
			t.Fatal(err)
		}
		if tag.RowsAffected() != 0 {
			t.Fatal("owner A changed owner B Source state")
		}
	})

	asTaskSourceOwner(t, st, a.tenantID, a.userID, func(tx pgx.Tx) {
		if _, err := tx.Exec(ctx,
			`INSERT INTO task_source_states (
			    tenant_id,user_id,task_id,source_id
			 ) VALUES ($1,$2,$3,$4)`,
			a.tenantID, a.userID, a.taskID, b.sourceID); err == nil {
			t.Fatal("composite Source FK accepted another owner's source_id")
		}
	})

	asTaskSourceOwner(t, st, a.tenantID, a.userID, func(tx pgx.Tx) {
		if _, err := tx.Exec(ctx,
			`INSERT INTO task_sources (
			    tenant_id,user_id,task_id,revision,schema_version,
			    tool_name,tool_version,tool_arguments,platform,capability,
			    endpoint_url,runtime_config,identity_digest
			 ) VALUES (
			    $1,$2,$3,2,'vane.task-source/v1',
			    'weibo_user_posts','v1','{}','weibo','user_posts',
			    'vane://forbidden','{}',$4
			 )`,
			b.tenantID, b.userID, b.taskID, strings.Repeat("b", 64)); err == nil {
			t.Fatal("owner A inserted a task Source into owner B scope")
		}
	})

	var bStatus string
	if err := st.pool.QueryRow(ctx,
		`SELECT status FROM task_source_states
		  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3 AND source_id=$4`,
		b.tenantID, b.userID, b.taskID, b.sourceID).Scan(&bStatus); err != nil {
		t.Fatal(err)
	}
	if bStatus != "active" {
		t.Fatalf("owner B Source state changed to %q", bStatus)
	}
}
