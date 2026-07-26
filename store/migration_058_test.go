package store

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/pressly/goose/v3"

	"github.com/YouToco/vane/types"
)

func TestMigration058EmptyDownAndOrphanV2Fence(t *testing.T) {
	db, provider := migration035Scratch(t)
	ctx := t.Context()
	if _, err := provider.UpTo(ctx, 58); err != nil {
		t.Fatalf("migrate to 058: %v", err)
	}
	if _, err := provider.DownTo(ctx, 57); err != nil {
		t.Fatalf("empty 058 Down: %v", err)
	}
	if _, err := provider.UpTo(ctx, 58); err != nil {
		t.Fatalf("reapply 058: %v", err)
	}
	var tenantID, userID, sessionID int64
	if err := db.QueryRowContext(ctx,
		`INSERT INTO tenants DEFAULT VALUES RETURNING id`,
	).Scan(&tenantID); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx,
		`INSERT INTO users (feishu_open_id,name)
		 VALUES ('migration-058-user','migration 058') RETURNING id`,
	).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO memberships (tenant_id,user_id,role)
		 VALUES ($1,$2,'owner')`,
		tenantID, userID,
	); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx,
		`INSERT INTO agent_sessions (tenant_id,user_id)
		 VALUES ($1,$2) RETURNING id`,
		tenantID, userID,
	).Scan(&sessionID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO pending_actions (
		     id,tenant_id,user_id,session_id,tool_name,args,
		     expires_at,execution_version
		 ) VALUES (
		     'migration-058-orphan',$1,$2,$3,'enable_source',
		     '{"source_id":1}',clock_timestamp()+interval '1 hour',2
		 )`,
		tenantID, userID, sessionID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.DownTo(ctx, 57); err == nil ||
		!strings.Contains(
			err.Error(),
			"refusing downgrade while durable Agent actions exist",
		) {
		t.Fatalf("058 Down accepted orphan v2 root: %v", err)
	}
}

func TestMigration058DownSerializesWithProjection(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL，跳过 058 迁移并发测试")
	}
	ctx := t.Context()
	scratchURL, drop := createScratchDB(ctx, t, dbURL)
	t.Cleanup(drop)
	db, err := sql.Open("pgx", scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	dir, err := fs.Sub(migrationsFS, "migrations")
	if err != nil {
		t.Fatal(err)
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, db, dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.UpTo(ctx, 58); err != nil {
		t.Fatalf("migrate to 058: %v", err)
	}
	st, err := New(ctx, scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	registerStoreClose(t, st)

	var tenantID, userID, sessionID int64
	if err := st.pool.QueryRow(ctx,
		`INSERT INTO tenants DEFAULT VALUES RETURNING id`,
	).Scan(&tenantID); err != nil {
		t.Fatal(err)
	}
	if err := st.pool.QueryRow(ctx,
		`INSERT INTO users (feishu_open_id,name)
		 VALUES ($1,'migration 058 project') RETURNING id`,
		"m058-project-"+uuid.NewString(),
	).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(ctx,
		`INSERT INTO memberships (tenant_id,user_id,role)
		 VALUES ($1,$2,'owner')`,
		tenantID, userID,
	); err != nil {
		t.Fatal(err)
	}
	if err := st.pool.QueryRow(ctx,
		`INSERT INTO agent_sessions (tenant_id,user_id)
		 VALUES ($1,$2) RETURNING id`,
		tenantID, userID,
	).Scan(&sessionID); err != nil {
		t.Fatal(err)
	}
	sourceID, _, err := st.UpsertSource(ctx, &types.Source{
		Platform: types.PlatformWeb, Capability: types.CapFeed,
		URL:   "https://example.com/m058-" + uuid.NewString(),
		Title: "migration 058 projection",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.AddSubscription(ctx, userID, sourceID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(ctx,
		`UPDATE sources SET status='disabled',fail_count=9 WHERE id=$1`,
		sourceID,
	); err != nil {
		t.Fatal(err)
	}
	actionID := uuid.NewString()
	if err := st.CreatePendingAction(ctx, &types.PendingAction{
		ID: actionID, UserID: userID, SessionID: &sessionID,
		ToolName: "enable_source",
		Args:     []byte(fmt.Sprintf(`{"source_id":%d}`, sourceID)),
		Summary:  "migration 058", Status: types.PendingActionStatusPending,
		ExpiresAt: time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.ActivateAgentActionContinuation(
		ctx, tenantID, userID, actionID, "migration 058",
	); err != nil {
		t.Fatal(err)
	}
	if _, err := st.ConfirmAgentActionContinuation(
		ctx, userID, actionID,
	); err != nil {
		t.Fatal(err)
	}
	acquired, err := st.AcquireAgentActionContinuation(
		ctx, actionID, tenantID, userID,
		"migration-058-project", time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := acquired.Lease()
	if err != nil {
		t.Fatal(err)
	}

	continuationLocked := make(chan struct{})
	resumeProject := make(chan struct{})
	var resumeOnce sync.Once
	resume := func() {
		resumeOnce.Do(func() { close(resumeProject) })
	}
	t.Cleanup(resume)
	projectStore := *st
	projectStore.beginTx = func(
		ctx context.Context,
		options pgx.TxOptions,
	) (pgx.Tx, error) {
		tx, err := st.pool.BeginTx(ctx, options)
		if err != nil {
			return nil, err
		}
		return &m058PauseAfterContinuationTx{
			Tx: tx, locked: continuationLocked, resume: resumeProject,
		}, nil
	}
	projectDone := make(chan error, 1)
	go func() {
		projectDone <- projectStore.ProjectAgentActionContinuation(ctx, lease)
	}()
	select {
	case <-continuationLocked:
	case <-time.After(10 * time.Second):
		t.Fatal("projection did not lock continuation")
	}

	downDone := make(chan error, 1)
	go func() {
		_, downErr := provider.DownTo(ctx, 57)
		downDone <- downErr
	}()
	waitForDatabaseLockQuery(
		t, st,
		"%migration 058 action root first%",
		"058 Down did not wait behind the projection root lock",
	)
	resume()
	select {
	case err := <-projectDone:
		if err != nil {
			t.Fatalf("projection failed while Down waited: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("projection did not converge")
	}
	select {
	case err := <-downDone:
		if err == nil ||
			!strings.Contains(
				err.Error(),
				"refusing downgrade while durable Agent actions exist",
			) ||
			strings.Contains(err.Error(), "40P01") ||
			strings.Contains(strings.ToLower(err.Error()), "deadlock") {
			t.Fatalf("concurrent 058 Down err=%v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("058 Down did not converge after projection")
	}
	var version int
	var sourceStatus, actionStatus string
	if err := st.pool.QueryRow(ctx,
		`SELECT
		    (SELECT COALESCE(max(version_id),0)
		       FROM goose_db_version WHERE is_applied),
		    (SELECT status FROM sources WHERE id=$1),
		    (SELECT status FROM agent_action_continuations
		      WHERE action_id=$2)`,
		sourceID, actionID,
	).Scan(&version, &sourceStatus, &actionStatus); err != nil {
		t.Fatal(err)
	}
	if version != 58 ||
		sourceStatus != string(types.SourceStatusActive) ||
		actionStatus != AgentActionStatusCompleted {
		t.Fatalf(
			"version/source/action=%d/%s/%s",
			version, sourceStatus, actionStatus,
		)
	}
}

type m058PauseAfterContinuationTx struct {
	pgx.Tx
	locked chan struct{}
	resume <-chan struct{}
	once   sync.Once
}

func (tx *m058PauseAfterContinuationTx) QueryRow(
	ctx context.Context,
	query string,
	args ...any,
) pgx.Row {
	row := tx.Tx.QueryRow(ctx, query, args...)
	if !strings.Contains(query, "FROM agent_action_continuations") ||
		!strings.Contains(query, "FOR UPDATE") {
		return row
	}
	return m058PauseAfterContinuationRow{
		Row: row,
		pause: func() {
			tx.once.Do(func() {
				close(tx.locked)
				<-tx.resume
			})
		},
	}
}

type m058PauseAfterContinuationRow struct {
	pgx.Row
	pause func()
}

func (row m058PauseAfterContinuationRow) Scan(dest ...any) error {
	if err := row.Row.Scan(dest...); err != nil {
		return err
	}
	row.pause()
	return nil
}
