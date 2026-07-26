package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/YouToco/vane/types"
	"github.com/jackc/pgx/v5"
	"github.com/pressly/goose/v3"
)

func TestMigration056RLSAndMinimumRuntimePrivileges(t *testing.T) {
	f := newAgentSessionFactFixture(t)
	ctx := t.Context()
	feedbackID := f.insertFeedback(t, types.FeedbackActionInterested)
	_ = feedbackID

	var rls, forceRLS bool
	var appCanSelect, appCanDelete, appCanTruncate, appCanInsert bool
	var appCanUpdateStatus, projectorCanUpdateStatus bool
	var projectorCanUpdatePayload, appSequenceUsage, appSequenceSelect bool
	var projectorLogin, projectorInherit, projectorBypassRLS bool
	if err := f.store.pool.QueryRow(ctx,
		`SELECT c.relrowsecurity,c.relforcerowsecurity,
		        has_table_privilege(
		            'vane_app','agent_session_fact_outbox','SELECT'),
		        has_table_privilege(
		            'vane_app','agent_session_fact_outbox','DELETE'),
		        has_table_privilege(
		            'vane_app','agent_session_fact_outbox','TRUNCATE'),
		        has_column_privilege(
		            'vane_app','agent_session_fact_outbox',
		            'fact_id','INSERT'),
		        has_column_privilege(
		            'vane_app','agent_session_fact_outbox',
		            'status','UPDATE'),
		        has_column_privilege(
		            'vane_agent_session_fact_projector',
		            'agent_session_fact_outbox','status','UPDATE'),
		        has_column_privilege(
		            'vane_agent_session_fact_projector',
		            'agent_session_fact_outbox',
		            'session_messages','UPDATE'),
		        has_sequence_privilege(
		            'vane_app','agent_session_fact_outbox_id_seq','USAGE'),
		        has_sequence_privilege(
		            'vane_app','agent_session_fact_outbox_id_seq','SELECT'),
		        r.rolcanlogin,r.rolinherit,r.rolbypassrls
		   FROM pg_class c
		   CROSS JOIN pg_roles r
		  WHERE c.oid='agent_session_fact_outbox'::regclass
		    AND r.rolname='vane_agent_session_fact_projector'`,
	).Scan(
		&rls, &forceRLS, &appCanSelect, &appCanDelete,
		&appCanTruncate, &appCanInsert, &appCanUpdateStatus,
		&projectorCanUpdateStatus, &projectorCanUpdatePayload,
		&appSequenceUsage, &appSequenceSelect, &projectorLogin,
		&projectorInherit, &projectorBypassRLS,
	); err != nil {
		t.Fatal(err)
	}
	if !rls || forceRLS || appCanSelect || appCanDelete ||
		appCanTruncate || !appCanInsert || appCanUpdateStatus ||
		!projectorCanUpdateStatus || projectorCanUpdatePayload ||
		!appSequenceUsage || appSequenceSelect || projectorLogin ||
		projectorInherit || projectorBypassRLS {
		t.Fatalf(
			"rls=%v force=%v app_select=%v delete=%v truncate=%v "+
				"insert=%v app_update=%v projector_update=%v "+
				"projector_payload=%v seq_usage=%v seq_select=%v "+
				"login=%v inherit=%v bypassrls=%v",
			rls, forceRLS, appCanSelect, appCanDelete,
			appCanTruncate, appCanInsert, appCanUpdateStatus,
			projectorCanUpdateStatus, projectorCanUpdatePayload,
			appSequenceUsage, appSequenceSelect, projectorLogin,
			projectorInherit, projectorBypassRLS,
		)
	}

	tx, err := f.store.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.tenant_id',$1,true)`,
		fmt.Sprintf("%d", f.tenantB),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx,
		`SET LOCAL ROLE vane_agent_session_fact_projector`,
	); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := tx.QueryRow(ctx,
		`SELECT count(*) FROM agent_session_fact_outbox
		  WHERE tenant_id=$1`,
		f.tenantA,
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("cross-tenant visible rows=%d", count)
	}
}

func TestAgentSessionFactProjectorRuntimeCapabilityValidator(t *testing.T) {
	f := newAgentSessionFactFixture(t)
	assertProjectorCapabilityValid(t, f.store)

	tests := []struct {
		name    string
		apply   string
		restore string
	}{
		{
			name:  "attrs",
			apply: `ALTER ROLE vane_agent_session_fact_projector LOGIN`,
			restore: `ALTER ROLE vane_agent_session_fact_projector
			          NOLOGIN NOINHERIT NOBYPASSRLS`,
		},
		{
			name: "config",
			apply: `ALTER ROLE vane_agent_session_fact_projector
			        SET statement_timeout='1s'`,
			restore: `ALTER ROLE vane_agent_session_fact_projector RESET ALL;
			          ALTER ROLE vane_agent_session_fact_projector
			          SET search_path=pg_catalog, public`,
		},
		{
			name: "membership",
			apply: `CREATE ROLE m056_projector_parent NOLOGIN;
			        GRANT m056_projector_parent
			        TO vane_agent_session_fact_projector`,
			restore: `REVOKE m056_projector_parent
			          FROM vane_agent_session_fact_projector;
			          DROP ROLE m056_projector_parent`,
		},
		{
			name: "extra table",
			apply: `GRANT SELECT ON feedbacks
			        TO vane_agent_session_fact_projector`,
			restore: `REVOKE SELECT ON feedbacks
			          FROM vane_agent_session_fact_projector`,
		},
		{
			name: "extra column",
			apply: `GRANT UPDATE (session_messages)
			        ON agent_session_fact_outbox
			        TO vane_agent_session_fact_projector`,
			restore: `REVOKE UPDATE (session_messages)
			          ON agent_session_fact_outbox
			          FROM vane_agent_session_fact_projector`,
		},
		{
			name: "extra sequence",
			apply: `GRANT USAGE ON SEQUENCE feedbacks_id_seq
			        TO vane_agent_session_fact_projector`,
			restore: `REVOKE USAGE ON SEQUENCE feedbacks_id_seq
			          FROM vane_agent_session_fact_projector`,
		},
		{
			name: "extra schema",
			apply: `CREATE SCHEMA m056_projector_drift;
			        GRANT USAGE ON SCHEMA m056_projector_drift
			        TO vane_agent_session_fact_projector`,
			restore: `DROP SCHEMA m056_projector_drift`,
		},
		{
			name: "public create",
			apply: `GRANT CREATE ON SCHEMA public
			        TO vane_agent_session_fact_projector`,
			restore: `REVOKE CREATE ON SCHEMA public
			          FROM vane_agent_session_fact_projector`,
		},
		{
			name: "security definer execute",
			apply: `CREATE FUNCTION m056_projector_secdef()
			          RETURNS integer
			          LANGUAGE sql SECURITY DEFINER
			          SET search_path=pg_catalog
			          AS 'SELECT 1';
			        REVOKE ALL ON FUNCTION m056_projector_secdef()
			          FROM PUBLIC;
			        GRANT EXECUTE ON FUNCTION m056_projector_secdef()
			          TO vane_agent_session_fact_projector`,
			restore: `DROP FUNCTION m056_projector_secdef()`,
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := f.store.pool.Exec(
				t.Context(), testCase.apply,
			); err != nil {
				t.Fatal(err)
			}
			defer func() {
				if _, err := f.store.pool.Exec(
					context.Background(), testCase.restore,
				); err != nil {
					t.Errorf("restore capability mutation: %v", err)
				}
			}()
			assertProjectorCapabilityRejected(t, f.store)
		})
	}
}

func TestAgentSessionFactProjectorDriftFailsAcquireProjectAndRelease(
	t *testing.T,
) {
	f := newAgentSessionFactFixture(t)
	firstID := f.insertFeedback(t, types.FeedbackActionInterested)
	first := f.loadFact(t, firstID)
	lease := acquireFact(t, f, first, "projector-drift")
	secondID := f.insertFeedback(t, types.FeedbackActionNotInterested)
	second := f.loadFact(t, secondID)

	if _, err := f.store.pool.Exec(t.Context(),
		`GRANT UPDATE (session_messages)
		   ON agent_session_fact_outbox
		   TO vane_agent_session_fact_projector`,
	); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := f.store.pool.Exec(context.Background(),
			`REVOKE UPDATE (session_messages)
			   ON agent_session_fact_outbox
			   FROM vane_agent_session_fact_projector`,
		); err != nil {
			t.Errorf("restore projector privilege: %v", err)
		}
	}()

	_, err := f.store.AcquireAgentSessionFact(
		t.Context(), AcquireAgentSessionFactParams{
			ID: second.ID, TenantID: second.TenantID,
			UserID: second.UserID, LeaseOwner: "drift-acquire",
			LeaseDuration: time.Minute,
		})
	if !errors.Is(err, types.ErrInternal) {
		t.Fatalf("Acquire drift err=%v", err)
	}
	if err := f.store.ProjectAgentSessionFact(
		t.Context(), lease,
	); !errors.Is(err, types.ErrInternal) {
		t.Fatalf("Project drift err=%v", err)
	}
	if err := f.store.ReleaseAgentSessionFact(
		t.Context(), lease, time.Minute,
	); !errors.Is(err, types.ErrInternal) {
		t.Fatalf("Release drift err=%v", err)
	}
	var events int
	if err := f.store.pool.QueryRow(t.Context(),
		`SELECT count(*) FROM agent_events
		  WHERE tenant_id=$1 AND session_id=$2
		    AND batch_idempotency_key LIKE 'side.%'`,
		f.tenantA, f.sessionA,
	).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if events != 0 {
		t.Fatalf("capability drift admitted %d event writes", events)
	}
}

func assertProjectorCapabilityValid(t *testing.T, st *Store) {
	t.Helper()
	tx, err := st.beginTx(t.Context(), pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background()) //nolint:errcheck
	if err := validateAgentSessionFactProjector(
		t.Context(), tx,
	); err != nil {
		t.Fatalf("baseline projector capability: %v", err)
	}
}

func assertProjectorCapabilityRejected(t *testing.T, st *Store) {
	t.Helper()
	tx, err := st.beginTx(t.Context(), pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background()) //nolint:errcheck
	if err := validateAgentSessionFactProjector(
		t.Context(), tx,
	); !errors.Is(err, types.ErrInternal) {
		t.Fatalf("capability drift err=%v", err)
	}
}

func TestMigration056DownRefusesDurableFacts(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL，跳过 056 Down fence 真库测试")
	}
	ctx := t.Context()
	scratchURL, drop := createScratchDB(ctx, t, dbURL)
	defer drop()

	db, err := sql.Open("pgx", scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	dir, err := fs.Sub(migrationsFS, "migrations")
	if err != nil {
		t.Fatal(err)
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, db, dir,
		goose.WithAllowOutofOrder(true))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Up(ctx); err != nil {
		t.Fatal(err)
	}
	var tenantID, userID, sessionID int64
	if err := db.QueryRowContext(ctx,
		`INSERT INTO tenants DEFAULT VALUES RETURNING id`,
	).Scan(&tenantID); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx,
		`INSERT INTO users (feishu_open_id,name)
		 VALUES ('ou_m056_down','m056') RETURNING id`,
	).Scan(&userID); err != nil {
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
		`INSERT INTO agent_session_fact_outbox (
		     tenant_id,user_id,fact_type,fact_id,source_identity,
		     session_id,session_messages,payload_digest,status
		 )
		 VALUES ($1,$2,'feedback',1,'feedback-click:1',$3,
		         '[{"role":"user","content":"x"}]'::bytea,
		         repeat('a',64),'pending')`,
		tenantID, userID, sessionID,
	); err != nil {
		t.Fatal(err)
	}
	_, err = provider.DownTo(ctx, 55)
	if err == nil || !strings.Contains(
		err.Error(),
		"refusing downgrade while Agent continuation facts exist",
	) {
		t.Fatalf("DownTo(55) err=%v", err)
	}
}

func TestMigration056EmptyOutboxDownConvergesWithAdmittedProducer(
	t *testing.T,
) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL，跳过 056 producer/Down 真库并发测试")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	scratchURL, drop := createScratchDB(ctx, t, dbURL)
	defer drop()
	if err := Migrate(ctx, scratchURL); err != nil {
		t.Fatal(err)
	}
	st, err := New(ctx, scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	var tenantID int64
	if err := st.pool.QueryRow(ctx,
		`INSERT INTO tenants DEFAULT VALUES RETURNING id`,
	).Scan(&tenantID); err != nil {
		t.Fatal(err)
	}
	user, err := st.UpsertUserByOpenID(
		ctx, "m056_down_producer", "m056 producer")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(ctx,
		`INSERT INTO memberships (tenant_id,user_id,role)
		 VALUES ($1,$2,'owner')`,
		tenantID, user.ID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(ctx,
		`INSERT INTO agent_sessions (tenant_id,user_id)
		 VALUES ($1,$2)`,
		tenantID, user.ID,
	); err != nil {
		t.Fatal(err)
	}
	var batchID, deliveryID int64
	if err := st.pool.QueryRow(ctx,
		`INSERT INTO push_batches (tenant_id,user_id)
		 VALUES ($1,$2) RETURNING id`,
		tenantID, user.ID,
	).Scan(&batchID); err != nil {
		t.Fatal(err)
	}
	if err := st.pool.QueryRow(ctx,
		`INSERT INTO deliveries (tenant_id,batch_id,user_id)
		 VALUES ($1,$2,$3) RETURNING id`,
		tenantID, batchID, user.ID,
	).Scan(&deliveryID); err != nil {
		t.Fatal(err)
	}

	sessionLocked := make(chan struct{})
	releaseProducer := make(chan struct{})
	producerStore := *st
	producerStore.beginTx = func(
		ctx context.Context,
		options pgx.TxOptions,
	) (pgx.Tx, error) {
		tx, err := st.pool.BeginTx(ctx, options)
		if err != nil {
			return nil, err
		}
		return &m056BlockingSessionTx{
			Tx: tx, locked: sessionLocked, release: releaseProducer,
		}, nil
	}
	producerDone := make(chan error, 1)
	go func() {
		_, err := producerStore.InsertFeedbackWithSessionCutoff(
			ctx, &types.Feedback{
				UserID: user.ID, DeliveryID: deliveryID,
				Action: types.FeedbackActionInterested,
			}, time.Now().Add(-time.Hour))
		producerDone <- err
	}()
	select {
	case <-sessionLocked:
	case <-ctx.Done():
		t.Fatal("producer did not reach frozen session lock")
	}

	db, err := sql.Open("pgx", scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	dir, err := fs.Sub(migrationsFS, "migrations")
	if err != nil {
		t.Fatal(err)
	}
	provider, err := goose.NewProvider(
		goose.DialectPostgres, db, dir,
		goose.WithAllowOutofOrder(true),
	)
	if err != nil {
		t.Fatal(err)
	}
	downDone := make(chan error, 1)
	go func() {
		_, err := provider.DownTo(ctx, 53)
		downDone <- err
	}()
	select {
	case err := <-downDone:
		t.Fatalf("Down bypassed producer admission: %v", err)
	case <-time.After(150 * time.Millisecond):
	}
	close(releaseProducer)
	if err := <-producerDone; err != nil {
		t.Fatalf("admitted producer: %v", err)
	}
	downErr := <-downDone
	if downErr == nil ||
		!strings.Contains(
			downErr.Error(),
			"refusing downgrade while Agent continuation facts exist",
		) ||
		strings.Contains(downErr.Error(), "40P01") ||
		strings.Contains(strings.ToLower(downErr.Error()), "deadlock") {
		t.Fatalf("concurrent Down err=%v", downErr)
	}
	var facts int
	if err := st.pool.QueryRow(ctx,
		`SELECT count(*) FROM agent_session_fact_outbox`,
	).Scan(&facts); err != nil {
		t.Fatal(err)
	}
	if facts != 1 {
		t.Fatalf("durable producer facts=%d want=1", facts)
	}
}

type m056BlockingSessionTx struct {
	pgx.Tx
	locked  chan struct{}
	release chan struct{}
}

func (tx *m056BlockingSessionTx) QueryRow(
	ctx context.Context,
	sql string,
	args ...any,
) pgx.Row {
	row := tx.Tx.QueryRow(ctx, sql, args...)
	if !strings.Contains(sql, "FOR SHARE") {
		return row
	}
	return m056BlockingRow{
		Row: row, ctx: ctx, locked: tx.locked, release: tx.release,
	}
}

type m056BlockingRow struct {
	pgx.Row
	ctx     context.Context
	locked  chan struct{}
	release chan struct{}
}

func (row m056BlockingRow) Scan(dest ...any) error {
	if err := row.Row.Scan(dest...); err != nil {
		return err
	}
	select {
	case <-row.locked:
	default:
		close(row.locked)
	}
	select {
	case <-row.release:
		return nil
	case <-row.ctx.Done():
		return row.ctx.Err()
	}
}
