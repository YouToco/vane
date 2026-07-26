package store

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/YouToco/vane/definitioneditwire"
	"github.com/YouToco/vane/types"
)

func TestMigration057RestrictedRebaseCapability(t *testing.T) {
	db, provider := migration035Scratch(t)
	ctx := t.Context()
	if _, err := provider.UpTo(ctx, 57); err != nil {
		t.Fatalf("migrate to 057: %v", err)
	}

	var (
		definer, owner, fixedSearchPath, publicExecute   bool
		coordinatorExecute, appExecute, operatorExecute  bool
		coordinatorEventInsert, coordinatorPointerUpdate bool
		sequenceUsage, sequenceSelect, sequenceUpdate    bool
	)
	if err := db.QueryRowContext(ctx, `
		SELECT
		  p.prosecdef,
		  p.proowner = (SELECT oid FROM pg_roles WHERE rolname=current_user),
		  p.proconfig = ARRAY['search_path=pg_catalog, public']::TEXT[],
		  EXISTS (
		      SELECT 1 FROM aclexplode(p.proacl) acl
		       WHERE acl.grantee=0 AND acl.privilege_type='EXECUTE'
		  ),
		  has_function_privilege(
		      'vane_edit_coordinator',
		      'task_run_snapshot_v2_rebase_definition_edit(text,bigint,text)',
		      'EXECUTE'),
		  has_function_privilege(
		      'vane_app',
		      'task_run_snapshot_v2_rebase_definition_edit(text,bigint,text)',
		      'EXECUTE'),
		  has_function_privilege(
		      'vane_snapshot_cutover_operator',
		      'task_run_snapshot_v2_rebase_definition_edit(text,bigint,text)',
		      'EXECUTE'),
		  has_table_privilege(
		      'vane_edit_coordinator',
		      'task_run_snapshot_v2_cutover_events',
		      'INSERT'),
		  has_column_privilege(
		      'vane_edit_coordinator',
		      'schedules',
		      'run_snapshot_cutover_event_id',
		      'UPDATE'),
		  has_sequence_privilege(
		      'vane_edit_coordinator',
		      'task_run_snapshot_v2_cutover_events_id_seq',
		      'USAGE'),
		  has_sequence_privilege(
		      'vane_edit_coordinator',
		      'task_run_snapshot_v2_cutover_events_id_seq',
		      'SELECT'),
		  has_sequence_privilege(
		      'vane_edit_coordinator',
		      'task_run_snapshot_v2_cutover_events_id_seq',
		      'UPDATE')
		  FROM pg_proc p
		 WHERE p.oid =
		  'task_run_snapshot_v2_rebase_definition_edit(text,bigint,text)'::regprocedure`,
	).Scan(
		&definer,
		&owner,
		&fixedSearchPath,
		&publicExecute,
		&coordinatorExecute,
		&appExecute,
		&operatorExecute,
		&coordinatorEventInsert,
		&coordinatorPointerUpdate,
		&sequenceUsage,
		&sequenceSelect,
		&sequenceUpdate,
	); err != nil {
		t.Fatal(err)
	}
	if !definer || !owner || !fixedSearchPath || publicExecute ||
		!coordinatorExecute || appExecute || operatorExecute ||
		coordinatorEventInsert || coordinatorPointerUpdate ||
		sequenceUsage || sequenceSelect || sequenceUpdate {
		t.Fatalf(
			"057 rebase capability drift: definer=%v owner=%v path=%v "+
				"execute(public/coordinator/app/operator)=%v/%v/%v/%v "+
				"raw(event/pointer/sequence)=%v/%v/%v/%v/%v",
			definer,
			owner,
			fixedSearchPath,
			publicExecute,
			coordinatorExecute,
			appExecute,
			operatorExecute,
			coordinatorEventInsert,
			coordinatorPointerUpdate,
			sequenceUsage,
			sequenceSelect,
			sequenceUpdate,
		)
	}

	if _, err := db.ExecContext(ctx, `
		SELECT * FROM task_run_snapshot_v2_rebase_definition_edit(
		    'migration-057-owner-forged',1,'migration-057-owner'
		)`); err == nil {
		t.Fatal("function owner bypassed the coordinator role gate")
	} else {
		requireSQLState057(t, err, "42501")
	}
	for _, role := range []string{
		"vane_app",
		"vane_snapshot_cutover_operator",
	} {
		t.Run(role, func(t *testing.T) {
			tx, err := db.BeginTx(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback() }()
			if _, err := tx.ExecContext(
				t.Context(), "SET LOCAL ROLE "+role,
			); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.ExecContext(t.Context(), `
				SELECT * FROM task_run_snapshot_v2_rebase_definition_edit(
				    'migration-057-forged',1,'migration-057-forged'
				)`); err == nil {
				t.Fatalf("%s directly executed the rebase capability", role)
			} else {
				requireSQLState057(t, err, "42501")
			}
		})
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx,
		`SET LOCAL ROLE vane_edit_coordinator`,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx,
		`SELECT set_config('app.tenant_id','1',true)`,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `
		SELECT * FROM task_run_snapshot_v2_rebase_definition_edit(
		    'migration-057-missing-operation',1,'migration-057-worker'
		)`); err == nil {
		t.Fatal("missing operation unexpectedly rebased")
	} else {
		// The coordinator crossed the EXECUTE boundary and then failed closed
		// on the operation/lease capability, rather than on SQL privileges.
		requireSQLState057(t, err, "40001")
	}
}

func TestMigration057DeferredActivePinIntegrityRejectsNakedHeadAdvance(
	t *testing.T,
) {
	f, taskID, _ := newTaskRunSnapshotCutoverControlFixture(t)
	ctx := t.Context()
	if _, err := f.st.ControlTaskRunSnapshotCutover(
		ctx,
		f.tenantID,
		f.userID,
		taskID,
		TaskRunSnapshotCutoverActivate,
	); err != nil {
		t.Fatalf("activate retained-v2 authority: %v", err)
	}
	base, err := f.st.GetCurrentApprovedDefinition(
		ctx, f.tenantID, f.userID, taskID)
	if err != nil {
		t.Fatalf("load base definition: %v", err)
	}
	targetPayload := append(append([]byte(nil), base.Payload...), '\n')
	sum := sha256.Sum256(targetPayload)
	targetDigest := hex.EncodeToString(sum[:])

	tx, err := f.st.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `
		INSERT INTO task_approved_definition_versions (
		    tenant_id,user_id,task_id,version,schema_version,
		    execution_mode,definition_digest,payload,approval_ref
		) VALUES ($1,$2,$3,$4,'approved-definition/v1','compiled',$5,$6,$7)`,
		f.tenantID,
		f.userID,
		taskID,
		base.Version+1,
		targetDigest,
		targetPayload,
		"migration-057-naked-head-"+taskID,
	); err != nil {
		t.Fatalf("insert valid target definition: %v", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE schedules
		   SET approved_definition_version=$4,
		       approved_definition_digest=$5
		 WHERE tenant_id=$1 AND user_id=$2 AND id=$3`,
		f.tenantID,
		f.userID,
		taskID,
		base.Version+1,
		targetDigest,
	); err != nil {
		t.Fatalf("head advance must remain temporarily legal in transaction: %v", err)
	}
	if err := tx.Commit(ctx); err == nil {
		t.Fatal("deferred invariant allowed active authority to commit stale pin")
	} else {
		requireSQLState057(t, err, "23514")
	}

	var headVersion, pointerVersion int64
	var headDigest, pointerDigest string
	if err := f.st.pool.QueryRow(ctx, `
		SELECT s.approved_definition_version,
		       s.approved_definition_digest,
		       e.approved_definition_version,
		       e.approved_definition_digest
		  FROM schedules s
		  JOIN task_run_snapshot_v2_cutover_events e
		    ON e.id=s.run_snapshot_cutover_event_id
		 WHERE s.tenant_id=$1 AND s.user_id=$2 AND s.id=$3`,
		f.tenantID,
		f.userID,
		taskID,
	).Scan(
		&headVersion,
		&headDigest,
		&pointerVersion,
		&pointerDigest,
	); err != nil {
		t.Fatal(err)
	}
	if headVersion != base.Version || headDigest != base.Digest ||
		pointerVersion != base.Version || pointerDigest != base.Digest {
		t.Fatalf(
			"failed drift commit leaked state: head=%d/%s pointer=%d/%s base=%d/%s",
			headVersion,
			headDigest,
			pointerVersion,
			pointerDigest,
			base.Version,
			base.Digest,
		)
	}
}

func TestMigration057CompleteDefinitionEditRebaseCommits(t *testing.T) {
	runFixture, taskID, _ := newTaskRunSnapshotCutoverControlFixture(t)
	ctx := t.Context()
	if _, err := runFixture.st.ControlTaskRunSnapshotCutover(
		ctx,
		runFixture.tenantID,
		runFixture.userID,
		taskID,
		TaskRunSnapshotCutoverActivate,
	); err != nil {
		t.Fatalf("activate retained-v2 authority: %v", err)
	}
	base, err := runFixture.st.GetCurrentApprovedDefinition(
		ctx, runFixture.tenantID, runFixture.userID, taskID)
	if err != nil {
		t.Fatalf("load base definition: %v", err)
	}
	f := newTaskDefinitionEditCutoverFixture(
		t, runFixture, taskID, base)
	acquired, err := f.state.store.AcquireTaskDefinitionEditOperation(
		ctx,
		types.AcquireTaskDefinitionEditOperationParams{
			Scope:           f.op.Scope(),
			LeaseOwner:      "migration-057-rebase-worker",
			LeaseDuration:   5 * time.Minute,
			ReceiptProvider: "feishu_card_patch:app-test",
			ReceiptTarget:   "migration-057-message/card",
		},
	)
	if err != nil {
		t.Fatalf("acquire definition edit: %v", err)
	}
	lease := acquired.Lease()
	if err := f.state.store.QuiesceTaskDefinitionEdit(ctx, lease); err != nil {
		t.Fatalf("quiesce definition edit: %v", err)
	}
	if err := f.state.store.CheckpointTaskDefinitionEditBasePaused(
		ctx,
		lease,
		taskDefinitionEditSnapshotFixture(
			t,
			f,
			definitioneditwire.SnapshotPhaseBasePaused,
			"Aw",
		),
	); err != nil {
		t.Fatalf("checkpoint paused base: %v", err)
	}
	if err := f.state.store.CommitTaskDefinitionEditDefinition(
		ctx, lease,
	); err != nil {
		t.Fatalf("atomic definition/cutover rebase commit: %v", err)
	}

	var (
		headVersion, pointerVersion int64
		headDigest, pointerDigest   string
		ownedEvents                 int
	)
	if err := f.state.store.pool.QueryRow(ctx, `
		SELECT s.approved_definition_version,
		       s.approved_definition_digest,
		       e.approved_definition_version,
		       e.approved_definition_digest,
		       (
		           SELECT count(*)
		             FROM task_run_snapshot_v2_cutover_events owned
		            WHERE owned.definition_edit_operation_id=$4
		       )
		  FROM schedules s
		  JOIN task_run_snapshot_v2_cutover_events e
		    ON e.id=s.run_snapshot_cutover_event_id
		 WHERE s.tenant_id=$1 AND s.user_id=$2 AND s.id=$3
		   AND e.action='activate'`,
		f.op.TargetTenantID,
		f.op.TargetUserID,
		f.op.TaskID,
		f.op.ID,
	).Scan(
		&headVersion,
		&headDigest,
		&pointerVersion,
		&pointerDigest,
		&ownedEvents,
	); err != nil {
		t.Fatal(err)
	}
	if headVersion != f.op.TargetDefinitionVersion ||
		headDigest != f.op.TargetDefinitionDigest ||
		pointerVersion != headVersion ||
		pointerDigest != headDigest ||
		ownedEvents != 2 {
		t.Fatalf(
			"committed rebase state: head=%d/%s pointer=%d/%s events=%d target=%d/%s",
			headVersion,
			headDigest,
			pointerVersion,
			pointerDigest,
			ownedEvents,
			f.op.TargetDefinitionVersion,
			f.op.TargetDefinitionDigest,
		)
	}
}

func TestMigration057RejectsPreexistingCorruptCutoverPointer(t *testing.T) {
	db, provider := migration035Scratch(t)
	ctx := t.Context()
	if _, err := provider.UpTo(ctx, 56); err != nil {
		t.Fatalf("migrate to pre-057 schema: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		ALTER TABLE schedules
		    DROP CONSTRAINT fk_schedules_run_snapshot_cutover_event;
		WITH created_user AS (
		    INSERT INTO users (feishu_open_id,name)
		    VALUES ('migration-057-corrupt-pointer','migration 057 corrupt')
		    RETURNING id
		), created_tenant AS (
		    INSERT INTO tenants (status,plan)
		    VALUES ('active','free')
		    RETURNING id
		), created_membership AS (
		    INSERT INTO memberships (tenant_id,user_id,role)
		    SELECT created_tenant.id,created_user.id,'owner'
		      FROM created_tenant,created_user
		    RETURNING tenant_id,user_id
		)
		INSERT INTO schedules (
		    id,tenant_id,user_id,nl_description,spec_json,scope_json,
		    status,push_strictness,execution_mode,
		    run_snapshot_cutover_event_id
		)
		SELECT 'migration-057-corrupt-pointer',tenant_id,user_id,
		       'corrupt pointer','{}'::jsonb,'{}'::jsonb,
		       'active','normal','compiled',9223372036854770000
		  FROM created_membership`); err != nil {
		t.Fatalf("seed corrupt pre-057 pointer: %v", err)
	}
	if _, err := provider.UpTo(ctx, 57); err == nil ||
		!strings.Contains(err.Error(), "active pin drift") {
		t.Fatalf("057 accepted a corrupt preexisting pointer: %v", err)
	}
	var operationColumnExists bool
	if err := db.QueryRowContext(ctx, `
		SELECT EXISTS (
		    SELECT 1
		      FROM information_schema.columns
		     WHERE table_schema='public'
		       AND table_name='task_run_snapshot_v2_cutover_events'
		       AND column_name='definition_edit_operation_id'
		)`).
		Scan(&operationColumnExists); err != nil {
		t.Fatal(err)
	}
	if operationColumnExists {
		t.Fatal("failed 057 migration leaked partial schema")
	}
}

func requireSQLState057(t *testing.T, err error, want string) {
	t.Helper()
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != want {
		t.Fatalf(
			"SQLSTATE=%q want=%q err=%v",
			func() string {
				if pgErr == nil {
					return ""
				}
				return pgErr.Code
			}(),
			want,
			err,
		)
	}
}
