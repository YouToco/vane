package store

import "testing"

func TestMigration049ExactAuthorityAndDownPreservePriorACL(t *testing.T) {
	f := pushBatchAuthorityFixture(t)
	ctx := t.Context()
	if _, err := f.provider.UpTo(ctx, 49); err != nil {
		t.Fatalf("migrate to 049: %v", err)
	}
	var (
		snapshotOld, snapshotNew, snapshotPayload bool
		tenantOld, tenantNew                      bool
		scheduleNew, membershipNew, pendingNew    bool
		deferCoordinator, deferPublic             bool
		conflictCoordinator, conflictPublic       bool
		exhaustedCoordinator, exhaustedPublic     bool
		securityDefinerFunctions, fixedSearchPath int
		policies                                  int
	)
	if err := f.db.QueryRowContext(ctx, `
		SELECT
		  has_column_privilege(
		    'vane_push_effect_coordinator','task_run_snapshots','id','SELECT'),
		  has_column_privilege(
		    'vane_push_effect_coordinator','task_run_snapshots',
		    'reference_digest','SELECT'),
		  has_column_privilege(
		    'vane_push_effect_coordinator','task_run_snapshots','payload','SELECT'),
		  has_column_privilege(
		    'vane_push_effect_coordinator','tenants','id','SELECT'),
		  has_column_privilege(
		    'vane_push_effect_coordinator','tenants','status','SELECT'),
		  has_column_privilege(
		    'vane_push_effect_coordinator','schedules','status','SELECT'),
		  has_column_privilege(
		    'vane_push_effect_coordinator','memberships','tenant_id','SELECT'),
		  has_column_privilege(
		    'vane_push_effect_coordinator','pending_actions','status','SELECT'),
		  has_function_privilege(
		    'vane_push_effect_coordinator',
		    'defer_or_block_push_effect_reconciliation_v1(' ||
		    'text,bigint,bigint,bigint,bigint,boolean)','EXECUTE'),
		  has_function_privilege(
		    'public',
		    'defer_or_block_push_effect_reconciliation_v1(' ||
		    'text,bigint,bigint,bigint,bigint,boolean)','EXECUTE'),
		  has_function_privilege(
		    'vane_push_effect_coordinator',
		    'block_conflicting_push_effect_history_v1(' ||
		    'text,bigint,bigint,bigint)','EXECUTE'),
		  has_function_privilege(
		    'public',
		    'block_conflicting_push_effect_history_v1(' ||
		    'text,bigint,bigint,bigint)','EXECUTE'),
		  has_function_privilege(
		    'vane_push_effect_coordinator',
		    'block_exhausted_push_effect_attempts_v1(' ||
		    'text,bigint,bigint,bigint,text)','EXECUTE'),
		  has_function_privilege(
		    'public',
		    'block_exhausted_push_effect_attempts_v1(' ||
		    'text,bigint,bigint,bigint,text)','EXECUTE'),
		  (SELECT count(*) FROM pg_proc p
		    JOIN pg_namespace n ON n.oid=p.pronamespace
		    WHERE n.nspname='public'
		      AND p.proname IN (
		        'defer_or_block_push_effect_reconciliation_v1',
		        'block_conflicting_push_effect_history_v1',
		        'block_exhausted_push_effect_attempts_v1')
		      AND p.prosecdef),
		  (SELECT count(*) FROM pg_proc p
		    JOIN pg_namespace n ON n.oid=p.pronamespace
		    WHERE n.nspname='public'
		      AND p.proname IN (
		        'defer_or_block_push_effect_reconciliation_v1',
		        'block_conflicting_push_effect_history_v1',
		        'block_exhausted_push_effect_attempts_v1')
		      AND p.proconfig @>
		          ARRAY['search_path=pg_catalog, public']::text[]),
		  (SELECT count(*) FROM pg_policies
		    WHERE schemaname='public'
		      AND policyname='push_effect_coordinator_tenant_isolation'
		      AND tablename IN ('tenants','memberships'))`,
	).Scan(
		&snapshotOld, &snapshotNew, &snapshotPayload,
		&tenantOld, &tenantNew,
		&scheduleNew, &membershipNew, &pendingNew,
		&deferCoordinator, &deferPublic,
		&conflictCoordinator, &conflictPublic,
		&exhaustedCoordinator, &exhaustedPublic,
		&securityDefinerFunctions, &fixedSearchPath,
		&policies,
	); err != nil {
		t.Fatal(err)
	}
	if !snapshotOld || !snapshotNew || snapshotPayload ||
		!tenantOld || !tenantNew ||
		!scheduleNew || !membershipNew || !pendingNew ||
		!deferCoordinator || deferPublic ||
		!conflictCoordinator || conflictPublic ||
		!exhaustedCoordinator || exhaustedPublic ||
		securityDefinerFunctions != 3 || fixedSearchPath != 3 ||
		policies != 2 {
		t.Fatalf(
			"049 ACL drift snapshot=%v/%v/payload:%v tenant=%v/%v "+
				"schedule/membership/pending=%v/%v/%v "+
				"functions=%v/%v/%v/%v/%v/%v security=%d/path=%d "+
				"policies=%d",
			snapshotOld, snapshotNew, snapshotPayload,
			tenantOld, tenantNew,
			scheduleNew, membershipNew, pendingNew,
			deferCoordinator, deferPublic,
			conflictCoordinator, conflictPublic,
			exhaustedCoordinator, exhaustedPublic,
			securityDefinerFunctions, fixedSearchPath, policies,
		)
	}

	if _, err := f.provider.Down(ctx); err != nil {
		t.Fatalf("049 down: %v", err)
	}
	var (
		version                            int
		snapshotOldAfter, snapshotNewAfter bool
		tenantOldAfter, tenantNewAfter     bool
		scheduleAfter, membershipAfter     bool
		pendingAfter                       bool
		deferExists, conflictExists        bool
		exhaustedExists                    bool
		remainingPolicies                  int
	)
	if err := f.db.QueryRowContext(ctx, `
		SELECT
		  (SELECT COALESCE(max(version_id),0)
		     FROM goose_db_version WHERE is_applied),
		  has_column_privilege(
		    'vane_push_effect_coordinator','task_run_snapshots','id','SELECT'),
		  has_column_privilege(
		    'vane_push_effect_coordinator','task_run_snapshots',
		    'reference_digest','SELECT'),
		  has_column_privilege(
		    'vane_push_effect_coordinator','tenants','id','SELECT'),
		  has_column_privilege(
		    'vane_push_effect_coordinator','tenants','status','SELECT'),
		  has_column_privilege(
		    'vane_push_effect_coordinator','schedules','status','SELECT'),
		  has_column_privilege(
		    'vane_push_effect_coordinator','memberships','tenant_id','SELECT'),
		  has_column_privilege(
		    'vane_push_effect_coordinator','pending_actions','status','SELECT'),
		  to_regprocedure(
		    'defer_or_block_push_effect_reconciliation_v1(' ||
		    'text,bigint,bigint,bigint,bigint,boolean)') IS NOT NULL,
		  to_regprocedure(
		    'block_conflicting_push_effect_history_v1(' ||
		    'text,bigint,bigint,bigint)') IS NOT NULL,
		  to_regprocedure(
		    'block_exhausted_push_effect_attempts_v1(' ||
		    'text,bigint,bigint,bigint,text)') IS NOT NULL,
		  (SELECT count(*) FROM pg_policies
		    WHERE schemaname='public'
		      AND policyname='push_effect_coordinator_tenant_isolation'
		      AND tablename IN ('tenants','memberships'))`,
	).Scan(
		&version,
		&snapshotOldAfter, &snapshotNewAfter,
		&tenantOldAfter, &tenantNewAfter,
		&scheduleAfter, &membershipAfter, &pendingAfter,
		&deferExists, &conflictExists, &exhaustedExists,
		&remainingPolicies,
	); err != nil {
		t.Fatal(err)
	}
	if version != 48 ||
		!snapshotOldAfter || snapshotNewAfter ||
		!tenantOldAfter || tenantNewAfter ||
		scheduleAfter || membershipAfter || pendingAfter ||
		deferExists || conflictExists || exhaustedExists ||
		remainingPolicies != 0 {
		t.Fatalf(
			"049 Down drift version=%d snapshot=%v/%v tenant=%v/%v "+
				"schedule/membership/pending=%v/%v/%v "+
				"functions=%v/%v/%v policies=%d",
			version,
			snapshotOldAfter, snapshotNewAfter,
			tenantOldAfter, tenantNewAfter,
			scheduleAfter, membershipAfter, pendingAfter,
			deferExists, conflictExists, exhaustedExists, remainingPolicies,
		)
	}
	if _, err := f.provider.UpTo(ctx, 49); err != nil {
		t.Fatalf("049 re-up: %v", err)
	}
	var reupVersion int
	if err := f.db.QueryRowContext(ctx, `
		SELECT COALESCE(max(version_id),0)
		  FROM goose_db_version WHERE is_applied`,
	).Scan(&reupVersion); err != nil {
		t.Fatal(err)
	}
	if reupVersion != 49 {
		t.Fatalf("049 re-up version=%d", reupVersion)
	}
}
