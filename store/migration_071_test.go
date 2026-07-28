package store

import (
	"strings"
	"testing"
)

func TestMigration071PeriodicRolesRLSAndEmptyDown(t *testing.T) {
	_, db, provider := openMigration066Database(
		t, "vane_periodic_brief_071_acl")
	if _, err := provider.UpTo(t.Context(), 71); err != nil {
		t.Fatal(err)
	}
	var canLogin, inherit, bypassRLS, superuser bool
	if err := db.QueryRowContext(t.Context(), `
		SELECT rolcanlogin,rolinherit,rolbypassrls,rolsuper
		  FROM pg_roles WHERE rolname='vane_periodic_brief_writer'
	`).Scan(&canLogin, &inherit, &bypassRLS, &superuser); err != nil {
		t.Fatal(err)
	}
	if canLogin || inherit || bypassRLS || superuser {
		t.Fatalf("unsafe periodic writer role: %t/%t/%t/%t",
			canLogin, inherit, bypassRLS, superuser)
	}
	for _, table := range []string{
		"brief_report_settings", "periodic_brief_intents",
		"periodic_synthesis_receipts", "periodic_brief_reports",
		"periodic_report_deliveries",
	} {
		var rls bool
		if err := db.QueryRowContext(t.Context(),
			`SELECT relrowsecurity FROM pg_class
			  WHERE oid=$1::regclass`, table).Scan(&rls); err != nil {
			t.Fatal(err)
		}
		if !rls {
			t.Fatalf("%s has RLS disabled", table)
		}
	}
	var appCanRead, writerCanDelete, readerCanRead,
		recoveryCanReadDelivery, recoveryCanReadReport,
		recoveryCanListMissing bool
	if err := db.QueryRowContext(t.Context(), `
		SELECT
		  has_table_privilege(
		    'vane_app','periodic_brief_reports','SELECT'),
		  has_table_privilege(
		    'vane_periodic_brief_writer',
		    'periodic_synthesis_receipts','DELETE'),
		  has_table_privilege(
		    'vane_brief_reader','periodic_brief_reports','SELECT'),
		  has_table_privilege(
		    'vane_brief_synthesis_recovery',
		    'periodic_report_deliveries','SELECT'),
		  has_table_privilege(
		    'vane_brief_synthesis_recovery',
		    'periodic_brief_reports','SELECT'),
		  has_function_privilege(
		    'vane_brief_synthesis_recovery',
		    'read_periodic_missing_delivery_recovery_v1(bigint,integer)',
		    'EXECUTE')
	`).Scan(
		&appCanRead, &writerCanDelete, &readerCanRead,
		&recoveryCanReadDelivery, &recoveryCanReadReport,
		&recoveryCanListMissing); err != nil {
		t.Fatal(err)
	}
	if appCanRead || writerCanDelete || !readerCanRead ||
		recoveryCanReadDelivery || recoveryCanReadReport ||
		!recoveryCanListMissing {
		t.Fatalf(
			"periodic ACL drifted: app_read=%v writer_delete=%v reader_read=%v recovery_delivery_read=%v recovery_report_read=%v recovery_missing_execute=%v",
			appCanRead, writerCanDelete, readerCanRead,
			recoveryCanReadDelivery, recoveryCanReadReport,
			recoveryCanListMissing)
	}
	var recoveryReportInsert, recoveryReportIDInsert bool
	if err := db.QueryRowContext(t.Context(), `
		SELECT has_table_privilege(
		           'vane_brief_synthesis_recovery',
		           'periodic_brief_reports','INSERT'),
		       has_column_privilege(
		           'vane_brief_synthesis_recovery',
		           'periodic_brief_reports','id','INSERT')`,
	).Scan(&recoveryReportInsert, &recoveryReportIDInsert); err != nil {
		t.Fatal(err)
	}
	if recoveryReportInsert || !recoveryReportIDInsert {
		t.Fatalf("recovery report INSERT boundary = %t/%t",
			recoveryReportInsert, recoveryReportIDInsert)
	}
	if _, err := provider.DownTo(t.Context(), 70); err != nil {
		t.Fatalf("empty 071 Down failed: %v", err)
	}
	var writerExists, writerPrivileged bool
	if err := db.QueryRowContext(t.Context(), `
		SELECT EXISTS (
		           SELECT 1 FROM pg_roles
		            WHERE rolname='vane_periodic_brief_writer'
		       ),
		       has_any_column_privilege(
		           'vane_periodic_brief_writer',
		           'brief_snapshots','SELECT')
		    OR has_any_column_privilege(
		           'vane_periodic_brief_writer',
		           'task_run_outcomes','SELECT')
	`).Scan(&writerExists, &writerPrivileged); err != nil {
		t.Fatal(err)
	}
	if !writerExists || writerPrivileged {
		t.Fatalf("071 Down role boundary exists=%v privileged=%v",
			writerExists, writerPrivileged)
	}
}

func TestMigration071DownRefusesPreparedIntent(t *testing.T) {
	_, db, provider := openMigration066Database(
		t, "vane_periodic_brief_071_pending_down")
	if _, err := provider.UpTo(t.Context(), 71); err != nil {
		t.Fatal(err)
	}
	var userID int64
	if err := db.QueryRowContext(t.Context(), `
		INSERT INTO users(feishu_open_id,name)
		VALUES('migration-071-pending','periodic Down')
		RETURNING id`,
	).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `
		INSERT INTO memberships(tenant_id,user_id,role)
		VALUES(1,$1,'owner')`, userID); err != nil {
		t.Fatal(err)
	}
	taskID := "task-periodic-071-pending"
	if _, err := db.ExecContext(t.Context(), `
		INSERT INTO schedules (
		    id,user_id,tenant_id,nl_description,spec_json,scope_json,status
		) VALUES ($1,$2,1,'periodic down fence',
		          '{"cron":"0 8 * * *","timezone":"UTC"}','{}','active')`,
		taskID, userID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `
		INSERT INTO periodic_brief_intents (
		    tenant_id,user_id,task_id,cadence,timezone,
		    period_start,period_end,workflow_id,input_digest,
		    outcome_digest,source_coverage,processing,status
		) VALUES (
		    1,$1,$2,'daily','UTC',
		    '2026-07-26T00:00:00Z','2026-07-27T00:00:00Z',
		    'periodic-071-pending',
		    repeat('a',64),repeat('b',64),
		    'partial','partial','prepared'
		)`, userID, taskID); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.DownTo(
		t.Context(), 70,
	); err == nil || !strings.Contains(
		err.Error(),
		"refusing Down while periodic Brief state exists",
	) {
		t.Fatalf("071 pending Down error=%v", err)
	}
	var intentExists bool
	if err := db.QueryRowContext(t.Context(),
		`SELECT to_regclass('public.periodic_brief_intents') IS NOT NULL`,
	).Scan(&intentExists); err != nil {
		t.Fatal(err)
	}
	if !intentExists {
		t.Fatal("071 pending Down removed periodic state")
	}
}
