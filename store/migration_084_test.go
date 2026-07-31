package store

import (
	"strings"
	"testing"
)

func TestMigration084TaskDeleteCascadesImmutablePeriodicReport(t *testing.T) {
	_, db, provider := openMigration066Database(
		t, "vane_periodic_report_task_delete_084")
	if _, err := provider.UpTo(t.Context(), 84); err != nil {
		t.Fatal(err)
	}

	var userID int64
	if err := db.QueryRowContext(t.Context(), `
		INSERT INTO users(feishu_open_id,name)
		VALUES('migration-084-delete','periodic task delete')
		RETURNING id`,
	).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `
		INSERT INTO memberships(tenant_id,user_id,role)
		VALUES(1,$1,'owner')`, userID); err != nil {
		t.Fatal(err)
	}

	const taskID = "task-periodic-delete-084"
	if _, err := db.ExecContext(t.Context(), `
		INSERT INTO schedules (
		    id,user_id,tenant_id,nl_description,spec_json,scope_json,status
		) VALUES ($1,$2,1,'periodic cascade acceptance',
		          '{"cron":"0 8 * * *","timezone":"UTC"}','{}','paused')`,
		taskID, userID); err != nil {
		t.Fatal(err)
	}

	var intentID int64
	if err := db.QueryRowContext(t.Context(), `
		INSERT INTO periodic_brief_intents (
		    tenant_id,user_id,task_id,cadence,timezone,
		    period_start,period_end,workflow_id,input_digest,
		    outcome_digest,source_coverage,processing,status
		) VALUES (
		    1,$1,$2,'daily','UTC',
		    '2026-07-29T00:00:00Z','2026-07-30T00:00:00Z',
		    'periodic-delete-084',repeat('a',64),repeat('b',64),
		    'complete','complete','prepared'
		) RETURNING id`, userID, taskID,
	).Scan(&intentID); err != nil {
		t.Fatal(err)
	}

	const payloadDigest = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	var reportID int64
	if err := db.QueryRowContext(t.Context(), `
		INSERT INTO periodic_brief_reports (
		    intent_id,tenant_id,user_id,task_id,cadence,
		    period_start,period_end,schema_version,request_digest,
		    payload_digest,payload,generated_at
		) VALUES (
		    $1,1,$2,$3,'daily',
		    '2026-07-29T00:00:00Z','2026-07-30T00:00:00Z',
		    'vane.periodic-brief/v1',repeat('d',64),$4,
		    convert_to(jsonb_build_object('digest',$4)::text,'UTF8'),
		    '2026-07-30T00:01:00Z'
		) RETURNING id`, intentID, userID, taskID, payloadDigest,
	).Scan(&reportID); err != nil {
		t.Fatal(err)
	}

	if _, err := db.ExecContext(t.Context(),
		`DELETE FROM periodic_brief_reports WHERE id=$1`, reportID,
	); err == nil || !strings.Contains(err.Error(), "reports are immutable") {
		t.Fatalf("direct report delete error = %v", err)
	}

	if _, err := db.ExecContext(t.Context(),
		`DELETE FROM schedules WHERE tenant_id=1 AND user_id=$1 AND id=$2`,
		userID, taskID,
	); err != nil {
		t.Fatalf("delete schedule with periodic report: %v", err)
	}
	for table, query := range map[string]string{
		"schedules":              `SELECT count(*) FROM schedules WHERE id=$1`,
		"periodic_brief_intents": `SELECT count(*) FROM periodic_brief_intents WHERE task_id=$1`,
		"periodic_brief_reports": `SELECT count(*) FROM periodic_brief_reports WHERE task_id=$1`,
	} {
		var count int
		if err := db.QueryRowContext(t.Context(), query, taskID).Scan(&count); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		if count != 0 {
			t.Errorf("%s rows after task delete = %d", table, count)
		}
	}
}
