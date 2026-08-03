package store

import (
	"context"
	"testing"
	"time"
)

func TestMigration113FeedbackIntelligenceIsolationAndSemanticsPostgres(t *testing.T) {
	dbURL, db, provider := openMigration066Database(
		t, "vane_feedback_intelligence_113",
	)
	if _, err := provider.UpTo(t.Context(), 111); err != nil {
		t.Fatal(err)
	}
	// Production provisions vane_server_runtime as the intentional NOINHERIT
	// member of vane_intelligence_reader. Migration 113 must accept that exact
	// role graph while rejecting every additional reader member.
	if _, err := db.ExecContext(t.Context(),
		`SELECT provision_vane_server_runtime_v2()`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(),
			`ALTER ROLE vane_server_runtime NOLOGIN`)
		_, _ = db.ExecContext(context.Background(),
			`SELECT deprovision_vane_server_runtime_v2()`)
	})
	if _, err := db.ExecContext(t.Context(),
		`ALTER ROLE vane_server_runtime REPLICATION`); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.UpTo(t.Context(), 113); err == nil {
		t.Fatal("migration 113 accepted replication-capable server runtime")
	}
	if _, err := db.ExecContext(t.Context(),
		`ALTER ROLE vane_server_runtime NOREPLICATION`); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.UpTo(t.Context(), 113); err != nil {
		t.Fatal(err)
	}

	createUser := func(openID string, tenantID int64) (userID, sessionID int64) {
		t.Helper()
		if err := db.QueryRowContext(t.Context(), `
			INSERT INTO users(feishu_open_id,name) VALUES($1,$1) RETURNING id`,
			openID,
		).Scan(&userID); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(t.Context(), `
			INSERT INTO memberships(tenant_id,user_id,role)
			VALUES($1,$2,'owner')`, tenantID, userID); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRowContext(t.Context(), `
			INSERT INTO agent_sessions(tenant_id,user_id)
			VALUES($1,$2) RETURNING id`, tenantID, userID,
		).Scan(&sessionID); err != nil {
			t.Fatal(err)
		}
		return
	}
	userA, sessionA := createUser("feedback-intelligence-a-113", 1)
	userSameTenant, sessionSameTenant := createUser(
		"feedback-intelligence-b-113", 1,
	)
	var tenantB int64
	if err := db.QueryRowContext(t.Context(), `
		INSERT INTO tenants(status,plan) VALUES('active','free') RETURNING id`,
	).Scan(&tenantB); err != nil {
		t.Fatal(err)
	}
	userOtherTenant, sessionOtherTenant := createUser(
		"feedback-intelligence-c-113", tenantB,
	)

	createFeedbacks := func(
		tenantID, userID int64, taskID string,
	) {
		t.Helper()
		if _, err := db.ExecContext(t.Context(), `
			INSERT INTO schedules(
			    id,tenant_id,user_id,nl_description,spec_json,scope_json,status
			) VALUES($1,$2,$3,$1,
			         '{"cron":"0 9 * * 1","timezone":"Asia/Shanghai"}',
			         '{}','paused')`, taskID, tenantID, userID); err != nil {
			t.Fatal(err)
		}
		var batchID int64
		if err := db.QueryRowContext(t.Context(), `
			INSERT INTO push_batches(tenant_id,user_id,schedule_id,status)
			VALUES($1,$2,$3,'pending') RETURNING id`,
			tenantID, userID, taskID,
		).Scan(&batchID); err != nil {
			t.Fatal(err)
		}
		var supersededDeliveryID, explicitDeliveryID int64
		if err := db.QueryRowContext(t.Context(), `
			INSERT INTO deliveries(tenant_id,user_id,batch_id,status,body_md)
			VALUES($1,$2,$3,'sent','当时推送：Kimi 套餐尚不可购买') RETURNING id`,
			tenantID, userID, batchID,
		).Scan(&supersededDeliveryID); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRowContext(t.Context(), `
			INSERT INTO deliveries(tenant_id,user_id,batch_id,status,body_md)
			VALUES($1,$2,$3,'sent','另一条推送：明确不感兴趣') RETURNING id`,
			tenantID, userID, batchID,
		).Scan(&explicitDeliveryID); err != nil {
			t.Fatal(err)
		}
		at := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
		if _, err := db.ExecContext(t.Context(), `
			INSERT INTO feedbacks(
			    tenant_id,user_id,delivery_id,action,reason_code,detail,created_at
			) VALUES
			    ($1,$2,$3,'interested',NULL,'',$4),
			    ($1,$2,$3,'not_interested',NULL,'',$4),
			    ($1,$2,$3,'misjudged','factually_wrong','官方原文相反',$4-interval '1 hour'),
			    ($1,$2,$5,'not_interested',NULL,'明确不感兴趣',$4)`,
			tenantID, userID, supersededDeliveryID, at, explicitDeliveryID,
		); err != nil {
			t.Fatal(err)
		}
	}
	createFeedbacks(1, userA, "feedback-task-a-113")
	createFeedbacks(1, userSameTenant, "feedback-task-b-113")
	createFeedbacks(tenantB, userOtherTenant, "feedback-task-c-113")

	st, err := New(t.Context(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	query := IntelligenceQuery{
		Dataset: IntelligenceFeedbacks,
		Select: []string{
			"task_ref", "delivered_summary", "action", "reason_code", "detail",
			"is_effective_attitude", "created_at",
		},
		OrderBy: []IntelligenceOrder{
			{Field: "created_at", Direction: "asc"},
			{Field: "record_id", Direction: "asc"},
		},
		Limit: 10,
	}
	result, err := st.QueryMyIntelligence(t.Context(), IntelligenceScope{
		TenantID: 1, UserID: userA, SessionID: &sessionA,
	}, query)
	if err != nil {
		t.Fatal(err)
	}
	if result.CatalogVersion != IntelligenceCatalogVersion ||
		result.Dataset != IntelligenceFeedbacks || len(result.Rows) != 4 {
		t.Fatalf("feedback result=%+v", result)
	}
	if result.Rows[0]["action"] != "misjudged" ||
		result.Rows[0]["reason_code"] != "factually_wrong" ||
		result.Rows[0]["detail"] != "官方原文相反" ||
		result.Rows[0]["is_effective_attitude"] != nil ||
		result.Rows[1]["action"] != "interested" ||
		result.Rows[1]["is_effective_attitude"] != false ||
		result.Rows[2]["action"] != "not_interested" ||
		result.Rows[2]["detail"] != "" ||
		result.Rows[2]["is_effective_attitude"] != false ||
		result.Rows[3]["action"] != "not_interested" ||
		result.Rows[3]["detail"] != "明确不感兴趣" ||
		result.Rows[3]["is_effective_attitude"] != true {
		t.Fatalf("effective attitude rows=%+v", result.Rows)
	}
	for index, row := range result.Rows {
		if row["task_ref"] != "feedback-task-a-113" {
			t.Fatalf("cross-subject feedback row=%+v", row)
		}
		wantSummary := "当时推送：Kimi 套餐尚不可购买"
		if index == 3 {
			wantSummary = "另一条推送：明确不感兴趣"
		}
		if row["delivered_summary"] != wantSummary {
			t.Fatalf("feedback lost delivered summary row=%+v", row)
		}
	}

	taskScoped, err := st.QueryMyIntelligence(t.Context(), IntelligenceScope{
		TenantID: 1, UserID: userA, SessionID: &sessionA,
		TaskID: "feedback-task-a-113",
	}, IntelligenceQuery{
		Dataset: IntelligenceFeedbacks,
		Select:  []string{"task_ref", "action", "created_at"}, Limit: 10,
	})
	if err != nil || len(taskScoped.Rows) != 4 {
		t.Fatalf("exact-task feedback rows=%+v err=%v", taskScoped, err)
	}
	crossTask, err := st.QueryMyIntelligence(t.Context(), IntelligenceScope{
		TenantID: 1, UserID: userA, SessionID: &sessionA,
		TaskID: "feedback-task-b-113",
	}, IntelligenceQuery{Dataset: IntelligenceFeedbacks, Limit: 10})
	if err != nil || len(crossTask.Rows) != 0 {
		t.Fatalf("cross-task feedback rows=%+v err=%v", crossTask, err)
	}

	for _, scope := range []IntelligenceScope{
		{TenantID: 1, UserID: userSameTenant, SessionID: &sessionSameTenant},
		{TenantID: tenantB, UserID: userOtherTenant, SessionID: &sessionOtherTenant},
	} {
		owned, err := st.QueryMyIntelligence(t.Context(), scope,
			IntelligenceQuery{Dataset: IntelligenceFeedbacks, Limit: 10})
		if err != nil || len(owned.Rows) != 4 {
			t.Fatalf("owned feedback scope=%+v rows=%+v err=%v", scope, owned, err)
		}
	}

	var audits int
	if err := db.QueryRowContext(t.Context(), `
		SELECT count(*) FROM agent_intelligence_query_audits
		 WHERE tenant_id=1 AND user_id=$1 AND dataset='feedbacks'`,
		userA,
	).Scan(&audits); err != nil {
		t.Fatal(err)
	}
	if audits != 3 {
		t.Fatalf("feedback audit count=%d, want 3", audits)
	}

	var tableSelect, bodyMDSelect, cardJSONSelect, policyCount bool
	if err := db.QueryRowContext(t.Context(), `
		SELECT
		  has_table_privilege('vane_intelligence_reader','feedbacks','SELECT'),
		  has_column_privilege('vane_intelligence_reader','deliveries','body_md','SELECT'),
		  has_column_privilege('vane_intelligence_reader','deliveries','card_json','SELECT'),
		  (SELECT count(*)=5 FROM pg_policies
		    WHERE policyname='intelligence_feedback_identity'
		      AND tablename IN (
		        'feedbacks','deliveries','push_batches','profiles','profile_claim_states'
		      ))`,
	).Scan(&tableSelect, &bodyMDSelect, &cardJSONSelect, &policyCount); err != nil {
		t.Fatal(err)
	}
	if tableSelect || !bodyMDSelect || cardJSONSelect || !policyCount {
		t.Fatalf("feedback reader capability table=%v body_md=%v card_json=%v policies=%v",
			tableSelect, bodyMDSelect, cardJSONSelect, policyCount)
	}
}
