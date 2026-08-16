package store

import (
	"testing"

	"github.com/google/uuid"

	"github.com/YouToco/vane/server/types"
)

func TestAggregateTelegramOnlySettlementDoesNotForgeFeishuReceiptPG(t *testing.T) {
	dbURL := freshMigrationDatabase(t, "aggregate_telegram_settlement")
	if err := migrate(t.Context(), dbURL, 0); err != nil {
		t.Fatal(err)
	}
	st, err := New(t.Context(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	userID := testUser(t, st)
	var tenantID int64
	if err := st.pool.QueryRow(t.Context(),
		`INSERT INTO tenants(status,plan) VALUES ('active','free') RETURNING id`,
	).Scan(&tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(t.Context(),
		`INSERT INTO memberships(tenant_id,user_id,role) VALUES ($1,$2,'owner')`,
		tenantID, userID); err != nil {
		t.Fatal(err)
	}
	const taskID = "aggregate-channel-task"
	if _, err := st.pool.Exec(t.Context(),
		`INSERT INTO schedules(id,tenant_id,user_id,nl_description)
		 VALUES ($1,$2,$3,'aggregate channel')`, taskID, tenantID, userID); err != nil {
		t.Fatal(err)
	}
	var identityID, routeID int64
	if err := st.pool.QueryRow(t.Context(),
		`INSERT INTO channel_identities
		 (tenant_id,user_id,provider,app_identity,external_user_id,
		  provider_chat_id,chat_type)
		 VALUES ($1,$2,'telegram','9201','7201','-1007201','supergroup')
		 RETURNING id`, tenantID, userID).Scan(&identityID); err != nil {
		t.Fatal(err)
	}
	if err := st.pool.QueryRow(t.Context(),
		`INSERT INTO channel_routes
		 (tenant_id,user_id,identity_id,provider,app_identity,provider_chat_id,
		  provider_thread_id,chat_type,route_kind)
		 VALUES ($1,$2,$3,'telegram','9201','-1007201','88','supergroup','topic')
		 RETURNING id`, tenantID, userID, identityID).Scan(&routeID); err != nil {
		t.Fatal(err)
	}
	preference, err := st.PutTaskDeliveryChannelPreference(t.Context(),
		tenantID, userID, taskID, DeliveryChannelTelegram, &routeID)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := st.PrepareArtifactDeliveryPlan(t.Context(), tenantID, userID,
		taskID, ArtifactDeliveryAggregateBrief, "snapshot:run:batch", preference)
	if err != nil {
		t.Fatal(err)
	}
	var batchID, deliveryID int64
	if err := st.pool.QueryRow(t.Context(),
		`INSERT INTO push_batches
		 (tenant_id,user_id,status,idempotency_key,schedule_id)
		 VALUES ($1,$2,'pending',$3,$4) RETURNING id`, tenantID, userID,
		"aggregate-channel-"+uuid.NewString(), taskID).Scan(&batchID); err != nil {
		t.Fatal(err)
	}
	winner, err := st.ClaimPushBatchDeliveryAuthority(t.Context(),
		types.PushBatchScope{TenantID: tenantID, UserID: userID, BatchID: batchID},
		types.PushBatchDeliveryAuthorityEffect)
	if err != nil || winner != types.PushBatchDeliveryAuthorityEffect {
		t.Fatalf("authority=%q err=%v", winner, err)
	}
	if err := st.pool.QueryRow(t.Context(),
		`INSERT INTO deliveries
		 (tenant_id,batch_id,user_id,score,body_md,card_json,status)
		 VALUES ($1,$2,$3,80,'body','{}'::jsonb,'pending') RETURNING id`,
		tenantID, batchID, userID).Scan(&deliveryID); err != nil {
		t.Fatal(err)
	}
	effectID := uuid.NewString()
	prepared, err := st.PrepareAggregateTelegramOutbound(t.Context(), plan.ID,
		tenantID, userID, taskID, batchID, 0, 1, []int64{deliveryID},
		routeID, effectID, "aggregate body")
	if err != nil || prepared.Status != "prepared" {
		t.Fatalf("prepared=%+v err=%v", prepared, err)
	}
	claimed, err := st.ClaimTelegramOutbound(t.Context(), effectID)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.CompleteTelegramOutbound(t.Context(), claimed, []string{"812"}); err != nil {
		t.Fatal(err)
	}
	var atomicDeliveryStatus, atomicBatchStatus, atomicMappingStatus string
	if err := st.pool.QueryRow(t.Context(),
		`SELECT d.status,b.status,m.status
		   FROM deliveries d
		   JOIN push_batches b ON b.id=d.batch_id
		   JOIN aggregate_channel_delivery_effects m
		     ON m.channel_effect_id=$2
		  WHERE d.id=$1`, deliveryID, effectID).Scan(
		&atomicDeliveryStatus, &atomicBatchStatus, &atomicMappingStatus); err != nil {
		t.Fatal(err)
	}
	if atomicDeliveryStatus != string(types.DeliveryStatusSent) ||
		atomicBatchStatus != string(types.BatchStatusDone) ||
		atomicMappingStatus != "sent" {
		t.Fatalf("atomic settlement delivery=%s batch=%s mapping=%s",
			atomicDeliveryStatus, atomicBatchStatus, atomicMappingStatus)
	}
	if err := st.SettleAggregateTelegramOutbound(t.Context(), tenantID, userID,
		plan.ID, effectID); err != nil {
		t.Fatal(err)
	}
	if err := st.SettleAggregateTelegramOutbound(t.Context(), tenantID, userID,
		plan.ID, effectID); err != nil {
		t.Fatalf("settlement replay: %v", err)
	}
	var deliveryStatus, feishuMessageID, batchStatus, mappingStatus string
	if err := st.pool.QueryRow(t.Context(),
		`SELECT status,feishu_message_id FROM deliveries WHERE id=$1`,
		deliveryID).Scan(&deliveryStatus, &feishuMessageID); err != nil {
		t.Fatal(err)
	}
	if err := st.pool.QueryRow(t.Context(),
		`SELECT status FROM push_batches WHERE id=$1`, batchID).Scan(&batchStatus); err != nil {
		t.Fatal(err)
	}
	if err := st.pool.QueryRow(t.Context(),
		`SELECT status FROM aggregate_channel_delivery_effects
		  WHERE channel_effect_id=$1`, effectID).Scan(&mappingStatus); err != nil {
		t.Fatal(err)
	}
	if deliveryStatus != string(types.DeliveryStatusSent) ||
		feishuMessageID != "" || batchStatus != string(types.BatchStatusDone) ||
		mappingStatus != "sent" {
		t.Fatalf("delivery=%s feishu=%q batch=%s mapping=%s",
			deliveryStatus, feishuMessageID, batchStatus, mappingStatus)
	}
}
