package store

import (
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/types"
)

func TestLatestSentDeliveryMessageID(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL，跳过 delivery receipt 集成测试")
	}
	ctx := t.Context()
	if err := Migrate(ctx, dbURL); err != nil {
		t.Fatalf("Migrate() 执行失败: %v", err)
	}
	st, err := New(ctx, dbURL)
	if err != nil {
		t.Fatalf("New() 建池失败: %v", err)
	}
	registerStoreClose(t, st)

	owner, err := st.UpsertUserByOpenID(ctx, "ou_receipt_"+uuid.NewString(), "receipt-owner")
	if err != nil {
		t.Fatalf("创建 owner: %v", err)
	}
	attachTenant(t, st, owner.ID)
	other, err := st.UpsertUserByOpenID(ctx, "ou_receipt_"+uuid.NewString(), "receipt-other")
	if err != nil {
		t.Fatalf("创建 other: %v", err)
	}
	attachTenant(t, st, other.ID)

	ownerBatch, err := st.CreatePushBatch(ctx, owner.ID)
	if err != nil {
		t.Fatalf("创建 owner batch: %v", err)
	}
	otherBatch, err := st.CreatePushBatch(ctx, other.ID)
	if err != nil {
		t.Fatalf("创建 other batch: %v", err)
	}
	insert := func(userID, batchID int64) int64 {
		t.Helper()
		id, insertErr := st.InsertDelivery(ctx, &types.Delivery{
			BatchID: batchID,
			UserID:  userID,
			Score:   1,
			BodyMD:  "receipt fixture",
		})
		if insertErr != nil {
			t.Fatalf("InsertDelivery(): %v", insertErr)
		}
		return id
	}
	older := insert(owner.ID, ownerBatch)
	newer := insert(owner.ID, ownerBatch)
	pending := insert(owner.ID, ownerBatch)
	otherDelivery := insert(other.ID, otherBatch)
	baseTime := time.Date(2026, 7, 25, 0, 0, 0, 0, time.UTC)
	if err := st.MarkDeliverySent(ctx, older, "om_owner_old", json.RawMessage(`{}`), baseTime); err != nil {
		t.Fatalf("标记旧 receipt: %v", err)
	}
	if err := st.MarkDeliverySent(ctx, newer, "om_owner_new", json.RawMessage(`{}`), baseTime.Add(time.Minute)); err != nil {
		t.Fatalf("标记新 receipt: %v", err)
	}
	if err := st.MarkDeliverySent(ctx, otherDelivery, "om_other_latest", json.RawMessage(`{}`), baseTime.Add(time.Hour)); err != nil {
		t.Fatalf("标记他人 receipt: %v", err)
	}

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM deliveries WHERE id = ANY($1)`,
			[]int64{older, newer, pending, otherDelivery})
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM push_batches WHERE id = ANY($1)`,
			[]int64{ownerBatch, otherBatch})
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM memberships WHERE user_id = ANY($1)`,
			[]int64{owner.ID, other.ID})
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM users WHERE id = ANY($1)`,
			[]int64{owner.ID, other.ID})
	})

	got, err := st.LatestSentDeliveryMessageID(ctx, owner.ID)
	if err != nil {
		t.Fatalf("LatestSentDeliveryMessageID(owner): %v", err)
	}
	if got != "om_owner_new" {
		t.Fatalf("latest owner receipt = %q, want om_owner_new", got)
	}

	if _, err := st.LatestSentDeliveryMessageID(ctx, owner.ID+9_000_000_000); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("无 receipt 用户 error = %v, want NotFound", err)
	}
}
