package store

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/pusheffect"
	"github.com/YouToco/vane/types"
)

// This adversarial test models the exact receipt-only workflow branch at
// 7743b29: all delivery projections are sent, the run is revoked, and the
// durable batch winner is effect. The effect aggregate is deliberately
// incomplete (chunk 1 is missing and one sent delivery belongs to no effect).
// A correct effect-authority receipt must reject batch completion.
func TestValidatorRecoveryOnlyRejectsIncompleteEffectAggregate(t *testing.T) {
	f := newCompiledRunWriteFixture(t)
	ctx := t.Context()
	key := "validator-recovery-" + uuid.NewString()

	batchID, err := f.base.st.CreatePushBatchForTaskRunV1(
		ctx, f.idA, f.refA, key,
	)
	if err != nil {
		t.Fatal(err)
	}
	deliveryIDs := make([]int64, 0, 2)
	for i := 0; i < 2; i++ {
		contentID := f.createContent(t, f.sourceA, "validator-recovery")
		deliveryID, existed, sent, err := f.base.st.InsertDeliveryForTaskRunV1(
			ctx, f.idA, f.refA, key, &types.Delivery{
				BatchID: batchID, UserID: f.idA.UserID,
				ContentItemID: &contentID, BodyMD: "validator",
			},
		)
		if err != nil || existed || sent {
			t.Fatalf("insert delivery %d: existed=%v sent=%v err=%v",
				i, existed, sent, err)
		}
		deliveryIDs = append(deliveryIDs, deliveryID)
	}
	winner, err := f.base.st.ClaimPushBatchDeliveryAuthority(
		ctx,
		types.PushBatchScope{
			TenantID: f.idA.TenantID,
			UserID:   f.idA.UserID,
			BatchID:  batchID,
		},
		types.PushBatchDeliveryAuthorityEffect,
	)
	if err != nil || winner != types.PushBatchDeliveryAuthorityEffect {
		t.Fatalf("claim effect authority: winner=%q err=%v", winner, err)
	}

	incomplete := pusheffect.Prepared{
		ID: "validator-effect-" + uuid.NewString(),
		TenantID: f.idA.TenantID, UserID: f.idA.UserID,
		TaskID: f.idA.TaskID, RunSnapshotID: f.refA.SnapshotID,
		RunID: f.idA.TemporalRunID, StepID: "push",
		ChunkIndex: 0, ChunkCount: 2, BatchID: batchID,
		DeliveryIDs: []int64{deliveryIDs[0]},
		Provider: "feishu", AppIdentity: "validator-app",
		ProviderChatID: "oc_validator", Target: "ou_validator",
		Card: []byte(`{"validator":true}`), ProviderUUID: uuid.NewString(),
		IdempotencyExpiresAt: time.Now().UTC().
			Truncate(time.Microsecond).Add(time.Hour),
	}
	if _, err := f.base.st.CreatePushEffect(ctx, incomplete); err != nil {
		t.Fatalf("create incomplete effect aggregate: %v", err)
	}
	for _, deliveryID := range deliveryIDs {
		if err := f.base.st.MarkDeliverySentForTaskRunV1(
			ctx, f.idA, f.refA, key, batchID, deliveryID,
			"om-validator", json.RawMessage(`{"sent":true}`), time.Now().UTC(),
		); err != nil {
			t.Fatalf("mark delivery %d sent: %v", deliveryID, err)
		}
	}
	if _, err := f.base.st.pool.Exec(ctx,
		`DELETE FROM memberships WHERE tenant_id=$1 AND user_id=$2`,
		f.idA.TenantID, f.idA.UserID,
	); err != nil {
		t.Fatal(err)
	}
	recovered, recoveryOnly, err :=
		f.base.st.CreateOrRecoverPushBatchForTaskRunV1(
			ctx, f.idA, f.refA, key,
		)
	if err != nil || !recoveryOnly || recovered != batchID {
		t.Fatalf("recover batch=%d recoveryOnly=%v err=%v",
			recovered, recoveryOnly, err)
	}

	// Current behavior unexpectedly succeeds. This assertion documents the
	// required invariant and should fail on the reviewed commit.
	if err := f.base.st.MarkPushBatchDoneReceiptV1(
		ctx, f.idA, f.refA, key, batchID,
	); err == nil {
		t.Fatal("effect-authority recovery marked an incomplete effect aggregate done")
	}
}
