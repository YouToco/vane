package store

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/pusheffect"
	"github.com/YouToco/vane/types"
)

func TestAuthorizePushEffectRunSideEffectUsesExactPersistedSnapshot(t *testing.T) {
	f := newTaskRunSnapshotFixture(t)
	taskID := f.taskID()
	f.createApprovedTask(t, taskID, 1)
	identity := scheduledRunIdentity(
		taskID,
		f.tenantID,
		f.userID,
		"push-effect-auth-"+uuid.NewString(),
	)
	ref, err := f.st.CreateOrGetCompiledTaskRunSnapshotV1(
		t.Context(),
		CreateOrGetCompiledTaskRunSnapshotV1Params{
			Identity: identity,
			Policy:   testCompiledRunPolicyV1(t),
		},
	)
	if err != nil {
		t.Fatalf("create compiled snapshot: %v", err)
	}

	var batchID, deliveryID int64
	if err := f.st.pool.QueryRow(t.Context(), `
		INSERT INTO push_batches (
			tenant_id,user_id,status,idempotency_key,schedule_id,run_snapshot_id
		) VALUES ($1,$2,'pending',$3,$4,$5) RETURNING id`,
		f.tenantID,
		f.userID,
		"push-effect-auth-batch-"+uuid.NewString(),
		taskID,
		ref.SnapshotID,
	).Scan(&batchID); err != nil {
		t.Fatalf("create push batch: %v", err)
	}
	if err := f.st.pool.QueryRow(t.Context(), `
		INSERT INTO deliveries (
			tenant_id,batch_id,user_id,score,card_json,status
		) VALUES ($1,$2,$3,80,'{}'::jsonb,'pending') RETURNING id`,
		f.tenantID,
		batchID,
		f.userID,
	).Scan(&deliveryID); err != nil {
		t.Fatalf("create delivery: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(cleanupCtx, t, f.st,
			`DELETE FROM push_effects WHERE tenant_id=$1`, f.tenantID)
		cleanupExec(cleanupCtx, t, f.st,
			`DELETE FROM deliveries WHERE tenant_id=$1`, f.tenantID)
		cleanupExec(cleanupCtx, t, f.st,
			`DELETE FROM push_batches WHERE tenant_id=$1`, f.tenantID)
	})

	prepared := pusheffect.Prepared{
		ID:             uuid.NewString(),
		TenantID:       f.tenantID,
		UserID:         f.userID,
		TaskID:         taskID,
		RunSnapshotID:  ref.SnapshotID,
		RunID:          identity.TemporalRunID,
		StepID:         "push",
		ChunkIndex:     0,
		ChunkCount:     1,
		BatchID:        batchID,
		DeliveryIDs:    []int64{deliveryID},
		Provider:       "feishu",
		AppIdentity:    "cli_push_effect_authorization",
		ProviderChatID: "oc_push_effect_authorization",
		Target:         "ou_push_effect_authorization",
		Card:           []byte(`{"card":"authorization"}`),
		ProviderUUID:   uuid.NewString(),
		IdempotencyExpiresAt: time.Now().UTC().
			Truncate(time.Microsecond).Add(time.Hour),
	}
	effect, err := f.st.CreatePushEffect(t.Context(), prepared)
	if err != nil {
		t.Fatalf("create push effect: %v", err)
	}

	authorized, err := f.st.AuthorizePushEffectRunSideEffect(
		t.Context(),
		effect.Scope(),
	)
	if err != nil || !authorized {
		t.Fatalf("authorize exact persisted effect = %v, err=%v", authorized, err)
	}

	crossScope := effect.Scope()
	crossScope.UserID++
	authorized, err = f.st.AuthorizePushEffectRunSideEffect(
		t.Context(),
		crossScope,
	)
	if authorized || !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("cross-scope authorization = %v, err=%v", authorized, err)
	}

	if _, err := f.st.pool.Exec(t.Context(),
		`DELETE FROM memberships WHERE tenant_id=$1 AND user_id=$2`,
		f.tenantID,
		f.userID,
	); err != nil {
		t.Fatalf("revoke task membership: %v", err)
	}
	authorized, err = f.st.AuthorizePushEffectRunSideEffect(
		t.Context(),
		effect.Scope(),
	)
	if err != nil || authorized {
		t.Fatalf("revoked effect authorization = %v, err=%v", authorized, err)
	}
}
