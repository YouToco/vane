package store

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/observation"
	"github.com/YouToco/vane/pusheffect"
	"github.com/YouToco/vane/types"
)

func TestPRBPushEffectReceiptAtomicallySettlesAggregate(t *testing.T) {
	t.Run("success and exact replay", func(t *testing.T) {
		f := newPushEffectFixture(t)
		ctx := t.Context()
		if _, err := f.provider.UpTo(ctx, 48); err != nil {
			t.Fatalf("migrate to 048: %v", err)
		}

		eventKey := strings.Repeat("b", 64)
		f.prepared.ObservationEventKeys = []string{eventKey, ""}
		insertPRBObservedEvent(
			t,
			f,
			eventKey,
			f.prepared.DeliveryIDs[0],
		)
		if _, err := f.store.CreatePushEffect(ctx, f.prepared); err != nil {
			t.Fatal(err)
		}
		claimed, err := f.store.ClaimPushEffect(
			ctx,
			pusheffect.ClaimParams{
				Scope:         f.prepared.Scope(),
				LeaseOwner:    "prb-receipt-worker",
				LeaseDuration: time.Minute,
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		receipt := pusheffect.SentReceipt{
			Scope:                f.prepared.Scope(),
			ExpectedFence:        claimed.Fence,
			LeaseOwner:           claimed.LeaseOwner,
			ProviderMessageID:    "om_prb_atomic",
			ObservationEventKeys: f.prepared.ObservationEventKeys,
		}
		if err := f.store.RecordPushEffectSentWithDeliveries(
			ctx,
			receipt,
		); err != nil {
			t.Fatal(err)
		}
		if err := f.store.RecordPushEffectSentWithDeliveries(
			ctx,
			receipt,
		); err != nil {
			t.Fatalf("exact receipt replay: %v", err)
		}

		var (
			effectStatus    string
			batchStatus     types.BatchStatus
			sentDeliveries  int
			cardsMatch      bool
			eventDelivered  bool
			providerMatches bool
		)
		if err := f.db.QueryRowContext(ctx, `
			SELECT
			  (SELECT status FROM push_effects WHERE id=$1),
			  (SELECT status FROM push_batches WHERE id=$9),
			  (SELECT count(*) FROM deliveries
			    WHERE id=ANY($2) AND status='sent'
			      AND feishu_message_id=$10 AND sent_at IS NOT NULL),
			  NOT EXISTS (
			      SELECT 1 FROM deliveries
			       WHERE id=ANY($2) AND card_json<>$3::jsonb
			  ),
			  EXISTS (
			      SELECT 1 FROM task_observed_events
			       WHERE tenant_id=$4 AND user_id=$5 AND task_id=$6
			         AND delivery_id=$7 AND event_key=$8
			         AND status='delivered' AND delivered_at IS NOT NULL
			  ),
			  (SELECT provider_message_id=$10 AND sent_at IS NOT NULL
			     FROM push_effects WHERE id=$1)`,
			f.prepared.ID,
			f.prepared.DeliveryIDs,
			f.prepared.Card,
			f.prepared.TenantID,
			f.prepared.UserID,
			f.prepared.TaskID,
			f.prepared.DeliveryIDs[0],
			eventKey,
			f.prepared.BatchID,
			receipt.ProviderMessageID,
		).Scan(
			&effectStatus,
			&batchStatus,
			&sentDeliveries,
			&cardsMatch,
			&eventDelivered,
			&providerMatches,
		); err != nil {
			t.Fatal(err)
		}
		if effectStatus != string(pusheffect.StatusSent) ||
			batchStatus != types.BatchStatusDone ||
			sentDeliveries != len(f.prepared.DeliveryIDs) ||
			!cardsMatch ||
			!eventDelivered ||
			!providerMatches {
			t.Fatalf(
				"settled effect=%q batch=%q deliveries=%d cards=%v event=%v provider=%v",
				effectStatus,
				batchStatus,
				sentDeliveries,
				cardsMatch,
				eventDelivered,
				providerMatches,
			)
		}
	})

	t.Run("effect failure rolls back delivery event and batch", func(t *testing.T) {
		f := newPushEffectFixture(t)
		ctx := t.Context()
		if _, err := f.provider.UpTo(ctx, 48); err != nil {
			t.Fatalf("migrate to 048: %v", err)
		}

		eventKey := strings.Repeat("c", 64)
		f.prepared.ObservationEventKeys = []string{eventKey, ""}
		insertPRBObservedEvent(
			t,
			f,
			eventKey,
			f.prepared.DeliveryIDs[0],
		)
		if _, err := f.store.CreatePushEffect(ctx, f.prepared); err != nil {
			t.Fatal(err)
		}
		claimed, err := f.store.ClaimPushEffect(
			ctx,
			pusheffect.ClaimParams{
				Scope:         f.prepared.Scope(),
				LeaseOwner:    "prb-rollback-worker",
				LeaseDuration: time.Minute,
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.db.ExecContext(ctx, `
			CREATE FUNCTION fail_prb_effect_receipt() RETURNS trigger
			LANGUAGE plpgsql AS $$
			BEGIN
			  IF NEW.status='sent' THEN
			    RAISE EXCEPTION 'injected prb effect receipt failure';
			  END IF;
			  RETURN NEW;
			END $$;
			CREATE TRIGGER fail_prb_effect_receipt
			BEFORE UPDATE ON push_effects
			FOR EACH ROW EXECUTE FUNCTION fail_prb_effect_receipt()`); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			cleanupCtx, cancel := cleanupContext()
			defer cancel()
			if _, err := f.db.ExecContext(cleanupCtx,
				`DROP TRIGGER IF EXISTS fail_prb_effect_receipt ON push_effects`,
			); err != nil {
				t.Errorf("drop injected trigger: %v", err)
			}
			if _, err := f.db.ExecContext(cleanupCtx,
				`DROP FUNCTION IF EXISTS fail_prb_effect_receipt()`,
			); err != nil {
				t.Errorf("drop injected function: %v", err)
			}
		})

		err = f.store.RecordPushEffectSentWithDeliveries(
			ctx,
			pusheffect.SentReceipt{
				Scope:                f.prepared.Scope(),
				ExpectedFence:        claimed.Fence,
				LeaseOwner:           claimed.LeaseOwner,
				ProviderMessageID:    "om_prb_rollback",
				ObservationEventKeys: f.prepared.ObservationEventKeys,
			},
		)
		if err == nil {
			t.Fatal("injected effect receipt failure returned nil")
		}

		var (
			effectStatus       string
			batchStatus        types.BatchStatus
			pendingDeliveries  int
			qualifiedEvent     bool
			effectMessageEmpty bool
		)
		if err := f.db.QueryRowContext(ctx, `
			SELECT
			  (SELECT status FROM push_effects WHERE id=$1),
			  (SELECT status FROM push_batches WHERE id=$8),
			  (SELECT count(*) FROM deliveries
			    WHERE id=ANY($2) AND status='pending'
			      AND feishu_message_id='' AND sent_at IS NULL),
			  EXISTS (
			      SELECT 1 FROM task_observed_events
			       WHERE tenant_id=$3 AND user_id=$4 AND task_id=$5
			         AND delivery_id=$6 AND event_key=$7
			         AND status='qualified' AND delivered_at IS NULL
			  ),
			  (SELECT provider_message_id='' AND sent_at IS NULL
			     FROM push_effects WHERE id=$1)`,
			f.prepared.ID,
			f.prepared.DeliveryIDs,
			f.prepared.TenantID,
			f.prepared.UserID,
			f.prepared.TaskID,
			f.prepared.DeliveryIDs[0],
			eventKey,
			f.prepared.BatchID,
		).Scan(
			&effectStatus,
			&batchStatus,
			&pendingDeliveries,
			&qualifiedEvent,
			&effectMessageEmpty,
		); err != nil {
			t.Fatal(err)
		}
		if effectStatus != string(pusheffect.StatusSending) ||
			batchStatus != types.BatchStatusPending ||
			pendingDeliveries != len(f.prepared.DeliveryIDs) ||
			!qualifiedEvent ||
			!effectMessageEmpty {
			t.Fatalf(
				"partial commit effect=%q batch=%q pending=%d qualified=%v providerEmpty=%v",
				effectStatus,
				batchStatus,
				pendingDeliveries,
				qualifiedEvent,
				effectMessageEmpty,
			)
		}
	})
}

func TestPRBSettlePushEffectBatchRejectsIncompleteAggregate(t *testing.T) {
	f := newPushEffectFixture(t)
	ctx := t.Context()
	if _, err := f.provider.UpTo(ctx, 48); err != nil {
		t.Fatalf("migrate to 048: %v", err)
	}

	f.prepared.ChunkCount = 2
	f.prepared.DeliveryIDs = f.prepared.DeliveryIDs[:1]
	f.prepared.ObservationEventKeys = nil
	if _, err := f.store.CreatePushEffect(ctx, f.prepared); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.ExecContext(ctx, `
		UPDATE push_effects
		   SET status='sent', fence=1, attempt=1,
		       provider_message_id='om_prb_history',
		       sent_at=clock_timestamp()
		 WHERE id=$1`,
		f.prepared.ID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.ExecContext(ctx, `
		UPDATE deliveries
		   SET status='sent', feishu_message_id='om_prb_history',
		       card_json=$1::jsonb, sent_at=clock_timestamp()
		 WHERE tenant_id=$2 AND user_id=$3 AND batch_id=$4`,
		f.prepared.Card,
		f.prepared.TenantID,
		f.prepared.UserID,
		f.prepared.BatchID,
	); err != nil {
		t.Fatal(err)
	}

	err := f.store.SettlePushEffectBatchReceipt(
		ctx,
		types.PushBatchScope{
			TenantID: f.prepared.TenantID,
			UserID:   f.prepared.UserID,
			BatchID:  f.prepared.BatchID,
		},
		f.prepared.RunSnapshotID,
	)
	if !errors.Is(err, types.ErrConflict) {
		t.Fatalf("incomplete aggregate error=%v, want conflict", err)
	}
	var batchStatus types.BatchStatus
	if err := f.db.QueryRowContext(ctx, `
		SELECT status FROM push_batches WHERE id=$1`,
		f.prepared.BatchID,
	).Scan(&batchStatus); err != nil {
		t.Fatal(err)
	}
	if batchStatus != types.BatchStatusPending {
		t.Fatalf("incomplete aggregate changed batch to %q", batchStatus)
	}
}

func TestPRBPushEffectReceiptRejectsFrozenAggregateMutation(t *testing.T) {
	tests := []struct {
		name   string
		before func(*testing.T, *pushEffectFixture)
		after  func(*testing.T, *pushEffectFixture)
	}{
		{
			name: "extra batch delivery",
			after: func(t *testing.T, f *pushEffectFixture) {
				t.Helper()
				if _, err := f.db.ExecContext(t.Context(), `
					INSERT INTO deliveries (
					    tenant_id,batch_id,user_id,score,card_json,status
					) VALUES ($1,$2,$3,1,'{}'::jsonb,'pending')`,
					f.prepared.TenantID,
					f.prepared.BatchID,
					f.prepared.UserID,
				); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "missing frozen delivery",
			after: func(t *testing.T, f *pushEffectFixture) {
				t.Helper()
				if _, err := f.db.ExecContext(t.Context(),
					`DELETE FROM deliveries WHERE id=$1`,
					f.prepared.DeliveryIDs[1],
				); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "failed frozen delivery",
			after: func(t *testing.T, f *pushEffectFixture) {
				t.Helper()
				if _, err := f.db.ExecContext(t.Context(),
					`UPDATE deliveries SET status='failed' WHERE id=$1`,
					f.prepared.DeliveryIDs[1],
				); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "extra bound event",
			after: func(t *testing.T, f *pushEffectFixture) {
				t.Helper()
				insertPRBObservedEvent(
					t,
					*f,
					strings.Repeat("e", 64),
					f.prepared.DeliveryIDs[0],
				)
			},
		},
		{
			name: "missing frozen event",
			before: func(t *testing.T, f *pushEffectFixture) {
				t.Helper()
				eventKey := strings.Repeat("f", 64)
				f.prepared.ObservationEventKeys = []string{eventKey, ""}
				insertPRBObservedEvent(
					t, *f, eventKey, f.prepared.DeliveryIDs[0],
				)
			},
			after: func(t *testing.T, f *pushEffectFixture) {
				t.Helper()
				if _, err := f.db.ExecContext(t.Context(), `
					DELETE FROM task_observed_events
					 WHERE tenant_id=$1 AND event_key=$2`,
					f.prepared.TenantID,
					f.prepared.ObservationEventKeys[0],
				); err != nil {
					t.Fatal(err)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := newPushEffectFixture(t)
			ctx := t.Context()
			if test.before != nil {
				test.before(t, &f)
			}
			if _, err := f.store.CreatePushEffect(
				ctx, f.prepared,
			); err != nil {
				t.Fatal(err)
			}
			claimed, err := f.store.ClaimPushEffect(
				ctx,
				pusheffect.ClaimParams{
					Scope:         f.prepared.Scope(),
					LeaseOwner:    "prb-mutation-worker",
					LeaseDuration: time.Minute,
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			if test.after != nil {
				test.after(t, &f)
			}
			err = f.store.RecordPushEffectSentWithDeliveries(
				ctx,
				pusheffect.SentReceipt{
					Scope:                f.prepared.Scope(),
					ExpectedFence:        claimed.Fence,
					LeaseOwner:           claimed.LeaseOwner,
					ProviderMessageID:    "om_prb_mutation",
					ObservationEventKeys: f.prepared.ObservationEventKeys,
				},
			)
			if !errors.Is(err, types.ErrConflict) {
				t.Fatalf("aggregate mutation error=%v, want conflict", err)
			}

			var (
				effectStatus string
				batchStatus  types.BatchStatus
				projected    int
				effectEmpty  bool
			)
			if err := f.db.QueryRowContext(ctx, `
				SELECT
				  (SELECT status FROM push_effects WHERE id=$1),
				  (SELECT status FROM push_batches WHERE id=$2),
				  (SELECT count(*) FROM deliveries
				    WHERE tenant_id=$3 AND batch_id=$2
				      AND feishu_message_id='om_prb_mutation'),
				  (SELECT provider_message_id='' AND sent_at IS NULL
				     FROM push_effects WHERE id=$1)`,
				f.prepared.ID,
				f.prepared.BatchID,
				f.prepared.TenantID,
			).Scan(
				&effectStatus,
				&batchStatus,
				&projected,
				&effectEmpty,
			); err != nil {
				t.Fatal(err)
			}
			if effectStatus != string(pusheffect.StatusSending) ||
				batchStatus != types.BatchStatusPending ||
				projected != 0 ||
				!effectEmpty {
				t.Fatalf(
					"partial mutation commit effect=%q batch=%q projected=%d effectEmpty=%v",
					effectStatus,
					batchStatus,
					projected,
					effectEmpty,
				)
			}
		})
	}
}

func TestPRBPushEffectReceiptSettlesTwoChunksExactly(t *testing.T) {
	f := newPushEffectFixture(t)
	ctx := t.Context()
	chunks := []pusheffect.Prepared{f.prepared, f.prepared}
	for index := range chunks {
		chunks[index].ID = "effect-two-chunk-" + uuid.NewString()
		chunks[index].ProviderUUID = uuid.NewString()
		chunks[index].ChunkIndex = index
		chunks[index].ChunkCount = len(chunks)
		chunks[index].DeliveryIDs = []int64{f.prepared.DeliveryIDs[index]}
		chunks[index].ObservationEventKeys = nil
		if _, err := f.store.CreatePushEffect(ctx, chunks[index]); err != nil {
			t.Fatal(err)
		}
	}

	receipts := make([]pusheffect.SentReceipt, len(chunks))
	for index := range chunks {
		claimed, err := f.store.ClaimPushEffect(
			ctx,
			pusheffect.ClaimParams{
				Scope:         chunks[index].Scope(),
				LeaseOwner:    "prb-two-chunk-worker",
				LeaseDuration: time.Minute,
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		receipts[index] = pusheffect.SentReceipt{
			Scope:             chunks[index].Scope(),
			ExpectedFence:     claimed.Fence,
			LeaseOwner:        claimed.LeaseOwner,
			ProviderMessageID: "om_prb_chunk_" + uuid.NewString(),
		}
		if err := f.store.RecordPushEffectSentWithDeliveries(
			ctx, receipts[index],
		); err != nil {
			t.Fatal(err)
		}
		var batchStatus types.BatchStatus
		if err := f.db.QueryRowContext(ctx,
			`SELECT status FROM push_batches WHERE id=$1`,
			f.prepared.BatchID,
		).Scan(&batchStatus); err != nil {
			t.Fatal(err)
		}
		want := types.BatchStatusPending
		if index == len(chunks)-1 {
			want = types.BatchStatusDone
		}
		if batchStatus != want {
			t.Fatalf("after chunk %d batch=%q, want %q",
				index, batchStatus, want)
		}
	}
	for index := range receipts {
		if err := f.store.RecordPushEffectSentWithDeliveries(
			ctx, receipts[index],
		); err != nil {
			t.Fatalf("chunk %d exact receipt replay: %v", index, err)
		}
	}

	var sentEffects, sentDeliveries int
	if err := f.db.QueryRowContext(ctx, `
		SELECT
		  (SELECT count(*) FROM push_effects
		    WHERE tenant_id=$1 AND batch_id=$2 AND status='sent'),
		  (SELECT count(*) FROM deliveries
		    WHERE tenant_id=$1 AND batch_id=$2 AND status='sent'
		      AND sent_at IS NOT NULL)`,
		f.prepared.TenantID,
		f.prepared.BatchID,
	).Scan(&sentEffects, &sentDeliveries); err != nil {
		t.Fatal(err)
	}
	if sentEffects != len(chunks) ||
		sentDeliveries != len(f.prepared.DeliveryIDs) {
		t.Fatalf("settled effects/deliveries=%d/%d",
			sentEffects, sentDeliveries)
	}
}

func TestPRBCreatePushEffectRejectsObservedEventProvenanceDrift(t *testing.T) {
	tests := []struct {
		name   string
		mutate string
	}{
		{
			name: "wrong temporal run",
			mutate: `UPDATE task_observed_events
			           SET temporal_run_id='wrong-run'
			         WHERE tenant_id=$1 AND delivery_id=$2`,
		},
		{
			name: "already delivered",
			mutate: `UPDATE task_observed_events
			           SET status='delivered',delivered_at=clock_timestamp()
			         WHERE tenant_id=$1 AND delivery_id=$2`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := newPushEffectFixture(t)
			ctx := t.Context()
			if _, err := f.provider.UpTo(ctx, 48); err != nil {
				t.Fatalf("migrate to 048: %v", err)
			}
			eventKey := strings.Repeat("d", 64)
			f.prepared.ObservationEventKeys = []string{eventKey, ""}
			insertPRBObservedEvent(
				t, f, eventKey, f.prepared.DeliveryIDs[0],
			)
			if _, err := f.db.ExecContext(
				ctx,
				test.mutate,
				f.prepared.TenantID,
				f.prepared.DeliveryIDs[0],
			); err != nil {
				t.Fatal(err)
			}

			if _, err := f.store.CreatePushEffect(
				ctx, f.prepared,
			); !errors.Is(err, types.ErrConflict) {
				t.Fatalf("provenance drift error=%v, want conflict", err)
			}
			var effects int
			if err := f.db.QueryRowContext(ctx,
				`SELECT count(*) FROM push_effects WHERE tenant_id=$1`,
				f.prepared.TenantID,
			).Scan(&effects); err != nil {
				t.Fatal(err)
			}
			if effects != 0 {
				t.Fatalf("provenance drift inserted %d effects", effects)
			}
		})
	}
}

func TestPRBCreatePushEffectRejectsTerminalDriftBeforeFreeze(t *testing.T) {
	tests := []struct {
		name   string
		mutate string
	}{
		{
			name:   "batch done",
			mutate: `UPDATE push_batches SET status='done' WHERE id=$1`,
		},
		{
			name:   "batch failed",
			mutate: `UPDATE push_batches SET status='failed' WHERE id=$1`,
		},
		{
			name: "delivery failed",
			mutate: `UPDATE deliveries SET status='failed'
			          WHERE batch_id=$1`,
		},
		{
			name: "delivery sent",
			mutate: `UPDATE deliveries
			           SET status='sent',feishu_message_id='om_drift',
			               card_json='{"drift":true}'::jsonb,
			               sent_at=clock_timestamp()
			         WHERE batch_id=$1`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := newPushEffectFixture(t)
			ctx := t.Context()
			if _, err := f.db.ExecContext(
				ctx, test.mutate, f.prepared.BatchID,
			); err != nil {
				t.Fatal(err)
			}
			if _, err := f.store.CreatePushEffect(
				ctx, f.prepared,
			); !errors.Is(err, types.ErrConflict) {
				t.Fatalf("terminal drift error=%v, want conflict", err)
			}
			var effects int
			if err := f.db.QueryRowContext(ctx,
				`SELECT count(*) FROM push_effects WHERE tenant_id=$1`,
				f.prepared.TenantID,
			).Scan(&effects); err != nil {
				t.Fatal(err)
			}
			if effects != 0 {
				t.Fatalf("terminal drift inserted %d effects", effects)
			}
		})
	}
}

func TestPRBCreatePushEffectExactReplaySurvivesTerminalProjection(t *testing.T) {
	f := newPushEffectFixture(t)
	ctx := t.Context()
	first, err := f.store.CreatePushEffect(ctx, f.prepared)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.ExecContext(ctx, `
		UPDATE deliveries
		   SET status='sent',feishu_message_id='om_replay',
		       card_json=$2::jsonb,sent_at=clock_timestamp()
		 WHERE batch_id=$1`,
		f.prepared.BatchID,
		f.prepared.Card,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.ExecContext(ctx,
		`UPDATE push_batches SET status='done' WHERE id=$1`,
		f.prepared.BatchID,
	); err != nil {
		t.Fatal(err)
	}
	replayed, err := f.store.CreatePushEffect(ctx, f.prepared)
	if err != nil {
		t.Fatalf("exact effect replay after terminal projection: %v", err)
	}
	if replayed.ID != first.ID ||
		replayed.PayloadDigest != first.PayloadDigest {
		t.Fatalf("terminal exact replay drifted first=%+v replay=%+v",
			first, replayed)
	}
}

func TestPRBPushEffectFreezesDeliveryAggregate(t *testing.T) {
	f := newCompiledRunWriteFixture(t)
	ctx := t.Context()
	cleanupPRBCompiledEffects(t, f)
	key := "prb-freeze-delivery-" + uuid.NewString()
	batchID := createObservationBatch(t, f, f.idA, f.refA, key)
	contentID := f.createContent(t, f.sourceA, "prb-freeze-delivery")
	input := &types.Delivery{
		BatchID:       batchID,
		UserID:        f.idA.UserID,
		ContentItemID: &contentID,
		Score:         88,
		BodyMD:        "immutable delivery body",
	}
	deliveryID, existed, sent, err := f.base.st.InsertDeliveryForTaskRunV1(
		ctx, f.idA, f.refA, key, input,
	)
	if err != nil || existed || sent {
		t.Fatalf("initial delivery=%d/%v/%v err=%v",
			deliveryID, existed, sent, err)
	}
	prepared := settlementPrepared(
		f, batchID, 0, 1, []int64{deliveryID},
	)
	if _, err := f.base.st.CreatePushEffect(ctx, prepared); err != nil {
		t.Fatal(err)
	}

	replayedID, existed, sent, err := f.base.st.InsertDeliveryForTaskRunV1(
		ctx, f.idA, f.refA, key, input,
	)
	if err != nil || replayedID != deliveryID || !existed || sent {
		t.Fatalf("pending frozen replay=%d/%v/%v err=%v",
			replayedID, existed, sent, err)
	}
	newContentID := f.createContent(t, f.sourceA, "prb-freeze-new")
	_, _, _, err = f.base.st.InsertDeliveryForTaskRunV1(
		ctx,
		f.idA,
		f.refA,
		key,
		&types.Delivery{
			BatchID:       batchID,
			UserID:        f.idA.UserID,
			ContentItemID: &newContentID,
			Score:         90,
			BodyMD:        "must not expand",
		},
	)
	if !errors.Is(err, types.ErrConflict) {
		t.Fatalf("post-effect new delivery error=%v, want conflict", err)
	}
	var deliveries int
	if err := f.base.st.pool.QueryRow(ctx, `
		SELECT count(*) FROM deliveries
		 WHERE tenant_id=$1 AND user_id=$2 AND batch_id=$3`,
		f.idA.TenantID,
		f.idA.UserID,
		batchID,
	).Scan(&deliveries); err != nil {
		t.Fatal(err)
	}
	if deliveries != 1 {
		t.Fatalf("post-effect aggregate expanded to %d deliveries", deliveries)
	}

	if _, err := f.base.st.pool.Exec(ctx, `
		UPDATE deliveries
		   SET status='sent',feishu_message_id='om_frozen',
		       card_json='{"sent":true}'::jsonb,
		       sent_at=clock_timestamp()
		 WHERE id=$1`,
		deliveryID,
	); err != nil {
		t.Fatal(err)
	}
	replayedID, existed, sent, err = f.base.st.InsertDeliveryForTaskRunV1(
		ctx, f.idA, f.refA, key, input,
	)
	if err != nil || replayedID != deliveryID || !existed || !sent {
		t.Fatalf("sent frozen replay=%d/%v/%v err=%v",
			replayedID, existed, sent, err)
	}
}

func TestPRBPushEffectFreezesObservedEventReservation(t *testing.T) {
	f := newCompiledRunWriteFixture(t)
	ctx := t.Context()
	cleanupPRBCompiledEffects(t, f)
	key := "prb-freeze-reserve-" + uuid.NewString()
	batchID := createObservationBatch(t, f, f.idA, f.refA, key)
	contentID := f.createContent(t, f.sourceA, "prb-freeze-reserve")
	deliveryID, _, _, err := f.base.st.InsertDeliveryForTaskRunV1(
		ctx,
		f.idA,
		f.refA,
		key,
		&types.Delivery{
			BatchID:       batchID,
			UserID:        f.idA.UserID,
			ContentItemID: &contentID,
			BodyMD:        "freeze reserve",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.base.st.CreatePushEffect(
		ctx,
		settlementPrepared(f, batchID, 0, 1, []int64{deliveryID}),
	); err != nil {
		t.Fatal(err)
	}
	event := prbQualifiedEvent("1", "2")
	accepted, err := f.base.st.ReserveObservedEventV1(
		ctx, f.idA, f.refA, batchID, event,
	)
	if accepted || !errors.Is(err, types.ErrConflict) {
		t.Fatalf("post-effect reserve accepted=%v err=%v", accepted, err)
	}
	var events int
	if err := f.base.st.pool.QueryRow(ctx, `
		SELECT count(*) FROM task_observed_events
		 WHERE tenant_id=$1 AND task_id=$2 AND event_key=$3`,
		f.idA.TenantID,
		f.idA.TaskID,
		event.EventKey,
	).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if events != 0 {
		t.Fatalf("post-effect reserve inserted %d events", events)
	}
}

func TestPRBPushEffectAllowsBoundCurrentObservedEventReservationReplay(
	t *testing.T,
) {
	f := newCompiledRunWriteFixture(t)
	ctx := t.Context()
	cleanupPRBCompiledEffects(t, f)
	key := "prb-freeze-reserve-replay-" + uuid.NewString()
	batchID := createObservationBatch(t, f, f.idA, f.refA, key)
	event := prbQualifiedEvent("9", "a")
	accepted, err := f.base.st.ReserveObservedEventV1(
		ctx, f.idA, f.refA, batchID, event,
	)
	if err != nil || !accepted {
		t.Fatalf("initial reserve accepted=%v err=%v", accepted, err)
	}
	contentID := f.createContent(t, f.sourceA, "prb-freeze-reserve-replay")
	deliveryID, _, _, err := f.base.st.InsertDeliveryForTaskRunV1(
		ctx,
		f.idA,
		f.refA,
		key,
		&types.Delivery{
			BatchID:       batchID,
			UserID:        f.idA.UserID,
			ContentItemID: &contentID,
			BodyMD:        "bound reservation replay",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.base.st.BindObservedEventDeliveryV1(
		ctx,
		f.idA,
		f.refA,
		event.PolicyDigest,
		event.EventKey,
		batchID,
		deliveryID,
	); err != nil {
		t.Fatal(err)
	}
	prepared := settlementPrepared(
		f, batchID, 0, 1, []int64{deliveryID})
	prepared.ObservationEventKeys = []string{event.EventKey}
	if _, err := f.base.st.CreatePushEffect(ctx, prepared); err != nil {
		t.Fatal(err)
	}
	accepted, err = f.base.st.ReserveObservedEventV1(
		ctx, f.idA, f.refA, batchID, event,
	)
	if err != nil || !accepted {
		t.Fatalf("frozen exact reserve replay accepted=%v err=%v",
			accepted, err)
	}
}

func TestPRBPushEffectRejectsStaleObservedEventReclaimWithoutMutation(
	t *testing.T,
) {
	f := newCompiledRunWriteFixture(t)
	ctx := t.Context()
	cleanupPRBCompiledEffects(t, f)
	event := prbQualifiedEvent("7", "8")

	oldKey := "prb-stale-effect-old-" + uuid.NewString()
	oldBatchID := createObservationBatch(t, f, f.idA, f.refA, oldKey)
	accepted, err := f.base.st.ReserveObservedEventV1(
		ctx, f.idA, f.refA, oldBatchID, event,
	)
	if err != nil || !accepted {
		t.Fatalf("old reserve accepted=%v err=%v", accepted, err)
	}
	oldContentID := f.createContent(t, f.sourceA, "prb-stale-effect-old")
	oldDeliveryID, _, _, err := f.base.st.InsertDeliveryForTaskRunV1(
		ctx,
		f.idA,
		f.refA,
		oldKey,
		&types.Delivery{
			BatchID:       oldBatchID,
			UserID:        f.idA.UserID,
			ContentItemID: &oldContentID,
			BodyMD:        "old bound delivery must remain unchanged",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.base.st.BindObservedEventDeliveryV1(
		ctx,
		f.idA,
		f.refA,
		event.PolicyDigest,
		event.EventKey,
		oldBatchID,
		oldDeliveryID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := f.base.st.pool.Exec(ctx, `
		UPDATE task_observed_events
		   SET created_at=clock_timestamp()-interval '11 minutes'
		 WHERE tenant_id=$1 AND task_id=$2
		   AND policy_digest=$3 AND event_key=$4`,
		f.idA.TenantID,
		f.idA.TaskID,
		event.PolicyDigest,
		event.EventKey,
	); err != nil {
		t.Fatal(err)
	}

	currentIdentity := scheduledRunIdentity(
		f.taskA,
		f.idA.TenantID,
		f.idA.UserID,
		"prb-current-run-"+uuid.NewString(),
	)
	currentRef, err := f.base.st.CreateOrGetCompiledTaskRunSnapshotV1(
		ctx,
		CreateOrGetCompiledTaskRunSnapshotV1Params{
			Identity: currentIdentity,
			Policy:   testCompiledRunPolicyV1(t),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	currentKey := "prb-stale-effect-current-" + uuid.NewString()
	currentBatchID := createObservationBatch(
		t, f, currentIdentity, currentRef, currentKey)
	currentContentID := f.createContent(
		t, f.sourceA, "prb-stale-effect-current")
	currentDeliveryID, _, _, err :=
		f.base.st.InsertDeliveryForTaskRunV1(
			ctx,
			currentIdentity,
			currentRef,
			currentKey,
			&types.Delivery{
				BatchID:       currentBatchID,
				UserID:        currentIdentity.UserID,
				ContentItemID: &currentContentID,
				BodyMD:        "current frozen delivery",
			},
		)
	if err != nil {
		t.Fatal(err)
	}
	prepared := settlementPrepared(
		f, currentBatchID, 0, 1, []int64{currentDeliveryID})
	prepared.RunSnapshotID = currentRef.SnapshotID
	prepared.RunID = currentIdentity.TemporalRunID
	if _, err := f.base.st.CreatePushEffect(ctx, prepared); err != nil {
		t.Fatal(err)
	}

	var oldDeliveryBefore, oldEventBefore string
	if err := f.base.st.pool.QueryRow(ctx,
		`SELECT row_to_json(d)::text FROM deliveries d WHERE id=$1`,
		oldDeliveryID,
	).Scan(&oldDeliveryBefore); err != nil {
		t.Fatal(err)
	}
	if err := f.base.st.pool.QueryRow(ctx, `
		SELECT row_to_json(e)::text
		  FROM task_observed_events e
		 WHERE tenant_id=$1 AND task_id=$2
		   AND policy_digest=$3 AND event_key=$4`,
		f.idA.TenantID,
		f.idA.TaskID,
		event.PolicyDigest,
		event.EventKey,
	).Scan(&oldEventBefore); err != nil {
		t.Fatal(err)
	}

	accepted, err = f.base.st.ReserveObservedEventV1(
		ctx, currentIdentity, currentRef, currentBatchID, event,
	)
	if accepted || !errors.Is(err, types.ErrConflict) {
		t.Fatalf("stale frozen reclaim accepted=%v err=%v", accepted, err)
	}

	var oldDeliveryAfter, oldEventAfter string
	if err := f.base.st.pool.QueryRow(ctx,
		`SELECT row_to_json(d)::text FROM deliveries d WHERE id=$1`,
		oldDeliveryID,
	).Scan(&oldDeliveryAfter); err != nil {
		t.Fatal(err)
	}
	if err := f.base.st.pool.QueryRow(ctx, `
		SELECT row_to_json(e)::text
		  FROM task_observed_events e
		 WHERE tenant_id=$1 AND task_id=$2
		   AND policy_digest=$3 AND event_key=$4`,
		f.idA.TenantID,
		f.idA.TaskID,
		event.PolicyDigest,
		event.EventKey,
	).Scan(&oldEventAfter); err != nil {
		t.Fatal(err)
	}
	if oldDeliveryAfter != oldDeliveryBefore {
		t.Fatalf("stale delivery mutated\nbefore=%s\nafter=%s",
			oldDeliveryBefore, oldDeliveryAfter)
	}
	if oldEventAfter != oldEventBefore {
		t.Fatalf("stale event mutated\nbefore=%s\nafter=%s",
			oldEventBefore, oldEventAfter)
	}
}

func TestPRBPushEffectFreezesObservedEventBinding(t *testing.T) {
	t.Run("null to delivery is rejected", func(t *testing.T) {
		f := newCompiledRunWriteFixture(t)
		ctx := t.Context()
		cleanupPRBCompiledEffects(t, f)
		key := "prb-freeze-bind-null-" + uuid.NewString()
		batchID := createObservationBatch(t, f, f.idA, f.refA, key)
		event := prbQualifiedEvent("3", "4")
		accepted, err := f.base.st.ReserveObservedEventV1(
			ctx, f.idA, f.refA, batchID, event,
		)
		if err != nil || !accepted {
			t.Fatalf("initial reserve accepted=%v err=%v", accepted, err)
		}
		contentID := f.createContent(t, f.sourceA, "prb-freeze-bind-null")
		deliveryID, _, _, err := f.base.st.InsertDeliveryForTaskRunV1(
			ctx,
			f.idA,
			f.refA,
			key,
			&types.Delivery{
				BatchID:       batchID,
				UserID:        f.idA.UserID,
				ContentItemID: &contentID,
				BodyMD:        "freeze null bind",
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.base.st.CreatePushEffect(
			ctx,
			settlementPrepared(
				f, batchID, 0, 1, []int64{deliveryID}),
		); err != nil {
			t.Fatal(err)
		}
		err = f.base.st.BindObservedEventDeliveryV1(
			ctx,
			f.idA,
			f.refA,
			event.PolicyDigest,
			event.EventKey,
			batchID,
			deliveryID,
		)
		if !errors.Is(err, types.ErrConflict) {
			t.Fatalf("post-effect null bind error=%v, want conflict", err)
		}
		var bound *int64
		if err := f.base.st.pool.QueryRow(ctx, `
			SELECT delivery_id FROM task_observed_events
			 WHERE tenant_id=$1 AND task_id=$2 AND event_key=$3`,
			f.idA.TenantID,
			f.idA.TaskID,
			event.EventKey,
		).Scan(&bound); err != nil {
			t.Fatal(err)
		}
		if bound != nil {
			t.Fatalf("post-effect bind expanded event to delivery %d", *bound)
		}
	})

	t.Run("qualified and delivered exact replay", func(t *testing.T) {
		f := newCompiledRunWriteFixture(t)
		ctx := t.Context()
		cleanupPRBCompiledEffects(t, f)
		key := "prb-freeze-bind-replay-" + uuid.NewString()
		batchID := createObservationBatch(t, f, f.idA, f.refA, key)
		event := prbQualifiedEvent("5", "6")
		accepted, err := f.base.st.ReserveObservedEventV1(
			ctx, f.idA, f.refA, batchID, event,
		)
		if err != nil || !accepted {
			t.Fatalf("initial reserve accepted=%v err=%v", accepted, err)
		}
		contentID := f.createContent(t, f.sourceA, "prb-freeze-bind-replay")
		deliveryID, _, _, err := f.base.st.InsertDeliveryForTaskRunV1(
			ctx,
			f.idA,
			f.refA,
			key,
			&types.Delivery{
				BatchID:       batchID,
				UserID:        f.idA.UserID,
				ContentItemID: &contentID,
				BodyMD:        "freeze bound replay",
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		if err := f.base.st.BindObservedEventDeliveryV1(
			ctx,
			f.idA,
			f.refA,
			event.PolicyDigest,
			event.EventKey,
			batchID,
			deliveryID,
		); err != nil {
			t.Fatal(err)
		}
		prepared := settlementPrepared(
			f, batchID, 0, 1, []int64{deliveryID})
		prepared.ObservationEventKeys = []string{event.EventKey}
		if _, err := f.base.st.CreatePushEffect(ctx, prepared); err != nil {
			t.Fatal(err)
		}
		if err := f.base.st.BindObservedEventDeliveryV1(
			ctx,
			f.idA,
			f.refA,
			event.PolicyDigest,
			event.EventKey,
			batchID,
			deliveryID,
		); err != nil {
			t.Fatalf("qualified frozen bind replay: %v", err)
		}
		if _, err := f.base.st.pool.Exec(ctx, `
			UPDATE task_observed_events
			   SET status='delivered',delivered_at=clock_timestamp()
			 WHERE tenant_id=$1 AND task_id=$2 AND event_key=$3`,
			f.idA.TenantID,
			f.idA.TaskID,
			event.EventKey,
		); err != nil {
			t.Fatal(err)
		}
		if err := f.base.st.BindObservedEventDeliveryV1(
			ctx,
			f.idA,
			f.refA,
			event.PolicyDigest,
			event.EventKey,
			batchID,
			deliveryID,
		); err != nil {
			t.Fatalf("delivered frozen bind replay: %v", err)
		}
	})
}

func cleanupPRBCompiledEffects(
	t *testing.T,
	f *compiledRunWriteFixture,
) {
	t.Helper()
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(cleanupCtx, t, f.base.st,
			`DELETE FROM task_observed_events WHERE tenant_id=$1`,
			f.idA.TenantID)
		cleanupExec(cleanupCtx, t, f.base.st,
			`DELETE FROM push_effects WHERE tenant_id=$1`,
			f.idA.TenantID)
	})
}

func prbQualifiedEvent(
	policyDigit string,
	eventDigit string,
) observation.QualifiedEvent {
	return observation.QualifiedEvent{
		PolicyDigest: strings.Repeat(policyDigit, 64),
		EventKey:     strings.Repeat(eventDigit, 64),
		EventType:    "release",
		Subject:      "PRB frozen event",
		OccurredAt:   time.Now().UTC().Truncate(time.Second),
		EvidenceJSON: []byte(`{"source":"prb-freeze"}`),
	}
}

func settlementPrepared(
	f *compiledRunWriteFixture,
	batchID int64,
	chunkIndex, chunkCount int,
	deliveryIDs []int64,
) pusheffect.Prepared {
	return pusheffect.Prepared{
		ID:             "prb-settlement-" + uuid.NewString(),
		TenantID:       f.idA.TenantID,
		UserID:         f.idA.UserID,
		TaskID:         f.idA.TaskID,
		RunSnapshotID:  f.refA.SnapshotID,
		RunID:          f.idA.TemporalRunID,
		StepID:         "push",
		ChunkIndex:     chunkIndex,
		ChunkCount:     chunkCount,
		BatchID:        batchID,
		DeliveryIDs:    deliveryIDs,
		Provider:       "feishu",
		AppIdentity:    "prb-settlement-app",
		ProviderChatID: "oc_prb_settlement",
		Target:         "ou_prb_settlement",
		Card:           []byte(`{"prb_settlement":true}`),
		ProviderUUID:   uuid.NewString(),
		IdempotencyExpiresAt: time.Now().UTC().
			Truncate(time.Microsecond).Add(time.Hour),
	}
}

func insertPRBObservedEvent(
	t *testing.T,
	f pushEffectFixture,
	eventKey string,
	deliveryID int64,
) {
	t.Helper()
	if _, err := f.db.ExecContext(t.Context(), `
		INSERT INTO task_observed_events (
		    tenant_id,user_id,task_id,policy_digest,event_key,event_type,
		    subject,occurred_at,evidence_json,run_snapshot_id,
		    temporal_run_id,delivery_id,status
		) VALUES (
		    $1,$2,$3,$4,$5,'release','prb fixture',clock_timestamp(),
		    '{"source":"prb-fixture"}'::jsonb,$6,$7,$8,'qualified'
		)`,
		f.prepared.TenantID,
		f.prepared.UserID,
		f.prepared.TaskID,
		strings.Repeat("a", 64),
		eventKey,
		f.prepared.RunSnapshotID,
		f.prepared.RunID,
		deliveryID,
	); err != nil {
		t.Fatal(err)
	}
}
