package store

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/YouToco/vane/pusheffect"
	"github.com/YouToco/vane/types"
)

func TestPushEffectSentAndDeliveryReceiptsAreAtomic(t *testing.T) {
	t.Run("success and exact replay", func(t *testing.T) {
		f := newPushEffectFixture(t)
		ctx := t.Context()
		if _, err := f.provider.UpTo(ctx, 48); err != nil {
			t.Fatalf("migrate to 048: %v", err)
		}
		eventKey := strings.Repeat("b", 64)
		f.prepared.ObservationEventKeys = []string{eventKey, ""}
		insertPushEffectObservedEvent(t, f, eventKey, f.prepared.DeliveryIDs[0])
		if _, err := f.store.CreatePushEffect(ctx, f.prepared); err != nil {
			t.Fatal(err)
		}
		claimed, err := f.store.ClaimPushEffect(ctx, pusheffect.ClaimParams{
			Scope: f.prepared.Scope(), LeaseOwner: "receipt-worker",
			LeaseDuration: time.Minute,
		})
		if err != nil {
			t.Fatal(err)
		}
		receipt := pusheffect.SentReceipt{
			Scope: f.prepared.Scope(), ExpectedFence: claimed.Fence,
			LeaseOwner: claimed.LeaseOwner, ProviderMessageID: "om_atomic",
			ObservationEventKeys: f.prepared.ObservationEventKeys,
		}
		if err := f.store.RecordPushEffectSentWithDeliveries(ctx, receipt); err != nil {
			t.Fatal(err)
		}
		if err := f.store.RecordPushEffectSentWithDeliveries(ctx, receipt); err != nil {
			t.Fatalf("exact response-lost replay: %v", err)
		}

		var effectStatus, batchStatus string
		var sentDeliveries int
		var cardsMatch, observedDelivered bool
		if err := f.db.QueryRowContext(ctx, `
			SELECT
			  (SELECT status FROM push_effects WHERE id=$1),
			  (SELECT status FROM push_batches WHERE id=$9),
			  (SELECT count(*) FROM deliveries
			    WHERE id=ANY($2) AND status='sent'
			      AND feishu_message_id='om_atomic' AND sent_at IS NOT NULL),
			  NOT EXISTS (
			      SELECT 1 FROM deliveries
			       WHERE id=ANY($2) AND card_json<>$3::jsonb
			  ),
			  EXISTS (
			      SELECT 1 FROM task_observed_events
			       WHERE tenant_id=$4 AND user_id=$5 AND task_id=$6
			         AND delivery_id=$7 AND event_key=$8
			         AND status='delivered' AND delivered_at IS NOT NULL
			  )`,
			f.prepared.ID, f.prepared.DeliveryIDs, f.prepared.Card,
			f.prepared.TenantID, f.prepared.UserID, f.prepared.TaskID,
			f.prepared.DeliveryIDs[0], eventKey,
			f.prepared.BatchID,
		).Scan(
			&effectStatus,
			&batchStatus,
			&sentDeliveries,
			&cardsMatch,
			&observedDelivered,
		); err != nil {
			t.Fatal(err)
		}
		if effectStatus != string(pusheffect.StatusSent) ||
			batchStatus != string(types.BatchStatusDone) ||
			sentDeliveries != len(f.prepared.DeliveryIDs) ||
			!cardsMatch || !observedDelivered {
			t.Fatalf(
				"effect/batch/deliveries=%s/%s/%d cardsMatch=%v observed=%v",
				effectStatus,
				batchStatus,
				sentDeliveries,
				cardsMatch,
				observedDelivered,
			)
		}

		if _, err := f.db.ExecContext(ctx, `
			UPDATE deliveries SET card_json='{"drift":true}'::jsonb
			 WHERE id=$1`, f.prepared.DeliveryIDs[0]); err != nil {
			t.Fatal(err)
		}
		if err := f.store.RecordPushEffectSentWithDeliveries(
			ctx, receipt,
		); !errors.Is(err, types.ErrConflict) {
			t.Fatalf("tampered replay error=%v, want conflict", err)
		}
	})

	t.Run("effect write failure rolls delivery updates back", func(t *testing.T) {
		f := newPushEffectFixture(t)
		ctx := t.Context()
		if _, err := f.provider.UpTo(ctx, 48); err != nil {
			t.Fatalf("migrate to 048: %v", err)
		}
		eventKey := strings.Repeat("c", 64)
		f.prepared.ObservationEventKeys = []string{eventKey, ""}
		insertPushEffectObservedEvent(t, f, eventKey, f.prepared.DeliveryIDs[0])
		if _, err := f.store.CreatePushEffect(ctx, f.prepared); err != nil {
			t.Fatal(err)
		}
		claimed, err := f.store.ClaimPushEffect(ctx, pusheffect.ClaimParams{
			Scope: f.prepared.Scope(), LeaseOwner: "rollback-worker",
			LeaseDuration: time.Minute,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.db.ExecContext(ctx, `
			CREATE FUNCTION fail_atomic_effect_receipt() RETURNS trigger
			LANGUAGE plpgsql AS $$
			BEGIN
			  IF NEW.status='sent' THEN
			    RAISE EXCEPTION 'injected effect receipt failure';
			  END IF;
			  RETURN NEW;
			END $$;
			CREATE TRIGGER fail_atomic_effect_receipt
			BEFORE UPDATE ON push_effects
			FOR EACH ROW EXECUTE FUNCTION fail_atomic_effect_receipt()`); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			cleanupCtx, cancel := cleanupContext()
			defer cancel()
			if _, cleanupErr := f.db.ExecContext(cleanupCtx,
				`DROP TRIGGER IF EXISTS fail_atomic_effect_receipt ON push_effects`,
			); cleanupErr != nil {
				t.Errorf("drop injected trigger: %v", cleanupErr)
			}
			if _, cleanupErr := f.db.ExecContext(cleanupCtx,
				`DROP FUNCTION IF EXISTS fail_atomic_effect_receipt()`,
			); cleanupErr != nil {
				t.Errorf("drop injected function: %v", cleanupErr)
			}
		})

		err = f.store.RecordPushEffectSentWithDeliveries(
			ctx,
			pusheffect.SentReceipt{
				Scope: f.prepared.Scope(), ExpectedFence: claimed.Fence,
				LeaseOwner:           claimed.LeaseOwner,
				ProviderMessageID:    "om_must_rollback",
				ObservationEventKeys: f.prepared.ObservationEventKeys,
			},
		)
		if err == nil {
			t.Fatal("injected effect receipt failure returned nil")
		}
		var effectStatus, batchStatus string
		var pending int
		var observationQualified bool
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
			  )`,
			f.prepared.ID, f.prepared.DeliveryIDs,
			f.prepared.TenantID, f.prepared.UserID, f.prepared.TaskID,
			f.prepared.DeliveryIDs[0], eventKey,
			f.prepared.BatchID,
		).Scan(
			&effectStatus,
			&batchStatus,
			&pending,
			&observationQualified,
		); err != nil {
			t.Fatal(err)
		}
		if effectStatus != string(pusheffect.StatusSending) ||
			batchStatus != string(types.BatchStatusPending) ||
			pending != len(f.prepared.DeliveryIDs) ||
			!observationQualified {
			t.Fatalf(
				"partial commit effect=%s batch=%s pending=%d observationQualified=%v",
				effectStatus,
				batchStatus,
				pending,
				observationQualified,
			)
		}
	})
}

func TestPushEffectObservedEventReceiptRejectsNonExactBinding(t *testing.T) {
	tests := []struct {
		name         string
		wrongReceipt bool
		mutate       func(*testing.T, pushEffectFixture, string)
	}{
		{
			name:         "wrong receipt event key",
			wrongReceipt: true,
			mutate: func(*testing.T, pushEffectFixture, string) {
			},
		},
		{
			name: "wrong event key",
			mutate: func(t *testing.T, f pushEffectFixture, _ string) {
				t.Helper()
				if _, err := f.db.ExecContext(t.Context(), `
					UPDATE task_observed_events
					   SET event_key=$1
					 WHERE tenant_id=$2 AND delivery_id=$3`,
					strings.Repeat("d", 64),
					f.prepared.TenantID,
					f.prepared.DeliveryIDs[0],
				); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "wrong delivery binding",
			mutate: func(t *testing.T, f pushEffectFixture, _ string) {
				t.Helper()
				if _, err := f.db.ExecContext(t.Context(), `
					UPDATE task_observed_events
					   SET delivery_id=$1
					 WHERE tenant_id=$2 AND delivery_id=$3`,
					f.prepared.DeliveryIDs[1],
					f.prepared.TenantID,
					f.prepared.DeliveryIDs[0],
				); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "wrong tenant binding",
			mutate: func(t *testing.T, f pushEffectFixture, _ string) {
				t.Helper()
				if _, err := f.db.ExecContext(t.Context(), `
					INSERT INTO tenants (id,status,plan)
					VALUES (2,'active','free')
					ON CONFLICT (id) DO NOTHING`); err != nil {
					t.Fatal(err)
				}
				if _, err := f.db.ExecContext(t.Context(), `
					UPDATE task_observed_events
					   SET tenant_id=2
					 WHERE tenant_id=$1 AND delivery_id=$2`,
					f.prepared.TenantID,
					f.prepared.DeliveryIDs[0],
				); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "missing qualified event",
			mutate: func(t *testing.T, f pushEffectFixture, _ string) {
				t.Helper()
				if _, err := f.db.ExecContext(t.Context(), `
					DELETE FROM task_observed_events
					 WHERE tenant_id=$1 AND delivery_id=$2`,
					f.prepared.TenantID,
					f.prepared.DeliveryIDs[0],
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
			if _, err := f.provider.UpTo(ctx, 48); err != nil {
				t.Fatalf("migrate to 048: %v", err)
			}
			eventKey := strings.Repeat("b", 64)
			f.prepared.ObservationEventKeys = []string{eventKey, ""}
			insertPushEffectObservedEvent(
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
					LeaseOwner:    "non-exact-worker",
					LeaseDuration: time.Minute,
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			test.mutate(t, f, eventKey)
			receiptKeys := f.prepared.ObservationEventKeys
			if test.wrongReceipt {
				receiptKeys = []string{strings.Repeat("f", 64), ""}
			}
			err = f.store.RecordPushEffectSentWithDeliveries(
				ctx,
				pusheffect.SentReceipt{
					Scope:                f.prepared.Scope(),
					ExpectedFence:        claimed.Fence,
					LeaseOwner:           claimed.LeaseOwner,
					ProviderMessageID:    "om_non_exact",
					ObservationEventKeys: receiptKeys,
				},
			)
			if !errors.Is(err, types.ErrConflict) {
				t.Fatalf("non-exact receipt error=%v, want conflict", err)
			}
			assertPushEffectReceiptUncommitted(t, f)
		})
	}
}

func TestPushEffectPositiveHistorySettlesFrozenObservedEvent(t *testing.T) {
	f := newPushEffectFixture(t)
	ctx := t.Context()
	if _, err := f.provider.UpTo(ctx, 48); err != nil {
		t.Fatalf("migrate to 048: %v", err)
	}
	eventKey := strings.Repeat("e", 64)
	f.prepared.ObservationEventKeys = []string{eventKey, ""}
	insertPushEffectObservedEvent(t, f, eventKey, f.prepared.DeliveryIDs[0])
	if _, err := f.store.CreatePushEffect(ctx, f.prepared); err != nil {
		t.Fatal(err)
	}
	claimed, err := f.store.ClaimPushEffect(ctx, pusheffect.ClaimParams{
		Scope: f.prepared.Scope(), LeaseOwner: "history-worker",
		LeaseDuration: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.store.RecordPushEffectAmbiguous(
		ctx,
		pusheffect.FailureParams{
			Lease: pusheffect.Lease{
				Scope:      f.prepared.Scope(),
				LeaseOwner: claimed.LeaseOwner,
				Fence:      claimed.Fence,
			},
			Class: "provider_response_unknown",
		},
	); err != nil {
		t.Fatal(err)
	}
	// Provider-history settlement intentionally has no live workflow event
	// input. The Store must derive the exact keys from immutable effect bytes.
	receipt := pusheffect.SentReceipt{
		Scope:             f.prepared.Scope(),
		ExpectedFence:     claimed.Fence,
		ProviderMessageID: "om_history",
	}
	if err := f.store.RecordPushEffectSentWithDeliveries(
		ctx,
		receipt,
	); err != nil {
		t.Fatal(err)
	}

	// Simulate a pre-fix partial observation receipt. Exact sent replay is the
	// recovery-only settlement path and repairs it without resending.
	if _, err := f.db.ExecContext(ctx, `
		UPDATE task_observed_events
		   SET status='qualified',delivered_at=NULL
		 WHERE tenant_id=$1 AND delivery_id=$2`,
		f.prepared.TenantID,
		f.prepared.DeliveryIDs[0],
	); err != nil {
		t.Fatal(err)
	}
	if err := f.store.RecordPushEffectSentWithDeliveries(
		ctx,
		receipt,
	); err != nil {
		t.Fatalf("exact history receipt replay: %v", err)
	}
	var delivered bool
	if err := f.db.QueryRowContext(ctx, `
		SELECT status='delivered' AND delivered_at IS NOT NULL
		  FROM task_observed_events
		 WHERE tenant_id=$1 AND delivery_id=$2 AND event_key=$3`,
		f.prepared.TenantID,
		f.prepared.DeliveryIDs[0],
		eventKey,
	).Scan(&delivered); err != nil {
		t.Fatal(err)
	}
	if !delivered {
		t.Fatal("history/recovery-only receipt left observation undelivered")
	}
}

func insertPushEffectObservedEvent(
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
		    $1,$2,$3,$4,$5,'release','fixture',clock_timestamp(),
		    '{"source":"fixture"}'::jsonb,$6,$7,$8,'qualified'
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

func assertPushEffectReceiptUncommitted(
	t *testing.T,
	f pushEffectFixture,
) {
	t.Helper()
	var effectStatus string
	var pending, qualified int
	if err := f.db.QueryRowContext(t.Context(), `
		SELECT
		  (SELECT status FROM push_effects WHERE id=$1),
		  (SELECT count(*) FROM deliveries
		    WHERE id=ANY($2) AND status='pending'
		      AND feishu_message_id='' AND sent_at IS NULL),
		  (SELECT count(*) FROM task_observed_events
		    WHERE tenant_id=$3 AND status='qualified'
		      AND delivered_at IS NULL)`,
		f.prepared.ID,
		f.prepared.DeliveryIDs,
		f.prepared.TenantID,
	).Scan(&effectStatus, &pending, &qualified); err != nil {
		t.Fatal(err)
	}
	if effectStatus != string(pusheffect.StatusSending) ||
		pending != len(f.prepared.DeliveryIDs) ||
		qualified > 1 {
		t.Fatalf(
			"receipt partially committed effect=%s pending=%d qualified=%d",
			effectStatus,
			pending,
			qualified,
		)
	}
}

func TestMigration047ReceiptRoleAndDowngradeFence(t *testing.T) {
	f := newPushEffectFixtureAt(t, 47)
	ctx := t.Context()
	installMigration047AuthorityCompatibility(t, f)
	var (
		effectCheckpointRead, deliveryRead, deliveryUpdate bool
		deliveryInsert, effectIdentityUpdate               bool
	)
	if err := f.db.QueryRowContext(ctx, `
		SELECT
		  has_column_privilege(
		    'vane_push_effect_receipt','push_effects','card_payload','SELECT'),
		  has_column_privilege(
		    'vane_push_effect_receipt','deliveries','card_json','SELECT'),
		  has_column_privilege(
		    'vane_push_effect_receipt','deliveries','card_json','UPDATE'),
		  has_column_privilege(
		    'vane_push_effect_receipt','deliveries','card_json','INSERT'),
		  has_column_privilege(
		    'vane_push_effect_receipt','push_effects','card_payload','UPDATE')`,
	).Scan(
		&effectCheckpointRead, &deliveryRead, &deliveryUpdate,
		&deliveryInsert, &effectIdentityUpdate,
	); err != nil {
		t.Fatal(err)
	}
	if !effectCheckpointRead || !deliveryRead || !deliveryUpdate ||
		deliveryInsert || effectIdentityUpdate {
		t.Fatalf("047 privilege drift: effectRead=%v delivery=%v/%v insert=%v effectUpdate=%v",
			effectCheckpointRead, deliveryRead, deliveryUpdate,
			deliveryInsert, effectIdentityUpdate)
	}

	if _, err := f.store.CreatePushEffect(ctx, f.prepared); err != nil {
		t.Fatal(err)
	}
	if _, err := f.provider.Down(ctx); err == nil {
		t.Fatal("047 downgrade accepted durable effect")
	}
	var version int
	if err := f.db.QueryRowContext(ctx, `
		SELECT COALESCE(max(version_id),0)
		  FROM goose_db_version WHERE is_applied`,
	).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 47 {
		t.Fatalf("failed 047 down changed version to %d", version)
	}
}
