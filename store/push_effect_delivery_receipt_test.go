package store

import (
	"errors"
	"testing"
	"time"

	"github.com/YouToco/vane/pusheffect"
	"github.com/YouToco/vane/types"
)

func TestPushEffectSentAndDeliveryReceiptsAreAtomic(t *testing.T) {
	t.Run("success and exact replay", func(t *testing.T) {
		f := newPushEffectFixture(t)
		ctx := t.Context()
		if _, err := f.provider.UpTo(ctx, 47); err != nil {
			t.Fatalf("migrate to 047: %v", err)
		}
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
		}
		if err := f.store.RecordPushEffectSentWithDeliveries(ctx, receipt); err != nil {
			t.Fatal(err)
		}
		if err := f.store.RecordPushEffectSentWithDeliveries(ctx, receipt); err != nil {
			t.Fatalf("exact response-lost replay: %v", err)
		}

		var effectStatus string
		var sentDeliveries int
		var cardsMatch bool
		if err := f.db.QueryRowContext(ctx, `
			SELECT
			  (SELECT status FROM push_effects WHERE id=$1),
			  (SELECT count(*) FROM deliveries
			    WHERE id=ANY($2) AND status='sent'
			      AND feishu_message_id='om_atomic' AND sent_at IS NOT NULL),
			  NOT EXISTS (
			      SELECT 1 FROM deliveries
			       WHERE id=ANY($2) AND card_json<>$3::jsonb
			  )`,
			f.prepared.ID, f.prepared.DeliveryIDs, f.prepared.Card,
		).Scan(&effectStatus, &sentDeliveries, &cardsMatch); err != nil {
			t.Fatal(err)
		}
		if effectStatus != string(pusheffect.StatusSent) ||
			sentDeliveries != len(f.prepared.DeliveryIDs) || !cardsMatch {
			t.Fatalf("effect/deliveries=%s/%d cardsMatch=%v",
				effectStatus, sentDeliveries, cardsMatch)
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
		if _, err := f.provider.UpTo(ctx, 47); err != nil {
			t.Fatalf("migrate to 047: %v", err)
		}
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
				LeaseOwner:        claimed.LeaseOwner,
				ProviderMessageID: "om_must_rollback",
			},
		)
		if err == nil {
			t.Fatal("injected effect receipt failure returned nil")
		}
		var effectStatus string
		var pending int
		if err := f.db.QueryRowContext(ctx, `
			SELECT
			  (SELECT status FROM push_effects WHERE id=$1),
			  (SELECT count(*) FROM deliveries
			    WHERE id=ANY($2) AND status='pending'
			      AND feishu_message_id='' AND sent_at IS NULL)`,
			f.prepared.ID, f.prepared.DeliveryIDs,
		).Scan(&effectStatus, &pending); err != nil {
			t.Fatal(err)
		}
		if effectStatus != string(pusheffect.StatusSending) ||
			pending != len(f.prepared.DeliveryIDs) {
			t.Fatalf("partial commit effect=%s pending=%d",
				effectStatus, pending)
		}
	})
}

func TestMigration047ReceiptRoleAndDowngradeFence(t *testing.T) {
	f := newPushEffectFixture(t)
	ctx := t.Context()
	if _, err := f.provider.UpTo(ctx, 47); err != nil {
		t.Fatalf("migrate to 047: %v", err)
	}
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
