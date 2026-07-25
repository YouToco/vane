package store

import (
	"context"
	"testing"
	"time"
)

func TestMigration048RestrictedReceiptCapabilitiesRLSAndExactDown(t *testing.T) {
	f := pushBatchAuthorityFixture(t)
	ctx := t.Context()
	if _, err := f.provider.UpTo(ctx, 48); err != nil {
		t.Fatalf("migrate to 048: %v", err)
	}

	var (
		deliveryRead, deliveryUpdate, deliveryInsert bool
		eventRead, eventUpdate, eventInsert          bool
		batchUpdate, effectPayloadRead               bool
		aggregateCoordinator, aggregateApp           bool
		observationApp, observationPublic            bool
		policyCount                                  int
	)
	if err := f.db.QueryRowContext(ctx, `
		SELECT
		  has_column_privilege(
		    'vane_push_effect_receipt','deliveries','card_json','SELECT'),
		  has_column_privilege(
		    'vane_push_effect_receipt','deliveries','card_json','UPDATE'),
		  has_table_privilege(
		    'vane_push_effect_receipt','deliveries','INSERT'),
		  has_column_privilege(
		    'vane_push_effect_receipt','task_observed_events','event_key','SELECT'),
		  has_column_privilege(
		    'vane_push_effect_receipt','task_observed_events','status','UPDATE'),
		  has_table_privilege(
		    'vane_push_effect_receipt','task_observed_events','INSERT'),
		  has_column_privilege(
		    'vane_push_effect_receipt','push_batches','status','UPDATE'),
		  has_column_privilege(
		    'vane_push_effect_receipt','push_effects','canonical_payload','SELECT'),
		  has_function_privilege(
		    'vane_push_effect_coordinator',
		    'lock_push_effect_aggregate_v1(bigint,bigint,bigint,bigint[])',
		    'EXECUTE'),
		  has_function_privilege(
		    'vane_app',
		    'lock_push_effect_aggregate_v1(bigint,bigint,bigint,bigint[])',
		    'EXECUTE'),
		  has_function_privilege(
		    'vane_app',
		    'lock_observed_event_push_effects_v1(bigint,bigint,bigint[])',
		    'EXECUTE'),
		  has_function_privilege(
		    'vane_push_effect_receipt',
		    'lock_observed_event_push_effects_v1(bigint,bigint,bigint[])',
		    'EXECUTE'),
		  (SELECT count(*) FROM pg_policies
		    WHERE schemaname='public' AND tablename='task_observed_events'
		      AND policyname LIKE 'push_effect_receipt_%')`,
	).Scan(
		&deliveryRead,
		&deliveryUpdate,
		&deliveryInsert,
		&eventRead,
		&eventUpdate,
		&eventInsert,
		&batchUpdate,
		&effectPayloadRead,
		&aggregateCoordinator,
		&aggregateApp,
		&observationApp,
		&observationPublic,
		&policyCount,
	); err != nil {
		t.Fatal(err)
	}
	if !deliveryRead || !deliveryUpdate || deliveryInsert ||
		!eventRead || !eventUpdate || eventInsert ||
		!batchUpdate || !effectPayloadRead ||
		!aggregateCoordinator || aggregateApp ||
		!observationApp || observationPublic ||
		policyCount != 3 {
		t.Fatalf(
			"048 capability drift delivery=%v/%v/%v event=%v/%v/%v "+
				"batch/effect=%v/%v aggregate=%v/%v observation=%v/%v policies=%d",
			deliveryRead,
			deliveryUpdate,
			deliveryInsert,
			eventRead,
			eventUpdate,
			eventInsert,
			batchUpdate,
			effectPayloadRead,
			aggregateCoordinator,
			aggregateApp,
			observationApp,
			observationPublic,
			policyCount,
		)
	}

	eventKey := "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	insertPRBObservedEvent(
		t, f, eventKey, f.prepared.DeliveryIDs[0],
	)
	tx, err := f.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(
		ctx, `SET LOCAL ROLE vane_push_effect_receipt`,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(
		ctx, `SELECT set_config('app.tenant_id','2',true)`,
	); err != nil {
		t.Fatal(err)
	}
	var visible int
	if err := tx.QueryRowContext(ctx, `
		SELECT count(event_key)
		  FROM task_observed_events
		 WHERE tenant_id=$1 AND event_key=$2`,
		f.prepared.TenantID,
		eventKey,
	).Scan(&visible); err != nil {
		t.Fatal(err)
	}
	if visible != 0 {
		t.Fatalf("receipt role saw %d cross-tenant observed events", visible)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	if _, err := f.db.ExecContext(ctx, `
		DELETE FROM task_observed_events WHERE tenant_id=$1 AND event_key=$2`,
		f.prepared.TenantID,
		eventKey,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := f.provider.Down(ctx); err != nil {
		t.Fatalf("empty 048 down: %v", err)
	}
	var (
		version, remainingPolicies               int
		deliveryStillRead, eventStillUpdate      bool
		baselineDigestRead, authorityBatchRead   bool
		aggregateExists, observationHelperExists bool
	)
	if err := f.db.QueryRowContext(ctx, `
		SELECT
		  (SELECT COALESCE(max(version_id),0)
		     FROM goose_db_version WHERE is_applied),
		  has_column_privilege(
		    'vane_push_effect_receipt','deliveries','card_json','SELECT'),
		  has_column_privilege(
		    'vane_push_effect_receipt','task_observed_events','status','UPDATE'),
		  has_column_privilege(
		    'vane_push_effect_receipt','push_effects','payload_digest','SELECT'),
		  has_column_privilege(
		    'vane_push_effect_receipt','push_effects','batch_id','SELECT'),
		  to_regprocedure(
		    'lock_push_effect_aggregate_v1(bigint,bigint,bigint,bigint[])')
		    IS NOT NULL,
		  to_regprocedure(
		    'lock_observed_event_push_effects_v1(bigint,bigint,bigint[])')
		    IS NOT NULL,
		  (SELECT count(*) FROM pg_policies
		    WHERE schemaname='public' AND tablename='task_observed_events'
		      AND policyname LIKE 'push_effect_receipt_%')`,
	).Scan(
		&version,
		&deliveryStillRead,
		&eventStillUpdate,
		&baselineDigestRead,
		&authorityBatchRead,
		&aggregateExists,
		&observationHelperExists,
		&remainingPolicies,
	); err != nil {
		t.Fatal(err)
	}
	if version != 47 || deliveryStillRead || eventStillUpdate ||
		!baselineDigestRead || !authorityBatchRead ||
		aggregateExists || observationHelperExists ||
		remainingPolicies != 0 {
		t.Fatalf(
			"048 down drift version=%d delivery/event=%v/%v baseline=%v/%v "+
				"functions=%v/%v policies=%d",
			version,
			deliveryStillRead,
			eventStillUpdate,
			baselineDigestRead,
			authorityBatchRead,
			aggregateExists,
			observationHelperExists,
			remainingPolicies,
		)
	}
}

func TestMigration048DownSerializesWithSettlementSchemaFence(t *testing.T) {
	f := pushBatchAuthorityFixture(t)
	ctx := t.Context()
	if _, err := f.provider.UpTo(ctx, 48); err != nil {
		t.Fatalf("migrate to 048: %v", err)
	}
	writer, err := f.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	writerDone := false
	defer func() {
		if !writerDone {
			_ = writer.Rollback()
		}
	}()
	if _, err := writer.ExecContext(ctx,
		`SELECT pg_advisory_xact_lock_shared(6215335020355474248)`,
	); err != nil {
		t.Fatal(err)
	}

	downStarted := make(chan struct{})
	downDone := make(chan error, 1)
	go func() {
		close(downStarted)
		_, downErr := f.provider.Down(context.Background())
		downDone <- downErr
	}()
	<-downStarted
	select {
	case err := <-downDone:
		t.Fatalf("048 Down bypassed active writer fence: %v", err)
	case <-time.After(150 * time.Millisecond):
	}
	if err := writer.Rollback(); err != nil {
		t.Fatal(err)
	}
	writerDone = true
	select {
	case err := <-downDone:
		if err != nil {
			t.Fatalf("048 Down after writer release: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("048 Down did not resume after writer fence released")
	}
}

func TestMigration048RefusesEffectAuthorityDowngrade(t *testing.T) {
	f := newPushEffectFixture(t)
	ctx := t.Context()
	if _, err := f.provider.UpTo(ctx, 48); err != nil {
		t.Fatalf("migrate to 048: %v", err)
	}
	if _, err := f.provider.Down(ctx); err == nil {
		t.Fatal("048 downgrade accepted effect-authority batch")
	}
	var version int
	if err := f.db.QueryRowContext(ctx, `
		SELECT COALESCE(max(version_id),0)
		  FROM goose_db_version WHERE is_applied`,
	).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 48 {
		t.Fatalf("failed 048 down changed migration version to %d", version)
	}
}
