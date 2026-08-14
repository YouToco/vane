package store

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestMigration080EmptyDownSucceeds(t *testing.T) {
	_, db, provider := openMigration066Database(t, "vane_provider_pricing_080_empty_down")
	if _, err := provider.UpTo(t.Context(), 80); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.DownTo(t.Context(), 79); err != nil {
		t.Fatalf("empty 080 Down failed: %v", err)
	}
	var tableExists, pricingColumnExists bool
	if err := db.QueryRowContext(t.Context(), `
		SELECT to_regclass('public.provider_price_rules') IS NOT NULL,
		       EXISTS (
		           SELECT 1
		             FROM information_schema.columns
		            WHERE table_schema='public'
		              AND table_name='llm_calls'
		              AND column_name='pricing_status'
		       )`,
	).Scan(&tableExists, &pricingColumnExists); err != nil {
		t.Fatal(err)
	}
	if tableExists || pricingColumnExists {
		t.Fatalf("empty Down left pricing schema table=%v column=%v",
			tableExists, pricingColumnExists)
	}
}

func TestMigration080DownSerializesWithWriterAndRefusesDurableReceipt(t *testing.T) {
	_, db, provider := openMigration066Database(t, "vane_provider_pricing_080_writer_down")
	if _, err := provider.UpTo(t.Context(), 80); err != nil {
		t.Fatal(err)
	}

	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(t.Context(), `
		SELECT pg_advisory_xact_lock_shared(
		    hashtextextended('vane-provider-pricing-v1', 0)
		)`); err != nil {
		t.Fatal(err)
	}

	downDone := make(chan error, 1)
	go func() {
		_, err := provider.DownTo(context.Background(), 79)
		downDone <- err
	}()
	select {
	case err := <-downDone:
		t.Fatalf("080 Down crossed an active writer fence: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	if _, err := tx.ExecContext(t.Context(), `
		INSERT INTO llm_calls (
		    trace_id,span_name,provider,model,pricing_status
		) VALUES (
		    'migration-080-concurrent','score','unknown','unknown','unpriced'
		)`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	err = <-downDone
	if err == nil || !strings.Contains(
		err.Error(), "refusing Down while provider pricing ledger state exists",
	) {
		t.Fatalf("080 Down accepted durable call receipt: %v", err)
	}

	var version int64
	if err := db.QueryRowContext(t.Context(), `
		SELECT COALESCE(max(version_id),0)
		  FROM goose_db_version
		 WHERE is_applied`,
	).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 80 {
		t.Fatalf("failed 080 Down changed migration version to %d", version)
	}
}

func TestMigration080DownRefusesAdministratorPriceVersion(t *testing.T) {
	_, db, provider := openMigration066Database(t, "vane_provider_pricing_080_admin_down")
	if _, err := provider.UpTo(t.Context(), 80); err != nil {
		t.Fatal(err)
	}
	var userID int64
	if err := db.QueryRowContext(t.Context(), `
		INSERT INTO users(feishu_open_id,name)
		VALUES('migration-080-admin','pricing admin')
		RETURNING id`,
	).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `
		INSERT INTO provider_price_rules (
		    provider,resource,meter,currency,
		    request_unit_price,request_included_quantity,
		    request_additional_unit_price,effective_from,
		    source_url,note,created_by,change_id,request_hash
		) VALUES (
		    'migration-080','/custom','request','USD',
		    0.01,1,0.01,now(),
		    'https://example.com/pricing','admin version',$1,
		    'migration-080-admin-change',repeat('a',64)
		)`, userID); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.DownTo(t.Context(), 79); err == nil ||
		!strings.Contains(err.Error(),
			"refusing Down while provider pricing ledger state exists") {
		t.Fatalf("080 Down accepted administrator price history: %v", err)
	}
}
