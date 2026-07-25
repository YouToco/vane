package store

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/YouToco/vane/pusheffect"
	"github.com/YouToco/vane/types"
)

const migration047TestPauseKey int64 = pushEffectSchemaFenceKey - 47

func migration047FenceFixture(t *testing.T) pushEffectFixture {
	t.Helper()
	f := newPushEffectFixtureAt(t, 47)
	installMigration047AuthorityCompatibility(t, f)
	return f
}

func installMigration047AuthorityCompatibility(
	t *testing.T,
	f pushEffectFixture,
) {
	t.Helper()
	if _, err := f.db.ExecContext(t.Context(), `
		ALTER TABLE push_batches ADD COLUMN delivery_authority TEXT;
		UPDATE push_batches SET delivery_authority='effect' WHERE id=$1;
		GRANT SELECT (delivery_authority)
		    ON push_batches TO vane_push_effect_coordinator`,
		f.prepared.BatchID,
	); err != nil {
		t.Fatal(err)
	}
}

func TestMigration047CreateFirstSerializesDowngrade(t *testing.T) {
	f := migration047FenceFixture(t)
	ctx := t.Context()
	installMigration047PauseTrigger(t, f, "push_effects", "INSERT")

	pauseTx, err := f.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	pauseDone := false
	defer func() {
		if !pauseDone {
			_ = pauseTx.Rollback()
		}
	}()
	if _, err := pauseTx.ExecContext(ctx,
		`SELECT pg_advisory_xact_lock($1)`,
		migration047TestPauseKey,
	); err != nil {
		t.Fatal(err)
	}

	createDone := make(chan error, 1)
	go func() {
		_, createErr := f.store.CreatePushEffect(ctx, f.prepared)
		createDone <- createErr
	}()
	if !waitForScratchQueryLock(
		ctx, f.db, "%INSERT INTO push_effects%", 5*time.Second,
	) {
		t.Fatal("CreatePushEffect did not reach the post-provenance pause")
	}

	downDone := make(chan error, 1)
	go func() {
		_, downErr := f.provider.Down(ctx)
		downDone <- downErr
	}()
	if !waitForScratchQueryLock(
		ctx,
		f.db,
		fmt.Sprintf("%%pg_advisory_xact_lock(%d)%%", pushEffectSchemaFenceKey),
		5*time.Second,
	) {
		t.Fatal("047 Down did not wait at schema admission")
	}
	if err := pauseTx.Commit(); err != nil {
		t.Fatal(err)
	}
	pauseDone = true

	select {
	case createErr := <-createDone:
		if createErr != nil {
			t.Fatalf("create-first writer failed: %v", createErr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("create-first writer did not converge")
	}
	select {
	case downErr := <-downDone:
		if downErr == nil ||
			!strings.Contains(downErr.Error(), "refusing downgrade") ||
			strings.Contains(downErr.Error(), "40P01") {
			t.Fatalf("create-first Down result=%v", downErr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("create-first Down did not converge")
	}
	assertMigration047State(t, f, 47, 1, true)
}

func TestMigration047DowngradeFirstRejectsWaitingCreate(t *testing.T) {
	f := migration047FenceFixture(t)
	ctx := t.Context()

	tableBlocker, err := f.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	blockerDone := false
	defer func() {
		if !blockerDone {
			_ = tableBlocker.Rollback()
		}
	}()
	if _, err := tableBlocker.ExecContext(ctx,
		`LOCK TABLE push_effects IN ACCESS SHARE MODE`,
	); err != nil {
		t.Fatal(err)
	}

	downDone := make(chan error, 1)
	go func() {
		_, downErr := f.provider.Down(ctx)
		downDone <- downErr
	}()
	if !waitForScratchQueryLock(
		ctx, f.db, "%migration 047 downgrade fence%", 5*time.Second,
	) {
		t.Fatal("047 Down did not reach its table fence")
	}

	createDone := make(chan error, 1)
	go func() {
		_, createErr := f.store.CreatePushEffect(ctx, f.prepared)
		createDone <- createErr
	}()
	if !waitForScratchQueryLock(
		ctx, f.db, "%pg_advisory_xact_lock_shared%", 5*time.Second,
	) {
		t.Fatal("CreatePushEffect did not wait behind 047 Down admission")
	}
	if err := tableBlocker.Commit(); err != nil {
		t.Fatal(err)
	}
	blockerDone = true

	select {
	case downErr := <-downDone:
		if downErr != nil {
			t.Fatalf("down-first downgrade: %v", downErr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("down-first downgrade did not converge")
	}
	select {
	case createErr := <-createDone:
		var appErr *types.AppError
		if !errors.As(createErr, &appErr) ||
			appErr.Code != types.CodeDatabase ||
			strings.Contains(createErr.Error(), "40P01") {
			t.Fatalf("down-first create result=%v", createErr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("down-first create did not converge")
	}
	assertMigration047State(t, f, 46, 0, false)

	if _, err := f.provider.UpTo(ctx, 47); err != nil {
		t.Fatalf("re-up 047: %v", err)
	}
	assertMigration047State(t, f, 47, 0, true)
	if _, err := f.store.CreatePushEffect(ctx, f.prepared); err != nil {
		t.Fatalf("re-up did not restore runtime admission: %v", err)
	}
}

func TestMigration047ReceiptFirstSerializesDowngrade(t *testing.T) {
	f := migration047FenceFixture(t)
	ctx := t.Context()
	// The current binary decodes canonical observation truth. Those additional
	// read grants belong to 048; this compatibility grant isolates the 047 lock
	// graph under test without changing 047's historical privilege contract.
	grantMigration048ReceiptCompatibility(t, f)
	if _, err := f.store.CreatePushEffect(ctx, f.prepared); err != nil {
		t.Fatal(err)
	}
	claimed, err := f.store.ClaimPushEffect(
		ctx,
		pusheffect.ClaimParams{
			Scope:         f.prepared.Scope(),
			LeaseOwner:    "migration-047-receipt",
			LeaseDuration: time.Minute,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	installMigration047PauseTrigger(t, f, "deliveries", "UPDATE")
	pauseTx, err := f.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	pauseDone := false
	defer func() {
		if !pauseDone {
			_ = pauseTx.Rollback()
		}
	}()
	if _, err := pauseTx.ExecContext(ctx,
		`SELECT pg_advisory_xact_lock($1)`,
		migration047TestPauseKey,
	); err != nil {
		t.Fatal(err)
	}

	receiptDone := make(chan error, 1)
	go func() {
		receiptDone <- f.store.RecordPushEffectSentWithDeliveries(
			ctx,
			pusheffect.SentReceipt{
				Scope:             claimed.Scope(),
				ExpectedFence:     claimed.Fence,
				LeaseOwner:        claimed.LeaseOwner,
				ProviderMessageID: "om_migration_047",
			},
		)
	}()
	if !waitForScratchQueryLock(
		ctx, f.db, "%UPDATE deliveries%", 5*time.Second,
	) {
		t.Fatal("receipt did not reach the delivery pause")
	}

	downDone := make(chan error, 1)
	go func() {
		_, downErr := f.provider.Down(ctx)
		downDone <- downErr
	}()
	if !waitForScratchQueryLock(
		ctx,
		f.db,
		fmt.Sprintf("%%pg_advisory_xact_lock(%d)%%", pushEffectSchemaFenceKey),
		5*time.Second,
	) {
		t.Fatal("047 Down did not wait behind receipt admission")
	}
	if err := pauseTx.Commit(); err != nil {
		t.Fatal(err)
	}
	pauseDone = true

	select {
	case receiptErr := <-receiptDone:
		if receiptErr != nil {
			t.Fatalf("receipt-first writer failed: %v", receiptErr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("receipt-first writer did not converge")
	}
	select {
	case downErr := <-downDone:
		if downErr == nil ||
			!strings.Contains(downErr.Error(), "refusing downgrade") ||
			strings.Contains(downErr.Error(), "40P01") {
			t.Fatalf("receipt-first Down result=%v", downErr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("receipt-first Down did not converge")
	}
	assertMigration047State(t, f, 47, 1, true)
}

func installMigration047PauseTrigger(
	t *testing.T,
	f pushEffectFixture,
	table, operation string,
) {
	t.Helper()
	ctx := t.Context()
	functionName := "pause_migration_047_" + strings.ToLower(table)
	triggerName := functionName + "_trigger"
	statement := fmt.Sprintf(`
		CREATE FUNCTION %s() RETURNS trigger
		LANGUAGE plpgsql AS $$
		BEGIN
		    PERFORM pg_advisory_xact_lock(%d);
		    RETURN NEW;
		END $$;
		CREATE TRIGGER %s
		BEFORE %s ON %s
		FOR EACH ROW EXECUTE FUNCTION %s()`,
		functionName,
		migration047TestPauseKey,
		triggerName,
		operation,
		table,
		functionName,
	)
	if _, err := f.db.ExecContext(ctx, statement); err != nil {
		t.Fatal(err)
	}
}

func grantMigration048ReceiptCompatibility(
	t *testing.T,
	f pushEffectFixture,
) {
	t.Helper()
	if _, err := f.db.ExecContext(t.Context(), `
		GRANT SELECT (canonical_payload,payload_digest)
		    ON push_effects TO vane_push_effect_receipt;
		GRANT SELECT (
		    event_key,tenant_id,user_id,task_id,run_snapshot_id,
		    temporal_run_id,delivery_id,status,delivered_at
		) ON task_observed_events TO vane_push_effect_receipt;
		GRANT UPDATE (status,delivered_at)
		    ON task_observed_events TO vane_push_effect_receipt;
		CREATE POLICY migration_047_receipt_select
		    ON task_observed_events
		    FOR SELECT TO vane_push_effect_receipt USING (true);
		CREATE POLICY migration_047_receipt_update
		    ON task_observed_events
		    FOR UPDATE TO vane_push_effect_receipt
		    USING (true) WITH CHECK (true);
		CREATE POLICY migration_047_receipt_tenant
		    ON task_observed_events AS RESTRICTIVE
		    FOR ALL TO vane_push_effect_receipt
		    USING (tenant_id IS NOT DISTINCT FROM
		        (SELECT current_setting('app.tenant_id',true))::bigint)
		    WITH CHECK (tenant_id IS NOT DISTINCT FROM
		        (SELECT current_setting('app.tenant_id',true))::bigint)`,
	); err != nil {
		t.Fatal(err)
	}
}

func assertMigration047State(
	t *testing.T,
	f pushEffectFixture,
	wantVersion, wantEffects int,
	wantAdmission bool,
) {
	t.Helper()
	var (
		version                        int
		effects                        int
		coordinator, receipt, operator bool
	)
	if err := f.db.QueryRowContext(t.Context(), `
		SELECT
		  (SELECT COALESCE(max(version_id),0)
		     FROM goose_db_version WHERE is_applied),
		  (SELECT count(*) FROM push_effects),
		  EXISTS (
		    SELECT 1 FROM pg_auth_members am
		    JOIN pg_roles r ON r.oid=am.roleid
		    JOIN pg_roles m ON m.oid=am.member
		    WHERE r.rolname='vane_push_effect_coordinator'
		      AND m.rolname=current_user
		  ),
		  EXISTS (
		    SELECT 1 FROM pg_auth_members am
		    JOIN pg_roles r ON r.oid=am.roleid
		    JOIN pg_roles m ON m.oid=am.member
		    WHERE r.rolname='vane_push_effect_receipt'
		      AND m.rolname=current_user
		  ),
		  EXISTS (
		    SELECT 1 FROM pg_auth_members am
		    JOIN pg_roles r ON r.oid=am.roleid
		    JOIN pg_roles m ON m.oid=am.member
		    WHERE r.rolname='vane_push_effect_operator'
		      AND m.rolname=current_user
		  )`,
	).Scan(
		&version,
		&effects,
		&coordinator,
		&receipt,
		&operator,
	); err != nil {
		t.Fatal(err)
	}
	if version != wantVersion || effects != wantEffects ||
		coordinator != wantAdmission ||
		receipt != wantAdmission ||
		operator != wantAdmission {
		t.Fatalf(
			"047 state version/effects=%d/%d roles=%v/%v/%v, want %d/%d/%v",
			version,
			effects,
			coordinator,
			receipt,
			operator,
			wantVersion,
			wantEffects,
			wantAdmission,
		)
	}
}
