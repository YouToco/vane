package store

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/YouToco/vane/types"
)

func pushBatchAuthorityFixture(t *testing.T) pushEffectFixture {
	t.Helper()
	f := newPushEffectFixture(t)
	if _, err := f.provider.UpTo(t.Context(), 48); err != nil {
		t.Fatalf("migrate to 048: %v", err)
	}
	return f
}

func TestClaimPushBatchDeliveryAuthorityFirstWriterWins(t *testing.T) {
	f := pushBatchAuthorityFixture(t)
	ctx := t.Context()
	scope := types.PushBatchScope{
		TenantID: f.prepared.TenantID,
		UserID:   f.prepared.UserID,
		BatchID:  f.prepared.BatchID,
	}

	winner, err := f.store.ClaimPushBatchDeliveryAuthority(
		ctx, scope, types.PushBatchDeliveryAuthorityLegacy,
	)
	if err != nil {
		t.Fatal(err)
	}
	if winner != types.PushBatchDeliveryAuthorityLegacy {
		t.Fatalf("first winner=%q", winner)
	}
	for _, desired := range []types.PushBatchDeliveryAuthority{
		types.PushBatchDeliveryAuthorityLegacy,
		types.PushBatchDeliveryAuthorityEffect,
	} {
		got, claimErr := f.store.ClaimPushBatchDeliveryAuthority(
			ctx, scope, desired,
		)
		if claimErr != nil {
			t.Fatal(claimErr)
		}
		if got != types.PushBatchDeliveryAuthorityLegacy {
			t.Fatalf("replay desired=%q winner=%q", desired, got)
		}
	}

	var stored string
	if err := f.db.QueryRowContext(ctx, `
		SELECT delivery_authority
		  FROM push_batches
		 WHERE id=$1`, scope.BatchID,
	).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if stored != string(types.PushBatchDeliveryAuthorityLegacy) {
		t.Fatalf("stored winner=%q", stored)
	}
}

func TestClaimPushBatchDeliveryAuthorityConcurrentWritersConverge(t *testing.T) {
	f := pushBatchAuthorityFixture(t)
	ctx := t.Context()
	scope := types.PushBatchScope{
		TenantID: f.prepared.TenantID,
		UserID:   f.prepared.UserID,
		BatchID:  f.prepared.BatchID,
	}

	const callers = 24
	start := make(chan struct{})
	results := make(chan types.PushBatchDeliveryAuthority, callers)
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for i := range callers {
		desired := types.PushBatchDeliveryAuthorityLegacy
		if i%2 == 1 {
			desired = types.PushBatchDeliveryAuthorityEffect
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			winner, err := f.store.ClaimPushBatchDeliveryAuthority(
				ctx, scope, desired,
			)
			if err != nil {
				errs <- err
				return
			}
			results <- winner
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Errorf("concurrent claim: %v", err)
	}

	var durableWinner types.PushBatchDeliveryAuthority
	if err := f.db.QueryRowContext(ctx, `
		SELECT delivery_authority
		  FROM push_batches
		 WHERE id=$1`, scope.BatchID,
	).Scan(&durableWinner); err != nil {
		t.Fatal(err)
	}
	if !durableWinner.Valid() {
		t.Fatalf("invalid durable winner=%q", durableWinner)
	}
	count := 0
	for winner := range results {
		count++
		if winner != durableWinner {
			t.Fatalf("caller observed %q, durable winner=%q",
				winner, durableWinner)
		}
	}
	if count != callers {
		t.Fatalf("successful callers=%d, want %d", count, callers)
	}
}

func TestClaimPushBatchDeliveryAuthorityExactScopeAndValidation(t *testing.T) {
	f := pushBatchAuthorityFixture(t)
	ctx := t.Context()
	valid := types.PushBatchScope{
		TenantID: f.prepared.TenantID,
		UserID:   f.prepared.UserID,
		BatchID:  f.prepared.BatchID,
	}
	tests := []struct {
		name    string
		scope   types.PushBatchScope
		desired types.PushBatchDeliveryAuthority
		code    types.ErrCode
	}{
		{
			name: "missing tenant",
			scope: types.PushBatchScope{
				UserID: valid.UserID, BatchID: valid.BatchID,
			},
			desired: types.PushBatchDeliveryAuthorityEffect,
			code:    types.CodeValidation,
		},
		{
			name:  "invalid authority",
			scope: valid, desired: "other",
			code: types.CodeValidation,
		},
		{
			name: "wrong tenant",
			scope: types.PushBatchScope{
				TenantID: valid.TenantID + 1,
				UserID:   valid.UserID,
				BatchID:  valid.BatchID,
			},
			desired: types.PushBatchDeliveryAuthorityEffect,
			code:    types.CodeNotFound,
		},
		{
			name: "wrong user",
			scope: types.PushBatchScope{
				TenantID: valid.TenantID,
				UserID:   valid.UserID + 1,
				BatchID:  valid.BatchID,
			},
			desired: types.PushBatchDeliveryAuthorityEffect,
			code:    types.CodeNotFound,
		},
		{
			name: "wrong batch",
			scope: types.PushBatchScope{
				TenantID: valid.TenantID,
				UserID:   valid.UserID,
				BatchID:  valid.BatchID + 1,
			},
			desired: types.PushBatchDeliveryAuthorityEffect,
			code:    types.CodeNotFound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := f.store.ClaimPushBatchDeliveryAuthority(
				ctx, tt.scope, tt.desired,
			)
			var appErr *types.AppError
			if !errors.As(err, &appErr) || appErr.Code != tt.code {
				t.Fatalf("err=%v, want code %s", err, tt.code)
			}
		})
	}

	var claimed bool
	if err := f.db.QueryRowContext(ctx, `
		SELECT delivery_authority IS NOT NULL
		  FROM push_batches
		 WHERE id=$1`, valid.BatchID,
	).Scan(&claimed); err != nil {
		t.Fatal(err)
	}
	if claimed {
		t.Fatal("invalid or mismatched scope claimed authority")
	}
}

func TestMigration048BackfillsEffectAuthority(t *testing.T) {
	f := newPushEffectFixture(t)
	ctx := t.Context()
	if _, err := f.store.CreatePushEffect(ctx, f.prepared); err != nil {
		t.Fatal(err)
	}
	if _, err := f.provider.UpTo(ctx, 48); err != nil {
		t.Fatalf("migrate to 048: %v", err)
	}

	var authority types.PushBatchDeliveryAuthority
	if err := f.db.QueryRowContext(ctx, `
		SELECT delivery_authority
		  FROM push_batches
		 WHERE id=$1`, f.prepared.BatchID,
	).Scan(&authority); err != nil {
		t.Fatal(err)
	}
	if authority != types.PushBatchDeliveryAuthorityEffect {
		t.Fatalf("backfilled authority=%q", authority)
	}
	winner, err := f.store.ClaimPushBatchDeliveryAuthority(
		ctx,
		types.PushBatchScope{
			TenantID: f.prepared.TenantID,
			UserID:   f.prepared.UserID,
			BatchID:  f.prepared.BatchID,
		},
		types.PushBatchDeliveryAuthorityLegacy,
	)
	if err != nil {
		t.Fatal(err)
	}
	if winner != types.PushBatchDeliveryAuthorityEffect {
		t.Fatalf("legacy stole backfilled authority: %q", winner)
	}
}

func TestMigration048AuthorityRoleAndDowngradeFence(t *testing.T) {
	f := pushBatchAuthorityFixture(t)
	ctx := t.Context()
	var (
		noLogin, noSuper, noCreateDB, noCreateRole bool
		noReplication, noBypassRLS, noInherit      bool
		ownerMember, appToRole, roleToApp          bool
		readAuthority, updateAuthority             bool
		updateStatus, insertBatch                  bool
		coordinatorRead, coordinatorUpdate         bool
		receiptEventRead, receiptEventUpdate       bool
		receiptEventKeyUpdate, receiptEventInsert  bool
		receiptCanonicalRead, receiptDigestRead    bool
		receiptCanonicalUpdate                     bool
		receiptPolicies                            int
	)
	if err := f.db.QueryRowContext(ctx, `
		SELECT
		  NOT r.rolcanlogin, NOT r.rolsuper, NOT r.rolcreatedb,
		  NOT r.rolcreaterole, NOT r.rolreplication,
		  NOT r.rolbypassrls, NOT r.rolinherit,
		  pg_has_role(current_user,r.oid,'MEMBER'),
		  pg_has_role('vane_app',r.oid,'MEMBER'),
		  pg_has_role(r.oid,'vane_app','MEMBER'),
		  has_column_privilege(
		    'vane_push_batch_authority','push_batches',
		    'delivery_authority','SELECT'),
		  has_column_privilege(
		    'vane_push_batch_authority','push_batches',
		    'delivery_authority','UPDATE'),
		  has_column_privilege(
		    'vane_push_batch_authority','push_batches','status','UPDATE'),
		  has_table_privilege(
		    'vane_push_batch_authority','push_batches','INSERT'),
		  has_column_privilege(
		    'vane_push_effect_coordinator','push_batches',
		    'delivery_authority','SELECT'),
		  has_column_privilege(
		    'vane_push_effect_coordinator','push_batches',
		    'delivery_authority','UPDATE'),
		  has_column_privilege(
		    'vane_push_effect_receipt','task_observed_events',
		    'event_key','SELECT'),
		  has_column_privilege(
		    'vane_push_effect_receipt','task_observed_events',
		    'delivered_at','UPDATE'),
		  has_column_privilege(
		    'vane_push_effect_receipt','task_observed_events',
		    'event_key','UPDATE'),
		  has_table_privilege(
		    'vane_push_effect_receipt','task_observed_events','INSERT'),
		  has_column_privilege(
		    'vane_push_effect_receipt','push_effects',
		    'canonical_payload','SELECT'),
		  has_column_privilege(
		    'vane_push_effect_receipt','push_effects',
		    'payload_digest','SELECT'),
		  has_column_privilege(
		    'vane_push_effect_receipt','push_effects',
		    'canonical_payload','UPDATE'),
		  (SELECT count(*) FROM pg_policies
		    WHERE schemaname='public'
		      AND tablename='task_observed_events'
		      AND policyname IN (
		        'push_effect_receipt_select_visible',
		        'push_effect_receipt_update_visible',
		        'push_effect_receipt_tenant_isolation'
		      )
		      AND 'vane_push_effect_receipt'=ANY(roles))
		FROM pg_roles r
		WHERE r.rolname='vane_push_batch_authority'`,
	).Scan(
		&noLogin, &noSuper, &noCreateDB, &noCreateRole,
		&noReplication, &noBypassRLS, &noInherit,
		&ownerMember, &appToRole, &roleToApp,
		&readAuthority, &updateAuthority, &updateStatus, &insertBatch,
		&coordinatorRead, &coordinatorUpdate,
		&receiptEventRead, &receiptEventUpdate,
		&receiptEventKeyUpdate, &receiptEventInsert,
		&receiptCanonicalRead, &receiptDigestRead,
		&receiptCanonicalUpdate, &receiptPolicies,
	); err != nil {
		t.Fatal(err)
	}
	if !noLogin || !noSuper || !noCreateDB || !noCreateRole ||
		!noReplication || !noBypassRLS || !noInherit ||
		!ownerMember || appToRole || roleToApp ||
		!readAuthority || !updateAuthority || updateStatus || insertBatch ||
		!coordinatorRead || coordinatorUpdate ||
		!receiptEventRead || !receiptEventUpdate ||
		receiptEventKeyUpdate || receiptEventInsert ||
		!receiptCanonicalRead || !receiptDigestRead ||
		receiptCanonicalUpdate || receiptPolicies != 3 {
		t.Fatalf("048 role matrix drift: attrs=%v/%v/%v/%v/%v/%v/%v memberships=%v/%v/%v authority=%v/%v/%v/%v coordinator=%v/%v receipt=%v/%v/%v/%v canonical=%v/%v/%v policies=%d",
			noLogin, noSuper, noCreateDB, noCreateRole, noReplication,
			noBypassRLS, noInherit, ownerMember, appToRole, roleToApp,
			readAuthority, updateAuthority, updateStatus, insertBatch,
			coordinatorRead, coordinatorUpdate,
			receiptEventRead, receiptEventUpdate,
			receiptEventKeyUpdate, receiptEventInsert,
			receiptCanonicalRead, receiptDigestRead,
			receiptCanonicalUpdate, receiptPolicies)
	}

	scope := types.PushBatchScope{
		TenantID: f.prepared.TenantID,
		UserID:   f.prepared.UserID,
		BatchID:  f.prepared.BatchID,
	}
	if _, err := f.store.ClaimPushBatchDeliveryAuthority(
		ctx, scope, types.PushBatchDeliveryAuthorityEffect,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := f.provider.Down(ctx); err == nil ||
		!strings.Contains(err.Error(), "refusing downgrade") {
		t.Fatalf("048 downgrade accepted claimed authority: %v", err)
	}
	var version int
	if err := f.db.QueryRowContext(ctx, `
		SELECT COALESCE(max(version_id),0)
		  FROM goose_db_version WHERE is_applied`,
	).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 48 {
		t.Fatalf("failed 048 down changed version to %d", version)
	}
}

func TestMigration048EmptyDowngrade(t *testing.T) {
	f := pushBatchAuthorityFixture(t)
	ctx := t.Context()
	if _, err := f.provider.Down(ctx); err != nil {
		t.Fatalf("empty 048 downgrade: %v", err)
	}
	var (
		version      int
		columnExists bool
	)
	if err := f.db.QueryRowContext(ctx, `
		SELECT
		  (SELECT COALESCE(max(version_id),0)
		     FROM goose_db_version WHERE is_applied),
		  EXISTS (
		    SELECT 1 FROM information_schema.columns
		     WHERE table_schema='public'
		       AND table_name='push_batches'
		       AND column_name='delivery_authority'
		  )`,
	).Scan(&version, &columnExists); err != nil {
		t.Fatal(err)
	}
	if version != 47 || columnExists {
		t.Fatalf("empty down version/column=%d/%v", version, columnExists)
	}
}

func TestMigration048ClaimFirstSerializesDowngrade(t *testing.T) {
	f := pushBatchAuthorityFixture(t)
	ctx := t.Context()
	scope := types.PushBatchScope{
		TenantID: f.prepared.TenantID,
		UserID:   f.prepared.UserID,
		BatchID:  f.prepared.BatchID,
	}

	rowBlocker, err := f.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	blockerDone := false
	defer func() {
		if !blockerDone {
			_ = rowBlocker.Rollback()
		}
	}()
	if _, err := rowBlocker.ExecContext(ctx, `
		SELECT id FROM push_batches WHERE id=$1 FOR UPDATE`,
		scope.BatchID,
	); err != nil {
		t.Fatal(err)
	}

	claimDone := make(chan struct {
		winner types.PushBatchDeliveryAuthority
		err    error
	}, 1)
	go func() {
		winner, claimErr := f.store.ClaimPushBatchDeliveryAuthority(
			ctx, scope, types.PushBatchDeliveryAuthorityEffect,
		)
		claimDone <- struct {
			winner types.PushBatchDeliveryAuthority
			err    error
		}{winner: winner, err: claimErr}
	}()
	if !waitForScratchQueryLock(
		ctx, f.db, "%UPDATE push_batches%", 5*time.Second,
	) {
		t.Fatal("claim did not hold schema admission while waiting for batch")
	}

	downDone := make(chan error, 1)
	go func() {
		_, downErr := f.provider.Down(ctx)
		downDone <- downErr
	}()
	if !waitForScratchQueryLock(
		ctx, f.db, "%pg_advisory_xact_lock(6215335020355474248)%",
		5*time.Second,
	) {
		t.Fatal("048 Down did not wait at schema admission")
	}
	if err := rowBlocker.Commit(); err != nil {
		t.Fatal(err)
	}
	blockerDone = true

	select {
	case result := <-claimDone:
		if result.err != nil {
			t.Fatalf("claim failed: %v", result.err)
		}
		if result.winner != types.PushBatchDeliveryAuthorityEffect {
			t.Fatalf("claim winner=%q", result.winner)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("claim did not converge")
	}
	select {
	case downErr := <-downDone:
		if downErr == nil ||
			!strings.Contains(downErr.Error(), "refusing downgrade") ||
			strings.Contains(downErr.Error(), "40P01") {
			t.Fatalf("claim-first Down result=%v", downErr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("claim-first Down did not converge")
	}
}

func TestMigration048DowngradeFirstSerializesClaim(t *testing.T) {
	f := pushBatchAuthorityFixture(t)
	ctx := t.Context()
	scope := types.PushBatchScope{
		TenantID: f.prepared.TenantID,
		UserID:   f.prepared.UserID,
		BatchID:  f.prepared.BatchID,
	}

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
		`LOCK TABLE push_batches IN ACCESS SHARE MODE`,
	); err != nil {
		t.Fatal(err)
	}

	downDone := make(chan error, 1)
	go func() {
		_, downErr := f.provider.Down(ctx)
		downDone <- downErr
	}()
	if !waitForScratchQueryLock(
		ctx, f.db, "%migration 048 downgrade fence%", 5*time.Second,
	) {
		t.Fatal("048 Down did not reach its table fence")
	}

	claimDone := make(chan error, 1)
	go func() {
		_, claimErr := f.store.ClaimPushBatchDeliveryAuthority(
			ctx, scope, types.PushBatchDeliveryAuthorityLegacy,
		)
		claimDone <- claimErr
	}()
	if !waitForScratchQueryLock(
		ctx, f.db, "%pg_advisory_xact_lock_shared%",
		5*time.Second,
	) {
		t.Fatal("claim did not wait behind 048 Down admission")
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
	case claimErr := <-claimDone:
		var appErr *types.AppError
		if !errors.As(claimErr, &appErr) ||
			appErr.Code != types.CodeDatabase ||
			strings.Contains(claimErr.Error(), "40P01") {
			t.Fatalf("down-first claim result=%v", claimErr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("down-first claim did not converge")
	}
}

func waitForScratchQueryLock(
	ctx context.Context,
	db *sql.DB,
	queryPattern string,
	timeout time.Duration,
) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var waiting bool
		err := db.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM pg_stat_activity
				 WHERE datname=current_database()
				   AND pid<>pg_backend_pid()
				   AND wait_event_type='Lock'
				   AND query LIKE $1
			)`, queryPattern,
		).Scan(&waiting)
		if err == nil && waiting {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

func TestMigration048BackfillRejectsUnknownAuthority(t *testing.T) {
	f := pushBatchAuthorityFixture(t)
	_, err := f.db.ExecContext(t.Context(), `
		UPDATE push_batches SET delivery_authority='unknown'
		 WHERE id=$1`, f.prepared.BatchID)
	if err == nil ||
		!strings.Contains(err.Error(), "push_batches_delivery_authority_valid") {
		t.Fatalf("invalid authority accepted: %v", err)
	}
}
