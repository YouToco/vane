package store

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/types"
)

func pushBatchAuthorityFixture(t *testing.T) pushEffectFixture {
	t.Helper()
	f := newPushEffectFixtureBeforeAuthority(t)
	if _, err := f.provider.UpTo(t.Context(), 47); err != nil {
		t.Fatalf("migrate to 047: %v", err)
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

func TestMigration047BackfillsExactDurableEvidence(t *testing.T) {
	f := newPushEffectFixtureBeforeAuthority(t)
	ctx := t.Context()
	insertRawPushEffectV39(t, f)

	// A sent receipt on the effect batch proves effect evidence has precedence.
	if _, err := f.db.ExecContext(ctx, `
		UPDATE deliveries
		   SET status='sent',
		       feishu_message_id=$2,
		       sent_at=clock_timestamp()
		 WHERE id=$1`,
		f.prepared.DeliveryIDs[0], "om-effect-"+uuid.NewString(),
	); err != nil {
		t.Fatal(err)
	}

	var sentOnlyBatchID, noEvidenceBatchID int64
	if err := f.db.QueryRowContext(ctx, `
		INSERT INTO push_batches (
			tenant_id,user_id,status,idempotency_key,run_snapshot_id
		) VALUES ($1,$2,'pending',$3,$4) RETURNING id`,
		f.prepared.TenantID, f.prepared.UserID,
		"sent-only-"+uuid.NewString(), f.prepared.RunSnapshotID,
	).Scan(&sentOnlyBatchID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.ExecContext(ctx, `
		INSERT INTO deliveries (
			tenant_id,batch_id,user_id,score,card_json,status,
			feishu_message_id,sent_at
		) VALUES ($1,$2,$3,80,'{}'::jsonb,'sent',$4,clock_timestamp())`,
		f.prepared.TenantID, sentOnlyBatchID, f.prepared.UserID,
		"om-legacy-"+uuid.NewString(),
	); err != nil {
		t.Fatal(err)
	}
	if err := f.db.QueryRowContext(ctx, `
		INSERT INTO push_batches (
			tenant_id,user_id,status,idempotency_key,run_snapshot_id
		) VALUES ($1,$2,'pending',$3,$4) RETURNING id`,
		f.prepared.TenantID, f.prepared.UserID,
		"no-evidence-"+uuid.NewString(), f.prepared.RunSnapshotID,
	).Scan(&noEvidenceBatchID); err != nil {
		t.Fatal(err)
	}

	if _, err := f.provider.UpTo(ctx, 47); err != nil {
		t.Fatalf("migrate to 047: %v", err)
	}

	tests := []struct {
		name    string
		batchID int64
		want    *types.PushBatchDeliveryAuthority
	}{
		{
			name:    "effect evidence wins over sent receipt",
			batchID: f.prepared.BatchID,
			want: authorityPointer(
				types.PushBatchDeliveryAuthorityEffect,
			),
		},
		{
			name:    "sent receipt only freezes legacy",
			batchID: sentOnlyBatchID,
			want: authorityPointer(
				types.PushBatchDeliveryAuthorityLegacy,
			),
		},
		{
			name:    "no durable evidence stays unclaimed",
			batchID: noEvidenceBatchID,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var authority sql.NullString
			if err := f.db.QueryRowContext(ctx, `
				SELECT delivery_authority
				  FROM push_batches
				 WHERE id=$1`, tt.batchID,
			).Scan(&authority); err != nil {
				t.Fatal(err)
			}
			if tt.want == nil {
				if authority.Valid {
					t.Fatalf("authority=%q, want NULL", authority.String)
				}
				return
			}
			if !authority.Valid ||
				types.PushBatchDeliveryAuthority(authority.String) != *tt.want {
				t.Fatalf("authority=%q valid=%v, want %q",
					authority.String, authority.Valid, *tt.want)
			}
		})
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

func authorityPointer(
	authority types.PushBatchDeliveryAuthority,
) *types.PushBatchDeliveryAuthority {
	return &authority
}

func TestMigration047AuthorityRoleAndDowngradeFence(t *testing.T) {
	f := pushBatchAuthorityFixture(t)
	ctx := t.Context()
	var (
		noLogin, noSuper, noCreateDB, noCreateRole bool
		noReplication, noBypassRLS, noInherit      bool
		ownerMember, appToRole, roleToApp          bool
		readAuthority, updateAuthority             bool
		updateStatus, insertBatch                  bool
		receiptCanonicalRead, receiptDigestRead    bool
		batchLockPublic, appBatchLock              bool
		coordinatorBatchLock, receiptBatchLock     bool
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
		    'vane_push_effect_receipt','push_effects',
		    'canonical_payload','SELECT'),
		  has_column_privilege(
		    'vane_push_effect_receipt','push_effects',
		    'payload_digest','SELECT'),
		  has_function_privilege(
		    'public','lock_push_effect_batch_v1(bigint,bigint,bigint,bigint,text)',
		    'EXECUTE'),
		  has_function_privilege(
		    'vane_app','lock_push_effect_batch_v1(bigint,bigint,bigint,bigint,text)',
		    'EXECUTE'),
		  has_function_privilege(
		    'vane_push_effect_coordinator',
		    'lock_push_effect_batch_v1(bigint,bigint,bigint,bigint,text)',
		    'EXECUTE'),
		  has_function_privilege(
		    'vane_push_effect_receipt',
		    'lock_push_effect_batch_v1(bigint,bigint,bigint,bigint,text)',
		    'EXECUTE')
		FROM pg_roles r
		WHERE r.rolname='vane_push_batch_authority'`,
	).Scan(
		&noLogin, &noSuper, &noCreateDB, &noCreateRole,
		&noReplication, &noBypassRLS, &noInherit,
		&ownerMember, &appToRole, &roleToApp,
		&readAuthority, &updateAuthority, &updateStatus, &insertBatch,
		&receiptCanonicalRead, &receiptDigestRead,
		&batchLockPublic, &appBatchLock,
		&coordinatorBatchLock, &receiptBatchLock,
	); err != nil {
		t.Fatal(err)
	}
	if !noLogin || !noSuper || !noCreateDB || !noCreateRole ||
		!noReplication || !noBypassRLS || !noInherit ||
		!ownerMember || appToRole || roleToApp ||
		!readAuthority || !updateAuthority || updateStatus || insertBatch ||
		receiptCanonicalRead || !receiptDigestRead ||
		batchLockPublic || !appBatchLock ||
		!coordinatorBatchLock || !receiptBatchLock {
		t.Fatalf("047 role matrix drift: attrs=%v/%v/%v/%v/%v/%v/%v memberships=%v/%v/%v authority=%v/%v/%v/%v receipt=%v/%v locks=%v/%v/%v/%v",
			noLogin, noSuper, noCreateDB, noCreateRole, noReplication,
			noBypassRLS, noInherit, ownerMember, appToRole, roleToApp,
			readAuthority, updateAuthority, updateStatus, insertBatch,
			receiptCanonicalRead, receiptDigestRead,
			batchLockPublic, appBatchLock,
			coordinatorBatchLock, receiptBatchLock)
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
		t.Fatalf("047 downgrade accepted claimed authority: %v", err)
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

func TestMigration047EmptyDowngrade(t *testing.T) {
	f := pushBatchAuthorityFixture(t)
	ctx := t.Context()
	if _, err := f.provider.Down(ctx); err != nil {
		t.Fatalf("empty 047 downgrade: %v", err)
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
	if version != 46 || columnExists {
		t.Fatalf("empty down version/column=%d/%v", version, columnExists)
	}
}

func TestMigration047DownPreservesMigration039ReceiptGrant(t *testing.T) {
	f := pushBatchAuthorityFixture(t)
	ctx := t.Context()
	if _, err := f.provider.Down(ctx); err != nil {
		t.Fatal(err)
	}
	var digestRead, batchRead bool
	if err := f.db.QueryRowContext(ctx, `
		SELECT
		  has_column_privilege(
		    'vane_push_effect_receipt','push_effects',
		    'payload_digest','SELECT'),
		  has_column_privilege(
		    'vane_push_effect_receipt','push_effects',
		    'batch_id','SELECT')`,
	).Scan(&digestRead, &batchRead); err != nil {
		t.Fatal(err)
	}
	if !digestRead || batchRead {
		t.Fatalf("047 Down changed 039/new grants: digest=%v batch=%v",
			digestRead, batchRead)
	}
}

func TestMigration047ClaimFirstSerializesDowngrade(t *testing.T) {
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
		t.Fatal("047 Down did not wait at schema admission")
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

func TestMigration047DowngradeFirstSerializesClaim(t *testing.T) {
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
		ctx, f.db, "%migration 047 downgrade fence%", 5*time.Second,
	) {
		t.Fatal("047 Down did not reach its table fence")
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
		t.Fatal("claim did not wait behind 047 Down admission")
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

func TestMigration047BackfillRejectsUnknownAuthority(t *testing.T) {
	f := pushBatchAuthorityFixture(t)
	_, err := f.db.ExecContext(t.Context(), `
		UPDATE push_batches SET delivery_authority='unknown'
		 WHERE id=$1`, f.prepared.BatchID)
	if err == nil ||
		!strings.Contains(err.Error(), "push_batches_delivery_authority_valid") {
		t.Fatalf("invalid authority accepted: %v", err)
	}
}
