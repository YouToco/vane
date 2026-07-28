package store

import (
	"context"
	"database/sql"
	"io/fs"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pressly/goose/v3"

	"github.com/YouToco/vane/types"
)

func migration062Provider(t *testing.T, db *sql.DB) *goose.Provider {
	t.Helper()
	dir, err := fs.Sub(migrationsFS, "migrations")
	if err != nil {
		t.Fatal(err)
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, db, dir)
	if err != nil {
		t.Fatal(err)
	}
	return provider
}

func TestMigration062DowngradeFailsClosedWithLedger(t *testing.T) {
	freshURL := freshMigrationDatabase(t, "vane_profile_claim_062")
	db, err := sql.Open("pgx", freshURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	provider := migration062Provider(t, db)
	if _, err := provider.UpTo(t.Context(), 60); err != nil {
		t.Fatalf("migrate to 060: %v", err)
	}
	var userID int64
	if err := db.QueryRowContext(t.Context(),
		`INSERT INTO users(feishu_open_id,name)
		 VALUES ('claim-062-user','claim') RETURNING id`).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(),
		`INSERT INTO memberships(tenant_id,user_id,role) VALUES (1,$1,'owner')`,
		userID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(),
		`INSERT INTO profiles(tenant_id,user_id,industry,tags,summary)
		 VALUES (1,$1,'AI',ARRAY['safe'],$2)`,
		userID, "安全事实。污染画像？！"); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.UpTo(t.Context(), 62); err != nil {
		t.Fatalf("migrate to 062: %v", err)
	}

	var sourceState string
	if err := db.QueryRowContext(t.Context(),
		`SELECT source_state FROM profile_claims
		  WHERE user_id=$1 AND field_name='industry'`, userID,
	).Scan(&sourceState); err != nil {
		t.Fatal(err)
	}
	if sourceState != "source_unavailable" {
		t.Fatalf("backfill invented provenance %q", sourceState)
	}
	var summaryClaims, maxSummaryRunes int
	if err := db.QueryRowContext(t.Context(),
		`SELECT count(*),COALESCE(max(char_length(claim_value)),0)
		   FROM profile_claims
		  WHERE user_id=$1 AND field_name='summary'`, userID,
	).Scan(&summaryClaims, &maxSummaryRunes); err != nil {
		t.Fatal(err)
	}
	if summaryClaims != 3 || maxSummaryRunes > 240 {
		t.Fatalf("summary backfill claims=%d max_runes=%d",
			summaryClaims, maxSummaryRunes)
	}
	if _, err := provider.UpTo(t.Context(), 66); err != nil {
		t.Fatalf("migrate current Store schema to 066: %v", err)
	}
	st, err := New(t.Context(), freshURL)
	if err != nil {
		t.Fatal(err)
	}
	registerStoreClose(t, st)
	compatTx, err := st.beginProfileClaimScopedTx(
		t.Context(), 1, false, userID)
	if err != nil {
		t.Fatal(err)
	}
	compatClaims, _, err := loadProfileClaimLedgerTx(
		t.Context(), compatTx, 1, userID, 0)
	_ = compatTx.Rollback(t.Context())
	if err != nil {
		t.Fatalf("067 binary must read 066 ledger without lineage columns: %v", err)
	}
	for _, claim := range compatClaims {
		if claim.CarriedFromEpoch != nil || claim.CarriedFromClaimID != nil {
			t.Fatalf("066 ledger invented carry lineage: %+v", claim)
		}
	}
	list, err := st.ListProfileClaims(t.Context(), 1, userID)
	if err != nil {
		t.Fatal(err)
	}
	polluted := findClaim(t, list.Claims, "summary", "污染画像？", true)
	bang := findClaim(t, list.Claims, "summary", "！", true)
	suppressed, err := st.ApplyProfileClaimAction(
		t.Context(), 1, userID,
		types.ProfileClaimAction{
			ExpectedVersion: 0, Action: "suppress",
			ClaimID: parseTestID(t, polluted.ID),
		},
		"migration-suppress", strings.Repeat("a", 64),
	)
	if err != nil {
		t.Fatal(err)
	}
	corrected, err := st.ApplyProfileClaimAction(
		t.Context(), 1, userID,
		types.ProfileClaimAction{
			ExpectedVersion: suppressed.Version, Action: "correct",
			ClaimID: parseTestID(t, bang.ID), Value: "已修正！",
		},
		"migration-correct", strings.Repeat("b", 64),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.EvolveProfile(
		t.Context(), userID, "新增事实。污染画像？！", []string{"safe"},
		10, corrected.Profile.UpdatedAt, 0,
		0, corrected.Version,
	); err != nil {
		t.Fatal(err)
	}
	profile, err := st.GetProfileForTenant(t.Context(), 1, userID)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(profile.Summary, "污染画像") ||
		!strings.Contains(profile.Summary, "已修正！") {
		t.Fatalf("migration claim split mismatch revived pollution: %q", profile.Summary)
	}
	if _, err := provider.DownTo(t.Context(), 62); err != nil {
		t.Fatalf("return to migration 062 for downgrade refusal: %v", err)
	}
	if _, err := provider.Down(t.Context()); err == nil ||
		!strings.Contains(err.Error(), "refusing to drop non-empty profile claim authority ledger") {
		t.Fatalf("062 Down accepted non-empty ledger: %v", err)
	}
	var version int64
	if err := db.QueryRowContext(t.Context(),
		`SELECT max(version_id) FROM goose_db_version WHERE is_applied`,
	).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 62 {
		t.Fatalf("failed Down changed version=%d", version)
	}
}

func TestMigration062BackfillFencesConcurrentLegacyProfileWrites(t *testing.T) {
	dbURL := freshMigrationDatabase(t, "vane_profile_claim_up_fence")
	db, err := sql.Open("pgx", dbURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(8)
	provider := migration062Provider(t, db)
	if _, err := provider.UpTo(t.Context(), 60); err != nil {
		t.Fatal(err)
	}
	var updateUser, createUser int64
	if err := db.QueryRowContext(t.Context(), `
		INSERT INTO users(feishu_open_id,name)
		VALUES ('claim-up-update','update') RETURNING id`,
	).Scan(&updateUser); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(t.Context(), `
		INSERT INTO users(feishu_open_id,name)
		VALUES ('claim-up-create','create') RETURNING id`,
	).Scan(&createUser); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `
		INSERT INTO memberships(tenant_id,user_id,role)
		VALUES (1,$1,'owner'),(1,$2,'member')`,
		updateUser, createUser); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `
		INSERT INTO profiles(tenant_id,user_id,industry)
		VALUES (1,$1,'before')`, updateUser); err != nil {
		t.Fatal(err)
	}
	beginLegacy := func(userID int64) *sql.Tx {
		t.Helper()
		tx, err := db.BeginTx(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(t.Context(), `
			SELECT set_config('app.tenant_id','1',true),
			       set_config('app.user_id',$1,true)`,
			strconv.FormatInt(userID, 10)); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(t.Context(),
			`SET LOCAL ROLE vane_profile_editor`); err != nil {
			t.Fatal(err)
		}
		return tx
	}
	updateTx := beginLegacy(updateUser)
	if _, err := updateTx.ExecContext(t.Context(), `
		UPDATE profiles SET industry='late-update',updated_at=clock_timestamp()
		 WHERE tenant_id=1 AND user_id=$1`, updateUser); err != nil {
		t.Fatal(err)
	}
	createTx := beginLegacy(createUser)
	if _, err := createTx.ExecContext(t.Context(), `
		INSERT INTO profiles(tenant_id,user_id,industry,updated_at)
		VALUES (1,$1,'late-create',clock_timestamp())`, createUser); err != nil {
		t.Fatal(err)
	}

	migrated := make(chan error, 1)
	go func() {
		_, err := provider.UpTo(context.Background(), 62)
		migrated <- err
	}()
	select {
	case err := <-migrated:
		t.Fatalf("062 did not wait for in-flight legacy writers: %v", err)
	case <-time.After(150 * time.Millisecond):
	}
	if err := updateTx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := createTx.Commit(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-migrated:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("062 migration remained blocked after legacy commits")
	}
	for userID, want := range map[int64]string{
		updateUser: "late-update",
		createUser: "late-create",
	} {
		var profileValue, claimValue string
		if err := db.QueryRowContext(t.Context(),
			`SELECT industry FROM profiles WHERE user_id=$1`, userID,
		).Scan(&profileValue); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRowContext(t.Context(), `
			SELECT claim_value FROM profile_claims
			 WHERE user_id=$1 AND field_name='industry'`, userID,
		).Scan(&claimValue); err != nil {
			t.Fatal(err)
		}
		if profileValue != want || claimValue != want {
			t.Fatalf("profile/ledger fork user=%d profile=%q claim=%q want=%q",
				userID, profileValue, claimValue, want)
		}
	}
}

func TestMigration062QueuedLegacyWriterIsRejectedAfterCommit(t *testing.T) {
	dbURL := freshMigrationDatabase(t, "vane_profile_claim_queued_writer")
	db, err := sql.Open("pgx", dbURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(8)
	provider := migration062Provider(t, db)
	if _, err := provider.UpTo(t.Context(), 60); err != nil {
		t.Fatal(err)
	}
	var userID int64
	if err := db.QueryRowContext(t.Context(), `
		INSERT INTO users(feishu_open_id,name)
		VALUES ('claim-queued-writer','queued') RETURNING id`,
	).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `
		INSERT INTO memberships(tenant_id,user_id,role)
		VALUES(1,$1,'owner')`, userID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `
		INSERT INTO profiles(tenant_id,user_id,industry)
		VALUES(1,$1,'before')`, userID); err != nil {
		t.Fatal(err)
	}
	// Hold a table touched late in 062 so the migration remains open after it
	// has acquired the profiles fence, giving the queued legacy writer a
	// deterministic place behind that fence.
	blocker, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := blocker.ExecContext(t.Context(),
		`LOCK TABLE memberships IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	migrated := make(chan error, 1)
	go func() {
		_, migrateErr := provider.UpTo(context.Background(), 62)
		migrated <- migrateErr
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var fenced bool
		if err := db.QueryRowContext(t.Context(), `
			SELECT EXISTS(
			  SELECT 1
			    FROM pg_locks
			   WHERE database=(SELECT oid FROM pg_database
			                    WHERE datname=current_database())
			     AND relation='profiles'::regclass
			     AND mode='ShareRowExclusiveLock'
			     AND granted
			)`).Scan(&fenced); err != nil {
			t.Fatal(err)
		}
		if fenced {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("062 never acquired profiles migration fence")
		}
		time.Sleep(10 * time.Millisecond)
	}
	writer := make(chan error, 1)
	go func() {
		_, writeErr := db.ExecContext(context.Background(), `
			UPDATE profiles SET industry='queued-legacy'
			 WHERE tenant_id=1 AND user_id=$1`, userID)
		writer <- writeErr
	}()
	select {
	case err := <-writer:
		t.Fatalf("legacy writer did not queue behind migration fence: %v", err)
	case <-time.After(150 * time.Millisecond):
	}
	if err := blocker.Commit(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-migrated:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("062 remained blocked after late-table blocker committed")
	}
	select {
	case err := <-writer:
		if err == nil ||
			!strings.Contains(err.Error(), "require vane_profile_claim_editor") {
			t.Fatalf("queued legacy writer crossed committed trigger: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("queued legacy writer remained blocked after migration commit")
	}
	var industry, claim string
	if err := db.QueryRowContext(t.Context(), `
		SELECT p.industry,c.claim_value
		  FROM profiles p
		  JOIN profile_claims c
		    ON c.tenant_id=p.tenant_id AND c.user_id=p.user_id
		   AND c.field_name='industry'
		 WHERE p.user_id=$1`, userID).Scan(&industry, &claim); err != nil {
		t.Fatal(err)
	}
	if industry != "before" || claim != "before" {
		t.Fatalf("queued writer forked projection/ledger=%q/%q", industry, claim)
	}
}

func TestMigration062DownWaitsForUncommittedProducer(t *testing.T) {
	dbURL := freshMigrationDatabase(t, "vane_profile_claim_down_fence")
	db, err := sql.Open("pgx", dbURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(6)
	provider := migration062Provider(t, db)
	if _, err := provider.UpTo(t.Context(), 62); err != nil {
		t.Fatal(err)
	}
	var userID int64
	if err := db.QueryRowContext(t.Context(), `
		INSERT INTO users(feishu_open_id,name)
		VALUES ('claim-down-producer','producer') RETURNING id`,
	).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `
		INSERT INTO memberships(tenant_id,user_id,role)
		VALUES(1,$1,'owner')`, userID); err != nil {
		t.Fatal(err)
	}
	producer, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := producer.ExecContext(t.Context(), `
		SELECT set_config('app.tenant_id','1',true),
		       set_config('app.user_id',$1,true)`,
		strconv.FormatInt(userID, 10)); err != nil {
		t.Fatal(err)
	}
	if _, err := producer.ExecContext(
		t.Context(), `SET LOCAL ROLE vane_profile_claim_editor`); err != nil {
		t.Fatal(err)
	}
	// Real producer order is profile first, ledger second.  Down must acquire
	// the same first table so it waits without forming a lock cycle.
	if _, err := producer.ExecContext(t.Context(), `
		INSERT INTO profiles(tenant_id,user_id) VALUES(1,$1)`, userID); err != nil {
		t.Fatal(err)
	}
	if _, err := producer.ExecContext(t.Context(), `
		INSERT INTO profile_claim_states(tenant_id,user_id) VALUES(1,$1)`,
		userID); err != nil {
		t.Fatal(err)
	}
	down := make(chan error, 1)
	go func() {
		_, err := provider.Down(context.Background())
		down <- err
	}()
	select {
	case err := <-down:
		t.Fatalf("062 Down did not wait for producer: %v", err)
	case <-time.After(150 * time.Millisecond):
	}
	if err := producer.Commit(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-down:
		if err == nil || !strings.Contains(
			err.Error(), "refusing to drop non-empty profile claim authority ledger") {
			t.Fatalf("Down deleted committed producer fact: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("062 Down remained blocked after producer commit")
	}
	var states, version int64
	if err := db.QueryRowContext(t.Context(),
		`SELECT count(*) FROM profile_claim_states`).Scan(&states); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(t.Context(),
		`SELECT max(version_id) FROM goose_db_version WHERE is_applied`,
	).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if states != 1 || version != 62 {
		t.Fatalf("Down lost producer state=%d version=%d", states, version)
	}
}

func TestMigration062ProfileProjectionTriggerFence(t *testing.T) {
	dbURL := freshMigrationDatabase(t, "vane_profile_claim_trigger")
	db, err := sql.Open("pgx", dbURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	provider := migration062Provider(t, db)
	if _, err := provider.UpTo(t.Context(), 62); err != nil {
		t.Fatal(err)
	}
	var userID int64
	if err := db.QueryRowContext(t.Context(), `
		INSERT INTO users(feishu_open_id,name)
		VALUES ('claim-trigger-owner','owner') RETURNING id`,
	).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `
		INSERT INTO memberships(tenant_id,user_id,role)
		VALUES(1,$1,'owner')`, userID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `
		INSERT INTO profiles(tenant_id,user_id,industry)
		VALUES(1,$1,'legacy-insert')`, userID); err == nil ||
		!strings.Contains(err.Error(), "require vane_profile_claim_editor") {
		t.Fatalf("owner legacy INSERT crossed trigger fence: %v", err)
	}
	claimTx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := claimTx.ExecContext(t.Context(), `
		SELECT set_config('app.tenant_id','1',true),
		       set_config('app.user_id',$1,true)`,
		strconv.FormatInt(userID, 10)); err != nil {
		t.Fatal(err)
	}
	if _, err := claimTx.ExecContext(
		t.Context(), `SET LOCAL ROLE vane_profile_claim_editor`); err != nil {
		t.Fatal(err)
	}
	if _, err := claimTx.ExecContext(t.Context(), `
		INSERT INTO profiles(tenant_id,user_id,industry)
		VALUES(1,$1,'claim-path')`, userID); err != nil {
		t.Fatalf("claim role INSERT rejected: %v", err)
	}
	if _, err := claimTx.ExecContext(t.Context(), `
		INSERT INTO profile_claim_states(tenant_id,user_id)
		VALUES(1,$1)`, userID); err != nil {
		t.Fatal(err)
	}
	if _, err := claimTx.ExecContext(t.Context(), `
		UPDATE profiles SET summary='claim-update'
		 WHERE tenant_id=1 AND user_id=$1`, userID); err != nil {
		t.Fatalf("claim role protected UPDATE rejected: %v", err)
	}
	if err := claimTx.Commit(); err != nil {
		t.Fatal(err)
	}
	for name, query := range map[string]string{
		"old upsert update": `
			UPDATE profiles SET industry='legacy-update'
			 WHERE tenant_id=1 AND user_id=$1`,
		"old evolve update": `
			UPDATE profiles SET summary='legacy-evolve'
			 WHERE tenant_id=1 AND user_id=$1`,
	} {
		if _, err := db.ExecContext(t.Context(), query, userID); err == nil ||
			!strings.Contains(err.Error(), "require vane_profile_claim_editor") {
			t.Fatalf("%s crossed trigger fence: %v", name, err)
		}
	}
	if _, err := db.ExecContext(t.Context(), `
		UPDATE profiles SET last_evolved_feedback_id=7
		 WHERE tenant_id=1 AND user_id=$1`, userID); err != nil {
		t.Fatalf("owner cursor-only update was fenced: %v", err)
	}
	var cursor int64
	if err := db.QueryRowContext(t.Context(), `
		SELECT last_evolved_feedback_id FROM profiles
		 WHERE tenant_id=1 AND user_id=$1`, userID).Scan(&cursor); err != nil {
		t.Fatal(err)
	}
	if cursor != 7 {
		t.Fatalf("cursor-only update=%d want 7", cursor)
	}
}

func TestMigration062From061AndEmptyDownRemovesTrigger(t *testing.T) {
	dbURL := freshMigrationDatabase(t, "vane_profile_claim_061_062_down")
	db, err := sql.Open("pgx", dbURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	provider := migration062Provider(t, db)
	if _, err := provider.UpTo(t.Context(), 61); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.UpTo(t.Context(), 62); err != nil {
		t.Fatalf("061 -> 062: %v", err)
	}
	if _, err := provider.Down(t.Context()); err != nil {
		t.Fatalf("empty 062 Down: %v", err)
	}
	var version int64
	var triggerExists, functionExists bool
	if err := db.QueryRowContext(t.Context(), `
		SELECT
		  (SELECT max(version_id) FROM goose_db_version WHERE is_applied),
		  EXISTS(
		    SELECT 1 FROM pg_trigger
		     WHERE tgrelid='profiles'::regclass
		       AND tgname='enforce_profile_claim_editor_v1'
		       AND NOT tgisinternal
		  ),
		  to_regprocedure(
		    'public.enforce_profile_claim_editor_v1()'
		  ) IS NOT NULL`,
	).Scan(&version, &triggerExists, &functionExists); err != nil {
		t.Fatal(err)
	}
	if version != 61 || triggerExists || functionExists {
		t.Fatalf("empty Down version/trigger/function=%d/%t/%t",
			version, triggerExists, functionExists)
	}
	var userID int64
	if err := db.QueryRowContext(t.Context(), `
		INSERT INTO users(feishu_open_id,name)
		VALUES('claim-down-trigger-removed','removed') RETURNING id`,
	).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `
		INSERT INTO memberships(tenant_id,user_id,role)
		VALUES(1,$1,'owner')`, userID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `
		INSERT INTO profiles(tenant_id,user_id,industry)
		VALUES(1,$1,'legacy-after-down')`, userID); err != nil {
		t.Fatalf("safe Down left trigger fence behind: %v", err)
	}
}

func TestMigration062LeastPrivilegeAndExactUserRLS(t *testing.T) {
	dbURL := freshMigrationDatabase(t, "vane_profile_claim_rls")
	if err := Migrate(t.Context(), dbURL); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("pgx", dbURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	var (
		claimsRead, claimsInsert, claimsUpdate, claimsDelete bool
		eventInsert, receiptInsert, stateUpdate              bool
		noLogin, noInherit, noBypass                         bool
		oldEditorSummaryUpdate                               bool
		ownerCanSet, appCanSet, oldEditorCanSet              bool
		appClaimsAccess, oldEditorClaimsAccess               bool
		oldEditorProfileInsert, oldEditorProfileUpdate       bool
	)
	if err := db.QueryRowContext(t.Context(), `
		SELECT
		  has_table_privilege('vane_profile_claim_editor','profile_claims','SELECT'),
		  has_table_privilege('vane_profile_claim_editor','profile_claims','INSERT'),
		  has_table_privilege('vane_profile_claim_editor','profile_claims','UPDATE'),
		  has_table_privilege('vane_profile_claim_editor','profile_claims','DELETE'),
		  has_table_privilege('vane_profile_claim_editor','profile_claim_events','INSERT'),
		  has_table_privilege('vane_profile_claim_editor','profile_claim_receipts','INSERT'),
		  has_table_privilege('vane_profile_claim_editor','profile_claim_states','UPDATE'),
		  NOT rolcanlogin,NOT rolinherit,NOT rolbypassrls,
		  has_column_privilege('vane_profile_editor','profiles','summary','UPDATE'),
		  pg_has_role(current_user,'vane_profile_claim_editor','SET'),
		  pg_has_role('vane_app','vane_profile_claim_editor','SET'),
		  pg_has_role('vane_profile_editor','vane_profile_claim_editor','SET'),
		  has_table_privilege('vane_app','profile_claims','SELECT,INSERT,UPDATE,DELETE'),
		  has_table_privilege('vane_profile_editor','profile_claims','SELECT,INSERT,UPDATE,DELETE'),
		  has_column_privilege('vane_profile_editor','profiles','industry','INSERT'),
		  has_column_privilege('vane_profile_editor','profiles','industry','UPDATE')
		FROM pg_roles WHERE rolname='vane_profile_claim_editor'`,
	).Scan(
		&claimsRead, &claimsInsert, &claimsUpdate, &claimsDelete,
		&eventInsert, &receiptInsert, &stateUpdate,
		&noLogin, &noInherit, &noBypass, &oldEditorSummaryUpdate,
		&ownerCanSet, &appCanSet, &oldEditorCanSet,
		&appClaimsAccess, &oldEditorClaimsAccess,
		&oldEditorProfileInsert, &oldEditorProfileUpdate,
	); err != nil {
		t.Fatal(err)
	}
	if !claimsRead || !claimsInsert || claimsUpdate || claimsDelete ||
		!eventInsert || !receiptInsert || !stateUpdate ||
		!noLogin || !noInherit || !noBypass || oldEditorSummaryUpdate ||
		!ownerCanSet || appCanSet || oldEditorCanSet ||
		appClaimsAccess || oldEditorClaimsAccess ||
		oldEditorProfileInsert || oldEditorProfileUpdate {
		t.Fatalf("unsafe 062 privileges read/insert/update/delete=%t/%t/%t/%t events=%t receipts=%t state=%t role=%t/%t/%t",
			claimsRead, claimsInsert, claimsUpdate, claimsDelete,
			eventInsert, receiptInsert, stateUpdate,
			noLogin, noInherit, noBypass)
	}

	var policyCount int
	if err := db.QueryRowContext(t.Context(), `
		SELECT count(*) FROM pg_policies
		 WHERE tablename IN (
		   'profile_claim_states','profile_claims',
		   'profile_claim_events','profile_claim_receipts'
		 )
		   AND qual LIKE '%app.tenant_id%'
		   AND qual LIKE '%app.user_id%'
		   AND qual LIKE '%NULLIF%'
		   AND with_check LIKE '%app.tenant_id%'
		   AND with_check LIKE '%app.user_id%'
		   AND with_check LIKE '%NULLIF%'`,
	).Scan(&policyCount); err != nil {
		t.Fatal(err)
	}
	if policyCount != 4 {
		t.Fatalf("exact-user RLS policies=%d want 4", policyCount)
	}

	var identityPolicyCount int
	if err := db.QueryRowContext(t.Context(), `
		SELECT count(*) FROM pg_policies
		 WHERE policyname='profile_claim_editor_identity'
		   AND tablename IN ('profiles','memberships')
		   AND qual LIKE '%app.tenant_id%'
		   AND qual LIKE '%app.user_id%'
		   AND qual LIKE '%NULLIF%'`,
	).Scan(&identityPolicyCount); err != nil {
		t.Fatal(err)
	}
	if identityPolicyCount != 2 {
		t.Fatalf("claim identity fail-closed policies=%d want 2", identityPolicyCount)
	}
	var profileTenantPolicySafe bool
	if err := db.QueryRowContext(t.Context(), `
		SELECT qual LIKE '%NULLIF%' AND with_check LIKE '%NULLIF%'
		  FROM pg_policies
		 WHERE tablename='profiles' AND policyname='tenant_isolation'`,
	).Scan(&profileTenantPolicySafe); err != nil {
		t.Fatal(err)
	}
	if !profileTenantPolicySafe {
		t.Fatal("profiles general tenant policy still has an empty-GUC bare cast")
	}

	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(t.Context(), `DISCARD ALL`); err != nil {
		t.Fatal(err)
	}
	assertNoClaimRows := func(tx *sql.Tx, state string) {
		t.Helper()
		var visible int
		if err := tx.QueryRowContext(t.Context(), `
			SELECT
			  (SELECT count(*) FROM profile_claim_states) +
			  (SELECT count(*) FROM profile_claims) +
			  (SELECT count(*) FROM profile_claim_events) +
			  (SELECT count(*) FROM profile_claim_receipts) +
			  (SELECT count(*) FROM profiles) +
			  (SELECT count(*) FROM memberships)`,
		).Scan(&visible); err != nil {
			t.Fatalf("%s GUC read raised instead of failing closed: %v", state, err)
		}
		if visible != 0 {
			t.Fatalf("%s GUC exposed %d scoped rows", state, visible)
		}
		if _, err := tx.ExecContext(t.Context(), `
			INSERT INTO profile_claim_states(tenant_id,user_id) VALUES(1,1)`,
		); err == nil {
			t.Fatalf("%s GUC allowed scoped insert", state)
		} else if strings.Contains(err.Error(), "invalid input syntax") {
			t.Fatalf("%s GUC raised bigint cast error: %v", state, err)
		}
	}

	tx, err := conn.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `SET LOCAL ROLE vane_profile_claim_editor`); err != nil {
		t.Fatal(err)
	}
	assertNoClaimRows(tx, "missing")
	_ = tx.Rollback()

	tx, err = conn.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `
		SELECT set_config('app.tenant_id','',true),
		       set_config('app.user_id','',true)`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `SET LOCAL ROLE vane_profile_claim_editor`); err != nil {
		t.Fatal(err)
	}
	assertNoClaimRows(tx, "empty")
	_ = tx.Rollback()
}
