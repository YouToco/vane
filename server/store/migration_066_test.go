package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pressly/goose/v3"

	"github.com/YouToco/vane/server/types"
)

type migration066Fixture struct {
	userID         int64
	claimID        int64
	eventID        int64
	deliveryID     int64
	receiptPayload []byte
}

func migration066Provider(t *testing.T, db *sql.DB) *goose.Provider {
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

func openMigration066Database(
	t *testing.T, prefix string,
) (string, *sql.DB, *goose.Provider) {
	t.Helper()
	dbURL := freshMigrationDatabase(t, prefix)
	db, err := sql.Open("pgx", dbURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	assertMigration066Postgres18(t, db)
	return dbURL, db, migration066Provider(t, db)
}

func assertMigration066Postgres18(t *testing.T, db *sql.DB) {
	t.Helper()
	var version int
	if err := db.QueryRowContext(t.Context(),
		`SELECT current_setting('server_version_num')::int`,
	).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version < 180000 || version >= 190000 {
		t.Fatalf("migration 066 Gate requires PostgreSQL 18, got server_version_num=%d", version)
	}
}

func requireMigration066SQLState(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected SQLSTATE %s, got nil", want)
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		t.Fatalf("expected SQLSTATE %s, got non-PostgreSQL error: %v", want, err)
	}
	if pgErr.Code != want {
		t.Fatalf("SQLSTATE=%s want %s: %v", pgErr.Code, want, err)
	}
}

func seedMigration066LegacyProfile(
	t *testing.T, db *sql.DB, suffix string,
) migration066Fixture {
	t.Helper()
	var fixture migration066Fixture
	if err := db.QueryRowContext(t.Context(), `
		INSERT INTO users(feishu_open_id,name)
		VALUES($1,'migration-066') RETURNING id`,
		"migration-066-"+suffix,
	).Scan(&fixture.userID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `
		INSERT INTO memberships(tenant_id,user_id,role)
		VALUES(1,$1,'owner')`, fixture.userID); err != nil {
		t.Fatal(err)
	}

	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(t.Context(), `
		SELECT set_config('app.tenant_id','1',true),
		       set_config('app.user_id',$1,true)`,
		strconv.FormatInt(fixture.userID, 10)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(
		t.Context(), `SET LOCAL ROLE vane_profile_claim_editor`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `
		INSERT INTO profiles(tenant_id,user_id,industry,tags)
		VALUES(1,$1,'AI',ARRAY['safety'])`,
		fixture.userID); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `
		INSERT INTO profile_claim_states(tenant_id,user_id,version)
		VALUES(1,$1,0)`, fixture.userID); err != nil {
		t.Fatal(err)
	}
	if err := tx.QueryRowContext(t.Context(), `
		INSERT INTO profile_claims
		    (tenant_id,user_id,field_name,claim_value,source_state)
		VALUES(1,$1,'industry','AI','manual')
		RETURNING id`, fixture.userID,
	).Scan(&fixture.claimID); err != nil {
		t.Fatal(err)
	}
	if err := tx.QueryRowContext(t.Context(), `
		INSERT INTO profile_claim_events
		    (tenant_id,user_id,actor_user_id,event_kind,target_claim_id,
		     expected_version,result_version)
		VALUES(1,$1,$1,'pin',$2,0,1)
		RETURNING id`, fixture.userID, fixture.claimID,
	).Scan(&fixture.eventID); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `
		UPDATE profile_claim_states SET version=1
		WHERE tenant_id=1 AND user_id=$1`, fixture.userID); err != nil {
		t.Fatal(err)
	}
	fixture.receiptPayload, err = json.Marshal(types.ProfileClaimActionResult{
		Version: 1,
		EventID: strconv.FormatInt(fixture.eventID, 10),
		Profile: types.ProfileView{},
		Claims:  []types.ProfileClaim{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `
		INSERT INTO profile_claim_receipts
		    (tenant_id,user_id,idempotency_key,request_digest,event_id,response_payload)
		VALUES(1,$1,'legacy-receipt',$2,$3,$4)`,
		fixture.userID, strings.Repeat("a", 64), fixture.eventID,
		fixture.receiptPayload); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	var batchID int64
	if err := db.QueryRowContext(t.Context(), `
		INSERT INTO push_batches(tenant_id,user_id)
		VALUES(1,$1) RETURNING id`, fixture.userID,
	).Scan(&batchID); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(t.Context(), `
		INSERT INTO deliveries(tenant_id,batch_id,user_id)
		VALUES(1,$1,$2) RETURNING id`, batchID, fixture.userID,
	).Scan(&fixture.deliveryID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `
		INSERT INTO feedbacks(tenant_id,user_id,delivery_id,action)
		VALUES(1,$1,$2,'interested')`,
		fixture.userID, fixture.deliveryID); err != nil {
		t.Fatal(err)
	}
	return fixture
}

func seedMigration066Delivery(
	t *testing.T, db *sql.DB, suffix string,
) (userID, deliveryID int64) {
	t.Helper()
	if err := db.QueryRowContext(t.Context(), `
		INSERT INTO users(feishu_open_id,name)
		VALUES($1,'migration-066-feedback') RETURNING id`,
		"migration-066-feedback-"+suffix,
	).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `
		INSERT INTO memberships(tenant_id,user_id,role)
		VALUES(1,$1,'member')`, userID); err != nil {
		t.Fatal(err)
	}
	return userID, seedMigration066DeliveryForUser(
		t, db, userID, suffix)
}

func seedMigration066DeliveryForUser(
	t *testing.T, db *sql.DB, userID int64, suffix string,
) (deliveryID int64) {
	t.Helper()
	var sourceID, contentID int64
	var targetTable string
	if err := db.QueryRowContext(t.Context(), `
		SELECT CASE
		         WHEN to_regclass('public.fetch_targets') IS NOT NULL
		         THEN 'fetch_targets'
		         ELSE 'sources'
		       END`).Scan(&targetTable); err != nil {
		t.Fatal(err)
	}
	insertTarget := fmt.Sprintf(`
		INSERT INTO %s(platform,capability,url,title,config,status)
		VALUES('web','search',$1,$2,'{}','active') RETURNING id`,
		targetTable)
	if err := db.QueryRowContext(t.Context(), insertTarget,
		"https://migration-066.example/"+suffix,
		"migration-066-"+suffix,
	).Scan(&sourceID); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(t.Context(), `
		INSERT INTO content_items
		    (source_id,external_id,canonical_key,url,title,content,content_hash)
		VALUES($1,$2,$3,$4,$5,$5,$6) RETURNING id`,
		sourceID, "external-"+suffix,
		"migration-066://"+suffix,
		"https://migration-066.example/content/"+suffix,
		"migration-066-"+suffix,
		strings.Repeat("b", 64-len(suffix))+suffix,
	).Scan(&contentID); err != nil {
		t.Fatal(err)
	}
	var batchID int64
	if err := db.QueryRowContext(t.Context(), `
		INSERT INTO push_batches(tenant_id,user_id)
		VALUES(1,$1) RETURNING id`, userID,
	).Scan(&batchID); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(t.Context(), `
		INSERT INTO deliveries
		    (tenant_id,batch_id,user_id,content_item_id,body_md)
		VALUES(1,$1,$2,$3,$4) RETURNING id`,
		batchID, userID, contentID, "migration-066-"+suffix,
	).Scan(&deliveryID); err != nil {
		t.Fatal(err)
	}
	return deliveryID
}

func TestMigration066EpochZeroBackfillFirstIntakeAndFeedbackStamp(t *testing.T) {
	dbURL, db, provider := openMigration066Database(t, "vane_066_backfill")
	if _, err := provider.UpTo(t.Context(), 65); err != nil {
		t.Fatalf("migrate to 065: %v", err)
	}
	legacy := seedMigration066LegacyProfile(t, db, "backfill")
	if _, err := provider.UpTo(t.Context(), 66); err != nil {
		t.Fatalf("migrate to 066: %v", err)
	}

	var (
		activeEpoch                      int64
		epochRows, claims, events        int
		receipts, feedbacks, nonzeroRows int
	)
	if err := db.QueryRowContext(t.Context(), `
		SELECT s.active_epoch,
		       (SELECT count(*) FROM profile_epochs e
		         WHERE e.tenant_id=s.tenant_id AND e.user_id=s.user_id
		           AND e.profile_epoch=0),
		       (SELECT count(*) FROM profile_claims c
		         WHERE c.tenant_id=s.tenant_id AND c.user_id=s.user_id
		           AND c.profile_epoch=0),
		       (SELECT count(*) FROM profile_claim_events e
		         WHERE e.tenant_id=s.tenant_id AND e.user_id=s.user_id
		           AND e.profile_epoch=0),
		       (SELECT count(*) FROM profile_claim_receipts r
		         WHERE r.tenant_id=s.tenant_id AND r.user_id=s.user_id
		           AND r.profile_epoch=0),
		       (SELECT count(*) FROM feedbacks f
		         WHERE f.tenant_id=s.tenant_id AND f.user_id=s.user_id
		           AND f.profile_epoch=0),
		       (SELECT count(*) FROM profile_epochs WHERE profile_epoch<>0) +
		       (SELECT count(*) FROM profile_claims WHERE profile_epoch<>0) +
		       (SELECT count(*) FROM profile_claim_events WHERE profile_epoch<>0) +
		       (SELECT count(*) FROM profile_claim_receipts WHERE profile_epoch<>0) +
		       (SELECT count(*) FROM feedbacks WHERE profile_epoch<>0)
		  FROM profile_claim_states s
		 WHERE s.tenant_id=1 AND s.user_id=$1`, legacy.userID,
	).Scan(
		&activeEpoch, &epochRows, &claims, &events,
		&receipts, &feedbacks, &nonzeroRows,
	); err != nil {
		t.Fatal(err)
	}
	if activeEpoch != 0 || epochRows != 1 || claims != 1 || events != 1 ||
		receipts != 1 || feedbacks != 1 || nonzeroRows != 0 {
		t.Fatalf("epoch-0 backfill mismatch active/epoch/claims/events/receipts/feedbacks/nonzero=%d/%d/%d/%d/%d/%d/%d",
			activeEpoch, epochRows, claims, events, receipts, feedbacks, nonzeroRows)
	}
	st, err := New(t.Context(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	replayed, err := st.ApplyProfileClaimAction(
		t.Context(), 1, legacy.userID,
		types.ProfileClaimAction{
			ExpectedVersion: 999,
			Action:          "suppress",
			ClaimID:         legacy.claimID,
		},
		"legacy-receipt", strings.Repeat("a", 64),
	)
	if err != nil {
		t.Fatalf("replay pre-066 action receipt: %v", err)
	}
	replayedPayload, err := json.Marshal(replayed)
	if err != nil {
		t.Fatal(err)
	}
	if string(replayedPayload) != string(legacy.receiptPayload) {
		t.Fatalf("pre-066 action receipt changed on replay:\n got %s\nwant %s",
			replayedPayload, legacy.receiptPayload)
	}

	for name, query := range map[string]string{
		"old style": `
			INSERT INTO feedbacks(tenant_id,user_id,delivery_id,action)
			VALUES(1,$1,$2,'not_interested')
			RETURNING profile_epoch`,
		"spoofed epoch": `
			INSERT INTO feedbacks
			    (tenant_id,user_id,delivery_id,action,profile_epoch)
			VALUES(1,$1,$2,'question',999)
			RETURNING profile_epoch`,
	} {
		var stamped int64
		if err := db.QueryRowContext(
			t.Context(), query, legacy.userID, legacy.deliveryID,
		).Scan(&stamped); err != nil {
			t.Fatalf("%s feedback insert: %v", name, err)
		}
		if stamped != 0 {
			t.Fatalf("%s feedback stamped epoch=%d want 0", name, stamped)
		}
	}

	var firstUser int64
	if err := db.QueryRowContext(t.Context(), `
		INSERT INTO users(feishu_open_id,name)
		VALUES('migration-066-first-intake','first') RETURNING id`,
	).Scan(&firstUser); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `
		INSERT INTO memberships(tenant_id,user_id,role)
		VALUES(1,$1,'member')`, firstUser); err != nil {
		t.Fatal(err)
	}
	industry, occupation := "SaaS", "Founder"
	if _, err := st.UpsertProfileFields(
		t.Context(), firstUser, &industry, &occupation, []string{"market"},
	); err != nil {
		t.Fatalf("first intake after 066: %v", err)
	}
	var firstEpoch, firstEpochRows, firstClaimRows int64
	if err := db.QueryRowContext(t.Context(), `
		SELECT s.active_epoch,
		       (SELECT count(*) FROM profile_epochs e
		         WHERE e.tenant_id=s.tenant_id AND e.user_id=s.user_id
		           AND e.profile_epoch=0),
		       (SELECT count(*) FROM profile_claims c
		         WHERE c.tenant_id=s.tenant_id AND c.user_id=s.user_id
		           AND c.profile_epoch=0)
		  FROM profile_claim_states s
		 WHERE s.tenant_id=1 AND s.user_id=$1`, firstUser,
	).Scan(&firstEpoch, &firstEpochRows, &firstClaimRows); err != nil {
		t.Fatal(err)
	}
	if firstEpoch != 0 || firstEpochRows != 1 || firstClaimRows != 3 {
		t.Fatalf("first intake epoch/state/claims=%d/%d/%d want 0/1/3",
			firstEpoch, firstEpochRows, firstClaimRows)
	}
}

func TestMigration066RejectsUnboundOldWritersAndClaimEditorEpochAdvance(t *testing.T) {
	dbURL, db, provider := openMigration066Database(t, "vane_066_writer")
	if _, err := provider.UpTo(t.Context(), 66); err != nil {
		t.Fatal(err)
	}
	var userID int64
	if err := db.QueryRowContext(t.Context(), `
		INSERT INTO users(feishu_open_id,name)
		VALUES('migration-066-writer','writer') RETURNING id`,
	).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `
		INSERT INTO memberships(tenant_id,user_id,role)
		VALUES(1,$1,'owner')`, userID); err != nil {
		t.Fatal(err)
	}
	st, err := New(t.Context(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	industry := "AI"
	if _, err := st.UpsertProfileFields(
		t.Context(), userID, &industry, nil, []string{"safety"},
	); err != nil {
		t.Fatal(err)
	}

	beginClaimEditor := func(bindEpoch bool) *sql.Tx {
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
		if bindEpoch {
			if _, err := tx.ExecContext(t.Context(),
				`SELECT set_config('app.profile_epoch','0',true)`); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := tx.ExecContext(
			t.Context(), `SET LOCAL ROLE vane_profile_claim_editor`); err != nil {
			t.Fatal(err)
		}
		return tx
	}

	tx := beginClaimEditor(false)
	_, err = tx.ExecContext(t.Context(), `
		INSERT INTO profile_claims
		    (tenant_id,user_id,field_name,claim_value,source_state)
		VALUES(1,$1,'tag','old-writer','manual')`, userID)
	requireMigration066SQLState(t, err, "42501")
	_ = tx.Rollback()

	tx = beginClaimEditor(false)
	_, err = tx.ExecContext(t.Context(), `
		UPDATE profiles SET last_evolved_feedback_id=9
		WHERE tenant_id=1 AND user_id=$1`, userID)
	requireMigration066SQLState(t, err, "42501")
	_ = tx.Rollback()

	tx = beginClaimEditor(true)
	_, err = tx.ExecContext(t.Context(), `
		INSERT INTO profile_epochs(tenant_id,user_id,profile_epoch)
		VALUES(1,$1,1)`, userID)
	requireMigration066SQLState(t, err, "42501")
	_ = tx.Rollback()

	tx = beginClaimEditor(true)
	_, err = tx.ExecContext(t.Context(), `
		UPDATE profile_claim_states
		   SET active_epoch=1,version=version+1
		 WHERE tenant_id=1 AND user_id=$1`, userID)
	requireMigration066SQLState(t, err, "42501")
	_ = tx.Rollback()

	var activeEpoch, version, cursor int64
	if err := db.QueryRowContext(t.Context(), `
		SELECT s.active_epoch,s.version,p.last_evolved_feedback_id
		  FROM profile_claim_states s
		  JOIN profiles p USING(tenant_id,user_id)
		 WHERE s.tenant_id=1 AND s.user_id=$1`, userID,
	).Scan(&activeEpoch, &version, &cursor); err != nil {
		t.Fatal(err)
	}
	if activeEpoch != 0 || version != 0 || cursor != 0 {
		t.Fatalf("rejected writers changed epoch/version/cursor=%d/%d/%d",
			activeEpoch, version, cursor)
	}
	var nonzeroEpochs int
	if err := db.QueryRowContext(t.Context(), `
		SELECT count(*) FROM profile_epochs
		WHERE tenant_id=1 AND user_id=$1 AND profile_epoch<>0`, userID,
	).Scan(&nonzeroEpochs); err != nil {
		t.Fatal(err)
	}
	if nonzeroEpochs != 0 {
		t.Fatalf("claim editor minted %d nonzero epochs", nonzeroEpochs)
	}
}

func TestMigration066EpochRLSFailsClosedForMissingEmptyPartialAndCrossUser(t *testing.T) {
	dbURL, db, provider := openMigration066Database(t, "vane_066_rls")
	if _, err := provider.UpTo(t.Context(), 66); err != nil {
		t.Fatal(err)
	}
	st, err := New(t.Context(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	userIDs := make([]int64, 2)
	for i := range userIDs {
		if err := db.QueryRowContext(t.Context(), `
			INSERT INTO users(feishu_open_id,name)
			VALUES($1,'rls') RETURNING id`,
			"migration-066-rls-"+strconv.Itoa(i),
		).Scan(&userIDs[i]); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(t.Context(), `
			INSERT INTO memberships(tenant_id,user_id,role)
			VALUES(1,$1,'member')`, userIDs[i]); err != nil {
			t.Fatal(err)
		}
		industry := "AI"
		if _, err := st.UpsertProfileFields(
			t.Context(), userIDs[i], &industry, nil, []string{"safety"},
		); err != nil {
			t.Fatal(err)
		}
	}

	visibleRows := func(name string, settings string) int {
		t.Helper()
		conn, err := db.Conn(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		if _, err := conn.ExecContext(t.Context(), `RESET ALL`); err != nil {
			t.Fatal(err)
		}
		tx, err := conn.BeginTx(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback() }()
		if settings != "" {
			if _, err := tx.ExecContext(t.Context(), settings); err != nil {
				t.Fatalf("%s settings: %v", name, err)
			}
		}
		if _, err := tx.ExecContext(
			t.Context(), `SET LOCAL ROLE vane_profile_claim_editor`); err != nil {
			t.Fatal(err)
		}
		var visible int
		if err := tx.QueryRowContext(t.Context(), `
			SELECT
			  (SELECT count(*) FROM profile_epochs) +
			  (SELECT count(*) FROM profile_claim_states) +
			  (SELECT count(*) FROM profile_claims) +
			  (SELECT count(*) FROM profile_claim_events) +
			  (SELECT count(*) FROM profile_claim_receipts) +
			  (SELECT count(*) FROM profiles) +
			  (SELECT count(*) FROM memberships)`,
		).Scan(&visible); err != nil {
			t.Fatalf("%s RLS read raised instead of failing closed: %v", name, err)
		}
		return visible
	}

	cases := []struct {
		name     string
		settings string
	}{
		{name: "missing"},
		{name: "empty", settings: `
			SELECT set_config('app.tenant_id','',true),
			       set_config('app.user_id','',true)`},
		{name: "tenant only", settings: `
			SELECT set_config('app.tenant_id','1',true)`},
		{name: "user only", settings: `
			SELECT set_config('app.user_id','` +
			strconv.FormatInt(userIDs[0], 10) + `',true)`},
	}
	for _, tc := range cases {
		if visible := visibleRows(tc.name, tc.settings); visible != 0 {
			t.Fatalf("%s GUC exposed %d exact-user rows", tc.name, visible)
		}
	}

	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(t.Context(), `RESET ALL`); err != nil {
		t.Fatal(err)
	}
	tx, err := conn.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(t.Context(), `
		SELECT set_config('app.tenant_id','1',true),
		       set_config('app.user_id',$1,true)`,
		strconv.FormatInt(userIDs[0], 10)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(
		t.Context(), `SET LOCAL ROLE vane_profile_claim_editor`); err != nil {
		t.Fatal(err)
	}
	var ownRows, crossRows int
	if err := tx.QueryRowContext(t.Context(), `
		SELECT
		  (SELECT count(*) FROM profile_epochs WHERE user_id=$1),
		  (SELECT count(*) FROM profile_epochs WHERE user_id=$2)`,
		userIDs[0], userIDs[1],
	).Scan(&ownRows, &crossRows); err != nil {
		t.Fatal(err)
	}
	if ownRows != 1 || crossRows != 0 {
		t.Fatalf("exact-user epoch RLS own/cross=%d/%d want 1/0",
			ownRows, crossRows)
	}
}

func TestMigration066DownAllowsOnlyEpochZero(t *testing.T) {
	_, db, provider := openMigration066Database(t, "vane_066_down_zero")
	if _, err := provider.UpTo(t.Context(), 65); err != nil {
		t.Fatal(err)
	}
	legacy := seedMigration066LegacyProfile(t, db, "down-zero")
	if _, err := provider.UpTo(t.Context(), 66); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Down(t.Context()); err != nil {
		t.Fatalf("epoch-zero 066 Down: %v", err)
	}

	var version int64
	var epochTableExists, activeColumnExists, claimEpochColumnExists bool
	if err := db.QueryRowContext(t.Context(), `
		SELECT
		  (SELECT max(version_id) FROM goose_db_version WHERE is_applied),
		  to_regclass('public.profile_epochs') IS NOT NULL,
		  EXISTS(SELECT 1 FROM information_schema.columns
		          WHERE table_schema='public'
		            AND table_name='profile_claim_states'
		            AND column_name='active_epoch'),
		  EXISTS(SELECT 1 FROM information_schema.columns
		          WHERE table_schema='public'
		            AND table_name='profile_claims'
		            AND column_name='profile_epoch')`,
	).Scan(
		&version, &epochTableExists, &activeColumnExists,
		&claimEpochColumnExists,
	); err != nil {
		t.Fatal(err)
	}
	if version != 65 || epochTableExists || activeColumnExists ||
		claimEpochColumnExists {
		t.Fatalf("epoch-zero Down version/table/columns=%d/%t/%t/%t",
			version, epochTableExists, activeColumnExists, claimEpochColumnExists)
	}
	var claims, events, receipts, feedbacks int
	if err := db.QueryRowContext(t.Context(), `
		SELECT
		  (SELECT count(*) FROM profile_claims
		    WHERE tenant_id=1 AND user_id=$1),
		  (SELECT count(*) FROM profile_claim_events
		    WHERE tenant_id=1 AND user_id=$1),
		  (SELECT count(*) FROM profile_claim_receipts
		    WHERE tenant_id=1 AND user_id=$1),
		  (SELECT count(*) FROM feedbacks
		    WHERE tenant_id=1 AND user_id=$1)`, legacy.userID,
	).Scan(&claims, &events, &receipts, &feedbacks); err != nil {
		t.Fatal(err)
	}
	if claims != 1 || events != 1 || receipts != 1 || feedbacks != 1 {
		t.Fatalf("epoch-zero Down lost facts claims/events/receipts/feedbacks=%d/%d/%d/%d",
			claims, events, receipts, feedbacks)
	}
}

func TestMigration066DownRefusesNonzeroEpochSentinel(t *testing.T) {
	dbURL, db, provider := openMigration066Database(t, "vane_066_down_nonzero")
	if _, err := provider.UpTo(t.Context(), 66); err != nil {
		t.Fatal(err)
	}
	var userID int64
	if err := db.QueryRowContext(t.Context(), `
		INSERT INTO users(feishu_open_id,name)
		VALUES('migration-066-down-nonzero','sentinel') RETURNING id`,
	).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `
		INSERT INTO memberships(tenant_id,user_id,role)
		VALUES(1,$1,'owner')`, userID); err != nil {
		t.Fatal(err)
	}
	st, err := New(t.Context(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	industry := "AI"
	if _, err := st.UpsertProfileFields(
		t.Context(), userID, &industry, nil, []string{"safety"},
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(),
		`ALTER TABLE profile_epochs DISABLE TRIGGER enforce_profile_epoch_seed_v1`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `
		INSERT INTO profile_epochs(tenant_id,user_id,profile_epoch)
		VALUES(1,$1,1)`, userID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(),
		`ALTER TABLE profile_epochs ENABLE TRIGGER enforce_profile_epoch_seed_v1`); err != nil {
		t.Fatal(err)
	}

	_, err = provider.Down(t.Context())
	requireMigration066SQLState(t, err, "P0001")
	if !strings.Contains(
		err.Error(), "refusing 066 downgrade after nonzero profile epoch facts exist",
	) {
		t.Fatalf("unexpected 066 Down refusal: %v", err)
	}
	var version, sentinels int64
	if err := db.QueryRowContext(t.Context(), `
		SELECT
		  (SELECT max(version_id) FROM goose_db_version WHERE is_applied),
		  (SELECT count(*) FROM profile_epochs
		    WHERE tenant_id=1 AND user_id=$1 AND profile_epoch=1)`,
		userID,
	).Scan(&version, &sentinels); err != nil {
		t.Fatal(err)
	}
	if version != 66 || sentinels != 1 {
		t.Fatalf("failed Down changed version/sentinel=%d/%d want 66/1",
			version, sentinels)
	}
}

func TestMigration066FeedbackSubjectScopeAndPooledGUCFailClosed(t *testing.T) {
	_, db, provider := openMigration066Database(t, "vane_066_feedback_scope")
	if _, err := provider.UpTo(t.Context(), 66); err != nil {
		t.Fatal(err)
	}
	userA, deliveryA := seedMigration066Delivery(t, db, "scope-a")
	userB, _ := seedMigration066Delivery(t, db, "scope-b")

	if _, err := db.ExecContext(t.Context(), `
		INSERT INTO feedbacks(tenant_id,user_id,delivery_id,action)
		VALUES(1,$1,$2,'interested')`, userB, deliveryA); err == nil {
		t.Fatal("same-tenant cross-user delivery was accepted")
	} else {
		requireMigration066SQLState(t, err, "23503")
	}

	cases := []struct {
		name     string
		settings string
	}{
		{name: "missing"},
		{name: "empty", settings: `
			SELECT set_config('app.tenant_id','',true),
			       set_config('app.user_id','',true)`},
		{name: "tenant only", settings: `
			SELECT set_config('app.tenant_id','1',true)`},
		{name: "user only", settings: `
			SELECT set_config('app.user_id','` +
			strconv.FormatInt(userA, 10) + `',true)`},
	}
	for _, tc := range cases {
		conn, err := db.Conn(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := conn.ExecContext(t.Context(), `RESET ALL`); err != nil {
			_ = conn.Close()
			t.Fatal(err)
		}
		tx, err := conn.BeginTx(t.Context(), nil)
		if err != nil {
			_ = conn.Close()
			t.Fatal(err)
		}
		if tc.settings != "" {
			if _, err := tx.ExecContext(t.Context(), tc.settings); err != nil {
				_ = tx.Rollback()
				_ = conn.Close()
				t.Fatalf("%s settings: %v", tc.name, err)
			}
		}
		if _, err := tx.ExecContext(t.Context(), `SET LOCAL ROLE vane_app`); err != nil {
			_ = tx.Rollback()
			_ = conn.Close()
			t.Fatal(err)
		}
		_, err = tx.ExecContext(t.Context(), `
			INSERT INTO feedbacks(tenant_id,user_id,delivery_id,action)
			VALUES(1,$1,$2,'interested')`, userA, deliveryA)
		requireMigration066SQLState(t, err, "42501")
		_ = tx.Rollback()
		_ = conn.Close()
	}

	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(t.Context(), `RESET ALL`); err != nil {
		t.Fatal(err)
	}
	tx, err := conn.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(t.Context(), `
		SELECT set_config('app.tenant_id','1',true),
		       set_config('app.user_id',$1,true)`,
		strconv.FormatInt(userA, 10)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `SET LOCAL ROLE vane_app`); err != nil {
		t.Fatal(err)
	}
	var epoch int64
	if err := tx.QueryRowContext(t.Context(), `
		INSERT INTO feedbacks(tenant_id,user_id,delivery_id,action)
		VALUES(1,$1,$2,'interested') RETURNING profile_epoch`,
		userA, deliveryA).Scan(&epoch); err != nil {
		t.Fatalf("exact vane_app feedback scope: %v", err)
	}
	if epoch != 0 {
		t.Fatalf("exact vane_app feedback epoch=%d want 0", epoch)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestMigration066PreProfileFeedbackAndActiveEpochReads(t *testing.T) {
	dbURL, db, provider := openMigration066Database(t, "vane_066_active_reads")
	if _, err := provider.UpTo(t.Context(), 66); err != nil {
		t.Fatal(err)
	}
	userID, delivery0 := seedMigration066Delivery(t, db, "pre-profile")
	st, err := New(t.Context(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	feedback0, err := st.InsertFeedback(t.Context(), &types.Feedback{
		UserID:     userID,
		DeliveryID: delivery0,
		Action:     types.FeedbackActionNotInterested,
	})
	if err != nil {
		t.Fatalf("real pre-profile InsertFeedback: %v", err)
	}
	industry := "AI"
	if _, err := st.UpsertProfileFields(
		t.Context(), userID, &industry, nil, []string{"safety"},
	); err != nil {
		t.Fatal(err)
	}
	rows, err := st.ListFeedbacksForEvolutionForTenant(
		t.Context(), 1, userID, 0, 10)
	if err != nil || len(rows) != 1 || rows[0].ID != feedback0 {
		t.Fatalf("pre-profile epoch-0 feedback not consumable: %+v err=%v", rows, err)
	}
	negative, err := st.ListRecentNegativeFeedbackTitlesForTenant(
		t.Context(), 1, userID, time.Now().Add(-time.Hour), 10)
	if err != nil || len(negative) != 1 ||
		negative[0] != "migration-066-pre-profile" {
		t.Fatalf("pre-profile epoch-0 negative fast path: %+v err=%v",
			negative, err)
	}

	if _, err := db.ExecContext(t.Context(),
		`ALTER TABLE profile_epochs DISABLE TRIGGER enforce_profile_epoch_seed_v1`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `
		INSERT INTO profile_epochs(tenant_id,user_id,profile_epoch)
		VALUES(1,$1,1)`, userID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(),
		`ALTER TABLE profile_epochs ENABLE TRIGGER enforce_profile_epoch_seed_v1`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(),
		`ALTER TABLE profile_claim_states DISABLE TRIGGER enforce_profile_epoch_state_v1`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `
		UPDATE profile_claim_states SET active_epoch=1
		WHERE tenant_id=1 AND user_id=$1`, userID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(),
		`ALTER TABLE profile_claim_states ENABLE TRIGGER enforce_profile_epoch_state_v1`); err != nil {
		t.Fatal(err)
	}
	feedbackPositive, err := st.InsertFeedback(t.Context(), &types.Feedback{
		UserID:     userID,
		DeliveryID: delivery0,
		Action:     types.FeedbackActionInterested,
	})
	if err != nil {
		t.Fatal(err)
	}
	delivery1 := seedMigration066DeliveryForUser(
		t, db, userID, "active-epoch")
	feedback1, err := st.InsertFeedback(t.Context(), &types.Feedback{
		UserID:     userID,
		DeliveryID: delivery1,
		Action:     types.FeedbackActionNotInterested,
	})
	if err != nil {
		t.Fatal(err)
	}
	var epoch1 int64
	if err := db.QueryRowContext(t.Context(),
		`SELECT profile_epoch FROM feedbacks WHERE id=$1`,
		feedback1).Scan(&epoch1); err != nil {
		t.Fatal(err)
	}
	if epoch1 != 1 {
		t.Fatalf("active feedback epoch=%d want 1", epoch1)
	}
	rows, err = st.ListFeedbacksForEvolutionForTenant(
		t.Context(), 1, userID, 0, 10)
	if err != nil || len(rows) != 2 ||
		rows[0].ID != feedbackPositive || rows[1].ID != feedback1 {
		t.Fatalf("active epoch feedback isolation failed: %+v err=%v", rows, err)
	}
	negative, err = st.ListRecentNegativeFeedbackTitlesForTenant(
		t.Context(), 1, userID, time.Now().Add(-time.Hour), 10)
	if err != nil || len(negative) != 1 ||
		negative[0] != "migration-066-active-epoch" {
		t.Fatalf("active epoch negative fast path leaked prior epoch: %+v err=%v",
			negative, err)
	}
	claims, err := st.ListProfileClaims(t.Context(), 1, userID)
	if err != nil || claims.ProfileEpoch != 1 || len(claims.Claims) != 0 {
		t.Fatalf("active epoch claim isolation failed: %+v err=%v", claims, err)
	}
}

func TestMigration066UpDrainsInFlightClaimWriterAtProfileRoot(t *testing.T) {
	_, db, provider := openMigration066Database(t, "vane_066_up_fence")
	db.SetMaxOpenConns(8)
	if _, err := provider.UpTo(t.Context(), 65); err != nil {
		t.Fatal(err)
	}
	fixture := seedMigration066LegacyProfile(t, db, "up-fence")
	writer, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = writer.Rollback() }()
	if _, err := writer.ExecContext(t.Context(), `
		SELECT set_config('app.tenant_id','1',true),
		       set_config('app.user_id',$1,true)`,
		strconv.FormatInt(fixture.userID, 10)); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.ExecContext(
		t.Context(), `SET LOCAL ROLE vane_profile_claim_editor`); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.ExecContext(t.Context(), `
		SELECT user_id FROM profiles
		WHERE tenant_id=1 AND user_id=$1 FOR UPDATE`, fixture.userID); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.ExecContext(t.Context(), `
		INSERT INTO profile_claims
		    (tenant_id,user_id,field_name,claim_value,source_state)
		VALUES(1,$1,'tag','in-flight','manual')`, fixture.userID); err != nil {
		t.Fatal(err)
	}

	migrationDone := make(chan error, 1)
	go func() {
		_, upErr := provider.UpTo(t.Context(), 66)
		migrationDone <- upErr
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var waiting bool
		if err := db.QueryRowContext(t.Context(), `
			SELECT EXISTS (
			  SELECT 1 FROM pg_locks l
			  JOIN pg_class c ON c.oid=l.relation
			  WHERE c.relname='profiles'
			    AND l.mode='AccessExclusiveLock'
			    AND NOT l.granted
			)`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("migration did not wait at the profiles ACCESS EXCLUSIVE root")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := writer.ExecContext(t.Context(), `
		UPDATE profile_claim_states SET version=version+1
		WHERE tenant_id=1 AND user_id=$1`, fixture.userID); err != nil {
		t.Fatalf("in-flight writer could not finish before cutover: %v", err)
	}
	if err := writer.Commit(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-migrationDone:
		if err != nil {
			t.Fatalf("066 Up after writer drain: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("066 Up did not finish after writer committed")
	}
	var epoch int64
	if err := db.QueryRowContext(t.Context(), `
		SELECT profile_epoch FROM profile_claims
		WHERE tenant_id=1 AND user_id=$1 AND claim_value='in-flight'`,
		fixture.userID).Scan(&epoch); err != nil {
		t.Fatal(err)
	}
	if epoch != 0 {
		t.Fatalf("in-flight claim backfilled epoch=%d want 0", epoch)
	}
}
