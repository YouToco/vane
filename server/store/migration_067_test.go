package store

import (
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/YouToco/vane/types"
)

func TestMigration067EmptyDownRestoresPhaseA(t *testing.T) {
	_, db, provider := openMigration066Database(t, "vane_profile_epoch_067_empty")
	if _, err := provider.UpTo(t.Context(), 67); err != nil {
		t.Fatal(err)
	}
	var defaultIsNull bool
	if err := db.QueryRowContext(t.Context(), `
		SELECT column_default IS NULL
		  FROM information_schema.columns
		 WHERE table_schema='public' AND table_name='feedbacks'
		   AND column_name='id'`).Scan(&defaultIsNull); err != nil {
		t.Fatal(err)
	}
	if !defaultIsNull {
		t.Fatal("067 must allocate feedback id inside the fenced trigger")
	}
	if _, err := provider.DownTo(t.Context(), 66); err != nil {
		t.Fatalf("empty 067 Down must succeed: %v", err)
	}
	var restoredDefault string
	if err := db.QueryRowContext(t.Context(), `
		SELECT COALESCE(column_default,'')
		  FROM information_schema.columns
		 WHERE table_schema='public' AND table_name='feedbacks'
		   AND column_name='id'`).Scan(&restoredDefault); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(restoredDefault, "feedbacks_id_seq") {
		t.Fatalf("Down did not restore Phase A sequence default: %q", restoredDefault)
	}
	for _, table := range []string{
		"profile_feedback_epoch_fences", "profile_epoch_checkpoints",
		"profile_epoch_events", "profile_epoch_receipts",
	} {
		var exists bool
		if err := db.QueryRowContext(t.Context(),
			`SELECT to_regclass('public.'||$1) IS NOT NULL`, table,
		).Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if exists {
			t.Fatalf("empty Down left Phase B table %s", table)
		}
	}
}

func TestMigration067FenceRLSAndTransitionDownRefusal(t *testing.T) {
	dbURL, db, provider := openMigration066Database(
		t, "vane_profile_epoch_067_authority")
	if _, err := provider.UpTo(t.Context(), 67); err != nil {
		t.Fatal(err)
	}
	var noLogin, noInherit, noBypass bool
	if err := db.QueryRowContext(t.Context(), `
		SELECT NOT rolcanlogin,NOT rolinherit,NOT rolbypassrls
		  FROM pg_roles WHERE rolname='vane_profile_epoch_editor'`,
	).Scan(&noLogin, &noInherit, &noBypass); err != nil {
		t.Fatal(err)
	}
	if !noLogin || !noInherit || !noBypass {
		t.Fatalf("unsafe epoch role: nologin=%v noinherit=%v nobypass=%v",
			noLogin, noInherit, noBypass)
	}

	userID, deliveryID := seedMigration066Delivery(t, db, uuid.NewString())
	var feedbackID int64
	if err := db.QueryRowContext(t.Context(), `
		INSERT INTO feedbacks(tenant_id,user_id,delivery_id,action)
		VALUES(1,$1,$2,'interested') RETURNING id`,
		userID, deliveryID,
	).Scan(&feedbackID); err != nil {
		t.Fatal(err)
	}
	var fenceID int64
	if err := db.QueryRowContext(t.Context(), `
		SELECT last_feedback_id FROM profile_feedback_epoch_fences
		 WHERE tenant_id=1 AND user_id=$1`, userID,
	).Scan(&fenceID); err != nil {
		t.Fatal(err)
	}
	if feedbackID <= 0 || fenceID != feedbackID {
		t.Fatalf("feedback was not allocated under fence: feedback=%d fence=%d",
			feedbackID, fenceID)
	}

	st, err := New(t.Context(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	industry := "AI"
	if _, err := st.PatchProfile(
		t.Context(), 1, userID, nil,
		types.ProfileEditPatch{Industry: &industry},
		"migration-067-profile-"+uuid.NewString(), strings.Repeat("1", 64),
	); err != nil {
		st.Close()
		t.Fatal(err)
	}
	claims, err := st.ListProfileClaims(t.Context(), 1, userID)
	if err != nil {
		st.Close()
		t.Fatal(err)
	}
	if _, err := st.ApplyProfileEpochAction(
		t.Context(), 1, userID,
		types.ProfileEpochAction{
			ExpectedEpoch: claims.ProfileEpoch, ExpectedVersion: claims.Version,
			Action: "reset", Scope: "history_learning",
		},
		"migration-067-reset-"+uuid.NewString(), strings.Repeat("2", 64),
	); err != nil {
		st.Close()
		t.Fatal(err)
	}
	st.Close()

	assertEpochEventRLS := func(tenant, user string, want int) {
		t.Helper()
		tx, err := db.BeginTx(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		if _, err := tx.ExecContext(t.Context(), `
			SELECT set_config('app.tenant_id',$1,true),
			       set_config('app.user_id',$2,true)`, tenant, user); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(
			t.Context(), `SET LOCAL ROLE vane_profile_claim_editor`); err != nil {
			t.Fatal(err)
		}
		var got int
		if err := tx.QueryRowContext(t.Context(),
			`SELECT count(*) FROM profile_epoch_events`).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("epoch event RLS tenant=%q user=%q got=%d want=%d",
				tenant, user, got, want)
		}
	}
	assertEpochEventRLS("", "", 0)
	assertEpochEventRLS("1", strconv.FormatInt(userID, 10), 1)
	assertEpochEventRLS("1", strconv.FormatInt(userID+1, 10), 0)

	_, err = provider.DownTo(t.Context(), 66)
	if err == nil {
		t.Fatal("067 Down must refuse after a transition")
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "P0001" {
		t.Fatalf("Down refusal SQLSTATE=%v error=%v", pgErr, err)
	}
	var version int64
	if err := db.QueryRowContext(t.Context(), `
		SELECT COALESCE(max(version_id),0)
		  FROM goose_db_version WHERE is_applied`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 67 {
		t.Fatalf("failed Down changed goose version to %d", version)
	}
}
