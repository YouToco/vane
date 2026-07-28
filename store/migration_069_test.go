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

func TestMigration069EmptyDownRestores068(t *testing.T) {
	dbURL, db, provider := openMigration066Database(
		t, "vane_profile_activity_069_empty")
	if _, err := provider.UpTo(t.Context(), 69); err != nil {
		t.Fatal(err)
	}
	st, err := New(t.Context(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	user, err := st.UpsertUserByOpenID(
		t.Context(), "profile_069_down_"+uuid.NewString(), "down")
	if err != nil {
		st.Close()
		t.Fatal(err)
	}
	attachTenant(t, st, user.ID)
	industry := "AI"
	if _, err := st.PatchProfile(
		t.Context(), 1, user.ID, nil,
		types.ProfileEditPatch{Industry: &industry},
		"069-down-profile-"+uuid.NewString(), strings.Repeat("1", 64),
	); err != nil {
		st.Close()
		t.Fatal(err)
	}
	beforeReset, err := st.ListProfileClaims(t.Context(), 1, user.ID)
	if err != nil {
		st.Close()
		t.Fatal(err)
	}
	reset, err := st.ApplyProfileEpochAction(
		t.Context(), 1, user.ID,
		types.ProfileEpochAction{
			ExpectedEpoch:   beforeReset.ProfileEpoch,
			ExpectedVersion: beforeReset.Version,
			Action:          "reset", Scope: "history_learning",
		},
		"069-down-reset-"+uuid.NewString(), strings.Repeat("2", 64),
	)
	if err != nil {
		st.Close()
		t.Fatal(err)
	}
	st.Close()
	if _, err := provider.DownTo(t.Context(), 68); err != nil {
		t.Fatalf("empty 069 Down must succeed: %v", err)
	}
	var exists bool
	if err := db.QueryRowContext(t.Context(), `
		SELECT to_regclass('public.profile_epoch_activities') IS NOT NULL`,
	).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("069 Down left profile_epoch_activities")
	}
	compat, err := New(t.Context(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer compat.Close()
	current, err := compat.ListProfileClaims(t.Context(), 1, user.ID)
	if err != nil {
		t.Fatalf("current binary must read safe-Down 068 schema: %v", err)
	}
	if !current.RestoreAllowed {
		t.Fatalf("068 reset should remain restorable after safe 069 Down: %+v",
			current)
	}
	if _, err := compat.ApplyProfileEpochAction(
		t.Context(), 1, user.ID,
		types.ProfileEpochAction{
			ExpectedEpoch:   reset.ProfileEpoch,
			ExpectedVersion: reset.Version,
			Action:          "restore",
		},
		"069-down-restore-"+uuid.NewString(), strings.Repeat("3", 64),
	); err != nil {
		t.Fatalf("current binary restore on safe-Down 068 schema: %v", err)
	}
}

func TestMigration069ActivityRLSPrivilegesAndDownRefusal(t *testing.T) {
	dbURL, db, provider := openMigration066Database(
		t, "vane_profile_activity_069_authority")
	if _, err := provider.UpTo(t.Context(), 69); err != nil {
		t.Fatal(err)
	}
	suffixA := "activity-rls-a-" + uuid.NewString()
	suffixB := "activity-rls-b-" + uuid.NewString()
	userID, deliveryA := seedMigration066Delivery(t, db, suffixA)
	deliveryB := seedMigration066DeliveryForUser(
		t, db, userID, suffixB)
	messageID := "om_activity_rls_" + uuid.NewString()
	if _, err := db.ExecContext(t.Context(), `
		UPDATE deliveries SET feishu_message_id=$1,status='sent',sent_at=now()
		 WHERE id=ANY($2)`,
		messageID, []int64{deliveryA, deliveryB},
	); err != nil {
		t.Fatal(err)
	}
	st, err := New(t.Context(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	const wrappedContext = "[追问上下文] migration 069 authority receipt"
	inboundKey := "om_inbound_" + uuid.NewString()
	requestDigest := strings.Repeat("a", 64)
	if stored, err := st.RecordAggregateQuestionActivity(
		t.Context(), userID, "cli_activity_rls",
		inboundKey, messageID, requestDigest,
		[]int64{deliveryA, deliveryB},
		wrappedContext,
	); err != nil {
		st.Close()
		t.Fatal(err)
	} else if stored != wrappedContext {
		st.Close()
		t.Fatalf("stored wrapped context=%q", stored)
	}
	if replay, found, err := st.LookupAggregateQuestionActivity(
		t.Context(), userID, "cli_activity_rls",
		inboundKey, requestDigest,
	); err != nil {
		st.Close()
		t.Fatal(err)
	} else if !found || replay != wrappedContext {
		st.Close()
		t.Fatalf("lookup found/context=%v/%q", found, replay)
	}
	st.Close()

	assertVisible := func(role, tenant, user string, want int) {
		t.Helper()
		tx, err := db.BeginTx(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		if _, err := tx.ExecContext(t.Context(), `
			SELECT set_config('app.tenant_id',$1,true),
			       set_config('app.user_id',$2,true)`,
			tenant, user,
		); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(
			t.Context(), `SET LOCAL ROLE `+role); err != nil {
			t.Fatal(err)
		}
		var got int
		if err := tx.QueryRowContext(t.Context(), `
			SELECT count(profile_epoch) FROM profile_epoch_activities`,
		).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf(
				"activity RLS role=%s tenant=%q user=%q got=%d want=%d",
				role, tenant, user, got, want,
			)
		}
	}
	for _, role := range []string{
		"vane_app", "vane_profile_claim_editor",
		"vane_profile_epoch_editor",
	} {
		assertVisible(role, "", "", 0)
		assertVisible(role, "1", strconv.FormatInt(userID, 10), 1)
		assertVisible(role, "1", strconv.FormatInt(userID+1, 10), 0)
	}

	var appCanUpdate, appCanDelete bool
	if err := db.QueryRowContext(t.Context(), `
		SELECT has_table_privilege('vane_app','profile_epoch_activities','UPDATE'),
		       has_table_privilege('vane_app','profile_epoch_activities','DELETE')`,
	).Scan(&appCanUpdate, &appCanDelete); err != nil {
		t.Fatal(err)
	}
	if appCanUpdate || appCanDelete {
		t.Fatalf("vane_app activity mutation too broad: update=%v delete=%v",
			appCanUpdate, appCanDelete)
	}

	_, err = provider.DownTo(t.Context(), 68)
	if err == nil {
		t.Fatal("069 Down must refuse after a durable activity")
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "P0001" {
		t.Fatalf("Down refusal SQLSTATE=%v error=%v", pgErr, err)
	}
	var version int64
	if err := db.QueryRowContext(t.Context(), `
		SELECT COALESCE(max(version_id),0)
		  FROM goose_db_version WHERE is_applied`,
	).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 69 {
		t.Fatalf("failed Down changed goose version to %d", version)
	}
}
