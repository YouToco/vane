package store

import (
	"context"
	"strings"
	"testing"
)

func TestDeliveryChannelSelectionIncludes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		selection DeliveryChannelSelection
		provider  string
		want      bool
	}{
		{DeliveryChannelFeishu, "feishu", true},
		{DeliveryChannelFeishu, "telegram", false},
		{DeliveryChannelTelegram, "telegram", true},
		{DeliveryChannelTelegram, "feishu", false},
		{DeliveryChannelBoth, "feishu", true},
		{DeliveryChannelBoth, "telegram", true},
		{DeliveryChannelBoth, "email", false},
	}
	for _, test := range tests {
		if got := test.selection.Includes(test.provider); got != test.want {
			t.Fatalf("%s includes %s=%v, want %v",
				test.selection, test.provider, got, test.want)
		}
	}
}

func TestDeliveryChannelPreferenceRejectsInvalidScopeAndSelection(t *testing.T) {
	st := &Store{}
	if _, err := st.ResolveDeliveryChannelPreference(
		t.Context(), 0, 1, ""); err == nil {
		t.Fatal("zero tenant unexpectedly accepted")
	}
	if _, err := st.ResolveDeliveryChannelPreference(
		t.Context(), 1, 1, " "); err == nil {
		t.Fatal("non-canonical task ID unexpectedly accepted")
	}
	if _, err := st.ResolveDeliveryChannelPreference(
		t.Context(), 1, 1, strings.Repeat("x", 256)); err == nil {
		t.Fatal("oversized task ID unexpectedly accepted")
	}
	if _, err := st.PutAccountDeliveryChannelPreference(
		t.Context(), 1, 1, "email", nil); err == nil {
		t.Fatal("unknown provider selection unexpectedly accepted")
	}
	badRoute := int64(-1)
	if _, err := st.PutAccountDeliveryChannelPreference(
		t.Context(), 1, 1, DeliveryChannelTelegram, &badRoute); err == nil {
		t.Fatal("negative Telegram route unexpectedly accepted")
	}
	if _, err := st.PutTaskDeliveryChannelPreference(
		t.Context(), 1, 1, "", DeliveryChannelBoth, nil); err == nil {
		t.Fatal("empty task override unexpectedly accepted")
	}
	if _, err := st.DeleteTaskDeliveryChannelPreference(
		t.Context(), 1, 1, ""); err == nil {
		t.Fatal("empty task override delete unexpectedly accepted")
	}
}

func TestDeliveryChannelPreferenceDatabaseFailuresAreClassifiedPG(t *testing.T) {
	dbURL := freshMigrationDatabase(t, "delivery_channel_preference_failures")
	if err := migrate(t.Context(), dbURL, 0); err != nil {
		t.Fatal(err)
	}
	st, err := New(t.Context(), dbURL)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := st.pool.Exec(t.Context(),
		`ALTER TABLE delivery_channel_preferences RENAME TO delivery_channel_preferences_hidden`); err != nil {
		t.Fatal(err)
	}
	if _, err := st.ResolveDeliveryChannelPreference(
		t.Context(), 1, 1, ""); err == nil {
		t.Fatal("missing preference table did not fail read")
	}
	if _, err := st.PutAccountDeliveryChannelPreference(
		t.Context(), 1, 1, DeliveryChannelBoth, nil); err == nil {
		t.Fatal("missing preference table did not fail write")
	}
	st.Close()

	closed, err := New(t.Context(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	closed.Close()
	if _, err := closed.ResolveDeliveryChannelPreference(
		t.Context(), 1, 1, ""); err == nil {
		t.Fatal("closed pool did not fail read transaction")
	}
	if _, err := closed.PutAccountDeliveryChannelPreference(
		t.Context(), 1, 1, DeliveryChannelBoth, nil); err == nil {
		t.Fatal("closed pool did not fail write transaction")
	}
}

func TestDeliveryChannelPreferenceResolutionPG(t *testing.T) {
	dbURL := freshMigrationDatabase(t, "delivery_channel_preference")
	if err := migrate(t.Context(), dbURL, 0); err != nil {
		t.Fatal(err)
	}
	st, err := New(t.Context(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	userID := testUser(t, st)
	var tenantID int64
	if err := st.pool.QueryRow(t.Context(),
		`INSERT INTO tenants(status,plan) VALUES ('active','free') RETURNING id`,
	).Scan(&tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(t.Context(),
		`INSERT INTO memberships(tenant_id,user_id,role) VALUES ($1,$2,'owner')`,
		tenantID, userID); err != nil {
		t.Fatal(err)
	}
	var identityID, routeID int64
	if err := st.pool.QueryRow(t.Context(),
		`INSERT INTO channel_identities
		 (tenant_id,user_id,provider,app_identity,external_user_id,
		  provider_chat_id,chat_type)
		 VALUES ($1,$2,'telegram','9001','7001','7001','private')
		 RETURNING id`, tenantID, userID).Scan(&identityID); err != nil {
		t.Fatal(err)
	}
	if err := st.pool.QueryRow(t.Context(),
		`INSERT INTO channel_routes
		 (tenant_id,user_id,identity_id,provider,app_identity,provider_chat_id,
		  provider_thread_id,chat_type,route_kind)
		 VALUES ($1,$2,$3,'telegram','9001','7001','0','private','private')
		 RETURNING id`, tenantID, userID, identityID).Scan(&routeID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(t.Context(),
		`INSERT INTO schedules(id,tenant_id,user_id,nl_description)
		 VALUES ('task-one',$1,$2,'one'),('task-two',$1,$2,'two')`,
		tenantID, userID); err != nil {
		t.Fatal(err)
	}

	preference, err := st.ResolveDeliveryChannelPreference(
		t.Context(), tenantID, userID, "task-one")
	if err != nil || preference.Selection != DeliveryChannelFeishu ||
		preference.Explicit {
		t.Fatalf("default=%+v err=%v", preference, err)
	}
	preference, err = st.PutAccountDeliveryChannelPreference(
		t.Context(), tenantID, userID, DeliveryChannelBoth, nil)
	if err != nil || preference.Selection != DeliveryChannelBoth ||
		!preference.Explicit {
		t.Fatalf("account=%+v err=%v", preference, err)
	}
	taskPreference, err := st.PutTaskDeliveryChannelPreference(
		t.Context(), tenantID, userID, "task-one",
		DeliveryChannelTelegram, &routeID)
	if err != nil || taskPreference.Selection != DeliveryChannelTelegram ||
		taskPreference.Scope != "task" || taskPreference.TelegramRouteID == nil ||
		*taskPreference.TelegramRouteID != routeID {
		t.Fatalf("task override=%+v err=%v", taskPreference, err)
	}
	accountPreference, err := st.ResolveDeliveryChannelPreference(
		t.Context(), tenantID, userID, "task-two")
	if err != nil || accountPreference.Selection != DeliveryChannelBoth ||
		accountPreference.Scope != "account" {
		t.Fatalf("account fallback=%+v err=%v", accountPreference, err)
	}
	inherited, err := st.DeleteTaskDeliveryChannelPreference(
		t.Context(), tenantID, userID, "task-one")
	if err != nil || inherited.Selection != DeliveryChannelBoth ||
		inherited.Scope != "account" || !inherited.Explicit {
		t.Fatalf("deleted task override=%+v err=%v", inherited, err)
	}
	if _, err := st.PutTaskDeliveryChannelPreference(
		t.Context(), tenantID, userID, "missing-task",
		DeliveryChannelBoth, nil); err == nil {
		t.Fatal("missing task override unexpectedly accepted")
	}
	withRoute, err := st.PutAccountDeliveryChannelPreference(
		t.Context(), tenantID, userID, DeliveryChannelBoth, &routeID)
	if err != nil || withRoute.TelegramRouteID == nil ||
		*withRoute.TelegramRouteID != routeID {
		t.Fatalf("valid Telegram route=%+v err=%v", withRoute, err)
	}
	feishuOnly, err := st.PutAccountDeliveryChannelPreference(
		t.Context(), tenantID, userID, DeliveryChannelFeishu, &routeID)
	if err != nil || feishuOnly.TelegramRouteID != nil {
		t.Fatalf("Feishu route clearing=%+v err=%v", feishuOnly, err)
	}

	// A nonexistent route cannot be smuggled into the preference even though
	// the application owner connection itself bypasses RLS.
	badRoute := int64(999999)
	if _, err := st.PutAccountDeliveryChannelPreference(
		context.Background(), tenantID, userID,
		DeliveryChannelTelegram, &badRoute); err == nil {
		t.Fatal("foreign Telegram route unexpectedly accepted")
	}
}
