package store

import (
	"testing"
)

func TestPrepareArtifactDeliveryPlanFreezesProviderSetAndRoutePG(t *testing.T) {
	dbURL := freshMigrationDatabase(t, "artifact_delivery_plan")
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
	if _, err := st.pool.Exec(t.Context(),
		`INSERT INTO schedules(id,tenant_id,user_id,nl_description)
		 VALUES ('artifact-task',$1,$2,'artifact')`, tenantID, userID); err != nil {
		t.Fatal(err)
	}
	var identityID, routeID int64
	if err := st.pool.QueryRow(t.Context(),
		`INSERT INTO channel_identities
		 (tenant_id,user_id,provider,app_identity,external_user_id,
		  provider_chat_id,chat_type)
		 VALUES ($1,$2,'telegram','9101','7101','7101','private') RETURNING id`,
		tenantID, userID).Scan(&identityID); err != nil {
		t.Fatal(err)
	}
	if err := st.pool.QueryRow(t.Context(),
		`INSERT INTO channel_routes
		 (tenant_id,user_id,identity_id,provider,app_identity,provider_chat_id,
		  provider_thread_id,chat_type,route_kind)
		 VALUES ($1,$2,$3,'telegram','9101','7101','0','private','private') RETURNING id`,
		tenantID, userID, identityID).Scan(&routeID); err != nil {
		t.Fatal(err)
	}
	first, err := st.PrepareArtifactDeliveryPlan(t.Context(), tenantID, userID,
		"artifact-task", ArtifactDeliveryPeriodicReport, "report:7:digest",
		DeliveryChannelPreference{Selection: DeliveryChannelBoth})
	if err != nil || first.Selection != DeliveryChannelBoth ||
		first.TelegramRouteID == nil || *first.TelegramRouteID != routeID {
		t.Fatalf("first plan=%+v err=%v", first, err)
	}
	// A mutable preference change cannot alter the already-authorized provider
	// set for this exact artifact replay.
	replayed, err := st.PrepareArtifactDeliveryPlan(t.Context(), tenantID, userID,
		"artifact-task", ArtifactDeliveryPeriodicReport, "report:7:digest",
		DeliveryChannelPreference{Selection: DeliveryChannelFeishu})
	if err != nil || replayed.ID != first.ID ||
		replayed.Selection != DeliveryChannelBoth || replayed.TelegramRouteID == nil ||
		*replayed.TelegramRouteID != routeID {
		t.Fatalf("replayed plan=%+v err=%v", replayed, err)
	}
	if _, err := st.PrepareArtifactDeliveryPlan(t.Context(), tenantID, userID,
		"artifact-task", ArtifactDeliveryResearchV3, "research:8",
		DeliveryChannelPreference{Selection: DeliveryChannelTelegram,
			TelegramRouteID: func() *int64 { value := int64(999999); return &value }()}); err == nil {
		t.Fatal("unknown Telegram route unexpectedly frozen")
	}
}

func TestPrepareArtifactDeliveryPlanRejectsInvalidInput(t *testing.T) {
	st := &Store{}
	for _, test := range []struct {
		task, kind, key string
	}{
		{"", ArtifactDeliveryPeriodicReport, "key"},
		{"task", "email", "key"},
		{"task", ArtifactDeliveryPeriodicReport, ""},
	} {
		if _, err := st.PrepareArtifactDeliveryPlan(t.Context(), 1, 1,
			test.task, test.kind, test.key,
			DeliveryChannelPreference{Selection: DeliveryChannelFeishu}); err == nil {
			t.Fatalf("invalid input accepted: %+v", test)
		}
	}
}
