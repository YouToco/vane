package telegram

import (
	"context"
	"testing"
	"time"
)

func TestReviewRevokedOldOwnerStillBlocksReassignedBot(t *testing.T) {
	provider, _ := telegramFleetProvider(t)
	defer provider.Close()
	st := newFakeFleetStore()
	st.setCredential(7, 70, 1, 111, "111:a", "secret-a")
	fleet, err := NewFleet(FleetConfig{WebhookURL: "https://api.vane.test/telegram/webhook",
		APIBaseURL: provider.URL, Workers: 1, Dynamic: true, ShutdownGrace: time.Second},
		st, &fakeChannelAgent{}, provider.Client(), nil)
	if err != nil { t.Fatal(err) }
	if err := fleet.Start(t.Context()); err != nil { t.Fatal(err) }
	t.Cleanup(func() { _ = fleet.Shutdown(context.Background()) })

	// This is the state after A's DB revoke committed and B acquired the same
	// bot's unique DB authority, but before A's API path reached DeactivateUser.
	st.mu.Lock()
	delete(st.active, userScope{tenantID: 7, userID: 70})
	st.mu.Unlock()
	st.setCredential(8, 80, 1, 111, "111:b", "secret-b")
	if err := fleet.ActivateUser(t.Context(), 8, 80); err == nil {
		t.Fatal("stale revoked owner did not block reassignment")
	} else {
		t.Logf("reassignment rejected by stale byBot: %v", err)
	}
	if err := fleet.DeactivateUser(t.Context(), 7, 70); err != nil { t.Fatal(err) }
	if got := fleet.PrincipalStatus(t.Context(), 8, 80); got.Ready {
		t.Fatalf("unexpected reassigned manager: %+v", got)
	}
}

func TestReviewLateOldDeactivateDeletesNewGeneration(t *testing.T) {
	provider, _ := telegramFleetProvider(t)
	defer provider.Close()
	st := newFakeFleetStore()
	st.setCredential(7, 70, 1, 111, "111:a", "secret-a")
	fleet, err := NewFleet(FleetConfig{WebhookURL: "https://api.vane.test/telegram/webhook",
		APIBaseURL: provider.URL, Workers: 1, Dynamic: true, ShutdownGrace: time.Second},
		st, &fakeChannelAgent{}, provider.Client(), nil)
	if err != nil { t.Fatal(err) }
	if err := fleet.Start(t.Context()); err != nil { t.Fatal(err) }
	t.Cleanup(func() { _ = fleet.Shutdown(context.Background()) })

	st.setCredential(7, 70, 2, 222, "222:new", "secret-new")
	if err := fleet.ActivateUser(t.Context(), 7, 70); err != nil { t.Fatal(err) }
	if got := fleet.PrincipalStatus(t.Context(), 7, 70); !got.Ready || got.BotID != 222 {
		t.Fatalf("new generation not active: %+v", got)
	}
	// Represents an older revoke request whose DB commit preceded generation 2,
	// but whose process-local cleanup acquired reconfigure after activation.
	if err := fleet.DeactivateUser(t.Context(), 7, 70); err != nil { t.Fatal(err) }
	if got := fleet.PrincipalStatus(t.Context(), 7, 70); got.Ready {
		t.Fatalf("late stale deactivate retained new manager: %+v", got)
	} else {
		t.Log("late stale DeactivateUser removed the current generation")
	}
}
