// This test builds a positive historical delivery fixture for the already
// frozen owner. It does not derive an HTTP/A2A principal or authorize a
// request; principal resolution remains in auth.PrincipalResolver.
//
//go:principal-exempt
package feishu

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	lark "github.com/larksuite/oapi-sdk-go/v3"

	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
)

func TestBackfillOwnerChatBindsHistoricalReceiptToCurrentApp(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("DATABASE_URL 未设置，跳过 owner chat 历史回填真库测试")
	}
	ctx := t.Context()
	if err := store.Migrate(ctx, dbURL); err != nil {
		t.Fatal(err)
	}
	st, err := store.New(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	registerStoreClose(t, st)
	old, oldErr := st.GetSetting(ctx, settingKeyOwner)
	if oldErr != nil && !errors.Is(oldErr, types.ErrNotFound) {
		t.Fatal(oldErr)
	}
	cleanupCtx, cancelCleanup := cleanupContext()
	t.Cleanup(cancelCleanup)
	t.Cleanup(func() {
		if oldErr == nil {
			cleanupExec(cleanupCtx, t, dbURL, `
				INSERT INTO settings (key, value, updated_at) VALUES ($1, $2, now())
				ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()`,
				settingKeyOwner, old)
			return
		}
		cleanupExec(cleanupCtx, t, dbURL,
			`DELETE FROM settings WHERE key = $1`, settingKeyOwner)
	})

	openID := fmt.Sprintf("ou_backfill_%d", time.Now().UnixNano())
	owner, err := st.UpsertUserByOpenID(ctx, openID, "backfill-owner")
	if err != nil {
		t.Fatal(err)
	}
	cleanupExec(ctx, t, dbURL, `
		INSERT INTO memberships (tenant_id, user_id, role)
		VALUES (1, $1, 'owner') ON CONFLICT DO NOTHING`, owner.ID)
	batchID, err := st.CreatePushBatch(ctx, owner.ID)
	if err != nil {
		t.Fatal(err)
	}
	deliveryID, err := st.InsertDelivery(ctx, &types.Delivery{
		BatchID: batchID,
		UserID:  owner.ID,
		Score:   1,
		BodyMD:  "owner chat backfill fixture",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.MarkDeliverySent(
		ctx,
		deliveryID,
		"om_backfill_receipt",
		json.RawMessage(`{}`),
		time.Now().UTC(),
	); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupExec(cleanupCtx, t, dbURL,
			`DELETE FROM deliveries WHERE id = $1`, deliveryID)
		cleanupExec(cleanupCtx, t, dbURL,
			`DELETE FROM push_batches WHERE id = $1`, batchID)
		cleanupExec(cleanupCtx, t, dbURL,
			`DELETE FROM memberships WHERE user_id = $1`, owner.ID)
		cleanupExec(cleanupCtx, t, dbURL,
			`DELETE FROM users WHERE id = $1`, owner.ID)
	})
	rawOwner, err := json.Marshal(ownerSetting{
		OpenID:     openID,
		Name:       "backfill-owner",
		CapturedAt: "2026-07-25T00:00:00Z",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.PutSetting(ctx, settingKeyOwner, rawOwner); err != nil {
		t.Fatal(err)
	}

	var getCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if servePushTestToken(w, r) {
			return
		}
		getCalls.Add(1)
		if !strings.Contains(r.URL.Path, "om_backfill_receipt") {
			http.Error(w, "unexpected message id", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w,
			`{"code":0,"msg":"success","data":{"items":[{`+
				`"message_id":"om_backfill_receipt","chat_id":"oc_backfilled_p2p"`+
				`}]}}`)
	}))
	t.Cleanup(server.Close)
	client := lark.NewClient(
		"current-app",
		"current-secret",
		lark.WithOpenBaseUrl(server.URL),
		lark.WithOAuthBaseUrl(server.URL),
		lark.WithHttpClient(server.Client()),
	)
	m := NewManager(st, nil, nil)
	m.mu.Lock()
	m.apiClient = client
	m.apiAppID = "current-app"
	m.mu.Unlock()
	m.loadOwner(ctx)

	if err := m.backfillOwnerChatID(ctx); err != nil {
		t.Fatalf("backfillOwnerChatID() error = %v", err)
	}
	if got := getCalls.Load(); got != 1 {
		t.Fatalf("provider message Get calls = %d, want 1", got)
	}
	if got := m.OwnerChatID(); got != "oc_backfilled_p2p" {
		t.Fatalf("OwnerChatID() = %q, want historical P2P chat", got)
	}
	persistedRaw, err := st.GetSetting(ctx, settingKeyOwner)
	if err != nil {
		t.Fatal(err)
	}
	var persisted ownerSetting
	if err := json.Unmarshal(persistedRaw, &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.AppIdentity != "current-app" ||
		persisted.ChatID != "oc_backfilled_p2p" {
		t.Fatalf("persisted owner = %+v, want current App bound chat", persisted)
	}
}
