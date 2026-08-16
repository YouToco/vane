package store

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/server/types"
)

func TestMigration139ChannelMediaEnvelopeBoundary(t *testing.T) {
	payload, err := migrationsFS.ReadFile("migrations/139_channel_media_envelope.sql")
	if err != nil {
		t.Fatal(err)
	}
	text := string(payload)
	for _, required := range []string{
		"ADD COLUMN media_envelope JSONB",
		"vane.channel-message/v1",
		"jsonb_array_length(media_envelope->'items') BETWEEN 1 AND 10",
		"octet_length(media_envelope::text) <= 65536",
		"-- +goose StatementBegin",
		"-- +goose StatementEnd",
		"refusing downgrade while channel media history exists",
	} {
		if !strings.Contains(text, required) {
			t.Errorf("migration 139 lost boundary %q", required)
		}
	}
	for _, forbidden := range []string{
		"bot_token", "file_path", "download_url", "media_bytes", "BYTEA",
	} {
		if strings.Contains(text, forbidden) {
			t.Errorf("migration 139 persists forbidden authority %q", forbidden)
		}
	}
}

func TestTelegramMediaEnvelopeReplayPostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		requireDatabaseCapability(t)
	}
	database, provider, scratchURL, drop := migration128Scratch(t, databaseURL)
	t.Cleanup(drop)
	if _, err := provider.UpTo(t.Context(), 139); err != nil {
		t.Fatal(err)
	}
	st, err := New(t.Context(), scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	registerStoreClose(t, st)
	userID, tenantID := migration129Identity(t, database, "telegram-media-owner")
	tokenHash := sha256.Sum256(bytes.Repeat([]byte{0x73}, 32))
	if err := st.IssueTelegramLinkRequest(t.Context(), tenantID, userID,
		"12345", tokenHash[:], time.Now().Add(10*time.Minute)); err != nil {
		t.Fatal(err)
	}
	identity, _, err := st.ConsumeTelegramLinkRequest(t.Context(), tokenHash[:],
		"12345", "777", "777", strings.Repeat("a", 64))
	if err != nil {
		t.Fatal(err)
	}
	_, route, err := st.ResolveTelegramRoute(t.Context(), "12345", "777", "777", "0")
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := types.MarshalChannelMessageEnvelopeV1(types.ChannelMessageEnvelopeV1{
		Schema:  types.ChannelMessageEnvelopeV1Schema,
		Caption: "分析这张截图",
		Items: []types.ChannelMessageMediaItemV1{{
			Kind: "image", ProviderFileID: "provider-file",
			ProviderUniqueID: "provider-unique", MIMEType: "image/jpeg",
			SizeBytes: 2048, Width: 1280, Height: 720,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	turnID := uuid.NewString()
	created, err := st.AcceptTelegramRoutedIngress(t.Context(), identity, route,
		"9300", strings.Repeat("b", 64), "telegram:media-help", turnID,
		"12", "message", "", envelope)
	if err != nil || !created {
		t.Fatalf("media accept=%t err=%v", created, err)
	}
	// JSONB normalizes key order; an exact semantic replay must remain exact.
	reordered := []byte(`{"items":[{"width":1280,"height":720,"size_bytes":2048,"mime_type":"image/jpeg","provider_unique_id":"provider-unique","provider_file_id":"provider-file","kind":"image"}],"caption":"分析这张截图","schema":"vane.channel-message/v1"}`)
	created, err = st.AcceptTelegramRoutedIngress(t.Context(), identity, route,
		"9300", strings.Repeat("b", 64), "telegram:media-help", turnID,
		"12", "message", "", reordered)
	if err != nil || created {
		t.Fatalf("media replay=%t err=%v", created, err)
	}
	changed := bytes.Replace(envelope, []byte("provider-file"), []byte("changed-file"), 1)
	if _, err := st.AcceptTelegramRoutedIngress(t.Context(), identity, route,
		"9300", strings.Repeat("b", 64), "telegram:media-help", turnID,
		"12", "message", "", changed); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("changed media replay err=%v", err)
	}
	claimed, err := st.ClaimNextTelegramIngress(t.Context(), "12345", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := types.DecodeChannelMessageEnvelopeV1(claimed.MediaEnvelope)
	if err != nil || decoded.Caption != "分析这张截图" || len(decoded.Items) != 1 ||
		decoded.Items[0].ProviderFileID != "provider-file" {
		t.Fatalf("claimed media=%+v err=%v", decoded, err)
	}
	var stored string
	if err := database.QueryRowContext(t.Context(),
		`SELECT media_envelope::text FROM channel_ingress_receipts
		  WHERE provider='telegram' AND app_identity='12345' AND provider_update_id='9300'`,
	).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(stored, "bot-token") || strings.Contains(stored, "api.telegram.org/file") {
		t.Fatalf("stored media leaked transport authority: %s", stored)
	}
	if _, err := st.AcceptTelegramRoutedIngress(t.Context(), identity, route,
		"9301", strings.Repeat("c", 64), "not-media-help", uuid.NewString(),
		"13", "message", "", envelope); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("media bypass err=%v", err)
	}
	if _, err := provider.DownTo(t.Context(), 138); err == nil ||
		!strings.Contains(err.Error(), "channel media history exists") ||
		strings.Contains(err.Error(), "unterminated") || strings.Contains(err.Error(), "42601") {
		t.Fatalf("media history downgrade err=%v", err)
	}
}
