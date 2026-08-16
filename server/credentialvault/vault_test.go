package credentialvault

import (
	"bytes"
	"strings"
	"testing"
)

func TestEnvelopeIsBoundToExactScopeAndDetectsTampering(t *testing.T) {
	vault, err := New("k2", map[string][]byte{
		"k1": bytes.Repeat([]byte{1}, 32),
		"k2": bytes.Repeat([]byte{2}, 32),
	})
	if err != nil {
		t.Fatal(err)
	}
	scope := Scope{TenantID: 7, UserID: 42, Kind: "mcp.oauth.refresh_token"}
	secret := []byte("refresh-token-that-must-not-leak")
	envelope, err := vault.Seal(scope, secret)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(envelope.Ciphertext, string(secret)) || envelope.KeyVersion != "k2" {
		t.Fatalf("unsafe envelope: %+v", envelope)
	}
	opened, err := vault.Open(scope, envelope)
	if err != nil || !bytes.Equal(opened, secret) {
		t.Fatalf("open=%q err=%v", opened, err)
	}
	for _, wrong := range []Scope{
		{TenantID: 8, UserID: 42, Kind: scope.Kind},
		{TenantID: 7, UserID: 43, Kind: scope.Kind},
		{TenantID: 7, UserID: 42, Kind: "feishu.app_secret"},
	} {
		if _, err := vault.Open(wrong, envelope); err == nil {
			t.Fatalf("cross-scope envelope accepted: %+v", wrong)
		}
	}
	tampered := envelope
	replacement := byte('A')
	if tampered.Ciphertext[0] == replacement {
		replacement = 'B'
	}
	tampered.Ciphertext = string(replacement) + tampered.Ciphertext[1:]
	if _, err := vault.Open(scope, tampered); err == nil {
		t.Fatal("tampered envelope accepted")
	}
}

func TestRetainedKeyCanReadButRequestsExplicitRotation(t *testing.T) {
	oldVault, err := New("k1", map[string][]byte{"k1": bytes.Repeat([]byte{1}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	scope := Scope{TenantID: 7, UserID: 0, Kind: "workspace.feishu.app_secret"}
	envelope, err := oldVault.Seal(scope, []byte("secret"))
	if err != nil {
		t.Fatal(err)
	}
	rotated, err := New("k2", map[string][]byte{
		"k1": bytes.Repeat([]byte{1}, 32),
		"k2": bytes.Repeat([]byte{2}, 32),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !rotated.NeedsRotation(envelope) {
		t.Fatal("retained envelope did not request rotation")
	}
	if opened, err := rotated.Open(scope, envelope); err != nil || string(opened) != "secret" {
		t.Fatalf("retained key read=%q err=%v", opened, err)
	}
}

func TestConfigurationAndEnvelopeParsingFailClosed(t *testing.T) {
	if _, err := New("missing", map[string][]byte{"k1": make([]byte, 32)}); err == nil {
		t.Fatal("missing primary accepted")
	}
	if _, err := New("k1", map[string][]byte{"k1": make([]byte, 16)}); err == nil {
		t.Fatal("short key accepted")
	}
	for _, raw := range [][]byte{
		[]byte(`{"schema":"vane.encrypted-credential/v1","key_version":"k1","nonce":"n","ciphertext":"c","extra":true}`),
		[]byte(`{"schema":"other","key_version":"k1","nonce":"n","ciphertext":"c"}`),
		[]byte(`{"schema":"vane.encrypted-credential/v1","key_version":"k1","nonce":"n","ciphertext":"c"}{}`),
	} {
		if _, err := Parse(raw); err == nil {
			t.Fatalf("invalid envelope accepted: %s", raw)
		}
	}
}
