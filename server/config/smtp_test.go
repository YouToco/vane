package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSMTPRequiresEncryptedCompleteConfiguration(t *testing.T) {
	cfg := &Config{DB: DBConfig{URL: "postgres://test-only"}, SMTP: SMTPConfig{Enabled: true}}
	if err := cfg.Validate(); err == nil {
		t.Fatal("enabled SMTP with missing fields must fail")
	}
	cfg.SMTP = SMTPConfig{
		Enabled: true, Host: "smtp.example.com", Port: 587,
		Username: "mailer", Password: "test-only-password",
		From: "Vane <noreply@example.com>", TLSMode: "plaintext",
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("plaintext SMTP must fail")
	}
	cfg.SMTP.TLSMode = "starttls"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("valid STARTTLS config rejected: %v", err)
	}
}

func TestLoadReadsSMTPPasswordFromEnvironment(t *testing.T) {
	t.Setenv("VANE_SMTP_ENABLED", "true")
	t.Setenv("VANE_DB_URL", "postgres://test-only")
	t.Setenv("VANE_SMTP_HOST", "smtp.example.com")
	t.Setenv("VANE_SMTP_PORT", "465")
	t.Setenv("VANE_SMTP_USERNAME", "mailer")
	t.Setenv("VANE_SMTP_PASSWORD", "environment-only-password")
	t.Setenv("VANE_SMTP_FROM", "noreply@example.com")
	t.Setenv("VANE_SMTP_TLS_MODE", "implicit_tls")
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.SMTP.Enabled || cfg.SMTP.Password != "environment-only-password" ||
		cfg.SMTP.Host != "smtp.example.com" || cfg.SMTP.Port != 465 {
		t.Fatalf("SMTP env binding mismatch: %+v", cfg.SMTP)
	}
}
