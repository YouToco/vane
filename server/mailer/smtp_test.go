package mailer

import (
	"strings"
	"testing"
)

func validConfig() Config {
	return Config{
		Host: "smtp.example.com", Port: 465,
		Username: "mailer", Password: "not-a-real-secret",
		From: "Vane <no-reply@example.com>", TLSMode: TLSModeImplicit,
	}
}

func TestNewSMTPRequiresEncryptedTransportAndCredentials(t *testing.T) {
	for _, mutate := range []func(*Config){
		func(cfg *Config) { cfg.Host = "" },
		func(cfg *Config) { cfg.Port = 0 },
		func(cfg *Config) { cfg.Username = "" },
		func(cfg *Config) { cfg.Password = "" },
		func(cfg *Config) { cfg.TLSMode = "plain" },
		func(cfg *Config) { cfg.From = "bad\r\nBcc: victim@example.com" },
	} {
		cfg := validConfig()
		mutate(&cfg)
		if _, err := NewSMTP(cfg); err == nil {
			t.Fatalf("unsafe SMTP config accepted: %+v", cfg)
		}
	}
	for _, mode := range []TLSMode{TLSModeImplicit, TLSModeSTARTTLS} {
		cfg := validConfig()
		cfg.TLSMode = mode
		sender, err := NewSMTP(cfg)
		if err != nil || sender.tlsConfig().MinVersion == 0 || sender.tlsConfig().ServerName != cfg.Host {
			t.Fatalf("TLS config mode=%s sender=%+v err=%v", mode, sender, err)
		}
	}
}

func TestMessageValidationRejectsHeaderInjectionBeforeNetwork(t *testing.T) {
	sender, err := NewSMTP(validConfig())
	if err != nil {
		t.Fatal(err)
	}
	for _, message := range []Message{
		{To: "victim@example.com\r\nBcc: other@example.com", Subject: "verify", Text: "body"},
		{To: "victim@example.com", Subject: "verify\r\nBcc: other@example.com", Text: "body"},
		{To: "victim@example.com", Subject: "", Text: "body"},
		{To: "victim@example.com", Subject: "verify", Text: ""},
	} {
		if err := sender.Send(t.Context(), message); err == nil || strings.Contains(err.Error(), validConfig().Password) {
			t.Fatalf("message validation err=%v", err)
		}
	}
}

func TestRenderMessageUsesCanonicalPlainTextHeaders(t *testing.T) {
	rendered := renderMessage("no-reply@example.com", "user@example.com", "验证邮箱", "第一行\n第二行")
	for _, want := range []string{
		"From: no-reply@example.com\r\n",
		"To: user@example.com\r\n",
		"Subject: 验证邮箱\r\n",
		"Content-Type: text/plain; charset=UTF-8\r\n",
		"第一行\r\n第二行\r\n",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("rendered message missing %q: %q", want, rendered)
		}
	}
}
