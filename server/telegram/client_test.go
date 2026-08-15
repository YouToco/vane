package telegram

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestClientSetWebhookUsesSecretAndMinimalUpdates(t *testing.T) {
	var body string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/setWebhook") {
			t.Fatalf("path=%q", r.URL.Path)
		}
		payload, _ := io.ReadAll(r.Body)
		body = string(payload)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"result":true}`))
	}))
	defer server.Close()
	client, err := NewClient("123:top_secret", server.URL,
		&http.Client{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.SetWebhook(t.Context(),
		"https://api.vane.test/telegram/webhook", "hook_secret"); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`"secret_token":"hook_secret"`, `"allowed_updates":["message","callback_query","my_chat_member"]`,
		`"drop_pending_updates":false`, `"max_connections":1`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("body=%s missing %s", body, want)
		}
	}
}

func TestClientInstallsMenuAndSendsIntoExactTopic(t *testing.T) {
	seen := map[string]string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload, _ := io.ReadAll(r.Body)
		seen[r.URL.Path] = string(payload)
		switch {
		case strings.HasSuffix(r.URL.Path, "/sendMessage"):
			_, _ = w.Write([]byte(`{"ok":true,"result":{"message_id":7,"chat":{"id":-1007}}}`))
		default:
			_, _ = w.Write([]byte(`{"ok":true,"result":true}`))
		}
	}))
	defer server.Close()
	client, err := NewClient("123:token", server.URL, http.DefaultClient)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.SetCommands(t.Context(), []BotCommand{{Command: "help", Description: "Help"}}); err != nil {
		t.Fatal(err)
	}
	if err := client.SetCommandsMenu(t.Context()); err != nil {
		t.Fatal(err)
	}
	messageID, err := client.SendMessageTo(t.Context(), "-1007", "hello",
		SendMessageOptions{MessageThreadID: 88, ReplyMarkup: &InlineKeyboardMarkup{
			InlineKeyboard: [][]InlineKeyboardButton{{{Text: "Tasks", CallbackData: "signed"}}},
		}})
	if err != nil || messageID != "7" {
		t.Fatalf("message=%q err=%v", messageID, err)
	}
	joined := ""
	for path, body := range seen {
		joined += path + body
	}
	for _, want := range []string{"/setMyCommands", "/setChatMenuButton",
		`"message_thread_id":88`, `"callback_data":"signed"`} {
		if !strings.Contains(joined, want) {
			t.Fatalf("requests=%s missing=%s", joined, want)
		}
	}
}

type failingTransport struct{}

func (failingTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("request failed at https://api.telegram.org/bot123:top_secret/sendMessage")
}

func TestClientTransportErrorNeverLeaksTokenURL(t *testing.T) {
	client, err := NewClient("123:top_secret", "https://api.telegram.org",
		&http.Client{Transport: failingTransport{}, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.SendMessage(context.Background(), "42", "hello")
	if err == nil || strings.Contains(err.Error(), "top_secret") ||
		strings.Contains(err.Error(), "api.telegram.org") {
		t.Fatalf("unsafe error=%v", err)
	}
	var deliveryErr *DeliveryError
	if !errors.As(err, &deliveryErr) || deliveryErr.DefinitelyNotSent {
		t.Fatalf("classification=%+v", deliveryErr)
	}
}

func TestClientClassifiesExplicit429AsDefinitelyNotSent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"ok":false,"error_code":429,"description":"retry","parameters":{"retry_after":7}}`))
	}))
	defer server.Close()
	client, err := NewClient("123:token", server.URL,
		&http.Client{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.SendMessage(t.Context(), "42", "hello")
	var deliveryErr *DeliveryError
	if !errors.As(err, &deliveryErr) || !deliveryErr.DefinitelyNotSent ||
		deliveryErr.Code != "rate_limited" || deliveryErr.RetryAfter != 7*time.Second {
		t.Fatalf("error=%+v", deliveryErr)
	}
}

func TestClientTreatsServerAndMalformedSuccessAsAmbiguous(t *testing.T) {
	for _, response := range []struct {
		status int
		body   string
	}{
		{status: 500, body: `{"ok":false,"error_code":500}`},
		{status: 200, body: `{"ok":true,"result":{"message_id":7,"chat":{"id":99}}}`},
	} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(response.status)
			_, _ = w.Write([]byte(response.body))
		}))
		client, err := NewClient("123:token", server.URL,
			&http.Client{Timeout: time.Second})
		if err != nil {
			server.Close()
			t.Fatal(err)
		}
		_, err = client.SendMessage(t.Context(), "42", "hello")
		server.Close()
		var deliveryErr *DeliveryError
		if !errors.As(err, &deliveryErr) || deliveryErr.DefinitelyNotSent {
			t.Fatalf("response=%+v error=%+v", response, deliveryErr)
		}
	}
}

func TestClientNeverFollowsTokenBearingRedirect(t *testing.T) {
	redirectHits := 0
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		redirectHits++
	}))
	defer target.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Redirect(w, &http.Request{}, target.URL+"/capture", http.StatusTemporaryRedirect)
	}))
	defer source.Close()
	client, err := NewClient("123:top_secret", source.URL,
		&http.Client{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.SendMessage(t.Context(), "42", "hello")
	var deliveryErr *DeliveryError
	if !errors.As(err, &deliveryErr) || deliveryErr.Code != "redirect_ambiguous" ||
		redirectHits != 0 {
		t.Fatalf("error=%+v redirect_hits=%d", deliveryErr, redirectHits)
	}
}

func TestClientConfigurationAndReadMethods(t *testing.T) {
	for _, tc := range []struct {
		token  string
		base   string
		client *http.Client
	}{
		{token: " bad", base: "https://api.telegram.org", client: http.DefaultClient},
		{token: "123:token", base: "http://example.com", client: http.DefaultClient},
		{token: "123:token", base: "https://api.telegram.org/path", client: http.DefaultClient},
		{token: "123:token", base: "https://api.telegram.org", client: nil},
	} {
		if _, err := NewClient(tc.token, tc.base, tc.client); err == nil {
			t.Fatalf("invalid client config accepted: %+v", tc)
		}
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/getMe"):
			_, _ = w.Write([]byte(`{"ok":true,"result":{"id":123,"username":"vane_bot"}}`))
		case strings.HasSuffix(r.URL.Path, "/getWebhookInfo"):
			_, _ = w.Write([]byte(`{"ok":true,"result":{"url":"https://vane.test/telegram/webhook","max_connections":1,"allowed_updates":["message","callback_query"]}}`))
		default:
			t.Fatalf("path=%s", r.URL.Path)
		}
	}))
	defer server.Close()
	client, err := NewClient("123:token", server.URL, http.DefaultClient)
	if err != nil {
		t.Fatal(err)
	}
	bot, err := client.GetMe(t.Context())
	if err != nil || bot.ID != 123 || bot.Username != "vane_bot" {
		t.Fatalf("bot=%+v err=%v", bot, err)
	}
	info, err := client.GetWebhookInfo(t.Context())
	if err != nil || info.MaxConnections != 1 || len(info.AllowedUpdates) != 2 {
		t.Fatalf("info=%+v err=%v", info, err)
	}
	if _, err := client.SendMessage(t.Context(), "", " "); err == nil {
		t.Fatal("empty message accepted")
	}
	if err := client.SetCommands(t.Context(), nil); err == nil {
		t.Fatal("empty command menu accepted")
	}
	if _, err := client.GetChatMember(t.Context(), "", 0); err == nil {
		t.Fatal("invalid chat member lookup accepted")
	}
	for _, callback := range []struct{ id, text string }{
		{}, {id: strings.Repeat("x", 129)}, {id: "cb", text: strings.Repeat("x", 201)},
	} {
		if err := client.AnswerCallbackQuery(t.Context(), callback.id, callback.text); err == nil {
			t.Fatalf("invalid callback accepted: %+v", callback)
		}
	}
	if got := sanitizeDeliveryCode(errors.New("boom")); !strings.HasPrefix(got, "internal_") {
		t.Fatalf("sanitized code=%s", got)
	}
}

func TestClientRejectsMalformedProviderPayloads(t *testing.T) {
	for _, tc := range []struct {
		name   string
		body   string
		method string
		want   string
	}{
		{name: "malformed envelope", body: `{`, method: "getMe", want: "malformed_response"},
		{name: "missing result", body: `{"ok":true}`, method: "getMe", want: "missing_result"},
		{name: "malformed result", body: `{"ok":true,"result":"bad"}`, method: "getMe", want: "malformed_result"},
		{name: "invalid bot identity", body: `{"ok":true,"result":{"id":0,"username":""}}`, method: "getMe", want: "malformed_get_me"},
		{name: "explicit reject", body: `{"ok":false,"error_code":400}`, method: "getMe", want: "rejected"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if tc.name == "explicit reject" {
					w.WriteHeader(http.StatusBadRequest)
				}
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			client, err := NewClient("123:token", server.URL, http.DefaultClient)
			if err != nil {
				t.Fatal(err)
			}
			_, err = client.GetMe(t.Context())
			var deliveryErr *DeliveryError
			if !errors.As(err, &deliveryErr) || deliveryErr.Code != tc.want {
				t.Fatalf("err=%+v", err)
			}
		})
	}
}
