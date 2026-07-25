package feishu

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"

	"github.com/YouToco/vane/types"
)

type pushTestLogger struct {
	mu      sync.Mutex
	entries []string
}

func (l *pushTestLogger) Debug(_ context.Context, args ...interface{}) {
	l.append(args...)
}

func (l *pushTestLogger) Info(_ context.Context, args ...interface{}) {
	l.append(args...)
}

func (l *pushTestLogger) Warn(_ context.Context, args ...interface{}) {
	l.append(args...)
}

func (l *pushTestLogger) Error(_ context.Context, args ...interface{}) {
	l.append(args...)
}

func (l *pushTestLogger) append(args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, fmt.Sprint(args...))
}

func (l *pushTestLogger) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Join(l.entries, "\n")
}

func newPushTestManager(
	t *testing.T,
	handler http.Handler,
	logger larkcore.Logger,
) *Manager {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	options := []lark.ClientOptionFunc{
		lark.WithOpenBaseUrl(server.URL),
		lark.WithOAuthBaseUrl(server.URL),
		lark.WithHttpClient(server.Client()),
	}
	if logger != nil {
		options = append(options, lark.WithLogger(logger))
	}
	client := lark.NewClient("test-app", "test-secret", options...)
	m := NewManager(nil, nil, nil)
	m.mu.Lock()
	m.apiClient = client
	m.apiAppID = "test-app"
	m.mu.Unlock()
	return m
}

func servePushTestToken(w http.ResponseWriter, r *http.Request) bool {
	w.Header().Set("Content-Type", "application/json")
	switch r.URL.Path {
	case "/oauth/v3/token":
		_, _ = io.WriteString(
			w,
			`{"access_token":"test-token","token_type":"Bearer","expires_in":7200}`,
		)
		return true
	case "/open-apis/auth/v3/tenant_access_token/internal":
		_, _ = io.WriteString(
			w,
			`{"code":0,"tenant_access_token":"test-token","expire":7200}`,
		)
		return true
	default:
		return false
	}
}

func TestManager_SendCardCompatibilityOmitsUUID(t *testing.T) {
	var createCalls int
	m := newPushTestManager(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if servePushTestToken(w, r) {
			return
		}
		createCalls++
		var body map[string]json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode request body: %v", err)
		}
		if _, ok := body["uuid"]; ok {
			t.Fatal("legacy SendCard request unexpectedly contains uuid")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(
			w,
			`{"code":0,"msg":"success","data":{"message_id":"om_legacy"}}`,
		)
	}), nil)

	messageID, err := m.SendCard(t.Context(), "ou_owner", `{"type":"card"}`)
	if err != nil {
		t.Fatalf("SendCard() error = %v", err)
	}
	if messageID != "om_legacy" {
		t.Fatalf("message id = %q, want om_legacy", messageID)
	}
	if createCalls != 1 {
		t.Fatalf("create calls = %d, want 1", createCalls)
	}
}

func TestManager_SendCardWithUUIDForwardsStableUUID(t *testing.T) {
	const (
		messageUUID = "019f9824-39b6-7e13-b247-b5ee5713c52b"
		openID      = "ou_owner"
		cardJSON    = `{"type":"card"}`
	)
	gotUUIDs := make([]string, 0, 2)
	m := newPushTestManager(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if servePushTestToken(w, r) {
			return
		}
		if r.Method != http.MethodPost ||
			r.URL.EscapedPath() != "/open-apis/im/v1/messages" {
			t.Fatalf("request = %s %s, want POST /open-apis/im/v1/messages",
				r.Method, r.URL.EscapedPath())
		}
		var body struct {
			ReceiveID string `json:"receive_id"`
			MsgType   string `json:"msg_type"`
			Content   string `json:"content"`
			UUID      string `json:"uuid"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode request body: %v", err)
		}
		if body.ReceiveID != openID ||
			body.MsgType != "interactive" ||
			body.Content != cardJSON {
			t.Fatalf("unexpected request body metadata: %+v", body)
		}
		gotUUIDs = append(gotUUIDs, body.UUID)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(
			w,
			`{"code":0,"msg":"success","data":{"message_id":"om_stable"}}`,
		)
	}), nil)

	for range 2 {
		messageID, err := m.SendCardWithUUID(
			t.Context(),
			openID,
			cardJSON,
			messageUUID,
		)
		if err != nil {
			t.Fatalf("SendCardWithUUID() error = %v", err)
		}
		if messageID != "om_stable" {
			t.Fatalf("message id = %q, want om_stable", messageID)
		}
	}
	if len(gotUUIDs) != 2 ||
		gotUUIDs[0] != messageUUID ||
		gotUUIDs[1] != messageUUID {
		t.Fatalf("request uuids = %v, want exact stable uuid twice", gotUUIDs)
	}
}

func TestManager_SendCardWithUUIDRejectsInvalidUUIDBeforeNetwork(t *testing.T) {
	var networkCalls int
	m := newPushTestManager(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		networkCalls++
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"code":0,"msg":"success"}`)
	}), nil)

	tests := []struct {
		name        string
		messageUUID string
	}{
		{name: "empty", messageUUID: ""},
		{name: "too short", messageUUID: "019f9824"},
		{
			name:        "too long",
			messageUUID: "019f9824-39b6-7e13-b247-b5ee5713c52b-extra",
		},
		{
			name:        "noncanonical uppercase",
			messageUUID: "019F9824-39B6-7E13-B247-B5EE5713C52B",
		},
		{
			name:        "unsafe characters",
			messageUUID: "019f9824-39b6-7e13-b247-b5ee5713c52!",
		},
		{
			name:        "zero uuid",
			messageUUID: "00000000-0000-0000-0000-000000000000",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := m.SendCardWithUUID(
				t.Context(),
				"ou_sensitive",
				`{"secret":"card"}`,
				tt.messageUUID,
			)
			if !errors.Is(err, types.ErrValidation) || types.IsRetryable(err) {
				t.Fatalf("error = %v, want non-retryable validation error", err)
			}
		})
	}
	if networkCalls != 0 {
		t.Fatalf("invalid uuids reached provider network: calls=%d", networkCalls)
	}
}

func TestManager_SendCardWithUUIDTransportErrorIsRetryableAndSanitized(t *testing.T) {
	const (
		messageUUID = "019f9824-39b6-7e13-b247-b5ee5713c52b"
		openID      = "ou_sensitive_owner"
		cardJSON    = `{"secret":"sensitive card"}`
	)
	logger := &pushTestLogger{}
	m := newPushTestManager(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if servePushTestToken(w, r) {
			return
		}
		http.Error(w, "temporary failure", http.StatusServiceUnavailable)
	}), logger)

	_, err := m.SendCardWithUUID(t.Context(), openID, cardJSON, messageUUID)
	if !errors.Is(err, types.ErrPush) || !types.IsRetryable(err) {
		t.Fatalf("error = %v, want retryable push error", err)
	}
	observed := err.Error() + "\n" + logger.String()
	for _, secret := range []string{openID, cardJSON, messageUUID} {
		if strings.Contains(observed, secret) {
			t.Fatalf("error or log output contains sensitive send material %q", secret)
		}
	}
}

func TestManager_SendCardWithUUIDPreservesProviderErrorClassification(t *testing.T) {
	const messageUUID = "019f9824-39b6-7e13-b247-b5ee5713c52b"
	tests := []struct {
		name      string
		code      int
		retryable bool
	}{
		{name: "permanent malformed card", code: 200673, retryable: false},
		{name: "unknown provider failure", code: 987654, retryable: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := newPushTestManager(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if servePushTestToken(w, r) {
					return
				}
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprintf(w, `{"code":%d,"msg":"test rejection"}`, tt.code)
			}), nil)

			_, err := m.SendCardWithUUID(
				t.Context(),
				"ou_owner",
				`{"type":"card"}`,
				messageUUID,
			)
			if !errors.Is(err, types.ErrPush) {
				t.Fatalf("error = %v, want push error", err)
			}
			if got := types.IsRetryable(err); got != tt.retryable {
				t.Fatalf("retryable = %v, want %v", got, tt.retryable)
			}
		})
	}
}

func TestManager_SendCardRejectsSuccessWithoutMessageID(t *testing.T) {
	const (
		messageUUID = "019f9824-39b6-7e13-b247-b5ee5713c52b"
		openID      = "ou_sensitive_owner"
		cardJSON    = `{"secret":"sensitive card"}`
	)
	responses := []struct {
		name string
		body string
	}{
		{
			name: "missing data",
			body: `{"code":0,"msg":"success"}`,
		},
		{
			name: "missing message id",
			body: `{"code":0,"msg":"success","data":{}}`,
		},
		{
			name: "empty message id",
			body: `{"code":0,"msg":"success","data":{"message_id":""}}`,
		},
	}
	sendPaths := []struct {
		name              string
		expectedRetryable bool
		send              func(context.Context, *Manager) (string, error)
	}{
		{
			name:              "legacy send",
			expectedRetryable: false,
			send: func(ctx context.Context, m *Manager) (string, error) {
				return m.SendCard(ctx, openID, cardJSON)
			},
		},
		{
			name:              "stable uuid send",
			expectedRetryable: true,
			send: func(ctx context.Context, m *Manager) (string, error) {
				return m.SendCardWithUUID(ctx, openID, cardJSON, messageUUID)
			},
		},
	}

	for _, response := range responses {
		t.Run(response.name, func(t *testing.T) {
			for _, sendPath := range sendPaths {
				t.Run(sendPath.name, func(t *testing.T) {
					logger := &pushTestLogger{}
					m := newPushTestManager(
						t,
						http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
							if servePushTestToken(w, r) {
								return
							}
							w.Header().Set("Content-Type", "application/json")
							_, _ = io.WriteString(w, response.body)
						}),
						logger,
					)

					messageID, err := sendPath.send(t.Context(), m)
					if messageID != "" {
						t.Fatalf("message id = %q, want empty", messageID)
					}
					if !errors.Is(err, types.ErrPush) {
						t.Fatalf("error = %v, want push error", err)
					}
					if got := types.IsRetryable(err); got != sendPath.expectedRetryable {
						t.Fatalf(
							"retryable = %v, want %v",
							got,
							sendPath.expectedRetryable,
						)
					}
					observed := err.Error() + "\n" + logger.String()
					for _, secret := range []string{openID, cardJSON, messageUUID} {
						if strings.Contains(observed, secret) {
							t.Fatalf(
								"error or log output contains sensitive send material %q",
								secret,
							)
						}
					}
				})
			}
		})
	}
}
