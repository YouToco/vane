package feishu

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	lark "github.com/larksuite/oapi-sdk-go/v3"

	"github.com/YouToco/vane/task"
	"github.com/YouToco/vane/types"
)

var creationReceiptTestProvider = task.FeishuCardPatchReceiptProviderForApp("test-app")

func newCreationReceiptTestManager(t *testing.T, handler http.Handler, httpClient *http.Client) *Manager {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	if httpClient == nil {
		httpClient = server.Client()
	}
	client := lark.NewClient("test-app", "test-secret",
		lark.WithOpenBaseUrl(server.URL),
		lark.WithOAuthBaseUrl(server.URL),
		lark.WithHttpClient(httpClient),
	)
	m := NewManager(nil, nil, nil)
	m.mu.Lock()
	m.apiClient = client
	m.apiAppID = "test-app"
	m.mu.Unlock()
	return m
}

func serveCreationReceiptToken(w http.ResponseWriter, r *http.Request) bool {
	w.Header().Set("Content-Type", "application/json")
	switch r.URL.Path {
	case "/oauth/v3/token":
		_, _ = io.WriteString(w, `{"access_token":"test-token","token_type":"Bearer","expires_in":7200}`)
		return true
	case "/open-apis/auth/v3/tenant_access_token/internal":
		_, _ = io.WriteString(w, `{"code":0,"tenant_access_token":"test-token","expire":7200}`)
		return true
	default:
		return false
	}
}

func TestSendCreationReceiptPatchRequestAndSuccessWithoutData(t *testing.T) {
	const (
		messageID = "om_creation_receipt"
		cardJSON  = `{"schema":"2.0","config":{"update_multi":true}}`
	)
	var patchCalls int
	m := newCreationReceiptTestManager(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if serveCreationReceiptToken(w, r) {
			return
		}
		patchCalls++
		if r.Method != http.MethodPatch {
			t.Errorf("method = %s, want PATCH", r.Method)
		}
		if got, want := r.URL.EscapedPath(), "/open-apis/im/v1/messages/"+messageID; got != want {
			t.Errorf("path = %q, want %q", got, want)
		}
		var body map[string]json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode patch body: %v", err)
		}
		if len(body) != 1 {
			t.Errorf("patch body keys = %v, want content only", body)
		}
		var gotContent string
		if err := json.Unmarshal(body["content"], &gotContent); err != nil {
			t.Fatalf("decode content: %v", err)
		}
		if gotContent != cardJSON {
			t.Errorf("content = %q, want %q", gotContent, cardJSON)
		}
		w.Header().Set("Content-Type", "application/json")
		// PatchMessage 成功响应按协议没有 data；不能把 data=nil 误判为失败。
		_, _ = io.WriteString(w, `{"code":0,"msg":"success"}`)
	}), nil)

	if err := m.SendCreationReceipt(t.Context(), creationReceiptTestProvider, messageID, cardJSON); err != nil {
		t.Fatalf("SendCreationReceipt() = %v, want nil", err)
	}
	if patchCalls != 1 {
		t.Fatalf("patch calls = %d, want 1", patchCalls)
	}
}

func TestSendCreationReceiptTimeoutRetryOverwritesSameResource(t *testing.T) {
	const (
		messageID = "om_timeout_then_retry"
		cardJSON  = `{"schema":"2.0","config":{"update_multi":true},"body":{"elements":[]}}`
	)
	var (
		mu         sync.Mutex
		patchCalls int
		resources  = make(map[string]string)
	)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if serveCreationReceiptToken(w, r) {
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
			return
		}
		var payload struct {
			Content string `json:"content"`
		}
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Errorf("decode body: %v", err)
			return
		}

		mu.Lock()
		patchCalls++
		call := patchCalls
		resources[r.URL.Path] = payload.Content
		mu.Unlock()

		if call == 1 {
			// 先提交资源更新，再让客户端等不到响应：复现“远端成功、响应丢失”。
			time.Sleep(80 * time.Millisecond)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"code":0,"msg":"success"}`)
	})
	client := &http.Client{Timeout: 25 * time.Millisecond}
	m := newCreationReceiptTestManager(t, handler, client)

	err := m.SendCreationReceipt(context.Background(), creationReceiptTestProvider, messageID, cardJSON)
	if err == nil || !errors.Is(err, types.ErrPush) || !types.IsRetryable(err) {
		t.Fatalf("first SendCreationReceipt() = %v, want retryable push error", err)
	}
	if err := m.SendCreationReceipt(context.Background(), creationReceiptTestProvider, messageID, cardJSON); err != nil {
		t.Fatalf("retry SendCreationReceipt() = %v, want nil", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if patchCalls != 2 {
		t.Fatalf("patch calls = %d, want 2", patchCalls)
	}
	if len(resources) != 1 {
		t.Fatalf("distinct resources = %d (%v), want exactly 1", len(resources), resources)
	}
	wantPath := "/open-apis/im/v1/messages/" + messageID
	if got := resources[wantPath]; got != cardJSON {
		t.Fatalf("resource[%q] = %q, want frozen content %q", wantPath, got, cardJSON)
	}
}

func TestSendCreationReceiptValidationAndUnavailableClient(t *testing.T) {
	m := NewManager(nil, nil, nil)
	if err := m.SendCreationReceipt(t.Context(), "feishu_card_patch:bad", "om_1", `{}`); !errors.Is(err, types.ErrValidation) || types.IsRetryable(err) {
		t.Fatalf("malformed provider error = %v, want non-retryable validation", err)
	}
	for _, tc := range []struct {
		name      string
		messageID string
		cardJSON  string
	}{
		{"empty message id", "", `{}`},
		{"empty card", "om_1", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := m.SendCreationReceipt(t.Context(), creationReceiptTestProvider, tc.messageID, tc.cardJSON)
			if !errors.Is(err, types.ErrValidation) || types.IsRetryable(err) {
				t.Fatalf("error = %v, want non-retryable validation", err)
			}
		})
	}

	err := m.SendCreationReceipt(t.Context(), creationReceiptTestProvider, "om_1", `{}`)
	if !errors.Is(err, types.ErrPush) || !types.IsRetryable(err) {
		t.Fatalf("nil api error = %v, want retryable push error", err)
	}
}

func TestSendCreationReceiptRejectsCrossAppIdentityBeforeNetwork(t *testing.T) {
	var calls int
	m := newCreationReceiptTestManager(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"code":0,"msg":"success"}`)
	}), nil)

	otherProvider := task.FeishuCardPatchReceiptProviderForApp("different-app")
	err := m.SendCreationReceipt(t.Context(), otherProvider, "om_other_app", `{}`)
	if !errors.Is(err, types.ErrPush) || !types.IsRetryable(err) {
		t.Fatalf("cross-app error = %v, want retryable push refusal", err)
	}
	if calls != 0 {
		t.Fatalf("cross-app receipt reached provider network: calls=%d", calls)
	}
}

func TestSendCreationReceiptErrorClassification(t *testing.T) {
	for _, tc := range []struct {
		code      int
		retryable bool
	}{
		{230001, false},
		{230006, true}, // bot 能力未启用，发布配置后可恢复。
		{230011, false},
		{230025, false},
		{230027, true}, // 权限缺失，补权限后可恢复。
		{230028, false},
		{230031, false},
		{230099, false},
		{230020, true},
		{99991400, true},
		{987654, true}, // 未知码保守重试。
	} {
		t.Run(fmt.Sprintf("code_%d", tc.code), func(t *testing.T) {
			m := newCreationReceiptTestManager(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if serveCreationReceiptToken(w, r) {
					return
				}
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprintf(w, `{"code":%d,"msg":"test rejection"}`, tc.code)
			}), nil)

			err := m.SendCreationReceipt(t.Context(), creationReceiptTestProvider, "om_classify", `{}`)
			if err == nil || !errors.Is(err, types.ErrPush) {
				t.Fatalf("error = %v, want push error", err)
			}
			if got := types.IsRetryable(err); got != tc.retryable {
				t.Fatalf("retryable = %v, want %v (err=%v)", got, tc.retryable, err)
			}
		})
	}
}
