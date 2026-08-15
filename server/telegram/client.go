// Package telegram implements Vane's authenticated Telegram Bot adapter.
// It intentionally uses the Bot HTTP API directly: the transport surface is
// small, explicit, and keeps the bot token out of third-party logging hooks.
package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

const (
	defaultAPIBaseURL = "https://api.telegram.org"
	maxAPIResponse    = 1 << 20
)

type Bot struct {
	ID       int64  `json:"id"`
	Username string `json:"username"`
}

type WebhookInfo struct {
	URL                string   `json:"url"`
	PendingUpdateCount int      `json:"pending_update_count"`
	LastErrorDate      int64    `json:"last_error_date"`
	LastErrorMessage   string   `json:"last_error_message"`
	MaxConnections     int      `json:"max_connections"`
	AllowedUpdates     []string `json:"allowed_updates"`
	IP                 string   `json:"ip_address"`
}

type User struct {
	ID       int64  `json:"id"`
	Username string `json:"username"`
}

type Chat struct {
	ID   int64  `json:"id"`
	Type string `json:"type"`
}

type Message struct {
	MessageID int64  `json:"message_id"`
	From      *User  `json:"from"`
	Chat      Chat   `json:"chat"`
	Text      string `json:"text"`
}

type Update struct {
	UpdateID int64    `json:"update_id"`
	Message  *Message `json:"message"`
}

// DeliveryError classifies whether the provider definitely rejected a send.
// Network errors, timeouts, 5xx and malformed success responses are ambiguous:
// callers must not blindly retry them because Telegram has no caller-provided
// idempotency key or arbitrary message-history recovery API.
type DeliveryError struct {
	Code              string
	DefinitelyNotSent bool
	HTTPStatus        int
}

func (e *DeliveryError) Error() string {
	return "telegram provider error: " + e.Code
}

type apiEnvelope struct {
	OK          bool            `json:"ok"`
	Result      json.RawMessage `json:"result"`
	ErrorCode   int             `json:"error_code"`
	Description string          `json:"description"`
	Parameters  struct {
		RetryAfter int `json:"retry_after"`
	} `json:"parameters"`
}

type Client struct {
	token   string
	baseURL string
	http    *http.Client
}

func NewClient(token, baseURL string, httpClient *http.Client) (*Client, error) {
	if token == "" || strings.TrimSpace(token) != token ||
		strings.ContainsAny(token, "\r\n/?#") {
		return nil, errors.New("telegram: bot token format is invalid")
	}
	if baseURL == "" {
		baseURL = defaultAPIBaseURL
	}
	parsed, err := url.Parse(baseURL)
	if err != nil || parsed.Host == "" || parsed.Path != "" ||
		(parsed.Scheme != "https" &&
			!(parsed.Scheme == "http" &&
				(parsed.Hostname() == "127.0.0.1" || parsed.Hostname() == "localhost"))) {
		return nil, errors.New("telegram: Bot API base URL is invalid")
	}
	if httpClient == nil {
		return nil, errors.New("telegram: HTTP client is required")
	}
	isolatedClient := *httpClient
	isolatedClient.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &Client{
		token: token, baseURL: strings.TrimRight(baseURL, "/"), http: &isolatedClient,
	}, nil
}

func (c *Client) GetMe(ctx context.Context) (Bot, error) {
	var bot Bot
	if err := c.call(ctx, "getMe", struct{}{}, &bot); err != nil {
		return Bot{}, err
	}
	if bot.ID <= 0 || strings.TrimSpace(bot.Username) == "" {
		return Bot{}, &DeliveryError{Code: "malformed_get_me"}
	}
	return bot, nil
}

func (c *Client) SetWebhook(
	ctx context.Context, webhookURL, secret string,
) error {
	payload := struct {
		URL                string   `json:"url"`
		SecretToken        string   `json:"secret_token"`
		AllowedUpdates     []string `json:"allowed_updates"`
		DropPendingUpdates bool     `json:"drop_pending_updates"`
		MaxConnections     int      `json:"max_connections"`
	}{
		URL: webhookURL, SecretToken: secret,
		AllowedUpdates: []string{"message"}, DropPendingUpdates: false,
		MaxConnections: 1,
	}
	return c.call(ctx, "setWebhook", payload, nil)
}

func (c *Client) GetWebhookInfo(ctx context.Context) (WebhookInfo, error) {
	var info WebhookInfo
	if err := c.call(ctx, "getWebhookInfo", struct{}{}, &info); err != nil {
		return WebhookInfo{}, err
	}
	return info, nil
}

func (c *Client) SendMessage(
	ctx context.Context, chatID, text string,
) (string, error) {
	if chatID == "" || strings.TrimSpace(text) == "" {
		return "", &DeliveryError{
			Code: "invalid_send_message", DefinitelyNotSent: true,
		}
	}
	payload := struct {
		ChatID string `json:"chat_id"`
		Text   string `json:"text"`
	}{ChatID: chatID, Text: text}
	var message Message
	if err := c.call(ctx, "sendMessage", payload, &message); err != nil {
		return "", err
	}
	if message.MessageID <= 0 || strconv.FormatInt(message.Chat.ID, 10) != chatID {
		return "", &DeliveryError{Code: "malformed_send_response"}
	}
	return strconv.FormatInt(message.MessageID, 10), nil
}

func (c *Client) call(
	ctx context.Context, method string, payload any, result any,
) error {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return &DeliveryError{
			Code: "encode_request", DefinitelyNotSent: true,
		}
	}
	endpoint := c.baseURL + "/bot" + c.token + "/" + method
	req, err := http.NewRequestWithContext(
		ctx, http.MethodPost, endpoint, bytes.NewReader(encoded))
	if err != nil {
		return &DeliveryError{
			Code: "build_request", DefinitelyNotSent: true,
		}
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		// Deliberately do not wrap url.Error: it contains the token-bearing URL.
		return &DeliveryError{Code: "transport_ambiguous"}
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxAPIResponse+1))
	if err != nil || len(body) > maxAPIResponse {
		return &DeliveryError{Code: "response_ambiguous"}
	}
	if resp.StatusCode >= 500 {
		return &DeliveryError{Code: "server_ambiguous"}
	}
	if resp.StatusCode >= 300 && resp.StatusCode < 400 {
		return &DeliveryError{Code: "redirect_ambiguous"}
	}
	var envelope apiEnvelope
	decoded := json.Unmarshal(body, &envelope) == nil
	if resp.StatusCode >= 400 && resp.StatusCode < 500 {
		code := "rejected"
		if resp.StatusCode == http.StatusTooManyRequests ||
			(decoded && envelope.ErrorCode == 429) {
			code = "rate_limited"
		}
		return &DeliveryError{
			Code: code, DefinitelyNotSent: true, HTTPStatus: resp.StatusCode,
		}
	}
	if !decoded {
		return &DeliveryError{Code: "malformed_response"}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 || !envelope.OK {
		code := "rejected"
		if resp.StatusCode == http.StatusTooManyRequests || envelope.ErrorCode == 429 {
			code = "rate_limited"
		}
		return &DeliveryError{
			Code: code, DefinitelyNotSent: true, HTTPStatus: resp.StatusCode,
		}
	}
	if result == nil {
		return nil
	}
	if len(envelope.Result) == 0 || bytes.Equal(envelope.Result, []byte("null")) {
		return &DeliveryError{Code: "missing_result"}
	}
	if err := json.Unmarshal(envelope.Result, result); err != nil {
		return &DeliveryError{Code: "malformed_result"}
	}
	return nil
}

func sanitizeDeliveryCode(err error) string {
	var providerErr *DeliveryError
	if errors.As(err, &providerErr) {
		return providerErr.Code
	}
	return fmt.Sprintf("internal_%T", err)
}
