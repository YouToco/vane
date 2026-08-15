package api

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/YouToco/vane/server/auth"
	"github.com/YouToco/vane/server/store"
	"github.com/YouToco/vane/server/telegram"
	"github.com/YouToco/vane/server/types"
)

const (
	telegramCredentialPurpose = "bot_api"
	feishuCredentialPurpose   = "app_credentials"
	llmCredentialPurpose      = "shared_runtime"
)

type credentialStatusResponse struct {
	Configured  bool            `json:"configured"`
	VaultReady  bool            `json:"vault_ready"`
	Generation  int64           `json:"generation,omitempty"`
	Fingerprint string          `json:"fingerprint,omitempty"`
	Metadata    json.RawMessage `json:"metadata,omitempty"`
	CreatedAt   *time.Time      `json:"created_at,omitempty"`
}

type telegramCredentialRequest struct {
	BotToken string `json:"bot_token"`
}

type feishuCredentialRequest struct {
	AppID     string `json:"app_id"`
	AppSecret string `json:"app_secret"`
}

type llmCredentialRequest struct {
	Provider      string `json:"provider"`
	BaseURL       string `json:"base_url"`
	APIKey        string `json:"api_key"`
	Model         string `json:"model"`
	AgentModel    string `json:"agent_model"`
	ResearchModel string `json:"research_model"`
	MaxConcurrent int    `json:"max_concurrent"`
}

func (s *server) handleTenantCredentialStatus(
	w http.ResponseWriter, r *http.Request, provider, purpose string,
) {
	principal, ok := s.requireTenantOwner(w, r)
	if !ok {
		return
	}
	s.writeCredentialStatus(w, r, store.CredentialScope{
		Kind: "tenant", TenantID: int64(principal.TenantID),
		Provider: provider, Purpose: purpose,
	}, principal.UserID)
}

func (s *server) handleTelegramCredentialStatus(w http.ResponseWriter, r *http.Request) {
	s.handleTenantCredentialStatus(w, r, "telegram", telegramCredentialPurpose)
}

func (s *server) handleFeishuCredentialStatus(w http.ResponseWriter, r *http.Request) {
	s.handleTenantCredentialStatus(w, r, "feishu", feishuCredentialPurpose)
}

func (s *server) handleLLMCredentialStatus(w http.ResponseWriter, r *http.Request) {
	if !s.requirePlatformOwner(w, r) {
		return
	}
	principal, err := auth.PrincipalFromContext(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}
	s.writeCredentialStatus(w, r, store.CredentialScope{
		Kind: "platform", Provider: "llm", Purpose: llmCredentialPurpose,
	}, principal.UserID)
}

func (s *server) writeCredentialStatus(
	w http.ResponseWriter, r *http.Request, scope store.CredentialScope, actor int64,
) {
	metadata, err := s.deps.Store.CredentialStatus(r.Context(), scope, actor)
	if errors.Is(err, types.ErrNotFound) {
		writeJSON(w, http.StatusOK, credentialStatusResponse{
			Configured: false, VaultReady: s.deps.Store.CredentialVaultReady(),
		})
		return
	}
	if err != nil {
		writeAppError(w, err)
		return
	}
	createdAt := metadata.CreatedAt
	writeJSON(w, http.StatusOK, credentialStatusResponse{
		Configured: true, VaultReady: s.deps.Store.CredentialVaultReady(),
		Generation: metadata.Generation, Fingerprint: metadata.Fingerprint,
		Metadata: metadata.Metadata, CreatedAt: &createdAt,
	})
}

func (s *server) handleTelegramCredentialPut(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.requireCredentialMutation(w, r, false)
	if !ok {
		return
	}
	var request telegramCredentialRequest
	if !decodeCredentialBody(w, r, &request) {
		return
	}
	request.BotToken = strings.TrimSpace(request.BotToken)
	client, err := telegram.NewClient(request.BotToken, "https://api.telegram.org",
		&http.Client{Timeout: 10 * time.Second})
	if err != nil {
		writeError(w, http.StatusBadRequest, "Telegram Bot token 格式无效")
		return
	}
	verifyCtx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	bot, err := client.GetMe(verifyCtx)
	if err != nil {
		writeError(w, http.StatusBadRequest, "Telegram Bot token 校验失败")
		return
	}
	webhookSecretBytes := make([]byte, 32)
	if _, err := rand.Read(webhookSecretBytes); err != nil {
		writeError(w, http.StatusInternalServerError, "生成 Webhook 密钥失败")
		return
	}
	secret, _ := json.Marshal(map[string]string{
		"bot_token":      request.BotToken,
		"webhook_secret": base64.RawURLEncoding.EncodeToString(webhookSecretBytes),
	})
	metadata, _ := json.Marshal(map[string]any{
		"bot_id": bot.ID, "bot_username": bot.Username,
	})
	rotated, err := s.deps.Store.RotateCredential(r.Context(), store.CredentialScope{
		Kind: "tenant", TenantID: int64(principal.TenantID),
		Provider: "telegram", Purpose: telegramCredentialPurpose,
	}, secret, metadata, principal.UserID)
	if err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"configured": true, "generation": rotated.Generation,
		"fingerprint": rotated.Fingerprint, "metadata": rotated.Metadata,
		"activation": "manager_fleet_pending",
	})
}

func (s *server) handleFeishuCredentialPut(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.requireCredentialMutation(w, r, false)
	if !ok {
		return
	}
	var request feishuCredentialRequest
	if !decodeCredentialBody(w, r, &request) {
		return
	}
	request.AppID = strings.TrimSpace(request.AppID)
	request.AppSecret = strings.TrimSpace(request.AppSecret)
	if request.AppID == "" || request.AppSecret == "" {
		writeError(w, http.StatusBadRequest, "app_id 与 app_secret 均不能为空")
		return
	}
	verify := s.deps.Manager.Verify(r.Context(), request.AppID, request.AppSecret)
	if !verify.CredentialsOK {
		writeError(w, http.StatusBadRequest, "飞书凭证校验失败")
		return
	}
	secret, _ := json.Marshal(request)
	metadata, _ := json.Marshal(map[string]string{"app_id": request.AppID})
	rotated, err := s.deps.Store.RotateCredential(r.Context(), store.CredentialScope{
		Kind: "tenant", TenantID: int64(principal.TenantID),
		Provider: "feishu", Purpose: feishuCredentialPurpose,
	}, secret, metadata, principal.UserID)
	if err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"configured": true, "generation": rotated.Generation,
		"fingerprint": rotated.Fingerprint, "metadata": rotated.Metadata,
		"activation": "manager_fleet_pending",
	})
}

func (s *server) handleLLMCredentialPut(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.requireCredentialMutation(w, r, true)
	if !ok {
		return
	}
	var request llmCredentialRequest
	if !decodeCredentialBody(w, r, &request) {
		return
	}
	request.Provider = strings.TrimSpace(request.Provider)
	request.BaseURL = strings.TrimRight(strings.TrimSpace(request.BaseURL), "/")
	request.APIKey = strings.TrimSpace(request.APIKey)
	request.Model = strings.TrimSpace(request.Model)
	request.AgentModel = strings.TrimSpace(request.AgentModel)
	request.ResearchModel = strings.TrimSpace(request.ResearchModel)
	parsedURL, err := url.Parse(request.BaseURL)
	if request.Provider != "deepseek" || request.APIKey == "" || request.Model == "" ||
		request.AgentModel == "" || request.ResearchModel == "" ||
		request.MaxConcurrent < 1 || request.MaxConcurrent > 128 || err != nil ||
		parsedURL.Scheme != "https" || parsedURL.Host == "" || parsedURL.User != nil ||
		parsedURL.RawQuery != "" || parsedURL.Fragment != "" {
		writeError(w, http.StatusBadRequest, "LLM provider、HTTPS 地址、模型或并发配置无效")
		return
	}
	secret, _ := json.Marshal(map[string]string{"api_key": request.APIKey})
	metadata, _ := json.Marshal(map[string]any{
		"provider": request.Provider, "base_url": request.BaseURL,
		"model": request.Model, "agent_model": request.AgentModel,
		"research_model": request.ResearchModel,
		"max_concurrent": request.MaxConcurrent,
	})
	rotated, err := s.deps.Store.RotateCredential(r.Context(), store.CredentialScope{
		Kind: "platform", Provider: "llm", Purpose: llmCredentialPurpose,
	}, secret, metadata, principal.UserID)
	if err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"configured": true, "generation": rotated.Generation,
		"fingerprint": rotated.Fingerprint, "metadata": rotated.Metadata,
		"activation": "restart_required",
	})
}

func (s *server) handleTelegramCredentialDelete(w http.ResponseWriter, r *http.Request) {
	s.handleTenantCredentialDelete(w, r, "telegram", telegramCredentialPurpose)
}

func (s *server) handleFeishuCredentialDelete(w http.ResponseWriter, r *http.Request) {
	s.handleTenantCredentialDelete(w, r, "feishu", feishuCredentialPurpose)
}

func (s *server) handleTenantCredentialDelete(
	w http.ResponseWriter, r *http.Request, provider, purpose string,
) {
	principal, ok := s.requireCredentialMutation(w, r, false)
	if !ok {
		return
	}
	if err := s.deps.Store.RevokeCredential(r.Context(), store.CredentialScope{
		Kind: "tenant", TenantID: int64(principal.TenantID),
		Provider: provider, Purpose: purpose,
	}, principal.UserID); err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *server) handleLLMCredentialDelete(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.requireCredentialMutation(w, r, true)
	if !ok {
		return
	}
	if err := s.deps.Store.RevokeCredential(r.Context(), store.CredentialScope{
		Kind: "platform", Provider: "llm", Purpose: llmCredentialPurpose,
	}, principal.UserID); err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *server) requireCredentialMutation(
	w http.ResponseWriter, r *http.Request, platform bool,
) (auth.Principal, bool) {
	if !s.checkOrigin(w, r) {
		return auth.Principal{}, false
	}
	if platform {
		if !s.requirePlatformOwner(w, r) {
			return auth.Principal{}, false
		}
		principal, err := auth.PrincipalFromContext(r.Context())
		if err != nil {
			writeAppError(w, err)
			return auth.Principal{}, false
		}
		return principal, true
	}
	return s.requireTenantOwner(w, r)
}

func decodeCredentialBody(w http.ResponseWriter, r *http.Request, target any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		writeError(w, http.StatusBadRequest, "凭证请求体不是合法 JSON")
		return false
	}
	return true
}
