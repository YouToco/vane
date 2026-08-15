package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"
	"unicode"

	"github.com/YouToco/vane/server/auth"
	"github.com/YouToco/vane/server/internal/credentialguard"
	"github.com/YouToco/vane/server/types"
)

const a2aTokenBodyLimit = 8 << 10

func (s *server) handleIssueA2AAccessToken(w http.ResponseWriter, r *http.Request) {
	p, err := auth.PrincipalFromContext(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}
	if p.ActorType != types.ActorTypeUser {
		writeError(w, http.StatusForbidden, "只有交互式用户可以签发 A2A token")
		return
	}
	st := s.a2aAccessStore()
	if st == nil {
		writeError(w, http.StatusServiceUnavailable, "A2A token 管理尚未启用")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, a2aTokenBodyLimit)
	var request struct {
		ActorType           types.ActorType  `json:"actor_type"`
		PrincipalUserID     int64            `json:"principal_user_id"`
		ServiceAccountLabel string           `json:"service_account_label"`
		Scopes              []types.A2AScope `json:"scopes"`
		ExpiresInDays       int              `json:"expires_in_days"`
	}
	if !decodeA2ATokenJSON(r, &request) {
		writeError(w, http.StatusBadRequest, "请求体不是合法的 A2A token JSON")
		return
	}
	if request.ActorType == "" {
		request.ActorType = types.ActorTypeUser
	}
	if !request.ActorType.Valid() ||
		(request.ActorType == types.ActorTypeUser && request.ServiceAccountLabel != "") ||
		(request.ActorType == types.ActorTypeServiceAccount &&
			!validA2AServiceAccountLabel(request.ServiceAccountLabel)) {
		writeError(w, http.StatusBadRequest, "A2A 身份类型或服务账号名称无效")
		return
	}
	if request.PrincipalUserID == 0 {
		request.PrincipalUserID = p.UserID
	}
	if request.PrincipalUserID != p.UserID {
		writeError(w, http.StatusForbidden, "A2A token 不能代表其他工作区成员")
		return
	}
	if request.ExpiresInDays == 0 {
		request.ExpiresInDays = 30
	}
	if request.ExpiresInDays < 1 || request.ExpiresInDays > 90 {
		writeError(w, http.StatusBadRequest, "A2A token 有效期必须为 1～90 天")
		return
	}
	proofHash, ok := decodeSecurityToken(r.Header.Get(reauthHeaderName))
	if !ok {
		writeError(w, http.StatusForbidden, "签发 A2A token 前需要重新验证身份")
		return
	}
	sessionCookie, err := r.Cookie(sessionCookieName)
	if err != nil || sessionCookie.Value == "" {
		writeError(w, http.StatusUnauthorized, "未登录或会话已过期")
		return
	}
	raw, hash, err := auth.NewSessionToken()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "服务器内部错误，请稍后重试")
		return
	}
	item, err := st.IssueA2AAccessToken(r.Context(), types.IssueA2AAccessToken{
		TenantID: int64(p.TenantID), ActorUserID: p.UserID,
		PrincipalUserID: request.PrincipalUserID, ActorType: request.ActorType,
		ServiceAccountLabel: request.ServiceAccountLabel, Scopes: request.Scopes,
		TokenHash: hash, SessionTokenHash: auth.HashSessionToken(sessionCookie.Value),
		ReauthProofHash: proofHash,
		ExpiresAt:       time.Now().Add(time.Duration(request.ExpiresInDays) * 24 * time.Hour),
	})
	if err != nil {
		writeAppError(w, err)
		return
	}
	item.RawTokenOnce = raw
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusCreated, item)
}

func (s *server) handleListA2AAccessTokens(w http.ResponseWriter, r *http.Request) {
	p, err := auth.PrincipalFromContext(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}
	st := s.a2aAccessStore()
	if st == nil {
		writeError(w, http.StatusServiceUnavailable, "A2A token 管理尚未启用")
		return
	}
	items, err := st.ListA2AAccessTokens(r.Context(), int64(p.TenantID), p.UserID)
	if err != nil {
		writeAppError(w, err)
		return
	}
	for index := range items {
		// List is never a bearer recovery endpoint, even if a future Store
		// implementation accidentally returns an issuance-only projection.
		items[index].TokenHash = nil
		items[index].RawTokenOnce = ""
	}
	writeJSON(w, http.StatusOK, map[string]any{"tokens": items})
}

func (s *server) handleRevokeA2AAccessToken(w http.ResponseWriter, r *http.Request) {
	p, err := auth.PrincipalFromContext(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}
	st := s.a2aAccessStore()
	if st == nil {
		writeError(w, http.StatusServiceUnavailable, "A2A token 管理尚未启用")
		return
	}
	if err := st.RevokeA2AAccessToken(r.Context(), int64(p.TenantID), p.UserID,
		r.PathValue("token_id")); err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func decodeA2ATokenJSON(r *http.Request, target any) bool {
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return false
	}
	var trailing any
	return errors.Is(decoder.Decode(&trailing), io.EOF)
}

// Keep the label visibly non-secret at the HTTP boundary; Store repeats the
// durable checks. Credential material belongs in credentialvault, never here.
func validA2AServiceAccountLabel(value string) bool {
	return value == strings.TrimSpace(value) && strings.IndexFunc(value, unicode.IsControl) < 0 &&
		!credentialguard.ContainsCredential(value)
}
