package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/YouToco/vane/server/auth"
	"github.com/YouToco/vane/server/mailer"
	"github.com/YouToco/vane/server/store"
	"github.com/YouToco/vane/server/types"
)

const (
	emailVerificationTTL = 24 * time.Hour
	passwordResetTTL     = 30 * time.Minute
	reauthProofTTL       = 10 * time.Minute
	securityTokenBytes   = 32
	reauthHeaderName     = "X-Vane-Reauth-Token"
)

type AccountSecurityStore interface {
	GetAccountSecurityIdentity(ctx context.Context, tenantID, userID int64) (*store.AccountSecurityIdentity, error)
	IssueEmailVerification(ctx context.Context, tenantID, userID int64, tokenHash []byte, expiresAt time.Time) (string, bool, error)
	IssuePasswordReset(ctx context.Context, email string, tokenHash []byte, expiresAt time.Time) (string, bool, error)
	VerifyEmailWithToken(ctx context.Context, tokenHash []byte) error
	PasswordResetTokenUsable(ctx context.Context, tokenHash []byte) (bool, error)
	ResetPasswordWithToken(ctx context.Context, tokenHash []byte, passwordHash string) error
	IssueReauthProof(ctx context.Context, tenantID, userID int64, sessionHash, proofHash []byte, expiresAt time.Time) error
	LogoutAllWithReauth(ctx context.Context, tenantID, userID int64, sessionHash, proofHash []byte) (int64, error)
}

type SecurityMailer interface {
	Send(ctx context.Context, message mailer.Message) error
}

func newSecurityToken() (string, []byte, error) {
	return auth.NewSessionToken()
}

func decodeSecurityToken(raw string) ([]byte, bool) {
	raw = strings.TrimSpace(raw)
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil || len(decoded) != securityTokenBytes {
		return nil, false
	}
	return auth.HashSessionToken(raw), true
}

func (s *server) accountSecurityStore() AccountSecurityStore {
	if s.deps.AccountSecurity != nil {
		return s.deps.AccountSecurity
	}
	if s.deps.Store != nil {
		return s.deps.Store
	}
	return nil
}

func (s *server) handleRequestEmailVerification(w http.ResponseWriter, r *http.Request) {
	if !s.checkOrigin(w, r) {
		return
	}
	p, err := auth.PrincipalFromContext(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}
	security := s.accountSecurityStore()
	if security == nil || s.deps.SecurityMailer == nil {
		writeError(w, http.StatusServiceUnavailable, "邮箱服务尚未配置")
		return
	}
	if !s.limiter.allowAndRecord("verify:"+stringPrincipal(p), time.Now()) {
		writeError(w, http.StatusTooManyRequests, "操作过于频繁，请稍后再试")
		return
	}
	raw, hash, err := newSecurityToken()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "生成验证令牌失败")
		return
	}
	email, issued, err := security.IssueEmailVerification(r.Context(), int64(p.TenantID), p.UserID,
		hash, time.Now().Add(emailVerificationTTL))
	if err != nil {
		writeAppError(w, err)
		return
	}
	if !issued {
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "already_verified": true})
		return
	}
	link := strings.TrimRight(s.deps.Origin, "/") + "/verify-email?token=" + raw
	if err := s.deps.SecurityMailer.Send(r.Context(), mailer.Message{
		To: email, Subject: "验证你的 Vane 邮箱",
		Text: "请在 24 小时内打开以下链接完成邮箱验证：\n" + link,
	}); err != nil {
		slog.Error("account security: 发送邮箱验证邮件失败", "user_id", p.UserID, "err", err)
		writeError(w, http.StatusBadGateway, "验证邮件发送失败，请稍后重试")
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]bool{"ok": true})
}

func stringPrincipal(p auth.Principal) string {
	return strconv.FormatInt(int64(p.TenantID), 10) + ":" + strconv.FormatInt(p.UserID, 10)
}

func (s *server) handleVerifyEmail(w http.ResponseWriter, r *http.Request) {
	if !s.checkOrigin(w, r) {
		return
	}
	if !s.limiter.allowAndRecord(ipLimitKey(r), time.Now()) {
		writeError(w, http.StatusTooManyRequests, "操作过于频繁，请稍后再试")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, authBodyLimit)
	var req struct {
		Token string `json:"token"`
	}
	if json.NewDecoder(r.Body).Decode(&req) != nil {
		writeError(w, http.StatusBadRequest, "请求体不是合法 JSON")
		return
	}
	hash, ok := decodeSecurityToken(req.Token)
	if !ok {
		writeError(w, http.StatusBadRequest, "验证令牌无效或已过期")
		return
	}
	security := s.accountSecurityStore()
	if security == nil {
		writeError(w, http.StatusServiceUnavailable, "账号安全服务不可用")
		return
	}
	if err := security.VerifyEmailWithToken(r.Context(), hash); err != nil {
		if errors.Is(err, types.ErrNotFound) {
			writeError(w, http.StatusBadRequest, "验证令牌无效或已过期")
			return
		}
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *server) handleRequestPasswordReset(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	defer func() {
		if wait := uniformAuthDelay - time.Since(started); wait > 0 {
			time.Sleep(wait)
		}
	}()
	if !s.checkOrigin(w, r) {
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, authBodyLimit)
	var req struct {
		Email string `json:"email"`
	}
	if json.NewDecoder(r.Body).Decode(&req) != nil {
		writeError(w, http.StatusBadRequest, "请求体不是合法 JSON")
		return
	}
	if !s.limiter.allowAndRecord(ipLimitKey(r), time.Now()) ||
		!s.limiter.allowAndRecord("reset:"+accountLimitKey(req.Email), time.Now()) {
		writeError(w, http.StatusTooManyRequests, "操作过于频繁，请稍后再试")
		return
	}
	security := s.accountSecurityStore()
	if security != nil && s.deps.SecurityMailer != nil {
		raw, hash, err := newSecurityToken()
		if err == nil {
			email, issued, issueErr := security.IssuePasswordReset(r.Context(), req.Email, hash,
				time.Now().Add(passwordResetTTL))
			if issueErr != nil {
				slog.Warn("account security: 密码重置请求未签发", "err", issueErr)
			} else if issued {
				link := strings.TrimRight(s.deps.Origin, "/") + "/reset-password?token=" + raw
				if sendErr := s.deps.SecurityMailer.Send(r.Context(), mailer.Message{
					To: email, Subject: "重置你的 Vane 密码",
					Text: "请在 30 分钟内打开以下链接重置密码：\n" + link,
				}); sendErr != nil {
					slog.Error("account security: 发送密码重置邮件失败", "err", sendErr)
				}
			}
		}
	}
	// Existing and absent accounts deliberately share status and body.
	writeJSON(w, http.StatusAccepted, map[string]any{
		"ok": true, "message": "如果该邮箱已注册，重置邮件将很快送达",
	})
}

func (s *server) handleResetPassword(w http.ResponseWriter, r *http.Request) {
	if !s.checkOrigin(w, r) {
		return
	}
	if !s.limiter.allowAndRecord(ipLimitKey(r), time.Now()) {
		writeError(w, http.StatusTooManyRequests, "操作过于频繁，请稍后再试")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, authBodyLimit)
	var req struct {
		Token    string `json:"token"`
		Password string `json:"password"`
	}
	if json.NewDecoder(r.Body).Decode(&req) != nil {
		writeError(w, http.StatusBadRequest, "请求体不是合法 JSON")
		return
	}
	hash, ok := decodeSecurityToken(req.Token)
	if !ok {
		writeError(w, http.StatusBadRequest, "重置令牌无效或已过期")
		return
	}
	security := s.accountSecurityStore()
	if security == nil {
		writeError(w, http.StatusServiceUnavailable, "账号安全服务不可用")
		return
	}
	usable, err := security.PasswordResetTokenUsable(r.Context(), hash)
	if err != nil {
		writeAppError(w, err)
		return
	}
	if !usable {
		writeError(w, http.StatusBadRequest, "重置令牌无效或已过期")
		return
	}
	if err := auth.ValidatePassword(req.Password); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	passwordHash, err := auth.HashPasswordCtx(r.Context(), req.Password)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "密码处理失败")
		return
	}
	if err := security.ResetPasswordWithToken(r.Context(), hash, passwordHash); err != nil {
		if errors.Is(err, types.ErrNotFound) {
			writeError(w, http.StatusBadRequest, "重置令牌无效或已过期")
			return
		}
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *server) handleReauth(w http.ResponseWriter, r *http.Request) {
	if !s.checkOrigin(w, r) {
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, authBodyLimit)
	var req struct {
		Password string `json:"password"`
	}
	if json.NewDecoder(r.Body).Decode(&req) != nil {
		writeError(w, http.StatusBadRequest, "请求体不是合法 JSON")
		return
	}
	p, err := auth.PrincipalFromContext(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}
	if !s.limiter.allowAndRecord("reauth:"+stringPrincipal(p), time.Now()) {
		writeError(w, http.StatusTooManyRequests, "操作过于频繁，请稍后再试")
		return
	}
	security := s.accountSecurityStore()
	if security == nil {
		writeError(w, http.StatusServiceUnavailable, "账号安全服务不可用")
		return
	}
	identity, err := security.GetAccountSecurityIdentity(r.Context(), int64(p.TenantID), p.UserID)
	if err != nil || identity.PasswordHash == "" ||
		auth.VerifyPasswordCtx(r.Context(), identity.PasswordHash, req.Password) != nil {
		writeError(w, http.StatusUnauthorized, authFailMsg)
		return
	}
	sessionCookie, err := r.Cookie(sessionCookieName)
	if err != nil || sessionCookie.Value == "" {
		writeError(w, http.StatusUnauthorized, "未登录或会话已过期")
		return
	}
	raw, proofHash, err := newSecurityToken()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "生成重新认证证明失败")
		return
	}
	if err := security.IssueReauthProof(r.Context(), int64(p.TenantID), p.UserID,
		auth.HashSessionToken(sessionCookie.Value), proofHash, time.Now().Add(reauthProofTTL)); err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "proof": raw, "expires_in": 600})
}

func (s *server) handleLogoutAll(w http.ResponseWriter, r *http.Request) {
	if !s.checkOrigin(w, r) {
		return
	}
	p, err := auth.PrincipalFromContext(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}
	proofHash, ok := decodeSecurityToken(r.Header.Get(reauthHeaderName))
	if !ok {
		writeError(w, http.StatusForbidden, "需要重新验证身份")
		return
	}
	sessionCookie, err := r.Cookie(sessionCookieName)
	if err != nil || sessionCookie.Value == "" {
		writeError(w, http.StatusUnauthorized, "未登录或会话已过期")
		return
	}
	security := s.accountSecurityStore()
	if security == nil {
		writeError(w, http.StatusServiceUnavailable, "账号安全服务不可用")
		return
	}
	count, err := security.LogoutAllWithReauth(r.Context(), int64(p.TenantID), p.UserID,
		auth.HashSessionToken(sessionCookie.Value), proofHash)
	if err != nil {
		writeAppError(w, err)
		return
	}
	clearSessionCookie(w)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "revoked_sessions": count})
}

func clearSessionCookie(w http.ResponseWriter) {
	http.SetCookie(w, &http.Cookie{
		Name: sessionCookieName, Value: "", Path: "/", MaxAge: -1,
		HttpOnly: true, Secure: true, SameSite: http.SameSiteLaxMode,
	})
}
