package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/YouToco/vane/server/auth"
	"github.com/YouToco/vane/server/types"
)

const (
	workspaceBodyLimit = 8 << 10
	workspaceInviteTTL = 7 * 24 * time.Hour
)

func (s *server) workspaceStore() (WorkspaceStore, bool) {
	if s.deps.Workspaces != nil {
		return s.deps.Workspaces, true
	}
	if s.deps.Store != nil {
		return s.deps.Store, true
	}
	return nil, false
}

func (s *server) requireWorkspaceStore(w http.ResponseWriter) (WorkspaceStore, bool) {
	st, ok := s.workspaceStore()
	if !ok {
		writeError(w, http.StatusServiceUnavailable, "工作区功能尚未启用")
	}
	return st, ok
}

func principalForWorkspace(r *http.Request, tenantID int64) (auth.Principal, bool) {
	p, err := auth.PrincipalFromContext(r.Context())
	return p, err == nil && int64(p.TenantID) == tenantID
}

func pathInt64(r *http.Request, key string) (int64, error) {
	return strconv.ParseInt(r.PathValue(key), 10, 64)
}

func (s *server) handleListWorkspaces(w http.ResponseWriter, r *http.Request) {
	p, err := auth.PrincipalFromContext(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}
	st, ok := s.requireWorkspaceStore(w)
	if !ok {
		return
	}
	items, err := st.ListWorkspacesForUser(r.Context(), p.UserID)
	if err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"workspaces": items, "current_tenant_id": int64(p.TenantID)})
}

func (s *server) handleCreateWorkspace(w http.ResponseWriter, r *http.Request) {
	p, err := auth.PrincipalFromContext(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, workspaceBodyLimit)
	var req struct {
		Name      string `json:"name"`
		SeatLimit int    `json:"seat_limit"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "请求体不是合法 JSON")
		return
	}
	st, ok := s.requireWorkspaceStore(w)
	if !ok {
		return
	}
	workspace, err := st.CreateTeamWorkspace(r.Context(), int64(p.TenantID), p.UserID, req.Name, req.SeatLimit)
	if err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, workspace)
}

func (s *server) handleSwitchWorkspace(w http.ResponseWriter, r *http.Request) {
	p, err := auth.PrincipalFromContext(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}
	tenantID, err := pathInt64(r, "tenant_id")
	if err != nil || tenantID <= 0 {
		writeError(w, http.StatusBadRequest, "工作区 ID 无效")
		return
	}
	cookie, err := r.Cookie(sessionCookieName)
	if err != nil || cookie.Value == "" {
		writeError(w, http.StatusUnauthorized, "未登录或会话已过期")
		return
	}
	st, ok := s.requireWorkspaceStore(w)
	if !ok {
		return
	}
	if _, err := st.GetWorkspaceForUser(r.Context(), tenantID, p.UserID); err != nil {
		writeAppError(w, err)
		return
	}
	newToken, newHash, err := auth.NewSessionToken()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "服务器内部错误，请稍后重试")
		return
	}
	expiresAt := time.Now().Add(sessionTTL)
	if err := st.RotateSession(r.Context(), auth.HashSessionToken(cookie.Value), newHash, p.UserID, tenantID, expiresAt); err != nil {
		writeAppError(w, err)
		return
	}
	setSessionCookie(w, newToken, expiresAt)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "tenant_id": tenantID})
}

func setSessionCookie(w http.ResponseWriter, token string, expiresAt time.Time) {
	http.SetCookie(w, &http.Cookie{Name: sessionCookieName, Value: token, Path: "/", Expires: expiresAt,
		HttpOnly: true, Secure: true, SameSite: http.SameSiteLaxMode})
}

func (s *server) handleIssueWorkspaceInvite(w http.ResponseWriter, r *http.Request) {
	tenantID, err := pathInt64(r, "tenant_id")
	if err != nil || tenantID <= 0 {
		writeError(w, http.StatusBadRequest, "工作区 ID 无效")
		return
	}
	p, exact := principalForWorkspace(r, tenantID)
	if !exact {
		writeError(w, http.StatusNotFound, "工作区不存在或无权访问")
		return
	}
	if p.Role != types.MembershipRoleOwner && p.Role != types.MembershipRoleAdmin {
		writeError(w, http.StatusForbidden, "当前角色不能邀请成员")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, workspaceBodyLimit)
	var req struct {
		Email string               `json:"email"`
		Role  types.MembershipRole `json:"role"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "请求体不是合法 JSON")
		return
	}
	if req.Role == "" {
		req.Role = types.MembershipRoleMember
	}
	raw, hash, err := auth.NewSessionToken()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "服务器内部错误，请稍后重试")
		return
	}
	st, ok := s.requireWorkspaceStore(w)
	if !ok {
		return
	}
	invite, err := st.IssueWorkspaceInvite(r.Context(), tenantID, p.UserID, req.Email, req.Role, hash, time.Now().Add(workspaceInviteTTL))
	if err != nil {
		writeAppError(w, err)
		return
	}
	invite.RawTokenOnce = raw
	writeJSON(w, http.StatusCreated, invite)
}

func (s *server) handleListWorkspaceInvites(w http.ResponseWriter, r *http.Request) {
	tenantID, err := pathInt64(r, "tenant_id")
	if err != nil || tenantID <= 0 {
		writeError(w, http.StatusBadRequest, "工作区 ID 无效")
		return
	}
	p, exact := principalForWorkspace(r, tenantID)
	if !exact {
		writeError(w, http.StatusNotFound, "工作区不存在或无权访问")
		return
	}
	if p.Role != types.MembershipRoleOwner && p.Role != types.MembershipRoleAdmin {
		writeError(w, http.StatusForbidden, "当前角色不能查看邀请")
		return
	}
	st, ok := s.requireWorkspaceStore(w)
	if !ok {
		return
	}
	items, err := st.ListWorkspaceInvites(r.Context(), tenantID, p.UserID)
	if err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"invites": items})
}

func (s *server) handleRevokeWorkspaceInvite(w http.ResponseWriter, r *http.Request) {
	tenantID, terr := pathInt64(r, "tenant_id")
	inviteID, ierr := pathInt64(r, "invite_id")
	if terr != nil || ierr != nil || tenantID <= 0 || inviteID <= 0 {
		writeError(w, http.StatusBadRequest, "工作区或邀请 ID 无效")
		return
	}
	p, exact := principalForWorkspace(r, tenantID)
	if !exact {
		writeError(w, http.StatusNotFound, "工作区不存在或无权访问")
		return
	}
	if p.Role != types.MembershipRoleOwner && p.Role != types.MembershipRoleAdmin {
		writeError(w, http.StatusForbidden, "当前角色不能撤销邀请")
		return
	}
	st, ok := s.requireWorkspaceStore(w)
	if !ok {
		return
	}
	if err := st.RevokeWorkspaceInvite(r.Context(), tenantID, p.UserID, inviteID); err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *server) handleAcceptWorkspaceInvite(w http.ResponseWriter, r *http.Request) {
	p, err := auth.PrincipalFromContext(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, workspaceBodyLimit)
	var req struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.Token) == "" {
		writeError(w, http.StatusBadRequest, "邀请 token 无效")
		return
	}
	st, ok := s.requireWorkspaceStore(w)
	if !ok {
		return
	}
	workspace, err := st.AcceptWorkspaceInvite(r.Context(), auth.HashSessionToken(req.Token), p.UserID)
	if err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "workspace": workspace})
}

func (s *server) handleWorkspaceInviteRegister(w http.ResponseWriter, r *http.Request) {
	if !s.checkOrigin(w, r) {
		return
	}
	if !s.limiter.allowAndRecord(ipLimitKey(r), time.Now()) {
		writeError(w, http.StatusTooManyRequests, "操作过于频繁，请稍后再试")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, workspaceBodyLimit)
	var req struct {
		Email    string `json:"email"`
		Password string `json:"password"`
		Token    string `json:"token"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "请求体不是合法 JSON")
		return
	}
	req.Email = strings.TrimSpace(req.Email)
	if req.Email == "" || !strings.Contains(req.Email, "@") || req.Token == "" {
		writeError(w, http.StatusBadRequest, "注册信息或邀请无效")
		return
	}
	if err := auth.ValidatePassword(req.Password); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	hash, err := auth.HashPasswordCtx(r.Context(), req.Password)
	if err != nil {
		slog.Error("workspace invite register: 密码哈希失败", "err", err)
		writeError(w, http.StatusInternalServerError, "服务器内部错误，请稍后重试")
		return
	}
	st, ok := s.requireWorkspaceStore(w)
	if !ok {
		return
	}
	user, personal, err := st.RegisterWithWorkspaceInvite(r.Context(), req.Email, hash, auth.HashSessionToken(req.Token))
	if err != nil {
		writeAppError(w, err)
		return
	}
	s.issueSession(w, r, user.ID, personal.ID)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "tenant_id": personal.ID})
}

func (s *server) handleListWorkspaceMembers(w http.ResponseWriter, r *http.Request) {
	tenantID, err := pathInt64(r, "tenant_id")
	if err != nil || tenantID <= 0 {
		writeError(w, http.StatusBadRequest, "工作区 ID 无效")
		return
	}
	p, exact := principalForWorkspace(r, tenantID)
	if !exact {
		writeError(w, http.StatusNotFound, "工作区不存在或无权访问")
		return
	}
	st, ok := s.requireWorkspaceStore(w)
	if !ok {
		return
	}
	members, err := st.ListWorkspaceMembers(r.Context(), tenantID, p.UserID)
	if err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"members": members})
}

func (s *server) handleUpdateWorkspaceMember(w http.ResponseWriter, r *http.Request) {
	tenantID, terr := pathInt64(r, "tenant_id")
	targetID, uerr := pathInt64(r, "user_id")
	if terr != nil || uerr != nil || tenantID <= 0 || targetID <= 0 {
		writeError(w, http.StatusBadRequest, "工作区或用户 ID 无效")
		return
	}
	p, exact := principalForWorkspace(r, tenantID)
	if !exact {
		writeError(w, http.StatusNotFound, "工作区不存在或无权访问")
		return
	}
	if p.Role != types.MembershipRoleOwner {
		writeError(w, http.StatusForbidden, "只有 Owner 可以修改成员角色")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, workspaceBodyLimit)
	var req struct {
		Role types.MembershipRole `json:"role"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "请求体不是合法 JSON")
		return
	}
	st, ok := s.requireWorkspaceStore(w)
	if !ok {
		return
	}
	if err := st.UpdateWorkspaceMemberRole(r.Context(), tenantID, p.UserID, targetID, req.Role); err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *server) handleRemoveWorkspaceMember(w http.ResponseWriter, r *http.Request) {
	tenantID, terr := pathInt64(r, "tenant_id")
	targetID, uerr := pathInt64(r, "user_id")
	if terr != nil || uerr != nil || tenantID <= 0 || targetID <= 0 {
		writeError(w, http.StatusBadRequest, "工作区或用户 ID 无效")
		return
	}
	p, exact := principalForWorkspace(r, tenantID)
	if !exact {
		writeError(w, http.StatusNotFound, "工作区不存在或无权访问")
		return
	}
	if p.Role == types.MembershipRoleMember && p.UserID != targetID {
		writeError(w, http.StatusForbidden, "当前角色不能移除其他成员")
		return
	}
	st, ok := s.requireWorkspaceStore(w)
	if !ok {
		return
	}
	if err := st.RemoveWorkspaceMember(r.Context(), tenantID, p.UserID, targetID); err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *server) handleTransferWorkspaceOwnership(w http.ResponseWriter, r *http.Request) {
	tenantID, err := pathInt64(r, "tenant_id")
	if err != nil || tenantID <= 0 {
		writeError(w, http.StatusBadRequest, "工作区 ID 无效")
		return
	}
	p, exact := principalForWorkspace(r, tenantID)
	if !exact {
		writeError(w, http.StatusNotFound, "工作区不存在或无权访问")
		return
	}
	if p.Role != types.MembershipRoleOwner {
		writeError(w, http.StatusForbidden, "只有 Owner 可以转移所有权")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, workspaceBodyLimit)
	var req struct {
		UserID int64 `json:"user_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID <= 0 {
		writeError(w, http.StatusBadRequest, "目标用户 ID 无效")
		return
	}
	st, ok := s.requireWorkspaceStore(w)
	if !ok {
		return
	}
	if err := st.TransferWorkspaceOwnership(r.Context(), tenantID, p.UserID, req.UserID); err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}
