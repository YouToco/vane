package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/YouToco/vane/server/auth"
	"github.com/YouToco/vane/server/types"
)

type fakeWorkspaceStore struct {
	WorkspaceStore // unused methods deliberately panic if a test crosses scope
	auth           *fakeAuthStore
	items          map[int64][]types.Workspace
	inviteHash     []byte
	inviteEmail    string
}

func (f *fakeWorkspaceStore) ListWorkspacesForUser(_ context.Context, userID int64) ([]types.Workspace, error) {
	return f.items[userID], nil
}

func (f *fakeWorkspaceStore) DefaultWorkspaceForUser(_ context.Context, userID int64) (int64, error) {
	items := f.items[userID]
	if len(items) == 0 {
		return 0, types.NewAppError(types.CodeNotFound, "no workspace", nil)
	}
	return items[0].ID, nil
}

func (f *fakeWorkspaceStore) GetWorkspaceForUser(_ context.Context, tenantID, userID int64) (*types.Workspace, error) {
	for _, item := range f.items[userID] {
		if item.ID == tenantID {
			copy := item
			return &copy, nil
		}
	}
	return nil, types.NewAppError(types.CodeNotFound, "工作区不存在或无权访问", nil)
}

func (f *fakeWorkspaceStore) RotateSession(_ context.Context, oldHash, newHash []byte, userID, tenantID int64, expiresAt time.Time) error {
	f.auth.mu.Lock()
	defer f.auth.mu.Unlock()
	old, ok := f.auth.sessions[string(oldHash)]
	if !ok || old.UserID != userID {
		return types.NewAppError(types.CodeNotFound, "old session missing", nil)
	}
	var role types.MembershipRole
	for _, membership := range f.auth.members[userID] {
		if membership.TenantID == tenantID {
			role = membership.Role
			break
		}
	}
	if !role.Valid() {
		return types.NewAppError(types.CodeNotFound, "membership missing", nil)
	}
	f.auth.sessions[string(newHash)] = &types.Session{TokenHash: newHash, UserID: userID, TenantID: tenantID,
		Role: role, ActorType: types.ActorTypeUser, ExpiresAt: expiresAt}
	delete(f.auth.sessions, string(oldHash))
	return nil
}

func (f *fakeWorkspaceStore) IssueWorkspaceInvite(_ context.Context, tenantID, actorUserID int64, email string, role types.MembershipRole, tokenHash []byte, expiresAt time.Time) (*types.WorkspaceInvite, error) {
	f.inviteHash = append([]byte(nil), tokenHash...)
	f.inviteEmail = email
	return &types.WorkspaceInvite{ID: 1, TenantID: tenantID, Email: email, Role: role,
		IssuedBy: actorUserID, ExpiresAt: expiresAt, CreatedAt: time.Now()}, nil
}

func TestWorkspaceMeSwitchRotatesSessionAndExposesExactRole(t *testing.T) {
	fake := newFakeAuthStore()
	user := fake.addUser(t, "switch@example.com", "switch-password-123", 7)
	fake.members[user.ID] = append(fake.members[user.ID], types.Membership{
		TenantID: 8, UserID: user.ID, Role: types.MembershipRoleAdmin,
	})
	workspaces := &fakeWorkspaceStore{auth: fake, items: map[int64][]types.Workspace{
		user.ID: {
			{ID: 7, Name: "Personal", Kind: types.WorkspaceKindPersonal, Role: types.MembershipRoleOwner},
			{ID: 8, Name: "Team", Kind: types.WorkspaceKindTeam, Role: types.MembershipRoleAdmin},
		},
	}}
	mux := http.NewServeMux()
	Mount(mux, Deps{Auth: fake, Workspaces: workspaces, Principal: auth.NewContextResolver()})
	login := postJSON(t, mux, "/api/auth/login", map[string]string{
		"email": "switch@example.com", "password": "switch-password-123",
	}, nil)
	if login.Code != http.StatusOK {
		t.Fatalf("login=%d %s", login.Code, login.Body.String())
	}
	oldCookie := sessionCookieFrom(t, login)

	before := httptest.NewRequest(http.MethodGet, "/api/auth/me", nil)
	before.AddCookie(oldCookie)
	beforeRec := httptest.NewRecorder()
	mux.ServeHTTP(beforeRec, before)
	var beforeBody struct {
		Role       types.MembershipRole `json:"role"`
		Workspaces []types.Workspace    `json:"workspaces"`
	}
	if err := json.Unmarshal(beforeRec.Body.Bytes(), &beforeBody); err != nil {
		t.Fatal(err)
	}
	if beforeBody.Role != types.MembershipRoleOwner || len(beforeBody.Workspaces) != 2 {
		t.Fatalf("unexpected me: %+v", beforeBody)
	}

	switched := postJSON(t, mux, "/api/workspaces/8/switch", map[string]any{}, oldCookie)
	if switched.Code != http.StatusOK {
		t.Fatalf("switch=%d %s", switched.Code, switched.Body.String())
	}
	newCookie := sessionCookieFrom(t, switched)
	oldProbe := httptest.NewRequest(http.MethodGet, "/api/auth/me", nil)
	oldProbe.AddCookie(oldCookie)
	oldRec := httptest.NewRecorder()
	mux.ServeHTTP(oldRec, oldProbe)
	if oldRec.Code != http.StatusUnauthorized {
		t.Fatalf("old cookie remains valid: %d", oldRec.Code)
	}
	newProbe := httptest.NewRequest(http.MethodGet, "/api/auth/me", nil)
	newProbe.AddCookie(newCookie)
	newRec := httptest.NewRecorder()
	mux.ServeHTTP(newRec, newProbe)
	var after struct {
		TenantID  int64                `json:"tenant_id"`
		Role      types.MembershipRole `json:"role"`
		ActorType types.ActorType      `json:"actor_type"`
	}
	if err := json.Unmarshal(newRec.Body.Bytes(), &after); err != nil {
		t.Fatal(err)
	}
	if after.TenantID != 8 || after.Role != types.MembershipRoleAdmin || after.ActorType != types.ActorTypeUser {
		t.Fatalf("switched principal mismatch: %+v", after)
	}
}

func TestWorkspaceInviteRequiresCurrentWorkspaceAdminAndReturnsRawOnce(t *testing.T) {
	fake := newFakeAuthStore()
	user := fake.addUser(t, "invite@example.com", "invite-password-123", 9)
	workspaces := &fakeWorkspaceStore{auth: fake, items: map[int64][]types.Workspace{
		user.ID: {{ID: 9, Name: "Team", Kind: types.WorkspaceKindTeam, Role: types.MembershipRoleOwner}},
	}}
	mux := http.NewServeMux()
	Mount(mux, Deps{Auth: fake, Workspaces: workspaces, Principal: auth.NewContextResolver()})
	login := postJSON(t, mux, "/api/auth/login", map[string]string{"email": "invite@example.com", "password": "invite-password-123"}, nil)
	cookie := sessionCookieFrom(t, login)
	rec := postJSON(t, mux, "/api/workspaces/9/invites", map[string]string{"email": "new@example.com", "role": "member"}, cookie)
	if rec.Code != http.StatusCreated {
		t.Fatalf("invite=%d %s", rec.Code, rec.Body.String())
	}
	var body struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Token == "" || len(workspaces.inviteHash) != 32 || workspaces.inviteEmail != "new@example.com" {
		t.Fatalf("raw/hash boundary broken: body=%+v hash=%d email=%q", body, len(workspaces.inviteHash), workspaces.inviteEmail)
	}
}
