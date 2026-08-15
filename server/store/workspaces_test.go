package store

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/YouToco/vane/server/types"
)

func workspaceIntegrationStore(t *testing.T) *Store {
	t.Helper()
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required for PostgreSQL workspace tests")
	}
	if err := Migrate(t.Context(), databaseURL); err != nil {
		t.Fatalf("migrate workspace test database: %v", err)
	}
	st, err := New(t.Context(), databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	return st
}

func workspaceUniqueCode(prefix string) string {
	return fmt.Sprintf("%s-%d-%d", prefix, time.Now().UnixNano(), os.Getpid())
}

func workspaceTestAccount(t *testing.T, st *Store, suffix string) (*types.User, *types.Tenant) {
	t.Helper()
	code := workspaceUniqueCode("workspace-account-" + suffix)
	if _, err := st.IssueInvite(t.Context(), code, nil, 1, nil); err != nil {
		t.Fatal(err)
	}
	email := fmt.Sprintf("workspace-%s-%d@example.com", suffix, time.Now().UnixNano())
	u, tenant, err := st.RegisterWithInvite(t.Context(), email, "test-password-hash", code)
	if err != nil {
		t.Fatal(err)
	}
	return u, tenant
}

func TestWorkspaceInviteIsolationAndSessionRotationPostgres(t *testing.T) {
	st := workspaceIntegrationStore(t)
	ctx := t.Context()
	owner, personal := workspaceTestAccount(t, st, "owner")
	member, _ := workspaceTestAccount(t, st, "member")
	stranger, _ := workspaceTestAccount(t, st, "stranger")

	team, err := st.CreateTeamWorkspace(ctx, personal.ID, owner.ID, "Vane Team", 2)
	if err != nil {
		t.Fatalf("create team: %v", err)
	}
	if team.Kind != types.WorkspaceKindTeam || team.Role != types.MembershipRoleOwner || team.SeatLimit != 2 {
		t.Fatalf("unexpected team: %+v", team)
	}

	raw := []byte(fmt.Sprintf("member-invite-token-%d", time.Now().UnixNano()))
	tokenHash := sha256.Sum256(raw)
	invite, err := st.IssueWorkspaceInvite(ctx, team.ID, owner.ID, *member.Email,
		types.MembershipRoleMember, tokenHash[:], time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("issue invite: %v", err)
	}
	if invite.Email != NormalizeEmail(*member.Email) || invite.ConsumedAt != nil {
		t.Fatalf("unexpected invite: %+v", invite)
	}
	// FORCE RLS must hide the row even from the table owner when no exact
	// transaction claims have been established.
	rlsTx, err := st.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rlsTx.Exec(ctx, `SET LOCAL ROLE vane_app`); err != nil {
		t.Fatal(err)
	}
	var unscopedCount int
	if err := rlsTx.QueryRow(ctx, `SELECT count(*) FROM workspace_invites WHERE tenant_id=$1`, team.ID).Scan(&unscopedCount); err != nil {
		t.Fatalf("unscoped RLS probe: %v", err)
	}
	if unscopedCount != 0 {
		t.Fatalf("unscoped invite visibility leaked %d rows", unscopedCount)
	}
	if err := setWorkspaceControlScope(ctx, rlsTx, team.ID, owner.ID); err != nil {
		t.Fatal(err)
	}
	var scopedCount int
	if err := rlsTx.QueryRow(ctx, `SELECT count(*) FROM workspace_invites WHERE tenant_id=$1`, team.ID).Scan(&scopedCount); err != nil {
		t.Fatal(err)
	}
	if err := rlsTx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if scopedCount != 1 {
		t.Fatalf("exact RLS scope should see one invite, got %d", scopedCount)
	}
	if _, err := st.AcceptWorkspaceInvite(ctx, tokenHash[:], stranger.ID); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("wrong email must be hidden as not found, got %v", err)
	}
	joined, err := st.AcceptWorkspaceInvite(ctx, tokenHash[:], member.ID)
	if err != nil {
		t.Fatalf("accept invite: %v", err)
	}
	if joined.ID != team.ID || joined.Role != types.MembershipRoleMember {
		t.Fatalf("unexpected joined workspace: %+v", joined)
	}
	if _, err := st.AcceptWorkspaceInvite(ctx, tokenHash[:], member.ID); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("invite replay must fail closed, got %v", err)
	}

	items, err := st.ListWorkspacesForUser(ctx, member.ID)
	if err != nil || len(items) != 2 || items[0].Kind != types.WorkspaceKindPersonal {
		t.Fatalf("personal + team list/order mismatch: %+v err=%v", items, err)
	}
	if _, err := st.GetWorkspaceForUser(ctx, team.ID, stranger.ID); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("cross-workspace lookup must be hidden, got %v", err)
	}

	nonce := time.Now().UnixNano()
	oldHash := sha256.Sum256([]byte(fmt.Sprintf("old-session-%d", nonce)))
	newHash := sha256.Sum256([]byte(fmt.Sprintf("new-session-%d", nonce)))
	expires := time.Now().Add(time.Hour)
	if err := st.CreateSession(ctx, oldHash[:], member.ID, personal.ID, expires); err != nil {
		t.Fatal(err)
	}
	if err := st.RotateSession(ctx, oldHash[:], newHash[:], member.ID, team.ID, expires); err != nil {
		t.Fatalf("rotate session: %v", err)
	}
	if _, err := st.LookupSession(ctx, oldHash[:]); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("old session must be revoked, got %v", err)
	}
	session, err := st.LookupSession(ctx, newHash[:])
	if err != nil || session.TenantID != team.ID || session.Role != types.MembershipRoleMember || session.ActorType != types.ActorTypeUser {
		t.Fatalf("rotated principal mismatch: %+v err=%v", session, err)
	}

	if err := st.UpdateWorkspaceMemberRole(ctx, team.ID, owner.ID, member.ID, types.MembershipRoleAdmin); err != nil {
		t.Fatalf("promote member: %v", err)
	}
	if _, err := st.LookupSession(ctx, newHash[:]); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("role change must revoke pinned sessions, got %v", err)
	}
}

func TestWorkspaceInviteSeatAndRoleGuardsPostgres(t *testing.T) {
	st := workspaceIntegrationStore(t)
	ctx := t.Context()
	owner, personal := workspaceTestAccount(t, st, "guard-owner")
	member, _ := workspaceTestAccount(t, st, "guard-member")
	team, err := st.CreateTeamWorkspace(ctx, personal.ID, owner.ID, "Guard Team", 2)
	if err != nil {
		t.Fatal(err)
	}
	one := sha256.Sum256([]byte(fmt.Sprintf("guard-one-%d", time.Now().UnixNano())))
	if _, err := st.IssueWorkspaceInvite(ctx, team.ID, owner.ID, *member.Email,
		types.MembershipRoleMember, one[:], time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	other := sha256.Sum256([]byte(fmt.Sprintf("guard-two-%d", time.Now().UnixNano())))
	if _, err := st.IssueWorkspaceInvite(ctx, team.ID, owner.ID, "another@example.com",
		types.MembershipRoleMember, other[:], time.Now().Add(time.Hour)); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("pending invite must reserve final seat, got %v", err)
	}
	if _, err := st.IssueWorkspaceInvite(ctx, personal.ID, owner.ID, "another@example.com",
		types.MembershipRoleMember, other[:], time.Now().Add(time.Hour)); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("personal workspace invite must fail, got %v", err)
	}
}
