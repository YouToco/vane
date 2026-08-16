package store

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/server/internal/testgate"
	"github.com/YouToco/vane/server/types"
)

func TestMigration139ScopedA2AContract(t *testing.T) {
	payload, err := migrationsFS.ReadFile("migrations/139_scoped_a2a_access_tokens.sql")
	if err != nil {
		t.Fatal(err)
	}
	sqlText := string(payload)
	for _, want := range []string{
		"CREATE TABLE a2a_access_tokens", "CREATE TABLE a2a_access_token_events",
		"octet_length(token_hash)=32", "FORCE ROW LEVEL SECURITY",
		"app.a2a_token_hash", "uq_a2a_access_token_event_lifecycle",
		"UPDATE public.a2a_access_tokens SET revoked_at", "seal_a2a_access_token_revocation_v139",
		"membership_authorization_generation_seq", "membership_generation",
		"preserve_membership_authorization_generation_v139", "reauth_token_id",
		"issue_a2a_access_token_v139", "recent session-bound reauthentication",
		"revoke_a2a_access_token_v139", "A2A token not found or already revoked",
		"139 down refused: retained scoped A2A authority exists",
		"service_account", "assistant.chat", "content.query",
	} {
		if !strings.Contains(sqlText, want) {
			t.Errorf("migration 139 missing %q", want)
		}
	}
	for _, forbidden := range []string{
		"raw_token", "token_value", "GRANT DELETE ON a2a_access",
		"GRANT UPDATE ON a2a_access_tokens", "GRANT SELECT,INSERT ON a2a_access",
		"GRANT INSERT(id,token_hash",
	} {
		if strings.Contains(sqlText, forbidden) {
			t.Errorf("migration 139 persists or grants forbidden authority %q", forbidden)
		}
	}
}

func TestMigration139ScopedA2AIsolationPostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		testgate.Database(t)
	}
	database, provider, scratchURL, drop := migration128Scratch(t, databaseURL)
	t.Cleanup(drop)
	if _, err := provider.UpTo(t.Context(), 139); err != nil {
		t.Fatal(err)
	}

	member := migration138User(t, database, "a2a-member")
	owner := migration138User(t, database, "a2a-owner")
	admin := migration138User(t, database, "a2a-admin")
	otherOwner := migration138User(t, database, "a2a-other-owner")
	outsider := migration138User(t, database, "a2a-outsider")
	team := migration138Workspace(t, database, "a2a-team", "team", nil)
	other := migration138Workspace(t, database, "a2a-other", "team", nil)
	for _, membership := range []struct {
		tenant, user int64
		role         string
	}{
		{team, owner, "owner"}, {team, admin, "admin"}, {team, member, "member"},
		{other, otherOwner, "owner"}, {other, member, "member"},
	} {
		if _, err := database.ExecContext(t.Context(), `INSERT INTO memberships(
			tenant_id,user_id,role) VALUES($1,$2,$3)`, membership.tenant,
			membership.user, membership.role); err != nil {
			t.Fatal(err)
		}
	}
	st, err := New(t.Context(), migration138ApplicationURL(t, scratchURL, "migration139-a2a"))
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	t.Cleanup(func() {
		if !closed {
			st.Close()
		}
	})

	memberHash := bytes.Repeat([]byte{0x11}, 32)
	memberToken := migration139Issue(t, st, types.IssueA2AAccessToken{
		TenantID: team, ActorUserID: member, PrincipalUserID: member,
		ActorType: types.ActorTypeUser, Scopes: []types.A2AScope{
			types.A2AScopeContentQuery, types.A2AScopeAssistantChat,
		}, TokenHash: memberHash, ExpiresAt: time.Now().Add(24 * time.Hour),
	}, "member-team")
	if len(memberToken.TokenHash) != 32 || memberToken.Scopes[0] != types.A2AScopeAssistantChat {
		t.Fatalf("token was not hash-only/canonical: %+v", memberToken)
	}
	migration139AssertEventCount(t, database, team, memberToken.ID, "issued", 1)

	principal, err := st.AuthenticateA2AAccessToken(t.Context(), memberHash)
	if err != nil || principal.TenantID != team || principal.UserID != member ||
		principal.Role != types.MembershipRoleMember || principal.ActorType != types.ActorTypeUser {
		t.Fatalf("authenticated principal=%+v err=%v", principal, err)
	}
	if _, err := database.ExecContext(t.Context(), `UPDATE memberships SET role='admin'
		WHERE tenant_id=$1 AND user_id=$2`, team, member); err != nil {
		t.Fatal(err)
	}
	principal, err = st.AuthenticateA2AAccessToken(t.Context(), memberHash)
	if err != nil || principal.Role != types.MembershipRoleAdmin {
		t.Fatalf("user token froze stale membership role: principal=%+v err=%v", principal, err)
	}
	if _, err := database.ExecContext(t.Context(), `UPDATE memberships SET role='member'
		WHERE tenant_id=$1 AND user_id=$2`, team, member); err != nil {
		t.Fatal(err)
	}

	serviceHash := bytes.Repeat([]byte{0x22}, 32)
	serviceToken := migration139Issue(t, st, types.IssueA2AAccessToken{
		TenantID: team, ActorUserID: owner, PrincipalUserID: owner,
		ActorType: types.ActorTypeServiceAccount, ServiceAccountLabel: "market-reader",
		Scopes: []types.A2AScope{types.A2AScopeContentQuery}, TokenHash: serviceHash,
		ExpiresAt: time.Now().Add(24 * time.Hour),
	}, "owner-service")
	servicePrincipal, err := st.AuthenticateA2AAccessToken(t.Context(), serviceHash)
	if err != nil || servicePrincipal.ActorType != types.ActorTypeServiceAccount ||
		servicePrincipal.UserID != owner || servicePrincipal.Role != types.MembershipRoleMember {
		t.Fatalf("service token escaped least privilege: principal=%+v err=%v", servicePrincipal, err)
	}
	if _, err := st.IssueA2AAccessToken(t.Context(), types.IssueA2AAccessToken{
		TenantID: team, ActorUserID: admin, PrincipalUserID: owner,
		ActorType: types.ActorTypeServiceAccount, ServiceAccountLabel: "owner-impersonator",
		Scopes:           []types.A2AScope{types.A2AScopeContentQuery},
		TokenHash:        bytes.Repeat([]byte{0x23}, 32),
		SessionTokenHash: bytes.Repeat([]byte{0xa3}, 32), ReauthProofHash: bytes.Repeat([]byte{0xb3}, 32),
		ExpiresAt: time.Now().Add(time.Hour),
	}); types.CodeOf(err) != types.CodeForbidden {
		t.Fatalf("admin minted owner credential code=%s err=%v", types.CodeOf(err), err)
	}
	if _, err := st.IssueA2AAccessToken(t.Context(), types.IssueA2AAccessToken{
		TenantID: team, ActorUserID: member, PrincipalUserID: member,
		ActorType: types.ActorTypeServiceAccount, ServiceAccountLabel: "member-bot",
		Scopes:           []types.A2AScope{types.A2AScopeContentQuery},
		TokenHash:        bytes.Repeat([]byte{0x24}, 32),
		SessionTokenHash: bytes.Repeat([]byte{0xa4}, 32), ReauthProofHash: bytes.Repeat([]byte{0xb4}, 32),
		ExpiresAt: time.Now().Add(time.Hour),
	}); types.CodeOf(err) != types.CodeForbidden {
		t.Fatalf("member issued service token code=%s err=%v", types.CodeOf(err), err)
	}
	if _, err := st.IssueA2AAccessToken(t.Context(), types.IssueA2AAccessToken{
		TenantID: team, ActorUserID: owner, PrincipalUserID: owner,
		ActorType: types.ActorTypeServiceAccount, ServiceAccountLabel: "sk-AAAAAAAAAAAAAAAAAAAA",
		Scopes:           []types.A2AScope{types.A2AScopeContentQuery},
		TokenHash:        bytes.Repeat([]byte{0x25}, 32),
		SessionTokenHash: bytes.Repeat([]byte{0xa5}, 32), ReauthProofHash: bytes.Repeat([]byte{0xb5}, 32),
		ExpiresAt: time.Now().Add(time.Hour),
	}); types.CodeOf(err) != types.CodeValidation {
		t.Fatalf("Store persisted credential-shaped label code=%s err=%v", types.CodeOf(err), err)
	}

	ownerHash := bytes.Repeat([]byte{0x33}, 32)
	migration139Issue(t, st, types.IssueA2AAccessToken{
		TenantID: team, ActorUserID: owner, PrincipalUserID: owner,
		ActorType: types.ActorTypeUser, Scopes: []types.A2AScope{types.A2AScopeAssistantChat},
		TokenHash: ownerHash, ExpiresAt: time.Now().Add(24 * time.Hour),
	}, "owner-user")
	otherMemberHash := bytes.Repeat([]byte{0x34}, 32)
	migration139Issue(t, st, types.IssueA2AAccessToken{
		TenantID: other, ActorUserID: member, PrincipalUserID: member,
		ActorType: types.ActorTypeUser, Scopes: []types.A2AScope{types.A2AScopeContentQuery},
		TokenHash: otherMemberHash, ExpiresAt: time.Now().Add(24 * time.Hour),
	}, "member-other")

	memberList, err := st.ListA2AAccessTokens(t.Context(), team, member)
	if err != nil || len(memberList) != 1 || memberList[0].ID != memberToken.ID {
		t.Fatalf("member discovered another principal token: items=%+v err=%v", memberList, err)
	}
	ownerList, err := st.ListA2AAccessTokens(t.Context(), team, owner)
	if err != nil || len(ownerList) != 3 {
		t.Fatalf("owner did not see workspace token inventory: items=%+v err=%v", ownerList, err)
	}
	migration139AssertRLSAndPrivileges(t, database, team, member, memberToken)
	migration139AssertEventBindings(t, database, memberToken, member, outsider)
	migration139AssertCrossTenantProofRejected(t, database, st, team, other, member)
	migration139AssertProofOneTime(t, database, st, team, admin)
	migration139AssertLogoutProofCannotIssue(t, st, team, owner)
	migration139AssertDefinerRejectsTemporaryAuthorityShadows(t, database, st, team, admin)

	if err := st.RevokeA2AAccessToken(t.Context(), team, member,
		serviceToken.ID); types.CodeOf(err) != types.CodeNotFound {
		t.Fatalf("member revoked service token code=%s err=%v", types.CodeOf(err), err)
	}
	if err := st.RevokeA2AAccessToken(t.Context(), team, owner, serviceToken.ID); err != nil {
		t.Fatal(err)
	}
	migration139AssertEventCount(t, database, team, serviceToken.ID, "revoked", 1)
	var revokeActor, revokePrincipal int64
	var revokeScopes string
	var revokeType string
	if err := database.QueryRowContext(t.Context(), `SELECT actor_user_id,array_to_string(scopes,','),
		principal_user_id,actor_type FROM a2a_access_token_events
		WHERE tenant_id=$1 AND token_id=$2 AND event_kind='revoked'`, team,
		serviceToken.ID).Scan(&revokeActor, &revokeScopes, &revokePrincipal, &revokeType); err != nil || revokeActor != owner || revokePrincipal != owner ||
		revokeType != string(types.ActorTypeServiceAccount) ||
		revokeScopes != string(types.A2AScopeContentQuery) {
		t.Fatalf("revocation event drift actor=%d principal=%d type=%s scopes=%v err=%v",
			revokeActor, revokePrincipal, revokeType, revokeScopes, err)
	}
	migration139AssertRevocationImmutable(t, database, team, owner, serviceToken.ID)
	if _, err := st.AuthenticateA2AAccessToken(t.Context(), serviceHash); types.CodeOf(err) != types.CodeForbidden {
		t.Fatalf("revoked token authenticated code=%s err=%v", types.CodeOf(err), err)
	}

	if _, err := database.ExecContext(t.Context(), `UPDATE tenants SET status='suspended'
		WHERE id=$1`, team); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AuthenticateA2AAccessToken(t.Context(), ownerHash); types.CodeOf(err) != types.CodeForbidden {
		t.Fatalf("suspended workspace token stayed live code=%s err=%v", types.CodeOf(err), err)
	}
	if _, err := database.ExecContext(t.Context(), `UPDATE tenants SET status='active'
		WHERE id=$1`, team); err != nil {
		t.Fatal(err)
	}

	migration139AssertMembershipIncarnation(t, database, st, team, member, memberHash)
	newMemberHash := bytes.Repeat([]byte{0x35}, 32)
	newMemberToken := migration139Issue(t, st, types.IssueA2AAccessToken{
		TenantID: team, ActorUserID: member, PrincipalUserID: member,
		ActorType: types.ActorTypeUser, Scopes: []types.A2AScope{types.A2AScopeContentQuery},
		TokenHash: newMemberHash, ExpiresAt: time.Now().Add(24 * time.Hour),
	}, "member-rejoined")
	if newPrincipal, err := st.AuthenticateA2AAccessToken(t.Context(), newMemberHash); err != nil ||
		newPrincipal.UserID != member || newPrincipal.TenantID != team {
		t.Fatalf("new membership credential failed: principal=%+v token=%+v err=%v",
			newPrincipal, newMemberToken, err)
	}
	migration139AssertDownAdmissionFence(t, database, team, member, newMemberToken.ID,
		func() error {
			_, err := provider.DownTo(t.Context(), 138)
			return err
		})

	if _, err := provider.DownTo(t.Context(), 138); err == nil ||
		!strings.Contains(err.Error(), "139 down refused") {
		t.Fatalf("nonempty durable A2A ledger downgraded: %v", err)
	}
	dry, err := st.PurgeTenant(t.Context(), team, true)
	if err != nil || dry.Rows["a2a_access_tokens"] == 0 ||
		dry.Rows["a2a_access_token_events"] == 0 {
		t.Fatalf("dry purge missed A2A authority: report=%+v err=%v", dry, err)
	}
	if _, err := st.PurgeTenant(t.Context(), team, false); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.DownTo(t.Context(), 138); err == nil ||
		!strings.Contains(err.Error(), "139 down refused") {
		t.Fatalf("other workspace A2A authority did not fence downgrade: %v", err)
	}
	if _, err := st.PurgeTenant(t.Context(), other, false); err != nil {
		t.Fatal(err)
	}
	st.Close()
	closed = true
	if _, err := provider.DownTo(t.Context(), 138); err != nil {
		t.Fatalf("empty migration 139 did not downgrade: %v", err)
	}
	var tokensTable, generationColumn bool
	if err := database.QueryRowContext(t.Context(), `SELECT
		to_regclass('public.a2a_access_tokens') IS NOT NULL,
		EXISTS(SELECT 1 FROM information_schema.columns
		 WHERE table_schema='public' AND table_name='memberships'
		   AND column_name='authorization_generation')`).Scan(&tokensTable, &generationColumn); err != nil {
		t.Fatal(err)
	}
	if tokensTable || generationColumn {
		t.Fatalf("empty Down retained schema tokens=%v generation=%v", tokensTable, generationColumn)
	}
}

func migration139Issue(t *testing.T, st *Store, input types.IssueA2AAccessToken,
	identity string,
) *types.A2AAccessToken {
	t.Helper()
	sessionHash, proofHash := migration139Reauth(t, st, input.TenantID,
		input.ActorUserID, identity)
	input.SessionTokenHash = sessionHash
	input.ReauthProofHash = proofHash
	item, err := st.IssueA2AAccessToken(t.Context(), input)
	if err != nil {
		t.Fatal(err)
	}
	return item
}

func migration139Reauth(t *testing.T, st *Store, tenantID, userID int64,
	identity string,
) ([]byte, []byte) {
	t.Helper()
	session := sha256.Sum256([]byte("a2a-session/" + identity + "/" + uuid.NewString()))
	proof := sha256.Sum256([]byte("a2a-proof/" + identity + "/" + uuid.NewString()))
	if err := st.CreateSession(t.Context(), session[:], userID, tenantID,
		time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := st.IssueReauthProof(t.Context(), tenantID, userID, session[:], proof[:],
		time.Now().Add(10*time.Minute)); err != nil {
		t.Fatal(err)
	}
	return session[:], proof[:]
}

func migration139AssertMembershipIncarnation(t *testing.T, database *sql.DB,
	st *Store, tenantID, userID int64, oldHash []byte,
) {
	t.Helper()
	var oldGeneration int64
	if err := database.QueryRowContext(t.Context(), `SELECT authorization_generation
		FROM memberships WHERE tenant_id=$1 AND user_id=$2`, tenantID, userID).
		Scan(&oldGeneration); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), `DELETE FROM memberships
		WHERE tenant_id=$1 AND user_id=$2`, tenantID, userID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AuthenticateA2AAccessToken(t.Context(), oldHash); types.CodeOf(err) != types.CodeForbidden {
		t.Fatalf("removed membership token stayed live code=%s err=%v", types.CodeOf(err), err)
	}
	tx, err := database.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(t.Context(), `SET LOCAL ROLE vane_app`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `CREATE TEMP SEQUENCE
		membership_authorization_generation_seq`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `SELECT setval(
		'pg_temp.membership_authorization_generation_seq',$1,false)`, oldGeneration); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `INSERT INTO memberships(
		tenant_id,user_id,role,authorization_generation) VALUES($1,$2,'member',$3)`,
		tenantID, userID, oldGeneration); err != nil {
		t.Fatalf("runtime membership insert lost sequence authority: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var newGeneration int64
	if err := database.QueryRowContext(t.Context(), `SELECT authorization_generation
		FROM memberships WHERE tenant_id=$1 AND user_id=$2`, tenantID, userID).
		Scan(&newGeneration); err != nil {
		t.Fatal(err)
	}
	if newGeneration == oldGeneration {
		t.Fatal("explicit old membership generation resurrected an old bearer")
	}
	if _, err := st.AuthenticateA2AAccessToken(t.Context(), oldHash); types.CodeOf(err) != types.CodeForbidden {
		t.Fatalf("re-added membership resurrected old token code=%s err=%v", types.CodeOf(err), err)
	}
	immutable, err := database.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer immutable.Rollback()
	if _, err := immutable.ExecContext(t.Context(), `SET LOCAL ROLE vane_app`); err != nil {
		t.Fatal(err)
	}
	if _, err := immutable.ExecContext(t.Context(), `UPDATE memberships
		SET authorization_generation=$3 WHERE tenant_id=$1 AND user_id=$2`,
		tenantID, userID, oldGeneration); err == nil {
		t.Fatal("restricted role rewrote immutable membership generation")
	}
}

func migration139AssertRLSAndPrivileges(t *testing.T, database *sql.DB,
	tenantID, memberID int64, memberToken *types.A2AAccessToken,
) {
	t.Helper()
	var forceTokens, forceEvents bool
	if err := database.QueryRowContext(t.Context(), `SELECT
		(SELECT relrowsecurity AND relforcerowsecurity FROM pg_class WHERE oid='a2a_access_tokens'::regclass),
		(SELECT relrowsecurity AND relforcerowsecurity FROM pg_class WHERE oid='a2a_access_token_events'::regclass)`).
		Scan(&forceTokens, &forceEvents); err != nil || !forceTokens || !forceEvents {
		t.Fatalf("A2A authority is not FORCE RLS: tokens=%v events=%v err=%v",
			forceTokens, forceEvents, err)
	}
	var tokenDelete, tokenInsert, revokedUpdate, labelUpdate, expiryInsert,
		revokedInsert, createdInsert, eventUpdate, eventDelete, eventIDInsert,
		eventCreatedInsert, eventInsert, eventSequenceUsage, sequenceUsage,
		issueExecute, publicIssueExecute, issueSecurityDefiner,
		revokeExecute, publicRevokeExecute, revokeSecurityDefiner bool
	if err := database.QueryRowContext(t.Context(), `SELECT
		has_table_privilege('vane_app','a2a_access_tokens','DELETE'),
		has_table_privilege('vane_app','a2a_access_tokens','INSERT'),
		has_column_privilege('vane_app','a2a_access_tokens','revoked_at','UPDATE'),
		has_column_privilege('vane_app','a2a_access_tokens','service_account_label','UPDATE'),
		has_column_privilege('vane_app','a2a_access_tokens','expires_at','INSERT'),
		has_column_privilege('vane_app','a2a_access_tokens','revoked_at','INSERT'),
		has_column_privilege('vane_app','a2a_access_tokens','created_at','INSERT'),
		has_table_privilege('vane_app','a2a_access_token_events','UPDATE'),
		has_table_privilege('vane_app','a2a_access_token_events','DELETE'),
		has_table_privilege('vane_app','a2a_access_token_events','INSERT'),
		has_column_privilege('vane_app','a2a_access_token_events','id','INSERT'),
		has_column_privilege('vane_app','a2a_access_token_events','created_at','INSERT'),
		has_sequence_privilege('vane_app','a2a_access_token_events_id_seq','USAGE'),
		has_sequence_privilege('vane_app','membership_authorization_generation_seq','USAGE'),
		has_function_privilege('vane_app','issue_a2a_access_token_v139(uuid,bytea,bigint,bigint,text,text,text[],bigint,bigint,bytea,bytea,timestamptz)','EXECUTE'),
		EXISTS(SELECT 1 FROM pg_proc WHERE oid='issue_a2a_access_token_v139(uuid,bytea,bigint,bigint,text,text,text[],bigint,bigint,bytea,bytea,timestamptz)'::regprocedure
		 AND prosecdef AND proconfig=ARRAY['search_path=pg_catalog, public, pg_temp']),
		EXISTS(SELECT 1 FROM pg_proc p,
		 LATERAL aclexplode(COALESCE(p.proacl,acldefault('f',p.proowner))) acl
		 WHERE p.oid='issue_a2a_access_token_v139(uuid,bytea,bigint,bigint,text,text,text[],bigint,bigint,bytea,bytea,timestamptz)'::regprocedure
		 AND acl.grantee=0 AND acl.privilege_type='EXECUTE'),
		has_function_privilege('vane_app','revoke_a2a_access_token_v139(bigint,uuid,bigint)','EXECUTE'),
		EXISTS(SELECT 1 FROM pg_proc WHERE oid='revoke_a2a_access_token_v139(bigint,uuid,bigint)'::regprocedure
		 AND prosecdef AND proconfig=ARRAY['search_path=pg_catalog, public, pg_temp']),
		EXISTS(SELECT 1 FROM pg_proc p,
		 LATERAL aclexplode(COALESCE(p.proacl,acldefault('f',p.proowner))) acl
		 WHERE p.oid='revoke_a2a_access_token_v139(bigint,uuid,bigint)'::regprocedure
		 AND acl.grantee=0 AND acl.privilege_type='EXECUTE')`).
		Scan(&tokenDelete, &tokenInsert, &revokedUpdate, &labelUpdate, &expiryInsert,
			&revokedInsert, &createdInsert, &eventUpdate, &eventDelete, &eventInsert,
			&eventIDInsert, &eventCreatedInsert, &eventSequenceUsage, &sequenceUsage,
			&issueExecute, &issueSecurityDefiner, &publicIssueExecute,
			&revokeExecute, &revokeSecurityDefiner, &publicRevokeExecute); err != nil || tokenDelete || tokenInsert || revokedUpdate || labelUpdate ||
		expiryInsert || revokedInsert || createdInsert || eventUpdate || eventDelete ||
		eventInsert || eventIDInsert || eventCreatedInsert || eventSequenceUsage ||
		!sequenceUsage || !issueExecute || publicIssueExecute || !issueSecurityDefiner ||
		!revokeExecute || publicRevokeExecute || !revokeSecurityDefiner {
		t.Fatalf("A2A ACL drift delete=%v insert=%v revoked_update=%v label_update=%v "+
			"expiry_insert=%v revoked_insert=%v created_insert=%v event_update=%v "+
			"event_delete=%v event_insert=%v event_id_insert=%v event_created_insert=%v "+
			"event_sequence=%v sequence=%v issue_execute=%v issue_definer=%v "+
			"public_issue=%v revoke_execute=%v revoke_definer=%v public_revoke=%v err=%v",
			tokenDelete, tokenInsert, revokedUpdate, labelUpdate, expiryInsert,
			revokedInsert, createdInsert, eventUpdate, eventDelete, eventInsert,
			eventIDInsert, eventCreatedInsert, eventSequenceUsage, sequenceUsage,
			issueExecute, issueSecurityDefiner, publicIssueExecute, revokeExecute,
			revokeSecurityDefiner, publicRevokeExecute, err)
	}

	noScope, err := database.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer noScope.Rollback()
	if _, err := noScope.ExecContext(t.Context(), `SET LOCAL ROLE vane_app`); err != nil {
		t.Fatal(err)
	}
	var tokenCount, eventCount int
	if err := noScope.QueryRowContext(t.Context(), `SELECT
		(SELECT count(*) FROM a2a_access_tokens),
		(SELECT count(*) FROM a2a_access_token_events)`).Scan(&tokenCount, &eventCount); err != nil || tokenCount != 0 || eventCount != 0 {
		t.Fatalf("unscoped RLS leaked token/event %d/%d err=%v", tokenCount, eventCount, err)
	}
	_ = noScope.Rollback()

	manage, err := database.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer manage.Rollback()
	migration139EnterRole(t, manage, tenantID, memberID, "member")
	var onlyToken, onlyEvent string
	if err := manage.QueryRowContext(t.Context(), `SELECT string_agg(id::text,',' ORDER BY id)
		FROM a2a_access_tokens`).Scan(&onlyToken); err != nil || onlyToken != memberToken.ID {
		t.Fatalf("member token RLS scope=%q want=%q err=%v", onlyToken, memberToken.ID, err)
	}
	if err := manage.QueryRowContext(t.Context(), `SELECT string_agg(token_id::text,',' ORDER BY token_id)
		FROM a2a_access_token_events`).Scan(&onlyEvent); err != nil || onlyEvent != memberToken.ID {
		t.Fatalf("member event RLS scope=%q want=%q err=%v", onlyEvent, memberToken.ID, err)
	}
	_ = manage.Rollback()

	authScope, err := database.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer authScope.Rollback()
	if _, err := authScope.ExecContext(t.Context(), `SELECT set_config(
		'app.a2a_token_hash',$1,true)`, fmt.Sprintf("%x", memberToken.TokenHash)); err != nil {
		t.Fatal(err)
	}
	if _, err := authScope.ExecContext(t.Context(), `SET LOCAL ROLE vane_app`); err != nil {
		t.Fatal(err)
	}
	if err := authScope.QueryRowContext(t.Context(), `SELECT count(*) FROM a2a_access_tokens`).
		Scan(&tokenCount); err != nil || tokenCount != 1 {
		t.Fatalf("exact hash authentication count=%d err=%v", tokenCount, err)
	}
}

func migration139AssertEventBindings(t *testing.T, database *sql.DB,
	token *types.A2AAccessToken, actorID, outsiderID int64,
) {
	t.Helper()
	tests := []struct {
		name      string
		actor     int64
		kind      string
		scopes    []string
		principal int64
		actorType string
		wantOK    bool
	}{
		{"baseline", actorID, "issued", a2aScopeStrings(token.Scopes), token.PrincipalUserID, string(token.ActorType), true},
		{"actor", outsiderID, "issued", a2aScopeStrings(token.Scopes), token.PrincipalUserID, string(token.ActorType), false},
		{"principal", actorID, "issued", a2aScopeStrings(token.Scopes), outsiderID, string(token.ActorType), false},
		{"scopes", actorID, "issued", []string{"content.query"}, token.PrincipalUserID, string(token.ActorType), false},
		{"actor type", actorID, "issued", a2aScopeStrings(token.Scopes), token.PrincipalUserID, "service_account", false},
		{"revocation state", actorID, "revoked", a2aScopeStrings(token.Scopes), token.PrincipalUserID, string(token.ActorType), false},
	}
	for _, test := range tests {
		t.Run("event_"+test.name, func(t *testing.T) {
			tx, err := database.BeginTx(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			if _, err := tx.ExecContext(t.Context(), `DELETE FROM a2a_access_token_events
				WHERE tenant_id=$1 AND token_id=$2 AND event_kind='issued'`,
				token.TenantID, token.ID); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.ExecContext(t.Context(), `GRANT INSERT(
				tenant_id,token_id,actor_user_id,event_kind,scopes,principal_user_id,actor_type)
				ON a2a_access_token_events TO vane_app;
				GRANT USAGE,SELECT ON SEQUENCE a2a_access_token_events_id_seq TO vane_app`); err != nil {
				t.Fatal(err)
			}
			migration139EnterRole(t, tx, token.TenantID, actorID, "member")
			_, err = tx.ExecContext(t.Context(), `INSERT INTO a2a_access_token_events(
				tenant_id,token_id,actor_user_id,event_kind,scopes,principal_user_id,actor_type)
				VALUES($1,$2,$3,$4,$5,$6,$7)`, token.TenantID, token.ID, test.actor,
				test.kind, test.scopes, test.principal, test.actorType)
			if test.wantOK && err != nil {
				t.Fatalf("exact event rejected: %v", err)
			}
			if !test.wantOK && err == nil {
				t.Fatalf("event policy accepted mutated %s binding", test.name)
			}
		})
	}
}

func migration139AssertCrossTenantProofRejected(t *testing.T, database *sql.DB,
	st *Store, teamID, otherID, memberID int64,
) {
	t.Helper()
	_, proof := migration139Reauth(t, st, otherID, memberID, "cross-proof")
	var proofID int64
	if err := database.QueryRowContext(t.Context(), `UPDATE account_security_tokens
		SET consumed_at=clock_timestamp() WHERE token_hash=$1 RETURNING id`, proof).Scan(&proofID); err != nil {
		t.Fatal(err)
	}
	var generation int64
	if err := database.QueryRowContext(t.Context(), `SELECT authorization_generation
		FROM memberships WHERE tenant_id=$1 AND user_id=$2`, teamID, memberID).
		Scan(&generation); err != nil {
		t.Fatal(err)
	}
	_, err := database.ExecContext(t.Context(), `INSERT INTO a2a_access_tokens(
		id,token_hash,tenant_id,principal_user_id,actor_type,service_account_label,
		scopes,issued_by,membership_generation,reauth_token_id,expires_at)
		VALUES($1,$2,$3,$4,'user','',ARRAY['content.query']::text[],$4,$5,$6,
		clock_timestamp()+interval '1 day')`, uuid.NewString(),
		bytes.Repeat([]byte{0x66}, 32), teamID, memberID, generation, proofID)
	if err == nil {
		t.Fatal("cross-workspace reauthentication proof satisfied A2A authority FK")
	}
}

func migration139AssertLogoutProofCannotIssue(t *testing.T, st *Store,
	tenantID, userID int64,
) {
	t.Helper()
	session, proof := migration139Reauth(t, st, tenantID, userID, "logout-proof")
	if _, err := st.LogoutAllWithReauth(t.Context(), tenantID, userID, session, proof); err != nil {
		t.Fatal(err)
	}
	_, err := st.IssueA2AAccessToken(t.Context(), types.IssueA2AAccessToken{
		TenantID: tenantID, ActorUserID: userID, PrincipalUserID: userID,
		ActorType: types.ActorTypeUser, Scopes: []types.A2AScope{types.A2AScopeContentQuery},
		TokenHash: bytes.Repeat([]byte{0x67}, 32), SessionTokenHash: session,
		ReauthProofHash: proof, ExpiresAt: time.Now().Add(time.Hour),
	})
	if types.CodeOf(err) != types.CodeForbidden {
		t.Fatalf("proof consumed by logout-all issued A2A token code=%s err=%v",
			types.CodeOf(err), err)
	}
}

func migration139AssertProofOneTime(t *testing.T, database *sql.DB, st *Store,
	tenantID, userID int64,
) {
	t.Helper()
	session, proof := migration139Reauth(t, st, tenantID, userID, "one-time-proof")
	base := types.IssueA2AAccessToken{
		TenantID: tenantID, ActorUserID: userID, PrincipalUserID: userID,
		ActorType: types.ActorTypeUser, Scopes: []types.A2AScope{types.A2AScopeContentQuery},
		SessionTokenHash: session, ReauthProofHash: proof,
		ExpiresAt: time.Now().Add(time.Hour),
	}
	base.TokenHash = bytes.Repeat([]byte{0x68}, 32)
	first, err := st.IssueA2AAccessToken(t.Context(), base)
	if err != nil {
		t.Fatal(err)
	}
	var before int
	if err := database.QueryRowContext(t.Context(), `SELECT count(*) FROM a2a_access_tokens
		WHERE tenant_id=$1`, tenantID).Scan(&before); err != nil {
		t.Fatal(err)
	}
	var proofID int64
	if err := database.QueryRowContext(t.Context(), `SELECT reauth_token_id
		FROM a2a_access_tokens WHERE tenant_id=$1 AND id=$2`, tenantID, first.ID).
		Scan(&proofID); err != nil {
		t.Fatal(err)
	}
	for _, mutation := range []string{"NULL", "clock_timestamp()+interval '1 minute'"} {
		tx, err := database.BeginTx(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}
		migration139EnterRole(t, tx, tenantID, userID, "admin")
		_, updateErr := tx.ExecContext(t.Context(), `UPDATE account_security_tokens
			SET consumed_at=`+mutation+` WHERE id=$1`, proofID)
		_ = tx.Rollback()
		if updateErr == nil {
			t.Fatalf("restricted role rewrote consumed reauth proof with %s", mutation)
		}
	}
	base.TokenHash = bytes.Repeat([]byte{0x69}, 32)
	if _, err := st.IssueA2AAccessToken(t.Context(), base); types.CodeOf(err) != types.CodeForbidden {
		t.Fatalf("reauth proof replay code=%s err=%v", types.CodeOf(err), err)
	}
	var after int
	if err := database.QueryRowContext(t.Context(), `SELECT count(*) FROM a2a_access_tokens
		WHERE tenant_id=$1`, tenantID).Scan(&after); err != nil || after != before {
		t.Fatalf("reauth replay changed token rows before=%d after=%d err=%v", before, after, err)
	}
	migration139AssertEventCount(t, database, tenantID, first.ID, "issued", 1)
}

// migration139AssertDefinerRejectsTemporaryAuthorityShadows proves the
// SECURITY DEFINER issuance primitive resolves every authority relation in
// public, even when an application role owns same-named pg_temp relations.
// The real proof is deliberately consumed by logout-all and its real session
// is gone; only the forged temporary rows claim that issuance is authorized.
func migration139AssertDefinerRejectsTemporaryAuthorityShadows(t *testing.T,
	database *sql.DB, st *Store, tenantID, userID int64,
) {
	t.Helper()
	sessionHash, proofHash := migration139Reauth(t, st, tenantID, userID,
		"temporary-authority-shadow")
	if _, err := st.LogoutAllWithReauth(t.Context(), tenantID, userID,
		sessionHash, proofHash); err != nil {
		t.Fatal(err)
	}
	var proofID, generation int64
	if err := database.QueryRowContext(t.Context(), `SELECT id
		FROM account_security_tokens WHERE token_hash=$1`, proofHash).Scan(&proofID); err != nil {
		t.Fatal(err)
	}
	if err := database.QueryRowContext(t.Context(), `SELECT authorization_generation
		FROM memberships WHERE tenant_id=$1 AND user_id=$2`, tenantID, userID).
		Scan(&generation); err != nil {
		t.Fatal(err)
	}
	var realSessions int
	if err := database.QueryRowContext(t.Context(), `SELECT count(*) FROM user_sessions
		WHERE token_hash=$1 AND tenant_id=$2 AND user_id=$3`, sessionHash, tenantID,
		userID).Scan(&realSessions); err != nil || realSessions != 0 {
		t.Fatalf("logout-all retained real session count=%d err=%v", realSessions, err)
	}

	tx, err := database.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for _, statement := range []string{
		`SET LOCAL ROLE vane_app`,
		`CREATE TEMP TABLE memberships(
			tenant_id bigint,user_id bigint,role text,authorization_generation bigint)`,
		`CREATE TEMP TABLE tenants(id bigint,status text,deleted_at timestamptz)`,
		`CREATE TEMP TABLE account_security_tokens(
			id bigint,token_hash bytea,token_kind text,tenant_id bigint,user_id bigint,
			session_token_hash bytea,consumed_at timestamptz,expires_at timestamptz)`,
		`CREATE TEMP TABLE user_sessions(
			token_hash bytea,tenant_id bigint,user_id bigint,expires_at timestamptz)`,
	} {
		if _, err := tx.ExecContext(t.Context(), statement); err != nil {
			t.Fatal(err)
		}
	}
	for _, query := range []struct {
		statement string
		args      []any
	}{
		{`INSERT INTO pg_temp.memberships VALUES($1,$2,'owner',$3)`,
			[]any{tenantID, userID, generation}},
		{`INSERT INTO pg_temp.tenants VALUES($1,'active',NULL)`, []any{tenantID}},
		{`INSERT INTO pg_temp.account_security_tokens
			VALUES($1,$2,'reauth',$3,$4,$5,NULL,clock_timestamp()+interval '10 minutes')`,
			[]any{proofID, proofHash, tenantID, userID, sessionHash}},
		{`INSERT INTO pg_temp.user_sessions
			VALUES($1,$2,$3,clock_timestamp()+interval '1 hour')`,
			[]any{sessionHash, tenantID, userID}},
		{`SELECT set_config('app.tenant_id',$1,true),
			set_config('app.user_id',$2,true)`, []any{fmt.Sprint(tenantID), fmt.Sprint(userID)}},
	} {
		if _, err := tx.ExecContext(t.Context(), query.statement, query.args...); err != nil {
			t.Fatal(err)
		}
	}
	shadowTokenID := uuid.NewString()
	_, err = tx.ExecContext(t.Context(), `SELECT issue_a2a_access_token_v139(
		$1,$2,$3,$4,'user','',ARRAY['content.query']::text[],$4,$5,$6,$7,
		clock_timestamp()+interval '1 hour')`, shadowTokenID,
		bytes.Repeat([]byte{0x6a}, 32), tenantID, userID, generation, proofHash,
		sessionHash)
	if err == nil {
		t.Fatal("SECURITY DEFINER issuance trusted pg_temp authority shadows")
	}
	_ = tx.Rollback()
	var durable int
	if err := database.QueryRowContext(t.Context(), `SELECT count(*)
		FROM public.a2a_access_tokens WHERE id=$1`, shadowTokenID).Scan(&durable); err != nil || durable != 0 {
		t.Fatalf("temporary authority attack persisted token count=%d err=%v", durable, err)
	}
}

func migration139AssertDownAdmissionFence(t *testing.T, database *sql.DB,
	tenantID, userID int64, tokenID string, down func() error,
) {
	t.Helper()
	purgeTx, err := database.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer purgeTx.Rollback()
	if _, err := purgeTx.ExecContext(t.Context(), `SELECT
		pg_advisory_xact_lock(hashtextextended('vane/tenant-admission/v1/'||($1::bigint)::text,1447120453)),
		pg_advisory_xact_lock(hashtextextended('vane/a2a-schema/v139',1447120453))`,
		tenantID); err != nil {
		t.Fatal(err)
	}
	var locked string
	if err := purgeTx.QueryRowContext(t.Context(), `SELECT id::text FROM a2a_access_tokens
		WHERE tenant_id=$1 AND id=$2 FOR UPDATE`, tenantID, tokenID).Scan(&locked); err != nil {
		t.Fatal(err)
	}
	downDone := make(chan error, 1)
	go func() { downDone <- down() }()
	deadline := time.Now().Add(3 * time.Second)
	for {
		var waiting int
		if err := database.QueryRowContext(t.Context(), `SELECT count(*) FROM pg_locks
			WHERE locktype='advisory' AND NOT granted`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("migration 139 Down never joined the schema admission barrier")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := purgeTx.ExecContext(t.Context(), `SET LOCAL lock_timeout='500ms'`); err != nil {
		t.Fatal(err)
	}
	if err := purgeTx.QueryRowContext(t.Context(), `SELECT role FROM memberships
		WHERE tenant_id=$1 AND user_id=$2 FOR UPDATE`, tenantID, userID).Scan(&locked); err != nil {
		t.Fatalf("Down held membership while waiting on purge admission: %v", err)
	}
	if err := purgeTx.Rollback(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-downDone:
		if err == nil || !strings.Contains(err.Error(), "139 down refused") {
			t.Fatalf("Down after admission release returned %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Down did not finish after purge admission released")
	}
}

func migration139EnterRole(t *testing.T, tx *sql.Tx, tenantID, userID int64,
	role string,
) {
	t.Helper()
	if _, err := tx.ExecContext(t.Context(), `SELECT
		set_config('app.tenant_id',$1,true),set_config('app.user_id',$2,true),
		set_config('app.membership_role',$3,true)`, fmt.Sprint(tenantID),
		fmt.Sprint(userID), role); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `SET LOCAL ROLE vane_app`); err != nil {
		t.Fatal(err)
	}
}

func migration139AssertEventCount(t *testing.T, database *sql.DB, tenantID int64,
	tokenID, kind string, want int,
) {
	t.Helper()
	var count int
	if err := database.QueryRowContext(t.Context(), `SELECT count(*)
		FROM a2a_access_token_events WHERE tenant_id=$1 AND token_id=$2 AND event_kind=$3`,
		tenantID, tokenID, kind).Scan(&count); err != nil || count != want {
		t.Fatalf("event %s count=%d want=%d err=%v", kind, count, want, err)
	}
}

func migration139AssertRevocationImmutable(t *testing.T, database *sql.DB,
	tenantID, ownerID int64, tokenID string,
) {
	t.Helper()
	tx, err := database.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	migration139EnterRole(t, tx, tenantID, ownerID, "owner")
	if _, err := tx.ExecContext(t.Context(), `UPDATE a2a_access_tokens
		SET revoked_at=NULL WHERE tenant_id=$1 AND id=$2`, tenantID, tokenID); err == nil {
		t.Fatal("restricted role cleared an append-only revocation")
	}
}

func TestCanonicalA2AScopesRejectsRemoteOrDuplicateAuthority(t *testing.T) {
	for _, scopes := range [][]types.A2AScope{
		{}, {types.A2AScope("tools.write")},
		{types.A2AScopeAssistantChat, types.A2AScopeAssistantChat},
	} {
		if _, err := canonicalA2AScopes(scopes); err == nil {
			t.Fatalf("invalid scopes accepted: %v", scopes)
		}
	}
	canonical, err := canonicalA2AScopes([]types.A2AScope{
		types.A2AScopeContentQuery, types.A2AScopeAssistantChat,
	})
	if err != nil || canonical[0] != types.A2AScopeAssistantChat {
		t.Fatalf("canonical scopes=%v err=%v", canonical, err)
	}
}
