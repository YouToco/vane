package store

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/YouToco/vane/server/internal/credentialguard"
	"github.com/YouToco/vane/server/types"
)

const maxActiveA2ATokensPerPrincipal = 20

func (s *Store) IssueA2AAccessToken(ctx context.Context, input types.IssueA2AAccessToken) (*types.A2AAccessToken, error) {
	scopes, err := canonicalA2AScopes(input.Scopes)
	if err != nil || input.TenantID <= 0 || input.ActorUserID <= 0 || input.PrincipalUserID <= 0 ||
		len(input.TokenHash) != 32 || len(input.SessionTokenHash) != 32 ||
		len(input.ReauthProofHash) != 32 || !input.ActorType.Valid() {
		return nil, a2aAccessValidation("A2A token request is invalid")
	}
	if input.PrincipalUserID != input.ActorUserID {
		return nil, a2aAccessForbidden("A2A credentials cannot impersonate another workspace member")
	}
	label := strings.TrimSpace(input.ServiceAccountLabel)
	if input.ActorType == types.ActorTypeUser {
		if label != "" {
			return nil, a2aAccessForbidden("user tokens may only represent the current user")
		}
	} else if label == "" || len(label) > 128 || !utf8.ValidString(label) ||
		strings.IndexFunc(label, unicode.IsControl) >= 0 || credentialguard.ContainsCredential(label) {
		return nil, a2aAccessValidation("service account label is invalid")
	}

	tx, actorRole, membershipGeneration, now, err := s.beginA2AAccessManagementTx(ctx,
		input.TenantID, input.ActorUserID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if input.ActorType == types.ActorTypeServiceAccount &&
		actorRole != types.MembershipRoleOwner && actorRole != types.MembershipRoleAdmin {
		return nil, a2aAccessForbidden("only workspace owners or admins can issue service account tokens")
	}
	if input.ExpiresAt.Before(now.Add(5*time.Minute)) || input.ExpiresAt.After(now.Add(90*24*time.Hour)) {
		return nil, a2aAccessValidation("A2A token expiry must be between five minutes and 90 days")
	}
	var active int
	if err := tx.QueryRow(ctx, `
		SELECT count(*) FROM a2a_access_tokens
		WHERE tenant_id=$1 AND principal_user_id=$2
		  AND revoked_at IS NULL AND expires_at>clock_timestamp()`,
		input.TenantID, input.PrincipalUserID).Scan(&active); err != nil {
		return nil, a2aAccessDatabase("count active A2A tokens", err)
	}
	if active >= maxActiveA2ATokensPerPrincipal {
		return nil, types.NewAppError(types.CodeConflict,
			"active A2A token limit reached", nil)
	}

	item := &types.A2AAccessToken{ID: uuid.NewString(), TokenHash: append([]byte(nil), input.TokenHash...),
		TenantID: input.TenantID, PrincipalUserID: input.PrincipalUserID,
		ActorType: input.ActorType, ServiceAccountLabel: label, Scopes: scopes,
		IssuedBy: input.ActorUserID, ExpiresAt: input.ExpiresAt}
	if _, err := tx.Exec(ctx, `SELECT issue_a2a_access_token_v139(
		$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
		item.ID, item.TokenHash, item.TenantID, item.PrincipalUserID, item.ActorType,
		item.ServiceAccountLabel, a2aScopeStrings(item.Scopes), item.IssuedBy,
		membershipGeneration, input.ReauthProofHash, input.SessionTokenHash,
		item.ExpiresAt); err != nil {
		return nil, a2aAccessDatabase("issue A2A access token", err)
	}
	if err := scanA2AAccessToken(tx.QueryRow(ctx, `
		SELECT id,token_hash,tenant_id,principal_user_id,actor_type,
		 service_account_label,scopes,issued_by,expires_at,revoked_at,created_at
		FROM a2a_access_tokens WHERE tenant_id=$1 AND id=$2`,
		item.TenantID, item.ID), item); err != nil {
		return nil, a2aAccessDatabase("issue A2A access token", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, a2aAccessDatabase("commit A2A token issue", err)
	}
	return item, nil
}

func (s *Store) ListA2AAccessTokens(ctx context.Context, tenantID, actorUserID int64) ([]types.A2AAccessToken, error) {
	tx, _, _, _, err := s.beginA2AAccessManagementTx(ctx, tenantID, actorUserID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, `
		SELECT id,token_hash,tenant_id,principal_user_id,actor_type,
		 service_account_label,scopes,issued_by,expires_at,revoked_at,created_at
		FROM a2a_access_tokens WHERE tenant_id=$1
		ORDER BY created_at DESC,id`, tenantID)
	if err != nil {
		return nil, a2aAccessDatabase("list A2A access tokens", err)
	}
	defer rows.Close()
	items := make([]types.A2AAccessToken, 0)
	for rows.Next() {
		var item types.A2AAccessToken
		if err := scanA2AAccessToken(rows, &item); err != nil {
			return nil, a2aAccessDatabase("scan A2A access token", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, a2aAccessDatabase("iterate A2A access tokens", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, a2aAccessDatabase("commit A2A token list", err)
	}
	return items, nil
}

func (s *Store) RevokeA2AAccessToken(ctx context.Context, tenantID, actorUserID int64, tokenID string) error {
	if _, err := uuid.Parse(tokenID); err != nil {
		return a2aAccessValidation("A2A token ID is invalid")
	}
	tx, _, _, _, err := s.beginA2AAccessManagementTx(ctx, tenantID, actorUserID)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var item types.A2AAccessToken
	if _, err := tx.Exec(ctx, `SELECT revoke_a2a_access_token_v139($1,$2,$3)`,
		tenantID, tokenID, actorUserID); err != nil {
		return a2aAccessDatabase("revoke A2A access token", err)
	}
	if err := scanA2AAccessToken(tx.QueryRow(ctx, `
		SELECT id,token_hash,tenant_id,principal_user_id,actor_type,
		 service_account_label,scopes,issued_by,expires_at,revoked_at,created_at
		FROM a2a_access_tokens WHERE tenant_id=$1 AND id=$2`, tenantID, tokenID), &item); err != nil {
		return a2aAccessDatabase("read revoked A2A access token", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return a2aAccessDatabase("commit A2A token revocation", err)
	}
	return nil
}

// AuthenticateA2AAccessToken resolves a hash-only bearer into a live
// membership. It deliberately does not update last-used state, avoiding a
// write-amplifying authentication side effect and response-loss ambiguity.
func (s *Store) AuthenticateA2AAccessToken(ctx context.Context, tokenHash []byte) (*types.A2AAuthenticatedPrincipal, error) {
	if len(tokenHash) != 32 {
		return nil, types.NewAppError(types.CodeForbidden, "invalid A2A token", nil)
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, a2aAccessDatabase("begin A2A authentication", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT set_config('app.a2a_token_hash',$1,true)`, fmt.Sprintf("%x", tokenHash)); err != nil {
		return nil, a2aAccessDatabase("set A2A token hash scope", err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_app`); err != nil {
		return nil, a2aAccessDatabase("enter A2A authentication role", err)
	}
	var out types.A2AAuthenticatedPrincipal
	var rawScopes []string
	err = tx.QueryRow(ctx, `
		SELECT token.id,token.tenant_id,token.principal_user_id,
		       CASE WHEN token.actor_type='service_account' THEN 'member' ELSE m.role END,
		       token.actor_type,token.scopes
		FROM a2a_access_tokens token
		JOIN tenants tenant ON tenant.id=token.tenant_id
		JOIN memberships m ON m.tenant_id=token.tenant_id
		 AND m.user_id=token.principal_user_id
		 AND m.authorization_generation=token.membership_generation
		WHERE token.token_hash=$1 AND token.revoked_at IS NULL
		  AND token.expires_at>clock_timestamp()
		  AND tenant.status='active' AND tenant.deleted_at IS NULL`, tokenHash).Scan(
		&out.TokenID, &out.TenantID, &out.UserID, &out.Role, &out.ActorType, &rawScopes)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, types.NewAppError(types.CodeForbidden, "invalid or expired A2A token", nil)
	}
	if err != nil {
		return nil, a2aAccessDatabase("authenticate A2A token", err)
	}
	out.Scopes = make([]types.A2AScope, len(rawScopes))
	for index, scope := range rawScopes {
		out.Scopes[index] = types.A2AScope(scope)
	}
	if !out.Role.Valid() || !out.ActorType.Valid() {
		return nil, types.NewAppError(types.CodeForbidden, "A2A principal is invalid", nil)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, a2aAccessDatabase("commit A2A authentication", err)
	}
	return &out, nil
}

func (s *Store) beginA2AAccessManagementTx(ctx context.Context, tenantID, actorUserID int64) (pgx.Tx, types.MembershipRole, int64, time.Time, error) {
	if tenantID <= 0 || actorUserID <= 0 {
		return nil, "", 0, time.Time{}, a2aAccessValidation("A2A principal is invalid")
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, "", 0, time.Time{}, a2aAccessDatabase("begin A2A token transaction", err)
	}
	exists, err := lockTenantAdmissionRoot(ctx, tx, tenantID)
	if err != nil || !exists {
		_ = tx.Rollback(ctx)
		if err == nil {
			return nil, "", 0, time.Time{}, a2aAccessForbidden("active workspace membership is required")
		}
		return nil, "", 0, time.Time{}, a2aAccessDatabase("lock A2A workspace admission", err)
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(
		hashtextextended('vane/a2a-schema/v139',1447120453))`); err != nil {
		_ = tx.Rollback(ctx)
		return nil, "", 0, time.Time{}, a2aAccessDatabase("lock A2A schema authority", err)
	}
	var actorRole types.MembershipRole
	var membershipGeneration int64
	actorErr := tx.QueryRow(ctx, `
		SELECT m.role,m.authorization_generation
		FROM memberships m JOIN tenants t ON t.id=m.tenant_id
		WHERE m.tenant_id=$1 AND m.user_id=$2
		  AND t.status='active' AND t.deleted_at IS NULL
		FOR UPDATE OF m`, tenantID, actorUserID).Scan(&actorRole, &membershipGeneration)
	if actorErr != nil || !actorRole.Valid() {
		_ = tx.Rollback(ctx)
		if actorErr != nil && !errors.Is(actorErr, pgx.ErrNoRows) {
			return nil, "", 0, time.Time{}, a2aAccessDatabase("lock A2A actor membership", actorErr)
		}
		return nil, "", 0, time.Time{}, a2aAccessForbidden("active workspace membership is required")
	}
	if _, err := tx.Exec(ctx, `
		SELECT set_config('app.tenant_id',$1,true),
		       set_config('app.user_id',$2,true),
		       set_config('app.membership_role',$3,true)`,
		fmt.Sprint(tenantID), fmt.Sprint(actorUserID), string(actorRole)); err != nil {
		_ = tx.Rollback(ctx)
		return nil, "", 0, time.Time{}, a2aAccessDatabase("set A2A management scope", err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_app`); err != nil {
		_ = tx.Rollback(ctx)
		return nil, "", 0, time.Time{}, a2aAccessDatabase("enter A2A management role", err)
	}
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		_ = tx.Rollback(ctx)
		return nil, "", 0, time.Time{}, a2aAccessDatabase("read A2A database clock", err)
	}
	return tx, actorRole, membershipGeneration, now, nil
}

type a2aAccessScanner interface{ Scan(...any) error }

func scanA2AAccessToken(row a2aAccessScanner, item *types.A2AAccessToken) error {
	var actor string
	var rawScopes []string
	if err := row.Scan(&item.ID, &item.TokenHash, &item.TenantID, &item.PrincipalUserID,
		&actor, &item.ServiceAccountLabel, &rawScopes, &item.IssuedBy, &item.ExpiresAt,
		&item.RevokedAt, &item.CreatedAt); err != nil {
		return err
	}
	item.ActorType = types.ActorType(actor)
	item.Scopes = make([]types.A2AScope, len(rawScopes))
	for index, value := range rawScopes {
		item.Scopes[index] = types.A2AScope(value)
	}
	return nil
}

func canonicalA2AScopes(input []types.A2AScope) ([]types.A2AScope, error) {
	if len(input) < 1 || len(input) > 2 {
		return nil, a2aAccessValidation("A2A token scopes are invalid")
	}
	seen := make(map[types.A2AScope]struct{}, len(input))
	for _, scope := range input {
		if !scope.Valid() {
			return nil, a2aAccessValidation("A2A token scope is not locally approved")
		}
		seen[scope] = struct{}{}
	}
	if len(seen) != len(input) {
		return nil, a2aAccessValidation("A2A token scopes contain duplicates")
	}
	result := append([]types.A2AScope(nil), input...)
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result, nil
}

func a2aScopeStrings(scopes []types.A2AScope) []string {
	result := make([]string, len(scopes))
	for index, scope := range scopes {
		result[index] = string(scope)
	}
	return result
}

func a2aAccessValidation(message string) error {
	return types.NewAppError(types.CodeValidation, message, nil)
}
func a2aAccessForbidden(message string) error {
	return types.NewAppError(types.CodeForbidden, message, nil)
}
func a2aAccessDatabase(message string, err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case pgUniqueViolation, "55000":
			return types.NewAppError(types.CodeConflict, message, err)
		case "42501":
			return types.NewAppError(types.CodeForbidden, message, err)
		case "22023":
			return types.NewAppError(types.CodeValidation, message, err)
		case "02000":
			return types.NewAppError(types.CodeNotFound, message, err)
		}
	}
	return types.NewAppError(types.CodeDatabase, message, err)
}
