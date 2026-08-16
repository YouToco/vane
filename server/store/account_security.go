package store

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/server/types"
)

const accountTokenHourlyLimit = 3

type AccountSecurityIdentity struct {
	UserID        int64
	TenantID      int64
	Email         string
	PasswordHash  string
	EmailVerified bool
}

func setAccountSecurityScope(ctx context.Context, tx pgx.Tx, tenantID, userID int64) error {
	if tenantID <= 0 || userID <= 0 {
		return types.NewAppError(types.CodeValidation, "账号安全范围无效", nil)
	}
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.tenant_id',$1,true),set_config('app.user_id',$2,true)`,
		fmt.Sprint(tenantID), fmt.Sprint(userID)); err != nil {
		return types.NewAppError(types.CodeDatabase, "设置账号安全范围", err)
	}
	return nil
}

func setAccountSecurityTokenBootstrap(ctx context.Context, tx pgx.Tx, tokenHash []byte) error {
	if len(tokenHash) != 32 {
		return types.NewAppError(types.CodeValidation, "安全令牌摘要无效", nil)
	}
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.account_security_token_hash',$1,true)`, hex.EncodeToString(tokenHash)); err != nil {
		return types.NewAppError(types.CodeDatabase, "设置安全令牌范围", err)
	}
	return nil
}

func (s *Store) GetAccountSecurityIdentity(ctx context.Context, tenantID, userID int64) (*AccountSecurityIdentity, error) {
	var out AccountSecurityIdentity
	err := s.pool.QueryRow(ctx,
		`SELECT u.id,m.tenant_id,COALESCE(u.email,''),COALESCE(u.password_hash,''),u.email_verified
		   FROM users u JOIN memberships m ON m.user_id=u.id
		  WHERE u.id=$1 AND m.tenant_id=$2`, userID, tenantID).
		Scan(&out.UserID, &out.TenantID, &out.Email, &out.PasswordHash, &out.EmailVerified)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, types.NewAppError(types.CodeNotFound, "账号不存在", err)
	}
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "查询账号安全身份", err)
	}
	return &out, nil
}

func (s *Store) issueAccountToken(ctx context.Context, tenantID, userID int64, kind string,
	tokenHash, sessionHash []byte, expiresAt time.Time, eventType string) (int64, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, types.NewAppError(types.CodeDatabase, "开启安全令牌事务", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := setAccountSecurityScope(ctx, tx, tenantID, userID); err != nil {
		return 0, err
	}
	var member bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM memberships WHERE tenant_id=$1 AND user_id=$2)`,
		tenantID, userID).Scan(&member); err != nil {
		return 0, types.NewAppError(types.CodeDatabase, "校验账号工作区", err)
	}
	if !member {
		return 0, types.NewAppError(types.CodeNotFound, "账号不存在", nil)
	}
	var recent int
	if err := tx.QueryRow(ctx,
		`SELECT count(*) FROM account_security_tokens
		  WHERE tenant_id=$1 AND user_id=$2 AND token_kind=$3
		    AND created_at > clock_timestamp()-interval '1 hour'`, tenantID, userID, kind).
		Scan(&recent); err != nil {
		return 0, types.NewAppError(types.CodeDatabase, "检查安全令牌频率", err)
	}
	if recent >= accountTokenHourlyLimit {
		return 0, types.NewAppError(types.CodeConflict, "安全邮件请求过于频繁，请稍后再试", nil)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE account_security_tokens SET consumed_at=clock_timestamp()
		  WHERE tenant_id=$1 AND user_id=$2 AND token_kind=$3 AND consumed_at IS NULL`,
		tenantID, userID, kind); err != nil {
		return 0, types.NewAppError(types.CodeDatabase, "使旧安全令牌失效", err)
	}
	var tokenID int64
	if err := tx.QueryRow(ctx,
		`INSERT INTO account_security_tokens
		 (tenant_id,user_id,token_hash,token_kind,session_token_hash,expires_at)
		 VALUES($1,$2,$3,$4,$5,$6) RETURNING id`,
		tenantID, userID, tokenHash, kind, nullableHash(sessionHash), expiresAt).Scan(&tokenID); err != nil {
		return 0, types.NewAppError(types.CodeDatabase, "签发安全令牌", err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO account_security_audit_events(tenant_id,user_id,token_id,event_type)
		 VALUES($1,$2,$3,$4)`, tenantID, userID, tokenID, eventType); err != nil {
		return 0, types.NewAppError(types.CodeDatabase, "记录安全令牌事件", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, types.NewAppError(types.CodeDatabase, "提交安全令牌事务", err)
	}
	return tokenID, nil
}

func nullableHash(value []byte) any {
	if len(value) == 0 {
		return nil
	}
	return value
}

func (s *Store) IssueEmailVerification(ctx context.Context, tenantID, userID int64,
	tokenHash []byte, expiresAt time.Time) (string, bool, error) {
	identity, err := s.GetAccountSecurityIdentity(ctx, tenantID, userID)
	if err != nil {
		return "", false, err
	}
	if identity.Email == "" || identity.PasswordHash == "" {
		return "", false, types.NewAppError(types.CodeConflict, "该账号没有邮箱身份", nil)
	}
	if identity.EmailVerified {
		return identity.Email, false, nil
	}
	_, err = s.issueAccountToken(ctx, tenantID, userID, "email_verification", tokenHash, nil,
		expiresAt, "email_verification_issued")
	return identity.Email, err == nil, err
}

func (s *Store) IssuePasswordReset(ctx context.Context, email string, tokenHash []byte,
	expiresAt time.Time) (string, bool, error) {
	email = NormalizeEmail(email)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return "", false, types.NewAppError(types.CodeDatabase, "开启密码重置查询", err)
	}
	var identity AccountSecurityIdentity
	err = tx.QueryRow(ctx,
		`SELECT u.id,m.tenant_id,u.email,u.password_hash,u.email_verified
		   FROM users u
		   JOIN memberships m ON m.user_id=u.id
		   JOIN tenants t ON t.id=m.tenant_id AND t.status='active' AND t.deleted_at IS NULL
		  WHERE lower(u.email)=$1
		  ORDER BY (t.personal_owner_user_id=u.id) DESC,t.id
		  LIMIT 1 FOR UPDATE OF u`, email).
		Scan(&identity.UserID, &identity.TenantID, &identity.Email, &identity.PasswordHash,
			&identity.EmailVerified)
	_ = tx.Rollback(ctx)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, types.NewAppError(types.CodeDatabase, "查询密码重置账号", err)
	}
	_, err = s.issueAccountToken(ctx, identity.TenantID, identity.UserID, "password_reset",
		tokenHash, nil, expiresAt, "password_reset_issued")
	return identity.Email, err == nil, err
}

func (s *Store) consumeToken(ctx context.Context, tokenHash []byte, expectedKind string,
	fn func(pgx.Tx, int64, int64, int64) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return types.NewAppError(types.CodeDatabase, "开启安全令牌消费事务", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := setAccountSecurityTokenBootstrap(ctx, tx, tokenHash); err != nil {
		return err
	}
	var tokenID, tenantID, userID int64
	err = tx.QueryRow(ctx,
		`SELECT id,tenant_id,user_id FROM account_security_tokens
		  WHERE token_hash=$1 AND token_kind=$2 AND consumed_at IS NULL
		    AND expires_at > clock_timestamp() FOR UPDATE`, tokenHash, expectedKind).
		Scan(&tokenID, &tenantID, &userID)
	if errors.Is(err, pgx.ErrNoRows) {
		return types.NewAppError(types.CodeNotFound, "安全令牌无效或已过期", err)
	}
	if err != nil {
		return types.NewAppError(types.CodeDatabase, "读取安全令牌", err)
	}
	if err := setAccountSecurityScope(ctx, tx, tenantID, userID); err != nil {
		return err
	}
	if fn != nil {
		if err := fn(tx, tokenID, tenantID, userID); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(ctx,
		`UPDATE account_security_tokens SET consumed_at=clock_timestamp()
		  WHERE id=$1 AND consumed_at IS NULL`, tokenID); err != nil {
		return types.NewAppError(types.CodeDatabase, "消费安全令牌", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return types.NewAppError(types.CodeDatabase, "提交安全令牌消费", err)
	}
	return nil
}

func (s *Store) VerifyEmailWithToken(ctx context.Context, tokenHash []byte) error {
	return s.consumeToken(ctx, tokenHash, "email_verification",
		func(tx pgx.Tx, tokenID, tenantID, userID int64) error {
			tag, err := tx.Exec(ctx, `UPDATE users SET email_verified=true WHERE id=$1`, userID)
			if err != nil || tag.RowsAffected() != 1 {
				return types.NewAppError(types.CodeDatabase, "验证邮箱", err)
			}
			_, err = tx.Exec(ctx,
				`INSERT INTO account_security_audit_events(tenant_id,user_id,token_id,event_type)
				 VALUES($1,$2,$3,'email_verified')`, tenantID, userID, tokenID)
			if err != nil {
				return types.NewAppError(types.CodeDatabase, "记录邮箱验证", err)
			}
			return nil
		})
}

// PasswordResetTokenUsable is a cheap preflight before Argon2 hashing. The
// consuming UPDATE remains authoritative; this check exists only to keep
// random unauthenticated tokens from turning the reset endpoint into a CPU and
// memory amplifier.
func (s *Store) PasswordResetTokenUsable(ctx context.Context, tokenHash []byte) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, types.NewAppError(types.CodeDatabase, "开启密码重置预检", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := setAccountSecurityTokenBootstrap(ctx, tx, tokenHash); err != nil {
		return false, err
	}
	var usable bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM account_security_tokens
		 WHERE token_hash=$1 AND token_kind='password_reset' AND consumed_at IS NULL
		   AND expires_at>clock_timestamp())`, tokenHash).Scan(&usable); err != nil {
		return false, types.NewAppError(types.CodeDatabase, "预检密码重置令牌", err)
	}
	return usable, nil
}

func (s *Store) ResetPasswordWithToken(ctx context.Context, tokenHash []byte, passwordHash string) error {
	if passwordHash == "" {
		return types.NewAppError(types.CodeValidation, "密码哈希不能为空", nil)
	}
	return s.consumeToken(ctx, tokenHash, "password_reset",
		func(tx pgx.Tx, tokenID, tenantID, userID int64) error {
			if _, err := tx.Exec(ctx, `UPDATE users SET password_hash=$2 WHERE id=$1`, userID, passwordHash); err != nil {
				return types.NewAppError(types.CodeDatabase, "重置密码", err)
			}
			if _, err := tx.Exec(ctx, `DELETE FROM user_sessions WHERE user_id=$1`, userID); err != nil {
				return types.NewAppError(types.CodeDatabase, "撤销账号会话", err)
			}
			_, err := tx.Exec(ctx,
				`INSERT INTO account_security_audit_events(tenant_id,user_id,token_id,event_type)
				 VALUES($1,$2,$3,'password_reset_completed')`, tenantID, userID, tokenID)
			if err != nil {
				return types.NewAppError(types.CodeDatabase, "记录密码重置", err)
			}
			return nil
		})
}

func (s *Store) IssueReauthProof(ctx context.Context, tenantID, userID int64, sessionHash,
	proofHash []byte, expiresAt time.Time) error {
	if len(sessionHash) != 32 {
		return types.NewAppError(types.CodeValidation, "会话摘要无效", nil)
	}
	var valid bool
	if err := s.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM user_sessions
		 WHERE token_hash=$1 AND tenant_id=$2 AND user_id=$3 AND expires_at>clock_timestamp())`,
		sessionHash, tenantID, userID).Scan(&valid); err != nil {
		return types.NewAppError(types.CodeDatabase, "校验当前会话", err)
	}
	if !valid {
		return types.NewAppError(types.CodeNotFound, "当前会话不存在", nil)
	}
	_, err := s.issueAccountToken(ctx, tenantID, userID, "reauth", proofHash, sessionHash,
		expiresAt, "reauth_issued")
	return err
}

func (s *Store) LogoutAllWithReauth(ctx context.Context, tenantID, userID int64,
	sessionHash, proofHash []byte) (int64, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, types.NewAppError(types.CodeDatabase, "开启全设备登出事务", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := setAccountSecurityScope(ctx, tx, tenantID, userID); err != nil {
		return 0, err
	}
	var tokenID int64
	err = tx.QueryRow(ctx,
		`SELECT id FROM account_security_tokens
		 WHERE token_hash=$1 AND token_kind='reauth' AND tenant_id=$2 AND user_id=$3
		   AND session_token_hash=$4 AND consumed_at IS NULL
		   AND expires_at>clock_timestamp() FOR UPDATE`, proofHash, tenantID, userID, sessionHash).
		Scan(&tokenID)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, types.NewAppError(types.CodeForbidden, "需要重新验证身份", err)
	}
	if err != nil {
		return 0, types.NewAppError(types.CodeDatabase, "校验重新认证证明", err)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE account_security_tokens SET consumed_at=clock_timestamp() WHERE id=$1`, tokenID); err != nil {
		return 0, types.NewAppError(types.CodeDatabase, "消费重新认证证明", err)
	}
	tag, err := tx.Exec(ctx, `DELETE FROM user_sessions WHERE user_id=$1`, userID)
	if err != nil {
		return 0, types.NewAppError(types.CodeDatabase, "撤销全部会话", err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO account_security_audit_events(tenant_id,user_id,token_id,event_type)
		 VALUES($1,$2,$3,'logout_all_completed')`, tenantID, userID, tokenID); err != nil {
		return 0, types.NewAppError(types.CodeDatabase, "记录全设备登出", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, types.NewAppError(types.CodeDatabase, "提交全设备登出", err)
	}
	return tag.RowsAffected(), nil
}
