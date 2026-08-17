package store

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/YouToco/vane/server/types"
)

const (
	installationBootstrapSettingKey = "installation_bootstrap_v1"
	installationBootstrapLockKey    = int64(0x56414e4553455455)
	installationBootstrapDigestSize = 32
)

type installationBootstrapRecord struct {
	State           string `json:"state"`
	TokenSHA256     string `json:"token_sha256,omitempty"`
	IssuedAt        string `json:"issued_at,omitempty"`
	ExpiresAt       string `json:"expires_at,omitempty"`
	ClaimedAt       string `json:"claimed_at,omitempty"`
	ClaimedByUserID int64  `json:"claimed_by_user_id,omitempty"`
}

// InstallationBootstrapPreparation is the durable result of reconciling the
// host-local one-time token with the database authority.
type InstallationBootstrapPreparation struct {
	SetupRequired bool
	TokenAccepted bool
	ExpiresAt     time.Time
}

// InstallationSetupRequired returns true only while tenant 1 has never gained
// an owner. Suspended/deleted tenants intentionally still count as installed:
// a public visitor must never be able to bypass an operator suspension by
// creating a replacement platform owner.
func (s *Store) InstallationSetupRequired(ctx context.Context) (bool, error) {
	var required bool
	err := s.pool.QueryRow(ctx, `
		SELECT NOT EXISTS (
			SELECT 1
			  FROM memberships
			 WHERE tenant_id = $1 AND role = $2
		)`, int64(types.SingleTenantID), types.MembershipRoleOwner).Scan(&required)
	if err != nil {
		return false, types.NewAppError(types.CodeDatabase,
			"检查安装初始化状态", err)
	}
	return required, nil
}

// PrepareInstallationBootstrap serializes all bootstrap writers with the
// claim transaction. The database stores only SHA-256(token); the raw token is
// host-local and mode 0600. A matching expired token is not extended, forcing
// the caller to rotate the host file instead of making an old token immortal
// through restarts.
func (s *Store) PrepareInstallationBootstrap(
	ctx context.Context,
	tokenDigest []byte,
	expiresAt time.Time,
) (InstallationBootstrapPreparation, error) {
	if len(tokenDigest) != installationBootstrapDigestSize ||
		!expiresAt.After(time.Now()) {
		return InstallationBootstrapPreparation{}, types.NewAppError(
			types.CodeValidation, "初始化令牌摘要或有效期无效", nil)
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return InstallationBootstrapPreparation{}, types.NewAppError(
			types.CodeDatabase, "开启安装初始化事务", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`,
		installationBootstrapLockKey); err != nil {
		return InstallationBootstrapPreparation{}, types.NewAppError(
			types.CodeDatabase, "锁定安装初始化 authority", err)
	}

	hasOwner, err := txHasPlatformOwner(ctx, tx)
	if err != nil {
		return InstallationBootstrapPreparation{}, err
	}
	if hasOwner {
		if err := tx.Commit(ctx); err != nil {
			return InstallationBootstrapPreparation{}, types.NewAppError(
				types.CodeDatabase, "提交安装状态检查", err)
		}
		return InstallationBootstrapPreparation{SetupRequired: false}, nil
	}

	var raw []byte
	var dbNow time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&dbNow); err != nil {
		return InstallationBootstrapPreparation{}, types.NewAppError(
			types.CodeDatabase, "读取数据库时间", err)
	}
	err = tx.QueryRow(ctx, `
		SELECT value
		  FROM settings
		 WHERE key = $1
		 FOR UPDATE`, installationBootstrapSettingKey).Scan(&raw)
	if err == nil {
		record, parseErr := parseInstallationBootstrapRecord(raw)
		if parseErr != nil {
			return InstallationBootstrapPreparation{}, parseErr
		}
		storedDigest, digestErr := hex.DecodeString(record.TokenSHA256)
		storedExpiry, expiryErr := time.Parse(time.RFC3339Nano, record.ExpiresAt)
		if record.State == "pending" && digestErr == nil && expiryErr == nil &&
			subtle.ConstantTimeCompare(storedDigest, tokenDigest) == 1 {
			if !storedExpiry.After(dbNow) {
				if err := tx.Commit(ctx); err != nil {
					return InstallationBootstrapPreparation{}, types.NewAppError(
						types.CodeDatabase, "提交过期初始化状态", err)
				}
				return InstallationBootstrapPreparation{
					SetupRequired: true, TokenAccepted: false, ExpiresAt: storedExpiry,
				}, nil
			}
			if err := tx.Commit(ctx); err != nil {
				return InstallationBootstrapPreparation{}, types.NewAppError(
					types.CodeDatabase, "提交安装初始化检查", err)
			}
			return InstallationBootstrapPreparation{
				SetupRequired: true, TokenAccepted: true, ExpiresAt: storedExpiry,
			}, nil
		}
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return InstallationBootstrapPreparation{}, types.NewAppError(
			types.CodeDatabase, "读取安装初始化 authority", err)
	}

	record := installationBootstrapRecord{
		State:       "pending",
		TokenSHA256: hex.EncodeToString(tokenDigest),
		IssuedAt:    dbNow.UTC().Format(time.RFC3339Nano),
		ExpiresAt:   expiresAt.UTC().Format(time.RFC3339Nano),
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		return InstallationBootstrapPreparation{}, types.NewAppError(
			types.CodeInternal, "编码安装初始化 authority", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO settings(key, value, updated_at)
		VALUES ($1, $2, now())
		ON CONFLICT (key) DO UPDATE
		SET value = EXCLUDED.value, updated_at = now()`,
		installationBootstrapSettingKey, encoded); err != nil {
		return InstallationBootstrapPreparation{}, types.NewAppError(
			types.CodeDatabase, "写入安装初始化 authority", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return InstallationBootstrapPreparation{}, types.NewAppError(
			types.CodeDatabase, "提交安装初始化 authority", err)
	}
	return InstallationBootstrapPreparation{
		SetupRequired: true, TokenAccepted: true, ExpiresAt: expiresAt.UTC(),
	}, nil
}

// InstallationBootstrapTokenUsable is a cheap precheck before Argon2. Claim
// repeats every check under the same transaction lock; this method controls
// cost only and is never the correctness authority.
func (s *Store) InstallationBootstrapTokenUsable(
	ctx context.Context,
	tokenDigest []byte,
) (bool, error) {
	if len(tokenDigest) != installationBootstrapDigestSize {
		return false, nil
	}
	required, err := s.InstallationSetupRequired(ctx)
	if err != nil || !required {
		return false, err
	}
	var raw []byte
	var dbNow time.Time
	err = s.pool.QueryRow(ctx, `
		SELECT value, clock_timestamp()
		  FROM settings
		 WHERE key = $1`, installationBootstrapSettingKey).Scan(&raw, &dbNow)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, types.NewAppError(types.CodeDatabase,
			"读取安装初始化 authority", err)
	}
	record, err := parseInstallationBootstrapRecord(raw)
	if err != nil {
		return false, err
	}
	storedDigest, digestErr := hex.DecodeString(record.TokenSHA256)
	storedExpiry, expiryErr := time.Parse(time.RFC3339Nano, record.ExpiresAt)
	return record.State == "pending" && digestErr == nil && expiryErr == nil &&
		storedExpiry.After(dbNow) &&
		subtle.ConstantTimeCompare(storedDigest, tokenDigest) == 1, nil
}

// ClaimInstallationBootstrap atomically creates the first platform account,
// grants the exact tenant-1 owner membership, creates the first hash-only
// session, and consumes the token. No invite or personal tenant is created by
// this appliance-only path.
func (s *Store) ClaimInstallationBootstrap(
	ctx context.Context,
	tokenDigest []byte,
	email string,
	passwordHash string,
	sessionHash []byte,
	sessionExpiresAt time.Time,
) (*types.User, error) {
	email = NormalizeEmail(email)
	if len(tokenDigest) != installationBootstrapDigestSize || email == "" ||
		passwordHash == "" || len(sessionHash) != sha256.Size ||
		!sessionExpiresAt.After(time.Now()) {
		return nil, types.NewAppError(types.CodeValidation,
			"初始化参数无效", nil)
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase,
			"开启安装初始化事务", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`,
		installationBootstrapLockKey); err != nil {
		return nil, types.NewAppError(types.CodeDatabase,
			"锁定安装初始化 authority", err)
	}
	hasOwner, err := txHasPlatformOwner(ctx, tx)
	if err != nil {
		return nil, err
	}
	if hasOwner {
		return nil, types.NewAppError(types.CodeConflict,
			"Vane 已完成初始化", nil)
	}

	var tenantStatus string
	if err := tx.QueryRow(ctx, `SELECT status FROM tenants WHERE id = $1 FOR UPDATE`,
		int64(types.SingleTenantID)).Scan(&tenantStatus); err != nil {
		return nil, types.NewAppError(types.CodeDatabase,
			"锁定平台租户", err)
	}
	if tenantStatus != "active" {
		return nil, types.NewAppError(types.CodeConflict,
			"平台租户当前不可初始化，请由运维人员恢复", nil)
	}

	var raw []byte
	var dbNow time.Time
	if err := tx.QueryRow(ctx, `
		SELECT value, clock_timestamp()
		  FROM settings
		 WHERE key = $1
		 FOR UPDATE`, installationBootstrapSettingKey).Scan(&raw, &dbNow); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, types.NewAppError(types.CodeValidation,
				"初始化令牌无效或已失效", nil)
		}
		return nil, types.NewAppError(types.CodeDatabase,
			"锁定安装初始化 authority", err)
	}
	record, err := parseInstallationBootstrapRecord(raw)
	if err != nil {
		return nil, err
	}
	storedDigest, digestErr := hex.DecodeString(record.TokenSHA256)
	storedExpiry, expiryErr := time.Parse(time.RFC3339Nano, record.ExpiresAt)
	if record.State != "pending" || digestErr != nil || expiryErr != nil ||
		!storedExpiry.After(dbNow) ||
		subtle.ConstantTimeCompare(storedDigest, tokenDigest) != 1 {
		return nil, types.NewAppError(types.CodeValidation,
			"初始化令牌无效或已失效", nil)
	}

	var user types.User
	err = tx.QueryRow(ctx, `
		INSERT INTO users(email, password_hash, name)
		VALUES ($1, $2, '')
		RETURNING id, feishu_open_id, name, created_at, email, password_hash, email_verified`,
		email, passwordHash).Scan(
		&user.ID, &user.FeishuOpenID, &user.Name, &user.CreatedAt,
		&user.Email, &user.PasswordHash, &user.EmailVerified)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation {
			return nil, types.NewAppError(types.CodeConflict, "该邮箱已注册", err)
		}
		return nil, types.NewAppError(types.CodeDatabase, "创建平台 owner", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO memberships(tenant_id, user_id, role)
		VALUES ($1, $2, $3)`, int64(types.SingleTenantID), user.ID,
		types.MembershipRoleOwner); err != nil {
		return nil, types.NewAppError(types.CodeDatabase,
			"创建平台 owner 成员关系", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO user_sessions(token_hash, user_id, tenant_id, expires_at)
		VALUES ($1, $2, $3, $4)`, sessionHash, user.ID,
		int64(types.SingleTenantID), sessionExpiresAt); err != nil {
		return nil, types.NewAppError(types.CodeDatabase,
			"创建平台 owner 首个会话", err)
	}
	claimed := installationBootstrapRecord{
		State:           "claimed",
		ClaimedAt:       dbNow.UTC().Format(time.RFC3339Nano),
		ClaimedByUserID: user.ID,
	}
	encoded, err := json.Marshal(claimed)
	if err != nil {
		return nil, types.NewAppError(types.CodeInternal,
			"编码安装初始化回执", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE settings SET value = $2, updated_at = now() WHERE key = $1`,
		installationBootstrapSettingKey, encoded); err != nil {
		return nil, types.NewAppError(types.CodeDatabase,
			"消费安装初始化令牌", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, types.NewAppError(types.CodeDatabase,
			"提交安装初始化", err)
	}
	return &user, nil
}

func txHasPlatformOwner(ctx context.Context, tx pgx.Tx) (bool, error) {
	var exists bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM memberships
			 WHERE tenant_id = $1 AND role = $2
		)`, int64(types.SingleTenantID), types.MembershipRoleOwner).Scan(&exists); err != nil {
		return false, types.NewAppError(types.CodeDatabase,
			"检查平台 owner", err)
	}
	return exists, nil
}

func parseInstallationBootstrapRecord(raw []byte) (installationBootstrapRecord, error) {
	var record installationBootstrapRecord
	if len(raw) == 0 || json.Unmarshal(raw, &record) != nil {
		return installationBootstrapRecord{}, types.NewAppError(
			types.CodeDatabase, "安装初始化 authority 已损坏", fmt.Errorf("invalid bootstrap record"))
	}
	return record, nil
}
