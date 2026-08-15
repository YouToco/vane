package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/server/credentialvault"
	"github.com/YouToco/vane/server/types"
)

const platformCredentialTenantID int64 = 1

type CredentialScope struct {
	Kind     string `json:"scope_kind"`
	TenantID int64  `json:"tenant_id,omitempty"`
	Provider string `json:"provider"`
	Purpose  string `json:"purpose"`
}

type CredentialMetadata struct {
	CredentialScope
	Generation      int64           `json:"generation"`
	Fingerprint     string          `json:"fingerprint"`
	Metadata        json.RawMessage `json:"metadata"`
	Status          string          `json:"status"`
	CreatedByUserID int64           `json:"created_by_user_id"`
	CreatedAt       time.Time       `json:"created_at"`
	RetiredAt       *time.Time      `json:"retired_at,omitempty"`
	RevokedAt       *time.Time      `json:"revoked_at,omitempty"`
}

type credentialEnvelopeRow struct {
	CredentialMetadata
	EnvelopeVersion string
	KeyID           string
	Nonce           []byte
	Ciphertext      []byte
}

// ConfigureCredentialVault installs an immutable in-process keyring. Key bytes
// come from environment/systemd credentials and never enter a database row.
func (s *Store) ConfigureCredentialVault(activeKeyID, activeKeyHex, retired string) error {
	if s == nil {
		return errors.New("store: credential vault is unavailable")
	}
	config, err := credentialvault.ParseKeyring(activeKeyID, activeKeyHex, retired)
	if err != nil {
		return err
	}
	vault, err := credentialvault.New(config)
	if err != nil {
		return err
	}
	s.credentialVault = vault
	return nil
}

func (s *Store) CredentialVaultReady() bool {
	return s != nil && s.credentialVault != nil
}

// RotateCredential atomically retires the previous active generation and
// inserts a freshly encrypted generation. Authorization is re-proved inside
// the same transaction that mutates the credential row.
func (s *Store) RotateCredential(
	ctx context.Context,
	scope CredentialScope,
	secretJSON, metadata json.RawMessage,
	actorUserID int64,
) (CredentialMetadata, error) {
	if s == nil || s.credentialVault == nil {
		return CredentialMetadata{}, types.NewAppError(types.CodeConflict,
			"凭证库尚未配置", nil)
	}
	if err := validateCredentialInput(scope, secretJSON, metadata, actorUserID); err != nil {
		return CredentialMetadata{}, err
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return CredentialMetadata{}, credentialDBError("开始凭证轮换事务", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := authorizeCredentialActor(ctx, tx, scope, actorUserID); err != nil {
		return CredentialMetadata{}, err
	}
	// All writers for one exact authority share this lock, including revoke.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`,
		credentialLockKey(scope)); err != nil {
		return CredentialMetadata{}, credentialDBError("锁定凭证作用域", err)
	}
	var generation int64
	if err := tx.QueryRow(ctx, `SELECT COALESCE(MAX(generation),0)+1
		FROM credential_vault_entries
		WHERE scope_kind=$1 AND tenant_id IS NOT DISTINCT FROM $2 AND
		      provider=$3 AND purpose=$4`,
		scope.Kind, nullableCredentialTenant(scope), scope.Provider, scope.Purpose,
	).Scan(&generation); err != nil {
		return CredentialMetadata{}, credentialDBError("分配凭证版本", err)
	}
	envelope, err := s.credentialVault.Seal(credentialvault.Scope{
		Kind: scope.Kind, TenantID: scope.TenantID, Provider: scope.Provider,
		Purpose: scope.Purpose, Generation: generation,
	}, secretJSON)
	if err != nil {
		return CredentialMetadata{}, types.NewAppError(types.CodeInternal,
			"加密凭证失败", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE credential_vault_entries
		SET status='retired',retired_at=clock_timestamp()
		WHERE scope_kind=$1 AND tenant_id IS NOT DISTINCT FROM $2 AND
		      provider=$3 AND purpose=$4 AND status='active'`,
		scope.Kind, nullableCredentialTenant(scope), scope.Provider, scope.Purpose,
	); err != nil {
		return CredentialMetadata{}, credentialDBError("归档旧凭证", err)
	}
	var result CredentialMetadata
	result.CredentialScope = scope
	err = tx.QueryRow(ctx, `INSERT INTO credential_vault_entries(
		scope_kind,tenant_id,provider,purpose,generation,envelope_version,key_id,
		nonce,ciphertext,fingerprint,metadata,status,created_by_user_id)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'active',$12)
		RETURNING generation,fingerprint,metadata,status,created_by_user_id,
		          created_at,retired_at,revoked_at`,
		scope.Kind, nullableCredentialTenant(scope), scope.Provider, scope.Purpose,
		generation, envelope.Version, envelope.KeyID, envelope.Nonce,
		envelope.Ciphertext, envelope.Fingerprint, metadata, actorUserID,
	).Scan(&result.Generation, &result.Fingerprint, &result.Metadata,
		&result.Status, &result.CreatedByUserID, &result.CreatedAt,
		&result.RetiredAt, &result.RevokedAt)
	if err != nil {
		return CredentialMetadata{}, credentialDBError("写入加密凭证", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return CredentialMetadata{}, credentialDBError("提交凭证轮换", err)
	}
	return result, nil
}

// CredentialStatus returns only non-secret metadata and re-proves that the
// caller owns the exact tenant or platform authority.
func (s *Store) CredentialStatus(
	ctx context.Context, scope CredentialScope, actorUserID int64,
) (CredentialMetadata, error) {
	if err := validateCredentialScope(scope); err != nil || actorUserID <= 0 {
		if err != nil {
			return CredentialMetadata{}, err
		}
		return CredentialMetadata{}, credentialValidationError("actor 无效")
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return CredentialMetadata{}, credentialDBError("开始凭证查询事务", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := authorizeCredentialActor(ctx, tx, scope, actorUserID); err != nil {
		return CredentialMetadata{}, err
	}
	row, err := queryCredentialRow(ctx, tx, scope, 0)
	if err != nil {
		return CredentialMetadata{}, err
	}
	return row.CredentialMetadata, nil
}

// ActiveCredentialMetadata is an internal control-plane read used during
// startup before HTTP principals exist. It never decrypts or returns envelope
// bytes and must not be exposed directly as a tenant API.
func (s *Store) ActiveCredentialMetadata(
	ctx context.Context, scope CredentialScope,
) (CredentialMetadata, error) {
	if err := validateCredentialScope(scope); err != nil {
		return CredentialMetadata{}, err
	}
	row, err := queryCredentialRow(ctx, s.pool, scope, 0)
	if err != nil {
		return CredentialMetadata{}, err
	}
	return row.CredentialMetadata, nil
}

// LatestCredentialMetadata distinguishes a never-configured scope from an
// explicitly revoked one. Startup compatibility may fall back to environment
// only for the former; a revocation tombstone must remain authoritative.
func (s *Store) LatestCredentialMetadata(
	ctx context.Context, scope CredentialScope,
) (CredentialMetadata, error) {
	if err := validateCredentialScope(scope); err != nil {
		return CredentialMetadata{}, err
	}
	var result CredentialMetadata
	result.CredentialScope = scope
	err := s.pool.QueryRow(ctx, `SELECT generation,fingerprint,metadata,status,
		created_by_user_id,created_at,retired_at,revoked_at
		FROM credential_vault_entries
		WHERE scope_kind=$1 AND tenant_id IS NOT DISTINCT FROM $2 AND
		      provider=$3 AND purpose=$4
		ORDER BY generation DESC LIMIT 1`, scope.Kind, nullableCredentialTenant(scope),
		scope.Provider, scope.Purpose).Scan(&result.Generation, &result.Fingerprint,
		&result.Metadata, &result.Status, &result.CreatedByUserID, &result.CreatedAt,
		&result.RetiredAt, &result.RevokedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return CredentialMetadata{}, types.NewAppError(types.CodeNotFound,
			"凭证历史不存在", err)
	}
	if err != nil {
		return CredentialMetadata{}, credentialDBError("读取凭证历史", err)
	}
	return result, nil
}

// UseCredential decrypts one exact generation only for an internal runtime
// callback. Plaintext is never put into a JSON-capable public struct.
func (s *Store) UseCredential(
	ctx context.Context, scope CredentialScope, generation int64,
	use func(secretJSON []byte, metadata CredentialMetadata) error,
) error {
	if s == nil || s.credentialVault == nil {
		return types.NewAppError(types.CodeConflict, "凭证库尚未配置", nil)
	}
	if err := validateCredentialScope(scope); err != nil {
		return err
	}
	if generation < 0 || use == nil {
		return credentialValidationError("凭证版本或消费函数无效")
	}
	row, err := queryCredentialRow(ctx, s.pool, scope, generation)
	if err != nil {
		return err
	}
	secret, err := s.credentialVault.Open(credentialvault.Scope{
		Kind: scope.Kind, TenantID: scope.TenantID, Provider: scope.Provider,
		Purpose: scope.Purpose, Generation: row.Generation,
	}, credentialvault.Envelope{
		Version: row.EnvelopeVersion, KeyID: row.KeyID, Nonce: row.Nonce,
		Ciphertext: row.Ciphertext, Fingerprint: row.Fingerprint,
	})
	if err != nil {
		return types.NewAppError(types.CodeInternal,
			"凭证无法通过完整性校验", err)
	}
	defer clear(secret)
	return use(secret, row.CredentialMetadata)
}

func (s *Store) RevokeCredential(
	ctx context.Context, scope CredentialScope, actorUserID int64,
) error {
	if err := validateCredentialScope(scope); err != nil || actorUserID <= 0 {
		if err != nil {
			return err
		}
		return credentialValidationError("actor 无效")
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return credentialDBError("开始凭证撤销事务", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := authorizeCredentialActor(ctx, tx, scope, actorUserID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`,
		credentialLockKey(scope)); err != nil {
		return credentialDBError("锁定凭证作用域", err)
	}
	result, err := tx.Exec(ctx, `UPDATE credential_vault_entries
		SET status='revoked',revoked_at=clock_timestamp()
		WHERE scope_kind=$1 AND tenant_id IS NOT DISTINCT FROM $2 AND
		      provider=$3 AND purpose=$4 AND status='active'`,
		scope.Kind, nullableCredentialTenant(scope), scope.Provider, scope.Purpose)
	if err != nil {
		return credentialDBError("撤销凭证", err)
	}
	if result.RowsAffected() != 1 {
		return types.NewAppError(types.CodeNotFound, "没有可撤销的凭证", nil)
	}
	if err := tx.Commit(ctx); err != nil {
		return credentialDBError("提交凭证撤销", err)
	}
	return nil
}

type credentialQuery interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func queryCredentialRow(
	ctx context.Context, query credentialQuery, scope CredentialScope, generation int64,
) (credentialEnvelopeRow, error) {
	statusClause := "AND status='active'"
	if generation > 0 {
		statusClause = "AND generation=$5 AND status<>'revoked'"
	}
	args := []any{scope.Kind, nullableCredentialTenant(scope), scope.Provider, scope.Purpose}
	if generation > 0 {
		args = append(args, generation)
	}
	var row credentialEnvelopeRow
	row.CredentialScope = scope
	err := query.QueryRow(ctx, `SELECT generation,envelope_version,key_id,nonce,ciphertext,
		fingerprint,metadata,status,created_by_user_id,created_at,retired_at,revoked_at
		FROM credential_vault_entries
		WHERE scope_kind=$1 AND tenant_id IS NOT DISTINCT FROM $2 AND
		      provider=$3 AND purpose=$4 `+statusClause+`
		ORDER BY generation DESC LIMIT 1`, args...).Scan(
		&row.Generation, &row.EnvelopeVersion, &row.KeyID, &row.Nonce,
		&row.Ciphertext, &row.Fingerprint, &row.Metadata, &row.Status,
		&row.CreatedByUserID, &row.CreatedAt, &row.RetiredAt, &row.RevokedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return credentialEnvelopeRow{}, types.NewAppError(types.CodeNotFound,
			"凭证不存在", err)
	}
	if err != nil {
		return credentialEnvelopeRow{}, credentialDBError("读取凭证", err)
	}
	return row, nil
}

func authorizeCredentialActor(
	ctx context.Context, tx pgx.Tx, scope CredentialScope, actorUserID int64,
) error {
	tenantID := scope.TenantID
	if scope.Kind == "platform" {
		tenantID = platformCredentialTenantID
	}
	var allowed bool
	err := tx.QueryRow(ctx, `SELECT true
		FROM memberships m JOIN tenants t ON t.id=m.tenant_id
		WHERE m.tenant_id=$1 AND m.user_id=$2 AND m.role='owner' AND
		      t.status='active' AND t.deleted_at IS NULL
		FOR KEY SHARE OF m,t`, tenantID, actorUserID).Scan(&allowed)
	if errors.Is(err, pgx.ErrNoRows) {
		return types.NewAppError(types.CodeNotFound, "凭证配置不存在", nil)
	}
	if err != nil {
		return credentialDBError("校验凭证管理员权限", err)
	}
	if !allowed {
		// Hide the existence of platform/foreign-tenant credential scopes.
		return types.NewAppError(types.CodeNotFound, "凭证配置不存在", nil)
	}
	return nil
}

func validateCredentialInput(
	scope CredentialScope, secretJSON, metadata json.RawMessage, actorUserID int64,
) error {
	if err := validateCredentialScope(scope); err != nil {
		return err
	}
	if actorUserID <= 0 || len(secretJSON) == 0 || len(secretJSON) > 64<<10 ||
		!json.Valid(secretJSON) || len(metadata) == 0 || len(metadata) > 8<<10 ||
		!json.Valid(metadata) || secretJSON[0] != '{' || metadata[0] != '{' {
		return credentialValidationError("凭证内容或元数据无效")
	}
	return nil
}

func validateCredentialScope(scope CredentialScope) error {
	validIdentifier := func(value string) bool {
		if len(value) < 1 || len(value) > 64 || value[0] < 'a' || value[0] > 'z' {
			return false
		}
		for _, r := range value[1:] {
			if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '_' && r != '-' {
				return false
			}
		}
		return true
	}
	if (scope.Kind != "platform" && scope.Kind != "tenant") ||
		(scope.Kind == "platform" && scope.TenantID != 0) ||
		(scope.Kind == "tenant" && scope.TenantID <= 0) ||
		!validIdentifier(scope.Provider) || !validIdentifier(scope.Purpose) {
		return credentialValidationError("凭证作用域无效")
	}
	return nil
}

func nullableCredentialTenant(scope CredentialScope) any {
	if scope.Kind == "platform" {
		return nil
	}
	return scope.TenantID
}

func credentialLockKey(scope CredentialScope) string {
	return fmt.Sprintf("credential-vault/%s/%d/%s/%s",
		scope.Kind, scope.TenantID, scope.Provider, scope.Purpose)
}

func credentialValidationError(message string) error {
	return types.NewAppError(types.CodeValidation, message, nil)
}

func credentialDBError(operation string, err error) error {
	return types.NewAppError(types.CodeDatabase,
		fmt.Sprintf("%s失败", strings.TrimSpace(operation)), err)
}
