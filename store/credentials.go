package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/YouToco/vane/types"
)

// pgUniqueViolation 是 Postgres 唯一约束冲突的 SQLSTATE。
const pgUniqueViolation = "23505"

// NormalizeEmail 归一化邮箱：去空白 + 转小写。
// **写入与查询必须用同一个归一函数**，否则 Alice@x.com 注册后用 alice@x.com
// 登录会查不到（而唯一索引 lower(email) 又不允许再注册），用户被彻底锁死。
func NormalizeEmail(email string) string {
	return strings.ToLower(strings.TrimSpace(email))
}

// RegisterWithInvite 是注册流的落库实现：**在一个事务里**完成
// 消费邀请码 → 建用户 → 建租户 → 建 owner 成员 → 回填邀请码归属。
//
// 与 CreateTenantWithInvite 的区别是这里连用户一起建（注册时用户尚不存在）。
// 两者共用同一条纪律：邀请码消费必须与建租户同事务且在其之前——
// 拆开会让 I-A2（无有效邀请码不得创建租户）在失败路径上失守，
// 而 I-A2 是 D3「平台全垫付第三方 API 成本」的唯一财务闸门。
//
// 邮箱重复返回 CodeConflict；邀请码无效返回 CodeValidation（不区分具体原因，
// 见 CreateTenantWithInvite 的说明）。
func (s *Store) RegisterWithInvite(ctx context.Context, email, passwordHash, code string) (*types.User, *types.Tenant, error) {
	email = NormalizeEmail(email)
	if email == "" {
		return nil, nil, types.NewAppError(types.CodeValidation, "邮箱不能为空", nil)
	}
	if passwordHash == "" {
		return nil, nil, types.NewAppError(types.CodeValidation, "密码哈希不能为空", nil)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, nil, types.NewAppError(types.CodeDatabase, "开启事务失败", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// 步骤 1：消费邀请码（必须在建租户之前，理由见 CreateTenantWithInvite）。
	var consumed string
	err = tx.QueryRow(ctx,
		`UPDATE invites
		    SET used_count = used_count + 1, consumed_at = now()
		  WHERE code = $1
		    AND used_count < max_uses
		    AND (expires_at IS NULL OR expires_at > now())
		RETURNING code`, code).Scan(&consumed)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil, types.NewAppError(types.CodeValidation, "邀请码无效或已失效", nil)
		}
		return nil, nil, types.NewAppError(types.CodeDatabase, "消费邀请码", err)
	}

	// 步骤 2：建用户。唯一索引冲突 = 邮箱已注册。
	var u types.User
	err = tx.QueryRow(ctx,
		`INSERT INTO users (email, password_hash, name)
		 VALUES ($1, $2, '')
		 RETURNING id, feishu_open_id, name, created_at, email, password_hash, email_verified`,
		email, passwordHash).Scan(
		&u.ID, &u.FeishuOpenID, &u.Name, &u.CreatedAt, &u.Email, &u.PasswordHash, &u.EmailVerified)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation {
			return nil, nil, types.NewAppError(types.CodeConflict, "该邮箱已注册", err)
		}
		return nil, nil, types.NewAppError(types.CodeDatabase, "创建用户", err)
	}

	// 步骤 3：建租户。
	var t types.Tenant
	if err := scanTenant(tx.QueryRow(ctx,
		`INSERT INTO tenants DEFAULT VALUES RETURNING `+tenantColumns), &t); err != nil {
		return nil, nil, types.NewAppError(types.CodeDatabase, "创建租户", err)
	}

	// 步骤 4：建 owner 成员。
	if _, err := tx.Exec(ctx,
		`INSERT INTO memberships (tenant_id, user_id, role) VALUES ($1, $2, $3)`,
		t.ID, u.ID, types.MembershipRoleOwner); err != nil {
		return nil, nil, types.NewAppError(types.CodeDatabase, "创建租户成员", err)
	}

	// 步骤 5：回填邀请码归属（审计）。
	if _, err := tx.Exec(ctx,
		`UPDATE invites SET consumed_by_tenant = $2 WHERE code = $1`, code, t.ID); err != nil {
		return nil, nil, types.NewAppError(types.CodeDatabase, "回填邀请码归属", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, nil, types.NewAppError(types.CodeDatabase, "提交事务失败", err)
	}
	return &u, &t, nil
}

// GetUserByEmail 按邮箱取用户（登录用）。邮箱按 NormalizeEmail 归一后比较。
//
// 未找到返回 CodeNotFound——**但调用方不得把它与「密码错误」区分后回给客户端**，
// 否则登录接口就成了账号枚举器（试邮箱看回哪种错，即可批量确认哪些邮箱注册过）。
func (s *Store) GetUserByEmail(ctx context.Context, email string) (*types.User, error) {
	email = NormalizeEmail(email)
	var u types.User
	err := s.pool.QueryRow(ctx,
		`SELECT id, feishu_open_id, name, created_at, email, password_hash, email_verified
		   FROM users WHERE lower(email) = $1`, email).Scan(
		&u.ID, &u.FeishuOpenID, &u.Name, &u.CreatedAt, &u.Email, &u.PasswordHash, &u.EmailVerified)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, types.NewAppError(types.CodeNotFound, "用户不存在", err)
		}
		return nil, types.NewAppError(types.CodeDatabase, "按邮箱查询用户", err)
	}
	return &u, nil
}

// UpdatePasswordHash 更新用户密码哈希（改密码、或登录后按新参数无感重算）。
func (s *Store) UpdatePasswordHash(ctx context.Context, userID int64, passwordHash string) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE users SET password_hash = $2 WHERE id = $1`, userID, passwordHash)
	if err != nil {
		return types.NewAppError(types.CodeDatabase, "更新密码哈希", err)
	}
	if tag.RowsAffected() == 0 {
		return types.NewAppError(types.CodeNotFound,
			fmt.Sprintf("用户 %d 不存在", userID), nil)
	}
	return nil
}

// ---- 会话 ----

// CreateSession 落库一条会话。tokenHash 由 auth.NewSessionToken 产出，
// **明文 token 绝不入库**。
func (s *Store) CreateSession(ctx context.Context, tokenHash []byte, userID int64, tenantID int64, expiresAt time.Time) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO user_sessions (token_hash, user_id, tenant_id, expires_at)
		 VALUES ($1, $2, $3, $4)`, tokenHash, userID, tenantID, expiresAt)
	if err != nil {
		return types.NewAppError(types.CodeDatabase, "创建会话", err)
	}
	return nil
}

// LookupSession 按 token 哈希查活跃会话，顺带刷新 last_seen_at。
//
// 过期判定放在 SQL 谓词里（`expires_at > now()`）而非取出后在 Go 里比：
// 前者让「过期」与「不存在」在同一条语句里归一为同一结果，不给调用方
// 误用「取到了但过期」的机会；也避免应用与数据库时钟不一致导致的边界分歧。
func (s *Store) LookupSession(ctx context.Context, tokenHash []byte) (*types.Session, error) {
	var sess types.Session
	err := s.pool.QueryRow(ctx,
		`UPDATE user_sessions SET last_seen_at = now()
		  WHERE token_hash = $1 AND expires_at > now()
		RETURNING token_hash, user_id, tenant_id, created_at, expires_at, last_seen_at`,
		tokenHash).Scan(&sess.TokenHash, &sess.UserID, &sess.TenantID,
		&sess.CreatedAt, &sess.ExpiresAt, &sess.LastSeenAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, types.NewAppError(types.CodeNotFound, "会话不存在或已过期", err)
		}
		return nil, types.NewAppError(types.CodeDatabase, "查询会话", err)
	}
	return &sess, nil
}

// DeleteSession 删除单条会话（登出）。不存在也返回 nil：登出是幂等操作，
// 重复登出不该报错。
func (s *Store) DeleteSession(ctx context.Context, tokenHash []byte) error {
	if _, err := s.pool.Exec(ctx,
		`DELETE FROM user_sessions WHERE token_hash = $1`, tokenHash); err != nil {
		return types.NewAppError(types.CodeDatabase, "删除会话", err)
	}
	return nil
}

// DeleteSessionsByUser 删除某用户全部会话（改密码、封禁、"登出所有设备"）。
// 返回删除条数。
func (s *Store) DeleteSessionsByUser(ctx context.Context, userID int64) (int64, error) {
	tag, err := s.pool.Exec(ctx, `DELETE FROM user_sessions WHERE user_id = $1`, userID)
	if err != nil {
		return 0, types.NewAppError(types.CodeDatabase, "删除用户会话", err)
	}
	return tag.RowsAffected(), nil
}

// DeleteExpiredSessions 清理过期会话，返回删除条数（定时任务调用）。
func (s *Store) DeleteExpiredSessions(ctx context.Context) (int64, error) {
	tag, err := s.pool.Exec(ctx, `DELETE FROM user_sessions WHERE expires_at <= now()`)
	if err != nil {
		return 0, types.NewAppError(types.CodeDatabase, "清理过期会话", err)
	}
	return tag.RowsAffected(), nil
}

// SetUserCredentials 给**已存在的用户**挂上邮箱+密码身份。
//
// 存在的唯一理由是**存量 owner 的接管**：改造前 Dashboard 用共享密码登录，
// 换成账号体系后，那位 owner 已经带着租户 1 与全部历史数据存在于 users 表，
// 却没有邮箱和密码——若让他重新注册，会得到一个空的新租户，历史数据全留在原处
// 看不见。本方法把身份挂到既有行上，租户归属与数据一概不动。
//
// 与 RegisterWithInvite 的分工：那个是「建新人」，这个是「给老人补身份」。
// 刻意不做成 HTTP 端点——它能给任意用户设密码，是纯粹的管理员操作，
// 只应由能登上 VPS 的人在本机执行（见 cmd/useradmin）。
func (s *Store) SetUserCredentials(ctx context.Context, userID int64, email, passwordHash string) error {
	email = NormalizeEmail(email)
	if email == "" || passwordHash == "" {
		return types.NewAppError(types.CodeValidation, "邮箱与密码哈希都不能为空", nil)
	}
	tag, err := s.pool.Exec(ctx,
		`UPDATE users SET email = $2, password_hash = $3 WHERE id = $1`,
		userID, email, passwordHash)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation {
			return types.NewAppError(types.CodeConflict, "该邮箱已被其他账号占用", err)
		}
		return types.NewAppError(types.CodeDatabase, "设置用户凭据", err)
	}
	if tag.RowsAffected() == 0 {
		return types.NewAppError(types.CodeNotFound,
			fmt.Sprintf("用户 %d 不存在", userID), nil)
	}
	return nil
}
