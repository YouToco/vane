package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/types"
)

// tenantColumns 是 tenants 表全列，SELECT 与 scanTenant 一一对应。
const tenantColumns = `id, status, plan, deleted_at, purge_after, created_at`

func scanTenant(row pgx.Row, t *types.Tenant) error {
	return row.Scan(&t.ID, &t.Status, &t.Plan, &t.DeletedAt, &t.PurgeAfter, &t.CreatedAt)
}

// CreateTenantWithInvite 是注册流的核心：**在一个事务里**消费邀请码、建租户、建 owner 成员。
//
// 为什么必须是事务（本方法是本仓库的第一个事务，此前 store 全是单语句）：
// 这三步一旦拆开就有两种败法——邀请码消费了但租户没建成（用户白白烧掉一个码），
// 或租户建成了但码没消费成（不变量 I-A2 被绕过，财务闸门失效）。后者更糟：
// D3 决定平台垫付第三方 API 成本，绕过邀请码等于把按次计费的敞口对公网开放。
//
// 并发安全性来自 UPDATE 自带的行锁 + `used_count < max_uses` 谓词：两个请求同抢
// 一个 max_uses=1 的码时，后到者的 UPDATE 会阻塞到前者提交，之后谓词不再成立、
// 影响 0 行 → 返回「已用完」。不需要 SELECT FOR UPDATE，也不需要更高隔离级别。
// 迁移 018 的 CHECK (used_count <= max_uses) 是同一件事的数据库层第二道锁。
//
// 错误语义：邀请码无效/过期/用完统一返回 CodeValidation（都是「你给的码不能用」，
// 且**不区分具体原因**——区分等于给暴力枚举送信息，说明哪个码存在只是次数用完了）。
func (s *Store) CreateTenantWithInvite(ctx context.Context, code string, userID int64) (*types.Tenant, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "开启事务失败", err)
	}
	// Commit 成功后 Rollback 是 no-op（pgx 返回 ErrTxClosed，此处忽略），
	// 故这一行同时覆盖「出错回滚」与「提前 return 回滚」两种路径。
	defer func() { _ = tx.Rollback(ctx) }()

	// 步骤 1：原子消费邀请码。必须在建租户**之前**（契约 §1.2）——
	// 先建后消费的话，消费失败时租户已经存在，回滚依赖事务而不是逻辑顺序，
	// 而顺序正确时即使将来有人误删事务，闸门仍然先于建租户生效。
	var consumedCode string
	err = tx.QueryRow(ctx,
		`UPDATE invites
		    SET used_count = used_count + 1, consumed_at = now()
		  WHERE code = $1
		    AND used_count < max_uses
		    AND (expires_at IS NULL OR expires_at > now())
		RETURNING code`, code).Scan(&consumedCode)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, types.NewAppError(types.CodeValidation, "邀请码无效或已失效", nil)
		}
		return nil, types.NewAppError(types.CodeDatabase, "消费邀请码", err)
	}

	// 步骤 2：建租户。
	var t types.Tenant
	if err := scanTenant(tx.QueryRow(ctx,
		`INSERT INTO tenants DEFAULT VALUES RETURNING `+tenantColumns), &t); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "创建租户", err)
	}

	// 步骤 3：建 owner 成员（D8：初期每租户恒 1 行）。
	if _, err := tx.Exec(ctx,
		`INSERT INTO memberships (tenant_id, user_id, role) VALUES ($1, $2, $3)`,
		t.ID, userID, types.MembershipRoleOwner); err != nil {
		return nil, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("创建租户成员（tenant=%d user=%d）", t.ID, userID), err)
	}

	// 步骤 4：回填邀请码的归属租户（审计用；used_count 已在步骤 1 递增）。
	if _, err := tx.Exec(ctx,
		`UPDATE invites SET consumed_by_tenant = $2 WHERE code = $1`, code, t.ID); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "回填邀请码归属", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "提交事务失败", err)
	}
	return &t, nil
}

// GetTenant 按主键取租户。
func (s *Store) GetTenant(ctx context.Context, id int64) (*types.Tenant, error) {
	var t types.Tenant
	err := scanTenant(s.pool.QueryRow(ctx,
		`SELECT `+tenantColumns+` FROM tenants WHERE id = $1`, id), &t)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, types.NewAppError(types.CodeNotFound,
				fmt.Sprintf("租户 %d 不存在", id), err)
		}
		return nil, types.NewAppError(types.CodeDatabase, "查询租户", err)
	}
	return &t, nil
}

// IssueInvite 签发邀请码。issuedBy 为 nil 表示平台自签（当前唯一用法：运维手工发码）。
// maxUses <= 0 归一为 1：默认一码一用，误传 0 不该变成「永远不能用」。
func (s *Store) IssueInvite(ctx context.Context, code string, issuedBy *int64, maxUses int, expiresAt *time.Time) (*types.Invite, error) {
	if code == "" {
		return nil, types.NewAppError(types.CodeValidation, "邀请码不能为空", nil)
	}
	if maxUses <= 0 {
		maxUses = 1
	}
	var inv types.Invite
	err := s.pool.QueryRow(ctx,
		`INSERT INTO invites (code, issued_by, max_uses, expires_at)
		 VALUES ($1, $2, $3, $4)
		 RETURNING code, issued_by, issued_at, expires_at, max_uses, used_count,
		           consumed_by_tenant, consumed_at`,
		code, issuedBy, maxUses, expiresAt).Scan(
		&inv.Code, &inv.IssuedBy, &inv.IssuedAt, &inv.ExpiresAt, &inv.MaxUses,
		&inv.UsedCount, &inv.ConsumedByTenant, &inv.ConsumedAt)
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("签发邀请码 %q", code), err)
	}
	return &inv, nil
}

// GetInvite 按码查邀请码（运维/测试用；注册流不走这里——它必须用
// CreateTenantWithInvite 的原子消费，先查后用会有 TOCTOU 竞态）。
func (s *Store) GetInvite(ctx context.Context, code string) (*types.Invite, error) {
	var inv types.Invite
	err := s.pool.QueryRow(ctx,
		`SELECT code, issued_by, issued_at, expires_at, max_uses, used_count,
		        consumed_by_tenant, consumed_at
		   FROM invites WHERE code = $1`, code).Scan(
		&inv.Code, &inv.IssuedBy, &inv.IssuedAt, &inv.ExpiresAt, &inv.MaxUses,
		&inv.UsedCount, &inv.ConsumedByTenant, &inv.ConsumedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, types.NewAppError(types.CodeNotFound, "邀请码不存在", err)
		}
		return nil, types.NewAppError(types.CodeDatabase, "查询邀请码", err)
	}
	return &inv, nil
}

// ListMembershipsByUser 返回用户所属的全部租户成员关系（认证后按 user 定位租户）。
func (s *Store) ListMembershipsByUser(ctx context.Context, userID int64) ([]types.Membership, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT tenant_id, user_id, role, created_at
		   FROM memberships WHERE user_id = $1 ORDER BY tenant_id`, userID)
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "查询用户租户成员关系", err)
	}
	defer rows.Close()

	var out []types.Membership
	for rows.Next() {
		var m types.Membership
		if err := rows.Scan(&m.TenantID, &m.UserID, &m.Role, &m.CreatedAt); err != nil {
			return nil, types.NewAppError(types.CodeDatabase, "扫描成员关系", err)
		}
		out = append(out, m)
	}
	if err := rows.Err(); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "遍历成员关系", err)
	}
	return out, nil
}

// AddMembership 把用户加入租户。
//
// 生产用途：将来做「邀请他人加入我的租户」时的落库入口（D8 预留的多人租户）。
// 当前唯一的租户归属来源仍是注册流（RegisterWithInvite 在同一事务里建成员关系），
// 本方法是那条路之外的显式补充——**刻意不做成 HTTP 端点**：谁能把谁加进哪个租户
// 是权限问题，得等 D8 的角色模型落地再开放。
//
// 幂等：重复加入同一租户不报错。
func (s *Store) AddMembership(ctx context.Context, tenantID, userID int64, role types.MembershipRole) error {
	if _, err := s.pool.Exec(ctx,
		`INSERT INTO memberships (tenant_id, user_id, role) VALUES ($1, $2, $3)
		 ON CONFLICT (tenant_id, user_id) DO NOTHING`,
		tenantID, userID, role); err != nil {
		return types.NewAppError(types.CodeDatabase, "加入租户", err)
	}
	return nil
}
