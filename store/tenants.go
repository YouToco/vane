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

// tenantRetentionDays 是软删除后的保留期（D9）：到期前可无损恢复，到期后由硬删任务清除。
// 30 天是契约 §1.3 定的数——给"手滑注销"和"注销后反悔"留出足够的回旋余地，
// 同时不让已注销租户的数据无限期占着库。
const tenantRetentionDays = 30

// SoftDeleteTenant 注销租户（D9 软删除）：置 deleting 状态并设定硬删期限。
//
// **软删除必须立刻让租户从产品面消失**，否则「已注销」只是个标记：调度照跑、
// Exa/TikHub/LLM 照花钱、推送照发到对方手机上。落库只是第一步，真正的停止靠
// 两处闸门——LookupSession 拒绝登录（既有），以及 pipeline 首个花钱的活动拒绝开工
// （见 workflow.Activities）。
//
// 幂等：已在 deleting 状态时原样返回，不刷新 purge_after——否则反复注销会把硬删期限
// 一路往后推，数据永远删不掉。
func (s *Store) SoftDeleteTenant(ctx context.Context, tenantID int64) (*types.Tenant, error) {
	var t types.Tenant
	err := s.pool.QueryRow(ctx,
		`UPDATE tenants
		    SET status = 'deleting',
		        deleted_at = COALESCE(deleted_at, now()),
		        purge_after = COALESCE(purge_after, now() + ($2 || ' days')::interval)
		  WHERE id = $1
		RETURNING `+tenantColumns,
		tenantID, tenantRetentionDays).Scan(
		&t.ID, &t.Status, &t.Plan, &t.DeletedAt, &t.PurgeAfter, &t.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, types.NewAppError(types.CodeNotFound, fmt.Sprintf("租户 %d 不存在", tenantID), nil)
	}
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "注销租户", err)
	}
	return &t, nil
}

// RestoreTenant 在保留期内恢复被注销的租户（D9）：清空三个字段回到 active。
//
// 只对 deleting 状态生效：对 active 租户调用是无操作而非报错（幂等），
// 但**已过 purge_after 的不给恢复**——那时硬删任务随时可能动手，
// 让用户以为恢复成功、几分钟后数据消失，比直接拒绝糟糕得多。
func (s *Store) RestoreTenant(ctx context.Context, tenantID int64) (*types.Tenant, error) {
	var t types.Tenant
	err := s.pool.QueryRow(ctx,
		`UPDATE tenants
		    SET status = 'active', deleted_at = NULL, purge_after = NULL
		  WHERE id = $1
		    AND (purge_after IS NULL OR purge_after > now())
		RETURNING `+tenantColumns,
		tenantID).Scan(&t.ID, &t.Status, &t.Plan, &t.DeletedAt, &t.PurgeAfter, &t.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		// 两种情况归一为一条消息：不存在，或已过保留期。
		return nil, types.NewAppError(types.CodeNotFound,
			fmt.Sprintf("租户 %d 不存在，或已过保留期无法恢复", tenantID), nil)
	}
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "恢复租户", err)
	}
	return &t, nil
}

// TenantLiveForUser 报告该用户所属租户是否仍在服务中（active 且未注销）。
//
// 供 pipeline 在**开始花钱之前**自查：软删除若不落到这一层，一个已注销的租户
// 会继续跑定时推送、继续调用 Exa/TikHub/LLM——账单照涨，推送照发到对方手机上。
//
// 用户不属于任何租户时返回 false：那是数据异常（注册流保证每人恰好一个），
// 此时"不开工"比"当作正常"安全——真出了异常，宁可停也不要在无归属的状态下花钱。
func (s *Store) TenantLiveForUser(ctx context.Context, userID int64) (bool, error) {
	var live bool
	err := s.pool.QueryRow(ctx,
		`SELECT EXISTS (
		   SELECT 1 FROM memberships m
		     JOIN tenants t ON t.id = m.tenant_id
		    WHERE m.user_id = $1
		      AND t.status = 'active'
		      AND t.deleted_at IS NULL
		 )`, userID).Scan(&live)
	if err != nil {
		return false, types.NewAppError(types.CodeDatabase, "查询租户服务状态", err)
	}
	return live, nil
}
