// 邀请码管理面的读写：列表与作废，供 /api/admin/invites 使用。
// 签发（IssueInvite）与消费（CreateTenantWithInvite 的原子 UPDATE）在 tenants.go——
// 那两条是注册流的地基，先于本文件存在；这里只补管理动作，不动既有语义。
package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/types"
)

// InviteWithConsumer 是管理面列表项：邀请码本体 + 最近一次消费租户的 owner 邮箱。
//
// 邮箱是给人看的：管理面上「被 tenant 42 用了」不如「被 alice@x.com 用了」有用。
// 取的是消费租户的 owner（D8 现阶段每租户恰一人）；owner 是纯飞书用户时无邮箱，
// 与「未消费」同样表现为 nil，前端统一按空处理。
type InviteWithConsumer struct {
	types.Invite
	ConsumerEmail *string
}

// ListInvites 返回全部邀请码，新签发的在前。
//
// 不分页：邀请制下码是运维一个个发出去的，量级是「十」不是「万」；
// 真到需要分页那天，这个产品的准入模式早就不是邀请制了。
func (s *Store) ListInvites(ctx context.Context) ([]InviteWithConsumer, error) {
	// LATERAL 而非平铺 JOIN：owner 邮箱按租户取**恰好一行**（ORDER BY + LIMIT 1），
	// 将来 D8 放开多人租户时列表也不会因 join 撑出重复的邀请码行。
	rows, err := s.pool.Query(ctx,
		`SELECT i.code, i.issued_by, i.issued_at, i.expires_at, i.max_uses, i.used_count,
		        i.consumed_by_tenant, i.consumed_at, oe.email
		   FROM invites i
		   LEFT JOIN LATERAL (
		     SELECT u.email
		       FROM memberships m
		       JOIN users u ON u.id = m.user_id
		      WHERE m.tenant_id = i.consumed_by_tenant AND m.role = $1
		      ORDER BY m.created_at
		      LIMIT 1
		   ) oe ON true
		  ORDER BY i.issued_at DESC, i.code`,
		types.MembershipRoleOwner)
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "查询邀请码列表", err)
	}
	defer rows.Close()

	var out []InviteWithConsumer
	for rows.Next() {
		var it InviteWithConsumer
		if err := rows.Scan(&it.Code, &it.IssuedBy, &it.IssuedAt, &it.ExpiresAt,
			&it.MaxUses, &it.UsedCount, &it.ConsumedByTenant, &it.ConsumedAt,
			&it.ConsumerEmail); err != nil {
			return nil, types.NewAppError(types.CodeDatabase, "扫描邀请码", err)
		}
		out = append(out, it)
	}
	if err := rows.Err(); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "遍历邀请码", err)
	}
	return out, nil
}

// DeleteUnusedInvite 作废一个**从未被使用**的邀请码（物理删除该行）。
//
// 为什么只许删 used_count = 0 的：已消费的码是「某租户从哪来的」的审计线索
// （consumed_by_tenant 回填就是为此），删它等于抹掉准入闸门的账本；
// 部分使用的多用码同理——它已经放进来过租户。这类请求**报错而非静默跳过**：
// 管理员以为码废了、实际还能用，是比报错糟糕得多的结局。
//
// 为什么物理删除而非标记：invites 表没有 revoked 列，加列要吃一个迁移号；
// 而未使用的码没有任何下游引用（consumed_by_tenant 为 NULL），删行即作废，
// 语义干净。「数据是资产」红线针对的是内容表，不适用于一张还没生效的门票。
func (s *Store) DeleteUnusedInvite(ctx context.Context, code string) error {
	if code == "" {
		return types.NewAppError(types.CodeValidation, "邀请码不能为空", nil)
	}
	// 单语句 CTE 原子完成「删 + 诊断」：DELETE 的谓词负责只删未使用的码，
	// 外层再看该码在语句快照里是否存在，区分「不存在」与「已使用」。
	// 拆成 DELETE + 补查两条语句会有 TOCTOU 窗口（间隙里码被注册流消费），
	// 这里守的是财务闸门的账本，不留这种窗口。
	var deleted, existed bool
	err := s.pool.QueryRow(ctx,
		`WITH del AS (
		   DELETE FROM invites WHERE code = $1 AND used_count = 0 RETURNING code
		 )
		 SELECT EXISTS(SELECT 1 FROM del),
		        EXISTS(SELECT 1 FROM invites WHERE code = $1)`,
		code).Scan(&deleted, &existed)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return types.NewAppError(types.CodeDatabase, fmt.Sprintf("作废邀请码 %q", code), err)
	}
	switch {
	case deleted:
		return nil
	case existed:
		return types.NewAppError(types.CodeConflict, "邀请码已被使用，不可作废", nil)
	default:
		return types.NewAppError(types.CodeNotFound, "邀请码不存在", nil)
	}
}
