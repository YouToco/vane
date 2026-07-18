package types

import "time"

// TenantStatus 租户状态（tenants.status）。
type TenantStatus string

const (
	TenantStatusActive    TenantStatus = "active"
	TenantStatusSuspended TenantStatus = "suspended"
	// TenantStatusDeleting 是 D9 软删除的中间态：租户立即从产品面消失
	// （登录被拒、调度停用、推送停发），但数据保留到 purge_after 之后才硬删。
	TenantStatusDeleting TenantStatus = "deleting"
)

// MembershipRole 成员角色（memberships.role）。
// 当前只用 owner（D8：每租户恒 1 人）；其余是给团队功能预留的位。
type MembershipRole string

const (
	MembershipRoleOwner  MembershipRole = "owner"
	MembershipRoleAdmin  MembershipRole = "admin"
	MembershipRoleMember MembershipRole = "member"
)

// Tenant 租户（tenants 表，migration 018）。
type Tenant struct {
	ID     int64        `json:"id"`
	Status TenantStatus `json:"status"`
	Plan   string       `json:"plan"`
	// DeletedAt / PurgeAfter 是 D9 软删除的两个时间点：前者标记注销、后者是硬删期限。
	// 均为 nil 表示租户存活。
	DeletedAt  *time.Time `json:"deleted_at,omitempty"`
	PurgeAfter *time.Time `json:"purge_after,omitempty"`
	CreatedAt  time.Time  `json:"created_at"`
}

// Membership 租户成员（memberships 表）。
type Membership struct {
	TenantID  int64          `json:"tenant_id"`
	UserID    int64          `json:"user_id"`
	Role      MembershipRole `json:"role"`
	CreatedAt time.Time      `json:"created_at"`
}

// Invite 邀请码（invites 表，D4）。
//
// 这不是产品功能而是**财务护栏**：D3 决定平台垫付第三方 API 成本（TikHub 按次、
// LLM 按 token），邀请码是这份敞口的唯一上限——不变量 I-A2「无有效邀请码不得创建租户」
// 是它的形式化表述。
type Invite struct {
	Code      string     `json:"code"`
	IssuedBy  *int64     `json:"issued_by,omitempty"` // nil = 平台自签
	IssuedAt  time.Time  `json:"issued_at"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"` // nil = 永不过期
	MaxUses   int        `json:"max_uses"`
	UsedCount int        `json:"used_count"`
	// ConsumedByTenant / ConsumedAt 在多次可用的码上记录**最近一次**消费；
	// UsedCount 才是权威计数（见 migration 018 的说明）。
	ConsumedByTenant *int64     `json:"consumed_by_tenant,omitempty"`
	ConsumedAt       *time.Time `json:"consumed_at,omitempty"`
}

// Exhausted 报告邀请码是否已用满。
func (i Invite) Exhausted() bool { return i.UsedCount >= i.MaxUses }

// Expired 报告邀请码在给定时刻是否已过期。
func (i Invite) Expired(now time.Time) bool {
	return i.ExpiresAt != nil && !i.ExpiresAt.After(now)
}
