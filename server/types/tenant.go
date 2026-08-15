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

// Valid reports whether role can be persisted in memberships.
func (r MembershipRole) Valid() bool {
	switch r {
	case MembershipRoleOwner, MembershipRoleAdmin, MembershipRoleMember:
		return true
	default:
		return false
	}
}

// WorkspaceKind separates a user's private workspace from shared team workspaces.
type WorkspaceKind string

const (
	WorkspaceKindPersonal WorkspaceKind = "personal"
	WorkspaceKindTeam     WorkspaceKind = "team"
)

// ActorType records how a principal authenticated. Interactive browser and
// channel identities are users; scoped automation credentials use service_account.
type ActorType string

const (
	ActorTypeUser           ActorType = "user"
	ActorTypeServiceAccount ActorType = "service_account"
)

// Workspace is the product projection of a tenant together with the caller's
// exact membership. It deliberately does not infer membership from user ID.
type Workspace struct {
	ID                  int64          `json:"id"`
	Name                string         `json:"name"`
	Kind                WorkspaceKind  `json:"kind"`
	Status              TenantStatus   `json:"status"`
	Plan                string         `json:"plan"`
	SeatLimit           int            `json:"seat_limit"`
	MemberCount         int            `json:"member_count"`
	Role                MembershipRole `json:"role"`
	PersonalOwnerUserID *int64         `json:"-"`
	CreatedAt           time.Time      `json:"created_at"`
	UpdatedAt           time.Time      `json:"updated_at"`
}

// WorkspaceMember is safe for member-directory responses. Password, session,
// channel identity and credential fields never cross this boundary.
type WorkspaceMember struct {
	TenantID int64          `json:"tenant_id"`
	UserID   int64          `json:"user_id"`
	Email    string         `json:"email"`
	Name     string         `json:"name"`
	Role     MembershipRole `json:"role"`
	JoinedAt time.Time      `json:"joined_at"`
}

// WorkspaceInvite is the durable, one-time team invitation. Only TokenHash is
// stored; the raw token is returned once by the API and never persisted.
type WorkspaceInvite struct {
	ID           int64          `json:"id"`
	TenantID     int64          `json:"tenant_id"`
	Email        string         `json:"email"`
	Role         MembershipRole `json:"role"`
	IssuedBy     int64          `json:"issued_by"`
	ExpiresAt    time.Time      `json:"expires_at"`
	ConsumedBy   *int64         `json:"consumed_by,omitempty"`
	ConsumedAt   *time.Time     `json:"consumed_at,omitempty"`
	RevokedAt    *time.Time     `json:"revoked_at,omitempty"`
	CreatedAt    time.Time      `json:"created_at"`
	RawTokenOnce string         `json:"token,omitempty"`
}

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

// Session 登录会话（user_sessions 表，migration 019）。
//
// TokenHash 是会话 token 的 sha256——**库里没有明文 token**（见 auth/session.go）。
type Session struct {
	TokenHash  []byte         `json:"-"`
	UserID     int64          `json:"user_id"`
	TenantID   int64          `json:"tenant_id"`
	Role       MembershipRole `json:"role"`
	ActorType  ActorType      `json:"actor_type"`
	CreatedAt  time.Time      `json:"created_at"`
	ExpiresAt  time.Time      `json:"expires_at"`
	LastSeenAt *time.Time     `json:"last_seen_at,omitempty"`
}
