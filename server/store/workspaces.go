package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/YouToco/vane/server/types"
)

const (
	defaultTeamSeatLimit       = 5
	maxOwnedTeamWorkspaces     = 10
	maxWorkspaceInvitesPerHour = 20
	maxPendingWorkspaceInvites = 100
)

const workspaceProjection = `
    t.id, t.display_name, t.workspace_kind, t.status, t.plan, t.seat_limit,
    (SELECT count(*) FROM memberships counted WHERE counted.tenant_id = t.id),
    m.role, t.personal_owner_user_id, t.created_at, t.updated_at`

func scanWorkspace(row pgx.Row, out *types.Workspace) error {
	return row.Scan(&out.ID, &out.Name, &out.Kind, &out.Status, &out.Plan,
		&out.SeatLimit, &out.MemberCount, &out.Role,
		&out.PersonalOwnerUserID, &out.CreatedAt, &out.UpdatedAt)
}

// ListWorkspacesForUser returns only active exact memberships. Tenant status is
// checked in the same query so a suspended/deleting workspace never appears as
// switchable.
func (s *Store) ListWorkspacesForUser(ctx context.Context, userID int64) ([]types.Workspace, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+workspaceProjection+`
          FROM memberships m JOIN tenants t ON t.id=m.tenant_id
         WHERE m.user_id=$1 AND t.status='active' AND t.deleted_at IS NULL
         ORDER BY (t.workspace_kind='personal') DESC, t.created_at, t.id`, userID)
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "查询工作区列表", err)
	}
	defer rows.Close()
	var out []types.Workspace
	for rows.Next() {
		var w types.Workspace
		if err := scanWorkspace(rows, &w); err != nil {
			return nil, types.NewAppError(types.CodeDatabase, "扫描工作区列表", err)
		}
		out = append(out, w)
	}
	if err := rows.Err(); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "遍历工作区列表", err)
	}
	return out, nil
}

func (s *Store) GetWorkspaceForUser(ctx context.Context, tenantID, userID int64) (*types.Workspace, error) {
	var out types.Workspace
	err := scanWorkspace(s.pool.QueryRow(ctx, `SELECT `+workspaceProjection+`
          FROM memberships m JOIN tenants t ON t.id=m.tenant_id
         WHERE m.tenant_id=$1 AND m.user_id=$2
           AND t.status='active' AND t.deleted_at IS NULL`, tenantID, userID), &out)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, types.NewAppError(types.CodeNotFound, "工作区不存在或无权访问", err)
	}
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "查询工作区", err)
	}
	return &out, nil
}

// DefaultWorkspaceForUser makes password login deterministic after a user
// joins teams: their personal workspace wins; legacy users fall back only when
// exactly one active membership exists.
func (s *Store) DefaultWorkspaceForUser(ctx context.Context, userID int64) (int64, error) {
	var personalID sql.NullInt64
	var onlyID sql.NullInt64
	var activeCount int
	err := s.pool.QueryRow(ctx, `SELECT
		min(t.id) FILTER (WHERE t.workspace_kind='personal' AND t.personal_owner_user_id=$1),
		min(t.id), count(*)
		FROM memberships m JOIN tenants t ON t.id=m.tenant_id
		WHERE m.user_id=$1 AND t.status='active' AND t.deleted_at IS NULL`, userID).
		Scan(&personalID, &onlyID, &activeCount)
	if err != nil {
		return 0, types.NewAppError(types.CodeDatabase, "选择默认工作区", err)
	}
	if personalID.Valid {
		return personalID.Int64, nil
	}
	if activeCount == 1 && onlyID.Valid {
		return onlyID.Int64, nil
	}
	if activeCount == 0 {
		return 0, types.NewAppError(types.CodeConflict, "账号尚未归属可用工作区", nil)
	}
	return 0, types.NewAppError(types.CodeConflict, "账号缺少唯一个人工作区，无法安全选择", nil)
}

func (s *Store) CreateTeamWorkspace(ctx context.Context, currentTenantID, actorUserID int64, name string, seatLimit int) (*types.Workspace, error) {
	name = strings.TrimSpace(name)
	if name == "" || len([]rune(name)) > 80 {
		return nil, types.NewAppError(types.CodeValidation, "工作区名称应为 1 至 80 个字符", nil)
	}
	if seatLimit == 0 {
		seatLimit = defaultTeamSeatLimit
	}
	if seatLimit < 2 || seatLimit > 100 {
		return nil, types.NewAppError(types.CodeValidation, "团队席位应为 2 至 100", nil)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "开启创建工作区事务", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := lockWorkspaceActor(ctx, tx, currentTenantID, actorUserID); err != nil {
		return nil, err
	}
	var owned int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM tenants t JOIN memberships m ON m.tenant_id=t.id
        WHERE m.user_id=$1 AND m.role='owner' AND t.workspace_kind='team'
          AND t.status='active' AND t.deleted_at IS NULL`, actorUserID).Scan(&owned); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "检查团队工作区上限", err)
	}
	if owned >= maxOwnedTeamWorkspaces {
		return nil, types.NewAppError(types.CodeConflict, "已达到团队工作区数量上限", nil)
	}
	var tenantID int64
	if err := tx.QueryRow(ctx, `INSERT INTO tenants(display_name,workspace_kind,seat_limit)
        VALUES($1,'team',$2) RETURNING id`, name, seatLimit).Scan(&tenantID); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "创建团队工作区", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO memberships(tenant_id,user_id,role) VALUES($1,$2,'owner')`, tenantID, actorUserID); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "创建团队 owner", err)
	}
	if err := setWorkspaceControlScope(ctx, tx, tenantID, actorUserID); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO workspace_audit_events(tenant_id,actor_user_id,target_user_id,event_type)
        VALUES($1,$2,$2,'workspace.created')`, tenantID, actorUserID); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "记录工作区审计事件", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "提交创建工作区事务", err)
	}
	if err := s.SeedTenantQuota(ctx, tenantID); err != nil {
		slog.Error("创建团队工作区后初始化额度失败", "tenant_id", tenantID, "user_id", actorUserID, "err", err)
	}
	return s.GetWorkspaceForUser(ctx, tenantID, actorUserID)
}

// IssueWorkspaceInvite atomically enforces actor role, current seats, pending
// invitations, issuance rate and the one-pending-invite-per-email invariant.
func (s *Store) IssueWorkspaceInvite(ctx context.Context, tenantID, actorUserID int64, email string, role types.MembershipRole, tokenHash []byte, expiresAt time.Time) (*types.WorkspaceInvite, error) {
	email = NormalizeEmail(email)
	if email == "" || !strings.Contains(email, "@") {
		return nil, types.NewAppError(types.CodeValidation, "请填写合法邮箱", nil)
	}
	if role != types.MembershipRoleAdmin && role != types.MembershipRoleMember {
		return nil, types.NewAppError(types.CodeValidation, "邀请角色只能是 admin 或 member", nil)
	}
	if len(tokenHash) != 32 || !expiresAt.After(time.Now()) || expiresAt.After(time.Now().Add(30*24*time.Hour)) {
		return nil, types.NewAppError(types.CodeValidation, "邀请 token 或有效期无效", nil)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "开启邀请事务", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := setWorkspaceControlScope(ctx, tx, tenantID, actorUserID); err != nil {
		return nil, err
	}
	actorRole, err := lockWorkspaceActor(ctx, tx, tenantID, actorUserID)
	if err != nil {
		return nil, err
	}
	if actorRole != types.MembershipRoleOwner && actorRole != types.MembershipRoleAdmin {
		return nil, types.NewAppError(types.CodeNotFound, "工作区不存在或无权邀请成员", nil)
	}
	if role == types.MembershipRoleAdmin && actorRole != types.MembershipRoleOwner {
		return nil, types.NewAppError(types.CodeNotFound, "只有 Owner 可以邀请 Admin", nil)
	}
	var kind types.WorkspaceKind
	var seatLimit, memberCount, pendingCount, hourlyCount int
	if err := tx.QueryRow(ctx, `SELECT t.workspace_kind,t.seat_limit,
        (SELECT count(*) FROM memberships m WHERE m.tenant_id=t.id),
        (SELECT count(*) FROM workspace_invites wi WHERE wi.tenant_id=t.id AND wi.consumed_at IS NULL AND wi.revoked_at IS NULL AND wi.expires_at>now()),
        (SELECT count(*) FROM workspace_invites wi WHERE wi.issued_by=$2 AND wi.created_at>now()-interval '1 hour')
        FROM tenants t WHERE t.id=$1 AND t.status='active' AND t.deleted_at IS NULL FOR UPDATE`, tenantID, actorUserID).
		Scan(&kind, &seatLimit, &memberCount, &pendingCount, &hourlyCount); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "检查工作区邀请额度", err)
	}
	if kind != types.WorkspaceKindTeam {
		return nil, types.NewAppError(types.CodeConflict, "个人工作区不能邀请成员", nil)
	}
	if memberCount+pendingCount >= seatLimit || pendingCount >= maxPendingWorkspaceInvites {
		return nil, types.NewAppError(types.CodeConflict, "工作区席位或待处理邀请已满", nil)
	}
	if hourlyCount >= maxWorkspaceInvitesPerHour {
		return nil, types.NewAppError(types.CodeConflict, "邀请签发过于频繁，请稍后再试", nil)
	}
	if _, err := tx.Exec(ctx, `UPDATE workspace_invites SET revoked_at=now()
        WHERE tenant_id=$1 AND email=$2 AND consumed_at IS NULL AND revoked_at IS NULL AND expires_at<=now()`, tenantID, email); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "清理已过期工作区邀请", err)
	}
	var alreadyMember bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM memberships m JOIN users u ON u.id=m.user_id
        WHERE m.tenant_id=$1 AND lower(u.email)=$2)`, tenantID, email).Scan(&alreadyMember); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "检查邮箱成员关系", err)
	}
	if alreadyMember {
		return nil, types.NewAppError(types.CodeConflict, "该邮箱已经是工作区成员", nil)
	}
	var out types.WorkspaceInvite
	err = tx.QueryRow(ctx, `INSERT INTO workspace_invites(token_hash,tenant_id,email,role,issued_by,expires_at)
        VALUES($1,$2,$3,$4,$5,$6)
        RETURNING id,tenant_id,email,role,issued_by,expires_at,consumed_by,consumed_at,revoked_at,created_at`,
		tokenHash, tenantID, email, role, actorUserID, expiresAt).Scan(
		&out.ID, &out.TenantID, &out.Email, &out.Role, &out.IssuedBy, &out.ExpiresAt,
		&out.ConsumedBy, &out.ConsumedAt, &out.RevokedAt, &out.CreatedAt)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation {
			return nil, types.NewAppError(types.CodeConflict, "该邮箱已有待处理邀请", err)
		}
		return nil, types.NewAppError(types.CodeDatabase, "签发工作区邀请", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO workspace_audit_events(tenant_id,actor_user_id,event_type,metadata)
		VALUES($1,$2,'member.invited',jsonb_build_object('email',$3::text,'role',$4::text))`, tenantID, actorUserID, email, role); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "记录邀请审计事件", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "提交邀请事务", err)
	}
	return &out, nil
}

func (s *Store) ListWorkspaceInvites(ctx context.Context, tenantID, actorUserID int64) ([]types.WorkspaceInvite, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "开启邀请列表事务", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := setWorkspaceControlScope(ctx, tx, tenantID, actorUserID); err != nil {
		return nil, err
	}
	var role types.MembershipRole
	if err := tx.QueryRow(ctx, `SELECT m.role FROM memberships m JOIN tenants t ON t.id=m.tenant_id
        WHERE m.tenant_id=$1 AND m.user_id=$2 AND t.status='active' AND t.deleted_at IS NULL`, tenantID, actorUserID).Scan(&role); err != nil {
		return nil, types.NewAppError(types.CodeNotFound, "工作区不存在或无权查看邀请", err)
	}
	if role != types.MembershipRoleOwner && role != types.MembershipRoleAdmin {
		return nil, types.NewAppError(types.CodeNotFound, "工作区不存在或无权查看邀请", nil)
	}
	rows, err := tx.Query(ctx, `SELECT id,tenant_id,email,role,issued_by,expires_at,consumed_by,consumed_at,revoked_at,created_at
        FROM workspace_invites WHERE tenant_id=$1 ORDER BY created_at DESC,id DESC LIMIT 100`, tenantID)
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "查询工作区邀请", err)
	}
	defer rows.Close()
	var out []types.WorkspaceInvite
	for rows.Next() {
		var invite types.WorkspaceInvite
		if err := rows.Scan(&invite.ID, &invite.TenantID, &invite.Email, &invite.Role, &invite.IssuedBy,
			&invite.ExpiresAt, &invite.ConsumedBy, &invite.ConsumedAt, &invite.RevokedAt, &invite.CreatedAt); err != nil {
			return nil, types.NewAppError(types.CodeDatabase, "扫描工作区邀请", err)
		}
		out = append(out, invite)
	}
	if err := rows.Err(); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "遍历工作区邀请", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "提交邀请列表事务", err)
	}
	return out, nil
}

func (s *Store) RevokeWorkspaceInvite(ctx context.Context, tenantID, actorUserID, inviteID int64) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return types.NewAppError(types.CodeDatabase, "开启撤销邀请事务", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := setWorkspaceControlScope(ctx, tx, tenantID, actorUserID); err != nil {
		return err
	}
	role, err := lockWorkspaceActor(ctx, tx, tenantID, actorUserID)
	if err != nil {
		return err
	}
	if role != types.MembershipRoleOwner && role != types.MembershipRoleAdmin {
		return types.NewAppError(types.CodeNotFound, "工作区不存在或无权撤销邀请", nil)
	}
	var email string
	err = tx.QueryRow(ctx, `UPDATE workspace_invites SET revoked_at=now()
        WHERE id=$1 AND tenant_id=$2 AND consumed_at IS NULL AND revoked_at IS NULL
        RETURNING email`, inviteID, tenantID).Scan(&email)
	if errors.Is(err, pgx.ErrNoRows) {
		return types.NewAppError(types.CodeNotFound, "待处理邀请不存在", err)
	}
	if err != nil {
		return types.NewAppError(types.CodeDatabase, "撤销工作区邀请", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO workspace_audit_events(tenant_id,actor_user_id,event_type,metadata)
		VALUES($1,$2,'member.invite_revoked',jsonb_build_object('invite_id',$3::bigint,'email',$4::text))`, tenantID, actorUserID, inviteID, email); err != nil {
		return types.NewAppError(types.CodeDatabase, "记录邀请撤销事件", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return types.NewAppError(types.CodeDatabase, "提交撤销邀请事务", err)
	}
	return nil
}

func (s *Store) AcceptWorkspaceInvite(ctx context.Context, tokenHash []byte, userID int64) (*types.Workspace, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "开启接受邀请事务", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := setInviteLookupScope(ctx, tx, tokenHash); err != nil {
		return nil, err
	}
	var tenantID int64
	var email string
	var role types.MembershipRole
	if err := tx.QueryRow(ctx, `SELECT tenant_id,email,role FROM workspace_invites
        WHERE token_hash=$1 AND consumed_at IS NULL AND revoked_at IS NULL AND expires_at>now()
        FOR UPDATE`, tokenHash).Scan(&tenantID, &email, &role); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, types.NewAppError(types.CodeNotFound, "邀请不存在或已失效", err)
		}
		return nil, types.NewAppError(types.CodeDatabase, "查询工作区邀请", err)
	}
	var userEmail string
	if err := tx.QueryRow(ctx, `SELECT lower(email) FROM users WHERE id=$1 AND email IS NOT NULL FOR UPDATE`, userID).Scan(&userEmail); err != nil {
		return nil, types.NewAppError(types.CodeNotFound, "当前账号没有可绑定的邮箱", err)
	}
	if userEmail != email {
		return nil, types.NewAppError(types.CodeNotFound, "邀请不存在或与当前账号不匹配", nil)
	}
	if err := acceptWorkspaceInviteTx(ctx, tx, tokenHash, tenantID, userID, role); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "提交接受邀请事务", err)
	}
	return s.GetWorkspaceForUser(ctx, tenantID, userID)
}

// RegisterWithWorkspaceInvite creates the invited account, its personal
// workspace, and the team membership in one transaction. The invitation is
// the admission and seat-cost authority for this path; it is never mixed with
// the separate platform invite ledger.
func (s *Store) RegisterWithWorkspaceInvite(ctx context.Context, email, passwordHash string, tokenHash []byte) (*types.User, *types.Workspace, error) {
	email = NormalizeEmail(email)
	if email == "" || !strings.Contains(email, "@") || passwordHash == "" || len(tokenHash) != 32 {
		return nil, nil, types.NewAppError(types.CodeValidation, "注册信息或邀请无效", nil)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, nil, types.NewAppError(types.CodeDatabase, "开启邀请注册事务", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := setInviteLookupScope(ctx, tx, tokenHash); err != nil {
		return nil, nil, err
	}
	var teamTenantID int64
	var invitedEmail string
	var teamRole types.MembershipRole
	if err := tx.QueryRow(ctx, `SELECT tenant_id,email,role FROM workspace_invites
        WHERE token_hash=$1 AND consumed_at IS NULL AND revoked_at IS NULL AND expires_at>now()
        FOR UPDATE`, tokenHash).Scan(&teamTenantID, &invitedEmail, &teamRole); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil, types.NewAppError(types.CodeNotFound, "邀请不存在或已失效", err)
		}
		return nil, nil, types.NewAppError(types.CodeDatabase, "查询注册邀请", err)
	}
	if invitedEmail != email {
		return nil, nil, types.NewAppError(types.CodeNotFound, "邀请不存在或与邮箱不匹配", nil)
	}
	var u types.User
	err = tx.QueryRow(ctx, `INSERT INTO users(email,password_hash,name)
        VALUES($1,$2,'') RETURNING id,feishu_open_id,name,created_at,email,password_hash,email_verified`, email, passwordHash).
		Scan(&u.ID, &u.FeishuOpenID, &u.Name, &u.CreatedAt, &u.Email, &u.PasswordHash, &u.EmailVerified)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation {
			return nil, nil, types.NewAppError(types.CodeConflict, "该邮箱已注册，请登录后接受邀请", err)
		}
		return nil, nil, types.NewAppError(types.CodeDatabase, "创建受邀用户", err)
	}
	var personalTenantID int64
	if err := tx.QueryRow(ctx, `INSERT INTO tenants(display_name,workspace_kind,personal_owner_user_id,seat_limit)
        VALUES($1,'personal',$2,1) RETURNING id`, email+" 的个人工作区", u.ID).Scan(&personalTenantID); err != nil {
		return nil, nil, types.NewAppError(types.CodeDatabase, "创建个人工作区", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO memberships(tenant_id,user_id,role) VALUES($1,$2,'owner')`, personalTenantID, u.ID); err != nil {
		return nil, nil, types.NewAppError(types.CodeDatabase, "创建个人工作区 owner", err)
	}
	if err := acceptWorkspaceInviteTx(ctx, tx, tokenHash, teamTenantID, u.ID, teamRole); err != nil {
		return nil, nil, err
	}
	if err := setWorkspaceControlScope(ctx, tx, personalTenantID, u.ID); err != nil {
		return nil, nil, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO workspace_audit_events(tenant_id,actor_user_id,target_user_id,event_type)
        VALUES($1,$2,$2,'workspace.created')`, personalTenantID, u.ID); err != nil {
		return nil, nil, types.NewAppError(types.CodeDatabase, "记录个人工作区创建事件", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, nil, types.NewAppError(types.CodeDatabase, "提交邀请注册事务", err)
	}
	if err := s.SeedTenantQuota(ctx, personalTenantID); err != nil {
		// Missing quota fails paid operations closed; account/team membership is
		// still durable and can be repaired without consuming the invite twice.
		slog.Error("受邀注册后初始化个人工作区额度失败", "tenant_id", personalTenantID, "user_id", u.ID, "err", err)
	}
	personal, err := s.GetWorkspaceForUser(ctx, personalTenantID, u.ID)
	if err != nil {
		return &u, nil, err
	}
	return &u, personal, nil
}

func acceptWorkspaceInviteTx(ctx context.Context, tx pgx.Tx, tokenHash []byte, tenantID, userID int64, role types.MembershipRole) error {
	if err := setWorkspaceControlScope(ctx, tx, tenantID, userID); err != nil {
		return err
	}
	var seatLimit, memberCount int
	if err := tx.QueryRow(ctx, `SELECT t.seat_limit,(SELECT count(*) FROM memberships m WHERE m.tenant_id=t.id)
        FROM tenants t WHERE t.id=$1 AND t.workspace_kind='team' AND t.status='active' AND t.deleted_at IS NULL FOR UPDATE`, tenantID).
		Scan(&seatLimit, &memberCount); err != nil {
		return types.NewAppError(types.CodeNotFound, "目标工作区不可用", err)
	}
	if memberCount >= seatLimit {
		return types.NewAppError(types.CodeConflict, "工作区席位已满", nil)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO memberships(tenant_id,user_id,role) VALUES($1,$2,$3)`, tenantID, userID, role); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation {
			return types.NewAppError(types.CodeConflict, "已经是该工作区成员", err)
		}
		return types.NewAppError(types.CodeDatabase, "加入工作区", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE workspace_invites SET consumed_by=$2,consumed_at=now()
        WHERE token_hash=$1 AND consumed_at IS NULL AND revoked_at IS NULL`, tokenHash, userID); err != nil {
		return types.NewAppError(types.CodeDatabase, "消费工作区邀请", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO workspace_audit_events(tenant_id,actor_user_id,target_user_id,event_type)
        VALUES($1,$2,$2,'member.joined')`, tenantID, userID); err != nil {
		return types.NewAppError(types.CodeDatabase, "记录加入工作区事件", err)
	}
	return nil
}

func (s *Store) ListWorkspaceMembers(ctx context.Context, tenantID, actorUserID int64) ([]types.WorkspaceMember, error) {
	rows, err := s.pool.Query(ctx, `SELECT m.tenant_id,m.user_id,COALESCE(u.email,''),u.name,m.role,m.created_at
        FROM memberships m JOIN users u ON u.id=m.user_id
		JOIN tenants t ON t.id=m.tenant_id
        WHERE m.tenant_id=$1 AND t.status='active' AND t.deleted_at IS NULL
		  AND EXISTS(SELECT 1 FROM memberships actor WHERE actor.tenant_id=$1 AND actor.user_id=$2)
		ORDER BY m.created_at,m.user_id`, tenantID, actorUserID)
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "查询工作区成员", err)
	}
	defer rows.Close()
	var out []types.WorkspaceMember
	for rows.Next() {
		var m types.WorkspaceMember
		if err := rows.Scan(&m.TenantID, &m.UserID, &m.Email, &m.Name, &m.Role, &m.JoinedAt); err != nil {
			return nil, types.NewAppError(types.CodeDatabase, "扫描工作区成员", err)
		}
		out = append(out, m)
	}
	if err := rows.Err(); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "遍历工作区成员", err)
	}
	if len(out) == 0 {
		return nil, types.NewAppError(types.CodeNotFound, "工作区不存在或无权访问", nil)
	}
	return out, nil
}

func (s *Store) UpdateWorkspaceMemberRole(ctx context.Context, tenantID, actorUserID, targetUserID int64, role types.MembershipRole) error {
	if role != types.MembershipRoleAdmin && role != types.MembershipRoleMember {
		return types.NewAppError(types.CodeValidation, "成员角色只能改为 admin 或 member", nil)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return types.NewAppError(types.CodeDatabase, "开启角色变更事务", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := setWorkspaceControlScope(ctx, tx, tenantID, actorUserID); err != nil {
		return err
	}
	actorRole, err := lockWorkspaceActor(ctx, tx, tenantID, actorUserID)
	if err != nil {
		return err
	}
	if actorRole != types.MembershipRoleOwner {
		return types.NewAppError(types.CodeNotFound, "工作区不存在或无权修改角色", nil)
	}
	var targetRole types.MembershipRole
	if err := tx.QueryRow(ctx, `SELECT role FROM memberships WHERE tenant_id=$1 AND user_id=$2 FOR UPDATE`, tenantID, targetUserID).Scan(&targetRole); err != nil {
		return types.NewAppError(types.CodeNotFound, "目标成员不存在", err)
	}
	if targetRole == types.MembershipRoleOwner || targetUserID == actorUserID {
		return types.NewAppError(types.CodeConflict, "Owner 角色只能通过所有权转移修改", nil)
	}
	if _, err := tx.Exec(ctx, `UPDATE memberships SET role=$3,updated_at=now() WHERE tenant_id=$1 AND user_id=$2`, tenantID, targetUserID, role); err != nil {
		return types.NewAppError(types.CodeDatabase, "更新成员角色", err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM user_sessions WHERE tenant_id=$1 AND user_id=$2`, tenantID, targetUserID); err != nil {
		return types.NewAppError(types.CodeDatabase, "撤销成员旧会话", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO workspace_audit_events(tenant_id,actor_user_id,target_user_id,event_type,metadata)
		VALUES($1,$2,$3,'member.role_changed',jsonb_build_object('from',$4::text,'to',$5::text))`, tenantID, actorUserID, targetUserID, targetRole, role); err != nil {
		return types.NewAppError(types.CodeDatabase, "记录角色变更事件", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return types.NewAppError(types.CodeDatabase, "提交角色变更事务", err)
	}
	return nil
}

func (s *Store) RemoveWorkspaceMember(ctx context.Context, tenantID, actorUserID, targetUserID int64) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return types.NewAppError(types.CodeDatabase, "开启移除成员事务", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := setWorkspaceControlScope(ctx, tx, tenantID, actorUserID); err != nil {
		return err
	}
	actorRole, err := lockWorkspaceActor(ctx, tx, tenantID, actorUserID)
	if err != nil {
		return err
	}
	var targetRole types.MembershipRole
	if err := tx.QueryRow(ctx, `SELECT role FROM memberships WHERE tenant_id=$1 AND user_id=$2 FOR UPDATE`, tenantID, targetUserID).Scan(&targetRole); err != nil {
		return types.NewAppError(types.CodeNotFound, "目标成员不存在", err)
	}
	if targetRole == types.MembershipRoleOwner {
		return types.NewAppError(types.CodeConflict, "必须先转移所有权", nil)
	}
	allowed := actorRole == types.MembershipRoleOwner ||
		(actorRole == types.MembershipRoleAdmin && targetRole == types.MembershipRoleMember) ||
		(actorUserID == targetUserID && targetRole != types.MembershipRoleOwner)
	if !allowed {
		return types.NewAppError(types.CodeNotFound, "工作区不存在或无权移除该成员", nil)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM user_sessions WHERE tenant_id=$1 AND user_id=$2`, tenantID, targetUserID); err != nil {
		return types.NewAppError(types.CodeDatabase, "撤销成员会话", err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM memberships WHERE tenant_id=$1 AND user_id=$2`, tenantID, targetUserID); err != nil {
		return types.NewAppError(types.CodeConflict, "成员仍拥有不可转移的工作区数据，暂不能移除", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO workspace_audit_events(tenant_id,actor_user_id,target_user_id,event_type)
        VALUES($1,$2,$3,'member.removed')`, tenantID, actorUserID, targetUserID); err != nil {
		return types.NewAppError(types.CodeDatabase, "记录成员移除事件", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return types.NewAppError(types.CodeDatabase, "提交移除成员事务", err)
	}
	return nil
}

func (s *Store) TransferWorkspaceOwnership(ctx context.Context, tenantID, actorUserID, targetUserID int64) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return types.NewAppError(types.CodeDatabase, "开启所有权转移事务", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := setWorkspaceControlScope(ctx, tx, tenantID, actorUserID); err != nil {
		return err
	}
	actorRole, err := lockWorkspaceActor(ctx, tx, tenantID, actorUserID)
	if err != nil {
		return err
	}
	if actorRole != types.MembershipRoleOwner || actorUserID == targetUserID {
		return types.NewAppError(types.CodeNotFound, "工作区不存在或无法转移所有权", nil)
	}
	var kind types.WorkspaceKind
	if err := tx.QueryRow(ctx, `SELECT workspace_kind FROM tenants WHERE id=$1 FOR UPDATE`, tenantID).Scan(&kind); err != nil {
		return types.NewAppError(types.CodeNotFound, "工作区不存在", err)
	}
	if kind != types.WorkspaceKindTeam {
		return types.NewAppError(types.CodeConflict, "个人工作区不能转移所有权", nil)
	}
	var targetRole types.MembershipRole
	if err := tx.QueryRow(ctx, `SELECT role FROM memberships WHERE tenant_id=$1 AND user_id=$2 FOR UPDATE`, tenantID, targetUserID).Scan(&targetRole); err != nil {
		return types.NewAppError(types.CodeNotFound, "目标成员不存在", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE memberships SET role=CASE WHEN user_id=$2 THEN 'member' ELSE 'owner' END,updated_at=now()
        WHERE tenant_id=$1 AND user_id IN ($2,$3)`, tenantID, actorUserID, targetUserID); err != nil {
		return types.NewAppError(types.CodeDatabase, "转移所有权", err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM user_sessions WHERE tenant_id=$1 AND user_id IN ($2,$3)`, tenantID, actorUserID, targetUserID); err != nil {
		return types.NewAppError(types.CodeDatabase, "撤销所有权相关会话", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO workspace_audit_events(tenant_id,actor_user_id,target_user_id,event_type,metadata)
		VALUES($1,$2,$3,'workspace.ownership_transferred',jsonb_build_object('previous_target_role',$4::text))`, tenantID, actorUserID, targetUserID, targetRole); err != nil {
		return types.NewAppError(types.CodeDatabase, "记录所有权转移事件", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return types.NewAppError(types.CodeDatabase, "提交所有权转移事务", err)
	}
	return nil
}

// RotateSession changes the pinned workspace without a window where both old
// and new cookies are valid. Current membership role is re-authorized under
// row locks and will be read again by LookupSession on every request.
func (s *Store) RotateSession(ctx context.Context, oldHash, newHash []byte, userID, tenantID int64, expiresAt time.Time) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return types.NewAppError(types.CodeDatabase, "开启会话切换事务", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var currentUser int64
	if err := tx.QueryRow(ctx, `SELECT user_id FROM user_sessions WHERE token_hash=$1 AND expires_at>now() FOR UPDATE`, oldHash).Scan(&currentUser); err != nil || currentUser != userID {
		return types.NewAppError(types.CodeNotFound, "当前会话不存在或已过期", err)
	}
	if _, err := lockWorkspaceActor(ctx, tx, tenantID, userID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO user_sessions(token_hash,user_id,tenant_id,expires_at) VALUES($1,$2,$3,$4)`, newHash, userID, tenantID, expiresAt); err != nil {
		return types.NewAppError(types.CodeDatabase, "创建切换后会话", err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM user_sessions WHERE token_hash=$1`, oldHash); err != nil {
		return types.NewAppError(types.CodeDatabase, "撤销切换前会话", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return types.NewAppError(types.CodeDatabase, "提交会话切换事务", err)
	}
	return nil
}

func lockWorkspaceActor(ctx context.Context, tx pgx.Tx, tenantID, userID int64) (types.MembershipRole, error) {
	var role types.MembershipRole
	err := tx.QueryRow(ctx, `SELECT m.role FROM memberships m JOIN tenants t ON t.id=m.tenant_id
        WHERE m.tenant_id=$1 AND m.user_id=$2 AND t.status='active' AND t.deleted_at IS NULL
        FOR UPDATE OF m`, tenantID, userID).Scan(&role)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", types.NewAppError(types.CodeNotFound, "工作区不存在或无权访问", err)
	}
	if err != nil {
		return "", types.NewAppError(types.CodeDatabase, fmt.Sprintf("校验工作区 %d 成员身份", tenantID), err)
	}
	if !role.Valid() {
		return "", types.NewAppError(types.CodeConflict, "工作区成员角色无效", nil)
	}
	return role, nil
}

func setWorkspaceControlScope(ctx context.Context, tx pgx.Tx, tenantID, userID int64) error {
	if tenantID <= 0 || userID <= 0 {
		return types.NewAppError(types.CodeValidation, "工作区控制面身份无效", nil)
	}
	if _, err := tx.Exec(ctx, `SELECT set_config('app.tenant_id',$1,true),set_config('app.user_id',$2,true)`,
		fmt.Sprint(tenantID), fmt.Sprint(userID)); err != nil {
		return types.NewAppError(types.CodeDatabase, "设置工作区控制面事务身份", err)
	}
	return nil
}

func setInviteLookupScope(ctx context.Context, tx pgx.Tx, tokenHash []byte) error {
	if len(tokenHash) != sha256.Size {
		return types.NewAppError(types.CodeValidation, "工作区邀请 token 无效", nil)
	}
	if _, err := tx.Exec(ctx, `SELECT set_config('app.workspace_invite_hash',$1,true)`, hex.EncodeToString(tokenHash)); err != nil {
		return types.NewAppError(types.CodeDatabase, "设置工作区邀请查询范围", err)
	}
	return nil
}
