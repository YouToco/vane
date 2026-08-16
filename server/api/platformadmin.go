package api

import (
	"net/http"

	"github.com/YouToco/vane/server/auth"
	"github.com/YouToco/vane/server/types"
)

// platformOwnerTenantID 是承载存量数据与平台级配置的租户号
// （migration 018 回填的那个）。
const platformOwnerTenantID = types.SingleTenantID

// requirePlatformOwner 是**平台级端点**的临时闸门。
//
// # 为什么需要它（对抗式安全审查的两条 CRITICAL/HIGH 发现）
//
// 有一批端点是单 owner 时代写的，它们操作的是**平台全局状态**而非某个租户的数据：
//   - /api/feishu/config —— 写的是 settings 表里**全局唯一**的飞书应用凭证。
//     真多用户下，任一受邀用户改掉它即可劫持**所有租户**的推送通道（把机器人
//     指向自己的飞书应用），这是跨租户完全沦陷。
//   - /api/feishu/status、/api/feishu/test —— 泄漏 owner 身份、可向 owner 定向发卡。
//   - /api/admin/observability、/api/admin/runstats、/api/admin/cost-calls
//     —— 返回**全库**聚合或逐笔调用账单
//     （成本、token 用量、推送量），任一登录用户可读走全平台经营数据。
//
// 这些端点的正解是接缝②：配置与统计都要按租户切分（飞书凭证 per-tenant 由决议
// D5 明确，统计要带 tenant_id 过滤）。**在那之前，把它们锁给平台 owner** ——
// 这不是最终形态，但它把「任一注册用户即可劫持全平台」压缩回「只有平台主人能动」，
// 与改造前的安全水位持平。
//
// 判据同时要求平台租户 1 与该租户的 exact active owner membership；仅仅是
// 租户 1 的 member/admin 不是超级管理员，不能修改共享 LLM 或读取平台数据。
//
// TODO(接缝②)：租户隔离落地后，本闸门应逐个端点解除——
// feishu 配置改为 per-tenant 凭证，admin 统计改为带租户过滤的自助视图。
func (s *server) requirePlatformOwner(w http.ResponseWriter, r *http.Request) bool {
	p, err := auth.PrincipalFromContext(r.Context())
	if err != nil {
		writeAppError(w, err)
		return false
	}
	if p.TenantID != platformOwnerTenantID ||
		!s.hasExactOwnerMembership(r, p) {
		// 回 404 而非 403：不向普通租户暴露「存在这么一个平台管理面」。
		writeError(w, http.StatusNotFound, "接口不存在")
		return false
	}
	return true
}

func (s *server) requireTenantOwner(w http.ResponseWriter, r *http.Request) (auth.Principal, bool) {
	p, err := auth.PrincipalFromContext(r.Context())
	if err != nil {
		writeAppError(w, err)
		return auth.Principal{}, false
	}
	if !s.hasExactOwnerMembership(r, p) {
		writeError(w, http.StatusNotFound, "接口不存在")
		return auth.Principal{}, false
	}
	return p, true
}

func (s *server) hasExactOwnerMembership(r *http.Request, p auth.Principal) bool {
	if s.deps.Auth == nil {
		return false
	}
	memberships, err := s.deps.Auth.ListMembershipsByUser(r.Context(), p.UserID)
	if err != nil {
		return false
	}
	for _, membership := range memberships {
		if membership.TenantID == int64(p.TenantID) &&
			membership.UserID == p.UserID &&
			membership.Role == types.MembershipRoleOwner {
			return true
		}
	}
	return false
}
