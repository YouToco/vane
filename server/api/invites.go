// 邀请码管理端点（D4 准入闸门的管理面）：列出、签发、作废。
// 此前发码的唯一途径是 SSH 上 VPS 跑 useradmin invite——功能对，但每次发码都要
// 登服务器不成体统。三个端点全部锁在 requirePlatformOwner 之后（非 owner 404，
// 不暴露管理面的存在），码的生成与 CLI 同源（auth.NewInviteCode），语义不分叉。
package api

import (
	"net/http"
	"time"

	"github.com/YouToco/vane/server/auth"
	"github.com/YouToco/vane/server/store"
)

// inviteItem 是管理面的邀请码 DTO（与前端 feat/admin-polish 的对接约定）。
//
// used / expired 在服务端算好：状态判定依赖「现在几点」与 max_uses 语义，
// 放前端算就是两份实现两种时钟；used_count/max_uses 仍原样给出，多用码的
// 「用了 1/5」这类展示留给前端自由发挥。
type inviteItem struct {
	Code      string     `json:"code"`
	CreatedAt time.Time  `json:"created_at"`           // 签发时刻（invites.issued_at）
	ExpiresAt *time.Time `json:"expires_at,omitempty"` // null = 永不过期
	MaxUses   int        `json:"max_uses"`
	UsedCount int        `json:"used_count"`
	Used      bool       `json:"used"`    // 已用满（used_count >= max_uses）
	Expired   bool       `json:"expired"` // 已过期（对未用码意味着不可再注册）
	// UsedBy 是最近一次消费租户的 owner 邮箱；未消费、或 owner 是纯飞书用户
	// （无邮箱）时为 null。多用码只反映最近一次（used_count 才是权威计数）。
	UsedBy *string    `json:"used_by,omitempty"`
	UsedAt *time.Time `json:"used_at,omitempty"`
}

func toInviteItem(it store.InviteWithConsumer, now time.Time) inviteItem {
	return inviteItem{
		Code:      it.Code,
		CreatedAt: it.IssuedAt,
		ExpiresAt: it.ExpiresAt,
		MaxUses:   it.MaxUses,
		UsedCount: it.UsedCount,
		Used:      it.Exhausted(),
		Expired:   it.Expired(now),
		UsedBy:    it.ConsumerEmail,
		UsedAt:    it.ConsumedAt,
	}
}

// handleListInvites 返回全部邀请码，新签发在前。
// GET /api/admin/invites → 200 {"invites":[inviteItem]}
func (s *server) handleListInvites(w http.ResponseWriter, r *http.Request) {
	if !s.requirePlatformOwner(w, r) {
		return
	}
	list, err := s.deps.Store.ListInvites(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}
	now := time.Now()
	items := make([]inviteItem, 0, len(list)) // 空列表回 [] 而非 null（与 runstats 同惯例）
	for _, it := range list {
		items = append(items, toInviteItem(it, now))
	}
	writeJSON(w, http.StatusOK, map[string]any{"invites": items})
}

// handleCreateInvite 签发一个新邀请码。
// POST /api/admin/invites → 201 inviteItem
//
// 不接收任何参数：管理面的常规动作就是「给我一个能发出去的码」——一次一码、
// 默认 7 天有效（auth.DefaultInviteExpireDays，与 CLI 同源）。多次使用、
// 自定义有效期这类特殊签发是运维动作，留在 useradmin CLI（那里有相应告警文案）。
func (s *server) handleCreateInvite(w http.ResponseWriter, r *http.Request) {
	if !s.requirePlatformOwner(w, r) {
		return
	}
	// requirePlatformOwner 刚校验过 principal，这里只是从 ctx 再读一份拿 UserID
	// 做 issued_by——与 CLI 的平台自签（NULL）不同，API 有明确的签发人，记下来。
	p, err := auth.PrincipalFromContext(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}
	code, err := auth.NewInviteCode()
	if err != nil {
		writeAppError(w, err)
		return
	}
	expiresAt := time.Now().Add(auth.DefaultInviteExpireDays * 24 * time.Hour)
	inv, err := s.deps.Store.IssueInvite(r.Context(), code, &p.UserID, 1, &expiresAt)
	if err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated,
		toInviteItem(store.InviteWithConsumer{Invite: *inv}, time.Now()))
}

// handleDeleteInvite 作废一个从未被使用的邀请码。
// DELETE /api/admin/invites/{code} → 200 {ok}；已使用 409；不存在 404
//
// 「只许作废未使用的码」的理由在 store.DeleteUnusedInvite：已消费的码是
// 准入账本的一部分。这里不做静默兜底——409 必须如实透传给前端，
// 管理员以为码废了、实际还能用，比看到一条错误糟糕得多。
func (s *server) handleDeleteInvite(w http.ResponseWriter, r *http.Request) {
	if !s.requirePlatformOwner(w, r) {
		return
	}
	code := r.PathValue("code")
	if code == "" {
		writeError(w, http.StatusBadRequest, "缺少邀请码")
		return
	}
	if err := s.deps.Store.DeleteUnusedInvite(r.Context(), code); err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}
