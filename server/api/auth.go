// 注册 / 登录 / 登出 / 会话自检（企业级契约 §1.1，决议 D2′/D4）。
//
// 本文件在 D2′ 落地时**整体重写**：改造前是「一个共享密码 + 无状态 HMAC token」，
// 没有用户概念——密码即凭据，谁知道谁就是主人。真 SaaS 下每个租户有自己的账号，
// 故改为「邮箱+密码 + 服务端会话表」。共享密码路径已彻底移除，不保留兼容窗口
// （Boss 拍板：直接换掉；存量 owner 的接管路径见 cmd/gate 的 set-password）。
package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/YouToco/vane/server/auth"
	"github.com/YouToco/vane/server/types"
)

const (
	sessionCookieName = "vane_session"
	sessionTTL        = 30 * 24 * time.Hour
	// authBodyLimit 是认证端点的请求体上限。这些端点**无需鉴权**即可访问，
	// 不设上限等于给出免费的内存 DoS 面。4KB 远超任何合法的邮箱+密码+邀请码。
	authBodyLimit = 4 << 10
	// uniformAuthDelay 是认证失败的固定延迟，压低在线爆破速率。
	uniformAuthDelay = 200 * time.Millisecond
)

// authFailMsg 是**所有**认证失败的统一文案。
//
// 不区分「邮箱不存在」与「密码错误」是硬性要求，不是含糊其辞:一旦区分，
// 登录接口就成了账号枚举器——攻击者批量试邮箱，按回哪种错即可确认哪些邮箱
// 注册过本站，拿去做撞库、钓鱼、或仅仅是泄漏用户名单本身。
const authFailMsg = "邮箱或密码不正确"

// handleRegister 邀请码注册。
// POST /api/auth/register {"email","password","invite_code"} → 200 + Set-Cookie
func (s *server) handleRegister(w http.ResponseWriter, r *http.Request) {
	if !s.checkOrigin(w, r) {
		return
	}
	if !s.limiter.allowAndRecord(ipLimitKey(r), time.Now()) {
		writeError(w, http.StatusTooManyRequests, "操作过于频繁，请稍后再试")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, authBodyLimit)

	var req struct {
		Email      string `json:"email"`
		Password   string `json:"password"`
		InviteCode string `json:"invite_code"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "请求体不是合法 JSON")
		return
	}
	req.Email = strings.TrimSpace(req.Email)
	if req.Email == "" || !strings.Contains(req.Email, "@") {
		writeError(w, http.StatusBadRequest, "请填写合法邮箱")
		return
	}
	// 密码强度在哈希**之前**校验：既给用户明确反馈，也避免为不合规输入白跑一次
	// argon2（19MiB 内存 × 每次请求）。
	if err := auth.ValidatePassword(req.Password); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	// **邀请码先于哈希校验**（安全审查 CRITICAL）：HashPassword 每次要 19MiB argon2，
	// 若放在邀请码校验之前，匿名攻击者用伪造邀请码即可反复触发昂贵计算——
	// 未鉴权端点就成了内存 DoS 放大器。这里先做一次廉价只读预检把明显无效的挡掉；
	// 真正的原子消费仍在 RegisterWithInvite 的事务里（预检与消费之间的竞态由事务兜住，
	// 预检只做成本过滤、不承担正确性）。
	if ok, ierr := s.deps.Auth.InviteUsable(r.Context(), req.InviteCode); ierr != nil {
		writeAppError(w, ierr)
		return
	} else if !ok {
		writeError(w, http.StatusBadRequest, "邀请码无效或已失效")
		return
	}

	hash, err := auth.HashPasswordCtx(r.Context(), req.Password)
	if err != nil {
		slog.Error("api: 密码哈希失败", "err", err)
		writeError(w, http.StatusInternalServerError, "服务器内部错误，请稍后重试")
		return
	}

	u, tenant, err := s.deps.Auth.RegisterWithInvite(r.Context(), req.Email, hash, req.InviteCode)
	if err != nil {
		// 邀请码无效与邮箱已注册都如实告知：前者用户需要拿到有效码才能继续，
		// 后者是用户自己的邮箱、不构成他人信息泄漏（且注册接口天然可被用于
		// 探测邮箱是否已注册，业界通行做法是靠限流而非含糊文案来控制）。
		writeAppError(w, err)
		return
	}

	s.issueSession(w, r, u.ID, tenant.ID)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "tenant_id": tenant.ID})
}

// handleLogin 邮箱+密码登录。
// POST /api/auth/login {"email","password"} → 200 + Set-Cookie / 401
func (s *server) handleLogin(w http.ResponseWriter, r *http.Request) {
	if !s.checkOrigin(w, r) {
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, authBodyLimit)

	var req struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "请求体不是合法 JSON")
		return
	}

	// 双维度限流：来源维度挡「一个源猜很多号」，账号维度挡「很多源猜一个号」。
	// 只有前者时，换 IP 即可无限猜同一个账号的密码。
	now := time.Now()
	if !s.limiter.allowAndRecord(ipLimitKey(r), now) ||
		!s.limiter.allowAndRecord(accountLimitKey(req.Email), now) {
		writeError(w, http.StatusTooManyRequests, "登录尝试过于频繁，请稍后再试")
		return
	}

	// **时序对齐**（安全审查 HIGH）：固定延迟原本是「失败后再睡 200ms」，
	// 而「邮箱不存在」走不到 argon2（省下约 30ms）、「密码错」要跑完 argon2——
	// 两条路径总耗时因此可区分，构成账号枚举的时序旁路。
	// 改为记录起点、在统一出口**对齐到同一截止时刻**，抹平路径差异。
	deadline := now.Add(uniformAuthDelay)

	u, err := s.deps.Auth.GetUserByEmail(r.Context(), req.Email)
	if err != nil {
		// **不区分「用户不存在」**——统一走失败路径，使响应内容与耗时都不泄漏
		// 该邮箱是否注册过。这里仍要跑一次等价的 argon2（对着固定假哈希），
		// 否则「不存在」会比「密码错」少花一次 argon2 的时间。
		auth.DummyVerify(r.Context(), req.Password)
		s.authFail(w, deadline)
		return
	}
	if u.PasswordHash == nil {
		// 纯飞书身份的用户没有密码（存量 owner 未接管前即是此态）。
		auth.DummyVerify(r.Context(), req.Password)
		s.authFail(w, deadline)
		return
	}
	if err := auth.VerifyPasswordCtx(r.Context(), *u.PasswordHash, req.Password); err != nil {
		if !errors.Is(err, auth.ErrPasswordMismatch) {
			// 哈希串损坏是数据问题，不是用户输错——落日志供排查，对外仍统一文案。
			slog.Error("api: 密码哈希损坏", "user_id", u.ID, "err", err)
		}
		s.authFail(w, deadline)
		return
	}

	tenantID, err := s.resolveTenant(r, u.ID)
	if err != nil {
		writeAppError(w, err)
		return
	}

	// 登录成功后按当前参数无感升级旧哈希（参数调高后的平滑迁移）。
	if auth.NeedsRehash(*u.PasswordHash) {
		if nh, herr := auth.HashPasswordCtx(r.Context(), req.Password); herr == nil {
			if uerr := s.deps.Auth.UpdatePasswordHash(r.Context(), u.ID, nh); uerr != nil {
				slog.Warn("api: 密码哈希升级失败（不影响本次登录）", "user_id", u.ID, "err", uerr)
			}
		}
	}

	s.issueSession(w, r, u.ID, tenantID)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "tenant_id": tenantID})
}

// authFail 是统一的认证失败出口：**对齐到同一截止时刻** + 统一文案。
//
// 睡到 deadline 而不是「再睡固定时长」：后者把各路径的固有耗时差原样保留
// （不存在的邮箱省掉一次 argon2，比密码错快约 30ms），构成账号枚举的时序旁路。
// 对齐后所有失败路径的总耗时收敛到同一个值。
// 限流已在入口处 allowAndRecord 计过，此处不再重复计数。
func (s *server) authFail(w http.ResponseWriter, deadline time.Time) {
	if d := time.Until(deadline); d > 0 {
		time.Sleep(d)
	}
	writeError(w, http.StatusUnauthorized, authFailMsg)
}

// checkOrigin 是未鉴权端点的 CSRF 防线（安全审查 HIGH：登录 CSRF）。
//
// 攻击场景：攻击者诱导受害者浏览器 POST /api/auth/login（表单可跨站提交，
// 无需读响应），把受害者**静默登进攻击者的账号**——此后受害者的一切操作
// （加信源、改画像）都记在攻击者账号下，攻击者随后登录即可全部读走。
//
// 防线：状态变更请求必须带可信 Origin。浏览器**不允许**页面伪造 Origin 头，
// 而跨站表单提交必然带上发起页的 Origin，故按 Origin 判定可靠。
// 无 Origin 头的请求（curl / 服务端调用）放行——它们不是 CSRF 的载体，
// CSRF 的前提是受害者浏览器自动携带 cookie。
func (s *server) checkOrigin(w http.ResponseWriter, r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" || (s.deps.Origin != "" && origin == s.deps.Origin) {
		return true
	}
	writeError(w, http.StatusForbidden, "请求来源不被允许")
	return false
}

// resolveTenant 取用户所属租户。当前每用户恒 1 个（D8：初期每租户 1 人），
// 多租户成员出现后这里要改成「按请求选租户」，届时会话仍钉住选定的那个。
func (s *server) resolveTenant(r *http.Request, userID int64) (int64, error) {
	if workspaces, ok := s.workspaceStore(); ok {
		return workspaces.DefaultWorkspaceForUser(r.Context(), userID)
	}
	ms, err := s.deps.Auth.ListMembershipsByUser(r.Context(), userID)
	if err != nil {
		return 0, err
	}
	switch len(ms) {
	case 0:
		// 有账号却无任何租户归属：数据不一致（注册流保证二者同事务产生）。
		return 0, types.NewAppError(types.CodeConflict,
			"账号尚未归属任何租户，请联系管理员", nil)
	case 1:
		return ms[0].TenantID, nil
	default:
		// **多租户成员：响亮失败，绝不替用户猜**。
		//
		// 原实现是 `return ms[0].TenantID`——静默选列表里的第一条。那意味着一个属于
		// 两个租户的人会登进「碰巧排在前面」的那个，而且毫不知情：他看到的是一份
		// 陌生的信源与推送历史，却没有任何提示告诉他「你在另一个租户里」。
		//
		// 这与写入侧的行为对齐（store/tenantderive.go）：那里的推导子查询在用户属于
		// 多个租户时会因「returned more than one row」直接报错。同一件「尚未支持」的事，
		// 两处必须都拦住，不能一处拦、一处骗。
		//
		// 解除条件：登录流支持「选择要进入哪个租户」（前端出选择页、会话记录选定值）。
		// 届时本分支替换为「按请求参数选租户，并校验该用户确实是其成员」。
		return 0, types.NewAppError(types.CodeConflict,
			"该账号属于多个租户，当前版本尚不支持在登录时选择，请联系管理员", nil)
	}
}

// issueSession 签发会话并下发 cookie。
//
// **每次登录都签发全新 token**（而非复用），这是防会话固定攻击的关键：
// 攻击者若能预先把一个已知 token 塞进受害者浏览器，登录后该 token 依然有效
// 的话，攻击者就直接持有了已认证会话。新签发让预置的 token 永远停留在未认证态。
func (s *server) issueSession(w http.ResponseWriter, r *http.Request, userID, tenantID int64) {
	token, hash, err := auth.NewSessionToken()
	if err != nil {
		slog.Error("api: 生成会话 token 失败", "err", err)
		writeError(w, http.StatusInternalServerError, "服务器内部错误，请稍后重试")
		return
	}
	exp := time.Now().Add(sessionTTL)
	if err := s.deps.Auth.CreateSession(r.Context(), hash, userID, tenantID, exp); err != nil {
		slog.Error("api: 会话落库失败", "user_id", userID, "err", err)
		writeError(w, http.StatusInternalServerError, "服务器内部错误，请稍后重试")
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    token,
		Path:     "/",
		Expires:  exp,
		HttpOnly: true, // 挡 XSS 窃取会话
		Secure:   true, // 只走 HTTPS
		SameSite: http.SameSiteLaxMode,
	})
}

// handleLogout 登出：**删库里的会话**（而非仅清 cookie）。
//
// 改造前只清 cookie、token 到期前理论上仍有效——那意味着「登出」在被窃取
// token 的场景下毫无作用，而那恰恰是最需要登出生效的场景。现在服务端删除，
// 即刻失效。
func (s *server) handleLogout(w http.ResponseWriter, r *http.Request) {
	if c, err := r.Cookie(sessionCookieName); err == nil && c.Value != "" {
		if err := s.deps.Auth.DeleteSession(r.Context(), auth.HashSessionToken(c.Value)); err != nil {
			slog.Warn("api: 删除会话失败", "err", err)
		}
	}
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteLaxMode,
	})
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// handleMe 返回当前登录身份，供前端探测登录态与展示用户块（头像/邮箱）。
func (s *server) handleMe(w http.ResponseWriter, r *http.Request) {
	p, err := auth.PrincipalFromContext(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}
	// email 仅供界面展示；查询失败降级为空，不阻断登录态探测（me 的首要职责）。
	email, eerr := s.deps.Auth.GetUserEmailByID(r.Context(), p.UserID)
	if eerr != nil {
		slog.Warn("me: 查询用户邮箱失败，降级为空", "user_id", p.UserID, "err", eerr)
		email = ""
	}
	response := map[string]any{
		"ok": true, "user_id": p.UserID, "tenant_id": int64(p.TenantID), "email": email,
		"role": p.Role, "actor_type": p.ActorType,
	}
	if workspaces, ok := s.workspaceStore(); ok {
		items, werr := workspaces.ListWorkspacesForUser(r.Context(), p.UserID)
		if werr != nil {
			slog.Warn("me: 查询工作区列表失败，降级为当前工作区", "user_id", p.UserID, "err", werr)
		} else {
			response["workspaces"] = items
		}
	}
	writeJSON(w, http.StatusOK, response)
}

// requireSession 是 /api/* 的认证中间件：校验会话 cookie → 把 principal 注入 ctx。
//
// 注入 ctx 是整条链路的关键：下游 handler 一律走 auth.PrincipalFromContext，
// 不再有任何「全局 owner」的回退（不变量 I-A1）。中间件漏挂时下游拿不到
// principal 而直接报错，不会静默降级成主人身份。
func (s *server) requireSession(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isPublicAuthPath(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}
		c, err := r.Cookie(sessionCookieName)
		if err != nil || c.Value == "" {
			writeError(w, http.StatusUnauthorized, "未登录或会话已过期")
			return
		}
		sess, err := s.deps.Auth.LookupSession(r.Context(), auth.HashSessionToken(c.Value))
		if err != nil {
			// 过期与不存在在 store 层已归一，这里一律按未登录处理。
			writeError(w, http.StatusUnauthorized, "未登录或会话已过期")
			return
		}
		ctx := auth.WithPrincipal(r.Context(), auth.Principal{
			TenantID:  types.TenantID(sess.TenantID),
			UserID:    sess.UserID,
			Role:      sess.Role,
			ActorType: sess.ActorType,
		})
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// isPublicAuthPath 列出**唯二**无需会话即可访问的路径。
// 写成显式白名单而非前缀匹配：`/api/auth/` 前缀会连 logout/me 一起放行，
// 而那两个需要会话。
func isPublicAuthPath(p string) bool {
	return p == "/api/auth/login" || p == "/api/auth/register" ||
		p == "/api/auth/workspace-invites/register" ||
		p == "/api/auth/email-verification/verify" ||
		p == "/api/auth/password-reset/request" ||
		p == "/api/auth/password-reset/complete"
}
