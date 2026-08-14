// Package auth 是全系统**唯一的 principal 来源**（企业级契约 §1.1，不变量 I-A1）。
//
// 为什么要有这一层：改造前「当前用户是谁」被三处各自复述了一遍——
// api/owner.go 的 ownerUserID、a2a/chat.go 的 resolveOwner、cmd/gate/main.go 的
// resolveOwnerUserID（后者注释自认「与 api.ownerUserID 逐字一致」）。三份副本意味着
// 认证一旦要改（加租户、换成真实登录），要同时改三处且极易漏改——而漏改的后果是
// 「某条入口仍以全局 owner 身份执行」，即越权。
//
// 不变量 I-A1：vane 代码中不得出现除 PrincipalResolver 以外的 principal 来源。
// 不变量 I-A4：认证的实现细节不得泄漏出本包——上层只认 Principal，
// 不认「settings 里的 owner」「密码」「token」「OIDC」任何一种具体机制。
// 满足这两条时，从当前的 owner 回退实现换成真实认证（邮箱+密码 / 托管 IdP / 社交登录）
// 是**单点替换，上层零改动**。
//
// 当前实现见 ownerResolver（过渡期：单租户 + 全局 owner，行为与改造前逐字一致）。
package auth

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/YouToco/vane/types"
)

// Principal 是「当前请求以谁的身份执行」。
//
// 刻意不含任何认证机制的痕迹（无 token、无 session、无 open_id）：这正是 I-A4
// 要保护的边界——上层拿到 Principal 就够了，不需要知道它是怎么来的。
type Principal struct {
	TenantID types.TenantID
	UserID   int64
}

// PrincipalResolver 解析当前请求的 principal。api / a2a / gate 依赖本接口而非具体实现。
//
// 方法名是 FromContext 而非契约里写的包级函数 PrincipalFromContext：过渡期的实现需要
// 查库（owner 存在 settings 里），无法做成无依赖的包级函数。真实认证落地后，principal
// 由认证中间件注入 ctx，store 依赖消失，届时可退化为纯 ctx 读取——接口形状不变，
// 上层不受影响。
type PrincipalResolver interface {
	FromContext(ctx context.Context) (Principal, error)
}

// OwnerStore 是 owner 回退实现所需的窄接口（生产实现 *store.Store）。
// 收窄后本包单测可用内存假实现覆盖，不依赖数据库。
type OwnerStore interface {
	GetSetting(ctx context.Context, key string) (json.RawMessage, error)
	UpsertUserByOpenID(ctx context.Context, openID, name string) (*types.User, error)
	ListMembershipsByUser(ctx context.Context, userID int64) ([]types.Membership, error)
}

// ownerRecord 对应 owner 设置项的 value 结构。
type ownerRecord struct {
	OpenID string `json:"open_id"`
	Name   string `json:"name"`
}

// ownerResolver 是**过渡期**的 principal 实现：忽略请求身份，一律回退到「全局 owner」，
// 租户恒为 types.SingleTenantID。
//
// 行为与改造前的三份副本逐字一致（含错误码与文案），本次收敛是纯重构、零行为变更：
//   - 直接读设置项而非走 feishu Manager 的内存缓存：owner 记录落库、跨进程重启存活，
//     而缓存只在飞书通道 enabled+连接后才预热——用户改配置期间来建调度时缓存可能为空。
//   - 拿到 open_id 后用 UpsertUserByOpenID 换主键：owner 首次发消息时飞书 handler 已建好
//     user 行，这里通常是幂等命中；传入 record 里的 name（非空串）避免把已有昵称覆盖成空。
//   - 尚无 owner 时返回 CodeConflict：这是「流程未走到」而非故障，调用方据此回 409
//     并引导用户先发消息。
//
// 已知瑕疵（继承自改造前，不是本次引入）：owner 设置只在首次捕获时写入，而 users.name
// 每条消息都刷新——owner 事后改昵称会让两者漂移，此时这次 upsert 会把 users.name 写回
// 捕获时的旧昵称。只影响展示字段。若日后 store 补上「按 open_id 只读查询」，应改用它。
type ownerResolver struct {
	store OwnerStore
	// settingKey 是 owner 记录在 settings 表里的键名，由装配方传入（生产是
	// feishu.SettingKeyOwner）。**刻意不在本包 import feishu**：auth 是地基层，
	// 不该知道任何推送渠道的存在（企业级契约 §3 要把飞书降为一个 channel）。
	// 由 main.go 传入，也让「owner 来自飞书设置」这件事显式地成为一处过渡期装配细节。
	settingKey string
}

// NewOwnerResolver 构造过渡期的 owner 回退解析器。
//
// 命名带 Owner 是刻意的：它标明「这是单租户阶段的实现」，将来会有
// NewSessionResolver 之类取代它，而 PrincipalResolver 接口保持不变。
func NewOwnerResolver(store OwnerStore, settingKey string) PrincipalResolver {
	return &ownerResolver{store: store, settingKey: settingKey}
}

// FromContext 解析 principal。过渡期忽略 ctx 中的身份信息（当前根本没有），
// 一律回退到全局 owner；ctx 仅用于传递超时与取消。
func (r *ownerResolver) FromContext(ctx context.Context) (Principal, error) {
	raw, err := r.store.GetSetting(ctx, r.settingKey)
	if err != nil {
		if errors.Is(err, types.ErrNotFound) {
			return Principal{}, types.NewAppError(types.CodeConflict,
				"尚未捕获 owner，请先在飞书向导给机器人发一条消息", nil)
		}
		return Principal{}, err
	}
	var rec ownerRecord
	if err := json.Unmarshal(raw, &rec); err != nil {
		return Principal{}, types.NewAppError(types.CodeInternal, "owner 设置格式异常", err)
	}
	if rec.OpenID == "" {
		return Principal{}, types.NewAppError(types.CodeConflict,
			"owner 记录缺少 open_id，请重新完成飞书向导", nil)
	}
	u, err := r.store.UpsertUserByOpenID(ctx, rec.OpenID, rec.Name)
	if err != nil {
		return Principal{}, err
	}

	// 租户**从 memberships 真查**，不写死 SingleTenantID。
	//
	// 写死曾经是对的（迁移把 owner 放进租户 1），但它是个会静默变错的假设：
	// owner 若因任何原因换到别的租户，a2a 与 gate（两条走本解析器的轨）会继续
	// 以租户 1 的身份读写——操作的是别人的数据，且没有任何报错。
	//
	// 多成员时同样响亮失败，与 HTTP 登录路径（api/auth.go 的 resolveTenant）
	// 和写入路径（store/tenantderive.go 的推导子查询）三处对齐：
	// 「尚未支持选择租户」这件事，必须处处都拦住，不能有一处替用户猜。
	ms, err := r.store.ListMembershipsByUser(ctx, u.ID)
	if err != nil {
		return Principal{}, err
	}
	switch len(ms) {
	case 0:
		return Principal{}, types.NewAppError(types.CodeConflict,
			"owner 尚未归属任何租户", nil)
	case 1:
		return Principal{TenantID: types.TenantID(ms[0].TenantID), UserID: u.ID}, nil
	default:
		return Principal{}, types.NewAppError(types.CodeConflict,
			"owner 属于多个租户，当前版本无法确定应使用哪一个", nil)
	}
}
