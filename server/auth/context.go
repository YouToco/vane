package auth

import (
	"context"

	"github.com/YouToco/vane/types"
)

// principalCtxKey 是 principal 在 context 里的键。私有类型防跨包碰撞。
type principalCtxKey struct{}

// WithPrincipal 把已认证的 principal 放进 ctx。**只应由认证中间件调用**——
// 任何其他地方调用它，等于凭空伪造身份。
func WithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, principalCtxKey{}, p)
}

// PrincipalFromContext 从 ctx 读已认证的 principal。
//
// 这是企业级契约 §1.1 一开始就设想的**最终形态**：principal 由认证中间件注入，
// 读取方不查库、不认识任何认证机制。收敛第 0 步时它还做不到（当时 principal
// 要从 settings 里的 owner 现查），故先做成 PrincipalResolver 接口；
// 现在真实认证落地，它终于退化成了一次纯 ctx 读取。
//
// 未认证返回 CodeValidation 而非 CodeNotFound：走到这里却没有 principal，
// 说明中间件漏挂或调用方在鉴权边界之外误用——那是编程错误，不是「查不到数据」。
func PrincipalFromContext(ctx context.Context) (Principal, error) {
	p, ok := ctx.Value(principalCtxKey{}).(Principal)
	if !ok {
		return Principal{}, types.NewAppError(types.CodeValidation,
			"请求上下文缺少已认证身份（认证中间件未挂载？）", nil)
	}
	return p, nil
}

// ctxResolver 是从 ctx 读 principal 的 PrincipalResolver 实现。
//
// 有了它，HTTP 面就不再需要 ownerResolver 的「全局 owner 回退」——
// 每个请求带自己的身份。a2a/gate 两条无 HTTP 会话的轨仍用 ownerResolver
// （见各自装配处的说明），这正是当初把 principal 做成接口的价值：
// 不同入口可以有不同的身份来源，而下游代码一律只认 Principal。
type ctxResolver struct{}

// NewContextResolver 返回从 ctx 读取 principal 的解析器（HTTP 面专用）。
func NewContextResolver() PrincipalResolver { return ctxResolver{} }

func (ctxResolver) FromContext(ctx context.Context) (Principal, error) {
	return PrincipalFromContext(ctx)
}
