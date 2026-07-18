package types

// TenantID 是租户主键（企业级契约 §1）。
//
// 现在恒为 SingleTenantID：vane 尚处单租户阶段，tenants 表与 tenant_id 列都还没建。
// 独立成类型而不是直接用 int64，是为了让「租户维度」在多租户改造前就存在于类型系统里——
// 等真的加 tenant_id 列时，编译器能帮忙找出所有该带租户而没带的地方。
type TenantID int64

// SingleTenantID 是单租户阶段的恒定租户号（企业级契约 §1.1 过渡期约定）。
//
// 多租户落地时本常量作废，届时 TenantID 来自请求身份而非常量——
// 用 grep 找本常量的引用，就是「还没接上真实租户的地方」的完整清单。
const SingleTenantID TenantID = 1
