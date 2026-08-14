package api

import (
	"context"
)

// ownerUserID 把「当前 principal」解析成 users 表主键。
//
// 逻辑本体已收敛到 auth 包（企业级契约 §1.1，不变量 I-A1）——本函数只剩一层适配：
// 十个 handler 目前只需要 userID，不需要整个 Principal，故在此收窄，
// 避免十处调用点同时改签名。等接缝② 的租户改造落地、handler 真的需要 TenantID 时，
// 这些调用点会逐个改为直接取 Principal，本函数随之删除。
//
// 错误原样透传：auth 返回的 CodeConflict / CodeInternal 与文案，与收敛前逐字一致，
// handler 的 409 映射行为不变。
func (s *server) ownerUserID(ctx context.Context) (int64, error) {
	p, err := s.deps.Principal.FromContext(ctx)
	if err != nil {
		return 0, err
	}
	return p.UserID, nil
}
