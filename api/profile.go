// 画像只读端点（M7 功能 6.3）：Dashboard 查看当前 owner 的结构化标签与摘要画像。
// 只读——不写表、不调模型、不碰 profiles 写路径（人工/演化三方法，见 store/profiles.go
// 头注释），画像编辑写回涉及演化恒赢逻辑（Gate ⑧ removed_tags）故留二期，本期只展示。
// 挂 /api/ 前缀自动继承会话中间件（单用户阶段 Dashboard 密码即 owner 凭证，
// 理由同 deliveries.go / observability.go）。
package api

import "net/http"

// handleProfile 返回当前 owner 的画像（结构化标签 + 摘要，摘要尾部含演化写入的负偏好句式）。
// GET /api/profile → 200 *types.Profile；owner 未捕获 → 409；画像尚未生成 → 404（前端按空态处理）。
func (s *server) handleProfile(w http.ResponseWriter, r *http.Request) {
	userID, err := s.ownerUserID(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}
	p, err := s.deps.Store.GetProfile(r.Context(), userID)
	if err != nil {
		writeAppError(w, err) // 无行 → CodeNotFound → 404
		return
	}
	writeJSON(w, http.StatusOK, p)
}
