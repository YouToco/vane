// 飞书接入管理端点：状态查询 / 凭证校验 / 保存配置 / 测试卡片（契约 §5）。
package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/YouToco/vane/feishu"
	"github.com/YouToco/vane/types"
)

// feishuCredentials 是 verify / config 共用的请求体。
type feishuCredentials struct {
	AppID     string `json:"app_id"`
	AppSecret string `json:"app_secret"`
}

// decodeCredentials 解析并校验凭证请求体；失败时已写好 400 响应，调用方直接 return。
func decodeCredentials(w http.ResponseWriter, r *http.Request) (feishuCredentials, bool) {
	r.Body = http.MaxBytesReader(w, r.Body, 8<<10) // 凭证请求体 8KB 足够
	var creds feishuCredentials
	if err := json.NewDecoder(r.Body).Decode(&creds); err != nil {
		writeError(w, http.StatusBadRequest, "请求体不是合法 JSON")
		return creds, false
	}
	if creds.AppID == "" || creds.AppSecret == "" {
		writeError(w, http.StatusBadRequest, "app_id 与 app_secret 均不能为空")
		return creds, false
	}
	return creds, true
}

// handleFeishuStatus 返回当前连接状态。
// GET /api/feishu/status → 200 feishu.Status JSON
func (s *server) handleFeishuStatus(w http.ResponseWriter, r *http.Request) {
	if !s.requirePlatformOwner(w, r) {
		return
	}
	writeJSON(w, http.StatusOK, s.deps.Manager.Status())
}

// handleFeishuVerify 只校验凭证，不保存（向导第 3 步的[检测]按钮）。
// POST /api/feishu/verify → 200 feishu.VerifyResult
func (s *server) handleFeishuVerify(w http.ResponseWriter, r *http.Request) {
	if !s.requirePlatformOwner(w, r) {
		return
	}
	creds, ok := decodeCredentials(w, r)
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, s.deps.Manager.Verify(r.Context(), creds.AppID, creds.AppSecret))
}

// handleFeishuConfig 校验 → 保存 settings → Reconfigure。
// POST /api/feishu/config → 200 {"status":Status,"verify":VerifyResult} / 400
//
// 凭证本身无效时拒绝保存（存了也连不上，只会留一份坏配置）；
// 但 BotOK=false（如应用尚未发布版本，activate_status=3）不阻塞保存——
// 向导第 4 步发布后自然就绪（契约 §8 明确此取舍）。
func (s *server) handleFeishuConfig(w http.ResponseWriter, r *http.Request) {
	if !s.requirePlatformOwner(w, r) {
		return
	}
	creds, ok := decodeCredentials(w, r)
	if !ok {
		return
	}

	verify := s.deps.Manager.Verify(r.Context(), creds.AppID, creds.AppSecret)
	if !verify.CredentialsOK {
		writeError(w, http.StatusBadRequest, "凭证校验失败："+verify.Detail)
		return
	}

	// value 结构固定为契约 §1 的 feishu key：{"app_id","app_secret","enabled"}。
	value, err := json.Marshal(map[string]any{
		"app_id":     creds.AppID,
		"app_secret": creds.AppSecret,
		"enabled":    true,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "序列化配置失败")
		return
	}
	if err := s.deps.Store.PutSetting(r.Context(), feishu.SettingKeyFeishu, value); err != nil {
		slog.Error("api: 保存飞书配置失败", "err", err)
		writeError(w, http.StatusInternalServerError, "保存配置失败，请稍后重试")
		return
	}

	if err := s.deps.Manager.Reconfigure(r.Context()); err != nil {
		slog.Error("api: 飞书重连失败", "err", err)
		// 配置已落库：前端可轮询 status 观察后续状态，这里如实报错。
		writeError(w, http.StatusInternalServerError, "配置已保存但重新连接失败，请稍后查看状态")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"status": s.deps.Manager.Status(),
		"verify": verify,
	})
}

// handleFeishuTest 给 owner 发测试卡片（向导第 5 步）。
// POST /api/feishu/test → 200 {"ok":true} / 409 尚无 owner
func (s *server) handleFeishuTest(w http.ResponseWriter, r *http.Request) {
	if !s.requirePlatformOwner(w, r) {
		return
	}
	if err := s.deps.Manager.SendTestCard(r.Context()); err != nil {
		// 无 owner 是"流程未走到"而非故障，用 409 引导用户先发一条消息。
		if errors.Is(err, types.ErrNotFound) {
			writeError(w, http.StatusConflict, "还没有捕获到 owner，请先给机器人发一条消息")
			return
		}
		slog.Error("api: 发送测试卡片失败", "err", err)
		writeError(w, http.StatusBadGateway, "发送测试卡片失败，请稍后重试")
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}
