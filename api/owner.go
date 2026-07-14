package api

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/YouToco/vane/feishu"
	"github.com/YouToco/vane/types"
)

// ownerRecord 对应 settings.feishu_owner 的 value 结构（与 feishu 包内部的 ownerSetting 同形）。
// 刻意在 api 包本地复述而非依赖 feishu 导出：字段少且稳定，跨包导出一个纯 DTO
// 只为读两个字段不划算；真正的耦合点（key 名）已由 feishu.SettingKeyOwner 收口。
type ownerRecord struct {
	OpenID string `json:"open_id"`
	Name   string `json:"name"`
}

// ownerUserID 把"当前 owner"解析成 users 表主键（M3 单 owner 模型：所有调度/推送都归属 owner）。
//
// 直接读 settings.feishu_owner 而非走 Manager 内存缓存：owner 记录落库、跨进程重启存活，
// 而缓存只在飞书通道 enabled+连接后才预热——若用户改配置期间来建调度，缓存可能为空。
// 拿到 open_id 后用 UpsertUserByOpenID 换主键：owner 首次发消息时飞书 handler 已建好 user 行，
// 这里通常是幂等命中；传入 record 里的 name（非空串）避免把已有昵称覆盖成空。
//
// 尚无 owner 时返回 CodeConflict：这是"流程未走到"而非故障，handler 据此回 409 引导用户先发消息。
func (s *server) ownerUserID(ctx context.Context) (int64, error) {
	raw, err := s.deps.Store.GetSetting(ctx, feishu.SettingKeyOwner)
	if err != nil {
		if errors.Is(err, types.ErrNotFound) {
			return 0, types.NewAppError(types.CodeConflict,
				"尚未捕获 owner，请先在飞书向导给机器人发一条消息", nil)
		}
		return 0, err
	}
	var rec ownerRecord
	if err := json.Unmarshal(raw, &rec); err != nil {
		return 0, types.NewAppError(types.CodeInternal, "owner 设置格式异常", err)
	}
	if rec.OpenID == "" {
		return 0, types.NewAppError(types.CodeConflict, "owner 记录缺少 open_id，请重新完成飞书向导", nil)
	}
	u, err := s.deps.Store.UpsertUserByOpenID(ctx, rec.OpenID, rec.Name)
	if err != nil {
		return 0, err
	}
	return u.ID, nil
}
