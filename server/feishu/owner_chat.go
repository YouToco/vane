// This file reads the already-frozen owner setting and resolves that same
// user's database id solely to locate a positive historical delivery receipt.
// It does not derive an HTTP/A2A principal or authorize a request; all request
// principal resolution remains in auth.PrincipalResolver.
//
//go:principal-exempt
package feishu

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"

	"github.com/YouToco/vane/types"
)

const ownerChatBackfillTimeout = 5 * time.Second

// backfillOwnerChatID reconstructs the owner's P2P chat from a positive
// historical sent-message receipt. It is intentionally positive-only: no
// delivery or provider-list miss can prove a chat identity.
func (m *Manager) backfillOwnerChatID(ctx context.Context) error {
	if m.st == nil {
		return nil
	}

	m.captureMu.Lock()
	defer m.captureMu.Unlock()

	openID, _, _ := m.ownerIdentity()
	if openID == "" {
		return nil
	}
	raw, err := m.st.GetSetting(ctx, settingKeyOwner)
	if err != nil {
		return types.NewAppError(
			types.CodeNotFound, "没有可回填会话的飞书 owner 设置", nil)
	}
	var owner ownerSetting
	if json.Unmarshal(raw, &owner) != nil || owner.OpenID == "" {
		return types.NewAppError(
			types.CodeValidation, "飞书 owner 设置无法用于会话回填", nil)
	}
	client, currentAppID, ok := m.currentAPI()
	if !ok {
		return types.NewAppError(
			types.CodeConflict, "飞书通道未就绪，无法回填 owner 会话", nil)
	}
	if owner.AppIdentity != "" && owner.AppIdentity != currentAppID {
		m.setOwnerWithChat(
			owner.OpenID,
			owner.Name,
			owner.AppIdentity,
			owner.ChatID,
		)
		return types.NewAppError(
			types.CodeConflict, "飞书 owner 会话属于另一 App", nil)
	}
	if owner.AppIdentity == currentAppID && owner.ChatID != "" {
		if !validOwnerChatID(owner.ChatID) {
			return types.NewAppError(
				types.CodeValidation, "飞书 owner 会话标识无效", nil)
		}
		m.setOwnerWithChat(
			owner.OpenID,
			owner.Name,
			owner.AppIdentity,
			owner.ChatID,
		)
		return nil
	}
	if owner.OpenID != openID {
		return types.NewAppError(
			types.CodeConflict, "飞书 owner 在会话回填期间发生变化", nil)
	}
	user, err := m.st.UpsertUserByOpenID(ctx, owner.OpenID, owner.Name)
	if err != nil {
		return types.NewAppError(
			types.CodeDatabase, "无法解析飞书 owner 用于会话回填", nil)
	}
	messageID, err := m.st.LatestSentDeliveryMessageID(ctx, user.ID)
	if err != nil {
		if errors.Is(err, types.ErrNotFound) {
			return types.NewAppError(
				types.CodeNotFound, "没有可用于会话回填的历史发送回执", nil)
		}
		return types.NewAppError(
			types.CodeDatabase, "无法读取会话回填的历史发送回执", nil)
	}
	fetchCtx, cancel := context.WithTimeout(ctx, ownerChatBackfillTimeout)
	defer cancel()
	resp, err := client.Im.Message.Get(
		fetchCtx,
		larkim.NewGetMessageReqBuilder().MessageId(messageID).Build(),
	)
	if err != nil {
		return types.NewAppError(
			types.CodePushFailed, "飞书历史消息查询失败，无法回填 owner 会话", nil)
	}
	if !resp.Success() || resp.Data == nil || len(resp.Data.Items) != 1 {
		return types.NewAppError(
			types.CodeConflict, "飞书历史消息不能作为 owner 会话证据", nil)
	}
	item := resp.Data.Items[0]
	if item == nil || item.MessageId == nil || *item.MessageId != messageID ||
		item.ChatId == nil || !validOwnerChatID(*item.ChatId) ||
		*item.ChatId == "" {
		return types.NewAppError(
			types.CodeConflict, "飞书历史消息缺少精确 owner 会话证据", nil)
	}

	if !m.appIsCurrent(currentAppID) {
		return types.NewAppError(
			types.CodeConflict, "飞书 App 在会话回填期间发生变化", nil)
	}
	owner.AppIdentity = currentAppID
	owner.ChatID = *item.ChatId
	value, err := json.Marshal(owner)
	if err != nil {
		return types.NewAppError(
			types.CodeValidation, "飞书 owner 会话设置无法序列化", nil)
	}
	if err := m.st.PutSetting(ctx, settingKeyOwner, value); err != nil {
		return types.NewAppError(
			types.CodeDatabase, "无法持久化飞书 owner 会话", nil)
	}
	m.setOwnerWithChat(
		owner.OpenID,
		owner.Name,
		owner.AppIdentity,
		owner.ChatID,
	)
	return nil
}
