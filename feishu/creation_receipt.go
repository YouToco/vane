package feishu

import (
	"context"
	"fmt"

	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"

	"github.com/YouToco/vane/task"
	"github.com/YouToco/vane/types"
)

// SendCreationReceipt 原地更新一张由 Vane 发出的确认卡，兑现耐久的任务创建
// 终态回执。它刻意使用 Message.Patch 而不是再 Create 一条消息：同一
// messageID + 同一冻结 cardJSON 的重复调用只覆盖同一个资源，因而可以收敛
// “飞书已成功、HTTP 响应丢失、数据库尚未记 sent”这一不确定窗口，而不会
// 给用户制造第二条结果消息。
//
// 确认卡和终态卡都必须带 config.update_multi=true；构卡函数已钉死该约束。
func (m *Manager) SendCreationReceipt(
	ctx context.Context,
	provider string,
	messageID string,
	cardJSON string,
) error {
	return m.sendCardPatchReceipt(
		ctx, provider, messageID, cardJSON, "创建",
	)
}

// SendDefinitionEditReceipt converges a terminal definition-edit outbox onto
// the same immutable confirmation-card resource. It shares the app-bound Patch
// adapter with creation receipts but never creates a new message.
func (m *Manager) SendDefinitionEditReceipt(
	ctx context.Context,
	provider string,
	messageID string,
	cardJSON string,
) error {
	return m.sendCardPatchReceipt(
		ctx, provider, messageID, cardJSON, "编辑",
	)
}

func (m *Manager) sendCardPatchReceipt(
	ctx context.Context,
	provider string,
	messageID string,
	cardJSON string,
	kind string,
) error {
	if !task.IsFeishuCardPatchReceiptProvider(provider) {
		return types.NewAppError(
			types.CodeValidation, kind+"回执通道身份无效", nil,
		)
	}
	if messageID == "" {
		return types.NewAppError(
			types.CodeValidation,
			kind+"回执目标 message_id 为空", nil,
		)
	}
	if cardJSON == "" {
		return types.NewAppError(
			types.CodeValidation, kind+"回执卡片内容为空", nil,
		)
	}

	// 与普通主动推送不同，这里由耐久 dispatcher 驱动：进程刚启动、飞书通道
	// 尚未装好客户端属于可恢复的瞬态状态，必须保留为 PUSH_FAILED/retryable。
	m.mu.Lock()
	client, appID := m.apiClient, m.apiAppID
	m.mu.Unlock()
	if client == nil {
		return types.NewAppError(
			types.CodePushFailed,
			"飞书通道未就绪，无法更新"+kind+"回执", nil,
		)
	}
	if provider != task.FeishuCardPatchReceiptProviderForApp(appID) {
		// A different current app has no authority over the resource emitted by
		// the original app. Keep this retryable: disabling or temporarily
		// switching channels must not discard the durable receipt, and returning
		// to the original app (including secret rotation) can converge it later.
		return types.NewAppError(types.CodePushFailed,
			"当前飞书 App 与"+kind+"回执所属 App 不一致", nil)
	}

	resp, err := client.Im.Message.Patch(ctx, larkim.NewPatchMessageReqBuilder().
		MessageId(messageID).
		Body(larkim.NewPatchMessageReqBodyBuilder().
			Content(cardJSON).
			Build()).
		Build())
	if err != nil {
		// 超时/断连时远端可能已经完成更新。调用方会用完全相同的 target 与
		// 冻结内容重试 Patch；资源覆盖语义使这一重试不会新增消息。
		return types.NewAppError(
			types.CodePushFailed, "更新任务"+kind+"回执失败", err,
		)
	}
	if !resp.Success() {
		ae := types.NewAppError(types.CodePushFailed,
			fmt.Sprintf(
				"更新任务%s回执被飞书拒绝（code %d：%s）",
				kind, resp.Code, resp.Msg,
			), nil)
		if creationReceiptPermanentRejection(resp.Code) {
			ae.Retryable = false
		}
		return ae
	}
	// PatchMessage 的成功响应没有 data；code=0 已是完整成功条件。
	return nil
}

// creationReceiptPermanentRejection 判断 PatchMessage 的确定性拒绝。
// 只枚举官方语义明确、原样重试不可能自愈的目标/内容错误；限流、内部错误、
// 未启用机器人能力、缺权限和未知码保持 retryable——能力发布或补权限后，同一
// App 仍应兑现旧 outbox，不能因一次配置态拒绝永久丢回执。
func creationReceiptPermanentRejection(code int) bool {
	switch code {
	case 230001, 230011, 230025, 230028, 230031, 230099:
		return true
	default:
		return false
	}
}
