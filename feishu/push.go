package feishu

import (
	"context"
	"fmt"

	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"

	"github.com/YouToco/vane/types"
)

// SendCard 主动发一张交互卡片给指定 open_id（Im.Message.Create，非 reply）。
// M3 推送管道的出口：pusher 通过 FeishuSender 接口调它把卡片推给 owner。
//
// 为什么与 SendTestCard 分开而不复用：SendTestCard 面向"向导自测"语义，
// 内部固定用 BuildReplyCard 造测试文案、收件人固定为缓存 owner、错误话术面向配置用户；
// 本方法是通用的"把已生成好的 card JSON 发给任意 open_id"，收件人与内容都由调用方决定，
// 二者语义不同，硬合并会让任一方多出无关分支。
func (m *Manager) SendCard(ctx context.Context, openID, cardJSON string) (string, error) {
	if openID == "" {
		return "", types.NewAppError(types.CodeValidation, "推送目标 open_id 为空", nil)
	}
	// api() 返回的是"当前已连接"的客户端；未连接（未配置/连接失败）时为 nil。
	// 用 CodeConflict 而非 CodePushFailed：这是"通道未就绪"的状态冲突，
	// 调用方（Push activity）据此判断为不可重试的前置条件缺失，而非瞬态发送失败。
	client := m.api()
	if client == nil {
		return "", types.NewAppError(types.CodeConflict, "飞书通道未连接，无法主动推送", nil)
	}

	resp, err := client.Im.Message.Create(ctx, larkim.NewCreateMessageReqBuilder().
		ReceiveIdType(larkim.CreateMessageV1ReceiveIDTypeOpenId).
		Body(larkim.NewCreateMessageReqBodyBuilder().
			ReceiveId(openID).
			MsgType(larkim.MsgTypeInteractive).
			Content(cardJSON).
			Build()).
		Build())
	if err != nil {
		// 传输层失败（网络/超时）默认可重试，沿用 CodePushFailed 的默认 Retryable。
		return "", types.NewAppError(types.CodePushFailed, "主动推送卡片失败", err)
	}
	if !resp.Success() {
		ae := types.NewAppError(types.CodePushFailed,
			fmt.Sprintf("主动推送卡片被飞书拒绝（code %d：%s）", resp.Code, resp.Msg), nil)
		// 确定性拒收不可重试（bug 狩猎 2026-07-19 MAJOR：此前一律按 CodePushFailed
		// 默认可重试，卡片 JSON 非法这类怎么重试都非法的错误会白烧满 Temporal 重试
		// 预算、把真实原因埋进一串重试噪音里）。清单只收实锤过语义的码，按需扩充：
		//   200673 卡片结构非法（2026-07-17 生产实锤：form 缺 name 整卡被拒）；
		//   230002 收件人 open_id 非法（收件人错重试不会变对）。
		// 其余（限流/内部错误/未知码）保持默认可重试——瞬态居多，宁可多试。
		if permanentRejection(resp.Code) {
			ae.Retryable = false
		}
		return "", ae
	}
	// message_id 回填 deliveries.feishu_message_id，用于后续追溯/撤回。
	// resp.Data 恒非 nil（Success 已判真），但 MessageId 是 *string，仍做空指针防护。
	if resp.Data != nil && resp.Data.MessageId != nil {
		return *resp.Data.MessageId, nil
	}
	return "", nil
}

// OwnerOpenID 导出 owner 的 open_id：推送管道需要知道"推给谁"。
// 是现有非导出 ownerID() 的导出包装；未捕获 owner 时返回空串，
// 调用方（Push activity / pusher）据空串判定为"尚无收件人"。
func (m *Manager) OwnerOpenID() string {
	return m.ownerID()
}

// permanentRejection 报告一个飞书拒收 code 是否为确定性失败（重试必然同样失败）。
// 清单只收实锤过语义的码，按需扩充：
//   200673 卡片结构非法（2026-07-17 生产实锤：form 缺 name 整卡被拒）；
//   230002 收件人 open_id 非法（收件人错重试不会变对）。
// 其余（限流/内部错误/未知码）按可重试处理——瞬态居多，宁可多试。
func permanentRejection(code int) bool {
	switch code {
	case 200673, 230002:
		return true
	}
	return false
}
