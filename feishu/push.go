package feishu

import (
	"context"
	"fmt"
	"net/http"

	"github.com/google/uuid"
	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"

	"github.com/YouToco/vane/pusheffect"
	"github.com/YouToco/vane/types"
)

// SendCard 主动发一张交互卡片给指定 open_id（Im.Message.Create，非 reply）。
// M3 推送管道的出口：pusher 通过 FeishuSender 接口调它把卡片推给 owner。
//
// 为什么与 SendTestCard 分开而不复用：SendTestCard 面向"向导自测"语义，
// 内部固定用 BuildReplyCard 造测试文案、收件人固定为缓存 owner、错误话术面向配置用户；
// 本方法是通用的"把已生成好的 card JSON 发给任意 open_id"，收件人与内容都由调用方决定，
// 二者语义不同，硬合并会让任一方多出无关分支。
//
// 兼容入口刻意不生成 UUID：没有调用方持久化的稳定值，现场生成只会制造虚假的
// 幂等保证。需要跨重试去重的调用方应改用 SendCardWithUUID。
func (m *Manager) SendCard(ctx context.Context, openID, cardJSON string) (string, error) {
	result, err := m.sendCard(ctx, openID, cardJSON, "")
	return result.MessageID, err
}

// SendCardWithUUID 使用调用方持久化的 UUID 主动发送交互卡片。
//
// 飞书对相同 UUID 的 Message.Create 请求在一小时内至多成功一次。该保证只有在
// 调用方跨重试复用同一个 UUID 时成立，因此本方法只接受显式、规范、非零 UUID：
// 它不会现场生成 UUID，也不会把非法值降级为无 UUID 发送。调用方仍须持久化远端
// message_id；超过飞书去重窗口后，不能仅凭本方法盲目重发结果未知的请求。
func (m *Manager) SendCardWithUUID(
	ctx context.Context,
	openID string,
	cardJSON string,
	messageUUID string,
) (string, error) {
	result, err := m.SendCardWithUUIDResult(
		ctx,
		m.AppIdentity(),
		openID,
		cardJSON,
		messageUUID,
	)
	return result.MessageID, err
}

// SendCardWithUUIDResult is the durable-effect provider boundary. Unlike the
// compatibility methods it returns whether the request was definitely not
// sent, definitely sent, or crossed the provider boundary with an unknown
// result. Callers must persist that disposition before deciding whether any
// retry is safe.
func (m *Manager) SendCardWithUUIDResult(
	ctx context.Context,
	expectedAppIdentity string,
	openID string,
	cardJSON string,
	messageUUID string,
) (pusheffect.ProviderObservation, error) {
	if !validStableMessageUUID(messageUUID) {
		return pusheffect.ProviderObservation{Disposition: pusheffect.AttemptDefiniteNotSent},
			types.NewAppError(
				types.CodeValidation,
				"主动推送消息 uuid 必须是规范的非零 uuid",
				nil,
			)
	}
	client, actualAppIdentity, ok := m.apiForExpectedApp(expectedAppIdentity)
	if !ok {
		return pusheffect.ProviderObservation{
				Disposition: pusheffect.AttemptDefiniteNotSent,
				AppIdentity: actualAppIdentity,
			},
			types.NewAppError(
				types.CodeConflict,
				"飞书 App 身份与耐久推送快照不一致",
				nil,
			)
	}
	return m.sendCardWithClient(
		ctx,
		client,
		actualAppIdentity,
		openID,
		cardJSON,
		messageUUID,
	)
}

func (m *Manager) sendCard(
	ctx context.Context,
	openID string,
	cardJSON string,
	messageUUID string,
) (pusheffect.ProviderObservation, error) {
	if openID == "" {
		return pusheffect.ProviderObservation{Disposition: pusheffect.AttemptDefiniteNotSent},
			types.NewAppError(types.CodeValidation, "推送目标 open_id 为空", nil)
	}
	// api() 返回的是"当前已连接"的客户端；未连接（未配置/连接失败）时为 nil。
	// 用 CodeConflict 而非 CodePushFailed：这是"通道未就绪"的状态冲突，
	// 调用方（Push activity）据此判断为不可重试的前置条件缺失，而非瞬态发送失败。
	client := m.api()
	if client == nil {
		return pusheffect.ProviderObservation{Disposition: pusheffect.AttemptDefiniteNotSent},
			types.NewAppError(types.CodeConflict, "飞书通道未连接，无法主动推送", nil)
	}
	return m.sendCardWithClient(ctx, client, "", openID, cardJSON, messageUUID)
}

func (m *Manager) sendCardWithClient(
	ctx context.Context,
	client *lark.Client,
	appIdentity string,
	openID string,
	cardJSON string,
	messageUUID string,
) (pusheffect.ProviderObservation, error) {
	body := larkim.NewCreateMessageReqBodyBuilder().
		ReceiveId(openID).
		MsgType(larkim.MsgTypeInteractive).
		Content(cardJSON)
	if messageUUID != "" {
		body.Uuid(messageUUID)
	}

	resp, err := client.Im.Message.Create(ctx, larkim.NewCreateMessageReqBuilder().
		ReceiveIdType(larkim.CreateMessageV1ReceiveIDTypeOpenId).
		Body(body.Build()).
		Build())
	if err != nil {
		// 传输层失败（网络/超时）默认可重试，沿用 CodePushFailed 的默认 Retryable。
		return pusheffect.ProviderObservation{
				Disposition: pusheffect.AttemptAmbiguous,
				AppIdentity: appIdentity,
			},
			types.NewAppError(types.CodePushFailed, "主动推送卡片失败", err)
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
		return pusheffect.ProviderObservation{
			Disposition: providerRejectionDisposition(
				resp.StatusCode,
				resp.Code,
			),
			AppIdentity: appIdentity,
		}, ae
	}
	// message_id 回填 deliveries.feishu_message_id，用于后续追溯/撤回。飞书即使
	// 返回 code=0，也可能因异常响应缺失 data/message_id；此时远端结果未知，
	// 绝不能用 ("", nil) 冒充发送成功。只有显式稳定 UUID 的入口可在飞书
	// 一小时去重窗内安全重试；legacy 入口必须不可重试，让上层将结果收敛为
	// ambiguous，而不是盲发重复卡或落一个无回执的 sent。
	if resp.Data == nil ||
		resp.Data.MessageId == nil ||
		*resp.Data.MessageId == "" {
		ae := types.NewAppError(
			types.CodePushFailed,
			"主动推送成功响应缺少消息 id",
			nil,
		)
		ae.Retryable = messageUUID != ""
		return pusheffect.ProviderObservation{
			Disposition: pusheffect.AttemptAmbiguous,
			AppIdentity: appIdentity,
		}, ae
	}
	result := pusheffect.ProviderObservation{
		Disposition: pusheffect.AttemptSent,
		AppIdentity: appIdentity,
		MessageID:   *resp.Data.MessageId,
	}
	if resp.Data.ChatId != nil {
		result.ChatID = *resp.Data.ChatId
	}
	return result, nil
}

func validStableMessageUUID(messageUUID string) bool {
	parsedUUID, err := uuid.Parse(messageUUID)
	return err == nil &&
		parsedUUID != uuid.Nil &&
		len(messageUUID) == len(uuid.Nil.String()) &&
		parsedUUID.String() == messageUUID
}

// OwnerOpenID 导出 owner 的 open_id：推送管道需要知道"推给谁"。
// 是现有非导出 ownerID() 的导出包装；未捕获 owner 时返回空串，
// 调用方（Push activity / pusher）据空串判定为"尚无收件人"。
func (m *Manager) OwnerOpenID() string {
	return m.ownerID()
}

// OwnerChatID returns the frozen P2P conversation used for positive provider
// reconciliation. Older owner settings may legitimately return empty until a
// new inbound owner message or a historical-message backfill supplies it.
func (m *Manager) OwnerChatID() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ownerAppID == "" || m.ownerAppID != m.apiAppID {
		return ""
	}
	return m.ownerChatID
}

// AppIdentity returns the non-secret identity of the currently installed API
// client. Secret rotation for the same App preserves this value; switching
// Apps changes it.
func (m *Manager) AppIdentity() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.apiAppID
}

// permanentRejection 报告一个飞书拒收 code 是否为确定性失败（重试必然同样失败）。
// 清单只收实锤过语义的码，按需扩充：
//
//	200673 卡片结构非法（2026-07-17 生产实锤：form 缺 name 整卡被拒）；
//	230002 收件人 open_id 非法（收件人错重试不会变对）。
//
// 其余（限流/内部错误/未知码）按可重试处理——瞬态居多，宁可多试。
func permanentRejection(code int) bool {
	switch code {
	case 200673, 230002:
		return true
	}
	return false
}

// providerRejectionDisposition separates an HTTP/provider rejection that
// positively proves no side effect from a response that may have been emitted
// after the provider committed the message. In particular, the SDK can decode
// a JSON 5xx into a normal response object; treating every non-zero provider
// code as definite would then authorize an unsafe duplicate retry.
func providerRejectionDisposition(
	httpStatus int,
	providerCode int,
) pusheffect.AttemptDisposition {
	if httpStatus >= http.StatusInternalServerError {
		return pusheffect.AttemptAmbiguous
	}
	if httpStatus == http.StatusTooManyRequests ||
		(httpStatus >= http.StatusBadRequest &&
			httpStatus < http.StatusInternalServerError) ||
		permanentRejection(providerCode) {
		return pusheffect.AttemptDefiniteNotSent
	}
	return pusheffect.AttemptAmbiguous
}
