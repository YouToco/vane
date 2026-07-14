// Package pusher 是推送管道的最后一步：把 cardgen 生成好的卡片 JSON
// 通过飞书主动推送给 owner。刻意只依赖一个窄接口 FeishuSender，
// 与 feishu 包解耦——pusher 不关心卡片怎么发出去，只关心"发给谁 + 发什么"，
// 便于 Push activity 单测时注入假 sender，也避免 workflow 层直接耦合飞书 SDK。
package pusher

import (
	"context"

	"github.com/YouToco/vane/types"
)

// FeishuSender 抽象"主动发一张卡片"的能力。接口定义在消费方（本包）：
// feishu.Manager 天然实现它（SendCard 签名一致），无需 feishu 包反向依赖 pusher。
type FeishuSender interface {
	SendCard(ctx context.Context, openID, cardJSON string) (feishuMessageID string, err error)
}

// Pusher 持有发送通道。零值不可用，必须经 New 构造。
type Pusher struct {
	fs FeishuSender
}

// New 构造 Pusher。sender 由 cmd/server 注入（生产是 feishu.Manager）。
func New(sender FeishuSender) *Pusher {
	return &Pusher{fs: sender}
}

// Push 把卡片推给指定 open_id，返回飞书消息 ID（回填 deliveries.feishu_message_id）。
// 空 open_id 直接判校验失败（不可重试），避免把空收件人一路带到飞书 API 才报错。
func (p *Pusher) Push(ctx context.Context, ownerOpenID, cardJSON string) (string, error) {
	if ownerOpenID == "" {
		return "", types.NewAppError(types.CodeValidation, "推送目标 open_id 为空，可能尚未捕获 owner", nil)
	}
	// SendCard 已返回统一的 AppError（CodePushFailed/CodeConflict/CodeValidation），
	// 这里直接透传：Push 本身不新增语义，多包一层只会掩盖底层错误码影响重试判定。
	return p.fs.SendCard(ctx, ownerOpenID, cardJSON)
}
