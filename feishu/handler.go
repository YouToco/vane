package feishu

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/larksuite/oapi-sdk-go/v3/event/dispatcher"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"

	"github.com/YouToco/vane/llm"
	"github.com/YouToco/vane/types"
)

// chatSystemPrompt 是 chat_reply 环节的 system prompt（契约 §4 原文）。
const chatSystemPrompt = `你是"见微 Vane"的 AI 助手。简洁、直接地回答用户问题；中文回复。`

// dedupTTL 是 message_id 去重窗口。飞书对 3 秒内未确认的事件会重推，
// 重推间隔远小于 10 分钟，这个窗口足以吸收所有重推。
const dedupTTL = 10 * time.Minute

// mentionRe 匹配群 @ 消息正文里的占位符（如 "@_user_1"）。
// 群里 @ 机器人时正文以占位符开头，去掉后再交给 LLM，避免模型困惑。
var mentionRe = regexp.MustCompile(`@_user_\d+\s*`)

// handler 是单条 WS 连接的消息处理链。每次 Reconfigure 都会新建一个
// handler（连同新的 wsCtx），去重表随连接一起换代——重连极低频，
// 丢掉旧去重表最多造成一次重复回复，可接受。
type handler struct {
	m *Manager
	// ctx 是本连接的生命周期 ctx：异步处理挂在它上面，
	// Reconfigure 断开连接时未完成的处理随之取消。
	ctx context.Context

	dedupMu sync.Mutex
	seen    map[string]time.Time // message_id → 首次见到的时间
}

func newHandler(m *Manager, ctx context.Context) *handler {
	return &handler{m: m, ctx: ctx, seen: make(map[string]time.Time)}
}

// eventDispatcher 构造 WS 客户端所需的事件分发器。
// verificationToken/eventEncryptKey 传空串：长连接模式下 SDK 不校验这两项。
func (h *handler) eventDispatcher() *dispatcher.EventDispatcher {
	return dispatcher.NewEventDispatcher("", "").
		OnP2MessageReceiveV1(func(_ context.Context, event *larkim.P2MessageReceiveV1) error {
			// 飞书要求 3 秒内返回 nil 确认，否则重推：这里只做判空 + 去重，
			// 真正的处理（查库 + LLM，秒级耗时）丢进 goroutine 立即返回。
			if event == nil || event.Event == nil || event.Event.Message == nil {
				return nil
			}
			msgID := strVal(event.Event.Message.MessageId)
			if msgID == "" || h.isDuplicate(msgID) {
				return nil
			}
			// 刻意不用回调入参 ctx（回调返回后可能失效），改用连接级 ctx。
			go h.handle(h.ctx, event)
			return nil
		})
}

// isDuplicate 报告 message_id 是否在去重窗口内出现过，顺带清理过期条目。
// 单用户 MVP 消息量极小，全表线性清理的开销可忽略。
func (h *handler) isDuplicate(msgID string) bool {
	h.dedupMu.Lock()
	defer h.dedupMu.Unlock()
	now := time.Now()
	for id, seenAt := range h.seen {
		if now.Sub(seenAt) > dedupTTL {
			delete(h.seen, id)
		}
	}
	if _, ok := h.seen[msgID]; ok {
		return true
	}
	h.seen[msgID] = now
	return false
}

// handle 执行完整消息链：过滤非文本 → upsert 用户 → 捕获 owner →
// LLM 生成回复 → 卡片回复。任何一步失败都尽量给用户一个人话兜底，
// 不让消息石沉大海。
func (h *handler) handle(ctx context.Context, event *larkim.P2MessageReceiveV1) {
	// WS 回调链上的 panic 会带崩整个进程，这里兜住只丢单条消息。
	defer func() {
		if r := recover(); r != nil {
			slog.Error("feishu: 消息处理 panic", "recover", r)
		}
	}()

	msg := event.Event.Message
	msgID := strVal(msg.MessageId)

	// 图片 / 表情包 / 富文本等暂不支持：礼貌回复而非沉默，
	// 避免用户以为机器人失灵。
	if strVal(msg.MessageType) != "text" {
		h.reply(ctx, msgID, BuildReplyCard("暂只支持文本消息，请直接输入文字与我对话。"))
		return
	}

	text := parseTextContent(strVal(msg.Content))
	if text == "" {
		h.reply(ctx, msgID, BuildReplyCard("没有读到文字内容，请重新输入。"))
		return
	}

	openID, name := senderIdentity(event)
	if openID == "" {
		// 拿不到 open_id 无法归属用户，只能记日志放弃。
		slog.Error("feishu: 事件缺少 sender open_id", "message_id", msgID)
		return
	}

	user, err := h.m.st.UpsertUserByOpenID(ctx, openID, name)
	if err != nil {
		slog.Error("feishu: upsert 用户失败", "err", err, "open_id", openID)
		h.reply(ctx, msgID, BuildReplyCard("这条消息我处理失败了：内部数据错误，请稍后重试。"))
		return
	}

	h.captureOwnerIfFirst(ctx, openID, name)

	// 授权白名单：只为 owner 服务。owner = 第一个发消息的人（上一步刚确定），
	// 其余租户成员即便发现了机器人也不能驱动付费 LLM 调用——防止 DeepSeek
	// 额度被他人烧掉（自用 MVP 的成本红线）。
	if owner := h.m.ownerID(); owner != "" && openID != owner {
		h.reply(ctx, msgID, BuildReplyCard("抱歉，我目前只为我的主人服务。"))
		return
	}

	resp, err := llm.Do(ctx, h.m.cli, h.m.rec, llm.CallMeta{
		TraceID:  uuid.NewString(),
		SpanName: "chat_reply",
		UserID:   &user.ID,
	}, llm.Request{
		System: chatSystemPrompt,
		User:   text,
	})
	if err != nil {
		slog.Error("feishu: chat_reply LLM 调用失败", "err", err, "user_id", user.ID)
		h.reply(ctx, msgID, BuildReplyCard("这条消息我处理失败了："+humanizeLLMError(err)))
		return
	}

	h.reply(ctx, msgID, BuildReplyCard(resp.Content))
}

// captureOwnerIfFirst 把第一个发消息的用户记为 owner（feishu_owner setting）。
// owner 一经写入不再更换：它是测试卡片与后续推送的收件人。
func (h *handler) captureOwnerIfFirst(ctx context.Context, openID, name string) {
	// captureMu 串行化整段 check-then-act：并发的首批消息可能都看到
	// ownerCaptured()==false 而各写各的 owner（last-write-wins）。持锁保证
	// owner 真正 write-once。owner 捕获是一次性低频操作，串行无性能顾虑。
	h.m.captureMu.Lock()
	defer h.m.captureMu.Unlock()

	if h.m.ownerCaptured() {
		return
	}
	// 缓存为空不代表库里没有（比如进程刚重启还没 reload owner），
	// 写之前先查一次库，库里已有就只刷新缓存。
	raw, err := h.m.st.GetSetting(ctx, settingKeyOwner)
	if err == nil {
		var own ownerSetting
		if json.Unmarshal(raw, &own) == nil && own.OpenID != "" {
			h.m.setOwner(own.OpenID, own.Name)
			return
		}
	} else if !errors.Is(err, types.ErrNotFound) {
		slog.Error("feishu: 查询 owner 设置失败", "err", err)
		return
	}

	own := ownerSetting{
		OpenID:     openID,
		Name:       name,
		CapturedAt: time.Now().Format(time.RFC3339),
	}
	value, _ := json.Marshal(own)
	if err := h.m.st.PutSetting(ctx, settingKeyOwner, json.RawMessage(value)); err != nil {
		slog.Error("feishu: 写入 owner 设置失败", "err", err)
		return
	}
	h.m.setOwner(openID, name)
	slog.Info("feishu: 已捕获 owner", "open_id", openID)
}

// reply 用交互卡片回复指定消息。回复失败只记日志：
// 消息链里没有比"回复"更下游的兜底手段了。
func (h *handler) reply(ctx context.Context, messageID, cardJSON string) {
	if messageID == "" {
		return
	}
	client := h.m.api()
	if client == nil {
		slog.Error("feishu: 回复时 API 客户端不可用", "message_id", messageID)
		return
	}
	resp, err := client.Im.Message.Reply(ctx, larkim.NewReplyMessageReqBuilder().
		MessageId(messageID).
		Body(larkim.NewReplyMessageReqBodyBuilder().
			MsgType(larkim.MsgTypeInteractive).
			Content(cardJSON).
			Build()).
		Build())
	if err != nil {
		slog.Error("feishu: 回复消息失败", "err", err, "message_id", messageID)
		return
	}
	if !resp.Success() {
		slog.Error("feishu: 回复消息被飞书拒绝",
			"code", resp.Code, "msg", resp.Msg, "message_id", messageID)
	}
}

// senderIdentity 从事件中提取发送者 open_id 与名字。
// SDK 的 EventSender 只有 SenderId/SenderType/TenantKey，没有昵称字段，
// 名字取不到，按约定返回空串（后续可通过通讯录 API 补全）。
func senderIdentity(event *larkim.P2MessageReceiveV1) (openID, name string) {
	sender := event.Event.Sender
	if sender == nil || sender.SenderId == nil {
		return "", ""
	}
	return strVal(sender.SenderId.OpenId), ""
}

// parseTextContent 解析 text 消息的 Content JSON（{"text":"..."}），
// 并剥离群 @ 机器人时的占位符。
func parseTextContent(raw string) string {
	var body struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal([]byte(raw), &body); err != nil {
		return ""
	}
	return strings.TrimSpace(mentionRe.ReplaceAllString(body.Text, ""))
}

// humanizeLLMError 把 LLM 错误翻成给飞书用户看的人话。
func humanizeLLMError(err error) string {
	switch types.CodeOf(err) {
	case types.CodeLLMRateLimit:
		return "模型被限流了，请稍后重试。"
	case types.CodeLLMBadRequest:
		return "请求被模型拒绝（内容或参数问题），换个说法试试。"
	case types.CodeLLMUnavailable:
		return "模型服务暂时不可用（可能是超时或服务端故障），请稍后重试。"
	default:
		var ae *types.AppError
		if errors.As(err, &ae) {
			return ae.Message
		}
		return "内部错误，请稍后重试。"
	}
}

// strVal 解引用 SDK 的 *string 字段，nil 时返回空串。
func strVal(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}
