// 本文件同时出现 owner 设置键与 UpsertUserByOpenID 调用，但**不是** principal
// 解析链——故对不变量 I-A1 的守卫（auth/invariant_test.go）显式豁免：
//   - captureOwnerIfFirst 是 owner 的**首次捕获**（写入），不是读取当前 principal；
//   - 多处 UpsertUserByOpenID 用的是**消息发送者的** open_id（为入站消息建 user 行），
//     而非 owner 记录里的 open_id。
//
// 要读「当前请求以谁的身份执行」，一律走 auth.PrincipalResolver，不要在此另起一份。
//
//go:principal-exempt
package feishu

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/larksuite/oapi-sdk-go/v3/event/dispatcher"
	"github.com/larksuite/oapi-sdk-go/v3/event/dispatcher/callback"
	larkapplication "github.com/larksuite/oapi-sdk-go/v3/service/application/v6"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"

	"github.com/YouToco/vane/agent"
	"github.com/YouToco/vane/feedback"
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
			// 由 Manager 准入并跟踪，停机时先拒绝新消息，再等待/取消在途处理。
			if !h.m.startAsync(h.ctx, agentMessageBudget, false, "message", func(ctx context.Context) {
				h.handle(ctx, event)
			}) {
				slog.Info("feishu: Manager 正在关闭，忽略新消息", "message_id", msgID)
			}
			return nil
		}).
		OnP2MessageReadV1(func(_ context.Context, event *larkim.P2MessageReadV1) error {
			if event == nil || event.Event == nil {
				return nil
			}
			d := event.Event
			var readerID, readTime string
			if d.Reader != nil {
				if d.Reader.ReaderId != nil {
					readerID = strVal(d.Reader.ReaderId.OpenId)
				}
				readTime = strVal(d.Reader.ReadTime)
			}
			slog.Info("feishu: 消息已读",
				"reader_open_id", readerID,
				"read_time", readTime,
				"message_ids", d.MessageIdList,
			)
			return nil
		}).
		OnP2MessageReactionCreatedV1(func(_ context.Context, event *larkim.P2MessageReactionCreatedV1) error {
			if event == nil || event.Event == nil {
				return nil
			}
			d := event.Event
			var emoji string
			if d.ReactionType != nil {
				emoji = strVal(d.ReactionType.EmojiType)
			}
			slog.Info("feishu: 消息被 Reaction",
				"message_id", strVal(d.MessageId),
				"emoji", emoji,
				"operator_type", strVal(d.OperatorType),
				"action_time", strVal(d.ActionTime),
			)
			return nil
		}).
		OnP2MessageReactionDeletedV1(func(_ context.Context, event *larkim.P2MessageReactionDeletedV1) error {
			if event == nil || event.Event == nil {
				return nil
			}
			d := event.Event
			var emoji string
			if d.ReactionType != nil {
				emoji = strVal(d.ReactionType.EmojiType)
			}
			slog.Info("feishu: 消息取消 Reaction",
				"message_id", strVal(d.MessageId),
				"emoji", emoji,
				"operator_type", strVal(d.OperatorType),
				"action_time", strVal(d.ActionTime),
			)
			return nil
		}).
		OnP2MessageRecalledV1(func(_ context.Context, event *larkim.P2MessageRecalledV1) error {
			if event == nil || event.Event == nil {
				return nil
			}
			d := event.Event
			slog.Info("feishu: 消息撤回",
				"message_id", strVal(d.MessageId),
				"chat_id", strVal(d.ChatId),
				"recall_time", strVal(d.RecallTime),
				"recall_type", strVal(d.RecallType),
			)
			return nil
		}).
		OnP2ChatAccessEventBotP2pChatEnteredV1(func(_ context.Context, event *larkim.P2ChatAccessEventBotP2pChatEnteredV1) error {
			if event == nil || event.Event == nil {
				return nil
			}
			d := event.Event
			var operatorID string
			if d.OperatorId != nil {
				operatorID = strVal(d.OperatorId.OpenId)
			}
			slog.Info("feishu: 用户进入机器人会话",
				"chat_id", strVal(d.ChatId),
				"operator_open_id", operatorID,
				"last_message_id", strVal(d.LastMessageId),
			)
			return nil
		}).
		OnP2BotMenuV6(func(_ context.Context, event *larkapplication.P2BotMenuV6) error {
			if event == nil || event.Event == nil {
				return nil
			}
			if !h.m.startAsync(h.ctx, agentMessageBudget, false, "bot_menu", func(ctx context.Context) {
				h.handleBotMenu(ctx, event)
			}) {
				slog.Info("feishu: Manager 正在关闭，忽略菜单事件")
			}
			return nil
		}).
		OnP2CardActionTrigger(h.onCardAction)
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

	// text 与 post 都能提取文字；图片 / 表情包等其余类型礼貌拒绝而非沉默，
	// 避免用户以为机器人失灵。post 必须支持：往输入框粘贴文字时飞书会自动
	// 把消息升格成富文本，一律拒绝会把粘贴这个高频操作直接顶回去。
	msgType := strVal(msg.MessageType)
	var text string
	switch msgType {
	case "text":
		text = parseTextContent(strVal(msg.Content))
	case "post":
		text = parsePostContent(strVal(msg.Content))
	default:
		h.reply(ctx, msgID, BuildReplyCard("暂只支持文本消息，请直接输入文字与我对话。"))
		return
	}
	if text == "" {
		// post 提取为空说明富文本里没有一个文字节点（纯图片等），按"不支持"
		// 拒绝；text 解析为空才是"没读到字"。
		if msgType == "post" {
			h.reply(ctx, msgID, BuildReplyCard("暂只支持文本消息，请直接输入文字与我对话。"))
		} else {
			h.reply(ctx, msgID, BuildReplyCard("没有读到文字内容，请重新输入。"))
		}
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

	reactionID := h.addTypingIndicator(ctx, msgID)
	defer h.removeTypingIndicator(ctx, msgID, reactionID)

	// agent 已注入时消息交给 agent loop（M4）；未注入（如装配阶段配置不全）
	// 回退下方 chat_reply 直连——保证任何装配形态下消息都有回应而非崩/沉默。
	if runner := h.m.agentRunner(); runner != nil {
		// 追问识别（M5 契约 §11）：用户回复某张推送卡时，把原文与解读摘要作为
		// 定界上下文并入本条消息。只在 agent 链路做——回退路径无会话语义，
		// 包装无意义。未命中（回复的是普通消息/聊天卡）原样按普通消息处理。
		externalContext := false
		if fb := h.m.feedbackRunner(); fb != nil {
			if w, ok := fb.WrapQuestion(ctx, user.ID, strVal(msg.ParentId), strVal(msg.RootId), text); ok {
				text = w
				externalContext = true
			}
		}
		if !externalContext {
			if quoted, ok := h.prependQuotedMessage(ctx, strVal(msg.ParentId), text); ok {
				text = quoted
				externalContext = true
			}
		}
		h.handleWithAgent(ctx, runner, msgID, openID, user.ID, text, externalContext)
		return
	}

	// 回退路径同样需要调用超时（连接级 ctx 无 deadline，llm.Client 由调用方控超时）。
	llmCtx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	resp, err := llm.Do(llmCtx, h.m.cli, h.m.rec, llm.CallMeta{
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

// handleWithAgent 把消息交给 agent loop 并回复 Outcome.Reply；Confirm 非 nil
// 时追加发一张确认卡。确认卡走 SendCard 新消息而非 reply：它有独立生命周期
// （按钮回调后原地更新为结果），挂在原消息的回复串里会把两种语义搅在一起。
func (h *handler) handleWithAgent(ctx context.Context, runner AgentRunner, msgID, openID string, userID int64, text string, externalContext bool) {
	// 整条消息的总预算（审查 #信号量瘫痪的纵深防御）：agent 单次模型调用已有
	// 120s 超时，这里再兜住工具执行/DB 调用同类挂死——连接级 ctx 无 deadline，
	// 没有这层预算时任何一环黑洞都会让 goroutine 永久滞留。
	ctx, cancel := context.WithTimeout(ctx, agentMessageBudget)
	defer cancel()

	var out agent.Outcome
	var err error
	if externalContext {
		out, err = runner.HandleExternalContextMessage(ctx, userID, text)
	} else {
		out, err = runner.HandleMessage(ctx, userID, text)
	}
	if err != nil {
		slog.Error("feishu: agent 处理消息失败", "err", err, "user_id", userID)
		h.reply(ctx, msgID, BuildReplyCard("这条消息我处理失败了："+humanizeLLMError(err)))
		return
	}
	h.reply(ctx, msgID, BuildReplyCard(out.Reply))
	if out.Confirm == nil {
		return
	}
	card := BuildConfirmCard(out.Confirm.Summary, out.Confirm.ActionID)
	if _, err := h.m.SendCard(ctx, openID, card); err != nil {
		// 确认卡丢失意味着动作永远无法被确认（24h 后过期），必须明确告知用户。
		slog.Error("feishu: 发送确认卡失败", "err", err, "action_id", out.Confirm.ActionID)
		h.reply(ctx, msgID, BuildReplyCard("确认卡发送失败，本次操作未生效，请稍后重试。"))
	}
}

// typingEmoji 是打字指示器使用的表情类型。处理消息期间在用户消息上展示，
// 回复完成后移除——让用户知道机器人正在工作而非失灵。
const typingEmoji = "OneSecond"

// addTypingIndicator 在消息上添加"正在输入"表情。返回 reactionID 供移除用；
// 失败静默降级（打字指示器是锦上添花，不能阻断消息处理）。
func (h *handler) addTypingIndicator(ctx context.Context, msgID string) string {
	client := h.m.api()
	if client == nil {
		return ""
	}
	resp, err := client.Im.MessageReaction.Create(ctx, larkim.NewCreateMessageReactionReqBuilder().
		MessageId(msgID).
		Body(larkim.NewCreateMessageReactionReqBodyBuilder().
			ReactionType(larkim.NewEmojiBuilder().EmojiType(typingEmoji).Build()).
			Build()).
		Build())
	if err != nil {
		return ""
	}
	if !resp.Success() || resp.Data == nil {
		return ""
	}
	return strVal(resp.Data.ReactionId)
}

// removeTypingIndicator 移除之前添加的打字指示器表情。
func (h *handler) removeTypingIndicator(ctx context.Context, msgID, reactionID string) {
	if reactionID == "" {
		return
	}
	client := h.m.api()
	if client == nil {
		return
	}
	_, _ = client.Im.MessageReaction.Delete(ctx, larkim.NewDeleteMessageReactionReqBuilder().
		MessageId(msgID).
		ReactionId(reactionID).
		Build())
}

// botMenuCommands 将菜单 event_key 映射到交给 agent 的合成指令。
// 菜单项在飞书开发者后台「机器人 → 机器人菜单」配置，event_key 需一致。
var botMenuCommands = map[string]string{
	"push_now":     "请立即执行一次推送",
	"list_sources": "请列出当前所有订阅源的状态",
}

// handleBotMenu 处理机器人菜单点击事件：将菜单 event_key 转换为合成消息，
// 交给 agent loop 统一处理，结果以新消息送达。
func (h *handler) handleBotMenu(ctx context.Context, event *larkapplication.P2BotMenuV6) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("feishu: 菜单处理 panic", "recover", r)
		}
	}()

	d := event.Event
	eventKey := strVal(d.EventKey)
	var openID string
	if d.Operator != nil && d.Operator.OperatorId != nil {
		openID = strVal(d.Operator.OperatorId.OpenId)
	}
	if openID == "" || eventKey == "" {
		return
	}

	if owner := h.m.ownerID(); owner == "" || openID != owner {
		h.m.SendCard(ctx, openID, BuildReplyCard("抱歉，我目前只为我的主人服务。"))
		return
	}

	text, known := botMenuCommands[eventKey]
	if !known {
		slog.Info("feishu: 未识别的菜单事件", "event_key", eventKey)
		return
	}

	runner := h.m.agentRunner()
	if runner == nil {
		h.m.SendCard(ctx, openID, BuildReplyCard("助手尚未就绪，请稍后再试。"))
		return
	}

	user, err := h.m.st.UpsertUserByOpenID(ctx, openID, "")
	if err != nil {
		slog.Error("feishu: 菜单处理 upsert 用户失败", "err", err, "open_id", openID)
		return
	}

	ctx, cancel := context.WithTimeout(ctx, agentMessageBudget)
	defer cancel()

	out, err := runner.HandleMessage(ctx, user.ID, text)
	if err != nil {
		slog.Error("feishu: 菜单 agent 处理失败", "err", err, "event_key", eventKey)
		h.m.SendCard(ctx, openID, BuildReplyCard("处理失败："+humanizeLLMError(err)))
		return
	}
	h.m.SendCard(ctx, openID, BuildReplyCard(out.Reply))
	if out.Confirm != nil {
		card := BuildConfirmCard(out.Confirm.Summary, out.Confirm.ActionID)
		if _, err := h.m.SendCard(ctx, openID, card); err != nil {
			slog.Error("feishu: 菜单确认卡发送失败", "err", err, "action_id", out.Confirm.ActionID)
		}
	}
}

// cardActionSyncBudget 是卡片回调的同步等待预算。飞书要求回调 3 秒内响应，
// 否则重推事件；预留网络与前置查库的余量后取 2.5s，超时转异步补发结果消息
// （契约 §9 降级路径：SDK 的 Token 延迟更新卡片要另走 cardkit API，MVP 不引入）。
const cardActionSyncBudget = 2500 * time.Millisecond

// agentMessageBudget 是一条 agent 消息端到端的硬预算（多轮模型调用 + 工具执行）。
// 5 分钟容得下 maxTurns 内的正常循环，同时保证任何一环挂死都不会永久滞留。
const agentMessageBudget = 5 * time.Minute

// cardActionExecBudget 是确认动作执行（Claim 之后）的硬预算：脱离连接级 ctx 后
// 必须有自己的上限，防工具内 DB/Temporal 调用无限阻塞。
const cardActionExecBudget = 30 * time.Second

// cardActionLifecycleBudget 覆盖动作执行及同步预算超时后的结果补发。两段各自
// 仍有独立 deadline；多留 1 秒只用于 goroutine 调度与阶段切换，不扩大外部调用预算。
const cardActionLifecycleBudget = cardActionExecBudget + 15*time.Second + time.Second

// cardActionResult 是确认/取消动作的执行结果：text 恒为可直接展示的人话
// （含失败话术），ok 仅用于区分 toast 的成功/失败样式。
type cardActionResult struct {
	text string
	ok   bool
}

// onCardAction 处理确认卡按钮回调（契约 §9）：owner 校验 → confirm/cancel
// 分发 → 预算内完成则原地把卡片更新为结果文本，超时先回「执行中」toast、
// 完成后补发结果消息。恒返回 nil error：返回 error 只会让飞书反复重推回调，
// 对用户没有任何额外价值。
func (h *handler) onCardAction(_ context.Context, event *callback.CardActionTriggerEvent) (resp *callback.CardActionTriggerResponse, _ error) {
	finish, accepted := h.m.beginCallback()
	if !accepted {
		return toastResponse("error", "服务正在重启，请稍后重试"), nil
	}
	defer finish()

	// 与 handle 相同的 panic 兜底：WS 回调链上的 panic 会带崩整个进程。
	// 命名返回值让 recover 后仍能给用户一个失败 toast 而非静默。
	defer func() {
		if r := recover(); r != nil {
			slog.Error("feishu: 卡片回调处理 panic", "recover", r)
			resp = toastResponse("error", "内部错误，请稍后重试")
		}
	}()

	if event == nil || event.Event == nil || event.Event.Action == nil {
		return toastResponse("error", "回调数据缺失"), nil
	}
	verb, actionID := parseCardActionValue(event.Event.Action.Value)
	fbAction, deliveryID, isFeedback := parseFeedbackValue(event.Event.Action.Value)
	reasonDeliveryID, isReasonSubmit := parseFeedbackReasonValue(event.Event.Action.Value)
	// 四类 value 之外（含 value 结构不识别）静默忽略，不弹错误打扰。
	if !isFeedback && !isReasonSubmit && (actionID == "" || (verb != cardActionConfirm && verb != cardActionCancel)) {
		return &callback.CardActionTriggerResponse{}, nil
	}

	var operatorID string
	if event.Event.Operator != nil {
		operatorID = event.Event.Operator.OpenID
	}
	// owner 为空（进程重启后缓存未预热等）时同样拒绝：宁可让主人重点一次，
	// 也不能让白名单出现空窗（契约 §10：写动作只有 owner 能确认）。
	// 反馈按钮共用这道校验：反馈会驱动画像演化与深度解读（付费调用）。
	if owner := h.m.ownerID(); operatorID == "" || owner == "" || operatorID != owner {
		return toastResponse("error", "仅主人可操作"), nil
	}

	// 推送卡反馈（M5）在此与确认卡（M4）分流：分流点放在 agent 就绪检查之前，
	// 且自带一次 UpsertUserByOpenID——反馈不依赖 agent loop（Notifier 缺席只是
	// 少一条会话通告），下面 M4 路径的每一行则保持原样不动。
	if isFeedback {
		user, err := h.m.st.UpsertUserByOpenID(h.ctx, operatorID, "")
		if err != nil {
			slog.Error("feishu: 反馈回调 upsert 用户失败", "err", err, "open_id", operatorID)
			return toastResponse("error", "内部数据错误，请稍后重试"), nil
		}
		return h.onFeedbackAction(user.ID, fbAction, deliveryID), nil
	}
	if isReasonSubmit {
		user, err := h.m.st.UpsertUserByOpenID(h.ctx, operatorID, "")
		if err != nil {
			slog.Error("feishu: 反馈原因回调 upsert 用户失败", "err", err, "open_id", operatorID)
			return toastResponse("error", "内部数据错误，请稍后重试"), nil
		}
		reason, rerr := extractReasonFromForm(event.Event.Action.Name, event.Event.Action.FormValue, reasonDeliveryID)
		if rerr != nil {
			// 三重对齐失败 = 提交按钮与 value 指向的条目对不上。绝不猜、绝不静默落库
			// ——错误归属的 misjudged 不可撤销且会毒化画像演化（附录 A.4 红线）。
			slog.Error("feishu: 反馈原因表单对齐校验失败，拒绝落库",
				"action_name", event.Event.Action.Name, "delivery_id", reasonDeliveryID, "err", rerr)
			return toastResponse("error", "表单数据异常，请重新点击 👎 后再试"), nil
		}
		return h.onFeedbackReasonSubmit(user.ID, reasonDeliveryID, reason), nil
	}

	runner := h.m.agentRunner()
	if runner == nil {
		return toastResponse("error", "助手尚未就绪，请稍后重试"), nil
	}

	// 拿内部 user.ID：ExecuteAction/CancelAction 还会用它比对
	// pending_action.user_id（服务端二次校验，契约 §10）。
	user, err := h.m.st.UpsertUserByOpenID(h.ctx, operatorID, "")
	if err != nil {
		slog.Error("feishu: 卡片回调 upsert 用户失败", "err", err, "open_id", operatorID)
		return toastResponse("error", "内部数据错误，请稍后重试"), nil
	}

	// 动作放受 Manager 跟踪的 goroutine 执行：既是 2.5s 预算的实现载体，
	// 也让优雅停机先等待已接纳动作，不在 DB/Temporal 仍被使用时关闭资源。
	// ctx 与连接生命周期解耦（审查 #Reconfigure 丢执行）：Claim 一旦成功动作即被
	// 置为 executed 且不可再领取，此后的执行不能随 WS 换代（Dashboard 保存配置触发
	// Reconfigure → h.ctx 取消）被中断，否则动作永久标记已执行但实际没执行、
	// 结果也无处送达。startAsync 的 detachParent 摆脱连接取消，但动作与补发各自
	// 仍有 deadline，并在 Manager 停机宽限耗尽时收到取消。
	done := make(chan cardActionResult, 1)
	followUp := make(chan bool, 1)
	if !h.m.startAsync(h.ctx, cardActionLifecycleBudget, true, "card_action", func(workCtx context.Context) {
		res := func() (res cardActionResult) {
			execCtx, cancel := context.WithTimeout(workCtx, cardActionExecBudget)
			defer cancel()
			defer func() {
				if recovered := recover(); recovered != nil {
					slog.Error("feishu: 卡片动作执行 panic", "recover", recovered, "action_id", actionID)
					res = cardActionResult{text: "内部错误，请稍后重试。"}
				}
			}()
			var text string
			var actionErr error
			if verb == cardActionConfirm {
				text, actionErr = runner.ExecuteAction(execCtx, user.ID, actionID)
			} else {
				text, actionErr = runner.CancelAction(execCtx, user.ID, actionID)
			}
			if actionErr != nil {
				slog.Error("feishu: 卡片动作执行失败", "err", actionErr, "action_id", actionID, "vane_action", verb)
				return cardActionResult{text: "执行失败：" + humanizeLLMError(actionErr)}
			}
			return cardActionResult{text: text, ok: true}
		}()

		done <- res
		select {
		case shouldSend := <-followUp:
			if !shouldSend {
				return
			}
		case <-workCtx.Done():
			return
		}

		// 补发仍属于同一项已接纳工作，避免动作 worker 完成后另起一个无法等待的
		// goroutine。A6 才会提供进程崩溃后的耐久回执；这里不作该承诺。
		sendCtx, cancel := context.WithTimeout(workCtx, 15*time.Second)
		defer cancel()
		if _, sendErr := h.m.SendCard(sendCtx, operatorID, BuildReplyCard(res.text)); sendErr != nil {
			slog.Error("feishu: 补发卡片动作结果失败", "err", sendErr, "action_id", actionID)
		}
	}) {
		return toastResponse("error", "服务正在重启，请稍后重试"), nil
	}

	timer := time.NewTimer(cardActionSyncBudget)
	defer timer.Stop()
	select {
	case res := <-done:
		followUp <- false
		toastType, toastText := "success", "已处理"
		if !res.ok {
			toastType, toastText = "error", "执行失败"
		}
		// 原地把卡片更新为结果文本，正文样式沿用 BuildReplyCard；
		// type=raw 表示 data 是完整卡片 JSON（SDK callback.Card 约定）。
		return &callback.CardActionTriggerResponse{
			Toast: &callback.Toast{Type: toastType, Content: toastText},
			Card: &callback.Card{
				Type: "raw",
				Data: json.RawMessage(BuildReplyCard(res.text)),
			},
		}, nil
	case <-timer.C:
		followUp <- true
		// Toast + 撤下按钮（审查 #二次点击误导）：只回 toast 时按钮仍在，用户再点会
		// 命中 Claim 幂等拒绝、卡片被替换成"已处理过"的三义文案，随后真结果又以新消息
		// 到达——顺序错乱像失败。原地把卡片换成"执行中"说明，消除整个二次点击窗口。
		return &callback.CardActionTriggerResponse{
			Toast: &callback.Toast{Type: "info", Content: "执行中，稍后看结果消息"},
			Card: &callback.Card{
				Type: "raw",
				Data: json.RawMessage(BuildReplyCard("执行中，结果稍后以新消息送达。")),
			},
		}, nil
	}
}

// parseCardActionValue 从按钮 value（SDK 定型 map[string]interface{}）提取
// vane_action 与 action_id；非字符串值按缺失处理（value 可被客户端伪造，
// 类型断言失败不能 panic）。
func parseCardActionValue(value map[string]interface{}) (verb, actionID string) {
	verb, _ = value["vane_action"].(string)
	actionID, _ = value["action_id"].(string)
	return verb, actionID
}

// feedbackButtonActions 是按钮 value 里 fb 字段的白名单（M5 契约 §10.1）：
// question 不出现在按钮上（它由回复消息产生）。
var feedbackButtonActions = map[string]types.FeedbackAction{
	string(types.FeedbackActionInterested):    types.FeedbackActionInterested,
	string(types.FeedbackActionNotInterested): types.FeedbackActionNotInterested,
	string(types.FeedbackActionMisjudged):     types.FeedbackActionMisjudged,
	string(types.FeedbackActionDeepDive):      types.FeedbackActionDeepDive,
}

// parseFeedbackValue 解析推送卡反馈按钮的 value（M5 契约 §10.1）。
// 任何字段缺失/类型不符/不在白名单/id 非法 → ok=false（调用方静默忽略）：
// value 完全由客户端提供，解析层只做形状校验，语义与归属由服务端库内裁决。
func parseFeedbackValue(value map[string]interface{}) (action types.FeedbackAction, deliveryID int64, ok bool) {
	if verb, _ := value["vane_action"].(string); verb != cardActionFeedback {
		return "", 0, false
	}
	raw, _ := value["fb"].(string)
	action, known := feedbackButtonActions[raw]
	if !known {
		return "", 0, false
	}
	// delivery_id 恒为字符串（构卡侧 FormatInt）：JSON number 经 SDK 会变 float64，
	// 大 id 有精度隐患。
	idStr, _ := value["delivery_id"].(string)
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil || id <= 0 {
		return "", 0, false
	}
	return action, id, true
}

// parseFeedbackReasonValue 解析 form 提交的"fbr" value：只需 delivery_id，
// 原因文本在 FormValue 里（由调用方提取）。
func parseFeedbackReasonValue(value map[string]interface{}) (deliveryID int64, ok bool) {
	if verb, _ := value["vane_action"].(string); verb != cardActionFeedbackReason {
		return 0, false
	}
	idStr, _ := value["delivery_id"].(string)
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil || id <= 0 {
		return 0, false
	}
	return id, true
}

// onFeedbackReasonSubmit 处理 👎 后 form 提交的反馈原因。
func (h *handler) onFeedbackReasonSubmit(userID, deliveryID int64, reason string) *callback.CardActionTriggerResponse {
	fb := h.m.feedbackRunner()
	if fb == nil {
		return toastResponse("error", "反馈功能尚未就绪，请稍后重试")
	}

	done := make(chan feedback.ClickResult, 1)
	if !h.m.startAsync(h.ctx, cardActionExecBudget, true, "feedback_reason", func(execCtx context.Context) {
		res := func() (res feedback.ClickResult) {
			defer func() {
				if recovered := recover(); recovered != nil {
					slog.Error("feishu: 反馈原因处理 panic", "recover", recovered, "delivery_id", deliveryID)
					res = feedback.ClickResult{Toast: "内部错误，请稍后重试"}
				}
			}()
			result, err := fb.HandleReasonSubmit(execCtx, userID, feedback.ReasonSubmit{
				DeliveryID: deliveryID, Reason: reason,
			})
			if err != nil {
				slog.Error("feishu: 反馈原因处理失败", "err", err, "delivery_id", deliveryID)
				return feedback.ClickResult{Toast: "处理失败，请稍后重试"}
			}
			return result
		}()
		done <- res
	}) {
		return toastResponse("error", "服务正在重启，请稍后重试")
	}

	timer := time.NewTimer(cardActionSyncBudget)
	defer timer.Stop()
	select {
	case res := <-done:
		toastType := "error"
		if res.ToastOK {
			toastType = "success"
		}
		resp := &callback.CardActionTriggerResponse{
			Toast: &callback.Toast{Type: toastType, Content: res.Toast},
		}
		if res.CardJSON != "" {
			resp.Card = &callback.Card{Type: "raw", Data: json.RawMessage(res.CardJSON)}
		}
		return resp
	case <-timer.C:
		return toastResponse("info", "处理中，可稍后重新点击")
	}
}

// onFeedbackAction 处理推送卡反馈按钮点击（M5 契约 §10.3）。
//
// 与 M4 确认卡回调的两点刻意差异：
//  1. 超时后**丢弃结果不补发**——态度/误判是纯 DB 快路径，超时即异常态；且反馈
//     幂等，用户重点一次即可自愈。确认卡则不同：Claim 成功后动作不可重复领取，
//     结果不补发就永久丢失，所以那边必须补发。
//  2. 卡片更新用 feedback 服务返回的整卡 JSON（正文原样保留），而非替换成结果文本。
func (h *handler) onFeedbackAction(userID int64, action types.FeedbackAction, deliveryID int64) *callback.CardActionTriggerResponse {
	fb := h.m.feedbackRunner()
	if fb == nil {
		return toastResponse("error", "反馈功能尚未就绪，请稍后重试")
	}

	done := make(chan feedback.ClickResult, 1)
	if !h.m.startAsync(h.ctx, cardActionExecBudget, true, "feedback", func(execCtx context.Context) {
		// 与连接生命周期解耦（同 M4 卡片动作）：deep_dive 会在 HandleClick 内再起
		// 生成 goroutine，回调链结束不能中断它。
		res := func() (res feedback.ClickResult) {
			defer func() {
				if recovered := recover(); recovered != nil {
					slog.Error("feishu: 反馈处理 panic", "recover", recovered, "delivery_id", deliveryID)
					res = feedback.ClickResult{Toast: "内部错误，请稍后重试"}
				}
			}()
			result, err := fb.HandleClick(execCtx, userID, feedback.Click{Action: action, DeliveryID: deliveryID})
			if err != nil {
				slog.Error("feishu: 反馈处理失败", "err", err, "delivery_id", deliveryID, "fb", action)
				return feedback.ClickResult{Toast: "处理失败：" + humanizeLLMError(err)}
			}
			return result
		}()
		done <- res
	}) {
		return toastResponse("error", "服务正在重启，请稍后重试")
	}

	timer := time.NewTimer(cardActionSyncBudget)
	defer timer.Stop()
	select {
	case res := <-done:
		toastType := "error"
		if res.ToastOK {
			toastType = "success"
		}
		resp := &callback.CardActionTriggerResponse{
			Toast: &callback.Toast{Type: toastType, Content: res.Toast},
		}
		if res.CardJSON != "" {
			resp.Card = &callback.Card{Type: "raw", Data: json.RawMessage(res.CardJSON)}
		}
		return resp
	case <-timer.C:
		// 结果丢弃不补发（见函数注释理由 1）：goroutine 仍会跑完并落库，
		// 用户重看/重点即可看到最新状态。
		return toastResponse("info", "处理中，可稍后重新点击")
	}
}

// toastResponse 构造仅含 toast 的回调响应（不更新卡片）。
func toastResponse(typ, content string) *callback.CardActionTriggerResponse {
	return &callback.CardActionTriggerResponse{
		Toast: &callback.Toast{Type: typ, Content: content},
	}
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

// prependQuotedMessage 在用户引用/回复某条消息时，用飞书 API 拉取被引用消息
// 的内容并拼到用户文本前面，让 LLM 看到完整上下文。parentID 为空时原样返回。
// 拉取失败静默降级（只记日志），不影响正常消息处理。第二个返回值是显式
// 信任标签：true 表示结果已经混入引用正文，调用方必须走外部上下文入口。
func (h *handler) prependQuotedMessage(ctx context.Context, parentID, text string) (string, bool) {
	if parentID == "" {
		return text, false
	}
	client := h.m.api()
	if client == nil {
		return text, false
	}
	fetchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resp, err := client.Im.Message.Get(fetchCtx, larkim.NewGetMessageReqBuilder().
		MessageId(parentID).
		Build())
	if err != nil {
		slog.Warn("feishu: 拉取引用消息网络失败", "parent_id", parentID, "err", err)
		return text, false
	}
	if !resp.Success() {
		slog.Warn("feishu: 拉取引用消息被拒", "parent_id", parentID, "code", resp.Code, "msg", resp.Msg)
		return text, false
	}
	if resp.Data == nil || len(resp.Data.Items) == 0 {
		return text, false
	}
	quoted := resp.Data.Items[0]
	if quoted.Body == nil || quoted.Body.Content == nil {
		return text, false
	}
	var quotedText string
	switch strVal(quoted.MsgType) {
	case "text":
		quotedText = parseTextContent(*quoted.Body.Content)
	case "post":
		quotedText = parsePostContent(*quoted.Body.Content)
	case "interactive":
		quotedText = parseInteractiveContent(*quoted.Body.Content)
	default:
		return text, false
	}
	if quotedText == "" {
		return text, false
	}
	return "[用户引用的消息]\n" + quotedText + "\n[用户的回复]\n" + text, true
}

// parseInteractiveContent 从交互卡片的 JSON 中提取可读文本（卡片标题 + 文本元素）。
// Vane 自己发的回复都是交互卡片，用户引用时需要提取其中的文字。
func parseInteractiveContent(raw string) string {
	var card struct {
		Header *struct {
			Title *struct {
				Content string `json:"content"`
			} `json:"title"`
		} `json:"header"`
		Elements []json.RawMessage `json:"elements"`
	}
	if err := json.Unmarshal([]byte(raw), &card); err != nil {
		return ""
	}
	var parts []string
	if card.Header != nil && card.Header.Title != nil && card.Header.Title.Content != "" {
		parts = append(parts, card.Header.Title.Content)
	}
	for _, elem := range card.Elements {
		var el struct {
			Tag     string `json:"tag"`
			Content string `json:"content"`
			Text    *struct {
				Content string `json:"content"`
			} `json:"text"`
		}
		if json.Unmarshal(elem, &el) != nil {
			continue
		}
		switch {
		case el.Tag == "markdown" && el.Content != "":
			parts = append(parts, el.Content)
		case el.Tag == "div" && el.Text != nil && el.Text.Content != "":
			parts = append(parts, el.Text.Content)
		}
	}
	return strings.TrimSpace(strings.Join(parts, "\n"))
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

// postNode 是 post（富文本）消息段落里的一个内容节点。只关心三个字段：
// tag 区分节点类型，text 承载该节点的可读文字（如有），href 是 a 节点的
// 链接目标。
type postNode struct {
	Tag  string `json:"tag"`
	Text string `json:"text"`
	Href string `json:"href"`
}

// postTextTags 是提取纯文本时保留的节点类型：正文（text）、markdown（md）、
// 代码块（code_block）与超链接锚文本（a）都是句子的组成部分，丢掉任何一种
// 都会让粘贴的内容残缺——从网页复制的文字几乎必带超链接。at/img/media/
// emotion/hr 等不承载正文，忽略。
var postTextTags = map[string]bool{"text": true, "md": true, "code_block": true, "a": true}

// parsePostContent 从 post（富文本）消息的 Content JSON 提取纯文本。
// 结构为 {"title":"...","content":[[节点,...],...]}（官方文档 + lark-cli 实测）：
// 接收侧顶层没有发送 API 的 zh_cn 语言包装，content 是段落二维数组。
// 同段落的文本节点直接相接（一行文字因样式变化会被拆成多个节点），段落间
// 以换行分隔，title 非空时作首行；段落内部空白原样保留（粘贴代码的缩进
// 有意义），仅对拼接结果做一次首尾 TrimSpace——与 text 路径的
// parseTextContent 对整条消息的归一化一致。post 的 @ 是结构化 at 节点
// （占位符在其 user_id 字段），正文不会出现 "@_user_N"，因此不做 text
// 消息那样的 mentionRe 剥离——粘贴内容恰好含该字样时不能误删。
// 没有任何文字（纯图片等）返回空串，由调用方回退提示文案。
func parsePostContent(raw string) string {
	var body struct {
		Title   string       `json:"title"`
		Content [][]postNode `json:"content"`
	}
	if err := json.Unmarshal([]byte(raw), &body); err != nil {
		return ""
	}
	lines := make([]string, 0, len(body.Content)+1)
	if title := strings.TrimSpace(body.Title); title != "" {
		lines = append(lines, title)
	}
	for _, para := range body.Content {
		var sb strings.Builder
		for _, node := range para {
			if !postTextTags[node.Tag] {
				continue
			}
			sb.WriteString(node.Text)
			// 超链接的目标不能静默丢失：agent 的工具（如 add_source）要的
			// 正是 URL，锚文本与 href 不同时以"锚文本 (href)"并入正文；
			// 相同（裸链接粘贴的常态）则不重复。
			if node.Tag == "a" && node.Href != "" && node.Href != node.Text {
				sb.WriteString(" (" + node.Href + ")")
			}
		}
		if line := sb.String(); strings.TrimSpace(line) != "" {
			lines = append(lines, line)
		}
	}
	return strings.TrimSpace(strings.Join(lines, "\n"))
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

// extractReasonFromForm 从 form 提交事件里取误判原因文本，带三重对齐校验（附录 A.4）。
//
// 两代卡片两条路径，靠提交按钮的 Action.Name 分流：
//   - 历史单条卡：submit 按钮 name 恒为 "submit_reason"，原因在 FormValue["reason"]；
//   - 聚合卡：name 三件套按 delivery_id 唯一化（submit_{id} / reason_{id}），
//     **Action.Name 的后缀必须与 value 里的 delivery_id 一致**——form 归属（name）与
//     按钮携带（value）两条独立通道互验，任何一条被串改/漂移都会在此对不上。
//
// 对齐通过后 key 缺失按空串处理（原因本就可跳过），不回退旧 key——回退等于把
// "取错 form 的数据"重新变成静默路径。Name 为空的兜底只服务极老事件形状：仅当
// FormValue 里只有旧世界的 "reason" 键时才按历史卡处理。
func extractReasonFromForm(actionName string, formValue map[string]interface{}, deliveryID int64) (string, error) {
	idStr := strconv.FormatInt(deliveryID, 10)
	switch {
	case actionName == "submit_reason":
		// 历史单条卡：一卡一 form，无串条面。
		reason, _ := formValue["reason"].(string)
		return reason, nil
	case actionName == "submit_"+idStr:
		// 聚合卡且对齐成立：只认该条自己的 keyed input。
		reason, _ := formValue["reason_"+idStr].(string)
		return reason, nil
	case actionName == "":
		// 极老事件形状兜底：只在毫无聚合痕迹（无任何 reason_ 前缀键）时按历史卡处理。
		for k := range formValue {
			if strings.HasPrefix(k, "reason_") {
				return "", fmt.Errorf("事件缺 action name 但含聚合键，无法安全归属")
			}
		}
		reason, _ := formValue["reason"].(string)
		return reason, nil
	default:
		return "", fmt.Errorf("提交按钮 name %q 与 delivery_id %s 不对齐", actionName, idStr)
	}
}
