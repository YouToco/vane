// assistant.chat executor（契约 §12 P2 / PR-4）：LLM 轨——把入站自然语言交给
// A2A 轨 agent.Loop（RunOnce；public-only 候选经 A2A 授权过滤后当前为零工具），
// 按 contextId 重建多轮历史。
//
// 与 content.query 的分工：query 是确定性检索（零 LLM、结果可复现），chat 是
// 对话式问答（仅当前请求与同 context 历史）。卡片 description 引导对端
// 各取所长（检索找 query、对话找 chat）。
package a2a

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"

	"github.com/YouToco/vane/server/llm"
	"github.com/YouToco/vane/server/types"
)

// chatHistoryTasks 是按 contextId 重建历史时纳入的最近任务数上限。
// 每任务折叠为一对 user/assistant 消息，8 对 ≈ 16 条，远低于 agent 的
// 60 条截断阈值——上限的意义是防单 context 无限膨胀，不是精确记忆承诺。
const chatHistoryTasks = 8

// executeChat 执行一次 assistant.chat（在 Execute 的 yield 流内被调用）。
//
// 失败语义：装配缺失 → REJECTED（配置问题，重试无益）；owner 未捕获/LLM 失败
// → FAILED + sanitize 文案（原始错误链只落日志，契约 §8.1 红线）。
// 总预算 chatBudget（120s）盖住整个多轮 FC；SDK 把执行跑在脱离请求的后台
// goroutine（taskstore 适配层注释），超预算时 HTTP 响应可能先断，任务经
// GetTask 兜底可取。
func (e *executor) executeChat(ctx context.Context, execCtx *a2asrv.ExecutorContext, text string, yield func(a2a.Event, error) bool) {
	if e.deps.Chat == nil || e.deps.Principal == nil {
		yield(a2a.NewStatusUpdateEvent(execCtx, a2a.TaskStateRejected,
			agentMessage(execCtx, skillAssistantChat+" 未启用（服务端未装配 agent 轨）")), nil)
		return
	}
	if !yield(a2a.NewStatusUpdateEvent(execCtx, a2a.TaskStateWorking, nil), nil) {
		return
	}

	cctx, cancel := context.WithTimeout(ctx, chatBudget)
	defer cancel()

	userID, err := e.resolveOwner(cctx)
	if err != nil {
		// 固定文案，**不经 sanitize 透传 Message**（对抗审查 B-F1）：owner 解析链上的
		// 错误 Message 会内嵌 open_id（store.UpsertUserByOpenID 的 "upsert 用户（open_id=…）"）
		// 与内部 settings key 名——sanitize 对 AppError 放行 Message，会把它们送给外部 agent。
		// 原始错误（含 open_id）只落上面的 slog，供内部排查。
		slog.Error("a2a: assistant.chat owner 解析失败",
			"task_id", execCtx.TaskID, "context_id", execCtx.ContextID, "err", err)
		yield(a2a.NewStatusUpdateEvent(execCtx, a2a.TaskStateFailed,
			agentMessage(execCtx, ownerErrText(err))), nil)
		return
	}

	history := e.chatHistory(cctx, execCtx)
	outcome, _, err := e.deps.Chat.RunOnce(cctx, userID, history, text)
	if err != nil {
		// 固定文案，**不透传 Message**（对抗审查 A-2）：RunOnce 的错误可能源自 llm 层，
		// 而 llm.mapHTTPError 把上游响应体全文拼进 Message（DeepSeek 4xx/5xx 的 provider
		// 原文、request id、内容过滤引用），sanitize 会原样放行。A2A 轨首次把 llm 错误接进
		// 对外出口，必须收窄为固定文案；原始错误只落 slog（契约 §8.1）。
		slog.Error("a2a: assistant.chat 执行失败",
			"task_id", execCtx.TaskID, "context_id", execCtx.ContextID, "err", err)
		yield(a2a.NewStatusUpdateEvent(execCtx, a2a.TaskStateFailed,
			agentMessage(execCtx, chatFailText())), nil)
		return
	}

	if !yield(a2a.NewArtifactEvent(execCtx, a2a.NewTextPart(outcome.Reply)), nil) {
		return
	}
	yield(a2a.NewStatusUpdateEvent(execCtx, a2a.TaskStateCompleted, nil), nil)
}

// ownerErrText 是 owner 解析失败的对外固定文案：区分"未初始化"（可行动提示）与
// 其余（笼统失败）。绝不透传底层 Message——它可能含 open_id / settings key 名（B-F1）。
func ownerErrText(err error) string {
	var ae *types.AppError
	if errors.As(err, &ae) && ae.Code == types.CodeConflict {
		return "服务尚未完成初始化，暂无法对话"
	}
	return "服务暂时无法处理对话，请稍后重试"
}

// chatFailText 是 assistant.chat 执行失败的对外固定文案（A-2：不透传 llm 层 Message）。
func chatFailText() string { return "对话处理失败，请稍后重试" }

// resolveOwner 把「当前 principal」解析成 users 主键。
//
// 逻辑本体已收敛到 auth 包（企业级契约 §1.1，不变量 I-A1）；本函数只剩两层适配：
// ① 加 dbQueryTimeout（A2A 特有的预算，auth 包不该替调用方决定超时）；
// ② 收窄成 userID——A2A 轨仅用该身份建立对话范围，不暴露 owner 工具目录。
// 错误的对外文案仍由 ownerErrText 按错误码收窄（B-F1），内层 Message 永不外露。
func (e *executor) resolveOwner(ctx context.Context) (int64, error) {
	octx, cancel := context.WithTimeout(ctx, dbQueryTimeout)
	defer cancel()
	p, err := e.deps.Principal.FromContext(octx)
	if err != nil {
		return 0, err
	}
	return p.UserID, nil
}

// chatHistory 按 contextId 重建多轮历史。A2A 的多轮语义：任务终态后不可续写
// （Gate ⑤），追问是同 contextId 下的新任务——上下文由服务端跨任务重建。
// 只纳入已 COMPLETED 的 assistant.chat 任务（content.query 任务不是对话轮次），
// 每任务折叠成一对 user/assistant，取最近 chatHistoryTasks 个、按时间正序返回。
//
// 失败与解析异常一律降级为空/部分历史并记日志：历史是增强不是门槛，
// 丢上下文的代价远小于把整个任务失败掉（与 agent.profileHint 同哲学）。
func (e *executor) chatHistory(ctx context.Context, execCtx *a2asrv.ExecutorContext) []llm.ChatMessage {
	octx, cancel := context.WithTimeout(ctx, dbQueryTimeout)
	defer cancel()
	rows, _, _, err := e.deps.Storage.ListA2ATasks(octx, types.A2ATaskQuery{
		ContextID: string(execCtx.ContextID),
		Status:    string(a2a.TaskStateCompleted),
	})
	if err != nil {
		slog.Warn("a2a: assistant.chat 历史查询失败，按空历史继续",
			"context_id", execCtx.ContextID, "err", err)
		return nil
	}

	// rows 按 created_at DESC（store 契约 §4.1）：从最新往回收集，够数即停，再反转为正序。
	type exchange struct{ user, agent string }
	var picked []exchange
	for _, row := range rows {
		if row.ID == string(execCtx.TaskID) {
			continue // 当前任务自身（SDK 已先落 SUBMITTED）不进历史。
		}
		ex, ok := chatExchange(row.Task)
		if !ok {
			continue
		}
		picked = append(picked, ex)
		if len(picked) == chatHistoryTasks {
			break
		}
	}

	hist := make([]llm.ChatMessage, 0, len(picked)*2)
	for i := len(picked) - 1; i >= 0; i-- {
		hist = append(hist,
			llm.ChatMessage{Role: "user", Content: picked[i].user},
			llm.ChatMessage{Role: "assistant", Content: picked[i].agent},
		)
	}
	return hist
}

// chatExchange 从任务 JSONB 还原一对对话轮次：输入必须是 assistant.chat 形态
// （history 首个 user text part 解析出 skill=assistant.chat），回复取首个 artifact
// 的首个 text part。任何一环缺失即 ok=false（不是 chat 任务或形态异常，跳过）。
func chatExchange(rawTask json.RawMessage) (struct{ user, agent string }, bool) {
	var out struct{ user, agent string }
	var task a2a.Task
	if err := json.Unmarshal(rawTask, &task); err != nil {
		slog.Warn("a2a: 历史任务反序列化失败，跳过", "err", err)
		return out, false
	}
	var inputText string
	for _, m := range task.History {
		if m == nil || m.Role != a2a.MessageRoleUser {
			continue
		}
		if t, ok := firstTextPart(m); ok {
			inputText = t
		}
		break
	}
	if inputText == "" {
		return out, false
	}
	input, reject := parseInput(inputText)
	if reject != "" || input.skill != skillAssistantChat {
		return out, false
	}
	var reply string
	for _, a := range task.Artifacts {
		if a == nil {
			continue
		}
		for _, p := range a.Parts {
			if p == nil {
				continue
			}
			if _, isText := p.Content.(a2a.Text); isText {
				reply = p.Text()
				break
			}
		}
		if reply != "" {
			break
		}
	}
	if reply == "" {
		return out, false
	}
	out.user = input.chatText
	out.agent = reply
	return out, true
}
