// content.query executor（契约 §5.4/§5.5）：确定性执行，直接查 store 不经 LLM。
package a2a

import (
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"iter"
	"log/slog"
	"strings"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"

	"github.com/YouToco/vane/server/promptguard"
	"github.com/YouToco/vane/server/types"
)

// SDK 内部对 artifact 做 gob DeepCopy：data part 接口位里的具体类型必须注册，
// 否则拷贝失败整任务转 FAILED（v2.3.1 httptest 实测："gob: type not registered
// for interface: []map[string]interface {}"）。
func init() {
	gob.Register([]map[string]any{})
}

// 入参缺省与钳制（契约 §5.4/§5.7）。
const (
	defaultDays  = 3
	minDays      = 1
	maxDays      = 30
	defaultLimit = 10
	minLimit     = 1
	maxLimit     = 25
	// excerptRunes 是 data part 里正文摘录的长度（契约 §5.4 产物定义）。
	excerptRunes = 300
	// untitledHeadRunes 是空标题内容在人读列表里的正文头回退长度
	//（X 官号类内容整批 title=''——Gate ⑥ 同款教训，这里从第一天就做对）。
	untitledHeadRunes = 50
)

type executor struct {
	deps Deps
}

func newExecutor(deps Deps) *executor { return &executor{deps: deps} }

// queryParams 是 content.query 解析后的入参。
type queryParams struct {
	keyword string
	days    int
	limit   int
}

// rawParams 是首个 text part 的 JSON 对象形态（契约 §5.4）。指针字段区分
// "未给"与"给了零值"：skill 键存在与否是 REJECTED 判定 ① 的依据。
// Text 仅 assistant.chat 使用（自然语言输入）。
type rawParams struct {
	Skill   *string `json:"skill"`
	Keyword string  `json:"keyword"`
	Days    *int    `json:"days"`
	Limit   *int    `json:"limit"`
	Text    string  `json:"text"`
}

// parsedInput 是分派后的入参：skill 决定走哪条执行路径。
type parsedInput struct {
	skill    string      // skillContentQuery | skillAssistantChat
	query    queryParams // skill == skillContentQuery 时有效
	chatText string      // skill == skillAssistantChat 时有效
}

// firstTextPart 取消息首个 text part 的文本。第二返回值=false 表示消息不含
// 任何 text part（纯 file/data part）——REJECTED 判定 ② 的依据。
func firstTextPart(msg *a2a.Message) (string, bool) {
	if msg == nil {
		return "", false
	}
	for _, p := range msg.Parts {
		if p == nil {
			continue
		}
		if _, isText := p.Content.(a2a.Text); isText {
			return p.Text(), true
		}
	}
	return "", false
}

// parseInput 按契约 §5.4 解析并分派入参：
//   - 纯文本 / 非法 JSON → content.query，整段文本 = keyword（缺省语义不变）；
//   - JSON 对象且 skill 缺省或 = content.query → content.query，取 {keyword,days,limit}；
//   - JSON 对象且 skill = assistant.chat → assistant.chat，取 text（必须非空）；
//   - 其余显式 skill → REJECTED 判定 ①。
//
// 缺省与钳制：days 3/[1,30]、limit 10/[1,25]、keyword 允许空。
func parseInput(text string) (parsedInput, string) {
	out := parsedInput{
		skill: skillContentQuery,
		query: queryParams{keyword: strings.TrimSpace(text), days: defaultDays, limit: defaultLimit},
	}
	trimmed := strings.TrimSpace(text)
	if !strings.HasPrefix(trimmed, "{") {
		return out, ""
	}
	var raw rawParams
	if err := json.Unmarshal([]byte(trimmed), &raw); err != nil {
		// 非法 JSON：按纯文本语义整段作 keyword（契约"解析失败则整段文本 = keyword"）。
		return out, ""
	}
	if raw.Skill != nil && *raw.Skill == skillAssistantChat {
		chatText := strings.TrimSpace(raw.Text)
		if chatText == "" {
			return out, fmt.Sprintf("%s 需要非空 text 字段（自然语言输入）", skillAssistantChat)
		}
		return parsedInput{skill: skillAssistantChat, chatText: chatText}, ""
	}
	if raw.Skill != nil && *raw.Skill != skillContentQuery {
		return out, fmt.Sprintf("不支持的 skill %q：本服务提供 %s / %s", *raw.Skill, skillContentQuery, skillAssistantChat)
	}
	out.query.keyword = strings.TrimSpace(raw.Keyword)
	if raw.Days != nil {
		out.query.days = clamp(*raw.Days, minDays, maxDays)
	}
	if raw.Limit != nil {
		out.query.limit = clamp(*raw.Limit, minLimit, maxLimit)
	}
	return out, ""
}

func clamp(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// Execute 实现 a2asrv.AgentExecutor：SendMessage 请求生命周期内同步完成
// （确定性查询阻塞返终态，无自有后台 goroutine——关停语义见契约 §5.2）。
func (e *executor) Execute(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {
		if execCtx.StoredTask == nil {
			if !yield(a2a.NewSubmittedTask(execCtx, execCtx.Message), nil) {
				return
			}
		}
		// REJECTED 前置判定（契约 §5.4 仅两种触发，不进执行）。
		text, hasText := firstTextPart(execCtx.Message)
		if !hasText {
			yield(a2a.NewStatusUpdateEvent(execCtx, a2a.TaskStateRejected,
				agentMessage(execCtx, "消息不含 text part，无从取参")), nil)
			return
		}
		input, rejectReason := parseInput(text)
		if rejectReason != "" {
			yield(a2a.NewStatusUpdateEvent(execCtx, a2a.TaskStateRejected,
				agentMessage(execCtx, rejectReason)), nil)
			return
		}
		requiredScope := types.A2AScopeContentQuery
		if input.skill == skillAssistantChat {
			requiredScope = types.A2AScopeAssistantChat
		}
		if !scopeGranted(ctx, requiredScope) {
			yield(a2a.NewStatusUpdateEvent(execCtx, a2a.TaskStateRejected,
				agentMessage(execCtx, "当前 A2A 凭证未获准使用 "+string(requiredScope))), nil)
			return
		}

		// assistant.chat 分派（契约 §12 P2）：LLM 轨，与下方确定性检索路径互斥。
		if input.skill == skillAssistantChat {
			e.executeChat(ctx, execCtx, input.chatText, yield)
			return
		}

		params := input.query
		if !yield(a2a.NewStatusUpdateEvent(execCtx, a2a.TaskStateWorking, nil), nil) {
			return
		}

		qctx, cancel := context.WithTimeout(ctx, dbQueryTimeout)
		defer cancel()
		since := time.Now().Add(-time.Duration(params.days) * 24 * time.Hour)
		scope, scopeErr := authorityFromContext(qctx)
		if scopeErr != nil {
			yield(a2a.NewStatusUpdateEvent(execCtx, a2a.TaskStateFailed,
				agentMessage(execCtx, "A2A 请求身份已失效")), nil)
			return
		}
		items, err := e.deps.Content.SearchContentItemsForA2A(qctx, scope, params.keyword, since, params.limit)
		if err != nil {
			// 原始错误链只落日志；对外只有 sanitize 文案（契约 §8.1 红线）。
			slog.Error("a2a: content.query 检索失败",
				"task_id", execCtx.TaskID, "context_id", execCtx.ContextID, "err", err)
			yield(a2a.NewStatusUpdateEvent(execCtx, a2a.TaskStateFailed,
				agentMessage(execCtx, sanitize(err))), nil)
			return
		}

		if !yield(a2a.NewArtifactEvent(execCtx, textPart(items, params), dataPart(items)), nil) {
			return
		}
		yield(a2a.NewStatusUpdateEvent(execCtx, a2a.TaskStateCompleted, nil), nil)
	}
}

// Cancel 实现 a2asrv.AgentExecutor：P1 的执行在请求生命周期内同步完成，收到取消
// 即按 SDK 最简语义 yield CANCELED（官方示例形态）；终态任务的取消由 SDK 侧拒绝。
func (e *executor) Cancel(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {
		yield(a2a.NewStatusUpdateEvent(execCtx, a2a.TaskStateCanceled, nil), nil)
	}
}

// agentMessage 构造 agent 角色的说明消息（REJECTED/FAILED 的人话载体）。
func agentMessage(execCtx *a2asrv.ExecutorContext, text string) *a2a.Message {
	return a2a.NewMessageForTask(a2a.MessageRoleAgent, execCtx, a2a.NewTextPart(text))
}

// itemTime 是条目的展示时间：published_at 可空回退 fetched_at（与检索窗口同口径）。
func itemTime(ci types.ContentItem) time.Time {
	if ci.PublishedAt != nil {
		return *ci.PublishedAt
	}
	return ci.FetchedAt
}

// itemTitle 是条目的展示标题：空标题回退正文头（Gate ⑥ 教训前置吸收）。
func itemTitle(ci types.ContentItem) string {
	if t := strings.TrimSpace(ci.Title); t != "" {
		return t
	}
	head := promptguard.TruncateRunes(promptguard.SingleLine(ci.Content), untitledHeadRunes)
	if head == "" {
		return "(无标题)"
	}
	return head
}

// textPart 产物之一：人读中文列表（标题+链接+时间，契约 §5.4）。
func textPart(items []types.ContentItem, params queryParams) *a2a.Part {
	if len(items) == 0 {
		return a2a.NewTextPart(fmt.Sprintf("最近 %d 天内没有匹配的内容（keyword=%q）。", params.days, params.keyword))
	}
	var b strings.Builder
	fmt.Fprintf(&b, "最近 %d 天匹配内容 %d 条：\n", params.days, len(items))
	for i, ci := range items {
		fmt.Fprintf(&b, "%d. %s\n   %s · %s\n", i+1, itemTitle(ci), ci.URL, itemTime(ci).Format("2006-01-02 15:04"))
	}
	return a2a.NewTextPart(strings.TrimRight(b.String(), "\n"))
}

// dataPart 产物之二：机器可读 JSON 列表。不含 score、不含 summary、不含画像信号
// （契约 §8.3——数据面窄接口本身就取不到这些列）。
func dataPart(items []types.ContentItem) *a2a.Part {
	rows := make([]map[string]any, 0, len(items))
	for _, ci := range items {
		rows = append(rows, map[string]any{
			"title":        ci.Title,
			"url":          ci.URL,
			"published_at": itemTime(ci).Format(time.RFC3339),
			"excerpt":      promptguard.TruncateRunes(ci.Content, excerptRunes),
		})
	}
	return a2a.NewDataPart(rows)
}
