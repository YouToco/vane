package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/YouToco/vane/llm"
	"github.com/YouToco/vane/types"
)

// ============================================================
// 内存假实现：Store / Tool / chatFn（契约 §11：store 用内存假实现，
// chatFn 注入假实现替代 llm.Client）。
// ============================================================

// fakeStore 是 Store 窄接口的内存实现，语义对齐契约 §2
// （Get 按 since 过滤、Claim 原子幂等、Cancel 仅 pending 可取消）。
type fakeStore struct {
	nextSessionID int64
	sessions      map[int64]*types.AgentSession
	actions       map[string]*types.PendingAction

	updateCalls   int
	lastMessages  json.RawMessage
	lastTurnCount int

	// mu 保护 appendCalls 与 sessions 内容：卡片回调回写在独立 goroutine
	// 里执行，与测试主 goroutine 的断言读取并发。
	mu          sync.Mutex
	appendCalls []appendRecord
}

// appendRecord 记录一次 AppendAgentSessionMessages 调用，供断言回写目标与内容。
type appendRecord struct {
	sessionID int64
	msgs      json.RawMessage
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		sessions: make(map[int64]*types.AgentSession),
		actions:  make(map[string]*types.PendingAction),
	}
}

func notFoundErr(msg string) error {
	return types.NewAppError(types.CodeNotFound, msg, nil)
}

func (f *fakeStore) GetActiveAgentSession(_ context.Context, userID int64, since time.Time) (*types.AgentSession, error) {
	for _, s := range f.sessions {
		if s.UserID == userID && s.Status == types.AgentSessionStatusActive && s.UpdatedAt.After(since) {
			cp := *s
			return &cp, nil
		}
	}
	return nil, notFoundErr("fake: 无 active 会话")
}

func (f *fakeStore) CreateAgentSession(_ context.Context, userID int64) (*types.AgentSession, error) {
	f.nextSessionID++
	s := &types.AgentSession{
		ID:        f.nextSessionID,
		UserID:    userID,
		Status:    types.AgentSessionStatusActive,
		Messages:  json.RawMessage("[]"),
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	f.sessions[s.ID] = s
	cp := *s
	return &cp, nil
}

func (f *fakeStore) UpdateAgentSession(_ context.Context, id int64, messages json.RawMessage, turnCount int) error {
	s, ok := f.sessions[id]
	if !ok {
		return notFoundErr("fake: 会话不存在")
	}
	s.Messages = messages
	s.TurnCount = turnCount
	s.UpdatedAt = time.Now()
	f.updateCalls++
	f.lastMessages = messages
	f.lastTurnCount = turnCount
	return nil
}

func (f *fakeStore) AppendAgentSessionMessages(ctx context.Context, sessionID int64, msgs json.RawMessage) error {
	// 对齐 pgx 行为：已取消/过期的 ctx 立即失败不触库——回写必须在拿到锁后
	// 用脱离调用方 deadline 的独立 ctx，否则这里就会把它打回。
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	s, ok := f.sessions[sessionID]
	if !ok {
		return notFoundErr("fake: 会话不存在")
	}
	// 模拟 jsonb || 的数组拼接语义（两边都必须是数组）。生产实现不刷 updated_at
	// （防点卡复活超时会话），fake 同步该语义。
	var existing, incoming []json.RawMessage
	if err := json.Unmarshal(s.Messages, &existing); err != nil {
		return err
	}
	if err := json.Unmarshal(msgs, &incoming); err != nil {
		return err
	}
	merged, err := json.Marshal(append(existing, incoming...))
	if err != nil {
		return err
	}
	s.Messages = merged
	f.appendCalls = append(f.appendCalls, appendRecord{sessionID: sessionID, msgs: msgs})
	return nil
}

func (f *fakeStore) appendCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.appendCalls)
}

// waitAppends 等回写 goroutine 落地到恰好 want 次（回写是异步的）。
// 等到后再多留一拍确认没有多余回写溜进来。
func waitAppends(t *testing.T, f *fakeStore, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for f.appendCount() != want {
		if time.Now().After(deadline) {
			t.Fatalf("等待 %d 次回写超时, 实得 %d", want, f.appendCount())
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)
	if got := f.appendCount(); got != want {
		t.Fatalf("回写次数应稳定在 %d, 实得 %d", want, got)
	}
}

// sessionMessages 锁内取会话当前 messages 快照（异步回写也写这份数据）。
func (f *fakeStore) sessionMessages(id int64) *types.AgentSession {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := *f.sessions[id]
	return &cp
}

func (f *fakeStore) CreatePendingAction(_ context.Context, a *types.PendingAction) error {
	cp := *a
	f.actions[a.ID] = &cp
	return nil
}

func (f *fakeStore) ClaimPendingAction(_ context.Context, id string, userID int64) (*types.PendingAction, error) {
	a, ok := f.actions[id]
	if !ok || a.UserID != userID || a.Status != types.PendingActionStatusPending || !a.ExpiresAt.After(time.Now()) {
		return nil, notFoundErr("fake: 动作不可领取")
	}
	a.Status = types.PendingActionStatusExecuted
	now := time.Now()
	a.ExecutedAt = &now
	cp := *a
	return &cp, nil
}

func (f *fakeStore) CancelPendingAction(_ context.Context, id string, userID int64) (*types.PendingAction, error) {
	a, ok := f.actions[id]
	if !ok || a.UserID != userID || a.Status != types.PendingActionStatusPending {
		return nil, notFoundErr("fake: 动作不可取消")
	}
	a.Status = types.PendingActionStatusCancelled
	cp := *a
	return &cp, nil
}

// fakeTool 记录每次 Execute 调用，供断言"执行了几次、带什么参数"。
type fakeTool struct {
	name     string
	mutating bool
	result   string
	execErr  error
	calls    []toolCallRecord
}

type toolCallRecord struct {
	userID int64
	args   string
}

func (t *fakeTool) Name() string                       { return t.name }
func (t *fakeTool) Description() string                { return "测试工具 " + t.name }
func (t *fakeTool) Parameters() json.RawMessage        { return json.RawMessage(`{"type":"object"}`) }
func (t *fakeTool) Mutating() bool                     { return t.mutating }
func (t *fakeTool) Summarize(a json.RawMessage) string { return "摘要:" + string(a) }

func (t *fakeTool) Execute(_ context.Context, userID int64, args json.RawMessage) (string, error) {
	t.calls = append(t.calls, toolCallRecord{userID: userID, args: string(args)})
	return t.result, t.execErr
}

// scriptedChat 按脚本顺序吐响应，并记录每次请求供断言（消息序列/工具携带情况）。
type scriptedChat struct {
	responses []*llm.ChatResponse
	requests  []llm.ChatRequest
}

func (s *scriptedChat) fn(_ context.Context, req llm.ChatRequest) (*llm.ChatResponse, error) {
	s.requests = append(s.requests, req)
	if len(s.responses) == 0 {
		return nil, types.NewAppError(types.CodeLLMUnavailable, "fake: 响应脚本耗尽", nil)
	}
	resp := s.responses[0]
	s.responses = s.responses[1:]
	return resp, nil
}

// newTestLoop 构造注入假 chatFn 的 Loop。Client/Recorder 传 nil：
// New 生成的默认 chatFn 随即被覆盖，永远不会被调用。
func newTestLoop(t *testing.T, fs *fakeStore, chat func(context.Context, llm.ChatRequest) (*llm.ChatResponse, error), tools ...Tool) *Loop {
	t.Helper()
	l := New(Deps{
		Store:      fs,
		Tools:      tools,
		Model:      "deepseek-v4-pro",
		MaxTurns:   5,
		SessionTTL: 30 * time.Minute,
	})
	l.chatFn = chat
	return l
}

// persistedMessages 解出最近一次 UpdateAgentSession 写入的消息数组。
func persistedMessages(t *testing.T, fs *fakeStore) []llm.ChatMessage {
	t.Helper()
	var msgs []llm.ChatMessage
	if err := json.Unmarshal(fs.lastMessages, &msgs); err != nil {
		t.Fatalf("持久化的 messages 不是合法 JSON: %v", err)
	}
	return msgs
}

// ============================================================
// 契约 §11 六类用例
// ============================================================

// 用例 1：纯聊天——模型不调工具直接回答，会话持久化且 system 不入库。
func TestHandleMessage_PlainChat(t *testing.T) {
	fs := newFakeStore()
	chat := &scriptedChat{responses: []*llm.ChatResponse{
		{Content: "你好，有什么可以帮你？", FinishReason: "stop"},
	}}
	l := newTestLoop(t, fs, chat.fn, &fakeTool{name: "list_sources", result: "无"})

	out, err := l.HandleMessage(context.Background(), 1, "你好")
	if err != nil {
		t.Fatalf("HandleMessage 意外报错: %v", err)
	}
	if out.Reply != "你好，有什么可以帮你？" {
		t.Fatalf("Reply = %q, 期望模型原文", out.Reply)
	}
	if out.Confirm != nil {
		t.Fatalf("纯聊天不应出确认卡, 实得 %+v", out.Confirm)
	}

	// 请求侧：system 动态前置 + 工具声明齐全。
	if len(chat.requests) != 1 {
		t.Fatalf("期望恰好 1 次模型调用, 实得 %d", len(chat.requests))
	}
	req := chat.requests[0]
	if req.Messages[0].Role != "system" || req.Messages[0].Content != systemPrompt {
		t.Fatalf("请求首条消息应为 system prompt, 实得 %+v", req.Messages[0])
	}
	if last := req.Messages[len(req.Messages)-1]; last.Role != "user" || last.Content != "你好" {
		t.Fatalf("请求末条消息应为用户输入, 实得 %+v", last)
	}
	if len(req.Tools) != 1 {
		t.Fatalf("请求应携带 1 个工具声明, 实得 %d", len(req.Tools))
	}

	// 持久化侧：user+assistant 两条，system 不入库，turn_count=1。
	if fs.updateCalls != 1 {
		t.Fatalf("期望 UpdateAgentSession 恰好 1 次, 实得 %d", fs.updateCalls)
	}
	msgs := persistedMessages(t, fs)
	if len(msgs) != 2 || msgs[0].Role != "user" || msgs[1].Role != "assistant" {
		t.Fatalf("持久化消息应为 [user, assistant], 实得 %+v", msgs)
	}
	if fs.lastTurnCount != 1 {
		t.Fatalf("turn_count = %d, 期望 1", fs.lastTurnCount)
	}
}

// 用例 2：读工具单轮——直接执行、结果以 role=tool 回给模型继续收敛。
func TestHandleMessage_ReadToolSingleRound(t *testing.T) {
	fs := newFakeStore()
	readTool := &fakeTool{name: "list_sources", result: "1. RSS 示例源（active）"}
	chat := &scriptedChat{responses: []*llm.ChatResponse{
		{ToolCalls: []llm.ToolCall{{ID: "call_1", Name: "list_sources", Arguments: "{}"}}, FinishReason: "tool_calls"},
		{Content: "你目前有 1 个信源。", FinishReason: "stop"},
	}}
	l := newTestLoop(t, fs, chat.fn, readTool)

	out, err := l.HandleMessage(context.Background(), 7, "我订了哪些源")
	if err != nil {
		t.Fatalf("HandleMessage 意外报错: %v", err)
	}
	if out.Reply != "你目前有 1 个信源。" || out.Confirm != nil {
		t.Fatalf("期望纯文字收敛, 实得 Reply=%q Confirm=%v", out.Reply, out.Confirm)
	}

	// 读工具被真实执行，且拿到正确的 userID。
	if len(readTool.calls) != 1 || readTool.calls[0].userID != 7 {
		t.Fatalf("读工具应执行 1 次且 userID=7, 实得 %+v", readTool.calls)
	}

	// 第二次请求必须携带：原样回传 tool_calls 的 assistant 消息 + 对应 tool 回执。
	if len(chat.requests) != 2 {
		t.Fatalf("期望 2 次模型调用, 实得 %d", len(chat.requests))
	}
	second := chat.requests[1].Messages
	asst := second[len(second)-2]
	if asst.Role != "assistant" || len(asst.ToolCalls) != 1 || asst.ToolCalls[0].ID != "call_1" {
		t.Fatalf("assistant 历史消息应原样携带 tool_calls, 实得 %+v", asst)
	}
	toolReply := second[len(second)-1]
	if toolReply.Role != "tool" || toolReply.ToolCallID != "call_1" || toolReply.Content != readTool.result {
		t.Fatalf("tool 回执不符, 实得 %+v", toolReply)
	}

	// 持久化：user / assistant(tool_calls) / tool / assistant 共 4 条，turn_count=2。
	if msgs := persistedMessages(t, fs); len(msgs) != 4 {
		t.Fatalf("持久化消息应为 4 条, 实得 %d: %+v", len(msgs), msgs)
	}
	if fs.lastTurnCount != 2 {
		t.Fatalf("turn_count = %d, 期望 2", fs.lastTurnCount)
	}
}

// 用例 3：写工具确认卡——不执行、落 pending_action、其余 tool_calls 补占位、
// 收尾调用不带 tools、Outcome.Confirm 非 nil、会话照常持久化。
func TestHandleMessage_MutatingToolConfirmCard(t *testing.T) {
	fs := newFakeStore()
	addTool := &fakeTool{name: "add_source", mutating: true, result: "不应被执行"}
	readTool := &fakeTool{name: "list_sources", result: "不应被执行"}
	argsJSON := `{"type":"rss","url":"https://example.com/feed"}`
	chat := &scriptedChat{responses: []*llm.ChatResponse{
		{ToolCalls: []llm.ToolCall{
			{ID: "call_1", Name: "add_source", Arguments: argsJSON},
			{ID: "call_2", Name: "list_sources", Arguments: "{}"},
		}, FinishReason: "tool_calls"},
		{Content: "已生成确认卡，点确认后我就添加。", FinishReason: "stop"},
	}}
	l := newTestLoop(t, fs, chat.fn, addTool, readTool)

	out, err := l.HandleMessage(context.Background(), 7, "帮我订阅这个 RSS")
	if err != nil {
		t.Fatalf("HandleMessage 意外报错: %v", err)
	}
	if out.Confirm == nil {
		t.Fatal("写工具应出确认卡, Confirm 为 nil")
	}
	if out.Reply != "已生成确认卡，点确认后我就添加。" {
		t.Fatalf("Reply 应为收尾文案, 实得 %q", out.Reply)
	}

	// 写工具绝不执行；挂起后的读工具同样不执行。
	if len(addTool.calls) != 0 || len(readTool.calls) != 0 {
		t.Fatalf("确认前任何工具都不应执行, 实得 add=%d read=%d", len(addTool.calls), len(readTool.calls))
	}

	// pending_action 落库且参数以库中为准。
	pa, ok := fs.actions[out.Confirm.ActionID]
	if !ok {
		t.Fatalf("Confirm.ActionID=%s 在 store 中不存在", out.Confirm.ActionID)
	}
	if pa.UserID != 7 || pa.ToolName != "add_source" || string(pa.Args) != argsJSON {
		t.Fatalf("pending_action 字段不符: %+v", pa)
	}
	if pa.Status != types.PendingActionStatusPending {
		t.Fatalf("pending_action 状态应为 pending, 实得 %s", pa.Status)
	}
	if until := time.Until(pa.ExpiresAt); until < 23*time.Hour || until > 25*time.Hour {
		t.Fatalf("pending_action 应约 24h 过期, 实得 %v", until)
	}
	// 卡片正文 = 工具名 + Summarize 摘要。
	if !strings.Contains(out.Confirm.Summary, "add_source") || !strings.Contains(out.Confirm.Summary, "摘要:"+argsJSON) {
		t.Fatalf("Confirm.Summary 应含工具名与参数摘要, 实得 %q", out.Confirm.Summary)
	}

	// 收尾调用：不带 tools；每个 tool_call 都有回执（call_1 确认卡、call_2 占位）。
	if len(chat.requests) != 2 {
		t.Fatalf("期望 2 次模型调用, 实得 %d", len(chat.requests))
	}
	final := chat.requests[1]
	if len(final.Tools) != 0 {
		t.Fatalf("收尾调用不得携带 tools, 实得 %d 个", len(final.Tools))
	}
	replies := map[string]string{}
	for _, m := range final.Messages {
		if m.Role == "tool" {
			replies[m.ToolCallID] = m.Content
		}
	}
	if replies["call_1"] != toolMsgConfirmCreated {
		t.Fatalf("call_1 回执 = %q, 期望 %q", replies["call_1"], toolMsgConfirmCreated)
	}
	if replies["call_2"] != toolMsgSuspended {
		t.Fatalf("call_2 回执 = %q, 期望 %q", replies["call_2"], toolMsgSuspended)
	}

	// 出确认卡路径同样要持久化会话（契约 §7）。
	if fs.updateCalls != 1 {
		t.Fatalf("确认卡路径应持久化会话 1 次, 实得 %d", fs.updateCalls)
	}
}

// 用例 4：未知工具自纠——以 role=tool 回"工具 X 不存在"，模型下一轮自纠收敛。
func TestHandleMessage_UnknownToolSelfCorrect(t *testing.T) {
	fs := newFakeStore()
	chat := &scriptedChat{responses: []*llm.ChatResponse{
		{ToolCalls: []llm.ToolCall{{ID: "call_1", Name: "ghost_tool", Arguments: "{}"}}, FinishReason: "tool_calls"},
		{Content: "抱歉，这个我做不了。", FinishReason: "stop"},
	}}
	l := newTestLoop(t, fs, chat.fn, &fakeTool{name: "list_sources"})

	out, err := l.HandleMessage(context.Background(), 1, "干点怪事")
	if err != nil {
		t.Fatalf("HandleMessage 意外报错: %v", err)
	}
	if out.Reply != "抱歉，这个我做不了。" || out.Confirm != nil {
		t.Fatalf("期望自纠后文字收敛, 实得 Reply=%q Confirm=%v", out.Reply, out.Confirm)
	}
	if len(fs.actions) != 0 {
		t.Fatalf("未注册工具绝不落 pending_action, 实得 %d 条", len(fs.actions))
	}

	second := chat.requests[1].Messages
	toolReply := second[len(second)-1]
	if toolReply.Role != "tool" || toolReply.ToolCallID != "call_1" || toolReply.Content != "工具 ghost_tool 不存在" {
		t.Fatalf("未知工具应回错误文本供自纠, 实得 %+v", toolReply)
	}
}

// 用例 5：maxTurns 兜底——模型一直调读工具不收敛，到上限回兜底文案而非报错。
func TestHandleMessage_MaxTurnsFallback(t *testing.T) {
	fs := newFakeStore()
	readTool := &fakeTool{name: "list_sources", result: "空"}
	calls := 0
	stubborn := func(_ context.Context, _ llm.ChatRequest) (*llm.ChatResponse, error) {
		calls++
		return &llm.ChatResponse{
			ToolCalls:    []llm.ToolCall{{ID: fmt.Sprintf("call_%d", calls), Name: "list_sources", Arguments: "{}"}},
			FinishReason: "tool_calls",
		}, nil
	}
	l := New(Deps{Store: fs, Tools: []Tool{readTool}, Model: "deepseek-v4-pro", MaxTurns: 3, SessionTTL: 30 * time.Minute})
	l.chatFn = stubborn

	out, err := l.HandleMessage(context.Background(), 1, "陷入循环吧")
	if err != nil {
		t.Fatalf("maxTurns 兜底不应报错: %v", err)
	}
	if out.Reply != replyMaxTurns {
		t.Fatalf("Reply = %q, 期望兜底文案 %q", out.Reply, replyMaxTurns)
	}
	if calls != 3 {
		t.Fatalf("模型调用次数 = %d, 期望恰好 MaxTurns=3", calls)
	}
	if fs.lastTurnCount != 3 {
		t.Fatalf("turn_count = %d, 期望 3", fs.lastTurnCount)
	}
}

// 用例 6：ExecuteAction 幂等——首次领取执行成功，双击/重放第二次拿人话文本、
// 工具绝不二次执行；过期动作同样不可领取。
func TestExecuteAction_Idempotent(t *testing.T) {
	fs := newFakeStore()
	addTool := &fakeTool{name: "add_source", mutating: true, result: "已添加信源：示例"}
	argsJSON := `{"type":"rss","url":"https://example.com/feed"}`
	fs.actions["act-1"] = &types.PendingAction{
		ID:        "act-1",
		UserID:    7,
		ToolName:  "add_source",
		Args:      json.RawMessage(argsJSON),
		Status:    types.PendingActionStatusPending,
		ExpiresAt: time.Now().Add(time.Hour),
	}
	fs.actions["act-expired"] = &types.PendingAction{
		ID:        "act-expired",
		UserID:    7,
		ToolName:  "add_source",
		Args:      json.RawMessage("{}"),
		Status:    types.PendingActionStatusPending,
		ExpiresAt: time.Now().Add(-time.Minute),
	}
	l := newTestLoop(t, fs, (&scriptedChat{}).fn, addTool)

	// 首次：领取成功、真实执行、参数取自库中。
	got, err := l.ExecuteAction(context.Background(), 7, "act-1")
	if err != nil {
		t.Fatalf("首次 ExecuteAction 意外报错: %v", err)
	}
	if got != addTool.result {
		t.Fatalf("首次结果 = %q, 期望工具输出 %q", got, addTool.result)
	}
	if len(addTool.calls) != 1 || addTool.calls[0].userID != 7 || addTool.calls[0].args != argsJSON {
		t.Fatalf("工具应执行 1 次且参数以库中为准, 实得 %+v", addTool.calls)
	}

	// 第二次（双击/重放）：人话文本 + nil error，工具不二次执行。
	got2, err := l.ExecuteAction(context.Background(), 7, "act-1")
	if err != nil {
		t.Fatalf("重复 ExecuteAction 应返回人话而非报错: %v", err)
	}
	if got2 == addTool.result || got2 == "" {
		t.Fatalf("重复执行应返回幂等提示文本, 实得 %q", got2)
	}
	if len(addTool.calls) != 1 {
		t.Fatalf("幂等失守：工具被执行 %d 次", len(addTool.calls))
	}

	// 过期动作：不可领取，同样人话 + nil error。
	got3, err := l.ExecuteAction(context.Background(), 7, "act-expired")
	if err != nil || got3 == "" || len(addTool.calls) != 1 {
		t.Fatalf("过期动作应拒绝执行且不报错, 实得 got=%q err=%v calls=%d", got3, err, len(addTool.calls))
	}
}

// 补充（契约 §10 红线）：ExecuteAction 校验动作归属，非本人一律拒绝执行。
func TestExecuteAction_RejectsForeignUser(t *testing.T) {
	fs := newFakeStore()
	addTool := &fakeTool{name: "add_source", mutating: true, result: "不应执行"}
	fs.actions["act-1"] = &types.PendingAction{
		ID:        "act-1",
		UserID:    7,
		ToolName:  "add_source",
		Args:      json.RawMessage("{}"),
		Status:    types.PendingActionStatusPending,
		ExpiresAt: time.Now().Add(time.Hour),
	}
	l := newTestLoop(t, fs, (&scriptedChat{}).fn, addTool)

	got, err := l.ExecuteAction(context.Background(), 8, "act-1")
	if err != nil {
		t.Fatalf("越权应返回人话拒绝而非报错: %v", err)
	}
	if got == addTool.result || len(addTool.calls) != 0 {
		t.Fatalf("越权用户绝不能触发执行, 实得 got=%q calls=%d", got, len(addTool.calls))
	}
}

// 补充：CancelAction 取消 pending 动作后，确认按钮不可再领取（互斥闭环）。
func TestCancelAction_ThenExecuteRejected(t *testing.T) {
	fs := newFakeStore()
	addTool := &fakeTool{name: "add_source", mutating: true, result: "不应执行"}
	fs.actions["act-1"] = &types.PendingAction{
		ID:        "act-1",
		UserID:    7,
		ToolName:  "add_source",
		Args:      json.RawMessage("{}"),
		Status:    types.PendingActionStatusPending,
		ExpiresAt: time.Now().Add(time.Hour),
	}
	l := newTestLoop(t, fs, (&scriptedChat{}).fn, addTool)

	got, err := l.CancelAction(context.Background(), 7, "act-1")
	if err != nil || got == "" {
		t.Fatalf("取消 pending 动作应成功, 实得 got=%q err=%v", got, err)
	}
	// 已取消后再确认：幂等出口，人话 + nil error，不执行。
	got2, err := l.ExecuteAction(context.Background(), 7, "act-1")
	if err != nil || len(addTool.calls) != 0 {
		t.Fatalf("已取消动作不可再执行, 实得 got=%q err=%v calls=%d", got2, err, len(addTool.calls))
	}
	// 重复取消：同样人话 + nil error。
	got3, err := l.CancelAction(context.Background(), 7, "act-1")
	if err != nil || got3 == "" {
		t.Fatalf("重复取消应返回人话而非报错, 实得 got=%q err=%v", got3, err)
	}
}

// ============================================================
// 卡片回调结果回写会话
// ============================================================

// appendedMessages 解出第 i 次 AppendAgentSessionMessages 收到的消息数组。
func appendedMessages(t *testing.T, fs *fakeStore, i int) []llm.ChatMessage {
	t.Helper()
	fs.mu.Lock()
	rec := fs.appendCalls[i]
	fs.mu.Unlock()
	var msgs []llm.ChatMessage
	if err := json.Unmarshal(rec.msgs, &msgs); err != nil {
		t.Fatalf("append 的 msgs 不是合法 JSON 数组: %v", err)
	}
	return msgs
}

// appendCallAt 锁内取第 i 次回写记录的快照。
func appendCallAt(fs *fakeStore, i int) appendRecord {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.appendCalls[i]
}

// newPendingAction 造一个 pending 状态的测试动作（1h 后过期）。
func newPendingAction(id string, userID int64, sessionID *int64, tool, summary string) *types.PendingAction {
	return &types.PendingAction{
		ID:        id,
		UserID:    userID,
		SessionID: sessionID,
		ToolName:  tool,
		Args:      json.RawMessage("{}"),
		Summary:   summary,
		Status:    types.PendingActionStatusPending,
		ExpiresAt: time.Now().Add(time.Hour),
	}
}

// 确认执行成功后要向来源会话回写「[卡片回调]」user 消息（含 Summary 与执行结果），
// 且消息与 HandleMessage 持久化的历史同构（decodeMessages 可无损解析）；
// 双击重放走幂等出口，不再回写。
func TestExecuteAction_AppendsCardCallback(t *testing.T) {
	fs := newFakeStore()
	sess, _ := fs.CreateAgentSession(context.Background(), 7)
	addTool := &fakeTool{name: "add_source", mutating: true, result: "已添加信源：示例"}
	fs.actions["act-1"] = newPendingAction("act-1", 7, &sess.ID, "add_source", "新增 RSS 信源 example.com")
	l := newTestLoop(t, fs, (&scriptedChat{}).fn, addTool)

	if _, err := l.ExecuteAction(context.Background(), 7, "act-1"); err != nil {
		t.Fatalf("ExecuteAction 意外报错: %v", err)
	}
	waitAppends(t, fs, 1)
	if rec := appendCallAt(fs, 0); rec.sessionID != sess.ID {
		t.Fatalf("应向会话 %d 回写, 实得 %+v", sess.ID, rec)
	}
	msgs := appendedMessages(t, fs, 0)
	if len(msgs) != 1 || msgs[0].Role != "user" {
		t.Fatalf("回写应为 1 条 role=user 消息, 实得 %+v", msgs)
	}
	c := msgs[0].Content
	if !strings.HasPrefix(c, "[卡片回调]") || !strings.Contains(c, "「确认」") ||
		!strings.Contains(c, "新增 RSS 信源 example.com") || !strings.Contains(c, addTool.result) {
		t.Fatalf("回写内容应含前缀/确认/Summary/执行结果, 实得 %q", c)
	}
	// 回写后的会话 messages 整体仍可按 []llm.ChatMessage 解析（与历史同构）。
	if got := decodeMessages(fs.sessionMessages(sess.ID)); len(got) != 1 || got[0].Content != c {
		t.Fatalf("会话内容应含回写消息且可解析, 实得 %+v", got)
	}

	// 双击重放（幂等出口）：人话文本，不再回写。
	if _, err := l.ExecuteAction(context.Background(), 7, "act-1"); err != nil {
		t.Fatalf("重复 ExecuteAction 不应报错: %v", err)
	}
	waitAppends(t, fs, 1)
}

// 执行失败与工具下线同样回写通告（模型该知道动作已被消耗）；返回语义不变：
// 失败照旧上抛 err，下线返回人话 + nil error。
func TestExecuteAction_AppendsOnFailureAndOfflineTool(t *testing.T) {
	fs := newFakeStore()
	sess, _ := fs.CreateAgentSession(context.Background(), 7)
	// Cause 里的连接串细节不得进入模型上下文，回写只落 AppError.Message。
	failTool := &fakeTool{name: "add_source", mutating: true,
		execErr: types.NewAppError(types.CodeDatabase, "上游超时",
			fmt.Errorf("dial tcp 10.0.0.1:5432: password=secret"))}
	fs.actions["act-fail"] = newPendingAction("act-fail", 7, &sess.ID, "add_source", "摘要-fail")
	fs.actions["act-ghost"] = newPendingAction("act-ghost", 7, &sess.ID, "ghost_tool", "摘要-ghost")
	l := newTestLoop(t, fs, (&scriptedChat{}).fn, failTool)

	if _, err := l.ExecuteAction(context.Background(), 7, "act-fail"); err == nil {
		t.Fatal("工具执行失败应照旧上抛 err")
	}
	waitAppends(t, fs, 1)
	c := appendedMessages(t, fs, 0)[0].Content
	if !strings.HasPrefix(c, "[卡片回调]") ||
		!strings.Contains(c, "执行失败") || !strings.Contains(c, "上游超时") {
		t.Fatalf("失败通告应含前缀与错误信息, 实得 %q", c)
	}
	if strings.Contains(c, "dial tcp") || strings.Contains(c, "secret") {
		t.Fatalf("失败通告不得泄漏底层错误链, 实得 %q", c)
	}

	reply, err := l.ExecuteAction(context.Background(), 7, "act-ghost")
	if err != nil || !strings.Contains(reply, "已不可用") {
		t.Fatalf("下线工具应返回人话 + nil error, 实得 reply=%q err=%v", reply, err)
	}
	waitAppends(t, fs, 2)
	if c := appendedMessages(t, fs, 1)[0].Content; !strings.Contains(c, "ghost_tool 已不可用") {
		t.Fatalf("下线通告应含工具名与不可用文案, 实得 %q", c)
	}
}

// 取消成功后回写「取消」通告（含 Summary）；重复取消走幂等出口，不回写。
func TestCancelAction_AppendsCardCallback(t *testing.T) {
	fs := newFakeStore()
	sess, _ := fs.CreateAgentSession(context.Background(), 7)
	fs.actions["act-1"] = newPendingAction("act-1", 7, &sess.ID, "add_source", "新增 RSS 信源 example.com")
	l := newTestLoop(t, fs, (&scriptedChat{}).fn)

	if _, err := l.CancelAction(context.Background(), 7, "act-1"); err != nil {
		t.Fatalf("CancelAction 意外报错: %v", err)
	}
	waitAppends(t, fs, 1)
	if rec := appendCallAt(fs, 0); rec.sessionID != sess.ID {
		t.Fatalf("取消应向会话 %d 回写, 实得 %+v", sess.ID, rec)
	}
	msgs := appendedMessages(t, fs, 0)
	if msgs[0].Role != "user" {
		t.Fatalf("回写应为 role=user, 实得 %+v", msgs[0])
	}
	c := msgs[0].Content
	if !strings.HasPrefix(c, "[卡片回调]") || !strings.Contains(c, "「取消」") ||
		!strings.Contains(c, "新增 RSS 信源 example.com") {
		t.Fatalf("取消通告应含前缀/取消/Summary, 实得 %q", c)
	}

	// 重复取消（幂等出口）：不回写。
	if _, err := l.CancelAction(context.Background(), 7, "act-1"); err != nil {
		t.Fatalf("重复取消不应报错: %v", err)
	}
	waitAppends(t, fs, 1)
}

// SessionID 为 nil（动作无来源会话）时确认/取消都跳过回写，其余行为不变。
func TestCardCallback_NilSessionSkipsAppend(t *testing.T) {
	fs := newFakeStore()
	addTool := &fakeTool{name: "add_source", mutating: true, result: "已添加"}
	fs.actions["act-exec"] = newPendingAction("act-exec", 7, nil, "add_source", "摘要-exec")
	fs.actions["act-cancel"] = newPendingAction("act-cancel", 7, nil, "add_source", "摘要-cancel")
	l := newTestLoop(t, fs, (&scriptedChat{}).fn, addTool)

	got, err := l.ExecuteAction(context.Background(), 7, "act-exec")
	if err != nil || got != addTool.result {
		t.Fatalf("无会话动作应照常执行, 实得 got=%q err=%v", got, err)
	}
	if _, err := l.CancelAction(context.Background(), 7, "act-cancel"); err != nil {
		t.Fatalf("无会话动作应照常取消: %v", err)
	}
	// nil session 路径根本不 spawn 回写 goroutine，稍等一拍后计数必须仍为 0。
	waitAppends(t, fs, 0)
}

// 工具返回裸 error（非 AppError）时回写用通用文案兜底——原始错误文本
// 一个字都不能进模型上下文。
func TestExecuteAction_PlainErrorFallsBackToGenericNotice(t *testing.T) {
	fs := newFakeStore()
	sess, _ := fs.CreateAgentSession(context.Background(), 7)
	failTool := &fakeTool{name: "add_source", mutating: true,
		execErr: fmt.Errorf("dial tcp 10.0.0.1:5432: password=hunter2")}
	fs.actions["act-1"] = newPendingAction("act-1", 7, &sess.ID, "add_source", "摘要")
	l := newTestLoop(t, fs, (&scriptedChat{}).fn, failTool)

	if _, err := l.ExecuteAction(context.Background(), 7, "act-1"); err == nil {
		t.Fatal("失败应照旧上抛 err")
	}
	waitAppends(t, fs, 1)
	c := appendedMessages(t, fs, 0)[0].Content
	if !strings.Contains(c, "内部错误") {
		t.Fatalf("裸 error 应回退通用文案, 实得 %q", c)
	}
	if strings.Contains(c, "dial tcp") || strings.Contains(c, "hunter2") {
		t.Fatalf("裸 error 原文不得进入通告, 实得 %q", c)
	}
}

// 互锁与 ctx 语义：HandleMessage 持 userMu 期间点卡——回写必须排在 saveSession
// 之后落地（不被全量覆盖写吞掉），且不受调用方已取消 ctx 的影响（拿锁后另起
// 独立 ctx；fakeStore.Append 对齐 pgx、见 ctx 已死立即拒绝）。
func TestAppendCardCallback_SerializesWithHandleMessage(t *testing.T) {
	fs := newFakeStore()
	sess, _ := fs.CreateAgentSession(context.Background(), 7)
	addTool := &fakeTool{name: "add_source", mutating: true, result: "已添加"}
	fs.actions["act-1"] = newPendingAction("act-1", 7, &sess.ID, "add_source", "摘要")

	entered := make(chan struct{})
	release := make(chan struct{})
	blockingChat := func(_ context.Context, _ llm.ChatRequest) (*llm.ChatResponse, error) {
		close(entered)
		<-release
		return &llm.ChatResponse{Content: "好的", FinishReason: "stop"}, nil
	}
	l := newTestLoop(t, fs, blockingChat, addTool)

	handleDone := make(chan struct{})
	go func() {
		defer close(handleDone)
		_, _ = l.HandleMessage(context.Background(), 7, "你好")
	}()
	<-entered // HandleMessage 已持锁、正卡在模型调用上

	// 调用方 ctx 已取消（模拟 30s 回调预算在等锁中耗尽）。
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := l.ExecuteAction(ctx, 7, "act-1"); err != nil {
		t.Fatalf("ExecuteAction 意外报错: %v", err)
	}
	// 锁仍被 HandleMessage 持有，回写不可能先落地。
	time.Sleep(30 * time.Millisecond)
	if got := fs.appendCount(); got != 0 {
		t.Fatalf("锁被持有期间不应有回写落地, 实得 %d", got)
	}

	close(release)
	<-handleDone
	waitAppends(t, fs, 1)

	// 会话 = saveSession 的完整历史 + 排在其后的回调通告。
	got := decodeMessages(fs.sessionMessages(sess.ID))
	if len(got) < 2 {
		t.Fatalf("会话应含 HandleMessage 历史与回调通告, 实得 %+v", got)
	}
	if last := got[len(got)-1]; !strings.HasPrefix(last.Content, "[卡片回调]") {
		t.Fatalf("通告应排在 saveSession 之后落地, 实得末条 %+v", last)
	}
	if prev := got[len(got)-2]; prev.Role != "assistant" || prev.Content != "好的" {
		t.Fatalf("倒数第二条应为模型回复, 实得 %+v", prev)
	}
}
