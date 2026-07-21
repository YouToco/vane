package agent

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/YouToco/vane/llm"
)

// newRunOnceLoop 构造 A2A 轨形态的 Loop：Store/Profiles 均 nil（RunOnce 不碰会话
// 与画像——nil 保证任何存储触碰都会 panic 而不是静默通过）、自定义 SystemPrompt。
func newRunOnceLoop(t *testing.T, chat func(context.Context, llm.ChatRequest) (*llm.ChatResponse, error), tools ...Tool) *Loop {
	t.Helper()
	l := New(Deps{
		Tools:        tools,
		Model:        "deepseek-v4-pro",
		MaxTurns:     5,
		SessionTTL:   30 * time.Minute,
		SystemPrompt: "你是 A2A 对外助理。",
	})
	l.chatFn = chat
	return l
}

// TestRunOnce_不碰会话存储 M4 契约 §7.1：RunOnce 在给定历史上执行，Store 为 nil
// 也全程无恙（任何 load/save/pending 触碰都会 nil panic——结构性证明而非计数器）。
// 历史与本轮交换齐全地出现在返回值里，由调用方自行留存。
func TestRunOnce_不碰会话存储(t *testing.T) {
	chat := &scriptedChat{responses: []*llm.ChatResponse{{Content: "好的", FinishReason: "stop"}}}
	l := newRunOnceLoop(t, chat.fn)

	history := []llm.ChatMessage{
		{Role: "user", Content: "上一问"},
		{Role: "assistant", Content: "上一答"},
	}
	outcome, msgs, err := l.RunOnce(context.Background(), 7, history, "这一问")
	if err != nil {
		t.Fatalf("RunOnce 失败: %v", err)
	}
	if outcome.Reply != "好的" || outcome.Confirm != nil {
		t.Fatalf("outcome 不符: %+v", outcome)
	}
	// 返回历史 = 输入历史 + 本轮 user + 本轮 assistant。
	want := []string{"上一问", "上一答", "这一问", "好的"}
	if len(msgs) != len(want) {
		t.Fatalf("返回历史长度 = %d, 期望 %d: %+v", len(msgs), len(want), msgs)
	}
	for i, w := range want {
		if msgs[i].Content != w {
			t.Errorf("msgs[%d].Content = %q, 期望 %q", i, msgs[i].Content, w)
		}
	}
	// 请求侧：system 是自定义 prompt 且不含 [用户画像] 段（A2A 轨画像非目标）。
	sys := chat.requests[0].Messages[0]
	if sys.Role != "system" || sys.Content != "你是 A2A 对外助理。" {
		t.Errorf("system 应为自定义 prompt 原文（无画像段），实得 %+v", sys)
	}
	if strings.Contains(sys.Content, "[用户画像]") {
		t.Error("自定义 SystemPrompt 实例不得渲染 [用户画像] 段")
	}
}

// TestRunOnce_只读工具执行 读工具正常执行并回填，多轮收敛；Store 仍为 nil。
func TestRunOnce_只读工具执行(t *testing.T) {
	tool := &fakeTool{name: "list_sources", result: "3 个信源"}
	chat := &scriptedChat{responses: []*llm.ChatResponse{
		{ToolCalls: []llm.ToolCall{{ID: "c1", Name: "list_sources", Arguments: "{}"}}},
		{Content: "你订了 3 个信源", FinishReason: "stop"},
	}}
	l := newRunOnceLoop(t, chat.fn, tool)

	outcome, _, err := l.RunOnce(context.Background(), 7, nil, "我订了哪些信源？")
	if err != nil {
		t.Fatalf("RunOnce 失败: %v", err)
	}
	if outcome.Reply != "你订了 3 个信源" {
		t.Errorf("Reply = %q", outcome.Reply)
	}
	if len(tool.calls) != 1 || tool.calls[0].userID != 7 {
		t.Errorf("工具应以 userID=7 执行一次，实得 %+v", tool.calls)
	}
}

// TestRunOnce_未注册写工具自纠 契约钉死项：A2A 轨实例只注册只读工具，模型报出
// 写工具名（如 add_source）走"工具不存在"自纠——不执行、不建 pending_action
// （Store=nil，任何 CreatePendingAction 都会 panic，结构性证明）。
func TestRunOnce_未注册写工具自纠(t *testing.T) {
	chat := &scriptedChat{responses: []*llm.ChatResponse{
		{ToolCalls: []llm.ToolCall{{ID: "c1", Name: "add_source", Arguments: `{"url":"x"}`}}},
		{Content: "A2A 通道只读，无法添加信源", FinishReason: "stop"},
	}}
	l := newRunOnceLoop(t, chat.fn /* 无任何工具 */)

	outcome, msgs, err := l.RunOnce(context.Background(), 7, nil, "帮我加个信源")
	if err != nil {
		t.Fatalf("RunOnce 失败: %v", err)
	}
	if outcome.Confirm != nil {
		t.Fatal("A2A 轨绝不产生确认卡")
	}
	if outcome.Reply != "A2A 通道只读，无法添加信源" {
		t.Errorf("Reply = %q", outcome.Reply)
	}
	// 自纠回执在历史里（role=tool，"不存在"文案）。
	var sawToolErr bool
	for _, m := range msgs {
		if m.Role == "tool" && strings.Contains(m.Content, "不存在") {
			sawToolErr = true
		}
	}
	if !sawToolErr {
		t.Errorf("历史应含工具不存在的自纠回执: %+v", msgs)
	}
}

// TestRunOnce_写工具装配防御 万一实例被错误装配进写工具（正常装配不可达）：
// 必须在写 pending/v1 proposal 前报错——外部 agent 没有确认卡通道，先落库再拒绝
// 会制造无人能处理的悬空动作。
func TestRunOnce_写工具装配防御(t *testing.T) {
	fs := newFakeStore()
	tool := &fakeTool{name: "add_source", mutating: true}
	chat := &scriptedChat{responses: []*llm.ChatResponse{
		{ToolCalls: []llm.ToolCall{{ID: "c1", Name: "add_source", Arguments: `{}`}}},
		{Content: "已生成确认卡", FinishReason: "stop"},
	}}
	l := New(Deps{
		Store: fs, Tools: []Tool{tool},
		Model: "deepseek-v4-pro", MaxTurns: 5, SessionTTL: 30 * time.Minute,
		SystemPrompt: "你是 A2A 对外助理。",
	})
	l.chatFn = chat.fn

	_, _, err := l.RunOnce(context.Background(), 7, nil, "加个信源")
	if err == nil || !strings.Contains(err.Error(), "只读") {
		t.Fatalf("写工具装配必须报错（含'只读'），实得 %v", err)
	}
	if len(fs.actions) != 0 || len(tool.calls) != 0 {
		t.Fatalf("只读防御必须发生在任何写副作用前: actions=%d execute=%d",
			len(fs.actions), len(tool.calls))
	}
}

// TestNew_SystemPrompt 回落语义：零值 → 默认飞书 prompt（含画像段两态），
// 非零 → 原文生效。默认轨行为已被 M5 §12.2 的画像注入用例钉死，这里只验非零覆盖。
func TestNew_SystemPrompt(t *testing.T) {
	l := New(Deps{Model: "m", MaxTurns: 1, SessionTTL: time.Minute})
	if l.sys != systemPrompt || !l.renderProfile {
		t.Error("零值 SystemPrompt 应回落默认常量且 renderProfile=true")
	}
	l2 := New(Deps{Model: "m", MaxTurns: 1, SessionTTL: time.Minute, SystemPrompt: "自定义"})
	if l2.sys != "自定义" || l2.renderProfile {
		t.Error("非零 SystemPrompt 应原文生效且 renderProfile=false")
	}
}
