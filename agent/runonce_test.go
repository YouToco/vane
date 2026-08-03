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
		Tools:        testToolSpecs(tools...),
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

// 返回历史 = 输入历史 + 本轮 user + 本轮 assistant。

// 请求侧：system 是自定义 prompt 且不含 [用户画像] 段（A2A 轨画像非目标）。

// TestRunOnce_只读工具执行 读工具正常执行并回填，多轮收敛；Store 仍为 nil。
func TestRunOnce_只读工具执行(t *testing.T) {
	tool := &fakeTool{name: "view_profile", result: "用户画像摘要"}
	chat := &scriptedChat{responses: []*llm.ChatResponse{
		{ToolCalls: []llm.ToolCall{{ID: "c1", Name: "view_profile", Arguments: "{}"}}},
		{Content: "已读取你的画像摘要", FinishReason: "stop"},
	}}
	l := newRunOnceLoop(t, chat.fn, tool)

	outcome, _, err := l.RunOnce(context.Background(), 7, nil, "我的画像摘要是什么？")
	if err != nil {
		t.Fatalf("RunOnce 失败: %v", err)
	}
	if outcome.Reply != "已读取你的画像摘要" {
		t.Errorf("Reply = %q", outcome.Reply)
	}
	if len(tool.calls) != 1 || tool.calls[0].userID != 7 {
		t.Errorf("工具应以 userID=7 执行一次，实得 %+v", tool.calls)
	}
}

func TestRunOnce_外部只读结果使用无协议续写(t *testing.T) {
	tool := &fakeTool{
		name:      "read_page",
		untrusted: true,
		result:    "外部标题来源",
	}
	call := 0
	chat := func(_ context.Context, req llm.ChatRequest) (*llm.ChatResponse, error) {
		call++
		if call == 1 {
			return &llm.ChatResponse{ToolCalls: []llm.ToolCall{{
				ID: "c1", Name: "read_page", Arguments: `{"url":"https://example.com"}`,
			}}}, nil
		}
		if len(req.Tools) != 0 || len(req.Messages) != 2 ||
			req.Messages[0].Role != "system" || req.Messages[1].Role != "user" {
			t.Fatalf("A2A taint 续写应为零工具 system+user，实得 %+v", req)
		}
		for _, msg := range req.Messages {
			if msg.Role == "tool" || len(msg.ToolCalls) > 0 {
				t.Fatalf("A2A taint 续写不得带原生工具协议: %+v", req.Messages)
			}
		}
		if !strings.Contains(req.Messages[1].Content, untrustedContinuationPrefix) {
			t.Fatalf("A2A taint 续写缺少不可信数据封装: %+v", req.Messages[1])
		}
		return &llm.ChatResponse{Content: "已总结外部页面", FinishReason: "stop"}, nil
	}
	l := newRunOnceLoop(t, chat, tool)

	outcome, history, err := l.RunOnce(context.Background(), 7, nil, "总结这个页面")
	if err != nil {
		t.Fatalf("RunOnce 失败: %v", err)
	}
	if outcome.Reply != "已总结外部页面" || call != 2 {
		t.Fatalf("A2A 外部结果未可靠收敛: outcome=%+v calls=%d", outcome, call)
	}
	if len(history) != 2 || history[0].Role != "user" ||
		history[1].Content != untrustedHistoryPlaceholder {
		t.Fatalf("A2A 外部结果历史仍应压平: %+v", history)
	}
}

// TestRunOnce_未注册写工具自纠 契约钉死项：A2A 轨实例只注册只读工具，模型报出
// 任意未注册写工具时走"工具不存在"自纠，绝不执行或落耐久写操作。

/* 无任何工具 */

// 自纠回执在历史里（role=tool，"不存在"文案）。

// TestRunOnce_写工具装配防御 万一实例被错误装配进写工具（正常装配不可达），
// 必须在任何耐久写入前报错，避免外部只读轨产生无人处理的操作。

// TestNew_SystemPrompt 回落语义：零值 → 默认飞书 prompt（含画像段两态），
// 非零 → 原文生效。默认轨行为已被 M5 §12.2 的画像注入用例钉死，这里只验非零覆盖。
func TestNew_SystemPrompt(t *testing.T) {
	l := New(Deps{Model: "m", MaxTurns: 1, SessionTTL: time.Minute})
	if l.sys != systemPrompt || l.renderProfile {
		t.Error("零值 SystemPrompt 应回落 Agent-first 常量且不直接注入画像")
	}
	l2 := New(Deps{Model: "m", MaxTurns: 1, SessionTTL: time.Minute, SystemPrompt: "自定义"})
	if l2.sys != "自定义" || l2.renderProfile {
		t.Error("非零 SystemPrompt 应原文生效且 renderProfile=false")
	}
}
