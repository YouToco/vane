package a2a

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"

	"github.com/YouToco/vane/types"
)

// collect 消费 Execute/Cancel 的事件迭代器。
func collect(t *testing.T, seq func(yield func(a2a.Event, error) bool)) []a2a.Event {
	t.Helper()
	var events []a2a.Event
	seq(func(ev a2a.Event, err error) bool {
		if err != nil {
			t.Fatalf("executor 不应 yield error（错误应转 FAILED 状态事件），得到: %v", err)
		}
		events = append(events, ev)
		return true
	})
	return events
}

func newExecCtx(msg *a2a.Message) *a2asrv.ExecutorContext {
	return &a2asrv.ExecutorContext{
		Message:   msg,
		TaskID:    a2a.NewTaskID(),
		ContextID: "ctx-test",
	}
}

// lastStatus 取事件序列的最后一个状态更新。
func lastStatus(t *testing.T, events []a2a.Event) *a2a.TaskStatusUpdateEvent {
	t.Helper()
	for i := len(events) - 1; i >= 0; i-- {
		if ev, ok := events[i].(*a2a.TaskStatusUpdateEvent); ok {
			return ev
		}
	}
	t.Fatal("事件序列里没有状态更新事件")
	return nil
}

func textMsg(text string) *a2a.Message {
	return a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart(text))
}

func TestExecuteCompleted(t *testing.T) {
	pub := time.Date(2026, 7, 16, 8, 0, 0, 0, time.UTC)
	content := &fakeContent{items: []types.ContentItem{
		{Title: "Claude 新模型发布", URL: "https://example.com/a", Content: "正文A", PublishedAt: &pub},
		{Title: "", URL: "https://example.com/b", Content: "无标题正文头应该出现在列表里", FetchedAt: pub},
	}}
	ex := newExecutor(Deps{Content: content})
	execCtx := newExecCtx(textMsg(`{"keyword":"Claude","days":7,"limit":5}`))

	events := collect(t, ex.Execute(t.Context(), execCtx))

	// 状态序列：SUBMITTED（Task 事件）→ WORKING → Artifact → COMPLETED。
	if _, ok := events[0].(*a2a.Task); !ok {
		t.Fatalf("首个事件应为 SubmittedTask，实际 %T", events[0])
	}
	if got := lastStatus(t, events).Status.State; got != a2a.TaskStateCompleted {
		t.Fatalf("终态应 COMPLETED，实际 %s", got)
	}
	var artifact *a2a.TaskArtifactUpdateEvent
	for _, ev := range events {
		if a, ok := ev.(*a2a.TaskArtifactUpdateEvent); ok {
			artifact = a
		}
	}
	if artifact == nil || len(artifact.Artifact.Parts) != 2 {
		t.Fatalf("应有一个含 text+data 两 part 的 Artifact，实际 %+v", artifact)
	}
	text := artifact.Artifact.Parts[0].Text()
	if !strings.Contains(text, "Claude 新模型发布") || !strings.Contains(text, "https://example.com/a") {
		t.Errorf("人读列表应含标题与链接，实际:\n%s", text)
	}
	// 空标题回退正文头（Gate ⑥ 教训前置吸收）。
	if !strings.Contains(text, "无标题正文头") {
		t.Errorf("空标题条目应回退正文头，实际:\n%s", text)
	}
	if artifact.Artifact.Parts[1].Data() == nil {
		t.Error("data part 应为结构化列表")
	}
	// 入参钳制与传参。
	if content.gotKeyword != "Claude" || content.gotLimit != 5 {
		t.Errorf("检索参数传递不符: keyword=%q limit=%d", content.gotKeyword, content.gotLimit)
	}
}

// TestExecuteParams 入参解析表驱动（契约 §5.4）。
func TestExecuteParams(t *testing.T) {
	cases := []struct {
		name        string
		text        string
		wantKeyword string
		wantLimit   int
		wantDaysMin float64 // since 距 now 的天数下界（校验 days 钳制）
	}{
		{"纯文本整段作keyword", "Anthropic 最新动态", "Anthropic 最新动态", defaultLimit, defaultDays},
		{"JSON缺省days与limit", `{"keyword":"GPT"}`, "GPT", defaultLimit, defaultDays},
		{"days与limit上钳", `{"keyword":"x","days":999,"limit":999}`, "x", maxLimit, maxDays},
		{"days与limit下钳", `{"keyword":"x","days":0,"limit":-3}`, "x", minLimit, minDays},
		{"非法JSON整段作keyword", `{broken json`, "{broken json", defaultLimit, defaultDays},
		{"显式skill等于content.query", `{"skill":"content.query","keyword":"ok"}`, "ok", defaultLimit, defaultDays},
		{"空keyword纯时间窗", `{"days":2}`, "", defaultLimit, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			content := &fakeContent{}
			ex := newExecutor(Deps{Content: content})
			events := collect(t, ex.Execute(t.Context(), newExecCtx(textMsg(tc.text))))
			if got := lastStatus(t, events).Status.State; got != a2a.TaskStateCompleted {
				t.Fatalf("应 COMPLETED，实际 %s", got)
			}
			if content.gotKeyword != tc.wantKeyword {
				t.Errorf("keyword=%q，期望 %q", content.gotKeyword, tc.wantKeyword)
			}
			if content.gotLimit != tc.wantLimit {
				t.Errorf("limit=%d，期望 %d", content.gotLimit, tc.wantLimit)
			}
			gotDays := time.Since(content.gotSince).Hours() / 24
			if gotDays < tc.wantDaysMin-0.1 || gotDays > tc.wantDaysMin+0.1 {
				t.Errorf("since 距今 %.2f 天，期望约 %v 天", gotDays, tc.wantDaysMin)
			}
		})
	}
}

// TestExecuteRejected REJECTED 仅两种触发（契约 §5.4/§5.5），各一用例。
func TestExecuteRejected(t *testing.T) {
	cases := []struct {
		name string
		msg  *a2a.Message
	}{
		{"显式skill非content.query", textMsg(`{"skill":"assistant.chat","keyword":"hi"}`)},
		{"消息无text part", a2a.NewMessage(a2a.MessageRoleUser, a2a.NewDataPart(map[string]any{"k": "v"}))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			content := &fakeContent{}
			ex := newExecutor(Deps{Content: content})
			events := collect(t, ex.Execute(t.Context(), newExecCtx(tc.msg)))
			st := lastStatus(t, events)
			if st.Status.State != a2a.TaskStateRejected {
				t.Fatalf("应 REJECTED，实际 %s", st.Status.State)
			}
			if st.Status.Message == nil || st.Status.Message.Parts[0].Text() == "" {
				t.Error("REJECTED 应带人话说明消息")
			}
			if content.calls != 0 {
				t.Error("REJECTED 前置判定不应进入检索执行")
			}
		})
	}
}

// TestExecuteFailedSanitized 错误卫生突变测试（契约 §9.1，红线 §8.1）：
// 注入含原始错误链的错误，断言 Execute 产出的**全部事件**序列化后逐字不含该串。
func TestExecuteFailedSanitized(t *testing.T) {
	const leak = "pgx: connection refused"
	content := &fakeContent{err: errors.New(leak)}
	ex := newExecutor(Deps{Content: content})
	events := collect(t, ex.Execute(t.Context(), newExecCtx(textMsg("q"))))

	st := lastStatus(t, events)
	if st.Status.State != a2a.TaskStateFailed {
		t.Fatalf("应 FAILED，实际 %s", st.Status.State)
	}
	for i, ev := range events {
		raw, err := json.Marshal(ev)
		if err != nil {
			t.Fatalf("事件 %d 序列化失败: %v", i, err)
		}
		if strings.Contains(string(raw), leak) {
			t.Fatalf("事件 %d 泄露原始错误链: %s", i, raw)
		}
	}
	if st.Status.Message.Parts[0].Text() != internalErrorText {
		t.Errorf("非 AppError 应翻译为固定文案，实际 %q", st.Status.Message.Parts[0].Text())
	}
	// AppError 的 Message 可对外（api.writeAppError 先例）。
	content.err = types.NewAppError(types.CodeDatabase, "检索内容条目失败", errors.New(leak))
	events = collect(t, ex.Execute(t.Context(), newExecCtx(textMsg("q"))))
	st = lastStatus(t, events)
	if got := st.Status.Message.Parts[0].Text(); got != "检索内容条目失败" {
		t.Errorf("AppError 应译为其 Message，实际 %q", got)
	}
	for _, ev := range events {
		raw, _ := json.Marshal(ev)
		if strings.Contains(string(raw), leak) {
			t.Fatalf("AppError 路径泄露 Cause 链: %s", raw)
		}
	}
}

func TestCancelYieldsCanceled(t *testing.T) {
	ex := newExecutor(Deps{})
	events := collect(t, ex.Cancel(t.Context(), newExecCtx(nil)))
	if got := lastStatus(t, events).Status.State; got != a2a.TaskStateCanceled {
		t.Fatalf("Cancel 应 yield CANCELED，实际 %s", got)
	}
}
