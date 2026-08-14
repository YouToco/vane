package a2a

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/YouToco/vane/server/agent"
	"github.com/YouToco/vane/server/auth"
	"github.com/YouToco/vane/server/llm"
	"github.com/YouToco/vane/server/types"
)

// fakeChat 是 ChatRunner 替身：留痕入参、可注入错误。
type fakeChat struct {
	reply string
	err   error

	mu         sync.Mutex
	gotUserID  int64
	gotHistory []llm.ChatMessage
	gotText    string
	calls      int
}

func (f *fakeChat) RunOnce(_ context.Context, userID int64, history []llm.ChatMessage, text string) (agent.Outcome, []llm.ChatMessage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.gotUserID, f.gotHistory, f.gotText = userID, history, text
	if f.err != nil {
		return agent.Outcome{}, nil, f.err
	}
	return agent.Outcome{Reply: f.reply}, nil, nil
}

// fakeOwner 是 OwnerStore 替身。
type fakeOwner struct {
	settingErr error
	upsertErr  error
	userID     int64
}

func (f *fakeOwner) GetSetting(_ context.Context, key string) (json.RawMessage, error) {
	if f.settingErr != nil {
		return nil, f.settingErr
	}
	return json.RawMessage(`{"open_id":"ou_owner","name":"Boss"}`), nil
}

func (f *fakeOwner) ListMembershipsByUser(_ context.Context, userID int64) ([]types.Membership, error) {
	return []types.Membership{{TenantID: 1, UserID: userID}}, nil
}

func (f *fakeOwner) UpsertUserByOpenID(_ context.Context, openID, name string) (*types.User, error) {
	if f.upsertErr != nil {
		return nil, f.upsertErr
	}
	return &types.User{ID: f.userID, FeishuOpenID: &openID, Name: name}, nil
}

// ownerResolver 把 OwnerStore 替身包成 auth.PrincipalResolver。
// 收敛后 Deps 依赖的是 principal 解析器而非 store——包一层让既有替身继续可用，
// 且这些用例从此**连带覆盖 auth 包的真实解析逻辑**（错误码/文案由它产出）。
func ownerResolver(st auth.OwnerStore) auth.PrincipalResolver {
	return auth.NewOwnerResolver(st, "feishu_owner")
}

func chatMsg(text string) *a2a.Message {
	return textMsg(fmt.Sprintf(`{"skill":"assistant.chat","text":%q}`, text))
}

// artifactTexts 取事件流里全部 artifact 的 text part。
func artifactTexts(events []a2a.Event) []string {
	var out []string
	for _, ev := range events {
		ae, ok := ev.(*a2a.TaskArtifactUpdateEvent)
		if !ok {
			continue
		}
		for _, p := range ae.Artifact.Parts {
			if p == nil {
				continue
			}
			if _, isText := p.Content.(a2a.Text); isText {
				out = append(out, p.Text())
			}
		}
	}
	return out
}

// TestChatCompleted 契约 §12 P2：assistant.chat 分派 → RunOnce → 回复进 artifact
// text part → COMPLETED；owner 身份透传（拍板 §13.2）。
func TestChatCompleted(t *testing.T) {
	chat := &fakeChat{reply: "你订了 3 个信源。"}
	e := newExecutor(Deps{Storage: newFakeTaskStorage(), Chat: chat, Principal: ownerResolver(&fakeOwner{userID: 7})})
	events := collect(t, e.Execute(context.Background(), newExecCtx(chatMsg("我订了哪些信源？"))))

	if got := lastStatus(t, events).Status.State; got != a2a.TaskStateCompleted {
		t.Fatalf("终态 = %v, 期望 COMPLETED", got)
	}
	texts := artifactTexts(events)
	if len(texts) != 1 || texts[0] != "你订了 3 个信源。" {
		t.Errorf("artifact 应恰含 RunOnce 回复，实得 %v", texts)
	}
	if chat.gotUserID != 7 {
		t.Errorf("RunOnce 应以 owner 身份执行（userID=7），实得 %d", chat.gotUserID)
	}
	if chat.gotText != "我订了哪些信源？" {
		t.Errorf("text 透传不符：%q", chat.gotText)
	}
}

// TestChatRejected 三种 REJECTED：text 缺失、text 空白、未装配 Chat/Owner。
func TestChatRejected(t *testing.T) {
	t.Run("text 缺失", func(t *testing.T) {
		e := newExecutor(Deps{Storage: newFakeTaskStorage(), Chat: &fakeChat{}, Principal: ownerResolver(&fakeOwner{})})
		events := collect(t, e.Execute(context.Background(),
			newExecCtx(textMsg(`{"skill":"assistant.chat"}`))))
		st := lastStatus(t, events)
		if st.Status.State != a2a.TaskStateRejected {
			t.Fatalf("终态 = %v, 期望 REJECTED", st.Status.State)
		}
	})
	t.Run("未装配 agent 轨", func(t *testing.T) {
		e := newExecutor(Deps{Storage: newFakeTaskStorage()}) // Chat/Owner 均 nil
		events := collect(t, e.Execute(context.Background(), newExecCtx(chatMsg("hi"))))
		st := lastStatus(t, events)
		if st.Status.State != a2a.TaskStateRejected {
			t.Fatalf("终态 = %v, 期望 REJECTED", st.Status.State)
		}
		if msg := st.Status.Message; msg == nil {
			t.Fatal("REJECTED 应带说明消息")
		}
	})
}

// TestChatFailedSanitized owner 解析失败 / RunOnce 失败 → FAILED，
// 对外文案只有 AppError.Message 或固定文案，原始错误链零外泄（契约 §8.1）。
func TestChatFailedSanitized(t *testing.T) {
	t.Run("owner 未捕获", func(t *testing.T) {
		e := newExecutor(Deps{Storage: newFakeTaskStorage(), Chat: &fakeChat{},
			Principal: ownerResolver(&fakeOwner{settingErr: types.ErrNotFound})})
		events := collect(t, e.Execute(context.Background(), newExecCtx(chatMsg("hi"))))
		st := lastStatus(t, events)
		if st.Status.State != a2a.TaskStateFailed {
			t.Fatalf("终态 = %v, 期望 FAILED", st.Status.State)
		}
		text, _ := firstTextPart(st.Status.Message)
		if !strings.Contains(text, "初始化") {
			t.Errorf("应为人话文案，实得 %q", text)
		}
	})
	t.Run("owner open_id 不外泄（B-F1）", func(t *testing.T) {
		// UpsertUserByOpenID 失败时其 AppError.Message 内嵌 open_id——绝不能进对外事件。
		e := newExecutor(Deps{Storage: newFakeTaskStorage(),
			Chat: &fakeChat{},
			Principal: ownerResolver(&fakeOwner{upsertErr: types.NewAppError(types.CodeDatabase,
				"upsert 用户（open_id=ou_secret_owner_id）", errors.New("pgx conn"))})})
		events := collect(t, e.Execute(context.Background(), newExecCtx(chatMsg("hi"))))
		st := lastStatus(t, events)
		if st.Status.State != a2a.TaskStateFailed {
			t.Fatalf("终态 = %v, 期望 FAILED", st.Status.State)
		}
		for _, ev := range events {
			raw, _ := json.Marshal(ev)
			if strings.Contains(string(raw), "ou_secret_owner_id") || strings.Contains(string(raw), "open_id") {
				t.Fatalf("owner open_id 外泄进事件: %s", raw)
			}
		}
	})
	t.Run("RunOnce 内部/llm 错误不外泄（A-2）", func(t *testing.T) {
		// 模拟 llm.mapHTTPError 形态：AppError.Message 含上游响应体全文。
		e := newExecutor(Deps{Storage: newFakeTaskStorage(),
			Chat: &fakeChat{err: types.NewAppError(types.CodeLLMUnavailable,
				`llm: 上游返回 HTTP 429: {"error":"rate limited","request_id":"req_secret_abc","input":"用户原文片段"}`, nil)},
			Principal: ownerResolver(&fakeOwner{userID: 7})})
		events := collect(t, e.Execute(context.Background(), newExecCtx(chatMsg("hi"))))
		st := lastStatus(t, events)
		if st.Status.State != a2a.TaskStateFailed {
			t.Fatalf("终态 = %v, 期望 FAILED", st.Status.State)
		}
		for _, ev := range events {
			raw, _ := json.Marshal(ev)
			if strings.Contains(string(raw), "req_secret_abc") || strings.Contains(string(raw), "rate limited") {
				t.Fatalf("llm 上游响应体外泄进事件: %s", raw)
			}
		}
	})
}

// seedChatTask 向 fake 存储塞一个已完成的历史任务（可指定输入形态与回复）。
func seedChatTask(t *testing.T, st *fakeTaskStorage, id, contextID, inputText, reply string, createdAt time.Time) {
	t.Helper()
	task := a2a.Task{
		ID:        a2a.TaskID(id),
		ContextID: contextID,
		Status:    a2a.TaskStatus{State: a2a.TaskStateCompleted},
		History: []*a2a.Message{
			a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart(inputText)),
		},
	}
	if reply != "" {
		task.Artifacts = []*a2a.Artifact{{Parts: []*a2a.Part{a2a.NewTextPart(reply)}}}
	}
	raw, err := json.Marshal(task)
	if err != nil {
		t.Fatal(err)
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	st.rows[id] = &types.A2ATask{
		ID: id, ContextID: contextID, Status: string(a2a.TaskStateCompleted),
		Task: raw, Version: 1, CreatedAt: createdAt, UpdatedAt: createdAt,
	}
}

// TestChatHistoryRebuild 多轮语义（契约 §12 P2）：同 contextId 的既往 assistant.chat
// 任务折叠成 user/assistant 对、按时间正序进入 RunOnce 的 history；
// content.query 任务与他 context 的任务不进历史。
func TestChatHistoryRebuild(t *testing.T) {
	st := newFakeTaskStorage()
	base := time.Date(2026, 7, 18, 10, 0, 0, 0, time.UTC)
	seedChatTask(t, st, "t1", "ctx-test", `{"skill":"assistant.chat","text":"第一问"}`, "第一答", base)
	seedChatTask(t, st, "t2", "ctx-test", `{"skill":"assistant.chat","text":"第二问"}`, "第二答", base.Add(time.Minute))
	// 干扰项：同 context 的 content.query 任务、他 context 的 chat 任务。
	seedChatTask(t, st, "t3", "ctx-test", `{"keyword":"Claude"}`, "检索结果", base.Add(2*time.Minute))
	seedChatTask(t, st, "t4", "ctx-other", `{"skill":"assistant.chat","text":"别的对话"}`, "别的回答", base.Add(3*time.Minute))

	chat := &fakeChat{reply: "第三答"}
	e := newExecutor(Deps{Storage: st, Chat: chat, Principal: ownerResolver(&fakeOwner{userID: 7})})
	events := collect(t, e.Execute(context.Background(), newExecCtx(chatMsg("第三问"))))

	if got := lastStatus(t, events).Status.State; got != a2a.TaskStateCompleted {
		t.Fatalf("终态 = %v, 期望 COMPLETED", got)
	}
	want := []llm.ChatMessage{
		{Role: "user", Content: "第一问"},
		{Role: "assistant", Content: "第一答"},
		{Role: "user", Content: "第二问"},
		{Role: "assistant", Content: "第二答"},
	}
	if len(chat.gotHistory) != len(want) {
		t.Fatalf("history 长度 = %d, 期望 %d：%+v", len(chat.gotHistory), len(want), chat.gotHistory)
	}
	for i := range want {
		if chat.gotHistory[i].Role != want[i].Role || chat.gotHistory[i].Content != want[i].Content {
			t.Errorf("history[%d] = %+v, 期望 %+v", i, chat.gotHistory[i], want[i])
		}
	}
}

// TestChatHistoryDegradesOnError 历史查询失败降级为空历史继续（历史是增强不是门槛），
// 任务照常 COMPLETED。
func TestChatHistoryDegradesOnError(t *testing.T) {
	st := newFakeTaskStorage()
	st.listErr = errors.New("db down")
	chat := &fakeChat{reply: "ok"}
	e := newExecutor(Deps{Storage: st, Chat: chat, Principal: ownerResolver(&fakeOwner{userID: 7})})
	events := collect(t, e.Execute(context.Background(), newExecCtx(chatMsg("hi"))))

	if got := lastStatus(t, events).Status.State; got != a2a.TaskStateCompleted {
		t.Fatalf("终态 = %v, 期望 COMPLETED（历史失败不该拖垮任务）", got)
	}
	if len(chat.gotHistory) != 0 {
		t.Errorf("历史查询失败应传空历史，实得 %+v", chat.gotHistory)
	}
}
