package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/YouToco/vane/llm"
	"github.com/YouToco/vane/task"
	"github.com/YouToco/vane/types"
)

type fakeDefinitionEditController struct {
	proposeCalls []task.DefinitionEditProposalInput
	proposal     task.DefinitionEditProposal
	proposeErr   error

	executeCalls []fakeDefinitionEditCall
	execute      task.TaskDefinitionEditOutcome
	executeErr   error
}

type fakeDefinitionEditCall struct {
	userID   int64
	actionID string
	receipt  task.TaskDefinitionEditReceiptTarget
}

func (f *fakeDefinitionEditController) Prepare(
	_ context.Context,
	in task.DefinitionEditProposalInput,
) (task.DefinitionEditProposal, error) {
	f.proposeCalls = append(f.proposeCalls, in)
	if f.proposeErr != nil {
		return task.DefinitionEditProposal{}, f.proposeErr
	}
	result := f.proposal
	if result.ID == "" {
		result.ID = in.ActionID
	}
	if result.Summary == "" {
		result.Summary = "编辑任务 task-edit-1"
	}
	return result, nil
}

func (f *fakeDefinitionEditController) Execute(
	_ context.Context,
	userID int64,
	actionID string,
	receipt task.TaskDefinitionEditReceiptTarget,
) (task.TaskDefinitionEditOutcome, error) {
	f.executeCalls = append(f.executeCalls, fakeDefinitionEditCall{
		userID: userID, actionID: actionID, receipt: receipt,
	})
	return f.execute, f.executeErr
}

type fakeDefinitionEditReceiptSessionStore struct {
	*fakeStore
	mu sync.Mutex

	calls    int
	lease    types.TaskDefinitionEditReceiptLease
	messages json.RawMessage
}

func TestDirectTaskDefinitionEditPreservesTargetedClarification(t *testing.T) {
	fs := newFakeStore()
	controller := &fakeDefinitionEditController{}
	loop := New(Deps{
		Store:              fs,
		TaskDefinitionEdit: controller,
		Model:              "test-model",
		MaxTurns:           4,
		SessionTTL:         30 * time.Minute,
	})
	loop.chatFn = func(context.Context, llm.ChatRequest) (*llm.ChatResponse, error) {
		return &llm.ChatResponse{
			Content: "你说的“减少推送”是改为每周一次，还是保留频率但只推重大事件？",
		}, nil
	}
	out, err := loop.HandleTaskDefinitionEditMessage(
		t.Context(), 7,
		"beff990d-9ed6-4a5d-8f72-9e6987516dda",
		"task-edit-1",
		"减少这个任务的推送",
	)
	if err != nil {
		t.Fatal(err)
	}
	if out.Reply != "你说的“减少推送”是改为每周一次，还是保留频率但只推重大事件？" {
		t.Fatalf("针对性编辑澄清被改写: %q", out.Reply)
	}
	if len(controller.proposeCalls) != 0 || len(controller.executeCalls) != 0 {
		t.Fatal("澄清问题不得产生定义编辑副作用")
	}
}

func TestNaturalTaskDefinitionEditResolvesNameThenEditsOnce(t *testing.T) {
	fs := newFakeStore()
	list := &fakeTool{
		name:       "list_schedules",
		parameters: json.RawMessage(listSchedulesSchema),
		result:     "- id=task-edit-1 按 cron「0 9 * * 1」触发（状态: active，描述: 每周一上午9:00推送AI官方重大更新）",
	}
	controller := &fakeDefinitionEditController{
		execute: task.TaskDefinitionEditOutcome{
			Status: types.TaskDefinitionEditOperationStatusCompleted,
			TaskID: "task-edit-1",
		},
	}
	chat := &scriptedChat{responses: []*llm.ChatResponse{
		{
			FinishReason: "tool_calls",
			ToolCalls: []llm.ToolCall{{
				ID:   "route-edit",
				Name: taskDefinitionEditIntentTool.Name,
				Arguments: `{
					"decision":"execute"
				}`,
			}},
		},
		{Content: replyMaxTurns},
		{
			FinishReason: "tool_calls",
			ToolCalls: []llm.ToolCall{{
				ID: "list-1", Name: "list_schedules",
				Arguments: `{"query":"每周一上午9:00推送AI官方重大更新"}`,
			}},
		},
		{
			FinishReason: "tool_calls",
			ToolCalls: []llm.ToolCall{{
				ID:   "edit-1",
				Name: "edit_task_definition",
				Arguments: `{
					"task_id":"task-edit-1",
					"intent":"每周一检查三家官方博客；未来运行时打开官方原文并交叉核验；没有重大更新就不推送"
				}`,
			}},
		},
	}}
	loop := New(Deps{
		Store: fs,
		Tools: []ToolSpec{
			newToolSpec(list, ownerPolicy(
				Effects(EffectInternalRead), BudgetNone)),
			newToolSpec(&editTaskDefinitionTool{}, ownerPolicy(
				Effects(
					EffectDurableProposal,
					EffectStateWrite,
					EffectDirectOwnerWrite,
				),
				BudgetNone,
			)),
		},
		TaskDefinitionEdit: controller,
		Model:              "test-model",
		MaxTurns:           20,
		SessionTTL:         30 * time.Minute,
	})
	loop.chatFn = chat.fn

	out, err := loop.HandleMessage(
		t.Context(),
		7,
		"请把“每周一上午9:00推送AI官方重大更新”更新为：任务手册写清三家官方博客、交叉核验和无更新不推送，无需确认，直接更新。",
	)
	if err != nil {
		t.Fatal(err)
	}
	if out.Reply != "已修改定时推送任务（id=task-edit-1）。" {
		t.Fatalf("Reply=%q", out.Reply)
	}
	if len(chat.requests) != 4 {
		t.Fatalf("model calls=%d, want route + 3 edit calls",
			len(chat.requests))
	}
	if got := toolDefNames(chat.requests[0].Tools); len(got) != 1 ||
		got[0] != taskDefinitionEditIntentTool.Name {
		t.Fatalf("route tools=%v, want semantic intent gate", got)
	}
	if got := toolDefNames(chat.requests[1].Tools); len(got) != 1 ||
		got[0] != "list_schedules" {
		t.Fatalf("first tools=%v, want list_schedules only", got)
	}
	if got := toolDefNames(chat.requests[2].Tools); len(got) != 1 ||
		got[0] != "list_schedules" {
		t.Fatalf("retry tools=%v, want list_schedules only", got)
	}
	if got := toolDefNames(chat.requests[3].Tools); len(got) != 1 ||
		got[0] != "edit_task_definition" {
		t.Fatalf("third tools=%v, want edit_task_definition only", got)
	}
	if len(list.calls) != 1 {
		t.Fatalf("list calls=%d, want 1", len(list.calls))
	}
	if len(controller.proposeCalls) != 1 ||
		len(controller.executeCalls) != 1 {
		t.Fatalf("prepare=%d execute=%d, want 1/1",
			len(controller.proposeCalls), len(controller.executeCalls))
	}
	if bytes.Contains(
		[]byte(chat.requests[1].Messages[0].Content),
		[]byte("请把需求拆小"),
	) {
		t.Fatal("natural edit lane must not instruct the user to split one edit")
	}
}

func TestNaturalTaskDefinitionEditSupportsPoliteShortName(t *testing.T) {
	fs := newFakeStore()
	list := &fakeTool{
		name:       "list_schedules",
		parameters: json.RawMessage(listSchedulesSchema),
		result:     "- id=task-brief 按 cron「0 8 * * *」触发（状态: active，描述: 早报）",
	}
	controller := &fakeDefinitionEditController{
		execute: task.TaskDefinitionEditOutcome{
			Status: types.TaskDefinitionEditOperationStatusCompleted,
			TaskID: "task-brief",
		},
	}
	chat := &scriptedChat{responses: []*llm.ChatResponse{
		{
			FinishReason: "tool_calls",
			ToolCalls: []llm.ToolCall{{
				ID: "list-short", Name: "list_schedules",
				Arguments: `{"query":"早报"}`,
			}},
		},
		{
			FinishReason: "tool_calls",
			ToolCalls: []llm.ToolCall{{
				ID:   "edit-short",
				Name: definitionEditToolName,
				Arguments: `{
					"task_id":"task-brief",
					"intent":"只看官方博客"
				}`,
			}},
		},
	}}
	loop := New(Deps{
		Store: fs,
		Tools: []ToolSpec{
			newToolSpec(list, ownerPolicy(
				Effects(EffectInternalRead), BudgetNone)),
			newToolSpec(&editTaskDefinitionTool{}, ownerPolicy(
				Effects(
					EffectDurableProposal,
					EffectStateWrite,
					EffectDirectOwnerWrite,
				),
				BudgetNone,
			)),
		},
		TaskDefinitionEdit: controller,
		Model:              "test-model",
		MaxTurns:           20,
		SessionTTL:         30 * time.Minute,
	})
	loop.chatFn = chat.fn
	loop.taskEditIntentFn = func(
		context.Context,
		[]llm.ChatMessage,
	) (bool, error) {
		return true, nil
	}

	out, err := loop.HandleMessage(
		t.Context(), 7,
		"能不能把早报改为只看官方博客？",
	)
	if err != nil {
		t.Fatal(err)
	}
	if out.Reply != "已修改定时推送任务（id=task-brief）。" {
		t.Fatalf("Reply=%q", out.Reply)
	}
	if len(list.calls) != 1 || list.calls[0].args != `{"query":"早报"}` {
		t.Fatalf("list calls=%+v", list.calls)
	}
	if len(controller.proposeCalls) != 1 ||
		len(controller.executeCalls) != 1 {
		t.Fatalf("prepare=%d execute=%d, want 1/1",
			len(controller.proposeCalls), len(controller.executeCalls))
	}
}

func TestNaturalTaskDefinitionEditAmbiguityExposesNoWriteTool(t *testing.T) {
	fs := newFakeStore()
	list := &fakeTool{
		name:       "list_schedules",
		parameters: json.RawMessage(listSchedulesSchema),
		results: []string{
			strings.Join([]string{
				"共 2 个定时推送任务：",
				"- id=task-ai-daily 按 cron「0 8 * * *」触发（状态: active，描述: AI更新日报）",
				"- id=task-ai-weekly 按 cron「0 9 * * 1」触发（状态: active，描述: AI更新周报）",
			}, "\n"),
			strings.Join([]string{
				"共 1 个定时推送任务：",
				"- id=task-ai-weekly 按 cron「0 9 * * 1」触发（状态: active，描述: AI更新周报）",
			}, "\n"),
		},
	}
	controller := &fakeDefinitionEditController{
		execute: task.TaskDefinitionEditOutcome{
			Status: types.TaskDefinitionEditOperationStatusCompleted,
			TaskID: "task-ai-weekly",
		},
	}
	chat := &scriptedChat{responses: []*llm.ChatResponse{
		{
			FinishReason: "tool_calls",
			ToolCalls: []llm.ToolCall{{
				ID: "list-ambiguous", Name: "list_schedules",
				Arguments: `{"query":"AI更新"}`,
			}},
		},
		{Content: "你要修改“AI更新日报”还是“AI更新周报”？"},
		{
			FinishReason: "tool_calls",
			ToolCalls: []llm.ToolCall{{
				ID: "list-followup", Name: "list_schedules",
				Arguments: `{"query":"周报"}`,
			}},
		},
		{
			FinishReason: "tool_calls",
			ToolCalls: []llm.ToolCall{{
				ID:   "edit-followup",
				Name: definitionEditToolName,
				Arguments: `{
					"task_id":"task-ai-weekly",
					"intent":"每周一只推重大官方发布"
				}`,
			}},
		},
	}}
	loop := New(Deps{
		Store: fs,
		Tools: []ToolSpec{
			newToolSpec(list, ownerPolicy(
				Effects(EffectInternalRead), BudgetNone)),
			newToolSpec(&editTaskDefinitionTool{}, ownerPolicy(
				Effects(
					EffectDurableProposal,
					EffectStateWrite,
					EffectDirectOwnerWrite,
				),
				BudgetNone,
			)),
		},
		TaskDefinitionEdit: controller,
		Model:              "test-model",
		MaxTurns:           20,
		SessionTTL:         30 * time.Minute,
	})
	loop.chatFn = chat.fn
	var intentMessages [][]llm.ChatMessage
	loop.taskEditIntentFn = func(
		_ context.Context,
		messages []llm.ChatMessage,
	) (bool, error) {
		intentMessages = append(
			intentMessages,
			append([]llm.ChatMessage(nil), messages...),
		)
		return true, nil
	}

	out, err := loop.HandleMessage(
		t.Context(),
		7,
		"请把“AI更新”任务更新为每周一只推重大官方发布。",
	)
	if err != nil {
		t.Fatal(err)
	}
	if out.Reply != "你要修改“AI更新日报”还是“AI更新周报”？" {
		t.Fatalf("Reply=%q", out.Reply)
	}
	if len(chat.requests) != 2 || len(chat.requests[1].Tools) != 0 {
		t.Fatalf("requests=%d second tools=%v, want no write tool",
			len(chat.requests), toolDefNames(chat.requests[1].Tools))
	}
	if len(controller.proposeCalls) != 0 || len(controller.executeCalls) != 0 {
		t.Fatal("ambiguous lookup reached durable edit controller")
	}

	followup, err := loop.HandleMessage(t.Context(), 7, "周报那个")
	if err != nil {
		t.Fatal(err)
	}
	if followup.Reply != "已修改定时推送任务（id=task-ai-weekly）。" {
		t.Fatalf("followup Reply=%q", followup.Reply)
	}
	if len(list.calls) != 2 {
		t.Fatalf("followup list_schedules calls=%d, want 2", len(list.calls))
	}
	if list.calls[1].args != `{"query":"周报"}` {
		t.Fatalf("followup query=%s, want readable selection", list.calls[1].args)
	}
	if len(controller.proposeCalls) != 1 ||
		len(controller.executeCalls) != 1 {
		t.Fatalf("followup prepare=%d execute=%d, want 1/1",
			len(controller.proposeCalls), len(controller.executeCalls))
	}
	if len(intentMessages) != 2 || len(intentMessages[1]) != 3 ||
		intentMessages[1][0].Content !=
			"请把“AI更新”任务更新为每周一只推重大官方发布。" ||
		intentMessages[1][2].Content != "周报那个" {
		t.Fatalf("intent continuation context=%+v", intentMessages)
	}
}

func TestNaturalTaskDefinitionEditCancellationLeavesEditLane(t *testing.T) {
	fs := newFakeStore()
	list := &fakeTool{
		name:       "list_schedules",
		parameters: json.RawMessage(listSchedulesSchema),
		result: strings.Join([]string{
			"共 2 个定时推送任务：",
			"- id=task-ai-daily 按 cron「0 8 * * *」触发（状态: active，描述: AI更新日报）",
			"- id=task-ai-weekly 按 cron「0 9 * * 1」触发（状态: active，描述: AI更新周报）",
		}, "\n"),
	}
	controller := &fakeDefinitionEditController{}
	chat := &scriptedChat{responses: []*llm.ChatResponse{
		{
			FinishReason: "tool_calls",
			ToolCalls: []llm.ToolCall{{
				ID: "list-cancel", Name: "list_schedules",
				Arguments: `{"query":"AI更新"}`,
			}},
		},
		{Content: "你要修改“AI更新日报”还是“AI更新周报”？"},
		{Content: "好的，AI 更新周报保持不变。"},
	}}
	loop := New(Deps{
		Store: fs,
		Tools: []ToolSpec{
			newToolSpec(list, ownerPolicy(
				Effects(EffectInternalRead), BudgetNone)),
			newToolSpec(&editTaskDefinitionTool{}, ownerPolicy(
				Effects(
					EffectDurableProposal,
					EffectStateWrite,
					EffectDirectOwnerWrite,
				),
				BudgetNone,
			)),
		},
		TaskDefinitionEdit: controller,
		Model:              "test-model",
		MaxTurns:           20,
		SessionTTL:         30 * time.Minute,
	})
	loop.chatFn = chat.fn
	intentCalls := 0
	loop.taskEditIntentFn = func(
		_ context.Context,
		messages []llm.ChatMessage,
	) (bool, error) {
		intentCalls++
		if messages[len(messages)-1].Content == "AI 周报不用改了" {
			return false, nil
		}
		return true, nil
	}

	first, err := loop.HandleMessage(
		t.Context(),
		7,
		"请把“AI更新”任务更新为每周一只推重大官方发布。",
	)
	if err != nil {
		t.Fatal(err)
	}
	if first.Reply != "你要修改“AI更新日报”还是“AI更新周报”？" {
		t.Fatalf("first Reply=%q", first.Reply)
	}
	second, err := loop.HandleMessage(
		t.Context(), 7, "AI 周报不用改了",
	)
	if err != nil {
		t.Fatal(err)
	}
	if second.Reply != "好的，AI 更新周报保持不变。" {
		t.Fatalf("second Reply=%q", second.Reply)
	}
	if intentCalls != 2 || len(list.calls) != 1 {
		t.Fatalf("intent calls=%d list calls=%d, want 2/1",
			intentCalls, len(list.calls))
	}
	for _, name := range toolDefNames(chat.requests[2].Tools) {
		if name == definitionEditToolName {
			t.Fatal("cancelled continuation exposed definition edit")
		}
	}
	if len(controller.proposeCalls) != 0 ||
		len(controller.executeCalls) != 0 {
		t.Fatal("cancelled continuation reached durable controller")
	}
}

func TestNaturalTaskDefinitionEditCandidate(t *testing.T) {
	for _, test := range []struct {
		text string
		want bool
	}{
		{"请把“每周 AI 更新”任务更新为只看官方博客", true},
		{"把日报改成每周一推送", true},
		{"如何修改这个任务？", true},
		{"不要修改这个任务", true},
		{"不要把日报改成周报", true},
		{"别把 AI 周报改为每天", true},
		{"我不是要把日报改成周报", true},
		{"我不想把 AI 日报改为周报", true},
		{"请勿把 AI 日报改为周报", true},
		{"我并非要把 AI 日报改成周报", true},
		{"你觉得把 AI 日报改为周报怎么样？", true},
		{"如果把 AI 日报改为周报，会有什么影响？", true},
		{"AI 日报是否应该改为周报？", true},
		{"可以把 AI 日报改为周报吗，如果这样会有什么影响？", true},
		{"可以把 AI 日报改成每周一推送吗？", true},
		{"能不能把早报改为只看官方博客？", true},
		{"能否把早报改为只看官方博客？", true},
		{"修改竞品情报：以后只看官方博客", true},
		{"创建任务：每天看官方博客", false},
	} {
		if got := isNaturalTaskDefinitionEditCandidate(test.text); got != test.want {
			t.Errorf("isNaturalTaskDefinitionEditCandidate(%q)=%v, want %v",
				test.text, got, test.want)
		}
	}
}

func TestTaskDefinitionEditIntentClassifierRequiresExplicitExecute(t *testing.T) {
	for _, test := range []struct {
		name     string
		response *llm.ChatResponse
		want     bool
	}{
		{
			name: "execute",
			response: &llm.ChatResponse{ToolCalls: []llm.ToolCall{{
				ID:   "route-1",
				Name: taskDefinitionEditIntentTool.Name,
				Arguments: `{
					"decision":"execute"
				}`,
			}}},
			want: true,
		},
		{
			name: "non execute",
			response: &llm.ChatResponse{ToolCalls: []llm.ToolCall{{
				ID:   "route-2",
				Name: taskDefinitionEditIntentTool.Name,
				Arguments: `{
					"decision":"non_execute"
				}`,
			}}},
		},
		{
			name:     "tool free",
			response: &llm.ChatResponse{Content: "execute"},
		},
		{
			name: "unknown field",
			response: &llm.ChatResponse{ToolCalls: []llm.ToolCall{{
				ID:   "route-3",
				Name: taskDefinitionEditIntentTool.Name,
				Arguments: `{
					"decision":"execute",
					"extra":true
				}`,
			}}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			loop := &Loop{model: "test-model"}
			loop.chatFn = func(
				context.Context,
				llm.ChatRequest,
			) (*llm.ChatResponse, error) {
				return test.response, nil
			}
			got, err := loop.classifyTaskDefinitionEditIntent(
				t.Context(),
				[]llm.ChatMessage{{
					Role: "user", Content: "请把 AI 日报改为周报",
				}},
			)
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("allowed=%v, want %v", got, test.want)
			}
		})
	}
}

func TestTaskDefinitionEditNonExecuteCannotWrite(t *testing.T) {
	for _, ownerRequest := range []string{
		"请把 AI 日报改为周报是否合适？",
		"把 AI 日报改为周报的利弊列一下",
		"可以把 AI 日报改为周报好不好？",
	} {
		t.Run(ownerRequest, func(t *testing.T) {
			controller := &fakeDefinitionEditController{}
			loop := New(Deps{
				Store: newFakeStore(),
				Tools: []ToolSpec{
					newToolSpec(
						&editTaskDefinitionTool{},
						ownerPolicy(
							Effects(
								EffectDurableProposal,
								EffectStateWrite,
								EffectDirectOwnerWrite,
							),
							BudgetNone,
						),
					),
				},
				TaskDefinitionEdit: controller,
				Model:              "test-model",
				MaxTurns:           20,
				SessionTTL:         30 * time.Minute,
			})
			var request llm.ChatRequest
			loop.chatFn = func(
				_ context.Context,
				got llm.ChatRequest,
			) (*llm.ChatResponse, error) {
				request = got
				return &llm.ChatResponse{
					Content: "这是咨询，不执行修改。",
				}, nil
			}
			loop.taskEditIntentFn = func(
				context.Context,
				[]llm.ChatMessage,
			) (bool, error) {
				return false, nil
			}
			out, err := loop.HandleMessage(
				t.Context(), 7, ownerRequest,
			)
			if err != nil {
				t.Fatal(err)
			}
			if out.Reply != "这是咨询，不执行修改。" {
				t.Fatalf("Reply=%q", out.Reply)
			}
			for _, name := range toolDefNames(request.Tools) {
				if name == definitionEditToolName {
					t.Fatal("non-execute request exposed definition edit")
				}
			}
			if len(controller.proposeCalls) != 0 ||
				len(controller.executeCalls) != 0 {
				t.Fatal("non-execute request reached durable controller")
			}
		})
	}
}

func TestNaturalTaskDefinitionEditBindsUniqueResolvedTask(t *testing.T) {
	controller := &fakeDefinitionEditController{}
	loop := New(Deps{
		Tools: []ToolSpec{
			newToolSpec(&editTaskDefinitionTool{}, ownerPolicy(
				Effects(
					EffectDurableProposal,
					EffectStateWrite,
					EffectDirectOwnerWrite,
				),
				BudgetNone,
			)),
		},
		TaskDefinitionEdit: controller,
	})
	state := &toolRunState{
		naturalTaskDefinitionEdit:           true,
		naturalTaskDefinitionEditTaskListed: true,
		naturalTaskDefinitionEditResolvedID: "task-edit-1",
	}
	ctx := context.WithValue(t.Context(), toolRunKey{}, state)
	sessionID := int64(9)
	msgs, err := loop.runToolCalls(ctx, 7, &sessionID, []llm.ToolCall{{
		ID: "wrong", Name: definitionEditToolName,
		Arguments: `{"task_id":"task-other","intent":"wrong target"}`,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(controller.proposeCalls) != 0 || len(controller.executeCalls) != 0 {
		t.Fatal("wrong same-owner task ID reached durable edit controller")
	}
	if len(msgs) != 1 ||
		!bytes.Contains([]byte(msgs[0].Content), []byte("唯一命中任务不一致")) {
		t.Fatalf("tool result=%+v", msgs)
	}
}

func TestNaturalTaskDefinitionEditRequiresDeclaredDurablePolicy(t *testing.T) {
	controller := &fakeDefinitionEditController{}
	loop := New(Deps{
		Tools: []ToolSpec{
			newToolSpec(&editTaskDefinitionTool{}, ownerPolicy(
				Effects(EffectStateWrite, EffectDirectOwnerWrite),
				BudgetNone,
			)),
		},
		TaskDefinitionEdit: controller,
	})
	state := &toolRunState{
		naturalTaskDefinitionEdit:           true,
		naturalTaskDefinitionEditTaskListed: true,
		naturalTaskDefinitionEditResolvedID: "task-edit-1",
	}
	ctx := context.WithValue(t.Context(), toolRunKey{}, state)
	sessionID := int64(9)
	msgs, err := loop.runToolCalls(ctx, 7, &sessionID, []llm.ToolCall{{
		ID: "hidden", Name: definitionEditToolName,
		Arguments: `{"task_id":"task-edit-1","intent":"hidden write"}`,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(controller.proposeCalls) != 0 || len(controller.executeCalls) != 0 {
		t.Fatal("undeclared edit policy reached durable controller")
	}
	if len(msgs) != 1 ||
		!bytes.Contains([]byte(msgs[0].Content), []byte("能力当前不可用")) {
		t.Fatalf("tool result=%+v", msgs)
	}
}

func TestGeneralChatCannotExecuteHiddenDefinitionEdit(t *testing.T) {
	controller := &fakeDefinitionEditController{}
	loop := New(Deps{
		Tools: []ToolSpec{
			newToolSpec(&editTaskDefinitionTool{}, ownerPolicy(
				Effects(
					EffectDurableProposal,
					EffectStateWrite,
					EffectDirectOwnerWrite,
				),
				BudgetNone,
			)),
		},
		TaskDefinitionEdit: controller,
	})
	state := &toolRunState{
		ownerRequest: "如果把 AI 日报改为周报，会有什么影响？",
		intents:      IntentTasks,
	}
	if names := toolDefNames(loop.requestTools(state)); len(names) != 0 {
		t.Fatalf("general tools=%v, want id-based edit hidden", names)
	}
	ctx := context.WithValue(t.Context(), toolRunKey{}, state)
	sessionID := int64(9)
	msgs, err := loop.runToolCalls(
		ctx,
		7,
		&sessionID,
		[]llm.ToolCall{{
			ID: "hallucinated", Name: definitionEditToolName,
			Arguments: `{"task_id":"task-edit-1","intent":"must not run"}`,
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(controller.proposeCalls) != 0 || len(controller.executeCalls) != 0 {
		t.Fatal("general hidden edit reached durable controller")
	}
	if len(msgs) != 1 ||
		!bytes.Contains([]byte(msgs[0].Content), []byte("唯一任务")) {
		t.Fatalf("tool result=%+v", msgs)
	}
}

func TestNaturalTaskDefinitionEditQueryMustComeFromOwnerRequest(t *testing.T) {
	owner := "请把“AI 官方更新”任务改为每周一推送"
	for _, test := range []struct {
		args string
		want bool
	}{
		{`{"query":"AI 官方更新"}`, true},
		{`{"query":"财经日报"}`, false},
		{`{"query":"每周一推送"}`, false},
		{`{"query":"任务"}`, false},
		{`{"query":"AI 官方更新","extra":true}`, false},
		{`{"query":"日报"}`, false},
	} {
		if got := validNaturalEditScheduleQuery(
			json.RawMessage(test.args), owner,
		); got != test.want {
			t.Errorf("valid query %s=%v, want %v", test.args, got, test.want)
		}
	}
	if !validNaturalEditScheduleQuery(
		json.RawMessage(`{"query":"日报"}`),
		"把日报改成每周一推送",
	) {
		t.Fatal("short readable task name must be a valid lookup")
	}
}

func (s *fakeDefinitionEditReceiptSessionStore) RecordTaskDefinitionEditReceiptSessionMessages(
	_ context.Context,
	lease types.TaskDefinitionEditReceiptLease,
	messages json.RawMessage,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	s.lease = lease
	s.messages = bytes.Clone(messages)
	return nil
}

func TestRecordDefinitionEditReceiptSessionUsesAgentUserLock(t *testing.T) {
	base := newFakeStore()
	store := &fakeDefinitionEditReceiptSessionStore{fakeStore: base}
	loop := New(Deps{Store: store})
	receipt := types.TaskDefinitionEditReceipt{
		ID: 9, TenantID: 2, UserID: 7,
		LeaseOwner: "definition-edit-receipt-worker", Fence: 4,
	}
	messages := json.RawMessage(
		`[{"role":"user","content":"[卡片回调] fixed edit fact"}]`,
	)
	muValue, _ := loop.userMu.LoadOrStore(int64(7), newUserTurnLock())
	userMu := muValue.(*userTurnLock)
	if err := userMu.Lock(t.Context()); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	err := loop.RecordDefinitionEditReceiptSession(
		t.Context(), receipt, messages,
	)
	if !errors.Is(err, errCreationReceiptSessionBusy) {
		t.Fatalf("busy user lock error=%v", err)
	}
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("edit receipt recorder blocked dispatcher for %v", elapsed)
	}
	userMu.Unlock()
	if err := loop.RecordDefinitionEditReceiptSession(
		t.Context(), receipt, messages,
	); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.calls != 1 || store.lease != receipt.Lease() ||
		!bytes.Equal(store.messages, messages) {
		t.Fatalf("calls=%d lease=%+v messages=%s",
			store.calls, store.lease, store.messages)
	}
}

func toolNamed(tools []ToolSpec, name string) *ToolSpec {
	for i := range tools {
		if tools[i].Name() == name {
			return &tools[i]
		}
	}
	return nil
}

func toolDefNames(defs []llm.ToolDef) []string {
	out := make([]string, 0, len(defs))
	for _, def := range defs {
		out = append(out, def.Name)
	}
	return out
}
