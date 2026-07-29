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
					"decision":"execute_edit"
				}`,
			}},
		},
		{Content: "任务尚未修改；请补充具体要改的内容。"},
		{
			FinishReason: "tool_calls",
			ToolCalls: []llm.ToolCall{{
				ID: "list-1", Name: "list_schedules",
				Arguments: `{"query":"每周一上午9:00推送AI官方重大更新"}`,
			}},
		},
		{Content: "任务尚未修改；请补充具体要改的内容。"},
		{Content: "任务尚未修改；请补充具体要改的内容。"},
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
	if len(chat.requests) != 6 {
		t.Fatalf("model calls=%d, want route + 5 edit calls",
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
	if got := toolDefNames(chat.requests[4].Tools); len(got) != 1 ||
		got[0] != "edit_task_definition" {
		t.Fatalf("fourth tools=%v, want edit_task_definition only", got)
	}
	if got := toolDefNames(chat.requests[5].Tools); len(got) != 1 ||
		got[0] != "edit_task_definition" {
		t.Fatalf("fifth tools=%v, want edit_task_definition only", got)
	}
	if !strings.Contains(
		chat.requests[5].Messages[0].Content,
		"不得再次要求用户重复、拆分或确认",
	) {
		t.Fatal("resolved no-tool retry lacks explicit execution guidance")
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

func TestNaturalTaskDefinitionEditUniqueTargetPreservesSemanticClarification(
	t *testing.T,
) {
	fs := newFakeStore()
	list := &fakeTool{
		name:       "list_schedules",
		parameters: json.RawMessage(listSchedulesSchema),
		result: "- id=task-brief 按 cron「0 8 * * *」触发" +
			"（状态: active，描述: AI早报）",
	}
	controller := &fakeDefinitionEditController{}
	chat := &scriptedChat{responses: []*llm.ChatResponse{
		{
			FinishReason: "tool_calls",
			ToolCalls: []llm.ToolCall{{
				ID: "list-brief", Name: "list_schedules",
				Arguments: `{"query":"AI早报"}`,
			}},
		},
		{Content: "你希望降低推送频率，还是保持频率但减少内容？"},
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
	) (taskEditIntentDecision, error) {
		return taskEditIntentExecute, nil
	}

	out, err := loop.HandleMessage(
		t.Context(), 7, "把“AI早报”减少一点。",
	)
	if err != nil {
		t.Fatal(err)
	}
	if out.Reply != "你希望降低推送频率，还是保持频率但减少内容？" {
		t.Fatalf("Reply=%q", out.Reply)
	}
	if len(chat.requests) != 2 {
		t.Fatalf("model calls=%d, want locate + clarification",
			len(chat.requests))
	}
	if got := toolDefNames(chat.requests[1].Tools); len(got) != 1 ||
		got[0] != definitionEditToolName {
		t.Fatalf("resolved tools=%v, want edit tool", got)
	}
	if len(controller.proposeCalls) != 0 ||
		len(controller.executeCalls) != 0 {
		t.Fatal("semantic clarification reached durable edit controller")
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
	) (taskEditIntentDecision, error) {
		return taskEditIntentExecute, nil
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
	) (taskEditIntentDecision, error) {
		intentMessages = append(
			intentMessages,
			append([]llm.ChatMessage(nil), messages...),
		)
		return taskEditIntentExecute, nil
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
	) (taskEditIntentDecision, error) {
		intentCalls++
		if messages[len(messages)-1].Content == "AI 周报不用改了" {
			return taskEditIntentAnswerOnly, nil
		}
		return taskEditIntentExecute, nil
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
		{"把“竞品动态”改成每周一次", true},
		{"把早报频率设成每周一", true},
		{"早报以后只看官方博客", true},
		{"删除“AI 更新”任务", true},
		{"不要删掉 AI 日报", true},
		{"删除任务会有什么后果？", true},
		{"立即运行“每周更新”任务", true},
		{"不要马上推送 AI 日报", true},
		{"不要立即检查 AI 日报", true},
		{"不要现在检查 AI 日报", true},
		{"不要新增任务", true},
		{"取消 AI 日报", true},
		{"停止 AI 日报", true},
		{"关掉 AI 日报", true},
		{"运行一下 AI 日报", true},
		{"执行 AI 日报", true},
		{"运行竞品动态", true},
		{"执行一下竞品动态", true},
		{"关闭 AI 日报", true},
		{"终止 AI 日报", true},
		{"新增一个每天监控竞品的任务", true},
		{"帮我设一个每天九点的任务", true},
		{"建立一个竞品洞察", true},
		{"更新我的岗位为产品经理", true},
		{"修改我的关注标签", true},
		{"我在互联网行业，是产品经理，关注 AI 和机器人", true},
		{"创建任务：每天看官方博客", true},
	} {
		if got := isNaturalTaskDefinitionEditCandidate(test.text); got != test.want {
			t.Errorf("isNaturalTaskDefinitionEditCandidate(%q)=%v, want %v",
				test.text, got, test.want)
		}
	}
}

func TestRemoveScheduleExplicitIntentRejectsNegation(t *testing.T) {
	for _, text := range []string{
		"不要删除任务，只把日报改成周报",
		"别取消任务，我只是想修改频率",
		"不用删除 AI 日报",
		"不要删掉 AI 日报",
		"请勿删除任务",
		"不要移除任务",
		"别关掉任务",
		"取消删除任务",
		"删除任务会有什么后果？",
		"删除任务好不好？",
		"删除“AI 更新”任务",
	} {
		if explicitOwnerToolIntent("remove_schedule", text) {
			t.Errorf("lexical removal authorization escaped: %q", text)
		}
	}
	spec := newToolSpec(
		&fakeTool{name: "remove_schedule"},
		ownerPolicy(
			Effects(EffectStateWrite, EffectDirectOwnerWrite),
			BudgetNone,
		),
	)
	if !toolVisibleForRequest(spec, &toolRunState{
		ownerRequest:          "删除“AI 更新”任务",
		intents:               IntentTasks,
		allowedSideEffectTool: "remove_schedule",
	}) {
		t.Fatal("semantic delete authorization did not expose remove tool")
	}
}

func TestProfileIntakePromptRecognizesShortAnswerContext(t *testing.T) {
	history := []llm.ChatMessage{
		{Role: "user", Content: "你好"},
		{
			Role:    "assistant",
			Content: "你所在的行业和职业/岗位是什么？另外最关注哪些主题？",
		},
	}
	if !isProfileIntakePrompt(history) {
		t.Fatal("explicit profile intake question was not recognized")
	}
	got := profileIntakeContinuationHistory(history)
	if len(got) != 1 ||
		got[0].Role != history[1].Role ||
		got[0].Content != history[1].Content {
		t.Fatalf("profile continuation history=%+v", got)
	}
}

func TestShortProfileIntakeAnswerReachesSemanticUpdate(t *testing.T) {
	fs := newFakeStore()
	update := &fakeTool{
		name:   "update_profile",
		result: "画像已建立",
	}
	chat := &scriptedChat{responses: []*llm.ChatResponse{
		{
			Content: "你所在的行业和职业/岗位是什么？另外最关注哪些主题？",
		},
		{
			FinishReason: "tool_calls",
			ToolCalls: []llm.ToolCall{{
				ID:   "profile-intake",
				Name: "update_profile",
				Arguments: `{
					"industry":"互联网",
					"occupation":"产品经理",
					"tags":["AI","机器人"]
				}`,
			}},
		},
		{Content: "画像已建立。"},
	}}
	loop := New(Deps{
		Store:    fs,
		Profiles: fs,
		Tools: []ToolSpec{newToolSpec(
			update,
			withToolSurface(
				ownerPolicy(
					Effects(
						EffectStateWrite,
						EffectDirectOwnerWrite,
					),
					BudgetNone,
				),
				ExposureIntent,
				IntentProfile,
				ResultTrustLocal,
				true,
			),
		)},
		Model:      "test-model",
		MaxTurns:   4,
		SessionTTL: 30 * time.Minute,
	})
	loop.chatFn = chat.fn
	var intentMessages []llm.ChatMessage
	loop.taskEditIntentFn = func(
		_ context.Context,
		messages []llm.ChatMessage,
	) (taskEditIntentDecision, error) {
		intentMessages = append([]llm.ChatMessage(nil), messages...)
		return taskEditIntentProfileUpdate, nil
	}

	first, err := loop.HandleMessage(t.Context(), 7, "你好")
	if err != nil {
		t.Fatal(err)
	}
	if first.Reply !=
		"你所在的行业和职业/岗位是什么？另外最关注哪些主题？" {
		t.Fatalf("first Reply=%q", first.Reply)
	}

	second, err := loop.HandleMessage(
		t.Context(), 7, "互联网，产品经理，AI、机器人",
	)
	if err != nil {
		t.Fatal(err)
	}
	if second.Reply != "画像已建立。" {
		t.Fatalf("second Reply=%q", second.Reply)
	}
	if len(update.calls) != 1 {
		t.Fatalf("update_profile calls=%d, want 1", len(update.calls))
	}
	if len(intentMessages) != 2 ||
		intentMessages[0].Role != "assistant" ||
		intentMessages[1].Content != "互联网，产品经理，AI、机器人" {
		t.Fatalf("intent messages=%+v", intentMessages)
	}
}

func TestTaskDefinitionEditIntentClassifierRequiresExplicitExecute(t *testing.T) {
	for _, test := range []struct {
		name     string
		response *llm.ChatResponse
		want     taskEditIntentDecision
	}{
		{
			name: "execute",
			response: &llm.ChatResponse{ToolCalls: []llm.ToolCall{{
				ID:   "route-1",
				Name: taskDefinitionEditIntentTool.Name,
				Arguments: `{
					"decision":"execute_edit"
				}`,
			}}},
			want: taskEditIntentExecute,
		},
		{
			name: "delete task",
			response: &llm.ChatResponse{ToolCalls: []llm.ToolCall{{
				ID:   "route-2",
				Name: taskDefinitionEditIntentTool.Name,
				Arguments: `{
					"decision":"delete_task"
				}`,
			}}},
			want: taskEditIntentDelete,
		},
		{
			name: "run task",
			response: &llm.ChatResponse{ToolCalls: []llm.ToolCall{{
				ID:   "route-3",
				Name: taskDefinitionEditIntentTool.Name,
				Arguments: `{
					"decision":"run_task"
				}`,
			}}},
			want: taskEditIntentRun,
		},
		{
			name: "create task",
			response: &llm.ChatResponse{ToolCalls: []llm.ToolCall{{
				ID:   "route-create",
				Name: taskDefinitionEditIntentTool.Name,
				Arguments: `{
					"decision":"create_task"
				}`,
			}}},
			want: taskEditIntentCreate,
		},
		{
			name: "one off search",
			response: &llm.ChatResponse{ToolCalls: []llm.ToolCall{{
				ID:   "route-4",
				Name: taskDefinitionEditIntentTool.Name,
				Arguments: `{
					"decision":"one_off_search"
				}`,
			}}},
			want: taskEditIntentSearch,
		},
		{
			name: "profile update",
			response: &llm.ChatResponse{ToolCalls: []llm.ToolCall{{
				ID:   "route-profile",
				Name: taskDefinitionEditIntentTool.Name,
				Arguments: `{
					"decision":"update_profile"
				}`,
			}}},
			want: taskEditIntentProfileUpdate,
		},
		{
			name: "answer only",
			response: &llm.ChatResponse{ToolCalls: []llm.ToolCall{{
				ID:   "route-5",
				Name: taskDefinitionEditIntentTool.Name,
				Arguments: `{
					"decision":"answer_only"
				}`,
			}}},
			want: taskEditIntentAnswerOnly,
		},
		{
			name:     "tool free",
			response: &llm.ChatResponse{Content: "execute"},
			want:     taskEditIntentUnavailable,
		},
		{
			name: "unknown field",
			response: &llm.ChatResponse{ToolCalls: []llm.ToolCall{{
				ID:   "route-6",
				Name: taskDefinitionEditIntentTool.Name,
				Arguments: `{
					"decision":"execute_edit",
					"extra":true
				}`,
			}}},
			want: taskEditIntentUnavailable,
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
				t.Fatalf("decision=%v, want %v", got, test.want)
			}
		})
	}
}

func TestTaskDefinitionEditIntentAndMainCallsSealOrderedContextSteps(t *testing.T) {
	store := &contextShadowStore{fakeStore: newFakeStore()}
	chat := &scriptedChat{responses: []*llm.ChatResponse{
		{
			FinishReason: "tool_calls",
			ToolCalls: []llm.ToolCall{{
				ID:   "route-shadow",
				Name: taskDefinitionEditIntentTool.Name,
				Arguments: `{
					"decision":"answer_only"
				}`,
			}},
		},
		{Content: "这是咨询，不执行修改。"},
	}}
	loop := New(Deps{
		Store:      store,
		Model:      "test-model",
		MaxTurns:   20,
		SessionTTL: 30 * time.Minute,
	})
	loop.chatFn = chat.fn

	if _, err := loop.HandleMessage(
		t.Context(), 7, "把“竞品动态”改成每周一次是否合适？",
	); err != nil {
		t.Fatal(err)
	}
	drainCtx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := loop.DrainSessionWrites(drainCtx); err != nil {
		t.Fatal(err)
	}
	steps := make(map[int]bool)
	for _, snapshot := range store.snapshots() {
		steps[snapshot.ContextStep] = true
	}
	if !steps[1] || !steps[2] {
		t.Fatalf("sealed context steps=%v, want adjudication=1 main=2",
			steps)
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
			) (taskEditIntentDecision, error) {
				return taskEditIntentAnswerOnly, nil
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

func TestTaskEditOtherOperationKeepsMatchingCapability(t *testing.T) {
	for _, test := range []struct {
		name     string
		request  string
		decision taskEditIntentDecision
		tool     ToolSpec
	}{
		{
			name:     "delete task whose name contains update",
			request:  "删除 AI 更新",
			decision: taskEditIntentDelete,
			tool: newToolSpec(
				&fakeTool{name: "remove_schedule"},
				ownerPolicy(
					Effects(
						EffectStateWrite,
						EffectDirectOwnerWrite,
					),
					BudgetNone,
				),
			),
		},
		{
			name:     "run task whose name contains update",
			request:  "立即运行“每周更新”任务",
			decision: taskEditIntentRun,
			tool: newToolSpec(
				&fakeTool{name: "run_task_now"},
				withToolSurface(
					ownerPolicy(
						Effects(EffectDelivery),
						BudgetDownstreamManaged,
					),
					ExposureIntent,
					IntentTasks,
					ResultTrustLocal,
					true,
				),
			),
		},
		{
			name:     "run task without immediate keyword",
			request:  "运行竞品动态",
			decision: taskEditIntentRun,
			tool: newToolSpec(
				&fakeTool{name: "run_task_now"},
				withToolSurface(
					ownerPolicy(
						Effects(EffectDelivery),
						BudgetDownstreamManaged,
					),
					ExposureIntent,
					IntentTasks,
					ResultTrustLocal,
					true,
				),
			),
		},
		{
			name:     "natural create wording",
			request:  "建立一个竞品洞察",
			decision: taskEditIntentCreate,
			tool: newToolSpec(
				&fakeTool{
					name:       "create_schedule",
					parameters: json.RawMessage(createScheduleSchema),
				},
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
		{
			name:     "first profile intake",
			request:  "我在互联网行业，是产品经理，关注 AI 和机器人",
			decision: taskEditIntentProfileUpdate,
			tool: newToolSpec(
				&fakeTool{name: "update_profile"},
				withToolSurface(
					ownerPolicy(
						Effects(
							EffectStateWrite,
							EffectDirectOwnerWrite,
						),
						BudgetNone,
					),
					ExposureIntent,
					IntentProfile,
					ResultTrustLocal,
					true,
				),
			),
		},
		{
			name:     "one off update search",
			request:  "更新一下 OpenAI 最新消息",
			decision: taskEditIntentSearch,
			tool: newToolSpec(
				&fakeTool{name: "web_search"},
				withToolSurface(
					ownerPolicy(
						Effects(
							EffectNetworkRead,
							EffectBillable,
							EffectTrustTaint,
						),
						BudgetToolManaged,
					),
					ExposureIntent,
					IntentWebResearch,
					ResultTrustExternal,
					false,
				),
			),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			loop := New(Deps{
				Store:      newFakeStore(),
				Tools:      []ToolSpec{test.tool},
				Model:      "test-model",
				MaxTurns:   20,
				SessionTTL: 30 * time.Minute,
			})
			var request llm.ChatRequest
			loop.chatFn = func(
				_ context.Context,
				got llm.ChatRequest,
			) (*llm.ChatResponse, error) {
				request = got
				return &llm.ChatResponse{Content: "已理解该操作。"}, nil
			}
			loop.taskEditIntentFn = func(
				context.Context,
				[]llm.ChatMessage,
			) (taskEditIntentDecision, error) {
				return test.decision, nil
			}
			if _, err := loop.HandleMessage(
				t.Context(), 7, test.request,
			); err != nil {
				t.Fatal(err)
			}
			found := false
			for _, name := range toolDefNames(request.Tools) {
				found = found || name == test.tool.Name()
			}
			if !found {
				t.Fatalf("tools=%v, want %s",
					toolDefNames(request.Tools), test.tool.Name())
			}
		})
	}
}

func TestSemanticDeleteGateRejectsNegatedHallucination(t *testing.T) {
	remove := &fakeTool{name: "remove_schedule", result: "must not run"}
	chat := &scriptedChat{responses: []*llm.ChatResponse{
		{
			FinishReason: "tool_calls",
			ToolCalls: []llm.ToolCall{{
				ID:   "hallucinated-delete",
				Name: "remove_schedule",
				Arguments: `{
					"schedule_ids":["task-ai-daily"]
				}`,
			}},
		},
		{Content: "好的，不删除 AI 日报。"},
	}}
	loop := New(Deps{
		Store: newFakeStore(),
		Tools: []ToolSpec{
			newToolSpec(remove, ownerPolicy(
				Effects(EffectStateWrite, EffectDirectOwnerWrite),
				BudgetNone,
			)),
		},
		Model:      "test-model",
		MaxTurns:   20,
		SessionTTL: 30 * time.Minute,
	})
	loop.chatFn = chat.fn
	loop.taskEditIntentFn = func(
		context.Context,
		[]llm.ChatMessage,
	) (taskEditIntentDecision, error) {
		return taskEditIntentAnswerOnly, nil
	}

	out, err := loop.HandleMessage(
		t.Context(), 7, "不要删掉 AI 日报",
	)
	if err != nil {
		t.Fatal(err)
	}
	if out.Reply != "好的，不删除 AI 日报。" {
		t.Fatalf("Reply=%q", out.Reply)
	}
	if len(remove.calls) != 0 {
		t.Fatalf("negated removal executed %d times", len(remove.calls))
	}
	for _, request := range chat.requests {
		for _, name := range toolDefNames(request.Tools) {
			if name == "remove_schedule" {
				t.Fatal("answer-only turn exposed remove_schedule")
			}
		}
	}
}

func TestSemanticDeleteGateExecutesPositiveOnce(t *testing.T) {
	remove := &fakeTool{name: "remove_schedule", result: "已删除任务。"}
	chat := &scriptedChat{responses: []*llm.ChatResponse{
		{
			FinishReason: "tool_calls",
			ToolCalls: []llm.ToolCall{{
				ID:   "semantic-delete",
				Name: "remove_schedule",
				Arguments: `{
					"schedule_ids":["task-ai-daily"]
				}`,
			}},
		},
		{Content: "已删除 AI 更新任务。"},
	}}
	loop := New(Deps{
		Store: newFakeStore(),
		Tools: []ToolSpec{
			newToolSpec(remove, ownerPolicy(
				Effects(EffectStateWrite, EffectDirectOwnerWrite),
				BudgetNone,
			)),
		},
		Model:      "test-model",
		MaxTurns:   20,
		SessionTTL: 30 * time.Minute,
	})
	loop.chatFn = chat.fn
	loop.taskEditIntentFn = func(
		context.Context,
		[]llm.ChatMessage,
	) (taskEditIntentDecision, error) {
		return taskEditIntentDelete, nil
	}

	out, err := loop.HandleMessage(
		t.Context(), 7, "删除“AI 更新”任务",
	)
	if err != nil {
		t.Fatal(err)
	}
	if out.Reply != "已删除 AI 更新任务。" {
		t.Fatalf("Reply=%q", out.Reply)
	}
	if len(remove.calls) != 1 {
		t.Fatalf("semantic removal calls=%d, want 1", len(remove.calls))
	}
}

func TestSemanticTaskActionGateRejectsNegatedRunAndCreate(t *testing.T) {
	for _, test := range []struct {
		name    string
		request string
		tool    *fakeTool
		policy  ToolPolicy
	}{
		{
			name:    "negated run",
			request: "不要马上推送 AI 日报",
			tool:    &fakeTool{name: "run_task_now"},
			policy: withToolSurface(
				ownerPolicy(
					Effects(EffectDelivery),
					BudgetDownstreamManaged,
				),
				ExposureIntent,
				IntentTasks,
				ResultTrustLocal,
				true,
			),
		},
		{
			name:    "negated create",
			request: "不要新增任务",
			tool: &fakeTool{
				name:       "create_schedule",
				parameters: json.RawMessage(createScheduleSchema),
			},
			policy: ownerPolicy(
				Effects(
					EffectDurableProposal,
					EffectStateWrite,
					EffectDirectOwnerWrite,
				),
				BudgetNone,
			),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			chat := &scriptedChat{responses: []*llm.ChatResponse{
				{
					FinishReason: "tool_calls",
					ToolCalls: []llm.ToolCall{{
						ID:        "hallucinated-action",
						Name:      test.tool.Name(),
						Arguments: `{}`,
					}},
				},
				{Content: "好的，本轮不执行该操作。"},
			}}
			loop := New(Deps{
				Store:      newFakeStore(),
				Tools:      []ToolSpec{newToolSpec(test.tool, test.policy)},
				Model:      "test-model",
				MaxTurns:   20,
				SessionTTL: 30 * time.Minute,
			})
			loop.chatFn = chat.fn
			loop.taskEditIntentFn = func(
				context.Context,
				[]llm.ChatMessage,
			) (taskEditIntentDecision, error) {
				return taskEditIntentAnswerOnly, nil
			}
			if _, err := loop.HandleMessage(
				t.Context(), 7, test.request,
			); err != nil {
				t.Fatal(err)
			}
			if len(test.tool.calls) != 0 {
				t.Fatalf("%s executed %d times",
					test.tool.Name(), len(test.tool.calls))
			}
			for _, request := range chat.requests {
				for _, name := range toolDefNames(request.Tools) {
					if name == test.tool.Name() {
						t.Fatalf("answer-only exposed %s", name)
					}
				}
			}
		})
	}
}

func TestOneOffSearchExcludesInternalReadsAtBothBoundaries(t *testing.T) {
	list := &fakeTool{
		name:       "list_schedules",
		parameters: json.RawMessage(listSchedulesSchema),
	}
	profile := &fakeTool{name: "view_profile"}
	web := &fakeTool{name: "web_search", untrusted: true}
	chat := &scriptedChat{responses: []*llm.ChatResponse{
		{
			FinishReason: "tool_calls",
			ToolCalls: []llm.ToolCall{{
				ID: "hallucinated-internal", Name: "list_schedules",
				Arguments: `{}`,
			}},
		},
		{Content: "没有读取任何内部状态。"},
	}}
	loop := New(Deps{
		Store: newFakeStore(),
		Tools: []ToolSpec{
			newToolSpec(list, withToolSurface(
				a2aReadPolicy(Effects(EffectInternalRead)),
				ExposureIntent, IntentTasks, ResultTrustLocal, false,
			)),
			newToolSpec(profile, withToolSurface(
				ownerPolicy(Effects(EffectInternalRead), BudgetNone),
				ExposureIntent, IntentProfile, ResultTrustLocal, false,
			)),
			newToolSpec(web, withToolSurface(
				ownerPolicy(
					Effects(
						EffectNetworkRead,
						EffectBillable,
						EffectTrustTaint,
					),
					BudgetToolManaged,
				),
				ExposureIntent,
				IntentWebResearch,
				ResultTrustExternal,
				false,
			)),
		},
		Model:      "test-model",
		MaxTurns:   20,
		SessionTTL: 30 * time.Minute,
	})
	loop.chatFn = chat.fn
	loop.taskEditIntentFn = func(
		context.Context,
		[]llm.ChatMessage,
	) (taskEditIntentDecision, error) {
		return taskEditIntentSearch, nil
	}

	if _, err := loop.HandleMessage(
		t.Context(),
		7,
		"更新一下我的 AI 任务和岗位相关的最新消息",
	); err != nil {
		t.Fatal(err)
	}
	if len(list.calls) != 0 || len(profile.calls) != 0 {
		t.Fatalf("internal calls list=%d profile=%d, want zero",
			len(list.calls), len(profile.calls))
	}
	for _, request := range chat.requests {
		names := toolDefNames(request.Tools)
		for _, name := range names {
			if name == "list_schedules" || name == "view_profile" {
				t.Fatalf("search tools=%v include internal read", names)
			}
		}
	}
}

func TestTaskDefinitionEditAdjudicationFailureSuppressesAllWrites(t *testing.T) {
	create := &fakeTool{
		name:       "create_schedule",
		parameters: json.RawMessage(createScheduleSchema),
		result:     "must not run",
	}
	remove := &fakeTool{
		name:   "remove_schedule",
		result: "must not run",
	}
	chat := &scriptedChat{responses: []*llm.ChatResponse{
		{
			FinishReason: "tool_calls",
			ToolCalls: []llm.ToolCall{{
				ID:   "hallucinated-create",
				Name: "create_schedule",
				Arguments: `{
					"intent":"错误代偿创建周报"
				}`,
			}},
		},
		{Content: "任务编辑意图无法安全判定，本轮没有执行写入。"},
	}}
	loop := New(Deps{
		Store: newFakeStore(),
		Tools: []ToolSpec{
			newToolSpec(create, ownerPolicy(
				Effects(
					EffectDurableProposal,
					EffectStateWrite,
					EffectDirectOwnerWrite,
				),
				BudgetNone,
			)),
			newToolSpec(remove, ownerPolicy(
				Effects(EffectStateWrite, EffectDirectOwnerWrite),
				BudgetNone,
			)),
		},
		Model:      "test-model",
		MaxTurns:   20,
		SessionTTL: 30 * time.Minute,
	})
	loop.chatFn = chat.fn
	loop.taskEditIntentFn = func(
		context.Context,
		[]llm.ChatMessage,
	) (taskEditIntentDecision, error) {
		return taskEditIntentUnavailable,
			errors.New("semantic gate unavailable")
	}

	out, err := loop.HandleMessage(
		t.Context(), 7, "把日报改成周报",
	)
	if err != nil {
		t.Fatal(err)
	}
	if out.Reply != "任务编辑意图无法安全判定，本轮没有执行写入。" {
		t.Fatalf("Reply=%q", out.Reply)
	}
	if len(chat.requests) != 2 {
		t.Fatalf("requests=%d, want hidden-write rejection + reply",
			len(chat.requests))
	}
	for _, request := range chat.requests {
		for _, name := range toolDefNames(request.Tools) {
			if name == "create_schedule" ||
				name == "remove_schedule" ||
				name == definitionEditToolName {
				t.Fatalf("side-effect-free tools=%v",
					toolDefNames(request.Tools))
			}
		}
	}
	if len(create.calls) != 0 || len(remove.calls) != 0 {
		t.Fatalf("create calls=%d remove calls=%d, want zero",
			len(create.calls), len(remove.calls))
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
