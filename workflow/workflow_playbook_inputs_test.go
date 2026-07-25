package workflow

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"

	"github.com/YouToco/vane/types"
)

// activityInputCapture 钉住 Workflow 发给两个 LLM Activity 的载荷。capture 发生在
// Temporal 完成序列化/反序列化之后；因此它测的是 Activity 真正收到的 wire shape，
// 不是测试里手造一个 ScoreIn 再自证 json.Marshal 能工作。
type activityInputCapture struct {
	mu      sync.Mutex
	score   []ScoreIn
	cardGen []CardGenIn
}

func (c *activityInputCapture) register(env *testsuite.TestWorkflowEnvironment) {
	reg := func(name string, fn any) {
		env.RegisterActivityWithOptions(fn, activity.RegisterOptions{Name: name})
	}
	reg("AuthorizeRun", func(context.Context, PushParams) (bool, error) { return true, nil })
	reg("EvolveProfile", func(context.Context, EvolveIn) error { return nil })
	reg("Fetch", func(context.Context, PushParams) ([]types.ContentItem, error) { return items(1), nil })
	reg("Dedup", func(_ context.Context, in DedupIn) ([]types.ContentItem, error) { return in.Items, nil })
	reg("QualifyEvents", func(_ context.Context, in QualifyEventsIn) (QualifyEventsResult, error) {
		return QualifyEventsResult{Items: in.Items, Outcome: "not_configured"}, nil
	})
	reg("Score", func(_ context.Context, in ScoreIn) ([]types.ScoredItem, error) {
		c.mu.Lock()
		c.score = append(c.score, in)
		c.mu.Unlock()
		return scoredItems(1), nil
	})
	reg("Select", func(_ context.Context, in SelectIn) ([]types.ScoredItem, error) { return in.Scored, nil })
	reg("CardGen", func(_ context.Context, in CardGenIn) ([]GeneratedCard, error) {
		c.mu.Lock()
		c.cardGen = append(c.cardGen, in)
		c.mu.Unlock()
		return cardsOf(1), nil
	})
	reg("Push", func(context.Context, PushIn) error { return nil })
}

func (c *activityInputCapture) snapshot() (ScoreIn, CardGenIn, int, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var score ScoreIn
	var cardGen CardGenIn
	if len(c.score) > 0 {
		score = c.score[0]
	}
	if len(c.cardGen) > 0 {
		cardGen = c.cardGen[0]
	}
	return score, cardGen, len(c.score), len(c.cardGen)
}

// TestPushPipelineWorkflow_P1cLLMActivityInputGolden 由 P1c 前的输入捕获校准而来：
// 当时定时任务已有 ScheduleID，但 Score/CardGen 的 wire payload 仍只有
// user_id/trace_id/items；ScheduleID 贯通改动曾让这条 golden 如预期变红。当前期望值
// 锁住修复后的新执行行为。旧在途历史是否仍可 replay 由 replay_test 独立守护。
func TestPushPipelineWorkflow_P1cLLMActivityInputGolden(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	capture := new(activityInputCapture)
	capture.register(env)

	env.ExecuteWorkflow(PushPipelineWorkflow, PushParams{
		TenantID:      1,
		UserID:        7,
		RunKind:       PushRunKindScheduled,
		ExecutionMode: types.ExecutionModeCompiled,
		ScheduleID:    "push-7-playbook",
		NLDesc:        "每日 AI 情报",
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("完整定时 pipeline 意外失败: %v", err)
	}

	score, cardGen, scoreCalls, cardGenCalls := capture.snapshot()
	if scoreCalls != 1 || cardGenCalls != 1 {
		t.Fatalf("Score/CardGen 应各调用一次，实得 score=%d cardgen=%d", scoreCalls, cardGenCalls)
	}
	if score.TraceID == "" || cardGen.TraceID != score.TraceID {
		t.Fatalf("两个 LLM Activity 应共享非空 trace_id，score=%q cardgen=%q", score.TraceID, cardGen.TraceID)
	}

	// SideEffect 生成的 trace 每次不同，归一化后再比逐字节 JSON；其余字段（包括
	// 顶层 key 集合）全部进入 golden。未来新增非空 schedule_id/playbook 字段会让
	// 这里精确红出 wire delta，而不是被 Go 结构体零值悄悄吞掉。
	score.TraceID = "trace"
	cardGen.TraceID = "trace"
	assertActivityInputGolden(t, "ScoreIn", score,
		`{"user_id":7,"trace_id":"trace","items":[{"id":1,"source_id":0,"external_id":"","canonical_key":"","kind":"","url":"","title":"t0","content":"","author":"","content_hash":"","fetched_at":"0001-01-01T00:00:00Z","created_at":"0001-01-01T00:00:00Z"}],"schedule_id":"push-7-playbook"}`)
	assertActivityInputGolden(t, "CardGenIn", cardGen,
		`{"user_id":7,"trace_id":"trace","items":[{"item":{"id":1,"source_id":0,"external_id":"","canonical_key":"","kind":"","url":"","title":"","content":"","author":"","content_hash":"","fetched_at":"0001-01-01T00:00:00Z","created_at":"0001-01-01T00:00:00Z"},"score":80}],"schedule_id":"push-7-playbook"}`)
}

func assertActivityInputGolden(t *testing.T, name string, got any, want string) {
	t.Helper()
	b, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("序列化 %s: %v", name, err)
	}
	if string(b) != want {
		t.Errorf("%s wire input 漂移\nwant: %s\n got: %s", name, want, b)
	}
}
