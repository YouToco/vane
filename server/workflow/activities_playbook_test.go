package workflow

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/YouToco/vane/server/types"
)

type instructionCaptureScorer struct {
	mu           sync.Mutex
	instructions []string
}

func (s *instructionCaptureScorer) Score(
	_ context.Context,
	_ int64,
	_ types.ContentItem,
	_, instruction string,
) (float64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.instructions = append(s.instructions, instruction)
	return 80, nil
}

func (s *instructionCaptureScorer) snapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string{}, s.instructions...)
}

type instructionCaptureCardGen struct {
	mu           sync.Mutex
	instructions []string
}

type failingInstructionScorer struct{ err error }

func (s failingInstructionScorer) Score(context.Context, int64, types.ContentItem, string, string) (float64, error) {
	return 0, s.err
}

type failingInstructionCardGen struct{ err error }

func (g failingInstructionCardGen) Generate(context.Context, int64, types.ScoredItem, string, string) (string, error) {
	return "", g.err
}

func (g *instructionCaptureCardGen) Generate(
	_ context.Context,
	_ int64,
	_ types.ScoredItem,
	_, instruction string,
) (string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.instructions = append(g.instructions, instruction)
	return "body", nil
}

func (g *instructionCaptureCardGen) snapshot() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string{}, g.instructions...)
}

func (f *fakeStore) playbookCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.playbookCalls
}

func (f *fakeStore) playbookReadSnapshot() []playbookReadCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]playbookReadCall{}, f.playbookReads...)
}

func newPlaybookActivities(
	st *fakeStore,
	sc Scorer,
	cg CardGenerator,
	opts ...ActivitiesOption,
) *Activities {
	return NewActivities(
		fakeFetcher{},
		sc,
		cg,
		&fakePusher{},
		st,
		fakeFeishu{},
		nil,
		nil,
		nil,
		nil,
		opts...,
	)
}

func playbookContentItems(n int) []types.ContentItem {
	items := make([]types.ContentItem, n)
	for i := range items {
		items[i] = types.ContentItem{ID: int64(i + 1), Title: "item"}
	}
	return items
}

func scoredPlaybookItems(n int) []types.ScoredItem {
	items := playbookContentItems(n)
	scored := make([]types.ScoredItem, n)
	for i, item := range items {
		scored[i] = types.ScoredItem{Item: item, Score: 80}
	}
	return scored
}

func assertTaskInstructions(t *testing.T, got []string, wantCount int, want string) {
	t.Helper()
	if len(got) != wantCount {
		t.Fatalf("下游调用次数 = %d，期望 %d", len(got), wantCount)
	}
	for i, instruction := range got {
		if instruction != want {
			t.Fatalf("第 %d 次任务指令 = %q，期望 %q", i+1, instruction, want)
		}
	}
}

func TestActivities_TaskPlaybookReadOncePerFanout(t *testing.T) {
	const (
		userID      = int64(37)
		scheduleID  = "sched-scope-9f3d7b"
		instruction = "只关注官方发布；卡片用三条要点呈现"
	)
	st := &fakeStore{playbook: &types.SchedulePlaybook{Content: instruction}}
	sc := &instructionCaptureScorer{}
	cg := &instructionCaptureCardGen{}
	a := newPlaybookActivities(
		st,
		sc,
		cg,
		WithPlaybookPromptPolicy(true, scheduleID),
	)

	scored, err := a.Score(t.Context(), ScoreIn{
		UserID: userID, TraceID: "trace-score", ScheduleID: scheduleID,
		Items: playbookContentItems(50),
	})
	if err != nil {
		t.Fatalf("Score 意外报错: %v", err)
	}
	if len(scored) != 50 {
		t.Fatalf("Score 产出 = %d 条，期望 50", len(scored))
	}
	if got := st.playbookCallCount(); got != 1 {
		t.Fatalf("Score 扇出前应只读一次任务手册，实得 %d 次", got)
	}
	assertTaskInstructions(t, sc.snapshot(), 50, instruction)

	cards, err := a.CardGen(t.Context(), CardGenIn{
		UserID: userID, TraceID: "trace-card", ScheduleID: scheduleID,
		Items: scoredPlaybookItems(5),
	})
	if err != nil {
		t.Fatalf("CardGen 意外报错: %v", err)
	}
	if len(cards) != 5 {
		t.Fatalf("CardGen 产出 = %d 条，期望 5", len(cards))
	}
	if got := st.playbookCallCount(); got != 2 {
		t.Fatalf("Score/CardGen 两个 Activity 应各读一次任务手册，实得 %d 次", got)
	}
	reads := st.playbookReadSnapshot()
	if len(reads) != 2 {
		t.Fatalf("任务手册读取记录 = %d 条，期望 2", len(reads))
	}
	for i, read := range reads {
		if read.userID != userID || read.scheduleID != scheduleID {
			t.Fatalf("第 %d 次读取作用域 = user:%d schedule:%q，期望 user:%d schedule:%q",
				i+1, read.userID, read.scheduleID, userID, scheduleID)
		}
	}
	assertTaskInstructions(t, cg.snapshot(), 5, instruction)
}

func TestActivities_TaskPlaybookRolloutPolicy(t *testing.T) {
	const instruction = "任务级指令"
	tests := []struct {
		name             string
		enabled          bool
		canaryScheduleID string
		inputScheduleID  string
		wantReads        int
		wantInstruction  string
	}{
		{
			name:            "disabled",
			inputScheduleID: "task-a",
		},
		{
			name:             "outside canary",
			enabled:          true,
			canaryScheduleID: "task-a",
			inputScheduleID:  "task-b",
		},
		{
			name:    "ad hoc run without schedule id",
			enabled: true,
		},
		{
			name:             "matching canary",
			enabled:          true,
			canaryScheduleID: " task-a ",
			inputScheduleID:  "task-a",
			wantReads:        1,
			wantInstruction:  instruction,
		},
		{
			name:            "enabled for all schedules",
			enabled:         true,
			inputScheduleID: "task-b",
			wantReads:       1,
			wantInstruction: instruction,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const userID = int64(41)
			st := &fakeStore{playbook: &types.SchedulePlaybook{Content: instruction}}
			sc := &instructionCaptureScorer{}
			a := newPlaybookActivities(
				st,
				sc,
				&instructionCaptureCardGen{},
				WithPlaybookPromptPolicy(tt.enabled, tt.canaryScheduleID),
			)

			got, err := a.Score(t.Context(), ScoreIn{
				UserID: userID, TraceID: "trace-policy", ScheduleID: tt.inputScheduleID,
				Items: playbookContentItems(1),
			})
			if err != nil || len(got) != 1 {
				t.Fatalf("Score 应保持正常执行，got=%d err=%v", len(got), err)
			}
			if reads := st.playbookCallCount(); reads != tt.wantReads {
				t.Fatalf("任务手册读取次数 = %d，期望 %d", reads, tt.wantReads)
			}
			if tt.wantReads == 1 {
				reads := st.playbookReadSnapshot()
				if reads[0].userID != userID || reads[0].scheduleID != tt.inputScheduleID {
					t.Fatalf("任务手册读取作用域 = user:%d schedule:%q，期望 user:%d schedule:%q",
						reads[0].userID, reads[0].scheduleID, userID, tt.inputScheduleID)
				}
			}
			assertTaskInstructions(t, sc.snapshot(), 1, tt.wantInstruction)
		})
	}
}

func TestActivities_TaskPlaybookReadFailuresKeepLegacyPath(t *testing.T) {
	tests := []struct {
		name     string
		playbook *types.SchedulePlaybook
		err      error
	}{
		{name: "not found", err: types.ErrNotFound},
		{name: "database error", err: errors.New("database unavailable")},
		{name: "nil response"},
		{name: "empty content", playbook: &types.SchedulePlaybook{}},
		{name: "whitespace content", playbook: &types.SchedulePlaybook{Content: " \n\t "}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := &fakeStore{playbook: tt.playbook, playbookErr: tt.err}
			sc := &instructionCaptureScorer{}
			cg := &instructionCaptureCardGen{}
			a := newPlaybookActivities(
				st,
				sc,
				cg,
				WithPlaybookPromptPolicy(true, ""),
			)

			scored, scoreErr := a.Score(t.Context(), ScoreIn{
				UserID: 7, TraceID: "trace-fallback-score", ScheduleID: "task-7",
				Items: playbookContentItems(1),
			})
			if scoreErr != nil || len(scored) != 1 {
				t.Fatalf("读取失败不得中断 Score，got=%d err=%v", len(scored), scoreErr)
			}

			cards, cardErr := a.CardGen(t.Context(), CardGenIn{
				UserID: 7, TraceID: "trace-fallback-card", ScheduleID: "task-7",
				Items: scoredPlaybookItems(1),
			})
			if cardErr != nil || len(cards) != 1 {
				t.Fatalf("读取失败不得中断 CardGen，got=%d err=%v", len(cards), cardErr)
			}
			if reads := st.playbookCallCount(); reads != 2 {
				t.Fatalf("两个 Activity 应分别尝试一次读取，实得 %d 次", reads)
			}
			assertTaskInstructions(t, sc.snapshot(), 1, "")
			assertTaskInstructions(t, cg.snapshot(), 1, "")
		})
	}
}

func TestActivities_TaskPlaybookContentNeverEntersLogs(t *testing.T) {
	const secret = "PLAYBOOK-BODY-MUST-NOT-APPEAR-IN-LOGS"
	var logs strings.Builder
	oldLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(oldLogger) })

	st := &fakeStore{playbook: &types.SchedulePlaybook{Content: secret}}
	a := newPlaybookActivities(
		st,
		&instructionCaptureScorer{},
		&instructionCaptureCardGen{},
		WithPlaybookPromptPolicy(true, "task-log"),
	)
	if _, err := a.Score(t.Context(), ScoreIn{
		UserID: 9, TraceID: "trace-log", ScheduleID: "task-log",
		Items: playbookContentItems(1),
	}); err != nil {
		t.Fatalf("Score 意外报错: %v", err)
	}

	output := logs.String()
	if strings.Contains(output, secret) {
		t.Fatalf("日志泄露了任务手册正文: %s", output)
	}
	if !strings.Contains(output, `"status":"loaded"`) ||
		!strings.Contains(output, `"stored_runes":`) {
		t.Fatalf("日志应保留不含正文的可观测状态与长度: %s", output)
	}
}

func TestActivities_TaskPlaybookCannotReturnThroughFailureLogs(t *testing.T) {
	const secret = "PLAYBOOK-ECHO-MUST-NOT-RETURN-THROUGH-ERROR"
	var logs strings.Builder
	oldLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(oldLogger) })

	downstreamErr := types.NewAppError(types.CodeLLMUnavailable, secret, nil)
	st := &fakeStore{playbook: &types.SchedulePlaybook{Content: secret}}
	a := newPlaybookActivities(
		st,
		failingInstructionScorer{err: downstreamErr},
		failingInstructionCardGen{err: downstreamErr},
		WithPlaybookPromptPolicy(true, "task-failure-log"),
	)
	_, _ = a.Score(t.Context(), ScoreIn{
		UserID: 9, TraceID: "trace-score-failure", ScheduleID: "task-failure-log",
		Items: playbookContentItems(1),
	})
	_, _ = a.CardGen(t.Context(), CardGenIn{
		UserID: 9, TraceID: "trace-card-failure", ScheduleID: "task-failure-log",
		Items: scoredPlaybookItems(1),
	})

	output := logs.String()
	if strings.Contains(output, secret) {
		t.Fatalf("下游错误回显导致任务手册正文进入日志: %s", output)
	}
	for _, want := range []string{`"error_code":"LLM_UNAVAILABLE"`, `"error_type":"*types.AppError"`, `"retryable":true`} {
		if !strings.Contains(output, want) {
			t.Fatalf("日志缺少安全结构化诊断字段 %q: %s", want, output)
		}
	}
}
