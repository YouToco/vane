package agent

import (
	"context"
	"testing"

	"github.com/YouToco/vane/server/types"
)

type contextCheckingToolCallInserter struct {
	called      bool
	contextErr  error
	hasDeadline bool
}

func (f *contextCheckingToolCallInserter) InsertToolCall(ctx context.Context, _ *types.ToolCall) (int64, error) {
	f.called = true
	f.contextErr = ctx.Err()
	_, f.hasDeadline = ctx.Deadline()
	return 1, nil
}

func TestToolCallRecorder_DetachesCallerCancellationWithBoundedDeadline(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	fake := &contextCheckingToolCallInserter{}
	NewToolCallRecorder(fake).Record(ctx, &types.ToolCall{ToolName: "web_search"})

	if !fake.called {
		t.Fatal("调用方取消后仍必须尝试写 Agent 外层账本")
	}
	if fake.contextErr != nil {
		t.Fatalf("记账 context 不应继承调用方取消: %v", fake.contextErr)
	}
	if !fake.hasDeadline {
		t.Fatal("脱离调用取消后必须重新设置有界 deadline，禁止旁路记账无限阻塞")
	}
}
