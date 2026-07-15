package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/YouToco/vane/types"
)

// fakePushTrigger 是 PushTrigger 的可编程假实现。
type fakePushTrigger struct {
	runID string
	err   error
	calls int
}

func (f *fakePushTrigger) TriggerPushNow(_ context.Context, _ int64) (string, error) {
	f.calls++
	return f.runID, f.err
}

// push_now 的错误分流：并发护栏的确定性拒绝（CodeValidation）要把文案回给模型
// 自纠而不是上抛——该分支在 TriggerPushNow 加 WorkflowExecutionErrorWhenAlreadyStarted
// 之前是死代码，专门补消费端覆盖，防契约在 scheduler 与 tools 两端各自漂移。
func TestPushNowTool_Execute(t *testing.T) {
	t.Run("成功触发返回runID文案", func(t *testing.T) {
		ft := &fakePushTrigger{runID: "push-agent-7"}
		tool := &pushNowTool{pusher: ft}
		got, err := tool.Execute(context.Background(), 7, json.RawMessage("{}"))
		if err != nil || !strings.Contains(got, "push-agent-7") || !strings.Contains(got, "已触发") {
			t.Fatalf("成功触发应返回含 runID 的文案, 实得 got=%q err=%v", got, err)
		}
		if ft.calls != 1 {
			t.Fatalf("应恰好触发 1 次, 实得 %d", ft.calls)
		}
	})

	t.Run("已在进行按文案回模型不上抛", func(t *testing.T) {
		// Cause 按生产形态给：TriggerPushNow 恒以 Temporal serviceerror 为 Cause，
		// 服务端原文不得跟着 Error() 一起进模型上下文。
		ae := types.NewAppError(types.CodeValidation,
			"已有一次推送正在进行，请等它完成后再触发",
			fmt.Errorf("Workflow execution is already running. WorkflowId: push-agent-7"))
		ae.Retryable = false
		tool := &pushNowTool{pusher: &fakePushTrigger{err: ae}}
		got, err := tool.Execute(context.Background(), 7, json.RawMessage("{}"))
		if err != nil {
			t.Fatalf("确定性拒绝不应上抛, 实得 err=%v", err)
		}
		if !strings.Contains(got, "已有一次推送正在进行") {
			t.Fatalf("应把拒绝文案回给模型, 实得 %q", got)
		}
		if strings.Contains(got, "already running") || strings.Contains(got, "VALIDATION") {
			t.Fatalf("拒绝文案不得携带错误链/错误码, 实得 %q", got)
		}
	})

	t.Run("基础设施错误照旧上抛", func(t *testing.T) {
		cause := types.NewAppError(types.CodeInternal, "触发即时推送失败", fmt.Errorf("temporal down"))
		tool := &pushNowTool{pusher: &fakePushTrigger{err: cause}}
		got, err := tool.Execute(context.Background(), 7, json.RawMessage("{}"))
		if err == nil || got != "" {
			t.Fatalf("基础设施错误应上抛, 实得 got=%q err=%v", got, err)
		}
		if !errors.Is(err, types.ErrInternal) {
			t.Fatalf("应保留原错误链, 实得 %v", err)
		}
	})
}
