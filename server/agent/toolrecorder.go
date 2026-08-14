package agent

import (
	"context"
	"log/slog"
	"time"

	"github.com/YouToco/vane/server/types"
)

const toolCallRecordTimeout = 5 * time.Second

// toolCallInserter 是记账所需的唯一 store 方法（生产实现 *store.Store）。
type toolCallInserter interface {
	InsertToolCall(ctx context.Context, c *types.ToolCall) (int64, error)
}

// ToolCallRecorder 把每次工具调用同步写入 tool_calls 表（端点注册表契约 §6，
// Boss 拍板 2026-07-18：全部 agent 工具调用都记）。与 llm.Recorder 同构同纪律：
// 记账是旁路可观测性，写失败只记日志、绝不向调用方返回错误——记账故障不能把
// 已成功的工具调用放大成业务失败。
type ToolCallRecorder struct {
	st toolCallInserter
}

// NewToolCallRecorder 构造记账器。
func NewToolCallRecorder(st toolCallInserter) *ToolCallRecorder {
	return &ToolCallRecorder{st: st}
}

// Record 同步写库，nil 接收者/依赖安全（未装配时全部调用为空操作）。
func (r *ToolCallRecorder) Record(ctx context.Context, c *types.ToolCall) {
	if r == nil || r.st == nil || c == nil {
		return
	}
	// 工具调用已经发生后，调用方取消不能只抹掉 Agent 外层账本而留下上游账本；
	// 但裸 WithoutCancel 会让连接池故障无限阻塞业务返回，因此重新加有界预算。
	recordCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), toolCallRecordTimeout)
	defer cancel()
	if _, err := r.st.InsertToolCall(recordCtx, c); err != nil {
		slog.Error("tool 记账写库失败",
			"trace_id", c.TraceID,
			"tool_name", c.ToolName,
			"tool_kind", c.ToolKind,
			"err", err)
	}
}
