package llm

import (
	"context"
	"log/slog"

	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
)

// Recorder 负责把每次 LLM 调用同步写入 llm_calls 表。
// 单独成类型而不是让 Client 直接持有 store：调用与记账解耦，
// 单测 Client 时不需要数据库。
type Recorder struct {
	st *store.Store
}

// NewRecorder 构造记账器。
func NewRecorder(st *store.Store) *Recorder {
	return &Recorder{st: st}
}

// Record 同步写库。写失败只记日志、绝不向调用方返回错误：
// 记账是旁路可观测性，记账故障不能放大成业务调用失败
// （否则数据库抖动会让本已成功的 LLM 回复被误判为失败）。
func (r *Recorder) Record(ctx context.Context, call *types.LLMCall) {
	if r == nil || r.st == nil || call == nil {
		return
	}
	if _, err := r.st.InsertLLMCall(ctx, call); err != nil {
		slog.Error("llm 记账写库失败",
			"trace_id", call.TraceID,
			"span_name", call.SpanName,
			"model", call.Model,
			"err", err)
	}
}
