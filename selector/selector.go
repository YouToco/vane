// Package selector 负责在打分之后做 Top-N 选择：按分数降序取前 n 条。
//
// 设计为纯函数（无 I/O、无状态），便于 workflow 的 Select Activity 直接调用
// 并单测。作用于 types.ScoredItem —— 该类型由 types 包统一定义并被
// scorer / selector / cardgen 共享（见 types/scored_item.go 的设计说明），
// selector 只依赖 types，不会与下游包形成 import 环。
package selector

import (
	"sort"

	"github.com/YouToco/vane/types"
)

// SelectTopN 按 Score 降序返回前 n 条。纯函数：不修改入参切片。
//
// 边界：n <= 0 或输入为空 → 返回空切片（非 nil，便于调用方直接 range 与 JSON 序列化）；
// n 大于可选数量 → 返回全部（已排序）。同分保持输入原有相对顺序（稳定排序），
// 使结果在同分场景下可复现。
func SelectTopN(scored []types.ScoredItem, n int) []types.ScoredItem {
	if n <= 0 || len(scored) == 0 {
		return []types.ScoredItem{}
	}

	// 拷贝后排序，避免对调用方持有的切片产生副作用。
	out := make([]types.ScoredItem, len(scored))
	copy(out, scored)
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].Score > out[j].Score
	})

	if n > len(out) {
		n = len(out)
	}
	return out[:n]
}
