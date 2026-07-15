// Package selector 负责在打分之后做 Top-N 选择。
//
// 两个入口：SelectTopN 是纯分数降序（M3，同分依赖稳定排序保持上游序）；
// RankTopN 叠加新鲜度衰减与全键同分裁决（M5 契约 §6，Select Activity 的
// 生产入口）。均设计为纯函数（无 I/O、无状态），便于 workflow 的 Select
// Activity 直接调用并单测。作用于 types.ScoredItem —— 该类型由 types 包
// 统一定义并被 scorer / selector / cardgen 共享（见 types/scored_item.go
// 的设计说明），selector 只依赖 types，不会与下游包形成 import 环。
package selector

import (
	"sort"
	"time"

	"github.com/YouToco/vane/types"
)

// 新鲜度衰减常量（M5 契约 §6，Boss 拍板④）：每 6 小时衰减 1 分、封顶 12 分
// （72 小时衰到顶）。封顶保证旧但高分的内容仍能与新内容竞争——衰减只调节
// "同档分数看新鲜度"，不把旧内容一票否决。
const (
	freshnessPenaltyPerHour = 1.0 / 6
	freshnessPenaltyCap     = 12.0
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

// RankTopN 按有效分排序并返回前 n 条（M5 契约 §6）。纯函数：不修改入参切片，
// 也不改写 ScoredItem.Score——有效分只用于排序，deliveries.score 落库仍是 LLM
// 原始相关分（有效分随 now 漂移，落库会让分数跨期不可比）。
//
// 有效分 = Score - min(freshnessPenaltyCap, ageHours*freshnessPenaltyPerHour)，
// age 锚 PublishedAt，缺失回退 FetchedAt（见 effectiveScore）。
//
// 排序键（显式同分裁决，全键确定化）：
//
//	有效分 desc → PublishedAt desc（nil 最后）→ FetchedAt desc → Item.ID desc
//
// 注意：SelectTopN 的同分序隐式继承上游 SQL（fetched_at DESC 无次键），本函数
// 把 PublishedAt 提前且全键确定化——同分顺序可能与旧隐式序不同，不声称"行为
// 不变"。末键 ID 保证全序确定，无需稳定排序。边界语义与 SelectTopN 一致：
// n <= 0 或输入为空 → 非 nil 空切片；n 超长 → 返回全部（已排序）。
func RankTopN(scored []types.ScoredItem, n int, now time.Time) []types.ScoredItem {
	if n <= 0 || len(scored) == 0 {
		return []types.ScoredItem{}
	}

	// 有效分预先算好存在包装结构里：排序中每次比较重算会把 O(n log n) 次
	// 时间运算浪费在纯函数上，且 ranked 与元素一起被 swap，不存在索引错位。
	type ranked struct {
		it  types.ScoredItem
		eff float64
	}
	rs := make([]ranked, len(scored))
	for i, it := range scored {
		rs[i] = ranked{it: it, eff: effectiveScore(it, now)}
	}

	sort.Slice(rs, func(i, j int) bool {
		a, b := rs[i], rs[j]
		if a.eff != b.eff {
			return a.eff > b.eff
		}
		ap, bp := a.it.Item.PublishedAt, b.it.Item.PublishedAt
		switch {
		case ap != nil && bp == nil:
			return true
		case ap == nil && bp != nil:
			return false
		case ap != nil && bp != nil && !ap.Equal(*bp):
			return ap.After(*bp)
		}
		if !a.it.Item.FetchedAt.Equal(b.it.Item.FetchedAt) {
			return a.it.Item.FetchedAt.After(b.it.Item.FetchedAt)
		}
		return a.it.Item.ID > b.it.Item.ID
	})

	if n > len(rs) {
		n = len(rs)
	}
	out := make([]types.ScoredItem, n)
	for i := range out {
		out[i] = rs[i].it
	}
	return out
}

// effectiveScore 计算排序用有效分：原始相关分减新鲜度衰减。
// age 锚 PublishedAt（源未提供时回退 FetchedAt——恒有值）。源偶见未来时间戳
// （时区错配等脏数据），负龄钳为 0：未来时间不得变成负衰减反向加分顶高排名。
func effectiveScore(it types.ScoredItem, now time.Time) float64 {
	anchor := it.Item.FetchedAt
	if it.Item.PublishedAt != nil {
		anchor = *it.Item.PublishedAt
	}
	ageHours := now.Sub(anchor).Hours()
	if ageHours < 0 {
		ageHours = 0
	}
	penalty := ageHours * freshnessPenaltyPerHour
	if penalty > freshnessPenaltyCap {
		penalty = freshnessPenaltyCap
	}
	return it.Score - penalty
}
