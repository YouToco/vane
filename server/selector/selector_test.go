package selector

import (
	"testing"
	"time"

	"github.com/YouToco/vane/types"
)

func tp(t time.Time) *time.Time { return &t }

func mkTimed(id int64, score float64, pub *time.Time, fetched time.Time) types.ScoredItem {
	return types.ScoredItem{
		Item:  types.ContentItem{ID: id, PublishedAt: pub, FetchedAt: fetched},
		Score: score,
	}
}

func ids(items []types.ScoredItem) []int64 {
	out := make([]int64, len(items))
	for i, it := range items {
		out[i] = it.Item.ID
	}
	return out
}

func eq(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestRankTopN_FreshnessDecay(t *testing.T) {
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	// A 原始分更高但发布 30h 前：90 - 30/6 = 85；B 88 分刚发布 → 88 排前。
	a := mkTimed(1, 90, tp(now.Add(-30*time.Hour)), now.Add(-time.Hour))
	b := mkTimed(2, 88, tp(now), now)
	got := RankTopN([]types.ScoredItem{a, b}, 2, now)
	if want := []int64{2, 1}; !eq(ids(got), want) {
		t.Errorf("衰减后新内容应排前，期望 %v，实际 %v", want, ids(got))
	}
	// 有效分只用于排序，ScoredItem.Score 保持 LLM 原始相关分。
	if got[0].Score != 88 || got[1].Score != 90 {
		t.Errorf("Score 不得被有效分覆写，实际 %v / %v", got[0].Score, got[1].Score)
	}
}

func TestRankTopN_PenaltyCap(t *testing.T) {
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	// A 90 分、600h 前发布：衰减封顶 12 → 78；B 77 分全新 → 77 → A 仍第一。
	// 若不封顶，A 的有效分是 -10，会被错误垫底。
	a := mkTimed(1, 90, tp(now.Add(-600*time.Hour)), now.Add(-600*time.Hour))
	b := mkTimed(2, 77, tp(now), now)
	got := RankTopN([]types.ScoredItem{a, b}, 2, now)
	if want := []int64{1, 2}; !eq(ids(got), want) {
		t.Errorf("衰减应封顶 %v 分，期望 %v，实际 %v", freshnessPenaltyCap, want, ids(got))
	}
}

func TestRankTopN_AnchorFallbackToFetchedAt(t *testing.T) {
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	// A 无 PublishedAt：锚回退 FetchedAt（30h 前）→ 90 - 5 = 85；B 88 分全新 → 88 排前。
	// 若回退未生效（按零龄），A 将以 90 分居首。
	a := mkTimed(1, 90, nil, now.Add(-30*time.Hour))
	b := mkTimed(2, 88, tp(now), now)
	got := RankTopN([]types.ScoredItem{a, b}, 2, now)
	if want := []int64{2, 1}; !eq(ids(got), want) {
		t.Errorf("PublishedAt 缺失应锚 FetchedAt 衰减，期望 %v，实际 %v", want, ids(got))
	}
}

func TestRankTopN_FuturePublishedAtNoBoost(t *testing.T) {
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	// 未来时间戳（源时区错配的脏数据）钳为零龄：A 50 分"未来 60h 发布"若按负衰减
	// 会反向加 10 分压过 B；钳零后 A=50 < B=50.5 → B 第一。
	a := mkTimed(1, 50, tp(now.Add(60*time.Hour)), now)
	b := mkTimed(2, 50.5, tp(now), now)
	got := RankTopN([]types.ScoredItem{a, b}, 2, now)
	if want := []int64{2, 1}; !eq(ids(got), want) {
		t.Errorf("未来时间戳不得反向加分，期望 %v，实际 %v", want, ids(got))
	}
}

func TestRankTopN_TieBreakAllKeys(t *testing.T) {
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	// 全部条目龄 ≥ 72h → 衰减一律封顶、有效分相等，逼出同分裁决全键：
	// PublishedAt desc（nil 最后）→ FetchedAt desc → Item.ID desc。
	in := []types.ScoredItem{
		mkTimed(4, 50, nil, now.Add(-100*time.Hour)),
		mkTimed(2, 50, tp(now.Add(-90*time.Hour)), now.Add(-80*time.Hour)),
		mkTimed(6, 50, nil, now.Add(-100*time.Hour)),
		mkTimed(1, 50, tp(now.Add(-80*time.Hour)), now.Add(-100*time.Hour)),
		mkTimed(3, 50, nil, now.Add(-90*time.Hour)),
		mkTimed(7, 50, tp(now.Add(-90*time.Hour)), now.Add(-90*time.Hour)),
		mkTimed(5, 50, nil, now.Add(-100*time.Hour)),
	}
	got := RankTopN(in, len(in), now)
	// 1：PublishedAt 最新（虽 FetchedAt 最旧，证明 PublishedAt 键优先）；
	// 2/7：PublishedAt 相同 → FetchedAt desc（2 较新）；
	// 3：无 PublishedAt 排全部有值者之后（即便 FetchedAt 不旧于 7）；
	// 6/5/4：FetchedAt 也相同 → ID desc。
	if want := []int64{1, 2, 7, 3, 6, 5, 4}; !eq(ids(got), want) {
		t.Errorf("同分裁决全键，期望 %v，实际 %v", want, ids(got))
	}
}

func TestRankTopN_Boundaries(t *testing.T) {
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	in := []types.ScoredItem{mkTimed(1, 30, tp(now), now), mkTimed(2, 90, tp(now), now)}
	if got := RankTopN(in, 0, now); len(got) != 0 {
		t.Errorf("n=0 应返回空，实际 %v", ids(got))
	}
	if got := RankTopN(in, -1, now); len(got) != 0 {
		t.Errorf("n<0 应返回空，实际 %v", ids(got))
	}
	if got := RankTopN(nil, 5, now); len(got) != 0 {
		t.Errorf("nil 输入应返回空，实际 %v", ids(got))
	}
	if got := RankTopN([]types.ScoredItem{}, 5, now); got == nil {
		t.Error("空输入应返回非 nil 空切片")
	}
	if got := RankTopN(in, 10, now); !eq(ids(got), []int64{2, 1}) {
		t.Errorf("n 超长应返回全部已排序，实际 %v", ids(got))
	}
}

func TestRankTopN_DoesNotMutateInput(t *testing.T) {
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	in := []types.ScoredItem{
		mkTimed(1, 30, tp(now.Add(-10*time.Hour)), now),
		mkTimed(2, 90, nil, now.Add(-5*time.Hour)),
		mkTimed(3, 60, tp(now), now),
	}
	before := ids(in)
	_ = RankTopN(in, 2, now)
	if !eq(ids(in), before) {
		t.Errorf("入参不应被修改，前 %v 后 %v", before, ids(in))
	}
}
