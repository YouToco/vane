package selector

import (
	"testing"

	"github.com/YouToco/vane/types"
)

func mk(id int64, score float64) types.ScoredItem {
	return types.ScoredItem{Item: types.ContentItem{ID: id}, Score: score}
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

func TestSelectTopN_SortsDescending(t *testing.T) {
	in := []types.ScoredItem{mk(1, 30), mk(2, 90), mk(3, 60), mk(4, 10)}
	got := SelectTopN(in, 2)
	if want := []int64{2, 3}; !eq(ids(got), want) {
		t.Errorf("期望 %v，实际 %v", want, ids(got))
	}
}

func TestSelectTopN_NGreaterThanLen(t *testing.T) {
	in := []types.ScoredItem{mk(1, 30), mk(2, 90)}
	got := SelectTopN(in, 10)
	if want := []int64{2, 1}; !eq(ids(got), want) {
		t.Errorf("n 超长应返回全部已排序，期望 %v，实际 %v", want, ids(got))
	}
}

func TestSelectTopN_Boundaries(t *testing.T) {
	in := []types.ScoredItem{mk(1, 30)}
	if got := SelectTopN(in, 0); len(got) != 0 {
		t.Errorf("n=0 应返回空，实际 %v", ids(got))
	}
	if got := SelectTopN(in, -1); len(got) != 0 {
		t.Errorf("n<0 应返回空，实际 %v", ids(got))
	}
	if got := SelectTopN(nil, 5); len(got) != 0 {
		t.Errorf("nil 输入应返回空，实际 %v", ids(got))
	}
	if got := SelectTopN([]types.ScoredItem{}, 5); got == nil {
		t.Error("空输入应返回非 nil 空切片")
	}
}

func TestSelectTopN_StableOnTies(t *testing.T) {
	// 同分保持输入顺序：1,2,3 同为 50 分，取前 2 应是 1,2。
	in := []types.ScoredItem{mk(1, 50), mk(2, 50), mk(3, 50)}
	got := SelectTopN(in, 2)
	if want := []int64{1, 2}; !eq(ids(got), want) {
		t.Errorf("同分应稳定，期望 %v，实际 %v", want, ids(got))
	}
}

func TestSelectTopN_DoesNotMutateInput(t *testing.T) {
	in := []types.ScoredItem{mk(1, 30), mk(2, 90), mk(3, 60)}
	before := ids(in)
	_ = SelectTopN(in, 2)
	if !eq(ids(in), before) {
		t.Errorf("入参不应被修改，前 %v 后 %v", before, ids(in))
	}
}
