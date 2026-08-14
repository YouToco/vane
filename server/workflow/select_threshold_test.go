package workflow

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/YouToco/vane/types"
)

// 本文件锁任务门槛过滤（契约 §6.1，2026-07-19 拍板）：Select 的过滤行为、
// 降级路径、空批文案。2026-07-19 07:26 UTC 五张 0 分卡（deliveries 155-159）
// 是这些用例存在的理由。

func scoredWith(scores ...float64) []types.ScoredItem {
	out := make([]types.ScoredItem, len(scores))
	for i, s := range scores {
		out[i] = types.ScoredItem{Item: types.ContentItem{ID: int64(i + 1)}, Score: s}
	}
	return out
}

func selectActivities(st *fakeStore) *Activities {
	return NewActivities(fakeFetcher{}, fakeScorer{}, fakeCardGen{}, &fakePusher{}, st, fakeFeishu{}, nil, nil, nil, nil)
}

func selectedIDs(items []types.ScoredItem) []int64 {
	ids := make([]int64, len(items))
	for i, it := range items {
		ids[i] = it.Item.ID
	}
	return ids
}

func TestSelect_GlobalFloorFiltersSemanticBand(t *testing.T) {
	// 无 ScheduleID（push_now / 即时触发）：全局兜底生效——0-20"不该推"档全滤，
	// 含边界 20 本身；21 是最低保留分。这正是修复 5 张 0 分卡的那道底线。
	a := selectActivities(&fakeStore{})
	got, err := a.Select(context.Background(), SelectIn{
		UserID: 1, TraceID: "tr", TopN: 5,
		Scored: scoredWith(0, 15, 20, 21, 55),
	})
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("兜底档应只保留 21 与 55 两条，实得 %d 条（ids=%v）", len(got), selectedIDs(got))
	}
	for _, it := range got {
		if it.Score <= 20 {
			t.Fatalf("0-20 语义档条目泄漏进推送：score=%v", it.Score)
		}
	}
}

func TestSelect_AllIrrelevantBatchYieldsEmpty(t *testing.T) {
	// 2026-07-19 事故的精确形状：整批 0 分 → 过滤后空，而不是硬凑 TopN。
	a := selectActivities(&fakeStore{})
	got, err := a.Select(context.Background(), SelectIn{
		UserID: 1, TraceID: "tr", TopN: 5, Scored: scoredWith(0, 0, 0, 0, 0),
	})
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("整批 0 分必须全滤（空批走 Select 闸门），实得 %d 条", len(got))
	}
}

func TestSelect_TaskStrictnessRaisesFloor(t *testing.T) {
	tests := []struct {
		strictness types.PushStrictness
		wantIDs    []int64
	}{
		{types.StrictnessLoose, []int64{2, 3, 4}}, // ≥21：39/40/60
		{types.StrictnessNormal, []int64{3, 4}},   // ≥40：40/60
		{types.StrictnessStrict, []int64{4}},      // ≥60：60
	}
	for _, tc := range tests {
		t.Run(string(tc.strictness), func(t *testing.T) {
			st := &fakeStore{strictness: tc.strictness}
			a := selectActivities(st)
			got, err := a.Select(context.Background(), SelectIn{
				UserID: 1, TraceID: "tr", TopN: 5, ScheduleID: "push-1-x",
				Scored: scoredWith(20, 39, 40, 60),
			})
			if err != nil {
				t.Fatalf("Select: %v", err)
			}
			gotIDs := selectedIDs(got)
			if len(gotIDs) != len(tc.wantIDs) {
				t.Fatalf("档位 %s 应保留 %v，实得 %v", tc.strictness, tc.wantIDs, gotIDs)
			}
			want := map[int64]bool{}
			for _, id := range tc.wantIDs {
				want[id] = true
			}
			for _, id := range gotIDs {
				if !want[id] {
					t.Fatalf("档位 %s 不该保留 id=%d（实得 %v）", tc.strictness, id, gotIDs)
				}
			}
		})
	}
}

func TestSelect_StrictnessQueryFailureDegradesToFloor(t *testing.T) {
	// 档位查询失败降级兜底而非中断推送（同画像读取失败降级先例）：
	// strict 任务在 DB 抖动时按 loose 放行，但 0-20 档仍然滤——底线不因降级消失。
	st := &fakeStore{strictnessErr: errors.New("db down")}
	a := selectActivities(st)
	got, err := a.Select(context.Background(), SelectIn{
		UserID: 1, TraceID: "tr", TopN: 5, ScheduleID: "push-1-x",
		Scored: scoredWith(10, 30, 70),
	})
	if err != nil {
		t.Fatalf("档位查询失败必须降级而非上抛: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("降级后应按兜底档保留 30/70 两条，实得 %d 条", len(got))
	}
}

func TestSelect_TopNStillApplies(t *testing.T) {
	// 门槛过滤与 TopN 叠加：过滤后仍超额时按有效分取前 N。
	a := selectActivities(&fakeStore{})
	got, err := a.Select(context.Background(), SelectIn{
		UserID: 1, TraceID: "tr", TopN: 2, Scored: scoredWith(30, 40, 50, 60),
	})
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("TopN=2 应只出 2 条，实得 %d", len(got))
	}
}

func TestEmptyResultMarkdown_SelectGate(t *testing.T) {
	scored := 5
	in := NotifyEmptyIn{
		Gate:     types.BatchExitGateSelect,
		Counts:   types.PipelineCounts{Scored: &scored},
		MaxScore: 12,
	}
	tests := []struct {
		strictness types.PushStrictness
		wants      []string
	}{
		{"", []string{"5 条内容", "最高 12 分", "推送底线", "松一点"}},
		{types.StrictnessNormal, []string{"「标准」门槛（≥40 分）"}},
		{types.StrictnessStrict, []string{"「严格」门槛（≥60 分）"}},
	}
	for _, tc := range tests {
		md := emptyResultMarkdown(in, tc.strictness)
		for _, w := range tc.wants {
			if !strings.Contains(md, w) {
				t.Errorf("档位 %q 的空批文案缺 %q：%s", tc.strictness, w, md)
			}
		}
	}
}
