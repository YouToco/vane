package llm

import "testing"

// TestContextWindowTokens_KnownAndFallback 锁定「未知模型取保守下限」这条取舍方向：
// 猜小只会让内联上限偏小（内容仍可经句柄取回），猜大会直接把请求打爆。
func TestContextWindowTokens_KnownAndFallback(t *testing.T) {
	if got := ContextWindowTokens("deepseek-v4-pro"); got != 1_000_000 {
		t.Errorf("已登记模型窗口漂移: %d", got)
	}
	if got := ContextWindowTokens("some-future-model"); got != fallbackContextWindowTokens {
		t.Errorf("未登记模型应取保守兜底 %d，实得 %d", fallbackContextWindowTokens, got)
	}
	// 登记表必须与 pricing 表同步覆盖生产在用的两个模型，否则派生上限会静默走兜底档。
	for _, m := range []string{"deepseek-v4-flash", "deepseek-v4-pro"} {
		if _, ok := modelContextWindows[m]; !ok {
			t.Errorf("生产在用模型 %s 未登记窗口", m)
		}
	}
}

// TestDeriveInlineLimits_LadderAndCap 锁定分档曲线与窗口占比封顶。
func TestDeriveInlineLimits_LadderAndCap(t *testing.T) {
	cases := []struct {
		window      int
		wantPerCall int
	}{
		{8_000, int(8_000 * maxWindowShare * charsPerToken)}, // 小窗口被 30% 封顶咬住
		{64_000, 16_000},                                     // 兜底档
		{128_000, 32_000},
		{200_000, 64_000},
		{1_000_000, 100_000},
	}
	for _, tc := range cases {
		got := DeriveInlineLimits(tc.window)
		if got.PerCall != tc.wantPerCall {
			t.Errorf("窗口 %d: PerCall=%d，期望 %d", tc.window, got.PerCall, tc.wantPerCall)
		}
		// 不变量：单次上限恒不超过窗口的 30%（换算成字符）。
		if capChars := int(float64(tc.window) * maxWindowShare * charsPerToken); got.PerCall > capChars {
			t.Errorf("窗口 %d: PerCall=%d 超过 30%% 封顶 %d", tc.window, got.PerCall, capChars)
		}
		// 不变量：预算 = 3×单次；保底 ≤ 单次且 ≥ 2000（除非单次本身更小）。
		if got.MsgBudget != got.PerCall*3 {
			t.Errorf("窗口 %d: MsgBudget=%d 应为 PerCall 的 3 倍", tc.window, got.MsgBudget)
		}
		if got.MinPerCall > got.PerCall {
			t.Errorf("窗口 %d: 保底 %d 超过单次上限 %d", tc.window, got.MinPerCall, got.PerCall)
		}
	}
}

// TestDeriveInlineLimits_DegenerateWindows 非法/极小窗口不得派生出负数或零上限
// ——装配疏漏（窗口传 0）必须落在保守一侧，而不是让模型收到空内容瞎猜。
func TestDeriveInlineLimits_DegenerateWindows(t *testing.T) {
	for _, w := range []int{0, -1, 1} {
		got := DeriveInlineLimits(w)
		if got.PerCall < 0 || got.MinPerCall < 0 || got.MsgBudget < 0 {
			t.Errorf("窗口 %d 派生出负数上限: %+v", w, got)
		}
		if got.MinPerCall > got.PerCall {
			t.Errorf("窗口 %d 保底超过单次上限: %+v", w, got)
		}
	}
}
