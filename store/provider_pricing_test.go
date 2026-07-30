package store

import (
	"errors"
	"math"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/types"
)

func TestProviderPricingLedger(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL，跳过供应商动态定价集成测试")
	}
	ctx := t.Context()
	if err := Migrate(ctx, dbURL); err != nil {
		t.Fatalf("Migrate() 失败: %v", err)
	}
	st, err := New(ctx, dbURL)
	if err != nil {
		t.Fatalf("New() 失败: %v", err)
	}
	registerStoreClose(t, st)

	suffix := uuid.NewString()
	provider := "pricing-" + suffix
	model := "model-" + suffix
	endpoint := "/endpoint-" + suffix
	openID := "pricing-user-" + suffix
	var userID int64
	if err := st.pool.QueryRow(ctx,
		`INSERT INTO users(feishu_open_id, name) VALUES ($1, 'pricing test') RETURNING id`,
		openID,
	).Scan(&userID); err != nil {
		t.Fatalf("创建价格管理员测试用户: %v", err)
	}
	t.Cleanup(func() {
		cctx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(cctx, t, st, `DELETE FROM llm_calls WHERE provider=$1`, provider)
		cleanupExec(cctx, t, st, `DELETE FROM tool_calls WHERE provider=$1`, provider)
		cleanupExec(cctx, t, st, `DELETE FROM provider_price_rules WHERE provider=$1`, provider)
		cleanupExec(cctx, t, st, `DELETE FROM users WHERE id=$1`, userID)
	})

	hit, miss, output := 1.0, 2.0, 3.0
	firstFrom := time.Now().UTC().Add(-time.Minute)
	firstInput := ReplaceProviderPriceRuleInput{
		Provider: provider, Resource: model, Meter: PriceMeterLLMTokens, Currency: "USD",
		InputCacheHitPerMillion: &hit, InputCacheMissPerMillion: &miss,
		OutputPerMillion: &output, EffectiveFrom: firstFrom,
		SourceURL: "https://example.com/pricing", Note: "first",
		CreatedBy: userID, ChangeID: "pricing-first-" + suffix,
	}
	first, err := st.ReplaceProviderPriceRule(ctx, firstInput)
	if err != nil {
		t.Fatalf("创建首个模型价格: %v", err)
	}
	replayed, err := st.ReplaceProviderPriceRule(ctx, firstInput)
	if err != nil || replayed.ID != first.ID {
		t.Fatalf("价格幂等重放 = rule %v err %v", replayed, err)
	}
	changedReplay := firstInput
	changedReplay.Note = "different"
	if _, err := st.ReplaceProviderPriceRule(ctx, changedReplay); err == nil ||
		!errors.Is(err, types.ErrConflict) {
		t.Fatalf("同幂等键异请求应冲突，实际 %v", err)
	}
	if _, err := st.pool.Exec(ctx,
		`UPDATE provider_price_rules
		    SET input_cache_hit_per_million = input_cache_hit_per_million + 1
		  WHERE id=$1`,
		first.ID,
	); err == nil {
		t.Fatal("数据库允许原地篡改历史价格版本")
	}

	cacheHit, cacheMiss, reasoning := 250_000, 750_000, 125_000
	callID, err := st.InsertLLMCall(ctx, &types.LLMCall{
		TraceID: "pricing-llm-exact-" + suffix, SpanName: "pricing_test",
		Provider: provider, Model: model,
		PromptTokens: 1_000_000, CompletionTokens: 500_000,
		PromptCacheHitTokens: &cacheHit, PromptCacheMissTokens: &cacheMiss,
		ReasoningTokens: &reasoning,
	})
	if err != nil {
		t.Fatalf("写入精确模型用量: %v", err)
	}
	assertPricingReceipt(t, st, "llm_calls", callID, first.ID, "calculated", "USD", 3.25)

	estimatedID, err := st.InsertLLMCall(ctx, &types.LLMCall{
		TraceID: "pricing-llm-estimated-" + suffix, SpanName: "pricing_test",
		Provider: provider, Model: model,
		PromptTokens: 1_000_000, CompletionTokens: 500_000,
	})
	if err != nil {
		t.Fatalf("写入无缓存明细模型用量: %v", err)
	}
	assertPricingReceipt(t, st, "llm_calls", estimatedID, first.ID, "estimated", "USD", 3.5)

	unpricedID, err := st.InsertLLMCall(ctx, &types.LLMCall{
		TraceID: "pricing-llm-unpriced-" + suffix, SpanName: "pricing_test",
		Provider: provider, Model: "unknown-" + suffix,
		PromptTokens: 123, CompletionTokens: 45,
	})
	if err != nil {
		t.Fatalf("写入未定价模型用量: %v", err)
	}
	assertUnpricedReceipt(t, st, "llm_calls", unpricedID)

	hit2, miss2, output2 := 2.0, 4.0, 6.0
	second, err := st.ReplaceProviderPriceRule(ctx, ReplaceProviderPriceRuleInput{
		Provider: provider, Resource: model, Meter: PriceMeterLLMTokens, Currency: "USD",
		InputCacheHitPerMillion: &hit2, InputCacheMissPerMillion: &miss2,
		OutputPerMillion: &output2, EffectiveFrom: time.Now().UTC().Add(-time.Second),
		SourceURL: "https://example.com/pricing-v2", Note: "second",
		CreatedBy: userID, ChangeID: "pricing-second-" + suffix,
	})
	if err != nil {
		t.Fatalf("更新模型价格: %v", err)
	}
	secondCallID, err := st.InsertLLMCall(ctx, &types.LLMCall{
		TraceID: "pricing-llm-second-" + suffix, SpanName: "pricing_test",
		Provider: provider, Model: model,
		PromptTokens: 1_000_000, CompletionTokens: 500_000,
		PromptCacheHitTokens: &cacheHit, PromptCacheMissTokens: &cacheMiss,
	})
	if err != nil {
		t.Fatalf("写入新价格模型用量: %v", err)
	}
	assertPricingReceipt(t, st, "llm_calls", secondCallID, second.ID, "calculated", "USD", 6.5)
	assertPricingReceipt(t, st, "llm_calls", callID, first.ID, "calculated", "USD", 3.25)

	// Hold the per-resource lock so Replace has acquired its global exclusive
	// ledger fence but cannot publish the new version yet. A call that starts in
	// that window must wait, then bind the newly committed price—not an old MVCC
	// snapshot whose interval has just been closed.
	blocker, err := st.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("开始价格并发阻塞事务: %v", err)
	}
	defer func() { _ = blocker.Rollback(ctx) }()
	resourceLockKey := provider + "\x1f" + model + "\x1f" + string(PriceMeterLLMTokens)
	if _, err := blocker.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`,
		resourceLockKey,
	); err != nil {
		t.Fatalf("锁定测试价格资源: %v", err)
	}
	hit3, miss3, output3 := 3.0, 6.0, 9.0
	type replaceResult struct {
		rule *ProviderPriceRule
		err  error
	}
	replaceDone := make(chan replaceResult, 1)
	go func() {
		rule, replaceErr := st.ReplaceProviderPriceRule(ctx, ReplaceProviderPriceRuleInput{
			Provider: provider, Resource: model, Meter: PriceMeterLLMTokens, Currency: "USD",
			InputCacheHitPerMillion: &hit3, InputCacheMissPerMillion: &miss3,
			OutputPerMillion: &output3,
			SourceURL:        "https://example.com/pricing-v3", Note: "concurrent",
			CreatedBy: userID, ChangeID: "pricing-third-" + suffix,
		})
		replaceDone <- replaceResult{rule: rule, err: replaceErr}
	}()
	globalFenceHeld := false
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
		probe, probeErr := st.pool.Begin(ctx)
		if probeErr != nil {
			t.Fatalf("开始价格 fence 探针: %v", probeErr)
		}
		if _, probeErr = probe.Exec(ctx, `SET LOCAL lock_timeout='20ms'`); probeErr == nil {
			_, probeErr = probe.Exec(ctx,
				`SELECT pg_advisory_xact_lock_shared(hashtextextended($1, 0))`,
				providerPricingLedgerLock,
			)
		}
		_ = probe.Rollback(ctx)
		if probeErr != nil {
			globalFenceHeld = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !globalFenceHeld {
		t.Fatal("价格替换未在时限内持有全局 exclusive fence")
	}
	type callResult struct {
		id  int64
		err error
	}
	callDone := make(chan callResult, 1)
	go func() {
		id, callErr := st.InsertLLMCall(ctx, &types.LLMCall{
			TraceID: "pricing-llm-concurrent-" + suffix, SpanName: "pricing_test",
			Provider: provider, Model: model,
			PromptTokens: 1_000_000, CompletionTokens: 500_000,
			PromptCacheHitTokens: &cacheHit, PromptCacheMissTokens: &cacheMiss,
		})
		callDone <- callResult{id: id, err: callErr}
	}()
	select {
	case result := <-callDone:
		t.Fatalf("调用越过未提交的价格替换 fence: id=%d err=%v", result.id, result.err)
	case <-time.After(100 * time.Millisecond):
	}
	if err := blocker.Rollback(ctx); err != nil {
		t.Fatalf("释放价格资源锁: %v", err)
	}
	var third *ProviderPriceRule
	select {
	case result := <-replaceDone:
		if result.err != nil {
			t.Fatalf("并发更新模型价格: %v", result.err)
		}
		third = result.rule
	case <-time.After(5 * time.Second):
		t.Fatal("并发价格更新未结束")
	}
	var concurrentCallID int64
	select {
	case result := <-callDone:
		if result.err != nil {
			t.Fatalf("写入等待新价格的模型用量: %v", result.err)
		}
		concurrentCallID = result.id
	case <-time.After(5 * time.Second):
		t.Fatal("等待新价格的模型调用未结束")
	}
	assertPricingReceipt(t, st, "llm_calls", concurrentCallID,
		third.ID, "calculated", "USD", 9.75)

	unit := 0.25
	exactToolRule, err := st.ReplaceProviderPriceRule(ctx, ReplaceProviderPriceRuleInput{
		Provider: provider, Resource: endpoint, Meter: PriceMeterRequest, Currency: "CNY",
		RequestUnitPrice: &unit, RequestIncludedQuantity: ptrFloat64(1),
		RequestAdditionalUnitPrice: &unit, EffectiveFrom: firstFrom,
		SourceURL: "https://example.com/tool-pricing", CreatedBy: userID,
		ChangeID: "pricing-tool-" + suffix,
	})
	if err != nil {
		t.Fatalf("创建工具价格: %v", err)
	}
	okStatus := 200
	toolID, err := st.InsertToolCall(ctx, &types.ToolCall{
		TraceID: "pricing-tool-exact-" + suffix, Provider: provider,
		ToolName: "pricing_tool", ToolKind: types.ToolCallKindTikHubEndpoint,
		EndpointPath: endpoint, HTTPStatus: &okStatus, UsageQuantity: 2,
	})
	if err != nil {
		t.Fatalf("写入工具用量: %v", err)
	}
	assertPricingReceipt(t, st, "tool_calls", toolID, exactToolRule.ID, "calculated", "CNY", 0.5)

	exaTraceID := "pricing-exa-search-" + suffix
	t.Cleanup(func() {
		cctx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(cctx, t, st, `DELETE FROM tool_calls WHERE trace_id=$1`, exaTraceID)
	})
	var exaRuleID int64
	if err := st.pool.QueryRow(ctx,
		`SELECT id
		   FROM provider_price_rules
		  WHERE provider='exa' AND resource='/search' AND meter='request'
		    AND effective_to IS NULL`,
	).Scan(&exaRuleID); err != nil {
		t.Fatalf("读取 Exa 官方初始价格: %v", err)
	}
	exaSearchID, err := st.InsertToolCall(ctx, &types.ToolCall{
		TraceID: exaTraceID, Provider: "exa",
		ToolName: "exa:search", ToolKind: types.ToolCallKindExaFetch,
		EndpointPath: "/search", HTTPStatus: &okStatus, UsageQuantity: 20,
	})
	if err != nil {
		t.Fatalf("写入 Exa 20 结果搜索用量: %v", err)
	}
	// 官方规则：$7/千次包含 10 条结果，超出的每条按 $1/千次。
	assertPricingReceipt(t, st, "tool_calls", exaSearchID, exaRuleID, "calculated", "USD", 0.017)

	tikhubTraceID := "pricing-tikhub-xhs-" + suffix
	tikhubUnknownTraceID := "pricing-tikhub-unknown-" + suffix
	t.Cleanup(func() {
		cctx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(cctx, t, st, `DELETE FROM tool_calls WHERE trace_id = ANY($1)`,
			[]string{tikhubTraceID, tikhubUnknownTraceID})
	})
	const xhsSearchPath = "/api/v1/xiaohongshu/app_v2/search_notes"
	var tikhubRuleID int64
	if err := st.pool.QueryRow(ctx,
		`SELECT id
		   FROM provider_price_rules
		  WHERE provider='tikhub' AND resource=$1 AND meter='request'
		    AND effective_to IS NULL`,
		xhsSearchPath,
	).Scan(&tikhubRuleID); err != nil {
		t.Fatalf("读取 TikHub 小红书价格: %v", err)
	}
	tikhubID, err := st.InsertToolCall(ctx, &types.ToolCall{
		TraceID: tikhubTraceID, Provider: "tikhub",
		ToolName:     "xiaohongshu_app_v2_search_notes",
		ToolKind:     types.ToolCallKindBindingFetch,
		EndpointPath: xhsSearchPath, HTTPStatus: &okStatus, UsageQuantity: 1,
	})
	if err != nil {
		t.Fatalf("写入 TikHub 小红书用量: %v", err)
	}
	assertPricingReceipt(t, st, "tool_calls", tikhubID, tikhubRuleID, "calculated", "USD", 0.01)

	tikhubUnknownID, err := st.InsertToolCall(ctx, &types.ToolCall{
		TraceID: tikhubUnknownTraceID, Provider: "tikhub",
		ToolName:     "unknown_tikhub_tool",
		ToolKind:     types.ToolCallKindBindingFetch,
		EndpointPath: "/api/v1/unknown", HTTPStatus: &okStatus, UsageQuantity: 1,
	})
	if err != nil {
		t.Fatalf("写入未知 TikHub 端点用量: %v", err)
	}
	var tikhubWildcardRuleID int64
	if err := st.pool.QueryRow(ctx,
		`SELECT id
		   FROM provider_price_rules
		  WHERE provider='tikhub' AND resource='*' AND meter='request'
		    AND effective_to IS NULL`,
	).Scan(&tikhubWildcardRuleID); err != nil {
		t.Fatalf("读取 TikHub 保守兜底价格: %v", err)
	}
	assertPricingReceipt(t, st, "tool_calls", tikhubUnknownID,
		tikhubWildcardRuleID, "estimated", "USD", 0.01)
	noContentStatus := 204
	tikhubNoChargeID, err := st.InsertToolCall(ctx, &types.ToolCall{
		TraceID: "pricing-tikhub-204-" + suffix, Provider: "tikhub",
		ToolName:     "xiaohongshu_app_v2_search_notes",
		ToolKind:     types.ToolCallKindBindingFetch,
		EndpointPath: xhsSearchPath, HTTPStatus: &noContentStatus, UsageQuantity: 1,
	})
	if err != nil {
		t.Fatalf("写入 TikHub 204 用量: %v", err)
	}
	assertUnpricedReceipt(t, st, "tool_calls", tikhubNoChargeID)

	upstreamAmount := 0.12345678
	reportedID, err := st.InsertToolCall(ctx, &types.ToolCall{
		TraceID: "pricing-tool-provider-" + suffix, Provider: provider,
		ToolName: "pricing_tool", ToolKind: types.ToolCallKindTikHubEndpoint,
		EndpointPath: endpoint, HTTPStatus: &okStatus, CostUSD: &upstreamAmount,
	})
	if err != nil {
		t.Fatalf("写入上游金额工具调用: %v", err)
	}
	assertPricingReceipt(t, st, "tool_calls", reportedID, 0, "provider_reported", "USD", upstreamAmount)

	wildcardUnit := 0.75
	wildcardRule, err := st.ReplaceProviderPriceRule(ctx, ReplaceProviderPriceRuleInput{
		Provider: provider, Resource: "*", Meter: PriceMeterRequest, Currency: "USD",
		RequestUnitPrice: &wildcardUnit, RequestIncludedQuantity: ptrFloat64(1),
		RequestAdditionalUnitPrice: &wildcardUnit, EffectiveFrom: firstFrom,
		SourceURL: "https://example.com/tool-default", CreatedBy: userID,
		ChangeID: "pricing-tool-wildcard-" + suffix,
	})
	if err != nil {
		t.Fatalf("创建工具兜底价格: %v", err)
	}
	wildcardID, err := st.InsertToolCall(ctx, &types.ToolCall{
		TraceID: "pricing-tool-estimated-" + suffix, Provider: provider,
		ToolName: "pricing_tool", ToolKind: types.ToolCallKindTikHubEndpoint,
		EndpointPath: "/other-" + suffix, HTTPStatus: &okStatus,
	})
	if err != nil {
		t.Fatalf("写入兜底工具用量: %v", err)
	}
	assertPricingReceipt(t, st, "tool_calls", wildcardID, wildcardRule.ID, "estimated", "USD", 0.75)

	badStatus := 500
	failedID, err := st.InsertToolCall(ctx, &types.ToolCall{
		TraceID: "pricing-tool-failed-" + suffix, Provider: provider,
		ToolName: "pricing_tool", ToolKind: types.ToolCallKindTikHubEndpoint,
		EndpointPath: endpoint, HTTPStatus: &badStatus, ErrorType: types.ToolErrHTTP,
	})
	if err != nil {
		t.Fatalf("写入失败工具用量: %v", err)
	}
	assertUnpricedReceipt(t, st, "tool_calls", failedID)

	t.Run("concurrent replacements keep one open interval", func(t *testing.T) {
		resource := "concurrent-" + suffix
		inputs := []ReplaceProviderPriceRuleInput{
			{
				Provider: provider, Resource: resource, Meter: PriceMeterRequest,
				Currency: "USD", RequestUnitPrice: &unit,
				RequestIncludedQuantity:    ptrFloat64(1),
				RequestAdditionalUnitPrice: &unit,
				EffectiveFrom:              time.Now().UTC().Add(-2 * time.Second),
				SourceURL:                  "https://example.com/concurrent-a", CreatedBy: userID,
				ChangeID: "pricing-concurrent-a-" + suffix,
			},
			{
				Provider: provider, Resource: resource, Meter: PriceMeterRequest,
				Currency: "USD", RequestUnitPrice: &wildcardUnit,
				RequestIncludedQuantity:    ptrFloat64(1),
				RequestAdditionalUnitPrice: &wildcardUnit,
				EffectiveFrom:              time.Now().UTC().Add(-time.Second),
				SourceURL:                  "https://example.com/concurrent-b", CreatedBy: userID,
				ChangeID: "pricing-concurrent-b-" + suffix,
			},
		}
		var wg sync.WaitGroup
		errs := make([]error, len(inputs))
		for i := range inputs {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				_, errs[i] = st.ReplaceProviderPriceRule(ctx, inputs[i])
			}(i)
		}
		wg.Wait()
		succeeded := 0
		for _, err := range errs {
			if err == nil {
				succeeded++
			}
		}
		if succeeded == 0 {
			t.Fatalf("concurrent updates both failed: %v", errs)
		}
		var open int
		if err := st.pool.QueryRow(ctx,
			`SELECT count(*) FROM provider_price_rules
			  WHERE provider=$1 AND resource=$2 AND meter='request'
			    AND effective_to IS NULL`,
			provider, resource,
		).Scan(&open); err != nil {
			t.Fatalf("count concurrent open intervals: %v", err)
		}
		if open != 1 {
			t.Fatalf("concurrent updates left %d open intervals, want 1", open)
		}
	})
}

func ptrFloat64(value float64) *float64 {
	return &value
}

func assertPricingReceipt(
	t *testing.T,
	st *Store,
	table string,
	id, wantRuleID int64,
	wantStatus, wantCurrency string,
	wantAmount float64,
) {
	t.Helper()
	var ruleID *int64
	var status string
	var amount *float64
	var currency *string
	if err := st.pool.QueryRow(t.Context(),
		`SELECT pricing_rule_id, pricing_status, cost_amount::float8, cost_currency
		   FROM `+table+` WHERE id=$1`,
		id,
	).Scan(&ruleID, &status, &amount, &currency); err != nil {
		t.Fatalf("读取 %s 定价回执: %v", table, err)
	}
	if wantRuleID == 0 {
		if ruleID != nil {
			t.Fatalf("%s rule_id=%v, want nil", table, ruleID)
		}
	} else if ruleID == nil || *ruleID != wantRuleID {
		t.Fatalf("%s rule_id=%v, want %d", table, ruleID, wantRuleID)
	}
	if status != wantStatus || currency == nil || *currency != wantCurrency ||
		amount == nil || math.Abs(*amount-wantAmount) > 1e-8 {
		t.Fatalf("%s receipt=(%s,%v,%v), want=(%s,%s,%.8f)",
			table, status, amount, currency, wantStatus, wantCurrency, wantAmount)
	}
}

func assertUnpricedReceipt(t *testing.T, st *Store, table string, id int64) {
	t.Helper()
	var ruleID *int64
	var status string
	var amount *float64
	var currency *string
	if err := st.pool.QueryRow(t.Context(),
		`SELECT pricing_rule_id, pricing_status, cost_amount::float8, cost_currency
		   FROM `+table+` WHERE id=$1`,
		id,
	).Scan(&ruleID, &status, &amount, &currency); err != nil {
		t.Fatalf("读取 %s 未定价回执: %v", table, err)
	}
	if ruleID != nil || status != "unpriced" || amount != nil || currency != nil {
		t.Fatalf("%s 应保持未定价，实际 rule=%v status=%s amount=%v currency=%v",
			table, ruleID, status, amount, currency)
	}
}
