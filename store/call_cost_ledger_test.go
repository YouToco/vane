package store

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/types"
)

func TestCallCostLedgerCursorBindsFilters(t *testing.T) {
	cursor := callCostLedgerCursor{
		Version: 1, BeforeAt: testNowUTC(), BeforeKind: CallCostKindTool,
		BeforeID: 42, PageSize: 25, Kind: CallCostKindTool,
		Provider: "exa", PricingStatus: "calculated", TaskID: "task-1",
	}
	token, err := encodeCallCostLedgerCursor(cursor)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeCallCostLedgerCursor(token)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.PageSize != cursor.PageSize || decoded.Provider != cursor.Provider ||
		decoded.BeforeID != cursor.BeforeID || !decoded.BeforeAt.Equal(cursor.BeforeAt) {
		t.Fatalf("cursor round trip drift: got %+v want %+v", decoded, cursor)
	}

	for _, invalid := range []string{
		"not-base64",
		base64.RawURLEncoding.EncodeToString([]byte(`{}`)),
		base64.RawURLEncoding.EncodeToString([]byte(`{"v":1,"unknown":true}`)),
		base64.RawURLEncoding.EncodeToString([]byte(`{"v":1} trailing`)),
	} {
		if _, err := decodeCallCostLedgerCursor(invalid); err == nil ||
			!errors.Is(err, types.ErrValidation) {
			t.Fatalf("invalid cursor %q should fail validation, got %v", invalid, err)
		}
	}
}

func TestListCallCostLedgerExactReceiptsAndPagination(t *testing.T) {
	f := newTaskRunSnapshotFixture(t)
	ctx := t.Context()
	taskID := f.taskID()
	f.createApprovedTask(t, taskID, 1)
	params := f.params(taskID, "run-"+uuid.NewString())
	snapshot, err := f.st.createOrGetTaskRunSnapshot(ctx, params)
	if err != nil {
		t.Fatalf("create task run snapshot: %v", err)
	}
	if snapshot.ID <= 0 {
		t.Fatal("snapshot id missing")
	}
	trace := params.TemporalWorkflowID
	batchID, err := f.st.CreatePushBatchIdempotent(ctx, f.userID, trace, taskID)
	if err != nil {
		t.Fatalf("create push batch: %v", err)
	}

	provider := "ledger-" + uuid.NewString()
	model := "model-" + uuid.NewString()
	endpoint := "/endpoint/" + uuid.NewString()
	hitPrice, missPrice, outputPrice := 1.0, 2.0, 3.0
	llmRule, err := f.st.ReplaceProviderPriceRule(ctx, ReplaceProviderPriceRuleInput{
		Provider: provider, Resource: model, Meter: PriceMeterLLMTokens, Currency: "USD",
		InputCacheHitPerMillion: &hitPrice, InputCacheMissPerMillion: &missPrice,
		OutputPerMillion: &outputPrice,
		SourceURL:        "https://example.com/llm-pricing", Note: "ledger llm rule",
		CreatedBy: f.userID, ChangeID: "ledger-llm-" + uuid.NewString(),
	})
	if err != nil {
		t.Fatalf("create llm price: %v", err)
	}
	unitPrice, included, additional := 0.01, 1.0, 0.002
	toolRule, err := f.st.ReplaceProviderPriceRule(ctx, ReplaceProviderPriceRuleInput{
		Provider: provider, Resource: endpoint, Meter: PriceMeterRequest, Currency: "CNY",
		RequestUnitPrice: &unitPrice, RequestIncludedQuantity: &included,
		RequestAdditionalUnitPrice: &additional,
		SourceURL:                  "https://example.com/tool-pricing", Note: "ledger tool rule",
		CreatedBy: f.userID, ChangeID: "ledger-tool-" + uuid.NewString(),
	})
	if err != nil {
		t.Fatalf("create tool price: %v", err)
	}

	hitTokens, missTokens, reasoningTokens := 250_000, 750_000, 25
	llmID, err := f.st.InsertLLMCall(ctx, &types.LLMCall{
		TenantID: &f.tenantID, UserID: &f.userID, TraceID: trace,
		SpanName: "issue_synthesis", Provider: provider, Model: model,
		SystemPrompt: "SECRET_SYSTEM_CANARY", UserPrompt: "SECRET_USER_CANARY",
		Completion: "SECRET_COMPLETION_CANARY", Error: "",
		PromptTokens: 1_000_000, PromptCacheHitTokens: &hitTokens,
		PromptCacheMissTokens: &missTokens, CompletionTokens: 100,
		ReasoningTokens: &reasoningTokens, LatencyMs: 1234,
	})
	if err != nil {
		t.Fatalf("insert llm call: %v", err)
	}
	httpOK := 200
	toolID, err := f.st.InsertToolCall(ctx, &types.ToolCall{
		TenantID: &f.tenantID, UserID: &f.userID, TraceID: trace,
		ToolName: "fetch_endpoint", ToolKind: types.ToolCallKindTikHubEndpoint,
		Provider: provider, EndpointPath: endpoint, HTTPStatus: &httpOK,
		Arguments:     json.RawMessage(`{"secret":"SECRET_ARGUMENT_CANARY"}`),
		ResultPreview: "SECRET_RESULT_CANARY", Error: "", DurationMs: 432,
		UsageQuantity: 4,
	})
	if err != nil {
		t.Fatalf("insert tool call: %v", err)
	}
	// Force an exact timestamp tie across the two physical tables. The cursor
	// must use kind+id after created_at or one receipt will be repeated/skipped.
	tiedAt := testNowUTC()
	if _, err := f.st.pool.Exec(ctx,
		`UPDATE llm_calls SET created_at=$1 WHERE id=$2`, tiedAt, llmID,
	); err != nil {
		t.Fatalf("tie llm receipt timestamp: %v", err)
	}
	if _, err := f.st.pool.Exec(ctx,
		`UPDATE tool_calls SET created_at=$1 WHERE id=$2`, tiedAt, toolID,
	); err != nil {
		t.Fatalf("tie tool receipt timestamp: %v", err)
	}

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(cleanupCtx, t, f.st, `DELETE FROM tool_calls WHERE id=$1`, toolID)
		cleanupExec(cleanupCtx, t, f.st, `DELETE FROM llm_calls WHERE id=$1`, llmID)
		cleanupExec(cleanupCtx, t, f.st, `DELETE FROM push_batches WHERE id=$1`, batchID)
		cleanupExec(cleanupCtx, t, f.st, `DELETE FROM provider_price_rules WHERE provider=$1`, provider)
	})

	page1, next, err := f.st.ListCallCostLedger(ctx, CallCostLedgerQuery{
		PageSize: 1, Provider: provider, TaskID: taskID,
	})
	if err != nil {
		t.Fatalf("list first page: %v", err)
	}
	if len(page1) != 1 || next == "" {
		t.Fatalf("first page shape: len=%d next=%q", len(page1), next)
	}
	page2, next2, err := f.st.ListCallCostLedger(ctx, CallCostLedgerQuery{
		PageSize: 1, PageToken: next, Provider: provider, TaskID: taskID,
	})
	if err != nil {
		t.Fatalf("list second page: %v", err)
	}
	if len(page2) != 1 {
		t.Fatalf("second page len=%d", len(page2))
	}
	if page1[0].ID == page2[0].ID && page1[0].Kind == page2[0].Kind {
		t.Fatal("keyset pagination repeated a receipt")
	}
	_ = next2

	all := append(page1, page2...)
	var llmItem, toolItem *CallCostLedgerItem
	for i := range all {
		switch all[i].Kind {
		case CallCostKindLLM:
			llmItem = &all[i]
		case CallCostKindTool:
			toolItem = &all[i]
		}
	}
	if llmItem == nil || llmItem.ID != llmID || llmItem.LLMUsage == nil ||
		llmItem.PricingRule == nil || llmItem.PricingRule.ID != llmRule.ID ||
		llmItem.TaskID != taskID || llmItem.CostAmount == nil {
		t.Fatalf("llm receipt incomplete: %+v", llmItem)
	}
	if math.Abs(*llmItem.CostAmount-1.7503) > 1e-9 {
		t.Fatalf("llm exact amount=%v want 1.7503", *llmItem.CostAmount)
	}
	if toolItem == nil || toolItem.ID != toolID || toolItem.ToolUsage == nil ||
		toolItem.PricingRule == nil || toolItem.PricingRule.ID != toolRule.ID ||
		toolItem.TaskID != taskID || toolItem.CostCurrency == nil ||
		*toolItem.CostCurrency != "CNY" {
		t.Fatalf("tool receipt incomplete: %+v", toolItem)
	}
	if toolItem.CostAmount == nil || math.Abs(*toolItem.CostAmount-0.016) > 1e-9 {
		t.Fatalf("tool exact amount=%v want 0.016", toolItem.CostAmount)
	}

	wire, err := json.Marshal(all)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{
		"SECRET_SYSTEM_CANARY", "SECRET_USER_CANARY", "SECRET_COMPLETION_CANARY",
		"SECRET_ARGUMENT_CANARY", "SECRET_RESULT_CANARY",
	} {
		if bytes.Contains(wire, []byte(secret)) {
			t.Fatalf("ledger leaked sensitive payload %q: %s", secret, wire)
		}
	}

	if _, _, err := f.st.ListCallCostLedger(ctx, CallCostLedgerQuery{
		PageSize: 1, PageToken: next, Provider: provider, TaskID: "other-task",
	}); err == nil || !errors.Is(err, types.ErrValidation) {
		t.Fatalf("cursor replay under changed filters should fail: %v", err)
	}
}

func testNowUTC() time.Time {
	return time.Date(2026, time.July, 30, 12, 34, 56, 123456000, time.UTC)
}
