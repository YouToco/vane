// @vitest-environment jsdom

import { cleanup, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, test, vi } from "vitest";

import { api, type CallCostLedgerItem } from "@/api";
import CallCostLedger, {
  describeCallCostFormula,
  describeCallUsage,
  formatCallCostAmount,
} from "@/pages/CallCostLedger";

const llmReceipt: CallCostLedgerItem = {
  kind: "llm",
  id: 11,
  created_at: "2026-07-30T12:00:00Z",
  provider: "kimi",
  resource: "kimi-k2.6",
  meter: "llm_tokens",
  pricing_status: "calculated",
  cost_amount: 0.000755,
  cost_currency: "USD",
  pricing_rule: {
    id: 7,
    provider: "kimi",
    resource: "kimi-k2.6",
    meter: "llm_tokens",
    currency: "USD",
    input_cache_hit_per_million: 0.16,
    input_cache_miss_per_million: 0.95,
    output_per_million: 4,
    effective_from: "2026-07-30T00:00:00Z",
    source_url: "https://platform.kimi.ai/docs/pricing/chat-k26",
    note: "official",
    created_at: "2026-07-30T00:00:00Z",
  },
  llm_usage: {
    prompt_tokens: 1_000,
    prompt_cache_hit_tokens: 500,
    prompt_cache_miss_tokens: 500,
    completion_tokens: 50,
    reasoning_tokens: 10,
  },
  trace_id: "trace-ledger-11",
  task_id: "task-11",
  task_title: "跟踪 AI 模型发布",
  span_name: "issue_synthesis",
  duration_ms: 6823,
  failed: false,
};

const toolReceipt: CallCostLedgerItem = {
  kind: "tool",
  id: 12,
  created_at: "2026-07-30T12:01:00Z",
  provider: "exa",
  resource: "/search",
  meter: "request",
  pricing_status: "provider_reported",
  cost_amount: 0.005,
  cost_currency: "USD",
  tool_usage: {
    tool_name: "exa_search",
    tool_kind: "exa_fetch",
    endpoint_path: "/search",
    usage_quantity: 5,
    http_status: 200,
  },
  trace_id: "trace-ledger-12",
  duration_ms: 800,
  failed: false,
};

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("call cost ledger", () => {
  test("renders exact amount, formula, task attribution and provider source", async () => {
    vi.spyOn(api, "adminListCostCalls").mockResolvedValue({
      items: [llmReceipt, toolReceipt],
    });

    render(<CallCostLedger />);

    expect((await screen.findAllByText("kimi-k2.6")).length).toBeGreaterThan(0);
    expect(screen.getAllByText("跟踪 AI 模型发布").length).toBeGreaterThan(0);
    expect(screen.getAllByText("$0.000755").length).toBeGreaterThan(0);
    expect(screen.getAllByText("供应商实报").length).toBeGreaterThan(0);
    expect(
      screen.getAllByText(/500 缓存命中输入/).length,
    ).toBeGreaterThan(0);
    expect(screen.getAllByText(/其中推理 10/).length).toBeGreaterThan(0);
    const sources = screen.getAllByRole("link", { name: /查看定价来源/ });
    expect(sources.length).toBeGreaterThan(0);
    expect(sources[0].getAttribute("href")).toContain("platform.kimi.ai");
  });

  test("keeps the opaque cursor and applies exact task filters", async () => {
    const list = vi
      .spyOn(api, "adminListCostCalls")
      .mockResolvedValueOnce({
        items: [llmReceipt],
        next_page_token: "opaque-next",
      })
      .mockResolvedValueOnce({ items: [toolReceipt] })
      .mockResolvedValueOnce({ items: [llmReceipt] });

    render(<CallCostLedger />);
    await screen.findAllByText("kimi-k2.6");
    await userEvent.click(screen.getByRole("button", { name: "加载更多" }));
    await waitFor(() => expect(list).toHaveBeenCalledTimes(2));
    expect(list.mock.calls[1]).toEqual([{}, "opaque-next", 50]);

    await userEvent.type(screen.getByLabelText("任务 ID"), " task-11 ");
    await userEvent.click(screen.getByRole("button", { name: "筛选" }));
    await waitFor(() => expect(list).toHaveBeenCalledTimes(3));
    expect(list.mock.calls[2]).toEqual([
      { task_id: "task-11" },
      undefined,
      50,
    ]);
  });

  test("explains direct, estimated and unpriced amounts without inventing prices", () => {
    expect(describeCallCostFormula(toolReceipt)).toContain("供应商响应直接返回");
    expect(
      describeCallCostFormula({
        ...llmReceipt,
        pricing_status: "estimated",
        llm_usage: {
          prompt_tokens: 1_000,
          completion_tokens: 50,
        },
      }),
    ).toContain("全部按缓存未命中价");
    expect(
      describeCallCostFormula({
        ...toolReceipt,
        pricing_status: "unpriced",
        cost_amount: undefined,
        cost_currency: undefined,
      }),
    ).toContain("不猜测金额");
    expect(
      formatCallCostAmount({
        ...toolReceipt,
        cost_amount: undefined,
        cost_currency: undefined,
      }),
    ).toBe("待定价");
    expect(describeCallUsage(llmReceipt)).toContain(
      "输入 1,000 token （缓存命中 500 / 未命中 500）",
    );
  });
});
