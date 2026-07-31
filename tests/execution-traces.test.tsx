// @vitest-environment jsdom

import { cleanup, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, test, vi } from "vitest";

import { api } from "@/api";
import ExecutionTraces from "@/pages/ExecutionTraces";

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("admin execution traces", () => {
  test("navigates user-task-run and renders exact prompt plus honest tool truncation", async () => {
    vi.spyOn(api, "adminTraceUsers").mockResolvedValue([
      {
        tenant_id: 7,
        user_id: 11,
        name: "Boss",
        email: "boss@example.com",
        task_count: 1,
      },
    ]);
    vi.spyOn(api, "adminTraceTasks").mockResolvedValue([
      {
        task_id: "task-v5",
        title: "Vane 官方更新交叉核验",
        status: "active",
        run_count: 1,
      },
    ]);
    vi.spyOn(api, "adminTraceRuns").mockResolvedValue([
      {
        snapshot_id: 19,
        schema_version: "vane.run-outcome/v1",
        status: "finalized",
        result: "content",
        source_coverage: "complete",
        processing: "complete",
        failure_code: "",
        failure_message: "",
        created_at: "2026-07-30T12:00:00Z",
        finalized_at: "2026-07-30T12:00:03Z",
        model_calls: 1,
        tool_calls: 1,
      },
    ]);
    const detail = vi.spyOn(api, "adminExecutionTrace").mockResolvedValue({
      run: {
        snapshot_id: 19,
        schema_version: "vane.run-outcome/v1",
        status: "finalized",
        result: "content",
        source_coverage: "complete",
        processing: "complete",
        failure_code: "",
        failure_message: "",
        created_at: "2026-07-30T12:00:00Z",
        finalized_at: "2026-07-30T12:00:03Z",
        model_calls: 1,
        tool_calls: 1,
      },
      events: [
        {
          kind: "model",
          created_at: "2026-07-30T12:00:01Z",
          span_name: "score",
          provider: "deepseek",
          model: "v4",
          system_prompt: "EXACT SYSTEM PROMPT",
          user_prompt: "EXACT USER PROMPT",
          completion: "EXACT OUTPUT",
          prompt_tokens: 120,
          completion_tokens: 35,
          latency_ms: 820,
        },
        {
          kind: "tool",
          created_at: "2026-07-30T12:00:02Z",
          tool_name: "web_search",
          tool_kind: "exa_fetch",
          arguments: { query: "official Vane update" },
          result_preview: "{\"items\":[]}",
          result_size: 40000,
          result_truncated: true,
          duration_ms: 320,
        },
      ],
    });

    render(<ExecutionTraces />);

    expect(await screen.findByText("Boss")).toBeTruthy();
    expect(await screen.findByText("Vane 官方更新交叉核验")).toBeTruthy();
    await waitFor(() =>
      expect(detail).toHaveBeenCalledWith(7, 11, "task-v5", 19),
    );
    expect(await screen.findByText("EXACT SYSTEM PROMPT")).toBeTruthy();
    expect(screen.getAllByText("历史结果已截断").length).toBeGreaterThan(0);
    expect(
      screen.getByText(/以下不是完整上游响应/),
    ).toBeTruthy();
    expect(screen.getByText(/official Vane update/)).toBeTruthy();
  });
});
