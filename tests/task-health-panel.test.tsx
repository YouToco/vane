// @vitest-environment jsdom

import { render, screen } from "@testing-library/react";
import { describe, expect, test } from "vitest";

import TaskHealthPanel from "@/components/TaskHealthPanel";
import { taskHealthCopy } from "@/i18n/task-health";
import type { TaskHealthProjection } from "@/api";

function health(
  failureReason: NonNullable<
    TaskHealthProjection["acquisition"]["failure_reason"]
  >,
  action: NonNullable<TaskHealthProjection["recommended_action"]>,
): TaskHealthProjection {
  return {
    schema_version: "vane.task-health/v1",
    state: "attention",
    issue: "acquisition_unavailable",
    recommended_action: action,
    acquisition: {
      total: 1,
      failing: 1,
      max_fail_count: 0,
      failure_reason: failureReason,
    },
    permissions: {
      role: "owner",
      can_run: true,
      can_pause: true,
      can_edit: true,
      can_delete: true,
      can_view_usage: true,
    },
  };
}

describe("TaskHealthPanel failure guidance", () => {
  test("renders support and usage guidance without dead buttons", () => {
    const { rerender } = render(
      <TaskHealthPanel
        health={health("internal", "contact_support")}
        copy={taskHealthCopy("zh")}
        locale="zh"
        onAction={() => undefined}
      />,
    );
    expect(screen.getAllByText(/联系支持/).length).toBeGreaterThan(0);
    expect(screen.queryByRole("button")).toBeNull();

    rerender(
      <TaskHealthPanel
        health={health("usage_limit", "review_usage")}
        copy={taskHealthCopy("zh")}
        locale="zh"
        onAction={() => undefined}
      />,
    );
    expect(screen.getAllByText(/查看用量/).length).toBeGreaterThan(0);
    expect(screen.queryByRole("button")).toBeNull();
  });

  test("renders exact token breakdown and multi-currency known costs", () => {
    const value = health("provider_error", "wait_for_retry");
    value.usage = {
      known_cost_usd: 0.25,
      known_costs: [
        { currency: "USD", amount: 0.25 },
        { currency: "CNY", amount: 1.8 },
      ],
      coverage: "llm_and_tools_partial",
      llm_calls: 2,
      llm_priced_calls: 2,
      llm_estimated_calls: 1,
      tool_calls: 1,
      tool_priced_calls: 1,
      tool_estimated_calls: 0,
      prompt_tokens: 1200,
      prompt_cache_hit_tokens: 200,
      prompt_cache_miss_tokens: 1000,
      completion_tokens: 300,
      reasoning_tokens: 80,
      budget_state: "incomplete",
    };
    render(
      <TaskHealthPanel
        health={value}
        copy={taskHealthCopy("zh")}
        locale="zh-CN"
      />,
    );
    expect(screen.getByText(/输入 token 1,200/)).toBeTruthy();
    expect(screen.getByText(/缓存命中.*200.*1,000/)).toBeTruthy();
    expect(screen.getByText(/使用估算价格的调用/)).toBeTruthy();
    expect(screen.getByText(/US\$0\.25.*¥1\.8/)).toBeTruthy();
  });
});
