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
});
