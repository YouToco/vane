// @vitest-environment jsdom

import React from "react";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, test, vi } from "vitest";
import ExplorationPanel, {
  type ExplorationCopy,
  type ExplorationItem,
} from "@/components/ExplorationPanel";
import TaskHealthPanel, {
  type TaskHealthCopy,
  type TaskHealthProjection,
} from "@/components/TaskHealthPanel";

afterEach(cleanup);

const explorationCopy: ExplorationCopy = {
  title: "Explore",
  description: "A small view beyond your ordinary brief.",
  empty: "Nothing useful outside the boundary right now.",
  reasons: {
    challenges_judgment: "Challenges your current view",
    adjacent_opportunity: "Adjacent opportunity",
    new_source: "New source",
  },
  feedback: {
    inspiring: "Useful",
    off_target: "Off target",
    mute_direction: "Show less of this direction",
  },
  feedbackFailed: "Could not save feedback",
  feedbackSaved: "Feedback saved",
  source: "Source",
};

const healthCopy: TaskHealthCopy = {
  title: "Task status",
  usageTitle: "Usage",
  accessTitle: "Your access",
  knownCost: "Known cost",
  states: {
    healthy: "Healthy",
    attention: "Needs attention",
    waiting: "Waiting",
    never_run: "Never run",
  },
  issues: {
    coverage_incomplete: "Coverage was incomplete",
    acquisition_unavailable: "Acquisition is unavailable",
    quota_paused: "Usage limit reached",
    model_temporarily_unavailable: "Analysis is temporarily unavailable",
    delivery_failed: "Delivery failed",
    check_interrupted: "Check was interrupted",
    check_failed: "Check failed",
  },
  actions: {
    wait_for_retry: "Wait for retry",
    review_task: "Review task",
    review_usage: "Review usage",
    review_delivery: "Review delivery",
    run_again: "Run again",
    contact_support: "Contact support",
  },
  coverage: {
    none: "No attributed cost",
    llm_only: "LLM cost only; tool cost is not yet attributed",
    tools_only: "Tool cost only; LLM cost is not yet attributed",
    llm_and_tools: "LLM and tool costs included",
  },
  budget: {
    not_configured: "No budget configured",
    ok: "Within budget",
    warning: "Near budget",
    exhausted: "Budget used",
    incomplete: "Budget status incomplete",
  },
  roles: {
    owner: "Owner",
    admin: "Admin",
    member: "Member",
    unknown: "Read-only",
  },
  allowedActions: "Allowed actions",
  capabilities: {
    run: "Run",
    pause: "Pause",
    edit: "Edit",
    delete: "Delete",
    viewUsage: "View usage",
  },
  readOnly: "Read-only",
  usageUnavailable: "Usage unavailable",
};

function explorationItem(id: number): ExplorationItem {
  return {
    content_item_id: id,
    direction_key: "a".repeat(64),
    title: `Boundary signal ${id}`,
    summary: "Evidence-backed context.",
    source_title: "Official source",
    source_url: "https://example.com/read",
    reason: "new_source",
  };
}

function health(): TaskHealthProjection {
  return {
    schema_version: "vane.task-health/v1",
    state: "attention",
    issue: "coverage_incomplete",
    recommended_action: "wait_for_retry",
    acquisition: {
      total: 2,
      failing: 0,
      max_fail_count: 0,
    },
    usage: {
      known_cost_usd: 1.25,
      coverage: "llm_only",
      budget_state: "not_configured",
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

describe("Web insight surfaces", () => {
  test("caps exploration at three and has no unread or push affordance", () => {
    render(
      <ExplorationPanel
        scopeKey="task-a"
        items={[1, 2, 3, 4].map(explorationItem)}
        copy={explorationCopy}
      />,
    );
    expect(screen.getAllByText(/Boundary signal/)).toHaveLength(3);
    expect(screen.queryByText(/unread/i)).toBeNull();
    expect(screen.queryByText(/push/i)).toBeNull();
  });

  test("drops credential-bearing links and submits stable direction feedback", async () => {
    const item = explorationItem(1);
    item.source_url = "https://example.com/read?token=secret";
    const onFeedback = vi.fn().mockResolvedValue(undefined);
    render(
      <ExplorationPanel
        scopeKey="task-a"
        items={[item]}
        copy={explorationCopy}
        onFeedback={onFeedback}
      />,
    );
    expect(screen.queryByRole("link")).toBeNull();
    fireEvent.click(screen.getByText("Show less of this direction"));
    await waitFor(() =>
      expect(onFeedback).toHaveBeenCalledWith(item, "mute_direction"),
    );
    expect(screen.getByRole("status").textContent).toBe("Feedback saved");
    expect(
      (screen.getByText("Show less of this direction") as HTMLButtonElement)
        .disabled,
    ).toBe(true);
  });

  test("labels known partial cost instead of claiming a total", () => {
    render(
      <TaskHealthPanel
        health={health()}
        copy={healthCopy}
        locale="en"
      />,
    );
    expect(screen.getByText(/Known cost/)).toBeTruthy();
    expect(
      screen.getByText((content) =>
        content.includes("LLM cost only; tool cost is not yet attributed"),
      ),
    ).toBeTruthy();
    expect(screen.queryByText(/total cost/i)).toBeNull();
  });

  test("hides usage and write authority for a read-only member", () => {
    const value = health();
    value.permissions = {
      role: "member",
      can_run: false,
      can_pause: false,
      can_edit: false,
      can_delete: false,
      can_view_usage: false,
    };
    delete value.usage;
    render(
      <TaskHealthPanel
        health={value}
        copy={healthCopy}
        locale="en"
      />,
    );
    expect(screen.queryByText(/Known cost/)).toBeNull();
    expect(screen.getAllByText("Read-only").length).toBeGreaterThan(0);
  });

  test("does not expose permission-gated recommended actions", () => {
    const value = health();
    value.recommended_action = "run_again";
    value.permissions = {
      role: "member",
      can_run: false,
      can_pause: false,
      can_edit: false,
      can_delete: false,
      can_view_usage: false,
    };
    delete value.usage;
    const onAction = vi.fn();
    render(
      <TaskHealthPanel
        health={value}
        copy={healthCopy}
        locale="en"
        onAction={onAction}
      />,
    );
    expect(screen.queryByText("Run again")).toBeNull();
  });

  test("lists partial permissions instead of claiming global management", () => {
    const value = health();
    value.permissions = {
      role: "admin",
      can_run: true,
      can_pause: true,
      can_edit: false,
      can_delete: false,
      can_view_usage: true,
    };
    render(
      <TaskHealthPanel
        health={value}
        copy={healthCopy}
        locale="en"
      />,
    );
    expect(
      screen.getByText("Allowed actions: Run · Pause · View usage"),
    ).toBeTruthy();
    expect(screen.queryByText(/Edit · Delete/)).toBeNull();
  });

  test("applies mobile-safe wrapping to long unbroken exploration text", () => {
    const item = explorationItem(1);
    item.title = "A".repeat(240);
    item.summary = "B".repeat(1200);
    item.source_title = "C".repeat(240);
    render(
      <ExplorationPanel
        scopeKey="task-long"
        items={[item]}
        copy={explorationCopy}
      />,
    );
    for (const text of [item.title, item.summary]) {
      expect(screen.getByText(text).className).toContain("overflow-wrap:anywhere");
    }
    expect(screen.getByRole("link").className).toContain("overflow-wrap:anywhere");
  });

  test("ignores feedback completion after the task scope changes", async () => {
    let rejectFeedback!: () => void;
    const promise = new Promise<void>((_, reject) => {
      rejectFeedback = () => reject(new Error("old task failed"));
    });
    const onFeedback = vi.fn().mockReturnValue(promise);
    const rendered = render(
      <ExplorationPanel
        scopeKey="task-a"
        items={[explorationItem(1)]}
        copy={explorationCopy}
        onFeedback={onFeedback}
      />,
    );
    fireEvent.click(screen.getByText("Useful"));
    rendered.rerender(
      <ExplorationPanel
        scopeKey="task-b"
        items={[explorationItem(1)]}
        copy={explorationCopy}
        onFeedback={onFeedback}
      />,
    );
    rejectFeedback();
    await waitFor(() => expect(screen.queryByRole("alert")).toBeNull());
    expect((screen.getByText("Useful") as HTMLButtonElement).disabled).toBe(false);
  });
});
