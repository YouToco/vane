import { describe, expect, test } from "vitest";
import {
  normalizeSchedule,
  normalizeScheduleDetail,
  normalizeTaskHealth,
  type ScheduleRunSummary,
} from "@/api";
import {
  nextRunPresentation,
  taskDefinitionEditEnabled,
} from "@/lib/task-detail-contract";
import { LOCALES } from "@/i18n";
import {
  taskHealthCopy,
  taskHealthLoadingCopy,
  taskHealthUnavailableCopy,
} from "@/i18n/task-health";

const summary: ScheduleRunSummary = {
  schedule_id: "task-1",
  last_status: "",
  last_exit_gate: "",
  batches_7d: 0,
  empty_batches_7d: 0,
  sent_pushes_7d: 0,
};

function rawDetail(capabilities?: unknown) {
  return {
    schedule: {
      id: "task-1",
      nl_description: "Track official updates",
      spec: {},
      scope: {},
      status: "active",
      next_run_state: "none",
    },
    summary,
    cost: { llm_cost_usd: 0, llm_calls: 0 },
    ...(capabilities === undefined ? {} : { capabilities }),
  };
}

describe("task detail capability contract", () => {
  test.each([
    {
      name: "enabled",
      capabilities: { definition_edit: true },
      expected: true,
    },
    {
      name: "disabled",
      capabilities: { definition_edit: false },
      expected: false,
    },
    {
      name: "legacy missing",
      capabilities: undefined,
      expected: false,
    },
  ])("$name definition edit capability", ({ capabilities, expected }) => {
    const detail = normalizeScheduleDetail(rawDetail(capabilities));
    expect(detail.capabilities.definition_edit).toBe(expected);
    expect(taskDefinitionEditEnabled(detail)).toBe(expected);
  });
});

describe("task health projection contract", () => {
  const raw = {
    schema_version: "vane.task-health/v1",
    state: "healthy",
    acquisition: { total: 2, failing: 0, max_fail_count: 0 },
    usage: {
      known_cost_usd: 1.25,
      coverage: "llm_only",
      llm_calls: 3,
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

  test("accepts the exact server projection", () => {
    expect(normalizeTaskHealth(raw)).toEqual(raw);
  });

  test("fails closed when permissions or acquisition shape are not exact", () => {
    expect(
      normalizeTaskHealth({
        ...raw,
        permissions: { ...raw.permissions, can_delete: "yes" },
      }),
    ).toBeUndefined();
    expect(
      normalizeTaskHealth({
        ...raw,
        acquisition: { total: 1, failing: 2, max_fail_count: 2 },
      }),
    ).toBeUndefined();
  });

  test("does not expose usage when the account lacks usage permission", () => {
    const health = normalizeTaskHealth({
      ...raw,
      permissions: { ...raw.permissions, can_view_usage: false },
    });
    expect(health).toBeDefined();
    expect(health).not.toHaveProperty("usage");
  });
});

describe("task health localization contract", () => {
  test("every supported locale has its own task health copy", () => {
    const english = taskHealthCopy("en");
    for (const { code } of LOCALES) {
      const copy = taskHealthCopy(code);
      expect(copy.title).toBeTruthy();
      expect(taskHealthLoadingCopy(code)).toBeTruthy();
      expect(taskHealthUnavailableCopy(code)).toBeTruthy();
      if (code !== "en") {
        expect(copy.title).not.toBe(english.title);
      }
    }
    expect(taskHealthCopy("zh-Hant").title).not.toBe(
      taskHealthCopy("zh").title,
    );
  });
});

describe("next run state contract", () => {
  test.each([
    {
      state: "scheduled",
      nextRun: "2026-07-27T01:00:00Z",
      expected: {
        kind: "scheduled",
        at: "2026-07-27T01:00:00Z",
      },
    },
    {
      state: "paused",
      nextRun: undefined,
      expected: { kind: "paused" },
    },
    {
      state: "none",
      nextRun: undefined,
      expected: { kind: "none" },
    },
    {
      state: "unavailable",
      nextRun: undefined,
      expected: { kind: "unavailable" },
    },
    {
      state: undefined,
      nextRun: "2026-07-27T01:00:00Z",
      expected: { kind: "unavailable" },
    },
  ])("normalizes $state", ({ state, nextRun, expected }) => {
    const schedule = normalizeSchedule({
      id: "task-1",
      nl_description: "Track official updates",
      spec: {},
      scope: {},
      status: state === "paused" ? "paused" : "active",
      ...(state ? { next_run_state: state } : {}),
      ...(nextRun ? { next_run: nextRun } : {}),
    });
    expect(nextRunPresentation(schedule)).toEqual(expected);
  });

  test("scheduled without a timestamp fails closed as unavailable", () => {
    const schedule = normalizeSchedule({
      id: "task-1",
      spec: {},
      scope: {},
      status: "active",
      next_run_state: "scheduled",
    });
    expect(nextRunPresentation(schedule)).toEqual({ kind: "unavailable" });
  });
});
