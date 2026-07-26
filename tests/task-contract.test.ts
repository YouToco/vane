import { describe, expect, test } from "vitest";
import {
  normalizeSchedule,
  normalizeScheduleDetail,
  type ScheduleRunSummary,
} from "@/api";
import {
  nextRunPresentation,
  taskDefinitionEditEnabled,
} from "@/lib/task-detail-contract";

const summary: ScheduleRunSummary = {
  schedule_id: "task-1",
  last_status: "",
  last_exit_gate: "",
  batches_7d: 0,
  empty_batches_7d: 0,
  sent_pushes_7d: 0,
  source_count: 0,
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
    sources: null,
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
