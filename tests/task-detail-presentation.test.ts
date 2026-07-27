import { describe, expect, test } from "vitest";
import {
  taskContentTimestamp,
  taskRunOutcome,
} from "@/lib/task-detail-presentation";

describe("task detail content presentation", () => {
  test("uses discovery time instead of delivery time for task content", () => {
    expect(
      taskContentTimestamp({
        created_at: "2026-07-27T01:02:03Z",
        sent_at: "2026-07-27T04:05:06Z",
      }),
    ).toBe("2026-07-27T01:02:03Z");
  });

  test.each([
    ["done", "completed"],
    ["empty", "no_important_change"],
    ["failed", "incomplete"],
    ["", "not_run"],
    ["unexpected", "incomplete"],
  ] as const)("maps %s to an honest reader-facing result", (status, expected) => {
    expect(taskRunOutcome(status)).toBe(expected);
  });
});
