import { describe, expect, test } from "vitest";
import {
  canonicalCheckOutcome,
  taskContentTimestamp,
  taskRunOutcome,
} from "@/lib/task-detail-presentation";
import {
  safeBriefMarkdownURL,
  safeBriefURL,
} from "@/lib/brief-presentation";

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

  test("keeps latest terminal check independent from Brief presence", () => {
    expect(
      canonicalCheckOutcome({
        finalized_at: "2026-07-27T01:00:00Z",
        result: "quiet",
        source_coverage: "complete",
        processing: "complete",
      }),
    ).toBe("no_important_change");
    expect(
      canonicalCheckOutcome({
        finalized_at: "2026-07-27T02:00:00Z",
        result: "content",
        source_coverage: "complete",
        processing: "partial",
      }),
    ).toBe("incomplete");
  });

  test.each([
    ["https://example.com/a", "https://example.com/a"],
    ["http://example.com/a", "http://example.com/a"],
    ["javascript:alert(1)", null],
    ["data:text/html,boom", null],
    ["https://user:secret@example.com/", null],
    ["not a URL", null],
  ])("allows only credential-free HTTP(S) Brief URLs", (raw, expected) => {
    expect(safeBriefURL(raw)).toBe(expected);
    expect(safeBriefMarkdownURL(raw)).toBe(expected ?? "");
  });
});
