import { describe, expect, test } from "vitest";
import {
  canonicalCheckOutcome,
  taskRunOutcome,
} from "@/shared/utils/task-detail-presentation";
import {
  safeBriefMarkdownURL,
  safeBriefURL,
} from "@/shared/utils/brief-presentation";

describe("task detail content presentation", () => {
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
