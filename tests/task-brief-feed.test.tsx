// @vitest-environment jsdom

import React from "react";
import { renderToStaticMarkup } from "react-dom/server";
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";

const apiMock = vi.hoisted(() => ({
  scheduleBriefs: vi.fn(),
}));

vi.mock("@/api", () => ({
  api: apiMock,
  ApiError: class ApiError extends Error {
    status: number;
    constructor(status: number, message: string) {
      super(message);
      this.status = status;
    }
  },
}));

vi.mock("@/i18n", () => ({
  fmt: (template: string, values: Record<string, string | number>) =>
    template.replace(/\{(\w+)\}/g, (_: string, key: string) =>
      String(values[key] ?? ""),
    ),
  useI18n: () => ({
    locale: "en",
    t: {
      app: {
        common: {
          loadFailed: "Load failed",
          loading: "Loading",
          loadMore: "Load more",
        },
        taskDetail: {
          briefFeedbackInterested: "Interested",
          briefFeedbackNotInterested: "Not interested",
          briefFeedbackIssue: "Issue",
          briefFeedbackDeepDive: "Deep dive",
          briefPartial: "Partial",
          briefTitle: "Brief",
          briefInsightCount: "{n} insights",
          briefUnknownSource: "Unknown",
          briefPublished: "Published",
          briefDiscovered: "Discovered",
          briefFeedback: "Feedback",
          briefsShown: "{shown} / {total}",
          briefsEmpty: "No briefs",
        },
      },
    },
  }),
}));

import TaskBriefFeed, {
  BriefInsightBody,
  InsightBody,
} from "@/components/TaskBriefFeed";
import type {
  TaskBrief,
  TaskBriefsResp,
  TaskBriefStructuredInsight,
} from "@/api";
import { briefDict } from "@/i18n/brief";

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

function page(
  task: string,
  id: number,
  nextPageToken?: string,
): TaskBriefsResp {
  const brief: TaskBrief = {
    id,
    push_batch_id: id,
    generated_at: "2026-07-27T10:00:00Z",
    source_coverage: "complete",
    processing: "complete",
    insights: [
      {
        id,
        rank_position: 1,
        title: `${task} insight ${id}`,
        body_md: `${task} body`,
        source_title: `${task} source`,
        source_url: "https://example.com/read",
        discovered_at: "2026-07-27T09:00:00Z",
        feedback: {
          misjudged: false,
          deep_dive_requested: false,
        },
      },
    ],
  };
  return {
    items: [brief],
    total: nextPageToken ? 2 : 1,
    next_page_token: nextPageToken,
    latest_check: {
      finalized_at: "2026-07-27T10:00:00Z",
      result: "content",
      source_coverage: "complete",
      processing: "complete",
      failure_code: task,
    },
  };
}

describe("TaskBriefFeed Markdown boundary", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    cleanup();
  });

  test("drops raw HTML, external images, and unsafe link schemes", () => {
    const html = renderToStaticMarkup(
      <InsightBody
        markdown={[
          "<script>alert('raw')</script>",
          "[unsafe](javascript:alert(1))",
          "![tracker](https://tracker.example/pixel.png)",
          "[safe](https://example.com/read)",
        ].join("\n\n")}
      />,
    );

    expect(html).not.toContain("<script");
    expect(html).not.toContain("<img");
    expect(html).not.toContain("javascript:");
    expect(html).not.toContain("tracker.example");
    expect(html).toContain('href="https://example.com/read"');
    expect(html).toContain('rel="noopener noreferrer"');
  });

  test("keeps heading text and constrains wide GFM tables", () => {
    const html = renderToStaticMarkup(
      <InsightBody
        markdown={[
          "# Core conclusion",
          "",
          "| Long column | Value |",
          "| --- | --- |",
          "| https://example.com/an/unbroken/path | kept |",
        ].join("\n")}
      />,
    );

    expect(html).toContain("<h1>Core conclusion</h1>");
    expect(html).toContain("overflow-x-auto");
    expect(html).toContain("min-w-full");
    expect(html).toContain("break-all");
  });

  test("renders a frozen structured insight and its cited excerpts", () => {
    const insight = page("structured", 7).items[0].insights[0];
    insight.structured = {
      schema_version: "vane.cardgen-insight/v1",
      body_md: insight.body_md,
      what_changed: "A new release changed the API.",
      why_it_matters: "The monitored integration depends on it.",
      importance_reason: "The source lists a breaking change.",
      claims: [
        {
          text: "The release contains a breaking change.",
          excerpt: "This release contains a breaking API change.",
          source_refs: ["source-1"],
        },
      ],
    };
    const html = renderToStaticMarkup(
      <BriefInsightBody insight={insight} d={briefDict("en")} />,
    );

    expect(html).toContain("What changed");
    expect(html).toContain("A new release changed the API.");
    expect(html).toContain("Why it matters");
    expect(html).toContain("Why it is important");
    expect(html).toContain("Verifiable evidence");
    expect(html).toContain("This release contains a breaking API change.");
    expect(html).not.toContain("structured body");
    expect(html).not.toContain("source-1");
  });

  test("falls back to body_md for an incomplete structured extension", () => {
    const insight = page("fallback", 8).items[0].insights[0];
    insight.structured = {
      schema_version: "vane.cardgen-insight/v1",
      body_md: insight.body_md,
      what_changed: "Only one field is present.",
      why_it_matters: "",
      importance_reason: "",
      claims: [],
    };
    const html = renderToStaticMarkup(
      <BriefInsightBody insight={insight} d={briefDict("en")} />,
    );

    expect(html).toContain("fallback body");
    expect(html).not.toContain("What changed");
  });

  test("renders the structured trio safely when claims is null", () => {
    const insight = page("structured-null-claims", 9).items[0].insights[0];
    insight.structured = {
      schema_version: "vane.cardgen-insight/v1",
      body_md: insight.body_md,
      what_changed: "A new release changed the API.",
      why_it_matters: "The monitored integration depends on it.",
      importance_reason: "The source lists a breaking change.",
      claims: null as unknown as TaskBriefStructuredInsight["claims"],
    };
    const html = renderToStaticMarkup(
      <BriefInsightBody insight={insight} d={briefDict("en")} />,
    );

    expect(html).toContain("A new release changed the API.");
    expect(html).not.toContain("Verifiable evidence");
  });

  test("discards an old task load-more response after navigation", async () => {
    const oldLoadMore = deferred<TaskBriefsResp>();
    apiMock.scheduleBriefs.mockImplementation(
      (scheduleID: string, _pageSize: number, token?: string) => {
        if (scheduleID === "task-a" && token === "task-a-next") {
          return oldLoadMore.promise;
        }
        if (scheduleID === "task-a") {
          return Promise.resolve(page("task-a", 1, "task-a-next"));
        }
        return Promise.resolve(page("task-b", 3));
      },
    );
    const onLatestCheck = vi.fn();

    const view = render(
      <TaskBriefFeed
        scheduleID="task-a"
        onLatestCheck={onLatestCheck}
      />,
    );
    await screen.findByText("task-a insight 1");

    fireEvent.click(screen.getByRole("button", { name: "Load more" }));
    await waitFor(() => {
      expect(apiMock.scheduleBriefs).toHaveBeenCalledWith(
        "task-a",
        10,
        "task-a-next",
      );
    });

    await act(async () => {
      view.rerender(
        <TaskBriefFeed
          scheduleID="task-b"
          onLatestCheck={onLatestCheck}
        />,
      );
    });
    await screen.findByText("task-b insight 3");
    expect(screen.queryByText(/task-a insight/)).toBeNull();

    await act(async () => {
      oldLoadMore.resolve(page("task-a-stale", 2));
    });
    expect(screen.getByText("task-b insight 3")).toBeTruthy();
    expect(screen.queryByText(/task-a/)).toBeNull();
    expect(onLatestCheck).toHaveBeenCalledTimes(2);
    expect(onLatestCheck.mock.calls.at(-1)?.[0]?.failure_code).toBe("task-b");
    view.unmount();
  });
});
