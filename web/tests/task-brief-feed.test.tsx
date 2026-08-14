// @vitest-environment jsdom

import React from "react";
import "./insight-surfaces.test";
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
  scheduleReports: vi.fn(),
  reportSettings: vi.fn(),
  patchReportSettings: vi.fn(),
  askBrief: vi.fn(),
  askReport: vi.fn(),
  deepDiveBrief: vi.fn(),
  deepDiveReport: vi.fn(),
  briefGrounding: vi.fn(),
  reportGrounding: vi.fn(),
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
  validatedEventEvidence,
} from "@/components/TaskBriefFeed";
import type {
  ExecutiveContent,
  TaskBrief,
  TaskBriefsResp,
  TaskBriefStructuredInsight,
} from "@/api";
import { ApiError } from "@/api";
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
    health: {
      schema_version: "vane.task-health/v1",
      state: "healthy",
      acquisition: {
        total: 1,
        failing: 0,
        max_fail_count: 0,
      },
      usage: {
        known_cost_usd: id,
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
    },
  };
}

describe("TaskBriefFeed Markdown boundary", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    apiMock.reportSettings.mockResolvedValue({
      mode: "auto",
      cadence: "weekly",
      delivery: "important",
      timezone: "Asia/Shanghai",
    });
    apiMock.scheduleReports.mockResolvedValue({ items: [] });
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

  test("binds claims to the full ordered frozen evidence set", () => {
    const insight = page("event", 10).items[0].insights[0];
    insight.source_url = "https://legacy.example/item";
    insight.structured = {
      schema_version: "vane.cardgen-insight/v1",
      body_md: insight.body_md,
      what_changed: "Two sources confirm the release.",
      why_it_matters: "The integration changes.",
      importance_reason: "Independent evidence agrees.",
      claims: [
        {
          text: "The release is available.",
          excerpt: "Version 2 is now available.",
          source_refs: ["source-2", "source-1"],
        },
      ],
    };
    insight.event_evidence = {
      schema_version: "vane.structured-event-evidence/v1",
      sources: [
        {
          ref: "source-1",
          title: "Official release",
          source_title: "Vendor",
          platform: "web",
          source_url: "https://vendor.example/release",
          published_at: "2026-07-27T08:00:00Z",
          discovered_at: "2026-07-27T09:00:00Z",
        },
        {
          ref: "source-2",
          title: "Release coverage",
          source_title: "Industry Wire",
          platform: "rss",
          source_url: "https://wire.example/release",
          discovered_at: "2026-07-27T09:05:00Z",
        },
      ],
    };
    const html = renderToStaticMarkup(
      <BriefInsightBody insight={insight} d={briefDict("en")} />,
    );

    expect(html).toContain("Supporting sources");
    expect(html).toContain("All evidence sources");
    expect(html).toContain('href="https://vendor.example/release"');
    expect(html).toContain('href="https://wire.example/release"');
    expect(html.indexOf("Official release")).toBeLessThan(
      html.indexOf("Release coverage"),
    );
    expect(html.indexOf(">Industry Wire<")).toBeLessThan(
      html.indexOf(">Vendor<"),
    );
    expect(html).not.toContain("https://legacy.example/item");
  });

  test("fails an unsafe evidence extension closed to structured presentation", () => {
    const insight = page("unsafe-event", 11).items[0].insights[0];
    insight.structured = {
      schema_version: "vane.cardgen-insight/v1",
      body_md: insight.body_md,
      what_changed: "Safe structured field.",
      why_it_matters: "Safe relevance.",
      importance_reason: "Safe reason.",
      claims: [
        {
          text: "Safe claim.",
          excerpt: "Safe excerpt.",
          source_refs: ["source-1"],
        },
      ],
    };
    insight.event_evidence = {
      schema_version: "vane.structured-event-evidence/v1",
      sources: [
        {
          ref: "source-1",
          title: "Unsafe target",
          source_title: "Unsafe",
          platform: "web",
          source_url: "javascript:alert(1)",
          discovered_at: "2026-07-27T09:00:00Z",
        },
      ],
    };
    const html = renderToStaticMarkup(
      <BriefInsightBody insight={insight} d={briefDict("en")} />,
    );

    expect(html).toContain("Safe structured field.");
    expect(html).toContain("Safe excerpt.");
    expect(html).not.toContain("All evidence sources");
    expect(html).not.toContain("Supporting sources");
    expect(html).not.toContain("javascript:");
  });

  test("fails an unresolved claim reference closed without inventing a source", () => {
    const insight = page("missing-ref", 12).items[0].insights[0];
    insight.structured = {
      schema_version: "vane.cardgen-insight/v1",
      body_md: insight.body_md,
      what_changed: "Structured field remains readable.",
      why_it_matters: "Still useful.",
      importance_reason: "Existing validated presentation.",
      claims: [
        {
          text: "Claim with a missing mapping.",
          excerpt: "Excerpt stays visible.",
          source_refs: ["source-2"],
        },
      ],
    };
    insight.event_evidence = {
      schema_version: "vane.structured-event-evidence/v1",
      sources: [
        {
          ref: "source-1",
          title: "Only source",
          source_title: "Source",
          platform: "web",
          source_url: "https://source.example/item",
          discovered_at: "2026-07-27T09:00:00Z",
        },
      ],
    };
    const html = renderToStaticMarkup(
      <BriefInsightBody insight={insight} d={briefDict("en")} />,
    );

    expect(html).toContain("Structured field remains readable.");
    expect(html).toContain("Excerpt stays visible.");
    expect(html).not.toContain("All evidence sources");
    expect(html).not.toContain("https://source.example/item");
    expect(html).not.toContain("source-2");
  });

  test("full feed replaces the legacy source link with frozen evidence", async () => {
    const response = page("channel", 13);
    const insight = response.items[0].insights[0];
    insight.source_url = "https://legacy.example/item";
    insight.structured = {
      schema_version: "vane.cardgen-insight/v1",
      body_md: insight.body_md,
      what_changed: "A release changed.",
      why_it_matters: "The task depends on it.",
      importance_reason: "The frozen source confirms it.",
      claims: [
        {
          text: "A release changed.",
          excerpt: "Release notes.",
          source_refs: ["source-1"],
        },
      ],
    };
    insight.event_evidence = {
      schema_version: "vane.structured-event-evidence/v1",
      sources: [
        {
          ref: "source-1",
          title: "Frozen release notes",
          source_title: "Vendor",
          platform: "web",
          source_url: "https://vendor.example/release",
          discovered_at: "2026-07-27T09:00:00Z",
        },
      ],
    };
    apiMock.scheduleBriefs.mockResolvedValue(response);

    const view = render(<TaskBriefFeed scheduleID="task-channel" />);
    await screen.findByText("channel insight 13");

    expect(view.container.innerHTML).toContain(
      'href="https://vendor.example/release"',
    );
    expect(view.container.innerHTML).not.toContain(
      "https://legacy.example/item",
    );
  });

  test("routes a frozen deep-dive step through existing feedback", async () => {
    const response = page("executive", 41);
    response.items[0].executive = {
      generation_mode: "model",
      processing: "complete",
      generated_at: "2026-07-27T10:00:00Z",
      content: {
        headline: "Act on the supplier change",
        executive_summary: "A verified change affects the buying window.",
        decision_state: "act",
        why_for_you: "Your monitored role owns this dependency.",
        signals: [
          {
            kind: "risk",
            title: "Lead time increased",
            summary: "The supplier published a longer lead time.",
            evidence_refs: [
              { insight_id: 41, claim_indexes: [0] },
            ],
          },
        ],
        next_steps: [
          {
            kind: "deep_dive",
            label: "Investigate now",
            rationale: "Understand the operational impact.",
            evidence_refs: [
              { insight_id: 41, claim_indexes: [0] },
            ],
          },
        ],
      },
    };
    apiMock.scheduleBriefs.mockResolvedValue(response);
    apiMock.deepDiveBrief.mockResolvedValue({
      message: "Deep dive is being generated",
      accepted: true,
    });

    render(<TaskBriefFeed scheduleID="task-executive" />);
    await screen.findByText("Act on the supplier change");
    fireEvent.click(
      screen.getByRole("button", { name: "Investigate now" }),
    );

    await waitFor(() => {
      expect(apiMock.deepDiveBrief).toHaveBeenCalledWith(
        "task-executive", 41, 41,
      );
    });
    expect(await screen.findByText("Deep dive is being generated")).toBeTruthy();
    expect(apiMock.askBrief).not.toHaveBeenCalled();
  });

  test("renders historical fallback content with nullable arrays", async () => {
    const response = page("fallback", 42);
    response.items[0].executive = {
      generation_mode: "deterministic_fallback",
      processing: "partial",
      generated_at: "2026-07-27T10:00:00Z",
      content: {
        headline: "Evidence is not sufficient yet",
        executive_summary: "The original item remains available.",
        decision_state: "insufficient_evidence",
        why_for_you: "No reliable personal impact can be stated yet.",
        signals: null as unknown as ExecutiveContent["signals"],
        next_steps: null as unknown as ExecutiveContent["next_steps"],
      },
    };
    apiMock.scheduleBriefs.mockResolvedValue(response);

    render(<TaskBriefFeed scheduleID="task-fallback" />);

    expect(
      await screen.findByText("Evidence is not sufficient yet"),
    ).toBeTruthy();
    expect(
      screen.getByText(
        "A conservative summary was used. Check the underlying evidence.",
      ),
    ).toBeTruthy();
  });

  test("keeps P2-D controls dark when the task is outside rollout", async () => {
    apiMock.scheduleBriefs.mockResolvedValue(page("dark", 51));
    apiMock.reportSettings.mockRejectedValue(
      new ApiError(404, "not enabled"),
    );

    render(<TaskBriefFeed scheduleID="task-dark" />);
    await screen.findByText("dark insight 51");

    expect(screen.queryByRole("tab", { name: "Daily" })).toBeNull();
    expect(screen.queryByRole("button", { name: "Report settings" })).toBeNull();
    expect(apiMock.scheduleReports).not.toHaveBeenCalled();
  });

  test("loads a daily report period for an enabled task", async () => {
    apiMock.scheduleBriefs.mockResolvedValue(page("daily", 52));

    render(<TaskBriefFeed scheduleID="task-daily" />);
    const dailyTab = await screen.findByRole("tab", { name: "Daily" });
    fireEvent.click(dailyTab);

    await waitFor(() => {
      expect(apiMock.scheduleReports).toHaveBeenCalledWith(
        "task-daily", "daily",
      );
    });
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
    insight.event_evidence = {
      schema_version: "vane.structured-event-evidence/v1",
      sources: [
        {
          ref: "source-1",
          title: "Source that must not suppress legacy presentation",
          source_title: "Source",
          platform: "web",
          source_url: "https://source.example/item",
          discovered_at: "2026-07-27T09:00:00Z",
        },
      ],
    };
    const html = renderToStaticMarkup(
      <BriefInsightBody insight={insight} d={briefDict("en")} />,
    );

    expect(html).toContain("fallback body");
    expect(html).not.toContain("What changed");
    expect(validatedEventEvidence(insight)).toBeNull();
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
    const onHealth = vi.fn();

    const view = render(
      <TaskBriefFeed
        scheduleID="task-a"
        onLatestCheck={onLatestCheck}
        onHealth={onHealth}
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
          onHealth={onHealth}
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
    expect(onHealth).toHaveBeenCalledTimes(2);
    expect(onHealth.mock.calls.at(-1)?.[0]?.usage?.known_cost_usd).toBe(3);
    view.unmount();
  });
});
